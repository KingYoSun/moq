//! Match the playback reference to the time the microphone samples were captured.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use fixed_resample::{Interleaved, PacketResampler, ResamplerConfig};
#[cfg(test)]
use ringbuf::traits::Observer;
use ringbuf::traits::{Consumer as _, Producer as _, Split};
use ringbuf::{HeapCons, HeapProd, HeapRb};
use rubato::audioadapter_buffers::direct::InterleavedSlice;

use super::{REFERENCE_CHANNELS, REFERENCE_FRAME, REFERENCE_RATE};

const CAPACITY: usize = 256;
const RETAIN: Duration = Duration::from_millis(500);

/// Map each device clock to monotonic time. A late callback may not move the
/// sample clock; earlier observations only remove initial dispatch latency.
#[derive(Default)]
pub(crate) struct Clock {
	anchor: Option<(cpal::StreamInstant, Instant)>,
}

impl Clock {
	pub(crate) fn at(&mut self, callback: cpal::StreamInstant, sample: cpal::StreamInstant) -> Instant {
		self.observe(callback, Instant::now());
		self.project(sample)
	}

	fn observe(&mut self, callback: cpal::StreamInstant, now: Instant) {
		self.anchor.get_or_insert((callback, now));
		let predicted = self.project(callback);
		if predicted > now {
			self.anchor.as_mut().unwrap().1 -= predicted.duration_since(now);
		}
	}

	fn project(&self, sample: cpal::StreamInstant) -> Instant {
		let (stream, monotonic) = self.anchor.expect("device clock observed");
		if sample >= stream {
			monotonic + sample.duration_since(stream)
		} else {
			monotonic - stream.duration_since(sample)
		}
	}
}

struct Frame {
	at: Instant,
	len: usize,
	samples: [f32; REFERENCE_FRAME * REFERENCE_CHANNELS],
}

impl Frame {
	fn end(&self) -> Instant {
		self.at + duration(self.len)
	}
}

fn duration(frames: usize) -> Duration {
	Duration::from_secs_f64(frames as f64 / f64::from(REFERENCE_RATE))
}

pub(crate) struct Producer {
	queue: HeapProd<Frame>,
	resampler: Option<PacketResampler<f32, Interleaved<f32>>>,
	rate: u32,
	input: u64,
	output: u64,
}

pub(crate) struct Consumer {
	queue: HeapCons<Frame>,
	history: VecDeque<Frame>,
}

pub(crate) fn channel(rate: u32) -> (Producer, Consumer) {
	let (queue, received) = HeapRb::new(CAPACITY).split();
	let resampler = (rate != REFERENCE_RATE)
		.then(|| PacketResampler::new(REFERENCE_CHANNELS, rate, REFERENCE_RATE, ResamplerConfig::default()));
	(
		Producer {
			queue,
			resampler,
			rate,
			input: 0,
			output: 0,
		},
		Consumer {
			queue: received,
			history: VecDeque::with_capacity(CAPACITY),
		},
	)
}

impl Producer {
	pub(crate) fn push_interleaved(&mut self, samples: &[f32], at: Instant) {
		let pending = self.input as f64 / f64::from(self.rate) - self.output as f64 / f64::from(REFERENCE_RATE);
		let at = if pending >= 0.0 {
			at - Duration::from_secs_f64(pending)
		} else {
			at + Duration::from_secs_f64(-pending)
		};
		let mut produced = 0;
		let queue = &mut self.queue;
		let mut send = |samples: &[f32]| {
			for chunk in samples.chunks(REFERENCE_FRAME * REFERENCE_CHANNELS) {
				let mut frame = Frame {
					at: at + duration(produced),
					len: chunk.len() / REFERENCE_CHANNELS,
					samples: [0.0; REFERENCE_FRAME * REFERENCE_CHANNELS],
				};
				frame.samples[..chunk.len()].copy_from_slice(chunk);
				produced += frame.len;
				// The audio callback cannot wait for a microphone that stopped reading.
				let _ = queue.try_push(frame);
			}
		};
		if let Some(resampler) = &mut self.resampler {
			let input = InterleavedSlice::new(samples, REFERENCE_CHANNELS, samples.len() / REFERENCE_CHANNELS)
				.expect("stereo reference");
			resampler.process(
				&input,
				None,
				None,
				|output, frames| send(&output[..frames * REFERENCE_CHANNELS]),
				None,
				true,
			);
		} else {
			send(samples);
		}
		self.input += (samples.len() / REFERENCE_CHANNELS) as u64;
		self.output += produced as u64;
	}
}

impl Consumer {
	#[cfg(test)]
	pub(crate) fn queued(&self) -> usize {
		self.queue.occupied_len()
	}
	pub(super) fn clear(&mut self) {
		while self.queue.try_pop().is_some() {}
		self.history.clear();
	}

	pub(super) fn available_until(&mut self) -> Option<Instant> {
		self.drain();
		self.history.back().map(Frame::end)
	}

	fn drain(&mut self) {
		while let Some(frame) = self.queue.try_pop() {
			// A shorter device buffer replaces audio that had been queued for later.
			while self.history.back().is_some_and(|last| last.at >= frame.at) {
				self.history.pop_back();
			}
			if let Some(last) = self.history.back_mut()
				&& last.end() > frame.at
			{
				last.len =
					(frame.at.duration_since(last.at).as_secs_f64() * f64::from(REFERENCE_RATE)).round() as usize;
			}
			if self.history.len() == CAPACITY {
				self.history.pop_front();
			}
			self.history.push_back(frame);
		}
	}

	pub(crate) fn fill(&mut self, at: Instant, output: &mut [f32]) {
		self.drain();
		while self
			.history
			.front()
			.is_some_and(|first| at.saturating_duration_since(first.end()) > RETAIN)
		{
			self.history.pop_front();
		}
		output.fill(0.0);
		let mut index = 0;
		for (offset, sample) in output.as_chunks_mut::<REFERENCE_CHANNELS>().0.iter_mut().enumerate() {
			let time = at + duration(offset);
			while index < self.history.len() && self.history[index].end() <= time {
				index += 1;
			}
			let Some(frame) = self.history.get(index) else { break };
			if time < frame.at {
				continue;
			}
			let position = time.duration_since(frame.at).as_secs_f64() * f64::from(REFERENCE_RATE);
			let first = (position.floor() as usize).min(frame.len - 1);
			let fraction = (position - first as f64) as f32;
			for (channel, output_sample) in sample.iter_mut().enumerate() {
				let a = frame.samples[first * REFERENCE_CHANNELS + channel];
				let b = if first + 1 < frame.len {
					frame.samples[(first + 1) * REFERENCE_CHANNELS + channel]
				} else {
					self.history
						.get(index + 1)
						.filter(|next| next.at.saturating_duration_since(frame.end()) < duration(1))
						.map_or(0.0, |next| next.samples[channel])
				};
				*output_sample = a + (b - a) * fraction;
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn sample_time_is_not_reanchored_by_a_later_callback() {
		let origin = Instant::now();
		let mut clock = Clock::default();
		clock.observe(cpal::StreamInstant::from_millis(1_000), origin);
		clock.observe(
			cpal::StreamInstant::from_millis(1_200),
			origin + Duration::from_millis(250),
		);
		assert_eq!(
			clock.project(cpal::StreamInstant::from_millis(1_150)),
			origin + Duration::from_millis(150)
		);
		assert_eq!(
			clock.project(cpal::StreamInstant::from_millis(950)),
			origin - Duration::from_millis(50)
		);
		let mut initially_late = Clock::default();
		initially_late.observe(
			cpal::StreamInstant::from_millis(1_000),
			origin + Duration::from_millis(50),
		);
		initially_late.observe(
			cpal::StreamInstant::from_millis(1_100),
			origin + Duration::from_millis(101),
		);
		assert_eq!(
			initially_late.project(cpal::StreamInstant::from_millis(1_150)),
			origin + Duration::from_millis(151)
		);
	}

	#[test]
	fn looks_up_samples_in_device_time_and_interpolates_between_them() {
		let (mut producer, mut consumer) = channel(48_000);
		let at = Instant::now();
		let samples: Vec<f32> = (0..1_920).flat_map(|i| [i as f32 / 480.0; 2]).collect();
		producer.push_interleaved(&samples, at);
		let mut heard = [0.0; 960];
		consumer.fill(at + duration(480) + Duration::from_secs_f64(0.5 / 48_000.0), &mut heard);
		for (i, frame) in heard.as_chunks::<2>().0.iter().enumerate() {
			let expected = (480.5 + i as f32) / 480.0;
			assert!((frame[0] - expected).abs() < 0.00001, "{i}: {} != {expected}", frame[0]);
			assert_eq!(frame[0], frame[1]);
		}
	}

	#[test]
	fn gaps_are_silence_and_newer_device_times_replace_overlapping_audio() {
		let (mut producer, mut consumer) = channel(48_000);
		let at = Instant::now();
		producer.push_interleaved(&[1.0; 960], at);
		producer.push_interleaved(&[2.0; 960], at + Duration::from_millis(5));
		producer.push_interleaved(&[3.0; 960], at + Duration::from_millis(30));
		let mut heard = [0.0; 960];
		consumer.fill(at, &mut heard);
		assert!(heard[..480].iter().all(|v| (*v - 1.0).abs() < 0.0001));
		assert!(heard[480..].iter().all(|v| (*v - 2.0).abs() < 0.0001));
		consumer.fill(at + Duration::from_millis(20), &mut heard);
		assert!(heard.iter().all(|v| *v == 0.0));
	}

	#[test]
	fn queue_and_history_have_fixed_capacity_and_expire() {
		let (mut producer, mut consumer) = channel(48_000);
		let at = Instant::now();
		for i in 0..CAPACITY * 2 {
			producer.push_interleaved(&[1.0; 960], at + duration(i * 480));
		}
		assert_eq!(consumer.queued(), CAPACITY);
		let mut heard = [0.0; 960];
		consumer.fill(at, &mut heard);
		assert_eq!(consumer.queued(), 0);
		assert_eq!(consumer.history.len(), CAPACITY);
		consumer.fill(at + Duration::from_secs(4), &mut heard);
		assert!(consumer.history.is_empty());
		for i in 400..1_400 {
			producer.push_interleaved(&[1.0; 960], at + duration(i * 480));
			consumer.fill(at + duration(i * 480), &mut heard);
			assert!(consumer.history.len() <= 52);
			assert_eq!(consumer.history.capacity(), CAPACITY);
		}
		consumer.clear();
		assert!(consumer.history.is_empty());
	}

	#[test]
	fn resampling_preserves_the_render_clock() {
		let (mut producer, mut consumer) = channel(44_100);
		let at = Instant::now();
		let source: Vec<f32> = (0..8_820)
			.flat_map(|i| [(std::f32::consts::TAU * 1_000.0 * i as f32 / 44_100.0).sin(); 2])
			.collect();
		for block in 0..20 {
			producer.push_interleaved(
				&source[block * 882..(block + 1) * 882],
				at + Duration::from_millis(block as u64 * 10),
			);
		}
		let mut resampler =
			PacketResampler::<f32, Interleaved<f32>>::new(2, 44_100, 48_000, ResamplerConfig::default());
		let input = InterleavedSlice::new(&source, 2, 8_820).unwrap();
		let mut expected = Vec::new();
		resampler.process(
			&input,
			None,
			None,
			|packet, frames| expected.extend_from_slice(&packet[..frames * 2]),
			None,
			true,
		);
		let mut heard = [0.0; 960];
		consumer.fill(at + Duration::from_millis(100), &mut heard);
		for (actual, expected) in heard.iter().zip(&expected[9_600..10_560]) {
			assert!(
				(actual - expected).abs() < 0.0001,
				"resampled reference shifted: {actual} != {expected}"
			);
		}
	}
}
