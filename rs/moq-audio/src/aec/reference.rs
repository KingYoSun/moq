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
	index: u64,
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
	// The next sample and capture time. Device-time noise must not rewrite PCM.
	next: Option<(u64, Instant)>,
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
			next: None,
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
		let base = self.output;
		let queue = &mut self.queue;
		let mut send = |samples: &[f32]| {
			for chunk in samples.chunks(REFERENCE_FRAME * REFERENCE_CHANNELS) {
				let mut frame = Frame {
					at: at + duration(produced),
					index: base + produced as u64,
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
		self.next = None;
	}

	pub(super) fn available_until(&mut self) -> Option<Instant> {
		self.drain();
		self.history.back().map(Frame::end)
	}

	fn drain(&mut self) {
		while let Some(frame) = self.queue.try_pop() {
			if self.history.len() == CAPACITY {
				self.history.pop_front();
			}
			self.history.push_back(frame);
		}
	}

	pub(crate) fn fill(&mut self, at: Instant, headroom: Duration, output: &mut [f32]) {
		self.drain();
		while self
			.history
			.front()
			.is_some_and(|first| at.saturating_duration_since(first.end()) > RETAIN)
		{
			self.history.pop_front();
		}
		output.fill(0.0);
		// Seek once, and again on a buffer step of half a processing frame.
		// Between those points preserve the exact played sample sequence: slicing
		// or interpolating it on each timestamp observation creates a moving echo.
		// Render headroom only selects the starting position; changing render
		// availability is not a discontinuity in the microphone's sample clock.
		let continued = self
			.next
			.filter(|(_, expected)| at.max(*expected) - at.min(*expected) < duration(REFERENCE_FRAME) / 2);
		let target = at + headroom;
		let cursor = continued
			.map(|(index, _)| index)
			.or_else(|| {
				self.history
					.iter()
					.min_by_key(|frame| frame.at.max(target) - frame.at.min(target))
					.filter(|frame| frame.at.max(target) - frame.at.min(target) < duration(REFERENCE_FRAME) / 2)
					.map(|frame| frame.index)
			})
			.or_else(|| {
				// Independent device clocks can place capture beyond the render
				// projection. Leave one complete frame for the next capture callback
				// when available: render may arrive just after that callback.
				let last = self.history.back()?;
				if target < last.end() {
					return None;
				}
				let frames = (output.len() / REFERENCE_CHANNELS) as u64;
				let end = last.index + last.len as u64;
				let retained = end - self.history.front()?.index;
				let reserve = if retained >= frames * 2 { frames * 2 } else { frames };
				let start = end.checked_sub(reserve)?;
				(start >= self.history.front()?.index).then_some(start)
			});
		let Some(mut index) = cursor else {
			self.next = None;
			return;
		};
		let mut copied = 0;
		for frame in &self.history {
			if index < frame.index || index >= frame.index + frame.len as u64 {
				continue;
			}
			let offset = (index - frame.index) as usize;
			let frames = (frame.len - offset).min((output.len() - copied) / REFERENCE_CHANNELS);
			let count = frames * REFERENCE_CHANNELS;
			output[copied..copied + count]
				.copy_from_slice(&frame.samples[offset * REFERENCE_CHANNELS..offset * REFERENCE_CHANNELS + count]);
			index += frames as u64;
			copied += count;
			if copied == output.len() {
				break;
			}
		}
		// Keep the next unconsumed index while waiting for more render data;
		// seeking again here could replay the previous frame.
		let awaiting_render = self
			.history
			.back()
			.is_some_and(|last| index == last.index + last.len as u64);
		self.next = (copied == output.len() || awaiting_render)
			.then_some((index, at + duration(output.len() / REFERENCE_CHANNELS)));
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
	fn late_clock_keeps_a_render_frame_for_the_next_capture_callback() {
		let (mut producer, mut consumer) = channel(48_000);
		let at = Instant::now();
		producer.push_interleaved(&[1.0; 960], at);
		producer.push_interleaved(&[2.0; 960], at + Duration::from_millis(10));
		let mut heard = [0.0; 960];
		consumer.fill(at + Duration::from_millis(35), Duration::ZERO, &mut heard);
		let first = heard[0];
		consumer.fill(at + Duration::from_millis(45), Duration::ZERO, &mut heard);
		assert!(
			heard.iter().all(|value| *value == first + 1.0),
			"available PCM was replaced by silence between render callbacks"
		);
	}

	#[test]
	fn a_late_capture_clock_uses_available_pcm_without_replaying_it() {
		let (mut producer, mut consumer) = channel(48_000);
		let at = Instant::now();
		let source: Vec<f32> = (0..1_024).flat_map(|i| [i as f32 / 1_024.0; 2]).collect();
		producer.push_interleaved(&source[..1_024], at);
		let mut heard = [0.0; 960];
		consumer.fill(at + Duration::from_millis(40), Duration::ZERO, &mut heard);
		assert!(
			heard.as_slice() == &source[64..1_024],
			"a clock estimate discarded the available render frame"
		);
		for ms in [50, 60] {
			consumer.fill(at + Duration::from_millis(ms), Duration::ZERO, &mut heard);
			assert!(
				heard.iter().all(|v| *v == 0.0),
				"an exhausted render frame was replayed"
			);
		}
		producer.push_interleaved(&source[1_024..], at + duration(512));
		consumer.fill(at + Duration::from_millis(70), Duration::ZERO, &mut heard);
		assert!(heard.as_slice() == &source[1_024..1_984]);
	}

	#[test]
	fn changing_render_headroom_does_not_rewind_the_played_samples() {
		let (mut producer, mut consumer) = channel(48_000);
		let at = Instant::now();
		for value in 0..2 {
			producer.push_interleaved(&[value as f32; 960], at + Duration::from_millis(value * 10));
		}
		let mut heard = [0.0; 960];
		// First capture uses 10 ms of already available render headroom.
		consumer.fill(at, Duration::from_millis(10), &mut heard);
		assert!(heard.iter().all(|v| *v == 1.0));
		// Capture advances 10 ms, but this callback has no render headroom.
		consumer.fill(at + Duration::from_millis(10), Duration::ZERO, &mut heard);
		assert!(
			heard.iter().all(|v| *v == 0.0),
			"render availability replayed already consumed samples"
		);
		producer.push_interleaved(&[2.0; 960], at + Duration::from_millis(20));
		consumer.fill(at + Duration::from_millis(20), Duration::ZERO, &mut heard);
		assert!(heard.iter().all(|v| *v == 2.0));
		producer.push_interleaved(&[3.0; 960], at + Duration::from_millis(30));
		producer.push_interleaved(&[4.0; 960], at + Duration::from_millis(40));
		consumer.fill(at + Duration::from_millis(30), Duration::from_millis(10), &mut heard);
		assert!(
			heard.iter().all(|v| *v == 3.0),
			"restored headroom skipped played samples"
		);
	}

	#[test]
	fn device_time_selects_the_start_then_timestamp_noise_preserves_the_pcm() {
		let (mut producer, mut consumer) = channel(48_000);
		let at = Instant::now();
		let samples: Vec<f32> = (0..1_920).flat_map(|i| [i as f32 / 480.0; 2]).collect();
		producer.push_interleaved(&samples, at);
		let mut heard = [0.0; 960];
		consumer.fill(
			at + duration(480) + Duration::from_secs_f64(0.5 / 48_000.0),
			Duration::ZERO,
			&mut heard,
		);
		assert_eq!(&heard, &samples[960..1_920]);
		consumer.fill(
			at + duration(960) + Duration::from_micros(500),
			Duration::ZERO,
			&mut heard,
		);
		assert_eq!(&heard, &samples[1_920..2_880]);
		consumer.fill(at, Duration::ZERO, &mut heard);
		assert_eq!(
			&heard,
			&samples[..960],
			"a device-buffer step must reposition the reference"
		);
	}

	#[test]
	fn overlapping_predictions_preserve_pcm_and_an_exhausted_reference_recovers() {
		let (mut producer, mut consumer) = channel(48_000);
		let at = Instant::now();
		producer.push_interleaved(&[1.0; 960], at);
		producer.push_interleaved(&[2.0; 960], at + Duration::from_millis(5));
		producer.push_interleaved(&[3.0; 960], at + Duration::from_millis(30));
		let mut heard = [0.0; 960];
		consumer.fill(at, Duration::ZERO, &mut heard);
		assert!(heard.iter().all(|v| *v == 1.0));
		consumer.fill(at + Duration::from_millis(10), Duration::ZERO, &mut heard);
		assert!(
			heard.iter().all(|v| *v == 2.0),
			"timing estimates must not delete played samples"
		);
		consumer.fill(at + Duration::from_millis(20), Duration::ZERO, &mut heard);
		assert!(heard.iter().all(|v| *v == 3.0));
		consumer.fill(at + Duration::from_millis(30), Duration::ZERO, &mut heard);
		assert!(heard.iter().all(|v| *v == 0.0));
		producer.push_interleaved(&[4.0; 960], at + Duration::from_millis(40));
		consumer.fill(at + Duration::from_millis(40), Duration::ZERO, &mut heard);
		assert!(heard.iter().all(|v| *v == 4.0));
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
		consumer.fill(at, Duration::ZERO, &mut heard);
		assert_eq!(consumer.queued(), 0);
		assert_eq!(consumer.history.len(), CAPACITY);
		consumer.fill(at + Duration::from_secs(4), Duration::ZERO, &mut heard);
		assert!(consumer.history.is_empty());
		for i in 400..1_400 {
			producer.push_interleaved(&[1.0; 960], at + duration(i * 480));
			consumer.fill(at + duration(i * 480), Duration::ZERO, &mut heard);
			assert!(consumer.history.len() <= 52);
			assert_eq!(consumer.history.capacity(), CAPACITY);
		}
		consumer.clear();
		assert!(consumer.history.is_empty());
	}

	#[test]
	fn resampling_preserves_the_backend_pcm_through_timestamp_noise() {
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
		consumer.available_until();
		let start = consumer.history.front().unwrap().at;
		for block in 0..10 {
			consumer.fill(
				start + duration(block * 480) + Duration::from_micros(if block % 2 == 0 { 0 } else { 500 }),
				Duration::ZERO,
				&mut heard,
			);
			for (actual, expected) in heard.iter().zip(&expected[block * 960..(block + 1) * 960]) {
				assert!(
					(actual - expected).abs() < 0.0001,
					"resampled reference changed: {actual} != {expected}"
				);
			}
		}
	}
}
