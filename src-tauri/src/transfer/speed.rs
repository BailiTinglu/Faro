//! Live per-transfer counters and the moving-average speed (Plan 24 Phase 7).
//!
//! Copy loops only bump atomics; the manager's 250 ms tick turns those into
//! `transferred`, `bytesPerSec` and `etaSecs` and emits one batched
//! `transfer://progress-batch` event for every row that changed.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How far back the speed average looks.
const WINDOW: Duration = Duration::from_secs(10);
/// One sample per second at most: a ring of ~10 one-second buckets.
const SAMPLE_EVERY: Duration = Duration::from_secs(1);

/// Moving-average speed over the last [`WINDOW`]. The average divides by the
/// time the samples actually span, not always by 10 s, so a transfer that is
/// still warming up isn't under-reported.
#[derive(Debug, Default)]
pub struct SpeedRing {
    samples: VecDeque<(Instant, u64)>,
}

impl SpeedRing {
    /// Feed the transfer's running byte total. Returns bytes/sec, or `None`
    /// until there is enough history to say anything (≥ 250 ms).
    pub fn sample(&mut self, total: u64, now: Instant) -> Option<u64> {
        // A counter that went backwards (restart from 0, resume mismatch)
        // invalidates the history.
        if self.samples.back().is_some_and(|&(_, t)| total < t) {
            self.samples.clear();
        }
        let push = self
            .samples
            .back()
            .is_none_or(|&(at, _)| now.duration_since(at) >= SAMPLE_EVERY);
        if push {
            self.samples.push_back((now, total));
        }
        while self.samples.len() > 2
            && now.duration_since(self.samples[0].0) > WINDOW + SAMPLE_EVERY
        {
            self.samples.pop_front();
        }
        let &(t0, b0) = self.samples.front()?;
        let span = now.duration_since(t0).as_secs_f64();
        if span < 0.25 {
            return None;
        }
        Some(((total - b0) as f64 / span) as u64)
    }

    pub fn reset(&mut self) {
        self.samples.clear();
    }
}

/// Seconds left at `speed`, or `None` when that can't be known (no speed yet,
/// size unknown, or already complete).
pub fn eta_secs(size: u64, transferred: u64, speed: Option<u64>) -> Option<u64> {
    let speed = speed.filter(|&s| s > 0)?;
    if size == 0 || transferred >= size {
        return None;
    }
    Some((size - transferred).div_ceil(speed))
}

fn now_ms() -> u64 {
    crate::db::now_ms().max(0) as u64
}

/// Atomics a running copy loop updates without taking any lock.
#[derive(Debug)]
pub struct Live {
    /// Bytes of the destination known to be done (resume base + this run).
    pub bytes: AtomicU64,
    /// Wall-clock ms of the last byte that moved (stall watchdog).
    last_progress_ms: AtomicU64,
    /// Active parallel ranges/parts (segmented transfers), 0 otherwise.
    pub segments: AtomicUsize,
    /// No bytes for a while (UI "not responding").
    pub stalled: AtomicBool,
    pub ring: Mutex<SpeedRing>,
}

impl Default for Live {
    fn default() -> Self {
        Self {
            bytes: AtomicU64::new(0),
            last_progress_ms: AtomicU64::new(now_ms()),
            segments: AtomicUsize::new(0),
            stalled: AtomicBool::new(false),
            ring: Mutex::new(SpeedRing::default()),
        }
    }
}

impl Live {
    /// `n` more bytes landed.
    pub fn add(&self, n: u64) {
        self.bytes.fetch_add(n, Ordering::Relaxed);
        self.touch();
    }

    /// The running total is now `total` (single-stream loops that count
    /// their own position, or a restart/resume rebase).
    pub fn set(&self, total: u64) {
        self.bytes.store(total, Ordering::Relaxed);
        self.touch();
    }

    pub fn get(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    /// Mark progress (bytes moved, or a run just started).
    pub fn touch(&self) {
        self.last_progress_ms.store(now_ms(), Ordering::Relaxed);
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warmup_is_not_under_reported() {
        let mut ring = SpeedRing::default();
        let t0 = Instant::now();
        assert_eq!(ring.sample(0, t0), None);
        // 1 MiB after 2 s → 512 KiB/s, not 1 MiB / 10 s.
        ring.sample(512 * 1024, t0 + Duration::from_secs(1));
        let s = ring.sample(1024 * 1024, t0 + Duration::from_secs(2)).unwrap();
        assert_eq!(s, 512 * 1024);
    }

    #[test]
    fn window_slides_and_follows_a_rate_change() {
        let mut ring = SpeedRing::default();
        let t0 = Instant::now();
        let mut total = 0u64;
        // 20 s at 100 B/s, then 20 s at 1000 B/s.
        for sec in 0..=40u64 {
            if sec > 0 {
                total += if sec <= 20 { 100 } else { 1000 };
            }
            ring.sample(total, t0 + Duration::from_secs(sec));
        }
        let s = ring.sample(total, t0 + Duration::from_secs(40)).unwrap();
        assert!((900..=1000).contains(&s), "speed {s}");
        assert!(ring.samples.len() <= 12);
    }

    #[test]
    fn counter_reset_clears_history() {
        let mut ring = SpeedRing::default();
        let t0 = Instant::now();
        ring.sample(10_000, t0);
        ring.sample(20_000, t0 + Duration::from_secs(1));
        // Restarted from 0: no negative or stale speed.
        assert_eq!(ring.sample(0, t0 + Duration::from_secs(2)), None);
    }

    #[test]
    fn eta_handles_unknowns() {
        assert_eq!(eta_secs(1000, 500, Some(100)), Some(5));
        assert_eq!(eta_secs(1000, 500, Some(0)), None);
        assert_eq!(eta_secs(1000, 500, None), None);
        assert_eq!(eta_secs(0, 500, Some(100)), None);
        assert_eq!(eta_secs(1000, 1000, Some(100)), None);
        assert_eq!(eta_secs(1001, 1000, Some(100)), Some(1));
    }
}
