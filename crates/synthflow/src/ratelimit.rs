//! Sliding-window admission control for provider rate limits.
//!
//! Reservations are recorded optimistically at their admission instant, so
//! concurrent callers can never push a window over its budget. A cancelled
//! caller merely wastes its own reservation.

use std::time::Duration;
use tokio::time::Instant;

pub(crate) const WINDOW: Duration = Duration::from_secs(60);

pub(crate) struct SlidingWindow {
    window: Duration,
    max: i64,
    events: std::sync::Mutex<Vec<(Instant, i64)>>,
}

impl SlidingWindow {
    pub(crate) fn new(max: u64) -> Self {
        Self::with_window(max, WINDOW)
    }

    pub(crate) fn with_window(max: u64, window: Duration) -> Self {
        Self {
            window,
            max: max as i64,
            events: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Reserve `cost` units and return the admission instant. It equals now
    /// when the budget allows, otherwise the earliest expiry instant of
    /// earlier events that frees enough budget.
    pub(crate) fn reserve(&self, cost: i64) -> Instant {
        let now = Instant::now();
        let mut events = self.events.lock().expect("rate limit lock");
        events.retain(|(ts, _)| *ts + self.window > now);
        let mut suffix = vec![0i64; events.len() + 1];
        for i in (0..events.len()).rev() {
            suffix[i] = suffix[i + 1] + events[i].1;
        }
        let admission = if suffix[0] + cost <= self.max {
            now
        } else {
            // Candidates are the instants at which existing events expire.
            // At events[k]'s expiry everything sharing its timestamp is gone.
            let mut chosen = events
                .last()
                .map(|(ts, _)| *ts + self.window)
                .unwrap_or(now);
            let mut k = 0;
            while k < events.len() {
                let expiry = events[k].0 + self.window;
                let mut j = k;
                while j < events.len() && events[j].0 + self.window == expiry {
                    j += 1;
                }
                if suffix[j] + cost <= self.max {
                    chosen = expiry;
                    break;
                }
                k = j;
            }
            chosen
        };
        let at = admission.max(now);
        let position = events.partition_point(|(ts, _)| *ts <= at);
        events.insert(position, (at, cost));
        at
    }

    /// Account a retroactive correction, typically actual usage replacing a
    /// token estimate. Negative deltas free budget immediately.
    pub(crate) fn adjust(&self, delta: i64) {
        let now = Instant::now();
        let mut events = self.events.lock().expect("rate limit lock");
        let position = events.partition_point(|(ts, _)| *ts <= now);
        events.insert(position, (now, delta));
    }
}

/// Rough pre-flight token estimate; corrected from real usage after the call.
pub(crate) fn estimate_tokens(prompt: &str) -> i64 {
    prompt.chars().count() as i64 / 4 + 32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn reservations_spread_evenly_across_the_window() {
        let limiter = SlidingWindow::with_window(2, WINDOW);
        let t0 = Instant::now();
        assert_eq!(limiter.reserve(1), t0);
        assert_eq!(limiter.reserve(1), t0);
        assert_eq!(limiter.reserve(1), t0 + WINDOW);
        assert_eq!(limiter.reserve(1), t0 + WINDOW);
        assert_eq!(limiter.reserve(1), t0 + WINDOW * 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_single_large_cost_blocks_until_full_expiry() {
        let limiter = SlidingWindow::with_window(10, WINDOW);
        let t0 = Instant::now();
        assert_eq!(limiter.reserve(10), t0);
        assert_eq!(limiter.reserve(1), t0 + WINDOW);
    }

    #[tokio::test(start_paused = true)]
    async fn negative_adjustments_release_budget_immediately() {
        let limiter = SlidingWindow::with_window(2, WINDOW);
        let t0 = Instant::now();
        assert_eq!(limiter.reserve(1), t0);
        assert_eq!(limiter.reserve(1), t0);
        limiter.adjust(-1);
        assert_eq!(limiter.reserve(1), t0);
        assert_eq!(limiter.reserve(1), t0 + WINDOW);
    }

    #[test]
    fn token_estimate_scales_with_prompt_length() {
        assert_eq!(estimate_tokens(""), 32);
        assert_eq!(estimate_tokens(&"x".repeat(400)), 132);
    }
}
