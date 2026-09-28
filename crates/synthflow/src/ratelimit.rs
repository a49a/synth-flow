//! Joint RPM/TPM admission at the actual send boundary. Waiting callers do
//! not reserve future slots; each rechecks both budgets before sending.
use crate::{Error, Result};
use std::{collections::VecDeque, sync::Mutex, time::Duration};
use tokio::time::Instant;

const WINDOW: Duration = Duration::from_secs(60);
struct Event {
    id: u64,
    at: Instant,
    tokens: u64,
}
#[derive(Default)]
struct State {
    next_id: u64,
    events: VecDeque<Event>,
}
pub(crate) struct RateLimiter {
    rpm: Option<u32>,
    tpm: Option<u32>,
    state: Mutex<State>,
}
pub(crate) enum Admission {
    Send(u64),
    Wait(Instant),
}
impl RateLimiter {
    pub(crate) fn new(rpm: Option<u32>, tpm: Option<u32>) -> Self {
        Self {
            rpm,
            tpm,
            state: Mutex::new(State::default()),
        }
    }
    pub(crate) fn admit(&self, tokens: u64) -> Result<Admission> {
        if self.rpm == Some(0) || self.tpm == Some(0) {
            return Err(Error::Configuration("rate limits must be positive".into()));
        }
        if self.tpm.is_some_and(|max| tokens > u64::from(max)) {
            return Err(Error::Configuration(
                "estimated request tokens exceed tokens_per_minute".into(),
            ));
        }
        let now = Instant::now();
        let mut state = self.state.lock().expect("rate limit lock");
        while state
            .events
            .front()
            .is_some_and(|event| event.at + WINDOW <= now)
        {
            state.events.pop_front();
        }
        let used: u128 = state.events.iter().map(|e| u128::from(e.tokens)).sum();
        let full = self
            .rpm
            .is_some_and(|max| state.events.len() >= max as usize)
            || self
                .tpm
                .is_some_and(|max| used + u128::from(tokens) > u128::from(max));
        if full {
            return Ok(Admission::Wait(
                state.events.front().expect("occupied budget").at + WINDOW,
            ));
        }
        let id = state.next_id;
        state.next_id += 1;
        if self.rpm.is_some() || self.tpm.is_some() {
            state.events.push_back(Event {
                id,
                at: now,
                tokens,
            });
        }
        Ok(Admission::Send(id))
    }
    /// Correct the original event, keeping its original expiry. No negative
    /// credit can survive after the associated request leaves the window.
    pub(crate) fn correct(&self, id: u64, actual: u64) {
        if self.tpm.is_some() {
            let mut state = self.state.lock().expect("rate limit lock");
            if let Some(event) = state.events.iter_mut().find(|event| event.id == id) {
                event.tokens = actual;
            }
        }
    }
}
pub(crate) fn estimate_tokens(prompt: &str) -> u64 {
    prompt.chars().count() as u64 / 4 + 32
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sent(value: Result<Admission>) -> u64 {
        match value.expect("admission") {
            Admission::Send(id) => id,
            Admission::Wait(_) => panic!("unexpected wait"),
        }
    }
    #[tokio::test(start_paused = true)]
    async fn both_budgets_apply_at_send_time() {
        let limiter = RateLimiter::new(Some(2), Some(100));
        sent(limiter.admit(100));
        assert!(matches!(limiter.admit(100).unwrap(), Admission::Wait(_)));
        tokio::time::advance(WINDOW).await;
        sent(limiter.admit(100));
        tokio::time::advance(WINDOW).await;
        sent(limiter.admit(32));
        sent(limiter.admit(32));
        assert!(matches!(limiter.admit(32).unwrap(), Admission::Wait(_)));
    }
    #[tokio::test(start_paused = true)]
    async fn corrections_expire_with_the_original_request() {
        let limiter = RateLimiter::new(None, Some(100));
        let id = sent(limiter.admit(100));
        tokio::time::advance(Duration::from_secs(10)).await;
        limiter.correct(id, 10);
        sent(limiter.admit(90));
        tokio::time::advance(Duration::from_secs(50)).await;
        sent(limiter.admit(10));
        assert!(matches!(limiter.admit(1).unwrap(), Admission::Wait(_)));
    }
    #[tokio::test]
    async fn oversized_request_is_rejected_without_consuming_budget() {
        let limiter = RateLimiter::new(Some(1), Some(10));
        assert!(limiter.admit(11).is_err());
        sent(limiter.admit(10));
    }
    #[test]
    fn token_estimate_scales_with_prompt_length() {
        assert_eq!(estimate_tokens(""), 32);
        assert_eq!(estimate_tokens(&"x".repeat(400)), 132);
    }
}
