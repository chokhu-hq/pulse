use std::num::NonZeroU32;

use tokio::time::Instant;

// token bucket that starts full, matching golang.org/x/time/rate
#[derive(Debug)]
pub(crate) struct RateLimiter {
    per_second: f64,
    burst: f64,
    tokens: f64,
    last: Instant,
}

impl RateLimiter {
    pub(crate) fn new(per_second: NonZeroU32, burst: NonZeroU32) -> Self {
        let burst = f64::from(burst.get());
        Self {
            per_second: f64::from(per_second.get()),
            burst,
            tokens: burst,
            last: Instant::now(),
        }
    }

    pub(crate) fn allow(&mut self) -> bool {
        let now = Instant::now();
        let refill = now.duration_since(self.last).as_secs_f64() * self.per_second;
        self.tokens = (self.tokens + refill).min(self.burst);
        self.last = now;

        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn allows_burst_then_refills() {
        let mut limiter =
            RateLimiter::new(NonZeroU32::new(2).unwrap(), NonZeroU32::new(3).unwrap());

        assert!((0..3).all(|_| limiter.allow()), "full burst is allowed");
        assert!(!limiter.allow(), "burst exhausted");

        tokio::time::advance(Duration::from_millis(500)).await;
        assert!(limiter.allow(), "one token refilled after half a second");
        assert!(!limiter.allow(), "only one token refilled");
    }
}
