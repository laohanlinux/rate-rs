// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
#![feature(duration_consts_2)]

use std::ops::{Sub, Add};
use std::time::{Duration, SystemTime};
use std::u64::MAX;
use std::fs::read;
use tokio::sync::Mutex;
use std::borrow::Borrow;

/// A limiter controls how frequently events are allowed to happen.
/// It implements a `token bucket` of size **b**, initially full and refilled
/// at rate `r` token *per second*.
/// Informally, in any large enough time interval, the `Limiter` limits the
/// rate to `r` tokens per second, with a maximum burst size of `b` events.
/// As a special case, if `r==INF` (the infinite rate), `b` is ignored.
/// See https://en.wikipedia.org/wiki/Token_bucket for more about token buckets.
///
/// The zero value is a valid Limiter, but it will reject all events.
/// Use `Self::new()` to create *no-zero* Limiters.
///
/// Limiter has three main methods, `allow, reserve, and wait`.
/// Most of callers should `wait`.
///
/// Each of the three methods consumes a single token.
/// They differ in their behavior when no token is available.
/// - If no token is available, `allow` returns false.
/// - If no token is available, `reserve` returns a reservation for a future token
/// and the amount of time the caller must wait before using it.
/// - If no token if available, `wait` blocks until one can be obtained
///
/// The methods `allow_n, reserve_n, and wait_n` consume n tokens.
#[derive(Debug, Clone)]
pub struct Limiter {
    limit: Limit,
    burst: usize,
    tokens: f64,
    // The last time the limiter's tokens field was updated.
    last: SystemTime,
    // The latest time of a raft-limited event (past or future).
    last_event: SystemTime,
}


/// ThreadSafe Limiter
#[derive(Debug)]
pub struct LimiterSync {}

impl Limiter {
    /// Returns a new `Limiter` that allows events up to rate r and permits
    /// bursts of at most b tokens.
    pub fn new(r: Limit, b: usize) -> Limiter {
        Limiter {
            limit: r,
            burst: b,
            tokens: 0.0,
            last: SystemTime::now(),
            last_event: SystemTime::now(),
        }
    }

    /// Returns the maximum overall event rate.
    pub fn limit(&self) -> Limit {
        self.limit
    }

    /// Returns the maximum burst size. `Burst` is the maximum number of tokens
    /// that can be consumed in a single call to `allow`, `reserve`, or `wait`, so higher
    /// `burst` values allow more events to happen at once.
    /// A `zero burst` allows no events. unless limit == Inf.
    pub fn burst(&self) -> usize {
        self.burst
    }

    /// Shorthand for `allow_n(SystemTime::now(), 1)`.
    pub fn allow(&mut self) -> bool {
        self.allow_n(SystemTime::now(), 1)
    }

    /// Reports whether n events may happen at time now.
    /// Use this method if you intend to drop/skip events that exceed the rate limit.
    /// Otherwise use `reserve` or `wait`.
    pub fn allow_n(&mut self, now: SystemTime, n: usize) -> bool {
        self.reserve_n(now, n, Duration::from_secs(0)).ok
    }

    /// Returns a `Reservation` that indicates how long the caller must wait before n events
    /// happen. 
    /// The Limiter takes this `Reservation` into account when allowing future events.
    /// The returned `Reservation` ok() method returns false if n exceeds the Limiter's burst size.
    /// User Example:
    ///  let r = lim.reserve_n(SystemTime::now(), 1)
    ///  if !r.ok() {
    ///     // Not allowed to act! Did you member to set lim.burst to be > 0 ?
    ///     return;
    ///  }
    ///  time.Sleep(r.delay())
    ///  act()
    /// Use this method if you wish to wait and slow down in accordance with the rate limit without
    /// dropping events. 
    /// If you need to respect a dealine or cancel the delay, use `wait` instead.
    /// To drop or skip events exceeding rate limit, use `allow` instead.
    pub fn reserve_n(&mut self, now: SystemTime, n: usize) -> Reservation {
        self.reserve_n2(now, n, INF_DURATION)
    }
   
    // A helper method for allow_n, reserve_n, and wait_n.
    // max_future_reserve specifies the maximum reservation wait duration allowed.
    // reserve_n returns `Reservation`, not &Reservation, to avoid allocation in allow_n and wait_n
    fn reserve_n2(&mut self, now: SystemTime, n: usize, max_future_reserve: Duration) -> Reservation{
        // TODO: add lock
        if self.limit.0 == INF {
            return Reservation{
                ok: true,
                lim: Some(self.clone()),
                limit: Limit(0.0),
                tokens: n,
                time_to_act: now,
            };
        }


        let (now, last, mut tokens) = self.advance(now);
        // Calculate the remaining number of tokens resulting from the request.
        tokens -= n as f64; 

        // Calculate the wait duration 
        let mut wait_duration = Duration::from_secs(0);
        if tokens <  0.0 {
            wait_duration = self.limit.duration_from_tokens(-tokens);
        }

        // Decide result
        let ok = n <= self.burst && wait_duration <= max_future_reserve;

        // Prepare reservation
        let mut r = Reservation{
            ok: true,
            lim: Some(self.clone()),
            limit: self.limit,
            tokens: 0,
            time_to_act: SystemTime::now(), // TODO: set zero time 
        };
        if ok {
            r.tokens = n;
            r.time_to_act = now.add(wait_duration);
        }

        // Update state
        if ok {
            self.last = now;
            self.tokens = tokens;
            self.last_event = r.time_to_act;
        }else {
            self.last = last;
        }

        r
    }

    /// `advance` calculates and returns an updated state for lim resulting from the passage of time.
    /// lim is not changed.
    /// --advance requires that lim.mu is held--.
    fn advance(&self, now: SystemTime) -> (SystemTime, SystemTime, f64) {
        let mut last = self.last;
        if now < last {
            last = now;
        }

        // Avoid making delta overflow below when last is very old.
        // |T1|T2|T3|T4|T5
        //  t < T5
        let max_elapsed = self.limit.duration_from_tokens(self.burst as f64 - self.tokens);
        let mut elapsed = now.duration_since(last).unwrap();
        if elapsed > max_elapsed {
            elapsed = max_elapsed;
        }

        // Calculate the new number of tokens, due to time that passed.
        let delta = self.limit.token_from_duration(elapsed);
        let mut tokens = self.tokens + delta;
        let burst = self.burst as f64;
        if tokens > burst {
            tokens = burst;
        }
        (now, last, tokens)
    }
}

/// A `Reservation` holds information about events that are permitted by a `Limiter` to happen after a delay.
/// A `Reservation` may be canceled, which may enable the `Limiter` to permit additional events.
#[derive(Debug, Clone)]
pub struct Reservation {
    ok: bool,
    lim: Option<Limiter>,
    tokens: usize,
    time_to_act: SystemTime,
    limit: Limit, // This is the `Limit` at reservation time, it can change later.
}

const INF_DURATION: Duration = Duration::new(MAX, 0);

impl Reservation {
    /// Returns whether the limiter can provide the requested number of tokens.
    /// within the maximum wait time. If `ok` is false, `Delay` returns `InfDuration`, and
    /// Cancel does nothing.
    pub fn ok(&self) -> bool { self.ok }

    /// Shorthand for `delay_from(SystemTime::now())`
    pub fn delay(&self) -> Duration { self.delay_from(SystemTime::now()) }

    /// Returns the duration for which the `reservation` holder must wait
    /// before taking the reserved action. Zero duration means act immediately.
    /// `INF_DURATION` means the limiter cannot grant the tokens requested in this
    /// `Reservation` within the maximum wait time.
    pub fn delay_from(&self, now: SystemTime) -> Duration {
        if !self.ok {
            return INF_DURATION;
        }
        self.time_to_act.duration_since(now).unwrap_or(Duration::from_secs(0))
    }

    /// Indicates that the `reservation` holder will not perform the `reserved` action
    /// and reverses the effects of this `Reservation` on the rate limit as much as possible,
    /// considering that other `reservations` may have already been made.
    pub fn cancel_at(&mut self, now: SystemTime) {
        if !self.ok {
            return;
        }

        // TODO: add lock
        let lim = self.lim.as_mut().unwrap();
        if lim.limit.0 == INF || self.tokens == 0 || self.time_to_act <= now {
            return;
        }

        // calculate tokens to restore
        // The duration between lim.last_event and self.time_to_act tells use how many tokens were reserved
        // after r was obtained. These tokens should not be restored.
        let mut lim = self.lim.as_mut().unwrap();
        let restore_tokens = self.tokens as f64 - self.limit.token_from_duration(lim.last_event.duration_since(self.time_to_act).unwrap());
        if restore_tokens <= 0.0 {
            return;
        }
        // advance time to now
        let (now, _, mut tokens) = lim.advance(now);
        // calculate new number of tokens
        tokens += restore_tokens;
        if tokens > lim.burst as f64 {
            tokens = lim.burst as f64;
        }
        // update state
        lim.last = now;
        lim.tokens = tokens;
        if self.time_to_act == lim.last_event {
            let pre_event = self.time_to_act.add(self.limit.duration_from_tokens(-1.0 * tokens as f64));
            if pre_event <= now {
                lim.last_event = pre_event;
            }
        }
    }

    /// Blocks until lim permits n events to happen.
    /// It returns an error if n exceeds the Limiter's burst size, the Context is
    /// canceled, or the expected wait time exceeds the Context's Deadline.
    /// The burst limit is ignored if the rate limit is INF.
    pub fn wait_n(&mut self, timer: SystemTime, n: isize) -> Result<(), &str> {
        let now = SystemTime::now();
        if now < timer {
            return Err("timer out");
        }

        Ok(())
    }
}

/// Limit defines the maximum frequency of some events.
/// Limit is represented as number of events **per second**.
/// A zero Limit allows no events.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Limit(f64);

impl Limit {
    pub fn new(interval: Duration) -> Self {
        if interval.as_nanos() <= 0u128 {
            return Limit(INF);
        } else {
            Limit((1 / interval.as_nanos()) as f64)
        }
    }

    /// A unit conversion function from the number of tokens to the duration
    /// of time it takes to accumulate them at a rate of limit tokens per second.
    pub fn duration_from_tokens(&self, tokens: f64) -> Duration {
        let seconds = tokens / self.0;
        Duration::from_secs_f64(seconds)
    }

    /// A unit conversion function from a time duration to the number of tokens
    /// which could be accumulated during that duration at a rate of limit tokens per second.
    pub fn token_from_duration(&self, d: Duration) -> f64 {
        d.as_nanos() as f64 / self.0
    }
}

impl From<f64> for Limit {
    fn from(f: f64) -> Self {
        Limit(f)
    }
}

/// The definite rate limit; it allows all events (event if burst is zero).
const INF: f64 = f64::MAX;

pub fn every(interval: Duration) -> Limit {
    if interval.as_nanos() == 0 {
        return INF.into();
    }

    ((1 / interval.as_secs()) as f64).into()
}


#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::{every, INF, Limit};

    #[test]
    fn t_limit() {
        assert_ne!(Limit::from(10.0).0, INF, "Limit(10) == INF should be false");
    }

    #[test]
    fn t_every() {
        let tests = vec![(Duration::from_secs(0), INF)];
        for (interval, exp) in tests {
            let limit = every(interval);
            assert_eq!(limit.0, exp);
        }
    }

    #[test]
    fn timer() {}

    fn t_close_enough() {}
}
