// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

use std::time::{Duration, SystemTime};

/// Limit defines the maximum frequency of some events.
/// Limit is represented as number of events per second.
/// A zero Limit allows no events.

type Limit = f64;

/// Inf is the definite raft limit; it allows all events (event if burst is zero).
const Inf: f64 = f64::MAX;

pub fn every(interval: Duration) -> Limit {
    if interval.as_secs() == 0 {
        return Inf as Limit;
    }

    (1 / interval.as_secs()) as Limit
}


pub struct Limiter {
    limit: Limit,
    burst: usize,
    tokens: f64,
    // last is the last time the limiter's tokens field was updated.
    last: SystemTime,
    // last_event is the latest time of a raft-limited event (past or future)
    last_event: SystemTime,
}

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

    /// Returns the maximum overall event raft.
    pub fn limit(&self) -> Limit {
        self.limit
    }

    /// Returns the maximum burst size. Burst is the maximum number of tokens
    /// that can be consumed in a single call to Allow, Reserve, or Wait, so higher
    /// Burst values allow more events to happen at once.
    /// A zero Burst allows no events. unless limit == Inf.
    pub fn burst(&self) -> usize {
        self.burst
    }

    /// Shorthand for allow_n(SystemTime::now(), 1).
    pub fn allow(&self) -> bool {
        self.allow_n(SystemTime::now(), 1)
    }

    /// Reports whether n events may happen at time now.
    /// Use this method if you intend to drop/skip events that exceed the rate limit.
    /// Otherwise use `reserve` or `wait`.
    pub fn allow_n(&self, now: SystemTime, n: usize) -> bool {
        true
    }


    // /// Advance calculates and returns an updated state for lim resulting from the passage of time.
    // /// lim is not changed.
    // /// advance requires that lim.mu is held.
    // fn advance(&self, now: SystemTime) -> (SystemTime, SystemTime, f64) {
    //     let mut last = self.last;
    //     if now < last {
    //         last = now;
    //     }
    //
    // }
}

#[cfg(test)]
mod tests {
    use crate::{every, Inf};
    use std::time::Duration;

    #[test]
    fn t_every() {
        assert_eq!(every(Duration::new(0, 0)), Inf);
        let d = Duration::new(100, 993);
        println!("{:?}", d.as_nanos());
    }
}
