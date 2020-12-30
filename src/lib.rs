// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3 with static-linking exception.
// See LICENCE file for details.

// Package ratelimit provides an efficient token bucket implementation
// that can be used to limit the rate of arbitrary things.
// See http://en.wikipedia.org/wiki/Token_bucket.
#![feature(duration_consts_2)]

use async_trait::async_trait;
use async_io::Timer;
use smol::lock::Mutex;

use std::time::{Duration, SystemTime};
use std::sync::Arc;

// The algorithm that this implementation uses does computational work
// only when tokens are removed from the bucket, and that work completes
// in short, bounded-constant time (Bucket.Wait benchmarks at 175ns on
// my laptop).
//
// Time is measured in equal measured ticks, a given interval
// (fillInterval) apart. On each tick a number of tokens (quantum) are
// added to the bucket.
//
// When any of the methods are called the bucket updates the number of
// tokens that are in the bucket, and it records the current tick
// number too. Note that it doesn't record the current time - by
// keeping things in units of whole ticks, it's easy to dish out tokens
// at exactly the right intervals as measured from the start time.
//
// This allows us to calculate the number of tokens that will be
// available at some time in the future with a few simple arithmetic
// operations.
//
// The main reason for being able to transfer multiple tokens on each tick
// is so that we can represent rates greater than 1e9 (the resolution of the Go
// time package) tokens per second, but it's also useful because
// it means we can easily represent situations like "a person gets
// five tokens an hour, replenished on the hour".

// Bucket represents a token bucket that fills at a predetermined rate.
// Methods on Bucket may be called concurrently.
const INFINITYDURATION: Duration = Duration::from_secs(0);

pub struct Bucket<T: Clock> {
    clock: T,

    // start holds the moment when the bucket was 
    // first created and ticks began.
    start_time: SystemTime,

    // capacity holds the overall capacity of the bucket.
    capacity: usize,

    // quantum holds how many tokens are added on
    // each tick.
    quantum: usize,

    // fill_interval holds the interval between each tick.
    fill_interval: Duration,

    // Guards the fields below it.
    lock: Arc<Mutex<()>>,

    // Holds the number of available tokens as of the associated latest_tick. it will be negative
    // when there are consumers waiting for tokens.
    available_tokens: usize,

    // Holds the latest tick for which we know the number of tokens in the bucket.
    latest_tick: usize,
}

impl<T: Clock> Bucket<T> {
    /// Indentical to `new_bucket_with_rate` but injects a
    /// testable clock interface.
    //pub fn new_bucket_with_rate(rate: f64, capacity: usize, clock: T) -> Self {
    //    Bucket{

    //    }
    //}

    /// Indentical to `new_bucket_with_rate` but injects a
    /// testable clock interface
    //pub fn new_bucket_with_rate_and_clock(rate: f64, capacity: usize, clock: Box<dyn Clock>) -> Self {
    //    // Use the same bucket each time through the loop to save allocations.
    //}


    /// Similar to `new_bucket`, but allows the specification of the quantum size - quantum tokens
    /// are added every fill_interval.
    pub fn new_bucket_with_quantum(fill_interval: Duration, capacity: usize, quantum: usize) -> Self {
        Self::new_bucket_with_quantum_and_clock(fill_interval, capacity, quantum, None)
    }

    /// Likes new_bucket_with_quantum, but also has a clock argument that allows clients to fake
    /// the passing of time. If clock is nil, the system clock will be used.
    pub fn new_bucket_with_quantum_and_clock(fill_interval: Duration, capacity: usize, quantum: usize, clock: Option<T>) -> Self {
        if fill_interval.as_nanos() <= 0 {
            panic!("token bucket fill interval is not > 0");
        }
        if capacity <= 0 {
            panic!("token bucket capacity is not > 0");
        }
        if quantum <= 0 {
            panic!("token bucket quantum is not > 0");
        }

        let clock = clock.unwrap();
        let start_time = clock.now();
        Bucket {
            clock,
            start_time,
            capacity,
            quantum,
            fill_interval,
            lock: Arc::new(Mutex::new(())),
            available_tokens: capacity,
            latest_tick: 0,
        }
    }

    /// Returns the capacity that this bucket was created with.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the fill rate of the bucket, in tokens per second.
    pub fn rate(&self) -> f64 {
        1e9 * f64(self.quantum) / f64(self.fill_interval.as_nanos())
    }

    pub fn take(&mut self, now: SystemTime, count: usize, max_wait: Duration) -> Option<Duration> {
        if count < 0 {
            return Some(Duration::from_secs(0));
        }
        let tick = self.current_tick(now);
        self.adjust_available_tokens(tick);
        let available = self.available_tokens - count;
        if available > 0 {
            self.available_tokens = available;
            return Some(Duration::from_secs(0));
        }
        None
    }

    // Returns the current time tick, measured in from self.start_time.
    fn current_tick(&self, now: SystemTime) -> usize {
        (now.duration_since(self.start_time).unwrap().as_nanos() / self.fill_interval.as_nanos()) as usize
    }

    // Adjusts the current number of tokens available in the bucket at the given time, which must
    // be in the future (positive) with respect to tb.latest_tick.
    fn adjust_available_tokens(&mut self, tick: usize) {
        let latest_tick = self.latest_tick;
        self.latest_tick = tick;
        if self.available_tokens >= self.capacity {
            return;
        }
        self.available_tokens += (tick - latest_tick) * self.quantum;
        if self.available_tokens > self.capacity {
            self.available_tokens = self.capacity;
        }
        return;
    }

    // Returns the next quantum to try after q.
    // We grow the quantum exponentially, but slowly, so we get a good that fit in the lower
    // numbers.
    fn next_quantum(quantum: usize) -> usize {
        let mut q1 = quantum * 11 / 10;
        if q1 == quantum {
            q1 += 1;
        }
        q1
    }
}

#[async_trait]
pub trait Clock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
    async fn sleep(&self, duration: Duration) {
        Timer::after(duration).await;
    }
}

struct RealClock;


impl Clock for RealClock {}
