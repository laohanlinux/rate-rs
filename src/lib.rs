// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3 with static-linking exception.
// See LICENCE file for details.

// Package ratelimit provides an efficient token bucket implementation
// that can be used to limit the rate of arbitrary things.
// See http://en.wikipedia.org/wiki/Token_bucket.
#![feature(duration_consts_2)]
#![feature(test)]
#![feature(async_closure)]

use std::time::{Duration, SystemTime};
use std::sync::{Arc, Mutex};
use std::ops::Add;
use tokio::time::Sleep;
use std::fmt::{Display, Formatter};

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
// at exactly the right intervals as measured from the epoch time.
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
const INFINITY_DURATION: Duration = Duration::from_secs(0x7fffffffffffffff);
// specifies the allowed variance of actual rate from specified rate. 1% seems reasonable.
const RATE_MARGIN: f64 = 0.01;

#[derive(Clone)]
pub struct Bucket {
    clock: RealClock,
    // Holds the moment when the bucket was first created and ticks began.
    epoch_time: SystemTime,

    // Holds the overall capacity of the bucket.
    capacity: i64,

    // Holds how many tokens are added on each tick.
    quantum: i64,

    // Holds the interval between each tick.
    fill_interval: Duration,

    state: Arc<Mutex<BucketState>>,
}

#[derive(Debug, Clone)]
struct BucketState {
    // Holds the number of available tokens as of the associated latest_tick. it will be negative
    // when there are consumers waiting for tokens.
    available_tokens: i64,

    // Holds the latest tick for which we know the number of tokens in the bucket.
    latest_tick: i64,
}

impl Display for Bucket {
    fn fmt(&self, f: &mut Formatter<'_>) -> ::std::fmt::Result {
        let state = self.state.lock().unwrap();
        writeln!(f, "epoch_time: {:?}, capacity: {}, quantum: {}, fill_interval: {:?}, available_tokens: {}, latest_tick: {}",
                 self.epoch_time, self.capacity, self.quantum, self.fill_interval, state.available_tokens, state.latest_tick)
    }
}

impl Bucket {
    /// Returns a new token bucket that fills at the
    /// rate of one token every fill_interval, up to the given
    /// maximum capacity. Both arguments must be positive. The bucket is initially full.
    pub fn new_bucket(fill_interval: Duration, capacity: i64) -> Self {
        Self::new_bucket_with_clock(fill_interval, capacity, None)
    }

    /// Identical to `new_bucket` but injects a testable clock interface.
    pub fn new_bucket_with_clock(
        fill_interval: Duration,
        capacity: i64,
        clock: Option<RealClock>,
    ) -> Self {
        Self::new_bucket_with_quantum_and_clock(fill_interval, capacity, 1, clock)
    }

    /// Returns a token bucket that fills the bucket
    /// at the rate of rate tokens per second up to the given
    /// maximum capacity. Because of limited clock resolution,
    /// at high rates, the actual rate may be up tp 1% different from the
    /// specified rate.
    pub fn new_bucket_with_rate(rate: f64, capacity: i64) -> Self {
        Self::new_bucket_with_rate_and_clock(rate, capacity, None)
    }

    /// Identical to `new_bucket_with_rate` but injects a
    /// testable clock interface
    pub fn new_bucket_with_rate_and_clock(
        rate: f64,
        capacity: i64,
        clock: Option<RealClock>,
    ) -> Self {
        // Use the same bucket each time through the loop to save allocations.
        let mut bucket =
            Self::new_bucket_with_quantum_and_clock(Duration::from_nanos(1), capacity, 1, clock);
        let mut quantum = 1;
        while quantum < 1 << 50 {
            let fill_interval = Duration::from_nanos((1e9 * (quantum as f64) / rate) as u64);
            if fill_interval.as_nanos() <= 0 {
                quantum = Self::next_quantum(quantum);
                continue;
            }
            bucket.quantum = quantum;
            quantum = Self::next_quantum(quantum);
            let diff = (bucket.rate() - rate).abs();
            if diff / rate <= RATE_MARGIN {
                return bucket;
            }
        }
        panic!("cannot find suitable quantum for {:+e}", rate);
    }

    /// Similar to `new_bucket`, but allows the specification of the quantum size - quantum tokens
    /// are added every fill_interval.
    pub fn new_bucket_with_quantum(fill_interval: Duration, capacity: i64, quantum: i64) -> Self {
        Self::new_bucket_with_quantum_and_clock(fill_interval, capacity, quantum, None)
    }

    /// Likes new_bucket_with_quantum, but also has a clock argument that allows clients to fake
    /// the passing of time. If clock is nil, the system clock will be used.
    pub fn new_bucket_with_quantum_and_clock(
        fill_interval: Duration,
        capacity: i64,
        quantum: i64,
        clock: Option<RealClock>,
    ) -> Self {
        if fill_interval.as_nanos() <= 0 {
            panic!("token bucket fill interval is not > 0");
        }
        if capacity <= 0 {
            panic!("token bucket capacity is not > 0");
        }
        if quantum <= 0 {
            panic!("token bucket quantum is not > 0");
        }
        let clock = clock.or_else(|| Some(RealClock {})).unwrap();
        let epoch_time = clock.now();
        Bucket {
            clock,
            epoch_time,
            capacity,
            quantum,
            fill_interval,
            state: Arc::new(Mutex::new(BucketState {
                available_tokens: capacity,
                latest_tick: 0,
            })),
        }
    }

    // Takes count tokens from the bucket, waiting until they are available.
    pub async fn wait(&self, count: i64) {
        let duration = self.take(count).await;
        if duration.as_nanos() > 0 {
            self.clock.sleep(duration).await;
        }
    }

    // Like `wait` except that it will only take tokens from the bucket if it needs to wait for no greater than *max_wait*.
    // It reports whether any tokens have been removed from the bucket
    // If no tokens have been removed, it returns immediately.
    pub async fn wait_max_duration(&self, count: i64, max_wait: Duration) -> bool {
        if let Some(duration) = self.take_max_duration(count, max_wait).await {
            self.clock.sleep(duration).await;
            true
        } else {
            false
        }
    }

    /// Takes count tokens from the bucket without blocking. It returns the time that the caller
    /// should wait until the tokens are actually available.
    ///
    /// Note that if the request is irrevocale - there is no way to return tokens to the bucket
    /// once this method commits use to taking them.
    pub async fn take(&self, count: i64) -> Duration {
        self.inner_take(self.clock.now(), count, INFINITY_DURATION)
            .or_else(|| Some(Duration::from_secs(0)))
            .unwrap()
    }

    /// Likes take, except that it will only take tokens from the bucket if the await time for the
    /// tokens is no greater than max_wait.
    ///
    /// If it would take longer than max_wait for the tokens to become avalible, it does nothing
    /// and reports false, otherwise it returns the time that the caller should wait until the
    /// tokens are actually avalible, and reports true.
    pub async fn take_max_duration(&self, count: i64, max_wait: Duration) -> Option<Duration> {
        self.inner_take(self.clock.now(), count, max_wait)
    }

    /// Takes up to count immediately avalible tokens from the
    /// bucket. It returns the number of tokens removed, or zero if there are no avalible tokens,
    /// It doesn't block.
    pub async fn take_available(&self, count: i64) -> i64 {
        self.inner_take_available(self.clock.now(), count)
    }

    // The interval version of `take_available` - it takes the
    // current time as an argument to enable easy testing.
    fn inner_take_available(&self, now: SystemTime, mut count: i64) -> i64 {
        if count <= 0 {
            return 0;
        }
        self.adjust_available_tokens(self.current_tick(now));
        let mut state = self.state.lock().unwrap();
        if state.available_tokens <= 0 {
            return 0;
        }
        if count > state.available_tokens {
            count = state.available_tokens;
        }
        state.available_tokens -= count;
        count
    }

    /// Returns the number of available tokens. It will be negative
    /// when there are consumers waiting for tokens. Note that if this
    /// returns greater than zero, it doesn't guarantee that calls that take
    /// tokens from the buffer will succeed, as the number of available
    /// tokens could have changed in the meantime. This method is intended
    /// primarily for metrics reporting and debugging.
    pub fn available(&self) -> i64 {
        self.inner_available(self.clock.now())
    }

    // The interval version of available - it takes the current time as
    // an argument to enable easy testing.
    fn inner_available(&self, now: SystemTime) -> i64 {
        self.adjust_available_tokens(self.current_tick(now));
        let state = self.state.lock().unwrap();
        state.available_tokens
    }

    /// Returns the capacity that this bucket was created with.
    pub fn capacity(&self) -> i64 {
        self.capacity
    }

    /// Returns the fill rate of the bucket, in tokens per second.
    pub fn rate(&self) -> f64 {
        (1e9 * self.quantum as f64) / (self.fill_interval.as_nanos() as f64)
    }

    // The interval version of available - it takes the current time as
    // an argument to enable easy testing.
    fn inner_take(&self, now: SystemTime, count: i64, max_wait: Duration) -> Option<Duration> {
        if count < 0 {
            return Some(Duration::from_secs(0));
        }
        let tick = self.current_tick(now);
        self.adjust_available_tokens(tick);
        let mut state = self.state.lock().unwrap();
        let available = state.available_tokens - count;
        if available >= 0 {
            state.available_tokens = available;
            return Some(Duration::from_secs(0));
        }

        // Round up the missing tokens to nearest multiple of quantum - the token won't be available
        // util that tick.
        //
        // end_tick holds the tick when all the requested tokens will become available.
        let end_tick = tick + (-available + self.quantum - 1) / self.quantum;
        let end_time = self.epoch_time.add(Duration::from_nanos(
            (end_tick * self.fill_interval.as_nanos() as i64) as u64,
        ));
        let wait_time = end_time.duration_since(now).unwrap();
        if wait_time > max_wait {
            return None;
        }
        state.available_tokens = available;
        Some(wait_time)
    }

    // Returns the current time tick, measured in from self.epoch_time.
    fn current_tick(&self, now: SystemTime) -> i64 {
        (now.duration_since(self.epoch_time).unwrap().as_nanos() / self.fill_interval.as_nanos())
            as i64
    }

    // Adjusts the current number of tokens available in the bucket at the given time, which must
    // be in the future (positive) with respect to tb.latest_tick.
    fn adjust_available_tokens(&self, tick: i64) {
        let mut state = self.state.lock().unwrap();
        let latest_tick = state.latest_tick;
        state.latest_tick = tick;
        if state.available_tokens >= self.capacity {
            return;
        }
        state.available_tokens += (tick - latest_tick) * self.quantum;
        if state.available_tokens > self.capacity {
            state.available_tokens = self.capacity;
        }
        return;
    }

    // Returns the next quantum to try after q.
    // We grow the quantum exponentially, but slowly, so we get a good that fit in the lower
    // numbers.
    fn next_quantum(quantum: i64) -> i64 {
        let mut q1 = quantum * 11 / 10;
        if q1 == quantum {
            q1 += 1;
        }
        q1
    }
}

pub trait Clock: Send + Sync {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
    fn sleep(&self, duration: Duration) -> Sleep {
        tokio::time::sleep(duration)
    }
}

#[derive(Debug, Default, Clone)]
pub struct RealClock;

impl Clock for RealClock {}

#[cfg(test)]
mod tests {
    #![feature(test)]

    use tokio::time::{Duration, sleep};
    use crate::{Bucket, INFINITY_DURATION, RATE_MARGIN, Clock};
    use std::ops::{Add, Sub};
    use std::panic::catch_unwind;
    use test::Bencher;
    use std::sync::{Arc, Mutex};
    use std::fs::File;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::SystemTime;

    #[derive(Debug)]
    struct TakeReq {
        time: Duration,
        count: i64,
        expect_wait: Duration,
    }

    impl Default for TakeReq {
        fn default() -> Self {
            TakeReq {
                time: Duration::from_secs(0),
                count: 0,
                expect_wait: Duration::from_secs(0),
            }
        }
    }

    struct TakeTest {
        about: &'static str,
        fill_interval: Duration,
        capacity: i64,
        reqs: Vec<TakeReq>,
    }

    impl TakeTest {
        fn new() -> Vec<TakeTest> {
            vec![
                TakeTest {
                    about: "serial requests",
                    fill_interval: Duration::from_millis(250),
                    capacity: 10,
                    reqs: vec![
                        TakeReq::default(),
                        TakeReq {
                            count: 10,
                            ..Default::default()
                        },
                        TakeReq {
                            count: 1,
                            expect_wait: Duration::from_millis(250),
                            ..Default::default()
                        },
                        TakeReq {
                            time: Duration::from_millis(250),
                            count: 1,
                            expect_wait: Duration::from_millis(250),
                        },
                    ],
                },
                TakeTest {
                    about: "concurrent requests",
                    fill_interval: Duration::from_millis(250),
                    capacity: 10,
                    reqs: vec![
                        TakeReq {
                            count: 10,
                            ..Default::default()
                        },
                        TakeReq {
                            count: 2,
                            expect_wait: Duration::from_millis(500),
                            ..Default::default()
                        },
                        TakeReq {
                            count: 2,
                            expect_wait: Duration::from_millis(1000),
                            ..Default::default()
                        },
                        TakeReq {
                            count: 1,
                            expect_wait: Duration::from_millis(1250),
                            ..Default::default()
                        },
                    ],
                },
                TakeTest {
                    about: "more than capacity",
                    fill_interval: Duration::from_millis(1),
                    capacity: 10,
                    reqs: vec![
                        TakeReq {
                            count: 10,
                            ..Default::default()
                        },
                        TakeReq {
                            time: Duration::from_millis(20),
                            count: 15,
                            expect_wait: Duration::from_millis(5),
                        },
                    ],
                },
                TakeTest {
                    about: "sub-quantum time",
                    fill_interval: Duration::from_millis(10),
                    capacity: 10,
                    reqs: vec![
                        TakeReq {
                            count: 10,
                            ..Default::default()
                        },
                        TakeReq {
                            time: Duration::from_millis(7),
                            count: 1,
                            expect_wait: Duration::from_millis(3),
                        },
                        TakeReq {
                            time: Duration::from_millis(8),
                            count: 1,
                            expect_wait: Duration::from_millis(12),
                        },
                    ],
                },
                TakeTest {
                    about: "within capacity",
                    fill_interval: Duration::from_millis(10),
                    capacity: 5,
                    reqs: vec![
                        TakeReq {
                            count: 5,
                            ..Default::default()
                        },
                        TakeReq {
                            time: Duration::from_millis(60),
                            count: 5,
                            ..Default::default()
                        },
                        TakeReq {
                            time: Duration::from_millis(60),
                            count: 1,
                            expect_wait: Duration::from_millis(10),
                        },
                        TakeReq {
                            time: Duration::from_millis(80),
                            count: 2,
                            expect_wait: Duration::from_millis(10),
                        },
                    ],
                },
            ]
        }
    }

    struct AvailTest {
        about: &'static str,
        capacity: i64,
        fill_interval: Duration,
        take: i64,
        sleep: Duration,
        expect_count_after_take: i64,
        expect_count_after_sleep: i64,
    }

    impl AvailTest {
        fn new() -> Vec<AvailTest> {
            vec![
                AvailTest {
                    about: "should fill tokens after interval",
                    capacity: 5,
                    fill_interval: Duration::from_secs(1),
                    take: 5,
                    sleep: Duration::from_secs(1),
                    expect_count_after_take: 0,
                    expect_count_after_sleep: 1,
                },
                AvailTest {
                    about: "should fill tokens plus existing count",
                    capacity: 2,
                    fill_interval: Duration::from_secs(1),
                    take: 1,
                    sleep: Duration::from_secs(1),
                    expect_count_after_take: 1,
                    expect_count_after_sleep: 2,
                },
                AvailTest {
                    about: "shouldn't fill before interval",
                    capacity: 2,
                    fill_interval: Duration::from_secs(2),
                    take: 1,
                    sleep: Duration::from_secs(1),
                    expect_count_after_take: 1,
                    expect_count_after_sleep: 1,
                },
                AvailTest {
                    about: "should fill only once after 1*interval before 2*interval",
                    capacity: 2,
                    fill_interval: Duration::from_secs(2),
                    take: 1,
                    sleep: Duration::from_secs(3),
                    expect_count_after_take: 1,
                    expect_count_after_sleep: 2,
                },
            ]
        }
    }

    #[test]
    fn t_take() {
        for (i, test) in TakeTest::new().iter().enumerate() {
            let mut bucket = Bucket::new_bucket(test.fill_interval, test.capacity);
            for (j, req) in test.reqs.iter().enumerate() {
                let d = bucket.inner_take(
                    bucket.epoch_time.add(req.time),
                    req.count,
                    INFINITY_DURATION,
                );
                assert!(d.is_some());
                assert_eq!(
                    d.unwrap(),
                    req.expect_wait,
                    "test {}.{}, {}, got {:?} want {:?}",
                    i,
                    j,
                    test.about,
                    d,
                    req.expect_wait
                );
                // println!("token {:?}, {}, {}, {:?}", req, bucket.available_tokens, bucket.latest_tick, bucket.epoch_time);
            }
        }
    }

    #[test]
    fn t_take_max_duration() {
        for (i, test) in TakeTest::new().iter().enumerate() {
            let mut bucket = Bucket::new_bucket(test.fill_interval, test.capacity);
            for (j, req) in test.reqs.iter().enumerate() {
                if req.expect_wait > Duration::from_nanos(0) {
                    let d = bucket.inner_take(
                        bucket.epoch_time.add(req.time),
                        req.count,
                        req.expect_wait.sub(Duration::from_nanos(1)),
                    );
                    assert!(d.is_none(), "{}:{}", i, j);
                }
                let d =
                    bucket.inner_take(bucket.epoch_time.add(req.time), req.count, req.expect_wait);
                assert!(d.is_some(), "{}:{}", i, j);
                assert_eq!(
                    d.unwrap(),
                    req.expect_wait,
                    "test {}.{}, {}, got {:?} want {:?}",
                    i,
                    j,
                    test.about,
                    d.unwrap(),
                    req.expect_wait
                );
            }
        }
    }

    struct TakeAvailableReq {
        time: Duration,
        count: i64,
        expect: i64,
    }

    impl Default for TakeAvailableReq {
        fn default() -> Self {
            TakeAvailableReq {
                time: Duration::from_secs(0),
                count: 0,
                expect: 0,
            }
        }
    }

    struct TakeAvailableTests {
        about: &'static str,
        fill_interval: Duration,
        capacity: i64,
        reqs: Vec<TakeAvailableReq>,
    }

    impl TakeAvailableTests {
        fn new() -> Vec<Self> {
            vec![
                TakeAvailableTests {
                    about: "serial requests",
                    fill_interval: Duration::from_millis(250),
                    capacity: 10,
                    reqs: vec![
                        TakeAvailableReq {
                            ..Default::default()
                        },
                        TakeAvailableReq {
                            count: 10,
                            expect: 10,
                            ..Default::default()
                        },
                        TakeAvailableReq {
                            count: 1,
                            ..Default::default()
                        },
                        TakeAvailableReq {
                            time: Duration::from_millis(250),
                            ..Default::default()
                        },
                    ],
                },
                TakeAvailableTests {
                    about: "concurrent requests",
                    fill_interval: Duration::from_millis(250),
                    capacity: 10,
                    reqs: vec![
                        TakeAvailableReq {
                            count: 5,
                            expect: 5,
                            ..Default::default()
                        },
                        TakeAvailableReq {
                            count: 2,
                            expect: 2,
                            ..Default::default()
                        },
                        TakeAvailableReq {
                            count: 5,
                            expect: 3,
                            ..Default::default()
                        },
                        TakeAvailableReq {
                            count: 1,
                            ..Default::default()
                        },
                    ],
                },
                TakeAvailableTests {
                    about: "more than capacity",
                    fill_interval: Duration::from_millis(1),
                    capacity: 10,
                    reqs: vec![
                        TakeAvailableReq {
                            count: 10,
                            expect: 10,
                            ..Default::default()
                        },
                        TakeAvailableReq {
                            time: Duration::from_millis(20),
                            count: 15,
                            expect: 10,
                        },
                    ],
                },
                TakeAvailableTests {
                    about: "within capacity",
                    fill_interval: Duration::from_millis(10),
                    capacity: 5,
                    reqs: vec![
                        TakeAvailableReq {
                            count: 5,
                            expect: 5,
                            ..Default::default()
                        },
                        TakeAvailableReq {
                            time: Duration::from_millis(60),
                            count: 5,
                            expect: 5,
                        },
                        TakeAvailableReq {
                            time: Duration::from_millis(70),
                            count: 1,
                            expect: 1,
                        },
                    ],
                },
            ]
        }
    }

    #[test]
    fn t_take_available() {
        for (i, test) in TakeAvailableTests::new().iter().enumerate() {
            let mut bucket = Bucket::new_bucket(test.fill_interval, test.capacity);
            for (j, req) in test.reqs.iter().enumerate() {
                let d = bucket.inner_take_available(bucket.epoch_time.add(req.time), req.count);
                assert_eq!(
                    d, req.expect,
                    "test {}.{}, {}, got {}, want {}",
                    i, j, test.about, d, req.expect
                );
            }
        }
    }

    #[test]
    fn t_into_panic() {
        assert_panic(0, 1, true);
        assert_panic(1, 0, true);
        assert_panic(1, -2, true);
    }

    #[test]
    fn t_rate() {
        // let bucket = Bucket::new_bucket(Duration::from_nanos(1), 1);
        // assert!(is_close_to(bucket.rate(), 1e9, 0.00001), "got {}, want {}", bucket.rate(), 1e9);
        // let bucket = Bucket::new_bucket(Duration::from_secs(2), 1);
        // assert!(is_close_to(bucket.rate(), 0.5, 0.00001), "got {}, want {}", bucket.rate(), 0.5);
        // let bucket = Bucket::new_bucket_with_quantum(Duration::from_millis(100), 1, 5);
        // assert!(is_close_to(bucket.rate(), 50.0, 0.00001), "got {}, want {}", bucket.rate(), 50.0);
        //
        // for rate in (1..1000_000).step_by(7) {
        //     check_rate(rate as f64);
        // }
        //
        // for rate in vec![
        //     1024.0 * 1024.0 * 1024.0,
        //     1e-5,
        //     0.9e-5,
        //     0.5,
        //     0.9,
        //     0.9e8,
        //     3e12,
        //     4e18,
        //     i64::max_value() as f64] {
        //     check_rate(rate);
        //     check_rate(rate / 3.0);
        //     check_rate(rate * 1.3);
        // }
    }

    #[test]
    fn t_available() {
        for (i, test) in AvailTest::new().iter().enumerate() {
            let mut bucket = Bucket::new_bucket(test.fill_interval, test.capacity);
            let c = bucket.inner_take_available(bucket.epoch_time, test.take);
            assert_eq!(
                c, test.take,
                "#{}: {}, take = {}, wait = {}",
                i, test.about, c, test.take
            );
            let c = bucket.inner_available(bucket.epoch_time);
            assert_eq!(
                c, test.expect_count_after_take,
                "#{}: {}, after take, available = {}, wait = {}",
                i, test.about, c, test.expect_count_after_take
            );
            let c = bucket.inner_available(bucket.epoch_time.add(test.sleep));
            assert_eq!(
                c, test.expect_count_after_sleep,
                "#{}: {}, after some time it should fill in new tokens, available = {}, wait = {}",
                i, test.about, c, test.expect_count_after_sleep
            );
        }
    }

    #[test]
    fn t_no_bonus_tokens_after_bucket_is_full() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut bucket = Bucket::new_bucket_with_quantum(Duration::from_secs(1), 100, 20);
            let cur_available = bucket.available();
            assert_eq!(
                cur_available, 100,
                "initially: actual available = {}, expected = {}",
                cur_available, 100
            );

            sleep(Duration::from_secs(5)).await;
            let cur_available = bucket.available();
            assert_eq!(
                cur_available, 100,
                "initially: actual available = {}, expected = {}",
                cur_available, 100
            );

            let cnt = bucket.take_available(100).await;
            assert_eq!(
                cnt, 100,
                "initially: actual available = {}, expected = {}",
                cnt, 100
            );

            let cur_available = bucket.available();
            assert_eq!(
                cur_available, 0,
                "after token: acutal available = {}, expected = {}",
                cur_available, 0
            );
        });
    }

    fn assert_panic(fill_interval: u64, capacity: i64, expect: bool) {
        let catch = catch_unwind(|| {
            Bucket::new_bucket(Duration::from_millis(fill_interval), capacity);
        });
        assert_eq!(catch.is_err(), expect);
    }

    fn check_rate(rate: f64) {
        let mut bucket = Bucket::new_bucket_with_rate(rate, 1 << 62);
        assert!(
            is_close_to(bucket.rate(), rate, RATE_MARGIN),
            "got {}, want {}",
            bucket.rate(),
            rate
        );
        let d = bucket.inner_take(bucket.epoch_time, 1 << 62, INFINITY_DURATION);
        assert!(d.is_some());
        assert_eq!(d.unwrap(), Duration::from_secs(0));

        // Checks that the actual rate is as expected by
        // asking for a not-quite multiple of the bucket's
        // quantum and checking that the wait time correct.
        let d = bucket.inner_take(
            bucket.epoch_time,
            bucket.quantum * 2 - bucket.quantum / 2,
            INFINITY_DURATION,
        );
        assert!(d.is_some());
        let expected_time = 1e9 * 2.0 * bucket.quantum as f64 / rate;
        let d = d.unwrap().as_nanos() as f64;
        assert!(
            is_close_to(d, expected_time, RATE_MARGIN),
            "rate:{}, got {}, want {:?}",
            rate,
            d,
            expected_time
        );
    }

    fn is_close_to(x: f64, y: f64, tolerance: f64) -> bool {
        (x - y).abs() / y < tolerance
    }

    #[test]
    fn draw_plot() {
        use plotlib::page::Page;
        use plotlib::repr::Plot;
        use plotlib::view::ContinuousView;
        use plotlib::style::{PointMarker, PointStyle};

        let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();
        let token_data: Arc<Mutex<Vec<(f64, f64)>>> = Arc::new(Mutex::new(vec![]));
        let mut bucket = Bucket::new_bucket(Duration::from_millis(1), 1 << 62);

        rt.block_on(async {
            let now = SystemTime::now();
            for i in 0..100 {
                let bucket = bucket.clone();
                let token_data = token_data.clone();
                rt.spawn(async move {
                    let d1 = SystemTime::now().duration_since(now).unwrap().as_nanos() as f64;
                    let duration = bucket.take(1).await;
                    if duration.as_nanos() > 0 {
                        bucket.clock.sleep(duration).await;
                    }
                    let mut token_data = token_data.lock().unwrap();
                    let d2 = SystemTime::now().duration_since(now).unwrap().as_nanos() as f64;
                    token_data.push((d1 / 1000000.0, d2 / 100000.0));
                });
            }
        });
        ::std::thread::sleep(Duration::from_secs(3));
        // We create our scatter plot from the data
        let v = token_data.lock().unwrap().clone();
        println!("{:?}", v);

        let s1: Plot = Plot::new(v).point_style(
            PointStyle::new()
                .marker(PointMarker::Square) // setting the marker to be a square
                .colour("#DD3355"),
        ); // and a custom colour
        let v = ContinuousView::new()
            .add(s1)
            .x_range(0., 10.)
            .y_range(0., 100.)
            .x_label("Some varying variable")
            .y_label("The response of something");

        // A page with a single view is then saved to an SVG file
        Page::single(&v).save("scatter.svg").unwrap();
    }

    extern crate test;

    #[bench]
    fn benchmarks(b: &mut Bencher) {
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let count = Arc::new(AtomicUsize::new(1));
        rt.block_on(async {
            let bucket = Arc::new(Bucket::new_bucket(Duration::from_nanos(1), 16 * 1024));
            b.iter(async || {
                bucket.wait(1).await;
                count.fetch_add(1, Ordering::SeqCst);
            });
            tokio::fs::write("count.text", format!("{}", count.load(Ordering::SeqCst))).await;
        });
    }
}
