//! The clock the mock reads time from.
//!
//! TTL expiry and stream timestamps are the only time-dependent behaviour, and
//! both are decided by [`Clock`]. Tests inject a [`MockClock`] to move time
//! forward without sleeping; everything else gets [`SystemClock`].
//!
//! This is hand-rolled because no maintained crate freezes [`SystemTime`]:
//! `mock_instant` 0.6.1 mocks `std::time::Instant` only (TTL and stream
//! timestamps are `SystemTime`), and `chronobreak` 0.1.0 is unmaintained since
//! 2021. Only the two types above are needed, so the trait stays local.

use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

/// A source of "now".
///
/// ```
/// use std::time::{Duration, SystemTime};
/// use mock_ddb::{Clock, MockClock};
///
/// let clock = MockClock::new(SystemTime::UNIX_EPOCH);
/// clock.advance(Duration::from_secs(60));
/// assert_eq!(clock.now(), SystemTime::UNIX_EPOCH + Duration::from_secs(60));
/// ```
pub trait Clock: Send + Sync + std::fmt::Debug {
    /// The current instant.
    fn now(&self) -> SystemTime;
}

/// The real clock.
///
/// ```
/// use mock_ddb::{Clock, SystemClock};
/// assert!(SystemClock.now() > std::time::UNIX_EPOCH);
/// ```
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// A clock a test controls.
///
/// ```
/// use std::time::{Duration, SystemTime};
/// use mock_ddb::MockClock;
///
/// let clock = MockClock::at_epoch_seconds(1_700_000_000);
/// clock.advance(Duration::from_secs(3_600));
/// assert_eq!(clock.epoch_seconds(), 1_700_003_600);
/// ```
#[derive(Debug)]
pub struct MockClock {
    now: Mutex<SystemTime>,
}

impl MockClock {
    /// A clock pinned to an instant.
    #[must_use]
    ///
    /// ```
    /// use std::time::SystemTime;
    /// use mock_ddb::MockClock;
    ///
    /// let clock = MockClock::new(SystemTime::UNIX_EPOCH);
    /// assert_eq!(clock.epoch_seconds(), 0);
    /// ```
    pub fn new(at: SystemTime) -> Self {
        Self {
            now: Mutex::new(at),
        }
    }

    /// A clock pinned to `seconds` since the Unix epoch.
    ///
    /// The constructor is total over `u64`: a `seconds` past the largest
    /// instant this platform can represent is clamped to that instant instead
    /// of panicking, so `at_epoch_seconds(u64::MAX)` is a clock far in the
    /// future rather than an aborted test binary.
    ///
    /// ```
    /// use mock_ddb::MockClock;
    ///
    /// let clock = MockClock::at_epoch_seconds(1_700_000_000);
    /// assert_eq!(clock.epoch_seconds(), 1_700_000_000);
    /// ```
    ///
    /// Asking for more than the platform can hold pins the clock to the last
    /// representable instant, and it stays there:
    ///
    /// ```
    /// use std::time::Duration;
    /// use mock_ddb::{Clock, MockClock};
    ///
    /// let clock = MockClock::at_epoch_seconds(u64::MAX);
    /// let last = clock.now();
    /// clock.advance(Duration::from_secs(1));
    /// assert_eq!(clock.now(), last);
    /// ```
    #[must_use]
    pub fn at_epoch_seconds(seconds: u64) -> Self {
        Self::new(saturating_add(UNIX_EPOCH, Duration::from_secs(seconds)))
    }

    /// Move the clock to an instant.
    ///
    /// ```
    /// use std::time::{Duration, UNIX_EPOCH};
    /// use mock_ddb::MockClock;
    ///
    /// let clock = MockClock::at_epoch_seconds(1);
    /// clock.set(UNIX_EPOCH + Duration::from_secs(60));
    /// assert_eq!(clock.epoch_seconds(), 60);
    /// ```
    ///
    /// Every representable [`SystemTime`] is accepted. This method does no
    /// arithmetic on the instant, so it cannot overflow.
    pub fn set(&self, at: SystemTime) {
        *self.now.lock() = at;
    }

    /// Move the clock forward.
    ///
    /// ```
    /// use std::time::Duration;
    /// use mock_ddb::MockClock;
    ///
    /// let clock = MockClock::at_epoch_seconds(100);
    /// clock.advance(Duration::from_secs(5));
    /// assert_eq!(clock.epoch_seconds(), 105);
    /// ```
    ///
    /// A step that would leave the representable range stops at the last
    /// representable instant instead of panicking, so the method is total over
    /// [`Duration`] — [`Duration::MAX`] included — and never moves the clock
    /// backwards:
    ///
    /// ```
    /// use std::time::Duration;
    /// use mock_ddb::{Clock, MockClock};
    ///
    /// let clock = MockClock::at_epoch_seconds(1);
    /// clock.advance(Duration::MAX);
    /// let last = clock.now();
    /// clock.advance(Duration::from_secs(1));
    /// assert_eq!(clock.now(), last);
    /// ```
    pub fn advance(&self, by: Duration) {
        let mut now = self.now.lock();
        *now = saturating_add(*now, by);
    }

    /// The current instant, in whole seconds since the Unix epoch.
    #[must_use]
    ///
    /// ```
    /// use mock_ddb::MockClock;
    ///
    /// let clock = MockClock::at_epoch_seconds(42);
    /// assert_eq!(clock.epoch_seconds(), 42);
    /// ```
    pub fn epoch_seconds(&self) -> i64 {
        epoch_seconds(self.now())
    }
}

impl Clock for MockClock {
    fn now(&self) -> SystemTime {
        *self.now.lock()
    }
}

/// `time + by`, clamped to the largest instant this platform can represent.
///
/// The `Add` operators of [`SystemTime`] panic when the result is not
/// representable. The clock is a test tool, so a large `u64` or `Duration` must
/// not abort the test binary: the overflow arm saturates at the platform's last
/// instant instead.
fn saturating_add(time: SystemTime, by: Duration) -> SystemTime {
    time.checked_add(by).unwrap_or_else(max_instant)
}

/// The largest instant a [`SystemTime`] can represent here.
///
/// `checked_add` succeeds for a prefix of the whole-second offsets from the
/// epoch, so a bisection over `u64` seconds finds the boundary in at most 64
/// steps and never performs an addition that could itself overflow. The result
/// is platform-dependent (about `i64::MAX` seconds on 64-bit Unix, less on
/// Windows, whose clock is narrower than `i64` seconds) and computed once.
fn max_instant() -> SystemTime {
    static MAX: OnceLock<SystemTime> = OnceLock::new();
    *MAX.get_or_init(|| {
        let (mut inside, mut outside) = (0_u64, u64::MAX);
        let mut best = UNIX_EPOCH;
        while outside - inside > 1 {
            let mid = inside + (outside - inside) / 2;
            match UNIX_EPOCH.checked_add(Duration::from_secs(mid)) {
                Some(instant) => {
                    inside = mid;
                    best = instant;
                }
                None => outside = mid,
            }
        }
        best
    })
}

/// Seconds since the Unix epoch.
///
/// Whole seconds, truncated towards the epoch: `0.9 s` after the epoch is `0`,
/// not `1`. Every instant before the epoch maps to the sentinel `-1` (not `0`,
/// which is a real second), so `time <= UNIX_EPOCH` is the only way to get a
/// negative value. Instants further out than `i64::MAX` seconds saturate at
/// `i64::MAX`; that arm is only reachable on platforms whose clock is wider
/// than `i64` seconds (Windows), not on 64-bit Unix.
pub(crate) fn epoch_seconds(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        Err(_) => -1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::prelude::*;

    /// The sentinel `epoch_seconds` returns for every pre-epoch instant.
    const BEFORE_EPOCH: i64 = -1;

    /// An instant `seconds` plus `nanos` away from the epoch, on either side.
    fn instant(seconds: i64, nanos: u32) -> SystemTime {
        let magnitude = Duration::new(seconds.unsigned_abs(), nanos % 1_000_000_000);
        if seconds < 0 {
            UNIX_EPOCH
                .checked_sub(magnitude)
                .expect("test offsets stay inside the representable range")
        } else {
            UNIX_EPOCH
                .checked_add(magnitude)
                .expect("test offsets stay inside the representable range")
        }
    }

    /// `(seconds, nanos)` pairs on both sides of the epoch, including both ends.
    fn offsets() -> impl Strategy<Value = (i64, u32)> {
        (
            prop_oneof![
                -1_000_000_000_000_i64..1_000_000_000_000,
                Just(i64::MAX),
                Just(i64::MAX - 1),
                Just(0),
                Just(-1),
                Just(1),
                Just(i64::MIN + 1),
            ],
            0..1_000_000_000_u32,
        )
    }

    #[test]
    fn epoch_itself_is_second_zero() {
        assert_eq!(epoch_seconds(UNIX_EPOCH), 0);
    }

    #[test]
    fn one_second_before_the_epoch_is_the_sentinel_not_zero() {
        // The just-inside/just-outside pair: 0 s is `0`, 1 s earlier is `-1`.
        assert_eq!(epoch_seconds(instant(0, 0)), 0);
        assert_eq!(epoch_seconds(instant(-1, 0)), BEFORE_EPOCH);
        assert_eq!(epoch_seconds(instant(-3_600, 0)), BEFORE_EPOCH);
    }

    #[test]
    fn one_second_after_the_epoch_is_one() {
        assert_eq!(epoch_seconds(instant(1, 0)), 1);
        assert_eq!(epoch_seconds(instant(2, 0)), 2);
    }

    #[test]
    fn sub_second_offsets_are_truncated_towards_the_epoch() {
        assert_eq!(epoch_seconds(instant(0, 999_999_999)), 0);
        assert_eq!(epoch_seconds(instant(1, 999_999_999)), 1);
        // A hair before the epoch is still before the epoch.
        assert_eq!(epoch_seconds(instant(-1, 1)), BEFORE_EPOCH);
    }

    #[test]
    fn i64_max_seconds_is_the_last_representable_second() {
        let last = UNIX_EPOCH
            .checked_add(Duration::from_secs(i64::MAX.unsigned_abs()))
            .expect("i64::MAX seconds is representable on a 64-bit clock");
        assert_eq!(epoch_seconds(last), i64::MAX);
    }

    #[test]
    fn a_mock_clock_before_the_epoch_reports_the_sentinel() {
        let clock = MockClock::new(instant(-1, 0));
        assert_eq!(clock.epoch_seconds(), BEFORE_EPOCH);
        clock.advance(Duration::from_secs(2));
        assert_eq!(clock.epoch_seconds(), 1);
    }

    /// Whole seconds between the epoch and the last representable instant.
    fn max_seconds() -> u64 {
        max_instant()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_secs())
    }

    #[test]
    fn the_last_representable_second_is_kept_exactly() {
        // Just inside the boundary: no clamping, the instant is exact.
        let clock = MockClock::at_epoch_seconds(max_seconds());
        assert_eq!(clock.now(), max_instant());
    }

    #[test]
    fn one_second_past_the_last_representable_second_is_clamped() {
        // Just outside the boundary: one second further does not exist, so the
        // constructor saturates instead of panicking.
        let clock = MockClock::at_epoch_seconds(max_seconds().saturating_add(1));
        assert_eq!(clock.now(), max_instant());
        assert_eq!(MockClock::at_epoch_seconds(u64::MAX).now(), max_instant());
    }

    #[test]
    fn advancing_one_second_past_the_last_instant_clamps_and_stays() {
        let clock = MockClock::at_epoch_seconds(max_seconds().saturating_sub(1));
        clock.advance(Duration::from_secs(2));
        assert_eq!(clock.now(), max_instant());
        // Clamping is a fixed point: further steps do not move the clock.
        clock.advance(Duration::from_secs(1));
        assert_eq!(clock.now(), max_instant());
    }

    #[test]
    fn advancing_by_duration_max_from_the_epoch_clamps() {
        let clock = MockClock::new(UNIX_EPOCH);
        clock.advance(Duration::MAX);
        assert_eq!(clock.now(), max_instant());
    }

    proptest! {
        #[test]
        fn seconds_round_trip_on_the_far_side_of_the_epoch((seconds, nanos) in offsets().prop_filter("non-negative", |(s, _)| *s >= 0)) {
            prop_assert_eq!(epoch_seconds(instant(seconds, nanos)), seconds);
        }

        #[test]
        fn every_pre_epoch_instant_is_the_sentinel((seconds, nanos) in offsets().prop_filter("negative", |(s, _)| *s < 0)) {
            prop_assert_eq!(epoch_seconds(instant(seconds, nanos)), BEFORE_EPOCH);
        }

        #[test]
        fn nanos_never_change_the_result((seconds, _) in offsets().prop_filter("non-negative", |(s, _)| *s >= 0), nanos in 0..1_000_000_000_u32) {
            prop_assert_eq!(epoch_seconds(instant(seconds, nanos)), epoch_seconds(instant(seconds, 0)));
        }

        #[test]
        fn the_result_is_monotonic(lo in offsets(), hi in offsets()) {
            let (lo, hi) = (instant(lo.0, lo.1), instant(hi.0, hi.1));
            let (lo, hi) = if lo <= hi { (lo, hi) } else { (hi, lo) };
            prop_assert!(epoch_seconds(lo) <= epoch_seconds(hi));
        }

        #[test]
        fn negative_only_before_the_epoch((seconds, nanos) in offsets()) {
            let time = instant(seconds, nanos);
            let negative = epoch_seconds(time) < 0;
            prop_assert_eq!(negative, time < UNIX_EPOCH);
        }

        /// `advance` is total and agrees with `checked_add` wherever that
        /// succeeds; the overflowing side saturates at the last instant.
        #[test]
        fn advance_is_total_and_never_moves_backwards(start in 0..u64::MAX, by in 0..u64::MAX) {
            let clock = MockClock::at_epoch_seconds(start);
            let before = clock.now();
            clock.advance(Duration::from_secs(by));
            let after = clock.now();
            prop_assert!(after >= before);
            if let Some(exact) = before.checked_add(Duration::from_secs(by)) {
                prop_assert_eq!(after, exact);
            } else {
                prop_assert_eq!(after, max_instant());
            }
        }
    }
}
