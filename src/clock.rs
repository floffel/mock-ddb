//! The clock the mock reads time from.
//!
//! TTL expiry and stream timestamps are the only time-dependent behaviour, and
//! both are decided by [`Clock`]. Tests inject a [`MockClock`] to move time
//! forward without sleeping; everything else gets [`SystemClock`].

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
    #[must_use]
    ///
    /// ```
    /// use mock_ddb::MockClock;
    ///
    /// let clock = MockClock::at_epoch_seconds(1_700_000_000);
    /// assert_eq!(clock.epoch_seconds(), 1_700_000_000);
    /// ```
    pub fn at_epoch_seconds(seconds: u64) -> Self {
        Self::new(UNIX_EPOCH + Duration::from_secs(seconds))
    }

    ///
    /// ```
    /// use std::time::{Duration, UNIX_EPOCH};
    /// use mock_ddb::MockClock;
    ///
    /// let clock = MockClock::at_epoch_seconds(1);
    /// clock.set(UNIX_EPOCH + Duration::from_secs(60));
    /// assert_eq!(clock.epoch_seconds(), 60);
    /// ```
    /// Move the clock to an instant.
    pub fn set(&self, at: SystemTime) {
        *self.now.lock() = at;
    }

    ///
    /// ```
    /// use std::time::Duration;
    /// use mock_ddb::MockClock;
    ///
    /// let clock = MockClock::at_epoch_seconds(100);
    /// clock.advance(Duration::from_secs(5));
    /// assert_eq!(clock.epoch_seconds(), 105);
    /// ```
    /// Move the clock forward.
    pub fn advance(&self, by: Duration) {
        let mut now = self.now.lock();
        *now += by;
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

/// Seconds since the Unix epoch, saturating at the epoch for pre-epoch times.
pub(crate) fn epoch_seconds(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        Err(_) => -1,
    }
}
