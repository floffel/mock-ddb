//! The clock a caller injects is user code: it may block (I/O, a channel, a
//! condvar) and it may call back into the mock it is registered on. Neither may
//! stall another task or deadlock, which means `Clock::now` must never run
//! while the store lock is held — these tests park a write inside `now` and
//! then demand that the store still answers.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mock_ddb::{item, s, Clock, KeySchema, KeyType, ListTablesRequest, MockDdb, PutItemRequest};

/// How long a test waits for work that must not block. Anything near this means
/// the operation deadlocked, not that the machine is slow.
const PATIENCE: Duration = Duration::from_secs(10);

fn counters() -> Arc<MockDdb> {
    Arc::new(MockDdb::with_table("counters", KeySchema::hash("pk", KeyType::String)).unwrap())
}

/// A latch a test opens from another thread.
///
/// `wait` gives up after [`PATIENCE`] and lets the caller continue, so a
/// regression fails an assertion instead of hanging the test binary: a task
/// parked in `Clock::now` cannot be cancelled by dropping its handle.
#[derive(Debug, Default)]
struct Gate {
    open: Mutex<bool>,
    opened: Condvar,
}

impl Gate {
    fn wait(&self) {
        let mut open = self
            .open
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !*open {
            let (guard, timeout) = self
                .opened
                .wait_timeout(open, PATIENCE)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            open = guard;
            if timeout.timed_out() {
                return;
            }
        }
    }

    fn open(&self) {
        *self
            .open
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.opened.notify_all();
    }
}

/// A clock that parks inside `now` until the test opens its gate.
#[derive(Debug)]
struct GatedClock {
    gate: Arc<Gate>,
    /// Set once `now` has been entered, so the test knows the write is inside
    /// the clock rather than still queueing for a lock.
    entered: Arc<AtomicBool>,
    at: i64,
}

impl Clock for GatedClock {
    fn now(&self) -> SystemTime {
        self.entered.store(true, Ordering::SeqCst);
        self.gate.wait();
        UNIX_EPOCH + Duration::from_secs(u64::try_from(self.at).unwrap())
    }
}

/// A clock that asks the mock it is registered on for a count, once.
///
/// Before the fix every write called `Clock::now` under the write guard, so this
/// read re-entered a lock the same thread already held — a deadlock, because
/// `parking_lot::RwLock` is not reentrant.
#[derive(Debug)]
struct ReentrantClock {
    mock: Weak<MockDdb>,
    reentered: AtomicBool,
    at: i64,
}

impl Clock for ReentrantClock {
    fn now(&self) -> SystemTime {
        if !self.reentered.swap(true, Ordering::SeqCst) {
            if let Some(mock) = self.mock.upgrade() {
                assert_eq!(
                    mock.count("counters"),
                    1,
                    "the re-entrant read sees the seeded row"
                );
            }
        }
        UNIX_EPOCH + Duration::from_secs(u64::try_from(self.at).unwrap())
    }
}

fn put(key: &str) -> PutItemRequest {
    PutItemRequest::new("counters", item([("pk", s(key))]))
}

/// The store answers while a write is parked inside `Clock::now`.
///
/// `list_tables` needs no time, so it is the probe: a mock that held the store
/// lock across the clock call would make it wait for the gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_blocked_clock_does_not_hold_the_store_lock() {
    let gate = Arc::new(Gate::default());
    let entered = Arc::new(AtomicBool::new(false));
    let mock = counters();
    mock.set_clock(Arc::new(GatedClock {
        gate: Arc::clone(&gate),
        entered: Arc::clone(&entered),
        at: 1_000_000,
    }));

    let writer = tokio::spawn({
        let mock = Arc::clone(&mock);
        async move { mock.put_item(put("parked")).await }
    });

    for _ in 0..2_000 {
        if entered.load(Ordering::SeqCst) {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        entered.load(Ordering::SeqCst),
        "the write reached the clock"
    );

    let listed = tokio::time::timeout(PATIENCE, mock.list_tables(ListTablesRequest::default()))
        .await
        .expect("the store lock must be free while the clock is parked")
        .unwrap();
    assert_eq!(listed.table_names, vec!["counters".to_owned()]);

    gate.open();
    writer.await.unwrap().unwrap();
    assert_eq!(mock.count("counters"), 1);
}

/// A clock that reads the mock does not deadlock the write that consulted it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clock_may_re_enter_the_mock() {
    let mock = counters();
    mock.seed("counters", vec![item([("pk", s("seed"))])])
        .unwrap();
    mock.set_clock(Arc::new(ReentrantClock {
        mock: Arc::downgrade(&mock),
        reentered: AtomicBool::new(false),
        at: 2_000_000,
    }));

    let writer = tokio::spawn({
        let mock = Arc::clone(&mock);
        async move { mock.put_item(put("after")).await }
    });
    let written = tokio::time::timeout(PATIENCE, writer)
        .await
        .expect("a re-entrant clock must not deadlock the write")
        .unwrap()
        .unwrap();

    assert!(written.attributes.is_none());
    assert_eq!(mock.count("counters"), 2);
}

/// A clock that returns at once and a clock that parks decide identically:
/// same item, same count, same TTL verdict. Only the latency differs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_clock_and_a_fast_clock_write_the_same_item() {
    let fast = counters();
    fast.set_clock(Arc::new(mock_ddb::MockClock::at_epoch_seconds(1_000_000)));
    fast.put_item(put("same")).await.unwrap();

    let gate = Arc::new(Gate::default());
    let entered = Arc::new(AtomicBool::new(false));
    let slow = counters();
    slow.set_clock(Arc::new(GatedClock {
        gate: Arc::clone(&gate),
        entered: Arc::clone(&entered),
        at: 1_000_000,
    }));
    gate.open();
    slow.put_item(put("same")).await.unwrap();

    assert_eq!(fast.dump("counters"), slow.dump("counters"));
    assert_eq!(fast.count("counters"), 1);
    assert_eq!(slow.count("counters"), 1);
    assert!(
        entered.load(Ordering::SeqCst),
        "the slow clock was consulted"
    );
}
