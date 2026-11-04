//! `ConsumedCapacity` and `ItemCollectionMetrics`: what an operation charged.
//!
//! The mock charges capacity the way the service does — one read unit per
//! started 4 KiB of an item read, one write unit per started 1 KiB of an item
//! written — and reports it when the request asks for it through
//! `return_consumed_capacity`. Nothing is enforced: there is no provisioned
//! throughput and no throttling, so a response can never be
//! `ProvisionedThroughputExceededException`. The numbers exist so a caller can
//! log, assert or budget against the same field names it reads in production.
//!
//! ```
//! use mock_ddb::{item, s, KeySchema, KeyType, MockDdb, PutItemRequest, ReturnConsumedCapacity};
//!
//! # #[tokio::main]
//! # async fn main() {
//! let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
//! let response = mock
//!     .put_item(
//!         PutItemRequest::new("users", item([("pk", s("u#1"))]))
//!             .with_return_consumed_capacity(ReturnConsumedCapacity::Total),
//!     )
//!     .await
//!     .unwrap();
//! let capacity = response.consumed_capacity.unwrap();
//! assert_eq!(capacity.table_name.as_deref(), Some("users"));
//! assert_eq!(capacity.capacity_units, Some(1.0));
//! # }
//! ```

use std::collections::BTreeMap;

use crate::table::IndexScope;
use crate::Item;

/// Bytes one read capacity unit covers: the service's 4 KiB.
///
/// ```
/// use mock_ddb::READ_UNIT_BYTES;
///
/// assert_eq!(READ_UNIT_BYTES, 4096);
/// ```
pub const READ_UNIT_BYTES: usize = 4 * 1024;

/// Bytes one write capacity unit covers: the service's 1 KiB.
///
/// ```
/// use mock_ddb::WRITE_UNIT_BYTES;
///
/// assert_eq!(WRITE_UNIT_BYTES, 1024);
/// ```
pub const WRITE_UNIT_BYTES: usize = 1024;

/// The step of an item collection's size estimate: the service reports whole
/// gibibytes, never bytes.
///
/// ```
/// use mock_ddb::COLLECTION_ESTIMATE_STEP_BYTES;
///
/// assert_eq!(COLLECTION_ESTIMATE_STEP_BYTES, 1024 * 1024 * 1024);
/// ```
pub const COLLECTION_ESTIMATE_STEP_BYTES: usize = 1024 * 1024 * 1024;

/// The capacity units a strongly consistent read of `bytes` bytes consumes.
///
/// One unit per started 4 KiB, and never less than one: a read that finds
/// nothing still costs the service a unit, so it costs one here too.
///
/// ```
/// use mock_ddb::{read_units, READ_UNIT_BYTES};
///
/// assert_eq!(read_units(0), 1.0);
/// assert_eq!(read_units(READ_UNIT_BYTES), 1.0);
/// assert_eq!(read_units(READ_UNIT_BYTES + 1), 2.0);
/// ```
#[must_use]
pub fn read_units(bytes: usize) -> f64 {
    started_units(bytes, READ_UNIT_BYTES)
}

/// The capacity units a write of `bytes` bytes consumes.
///
/// One unit per started 1 KiB, and never less than one, which is what a delete
/// of a key the table does not hold costs.
///
/// ```
/// use mock_ddb::{write_units, WRITE_UNIT_BYTES};
///
/// assert_eq!(write_units(0), 1.0);
/// assert_eq!(write_units(WRITE_UNIT_BYTES), 1.0);
/// assert_eq!(write_units(WRITE_UNIT_BYTES + 1), 2.0);
/// ```
#[must_use]
pub fn write_units(bytes: usize) -> f64 {
    started_units(bytes, WRITE_UNIT_BYTES)
}

/// The whole-gibibyte range an item collection of `bytes` bytes falls in.
///
/// The service reports a lower bound rounded down to whole gibibytes and a
/// range one gibibyte wide, because it only tracks approximate collection
/// sizes. The mock computes the exact bytes and then reports the same shape, so
/// a caller that logs the estimate sees what it would see in production.
///
/// ```
/// use mock_ddb::{size_estimate_range, COLLECTION_ESTIMATE_STEP_BYTES};
///
/// assert_eq!(size_estimate_range(0), [0.0, 1.0]);
/// assert_eq!(
///     size_estimate_range(COLLECTION_ESTIMATE_STEP_BYTES - 1),
///     [0.0, 1.0]
/// );
/// assert_eq!(size_estimate_range(COLLECTION_ESTIMATE_STEP_BYTES), [1.0, 2.0]);
/// ```
#[must_use]
pub fn size_estimate_range(bytes: usize) -> [f64; 2] {
    let whole = bytes / COLLECTION_ESTIMATE_STEP_BYTES;
    // Saturation rather than a lossy cast: the estimate is a small integer.
    let lower = f64::from(u32::try_from(whole).unwrap_or(u32::MAX));
    [lower, lower + 1.0]
}

/// How many units `bytes` costs when one unit covers `unit_bytes`.
fn started_units(bytes: usize, unit_bytes: usize) -> f64 {
    let started = bytes.div_ceil(unit_bytes).max(1);
    // Saturation rather than a lossy cast: an item is capped at 400 KiB, so the
    // only value that can ever reach the ceiling is an invalid one.
    f64::from(u32::try_from(started).unwrap_or(u32::MAX))
}

/// Whether an operation reports its capacity and how much of it.
///
/// `NONE` is the default and reports nothing; `TOTAL` reports the summed units
/// on [`ConsumedCapacity::capacity_units`]; `INDEXES` adds the per-table and
/// per-index breakdown, which is what a test asserting on index cost needs.
///
/// ```
/// use mock_ddb::ReturnConsumedCapacity;
///
/// assert_eq!(ReturnConsumedCapacity::default(), ReturnConsumedCapacity::None);
/// assert_eq!(ReturnConsumedCapacity::Indexes.as_str(), "INDEXES");
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReturnConsumedCapacity {
    /// Report nothing (the default).
    #[default]
    None,
    /// Report the summed units per table.
    Total,
    /// Report the units per table and per secondary index.
    Indexes,
}

impl ReturnConsumedCapacity {
    /// The string the service accepts on the wire.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Total => "TOTAL",
            Self::Indexes => "INDEXES",
        }
    }
}

/// Whether a write reports the size of the item collection it touched.
///
/// The service reports collection metrics only for a table with at least one
/// local secondary index; the mock follows that rule, so
/// [`ItemCollectionMetrics`] stays absent on a table whose items cannot form a
/// collection the service would track.
///
/// ```
/// use mock_ddb::ReturnItemCollectionMetrics;
///
/// assert_eq!(
///     ReturnItemCollectionMetrics::default(),
///     ReturnItemCollectionMetrics::None
/// );
/// assert_eq!(ReturnItemCollectionMetrics::Size.as_str(), "SIZE");
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReturnItemCollectionMetrics {
    /// Report nothing (the default).
    #[default]
    None,
    /// Report the collection key and its size estimate range.
    Size,
}

impl ReturnItemCollectionMetrics {
    /// The string the service accepts on the wire.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Size => "SIZE",
        }
    }
}

/// The units one table or one index consumed, split by direction.
///
/// The service fills the field that applies and leaves the other absent: a read
/// has `read_capacity_units`, a write `write_capacity_units`. `capacity_units`
/// is always present and is the sum of the two.
///
/// ```
/// use mock_ddb::Capacity;
///
/// let capacity = Capacity {
///     read_capacity_units: Some(2.0),
///     write_capacity_units: None,
///     capacity_units: Some(2.0),
/// };
/// assert_eq!(capacity.read_capacity_units, Some(2.0));
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Capacity {
    /// Read units consumed, when the operation was a read.
    pub read_capacity_units: Option<f64>,
    /// Write units consumed, when the operation was a write.
    pub write_capacity_units: Option<f64>,
    /// Total units: the sum of the read and write units.
    pub capacity_units: Option<f64>,
}

/// The capacity one table consumed, with the per-index breakdown when asked.
///
/// ```
/// use mock_ddb::{item, s, KeySchema, KeyType, MockDdb, PutItemRequest, ReturnConsumedCapacity};
///
/// # #[tokio::main]
/// # async fn main() {
/// let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
/// let response = mock
///     .put_item(
///         PutItemRequest::new("users", item([("pk", s("u#1"))]))
///             .with_return_consumed_capacity(ReturnConsumedCapacity::Indexes),
///     )
///     .await
///     .unwrap();
/// let capacity = response.consumed_capacity.unwrap();
/// assert_eq!(capacity.capacity_units, Some(1.0));
/// assert_eq!(capacity.table.unwrap().capacity_units, Some(1.0));
/// assert!(capacity.global_secondary_indexes.is_empty());
/// # }
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConsumedCapacity {
    /// Name of the table the operation ran against.
    pub table_name: Option<String>,
    /// Total units consumed by the operation, indexes included.
    pub capacity_units: Option<f64>,
    /// Read units the operation consumed, absent when it consumed none.
    pub read_capacity_units: Option<f64>,
    /// Write units the operation consumed, absent when it consumed none.
    pub write_capacity_units: Option<f64>,
    /// The table's own share, reported with `INDEXES`.
    pub table: Option<Capacity>,
    /// Index name → its share, reported with `INDEXES`.
    pub local_secondary_indexes: BTreeMap<String, Capacity>,
    /// Index name → its share, reported with `INDEXES`.
    pub global_secondary_indexes: BTreeMap<String, Capacity>,
}

/// The item collection a write touched, and how big it is.
///
/// The collection is every item that shares the written item's partition key,
/// which the service tracks so that a local secondary index cannot grow past
/// 10 GB. `size_estimate_range_gb` is a range of whole gibibytes, exactly as
/// the service reports it.
///
/// ```
/// use mock_ddb::{
///     item, s, CreateTableRequest, KeySchema, KeyType, MockDdb, PutItemRequest,
///     ReturnItemCollectionMetrics, SecondaryIndex,
/// };
///
/// # #[tokio::main]
/// # async fn main() {
/// let mock = MockDdb::new();
/// mock.create_table(
///     CreateTableRequest::new(
///         "orders",
///         KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::String),
///     )
///     .with_local_index(SecondaryIndex::new(
///         "by-total",
///         KeySchema::hash_and_range("pk", KeyType::String, "total", KeyType::Number),
///     )),
/// )
/// .await
/// .unwrap();
///
/// let response = mock
///     .put_item(
///         PutItemRequest::new(
///             "orders",
///             item([("pk", s("customer#1")), ("sk", s("a")), ("total", mock_ddb::n("9"))]),
///         )
///         .with_return_item_collection_metrics(ReturnItemCollectionMetrics::Size),
///     )
///     .await
///     .unwrap();
/// let metrics = response.item_collection_metrics.unwrap();
/// assert_eq!(metrics.item_collection_key.unwrap()["pk"], s("customer#1"));
/// assert_eq!(metrics.size_estimate_range_gb, Some([0.0, 1.0]));
/// # }
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ItemCollectionMetrics {
    /// The partition key value every item of the collection shares.
    pub item_collection_key: Option<Item>,
    /// Lower bound and lower bound plus one gibibyte, in whole gibibytes.
    pub size_estimate_range_gb: Option<[f64; 2]>,
}

/// The units of one table or index, before they become a [`Capacity`].
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct Units {
    pub(crate) read: f64,
    pub(crate) write: f64,
}

impl Units {
    pub(crate) fn read(units: f64) -> Self {
        Self {
            read: units,
            write: 0.0,
        }
    }

    pub(crate) fn write(units: f64) -> Self {
        Self {
            read: 0.0,
            write: units,
        }
    }

    fn total(self) -> f64 {
        self.read + self.write
    }

    fn add(&mut self, other: Self) {
        self.read += other.read;
        self.write += other.write;
    }
}

impl From<Units> for Capacity {
    fn from(units: Units) -> Self {
        Self {
            read_capacity_units: (units.read > 0.0).then_some(units.read),
            write_capacity_units: (units.write > 0.0).then_some(units.write),
            capacity_units: Some(units.total()),
        }
    }
}

/// What one operation consumed, per table and per index, before the request's
/// `return_consumed_capacity` decides how much of it to report.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Breakdown {
    table: Units,
    local: BTreeMap<String, Units>,
    global: BTreeMap<String, Units>,
}

impl Breakdown {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn charge_table(&mut self, units: Units) {
        self.table.add(units);
    }

    pub(crate) fn charge_index(&mut self, scope: IndexScope, name: &str, units: Units) {
        let indexes = match scope {
            IndexScope::Global => &mut self.global,
            IndexScope::Local => &mut self.local,
        };
        indexes.entry(name.to_string()).or_default().add(units);
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        self.table.add(other.table);
        for (name, units) in &other.global {
            self.charge_index(IndexScope::Global, name, *units);
        }
        for (name, units) in &other.local {
            self.charge_index(IndexScope::Local, name, *units);
        }
    }

    /// Read units the whole operation consumed, indexes included.
    fn read(&self) -> f64 {
        self.table.read
            + self.global.values().map(|units| units.read).sum::<f64>()
            + self.local.values().map(|units| units.read).sum::<f64>()
    }

    /// Write units the whole operation consumed, indexes included.
    fn write(&self) -> f64 {
        self.table.write
            + self.global.values().map(|units| units.write).sum::<f64>()
            + self.local.values().map(|units| units.write).sum::<f64>()
    }

    /// The response shape `mode` asks for, if any.
    ///
    /// `TOTAL` carries the summed units only; `INDEXES` adds the table's own
    /// share and one entry per secondary index. The top-level read and write
    /// units are the operation's totals either way, so a caller that logs one
    /// number and one direction does not have to add up the breakdown.
    pub(crate) fn consume(
        &self,
        table_name: &str,
        mode: ReturnConsumedCapacity,
    ) -> Option<ConsumedCapacity> {
        if mode == ReturnConsumedCapacity::None {
            return None;
        }
        let read = self.read();
        let write = self.write();
        let mut consumed = ConsumedCapacity {
            table_name: Some(table_name.to_string()),
            capacity_units: Some(read + write),
            read_capacity_units: (read > 0.0).then_some(read),
            write_capacity_units: (write > 0.0).then_some(write),
            ..ConsumedCapacity::default()
        };
        if mode == ReturnConsumedCapacity::Indexes {
            consumed.table = Some(self.table.into());
            consumed.local_secondary_indexes = self
                .local
                .iter()
                .map(|(name, units)| (name.clone(), (*units).into()))
                .collect();
            consumed.global_secondary_indexes = self
                .global
                .iter()
                .map(|(name, units)| (name.clone(), (*units).into()))
                .collect();
        }
        Some(consumed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The crate's own item limit, so the unit ceiling can be checked against a
    /// size the mock can actually produce.
    const MAX_ITEM_BYTES: usize = 400 * 1024;

    /// Whether two computed unit counts are the same number.
    ///
    /// Every value here is a whole number, so exact equality is what the test
    /// means; the comparison is spelled out because `clippy::float_cmp` (rightly)
    /// rejects `==` on floats.
    fn same(left: f64, right: f64) -> bool {
        (left - right).abs() < f64::EPSILON
    }

    /// Whether two computed estimate ranges are the same two numbers.
    fn same_range(left: [f64; 2], right: [f64; 2]) -> bool {
        same(left[0], right[0]) && same(left[1], right[1])
    }

    #[test]
    fn read_and_write_units_round_up_to_the_whole_unit() {
        assert!(same(read_units(0), 1.0));
        assert!(same(read_units(1), 1.0));
        assert!(same(read_units(READ_UNIT_BYTES), 1.0));
        assert!(same(read_units(READ_UNIT_BYTES + 1), 2.0));
        assert!(same(read_units(2 * READ_UNIT_BYTES), 2.0));

        assert!(same(write_units(0), 1.0));
        assert!(same(write_units(1), 1.0));
        assert!(same(write_units(WRITE_UNIT_BYTES), 1.0));
        assert!(same(write_units(WRITE_UNIT_BYTES + 1), 2.0));
        assert!(same(write_units(MAX_ITEM_BYTES), 400.0));
    }

    #[test]
    fn size_estimate_range_steps_once_per_gibibyte() {
        assert!(same_range(size_estimate_range(0), [0.0, 1.0]));
        assert!(same_range(
            size_estimate_range(COLLECTION_ESTIMATE_STEP_BYTES - 1),
            [0.0, 1.0]
        ));
        assert!(same_range(
            size_estimate_range(COLLECTION_ESTIMATE_STEP_BYTES),
            [1.0, 2.0]
        ));
        assert!(same_range(
            size_estimate_range(3 * COLLECTION_ESTIMATE_STEP_BYTES + 7),
            [3.0, 4.0]
        ));
    }

    #[test]
    fn the_report_modes_name_themselves_the_way_the_service_does() {
        assert_eq!(ReturnConsumedCapacity::None.as_str(), "NONE");
        assert_eq!(ReturnConsumedCapacity::Total.as_str(), "TOTAL");
        assert_eq!(ReturnConsumedCapacity::Indexes.as_str(), "INDEXES");
        assert_eq!(
            ReturnConsumedCapacity::default(),
            ReturnConsumedCapacity::None
        );
        assert_eq!(ReturnItemCollectionMetrics::None.as_str(), "NONE");
        assert_eq!(ReturnItemCollectionMetrics::Size.as_str(), "SIZE");
        assert_eq!(
            ReturnItemCollectionMetrics::default(),
            ReturnItemCollectionMetrics::None
        );
    }

    #[test]
    fn a_breakdown_sums_table_and_indexes_per_mode() {
        let mut breakdown = Breakdown::new();
        breakdown.charge_table(Units::write(1.0));
        breakdown.charge_index(IndexScope::Global, "by-email", Units::write(1.0));
        breakdown.charge_index(IndexScope::Local, "by-total", Units::write(1.0));

        assert!(same(breakdown.read(), 0.0));
        assert!(same(breakdown.write(), 3.0));
        assert!(breakdown
            .consume("orders", ReturnConsumedCapacity::None)
            .is_none());

        let total = breakdown
            .consume("orders", ReturnConsumedCapacity::Total)
            .unwrap();
        assert_eq!(total.capacity_units, Some(3.0));
        assert_eq!(total.write_capacity_units, Some(3.0));
        assert!(total.table.is_none(), "TOTAL reports no breakdown");

        let indexes = breakdown
            .consume("orders", ReturnConsumedCapacity::Indexes)
            .unwrap();
        assert_eq!(indexes.capacity_units, Some(3.0));
        assert_eq!(
            indexes.global_secondary_indexes["by-email"].capacity_units,
            Some(1.0)
        );
        assert_eq!(
            indexes.local_secondary_indexes["by-total"].capacity_units,
            Some(1.0)
        );
        assert!(indexes.global_secondary_indexes["by-email"]
            .read_capacity_units
            .is_none());
    }

    #[test]
    fn merging_adds_the_same_index_twice() {
        let mut left = Breakdown::new();
        left.charge_table(Units::read(1.0));
        left.charge_index(IndexScope::Global, "g", Units::read(1.0));

        let mut right = Breakdown::new();
        right.charge_table(Units::read(2.0));
        right.charge_index(IndexScope::Global, "g", Units::read(3.0));

        left.merge(&right);
        let consumed = left.consume("t", ReturnConsumedCapacity::Indexes).unwrap();
        assert_eq!(consumed.capacity_units, Some(7.0));
        assert_eq!(consumed.table.unwrap().read_capacity_units, Some(3.0));
        assert_eq!(
            consumed.global_secondary_indexes["g"].capacity_units,
            Some(4.0)
        );
    }
}
