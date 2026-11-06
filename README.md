# mock-ddb

An in-process DynamoDB for Rust tests — tables, expression parsing, queries,
batches, transactions, TTL and streams, with no network, no Docker and no AWS
account.

`mock-ddb` is not a stub that returns canned responses. It parses condition and
update expressions, validates every key against the table's own key schema,
maintains secondary indexes on every write, evaluates conditions atomically and
keeps transactions all-or-nothing. Code that runs against
`aws_sdk_dynamodb::Client` can run against `MockDdb` in a test with the same
request shapes and the same value types.

```toml
[dev-dependencies]
mock-ddb = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
aws-sdk-dynamodb = "1"
```

## Quickstart

```rust
use mock_ddb::{
    item, n, s, values, CreateTableRequest, GetItemRequest, KeySchema, KeyType, MockDdb,
    PutItemRequest, QueryRequest, UpdateItemRequest,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mock = MockDdb::new();
    mock.create_table(CreateTableRequest::new(
        "orders",
        KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::Number),
    ))
    .await?;

    // Write, with the idempotency condition a create handler would use.
    mock.put_item(
        PutItemRequest::new("orders", item([
            ("pk", s("customer#1")),
            ("sk", n("1")),
            ("total", n("42.50")),
        ]))
        .with_condition("attribute_not_exists(pk)"),
    )
    .await?;

    // Update in place; the arithmetic is exact decimal, not floating point.
    mock.update_item(
        UpdateItemRequest::new(
            "orders",
            item([("pk", s("customer#1")), ("sk", n("1"))]),
            "SET total = total + :tip",
        )
        .with_values(values([(":tip", n("7.50"))])),
    )
    .await?;

    // Query a partition, key-ordered, with a filter and a projection.
    let page = mock
        .query(
            QueryRequest::new("orders", "pk = :pk")
                .with_values(values([(":pk", s("customer#1"))]))
                .with_projection("sk, total"),
        )
        .await?;
    assert_eq!(page.items[0]["total"], n("50"));

    // Point read.
    let single = mock
        .get_item(GetItemRequest::new(
            "orders",
            item([("pk", s("customer#1")), ("sk", n("1"))]),
        ))
        .await?;
    assert!(single.item.is_some());

    Ok(())
}
```

A test that only needs data can skip the async calls entirely:

```rust
use mock_ddb::{item, n, s, KeySchema, KeyType, MockDdb};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String))?;
    mock.seed("users", vec![item([("pk", s("u#1")), ("age", n("42"))])])?;
    assert_eq!(mock.count("users"), 1);
    Ok(())
}
```

## Drop-in shape

- **Values are the SDK's.** `mock_ddb::Item` is
  `HashMap<String, aws_sdk_dynamodb::types::AttributeValue>`, re-exported along
  with `Blob`. Nothing is converted at the boundary, which also means attribute
  order is the map's: an item's `Debug` rendering differs per process, so a
  snapshot assertion uses `ordered_item` (see [Time, determinism and
  concurrency](#time-determinism-and-concurrency)).
- **Requests mirror the SDK's fields**, down to the names
  (`condition_expression`, `expression_attribute_values`, `scan_index_forward`,
  `return_values_on_condition_check_failure`, …), so an assertion written
  against the client keeps working.
- **Responses are `#[non_exhaustive]`.** Every output's fields are public and
  readable, and one is built with `Default` plus field assignment rather than a
  struct literal — so a field the service adds to a response (the
  `consumed_capacity` / `item_collection_metrics` pair was the last one) is a
  minor release instead of a breaking change.
- **Calls take a request struct** rather than the SDK's fluent builders:
  `mock.put_item(PutItemRequest::new("users", item).with_condition(...))`
  instead of `client.put_item().table_name(...).send()`.
- **Everything is `async` and takes `&self`.** `MockDdb` is `Send + Sync +
  Clone`, so it can sit behind an `Arc<dyn Trait>` exactly where the client
  does, and one instance can be shared by any number of tasks.
- **Errors are typed**, with the service's error codes on
  [`Error::code`](https://docs.rs/mock-ddb/latest/mock_ddb/enum.Error.html):
  `ResourceNotFoundException`, `ValidationException`,
  `ConditionalCheckFailedException`, `ResourceInUseException`,
  `TransactionCanceledException`.

## What is covered

| Area | Operations |
| --- | --- |
| Tables | `create_table` (key types `S`/`N`/`B`, up to 20 GSIs, 5 LSIs, projections, streams), `delete_table`, `describe_table`, `list_tables` (with limit and cursor) |
| Items | `put_item`, `get_item`, `update_item` (upsert), `delete_item` |
| Return values | `NONE`, `ALL_OLD`, `UPDATED_OLD`, `ALL_NEW`, `UPDATED_NEW`, plus `ReturnValuesOnConditionCheckFailure` |
| Reads | `query` and `scan` with `FilterExpression`, `ProjectionExpression`, `Limit`, `ExclusiveStartKey`, `ScanIndexForward`, `Select` (`ALL_ATTRIBUTES`, `COUNT`, `SPECIFIC_ATTRIBUTES`), index reads |
| Parallel scan | `Segment` / `TotalSegments`, disjoint and complete |
| Batches | `batch_write_item` (25 writes, duplicate-key detection), `batch_get_item` (100 keys, duplicate-key detection per table, per-table projection); `throttle_next_batch_write` / `throttle_next_batch_get` arm one partial response so a caller's `UnprocessedItems`/`UnprocessedKeys` retry can be tested |
| Capacity | `ReturnConsumedCapacity` (`NONE`/`TOTAL`/`INDEXES`) and `ReturnItemCollectionMetrics` (`SIZE`) on every data-plane operation, with the units computed from the stored items and split per table and per index |
| Transactions | `transact_write_items` (100 actions: `Put`, `Update`, `Delete`, `ConditionCheck`; atomic; per-member cancellation reasons; `ClientRequestToken` idempotency), `transact_get_items` |
| TTL | `update_time_to_live`, `describe_time_to_live`, lazy expiry on every read path, `purge_expired` |
| Streams | one ordered log per table, bounded at 10 000 records (oldest trimmed), `get_records`, `list_streams`, `NEW_IMAGE` / `OLD_IMAGE` / `NEW_AND_OLD_IMAGES` / `KEYS_ONLY` (feature `streams`, on by default) |
| Streams API | `MockDdb::streams()` returns the wire API: `list_streams` (with `ExclusiveStartStreamArn` and limit), `describe_stream` (label, view type, shard, sequence range), `get_shard_iterator` (`TRIM_HORIZON`, `LATEST`, `AT_SEQUENCE_NUMBER`, `AFTER_SEQUENCE_NUMBER`) and `get_records` (limit 1–1000, `NextShardIterator`), with records in the Kinesis adapter's shape |
| PartiQL | `execute_statement` (`SELECT`, `INSERT`, `UPDATE`, `DELETE`) and `batch_execute_statement` (1–25 statements, all reads or all writes), lowered onto the native calls so capacity, TTL, streams and index maintenance stay in one place. `PartiqlStatement` types a statement (`kind`, `table_name`, `parameters`) before the wire sees it |
| Test helpers | `seed`, `dump`, `count`, `clear`, `clear_records`, `set_clock`, `purge_expired`, `item_to_value` for embedding an item as `AttributeValue::M`, and `item_ref_from_value` (borrowing), `into_item` (consuming) or `item_from_value` (cloning) to read it back |

### Expressions

| Construct | Supported |
| --- | --- |
| Comparisons | `=`, `<>`, `<`, `<=`, `>`, `>=` on `S`, `N`, `B` (`=` also on sets, lists, maps, booleans, null) |
| Range and set tests | `BETWEEN … AND …`, `IN (…)` |
| Functions | `attribute_exists`, `attribute_not_exists`, `attribute_type`, `begins_with`, `contains`, `size` |
| Connectives | `AND`, `OR`, `NOT`, parentheses |
| Paths | nested maps (`profile.city`), list indices (`items[0]`), `#aliased` and `#aliased.path` |
| Update clauses | `SET` (with `+`, `-`, `if_not_exists`, `list_append` — including nested calls and `list_append(if_not_exists(list, :empty), :v)`), `REMOVE`, `ADD` (numbers and sets), `DELETE` (sets) |
| Update safety | key attributes cannot be rewritten; overlapping paths, repeated clauses and malformed syntax are rejected before any write |
| Expression limits | the service's 4096-byte limit per expression string, checked before tokenization, and its 300-operator/function limit checked while parsing; SET assignment `=` is syntax, not an operator |
| Projections | comma-separated paths with aliases; a list index keeps the named elements, in ascending order |

Condition and update expressions can also be parsed and evaluated directly,
without executing an operation — useful for asserting on the expression a
production code path builds:

```rust
use std::collections::HashMap;
use mock_ddb::{n, s, ConditionExpression, Item, KeySchema, KeyType, UpdateExpression};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let condition =
        ConditionExpression::parse("attribute_exists(pk) AND #n BETWEEN :low AND :high")?;
    let mut item = Item::new();
    item.insert("pk".into(), s("u#1"));
    item.insert("n".into(), n("5"));

    let names = HashMap::from([("#n".to_string(), "n".to_string())]);
    let values = HashMap::from([(":low".to_string(), n("1")), (":high".to_string(), n("10"))]);
    assert!(condition.evaluate(Some(&item), &names, &values)?);

    let update = UpdateExpression::parse("SET hits = if_not_exists(hits, :zero) + :one")?;
    let values = HashMap::from([(":zero".to_string(), n("0")), (":one".to_string(), n("1"))]);
    let schema = KeySchema::hash("pk", KeyType::String);
    update.apply(&mut item, &schema, &names, &values)?;
    assert_eq!(item.get("hits"), Some(&n("1")));
    Ok(())
}
```

## Deliberate differences from the service

These are choices, not omissions. Each one keeps a test deterministic; anything
that changes what a caller must do is called out.

1. **No table lifecycle.** `create_table` returns with `TableStatus = ACTIVE`;
   there is no `CREATING`/`DELETING` window to poll.
2. **Capacity is reported, not enforced.** `ConsumedCapacity` is computed from
   the item sizes the mock actually stores — one read unit per started 4 KiB of
   an item read, one write unit per started 1 KiB of an item written, items the
   filter rejected included, and a minimum of one unit per operation — and split
   per table and per secondary index for `ReturnConsumedCapacity=INDEXES`.
   `ItemCollectionMetrics` reports the collection a write touched, keyed by its
   partition key and sized in whole gibibytes, for a table with a local
   secondary index. Provisioned throughput, throttling,
   `ProvisionedThroughputExceededException` and the 10 GB collection limit stay
   unmodelled: `BillingMode` is reported back, never enforced.
3. **Always strongly consistent.** `ConsistentRead` is accepted and ignored.
   Global-secondary-index reads are immediately consistent too; the service's
   eventual consistency is not simulated.
4. **Item size is enforced; collection size is not.** An item over 400 KiB is a
   `ValidationException` (the mock charges the same bytes the service does).
   The 10 GB local-index collection limit is not modelled, although the error
   variant exists for callers that match on error classes.
5. **A wrongly typed operand of `begins_with`/`contains`/`size` is rejected.** A
   number where a string prefix belongs, a string prefix on a binary, a set
   needle of a type the set cannot hold, `size()` of a number: the service
   raises `ValidationException`
   ("Incorrect operand type for operator or function") and so does the mock.
   Cross-type comparisons that the service answers with false — `count = :s`,
   `size(NULL) > :n`, an expression on a missing attribute — still evaluate to
   false. Conditions are also rejected for malformed syntax, unknown aliases
   and undefined values.
6. **TTL deletes lazily.** An expired item is invisible to `get_item`, `query`,
   `scan`, `dump` and `count` the moment its timestamp passes, but its row is
   only removed when it is read or `purge_expired` runs. The service deletes
   within 48 hours in the background.
7. **Streams are one open shard with a bounded log.** A table's log is readable
   through the in-process `get_records`/`list_streams` helpers or through
   `MockDdb::streams()`, which speaks `ListStreams`, `DescribeStream`,
   `GetShardIterator` and `GetRecords` with their real shapes, limits and
   `NextShardIterator` chaining. The service ages records out after 24 hours;
   the mock has no clock for its log, so it trims by count instead: a stream
   keeps its most recent `MAX_RETAINED_STREAM_RECORDS` (10 000) records and an
   append past that drops the oldest, which keeps a test or a property run from
   growing the mock with its whole write history. The in-process `get_records`
   returns a point-in-time snapshot that shares the retained records instead of
   deep-cloning their item images. `clear_records` empties a log outright. A
   shard iterator's position counts every record the stream ever appended, so a
   token taken before a trim resumes at the oldest record still retained rather
   than at one the trim removed. There are no shard splits, so `ParentShardId`
   is always absent and `DescribeStream` returns exactly one shard.
8. **Transaction conflicts do not happen.** Nothing else writes concurrently
   inside the mock, so a transaction either commits or is cancelled by a failed
   condition. `ClientRequestToken` follows the service: the transaction that
   spends a token is remembered for ten minutes on the mock's clock, a replay of
   the same actions writes nothing, and a non-identical request that reuses the
   token fails with `IdempotentParameterMismatchException`. Two differences
   remain: a replay returns an empty response instead of the original
   `ConsumedCapacity`/`ItemCollectionMetrics`, and only a transaction that
   applied spends its token, so a retry of a cancelled one re-evaluates the
   conditions.
9. **Batch writes are all-or-nothing unless a test asks otherwise.** Up to 25
    writes are validated first and then applied; `UnprocessedItems` and
    `UnprocessedKeys` are empty by default, where the service can return
    unprocessed work under load. A test that has to exercise the caller's
    partial-batch retry arms one partial response with
    `throttle_next_batch_write` / `throttle_next_batch_get`: the next batch call
    leaves a prefix of the writes or keys unprocessed (no capacity charged, the
    rest applied or read) and every later call runs unthrottled again.
10. **Update expressions read the pre-update image.** All right-hand sides are
    evaluated against the item as it was before the first clause, so
    `SET a = b, b = a` swaps. This matches the service's single atomic update
    and makes an update's result independent of clause order.
11. **Parallel scan segments are hash-based.** Items are assigned to a segment
    by an FNV-1a digest of the primary key mixed through SplitMix64's finalizer,
    so segments are disjoint, spread across workers and stable across runs; the
    service's split is internal.
12. **Numbers are exact decimals.** 38 significant digits and the documented
    exponent range (`1E-130` … `9.99E+125`) are enforced, `"1.0"` and `"1"` are
    the same value (and the same key), and arithmetic is exact — no `f64`
    rounding. A number outside the range is a validation error, never an
    oversized allocation.
13. **Reserved-word quoting uses placeholders.** DynamoDB's reserved-word list
    is not modelled: a bare attribute name is accepted whenever it is not a
    keyword of the expression grammar itself (`AND`, `BETWEEN`, `SET`, …). Use
    `#alias` for those.
14. **Table and index names are validated** against the documented pattern and
    length (3–255 characters, `[a-zA-Z0-9_.-]+`), so a test cannot pass with a
    name the service would reject.
15. **A `SELECT` whose `WHERE` does not pin the partition key scans.** The
    service raises `ValidationException` for such a statement; the mock reads
    the table with a `Scan` instead, which is what a test that wants to inspect
    a whole table actually means. A `WHERE` that does pin it becomes a `Query`,
    so ordering, `Limit` and `NextToken` behave like the native call.
16. **PartiQL covers DynamoDB's dialect, not full PartiQL.** `RETURNING`,
    `ORDER BY`, aggregates, joins and multi-statement strings are absent from
    the service's dialect too and are rejected with the service's
    `ValidationException`. An `INSERT` on an existing primary key is a
    `DuplicateItemException` (the service's class: `INSERT` never overwrites),
    and every write has to name the whole primary key: a `WHERE` that matches no
    row changes nothing and reports `ConditionalCheckFailedException`, and an
    `UPDATE` without a `WHERE` is a `ValidationException` rather than an
    implicit insert. `IS NULL` asks whether the attribute holds DynamoDB's NULL
    value and `IS MISSING` whether it is absent — an absent attribute is neither
    `NULL` nor `NOT NULL`.

## Not modelled at all

- DAX, global tables, backups, point-in-time recovery, tags, resource policies.
- `ConditionExpression` on `BatchWriteItem` (the service does not have one
  either) and `ReturnValues` on `BatchGetItem`.

If you need one of these for a test, the mock is the wrong tool for that
assertion — integration tests against DynamoDB Local or a real account still
have their place.

## Why not an existing crate

A crates.io lookup on 2026-11-05 (`in-process DynamoDB mock for tests`, ranked
by adoption) returned `skyzen-services`, `rusty-gasket` and
`winterbaume-partiql` — none of them an in-process DynamoDB double — so no
maintained crate does this job and the implementation here is local on purpose.
The doubles that do exist are services, not libraries: DynamoDB Local is a Java
process (or a container), LocalStack a Python container and dynalite a Node.js
server, so each brings a runtime, a port and start/stop sequencing into a test
suite that should need none of them, while `aws_sdk_dynamodb` needs a real
endpoint and real credentials. A `HashMap` in the test's own address space has
neither.

## Time, determinism and concurrency

TTL expiry reads the clock through `mock_ddb::Clock`. Pin it to make expiry
tests instant:

```rust
use std::sync::Arc;
use std::time::Duration;
use mock_ddb::{MockClock, MockDdb};

fn main() {
    let clock = Arc::new(MockClock::at_epoch_seconds(1_700_000_000));
    let mock = MockDdb::with_clock(clock.clone());
    clock.advance(Duration::from_secs(3_600));
    assert_eq!(mock.now_seconds(), 1_700_003_600);
}
```

Writes hold a single lock for their duration, which is what makes transactions
atomic and batch writes all-or-nothing. Reads take a shared lock, so many tasks
can read concurrently while a write waits. The injected `Clock` is read *before*
that lock is taken, so a clock that blocks on I/O — or that calls back into the
mock — never stretches a critical section or deadlocks another task. There is no
`unsafe`, no ambient state, and no lock is ever held across an `.await`.

Attribute order is not stable, and no assertion needs it to be: `Item` is the
SDK's `HashMap`, so the `Debug` form of an item follows the map's per-process
seed, and DynamoDB does not define an order either. `assert_eq!` on two items is
unaffected — map equality compares entries, not order. A test that *renders* an
item, into a golden file or a log line, uses `ordered_item`, which lists the
attributes by name and borrows the values:

```rust
use mock_ddb::{item, n, ordered_item, s};

let read = item([("age", n("42")), ("pk", s("u#1"))]);
assert_eq!(
    ordered_item(&read).keys().copied().collect::<Vec<_>>(),
    ["age", "pk"]
);
```

Seconds are signed: an instant before the Unix epoch reports its real negative
second (`-3_600` an hour early), and a fraction of a second on either side of
the epoch reports `0`, so a TTL deadline is never parked on a value that only
looks like a timestamp.

## Testing this crate

```bash
cargo test --all-features          # unit, integration, property and doctests
cargo test --no-default-features   # the streams-free surface
cargo clippy --all-targets --all-features -- -D warnings
cargo llvm-cov --all-features --summary-only --fail-under-lines 95
cargo +nightly fuzz build
cargo +nightly fuzz run parse_expression -- -max_total_time=60
cargo +nightly fuzz run random_items -- -max_total_time=60
```

- 498 tests (761 with doctests) in 16 integration suites, unit tests per module
  (`surface`, `expressions`, `query_scan`, `batch_transact`, `ttl_streams`,
  `streams_api`, `stream_retention`, `edge_cases`, `concurrency`, `coverage`),
  property tests with `proptest`, and a doctest on every public type, every
  operation and every error method. Builder setters are exercised by their
  request type's example.
- Line coverage is 97 %+ with all features on (`cargo llvm-cov --all-features`).
- Two fuzz targets: `parse_expression` (the condition/update/projection
  parsers and evaluators) and `random_items` (arbitrary attribute trees through
  every expression path). Both ran for 60 seconds each with hundreds of
  thousands of executions and no crash before release.

## Feature flags

| Feature | Default | Effect |
| --- | --- | --- |
| `streams` | on | DynamoDB Streams emulation: stream ARNs on create, records on every mutation, `get_records`, `list_streams`, and the wire API behind `MockDdb::streams()` (`ListStreams`, `DescribeStream`, `GetShardIterator`, `GetRecords`). With the feature off, `create_table` rejects a `StreamSpecification` instead of silently ignoring it. |

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your
option.
