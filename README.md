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
  with `Blob`. Nothing is converted at the boundary.
- **Requests mirror the SDK's fields**, down to the names
  (`condition_expression`, `expression_attribute_values`, `scan_index_forward`,
  `return_values_on_condition_check_failure`, …), so an assertion written
  against the client keeps working.
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
| Batches | `batch_write_item` (25 writes, duplicate-key detection), `batch_get_item` (100 keys, per-table projection) |
| Transactions | `transact_write_items` (100 actions: `Put`, `Update`, `Delete`, `ConditionCheck`; atomic; per-member cancellation reasons; `ClientRequestToken` idempotency), `transact_get_items` |
| TTL | `update_time_to_live`, `describe_time_to_live`, lazy expiry on every read path, `purge_expired` |
| Streams | one ordered log per table, `get_records`, `list_streams`, `NEW_IMAGE` / `OLD_IMAGE` / `NEW_AND_OLD_IMAGES` / `KEYS_ONLY` (feature `streams`, on by default) |
| Test helpers | `seed`, `dump`, `count`, `clear`, `set_clock`, `purge_expired` |

### Expressions

| Construct | Supported |
| --- | --- |
| Comparisons | `=`, `<>`, `<`, `<=`, `>`, `>=` on `S`, `N`, `B` (`=` also on sets, lists, maps, booleans, null) |
| Range and set tests | `BETWEEN … AND …`, `IN (…)` |
| Functions | `attribute_exists`, `attribute_not_exists`, `attribute_type`, `begins_with`, `contains`, `size` |
| Connectives | `AND`, `OR`, `NOT`, parentheses |
| Paths | nested maps (`profile.city`), list indices (`items[0]`), `#aliased` and `#aliased.path` |
| Update clauses | `SET` (with `+`, `-`, `if_not_exists`, `list_append`), `REMOVE`, `ADD` (numbers and sets), `DELETE` (sets) |
| Update safety | key attributes cannot be rewritten; overlapping paths, repeated clauses and malformed syntax are rejected before any write |
| Projections | comma-separated paths with aliases; projecting a list index returns the containing list |

Condition and update expressions can also be parsed and evaluated directly,
without executing an operation — useful for asserting on the expression a
production code path builds:

```rust
use std::collections::HashMap;
use mock_ddb::{n, s, ConditionExpression, Item, UpdateExpression};

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
    update.apply(&mut item, &names, &values)?;
    assert_eq!(item.get("hits"), Some(&n("1")));
    Ok(())
}
```

## Deliberate differences from the service

These are choices, not omissions. Each one keeps a test deterministic; anything
that changes what a caller must do is called out.

1. **No table lifecycle.** `create_table` returns with `TableStatus = ACTIVE`;
   there is no `CREATING`/`DELETING` window to poll.
2. **No capacity.** `ConsumedCapacity`, provisioned throughput, throttling and
   `ProvisionedThroughputExceededException` are not modelled. `BillingMode` is
   reported back, never enforced.
3. **Always strongly consistent.** `ConsistentRead` is accepted and ignored.
   Global-secondary-index reads are immediately consistent too; the service's
   eventual consistency is not simulated.
4. **Item size is enforced; collection size is not.** An item over 400 KiB is a
   `ValidationException` (the mock charges the same bytes the service does).
   The 10 GB local-index collection limit is not modelled, although the error
   variant exists for callers that match on error classes.
5. **`Query` without a key condition is allowed.** The service requires one.
   The mock returns every item in key order, which is what tests that want to
   inspect a whole table actually mean.
6. **Type errors inside comparisons are "false", not errors.** `begins_with`
   on a number, or `size(NULL) > :n`, evaluates to false; the service raises
   `ValidationException`. Conditions are still rejected for malformed syntax,
   unknown aliases and undefined values.
7. **TTL deletes lazily.** An expired item is invisible to `get_item`, `query`,
   `scan`, `dump` and `count` the moment its timestamp passes, but its row is
   only removed when it is read or `purge_expired` runs. The service deletes
   within 48 hours in the background.
8. **Streams are a log, not a shard API.** One shard per table, read through
   `MockDdb::get_records`; there are no shard iterators, no 24-hour retention
   and no trimming. `get_records` returns records from the start of the log
   every time.
9. **Transaction conflicts do not happen.** Nothing else writes concurrently
   inside the mock, so a transaction either commits or is cancelled by a failed
   condition. `ClientRequestToken` deduplicates in-process for the life of the
   mock instead of the service's 10-minute window.
10. **Batch writes are never partial.** Up to 25 writes are validated first and
    then applied; `UnprocessedItems` and `UnprocessedKeys` are always empty. The
    service can return unprocessed work under load.
11. **Update expressions read the pre-update image.** All right-hand sides are
    evaluated against the item as it was before the first clause, so
    `SET a = b, b = a` swaps. This matches the service's single atomic update
    and makes an update's result independent of clause order.
12. **Parallel scan segments are hash-based.** Items are assigned to a segment
    by an FNV-1a hash of the primary key, so segments are disjoint and stable
    across runs; the service's split is internal.
13. **Numbers are exact decimals.** 38 significant digits are enforced, `"1.0"`
    and `"1"` are the same value (and the same key), and arithmetic is exact —
    no `f64` rounding.
14. **Reserved-word quoting uses placeholders.** DynamoDB's reserved-word list
    is not modelled: a bare attribute name is accepted whenever it is not a
    keyword of the expression grammar itself (`AND`, `BETWEEN`, `SET`, …). Use
    `#alias` for those.
15. **Table names are validated** against the documented pattern and length
    (3–255 characters, `[a-zA-Z0-9_.-]+`), so a test cannot pass with a name the
    service would reject.

## Not modelled at all

- PartiQL / `ExecuteStatement`.
- DynamoDB Streams' Kinesis adapter, shard iterators and `GetRecords` wire API.
- DAX, global tables, backups, point-in-time recovery, tags, resource policies.
- `ConditionExpression` on `BatchWriteItem` (the service does not have one
  either) and `ReturnValues` on `BatchGetItem`.
- `ConsumedCapacity` and `ItemCollectionMetrics` in every response.
- List-element projection: `items[0]` projects the whole list.

If you need one of these for a test, the mock is the wrong tool for that
assertion — integration tests against DynamoDB Local or a real account still
have their place.

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
can read concurrently while a write waits. There is no `unsafe`, no ambient
state, and no lock is ever held across an `.await`.

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

- 230 tests (184 test functions plus 47 doctests): unit tests per module, an integration suite per area
  (`surface`, `expressions`, `query_scan`, `batch_transact`, `ttl_streams`,
  `edge_cases`, `concurrency`, `coverage`), property tests with `proptest`, and
  a doctest on every public item.
- Line coverage is 97 %+ with all features on (`cargo llvm-cov --all-features`).
- Two fuzz targets: `parse_expression` (the condition/update/projection
  parsers and evaluators) and `random_items` (arbitrary attribute trees through
  every expression path). Both ran for 60 seconds each with hundreds of
  thousands of executions and no crash before release.

## Feature flags

| Feature | Default | Effect |
| --- | --- | --- |
| `streams` | on | DynamoDB Streams emulation: stream ARNs on create, records on every mutation, `get_records`, `list_streams`. With the feature off, `create_table` rejects a `StreamSpecification` instead of silently ignoring it. |

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your
option.
