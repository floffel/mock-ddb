//! Surface completeness: every builder, every enum rendering, every error
//! branch reachable through the public API.
//!
//! These tests are deliberately mechanical. Their job is to make a public method
//! that no other test calls fail the coverage gate, so an untested builder
//! cannot hide behind a green suite.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mock_ddb::{
    item, n, names, s, values, AttributeValue, BatchGetItemRequest, BatchWriteItemRequest,
    BillingMode, Clock, CreateTableRequest, DeleteItemRequest, GetItemRequest, KeySchema, KeyType,
    KeysAndAttributes, ListTablesRequest, MockClock, MockDdb, Projection, PutItemRequest,
    QueryRequest, ReturnValues, ScanRequest, SecondaryIndex, Select, StreamViewType, SystemClock,
    TimeToLiveStatus, TransactGet, TransactGetItemsRequest, TransactWriteItem,
    TransactWriteItemsRequest, UpdateItemRequest, UpdateTimeToLiveRequest, WriteRequest,
};

fn users() -> MockDdb {
    MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap()
}

#[test]
fn enum_renderings_and_defaults() {
    assert_eq!(KeyType::String.as_str(), "S");
    assert_eq!(KeyType::Number.as_str(), "N");
    assert_eq!(KeyType::Binary.as_str(), "B");
    assert_eq!(KeyType::String.to_string(), "S");

    assert_eq!(BillingMode::Provisioned.as_str(), "PROVISIONED");
    assert_eq!(BillingMode::PayPerRequest.as_str(), "PAY_PER_REQUEST");
    assert_eq!(BillingMode::default(), BillingMode::PayPerRequest);

    assert_eq!(StreamViewType::NewImage.as_str(), "NEW_IMAGE");
    assert_eq!(StreamViewType::default(), StreamViewType::NewAndOldImages);

    assert_eq!(TimeToLiveStatus::Enabled.as_str(), "ENABLED");
    assert_eq!(TimeToLiveStatus::Disabled.as_str(), "DISABLED");

    assert_eq!(ReturnValues::default(), ReturnValues::None);
    assert_eq!(Select::default(), Select::AllAttributes);
    assert_eq!(Projection::default(), Projection::All);

    assert_eq!(KeySchema::default(), KeySchema::hash("pk", KeyType::String));
    assert_eq!(MockDdb::default().count("nothing"), 0);
}

#[test]
fn request_builders_set_every_field() {
    let put = PutItemRequest::new("users", item([("pk", s("a"))]))
        .with_condition("attribute_not_exists(pk)")
        .with_names(names([("#a", "a")]))
        .with_values(values([(":a", s("a"))]))
        .with_return_values(ReturnValues::AllOld)
        .with_return_values_on_condition_check_failure();
    assert!(put.condition_expression.is_some());
    assert!(put.expression_attribute_names.is_some());
    assert!(put.expression_attribute_values.is_some());
    assert_eq!(put.return_values, ReturnValues::AllOld);
    assert!(put.return_values_on_condition_check_failure);

    let get = GetItemRequest::new("users", item([("pk", s("a"))]))
        .with_projection("pk")
        .with_names(names([("#pk", "pk")]));
    assert!(get.projection_expression.is_some());
    assert!(get.expression_attribute_names.is_some());

    let delete = DeleteItemRequest::new("users", item([("pk", s("a"))]))
        .with_condition("attribute_exists(pk)")
        .with_names(names([("#pk", "pk")]))
        .with_values(values([(":pk", s("a"))]))
        .with_return_values(ReturnValues::AllOld)
        .with_return_values_on_condition_check_failure();
    assert!(delete.condition_expression.is_some());
    assert!(delete.expression_attribute_names.is_some());
    assert!(delete.expression_attribute_values.is_some());
    assert!(delete.return_values_on_condition_check_failure);

    let update = UpdateItemRequest::new("users", item([("pk", s("a"))]), "SET v = :v")
        .with_condition("attribute_exists(pk)")
        .with_names(names([("#v", "v")]))
        .with_values(values([(":v", s("v"))]))
        .with_return_values(ReturnValues::UpdatedNew)
        .with_return_values_on_condition_check_failure();
    assert!(update.condition_expression.is_some());
    assert!(update.expression_attribute_names.is_some());
    assert!(update.expression_attribute_values.is_some());
    assert!(update.return_values_on_condition_check_failure);

    let query = QueryRequest::new("users", "pk = :pk")
        .with_index("by-state")
        .with_filter("attribute_exists(pk)")
        .with_scan_index_forward(false)
        .with_limit(5)
        .with_exclusive_start_key(item([("pk", s("a"))]))
        .with_projection("pk")
        .with_names(names([("#pk", "pk")]))
        .with_values(values([(":pk", s("a"))]))
        .with_select(Select::AllAttributes);
    assert_eq!(query.index_name.as_deref(), Some("by-state"));
    assert_eq!(query.scan_index_forward, Some(false));
    assert_eq!(query.limit, Some(5));
    assert!(query.exclusive_start_key.is_some());
    assert!(query.projection_expression.is_some());
    assert!(query.expression_attribute_names.is_some());
    assert!(query.expression_attribute_values.is_some());

    let scan = ScanRequest::new("users")
        .with_filter("attribute_exists(pk)")
        .with_limit(3)
        .with_exclusive_start_key(item([("pk", s("a"))]))
        .with_segment(0, 2)
        .with_index("by-state")
        .with_projection("pk")
        .with_names(names([("#pk", "pk")]))
        .with_values(values([(":pk", s("a"))]))
        .with_select(Select::Count);
    assert_eq!(scan.segment, Some(0));
    assert_eq!(scan.total_segments, Some(2));
    assert_eq!(scan.index_name.as_deref(), Some("by-state"));
    assert_eq!(scan.limit, Some(3));
    assert!(scan.projection_expression.is_some());

    let _ = (&query, &scan);
}

#[test]
fn batch_and_transaction_builders_set_every_field() {
    let batch_get = BatchGetItemRequest::new().add(
        "users",
        KeysAndAttributes::new(vec![item([("pk", s("a"))])]).with_projection("pk"),
    );
    assert_eq!(batch_get.request_items["users"].keys.len(), 1);
    assert!(batch_get.request_items["users"]
        .projection_expression
        .is_some());

    let batch_write = BatchWriteItemRequest::new().add(
        "users",
        WriteRequest::Put {
            item: item([("pk", s("a"))]),
        },
    );
    assert!(batch_write.request_items.contains_key("users"));

    let transaction = TransactWriteItemsRequest::new(vec![
        TransactWriteItem::put("users", item([("pk", s("a"))])),
        TransactWriteItem::update("users", item([("pk", s("b"))]), "SET v = :v")
            .with_names(names([("#v", "v")]))
            .with_values(values([(":v", s("v"))]))
            .with_condition("attribute_exists(pk)"),
        TransactWriteItem::delete("users", item([("pk", s("c"))])),
        TransactWriteItem::condition_check("users", item([("pk", s("d"))]), "attribute_exists(pk)")
            .with_names(names([("#pk", "pk")]))
            .with_values(values([(":pk", s("d"))]))
            .with_condition("attribute_exists(pk)"),
    ])
    .with_client_request_token("token");
    assert_eq!(transaction.transact_items.len(), 4);
    assert_eq!(
        transaction
            .transact_items
            .iter()
            .map(TransactWriteItem::table_name)
            .collect::<Vec<_>>(),
        vec!["users", "users", "users", "users"]
    );
    assert_eq!(transaction.client_request_token.as_deref(), Some("token"));

    let transact_get =
        TransactGetItemsRequest::new(vec![TransactGet::new("users", item([("pk", s("a"))]))]);
    assert_eq!(transact_get.transact_items.len(), 1);

    let ttl = UpdateTimeToLiveRequest::new("users", "expires_at", true);
    assert!(ttl.enabled);

    let list = ListTablesRequest {
        exclusive_start_table_name: Some("a".to_string()),
        limit: Some(2),
    };
    assert_eq!(list.limit, Some(2));
}

#[tokio::test]
async fn every_transaction_action_variant_applies() {
    let mock = users();
    mock.seed(
        "users",
        vec![
            item([("pk", s("u#1")), ("v", n("1"))]),
            item([("pk", s("u#2"))]),
        ],
    )
    .unwrap();

    mock.transact_write_items(TransactWriteItemsRequest::new(vec![
        TransactWriteItem::update("users", item([("pk", s("u#1"))]), "SET v = v + :one")
            .with_values(values([(":one", n("1"))]))
            .with_condition("attribute_exists(pk)"),
        TransactWriteItem::delete("users", item([("pk", s("u#2"))]))
            .with_condition("attribute_exists(pk)"),
        TransactWriteItem::condition_check(
            "users",
            item([("pk", s("u#4"))]),
            "attribute_not_exists(pk)",
        )
        .with_values(values([(":t", s("N"))])),
        TransactWriteItem::put("users", item([("pk", s("u#3"))]))
            .with_condition("attribute_not_exists(pk)"),
    ]))
    .await
    .unwrap();

    assert_eq!(mock.count("users"), 2);
}

#[test]
fn clock_controls() {
    let clock = MockClock::new(UNIX_EPOCH);
    assert_eq!(clock.now(), UNIX_EPOCH);
    clock.set(UNIX_EPOCH + Duration::from_secs(10));
    assert_eq!(clock.epoch_seconds(), 10);
    clock.advance(Duration::from_secs(5));
    assert_eq!(clock.epoch_seconds(), 15);

    let pre_epoch = MockClock::new(UNIX_EPOCH - Duration::from_secs(10));
    assert_eq!(pre_epoch.epoch_seconds(), -1);

    assert!(SystemClock.now() > UNIX_EPOCH - Duration::from_secs(1));

    let mock = MockDdb::with_clock(Arc::new(MockClock::at_epoch_seconds(1_000)));
    assert_eq!(mock.now_seconds(), 1_000);
    mock.set_clock(Arc::new(MockClock::at_epoch_seconds(2_000)));
    assert_eq!(mock.now_seconds(), 2_000);
}

#[tokio::test]
async fn explicit_attribute_definitions_are_accepted() {
    let description = MockDdb::new()
        .create_table(CreateTableRequest {
            table_name: "orders".to_string(),
            key_schema: KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::Number),
            attribute_definitions: vec![
                mock_ddb::AttributeDefinition::new("pk", KeyType::String),
                mock_ddb::AttributeDefinition::new("sk", KeyType::Number),
            ],
            billing_mode: BillingMode::Provisioned,
            ..CreateTableRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(description.billing_mode, BillingMode::Provisioned);
    assert_eq!(description.attribute_definitions.len(), 2);
}

#[tokio::test]
async fn definition_errors_from_the_public_api() {
    // More than 20 global secondary indexes.
    let mut many = CreateTableRequest::new("orders", KeySchema::hash("pk", KeyType::String));
    for position in 0..21 {
        many = many.with_global_index(SecondaryIndex::new(
            format!("gsi-{position}"),
            KeySchema::hash(format!("g{position}"), KeyType::String),
        ));
    }
    assert!(MockDdb::new().create_table(many).await.is_err());

    // The same attribute is key of two indexes with different types.
    let mismatched = CreateTableRequest::new("orders", KeySchema::hash("pk", KeyType::String))
        .with_global_index(SecondaryIndex::new(
            "by-a",
            KeySchema::hash("shared", KeyType::String),
        ))
        .with_global_index(SecondaryIndex::new(
            "by-b",
            KeySchema::hash("shared", KeyType::Number),
        ));
    assert!(MockDdb::new().create_table(mismatched).await.is_err());

    // An index with an empty name.
    let unnamed = CreateTableRequest::new("orders", KeySchema::hash("pk", KeyType::String))
        .with_global_index(SecondaryIndex::new(
            "",
            KeySchema::hash("a", KeyType::String),
        ));
    assert!(MockDdb::new().create_table(unnamed).await.is_err());

    // The same attribute defined twice.
    let duplicated = CreateTableRequest {
        table_name: "orders".to_string(),
        key_schema: KeySchema::hash("pk", KeyType::String),
        attribute_definitions: vec![
            mock_ddb::AttributeDefinition::new("pk", KeyType::String),
            mock_ddb::AttributeDefinition::new("pk", KeyType::String),
        ],
        ..CreateTableRequest::default()
    };
    assert!(MockDdb::new().create_table(duplicated).await.is_err());
}

async fn indexed() -> MockDdb {
    let request = CreateTableRequest::new(
        "events",
        KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::Number),
    )
    .with_global_index(
        SecondaryIndex::new(
            "by-state",
            KeySchema::hash_and_range("state", KeyType::String, "at", KeyType::Number),
        )
        .with_projection(Projection::Include(vec!["payload".to_string()])),
    );
    let mock = MockDdb::new();
    mock.create_table(request).await.unwrap();
    mock
}

#[tokio::test]
async fn index_projection_include_and_cursors() {
    let mock = indexed().await;
    for at in 1..=5 {
        mock.put_item(PutItemRequest::new(
            "events",
            item([
                ("pk", s(format!("e{at}"))),
                ("sk", n(at.to_string())),
                ("state", s("open")),
                ("at", n(at.to_string())),
                ("payload", s("x")),
                ("noise", s("y")),
            ]),
        ))
        .await
        .unwrap();
    }

    let page = mock
        .query(
            QueryRequest::new("events", "state = :state")
                .with_values(values([(":state", s("open"))]))
                .with_index("by-state")
                .with_limit(2),
        )
        .await
        .unwrap();
    assert_eq!(page.items[0]["payload"], s("x"));
    assert!(!page.items[0].contains_key("noise"));

    // A cursor that omits the index keys is rejected.
    let bad_cursor = mock
        .query(
            QueryRequest::new("events", "state = :state")
                .with_values(values([(":state", s("open"))]))
                .with_index("by-state")
                .with_exclusive_start_key(item([("pk", s("e1")), ("sk", n("1"))])),
        )
        .await;
    assert!(bad_cursor.is_err());

    // A cursor with only the index keys resumes after the whole key group.
    let index_only_cursor = mock
        .query(
            QueryRequest::new("events", "state = :state")
                .with_values(values([(":state", s("open"))]))
                .with_index("by-state")
                .with_exclusive_start_key(item([("state", s("open")), ("at", n("2"))])),
        )
        .await
        .unwrap();
    assert_eq!(index_only_cursor.count, 3);

    // Descending with a cursor.
    let descending = mock
        .query(
            QueryRequest::new("events", "state = :state")
                .with_values(values([(":state", s("open"))]))
                .with_index("by-state")
                .with_scan_index_forward(false)
                .with_exclusive_start_key(item([
                    ("state", s("open")),
                    ("at", n("4")),
                    ("pk", s("e4")),
                    ("sk", n("4")),
                ])),
        )
        .await
        .unwrap();
    assert_eq!(descending.count, 3);
    assert_eq!(descending.items[0]["at"], n("3"));

    // Scanning the index with a segment exercises the indexed-segment path.
    let segmented = mock
        .scan(
            ScanRequest::new("events")
                .with_index("by-state")
                .with_segment(0, 2),
        )
        .await
        .unwrap();
    assert!(segmented.count <= 5);

    // A projection expression on top of the index projection.
    let projected = mock
        .query(
            QueryRequest::new("events", "state = :state")
                .with_values(values([(":state", s("open"))]))
                .with_index("by-state")
                .with_projection("payload"),
        )
        .await
        .unwrap();
    assert_eq!(projected.items[0].len(), 1);
}

#[tokio::test]
async fn binary_keys_work_in_parallel_scans() {
    let mock = MockDdb::with_table("blobs", KeySchema::hash("pk", KeyType::Binary)).unwrap();
    for byte in 0..6_u8 {
        mock.put_item(PutItemRequest::new(
            "blobs",
            item([("pk", AttributeValue::B(mock_ddb::Blob::new(vec![byte])))]),
        ))
        .await
        .unwrap();
    }
    let mut total = 0;
    for segment in 0..2 {
        total += mock
            .scan(ScanRequest::new("blobs").with_segment(segment, 2))
            .await
            .unwrap()
            .count;
    }
    assert_eq!(total, 6);
}

#[tokio::test]
async fn index_key_type_mismatch_is_rejected() {
    let mock = indexed().await;
    let error = mock
        .put_item(PutItemRequest::new(
            "events",
            item([
                ("pk", s("e1")),
                ("sk", n("1")),
                // `at` must be a number in the index schema.
                ("state", s("open")),
                ("at", s("not-a-number")),
            ]),
        ))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Index Key at"));
    assert_eq!(mock.count("events"), 0);
}

#[tokio::test]
async fn fractional_ttl_values_are_ignored() {
    let mock = users();
    mock.update_time_to_live(UpdateTimeToLiveRequest::new("users", "expires_at", true))
        .await
        .unwrap();
    mock.seed(
        "users",
        vec![item([("pk", s("u#1")), ("expires_at", n("1.5"))])],
    )
    .unwrap();
    // Not an integral epoch second, so the item never expires.
    assert_eq!(mock.count("users"), 1);
    assert_eq!(mock.purge_expired("users").unwrap(), 0);
}

#[tokio::test]
async fn deleting_an_expired_item_emits_nothing() {
    let clock = Arc::new(MockClock::at_epoch_seconds(1_000));
    let mock = MockDdb::with_clock(clock);
    mock.create_table(CreateTableRequest::new(
        "users",
        KeySchema::hash("pk", KeyType::String),
    ))
    .await
    .unwrap();
    mock.update_time_to_live(UpdateTimeToLiveRequest::new("users", "expires_at", true))
        .await
        .unwrap();
    mock.seed(
        "users",
        vec![item([("pk", s("u#1")), ("expires_at", n("999"))])],
    )
    .unwrap();

    let deleted = mock
        .delete_item(
            DeleteItemRequest::new("users", item([("pk", s("u#1"))]))
                .with_return_values(ReturnValues::AllOld),
        )
        .await
        .unwrap();
    assert!(
        deleted.attributes.is_none(),
        "an expired item is not returned"
    );

    // Batch-deleting an expired row also skips the stream hook.
    mock.seed(
        "users",
        vec![item([("pk", s("u#2")), ("expires_at", n("999"))])],
    )
    .unwrap();
    mock.batch_write_item(BatchWriteItemRequest::new().add(
        "users",
        WriteRequest::Delete {
            key: item([("pk", s("u#2"))]),
        },
    ))
    .await
    .unwrap();
    assert_eq!(mock.count("users"), 0);
}

#[tokio::test]
async fn update_expression_error_branches() {
    let mock = users();
    mock.seed("users", vec![item([("pk", s("u#1"))])]).unwrap();

    // ADD a scalar to a missing attribute.
    assert!(mock
        .update_item(
            UpdateItemRequest::new("users", item([("pk", s("u#1"))]), "ADD fresh :v")
                .with_values(values([(":v", s("x"))])),
        )
        .await
        .is_err());

    // DELETE a member from an attribute that is not a set.
    mock.update_item(
        UpdateItemRequest::new("users", item([("pk", s("u#1"))]), "SET tags = :v")
            .with_values(values([(":v", s("not-a-set"))])),
    )
    .await
    .unwrap();
    assert!(mock
        .update_item(
            UpdateItemRequest::new("users", item([("pk", s("u#1"))]), "DELETE tags :v")
                .with_values(values([(":v", s("x"))])),
        )
        .await
        .is_err());

    // DELETE from an attribute that does not exist is a no-op.
    mock.update_item(
        UpdateItemRequest::new("users", item([("pk", s("u#1"))]), "DELETE absent :v")
            .with_values(values([(":v", s("x"))])),
    )
    .await
    .unwrap();

    // Number arithmetic with an unparseable expression value.
    mock.update_item(
        UpdateItemRequest::new("users", item([("pk", s("u#1"))]), "SET count = :one")
            .with_values(values([(":one", n("1"))])),
    )
    .await
    .unwrap();
    assert!(mock
        .update_item(
            UpdateItemRequest::new(
                "users",
                item([("pk", s("u#1"))]),
                "SET count = count + :bad"
            )
            .with_values(values([(":bad", n("not-a-number"))])),
        )
        .await
        .is_err());

    // size() of a missing attribute has no value.
    assert!(mock
        .update_item(UpdateItemRequest::new(
            "users",
            item([("pk", s("u#1"))]),
            "SET size = size(absent)"
        ),)
        .await
        .is_err());
}

#[tokio::test]
async fn delete_item_condition_failure_can_return_the_item() {
    let mock = users();
    mock.seed("users", vec![item([("pk", s("u#1")), ("v", n("1"))])])
        .unwrap();

    let error = mock
        .delete_item(
            DeleteItemRequest::new("users", item([("pk", s("u#1"))]))
                .with_condition("v = :other")
                .with_values(values([(":other", n("2"))]))
                .with_return_values_on_condition_check_failure(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.cancellation_reasons().len(),
        0,
        "a single failed condition is not a transaction"
    );
    match error {
        mock_ddb::Error::ConditionalCheckFailed { item, .. } => {
            assert_eq!(item.unwrap().get("v"), Some(&n("1")));
        }
        other => panic!("expected a conditional failure, got {other:?}"),
    }
}

#[tokio::test]
async fn query_key_conditions_using_functions_and_in() {
    let mock = indexed().await;
    mock.put_item(PutItemRequest::new(
        "events",
        item([
            ("pk", s("e1")),
            ("sk", n("1")),
            ("state", s("open")),
            ("at", n("1")),
        ]),
    ))
    .await
    .unwrap();

    let begins = mock
        .query(
            QueryRequest::new("events", "pk = :pk AND begins_with(#sk, :prefix)")
                .with_names(names([("#sk", "sk")]))
                .with_values(values([(":pk", s("e1")), (":prefix", s("1"))])),
        )
        .await
        .unwrap();
    // A numeric sort key can never begin with a string, so the condition holds
    // for no item. (The service rejects the type mismatch outright; the mock
    // treats an unsatisfiable comparison as false.)
    assert_eq!(begins.count, 0);

    let in_condition = mock
        .query(
            QueryRequest::new("events", "pk IN (:a, :b)")
                .with_values(values([(":a", s("e1")), (":b", s("e2"))])),
        )
        .await;
    assert!(in_condition.is_err());

    let not_condition = mock
        .query(QueryRequest::new("events", "NOT pk = :a").with_values(values([(":a", s("e1"))])))
        .await;
    assert!(not_condition.is_err());

    let contains = mock
        .query(
            QueryRequest::new("events", "contains(pk, :a)").with_values(values([(":a", s("e1"))])),
        )
        .await;
    assert!(contains.is_err());

    let type_condition = mock
        .query(
            QueryRequest::new("events", "attribute_type(pk, :t)")
                .with_values(values([(":t", s("S"))])),
        )
        .await;
    assert!(type_condition.is_err());

    let between = mock
        .query(
            QueryRequest::new("events", "pk BETWEEN :a AND :b")
                .with_values(values([(":a", s("a")), (":b", s("b"))])),
        )
        .await;
    assert!(between.is_err());
}

#[tokio::test]
async fn malformed_expressions_report_their_token() {
    let cases = [
        "",
        "AND",
        "a . = :v",
        "a[1 = :v",
        "a[abc] = :v",
        "a[99999999999999999999] = :v",
        "a. = :v",
        "a != :v",
        "a%b = :v",
        "SET",
        "+",
        "REMOVE a REMOVE b",
        "DELETE a",
        "attribute_exists",
        "a BETWEEN :v",
        "size(a",
        "if_not_exists(a)",
        "list_append(a)",
        "#",
        ": = :v",
    ];
    for case in cases {
        let condition = mock_ddb::ConditionExpression::parse(case);
        let update = mock_ddb::UpdateExpression::parse(case);
        assert!(
            condition.is_err() || update.is_err(),
            "`{case}` must be rejected by at least one parser"
        );
    }

    // `!=` is tolerated as an alias for `<>`.
    let condition = mock_ddb::ConditionExpression::parse("v != :v").unwrap();
    let mut item = HashMap::new();
    item.insert("v".to_string(), n("2"));
    assert!(condition
        .evaluate(Some(&item), &HashMap::new(), &values([(":v", n("1"))]))
        .unwrap());
}

#[tokio::test]
async fn batch_and_transaction_edge_limits() {
    let mock = users();

    // Deleting a key that is absent from a batch is fine.
    mock.batch_write_item(BatchWriteItemRequest::new().add(
        "users",
        WriteRequest::Delete {
            key: item([("pk", s("absent"))]),
        },
    ))
    .await
    .unwrap();

    // A transaction whose action targets an expired item still works.
    mock.update_time_to_live(UpdateTimeToLiveRequest::new("users", "expires_at", true))
        .await
        .unwrap();
    mock.seed(
        "users",
        vec![item([("pk", s("u#1")), ("expires_at", n("1"))])],
    )
    .unwrap();
    mock.transact_write_items(TransactWriteItemsRequest::new(vec![
        TransactWriteItem::put("users", item([("pk", s("u#1")), ("v", s("new"))])),
    ]))
    .await
    .unwrap();
    assert_eq!(mock.count("users"), 1);

    // A batch get with a projection and a missing key.
    let response = mock
        .batch_get_item(
            BatchGetItemRequest::new().add(
                "users",
                KeysAndAttributes::new(vec![item([("pk", s("u#1"))]), item([("pk", s("nope"))])])
                    .with_projection("pk"),
            ),
        )
        .await
        .unwrap();
    assert_eq!(response.responses["users"].len(), 1);
    assert_eq!(response.responses["users"][0].len(), 1);
}

#[tokio::test]
async fn tables_of_every_key_type_work() {
    let mock = MockDdb::new();
    for (name, key_type) in [
        ("tbl-string", KeyType::String),
        ("tbl-number", KeyType::Number),
        ("tbl-binary", KeyType::Binary),
    ] {
        mock.create_table(CreateTableRequest::new(
            name,
            KeySchema::hash("pk", key_type),
        ))
        .await
        .unwrap();
        assert!(mock.describe_table(name).await.is_ok());
    }
    assert_eq!(
        mock.list_tables(ListTablesRequest::default())
            .await
            .unwrap()
            .table_names
            .len(),
        3
    );
    let _ = SystemTime::now();
    let _ = UpdateTimeToLiveRequest::new("tbl-string", "ttl", true);
}
