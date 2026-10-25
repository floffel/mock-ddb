//! Batches and transactions: limits, atomicity, cancellation reasons and
//! idempotency.

mod common;

use common::{order, order_key, orders, user, user_key, users};
use mock_ddb::{
    item, n, s, values, BatchGetItemRequest, BatchWriteItemRequest, Error, GetItemRequest,
    KeysAndAttributes, MockDdb, TransactGet, TransactGetItemsRequest, TransactWriteItem,
    TransactWriteItemsRequest, UpdateItemRequest, WriteRequest,
};

#[tokio::test]
async fn batch_write_accepts_twenty_five_writes_across_tables() {
    let mock = orders();
    mock.create_table(mock_ddb::CreateTableRequest::new(
        "users",
        mock_ddb::KeySchema::hash("pk", mock_ddb::KeyType::String),
    ))
    .await
    .unwrap();

    let mut request = BatchWriteItemRequest::new();
    for id in 1..=20 {
        request = request.add(
            "orders",
            WriteRequest::Put {
                item: order("c1", id, "1"),
            },
        );
    }
    for id in 0..5 {
        request = request.add(
            "users",
            WriteRequest::Put {
                item: user(&format!("u#{id}"), "a@example.com"),
            },
        );
    }
    let response = mock.batch_write_item(request).await.unwrap();
    assert_eq!(response.unprocessed_items.len(), 0);
    assert_eq!(mock.count("orders"), 20);
    assert_eq!(mock.count("users"), 5);

    let mut too_many = BatchWriteItemRequest::new();
    for id in 1..=26 {
        too_many = too_many.add(
            "orders",
            WriteRequest::Put {
                item: order("c2", id, "1"),
            },
        );
    }
    assert!(mock.batch_write_item(too_many).await.is_err());

    assert!(mock
        .batch_write_item(BatchWriteItemRequest::new())
        .await
        .is_err());
}

#[tokio::test]
async fn batch_write_rejects_duplicate_keys_and_unknown_tables() {
    let mock = orders();
    let duplicate = BatchWriteItemRequest::new()
        .add(
            "orders",
            WriteRequest::Put {
                item: order("c1", 1, "1"),
            },
        )
        .add(
            "orders",
            WriteRequest::Delete {
                key: order_key("c1", 1),
            },
        );
    let error = mock.batch_write_item(duplicate).await.unwrap_err();
    assert!(error.to_string().contains("duplicates"));
    assert_eq!(mock.count("orders"), 0);

    let unknown = BatchWriteItemRequest::new().add(
        "nope",
        WriteRequest::Put {
            item: item([("pk", s("x"))]),
        },
    );
    assert!(mock.batch_write_item(unknown).await.is_err());
}

#[tokio::test]
async fn batch_write_validates_every_item_before_writing_any() {
    let mock = orders();
    let request = BatchWriteItemRequest::new()
        .add(
            "orders",
            WriteRequest::Put {
                item: order("c1", 1, "1"),
            },
        )
        .add(
            "orders",
            WriteRequest::Put {
                // Missing the sort key: the whole batch must be rejected.
                item: item([("pk", s("customer#c1"))]),
            },
        );
    assert!(mock.batch_write_item(request).await.is_err());
    assert_eq!(mock.count("orders"), 0);
}

#[tokio::test]
async fn batch_write_deletes_and_maintains_indexes() {
    let mock = MockDdb::new();
    mock.create_table(
        mock_ddb::CreateTableRequest::new(
            "orders",
            mock_ddb::KeySchema::hash_and_range(
                "pk",
                mock_ddb::KeyType::String,
                "sk",
                mock_ddb::KeyType::Number,
            ),
        )
        .with_global_index(mock_ddb::SecondaryIndex::new(
            "by-total",
            mock_ddb::KeySchema::hash("total", mock_ddb::KeyType::Number),
        )),
    )
    .await
    .unwrap();

    mock.batch_write_item(BatchWriteItemRequest::new().add(
        "orders",
        WriteRequest::Put {
            item: order("c1", 1, "10"),
        },
    ))
    .await
    .unwrap();

    let page = mock
        .query(
            mock_ddb::QueryRequest::new("orders", "total = :total")
                .with_index("by-total")
                .with_values(values([(":total", n("10"))])),
        )
        .await
        .unwrap();
    assert_eq!(page.count, 1);

    mock.batch_write_item(BatchWriteItemRequest::new().add(
        "orders",
        WriteRequest::Delete {
            key: order_key("c1", 1),
        },
    ))
    .await
    .unwrap();
    assert_eq!(mock.count("orders"), 0);
    let page = mock
        .query(
            mock_ddb::QueryRequest::new("orders", "total = :total")
                .with_index("by-total")
                .with_values(values([(":total", n("10"))])),
        )
        .await
        .unwrap();
    assert_eq!(page.count, 0);
}

#[tokio::test]
async fn batch_get_spans_tables_and_keeps_request_order() {
    let mock = orders();
    mock.create_table(mock_ddb::CreateTableRequest::new(
        "users",
        mock_ddb::KeySchema::hash("pk", mock_ddb::KeyType::String),
    ))
    .await
    .unwrap();
    mock.seed("orders", vec![order("c1", 1, "1"), order("c1", 2, "2")])
        .unwrap();
    mock.seed("users", vec![user("u#1", "a@example.com")])
        .unwrap();

    let response = mock
        .batch_get_item(
            BatchGetItemRequest::new()
                .add(
                    "orders",
                    KeysAndAttributes::new(vec![order_key("c1", 2), order_key("c1", 1)]),
                )
                .add("users", KeysAndAttributes::new(vec![user_key("u#1")])),
        )
        .await
        .unwrap();

    let order_ids: Vec<String> = response.responses["orders"]
        .iter()
        .map(|item| item["sk"].as_n().unwrap().clone())
        .collect();
    assert_eq!(order_ids, vec!["2", "1"]);
    assert_eq!(response.responses["users"].len(), 1);
    assert_eq!(response.unprocessed_keys.len(), 0);

    let bad_key = mock
        .batch_get_item(BatchGetItemRequest::new().add(
            "orders",
            KeysAndAttributes::new(vec![item([("pk", s("customer#c1"))])]),
        ))
        .await;
    assert!(bad_key.is_err());
}

#[tokio::test]
async fn transaction_applies_every_action_or_none() {
    let mock = orders();
    mock.create_table(mock_ddb::CreateTableRequest::new(
        "users",
        mock_ddb::KeySchema::hash("pk", mock_ddb::KeyType::String),
    ))
    .await
    .unwrap();
    mock.seed("orders", vec![order("c1", 1, "10"), order("c1", 2, "20")])
        .unwrap();
    mock.seed("users", vec![user("u#1", "a@example.com")])
        .unwrap();

    mock.transact_write_items(TransactWriteItemsRequest::new(vec![
        TransactWriteItem::put("orders", order("c1", 3, "30")),
        TransactWriteItem::update("orders", order_key("c1", 1), "SET total = total + :ten")
            .with_values(values([(":ten", n("10"))])),
        TransactWriteItem::delete("orders", order_key("c1", 2)),
        TransactWriteItem::condition_check("users", user_key("u#1"), "attribute_exists(pk)"),
    ]))
    .await
    .unwrap();

    assert_eq!(mock.count("orders"), 2);
    let updated = mock
        .get_item(GetItemRequest::new("orders", order_key("c1", 1)))
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(updated.get("total"), Some(&n("20")));
}

#[tokio::test]
async fn cancelled_transaction_leaves_no_partial_state() {
    let mock = orders();
    mock.seed("orders", vec![order("c1", 1, "10")]).unwrap();

    let result = mock
        .transact_write_items(TransactWriteItemsRequest::new(vec![
            TransactWriteItem::put("orders", order("c1", 2, "20")),
            TransactWriteItem::update("orders", order_key("c1", 1), "SET total = :new")
                .with_condition("total = :expected")
                .with_values(values([(":new", n("99")), (":expected", n("11"))])),
        ]))
        .await
        .unwrap_err();

    match &result {
        Error::TransactionCanceled { reasons, .. } => {
            assert_eq!(reasons.len(), 2);
            assert_eq!(reasons[0].code, mock_ddb::CancellationReasonCode::None);
            assert_eq!(
                reasons[1].code,
                mock_ddb::CancellationReasonCode::ConditionalCheckFailed
            );
            assert!(reasons[1].message.contains("orders"));
            assert!(reasons[1].item.is_none());
        }
        other => panic!("expected a cancelled transaction, got {other:?}"),
    }
    assert_eq!(mock.count("orders"), 1);
    let untouched = mock
        .get_item(GetItemRequest::new("orders", order_key("c1", 1)))
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(untouched.get("total"), Some(&n("10")));
}

#[tokio::test]
async fn cancellation_reason_carries_the_old_item_when_requested() {
    let mock = orders();
    mock.seed("orders", vec![order("c1", 1, "10")]).unwrap();

    let error = mock
        .transact_write_items(TransactWriteItemsRequest::new(vec![
            TransactWriteItem::put("orders", order("c1", 2, "20")),
            TransactWriteItem::Update(mock_ddb::TransactUpdate {
                table_name: "orders".to_string(),
                key: order_key("c1", 1),
                update_expression: "SET total = :new".to_string(),
                condition_expression: Some("total = :expected".to_string()),
                expression_attribute_values: Some(values([
                    (":new", n("99")),
                    (":expected", n("11")),
                ])),
                return_values_on_condition_check_failure: true,
                ..mock_ddb::TransactUpdate::default()
            }),
        ]))
        .await
        .unwrap_err();

    let reasons = error.cancellation_reasons();
    assert_eq!(reasons.len(), 2);
    assert_eq!(reasons[0].code, mock_ddb::CancellationReasonCode::None);
    assert_eq!(
        reasons[1].code,
        mock_ddb::CancellationReasonCode::ConditionalCheckFailed
    );
    assert_eq!(
        reasons[1].item,
        Some(order("c1", 1, "10")),
        "the reason must carry the pre-transaction image"
    );
    assert_eq!(mock.count("orders"), 1);
}

#[tokio::test]
async fn transaction_rejects_duplicate_items_and_bad_requests() {
    let mock = orders();

    let duplicate = mock
        .transact_write_items(TransactWriteItemsRequest::new(vec![
            TransactWriteItem::put("orders", order("c1", 1, "1")),
            TransactWriteItem::delete("orders", order_key("c1", 1)),
        ]))
        .await;
    assert!(duplicate
        .unwrap_err()
        .to_string()
        .contains("multiple operations on one item"));

    let empty = mock
        .transact_write_items(TransactWriteItemsRequest::new(Vec::new()))
        .await;
    assert!(empty.is_err());

    let too_many = mock
        .transact_write_items(TransactWriteItemsRequest::new(
            (0..101)
                .map(|position| TransactWriteItem::put("orders", order("c1", position, "1")))
                .collect(),
        ))
        .await;
    assert!(too_many.is_err());

    let unknown_table = mock
        .transact_write_items(TransactWriteItemsRequest::new(vec![
            TransactWriteItem::put("nope", item([("pk", s("a"))])),
        ]))
        .await;
    assert!(unknown_table.is_err());

    let bad_expression = mock
        .transact_write_items(TransactWriteItemsRequest::new(vec![
            TransactWriteItem::update("orders", order_key("c1", 1), "SET nope"),
        ]))
        .await;
    assert!(bad_expression.is_err());

    let long_token = mock
        .transact_write_items(
            TransactWriteItemsRequest::new(vec![TransactWriteItem::put(
                "orders",
                order("c1", 1, "1"),
            )])
            .with_client_request_token("x".repeat(37)),
        )
        .await;
    assert!(long_token.is_err());

    // None of the rejected calls wrote anything.
    assert_eq!(mock.count("orders"), 0);
}

#[tokio::test]
async fn transaction_token_makes_a_retry_a_no_op() {
    let mock = orders();
    let token = "retry-token-1";

    mock.transact_write_items(
        TransactWriteItemsRequest::new(vec![TransactWriteItem::put(
            "orders",
            order("c1", 1, "10"),
        )])
        .with_client_request_token(token),
    )
    .await
    .unwrap();

    // Same token, different payload: the service replays the earlier result and
    // applies nothing.
    mock.transact_write_items(
        TransactWriteItemsRequest::new(vec![TransactWriteItem::put(
            "orders",
            order("c1", 2, "20"),
        )])
        .with_client_request_token(token),
    )
    .await
    .unwrap();

    assert_eq!(mock.count("orders"), 1);
    assert!(mock
        .get_item(GetItemRequest::new("orders", order_key("c1", 2)))
        .await
        .unwrap()
        .item
        .is_none());
}

#[tokio::test]
async fn transact_get_keeps_order_and_reports_misses() {
    let mock = orders();
    mock.seed("orders", vec![order("c1", 1, "10")]).unwrap();

    let response = mock
        .transact_get_items(TransactGetItemsRequest::new(vec![
            TransactGet::new("orders", order_key("c1", 1)),
            TransactGet::new("orders", order_key("c1", 2)),
        ]))
        .await
        .unwrap();
    assert_eq!(response.responses.len(), 2);
    assert!(response.responses[0].is_some());
    assert!(response.responses[1].is_none());

    let projected = mock
        .transact_get_items(TransactGetItemsRequest::new(vec![TransactGet {
            projection_expression: Some("pk".to_string()),
            ..TransactGet::new("orders", order_key("c1", 1))
        }]))
        .await
        .unwrap();
    assert_eq!(projected.responses[0].as_ref().unwrap().len(), 1);

    assert!(mock
        .transact_get_items(TransactGetItemsRequest::new(Vec::new()))
        .await
        .is_err());
    assert!(mock
        .transact_get_items(TransactGetItemsRequest::new(vec![TransactGet::new(
            "nope",
            item([("pk", s("a"))])
        )]))
        .await
        .is_err());
}

#[tokio::test]
async fn transaction_update_can_create_a_missing_item() {
    let mock = orders();
    mock.transact_write_items(TransactWriteItemsRequest::new(vec![
        TransactWriteItem::update("orders", order_key("c1", 1), "SET total = :total")
            .with_values(values([(":total", n("42"))])),
    ]))
    .await
    .unwrap();

    let created = mock
        .get_item(GetItemRequest::new("orders", order_key("c1", 1)))
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(created.get("total"), Some(&n("42")));
    assert_eq!(created.get("sk"), Some(&n("1")));
}

#[tokio::test]
async fn transaction_delete_and_condition_check_on_missing_items() {
    let mock = users();
    // Deleting a missing item is a no-op, and a condition check on a missing
    // item is satisfied by attribute_not_exists.
    mock.transact_write_items(TransactWriteItemsRequest::new(vec![
        TransactWriteItem::delete("users", user_key("nobody")),
        TransactWriteItem::condition_check("users", user_key("other"), "attribute_not_exists(pk)"),
    ]))
    .await
    .unwrap();

    let failed = mock
        .transact_write_items(TransactWriteItemsRequest::new(vec![
            TransactWriteItem::condition_check("users", user_key("nobody"), "attribute_exists(pk)"),
        ]))
        .await;
    assert!(failed.is_err());
}

#[tokio::test]
async fn update_item_with_condition_on_missing_item_creates_it() {
    let mock = users();
    let created = mock
        .update_item(
            UpdateItemRequest::new("users", user_key("u#1"), "SET email = :email")
                .with_condition("attribute_not_exists(pk)")
                .with_values(values([(":email", s("a@example.com"))]))
                .with_return_values(mock_ddb::ReturnValues::AllNew),
        )
        .await
        .unwrap();
    assert!(created.attributes.is_some());

    let second = mock
        .update_item(
            UpdateItemRequest::new("users", user_key("u#1"), "SET email = :email")
                .with_condition("attribute_not_exists(pk)")
                .with_values(values([(":email", s("b@example.com"))])),
        )
        .await;
    assert!(second.is_err());
}
