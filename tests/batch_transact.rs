//! Batches and transactions: limits, atomicity, cancellation reasons and
//! idempotency.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{order, order_key, orders, user, user_key, users};
use mock_ddb::{
    item, n, s, values, BatchGetItemRequest, BatchWriteItemRequest, Error, GetItemRequest,
    KeysAndAttributes, MockClock, MockDdb, ReturnConsumedCapacity, TransactGet,
    TransactGetItemsRequest, TransactWriteItem, TransactWriteItemsRequest, UpdateItemRequest,
    WriteRequest,
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
async fn a_partial_batch_write_is_drained_by_retrying_the_remainder() {
    // The delivery path the knob exists for: a handler that retries
    // `UnprocessedItems` until the service reports none, instead of dropping
    // them on the floor or looping forever on them.
    let mock = orders();
    let mut pending: Vec<WriteRequest> = (1..=4)
        .map(|sk| WriteRequest::Put {
            item: order("c#retry", sk, "1"),
        })
        .collect();

    let mut rounds = 0;
    while !pending.is_empty() {
        rounds += 1;
        assert!(rounds <= 8, "the retry loop has to terminate");
        // Each round leaves part of the batch behind, so the handler loops.
        if pending.len() > 1 {
            mock.throttle_next_batch_write(pending.len() / 2);
        }
        let mut request = BatchWriteItemRequest::new();
        for write in std::mem::take(&mut pending) {
            request = request.add("orders", write);
        }
        let output = mock.batch_write_item(request).await.unwrap();
        pending = output
            .unprocessed_items
            .get("orders")
            .cloned()
            .unwrap_or_default();
    }

    assert_eq!(rounds, 3);
    assert_eq!(mock.count("orders"), 4);
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
                .unwrap()
                .add("users", KeysAndAttributes::new(vec![user_key("u#1")]))
                .unwrap(),
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
        .batch_get_item(
            BatchGetItemRequest::new()
                .add(
                    "orders",
                    KeysAndAttributes::new(vec![item([("pk", s("customer#c1"))])]),
                )
                .unwrap(),
        )
        .await;
    assert!(bad_key.is_err());
}

#[tokio::test]
async fn batch_get_refuses_a_key_repeated_within_one_table() {
    let mock = orders();
    for table in ["users", "mirror"] {
        mock.create_table(mock_ddb::CreateTableRequest::new(
            table,
            mock_ddb::KeySchema::hash("pk", mock_ddb::KeyType::String),
        ))
        .await
        .unwrap();
        mock.seed(table, vec![user("u#1", "a@example.com")])
            .unwrap();
    }
    mock.seed("orders", vec![order("c1", 1, "1"), order("c1", 2, "2")])
        .unwrap();

    // Just inside: two distinct keys of one table, read in one call.
    let distinct = mock
        .batch_get_item(
            BatchGetItemRequest::new()
                .add(
                    "orders",
                    KeysAndAttributes::new(vec![order_key("c1", 1), order_key("c1", 2)]),
                )
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(distinct.responses["orders"].len(), 2);

    // Just outside: the same key twice names one item, so the whole batch is
    // refused instead of quietly reading it twice.
    let repeated = mock
        .batch_get_item(
            BatchGetItemRequest::new()
                .add(
                    "orders",
                    KeysAndAttributes::new(vec![order_key("c1", 1), order_key("c1", 1)]),
                )
                .unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(repeated.code(), "ValidationException");
    assert!(repeated.to_string().contains("duplicates"));

    // The order a composite key is spelled in does not make it another key.
    let reordered = mock
        .batch_get_item(
            BatchGetItemRequest::new()
                .add(
                    "orders",
                    KeysAndAttributes::new(vec![
                        order_key("c1", 1),
                        item([("sk", n("1")), ("pk", s("customer#c1"))]),
                    ]),
                )
                .unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(reordered.code(), "ValidationException");

    // Two `add` calls for one table are one key list, so the second call cannot
    // slip a key past the check that the first call already sent.
    let across_adds = BatchGetItemRequest::new()
        .add("orders", KeysAndAttributes::new(vec![order_key("c1", 1)]))
        .unwrap()
        .add("orders", KeysAndAttributes::new(vec![order_key("c1", 1)]))
        .unwrap();
    let error = mock.batch_get_item(across_adds).await.unwrap_err();
    assert_eq!(error.code(), "ValidationException");

    // The same key value in two tables is two items, not a duplicate.
    let shared = mock
        .batch_get_item(
            BatchGetItemRequest::new()
                .add("users", KeysAndAttributes::new(vec![user_key("u#1")]))
                .unwrap()
                .add("mirror", KeysAndAttributes::new(vec![user_key("u#1")]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(shared.responses["users"].len(), 1);
    assert_eq!(shared.responses["mirror"].len(), 1);
}

#[tokio::test]
async fn a_refused_batch_get_leaves_the_armed_throttle_unspent() {
    let mock = orders();
    mock.seed("orders", vec![order("c1", 1, "1"), order("c1", 2, "2")])
        .unwrap();
    mock.throttle_next_batch_get(1);

    let repeated = mock
        .batch_get_item(
            BatchGetItemRequest::new()
                .add(
                    "orders",
                    KeysAndAttributes::new(vec![order_key("c1", 1), order_key("c1", 1)]),
                )
                .unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(repeated.code(), "ValidationException");

    // The refusal happened before the throttle was spent, so the next batch
    // still sees the one deferred key.
    let response = mock
        .batch_get_item(
            BatchGetItemRequest::new()
                .add(
                    "orders",
                    KeysAndAttributes::new(vec![order_key("c1", 1), order_key("c1", 2)]),
                )
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.unprocessed_keys["orders"].keys.len(), 1);
    assert_eq!(response.responses["orders"].len(), 1);
}

#[tokio::test]
async fn a_batch_get_built_in_two_steps_reads_both_steps_keys() {
    let mock = orders();
    mock.seed("orders", vec![order("c1", 1, "1"), order("c1", 2, "2")])
        .unwrap();

    // The second `add` appends to the first. Before, it replaced the table's
    // `KeysAndAttributes` and this batch read one key while claiming to read
    // two, without an error anywhere.
    let request = BatchGetItemRequest::new()
        .add("orders", KeysAndAttributes::new(vec![order_key("c1", 1)]))
        .unwrap()
        .add("orders", KeysAndAttributes::new(vec![order_key("c1", 2)]))
        .unwrap();
    assert_eq!(request.request_items["orders"].keys.len(), 2);
    let response = mock.batch_get_item(request).await.unwrap();
    assert_eq!(response.responses["orders"].len(), 2);
}

#[tokio::test]
async fn two_adds_that_together_overshoot_a_hundred_keys_are_refused() {
    let mock = orders();
    let first: Vec<_> = (1..=60).map(|id| order_key("c1", id)).collect();
    let forty: Vec<_> = (61..=100).map(|id| order_key("c1", id)).collect();
    let forty_one: Vec<_> = (61..=101).map(|id| order_key("c1", id)).collect();

    let at_limit = BatchGetItemRequest::new()
        .add("orders", KeysAndAttributes::new(first.clone()))
        .unwrap()
        .add("orders", KeysAndAttributes::new(forty))
        .unwrap();
    assert_eq!(at_limit.request_items["orders"].keys.len(), 100);
    // An entry for a table with no matching item is omitted, so a batch read
    // that matches nothing answers with nothing at all.
    assert!(mock
        .batch_get_item(at_limit)
        .await
        .unwrap()
        .responses
        .is_empty());

    // One key past the limit is refused, and the merged keys are what the
    // check counts.
    let past_limit = BatchGetItemRequest::new()
        .add("orders", KeysAndAttributes::new(first))
        .unwrap()
        .add("orders", KeysAndAttributes::new(forty_one))
        .unwrap();
    assert_eq!(past_limit.request_items["orders"].keys.len(), 101);
    let err = mock.batch_get_item(past_limit).await.unwrap_err();
    assert_eq!(err.code(), "ValidationException");
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
            assert_eq!(reasons[0].code(), mock_ddb::CancellationReasonCode::None);
            assert_eq!(
                reasons[1].code(),
                mock_ddb::CancellationReasonCode::ConditionalCheckFailed
            );
            assert!(reasons[1].message().contains("orders"));
            assert!(reasons[1].item().is_none());
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
    assert_eq!(reasons[0].code(), mock_ddb::CancellationReasonCode::None);
    assert_eq!(
        reasons[1].code(),
        mock_ddb::CancellationReasonCode::ConditionalCheckFailed
    );
    assert_eq!(
        reasons[1].item(),
        Some(&order("c1", 1, "10")),
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
async fn transaction_token_limit_counts_characters_not_bytes() {
    let mock = orders();
    let write = |token: &str| {
        TransactWriteItemsRequest::new(vec![TransactWriteItem::put("orders", order("c1", 1, "1"))])
            .with_client_request_token(token)
    };

    // Just inside: 36 four-byte characters are 144 bytes and still valid, so a
    // byte-based check would reject a token the service accepts.
    let at_limit = "🎉".repeat(36);
    assert_eq!(at_limit.chars().count(), 36);
    assert_eq!(at_limit.len(), 144);
    mock.transact_write_items(write(&at_limit))
        .await
        .expect("36 characters are inside the constraint");

    // Just outside: one more character is rejected, whatever its width.
    let error = mock
        .transact_write_items(write(&"🎉".repeat(37)))
        .await
        .expect_err("37 characters are outside the constraint");
    assert_eq!(error.code(), "ValidationException");
    let text = error.to_string();
    assert!(
        text.contains("clientRequestToken"),
        "message names the member: {text}"
    );

    // A narrow token of the same length is refused as well.
    assert!(mock
        .transact_write_items(write(&"x".repeat(37)))
        .await
        .is_err());

    // An empty token is refused too.
    assert!(mock.transact_write_items(write("")).await.is_err());

    // Only the first call wrote; the rejections changed nothing.
    assert_eq!(mock.count("orders"), 1);
}

#[tokio::test]
async fn transaction_token_makes_a_retry_a_no_op() {
    let mock = orders();
    let token = "retry-token-1";

    let first = TransactWriteItemsRequest::new(vec![TransactWriteItem::put(
        "orders",
        order("c1", 1, "10"),
    )])
    .with_client_request_token(token);

    mock.transact_write_items(first.clone()).await.unwrap();

    // Same token, same payload: the retry writes nothing.
    mock.transact_write_items(first.clone()).await.unwrap();
    assert_eq!(mock.count("orders"), 1);

    // The payload is compared once the token and the response-shaping flags are
    // cleared, so a retry may ask for a different capacity report and still be
    // the same request.
    mock.transact_write_items(
        first
            .clone()
            .with_return_consumed_capacity(ReturnConsumedCapacity::Total),
    )
    .await
    .unwrap();
    assert_eq!(mock.count("orders"), 1);
}

#[tokio::test]
async fn transaction_token_with_a_different_payload_is_refused() {
    let mock = orders();
    let token = "constant-token";

    mock.transact_write_items(
        TransactWriteItemsRequest::new(vec![TransactWriteItem::put(
            "orders",
            order("c1", 1, "10"),
        )])
        .with_client_request_token(token),
    )
    .await
    .unwrap();

    // Same token, different item: the service refuses instead of discarding the
    // second write, so a lost write cannot pass as a retry.
    let error = mock
        .transact_write_items(
            TransactWriteItemsRequest::new(vec![TransactWriteItem::put(
                "orders",
                order("c1", 2, "20"),
            )])
            .with_client_request_token(token),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "IdempotentParameterMismatchException");
    assert!(!error.is_retryable());

    // Same token and same item, but with a different condition: a different
    // request as well.
    let updated = mock
        .transact_write_items(
            TransactWriteItemsRequest::new(vec![TransactWriteItem::put(
                "orders",
                order("c1", 1, "10"),
            )
            .with_condition("attribute_exists(pk)")])
            .with_client_request_token(token),
        )
        .await
        .unwrap_err();
    assert_eq!(updated.code(), "IdempotentParameterMismatchException");

    assert_eq!(mock.count("orders"), 1);
    assert!(mock
        .get_item(GetItemRequest::new("orders", order_key("c1", 2)))
        .await
        .unwrap()
        .item
        .is_none());
}

#[tokio::test]
async fn transaction_token_is_forgotten_after_ten_minutes() {
    let clock = Arc::new(MockClock::at_epoch_seconds(1_700_000_000));
    let mock = orders();
    mock.set_clock(clock.clone());
    let token = "expiring-token";
    let request = |total: &str| {
        TransactWriteItemsRequest::new(vec![TransactWriteItem::put(
            "orders",
            order("c1", 1, total),
        )])
        .with_client_request_token(token)
    };

    mock.transact_write_items(request("10")).await.unwrap();

    // Inside the window: still a replay, even with a different payload, which the
    // service refuses rather than replays.
    clock.advance(Duration::from_secs(599));
    let inside = mock.transact_write_items(request("20")).await.unwrap_err();
    assert_eq!(inside.code(), "IdempotentParameterMismatchException");
    assert_eq!(mock.count("orders"), 1);

    // Outside the window — ten minutes after the token was spent — the token is
    // forgotten, so the request is a new one and applies.
    clock.advance(Duration::from_secs(1));
    mock.transact_write_items(request("30")).await.unwrap();
    assert_eq!(mock.count("orders"), 1);
    let stored = mock
        .get_item(GetItemRequest::new("orders", order_key("c1", 1)))
        .await
        .unwrap();
    assert_eq!(stored.item.unwrap()["total"], n("30"));
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
