//! 32 tasks over one shared mock: the mock must behave like a shared client,
//! not like a single-threaded scratch space.

use std::sync::Arc;

use mock_ddb::{
    item, n, s, values, GetItemRequest, KeySchema, KeyType, MockDdb, PutItemRequest, QueryRequest,
    ReturnValues, TransactWriteItem, TransactWriteItemsRequest, UpdateItemRequest,
};

const TASKS: usize = 32;
/// Accounts used by the transfer invariant test.
const ACCOUNTS: usize = 8;
/// Opening balance of each account.
const START: i64 = 100;

fn shared() -> Arc<MockDdb> {
    Arc::new(MockDdb::with_table("counters", KeySchema::hash("pk", KeyType::String)).unwrap())
}

#[test]
fn the_client_is_send_sync_and_clone() {
    fn assert_send_sync<T: Send + Sync + Clone>() {}
    assert_send_sync::<MockDdb>();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_writes_and_reads_do_not_lose_items() {
    let mock = shared();
    let mut handles = Vec::new();
    for task in 0..TASKS {
        let mock = Arc::clone(&mock);
        handles.push(tokio::spawn(async move {
            for position in 0..10 {
                let key = format!("task#{task}#{position}");
                mock.put_item(PutItemRequest::new(
                    "counters",
                    item([("pk", s(&key)), ("task", n(task.to_string()))]),
                ))
                .await
                .unwrap();
                let found = mock
                    .get_item(GetItemRequest::new("counters", item([("pk", s(&key))])))
                    .await
                    .unwrap()
                    .item
                    .expect("the item we just wrote");
                assert_eq!(found.get("pk"), Some(&s(&key)));
            }
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }
    assert_eq!(mock.count("counters"), TASKS * 10);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn exactly_one_conditional_create_wins() {
    let mock = shared();
    let mut handles = Vec::new();
    for task in 0..TASKS {
        let mock = Arc::clone(&mock);
        handles.push(tokio::spawn(async move {
            mock.put_item(
                PutItemRequest::new(
                    "counters",
                    item([("pk", s("singleton")), ("owner", n(task.to_string()))]),
                )
                .with_condition("attribute_not_exists(pk)"),
            )
            .await
            .is_ok()
        }));
    }

    let mut winners = 0;
    for handle in handles {
        if handle.await.unwrap() {
            winners += 1;
        }
    }
    assert_eq!(
        winners, 1,
        "a conditional create is a mutual-exclusion primitive"
    );
    assert_eq!(mock.count("counters"), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn compare_and_set_counter_has_no_lost_updates() {
    let mock = shared();
    mock.put_item(PutItemRequest::new(
        "counters",
        item([("pk", s("counter")), ("value", n("0"))]),
    ))
    .await
    .unwrap();

    let mut handles = Vec::new();
    for _ in 0..TASKS {
        let mock = Arc::clone(&mock);
        handles.push(tokio::spawn(async move {
            for _ in 0..5 {
                loop {
                    let current = mock
                        .get_item(GetItemRequest::new(
                            "counters",
                            item([("pk", s("counter"))]),
                        ))
                        .await
                        .unwrap()
                        .item
                        .unwrap()["value"]
                        .as_n()
                        .unwrap()
                        .clone();
                    let updated = mock
                        .update_item(
                            UpdateItemRequest::new(
                                "counters",
                                item([("pk", s("counter"))]),
                                "SET #v = #v + :one",
                            )
                            .with_names(mock_ddb::names([("#v", "value")]))
                            .with_condition("#v = :current")
                            .with_values(values([(":one", n("1")), (":current", n(current))]))
                            .with_return_values(ReturnValues::AllNew),
                        )
                        .await;
                    match updated {
                        Ok(_) => break,
                        Err(error) if error.is_conditional_check_failed() => {}
                        Err(error) => panic!("unexpected failure: {error}"),
                    }
                }
            }
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }

    let final_value = mock
        .get_item(GetItemRequest::new(
            "counters",
            item([("pk", s("counter"))]),
        ))
        .await
        .unwrap()
        .item
        .unwrap()["value"]
        .as_n()
        .unwrap()
        .clone();
    assert_eq!(final_value, (TASKS * 5).to_string());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_transactions_preserve_the_invariant() {
    let mock = shared();
    for account in 0..ACCOUNTS {
        mock.put_item(PutItemRequest::new(
            "counters",
            item([
                ("pk", s(format!("account#{account}"))),
                ("balance", n(START.to_string())),
            ]),
        ))
        .await
        .unwrap();
    }

    let mut handles = Vec::new();
    for task in 0..TASKS {
        let mock = Arc::clone(&mock);
        handles.push(tokio::spawn(async move {
            let from = format!("account#{}", task % ACCOUNTS);
            let to = format!("account#{}", (task + 1) % ACCOUNTS);
            for _ in 0..5 {
                let result = mock
                    .transact_write_items(TransactWriteItemsRequest::new(vec![
                        TransactWriteItem::update(
                            "counters",
                            item([("pk", s(&from))]),
                            "SET balance = balance - :one",
                        )
                        .with_condition("balance >= :one")
                        .with_values(values([(":one", n("1"))])),
                        TransactWriteItem::update(
                            "counters",
                            item([("pk", s(&to))]),
                            "SET balance = balance + :one",
                        )
                        .with_values(values([(":one", n("1"))])),
                    ]))
                    .await;
                assert!(result.is_ok(), "transaction failed: {result:?}");
            }
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }

    let total: i64 = mock
        .scan(mock_ddb::ScanRequest::new("counters"))
        .await
        .unwrap()
        .items
        .iter()
        .map(|item| item["balance"].as_n().unwrap().parse::<i64>().unwrap())
        .sum();
    assert_eq!(
        total,
        START * i64::try_from(ACCOUNTS).unwrap(),
        "money is neither created nor destroyed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_queries_see_a_consistent_snapshot() {
    let mock = Arc::new(
        MockDdb::with_table(
            "events",
            KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::Number),
        )
        .unwrap(),
    );
    for id in 0..50 {
        mock.put_item(PutItemRequest::new(
            "events",
            item([("pk", s("p")), ("sk", n(id.to_string()))]),
        ))
        .await
        .unwrap();
    }

    let mut handles = Vec::new();
    for _ in 0..TASKS {
        let mock = Arc::clone(&mock);
        handles.push(tokio::spawn(async move {
            let page = mock
                .query(
                    QueryRequest::new("events", "pk = :pk").with_values(values([(":pk", s("p"))])),
                )
                .await
                .unwrap();
            assert_eq!(page.count, 50);
            let keys: Vec<i64> = page
                .items
                .iter()
                .map(|item| item["sk"].as_n().unwrap().parse().unwrap())
                .collect();
            let mut sorted = keys.clone();
            sorted.sort_unstable();
            assert_eq!(keys, sorted);
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_batch_writes_and_deletes_stay_consistent() {
    let mock = shared();
    let mut handles = Vec::new();
    for task in 0..TASKS {
        let mock = Arc::clone(&mock);
        handles.push(tokio::spawn(async move {
            let mut request = mock_ddb::BatchWriteItemRequest::new();
            for position in 0..10 {
                request = request.add(
                    "counters",
                    mock_ddb::WriteRequest::Put {
                        item: item([("pk", s(format!("batch#{task}#{position}")))]),
                    },
                );
            }
            mock.batch_write_item(request).await.unwrap();
            // Delete half of them again.
            let mut cleanup = mock_ddb::BatchWriteItemRequest::new();
            for position in 0..5 {
                cleanup = cleanup.add(
                    "counters",
                    mock_ddb::WriteRequest::Delete {
                        key: item([("pk", s(format!("batch#{task}#{position}")))]),
                    },
                );
            }
            mock.batch_write_item(cleanup).await.unwrap();
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }
    assert_eq!(mock.count("counters"), TASKS * 5);
}
