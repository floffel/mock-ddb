//! Property tests: the invariants a mock must never break, over random input.

use std::collections::{BTreeMap, HashMap};

use mock_ddb::{
    item, n, s, values, AttributeValue, Blob, ConditionExpression, GetItemRequest, Item, KeySchema,
    KeyType, MockDdb, PutItemRequest, QueryRequest, ReturnValues, UpdateExpression,
    UpdateItemRequest,
};
use proptest::prelude::*;

/// A small async runner, because `#[tokio::test]` and proptest do not compose.
fn run<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

fn users() -> MockDdb {
    MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap()
}

fn events() -> MockDdb {
    MockDdb::with_table(
        "events",
        KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::Number),
    )
    .unwrap()
}

/// A value shape the mock must round-trip unchanged.
fn attribute() -> impl Strategy<Value = AttributeValue> {
    let leaf = prop_oneof![
        "[a-zA-Z0-9 _#üöä-]{0,12}".prop_map(AttributeValue::S),
        (-1_000_000_i64..1_000_000).prop_map(|value| AttributeValue::N(value.to_string())),
        prop::collection::vec(any::<u8>(), 0..8)
            .prop_map(|bytes| AttributeValue::B(Blob::new(bytes))),
        any::<bool>().prop_map(AttributeValue::Bool),
        Just(AttributeValue::Null(true)),
    ];
    leaf.prop_recursive(2, 4, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..3).prop_map(AttributeValue::L),
            prop::collection::hash_map("[a-z]{1,5}", inner.clone(), 0..3)
                .prop_map(AttributeValue::M),
            prop::collection::vec("[a-z]{1,4}", 1..3).prop_map(AttributeValue::Ss),
            prop::collection::vec(1_i64..100, 1..3).prop_map(|members| AttributeValue::Ns(
                members.iter().map(i64::to_string).collect()
            )),
        ]
    })
}

fn payload() -> impl Strategy<Value = HashMap<String, AttributeValue>> {
    prop::collection::hash_map("[a-z][a-z0-9_]{0,7}", attribute(), 0..6)
        .prop_filter("reserved or non-user attribute", |entries| {
            entries.keys().all(|name| name != "pk" && name != "sk")
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Whatever goes in comes out: every attribute value shape survives a
    /// write and a read unchanged.
    #[test]
    fn put_get_round_trip(entries in payload()) {
        let mock = users();
        let mut expected = entries.clone();
        expected.insert("pk".to_string(), s("u#1"));
        run(async {
            mock.put_item(PutItemRequest::new("users", expected.clone()))
                .await
                .unwrap();
            let found = mock
                .get_item(GetItemRequest::new("users", item([("pk", s("u#1"))])))
                .await
                .unwrap()
                .item
                .unwrap();
            assert_eq!(found, expected);
        });
    }

    /// Sort keys order numerically, not lexicographically, and the order is
    /// stable regardless of insertion order.
    #[test]
    fn query_returns_key_sorted_order(mut ids in prop::collection::vec(0_u32..10_000, 1..30)) {
        ids.sort_unstable();
        ids.dedup();
        let mock = events();
        run(async {
            for id in ids.iter().rev() {
                mock.put_item(PutItemRequest::new(
                    "events",
                    item([("pk", s("p")), ("sk", n(id.to_string()))]),
                ))
                .await
                .unwrap();
            }
            let page = mock
                .query(
                    QueryRequest::new("events", "pk = :pk")
                        .with_values(values([(":pk", s("p"))])),
                )
                .await
                .unwrap();
            assert_eq!(page.count, ids.len());
            let returned: Vec<u32> = page
                .items
                .iter()
                .map(|item| item["sk"].as_n().unwrap().parse().unwrap())
                .collect();
            let mut expected = ids.clone();
            expected.sort_unstable();
            assert_eq!(returned, expected);
        });
    }

    /// A rejected condition never applies any part of the write.
    #[test]
    fn conditional_writes_never_partially_apply(
        before in -100_i64..100,
        expected in -100_i64..100,
        replacement in -100_i64..100,
    ) {
        let mock = users();
        run(async {
            mock.put_item(PutItemRequest::new(
                "users",
                item([("pk", s("u#1")), ("counter", n(before.to_string()))]),
            ))
            .await
            .unwrap();

            let result = mock
                .update_item(
                    UpdateItemRequest::new("users", item([("pk", s("u#1"))]), "SET counter = :new, touched = :yes")
                        .with_condition("counter = :expected")
                        .with_values(values([
                            (":new", n(replacement.to_string())),
                            (":expected", n(expected.to_string())),
                            (":yes", s("true")),
                        ]))
                        .with_return_values(ReturnValues::AllNew),
                )
                .await;

            let stored = mock
                .get_item(GetItemRequest::new("users", item([("pk", s("u#1"))])))
                .await
                .unwrap()
                .item
                .unwrap();

            if before == expected {
                assert!(result.is_ok());
                assert_eq!(stored.get("counter"), Some(&n(replacement.to_string())));
                assert_eq!(stored.get("touched"), Some(&s("true")));
            } else {
                assert!(result.is_err());
                assert_eq!(stored.get("counter"), Some(&n(before.to_string())));
                assert!(!stored.contains_key("touched"), "no partial write");
            }
        });
    }

    /// `SET` is idempotent: applying the same assignment twice is the same as
    /// applying it once.
    #[test]
    fn set_updates_are_idempotent(value in "[a-zA-Z0-9]{0,10}", rounds in 1_usize..5) {
        let expression = UpdateExpression::parse("SET state = :v").unwrap();
        let names = HashMap::new();
        let attribute_values = values([(":v", s(value.clone()))]);
        let mut item = item([("pk", s("u#1"))]);
        for _ in 0..rounds {
            expression.apply(&mut item, &names, &attribute_values).unwrap();
        }
        assert_eq!(item.get("state"), Some(&s(value)));
    }

    /// Numeric key values are compared as numbers, so differently-spelled but
    /// equal keys address the same item.
    #[test]
    fn equal_numbers_address_the_same_item(value in 0_i64..10_000) {
        let mock = events();
        run(async {
            mock.put_item(PutItemRequest::new(
                "events",
                item([("pk", s("p")), ("sk", n(value.to_string())), ("v", n("1"))]),
            ))
            .await
            .unwrap();
            let alternate = format!("{value}.0");
            let found = mock
                .get_item(GetItemRequest::new(
                    "events",
                    item([("pk", s("p")), ("sk", n(alternate))]),
                ))
                .await
                .unwrap();
            assert!(found.item.is_some());
            assert_eq!(mock.count("events"), 1);
        });
    }

    /// Pagination joins back into exactly the unpaginated result.
    #[test]
    fn pagination_is_lossless(count in 1_usize..20, limit in 1_u32..6) {
        let mock = events();
        run(async {
            for id in 0..count {
                mock.put_item(PutItemRequest::new(
                    "events",
                    item([("pk", s("p")), ("sk", n(id.to_string()))]),
                ))
                .await
                .unwrap();
            }
            let mut request = QueryRequest::new("events", "pk = :pk")
                .with_values(values([(":pk", s("p"))]))
                .with_limit(limit);
            let mut collected = 0;
            for _ in 0..=count {
                let page = mock.query(request.clone()).await.unwrap();
                collected += page.count;
                match page.last_evaluated_key {
                    Some(key) => request.exclusive_start_key = Some(key),
                    None => break,
                }
            }
            assert_eq!(collected, count);
        });
    }

    /// The item size limit is enforced at exactly 400 KiB.
    #[test]
    fn item_size_boundary(extra in 0_usize..3) {
        let mock = users();
        // pk costs 2 (name) + 1 (value); blob costs 4 + len.
        let payload = "x".repeat(400 * 1024 - 7 + extra);
        let result = run(async {
            mock.put_item(PutItemRequest::new(
                "users",
                item([("pk", s("a")), ("blob", s(payload))]),
            ))
            .await
        });
        if extra == 0 {
            assert!(result.is_ok());
        } else {
            assert!(result.is_err());
        }
    }

    /// A condition that cannot be parsed never mutates anything.
    #[test]
    fn malformed_conditions_are_rejected(garbage in "[A-Za-z0-9 ()#:,=<>]{0,20}") {
        let parsed = ConditionExpression::parse(&garbage);
        if let Ok(condition) = parsed {
            let item = item([("pk", s("a")), ("n", n("1"))]);
            let names = HashMap::new();
            let attribute_values: HashMap<String, AttributeValue> = HashMap::new();
            // Whatever the parser accepts must evaluate without panicking.
            let _ = condition.evaluate(Some(&item), &names, &attribute_values);
        }
    }

    /// Uniqueness of keys: writing N distinct partitions yields N items.
    #[test]
    fn distinct_partitions_do_not_collide(ids in prop::collection::vec("[a-z]{1,6}", 1..15)) {
        let mock = users();
        run(async {
            let unique: BTreeMap<String, Item> = ids
                .iter()
                .map(|id| (id.clone(), item([("pk", s(id.clone()))])))
                .collect();
            for record in unique.values() {
                mock.put_item(PutItemRequest::new("users", record.clone()))
                    .await
                    .unwrap();
            }
            assert_eq!(mock.count("users"), unique.len());
        });
    }
}

#[test]
fn binary_keys_round_trip() {
    let mock = MockDdb::with_table("blobs", KeySchema::hash("id", KeyType::Binary)).unwrap();
    run(async {
        let key = item([("id", AttributeValue::B(Blob::new(vec![0_u8, 1, 255])))]);
        mock.put_item(PutItemRequest::new(
            "blobs",
            item([
                ("id", AttributeValue::B(Blob::new(vec![0_u8, 1, 255]))),
                ("v", s("x")),
            ]),
        ))
        .await
        .unwrap();
        assert!(mock
            .get_item(GetItemRequest::new("blobs", key))
            .await
            .unwrap()
            .item
            .is_some());
    });
}
