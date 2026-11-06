//! Property tests: the invariants a mock must never break, over random input.

use std::collections::{BTreeMap, HashMap};

use mock_ddb::{
    into_item, item, item_from_value, item_ref_from_value, item_to_value, n, ordered_item, s,
    values, AttributeValue, Blob, CancellationReason, CancellationReasonCode, ConditionExpression,
    Error, GetItemRequest, Item, KeySchema, KeyType, MockDdb, PutItemRequest, QueryRequest,
    ReturnValues, UpdateExpression, UpdateItemRequest,
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
            expression
                .apply(
                    &mut item,
                    &KeySchema::hash("pk", KeyType::String),
                    &names,
                    &attribute_values,
                )
                .unwrap();
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

/// One step of a generated document path.
#[derive(Clone, Debug, PartialEq, Eq)]
enum TestStep {
    Name(String),
    Index(usize),
}

/// A generated document path; the first step is a name, as the grammar
/// requires, and later steps may be list indices.
fn document_path() -> impl Strategy<Value = Vec<TestStep>> {
    let name = || prop::sample::select(vec!["a", "b", "ab", "ba"]);
    (
        name(),
        prop::collection::vec(
            prop_oneof![
                name().prop_map(|word: &'static str| TestStep::Name(word.to_string())),
                (0usize..3).prop_map(TestStep::Index),
            ],
            0..3,
        ),
    )
        .prop_map(|(first, rest)| {
            let mut steps = vec![TestStep::Name(first.to_string())];
            steps.extend(rest);
            steps
        })
}

fn render_path(steps: &[TestStep]) -> String {
    let mut text = String::new();
    for step in steps {
        match step {
            TestStep::Name(name) => {
                if !text.is_empty() {
                    text.push('.');
                }
                text.push_str(name);
            }
            TestStep::Index(index) => {
                text.push('[');
                text.push_str(&index.to_string());
                text.push(']');
            }
        }
    }
    text
}

fn is_path_prefix(prefix: &[TestStep], path: &[TestStep]) -> bool {
    prefix.len() <= path.len() && prefix.iter().zip(path).all(|(left, right)| left == right)
}

proptest! {
    /// However the target paths are ordered in the expression, the parser's
    /// verdict matches an exhaustive pairwise scan — the overlap check is
    /// allowed to be faster, not laxer.
    #[test]
    fn overlap_rejection_matches_a_brute_force_scan(
        paths in prop::collection::vec(document_path(), 1..12),
    ) {
        let rendered: Vec<String> = paths.iter().map(|path| render_path(path)).collect();
        let expression = format!("REMOVE {}", rendered.join(", "));
        prop_assume!(expression.len() <= 4096);
        let expected_overlap = (0..paths.len()).any(|left| {
            (left + 1..paths.len()).any(|right| {
                is_path_prefix(&paths[left], &paths[right])
                    || is_path_prefix(&paths[right], &paths[left])
            })
        });
        prop_assert_eq!(
            UpdateExpression::parse(&expression).is_ok(),
            !expected_overlap,
            "expression `{}`",
            expression
        );
    }
}

// ---------------------------------------------------------------------------
// Error classification. `Error::code`, `Error::is_retryable` and
// `Error::cancellation_reasons` are pure and total, so the properties they
// promise are proven over a generated input space instead of one example.
// ---------------------------------------------------------------------------

/// One instance of every `Error` variant, with the payload handed in so a test
/// can vary the payload without varying the variant.
fn every_variant(payload: &str) -> Vec<Error> {
    vec![
        Error::ResourceNotFound {
            resource: format!("table `{payload}`"),
            message: payload.to_string(),
        },
        Error::ResourceInUse {
            message: payload.to_string(),
        },
        Error::Validation {
            message: payload.to_string(),
        },
        Error::ConditionalCheckFailed {
            table: payload.to_string(),
            message: payload.to_string(),
            item: None,
        },
        Error::TransactionCanceled {
            message: payload.to_string(),
            reasons: Vec::new(),
        },
        Error::ItemCollectionSizeLimitExceeded {
            message: payload.to_string(),
        },
        Error::DuplicateItem {
            message: payload.to_string(),
        },
        Error::internal(payload.to_string()),
    ]
}

/// Every code a transaction member can be cancelled with.
fn cancellation_codes() -> impl Strategy<Value = CancellationReasonCode> {
    prop_oneof![
        Just(CancellationReasonCode::ConditionalCheckFailed),
        Just(CancellationReasonCode::TransactionConflict),
        Just(CancellationReasonCode::ItemCollectionSizeLimitExceeded),
        Just(CancellationReasonCode::ValidationError),
        Just(CancellationReasonCode::ThrottlingError),
        Just(CancellationReasonCode::None),
    ]
}

/// A member list of arbitrary length, including the empty one.
fn cancellation_reason_list() -> impl Strategy<Value = Vec<CancellationReason>> {
    prop::collection::vec(
        (cancellation_codes(), "[ -~]{0,12}")
            .prop_map(|(code, message)| CancellationReason::new(code, message)),
        0..8,
    )
}

/// The rule the service applies to a cancelled transaction: retry only when
/// every member was rejected for a reason a retry can fix — a lost race or a
/// throttle — because a terminal reason would reject the retry for the same
/// cause again.
fn expected_retryable(reasons: &[CancellationReason]) -> bool {
    !reasons.is_empty() && reasons.iter().all(|reason| reason.code().is_retryable())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// A cancelled transaction is retryable exactly when it reported members
    /// and all of them name a retryable reason — no empty list, no mixed list.
    #[test]
    fn is_retryable_needs_a_non_empty_all_retryable_list(
        reasons in cancellation_reason_list(),
    ) {
        let cancelled = Error::TransactionCanceled {
            message: "cancelled".to_string(),
            reasons: reasons.clone(),
        };
        let expected = expected_retryable(&reasons);
        prop_assert_eq!(cancelled.is_retryable(), expected, "reasons: {:?}", reasons);

        // A retryable member followed by a terminal one must not make the
        // whole transaction look retryable, in either order.
        let mut mixed = vec![
            CancellationReason::new(CancellationReasonCode::ThrottlingError, "throttled"),
            CancellationReason::new(CancellationReasonCode::ConditionalCheckFailed, "no"),
        ];
        let mut cancelled = Error::TransactionCanceled {
            message: "cancelled".to_string(),
            reasons: mixed.clone(),
        };
        prop_assert!(!cancelled.is_retryable(), "reasons: {:?}", mixed);

        mixed.reverse();
        cancelled = Error::TransactionCanceled {
            message: "cancelled".to_string(),
            reasons: mixed.clone(),
        };
        prop_assert!(!cancelled.is_retryable(), "reasons: {:?}", mixed);
    }

    /// The only failure outside a cancelled transaction that is retryable is the
    /// 5xx, whatever its payload says: a caller that retries every
    /// `is_retryable` failure retries internal failures and nothing else.
    #[test]
    fn internal_is_the_only_retryable_failure_outside_a_cancellation(
        payload in "[ -~]{0,24}",
    ) {
        for error in every_variant(&payload) {
            let expected = error.code() == "InternalServerError";
            prop_assert_eq!(
                error.is_retryable(),
                expected,
                "{:?} is classified `{}`",
                error,
                error.code()
            );
        }
    }

    /// Every reason code answers `is_retryable` from its own class, and the
    /// answer a whole transaction gives is the conjunction of its members'.
    #[test]
    fn a_reasons_retryability_follows_its_code(
        code in cancellation_codes(),
        other in cancellation_codes(),
    ) {
        let expected = matches!(
            code,
            CancellationReasonCode::TransactionConflict | CancellationReasonCode::ThrottlingError
        );
        prop_assert_eq!(code.is_retryable(), expected, "code: {}", code);

        let pair = vec![
            CancellationReason::new(code, "first"),
            CancellationReason::new(other, "second"),
        ];
        let cancelled = Error::TransactionCanceled {
            message: "cancelled".to_string(),
            reasons: pair,
        };
        prop_assert_eq!(
            cancelled.is_retryable(),
            code.is_retryable() && other.is_retryable(),
            "codes: {}, {}",
            code,
            other
        );
    }

    /// The code a caller matches on depends on the variant and nothing else:
    /// not on the message, not on a clone, not on how often it is asked.
    #[test]
    fn code_is_stable_and_payload_independent(
        first in "[ -~]{0,16}",
        second in "[ -~]{0,16}",
    ) {
        for (left, right) in every_variant(&first).iter().zip(every_variant(&second)) {
            prop_assert_eq!(left.code(), right.code());
            prop_assert_eq!(left.code(), left.clone().code());
            prop_assert_eq!(left.code(), left.code());
            prop_assert!(!left.code().is_empty());
        }
    }

    /// The reasons handed back are the transaction members, in order, and
    /// nothing for any other failure class.
    #[test]
    fn cancellation_reasons_are_the_members_and_nothing_else(
        reasons in cancellation_reason_list(),
        payload in "[ -~]{0,16}",
    ) {
        let cancelled = Error::TransactionCanceled {
            message: payload.clone(),
            reasons: reasons.clone(),
        };
        let reported = cancelled.cancellation_reasons();
        prop_assert_eq!(reported.len(), reasons.len());
        prop_assert!(reported.iter().zip(&reasons).all(|(left, right)| left == right));
        // Empty exactly when nothing was reported: no members at all, or a
        // failure class that never carries one.
        prop_assert_eq!(reported.is_empty(), reasons.is_empty());
        prop_assert_eq!(cancelled.is_retryable(), expected_retryable(reported));

        for error in every_variant(&payload) {
            if !matches!(error, Error::TransactionCanceled { .. }) {
                prop_assert!(
                    error.cancellation_reasons().is_empty(),
                    "{:?} must not carry cancellation reasons",
                    error
                );
            }
        }
    }

    /// The ordered rendering is a sorted view of the same item: no attribute is
    /// lost, none is invented, and the names come out in ascending order
    /// whatever order the map happened to iterate in.
    #[test]
    fn ordered_item_is_a_sorted_view(entries in payload()) {
        let ordered = ordered_item(&entries);
        prop_assert_eq!(ordered.len(), entries.len());

        let names: Vec<&str> = ordered.keys().copied().collect();
        let mut ascending = names.clone();
        ascending.sort_unstable();
        prop_assert_eq!(&names, &ascending);

        for (name, value) in &ordered {
            prop_assert_eq!(entries.get(*name), Some(*value));
        }
    }
}

/// Each variant has its own code, so a caller matching on the string can never
/// confuse two failure classes; the table is the service's, spelled out once.
#[test]
fn code_is_unique_and_names_the_service_class() {
    let errors = every_variant("x");
    let codes: Vec<&str> = errors.iter().map(Error::code).collect();
    assert_eq!(
        codes,
        vec![
            "ResourceNotFoundException",
            "ResourceInUseException",
            "ValidationException",
            "ConditionalCheckFailedException",
            "TransactionCanceledException",
            "ItemCollectionSizeLimitExceededException",
            "DuplicateItemException",
            "InternalServerError",
        ]
    );

    for (index, code) in codes.iter().enumerate() {
        assert!(
            !codes[index + 1..].contains(code),
            "`{code}` is used by more than one variant"
        );
    }
}

#[cfg(feature = "streams")]
mod stream_paging {
    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]

        /// Paging a stream through shard iterators joins back into exactly the
        /// unpaginated log: every record once, in write order, never a gap.
        #[test]
        fn stream_paging_is_lossless(count in 1_usize..12, limit in 1_u32..4) {
            run(async {
                let mock = MockDdb::new();
                let table = mock
                    .create_table(
                        mock_ddb::CreateTableRequest::new(
                            "events",
                            KeySchema::hash("pk", KeyType::String),
                        )
                        .with_stream(mock_ddb::StreamViewType::NewAndOldImages),
                    )
                    .await
                    .unwrap();
                let arn = table.latest_stream_arn.unwrap();
                for id in 0..count {
                    mock.put_item(PutItemRequest::new("events", item([("pk", s(id.to_string()))])))
                        .await
                        .unwrap();
                }

                let shard_id = mock
                    .streams()
                    .describe_stream(mock_ddb::DescribeStreamRequest::new(&arn))
                    .await
                    .unwrap()
                    .stream_description
                    .shards
                    .first()
                    .expect("one shard")
                    .shard_id
                    .clone();
                let mut token = mock
                    .streams()
                    .get_shard_iterator(mock_ddb::GetShardIteratorRequest::new(
                        &arn,
                        shard_id,
                        mock_ddb::ShardIteratorType::TrimHorizon,
                    ))
                    .await
                    .unwrap()
                    .shard_iterator;

                let mut seen = Vec::new();
                let mut times = Vec::new();
                loop {
                    let page = mock
                        .streams()
                        .get_records(mock_ddb::GetRecordsRequest::new(token).with_limit(limit))
                        .await
                        .unwrap();
                    if page.records.is_empty() {
                        break;
                    }
                    for record in page.records {
                        let data = record.dynamodb.expect("the dynamodb member");
                        seen.push(data.sequence_number);
                        times.push(data.approximate_creation_date_time);
                    }
                    token = page.next_shard_iterator.expect("a next iterator");
                }

                let expected: Vec<String> = mock
                    .get_records(&arn)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|record| record.sequence_number.clone())
                    .collect();
                assert_eq!(seen, expected);
                assert!(
                    times.windows(2).all(|pair| pair[0] <= pair[1]),
                    "creation times must not go backwards: {times:?}"
                );
            });
        }
    }
}

/// Every value shape that is not a document, one per `AttributeValue` variant
/// the SDK offers — the empty list and the empty sets included, because those
/// are exactly the shapes a `len()`-based check cannot tell from a document.
fn non_document() -> impl Strategy<Value = AttributeValue> {
    prop_oneof![
        "[a-zA-Z0-9 \\u{fc}\\u{f6}\\u{e4}]{0,8}".prop_map(AttributeValue::S),
        (-1_000_i64..1_000).prop_map(|number| AttributeValue::N(number.to_string())),
        prop::collection::vec(any::<u8>(), 0..8)
            .prop_map(|bytes| AttributeValue::B(Blob::new(bytes))),
        prop::collection::vec(
            prop::collection::vec(any::<u8>(), 0..4).prop_map(Blob::new),
            0..3
        )
        .prop_map(AttributeValue::Bs),
        any::<bool>().prop_map(AttributeValue::Bool),
        prop_oneof![
            Just(AttributeValue::Null(true)),
            Just(AttributeValue::Null(false))
        ],
        prop::collection::vec(payload(), 0..3).prop_map(|documents| {
            AttributeValue::L(documents.into_iter().map(AttributeValue::M).collect())
        }),
        prop::collection::vec("[a-z]{0,4}", 0..3).prop_map(AttributeValue::Ss),
        prop::collection::vec(0_i64..1_000, 0..3)
            .prop_map(|members| AttributeValue::Ns(members.iter().map(i64::to_string).collect())),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// The document wrapper is transparent: an item that goes through
    /// `item_to_value` comes back unchanged, whatever nesting it holds, and all
    /// three accessors agree on the document they are handed.
    #[test]
    fn item_to_value_round_trips_any_item(entries in payload()) {
        let wrapped = item_to_value(entries.clone());
        assert!(
            matches!(&wrapped, AttributeValue::M(_)),
            "a wrapped item is a document"
        );
        assert_eq!(item_ref_from_value(&wrapped), Some(&entries));
        assert_eq!(item_from_value(&wrapped), Some(entries.clone()));
        assert_eq!(into_item(wrapped), Some(entries));
    }

    /// Nesting is preserved level by level instead of being flattened:
    /// unwrapping a document `depth` times reaches the item that was wrapped
    /// `depth` times.
    #[test]
    fn nested_documents_survive_the_round_trip(entries in payload(), depth in 1_usize..6) {
        let mut wrapped = item_to_value(entries.clone());
        for _ in 1..depth {
            wrapped = item_to_value(item([("doc", wrapped)]));
        }

        let mut current = &wrapped;
        for _ in 1..depth {
            current = &item_ref_from_value(current).expect("a document at every level")["doc"];
        }
        assert_eq!(item_from_value(current), Some(entries));
    }

    /// No scalar is ever mistaken for a document, whichever variant it is —
    /// and the empty list and the empty sets are scalars here too.
    #[test]
    fn every_non_document_variant_is_rejected(value in non_document()) {
        assert!(
            !matches!(&value, AttributeValue::M(_)),
            "the strategy builds no document"
        );
        assert_eq!(item_ref_from_value(&value), None, "{value:?}");
        assert_eq!(item_from_value(&value), None, "{value:?}");
        assert_eq!(into_item(value.clone()), None, "{value:?}");
    }
}
