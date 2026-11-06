//! Queries and scans: ordering, filters, limits, cursors, indexes, segments.

mod common;

use std::collections::HashMap;

use common::orders;
use mock_ddb::{
    item, n, s, values, AttributeValue, CreateTableRequest, Error, GetItemRequest, Item, KeySchema,
    KeyType, MockDdb, Projection, PutItemRequest, QueryRequest, ScanRequest, SecondaryIndex,
    Select,
};

async fn collect(mock: &MockDdb, mut request: QueryRequest) -> Vec<Item> {
    let mut all = Vec::new();
    for _ in 0..100 {
        let page = mock.query(request.clone()).await.unwrap();
        all.extend(page.items);
        match page.last_evaluated_key {
            Some(key) => request.exclusive_start_key = Some(key),
            None => return all,
        }
    }
    panic!("query did not terminate");
}

async fn collect_scan(mock: &MockDdb, mut request: ScanRequest) -> Vec<Item> {
    let mut all = Vec::new();
    for _ in 0..100 {
        let page = mock.scan(request.clone()).await.unwrap();
        all.extend(page.items);
        match page.last_evaluated_key {
            Some(key) => request.exclusive_start_key = Some(key),
            None => return all,
        }
    }
    panic!("scan did not terminate");
}

#[tokio::test]
async fn query_orders_numerically_ascending_and_descending() {
    let mock = orders();
    // Insert out of order; "10" must sort after "2", which string ordering
    // would get wrong.
    for id in [10_u32, 2, 1, 11, 3] {
        mock.put_item(mock_ddb::PutItemRequest::new(
            "orders",
            common::order("c1", id, "1"),
        ))
        .await
        .unwrap();
    }

    let page = mock
        .query(
            QueryRequest::new("orders", "pk = :pk")
                .with_values(values([(":pk", s("customer#c1"))])),
        )
        .await
        .unwrap();
    let ids: Vec<String> = page
        .items
        .iter()
        .map(|item| item["sk"].as_n().unwrap().clone())
        .collect();
    assert_eq!(ids, vec!["1", "2", "3", "10", "11"]);
    assert_eq!(page.count, 5);
    assert_eq!(page.scanned_count, 5);
    assert!(page.last_evaluated_key.is_none());

    let descending = mock
        .query(
            QueryRequest::new("orders", "pk = :pk")
                .with_values(values([(":pk", s("customer#c1"))]))
                .with_scan_index_forward(false),
        )
        .await
        .unwrap();
    let ids: Vec<String> = descending
        .items
        .iter()
        .map(|item| item["sk"].as_n().unwrap().clone())
        .collect();
    assert_eq!(ids, vec!["11", "10", "3", "2", "1"]);
}

#[tokio::test]
async fn query_sort_key_conditions() {
    let mock = orders();
    for id in 1..=5 {
        mock.put_item(mock_ddb::PutItemRequest::new(
            "orders",
            common::order("c1", id, "1"),
        ))
        .await
        .unwrap();
    }

    let page = mock
        .query(
            QueryRequest::new("orders", "pk = :pk AND sk >= :low")
                .with_values(values([(":pk", s("customer#c1")), (":low", n("3"))])),
        )
        .await
        .unwrap();
    assert_eq!(page.count, 3);

    let between = mock
        .query(
            QueryRequest::new("orders", "pk = :pk AND sk BETWEEN :low AND :high").with_values(
                values([
                    (":pk", s("customer#c1")),
                    (":low", n("2")),
                    (":high", n("4")),
                ]),
            ),
        )
        .await
        .unwrap();
    assert_eq!(between.count, 3);

    let eq = mock
        .query(
            QueryRequest::new("orders", "pk = :pk AND sk = :only")
                .with_values(values([(":pk", s("customer#c1")), (":only", n("3"))])),
        )
        .await
        .unwrap();
    assert_eq!(eq.count, 1);
}

#[tokio::test]
async fn query_paginates_without_gaps_or_repeats() {
    let mock = orders();
    for id in 1..=25 {
        mock.put_item(mock_ddb::PutItemRequest::new(
            "orders",
            common::order("c1", id, "1"),
        ))
        .await
        .unwrap();
    }

    let all = collect(
        &mock,
        QueryRequest::new("orders", "pk = :pk")
            .with_values(values([(":pk", s("customer#c1"))]))
            .with_limit(7),
    )
    .await;
    assert_eq!(all.len(), 25);
    let ids: Vec<String> = all
        .iter()
        .map(|item| item["sk"].as_n().unwrap().clone())
        .collect();
    assert_eq!(
        ids,
        (1..=25).map(|id| id.to_string()).collect::<Vec<_>>(),
        "pages must join into the full ordered list"
    );
}

#[tokio::test]
async fn query_limit_counts_scanned_items_before_the_filter() {
    let mock = orders();
    for id in 1..=10 {
        let mut order = common::order("c1", id, "1");
        if id % 2 == 0 {
            order.insert("even".into(), s("yes"));
        }
        mock.put_item(mock_ddb::PutItemRequest::new("orders", order))
            .await
            .unwrap();
    }

    let page = mock
        .query(
            QueryRequest::new("orders", "pk = :pk")
                .with_values(values([(":pk", s("customer#c1"))]))
                .with_filter("attribute_exists(even)")
                .with_limit(5),
        )
        .await
        .unwrap();
    assert_eq!(page.scanned_count, 5);
    assert_eq!(page.count, 2, "only items 2 and 4 of the first five match");
    assert!(page.last_evaluated_key.is_some());
}

#[tokio::test]
async fn query_projection_and_count_select() {
    let mock = orders();
    mock.put_item(mock_ddb::PutItemRequest::new(
        "orders",
        item([
            ("pk", s("customer#c1")),
            ("sk", n("1")),
            ("total", n("10")),
            ("secret", s("x")),
        ]),
    ))
    .await
    .unwrap();

    let projected = mock
        .query(
            QueryRequest::new("orders", "pk = :pk")
                .with_values(values([(":pk", s("customer#c1"))]))
                .with_projection("sk, #t")
                .with_names(mock_ddb::names([("#t", "total")])),
        )
        .await
        .unwrap();
    assert_eq!(projected.items[0].len(), 2);
    assert!(!projected.items[0].contains_key("secret"));

    let counted = mock
        .query(
            QueryRequest::new("orders", "pk = :pk")
                .with_values(values([(":pk", s("customer#c1"))]))
                .with_select(Select::Count),
        )
        .await
        .unwrap();
    assert_eq!(counted.count, 1);
    assert_eq!(counted.items.len(), 0);
}

#[tokio::test]
async fn query_without_key_condition_is_rejected() {
    let mock = orders();
    mock.put_item(mock_ddb::PutItemRequest::new(
        "orders",
        common::order("c1", 1, "1"),
    ))
    .await
    .unwrap();
    mock.put_item(mock_ddb::PutItemRequest::new(
        "orders",
        common::order("c2", 1, "1"),
    ))
    .await
    .unwrap();

    // Outside: no condition at all is what the service rejects, so the mock has
    // to fail closed instead of reading the whole table.
    let missing = mock
        .query(QueryRequest {
            table_name: "orders".to_string(),
            ..QueryRequest::default()
        })
        .await
        .expect_err("a query needs a key condition");
    assert_eq!(missing.code(), "ValidationException");
    assert_eq!(
        missing.to_string(),
        "1 validation error detected: Value null at 'keyConditionExpression' failed to satisfy constraint: Member must not be null"
    );

    // Outside, the other side of the boundary: an empty string is present but is
    // not a condition, so it fails as a malformed expression instead.
    let empty = mock
        .query(QueryRequest::new("orders", ""))
        .await
        .expect_err("an empty key condition is not a condition");
    assert_eq!(empty.code(), "ValidationException");

    // Inside: the same table with a condition still reads that partition, so the
    // rejections above are the condition and not a broken query path.
    let page = mock
        .query(
            QueryRequest::new("orders", "pk = :pk")
                .with_values(values([(":pk", s("customer#c1"))])),
        )
        .await
        .unwrap();
    assert_eq!(page.count, 1);
}

#[tokio::test]
async fn key_conditions_are_validated() {
    let mock = orders();

    let non_key = mock
        .query(QueryRequest::new("orders", "total = :v").with_values(values([(":v", n("1"))])))
        .await;
    assert!(non_key.is_err());

    let no_partition = mock
        .query(QueryRequest::new("orders", "sk > :v").with_values(values([(":v", n("1"))])))
        .await;
    assert!(no_partition.is_err());

    let or_condition = mock
        .query(
            QueryRequest::new("orders", "pk = :a OR pk = :b")
                .with_values(values([(":a", s("a")), (":b", s("b"))])),
        )
        .await;
    assert!(or_condition.is_err());

    let nested = mock
        .query(QueryRequest::new("orders", "pk.nested = :v").with_values(values([(":v", s("a"))])))
        .await;
    assert!(nested.is_err());

    let unknown_index = mock
        .query(
            QueryRequest::new("orders", "pk = :v")
                .with_values(values([(":v", s("a"))]))
                .with_index("no-such-index"),
        )
        .await;
    assert!(unknown_index.is_err());

    let zero_limit = mock
        .query(
            QueryRequest::new("orders", "pk = :v")
                .with_values(values([(":v", s("a"))]))
                .with_limit(0),
        )
        .await;
    assert!(zero_limit.is_err());
}

async fn indexed_mock() -> MockDdb {
    let request = CreateTableRequest::new(
        "events",
        KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::String),
    )
    .with_global_index(
        SecondaryIndex::new(
            "by-state",
            KeySchema::hash_and_range("state", KeyType::String, "at", KeyType::Number),
        )
        .with_projection(Projection::KeysOnly),
    )
    .with_local_index(SecondaryIndex::new(
        "by-title",
        KeySchema::hash_and_range("pk", KeyType::String, "title", KeyType::String),
    ));
    let mock = MockDdb::new();
    mock.create_table(request).await.unwrap();
    mock
}

#[tokio::test]
async fn global_index_queries_use_the_index_order_and_projection() {
    let mock = indexed_mock().await;
    for (position, (state, at, payload)) in [
        ("open", 3, "c"),
        ("open", 1, "a"),
        ("open", 2, "b"),
        ("closed", 1, "d"),
    ]
    .into_iter()
    .enumerate()
    {
        mock.put_item(mock_ddb::PutItemRequest::new(
            "events",
            item([
                ("pk", s(format!("event#{position}"))),
                ("sk", s("detail")),
                ("state", s(state)),
                ("at", n(at.to_string())),
                ("payload", s(payload)),
            ]),
        ))
        .await
        .unwrap();
    }

    let page = mock
        .query(
            QueryRequest::new("events", "state = :state")
                .with_values(values([(":state", s("open"))]))
                .with_index("by-state"),
        )
        .await
        .unwrap();
    assert_eq!(page.count, 3);
    let positions: Vec<String> = page
        .items
        .iter()
        .map(|item| item["at"].as_n().unwrap().clone())
        .collect();
    assert_eq!(positions, vec!["1", "2", "3"]);
    // KEYS_ONLY projection: only the table key and index key attributes.
    assert!(!page.items[0].contains_key("payload"));

    let bounded = mock
        .query(
            QueryRequest::new("events", "state = :state AND at > :low")
                .with_values(values([(":state", s("open")), (":low", n("1"))]))
                .with_index("by-state"),
        )
        .await
        .unwrap();
    assert_eq!(bounded.count, 2);
}

#[tokio::test]
async fn sparse_index_omits_items_without_the_index_key() {
    let mock = indexed_mock().await;
    mock.put_item(mock_ddb::PutItemRequest::new(
        "events",
        item([("pk", s("e1")), ("sk", s("detail"))]),
    ))
    .await
    .unwrap();
    mock.put_item(mock_ddb::PutItemRequest::new(
        "events",
        item([
            ("pk", s("e2")),
            ("sk", s("detail")),
            ("state", s("open")),
            ("at", n("1")),
        ]),
    ))
    .await
    .unwrap();

    let page = mock
        .query(
            QueryRequest::new("events", "state = :state")
                .with_values(values([(":state", s("open"))]))
                .with_index("by-state"),
        )
        .await
        .unwrap();
    assert_eq!(page.count, 1);
    assert_eq!(page.items[0]["pk"], s("e2"));
}

#[tokio::test]
async fn local_index_queries_share_the_partition() {
    let mock = indexed_mock().await;
    for (pk, title) in [("c1", "b"), ("c1", "a"), ("c2", "a")] {
        mock.put_item(mock_ddb::PutItemRequest::new(
            "events",
            item([("pk", s(pk)), ("sk", s(title)), ("title", s(title))]),
        ))
        .await
        .unwrap();
    }

    let page = mock
        .query(
            QueryRequest::new("events", "pk = :pk")
                .with_values(values([(":pk", s("c1"))]))
                .with_index("by-title"),
        )
        .await
        .unwrap();
    assert_eq!(page.count, 2);
    assert_eq!(page.items[0]["title"], s("a"));
}

#[tokio::test]
async fn query_index_pagination_round_trips() {
    let mock = indexed_mock().await;
    for at in 1..=12 {
        mock.put_item(mock_ddb::PutItemRequest::new(
            "events",
            item([
                ("pk", s(format!("e{at}"))),
                ("sk", s("detail")),
                ("state", s("open")),
                ("at", n(at.to_string())),
            ]),
        ))
        .await
        .unwrap();
    }

    let first = mock
        .query(
            QueryRequest::new("events", "state = :state")
                .with_values(values([(":state", s("open"))]))
                .with_index("by-state")
                .with_limit(5),
        )
        .await
        .unwrap();
    assert_eq!(first.count, 5);
    let cursor = first.last_evaluated_key.expect("a cursor after five items");

    let second = mock
        .query(
            QueryRequest::new("events", "state = :state")
                .with_values(values([(":state", s("open"))]))
                .with_index("by-state")
                .with_exclusive_start_key(cursor),
        )
        .await
        .unwrap();
    assert_eq!(second.count, 7);
    assert_eq!(second.items[0]["at"], n("6"));
}

#[tokio::test]
async fn scan_filters_and_counts() {
    let mock = orders();
    for id in 1..=6 {
        let mut order = common::order("c1", id, "1");
        if id > 3 {
            order.insert("big".into(), s("yes"));
        }
        mock.put_item(mock_ddb::PutItemRequest::new("orders", order))
            .await
            .unwrap();
    }

    let page = mock
        .scan(
            ScanRequest::new("orders")
                .with_filter("attribute_exists(big)")
                .with_select(Select::Count),
        )
        .await
        .unwrap();
    assert_eq!(page.count, 3);
    assert_eq!(page.scanned_count, 6);
    assert_eq!(page.items.len(), 0);
}

#[tokio::test]
async fn scan_paginates_with_limit_and_cursor() {
    let mock = orders();
    for id in 1..=10 {
        mock.put_item(mock_ddb::PutItemRequest::new(
            "orders",
            common::order("c1", id, "1"),
        ))
        .await
        .unwrap();
    }

    let all = collect_scan(&mock, ScanRequest::new("orders").with_limit(3)).await;
    assert_eq!(all.len(), 10);
    let mut ids: Vec<String> = all
        .iter()
        .map(|item| item["sk"].as_n().unwrap().clone())
        .collect();
    ids.sort_by_key(|id| id.parse::<u32>().unwrap());
    assert_eq!(ids, (1..=10).map(|id| id.to_string()).collect::<Vec<_>>());
}

#[tokio::test]
async fn parallel_scan_segments_are_disjoint_and_complete() {
    let mock = orders();
    let total_orders = 40;
    for id in 1..=total_orders {
        mock.put_item(mock_ddb::PutItemRequest::new(
            "orders",
            common::order(&format!("c{}", id % 4), id, "1"),
        ))
        .await
        .unwrap();
    }

    let mut seen = std::collections::BTreeSet::new();
    for segment in 0..4 {
        let page = mock
            .scan(ScanRequest::new("orders").with_segment(segment, 4))
            .await
            .unwrap();
        for item in page.items {
            let key = format!(
                "{}-{}",
                item["pk"].as_s().unwrap(),
                item["sk"].as_n().unwrap()
            );
            assert!(seen.insert(key), "segments must not overlap");
        }
    }
    assert_eq!(seen.len(), total_orders as usize);

    assert!(mock
        .scan(ScanRequest::new("orders").with_segment(4, 4))
        .await
        .is_err());
    assert!(mock
        .scan(ScanRequest::new("orders").with_segment(0, 0))
        .await
        .is_err());
    assert!(mock
        .scan(ScanRequest {
            segment: Some(0),
            ..ScanRequest::new("orders")
        })
        .await
        .is_err());
    assert!(mock
        .scan(ScanRequest {
            total_segments: Some(2),
            ..ScanRequest::new("orders")
        })
        .await
        .is_err());
    assert!(mock
        .scan(ScanRequest::new("orders").with_limit(0))
        .await
        .is_err());
}

#[tokio::test]
async fn scan_of_an_index_applies_the_index_projection() {
    let mock = indexed_mock().await;
    mock.put_item(mock_ddb::PutItemRequest::new(
        "events",
        item([
            ("pk", s("e1")),
            ("sk", s("detail")),
            ("state", s("open")),
            ("at", n("1")),
            ("payload", s("hidden")),
        ]),
    ))
    .await
    .unwrap();

    let page = mock
        .scan(ScanRequest::new("events").with_index("by-state"))
        .await
        .unwrap();
    assert_eq!(page.count, 1);
    assert!(!page.items[0].contains_key("payload"));

    assert!(mock
        .scan(ScanRequest::new("events").with_index("nope"))
        .await
        .is_err());
    assert!(mock.scan(ScanRequest::new("nope")).await.is_err());
}

#[tokio::test]
async fn projection_selects_list_elements_through_every_read_path() {
    // `rows[0]` projects the element, not the containing list. Verified through
    // the mock's own transport surface (get, query, scan) so a test written
    // against the mock sees the shape the service returns.
    let mock = MockDdb::with_table("docs", KeySchema::hash("pk", KeyType::String)).unwrap();
    let rows = AttributeValue::L(vec![
        AttributeValue::M(HashMap::from([
            ("v".to_string(), s("first")),
            ("w".to_string(), s("1")),
        ])),
        AttributeValue::M(HashMap::from([("v".to_string(), s("second"))])),
    ]);
    let first = AttributeValue::L(vec![AttributeValue::M(HashMap::from([(
        "v".to_string(),
        s("first"),
    )]))]);
    let second = AttributeValue::L(vec![AttributeValue::M(HashMap::from([(
        "v".to_string(),
        s("second"),
    )]))]);

    mock.put_item(PutItemRequest::new(
        "docs",
        item([("pk", s("d#1")), ("rows", rows), ("secret", s("hidden"))]),
    ))
    .await
    .unwrap();

    let gotten = mock
        .get_item(
            GetItemRequest::new("docs", item([("pk", s("d#1"))])).with_projection("rows[0].v, pk"),
        )
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(gotten.get("rows"), Some(&first));
    assert!(gotten.contains_key("pk"));
    assert!(!gotten.contains_key("secret"));

    let queried = mock
        .query(
            QueryRequest::new("docs", "pk = :pk")
                .with_values(values([(":pk", s("d#1"))]))
                .with_projection("rows[0].v"),
        )
        .await
        .unwrap();
    assert_eq!(queried.items[0].get("rows"), Some(&first));

    let scanned = mock
        .scan(ScanRequest::new("docs").with_projection("rows[0].v"))
        .await
        .unwrap();
    assert_eq!(scanned.items[0].get("rows"), Some(&first));

    // Edge pair: index 1 is the last element (inside), index 2 is one past it.
    let last = mock
        .get_item(
            GetItemRequest::new("docs", item([("pk", s("d#1"))])).with_projection("rows[1].v"),
        )
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(last.get("rows"), Some(&second));

    let past = mock
        .get_item(
            GetItemRequest::new("docs", item([("pk", s("d#1"))])).with_projection("rows[2].v"),
        )
        .await
        .unwrap()
        .item
        .unwrap();
    assert!(!past.contains_key("rows"));
    assert!(!past.contains_key("secret"));
}

#[tokio::test]
async fn an_empty_table_still_validates_the_filter() {
    // The filter used to be parsed inside the per-item loop, so a page that
    // reached no item returned `Ok` where the service raises Validation.
    let mock = orders();

    let malformed = mock
        .scan(ScanRequest::new("orders").with_filter("total >"))
        .await
        .unwrap_err();
    assert!(matches!(malformed, Error::Validation { .. }), "{malformed}");

    let undefined = mock
        .scan(ScanRequest::new("orders").with_filter("total > :floor"))
        .await
        .unwrap_err();
    assert!(undefined.to_string().contains(":floor"), "{undefined}");

    // Edge pair: the same filter with its placeholder defined is valid, and an
    // empty page is not an error.
    let page = mock
        .scan(
            ScanRequest::new("orders")
                .with_filter("total > :floor")
                .with_values(values([(":floor", n("0"))])),
        )
        .await
        .unwrap();
    assert_eq!(page.count, 0);
    assert_eq!(page.scanned_count, 0);
}

#[tokio::test]
async fn a_filter_is_validated_where_the_key_condition_matches_nothing() {
    let mock = orders();
    mock.put_item(mock_ddb::PutItemRequest::new(
        "orders",
        common::order("c1", 1, "1"),
    ))
    .await
    .unwrap();

    let error = mock
        .query(
            QueryRequest::new("orders", "pk = :other")
                .with_values(values([(":other", s("customer#c2"))]))
                .with_filter("total > :missing"),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains(":missing"), "{error}");
}

#[tokio::test]
async fn a_projection_is_validated_before_any_item_is_read() {
    let mock = orders();

    let malformed = mock
        .scan(ScanRequest::new("orders").with_projection(""))
        .await
        .unwrap_err();
    assert!(matches!(malformed, Error::Validation { .. }), "{malformed}");

    let undefined = mock
        .query(
            QueryRequest::new("orders", "pk = :pk")
                .with_values(values([(":pk", s("customer#c1"))]))
                .with_projection("#missing"),
        )
        .await
        .unwrap_err();
    assert!(undefined.to_string().contains("#missing"), "{undefined}");

    // Edge pair: a defined alias on an empty table is a valid, empty page.
    let page = mock
        .query(
            QueryRequest::new("orders", "pk = :pk")
                .with_values(values([(":pk", s("customer#c1"))]))
                .with_projection("#total")
                .with_names(mock_ddb::names([("#total", "total")])),
        )
        .await
        .unwrap();
    assert_eq!(page.count, 0);
    assert_eq!(page.items.len(), 0);
}

#[tokio::test]
async fn an_undefined_key_condition_value_is_rejected_on_an_empty_table() {
    // The key condition is evaluated per candidate item too, so the same
    // fail-open shape applied to it before the maps were checked up front.
    let mock = orders();
    let error = mock
        .query(QueryRequest::new("orders", "pk = :pk"))
        .await
        .unwrap_err();
    assert!(error.to_string().contains(":pk"), "{error}");
}

#[tokio::test]
async fn get_item_validates_the_projection_before_looking_up_the_key() {
    let mock = MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap();
    mock.put_item(mock_ddb::PutItemRequest::new(
        "users",
        common::user("u#1", "a@example.com"),
    ))
    .await
    .unwrap();

    let absent_key = item([("pk", s("u#2"))]);
    let undefined = mock
        .get_item(GetItemRequest::new("users", absent_key.clone()).with_projection("#missing"))
        .await
        .unwrap_err();
    assert!(undefined.to_string().contains("#missing"), "{undefined}");

    let malformed = mock
        .get_item(GetItemRequest::new("users", absent_key.clone()).with_projection(""))
        .await
        .unwrap_err();
    assert!(matches!(malformed, Error::Validation { .. }), "{malformed}");

    // Edge pair: a valid projection on a missing key stays a `None` item...
    let absent = mock
        .get_item(GetItemRequest::new("users", absent_key).with_projection("email"))
        .await
        .unwrap();
    assert!(absent.item.is_none());

    // ...and a found item is still projected.
    let found = mock
        .get_item(GetItemRequest::new("users", item([("pk", s("u#1"))])).with_projection("email"))
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(found.len(), 1);
    assert!(found.contains_key("email"));
}
