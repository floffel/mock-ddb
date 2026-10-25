//! Queries and scans: ordering, filters, limits, cursors, indexes, segments.

mod common;

use common::orders;
use mock_ddb::{
    item, n, s, values, CreateTableRequest, Item, KeySchema, KeyType, MockDdb, Projection,
    QueryRequest, ScanRequest, SecondaryIndex, Select,
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
async fn query_without_key_condition_returns_everything() {
    // Documented divergence: the service requires a key condition.
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

    let page = mock
        .query(QueryRequest {
            table_name: "orders".to_string(),
            ..QueryRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(page.count, 2);
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
