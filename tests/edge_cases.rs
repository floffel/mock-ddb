//! Boundary values: unicode, sizes, precision, empty values, deep nesting.

mod common;

use mock_ddb::{
    item, n, names, s, values, AttributeValue, BatchGetItemRequest, Blob, CancellationReason,
    CancellationReasonCode, Clock, ConditionExpression, CreateTableRequest, DeleteItemRequest,
    Error, GetItemRequest, KeySchema, KeyType, KeysAndAttributes, ListTablesRequest, MockClock,
    MockDdb, PutItemRequest, QueryRequest, ReturnValues, ScanRequest, Select, TransactGet,
    TransactGetItemsRequest, TransactWriteItem, TransactWriteItemsRequest, UpdateItemRequest,
};

fn numbers() -> MockDdb {
    MockDdb::with_table(
        "numbers",
        KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::Number),
    )
    .unwrap()
}

fn users() -> MockDdb {
    MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap()
}

fn assert_unused_names(error: &Error, alias: &str) {
    assert_eq!(error.code(), "ValidationException");
    assert!(
        error.to_string().contains(&format!(
            "ExpressionAttributeNames unused in expressions: keys: {{{alias}}}"
        )),
        "unexpected error: {error}"
    );
}

fn assert_unused_values(error: &Error, placeholder: &str) {
    assert_eq!(error.code(), "ValidationException");
    assert!(
        error.to_string().contains(&format!(
            "ExpressionAttributeValues unused in expressions: keys: {{{placeholder}}}"
        )),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn unused_expression_entries_are_rejected_on_every_single_expression_operation() {
    let mock = users();
    mock.put_item(PutItemRequest::new(
        "users",
        item([("pk", s("u#1")), ("email", s("a@example.com"))]),
    ))
    .await
    .unwrap();

    let put = mock
        .put_item(
            PutItemRequest::new("users", item([("pk", s("u#2"))]))
                .with_condition("attribute_not_exists(pk)")
                .with_names(names([("#stale", "email")]))
                .with_values(values([(":stale", s("x"))])),
        )
        .await
        .unwrap_err();
    assert_unused_names(&put, "#stale");

    let put_value = mock
        .put_item(
            PutItemRequest::new("users", item([("pk", s("u#2"))]))
                .with_condition("attribute_not_exists(pk)")
                .with_values(values([(":stale", s("x"))])),
        )
        .await
        .unwrap_err();
    assert_unused_values(&put_value, ":stale");

    let delete = mock
        .delete_item(
            DeleteItemRequest::new("users", item([("pk", s("u#1"))]))
                .with_condition("attribute_exists(pk)")
                .with_names(names([("#stale", "email")])),
        )
        .await
        .unwrap_err();
    assert_unused_names(&delete, "#stale");

    let update = mock
        .update_item(
            UpdateItemRequest::new("users", item([("pk", s("u#1"))]), "SET #email = :email")
                .with_condition("attribute_exists(pk)")
                .with_names(names([("#email", "email"), ("#stale", "other")]))
                .with_values(values([(":email", s("b@example.com"))])),
        )
        .await
        .unwrap_err();
    assert_unused_names(&update, "#stale");

    let get = mock
        .get_item(
            GetItemRequest::new("users", item([("pk", s("u#1"))]))
                .with_projection("pk")
                .with_names(names([("#stale", "email")])),
        )
        .await
        .unwrap_err();
    assert_unused_names(&get, "#stale");
}

#[tokio::test]
async fn unused_expression_entries_are_rejected_across_shared_read_and_write_maps() {
    let mock = common::orders();
    mock.seed("orders", vec![common::order("c1", 1, "10")])
        .unwrap();

    let query = mock
        .query(
            QueryRequest::new("orders", "#p = :pk")
                .with_filter("#total > :floor")
                .with_projection("#sk")
                .with_names(names([
                    ("#p", "pk"),
                    ("#total", "total"),
                    ("#sk", "sk"),
                    ("#stale", "unused"),
                ]))
                .with_values(values([(":pk", s("customer#c1")), (":floor", n("0"))])),
        )
        .await
        .unwrap_err();
    assert_unused_names(&query, "#stale");

    let query_value = mock
        .query(
            QueryRequest::new("orders", "pk = :pk")
                .with_filter("total > :floor")
                .with_values(values([
                    (":pk", s("customer#c1")),
                    (":floor", n("0")),
                    (":stale", n("1")),
                ])),
        )
        .await
        .unwrap_err();
    assert_unused_values(&query_value, ":stale");

    let scan = mock
        .scan(
            ScanRequest::new("orders")
                .with_filter("#total > :floor")
                .with_projection("#sk")
                .with_names(names([
                    ("#total", "total"),
                    ("#sk", "sk"),
                    ("#stale", "unused"),
                ]))
                .with_values(values([(":floor", n("0"))])),
        )
        .await
        .unwrap_err();
    assert_unused_names(&scan, "#stale");

    let update = mock
        .update_item(
            UpdateItemRequest::new("orders", common::order_key("c1", 1), "SET #total = :new")
                .with_condition("#total = :old")
                .with_names(names([("#total", "total"), ("#stale", "unused")]))
                .with_values(values([(":new", n("11")), (":old", n("10"))])),
        )
        .await
        .unwrap_err();
    assert_unused_names(&update, "#stale");
    assert_eq!(
        mock.get_item(GetItemRequest::new("orders", common::order_key("c1", 1)))
            .await
            .unwrap()
            .item
            .unwrap()["total"],
        n("10")
    );
}

#[tokio::test]
async fn unused_expression_entries_are_rejected_in_batches_and_transactions() {
    let mock = users();
    mock.seed("users", vec![common::user("u#1", "a@example.com")])
        .unwrap();

    let batch_get = mock
        .batch_get_item(
            BatchGetItemRequest::new()
                .add(
                    "users",
                    KeysAndAttributes {
                        keys: vec![common::user_key("u#1")],
                        projection_expression: Some("pk".to_string()),
                        expression_attribute_names: Some(names([("#stale", "email")])),
                        ..KeysAndAttributes::default()
                    },
                )
                .unwrap(),
        )
        .await
        .unwrap_err();
    assert_unused_names(&batch_get, "#stale");

    let transaction = mock
        .transact_write_items(TransactWriteItemsRequest::new(vec![
            TransactWriteItem::update("users", common::user_key("u#1"), "SET #email = :new")
                .with_condition("#email = :old")
                .with_names(names([("#email", "email"), ("#stale", "unused")]))
                .with_values(values([
                    (":new", s("b@example.com")),
                    (":old", s("a@example.com")),
                ])),
        ]))
        .await
        .unwrap_err();
    assert_unused_names(&transaction, "#stale");

    let transaction_put = mock
        .transact_write_items(TransactWriteItemsRequest::new(vec![
            TransactWriteItem::put("users", item([("pk", s("u#2"))]))
                .with_condition("attribute_not_exists(pk)")
                .with_values(values([(":stale", s("x"))])),
        ]))
        .await
        .unwrap_err();
    assert_unused_values(&transaction_put, ":stale");

    let transaction_delete = mock
        .transact_write_items(TransactWriteItemsRequest::new(vec![
            TransactWriteItem::delete("users", common::user_key("u#1"))
                .with_condition("attribute_exists(pk)")
                .with_names(names([("#stale", "unused")])),
        ]))
        .await
        .unwrap_err();
    assert_unused_names(&transaction_delete, "#stale");

    let condition_check = mock
        .transact_write_items(TransactWriteItemsRequest::new(vec![
            TransactWriteItem::condition_check("users", common::user_key("u#1"), "email = :old")
                .with_values(values([(":old", s("a@example.com")), (":stale", s("x"))])),
        ]))
        .await
        .unwrap_err();
    assert_unused_values(&condition_check, ":stale");

    let transact_get = mock
        .transact_get_items(TransactGetItemsRequest::new(vec![TransactGet {
            projection_expression: Some("pk".to_string()),
            expression_attribute_names: Some(names([("#stale", "email")])),
            ..TransactGet::new("users", common::user_key("missing"))
        }]))
        .await
        .unwrap_err();
    assert_unused_names(&transact_get, "#stale");
}

#[tokio::test]
async fn unused_expression_entries_fail_before_a_false_condition_is_evaluated() {
    let mock = users();
    mock.seed("users", vec![common::user("u#1", "a@example.com")])
        .unwrap();

    let error = mock
        .update_item(
            UpdateItemRequest::new("users", common::user_key("u#1"), "SET email = :new")
                .with_condition("attribute_not_exists(pk)")
                .with_values(values([(":new", s("b@example.com")), (":stale", s("x"))])),
        )
        .await
        .unwrap_err();
    assert_unused_values(&error, ":stale");
    assert_eq!(
        mock.get_item(GetItemRequest::new("users", common::user_key("u#1")))
            .await
            .unwrap()
            .item
            .unwrap()["email"],
        s("a@example.com")
    );
}

#[tokio::test]
async fn unicode_keys_sort_by_byte_order() {
    let mock = users();
    for key in ["ä", "b", "a", "Z", "ü"] {
        mock.put_item(PutItemRequest::new("users", item([("pk", s(key))])))
            .await
            .unwrap();
    }
    let keys: Vec<String> = mock
        .dump("users")
        .iter()
        .map(|item| item["pk"].as_s().unwrap().clone())
        .collect();
    // UTF-8 byte order: ASCII before multi-byte.
    assert_eq!(keys, vec!["Z", "a", "b", "ä", "ü"]);

    let found = mock
        .get_item(GetItemRequest::new("users", item([("pk", s("ü"))])))
        .await
        .unwrap();
    assert!(found.item.is_some());
}

#[tokio::test]
async fn unicode_conditions_and_placeholders_work() {
    let mock = users();
    mock.put_item(PutItemRequest::new(
        "users",
        item([("pk", s("u#1")), ("name", s("Jürgen Müller"))]),
    ))
    .await
    .unwrap();

    let page = mock
        .scan(
            ScanRequest::new("users")
                .with_filter("contains(name, :needle)")
                .with_values(values([(":needle", s("Müller"))])),
        )
        .await
        .unwrap();
    assert_eq!(page.count, 1);

    let alias = mock
        .scan(
            ScanRequest::new("users")
                .with_filter("#größe_ok = :yes")
                .with_names(mock_ddb::names([("#größe_ok", "name")]))
                .with_values(values([(":yes", s("Jürgen Müller"))])),
        )
        .await
        .unwrap();
    assert_eq!(alias.count, 1);
}

#[tokio::test]
async fn item_size_limit_is_exactly_400_kib() {
    let mock = users();
    // pk: name 2 + value 1 = 3 bytes; blob: name 4 + value len.
    let fitting = "x".repeat(400 * 1024 - 7);
    mock.put_item(PutItemRequest::new(
        "users",
        item([("pk", s("a")), ("blob", s(&fitting))]),
    ))
    .await
    .expect("400 KiB exactly is allowed");

    let too_large = "x".repeat(400 * 1024 - 6);
    let error = mock
        .put_item(PutItemRequest::new(
            "users",
            item([("pk", s("b")), ("blob", s(too_large))]),
        ))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("maximum allowed size"));
}

#[tokio::test]
async fn an_update_that_would_pass_the_item_limit_is_refused_whole() {
    let mock = users();
    // pk 3 bytes, list: name 3 + 2 elements of MAX/2 - 16 bytes each.
    let element = "x".repeat(400 * 1024 / 2 - 16);
    let list = AttributeValue::L(vec![s(&element), s(&element)]);
    mock.put_item(PutItemRequest::new(
        "users",
        item([("pk", s("a")), ("l", list.clone())]),
    ))
    .await
    .expect("the list itself is inside the limit");

    let error = mock
        .update_item(
            UpdateItemRequest::new("users", item([("pk", s("a"))]), "SET l = list_append(l, l)")
                .with_return_values(ReturnValues::AllOld),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "ValidationException");
    assert!(error.to_string().contains("maximum allowed size"));

    // The stored item is exactly the one that was put: no half-applied growth.
    let stored = mock
        .get_item(GetItemRequest::new("users", item([("pk", s("a"))])))
        .await
        .unwrap();
    assert_eq!(
        stored.item.and_then(|item| item.get("l").cloned()),
        Some(list)
    );

    // Just inside: the same update on a list that doubles to the limit lands.
    let half = "x".repeat(400 * 1024 / 2 - 16);
    let small = AttributeValue::L(vec![s(&half)]);
    mock.put_item(PutItemRequest::new(
        "users",
        item([("pk", s("b")), ("l", small)]),
    ))
    .await
    .unwrap();
    let updated = mock
        .update_item(
            UpdateItemRequest::new("users", item([("pk", s("b"))]), "SET l = list_append(l, l)")
                .with_return_values(ReturnValues::AllNew),
        )
        .await
        .unwrap();
    assert_eq!(
        updated.attributes.expect("returned item")["l"],
        AttributeValue::L(vec![s(&half), s(&half)])
    );
}

#[tokio::test]
async fn number_precision_and_ordering() {
    let mock = numbers();
    let values_to_store = ["-1", "0", "0.5", "1", "10", "1e2", "-1e-2"];
    for value in values_to_store {
        mock.put_item(PutItemRequest::new(
            "numbers",
            item([("pk", s("p")), ("sk", n(value))]),
        ))
        .await
        .unwrap();
    }
    let page = mock
        .query(QueryRequest::new("numbers", "pk = :pk").with_values(values([(":pk", s("p"))])))
        .await
        .unwrap();
    let order: Vec<String> = page
        .items
        .iter()
        .map(|item| item["sk"].as_n().unwrap().clone())
        .collect();
    assert_eq!(order, vec!["-1", "-1e-2", "0", "0.5", "1", "10", "1e2"]);

    // 38 significant digits are the documented maximum.
    let max_digits = "9".repeat(38);
    mock.put_item(PutItemRequest::new(
        "numbers",
        item([("pk", s("p")), ("sk", n("1")), ("big", n(&max_digits))]),
    ))
    .await
    .unwrap();
    let too_precise = "9".repeat(39);
    let error = mock
        .put_item(PutItemRequest::new(
            "numbers",
            item([("pk", s("p")), ("sk", n("1")), ("big", n(&too_precise))]),
        ))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("significant digits"));

    // A literal far past the parser's length cap is refused without being
    // copied into the message (issue #61).
    let hostile = "9".repeat(1 << 20);
    let error = mock
        .put_item(PutItemRequest::new(
            "numbers",
            item([("pk", s("p")), ("sk", n("1")), ("big", n(&hostile))]),
        ))
        .await
        .unwrap_err();
    let message = error.to_string();
    assert!(message.len() < 200, "{} bytes", message.len());
    assert!(message.ends_with('…'), "{message}");

    let malformed = mock
        .put_item(PutItemRequest::new(
            "numbers",
            item([("pk", s("p")), ("sk", n("1")), ("bad", n("1.2.3"))]),
        ))
        .await
        .unwrap_err();
    assert!(malformed.to_string().contains("numeric value"));
}

#[tokio::test]
async fn exponents_outside_the_documented_range_are_rejected() {
    let mock = numbers();
    let put = |key: &'static str, value: String| {
        let mock = &mock;
        async move {
            mock.put_item(PutItemRequest::new(
                "numbers",
                item([("pk", s("exp")), ("sk", n(key)), ("v", n(&value))]),
            ))
            .await
        }
    };

    // Just inside the documented range at both ends: 1E-130 and 9.99…E+125.
    put("1", "1e-130".to_string())
        .await
        .expect("1E-130 is storable");
    put("2", "1e125".to_string())
        .await
        .expect("1E+125 is storable");
    put("3", format!("{}e88", "9".repeat(38)))
        .await
        .expect("9.99…E+125 is storable");

    // One step outside at both ends.
    for (key, value) in [("4", "1e-131"), ("5", "1e126")] {
        let error = put(key, value.to_string()).await.expect_err("out of range");
        assert!(error.to_string().contains("exponent range"), "{error}");
    }

    // The audit's hostile value: a five-byte number that used to be accepted
    // and then asked the formatter for 2^31 zeros. It must be a validation
    // error, and it must arrive without allocating.
    let error = put("6", "1e2147483647".to_string())
        .await
        .expect_err("out of range");
    assert!(error.to_string().contains("exponent range"), "{error}");

    // Issue #58: an `N` attribute whose exponent sits at the `i64` boundary and
    // whose fraction length pushes it past it. The old parser added the two in
    // raw `i64` arithmetic, so a debug build aborted the whole test process
    // instead of rejecting the write.
    for (key, value) in [
        ("7", "0.5e-9223372036854775808"),
        ("8", "10e9223372036854775807"),
    ] {
        let error = put(key, value.to_string())
            .await
            .expect_err("an exponent past i64 is not a number");
        assert!(error.to_string().contains("numeric value"), "{error}");
    }
}

#[tokio::test]
async fn large_numbers_compare_without_losing_precision() {
    let mock = numbers();
    let a = "10000000000000000000000000000000000001";
    let b = "10000000000000000000000000000000000002";
    mock.put_item(PutItemRequest::new(
        "numbers",
        item([("pk", s("p")), ("sk", n(a))]),
    ))
    .await
    .unwrap();
    mock.put_item(PutItemRequest::new(
        "numbers",
        item([("pk", s("p")), ("sk", n(b))]),
    ))
    .await
    .unwrap();

    let page = mock
        .query(QueryRequest::new("numbers", "pk = :pk").with_values(values([(":pk", s("p"))])))
        .await
        .unwrap();
    assert_eq!(page.items[0]["sk"], n(a));
    assert_eq!(page.items[1]["sk"], n(b));

    let condition = ConditionExpression::parse("sk > :low").unwrap();
    let mut low = std::collections::HashMap::new();
    low.insert(":low".to_string(), n(a));
    assert!(condition
        .evaluate(
            Some(&page.items[1]),
            &std::collections::HashMap::new(),
            &low
        )
        .unwrap());
    assert!(!condition
        .evaluate(
            Some(&page.items[0]),
            &std::collections::HashMap::new(),
            &low
        )
        .unwrap());
}

#[tokio::test]
async fn empty_strings_and_binary_are_allowed_for_non_key_attributes() {
    let mock = users();
    mock.put_item(PutItemRequest::new(
        "users",
        item([
            ("pk", s("u#1")),
            ("empty", s("")),
            ("empty_blob", AttributeValue::B(Blob::new(Vec::new()))),
            ("nothing", AttributeValue::Null(true)),
            ("flag", AttributeValue::Bool(false)),
        ]),
    ))
    .await
    .unwrap();

    let stored = mock
        .get_item(GetItemRequest::new("users", item([("pk", s("u#1"))])))
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(stored.get("empty"), Some(&s("")));
    assert_eq!(
        stored.get("empty_blob"),
        Some(&AttributeValue::B(Blob::new(Vec::new())))
    );
    assert_eq!(stored.get("nothing"), Some(&AttributeValue::Null(true)));
    assert_eq!(stored.get("flag"), Some(&AttributeValue::Bool(false)));

    // The same value is rejected as a key.
    let error = mock
        .put_item(PutItemRequest::new("users", item([("pk", s(""))])))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("empty string"));
}

#[tokio::test]
async fn empty_sets_are_rejected_but_lists_and_maps_are_not() {
    let mock = users();
    let error = mock
        .put_item(PutItemRequest::new(
            "users",
            item([("pk", s("u#1")), ("tags", AttributeValue::Ss(Vec::new()))]),
        ))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("string set may not be empty"));

    mock.put_item(PutItemRequest::new(
        "users",
        item([
            ("pk", s("u#2")),
            ("tags", AttributeValue::L(Vec::new())),
            ("meta", AttributeValue::M(std::collections::HashMap::new())),
        ]),
    ))
    .await
    .unwrap();
    assert_eq!(mock.count("users"), 1);
}

#[tokio::test]
async fn deeply_nested_maps_round_trip() {
    let mock = users();
    let mut value = s("leaf");
    let mut path = Vec::new();
    for depth in 0..10 {
        let name = format!("level{depth}");
        path.push(name.clone());
        value = AttributeValue::M(std::collections::HashMap::from([(name, value)]));
    }
    mock.put_item(PutItemRequest::new(
        "users",
        item([("pk", s("u#1")), ("tree", value.clone())]),
    ))
    .await
    .unwrap();

    let stored = mock
        .get_item(GetItemRequest::new("users", item([("pk", s("u#1"))])))
        .await
        .unwrap()
        .item
        .unwrap();
    let mut current = stored.get("tree").unwrap();
    for name in path.iter().rev() {
        current = current.as_m().unwrap().get(name).unwrap();
    }
    assert_eq!(current, &s("leaf"));

    // Nested updates reach the same depth.
    let expression = format!(
        "SET tree.{} = :v",
        path.iter().rev().cloned().collect::<Vec<_>>().join(".")
    );
    mock.update_item(
        UpdateItemRequest::new("users", item([("pk", s("u#1"))]), expression)
            .with_values(values([(":v", s("changed"))])),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn long_attribute_names_and_values() {
    let mock = users();
    let long_name = "a".repeat(1_000);
    let long_value = "v".repeat(50_000);
    mock.put_item(PutItemRequest::new(
        "users",
        item([
            ("pk".to_string(), s("u#1")),
            (long_name.clone(), s(&long_value)),
        ]),
    ))
    .await
    .unwrap();

    let stored = mock
        .get_item(GetItemRequest::new("users", item([("pk", s("u#1"))])))
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(stored.get(&long_name), Some(&s(long_value)));
}

#[tokio::test]
async fn hash_only_tables_query_and_scan() {
    let mock = users();
    mock.seed(
        "users",
        vec![
            item([("pk", s("a")), ("type", s("admin"))]),
            item([("pk", s("b")), ("type", s("member"))]),
        ],
    )
    .unwrap();

    let page = mock
        .query(QueryRequest::new("users", "pk = :pk").with_values(values([(":pk", s("a"))])))
        .await
        .unwrap();
    assert_eq!(page.count, 1);

    let scanned = mock
        .scan(
            ScanRequest::new("users")
                .with_filter("attribute_not_exists(#t)")
                .with_names(mock_ddb::names([("#t", "missing")])),
        )
        .await
        .unwrap();
    assert_eq!(scanned.count, 2);

    let specific = mock
        .query(
            QueryRequest::new("users", "pk = :pk")
                .with_values(values([(":pk", s("a"))]))
                .with_projection("type")
                .with_select(Select::SpecificAttributes),
        )
        .await
        .unwrap();
    assert_eq!(specific.items[0].len(), 1);
}

#[tokio::test]
async fn consistent_reads_are_accepted() {
    let mock = users();
    mock.seed("users", vec![item([("pk", s("u#1"))])]).unwrap();
    let found = mock
        .get_item(GetItemRequest {
            consistent_read: true,
            ..GetItemRequest::new("users", item([("pk", s("u#1"))]))
        })
        .await
        .unwrap();
    assert!(found.item.is_some());

    let page = mock
        .query(QueryRequest {
            consistent_read: true,
            ..QueryRequest::new("users", "pk = :pk").with_values(values([(":pk", s("u#1"))]))
        })
        .await
        .unwrap();
    assert_eq!(page.count, 1);
}

#[tokio::test]
async fn binary_sort_keys_order_bytewise() {
    let mock = MockDdb::with_table(
        "blobs",
        KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::Binary),
    )
    .unwrap();
    for bytes in [vec![2_u8], vec![0_u8, 1], vec![1_u8]] {
        mock.put_item(PutItemRequest::new(
            "blobs",
            item([("pk", s("p")), ("sk", AttributeValue::B(Blob::new(bytes)))]),
        ))
        .await
        .unwrap();
    }
    let page = mock
        .query(QueryRequest::new("blobs", "pk = :pk").with_values(values([(":pk", s("p"))])))
        .await
        .unwrap();
    let order: Vec<Vec<u8>> = page
        .items
        .iter()
        .map(|item| item["sk"].as_b().unwrap().as_ref().to_vec())
        .collect();
    assert_eq!(order, vec![vec![0_u8, 1], vec![1_u8], vec![2_u8]]);
}

#[tokio::test]
async fn updates_and_deletes_require_the_full_key() {
    let mock = numbers();
    let partial_update = mock
        .update_item(UpdateItemRequest::new(
            "numbers",
            item([("pk", s("p"))]),
            "SET v = :v",
        ))
        .await;
    assert!(partial_update.is_err());

    let partial_delete = mock
        .delete_item(DeleteItemRequest::new("numbers", item([("pk", s("p"))])))
        .await;
    assert!(partial_delete.is_err());
}

#[tokio::test]
async fn table_lifecycle_and_defaults() {
    let mock = MockDdb::default();
    assert_eq!(mock.count("nothing"), 0);

    let name = "a".repeat(255);
    let description = mock
        .create_table(CreateTableRequest::new(
            name.clone(),
            KeySchema::hash("pk", KeyType::String),
        ))
        .await
        .unwrap();
    assert_eq!(description.table_name, name);
    assert_eq!(description.item_count, 0);
    assert_eq!(description.global_secondary_indexes.len(), 0);
    assert_eq!(description.local_secondary_indexes.len(), 0);
    assert_eq!(
        mock.list_tables(ListTablesRequest::default())
            .await
            .unwrap()
            .table_names
            .len(),
        1
    );
}

#[tokio::test]
async fn return_values_all_old_on_overwrite_and_delete() {
    let mock = users();
    mock.put_item(PutItemRequest::new(
        "users",
        item([("pk", s("u#1")), ("v", n("1"))]),
    ))
    .await
    .unwrap();
    mock.put_item(PutItemRequest::new(
        "users",
        item([("pk", s("u#1")), ("v", n("2"))]),
    ))
    .await
    .unwrap();

    let deleted = mock
        .delete_item(
            DeleteItemRequest::new("users", item([("pk", s("u#1"))]))
                .with_return_values(ReturnValues::AllOld),
        )
        .await
        .unwrap();
    assert_eq!(deleted.attributes.unwrap().get("v"), Some(&n("2")));
}

#[tokio::test]
async fn sort_key_conditions_on_a_decimal_boundary() {
    let mock = numbers();
    for value in ["1.5", "2.5", "3.5"] {
        mock.put_item(PutItemRequest::new(
            "numbers",
            item([("pk", s("p")), ("sk", n(value))]),
        ))
        .await
        .unwrap();
    }
    let page = mock
        .query(
            QueryRequest::new("numbers", "pk = :pk AND sk >= :low")
                .with_values(values([(":pk", s("p")), (":low", n("2.5"))])),
        )
        .await
        .unwrap();
    assert_eq!(page.count, 2);

    let exclusive = mock
        .query(
            QueryRequest::new("numbers", "pk = :pk AND sk > :low")
                .with_values(values([(":pk", s("p")), (":low", n("2.5"))])),
        )
        .await
        .unwrap();
    assert_eq!(exclusive.count, 1);
}

#[tokio::test]
async fn deleting_a_table_removes_its_stream_and_items() {
    let mock = users();
    mock.seed("users", vec![item([("pk", s("u#1"))])]).unwrap();
    let description = mock.delete_table("users").await.unwrap();
    assert_eq!(description.item_count, 1);
    assert_eq!(mock.count("users"), 0);
    assert_eq!(
        mock.list_tables(ListTablesRequest::default())
            .await
            .unwrap()
            .table_names
            .len(),
        0
    );
}

/// The service's 4096-byte limit on an expression string, just inside and just
/// outside, on all three expression kinds.
#[test]
fn expression_size_limit_is_enforced_before_parsing() {
    fn padded(expression: &str, size: usize) -> String {
        let mut text = String::from(expression);
        text.push_str(&" ".repeat(size - expression.len()));
        text
    }

    let at_limit = padded("REMOVE a", 4096);
    assert_eq!(at_limit.len(), 4096);
    assert!(mock_ddb::UpdateExpression::parse(&at_limit).is_ok());
    let over_limit = padded("REMOVE a", 4097);
    assert_eq!(over_limit.len(), 4097);
    let error = mock_ddb::UpdateExpression::parse(&over_limit).unwrap_err();
    assert!(
        error.to_string().contains("maximum allowed size"),
        "unexpected error: {error}"
    );

    assert!(ConditionExpression::parse(&padded("attribute_exists(pk)", 4096)).is_ok());
    assert!(ConditionExpression::parse(&padded("attribute_exists(pk)", 4097)).is_err());
    assert!(mock_ddb::ProjectionExpression::parse(&padded("pk, profile.city", 4096)).is_ok());
    assert!(mock_ddb::ProjectionExpression::parse(&padded("pk, profile.city", 4097)).is_err());
}

/// The limit counts bytes of the expression as sent, not characters.
#[test]
fn expression_size_limit_counts_bytes() {
    let name = "ü".repeat(2048);
    assert_eq!(name.len(), 4096);
    assert!(mock_ddb::ProjectionExpression::parse(&name).is_ok());
    assert!(mock_ddb::ProjectionExpression::parse(&format!("{name}ü")).is_err());
}

/// The service counts operators and function calls separately from bytes. A
/// condition at the operator boundary is accepted and one operator past it is
/// rejected with the service's stable reported count.
#[test]
fn expression_operator_limit_applies_to_conditions() {
    let mut terms = vec![
        "a BETWEEN :l AND :h",
        "a IN (:a, :b)",
        "NOT a = :v",
        "attribute_exists(a)",
        "size(a) = :v",
        "NOT (a = :v OR a = :v)",
        "(a = :v AND a = :v)",
    ];
    terms.extend(["a = :v"; 140]);
    let at_limit = terms.join(" AND ");
    assert!(ConditionExpression::parse(&at_limit).is_ok());

    let over_limit = at_limit.replacen("attribute_exists(a)", "NOT attribute_exists(a)", 1);
    let error = ConditionExpression::parse(&over_limit).unwrap_err();
    assert_eq!(
        error.to_string(),
        "Invalid ConditionExpression: The expression contains too many operators; operator count: 301"
    );
}

/// Arithmetic operators count in updates, while the `=` in each SET assignment
/// and the commas and clause keywords are syntax only.
#[test]
fn expression_operator_limit_applies_to_updates_without_counting_assignment() {
    let mut at_limit = String::from("SET v = list_append(a, :x)");
    for index in 0..149 {
        at_limit.push_str(if index % 2 == 0 { " + " } else { " - " });
        at_limit.push_str(if index % 2 == 0 {
            "if_not_exists(a, :x)"
        } else {
            "list_append(a, :x)"
        });
    }
    at_limit.push_str(" + :v");
    assert!(mock_ddb::UpdateExpression::parse(&at_limit).is_ok());

    let over_limit = format!("{at_limit} + :v");
    let error = mock_ddb::UpdateExpression::parse(&over_limit).unwrap_err();
    assert_eq!(
        error.to_string(),
        "Invalid UpdateExpression: The expression contains too many operators; operator count: 301"
    );

    let assignments = (0..300)
        .map(|index| format!("a{index}=:v"))
        .collect::<Vec<_>>()
        .join(",");
    assert!(mock_ddb::UpdateExpression::parse(&format!("SET {assignments}")).is_ok());
}

/// An oversized expression is rejected for its size, not for its syntax: the
/// check runs before the text is tokenized.
#[test]
fn an_oversized_expression_is_rejected_before_it_is_parsed() {
    let expression = format!("REMOVE {}%%%", "a".repeat(4090));
    assert!(expression.len() > 4096);
    let error = mock_ddb::UpdateExpression::parse(&expression).unwrap_err();
    assert!(
        error.to_string().contains("maximum allowed size"),
        "unexpected error: {error}"
    );
}

/// A `REMOVE` naming twenty thousand paths is rejected up front instead of
/// costing a quadratic overlap scan first.
#[test]
fn oversized_remove_list_is_rejected() {
    let paths: Vec<String> = (0..20_000).map(|index| format!("a{index}")).collect();
    let expression = format!("REMOVE {}", paths.join(","));
    assert!(expression.len() > 100_000);
    let error = mock_ddb::UpdateExpression::parse(&expression).unwrap_err();
    assert!(
        error.to_string().contains("maximum allowed size"),
        "unexpected error: {error}"
    );
}

/// A list of paths the service accepts is parsed and applied in full, and the
/// overlap check still finds a duplicate among hundreds of paths.
#[test]
fn a_large_remove_list_within_the_limit_is_applied() {
    use std::collections::HashMap;

    let paths: Vec<String> = (0..300).map(|index| format!("a{index}")).collect();
    let expression = format!("REMOVE {}", paths.join(","));
    assert!(expression.len() < 4096);

    let update = mock_ddb::UpdateExpression::parse(&expression).unwrap();
    let mut stored = item([("pk", s("u#1"))]);
    for path in &paths {
        stored.insert(path.clone(), n("1"));
    }
    let touched = update
        .apply(
            &mut stored,
            &KeySchema::hash("pk", KeyType::String),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
    assert_eq!(touched.len(), paths.len());
    assert!(paths.iter().all(|path| !stored.contains_key(path)));
    assert!(stored.contains_key("pk"));

    let duplicated = format!("{expression},a0");
    assert!(duplicated.len() < 4096);
    assert!(mock_ddb::UpdateExpression::parse(&duplicated).is_err());
}

/// The retry decision turns on the last terminal reason: an empty list is not
/// retryable, one retryable reason is, and a single terminal member on either
/// side of it ends that.
#[test]
fn retryability_edges_around_the_terminal_reason() {
    fn conflict() -> CancellationReason {
        CancellationReason::new(CancellationReasonCode::TransactionConflict, "conflict")
    }
    fn throttled() -> CancellationReason {
        CancellationReason::new(CancellationReasonCode::ThrottlingError, "throttled")
    }
    fn terminal() -> CancellationReason {
        CancellationReason::new(CancellationReasonCode::ConditionalCheckFailed, "no")
    }
    fn cancelled(reasons: Vec<CancellationReason>) -> Error {
        Error::TransactionCanceled {
            message: "cancelled".to_string(),
            reasons,
        }
    }

    // Just inside the boundary: empty is not retryable, one conflict is.
    assert!(!cancelled(Vec::new()).is_retryable());
    assert!(cancelled(vec![conflict()]).is_retryable());
    // Two conflicts stay retryable — length is not the limit, the reason is —
    // and a throttle is as fixable as a lost race.
    assert!(cancelled(vec![conflict(), conflict()]).is_retryable());
    assert!(cancelled(vec![throttled()]).is_retryable());
    assert!(cancelled(vec![conflict(), throttled()]).is_retryable());
    // Just outside: a terminal member before or after a retryable one.
    assert!(!cancelled(vec![terminal(), conflict()]).is_retryable());
    assert!(!cancelled(vec![conflict(), terminal()]).is_retryable());
    assert!(!cancelled(vec![throttled(), terminal()]).is_retryable());
    assert!(!cancelled(vec![terminal()]).is_retryable());
    // A member whose item collection is exhausted is terminal too: the data is
    // too large, and the second attempt meets the same limit.
    assert!(!cancelled(vec![CancellationReason::new(
        CancellationReasonCode::ItemCollectionSizeLimitExceeded,
        "too big"
    )])
    .is_retryable());
    // Outside a cancellation only the 5xx is retryable.
    assert!(Error::internal("bug").is_retryable());
    assert!(Error::internal_from("bug", "not a number".parse::<u32>().unwrap_err()).is_retryable());
    assert!(!Error::validation("bad").is_retryable());
    assert!(!Error::table_not_found("users").is_retryable());
    assert!(!Error::ResourceInUse {
        message: "exists".to_string()
    }
    .is_retryable());
}

/// `cancellation_reasons` is the member list for a cancelled transaction and
/// empty for every other failure, including one with no members.
#[test]
fn cancellation_reason_edges_between_variants() {
    let reason = CancellationReason::new(CancellationReasonCode::ValidationError, "bad action");
    let cancelled = Error::TransactionCanceled {
        message: "cancelled".to_string(),
        reasons: vec![reason.clone()],
    };
    assert_eq!(cancelled.cancellation_reasons(), [reason]);
    assert_eq!(
        Error::TransactionCanceled {
            message: "cancelled".to_string(),
            reasons: Vec::new(),
        }
        .cancellation_reasons(),
        []
    );
    assert_eq!(Error::validation("bad").cancellation_reasons(), []);
    assert_eq!(
        Error::ConditionalCheckFailed {
            table: "users".to_string(),
            message: "condition failed".to_string(),
            item: None,
        }
        .cancellation_reasons(),
        []
    );
}

/// The clock surface is total over its input range: an instant past the last
/// representable one clamps instead of aborting the test binary with an
/// arithmetic-overflow panic.
#[test]
fn clocks_clamp_instead_of_panicking_at_the_top_of_the_range() {
    let clock = MockClock::at_epoch_seconds(u64::MAX);
    let last = clock.now();
    clock.advance(std::time::Duration::MAX);
    assert_eq!(clock.now(), last);

    let from_epoch = MockClock::new(std::time::UNIX_EPOCH);
    from_epoch.advance(std::time::Duration::MAX);
    assert!(from_epoch.epoch_seconds() > 0);

    clock.set(std::time::UNIX_EPOCH);
    assert_eq!(clock.epoch_seconds(), 0);
}
