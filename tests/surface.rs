//! Control plane, item operations and the typed error surface.

mod common;

use common::{order, order_key, orders, user, user_key, users};
#[cfg(feature = "streams")]
use mock_ddb::StreamViewType;
use mock_ddb::{
    item, n, s, AttributeDefinition, BillingMode, CancellationReason, CancellationReasonCode,
    CreateTableRequest, DeleteItemRequest, Error, GetItemRequest, Item, KeySchema, KeyType,
    KeysAndAttributes, ListTablesRequest, MockDdb, Projection, PutItemRequest, ReturnValues,
    SecondaryIndex, Select, UpdateItemRequest,
};

#[tokio::test]
async fn create_describe_and_delete_tables() {
    let mock = MockDdb::new();
    let created = mock
        .create_table(CreateTableRequest::new(
            "orders",
            KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::Number),
        ))
        .await
        .unwrap();
    assert_eq!(created.table_name, "orders");
    assert_eq!(created.table_status, "ACTIVE");
    assert_eq!(created.billing_mode, BillingMode::PayPerRequest);
    assert_eq!(created.item_count, 0);
    assert!(created.creation_date_time.is_some());
    assert_eq!(created.attribute_definitions.len(), 2);
    assert_eq!(
        created.attribute_definitions[0],
        AttributeDefinition::new("pk", KeyType::String)
    );

    let described = mock.describe_table("orders").await.unwrap();
    assert_eq!(described, created);

    let deleted = mock.delete_table("orders").await.unwrap();
    assert_eq!(deleted.table_name, "orders");
    assert!(matches!(
        mock.describe_table("orders").await,
        Err(Error::ResourceNotFound { .. })
    ));
    assert!(matches!(
        mock.delete_table("orders").await,
        Err(Error::ResourceNotFound { .. })
    ));
}

#[tokio::test]
async fn duplicate_create_is_resource_in_use() {
    let mock = users();
    let error = mock
        .create_table(CreateTableRequest::new(
            "users",
            KeySchema::hash("pk", KeyType::String),
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), "ResourceInUseException");
    assert!(error.to_string().contains("preexisting table"));
}

#[tokio::test]
async fn table_names_are_validated() {
    for name in ["ab", "", "with space", "sla/sh", "ümlaut"] {
        let error = MockDdb::new()
            .create_table(CreateTableRequest::new(
                name,
                KeySchema::hash("pk", KeyType::String),
            ))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Validation { .. }), "{name}: {error}");
    }
    let long = "a".repeat(256);
    assert!(MockDdb::new()
        .create_table(CreateTableRequest::new(
            long,
            KeySchema::hash("pk", KeyType::String)
        ))
        .await
        .is_err());
    // 3 characters is the documented minimum.
    assert!(MockDdb::new()
        .create_table(CreateTableRequest::new(
            "abc",
            KeySchema::hash("pk", KeyType::String)
        ))
        .await
        .is_ok());
}

#[tokio::test]
async fn index_definitions_are_validated() {
    let base = CreateTableRequest::new(
        "orders",
        KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::String),
    );

    let lsi_wrong_partition = base.clone().with_local_index(SecondaryIndex::new(
        "by-other",
        KeySchema::hash_and_range("other", KeyType::String, "lsi_sk", KeyType::String),
    ));
    assert!(rejects(lsi_wrong_partition).await);

    let lsi_without_sort = base.clone().with_local_index(SecondaryIndex::new(
        "by-lsi",
        KeySchema::hash("pk", KeyType::String),
    ));
    assert!(rejects(lsi_without_sort).await);

    let lsi_same_sort = base.clone().with_local_index(SecondaryIndex::new(
        "by-lsi",
        KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::String),
    ));
    assert!(rejects(lsi_same_sort).await);

    let duplicate_index = base
        .clone()
        .with_global_index(SecondaryIndex::new(
            "dup",
            KeySchema::hash("a", KeyType::String),
        ))
        .with_global_index(SecondaryIndex::new(
            "dup",
            KeySchema::hash("b", KeyType::String),
        ));
    assert!(rejects(duplicate_index).await);

    let unused_definition = CreateTableRequest {
        attribute_definitions: vec![
            AttributeDefinition::new("pk", KeyType::String),
            AttributeDefinition::new("sk", KeyType::String),
            AttributeDefinition::new("unused", KeyType::String),
        ],
        ..base.clone()
    };
    assert!(rejects(unused_definition).await);

    let missing_definition = CreateTableRequest {
        attribute_definitions: vec![AttributeDefinition::new("pk", KeyType::String)],
        ..base.clone()
    };
    assert!(rejects(missing_definition).await);

    let wrong_type = CreateTableRequest {
        attribute_definitions: vec![
            AttributeDefinition::new("pk", KeyType::Number),
            AttributeDefinition::new("sk", KeyType::String),
        ],
        ..base.clone()
    };
    assert!(rejects(wrong_type).await);

    let too_many_lsis = (0..6).fold(base.clone(), |request, position| {
        request.with_local_index(SecondaryIndex::new(
            format!("lsi-{position}"),
            KeySchema::hash_and_range(
                "pk",
                KeyType::String,
                format!("s{position}"),
                KeyType::String,
            ),
        ))
    });
    assert!(rejects(too_many_lsis).await);

    let valid = base.clone().with_global_index(
        SecondaryIndex::new("by-state", KeySchema::hash("state", KeyType::String))
            .with_projection(Projection::Include(vec!["total".to_string()])),
    );
    let description = MockDdb::new().create_table(valid).await.unwrap();
    assert_eq!(description.global_secondary_indexes.len(), 1);
    assert_eq!(description.attribute_definitions.len(), 3);
    assert!(description.latest_stream_arn.is_none());

    #[cfg(feature = "streams")]
    {
        let streamed = MockDdb::new()
            .create_table(base.with_stream(StreamViewType::NewAndOldImages))
            .await
            .unwrap();
        assert!(streamed.latest_stream_arn.is_some());
    }
}

/// True when the definition is rejected.
async fn rejects(request: CreateTableRequest) -> bool {
    MockDdb::new().create_table(request).await.is_err()
}

/// The message the definition is rejected with.
async fn rejection(request: CreateTableRequest) -> String {
    MockDdb::new()
        .create_table(request)
        .await
        .expect_err("the definition has to be rejected")
        .to_string()
}

#[tokio::test]
async fn index_names_carry_the_service_name_rule() {
    // The service applies one rule to table and index names: 3 to 255
    // characters out of [a-zA-Z0-9_.-]. An index name outside it used to create
    // a mock table that a deploy would fail to create.
    let base = CreateTableRequest::new(
        "orders",
        KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::String),
    );
    let global = |name: String| {
        base.clone().with_global_index(SecondaryIndex::new(
            name,
            KeySchema::hash("state", KeyType::String),
        ))
    };

    // Lower length bound: one character short, then exactly the minimum.
    let too_short = rejection(global("ab".to_string())).await;
    assert!(
        too_short.contains("globalSecondaryIndexes.1.member.indexName"),
        "{too_short}"
    );
    assert!(
        too_short.contains("length greater than or equal to 3"),
        "{too_short}"
    );
    assert!(MockDdb::new()
        .create_table(global("abc".to_string()))
        .await
        .is_ok());

    // An empty name falls under the same rule instead of a separate message.
    let empty = rejection(global(String::new())).await;
    assert!(
        empty.contains("globalSecondaryIndexes.1.member.indexName"),
        "{empty}"
    );
    assert!(
        empty.contains("length greater than or equal to 3"),
        "{empty}"
    );

    // Upper length bound: exactly the maximum, then one character over.
    assert!(MockDdb::new()
        .create_table(global("a".repeat(255)))
        .await
        .is_ok());
    let too_long = rejection(global("a".repeat(256))).await;
    assert!(
        too_long.contains("globalSecondaryIndexes.1.member.indexName"),
        "{too_long}"
    );
    assert!(
        too_long.contains("length less than or equal to 255"),
        "{too_long}"
    );

    // Charset: every allowed character passes, one foreign character does not.
    assert!(MockDdb::new()
        .create_table(global("a.b_c-9".to_string()))
        .await
        .is_ok());
    for forbidden in ["by email", "by@email", "by-emäil", "by\u{0}name"] {
        let error = rejection(global(forbidden.to_string())).await;
        assert!(
            error.contains("regular expression pattern: [a-zA-Z0-9_.-]+"),
            "{forbidden:?}: {error}"
        );
        assert!(
            error.contains("globalSecondaryIndexes.1.member.indexName"),
            "{forbidden:?}: {error}"
        );
    }

    // A local index names its own path, and the member number is 1-based.
    let local = base.clone().with_local_index(SecondaryIndex::new(
        "xy",
        KeySchema::hash_and_range("pk", KeyType::String, "lsi_sk", KeyType::String),
    ));
    let local_error = rejection(local).await;
    assert!(
        local_error.contains("localSecondaryIndexes.1.member.indexName"),
        "{local_error}"
    );

    let second_index = base
        .clone()
        .with_global_index(SecondaryIndex::new(
            "by-state",
            KeySchema::hash("state", KeyType::String),
        ))
        .with_global_index(SecondaryIndex::new(
            "no",
            KeySchema::hash("city", KeyType::String),
        ));
    let second_error = rejection(second_index).await;
    assert!(
        second_error.contains("globalSecondaryIndexes.2.member.indexName"),
        "{second_error}"
    );
}

#[tokio::test]
async fn a_local_index_needs_a_table_range_key() {
    // DynamoDB offers local secondary indexes only on tables with a range key,
    // so a hash-only table that carries one is refused instead of silently
    // becoming a table production would never create.
    let hash_only = CreateTableRequest::new("orders", KeySchema::hash("pk", KeyType::String));
    let with_lsi = hash_only.clone().with_local_index(SecondaryIndex::new(
        "by-status",
        KeySchema::hash_and_range("pk", KeyType::String, "status", KeyType::String),
    ));
    let error = MockDdb::new().create_table(with_lsi).await.unwrap_err();
    assert!(matches!(error, Error::Validation { .. }), "{error}");
    assert!(error.to_string().contains("range key"), "{error}");

    // Just inside: the same table without the local index is accepted.
    let description = MockDdb::new().create_table(hash_only).await.unwrap();
    assert!(description.local_secondary_indexes.is_empty());

    // And the indexed variant of a table that does have a range key stays valid.
    let ranged = CreateTableRequest::new(
        "orders",
        KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::String),
    )
    .with_local_index(SecondaryIndex::new(
        "by-status",
        KeySchema::hash_and_range("pk", KeyType::String, "status", KeyType::String),
    ));
    let description = MockDdb::new().create_table(ranged).await.unwrap();
    assert_eq!(description.local_secondary_indexes.len(), 1);
}

#[tokio::test]
async fn one_attribute_cannot_hold_both_key_roles() {
    // A schema whose sort key names the partition key's attribute leaves the
    // primary key ambiguous and the derived definitions silently collapse the
    // duplicate, so the table is refused instead of half-built.
    let ambiguous = CreateTableRequest::new(
        "events",
        KeySchema::hash_and_range("pk", KeyType::String, "pk", KeyType::String),
    );
    let error = MockDdb::new().create_table(ambiguous).await.unwrap_err();
    assert!(matches!(error, Error::Validation { .. }), "{error}");
    let message = error.to_string();
    assert!(message.contains("the table"), "{message}");
    assert!(message.contains("attribute pk as both"), "{message}");
    assert!(message.contains("both partition and sort key"), "{message}");

    // Just inside: two attributes of the same type are accepted and both are
    // derived, so the rejected case above is not a same-type artefact.
    let distinct = CreateTableRequest::new(
        "events",
        KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::String),
    );
    let description = MockDdb::new().create_table(distinct).await.unwrap();
    assert_eq!(description.attribute_definitions.len(), 2);

    // An index has the same constraint, and the error names the index.
    let indexed = CreateTableRequest::new("events", KeySchema::hash("pk", KeyType::String))
        .with_global_index(SecondaryIndex::new(
            "by-time",
            KeySchema::hash_and_range("at", KeyType::String, "at", KeyType::String),
        ));
    let error = MockDdb::new().create_table(indexed).await.unwrap_err();
    let message = error.to_string();
    assert!(message.contains("by-time"), "{message}");
    assert!(message.contains("both partition and sort key"), "{message}");
}

#[tokio::test]
async fn list_tables_paginates() {
    let mock = MockDdb::new();
    for name in ["tbl-a", "tbl-b", "tbl-c"] {
        mock.create_table(CreateTableRequest::new(
            name,
            KeySchema::hash("pk", KeyType::String),
        ))
        .await
        .unwrap();
    }

    let all = mock
        .list_tables(ListTablesRequest::default())
        .await
        .unwrap();
    assert_eq!(all.table_names, vec!["tbl-a", "tbl-b", "tbl-c"]);
    assert!(all.last_evaluated_table_name.is_none());

    let first = mock
        .list_tables(ListTablesRequest {
            limit: Some(2),
            ..ListTablesRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(first.table_names, vec!["tbl-a", "tbl-b"]);
    assert_eq!(first.last_evaluated_table_name.as_deref(), Some("tbl-b"));

    let next = mock
        .list_tables(ListTablesRequest {
            exclusive_start_table_name: first.last_evaluated_table_name,
            ..ListTablesRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(next.table_names, vec!["tbl-c"]);

    assert!(mock
        .list_tables(ListTablesRequest {
            limit: Some(0),
            ..ListTablesRequest::default()
        })
        .await
        .is_err());
    assert!(mock
        .list_tables(ListTablesRequest {
            limit: Some(101),
            ..ListTablesRequest::default()
        })
        .await
        .is_err());
}

#[tokio::test]
async fn put_get_delete_round_trip() {
    let mock = orders();
    mock.put_item(PutItemRequest::new("orders", order("c1", 1, "10")))
        .await
        .unwrap();

    let found = mock
        .get_item(GetItemRequest::new("orders", order_key("c1", 1)))
        .await
        .unwrap();
    assert_eq!(found.item, Some(order("c1", 1, "10")));

    let missing = mock
        .get_item(GetItemRequest::new("orders", order_key("c1", 2)))
        .await
        .unwrap();
    assert!(missing.item.is_none());

    let deleted = mock
        .delete_item(
            DeleteItemRequest::new("orders", order_key("c1", 1))
                .with_return_values(ReturnValues::AllOld),
        )
        .await
        .unwrap();
    assert_eq!(deleted.attributes, Some(order("c1", 1, "10")));
    assert_eq!(mock.count("orders"), 0);

    // Deleting twice is a success with no attributes, like the service.
    let again = mock
        .delete_item(
            DeleteItemRequest::new("orders", order_key("c1", 1))
                .with_return_values(ReturnValues::AllOld),
        )
        .await
        .unwrap();
    assert!(again.attributes.is_none());
}

#[tokio::test]
async fn item_keys_are_validated() {
    let mock = orders();

    let missing_sort = mock
        .put_item(PutItemRequest::new(
            "orders",
            item([("pk", s("customer#c1"))]),
        ))
        .await
        .unwrap_err();
    assert!(missing_sort.to_string().contains("Missing the key sk"));

    let wrong_type = mock
        .put_item(PutItemRequest::new(
            "orders",
            item([("pk", s("customer#c1")), ("sk", s("not-a-number"))]),
        ))
        .await
        .unwrap_err();
    assert!(wrong_type.to_string().contains("Type mismatch for key sk"));

    let empty_key = mock
        .put_item(PutItemRequest::new(
            "orders",
            item([("pk", s("")), ("sk", n("1"))]),
        ))
        .await
        .unwrap_err();
    assert!(empty_key.to_string().contains("empty string value"));

    let extra_attribute = mock
        .get_item(GetItemRequest::new(
            "orders",
            item([("pk", s("customer#c1")), ("sk", n("1")), ("extra", s("x"))]),
        ))
        .await
        .unwrap_err();
    assert!(extra_attribute
        .to_string()
        .contains("key element does not match the schema"));

    let unknown_table = mock
        .get_item(GetItemRequest::new("nope", order_key("c1", 1)))
        .await
        .unwrap_err();
    assert_eq!(unknown_table.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn put_conditions_and_return_values() {
    let mock = users();
    mock.put_item(PutItemRequest::new("users", user("u#1", "a@example.com")))
        .await
        .unwrap();

    let overwrite = mock
        .put_item(
            PutItemRequest::new("users", user("u#1", "b@example.com"))
                .with_condition("attribute_not_exists(pk)"),
        )
        .await
        .unwrap_err();
    assert!(overwrite.is_conditional_check_failed());
    assert_eq!(overwrite.code(), "ConditionalCheckFailedException");
    assert!(!overwrite.is_retryable());
    assert_eq!(overwrite.cancellation_reasons().len(), 0);

    // The failed condition left the original in place.
    let found = mock
        .get_item(GetItemRequest::new("users", user_key("u#1")))
        .await
        .unwrap();
    assert_eq!(found.item, Some(user("u#1", "a@example.com")));

    let with_item = mock
        .put_item(
            PutItemRequest::new("users", user("u#1", "c@example.com"))
                .with_condition("attribute_not_exists(pk)")
                .with_return_values(ReturnValues::AllOld)
                .with_return_values_on_condition_check_failure(),
        )
        .await;
    match with_item {
        Err(Error::ConditionalCheckFailed { item, table, .. }) => {
            assert_eq!(table, "users");
            assert_eq!(item, Some(user("u#1", "a@example.com")));
        }
        other => panic!("expected a conditional failure, got {other:?}"),
    }

    let replaced = mock
        .put_item(
            PutItemRequest::new("users", user("u#1", "d@example.com"))
                .with_return_values(ReturnValues::AllOld),
        )
        .await
        .unwrap();
    assert_eq!(replaced.attributes, Some(user("u#1", "a@example.com")));

    // Any other ReturnValues setting yields no attributes for PutItem.
    let silent = mock
        .put_item(
            PutItemRequest::new("users", user("u#1", "e@example.com"))
                .with_return_values(ReturnValues::AllNew),
        )
        .await
        .unwrap();
    assert!(silent.attributes.is_none());
}

#[tokio::test]
async fn get_projection_keeps_only_requested_paths() {
    let mock = orders();
    mock.put_item(PutItemRequest::new(
        "orders",
        item([
            ("pk", s("customer#c1")),
            ("sk", n("1")),
            ("total", n("10")),
            ("secret", s("hide")),
        ]),
    ))
    .await
    .unwrap();

    let projected = mock
        .get_item(
            GetItemRequest::new("orders", order_key("c1", 1))
                .with_projection("#total, sk")
                .with_names(mock_ddb::names([("#total", "total")])),
        )
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(projected.len(), 2);
    assert!(!projected.contains_key("secret"));
    assert_eq!(projected.get("total"), Some(&n("10")));

    let bad_projection = mock
        .get_item(GetItemRequest::new("orders", order_key("c1", 1)).with_projection("a b c"))
        .await;
    assert!(bad_projection.is_err());
}

#[tokio::test]
async fn update_creates_missing_items_and_returns_values() {
    let mock = users();
    let created = mock
        .update_item(
            UpdateItemRequest::new("users", user_key("u#1"), "SET email = :email")
                .with_values(mock_ddb::values([(":email", s("a@example.com"))]))
                .with_return_values(ReturnValues::AllNew),
        )
        .await
        .unwrap();
    assert_eq!(created.attributes, Some(user("u#1", "a@example.com")));

    let old = mock
        .update_item(
            UpdateItemRequest::new("users", user_key("u#1"), "SET email = :email")
                .with_values(mock_ddb::values([(":email", s("b@example.com"))]))
                .with_return_values(ReturnValues::AllOld),
        )
        .await
        .unwrap();
    assert_eq!(old.attributes, Some(user("u#1", "a@example.com")));

    let updated_new = mock
        .update_item(
            UpdateItemRequest::new("users", user_key("u#1"), "SET email = :email")
                .with_values(mock_ddb::values([(":email", s("c@example.com"))]))
                .with_return_values(ReturnValues::UpdatedNew),
        )
        .await
        .unwrap()
        .attributes
        .unwrap();
    assert_eq!(updated_new.len(), 1);
    assert_eq!(updated_new.get("email"), Some(&s("c@example.com")));

    let updated_old = mock
        .update_item(
            UpdateItemRequest::new("users", user_key("u#1"), "SET email = :email")
                .with_values(mock_ddb::values([(":email", s("d@example.com"))]))
                .with_return_values(ReturnValues::UpdatedOld),
        )
        .await
        .unwrap()
        .attributes
        .unwrap();
    assert_eq!(updated_old.get("email"), Some(&s("c@example.com")));

    let silent = mock
        .update_item(
            UpdateItemRequest::new("users", user_key("u#1"), "SET email = :email")
                .with_values(mock_ddb::values([(":email", s("e@example.com"))])),
        )
        .await
        .unwrap();
    assert!(silent.attributes.is_none());

    // Removing an attribute is absent from UPDATED_NEW.
    let removed = mock
        .update_item(
            UpdateItemRequest::new("users", user_key("u#1"), "REMOVE email")
                .with_return_values(ReturnValues::UpdatedNew),
        )
        .await
        .unwrap()
        .attributes
        .unwrap();
    assert_eq!(removed.len(), 0);
}

#[tokio::test]
async fn update_rejects_key_rewrites_and_bad_expressions() {
    let mock = users();
    mock.put_item(PutItemRequest::new("users", user("u#1", "a@example.com")))
        .await
        .unwrap();

    let key_update = mock
        .update_item(
            UpdateItemRequest::new("users", user_key("u#1"), "SET pk = :v")
                .with_values(mock_ddb::values([(":v", s("u#2"))])),
        )
        .await
        .unwrap_err();
    assert!(key_update.to_string().contains("part of the key"));

    assert!(mock
        .update_item(UpdateItemRequest::new(
            "users",
            user_key("u#1"),
            "SETT email = :v"
        ))
        .await
        .is_err());

    // The item is untouched after a rejected update.
    assert_eq!(mock.count("users"), 1);
    let found = mock
        .get_item(GetItemRequest::new("users", user_key("u#1")))
        .await
        .unwrap();
    assert_eq!(found.item, Some(user("u#1", "a@example.com")));
}

#[tokio::test]
async fn conditional_update_and_delete_are_atomic() {
    let mock = orders();
    mock.put_item(PutItemRequest::new("orders", order("c1", 1, "10")))
        .await
        .unwrap();

    let failed = mock
        .update_item(
            UpdateItemRequest::new("orders", order_key("c1", 1), "SET total = :new")
                .with_condition("total = :expected")
                .with_values(mock_ddb::values([
                    (":new", n("99")),
                    (":expected", n("11")),
                ])),
        )
        .await
        .unwrap_err();
    assert!(failed.is_conditional_check_failed());

    let still = mock
        .get_item(GetItemRequest::new("orders", order_key("c1", 1)))
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(still.get("total"), Some(&n("10")));

    let deleted = mock
        .delete_item(
            DeleteItemRequest::new("orders", order_key("c1", 1))
                .with_condition("total <> :expected")
                .with_values(mock_ddb::values([(":expected", n("10"))])),
        )
        .await;
    assert!(
        deleted.is_err(),
        "condition is false, so the delete must fail"
    );
    assert_eq!(mock.count("orders"), 1);

    mock.delete_item(
        DeleteItemRequest::new("orders", order_key("c1", 1))
            .with_condition("total = :expected")
            .with_values(mock_ddb::values([(":expected", n("10"))])),
    )
    .await
    .unwrap();
    assert_eq!(mock.count("orders"), 0);
}

#[tokio::test]
async fn seed_dump_and_clear() {
    let mock = users();
    mock.seed(
        "users",
        vec![user("u#2", "b@example.com"), user("u#1", "a@example.com")],
    )
    .unwrap();
    assert_eq!(mock.count("users"), 2);
    let dumped = mock.dump("users");
    assert_eq!(dumped.len(), 2);
    // dump is ordered by primary key.
    assert_eq!(dumped[0].get("pk"), Some(&s("u#1")));
    assert_eq!(dumped[1].get("pk"), Some(&s("u#2")));

    assert!(mock.seed("missing", vec![]).is_err());
    assert!(mock.seed("users", vec![Item::new()]).is_err());
    assert_eq!(mock.count("missing"), 0);
    assert_eq!(mock.dump("missing").len(), 0);

    mock.clear();
    assert_eq!(mock.count("users"), 0);
}

#[tokio::test]
async fn seed_is_all_or_nothing_on_validation_errors() {
    let mock = users();
    mock.seed("users", vec![user("u#1", "a@example.com")])
        .unwrap();

    // The invalid item sits last: the valid ones before it must not land.
    let refused = mock.seed(
        "users",
        vec![
            user("u#2", "b@example.com"),
            user("u#3", "c@example.com"),
            Item::new(),
        ],
    );
    assert!(matches!(refused, Err(Error::Validation { .. })));
    assert_eq!(mock.count("users"), 1);
    assert_eq!(mock.dump("users"), vec![user("u#1", "a@example.com")]);

    // Same fixture with the invalid item first: still nothing changes.
    let refused_first = mock.seed("users", vec![Item::new(), user("u#2", "b@example.com")]);
    assert!(matches!(refused_first, Err(Error::Validation { .. })));
    assert_eq!(mock.count("users"), 1);
    assert_eq!(mock.dump("users"), vec![user("u#1", "a@example.com")]);

    // The inside of the boundary: without the invalid item the same fixture
    // applies in full.
    mock.seed(
        "users",
        vec![user("u#2", "b@example.com"), user("u#3", "c@example.com")],
    )
    .unwrap();
    assert_eq!(mock.count("users"), 3);
}

#[tokio::test]
async fn batch_get_respects_projection_and_limits() {
    let mock = users();
    mock.seed(
        "users",
        vec![user("u#1", "a@example.com"), user("u#2", "b@example.com")],
    )
    .unwrap();

    let response = mock
        .batch_get_item(
            mock_ddb::BatchGetItemRequest::new()
                .add(
                    "users",
                    KeysAndAttributes::new(vec![user_key("u#1"), user_key("missing")])
                        .with_projection("pk"),
                )
                .unwrap(),
        )
        .await
        .unwrap();
    let found = &response.responses["users"];
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].len(), 1);

    let too_many = mock
        .batch_get_item(
            mock_ddb::BatchGetItemRequest::new()
                .add(
                    "users",
                    KeysAndAttributes::new((0..101).map(|i| user_key(&format!("u#{i}"))).collect()),
                )
                .unwrap(),
        )
        .await;
    assert!(too_many.is_err());

    let empty = mock
        .batch_get_item(mock_ddb::BatchGetItemRequest::new())
        .await;
    assert!(empty.is_err());

    let unknown = mock
        .batch_get_item(
            mock_ddb::BatchGetItemRequest::new()
                .add("nope", KeysAndAttributes::new(vec![user_key("u#1")]))
                .unwrap(),
        )
        .await;
    assert!(unknown.is_err());
}

#[tokio::test]
async fn select_counts_query_without_projection() {
    let mock = users();
    mock.seed(
        "users",
        vec![user("u#1", "a@example.com"), user("u#2", "b@example.com")],
    )
    .unwrap();

    let counted = mock
        .scan(
            mock_ddb::ScanRequest::new("users")
                .with_select(Select::Count)
                .with_filter("attribute_exists(email)"),
        )
        .await
        .unwrap();
    assert_eq!(counted.count, 2);
    assert_eq!(counted.items.len(), 0);

    let specific = mock
        .scan(mock_ddb::ScanRequest::new("users").with_select(Select::SpecificAttributes))
        .await;
    assert!(specific.is_err());
}

#[tokio::test]
async fn error_codes_and_retry_classification() {
    let validation = Error::validation("bad request");
    assert_eq!(validation.code(), "ValidationException");
    assert!(!validation.is_retryable());
    assert!(validation.to_string().contains("bad request"));

    let not_found = Error::table_not_found("orders");
    assert_eq!(not_found.code(), "ResourceNotFoundException");

    let too_large = Error::ItemCollectionSizeLimitExceeded {
        message: "too big".to_string(),
    };
    assert_eq!(too_large.code(), "ItemCollectionSizeLimitExceededException");

    let internal = Error::internal("bug");
    assert_eq!(internal.code(), "InternalServerError");
    // The code says 5xx, so the verdict says retryable — the two cannot drift.
    assert!(internal.is_retryable());
    assert!(!too_large.is_retryable());
    assert!(!not_found.is_retryable());

    let mut reason = mock_ddb::CancellationReason::new(
        mock_ddb::CancellationReasonCode::TransactionConflict,
        "conflict",
    );
    assert_eq!(reason.code().as_str(), "TransactionConflict");
    assert_eq!(reason.code().to_string(), "TransactionConflict");
    reason = reason.with_item(None);
    assert!(reason.item().is_none());

    let cancelled = Error::TransactionCanceled {
        message: "cancelled".to_string(),
        reasons: vec![reason],
    };
    assert_eq!(cancelled.code(), "TransactionCanceledException");
    assert!(cancelled.is_retryable());
    assert_eq!(cancelled.cancellation_reasons().len(), 1);
    assert_eq!(mock_ddb::CancellationReasonCode::None.as_str(), "None");
    assert_eq!(
        mock_ddb::CancellationReasonCode::ConditionalCheckFailed.as_str(),
        "ConditionalCheckFailed"
    );
    assert_eq!(
        mock_ddb::CancellationReasonCode::ValidationError.as_str(),
        "ValidationError"
    );
    assert_eq!(
        mock_ddb::CancellationReasonCode::ThrottlingError.as_str(),
        "ThrottlingError"
    );
    // A throttled member is retryable, a malformed one is not: the reason table
    // is the same one `Error::is_retryable` reads.
    assert!(mock_ddb::CancellationReasonCode::ThrottlingError.is_retryable());
    assert!(!mock_ddb::CancellationReasonCode::ValidationError.is_retryable());
    let throttled = mock_ddb::CancellationReason::new(
        mock_ddb::CancellationReasonCode::ThrottlingError,
        "throughput exceeded",
    );
    assert!(Error::TransactionCanceled {
        message: "cancelled".to_string(),
        reasons: vec![throttled],
    }
    .is_retryable());
    assert_eq!(
        mock_ddb::CancellationReasonCode::ItemCollectionSizeLimitExceeded.as_str(),
        "ItemCollectionSizeLimitExceeded"
    );
}

#[test]
fn cancellation_reason_cannot_hold_a_state_the_service_never_reports() {
    // A `None` code is the "nothing reported here" placeholder: any message or
    // image handed to the constructors is dropped, so the reason cannot claim a
    // failure the service would not have sent.
    let none = CancellationReason::new(CancellationReasonCode::None, "not reported");
    assert_eq!(none.code(), CancellationReasonCode::None);
    assert_eq!(none.message(), "");
    assert!(none.item().is_none());

    let image = item([("pk", s("u#1")), ("total", n("10"))]);
    let none_with_image = CancellationReason::new(CancellationReasonCode::None, "still nothing")
        .with_item(Some(image.clone()));
    assert_eq!(none_with_image.message(), "");
    assert!(none_with_image.item().is_none());

    // Just outside: a real code keeps both the message and the image.
    let failed = CancellationReason::new(
        CancellationReasonCode::ConditionalCheckFailed,
        "condition failed",
    )
    .with_item(Some(image.clone()));
    assert_eq!(
        failed.code(),
        CancellationReasonCode::ConditionalCheckFailed
    );
    assert_eq!(failed.message(), "condition failed");
    assert_eq!(failed.item(), Some(&image));

    // Dropping the image again leaves code and message untouched.
    let cleared = failed.with_item(None);
    assert!(cleared.item().is_none());
    assert_eq!(
        cleared.code(),
        CancellationReasonCode::ConditionalCheckFailed
    );
    assert_eq!(cleared.message(), "condition failed");
}

#[tokio::test]
async fn error_text_and_accessors_name_the_table_or_resource() {
    let mock = users();
    mock.put_item(PutItemRequest::new("users", user("u#1", "a@example.com")))
        .await
        .unwrap();

    // The rendered error used to be the bare reason, so the table the write
    // was rejected for appeared nowhere in the text a log would show.
    let rejected = mock
        .put_item(
            PutItemRequest::new("users", user("u#1", "b@example.com"))
                .with_condition("attribute_not_exists(pk)"),
        )
        .await
        .unwrap_err();
    let rendered = rejected.to_string();
    assert!(rendered.contains("table users"), "rendered: {rendered}");
    assert!(
        rendered.contains("attribute_not_exists(pk)"),
        "rendered: {rendered}"
    );
    assert_eq!(rejected.table(), Some("users"));
    assert_eq!(
        rejected.resource(),
        None,
        "a rejected write names a table, not a resource"
    );

    // One table away from the existing one, the miss carries the looked-up
    // resource as a value as well as in the text.
    let missing = mock
        .get_item(GetItemRequest::new("userz", user_key("u#1")))
        .await
        .unwrap_err();
    assert_eq!(missing.resource(), Some("table `userz`"));
    assert!(missing.to_string().contains("userz"));
    assert_eq!(
        missing.table(),
        None,
        "a missing table is not a rejected condition"
    );

    // A failure that carries neither identifier answers `None` from both.
    assert_eq!(Error::validation("bad").resource(), None);
    assert_eq!(Error::validation("bad").table(), None);
}

#[tokio::test]
async fn a_stream_specification_must_be_coherent() {
    // The fields are freely settable, so a caller could describe an enabled
    // stream without a view type - a table the service rejects. Both halves of
    // the invariant are enforced, not just the one the builder happens to keep.
    let base = CreateTableRequest::new("events", KeySchema::hash("pk", KeyType::String));
    let with_stream = |enabled: bool, view: Option<mock_ddb::StreamViewType>| CreateTableRequest {
        stream_specification: Some(mock_ddb::StreamSpecification {
            stream_enabled: enabled,
            stream_view_type: view,
        }),
        ..base.clone()
    };

    let missing_view = MockDdb::new()
        .create_table(with_stream(true, None))
        .await
        .unwrap_err();
    assert!(
        missing_view
            .to_string()
            .contains("StreamViewType must be specified"),
        "{missing_view}"
    );

    let stray_view = MockDdb::new()
        .create_table(with_stream(false, Some(mock_ddb::StreamViewType::NewImage)))
        .await
        .unwrap_err();
    assert!(
        stray_view
            .to_string()
            .contains("StreamViewType must not be specified"),
        "{stray_view}"
    );

    // Just inside: a disabled stream with no view type stays a valid, streamless
    // table.
    let disabled = MockDdb::new()
        .create_table(with_stream(false, None))
        .await
        .unwrap();
    assert!(disabled.latest_stream_arn.is_none());

    #[cfg(feature = "streams")]
    {
        let streamed = MockDdb::new()
            .create_table(with_stream(true, Some(mock_ddb::StreamViewType::KeysOnly)))
            .await
            .unwrap();
        assert!(streamed.latest_stream_arn.is_some());
    }
}
