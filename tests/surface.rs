//! Control plane, item operations and the typed error surface.

mod common;

use common::{order, order_key, orders, user, user_key, users};
#[cfg(feature = "streams")]
use mock_ddb::StreamViewType;
use mock_ddb::{
    item, n, s, AttributeDefinition, BillingMode, CreateTableRequest, DeleteItemRequest, Error,
    GetItemRequest, Item, KeySchema, KeyType, KeysAndAttributes, ListTablesRequest, MockDdb,
    Projection, PutItemRequest, ReturnValues, SecondaryIndex, Select, UpdateItemRequest,
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
async fn batch_get_respects_projection_and_limits() {
    let mock = users();
    mock.seed(
        "users",
        vec![user("u#1", "a@example.com"), user("u#2", "b@example.com")],
    )
    .unwrap();

    let response = mock
        .batch_get_item(
            mock_ddb::BatchGetItemRequest::new().add(
                "users",
                KeysAndAttributes::new(vec![user_key("u#1"), user_key("missing")])
                    .with_projection("pk"),
            ),
        )
        .await
        .unwrap();
    let found = &response.responses["users"];
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].len(), 1);

    let too_many = mock
        .batch_get_item(mock_ddb::BatchGetItemRequest::new().add(
            "users",
            KeysAndAttributes::new((0..101).map(|i| user_key(&format!("u#{i}"))).collect()),
        ))
        .await;
    assert!(too_many.is_err());

    let empty = mock
        .batch_get_item(mock_ddb::BatchGetItemRequest::new())
        .await;
    assert!(empty.is_err());

    let unknown = mock
        .batch_get_item(
            mock_ddb::BatchGetItemRequest::new()
                .add("nope", KeysAndAttributes::new(vec![user_key("u#1")])),
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

    let mut reason = mock_ddb::CancellationReason::new(
        mock_ddb::CancellationReasonCode::TransactionConflict,
        "conflict",
    );
    assert_eq!(reason.code.as_str(), "TransactionConflict");
    assert_eq!(reason.code.to_string(), "TransactionConflict");
    reason = reason.with_item(None);
    assert!(reason.item.is_none());

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
    assert_eq!(
        mock_ddb::CancellationReasonCode::ItemCollectionSizeLimitExceeded.as_str(),
        "ItemCollectionSizeLimitExceeded"
    );
}
