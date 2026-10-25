//! Shared fixtures for the integration tests.
//!
//! Every helper builds through the public API only, so the tests double as
//! documentation of what a caller can actually write.

#![allow(dead_code)]

use mock_ddb::{item, n, s, Item, KeySchema, KeyType, MockDdb};

/// `orders`: composite key, string partition and numeric sort.
pub fn orders() -> MockDdb {
    MockDdb::with_table(
        "orders",
        KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::Number),
    )
    .unwrap()
}

/// `users`: hash-only key.
pub fn users() -> MockDdb {
    MockDdb::with_table("users", KeySchema::hash("pk", KeyType::String)).unwrap()
}

/// A key for the `orders` table.
pub fn order_key(customer: &str, id: u32) -> Item {
    item([
        ("pk", s(format!("customer#{customer}"))),
        ("sk", n(id.to_string())),
    ])
}

/// An order item.
pub fn order(customer: &str, id: u32, total: &str) -> Item {
    item([
        ("pk", s(format!("customer#{customer}"))),
        ("sk", n(id.to_string())),
        ("total", n(total)),
    ])
}

/// A user key.
pub fn user_key(id: &str) -> Item {
    item([("pk", s(id))])
}

/// A user item.
pub fn user(id: &str, email: &str) -> Item {
    item([("pk", s(id)), ("email", s(email))])
}
