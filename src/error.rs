//! Every failure the mock reports, shaped like the service's own error classes.
//!
//! DynamoDB distinguishes failures by a small set of error codes
//! (`ResourceNotFoundException`, `ValidationException`,
//! `ConditionalCheckFailedException`, `ResourceInUseException`,
//! `TransactionCanceledException`). Tests usually assert on the class rather
//! than the wording, so [`Error::code`] returns the code the real service
//! would have used and the variants carry the details a test needs.
//!
//! ```
//! use mock_ddb::{s, Error, GetItemRequest, MockDdb, item};
//!
//! # #[tokio::main]
//! # async fn main() {
//! let mock = MockDdb::new();
//! let err = mock
//!     .get_item(GetItemRequest::new("no-such-table", item([("pk", s("a"))])))
//!     .await
//!     .unwrap_err();
//! assert_eq!(err.code(), "ResourceNotFoundException");
//! # }
//! ```

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

use crate::Item;

/// The result type used throughout the crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A DynamoDB failure.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The table (or index) named in the request does not exist.
    #[error("{message}")]
    ResourceNotFound {
        /// The resource that was looked up, for example the `users` table.
        resource: String,
        /// The message the service would have returned.
        message: String,
    },

    /// `CreateTable` was called for a table that already exists.
    #[error("{message}")]
    ResourceInUse {
        /// The message the service would have returned.
        message: String,
    },

    /// The request was malformed: missing key attributes, a bad expression, an
    /// oversized item, too many items in a batch, and so on.
    #[error("{message}")]
    Validation {
        /// What was wrong, phrased so a test failure reads like a service error.
        message: String,
    },

    /// A `ConditionExpression` (including every condition in a transaction)
    /// evaluated to false, so the write was rejected.
    #[error("{message}")]
    ConditionalCheckFailed {
        /// The table the rejected write targeted.
        table: String,
        /// The message the service would have returned.
        message: String,
        /// The item as it was before the write, when the request asked for
        /// `ReturnValuesOnConditionCheckFailure`.
        item: Option<Item>,
    },

    /// A transaction was cancelled: nothing was written and at least one of its
    /// conditions failed.
    #[error("{message}")]
    TransactionCanceled {
        /// The message the service would have returned.
        message: String,
        /// One entry per transaction member, in request order.
        reasons: Vec<CancellationReason>,
    },

    /// A local secondary index would exceed the 10 GB item-collection limit.
    ///
    /// The mock never generates this today — the limit is documented as not
    /// modelled — but the variant exists so callers can match exhaustively on
    /// the service's error classes.
    #[error("{message}")]
    ItemCollectionSizeLimitExceeded {
        /// The message the service would have returned.
        message: String,
    },

    /// An invariant of the mock itself was violated. Reaching this is a bug in
    /// `mock-ddb`, not a property of the request.
    #[error("{message}")]
    Internal {
        /// What went wrong inside the mock.
        message: String,
    },
}

impl Error {
    /// The DynamoDB error code for this failure.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::ResourceNotFound { .. } => "ResourceNotFoundException",
            Self::ResourceInUse { .. } => "ResourceInUseException",
            Self::Validation { .. } => "ValidationException",
            Self::ConditionalCheckFailed { .. } => "ConditionalCheckFailedException",
            Self::TransactionCanceled { .. } => "TransactionCanceledException",
            Self::ItemCollectionSizeLimitExceeded { .. } => {
                "ItemCollectionSizeLimitExceededException"
            }
            Self::Internal { .. } => "InternalServerError",
        }
    }

    /// Whether the real service would expect the caller to retry.
    ///
    /// Every failure the mock produces is deterministic, so this is `false`
    /// except for a transaction cancelled by a simulated
    /// [`CancellationReasonCode::TransactionConflict`], which the real service
    /// does ask clients to retry.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::TransactionCanceled { reasons, .. } => reasons
                .iter()
                .any(|reason| reason.code == CancellationReasonCode::TransactionConflict),
            _ => false,
        }
    }

    /// Whether this is a rejected condition rather than a malformed request.
    #[must_use]
    pub fn is_conditional_check_failed(&self) -> bool {
        matches!(self, Self::ConditionalCheckFailed { .. })
    }

    /// The cancellation reasons of a cancelled transaction, or an empty slice.
    #[must_use]
    pub fn cancellation_reasons(&self) -> &[CancellationReason] {
        match self {
            Self::TransactionCanceled { reasons, .. } => reasons,
            _ => &[],
        }
    }

    /// Build a `ValidationException`.
    ///
    /// ```
    /// use mock_ddb::Error;
    /// let error = Error::validation("bad request");
    /// assert_eq!(error.code(), "ValidationException");
    /// ```
    #[must_use]
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation {
            message: message.into(),
        }
    }

    /// Build a `ResourceNotFoundException` for a table.
    ///
    /// ```
    /// use mock_ddb::Error;
    /// let error = Error::table_not_found("orders");
    /// assert_eq!(error.code(), "ResourceNotFoundException");
    /// ```
    #[must_use]
    pub fn table_not_found(table: &str) -> Self {
        Self::ResourceNotFound {
            resource: format!("table `{table}`"),
            message: format!("Requested resource not found: Table: {table} not found"),
        }
    }

    /// Build an `InternalServerError`.
    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    /// Build a `ConditionalCheckFailedException`.
    pub(crate) fn conditional_failed(table: &str, expression: &str, item: Option<Item>) -> Self {
        Self::ConditionalCheckFailed {
            table: table.to_string(),
            message: format!("The conditional request failed (condition: `{expression}`)"),
            item,
        }
    }
}

/// The reason one member of a cancelled transaction was rejected.
#[derive(Debug, Clone, PartialEq)]
pub struct CancellationReason {
    /// The class of failure, as the service reports it.
    pub code: CancellationReasonCode,
    /// A human-readable explanation.
    pub message: String,
    /// The member's item as it was before the transaction, when the member
    /// asked for `ReturnValuesOnConditionCheckFailure`.
    pub item: Option<HashMap<String, AttributeValue>>,
}

impl CancellationReason {
    /// Build a reason with no item image.
    ///
    /// ```
    /// use mock_ddb::{CancellationReason, CancellationReasonCode};
    /// let reason = CancellationReason::new(CancellationReasonCode::TransactionConflict, "conflict");
    /// assert_eq!(reason.code.as_str(), "TransactionConflict");
    /// ```
    #[must_use]
    pub fn new(code: CancellationReasonCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            item: None,
        }
    }

    /// Attach the item image reported with the reason.
    #[must_use]
    pub fn with_item(mut self, item: Option<Item>) -> Self {
        self.item = item;
        self
    }
}

/// The error code of one cancelled transaction member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CancellationReasonCode {
    /// The member's condition evaluated to false.
    ConditionalCheckFailed,
    /// Another transaction touched the same item at the same time.
    TransactionConflict,
    /// The item collection would have exceeded its size limit.
    ItemCollectionSizeLimitExceeded,
    /// The member did not have exactly one action, or its expression was bad.
    ValidationError,
    /// The request exceeded the table's throughput.
    ThrottlingError,
    /// No reason was reported for this position.
    None,
}

impl CancellationReasonCode {
    /// The code string the service uses.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ConditionalCheckFailed => "ConditionalCheckFailed",
            Self::TransactionConflict => "TransactionConflict",
            Self::ItemCollectionSizeLimitExceeded => "ItemCollectionSizeLimitExceeded",
            Self::ValidationError => "ValidationError",
            Self::ThrottlingError => "ThrottlingError",
            Self::None => "None",
        }
    }
}

impl std::fmt::Display for CancellationReasonCode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Convenience: build an [`AttributeValue::S`] from anything stringy.
///
/// ```
/// use mock_ddb::{s, AttributeValue};
/// assert_eq!(s("abc"), AttributeValue::S("abc".to_string()));
/// ```
#[must_use]
pub fn s(value: impl Into<String>) -> AttributeValue {
    AttributeValue::S(value.into())
}

/// Convenience: build an [`AttributeValue::N`] from anything stringy.
///
/// Numbers travel as strings in DynamoDB, so nothing is parsed here — an
/// invalid number is reported by the operation that stores it.
///
/// ```
/// use mock_ddb::{n, AttributeValue};
/// assert_eq!(n("42"), AttributeValue::N("42".to_string()));
/// ```
#[must_use]
pub fn n(value: impl Into<String>) -> AttributeValue {
    AttributeValue::N(value.into())
}
