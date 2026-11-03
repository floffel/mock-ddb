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

use aws_sdk_dynamodb::types::AttributeValue;

use crate::Item;

/// The result type used throughout the crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A DynamoDB failure.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The table (or index) named in the request does not exist.
    ///
    /// The rendered message already names the table or stream, the way the
    /// service words it; [`Error::resource`] hands the same identifier back as
    /// a value, so a test can assert on it without parsing the text.
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
    #[error("The conditional request failed for table {table}: {message}")]
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
    ///
    /// ```
    /// use mock_ddb::Error;
    ///
    /// assert_eq!(Error::validation("bad").code(), "ValidationException");
    /// assert_eq!(Error::table_not_found("users").code(), "ResourceNotFoundException");
    /// ```
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
    ///
    /// ```
    /// use mock_ddb::{CancellationReason, CancellationReasonCode, Error};
    ///
    /// let reason = CancellationReason::new(CancellationReasonCode::TransactionConflict, "conflict");
    /// let cancelled = Error::TransactionCanceled {
    ///     message: "cancelled".to_string(),
    ///     reasons: vec![reason],
    /// };
    /// assert!(cancelled.is_retryable());
    /// assert!(!Error::validation("bad").is_retryable());
    /// ```
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::TransactionCanceled { reasons, .. } => reasons
                .iter()
                .any(|reason| reason.code() == CancellationReasonCode::TransactionConflict),
            _ => false,
        }
    }

    /// Whether this is a rejected condition rather than a malformed request.
    #[must_use]
    ///
    /// ```
    /// use mock_ddb::Error;
    ///
    /// assert!(!Error::validation("bad").is_conditional_check_failed());
    /// ```
    pub fn is_conditional_check_failed(&self) -> bool {
        matches!(self, Self::ConditionalCheckFailed { .. })
    }

    /// The cancellation reasons of a cancelled transaction, or an empty slice.
    #[must_use]
    ///
    /// ```
    /// use mock_ddb::{CancellationReason, CancellationReasonCode, Error};
    ///
    /// let cancelled = Error::TransactionCanceled {
    ///     message: "cancelled".to_string(),
    ///     reasons: vec![CancellationReason::new(
    ///         CancellationReasonCode::ConditionalCheckFailed,
    ///         "no",
    ///     )],
    /// };
    /// assert_eq!(cancelled.cancellation_reasons().len(), 1);
    /// assert_eq!(Error::validation("bad").cancellation_reasons().len(), 0);
    /// ```
    pub fn cancellation_reasons(&self) -> &[CancellationReason] {
        match self {
            Self::TransactionCanceled { reasons, .. } => reasons,
            _ => &[],
        }
    }

    /// The resource a [`Error::ResourceNotFound`] was raised for, for example
    /// ``table `users` `` or ``stream `arn:aws:dynamodb:...` ``, or `None` for
    /// every other failure.
    #[must_use]
    ///
    /// ```
    /// use mock_ddb::Error;
    ///
    /// let error = Error::table_not_found("users");
    /// assert_eq!(error.resource(), Some("table `users`"));
    /// assert_eq!(Error::validation("bad").resource(), None);
    /// ```
    pub fn resource(&self) -> Option<&str> {
        match self {
            Self::ResourceNotFound { resource, .. } => Some(resource),
            _ => None,
        }
    }

    /// The table a [`Error::ConditionalCheckFailed`] was raised for, or `None`
    /// for every other failure.
    #[must_use]
    ///
    /// ```
    /// use mock_ddb::Error;
    ///
    /// let error = Error::ConditionalCheckFailed {
    ///     table: "users".to_string(),
    ///     message: "condition `v = :two` evaluated to false".to_string(),
    ///     item: None,
    /// };
    /// assert_eq!(error.table(), Some("users"));
    /// assert_eq!(Error::table_not_found("users").table(), None);
    /// ```
    pub fn table(&self) -> Option<&str> {
        match self {
            Self::ConditionalCheckFailed { table, .. } => Some(table),
            _ => None,
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
    ///
    /// ```
    /// use mock_ddb::Error;
    ///
    /// assert_eq!(Error::internal("bug").code(), "InternalServerError");
    /// ```
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    /// Build a `ConditionalCheckFailedException`.
    pub(crate) fn conditional_failed(table: &str, expression: &str, item: Option<Item>) -> Self {
        Self::ConditionalCheckFailed {
            table: table.to_string(),
            message: format!("condition `{expression}` evaluated to false"),
            item,
        }
    }
}

/// The reason one member of a cancelled transaction was rejected.
///
/// The code, the message and the optional item image belong together, so they
/// are private and a reason is built through [`CancellationReason::new`] and
/// [`CancellationReason::with_item`]. [`CancellationReasonCode::None`] is the
/// "nothing was reported for this position" placeholder: such a reason never
/// carries a message or an image, and the constructors enforce that.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct CancellationReason {
    code: CancellationReasonCode,
    message: String,
    item: Option<Item>,
}

impl CancellationReason {
    /// Build a reason with no item image.
    ///
    /// [`CancellationReasonCode::None`] means the service reported no reason at
    /// all for this position, so `message` is dropped for that code.
    ///
    /// ```
    /// use mock_ddb::{CancellationReason, CancellationReasonCode};
    /// let reason = CancellationReason::new(CancellationReasonCode::TransactionConflict, "conflict");
    /// assert_eq!(reason.code().as_str(), "TransactionConflict");
    /// assert_eq!(reason.message(), "conflict");
    /// assert!(reason.item().is_none());
    /// ```
    #[must_use]
    pub fn new(code: CancellationReasonCode, message: impl Into<String>) -> Self {
        let message = match code {
            CancellationReasonCode::None => String::new(),
            _ => message.into(),
        };
        Self {
            code,
            message,
            item: None,
        }
    }

    /// The class of failure the service reported.
    ///
    /// ```
    /// use mock_ddb::{CancellationReason, CancellationReasonCode};
    ///
    /// let reason = CancellationReason::new(
    ///     CancellationReasonCode::ConditionalCheckFailed,
    ///     "condition evaluated to false",
    /// );
    /// assert_eq!(reason.code(), CancellationReasonCode::ConditionalCheckFailed);
    /// ```
    #[must_use]
    pub fn code(&self) -> CancellationReasonCode {
        self.code
    }

    /// The human-readable explanation, empty for
    /// [`CancellationReasonCode::None`].
    ///
    /// ```
    /// use mock_ddb::{CancellationReason, CancellationReasonCode};
    ///
    /// let reason = CancellationReason::new(CancellationReasonCode::ValidationError, "bad action");
    /// assert_eq!(reason.message(), "bad action");
    /// assert_eq!(CancellationReason::new(CancellationReasonCode::None, "ignored").message(), "");
    /// ```
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// The member's item image, or `None` when it was not reported.
    ///
    /// ```
    /// use mock_ddb::{item, s, CancellationReason, CancellationReasonCode};
    ///
    /// let reason = CancellationReason::new(CancellationReasonCode::ConditionalCheckFailed, "")
    ///     .with_item(Some(item([("pk", s("u#1"))])));
    /// assert_eq!(reason.item().and_then(|image| image.get("pk")), Some(&s("u#1")));
    /// assert!(CancellationReason::new(CancellationReasonCode::TransactionConflict, "")
    ///     .item()
    ///     .is_none());
    /// ```
    #[must_use]
    pub fn item(&self) -> Option<&Item> {
        self.item.as_ref()
    }

    /// Attach the item image reported with the reason.
    ///
    /// A [`CancellationReasonCode::None`] reason carries no image, so `item` is
    /// dropped for that code.
    ///
    /// ```
    /// use mock_ddb::{item, s, CancellationReason, CancellationReasonCode};
    ///
    /// let reason = CancellationReason::new(CancellationReasonCode::None, "")
    ///     .with_item(Some(item([("pk", s("u#1"))])));
    /// assert!(reason.item().is_none());
    /// ```
    #[must_use]
    pub fn with_item(mut self, item: Option<Item>) -> Self {
        if self.code != CancellationReasonCode::None {
            self.item = item;
        }
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
    ///
    /// ```
    /// use mock_ddb::CancellationReasonCode;
    ///
    /// assert_eq!(CancellationReasonCode::ConditionalCheckFailed.as_str(), "ConditionalCheckFailed");
    /// assert_eq!(CancellationReasonCode::None.to_string(), "None");
    /// ```
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
