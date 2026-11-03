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

use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;

use crate::Item;

/// The result type used throughout the crate.
///
/// The error parameter defaults to [`Error`], so a signature can write
/// `Result<T>` and still name a different failure type when it needs to.
///
/// ```
/// use mock_ddb::{Error, Result};
///
/// fn parse_count(text: &str) -> Result<u32> {
///     text.parse::<u32>()
///         .map_err(|_| Error::validation(format!("`{text}` is not a number")))
/// }
///
/// assert_eq!(parse_count("7").unwrap(), 7);
/// assert_eq!(parse_count("seven").unwrap_err().code(), "ValidationException");
/// ```
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A DynamoDB failure.
///
/// Equality ignores nothing that a test can observe: two failures are equal
/// when they are the same class, carry the same details and — for
/// [`Error::Internal`] — name the same root cause. The cause is compared by its
/// rendered text, because a boxed [`std::error::Error`] is not itself
/// comparable.
#[derive(Debug, Clone, thiserror::Error)]
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
    ///
    /// When a lower layer raised the failure — a number that does not parse, a
    /// blob that is not UTF-8 — [`Error::internal_from`] keeps that error as the
    /// [`source`](std::error::Error::source), so the chain ends at the real
    /// cause instead of at a rendered string.
    #[error("{message}")]
    Internal {
        /// What went wrong inside the mock.
        message: String,
        /// The lower-layer failure that made the invariant fail, or `None` when
        /// the message is the whole story.
        #[source]
        source: Option<Arc<dyn std::error::Error + Send + Sync>>,
    },
}

impl PartialEq for Error {
    /// Compare class, details and — for [`Error::Internal`] — the rendered root
    /// cause.
    ///
    /// A boxed cause cannot be compared structurally, so it takes part in the
    /// comparison through its own [`Display`](std::fmt::Display) text: two
    /// internals are equal when they say the same thing and were caused by
    /// failures that say the same thing.
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::ResourceNotFound {
                    resource: left_resource,
                    message: left_message,
                },
                Self::ResourceNotFound {
                    resource: right_resource,
                    message: right_message,
                },
            ) => left_resource == right_resource && left_message == right_message,
            (
                Self::ResourceInUse {
                    message: left_message,
                },
                Self::ResourceInUse {
                    message: right_message,
                },
            )
            | (
                Self::Validation {
                    message: left_message,
                },
                Self::Validation {
                    message: right_message,
                },
            )
            | (
                Self::ItemCollectionSizeLimitExceeded {
                    message: left_message,
                },
                Self::ItemCollectionSizeLimitExceeded {
                    message: right_message,
                },
            ) => left_message == right_message,
            (
                Self::ConditionalCheckFailed {
                    table: left_table,
                    message: left_message,
                    item: left_item,
                },
                Self::ConditionalCheckFailed {
                    table: right_table,
                    message: right_message,
                    item: right_item,
                },
            ) => {
                left_table == right_table
                    && left_message == right_message
                    && left_item == right_item
            }
            (
                Self::TransactionCanceled {
                    message: left_message,
                    reasons: left_reasons,
                },
                Self::TransactionCanceled {
                    message: right_message,
                    reasons: right_reasons,
                },
            ) => left_message == right_message && left_reasons == right_reasons,
            (
                Self::Internal {
                    message: left_message,
                    source: left_source,
                },
                Self::Internal {
                    message: right_message,
                    source: right_source,
                },
            ) => {
                left_message == right_message
                    && render_source(left_source.as_deref())
                        == render_source(right_source.as_deref())
            }
            _ => false,
        }
    }
}

/// The root cause as text, so that two sources can be compared at all.
fn render_source(source: Option<&(dyn std::error::Error + Send + Sync)>) -> Option<String> {
    source.map(ToString::to_string)
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
    /// except for a transaction that was cancelled purely because its members
    /// lost a race — that is, one whose reasons all name
    /// [`CancellationReasonCode::TransactionConflict`], which the real service
    /// does ask clients to retry.
    ///
    /// One terminal reason is enough to make the answer `false`: a retry would
    /// meet the same rejected condition, or the same malformed member, again. A
    /// cancelled transaction that reports no reason at all is not retryable
    /// either, because nothing says the second attempt could succeed.
    #[must_use]
    ///
    /// ```
    /// use mock_ddb::{CancellationReason, CancellationReasonCode, Error};
    ///
    /// let conflict = || {
    ///     CancellationReason::new(CancellationReasonCode::TransactionConflict, "conflict")
    /// };
    /// let failed = || {
    ///     CancellationReason::new(CancellationReasonCode::ConditionalCheckFailed, "no")
    /// };
    /// let cancelled = |reasons| Error::TransactionCanceled {
    ///     message: "cancelled".to_string(),
    ///     reasons,
    /// };
    ///
    /// assert!(cancelled(vec![conflict()]).is_retryable());
    /// assert!(cancelled(vec![conflict(), conflict()]).is_retryable());
    /// // Inside: every member conflicted. Outside: one member was rejected for
    /// // a terminal reason, so a retry cannot succeed.
    /// assert!(!cancelled(vec![conflict(), failed()]).is_retryable());
    /// assert!(!cancelled(vec![failed(), conflict()]).is_retryable());
    /// // No reasons reported at all.
    /// assert!(!cancelled(Vec::new()).is_retryable());
    /// assert!(!Error::validation("bad").is_retryable());
    /// ```
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::TransactionCanceled { reasons, .. } => {
                !reasons.is_empty()
                    && reasons
                        .iter()
                        .all(|reason| reason.code() == CancellationReasonCode::TransactionConflict)
            }
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

    /// Build an `InternalServerError` whose message is the whole story.
    ///
    /// ```
    /// use mock_ddb::Error;
    ///
    /// assert_eq!(Error::internal("bug").code(), "InternalServerError");
    /// ```
    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
            source: None,
        }
    }

    /// Build an `InternalServerError` caused by a lower-layer failure.
    ///
    /// The message is what the caller reads; the source is what it walks to
    /// when it wants the root cause. Nothing of the source is repeated in the
    /// message, and the cause stays reachable through
    /// [`source`](std::error::Error::source).
    ///
    /// ```
    /// use std::error::Error as _;
    ///
    /// use mock_ddb::Error;
    ///
    /// let cause = "7,5".parse::<u32>().unwrap_err();
    /// let error = Error::internal_from("the stored count is not an integer", cause);
    ///
    /// assert_eq!(error.code(), "InternalServerError");
    /// assert_eq!(error.to_string(), "the stored count is not an integer");
    /// let source = error.source().expect("the cause is kept");
    /// assert_eq!(source.to_string(), "invalid digit found in string");
    ///
    /// // Inside: no lower layer, so no chain. Outside: the cause is walked.
    /// assert!(Error::internal("bug").source().is_none());
    /// ```
    #[must_use]
    pub fn internal_from(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Internal {
            message: message.into(),
            source: Some(Arc::new(source)),
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
