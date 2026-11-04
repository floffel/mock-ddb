//! PartiQL: `ExecuteStatement` and `BatchExecuteStatement`.
//!
//! The module is a front-end. A statement is parsed into typed nodes
//! ([`parse`]), lowered onto the crate's own request types ([`lower`]) and then
//! executed exactly like the native call it stands for — so capacity
//! accounting, TTL expiry, streams, index maintenance and every validation stay
//! in one place.
//!
//! # What it supports
//!
//! | Statement | Form |
//! | --- | --- |
//! | `SELECT` | `SELECT * \| a, b.c FROM "table"[."index"] [WHERE …]` |
//! | `INSERT` | `INSERT INTO "table" VALUE {'pk': ?, 'x': 1}` |
//! | `UPDATE` | `UPDATE "table" SET a = a + ?, b = ? [REMOVE c] WHERE …` |
//! | `DELETE` | `DELETE FROM "table" WHERE …` |
//!
//! A `WHERE` clause accepts `=`, `<>`/`!=`, `<`, `<=`, `>`, `>=`, `BETWEEN …
//! AND …`, `IN (…)`, `IS [NOT] MISSING`, `IS [NOT] NULL`, `AND`, `OR`, `NOT`,
//! parentheses and the functions `begins_with`, `contains`, `attribute_exists`,
//! `attribute_not_exists`, `attribute_type` and `size`. Values are literals
//! (`'text'`, `1.5`, `true`, `null`), positional `?` parameters or document
//! paths.
//!
//! # What it does not support
//!
//! `RETURNING`, `ORDER BY`, aggregates, joins, `<<set>>` literals and
//! multi-statement strings are not part of DynamoDB's PartiQL dialect either.
//! A `SELECT` whose `WHERE` does not pin the partition key reads the table with
//! a `Scan` instead of a `Query`; the service refuses such a statement. Both
//! are recorded in the README next to the other deliberate differences.

mod execute;
mod lex;
mod lower;
mod parse;

use aws_sdk_dynamodb::types::AttributeValue;

use crate::capacity::{ConsumedCapacity, ReturnConsumedCapacity};
use crate::{Item, Result};

pub(crate) use execute::{batch_execute_statement, execute_statement};
pub(crate) use lower::{
    key_from_condition, resolve_item, split_where, update_expression, Renderer,
};
pub(crate) use parse::{parse, Cond, Parsed, Select, Statement};

/// The service's ceiling on the length of one PartiQL statement, in characters.
///
/// ```
/// use mock_ddb::MAX_STATEMENT_LENGTH;
///
/// assert_eq!(MAX_STATEMENT_LENGTH, 8192);
/// ```
pub const MAX_STATEMENT_LENGTH: usize = lex::MAX_STATEMENT_BYTES;

/// The most statements one `BatchExecuteStatement` call may carry.
///
/// ```
/// use mock_ddb::MAX_BATCH_STATEMENTS;
///
/// assert_eq!(MAX_BATCH_STATEMENTS, 25);
/// ```
pub const MAX_BATCH_STATEMENTS: usize = 25;

/// What an `UPDATE` or `DELETE` reports when its `WHERE` clause does not hold.
///
/// ```
/// use mock_ddb::ReturnValuesOnConditionCheckFailure;
///
/// assert_eq!(
///     ReturnValuesOnConditionCheckFailure::default(),
///     ReturnValuesOnConditionCheckFailure::None
/// );
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReturnValuesOnConditionCheckFailure {
    /// Report the item as it was, so a test can assert on the stale image.
    AllOld,
    /// Report nothing (the default).
    #[default]
    None,
}

/// The kind of a parsed PartiQL statement.
///
/// ```
/// use mock_ddb::{PartiqlStatement, StatementKind};
///
/// let statement = PartiqlStatement::parse("DELETE FROM \"users\" WHERE pk = ?").unwrap();
/// assert_eq!(statement.kind(), StatementKind::Delete);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StatementKind {
    /// `SELECT … FROM …`
    Select,
    /// `INSERT INTO … VALUE …`
    Insert,
    /// `UPDATE … SET …`
    Update,
    /// `DELETE FROM …`
    Delete,
}

/// A PartiQL statement, checked once so a test can assert its shape without
/// running it.
///
/// Parsing is the same work `execute_statement` does first, so a statement that
/// parses here is a statement the mock accepts syntactically.
///
/// ```
/// use mock_ddb::{PartiqlStatement, StatementKind};
///
/// let statement =
///     PartiqlStatement::parse("SELECT * FROM \"orders\" WHERE pk = ?").unwrap();
/// assert_eq!(statement.table_name(), "orders");
/// assert_eq!(statement.kind(), StatementKind::Select);
///
/// let error = PartiqlStatement::parse("SELECT * FROM").unwrap_err();
/// assert!(error.to_string().contains("Syntax error"));
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct PartiqlStatement {
    parsed: Parsed,
}

impl PartiqlStatement {
    /// Parse one statement, rejecting an empty or oversized one.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`](crate::Error::Validation) for a statement
    /// that is empty, longer than
    /// [`MAX_STATEMENT_LENGTH`], or not in the supported grammar.
    ///
    /// ```
    /// use mock_ddb::PartiqlStatement;
    ///
    /// assert!(PartiqlStatement::parse("INSERT INTO \"t\" VALUE {'pk': ?}").is_ok());
    /// assert!(PartiqlStatement::parse("").is_err());
    /// ```
    pub fn parse(input: &str) -> Result<Self> {
        parse::parse(input).map(|parsed| Self { parsed })
    }

    /// Which verb the statement uses.
    #[must_use]
    pub fn kind(&self) -> StatementKind {
        match self.parsed.statement {
            Statement::Select(_) => StatementKind::Select,
            Statement::Insert(_) => StatementKind::Insert,
            Statement::Update(_) => StatementKind::Update,
            Statement::Delete(_) => StatementKind::Delete,
        }
    }

    /// The table the statement targets.
    #[must_use]
    pub fn table_name(&self) -> &str {
        self.parsed.statement.table_name()
    }

    /// How many `?` parameters the statement binds.
    ///
    /// ```
    /// use mock_ddb::PartiqlStatement;
    ///
    /// let statement = PartiqlStatement::parse(
    ///     "SELECT * FROM \"t\" WHERE pk = ? AND sk > ?",
    /// )
    /// .unwrap();
    /// assert_eq!(statement.parameters(), 2);
    /// ```
    #[must_use]
    pub fn parameters(&self) -> usize {
        self.parsed.parameters
    }
}

/// An `ExecuteStatement` request.
///
/// ```
/// use mock_ddb::{s, ExecuteStatementRequest};
///
/// let request = ExecuteStatementRequest::new("SELECT * FROM \"users\" WHERE pk = ?")
///     .with_parameters(vec![s("u#1")])
///     .with_limit(10);
/// assert_eq!(request.limit, Some(10));
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExecuteStatementRequest {
    /// The PartiQL statement. The only required field.
    pub statement: String,
    /// The values the statement's `?` placeholders stand for, in order.
    pub parameters: Option<Vec<AttributeValue>>,
    /// Accepted and ignored: every read in the mock is strongly consistent.
    pub consistent_read: Option<bool>,
    /// The most items to evaluate.
    pub limit: Option<u32>,
    /// Continue a `SELECT` from a token the previous call returned.
    pub next_token: Option<String>,
    /// Whether to report consumed capacity.
    pub return_consumed_capacity: ReturnConsumedCapacity,
    /// Whether a rejected `WHERE` clause reports the item it found.
    pub return_values_on_condition_check_failure: ReturnValuesOnConditionCheckFailure,
}

impl ExecuteStatementRequest {
    /// A request for `statement`.
    #[must_use]
    pub fn new(statement: impl Into<String>) -> Self {
        Self {
            statement: statement.into(),
            ..Self::default()
        }
    }

    /// Bind the statement's `?` placeholders.
    #[must_use]
    pub fn with_parameters(mut self, parameters: Vec<AttributeValue>) -> Self {
        self.parameters = Some(parameters);
        self
    }

    /// Cap the number of evaluated items.
    #[must_use]
    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Continue from a token an earlier `SELECT` returned.
    #[must_use]
    pub fn with_next_token(mut self, next_token: impl Into<String>) -> Self {
        self.next_token = Some(next_token.into());
        self
    }

    /// Choose what the response reports about consumed capacity.
    #[must_use]
    pub fn with_return_consumed_capacity(mut self, mode: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = mode;
        self
    }
}

/// An `ExecuteStatement` response.
///
/// ```
/// use mock_ddb::ExecuteStatementOutput;
///
/// let output = ExecuteStatementOutput::default();
/// assert!(output.items.is_none());
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExecuteStatementOutput {
    /// The rows a `SELECT` matched; empty for a write.
    pub items: Option<Vec<Item>>,
    /// Set when the read stopped at `limit` and more rows remain.
    pub next_token: Option<String>,
    /// The key the read stopped at, the same value the native read returns.
    pub last_evaluated_key: Option<Item>,
    /// The capacity the statement consumed, when requested.
    pub consumed_capacity: Option<ConsumedCapacity>,
}

/// One statement of a `BatchExecuteStatement` call.
///
/// ```
/// use mock_ddb::{s, BatchStatementRequest};
///
/// let statement = BatchStatementRequest::new("SELECT * FROM \"users\" WHERE pk = ?")
///     .with_parameters(vec![s("u#1")]);
/// assert_eq!(statement.parameters.as_ref().map(Vec::len), Some(1));
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BatchStatementRequest {
    /// The PartiQL statement.
    pub statement: String,
    /// The values its `?` placeholders stand for, in order.
    pub parameters: Option<Vec<AttributeValue>>,
    /// Accepted and ignored: reads are always strongly consistent.
    pub consistent_read: Option<bool>,
    /// Whether a rejected `WHERE` clause reports the item it found.
    pub return_values_on_condition_check_failure: ReturnValuesOnConditionCheckFailure,
}

impl BatchStatementRequest {
    /// A batch member for `statement`.
    #[must_use]
    pub fn new(statement: impl Into<String>) -> Self {
        Self {
            statement: statement.into(),
            ..Self::default()
        }
    }

    /// Bind the statement's `?` placeholders.
    #[must_use]
    pub fn with_parameters(mut self, parameters: Vec<AttributeValue>) -> Self {
        self.parameters = Some(parameters);
        self
    }
}

/// A `BatchExecuteStatement` request.
///
/// Every statement must be a read or every statement a write: the service
/// rejects a mixed batch, and so does the mock.
///
/// ```
/// use mock_ddb::{BatchExecuteStatementRequest, BatchStatementRequest};
///
/// let request = BatchExecuteStatementRequest::new()
///     .with_statement(BatchStatementRequest::new("DELETE FROM \"users\" WHERE pk = ?"));
/// assert_eq!(request.statements.len(), 1);
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BatchExecuteStatementRequest {
    /// The statements, at most [`MAX_BATCH_STATEMENTS`].
    pub statements: Vec<BatchStatementRequest>,
    /// Whether to report consumed capacity.
    pub return_consumed_capacity: ReturnConsumedCapacity,
}

impl BatchExecuteStatementRequest {
    /// An empty batch.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one statement to the batch.
    #[must_use]
    pub fn with_statement(mut self, statement: BatchStatementRequest) -> Self {
        self.statements.push(statement);
        self
    }

    /// Choose what the response reports about consumed capacity.
    #[must_use]
    pub fn with_return_consumed_capacity(mut self, mode: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = mode;
        self
    }
}

/// The failure class of one rejected batch member.
///
/// ```
/// use mock_ddb::BatchStatementErrorCode;
///
/// assert_eq!(
///     BatchStatementErrorCode::ConditionalCheckFailed.to_string(),
///     "ConditionalCheckFailed"
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BatchStatementErrorCode {
    /// The member's `WHERE` clause did not hold.
    ConditionalCheckFailed,
    /// An `INSERT` met an existing primary key.
    DuplicateItem,
    /// The request was malformed.
    ValidationError,
    /// The item collection would exceed its limit.
    ItemCollectionSizeLimitExceeded,
    /// The account's throughput quota was exceeded.
    RequestLimitExceeded,
    /// The mock itself failed.
    InternalServerError,
}

impl BatchStatementErrorCode {
    /// The code the service reports, which is also the name of the failure.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ConditionalCheckFailed => "ConditionalCheckFailed",
            Self::DuplicateItem => "DuplicateItem",
            Self::ValidationError => "ValidationError",
            Self::ItemCollectionSizeLimitExceeded => "ItemCollectionSizeLimitExceeded",
            Self::RequestLimitExceeded => "RequestLimitExceeded",
            Self::InternalServerError => "InternalServerError",
        }
    }
}

impl std::fmt::Display for BatchStatementErrorCode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Why one statement of a batch was rejected.
///
/// ```
/// use mock_ddb::{BatchStatementError, BatchStatementErrorCode};
///
/// let error = BatchStatementError::new(
///     BatchStatementErrorCode::ConditionalCheckFailed,
///     "The conditional request failed",
/// );
/// assert_eq!(error.code, BatchStatementErrorCode::ConditionalCheckFailed);
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct BatchStatementError {
    /// The failure class.
    pub code: BatchStatementErrorCode,
    /// The message the service would have returned.
    pub message: String,
    /// The existing item, when the member asked for it and the clause failed.
    pub item: Option<Item>,
}

impl BatchStatementError {
    /// A failure without an item image.
    #[must_use]
    pub fn new(code: BatchStatementErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            item: None,
        }
    }
}

/// One member's outcome in a `BatchExecuteStatement` response.
///
/// ```
/// use mock_ddb::BatchStatementResponse;
///
/// let response = BatchStatementResponse::default();
/// assert!(response.error.is_none());
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BatchStatementResponse {
    /// Why the member failed, or `None` when it succeeded.
    pub error: Option<BatchStatementError>,
    /// The table the member touched.
    pub table_name: Option<String>,
    /// The item a `SELECT` returned.
    pub item: Option<Item>,
}

/// A `BatchExecuteStatement` response.
///
/// The members are in request order; a failed member reports its error in place
/// instead of failing the whole call.
///
/// ```
/// use mock_ddb::BatchExecuteStatementOutput;
///
/// let output = BatchExecuteStatementOutput::default();
/// assert!(output.responses.is_empty());
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BatchExecuteStatementOutput {
    /// One entry per requested statement, in order.
    pub responses: Vec<BatchStatementResponse>,
    /// The capacity each statement consumed, when requested.
    pub consumed_capacity: Vec<ConsumedCapacity>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_statement_for_inspection() {
        let statement = PartiqlStatement::parse("UPDATE \"t\" SET a = ? WHERE pk = ?").unwrap();
        assert_eq!(statement.kind(), StatementKind::Update);
        assert_eq!(statement.table_name(), "t");
        assert_eq!(statement.parameters(), 2);
    }

    #[test]
    fn reports_an_oversized_statement() {
        let text = format!("SELECT {} FROM t", "*".repeat(MAX_STATEMENT_LENGTH));
        let error = PartiqlStatement::parse(&text).unwrap_err();
        assert!(error.to_string().contains("8192"));
    }

    #[test]
    fn renders_the_error_codes() {
        assert_eq!(
            BatchStatementErrorCode::DuplicateItem.to_string(),
            "DuplicateItem"
        );
        assert_eq!(
            BatchStatementErrorCode::ValidationError.as_str(),
            "ValidationError"
        );
    }
}
