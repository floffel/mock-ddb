//! PartiQL execution: `execute_statement` and `batch_execute_statement`.
//!
//! Split out of `mock.rs` so the statement front-end does not bury the native
//! data plane. Every statement is lowered onto a native request and run through
//! the same `*_inner` method the native call uses, which keeps capacity, the
//! stream log, TTL expiry and validation in one place instead of duplicating
//! them for a second entry point.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

use crate::capacity::{ConsumedCapacity, ReturnConsumedCapacity, ReturnItemCollectionMetrics};
use crate::error::Error;
use crate::partiql::{
    key_from_condition, parse, resolve_item, split_where, update_expression,
    BatchExecuteStatementOutput, BatchExecuteStatementRequest, BatchStatementError,
    BatchStatementErrorCode, BatchStatementResponse, Cond, ExecuteStatementOutput,
    ExecuteStatementRequest, Renderer, ReturnValuesOnConditionCheckFailure,
    Select as SelectStatement, Statement, MAX_BATCH_STATEMENTS,
};
use crate::request::{
    DeleteItemRequest, PutItemRequest, QueryRequest, ReturnValues, ScanRequest, Select,
    UpdateItemRequest,
};
use crate::{Item, MockDdb, Result};

/// What one statement's response reports, independent of its verb.
#[derive(Clone, Copy, Debug)]
struct Options {
    /// Whether to report consumed capacity.
    consumed_capacity: ReturnConsumedCapacity,
    /// Whether a rejected `WHERE` clause reports the item it found.
    all_old_on_failure: bool,
    /// The most items to evaluate, for a read.
    limit: Option<u32>,
}

impl Options {
    /// Read the options a single-statement request asks for.
    fn of(request: &ExecuteStatementRequest) -> Self {
        Self {
            consumed_capacity: request.return_consumed_capacity,
            all_old_on_failure: request.return_values_on_condition_check_failure
                == ReturnValuesOnConditionCheckFailure::AllOld,
            limit: request.limit,
        }
    }
}

/// Run one PartiQL statement of a single-statement request.
pub(crate) fn execute_statement(
    mock: &MockDdb,
    request: &ExecuteStatementRequest,
) -> Result<ExecuteStatementOutput> {
    let parameters = request.parameters.clone().unwrap_or_default();
    let parsed = parse(&request.statement)?;
    check_parameters(parsed.parameters, &parameters)?;
    validate_limit(request.limit)?;
    validate_next_token(request.next_token.as_deref())?;
    run(
        mock,
        &parsed.statement,
        &parameters,
        request.next_token.as_deref(),
        Options::of(request),
    )
}

/// Run a batch of PartiQL statements.
pub(crate) fn batch_execute_statement(
    mock: &MockDdb,
    request: &BatchExecuteStatementRequest,
) -> Result<BatchExecuteStatementOutput> {
    if request.statements.is_empty() {
        return Err(Error::validation(
            "1 validation error detected: Value at 'statements' failed to satisfy constraint: Member must have length greater than or equal to 1",
        ));
    }
    if request.statements.len() > MAX_BATCH_STATEMENTS {
        return Err(Error::validation(format!(
            "1 validation error detected: Value at 'statements' failed to satisfy constraint: Member must have length less than or equal to {MAX_BATCH_STATEMENTS}"
        )));
    }
    let mut members = Vec::with_capacity(request.statements.len());
    for statement in &request.statements {
        let parameters = statement.parameters.clone().unwrap_or_default();
        let parsed = parse(&statement.statement)?;
        check_parameters(parsed.parameters, &parameters)?;
        members.push((parsed, parameters));
    }
    let reads = members
        .iter()
        .filter(|(parsed, _)| parsed.statement.is_read())
        .count();
    if reads != 0 && reads != members.len() {
        return Err(Error::validation(
            "The entire batch must consist of either read statements or write statements",
        ));
    }

    let options = Options {
        consumed_capacity: request.return_consumed_capacity,
        all_old_on_failure: false,
        limit: None,
    };
    let mut responses = Vec::with_capacity(members.len());
    let mut capacities = Vec::new();
    for (parsed, parameters) in members {
        // A read in a batch must name the whole primary key, so it matches at
        // most one item; the service refuses the batch otherwise.
        let outcome = match batch_read_check(mock, &parsed.statement, &parameters) {
            Ok(()) => run(mock, &parsed.statement, &parameters, None, options),
            Err(error) => Err(error),
        };
        responses.push(match outcome {
            Ok(output) => {
                if let Some(capacity) = output.consumed_capacity {
                    capacities.push(capacity);
                }
                BatchStatementResponse {
                    error: None,
                    table_name: Some(parsed.statement.table_name().to_string()),
                    item: output.items.and_then(|items| items.into_iter().next()),
                }
            }
            Err(error) => BatchStatementResponse {
                error: Some(member_error(&error)),
                table_name: Some(parsed.statement.table_name().to_string()),
                item: None,
            },
        });
    }
    Ok(BatchExecuteStatementOutput {
        responses,
        consumed_capacity: capacities,
    })
}

/// Reject a batch read that does not pin every key attribute.
fn batch_read_check(
    mock: &MockDdb,
    statement: &Statement,
    parameters: &[AttributeValue],
) -> Result<()> {
    let Statement::Select(select) = statement else {
        return Ok(());
    };
    let key_schema = mock.key_schema_of(&select.table, select.index.as_deref())?;
    key_from_condition(select.condition.as_ref(), &key_schema, parameters)
        .map(|_| ())
        .map_err(|_| {
            Error::validation(
                "A SELECT statement in a batch must specify an equality condition on all key attributes",
            )
        })
}

/// Run one parsed statement.
fn run(
    mock: &MockDdb,
    statement: &Statement,
    parameters: &[AttributeValue],
    next_token: Option<&str>,
    options: Options,
) -> Result<ExecuteStatementOutput> {
    match statement {
        Statement::Select(select) => select_rows(mock, select, parameters, next_token, options),
        Statement::Insert(insert) => {
            let item = resolve_item(&insert.item, parameters)?;
            insert_item(mock, &insert.table, item, options)
        }
        Statement::Update(update) => {
            let key_schema = mock.key_schema_of(&update.table, None)?;
            let key = key_from_condition(update.condition.as_ref(), &key_schema, parameters)?;
            let mut renderer = Renderer::new();
            let expression = update_expression(update, &mut renderer, parameters)?;
            let condition = render_optional(&mut renderer, update.condition.as_ref(), parameters)?;
            let (names, values) = renderer.names();
            let request = UpdateItemRequest {
                table_name: update.table.clone(),
                key,
                update_expression: expression,
                condition_expression: condition,
                expression_attribute_names: names,
                expression_attribute_values: values,
                return_values: ReturnValues::None,
                return_values_on_condition_check_failure: options.all_old_on_failure,
                return_consumed_capacity: options.consumed_capacity,
                return_item_collection_metrics: ReturnItemCollectionMetrics::None,
            };
            let output = mock.update_item_inner(&request)?;
            Ok(write_output(output.consumed_capacity))
        }
        Statement::Delete(delete) => {
            let key_schema = mock.key_schema_of(&delete.table, None)?;
            let key = key_from_condition(delete.condition.as_ref(), &key_schema, parameters)?;
            let mut renderer = Renderer::new();
            let condition = render_optional(&mut renderer, delete.condition.as_ref(), parameters)?;
            let (names, values) = renderer.names();
            let request = DeleteItemRequest {
                table_name: delete.table.clone(),
                key,
                condition_expression: condition,
                expression_attribute_names: names,
                expression_attribute_values: values,
                return_values: ReturnValues::None,
                return_values_on_condition_check_failure: options.all_old_on_failure,
                return_consumed_capacity: options.consumed_capacity,
                return_item_collection_metrics: ReturnItemCollectionMetrics::None,
            };
            let output = mock.delete_item_inner(&request)?;
            Ok(write_output(output.consumed_capacity))
        }
    }
}

/// Run a `SELECT`, lowering its `WHERE` clause onto a `Query` or a `Scan`.
fn select_rows(
    mock: &MockDdb,
    select: &SelectStatement,
    parameters: &[AttributeValue],
    next_token: Option<&str>,
    options: Options,
) -> Result<ExecuteStatementOutput> {
    let (key, filter) = match &select.condition {
        Some(condition) => {
            let key_schema = mock.key_schema_of(&select.table, select.index.as_deref())?;
            split_where(condition, &key_schema)
        }
        None => (None, None),
    };
    let mut renderer = Renderer::new();
    let key_condition = render_optional(&mut renderer, key.as_ref(), parameters)?;
    let filter_expression = render_optional(&mut renderer, filter.as_ref(), parameters)?;
    let projection = if select.projection.is_empty() {
        None
    } else {
        Some(renderer.projection(&select.projection))
    };
    let (names, values) = renderer.names();
    let start_key = match next_token {
        Some(token) => Some(mock.take_next_token(token)?),
        None => None,
    };
    let (items, last_key, capacity) = if let Some(key_condition_expression) = key_condition {
        let page = mock.query_inner(&QueryRequest {
            table_name: select.table.clone(),
            index_name: select.index.clone(),
            key_condition_expression: Some(key_condition_expression),
            filter_expression,
            expression_attribute_names: names,
            expression_attribute_values: values,
            projection_expression: projection,
            scan_index_forward: None,
            limit: options.limit,
            exclusive_start_key: start_key,
            consistent_read: false,
            select: Select::AllAttributes,
            return_consumed_capacity: options.consumed_capacity,
        })?;
        (page.items, page.last_evaluated_key, page.consumed_capacity)
    } else {
        let page = mock.scan_inner(&ScanRequest {
            table_name: select.table.clone(),
            index_name: select.index.clone(),
            filter_expression,
            expression_attribute_names: names,
            expression_attribute_values: values,
            projection_expression: projection,
            limit: options.limit,
            exclusive_start_key: start_key,
            segment: None,
            total_segments: None,
            consistent_read: false,
            select: Select::AllAttributes,
            return_consumed_capacity: options.consumed_capacity,
        })?;
        (page.items, page.last_evaluated_key, page.consumed_capacity)
    };
    let next_token = last_key.as_ref().map(|key| mock.store_next_token(key));
    Ok(ExecuteStatementOutput {
        items: Some(items),
        next_token,
        last_evaluated_key: last_key,
        consumed_capacity: capacity,
    })
}

/// Write one item as an `INSERT`: it must not exist yet.
fn insert_item(
    mock: &MockDdb,
    table_name: &str,
    item: Item,
    options: Options,
) -> Result<ExecuteStatementOutput> {
    let key_schema = mock.key_schema_of(table_name, None)?;
    let mut names = HashMap::new();
    names.insert(
        "#p0".to_string(),
        key_schema.partition_key.attribute_name.clone(),
    );
    let output = match mock.put_item_inner(&PutItemRequest {
        table_name: table_name.to_string(),
        item,
        condition_expression: Some("attribute_not_exists(#p0)".to_string()),
        expression_attribute_names: Some(names),
        expression_attribute_values: None,
        return_values: ReturnValues::None,
        return_values_on_condition_check_failure: options.all_old_on_failure,
        return_consumed_capacity: options.consumed_capacity,
        return_item_collection_metrics: ReturnItemCollectionMetrics::None,
    }) {
        Ok(output) => output,
        // `attribute_not_exists(pk)` is the only condition, so a rejection can
        // only mean the key is already there.
        Err(Error::ConditionalCheckFailed { .. }) => {
            return Err(Error::duplicate_item(table_name));
        }
        Err(error) => return Err(error),
    };
    Ok(write_output(output.consumed_capacity))
}

/// The output of a write: no rows, but the capacity it consumed.
fn write_output(consumed_capacity: Option<ConsumedCapacity>) -> ExecuteStatementOutput {
    ExecuteStatementOutput {
        items: Some(Vec::new()),
        next_token: None,
        last_evaluated_key: None,
        consumed_capacity,
    }
}

/// Render an optional clause onto the expression language.
fn render_optional(
    renderer: &mut Renderer,
    condition: Option<&Cond>,
    parameters: &[AttributeValue],
) -> Result<Option<String>> {
    match condition {
        Some(condition) => Ok(Some(renderer.condition(condition, parameters)?)),
        None => Ok(None),
    }
}

/// Reject a parameter list that does not match the statement's placeholders.
fn check_parameters(bound: usize, parameters: &[AttributeValue]) -> Result<()> {
    if bound != parameters.len() {
        return Err(Error::validation(
            "Number of parameters in request and statement does not match",
        ));
    }
    Ok(())
}

/// Reject a `limit` the service would reject.
fn validate_limit(limit: Option<u32>) -> Result<()> {
    if limit == Some(0) {
        return Err(Error::validation(
            "1 validation error detected: Value at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1",
        ));
    }
    Ok(())
}

/// Reject a `NextToken` that is empty or longer than the service allows.
fn validate_next_token(token: Option<&str>) -> Result<()> {
    let Some(token) = token else {
        return Ok(());
    };
    if token.is_empty() || token.chars().count() > 32_768 {
        return Err(Error::validation(format!(
            "1 validation error detected: Value at 'nextToken' failed to satisfy constraint: Member must have length between 1 and 32768, got {}",
            token.chars().count()
        )));
    }
    Ok(())
}

/// The batch error one failed member reports.
fn member_error(error: &Error) -> BatchStatementError {
    let code = match error {
        Error::ConditionalCheckFailed { .. } => BatchStatementErrorCode::ConditionalCheckFailed,
        Error::DuplicateItem { .. } => BatchStatementErrorCode::DuplicateItem,
        Error::ItemCollectionSizeLimitExceeded { .. } => {
            BatchStatementErrorCode::ItemCollectionSizeLimitExceeded
        }
        Error::Internal { .. } => BatchStatementErrorCode::InternalServerError,
        _ => BatchStatementErrorCode::ValidationError,
    };
    BatchStatementError {
        code,
        message: error.to_string(),
        item: match error {
            Error::ConditionalCheckFailed { item, .. } => item.clone(),
            _ => None,
        },
    }
}
