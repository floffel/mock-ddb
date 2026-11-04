//! Lower a parsed statement onto the crate's own request types.
//!
//! PartiQL is a front-end, not a second storage engine: every statement becomes
//! the equivalent `Query`, `Scan`, `PutItem`, `UpdateItem` or `DeleteItem`
//! request, which then runs through the same validation, capacity accounting,
//! TTL and stream bookkeeping as the native call. Two consequences are worth
//! stating:
//!
//! * A `WHERE` clause is re-rendered in the expression language with generated
//!   `#p0`/`:v0` placeholders, so the battle-tested condition parser and
//!   evaluator decide what a predicate means. Only the split between the key
//!   condition and the filter is PartiQL-specific, because it needs the table's
//!   key schema.
//! * An `INSERT` tuple is resolved into an [`Item`] here, so an item size or a
//!   number range is validated before anything reaches the store.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

use super::parse::{Cond, Doc, Function, Update, Value};
use crate::expr::{CmpOp, Path, Segment};
use crate::schema::KeySchema;
use crate::{Error, Item, Result};

/// Accumulates the aliases and values one statement needs.
#[derive(Debug, Default)]
pub(crate) struct Renderer {
    /// `#p0` → attribute name.
    names: HashMap<String, String>,
    /// Attribute name → `#p0`, so one attribute keeps one alias.
    aliases: HashMap<String, String>,
    /// `:v0` → value.
    values: HashMap<String, AttributeValue>,
}

impl Renderer {
    /// An empty renderer.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The alias for one attribute name, allocating it on first use.
    fn alias(&mut self, name: &str) -> String {
        if let Some(alias) = self.aliases.get(name) {
            return alias.clone();
        }
        let alias = format!("#p{}", self.aliases.len());
        self.aliases.insert(name.to_string(), alias.clone());
        self.names.insert(alias.clone(), name.to_string());
        alias
    }

    /// Render a document path.
    pub(crate) fn path(&mut self, path: &Path) -> String {
        let mut text = String::new();
        for segment in path {
            match segment {
                Segment::Name(name) => {
                    if !text.is_empty() {
                        text.push('.');
                    }
                    text.push_str(&self.alias(name));
                }
                Segment::Index(index) => {
                    text.push('[');
                    text.push_str(&index.to_string());
                    text.push(']');
                }
            }
        }
        text
    }

    /// Store a value and return its `:vN` placeholder.
    fn value(&mut self, value: AttributeValue) -> String {
        let placeholder = format!(":v{}", self.values.len());
        self.values.insert(placeholder.clone(), value);
        placeholder
    }

    /// Render one operand of a predicate.
    pub(crate) fn operand(
        &mut self,
        value: &Value,
        parameters: &[AttributeValue],
    ) -> Result<String> {
        match value {
            Value::Path(path) => Ok(self.path(path)),
            Value::Size(path) => Ok(format!("size({})", self.path(path))),
            Value::Parameter(index) => {
                let resolved = parameters.get(*index).cloned().ok_or_else(|| {
                    Error::validation(
                        "Number of parameters in request and statement does not match",
                    )
                })?;
                Ok(self.value(resolved))
            }
            Value::Literal(literal) => Ok(self.value(literal.clone())),
        }
    }

    /// Render a `WHERE` clause.
    pub(crate) fn condition(
        &mut self,
        condition: &Cond,
        parameters: &[AttributeValue],
    ) -> Result<String> {
        match condition {
            Cond::Compare(left, operator, right) => Ok(format!(
                "{} {} {}",
                self.operand(left, parameters)?,
                comparison(*operator),
                self.operand(right, parameters)?
            )),
            Cond::Between(value, low, high) => Ok(format!(
                "{} BETWEEN {} AND {}",
                self.operand(value, parameters)?,
                self.operand(low, parameters)?,
                self.operand(high, parameters)?
            )),
            Cond::In(value, members) => {
                let rendered = members
                    .iter()
                    .map(|member| self.operand(member, parameters))
                    .collect::<Result<Vec<_>>>()?;
                Ok(format!(
                    "{} IN ({})",
                    self.operand(value, parameters)?,
                    rendered.join(", ")
                ))
            }
            Cond::Missing { value, negated } => {
                let function = if *negated {
                    "attribute_exists"
                } else {
                    "attribute_not_exists"
                };
                Ok(format!("{function}({})", self.operand(value, parameters)?))
            }
            Cond::Null { value, negated } => {
                let Value::Path(path) = value else {
                    return Err(Error::validation(
                        "The IS operator needs an attribute path, not a literal or a parameter",
                    ));
                };
                // `NULL` is a stored value, so the test is its type, not its
                // presence: an absent attribute is neither `IS NULL` nor
                // `IS NOT NULL`.
                let placeholder = self.value(AttributeValue::S("NULL".to_string()));
                let test = format!("attribute_type({}, {placeholder})", self.path(path));
                Ok(if *negated {
                    format!("NOT ({test})")
                } else {
                    test
                })
            }
            Cond::Function(function, arguments) => {
                let rendered = arguments
                    .iter()
                    .map(|argument| self.operand(argument, parameters))
                    .collect::<Result<Vec<_>>>()?;
                Ok(format!(
                    "{}({})",
                    function_name(*function),
                    rendered.join(", ")
                ))
            }
            Cond::And(terms) => self.join(terms, " AND ", parameters),
            Cond::Or(terms) => self.join(terms, " OR ", parameters),
            Cond::Not(term) => Ok(format!("NOT ({})", self.condition(term, parameters)?)),
        }
    }

    /// Join terms with `operator`, parenthesising an `OR` inside an `AND`.
    fn join(
        &mut self,
        terms: &[Cond],
        operator: &str,
        parameters: &[AttributeValue],
    ) -> Result<String> {
        let rendered = terms
            .iter()
            .map(|term| {
                let text = self.condition(term, parameters)?;
                Ok::<String, Error>(if operator == " AND " && matches!(term, Cond::Or(_)) {
                    format!("({text})")
                } else {
                    text
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(rendered.join(operator))
    }

    /// Render a `SELECT` projection.
    pub(crate) fn projection(&mut self, paths: &[Path]) -> String {
        paths
            .iter()
            .map(|path| self.path(path))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The `#name` and `:value` maps the renderer built, empty when nothing was
    /// aliased or bound.
    pub(crate) fn names(self) -> Maps {
        let names = if self.names.is_empty() {
            None
        } else {
            Some(self.names)
        };
        let values = if self.values.is_empty() {
            None
        } else {
            Some(self.values)
        };
        (names, values)
    }
}

/// The expression-attribute maps one rendered statement needs.
pub(crate) type Maps = (
    Option<HashMap<String, String>>,
    Option<HashMap<String, AttributeValue>>,
);

/// The expression-language spelling of a comparison operator.
fn comparison(operator: CmpOp) -> &'static str {
    match operator {
        CmpOp::Eq => "=",
        CmpOp::Ne => "<>",
        CmpOp::Lt => "<",
        CmpOp::Le => "<=",
        CmpOp::Gt => ">",
        CmpOp::Ge => ">=",
    }
}

/// The expression-language name of a PartiQL function.
fn function_name(function: Function) -> &'static str {
    match function {
        Function::BeginsWith => "begins_with",
        Function::Contains => "contains",
        Function::AttributeExists => "attribute_exists",
        Function::AttributeNotExists => "attribute_not_exists",
        Function::AttributeType => "attribute_type",
    }
}

/// One key attribute a conjunct constrains.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Target {
    /// The partition key, constrained with `=`.
    Partition,
    /// The sort key, constrained with `=` or an ordering comparison.
    Sort,
}

/// Split a `WHERE` clause into the part a `Query` can use as its key condition
/// and the part that filters.
///
/// The key condition exists only when a top-level conjunct pins the partition
/// key with `=`, which is what the service requires; the sort-key conjuncts
/// join it, and everything else filters. A clause that pins no partition key
/// yields `(None, Some(whole clause))` — the caller reads the table with a
/// `Scan` instead, which is the crate's documented difference from the
/// service's insistence on a key condition.
pub(crate) fn split_where(
    condition: &Cond,
    key_schema: &KeySchema,
) -> (Option<Cond>, Option<Cond>) {
    let mut partition = Vec::new();
    let mut sort = Vec::new();
    let mut filter = Vec::new();
    for conjunct in condition.conjuncts() {
        match key_target(conjunct, key_schema) {
            Some(Target::Partition) => partition.push(conjunct.clone()),
            Some(Target::Sort) => sort.push(conjunct.clone()),
            None => filter.push(conjunct.clone()),
        }
    }
    if partition.is_empty() {
        filter.extend(sort);
        filter.extend(partition);
        return (None, combine(filter));
    }
    partition.extend(sort);
    (combine(partition), combine(filter))
}

/// Combine conjuncts with `AND`, or `None` when there are none.
fn combine(terms: Vec<Cond>) -> Option<Cond> {
    match terms.len() {
        0 => None,
        1 => terms.into_iter().next(),
        _ => Some(Cond::And(terms)),
    }
}

/// Which key attribute a conjunct constrains, if any.
fn key_target(condition: &Cond, key_schema: &KeySchema) -> Option<Target> {
    let sort = key_schema
        .sort_key
        .as_ref()
        .map(|element| element.attribute_name.as_str());
    match condition {
        Cond::Compare(Value::Path(path), operator, _) => {
            let name = single_name(path)?;
            if name == key_schema.partition_key.attribute_name && *operator == CmpOp::Eq {
                return Some(Target::Partition);
            }
            let ordering = !matches!(operator, CmpOp::Ne);
            if Some(name) == sort && ordering {
                return Some(Target::Sort);
            }
            None
        }
        Cond::Between(Value::Path(path), _, _) => {
            let name = single_name(path)?;
            (Some(name) == sort).then_some(Target::Sort)
        }
        Cond::Function(Function::BeginsWith, arguments) => {
            let Value::Path(path) = arguments.first()? else {
                return None;
            };
            let name = single_name(path)?;
            (Some(name) == sort).then_some(Target::Sort)
        }
        _ => None,
    }
}

/// The attribute name of a one-segment path.
fn single_name(path: &Path) -> Option<&str> {
    match path.as_slice() {
        [Segment::Name(name)] => Some(name),
        _ => None,
    }
}

/// Build the full primary key a write needs from a `WHERE` clause.
///
/// The service requires `UPDATE` and `DELETE` to name every key attribute with
/// `=`, so a clause that does not is a validation failure rather than a
/// table-wide write.
pub(crate) fn key_from_condition(
    condition: Option<&Cond>,
    key_schema: &KeySchema,
    parameters: &[AttributeValue],
) -> Result<Item> {
    let condition = condition.ok_or_else(missing_key)?;
    let conjuncts = condition.conjuncts();
    let mut key = Item::new();
    let mut wanted = vec![&key_schema.partition_key.attribute_name];
    if let Some(sort) = &key_schema.sort_key {
        wanted.push(&sort.attribute_name);
    }
    for attribute in wanted {
        let value = conjuncts.iter().find_map(|conjunct| {
            let Cond::Compare(Value::Path(path), CmpOp::Eq, value) = conjunct else {
                return None;
            };
            (single_name(path) == Some(attribute.as_str())).then_some(value)
        });
        let Some(value) = value else {
            return Err(missing_key());
        };
        key.insert(attribute.clone(), resolve(value, parameters)?);
    }
    Ok(key)
}

/// The failure a `WHERE` clause earns when it does not name the whole key.
fn missing_key() -> Error {
    Error::validation("Where clause does not contain a mandatory equality on all key attributes")
}

/// Resolve a parameter or literal to the value it stands for.
fn resolve(value: &Value, parameters: &[AttributeValue]) -> Result<AttributeValue> {
    match value {
        Value::Parameter(index) => parameters.get(*index).cloned().ok_or_else(|| {
            Error::validation("Number of parameters in request and statement does not match")
        }),
        Value::Literal(literal) => Ok(literal.clone()),
        Value::Path(_) | Value::Size(_) => Err(Error::validation(
            "Where clause does not contain a mandatory equality on all key attributes",
        )),
    }
}

/// Resolve an `INSERT` tuple into the item it stands for.
pub(crate) fn resolve_item(
    entries: &[(String, Doc)],
    parameters: &[AttributeValue],
) -> Result<Item> {
    entries
        .iter()
        .map(|(name, value)| Ok((name.clone(), value.resolve(parameters)?)))
        .collect()
}

/// Render an `UPDATE` statement's `SET` and `REMOVE` clauses.
pub(crate) fn update_expression(
    update: &Update,
    renderer: &mut Renderer,
    parameters: &[AttributeValue],
) -> Result<String> {
    let mut clauses = Vec::new();
    if !update.assignments.is_empty() {
        let mut parts = Vec::new();
        for (path, terms) in &update.assignments {
            let target = renderer.path(path);
            let mut text = String::new();
            for (position, (sign, value)) in terms.iter().enumerate() {
                if position > 0 {
                    text.push(' ');
                    text.push_str(sign.as_str());
                    text.push(' ');
                }
                text.push_str(&renderer.operand(value, parameters)?);
            }
            parts.push(format!("{target} = {text}"));
        }
        clauses.push(format!("SET {}", parts.join(", ")));
    }
    if !update.remove.is_empty() {
        let removed = update
            .remove
            .iter()
            .map(|path| renderer.path(path))
            .collect::<Vec<_>>()
            .join(", ");
        clauses.push(format!("REMOVE {removed}"));
    }
    Ok(clauses.join(" "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partiql::parse::parse;
    use crate::schema::KeyType;

    fn schema() -> KeySchema {
        KeySchema::hash_and_range("pk", KeyType::String, "sk", KeyType::Number)
    }

    fn condition_of(statement: &str) -> Cond {
        match parse(statement).unwrap().statement {
            crate::partiql::parse::Statement::Select(select) => select.condition.unwrap(),
            other => panic!("expected a SELECT, got {other:?}"),
        }
    }

    #[test]
    fn splits_key_conjuncts_from_filters() {
        let condition = condition_of("SELECT * FROM t WHERE pk = ? AND sk > ? AND state = 'open'");
        let (key, filter) = split_where(&condition, &schema());
        let mut renderer = Renderer::new();
        let parameters = vec![AttributeValue::S("a".into()), AttributeValue::N("3".into())];
        assert_eq!(
            renderer
                .condition(&key.expect("key condition"), &parameters)
                .unwrap(),
            "#p0 = :v0 AND #p1 > :v1"
        );
        assert_eq!(
            renderer
                .condition(&filter.expect("filter"), &parameters)
                .unwrap(),
            "#p2 = :v2"
        );
    }

    #[test]
    fn a_clause_without_the_partition_key_is_all_filter() {
        let condition = condition_of("SELECT * FROM t WHERE state = 'open'");
        let (key, filter) = split_where(&condition, &schema());
        assert!(key.is_none());
        let mut renderer = Renderer::new();
        assert_eq!(
            renderer.condition(&filter.expect("filter"), &[]).unwrap(),
            "#p0 = :v0"
        );
    }

    #[test]
    fn sort_key_alone_is_a_filter() {
        let condition = condition_of("SELECT * FROM t WHERE sk > ?");
        let (key, filter) = split_where(&condition, &schema());
        assert!(key.is_none());
        assert!(filter.is_some());
    }

    #[test]
    fn extracts_the_full_primary_key() {
        let condition = condition_of("SELECT * FROM t WHERE pk = ? AND sk = ?");
        let parameters = vec![AttributeValue::S("a".into()), AttributeValue::N("3".into())];
        let key = key_from_condition(Some(&condition), &schema(), &parameters).unwrap();
        assert_eq!(key.len(), 2);
        assert_eq!(key["pk"], AttributeValue::S("a".into()));
        assert_eq!(key["sk"], AttributeValue::N("3".into()));
    }

    #[test]
    fn a_partial_key_is_rejected() {
        let condition = condition_of("SELECT * FROM t WHERE pk = ?");
        let parameters = vec![AttributeValue::S("a".into())];
        let error = key_from_condition(Some(&condition), &schema(), &parameters).unwrap_err();
        assert!(error.to_string().contains("mandatory equality"));
        assert!(key_from_condition(None, &schema(), &[]).is_err());
    }

    #[test]
    fn renders_nested_paths_and_functions() {
        let condition = condition_of(
            "SELECT * FROM t WHERE begins_with(profile.name, ?) AND attribute_exists(tags[0])",
        );
        let mut renderer = Renderer::new();
        let text = renderer
            .condition(&condition, &[AttributeValue::S("al".into())])
            .unwrap();
        assert!(
            text.starts_with("begins_with(#p0.#p1, :v0) AND attribute_exists(#p2[0])"),
            "{text}"
        );
    }

    #[test]
    fn renders_an_or_inside_an_and_with_parentheses() {
        let condition = condition_of("SELECT * FROM t WHERE (a = ? OR b = ?) AND c = ?");
        let mut renderer = Renderer::new();
        let text = renderer
            .condition(
                &condition,
                &[
                    AttributeValue::S("1".into()),
                    AttributeValue::S("2".into()),
                    AttributeValue::S("3".into()),
                ],
            )
            .unwrap();
        assert_eq!(text, "(#p0 = :v0 OR #p1 = :v1) AND #p2 = :v2");
    }

    #[test]
    fn renders_an_update_expression() {
        let parsed = parse(
            "UPDATE t SET plays = plays + ?, name = 'x' REMOVE legacy WHERE pk = ? AND sk = ?",
        )
        .unwrap();
        let crate::partiql::parse::Statement::Update(update) = parsed.statement else {
            panic!("expected an UPDATE");
        };
        let parameters = vec![
            AttributeValue::N("1".into()),
            AttributeValue::S("a".into()),
            AttributeValue::N("2".into()),
        ];
        let mut renderer = Renderer::new();
        let text = update_expression(&update, &mut renderer, &parameters).unwrap();
        assert_eq!(text, "SET #p0 = #p0 + :v0, #p1 = :v1 REMOVE #p2");
    }

    #[test]
    fn resolves_an_insert_tuple() {
        let parsed = parse("INSERT INTO t VALUE {'pk': ?, 'tags': ['a'], 'n': 1}").unwrap();
        let crate::partiql::parse::Statement::Insert(insert) = parsed.statement else {
            panic!("expected an INSERT");
        };
        let item = resolve_item(&insert.item, &[AttributeValue::S("a".into())]).unwrap();
        assert_eq!(item.len(), 3);
        assert_eq!(item["pk"], AttributeValue::S("a".into()));
        assert!(matches!(item["tags"], AttributeValue::L(_)));
    }
}
