//! The DynamoDB expression language.
//!
//! Expressions are parsed once into a syntax tree and evaluated against items.
//! The three public wrappers — [`ConditionExpression`],
//! [`UpdateExpression`] and [`ProjectionExpression`] — exist for two reasons:
//! callers can assert that an expression their production code builds is valid
//! without executing an operation, and the parsers are the part of a mock most
//! likely to be fed hostile input, so they are fuzzed directly.
//!
//! ```
//! use std::collections::HashMap;
//! use mock_ddb::{s, ConditionExpression, Item};
//!
//! let condition = ConditionExpression::parse("attribute_not_exists(pk)").unwrap();
//! let empty = Item::new();
//! assert!(condition.evaluate(Some(&empty), &HashMap::new(), &HashMap::new()).unwrap());
//!
//! let mut item = Item::new();
//! item.insert("pk".into(), s("u#1"));
//! assert!(!condition.evaluate(Some(&item), &HashMap::new(), &HashMap::new()).unwrap());
//! ```

mod eval;
mod lex;
mod parse;
mod path;
mod update;

use std::collections::{BTreeSet, HashMap};

use aws_sdk_dynamodb::types::AttributeValue;

pub(crate) use eval::{Ctx, NameMap, ValueMap};
pub(crate) use parse::{
    parse_condition, parse_projection, parse_update, CmpOp, Condition, Operand, Path, Segment,
    Update,
};
pub(crate) use path::project;

use crate::schema::KeySchema;
use crate::{Item, Result};

/// A parsed `ConditionExpression`.
///
/// Supports the full condition grammar: `AND`, `OR`, `NOT`, `=`, `<>`, `<`,
/// `<=`, `>`, `>=`, `BETWEEN`, `IN`, and `attribute_exists`,
/// `attribute_not_exists`, `attribute_type`, `begins_with`, `contains` and
/// `size`.
///
/// ```
/// use std::collections::HashMap;
/// use mock_ddb::{n, s, AttributeValue, ConditionExpression, Item};
///
/// let expression = ConditionExpression::parse(
///     "attribute_exists(pk) AND #c BETWEEN :low AND :high AND begins_with(#n, :prefix)",
/// )
/// .unwrap();
///
/// let mut item = Item::new();
/// item.insert("pk".into(), s("u#1"));
/// item.insert("count".into(), n("5"));
/// item.insert("name".into(), s("alpha"));
///
/// let names = HashMap::from([
///     ("#c".to_string(), "count".to_string()),
///     ("#n".to_string(), "name".to_string()),
/// ]);
/// let values = HashMap::from([
///     (":low".to_string(), n("1")),
///     (":high".to_string(), n("9")),
///     (":prefix".to_string(), s("al")),
/// ]);
/// assert!(expression.evaluate(Some(&item), &names, &values).unwrap());
/// # let _: AttributeValue = n("0");
/// ```
#[derive(Debug, Clone)]
pub struct ConditionExpression {
    condition: Condition,
}

impl ConditionExpression {
    /// Parse a condition expression.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`](crate::Error::Validation) when the
    /// expression is empty, malformed, nested too deeply or uses an unknown
    /// function.
    pub fn parse(input: &str) -> Result<Self> {
        Ok(Self {
            condition: parse_condition(input)?,
        })
    }

    /// Evaluate against an item. `None` means "the item does not exist", which
    /// is what makes `attribute_not_exists(pk)` true on a create.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`](crate::Error::Validation) for an
    /// undefined `#name` or `:value` placeholder, or for an ordered comparison
    /// between values of different types.
    pub fn evaluate(
        &self,
        item: Option<&Item>,
        names: &HashMap<String, String>,
        values: &HashMap<String, AttributeValue>,
    ) -> Result<bool> {
        let context = Ctx { names, values };
        eval::evaluate(&self.condition, item, &context)
    }

    /// The parsed condition, for the operations that need to inspect it.
    pub(crate) fn condition(&self) -> &Condition {
        &self.condition
    }
}

/// A parsed `UpdateExpression`.
///
/// ```
/// use std::collections::HashMap;
/// use mock_ddb::{n, s, Item, UpdateExpression};
///
/// let update = UpdateExpression::parse(
///     "SET #n = :name, hits = hits + :one REMOVE stale ADD tags :more",
/// )
/// .unwrap();
///
/// let mut item = Item::new();
/// item.insert("hits".into(), n("1"));
/// item.insert("stale".into(), s("x"));
/// let names = HashMap::from([("#n".to_string(), "name".to_string())]);
/// let values = HashMap::from([
///     (":name".to_string(), s("Bob")),
///     (":one".to_string(), n("1")),
///     (":more".to_string(), mock_ddb::AttributeValue::Ss(vec!["a".into()])),
/// ]);
///
/// update.apply(&mut item, &names, &values).unwrap();
/// assert!(!item.contains_key("stale"));
/// assert_eq!(item.get("hits"), Some(&n("2")));
/// assert_eq!(item.get("name"), Some(&s("Bob")));
/// ```
#[derive(Debug, Clone)]
pub struct UpdateExpression {
    update: Update,
}

impl UpdateExpression {
    /// Parse an update expression with `SET`, `REMOVE`, `ADD` and `DELETE`
    /// clauses.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`](crate::Error::Validation) when the
    /// expression is malformed, repeats a clause, or writes the same document
    /// path twice.
    pub fn parse(input: &str) -> Result<Self> {
        Ok(Self {
            update: parse_update(input)?,
        })
    }

    /// Apply to an item, returning the top-level attributes the expression
    /// touched.
    ///
    /// This entry point does not know the table, so it will happily rewrite an
    /// attribute that happens to be a key. The table operations in
    /// [`MockDdb`](crate::MockDdb) pass the key schema through and reject that,
    /// as the service does.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`](crate::Error::Validation) when a
    /// right-hand side refers to a missing attribute, a nested path's parent
    /// does not exist, a list index is out of range, or an operand has the
    /// wrong type for the operator.
    pub fn apply(
        &self,
        item: &mut Item,
        names: &HashMap<String, String>,
        values: &HashMap<String, AttributeValue>,
    ) -> Result<BTreeSet<String>> {
        let context = Ctx { names, values };
        update::apply(&self.update, item, &context, &KeySchema::default())
    }

    /// Apply with key protection, as the table operations do.
    pub(crate) fn apply_to_table(
        &self,
        item: &mut Item,
        names: &HashMap<String, String>,
        values: &HashMap<String, AttributeValue>,
        key_schema: &KeySchema,
    ) -> Result<BTreeSet<String>> {
        let context = Ctx { names, values };
        update::apply(&self.update, item, &context, key_schema)
    }
}

/// A parsed `ProjectionExpression`.
///
/// ```
/// use std::collections::HashMap;
/// use mock_ddb::{s, Item, ProjectionExpression};
///
/// let projection = ProjectionExpression::parse("pk, profile.city").unwrap();
/// let mut item = Item::new();
/// item.insert("pk".into(), s("u#1"));
/// item.insert("secret".into(), s("x"));
/// item.insert(
///     "profile".into(),
///     mock_ddb::AttributeValue::M(HashMap::from([(
///         "city".to_string(),
///         s("Berlin"),
///     )])),
/// );
///
/// let projected = projection.project(&item, &HashMap::new()).unwrap();
/// assert!(projected.contains_key("pk"));
/// assert!(!projected.contains_key("secret"));
/// ```
#[derive(Debug, Clone)]
pub struct ProjectionExpression {
    paths: Vec<Path>,
}

impl ProjectionExpression {
    /// Parse a comma-separated list of document paths.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`](crate::Error::Validation) when the
    /// expression is empty or a path is malformed.
    pub fn parse(input: &str) -> Result<Self> {
        Ok(Self {
            paths: parse_projection(input)?,
        })
    }

    /// Keep only the projected attributes.
    ///
    /// A path that runs into a list index contributes the whole list, because
    /// the mock does not rebuild a list from a single element.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`](crate::Error::Validation) for an undefined
    /// `#name` placeholder.
    pub fn project(&self, item: &Item, names: &HashMap<String, String>) -> Result<Item> {
        let resolved = self.resolve(names)?;
        Ok(project(
            item,
            &resolved.iter().map(Vec::as_slice).collect::<Vec<_>>(),
        ))
    }

    /// Resolve `#alias` segments.
    pub(crate) fn resolve(&self, names: &HashMap<String, String>) -> Result<Vec<Path>> {
        let context = Ctx {
            names,
            values: &HashMap::new(),
        };
        self.paths
            .iter()
            .map(|path| context.resolve(path))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{n, s};

    #[test]
    fn parses_every_clause_in_one_expression() {
        let update =
            UpdateExpression::parse("SET a = :v REMOVE b ADD c :one DELETE d :gone").unwrap();
        let mut item = Item::new();
        item.insert("c".into(), n("1"));
        let names = HashMap::new();
        let values = HashMap::from([
            (":v".to_string(), s("x")),
            (":one".to_string(), n("1")),
            (":gone".to_string(), s("g")),
        ]);
        update.apply(&mut item, &names, &values).unwrap();
        assert_eq!(item.get("a"), Some(&s("x")));
        assert_eq!(item.get("c"), Some(&n("2")));
    }

    #[test]
    fn rejects_repeated_clauses_and_overlapping_paths() {
        assert!(UpdateExpression::parse("SET a = :v SET b = :v").is_err());
        assert!(UpdateExpression::parse("SET a = :v REMOVE a").is_err());
        assert!(UpdateExpression::parse("SET a.b = :v REMOVE a").is_err());
        assert!(UpdateExpression::parse("SET a = ").is_err());
        assert!(UpdateExpression::parse("ADD a").is_err());
        assert!(UpdateExpression::parse("GROW a :v").is_err());
    }

    #[test]
    fn rejects_malformed_conditions() {
        assert!(ConditionExpression::parse("").is_err());
        assert!(ConditionExpression::parse("a =").is_err());
        assert!(ConditionExpression::parse("a = :v AND").is_err());
        assert!(ConditionExpression::parse("(a = :v").is_err());
        assert!(ConditionExpression::parse("attribute_exists()").is_err());
        assert!(ConditionExpression::parse("a BETWEEN :v").is_err());
        assert!(ConditionExpression::parse("a IN :v").is_err());
        assert!(ConditionExpression::parse("unknown_fn(a)").is_err());
        assert!(ConditionExpression::parse("size(a > :v").is_err());
    }

    #[test]
    fn deep_nesting_is_rejected_not_overflowed() {
        let deep = format!("{}a = :v{}", "(".repeat(500), ")".repeat(500));
        assert!(ConditionExpression::parse(&deep).is_err());
        let negations = format!("{}a = :v", "NOT ".repeat(500));
        assert!(ConditionExpression::parse(&negations).is_err());
    }

    #[test]
    fn undefined_placeholders_are_reported() {
        let condition = ConditionExpression::parse("a = :v").unwrap();
        assert!(condition
            .evaluate(Some(&Item::new()), &HashMap::new(), &HashMap::new())
            .is_err());
        let condition = ConditionExpression::parse("#a = :v").unwrap();
        let values = HashMap::from([(":v".to_string(), n("1"))]);
        assert!(condition
            .evaluate(Some(&Item::new()), &HashMap::new(), &values)
            .is_err());
    }

    #[test]
    fn projection_keeps_nested_paths() {
        let projection = ProjectionExpression::parse("#p.city, pk").unwrap();
        let mut item = Item::new();
        item.insert("pk".into(), s("a"));
        item.insert("other".into(), s("b"));
        item.insert(
            "profile".into(),
            AttributeValue::M(HashMap::from([("city".to_string(), s("Berlin"))])),
        );
        let names = HashMap::from([("#p".to_string(), "profile".to_string())]);
        let projected = projection.project(&item, &names).unwrap();
        assert_eq!(projected.len(), 2);
    }
}
