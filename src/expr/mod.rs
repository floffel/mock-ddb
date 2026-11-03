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

// `lex` and `parse` are hand-written rather than generated from a `pest` or
// `lalrpop` grammar, against the reuse-before-build default. Three
// requirements of this crate make a generated parser a net loss:
//
// * The error strings are the contract. This is a mock, so callers assert on
//   the wording the service returns — for example "Invalid UpdateExpression:
//   Syntax error; token: `AND`, expected SET, REMOVE, ADD or DELETE", "Two
//   document paths overlap with each other; must remove or rewrite one of
//   these paths", "The expression can not be empty", and the lexical
//   "unexpected character '!' at position 7", where the position counts
//   characters, not bytes. A generated parser reports its own error type and
//   byte spans, so every message would still be a case-by-case mapping off its
//   tree: the grammar buys a dependency and a second error model without
//   removing that work. `tests/expressions.rs::syntax_errors_keep_the_service_wording`
//   pins the strings so a replacement cannot silently change them.
// * The size limit must be enforced before anything is allocated: the
//   4096-byte check in `lex::tokenize` runs on the `&str` before it becomes a
//   `Vec<char>`. A generated parser builds its tree first, so that check stays
//   outside it either way.
// * The depth limit is semantic, not syntactic: one level per `(` group, one
//   per `NOT`, one per `if_not_exists` fallback, one per `list_append`
//   argument — while repetition stays free because `AND`/`OR` and `+`/`-`
//   chains are stored flat. No grammar expresses "this construct costs a
//   level, that repetition does not"; it would move into hand-written tree
//   validation after parsing.
//
// The grammar is also small and closed — no literals, a fixed keyword set, no
// upstream version to track — so the maintenance a generated parser saves is
// small, and `fuzz/fuzz_targets/parse_expression.rs` holds the hand-written
// pair to the hostile-input bar a generated one would get.

mod eval;
mod lex;
mod parse;
mod path;
mod update;

use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};

use aws_sdk_dynamodb::types::AttributeValue;

pub(crate) use eval::{Ctx, NameMap, ValueMap};
pub(crate) use parse::{
    parse_condition, parse_projection, parse_update, CmpOp, Condition, Operand, Path, Segment,
    Update,
};
pub(crate) use path::{check_path_count, project};

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
    ///
    /// ```
    /// use mock_ddb::ConditionExpression;
    ///
    /// assert!(ConditionExpression::parse("attribute_exists(pk)").is_ok());
    /// assert!(ConditionExpression::parse("attribute_exists(").is_err());
    /// ```
    /// Parse a condition expression.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`](crate::Error::Validation) when the
    /// expression is empty, longer than the service's 4096-byte limit,
    /// malformed, nested too deeply or uses an unknown function. At most 64
    /// nesting constructs — `(` groups and `NOT` prefixes together — may
    /// enclose a comparison; the 65th is rejected. Repetition is not nesting:
    /// `a = :v AND a = :v AND …` builds one flat chain, so the 4096-byte limit
    /// bounds the number of terms and no term costs stack depth.
    pub fn parse(input: &str) -> Result<Self> {
        Ok(Self {
            condition: parse_condition(input)?,
        })
    }

    ///
    /// ```
    /// use std::collections::HashMap;
    /// use mock_ddb::{s, ConditionExpression, Item};
    ///
    /// let condition = ConditionExpression::parse("pk = :pk").unwrap();
    /// let mut item = Item::new();
    /// item.insert("pk".into(), s("u#1"));
    /// let values = HashMap::from([(":pk".to_string(), s("u#1"))]);
    /// assert!(condition.evaluate(Some(&item), &HashMap::new(), &values).unwrap());
    /// ```
    /// Evaluate against an item. `None` means "the item does not exist", which
    /// is what makes `attribute_not_exists(pk)` true on a create.
    ///
    /// Reads borrow: a path reads its value out of `item`, a `:placeholder`
    /// reads its value out of `values`, and neither is copied — a 400 KB
    /// attribute stays where it is. Only `size()` builds a number, and only
    /// an `#alias` path is rebuilt with the alias replaced.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`](crate::Error::Validation) for an
    /// undefined `#name` or `:value` placeholder, for an ordered comparison
    /// between values of different types, and for `size()` of an operand with
    /// no length — a number, which the service refuses to measure.
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
/// use mock_ddb::{n, s, Item, KeySchema, KeyType, UpdateExpression};
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
/// let schema = KeySchema::hash("id", KeyType::String);
/// update.apply(&mut item, &schema, &names, &values).unwrap();
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
    /// expression is longer than the service's 4096-byte limit, malformed,
    /// repeats a clause, nests `if_not_exists` or `list_append` more than 64
    /// deep, or writes two document paths that overlap — the same path, or an
    /// attribute and anything below it. A `+`/`-` chain is repetition, not
    /// nesting: it is stored flat, so the 4096-byte limit bounds its length
    /// and no term costs stack depth.
    ///
    /// Overlaps that only appear once `#alias` placeholders are resolved are
    /// rejected by [`UpdateExpression::apply`], the first point that has the
    /// expression-attribute-names map.
    pub fn parse(input: &str) -> Result<Self> {
        Ok(Self {
            update: parse_update(input)?,
        })
    }

    ///
    /// ```
    /// use std::collections::HashMap;
    /// use mock_ddb::{n, Item, KeySchema, KeyType, UpdateExpression};
    ///
    /// let update = UpdateExpression::parse("SET v = :v").unwrap();
    /// let mut item = Item::new();
    /// let schema = KeySchema::hash("id", KeyType::String);
    /// let touched = update
    ///     .apply(
    ///         &mut item,
    ///         &schema,
    ///         &HashMap::new(),
    ///         &HashMap::from([(":v".to_string(), n("1"))]),
    ///     )
    ///     .unwrap();
    /// assert!(touched.contains("v"));
    /// assert_eq!(item.get("v"), Some(&n("1")));
    ///
    /// // The same expression against a table whose partition key is `v`.
    /// let table_schema = KeySchema::hash("v", KeyType::Number);
    /// assert!(update
    ///     .apply(
    ///         &mut Item::new(),
    ///         &table_schema,
    ///         &HashMap::new(),
    ///         &HashMap::from([(":v".to_string(), n("1"))]),
    ///     )
    ///     .is_err());
    /// ```
    /// Apply to an item, returning the top-level attributes the expression
    /// touched.
    ///
    /// `key_schema` is the schema of the table the item belongs to, and it is
    /// required because the service rejects an update that rewrites a key
    /// attribute: without the real schema an expression that rewrites `pk`
    /// would silently reach the item, which is exactly the failure a caller
    /// validates against. Pass the table's own schema — the operations on
    /// [`MockDdb`](crate::MockDdb) do.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`](crate::Error::Validation) when the
    /// expression targets a key attribute of `key_schema`, a right-hand side
    /// refers to a missing attribute, a nested path's parent does not exist, a
    /// list index is out of range, an operand has the wrong type for the
    /// operator, or the target paths overlap once `#alias` placeholders are
    /// resolved.
    ///
    /// An error leaves `item` exactly as the caller passed it: the writes are
    /// staged on a copy and committed only once every clause has succeeded, so
    /// an item kept after an error is never half-updated.
    ///
    /// ```
    /// use std::collections::HashMap;
    /// use mock_ddb::{s, Item, KeySchema, KeyType, UpdateExpression};
    ///
    /// let update = UpdateExpression::parse("SET a = :v, gone.nested = :v").unwrap();
    /// let mut item = Item::new();
    /// item.insert("a".into(), s("kept"));
    /// let error = update
    ///     .apply(
    ///         &mut item,
    ///         &KeySchema::hash("id", KeyType::String),
    ///         &HashMap::new(),
    ///         &HashMap::from([(":v".to_string(), s("new"))]),
    ///     )
    ///     .unwrap_err();
    /// assert!(error.to_string().contains("invalid for update"));
    /// // `a` was the first clause and is still the old value: nothing landed.
    /// assert_eq!(item.get("a"), Some(&s("kept")));
    /// assert!(!item.contains_key("gone"));
    /// ```
    pub fn apply(
        &self,
        item: &mut Item,
        key_schema: &KeySchema,
        names: &HashMap<String, String>,
        values: &HashMap<String, AttributeValue>,
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
    ///
    /// ```
    /// use std::collections::HashMap;
    /// use mock_ddb::{s, Error, Item, ProjectionExpression};
    ///
    /// let projection = ProjectionExpression::parse("pk, profile.city").unwrap();
    /// let mut item = Item::new();
    /// item.insert("pk".into(), s("u#1"));
    /// item.insert("secret".into(), s("x"));
    ///
    /// let projected = projection.project(&item, &HashMap::new()).unwrap();
    /// assert_eq!(projected.get("pk"), Some(&s("u#1")));
    /// assert!(!projected.contains_key("secret"));
    ///
    /// let error = ProjectionExpression::parse("").unwrap_err();
    /// assert!(matches!(error, Error::Validation { .. }));
    /// ```
    /// Parse a comma-separated list of document paths.
    ///
    /// At most 100 paths are accepted (`MAX_PROJECTION_PATHS`); a longer list
    /// is rejected instead of letting one request select an unbounded number
    /// of branches from a single stored item.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`](crate::Error::Validation) when the
    /// expression is empty, longer than the service's 4096-byte limit, a path
    /// is malformed, or it names more than 100 document paths.
    pub fn parse(input: &str) -> Result<Self> {
        let paths = parse_projection(input)?;
        check_path_count(paths.len())?;
        Ok(Self { paths })
    }

    ///
    /// ```
    /// use std::collections::HashMap;
    /// use mock_ddb::{s, Item, ProjectionExpression};
    ///
    /// let projection = ProjectionExpression::parse("pk").unwrap();
    /// let mut item = Item::new();
    /// item.insert("pk".into(), s("u#1"));
    /// item.insert("secret".into(), s("x"));
    /// let projected = projection.project(&item, &HashMap::new()).unwrap();
    /// assert_eq!(projected.len(), 1);
    /// ```
    /// Keep only the projected attributes.
    ///
    /// Document paths are followed to their end: a map keeps only the requested
    /// keys, and a list keeps only the elements named by an index, in ascending
    /// order — the shape the service returns for `items[0]`, not the containing
    /// list. A path the item does not reach contributes nothing.
    ///
    /// ```
    /// use std::collections::HashMap;
    /// use mock_ddb::{s, AttributeValue, Item, ProjectionExpression};
    ///
    /// let projection = ProjectionExpression::parse("pk, rows[0].v").unwrap();
    /// let mut item = Item::new();
    /// item.insert("pk".into(), s("u#1"));
    /// item.insert("secret".into(), s("x"));
    /// item.insert(
    ///     "rows".into(),
    ///     AttributeValue::L(vec![
    ///         AttributeValue::M(HashMap::from([
    ///             ("v".to_string(), s("1")),
    ///             ("w".to_string(), s("9")),
    ///         ])),
    ///         AttributeValue::M(HashMap::from([("v".to_string(), s("2"))])),
    ///     ]),
    /// );
    ///
    /// let projected = projection.project(&item, &HashMap::new()).unwrap();
    /// assert_eq!(projected.len(), 2);
    /// // Only the first element, and only its `v`.
    /// assert_eq!(
    ///     projected.get("rows"),
    ///     Some(&AttributeValue::L(vec![AttributeValue::M(HashMap::from([(
    ///         "v".to_string(),
    ///         s("1")
    ///     )]))]))
    /// );
    /// ```
    /// # Errors
    ///
    /// Returns [`Error::Validation`](crate::Error::Validation) for an undefined
    /// `#name` placeholder, or when the expression names more than 100
    /// document paths.
    pub fn project(&self, item: &Item, names: &HashMap<String, String>) -> Result<Item> {
        check_path_count(self.paths.len())?;
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
            .map(|path| context.resolve(path).map(Cow::into_owned))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::KeyType;
    use crate::{n, s};

    /// A table schema none of these expressions target, so the update engine
    /// runs without key protection getting in the way.
    fn schema() -> KeySchema {
        KeySchema::hash("pk", KeyType::String)
    }

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
        update.apply(&mut item, &schema(), &names, &values).unwrap();
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

    /// `groups` parenthesised groups around a comparison.
    fn parenthesised(groups: usize) -> String {
        format!("{}a = :v{}", "(".repeat(groups), ")".repeat(groups))
    }

    #[test]
    fn nesting_limit_counts_one_level_per_construct() {
        // One `(` group is one level, so the documented 64 levels are usable:
        // a group used to cost two, which made 32 the real ceiling.
        assert!(ConditionExpression::parse(&parenthesised(1)).is_ok());
        assert!(ConditionExpression::parse(&parenthesised(64)).is_ok());
        let too_deep = ConditionExpression::parse(&parenthesised(65)).unwrap_err();
        assert!(too_deep.to_string().contains("nested too deeply"));

        // A `NOT` prefix is one level as well.
        let negations = |count: usize| format!("{}a = :v", "NOT ".repeat(count));
        assert!(ConditionExpression::parse(&negations(64)).is_ok());
        assert!(ConditionExpression::parse(&negations(65)).is_err());

        // Groups and negations share the budget level for level.
        let mixed = |groups: usize, count: usize| {
            format!(
                "{}{}a = :v{}",
                "(".repeat(groups),
                "NOT ".repeat(count),
                ")".repeat(groups)
            )
        };
        assert!(ConditionExpression::parse(&mixed(32, 32)).is_ok());
        assert!(ConditionExpression::parse(&mixed(33, 32)).is_err());
        assert!(ConditionExpression::parse(&mixed(32, 33)).is_err());
    }

    #[test]
    fn a_deep_expression_still_parses_its_condition() {
        // The guard must not swallow the innermost comparison: 64 levels wrap
        // a condition that evaluates.
        let condition = ConditionExpression::parse(&parenthesised(64)).unwrap();
        let values = HashMap::from([(":v".to_string(), n("1"))]);
        let mut item = Item::new();
        item.insert("a".into(), n("1"));
        assert!(condition
            .evaluate(Some(&item), &HashMap::new(), &values)
            .unwrap());
    }

    #[test]
    fn repeated_chains_evaluate_on_a_small_stack() {
        // Repetition is not nesting: 300 `AND` terms and 600 `+` terms are
        // what the 4096-byte limit leaves, and neither may cost one stack
        // frame per term — parse, evaluate, drop and the path collection all
        // walk a flat `Vec`. 128 KiB catches a regression that recurses.
        std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(|| {
                let values = HashMap::from([(":v".to_string(), n("1"))]);
                let mut item = Item::new();
                item.insert("a".into(), n("1"));

                let condition =
                    ConditionExpression::parse(&vec!["a = :v"; 300].join(" AND ")).unwrap();
                assert!(condition
                    .evaluate(Some(&item), &HashMap::new(), &values)
                    .unwrap());

                let update =
                    UpdateExpression::parse(&format!("SET v = {}", vec![":v"; 600].join(" + ")))
                        .unwrap();
                update
                    .apply(&mut item, &schema(), &HashMap::new(), &values)
                    .unwrap();
                assert_eq!(item["v"].as_n().unwrap(), "600");

                // 10^5 terms is not a crash: the 4096-byte limit rejects the
                // expression before a single tree node is allocated.
                let huge = vec!["a = :v"; 100_000].join(" AND ");
                let error = ConditionExpression::parse(&huge).unwrap_err();
                assert!(
                    error.to_string().contains("maximum allowed size"),
                    "unexpected error: {error}"
                );
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn a_mixed_arithmetic_chain_folds_left_to_right() {
        let update = UpdateExpression::parse("SET v = :ten - :three - :two").unwrap();
        let mut item = Item::new();
        let values = HashMap::from([
            (":ten".to_string(), n("10")),
            (":three".to_string(), n("3")),
            (":two".to_string(), n("2")),
        ]);
        update
            .apply(&mut item, &schema(), &HashMap::new(), &values)
            .unwrap();
        // Left to right: (10 - 3) - 2. A right fold would give 9.
        assert_eq!(item["v"].as_n().unwrap(), "5");

        // Two terms are the smallest chain, three the next; the type error
        // names the two operands of the step that fails.
        assert!(UpdateExpression::parse("SET v = :ten + :two").is_ok());
        let error = UpdateExpression::parse("SET v = :ten + :one + :two")
            .unwrap()
            .apply(
                &mut item,
                &schema(),
                &HashMap::new(),
                &HashMap::from([
                    (":ten".to_string(), n("10")),
                    (":one".to_string(), n("1")),
                    (":two".to_string(), s("x")),
                ]),
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("operand type: N and S"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn update_function_nesting_counts_against_the_limit() {
        // `if_not_exists` and `list_append` recurse, so each enclosing call is
        // one level of the same budget `(` groups and `NOT` share.
        let if_not_exists = |levels: usize| {
            format!(
                "SET v = {}:zero{}",
                "if_not_exists(v, ".repeat(levels),
                ")".repeat(levels)
            )
        };
        assert!(UpdateExpression::parse(&if_not_exists(1)).is_ok());
        assert!(UpdateExpression::parse(&if_not_exists(64)).is_ok());
        let too_deep = UpdateExpression::parse(&if_not_exists(65)).unwrap_err();
        assert!(
            too_deep.to_string().contains("nested too deeply"),
            "unexpected error: {too_deep}"
        );

        let list_append = |levels: usize| {
            format!(
                "SET v = {}:a{}",
                "list_append(".repeat(levels),
                ", :a)".repeat(levels)
            )
        };
        assert!(UpdateExpression::parse(&list_append(1)).is_ok());
        assert!(UpdateExpression::parse(&list_append(64)).is_ok());
        assert!(UpdateExpression::parse(&list_append(65)).is_err());
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
