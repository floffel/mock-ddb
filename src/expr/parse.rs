//! The expression grammar: document paths, condition trees and update clauses.
//!
//! Parsing is a hand-written recursive-descent pass over the token stream from
//! [`super::lex`]. It accepts the whole condition grammar the service accepts —
//! `AND`/`OR`/`NOT`, the comparison operators, `BETWEEN`, `IN`, and the
//! functions `attribute_exists`, `attribute_not_exists`, `attribute_type`,
//! `begins_with`, `contains` and `size` — and the four update clauses `SET`,
//! `REMOVE`, `ADD` and `DELETE` with nested paths, list indices,
//! `if_not_exists` and `list_append`.

use super::lex::{tokenize, Token};
use crate::{Error, Result};

/// Maximum nesting depth, so a hostile expression cannot exhaust the stack.
///
/// One parenthesised group (`(` … `)`) or one `NOT` prefix costs exactly one
/// level, so an expression may wrap its innermost comparison in
/// [`MAX_DEPTH`] such constructs; the next one is rejected. The count is a
/// value threaded through the recursion, never a counter that has to be
/// restored, so no path can double-count a level or leak one.
const MAX_DEPTH: u8 = 64;

/// The most document paths one update expression may write.
///
/// The 4096-byte expression limit in `lex` already bounds this: the shortest
/// entry is a one-character name plus its separator, so no accepted expression
/// names more than about 2000 paths. The constant states that bound where the
/// overlap check relies on it — and keeps the check bounded if the size limit
/// is ever relaxed.
const MAX_UPDATE_TARGETS: usize = 2048;

/// One step of a document path.
///
/// The ordering exists for the overlap check, which sorts paths before
/// comparing neighbours: it is lexicographic over the steps, so a path sorts
/// before every longer path it is a prefix of.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Segment {
    /// A map key or top-level attribute. `#alias` entries are unresolved
    /// placeholders until evaluation.
    Name(String),
    /// A zero-based list index.
    Index(usize),
}

/// A document path: `profile.address.city`, `tags[0]`, `#n.#m`.
pub(crate) type Path = Vec<Segment>;

/// A value in an expression: an attribute, a `:placeholder`, or `size(path)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Operand {
    /// Read an attribute.
    Path(Path),
    /// Read an expression-attribute value, stored without the `:`.
    Value(String),
    /// The size of an attribute's value.
    Size(Path),
}

impl Operand {
    /// Every document path the operand reads.
    pub(crate) fn paths(&self) -> Vec<&Path> {
        match self {
            Self::Path(path) | Self::Size(path) => vec![path],
            Self::Value(_) => Vec::new(),
        }
    }
}

/// A comparison operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CmpOp {
    /// `=`
    Eq,
    /// `<>`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
}

/// A parsed condition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Condition {
    /// `attribute_exists(path)`
    AttributeExists(Path),
    /// `attribute_not_exists(path)`
    AttributeNotExists(Path),
    /// `attribute_type(path, :type)`
    AttributeType(Path, Operand),
    /// `begins_with(path, :prefix)`
    BeginsWith(Path, Operand),
    /// `contains(path, :needle)`
    Contains(Path, Operand),
    /// `left <op> right`
    Compare(Operand, CmpOp, Operand),
    /// `value BETWEEN low AND high`
    Between(Operand, Operand, Operand),
    /// `value IN (:a, :b)`
    In(Operand, Vec<Operand>),
    /// `left AND right`
    And(Box<Condition>, Box<Condition>),
    /// `left OR right`
    Or(Box<Condition>, Box<Condition>),
    /// `NOT condition`
    Not(Box<Condition>),
}

impl Condition {
    /// Every document path the condition reads, in source order.
    pub(crate) fn paths(&self) -> Vec<&Path> {
        let mut paths = Vec::new();
        self.collect_paths(&mut paths);
        paths
    }

    fn collect_paths<'a>(&'a self, paths: &mut Vec<&'a Path>) {
        match self {
            Self::AttributeExists(path) | Self::AttributeNotExists(path) => paths.push(path),
            Self::AttributeType(path, operand)
            | Self::BeginsWith(path, operand)
            | Self::Contains(path, operand) => {
                paths.push(path);
                paths.extend(operand.paths());
            }
            Self::Compare(left, _, right) => {
                paths.extend(left.paths());
                paths.extend(right.paths());
            }
            Self::Between(value, low, high) => {
                paths.extend(value.paths());
                paths.extend(low.paths());
                paths.extend(high.paths());
            }
            Self::In(value, list) => {
                paths.extend(value.paths());
                for entry in list {
                    paths.extend(entry.paths());
                }
            }
            Self::And(left, right) | Self::Or(left, right) => {
                left.collect_paths(paths);
                right.collect_paths(paths);
            }
            Self::Not(inner) => inner.collect_paths(paths),
        }
    }

    /// Split a top-level `AND` chain into its leaves. A condition with any
    /// `OR` or `NOT` at the top yields a single element: the whole condition.
    pub(crate) fn conjuncts(&self) -> Vec<&Condition> {
        match self {
            Self::And(left, right) => {
                let mut conjuncts = left.conjuncts();
                conjuncts.extend(right.conjuncts());
                conjuncts
            }
            other => vec![other],
        }
    }
}

/// The right-hand side of a `SET` assignment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SetValue {
    /// A literal operand: an attribute path, a placeholder or `size(path)`.
    Operand(Operand),
    /// `left + right`
    Plus(Box<Self>, Box<Self>),
    /// `left - right`
    Minus(Box<Self>, Box<Self>),
    /// `if_not_exists(path, fallback)`
    IfNotExists(Path, Box<Self>),
    /// `list_append(first, second)`. Both arguments are full set values, so
    /// `list_append(if_not_exists(items, :empty), :more)` and nested
    /// `list_append` calls parse the same way DynamoDB accepts them.
    ListAppend(Box<Self>, Box<Self>),
}

/// One update clause.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum UpdateClause {
    /// `SET a = :v, b.c = :w`
    Set(Vec<(Path, SetValue)>),
    /// `REMOVE a, b[2]`
    Remove(Vec<Path>),
    /// `ADD counter :one, tags :more`
    Add(Vec<(Path, Operand)>),
    /// `DELETE tags :gone`
    Delete(Vec<(Path, Operand)>),
}

impl UpdateClause {
    /// The paths this clause writes.
    pub(crate) fn target_paths(&self) -> Vec<&Path> {
        match self {
            Self::Set(assignments) => assignments.iter().map(|(path, _)| path).collect(),
            Self::Remove(paths) => paths.iter().collect(),
            Self::Add(pairs) | Self::Delete(pairs) => pairs.iter().map(|(path, _)| path).collect(),
        }
    }

    /// The clause keyword, for error messages.
    pub(crate) fn keyword(&self) -> &'static str {
        match self {
            Self::Set(_) => "SET",
            Self::Remove(_) => "REMOVE",
            Self::Add(_) => "ADD",
            Self::Delete(_) => "DELETE",
        }
    }
}

/// A parsed update expression.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Update {
    /// The clauses, in source order.
    pub(crate) clauses: Vec<UpdateClause>,
}

impl Update {
    /// Every path the update writes.
    pub(crate) fn target_paths(&self) -> Vec<&Path> {
        self.clauses
            .iter()
            .flat_map(UpdateClause::target_paths)
            .collect()
    }
}

/// Parse a condition expression.
pub(crate) fn parse_condition(input: &str) -> Result<Condition> {
    let tokens = tokenize(input)?;
    if tokens.is_empty() {
        return Err(Error::validation(
            "Invalid ConditionExpression: The expression can not be empty",
        ));
    }
    let mut parser = Parser::new(&tokens);
    let condition = parser.condition(0)?;
    if !parser.at_end() {
        return Err(parser.unexpected("end of expression"));
    }
    Ok(condition)
}

/// Parse an update expression.
pub(crate) fn parse_update(input: &str) -> Result<Update> {
    let tokens = tokenize(input)?;
    if tokens.is_empty() {
        return Err(Error::validation(
            "Invalid UpdateExpression: The expression can not be empty",
        ));
    }
    let mut parser = Parser::new(&tokens);
    let mut clauses: Vec<UpdateClause> = Vec::new();
    while !parser.at_end() {
        let Some(Token::Ident(keyword)) = parser.peek() else {
            return Err(parser.unexpected("SET, REMOVE, ADD or DELETE"));
        };
        let keyword = keyword.to_ascii_uppercase();
        if let Some(existing) = clauses
            .iter()
            .find(|clause| clause.keyword() == keyword.as_str())
        {
            return Err(Error::validation(format!(
                "Invalid UpdateExpression: The \"{}\" section can only be used once in an update expression",
                existing.keyword()
            )));
        }
        parser.advance();
        let clause = match keyword.as_str() {
            "SET" => parser.set_clause()?,
            "REMOVE" => parser.remove_clause()?,
            "ADD" => parser.add_or_delete_clause(true)?,
            "DELETE" => parser.add_or_delete_clause(false)?,
            other => {
                return Err(Error::validation(format!(
                    "Invalid UpdateExpression: Syntax error; token: `{other}`, expected SET, REMOVE, ADD or DELETE"
                )));
            }
        };
        clauses.push(clause);
    }
    let update = Update { clauses };
    validate_no_overlap(&update)?;
    Ok(update)
}

/// Parse a projection expression: a comma-separated list of document paths.
pub(crate) fn parse_projection(input: &str) -> Result<Vec<Path>> {
    let tokens = tokenize(input)?;
    if tokens.is_empty() {
        return Err(Error::validation(
            "Invalid ProjectionExpression: The expression can not be empty",
        ));
    }
    let mut parser = Parser::new(&tokens);
    let mut paths = vec![parser.path()?];
    while parser.eat(&Token::Comma) {
        paths.push(parser.path()?);
    }
    if !parser.at_end() {
        return Err(parser.unexpected("end of expression"));
    }
    Ok(paths)
}

/// Reject two target paths where one is a prefix of the other, as the service
/// does.
///
/// Sorting first turns this from a quadratic pairwise scan into one pass over
/// the neighbours: a path that would sort between `a` and a longer path that
/// starts with `a` has to agree with `a` on every one of its own steps, so it
/// starts with `a` too and is caught just the same. Equal paths sort next to
/// each other and are caught by the same test.
fn validate_no_overlap(update: &Update) -> Result<()> {
    let mut paths = update.target_paths();
    if paths.len() > MAX_UPDATE_TARGETS {
        return Err(Error::validation(format!(
            "Invalid UpdateExpression: The expression writes more than {MAX_UPDATE_TARGETS} document paths"
        )));
    }
    paths.sort_unstable();
    for pair in paths.windows(2) {
        // Sorted, only the earlier path can prefix the later one.
        if is_prefix(pair[0], pair[1]) {
            return Err(Error::validation(
                "Invalid UpdateExpression: Two document paths overlap with each other; must remove or rewrite one of these paths",
            ));
        }
    }
    Ok(())
}

/// Whether `prefix` is a path prefix of `path` (or equal to it).
fn is_prefix(prefix: &Path, path: &Path) -> bool {
    prefix.len() <= path.len() && path.iter().zip(prefix).all(|(a, b)| a == b)
}

/// The level one syntactic nesting construct deeper than `level`, or the
/// "nested too deeply" error once [`MAX_DEPTH`] constructs already enclose the
/// comparison.
///
/// Called exactly once per `(` group and once per `NOT` prefix — the two
/// constructs that recurse — so a level is worth one unit and never two.
fn nested(level: u8) -> Result<u8> {
    match level.checked_add(1) {
        Some(inner) if inner <= MAX_DEPTH => Ok(inner),
        _ => Err(Error::validation(
            "Invalid expression: the expression is nested too deeply",
        )),
    }
}

struct Parser<'a> {
    tokens: &'a [Token],
    position: usize,
}

impl<'a> Parser<'a> {
    fn new(tokens: &'a [Token]) -> Self {
        Self {
            tokens,
            position: 0,
        }
    }

    fn at_end(&self) -> bool {
        self.position >= self.tokens.len()
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.position)
    }

    fn peek_at(&self, offset: usize) -> Option<&Token> {
        self.tokens.get(self.position + offset)
    }

    fn advance(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.position).cloned();
        if token.is_some() {
            self.position += 1;
        }
        token
    }

    fn eat(&mut self, expected: &Token) -> bool {
        if self.peek() == Some(expected) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn peek_keyword(&self, keyword: &str) -> bool {
        matches!(self.peek(), Some(Token::Ident(name)) if name.eq_ignore_ascii_case(keyword))
    }

    fn eat_keyword(&mut self, keyword: &str) -> bool {
        if self.peek_keyword(keyword) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    /// Whether the next token starts a new update clause.
    fn at_clause_boundary(&self) -> bool {
        matches!(
            self.peek(),
            Some(Token::Ident(name))
                if ["SET", "REMOVE", "ADD", "DELETE"]
                    .iter()
                    .any(|keyword| name.eq_ignore_ascii_case(keyword))
        )
    }

    fn expect(&mut self, expected: &Token) -> Result<()> {
        if self.eat(expected) {
            Ok(())
        } else {
            Err(self.unexpected(&expected.describe()))
        }
    }

    fn unexpected(&self, expected: &str) -> Error {
        match self.peek() {
            Some(token) => Error::validation(format!(
                "Invalid expression: Syntax error; found {}, expected {expected}",
                token.describe()
            )),
            None => Error::validation(format!(
                "Invalid expression: Syntax error; the expression ended, expected {expected}"
            )),
        }
    }

    // -- conditions ---------------------------------------------------------

    fn condition(&mut self, level: u8) -> Result<Condition> {
        let mut left = self.and_condition(level)?;
        while self.eat_keyword("OR") {
            let right = self.and_condition(level)?;
            left = Condition::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn and_condition(&mut self, level: u8) -> Result<Condition> {
        let mut left = self.not_condition(level)?;
        while self.eat_keyword("AND") {
            let right = self.not_condition(level)?;
            left = Condition::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn not_condition(&mut self, level: u8) -> Result<Condition> {
        let condition = if self.eat_keyword("NOT") {
            Condition::Not(Box::new(self.not_condition(nested(level)?)?))
        } else {
            self.primary_condition(level)?
        };
        Ok(condition)
    }

    fn primary_condition(&mut self, level: u8) -> Result<Condition> {
        if self.peek() == Some(&Token::LParen) {
            self.advance();
            let inner = self.condition(nested(level)?)?;
            self.expect(&Token::RParen)?;
            return Ok(inner);
        }
        if let Some(Token::Ident(name)) = self.peek() {
            let lowercase = name.to_ascii_lowercase();
            let is_function = matches!(
                lowercase.as_str(),
                "attribute_exists"
                    | "attribute_not_exists"
                    | "attribute_type"
                    | "begins_with"
                    | "contains"
            );
            if is_function && self.peek_at(1) == Some(&Token::LParen) {
                return self.condition_function(&lowercase);
            }
        }
        self.comparison()
    }

    fn condition_function(&mut self, name: &str) -> Result<Condition> {
        let _ = self.advance();
        self.expect(&Token::LParen)?;
        let condition = match name {
            "attribute_exists" => {
                let path = self.path()?;
                Condition::AttributeExists(path)
            }
            "attribute_not_exists" => {
                let path = self.path()?;
                Condition::AttributeNotExists(path)
            }
            "attribute_type" => {
                let path = self.path()?;
                self.expect(&Token::Comma)?;
                let operand = self.operand()?;
                Condition::AttributeType(path, operand)
            }
            "begins_with" => {
                let path = self.path()?;
                self.expect(&Token::Comma)?;
                let operand = self.operand()?;
                Condition::BeginsWith(path, operand)
            }
            "contains" => {
                let path = self.path()?;
                self.expect(&Token::Comma)?;
                let operand = self.operand()?;
                Condition::Contains(path, operand)
            }
            _ => return Err(self.unexpected("a condition function")),
        };
        self.expect(&Token::RParen)?;
        Ok(condition)
    }

    fn comparison(&mut self) -> Result<Condition> {
        let left = self.operand()?;
        if self.eat_keyword("BETWEEN") {
            let low = self.operand()?;
            if !self.eat_keyword("AND") {
                return Err(self.unexpected("AND"));
            }
            let high = self.operand()?;
            return Ok(Condition::Between(left, low, high));
        }
        if self.eat_keyword("IN") {
            self.expect(&Token::LParen)?;
            let mut entries = vec![self.operand()?];
            while self.eat(&Token::Comma) {
                entries.push(self.operand()?);
            }
            self.expect(&Token::RParen)?;
            return Ok(Condition::In(left, entries));
        }
        let operator = match self.peek() {
            Some(Token::Eq) => CmpOp::Eq,
            Some(Token::Ne) => CmpOp::Ne,
            Some(Token::Lt) => CmpOp::Lt,
            Some(Token::Le) => CmpOp::Le,
            Some(Token::Gt) => CmpOp::Gt,
            Some(Token::Ge) => CmpOp::Ge,
            _ => return Err(self.unexpected("a comparison operator")),
        };
        self.advance();
        let right = self.operand()?;
        Ok(Condition::Compare(left, operator, right))
    }

    // -- operands and paths -------------------------------------------------

    fn operand(&mut self) -> Result<Operand> {
        match self.advance() {
            Some(Token::Value(name)) => Ok(Operand::Value(name)),
            Some(Token::Ident(name))
                if name.eq_ignore_ascii_case("size") && self.peek() == Some(&Token::LParen) =>
            {
                self.advance();
                let path = self.path()?;
                self.expect(&Token::RParen)?;
                Ok(Operand::Size(path))
            }
            Some(Token::Ident(name)) => Ok(Operand::Path(self.path_from(Segment::Name(name))?)),
            Some(Token::Name(name)) => Ok(Operand::Path(
                self.path_from(Segment::Name(format!("#{name}")))?,
            )),
            _ => Err(self.unexpected("an attribute path or a value placeholder")),
        }
    }

    fn path(&mut self) -> Result<Path> {
        match self.advance() {
            Some(Token::Ident(name)) => self.path_from(Segment::Name(name)),
            Some(Token::Name(name)) => self.path_from(Segment::Name(format!("#{name}"))),
            _ => Err(self.unexpected("an attribute path")),
        }
    }

    fn path_from(&mut self, first: Segment) -> Result<Path> {
        let mut segments = vec![first];
        loop {
            if self.eat(&Token::Dot) {
                match self.advance() {
                    Some(Token::Ident(name)) => segments.push(Segment::Name(name)),
                    Some(Token::Name(name)) => segments.push(Segment::Name(format!("#{name}"))),
                    _ => return Err(self.unexpected("an attribute name")),
                }
            } else if self.eat(&Token::LBracket) {
                let index = match self.advance() {
                    Some(Token::Ident(digits))
                        if !digits.is_empty()
                            && digits.bytes().all(|byte| byte.is_ascii_digit()) =>
                    {
                        digits.parse::<usize>().map_err(|_| {
                            Error::validation("Invalid expression: list index is out of range")
                        })?
                    }
                    _ => {
                        return Err(Error::validation(
                            "Invalid expression: a list index must be a non-negative integer",
                        ));
                    }
                };
                self.expect(&Token::RBracket)?;
                segments.push(Segment::Index(index));
            } else {
                break;
            }
        }
        Ok(segments)
    }

    // -- updates ------------------------------------------------------------

    fn set_clause(&mut self) -> Result<UpdateClause> {
        let mut assignments = Vec::new();
        loop {
            let path = self.path()?;
            self.expect(&Token::Eq)?;
            let value = self.set_value()?;
            assignments.push((path, value));
            if !self.eat(&Token::Comma) || self.at_clause_boundary() || self.at_end() {
                break;
            }
        }
        Ok(UpdateClause::Set(assignments))
    }

    fn remove_clause(&mut self) -> Result<UpdateClause> {
        let mut paths = Vec::new();
        loop {
            paths.push(self.path()?);
            if !self.eat(&Token::Comma) || self.at_clause_boundary() || self.at_end() {
                break;
            }
        }
        Ok(UpdateClause::Remove(paths))
    }

    fn add_or_delete_clause(&mut self, is_add: bool) -> Result<UpdateClause> {
        let mut pairs = Vec::new();
        loop {
            let path = self.path()?;
            let operand = self.operand()?;
            pairs.push((path, operand));
            if !self.eat(&Token::Comma) || self.at_clause_boundary() || self.at_end() {
                break;
            }
        }
        Ok(if is_add {
            UpdateClause::Add(pairs)
        } else {
            UpdateClause::Delete(pairs)
        })
    }

    fn set_value(&mut self) -> Result<SetValue> {
        let mut value = self.term()?;
        loop {
            if self.eat(&Token::Plus) {
                let right = self.term()?;
                value = SetValue::Plus(Box::new(value), Box::new(right));
            } else if self.eat(&Token::Minus) {
                let right = self.term()?;
                value = SetValue::Minus(Box::new(value), Box::new(right));
            } else {
                break;
            }
        }
        Ok(value)
    }

    fn term(&mut self) -> Result<SetValue> {
        if self.peek_keyword("if_not_exists") && self.peek_at(1) == Some(&Token::LParen) {
            self.advance();
            self.advance();
            let path = self.path()?;
            self.expect(&Token::Comma)?;
            let fallback = self.set_value()?;
            self.expect(&Token::RParen)?;
            return Ok(SetValue::IfNotExists(path, Box::new(fallback)));
        }
        if self.peek_keyword("list_append") && self.peek_at(1) == Some(&Token::LParen) {
            self.advance();
            self.advance();
            let first = self.term()?;
            self.expect(&Token::Comma)?;
            let second = self.term()?;
            self.expect(&Token::RParen)?;
            return Ok(SetValue::ListAppend(Box::new(first), Box::new(second)));
        }
        Ok(SetValue::Operand(self.operand()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `count` distinct top-level paths: `a0`, `a1`, ...
    fn distinct_paths(count: usize) -> Vec<Path> {
        (0..count)
            .map(|index| vec![Segment::Name(format!("a{index}"))])
            .collect()
    }

    fn overlap(paths: Vec<Path>) -> Result<()> {
        validate_no_overlap(&Update {
            clauses: vec![UpdateClause::Remove(paths)],
        })
    }

    fn name(text: &str) -> Segment {
        Segment::Name(text.to_string())
    }

    #[test]
    fn accepts_the_target_path_limit_and_rejects_one_more() {
        assert!(overlap(distinct_paths(MAX_UPDATE_TARGETS)).is_ok());

        let error = overlap(distinct_paths(MAX_UPDATE_TARGETS + 1)).unwrap_err();
        assert!(
            error.to_string().contains("more than 2048 document paths"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn finds_an_overlap_that_is_not_a_neighbour_in_source_order() {
        assert!(overlap(vec![
            vec![name("a")],
            vec![name("b")],
            vec![name("a"), name("b")],
        ])
        .is_err());
    }

    #[test]
    fn accepts_siblings_and_names_that_merely_share_a_string_prefix() {
        assert!(overlap(vec![
            vec![name("a"), name("b")],
            vec![name("a"), name("c")],
            vec![name("ab")],
            vec![name("a"), Segment::Index(0)],
            vec![name("a"), Segment::Index(1)],
        ])
        .is_ok());
    }

    #[test]
    fn rejects_a_path_and_anything_below_it_in_either_source_order() {
        let child = vec![name("a"), name("b")];
        assert!(overlap(vec![vec![name("a")], child.clone()]).is_err());
        assert!(overlap(vec![child, vec![name("a")]]).is_err());
        assert!(overlap(vec![
            vec![name("a"), Segment::Index(0)],
            vec![name("a"), Segment::Index(0), name("b")],
        ])
        .is_err());
        assert!(overlap(vec![vec![name("a")], vec![name("a")]]).is_err());
    }

    #[test]
    fn nested_counts_one_level_and_stops_at_the_maximum() {
        for level in 0..MAX_DEPTH {
            assert_eq!(nested(level), Ok(level + 1));
        }
        assert_eq!(
            nested(MAX_DEPTH).unwrap_err().to_string(),
            "Invalid expression: the expression is nested too deeply"
        );
        // A level counter at its type's maximum reports the limit instead of
        // overflowing the `+=`.
        assert!(nested(u8::MAX).is_err());
    }
}
