//! Parser for the PartiQL subset DynamoDB accepts.
//!
//! The statement is parsed into typed nodes — a [`Statement`] with one variant
//! per accepted verb, [`Cond`] for the `WHERE` clause and [`Doc`] for the tuple
//! of an `INSERT`. Nothing is rendered into an expression string here; that is
//! [`super::lower`]'s job, because it needs the table's key schema to decide
//! which part of a `WHERE` becomes the key condition and which part becomes a
//! filter.
//!
//! Hand-written recursive descent, for the reasons recorded above `mod lex`:
//! the messages are the contract, and the parse is bounded by the statement
//! size before it allocates.

use aws_sdk_dynamodb::types::AttributeValue;

use super::lex::{tokenize, Token};
use crate::expr::{CmpOp, Path, Segment};
use crate::{Error, Result};

/// A parsed PartiQL statement.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Statement {
    /// `SELECT … FROM …`
    Select(Select),
    /// `INSERT INTO … VALUE …`
    Insert(Insert),
    /// `UPDATE … SET … [REMOVE …]`
    Update(Update),
    /// `DELETE FROM …`
    Delete(Delete),
}

impl Statement {
    /// The table the statement targets.
    pub(crate) fn table_name(&self) -> &str {
        match self {
            Self::Select(select) => &select.table,
            Self::Insert(insert) => &insert.table,
            Self::Update(update) => &update.table,
            Self::Delete(delete) => &delete.table,
        }
    }

    /// Whether the statement reads.
    pub(crate) fn is_read(&self) -> bool {
        matches!(self, Self::Select(_))
    }
}

/// `SELECT projection FROM table[.index] [WHERE condition]`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Select {
    /// The table to read.
    pub(crate) table: String,
    /// The index to read instead of the table, from `"table"."index"`.
    pub(crate) index: Option<String>,
    /// The projected paths, empty for `SELECT *`.
    pub(crate) projection: Vec<Path>,
    /// The `WHERE` clause.
    pub(crate) condition: Option<Cond>,
}

/// `INSERT INTO table VALUE { … }`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Insert {
    /// The table to write.
    pub(crate) table: String,
    /// The item as it was written, parameters unresolved.
    pub(crate) item: Vec<(String, Doc)>,
}

/// `UPDATE table SET … [REMOVE …] [WHERE condition]`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Update {
    /// The table to write.
    pub(crate) table: String,
    /// The `SET` assignments, in source order.
    pub(crate) assignments: Vec<(Path, Vec<(Sign, Value)>)>,
    /// The attributes `REMOVE` names, in source order.
    pub(crate) remove: Vec<Path>,
    /// The `WHERE` clause.
    pub(crate) condition: Option<Cond>,
}

/// `DELETE FROM table [WHERE condition]`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Delete {
    /// The table to write.
    pub(crate) table: String,
    /// The `WHERE` clause.
    pub(crate) condition: Option<Cond>,
}

/// The operator that joins two terms of an arithmetic right-hand side.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Sign {
    /// `+`
    Plus,
    /// `-`
    Minus,
}

impl Sign {
    /// The character this sign renders as.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Plus => "+",
            Self::Minus => "-",
        }
    }
}

/// One value inside a `WHERE`, an assignment or a tuple: a document path, a
/// positional parameter or a literal.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Value {
    /// `profile.city`
    Path(Path),
    /// `size(tags)`
    Size(Path),
    /// `?` — the n-th positional parameter, counting from zero.
    Parameter(usize),
    /// `'text'`, `1.5`, `true`, `null`, `[…]`, `{…}`
    Literal(AttributeValue),
}

/// A `WHERE` clause, as the service's PartiQL dialect allows it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Cond {
    /// `left <op> right`
    Compare(Value, CmpOp, Value),
    /// `value BETWEEN low AND high`
    Between(Value, Value, Value),
    /// `value IN (a, b, …)`
    In(Value, Vec<Value>),
    /// `value IS [NOT] MISSING`: whether the attribute is absent.
    Missing {
        /// The operand that is tested.
        value: Value,
        /// `true` for `IS NOT MISSING`.
        negated: bool,
    },
    /// `value IS [NOT] NULL`: whether the attribute holds DynamoDB's NULL
    /// value.
    ///
    /// `NULL` is a stored value, like any other, so this is not the same
    /// question as [`Cond::Missing`]: an absent attribute is neither `NULL`
    /// nor `NOT NULL`, exactly as in SQL.
    Null {
        /// The operand that is tested.
        value: Value,
        /// `true` for `IS NOT NULL`.
        negated: bool,
    },
    /// `begins_with`, `contains`, `attribute_exists`, `attribute_not_exists`
    /// and `attribute_type`.
    Function(Function, Vec<Value>),
    /// `left AND right AND …`, in source order.
    And(Vec<Cond>),
    /// `left OR right OR …`, in source order.
    Or(Vec<Cond>),
    /// `NOT condition`
    Not(Box<Cond>),
}

impl Cond {
    /// The conjuncts of a top-level `AND`, or the condition itself.
    pub(crate) fn conjuncts(&self) -> Vec<&Cond> {
        match self {
            Self::And(terms) => terms.iter().collect(),
            other => vec![other],
        }
    }
}

/// The PartiQL functions the service's `WHERE` clause accepts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Function {
    /// `begins_with(path, prefix)`
    BeginsWith,
    /// `contains(path, needle)`
    Contains,
    /// `attribute_exists(path)`
    AttributeExists,
    /// `attribute_not_exists(path)`
    AttributeNotExists,
    /// `attribute_type(path, type)`
    AttributeType,
}

/// A value inside an `INSERT` tuple, parameters unresolved.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Doc {
    /// A value that needs no resolution.
    Literal(AttributeValue),
    /// `?`
    Parameter(usize),
    /// `[…]`
    List(Vec<Doc>),
    /// `{…}`
    Map(Vec<(String, Doc)>),
}

impl Doc {
    /// Replace every parameter with its value.
    pub(crate) fn resolve(&self, parameters: &[AttributeValue]) -> Result<AttributeValue> {
        match self {
            Self::Literal(value) => Ok(value.clone()),
            Self::Parameter(index) => parameters.get(*index).cloned().ok_or_else(|| {
                Error::validation("Number of parameters in request and statement does not match")
            }),
            Self::List(elements) => elements
                .iter()
                .map(|element| element.resolve(parameters))
                .collect::<Result<Vec<_>>>()
                .map(AttributeValue::L),
            Self::Map(entries) => entries
                .iter()
                .map(|(name, value)| Ok((name.clone(), value.resolve(parameters)?)))
                .collect::<Result<std::collections::HashMap<_, _>>>()
                .map(AttributeValue::M),
        }
    }
}

/// A statement together with the number of parameters it binds.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Parsed {
    /// The statement itself.
    pub(crate) statement: Statement,
    /// How many `?` placeholders it contains.
    pub(crate) parameters: usize,
}

/// Parse one statement.
pub(crate) fn parse(input: &str) -> Result<Parsed> {
    let mut parser = Parser {
        tokens: tokenize(input)?,
        position: 0,
        parameters: 0,
    };
    let statement = parser.statement()?;
    parser.end()?;
    Ok(Parsed {
        statement,
        parameters: parser.parameters,
    })
}

/// A cursor over the token stream.
struct Parser {
    tokens: Vec<Token>,
    position: usize,
    parameters: usize,
}

impl Parser {
    /// The next token, without consuming it.
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.position)
    }

    /// The token after the next one, without consuming either.
    fn peek_second(&self) -> Option<&Token> {
        self.tokens.get(self.position + 1)
    }

    /// Consume the next token.
    fn advance(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.position).cloned();
        if token.is_some() {
            self.position += 1;
        }
        token
    }

    /// Whether the next token is the keyword `word`, case-insensitively.
    fn at_keyword(&self, word: &str) -> bool {
        matches!(self.peek(), Some(Token::Ident(found)) if found.eq_ignore_ascii_case(word))
    }

    /// Consume the keyword `word`, or fail naming it.
    fn expect_keyword(&mut self, word: &str) -> Result<()> {
        if self.at_keyword(word) {
            self.position += 1;
            return Ok(());
        }
        Err(self.unexpected(word))
    }

    /// Consume `token`, or fail naming `expected`.
    fn expect(&mut self, token: &Token, expected: &str) -> Result<()> {
        if self.peek() == Some(token) {
            self.position += 1;
            return Ok(());
        }
        Err(self.unexpected(expected))
    }

    /// Fail unless the whole input has been consumed.
    fn end(&self) -> Result<()> {
        if self.position >= self.tokens.len() {
            return Ok(());
        }
        Err(self.unexpected("end of statement"))
    }

    /// The syntax error the service would raise at this position.
    fn unexpected(&self, expected: &str) -> Error {
        let token = self
            .peek()
            .map_or_else(|| "end of statement".to_string(), Token::describe);
        Error::validation(format!(
            "Statement wasn't well formed, can't be processed: Syntax error; token: {token}; expected {expected}"
        ))
    }

    /// Consume a `?`, returning its parameter index.
    fn parameter(&mut self) -> usize {
        self.position += 1;
        let index = self.parameters;
        self.parameters += 1;
        index
    }

    /// Parse `SELECT …`, `INSERT …`, `UPDATE …` or `DELETE …`.
    fn statement(&mut self) -> Result<Statement> {
        if self.at_keyword("SELECT") {
            return self.select().map(Statement::Select);
        }
        if self.at_keyword("INSERT") {
            return self.insert().map(Statement::Insert);
        }
        if self.at_keyword("UPDATE") {
            return self.update().map(Statement::Update);
        }
        if self.at_keyword("DELETE") {
            return self.delete().map(Statement::Delete);
        }
        Err(self.unexpected("SELECT, INSERT, UPDATE or DELETE"))
    }

    /// Parse the body of a `SELECT`.
    fn select(&mut self) -> Result<Select> {
        self.expect_keyword("SELECT")?;
        let projection = if self.peek() == Some(&Token::Star) {
            self.position += 1;
            Vec::new()
        } else {
            let mut paths = vec![self.path()?];
            while self.peek() == Some(&Token::Comma) {
                self.position += 1;
                paths.push(self.path()?);
            }
            paths
        };
        self.expect_keyword("FROM")?;
        let (table, index) = self.table_reference()?;
        let condition = self.where_clause()?;
        Ok(Select {
            table,
            index,
            projection,
            condition,
        })
    }

    /// Parse `table["."index]`.
    fn table_reference(&mut self) -> Result<(String, Option<String>)> {
        let table = self.name()?;
        if self.peek() != Some(&Token::Dot) {
            return Ok((table, None));
        }
        self.position += 1;
        let index = self.name()?;
        Ok((table, Some(index)))
    }

    /// Parse an optional `WHERE` clause.
    fn where_clause(&mut self) -> Result<Option<Cond>> {
        if !self.at_keyword("WHERE") {
            return Ok(None);
        }
        self.position += 1;
        Ok(Some(self.or_condition()?))
    }

    /// Parse the body of an `INSERT`.
    fn insert(&mut self) -> Result<Insert> {
        self.expect_keyword("INSERT")?;
        self.expect_keyword("INTO")?;
        let (table, index) = self.table_reference()?;
        if index.is_some() {
            return Err(Error::validation(
                "Statement wasn't well formed, can't be processed: an INSERT targets a table, not an index",
            ));
        }
        self.expect_keyword("VALUE")?;
        let item = self.tuple()?;
        let condition = self.where_clause()?;
        if condition.is_some() {
            return Err(Error::validation(
                "Statement wasn't well formed, can't be processed: an INSERT has no WHERE clause",
            ));
        }
        Ok(Insert { table, item })
    }

    /// Parse the body of an `UPDATE`.
    fn update(&mut self) -> Result<Update> {
        self.expect_keyword("UPDATE")?;
        let (table, index) = self.table_reference()?;
        if index.is_some() {
            return Err(Error::validation(
                "Statement wasn't well formed, can't be processed: an UPDATE targets a table, not an index",
            ));
        }
        self.expect_keyword("SET")?;
        let mut assignments: Vec<(Path, Vec<(Sign, Value)>)> = Vec::new();
        loop {
            let target = self.path()?;
            if assignments.iter().any(|(existing, _)| *existing == target) {
                return Err(Error::validation(format!(
                    "Two document paths overlap with each other; must remove or rewrite one of these paths. [{}]",
                    render_path(&target)
                )));
            }
            self.expect(&Token::Eq, "`=`")?;
            let mut terms = vec![(Sign::Plus, self.arithmetic_term()?)];
            while let Some(sign) = self.sign() {
                self.position += 1;
                terms.push((sign, self.arithmetic_term()?));
            }
            assignments.push((target, terms));
            if self.peek() != Some(&Token::Comma) {
                break;
            }
            self.position += 1;
        }
        let mut remove = Vec::new();
        if self.at_keyword("REMOVE") {
            self.position += 1;
            loop {
                remove.push(self.path()?);
                if self.peek() != Some(&Token::Comma) {
                    break;
                }
                self.position += 1;
            }
        }
        let condition = self.where_clause()?;
        Ok(Update {
            table,
            assignments,
            remove,
            condition,
        })
    }

    /// Parse the `+` or `-` that joins two arithmetic terms, if there is one.
    fn sign(&self) -> Option<Sign> {
        match self.peek() {
            Some(Token::Plus) => Some(Sign::Plus),
            Some(Token::Minus) => Some(Sign::Minus),
            _ => None,
        }
    }

    /// Parse one term of an arithmetic right-hand side.
    fn arithmetic_term(&mut self) -> Result<Value> {
        self.operand()
    }

    /// Parse the body of a `DELETE`.
    fn delete(&mut self) -> Result<Delete> {
        self.expect_keyword("DELETE")?;
        self.expect_keyword("FROM")?;
        let (table, index) = self.table_reference()?;
        if index.is_some() {
            return Err(Error::validation(
                "Statement wasn't well formed, can't be processed: a DELETE targets a table, not an index",
            ));
        }
        let condition = self.where_clause()?;
        Ok(Delete { table, condition })
    }

    /// Parse `{ 'name': value, … }`.
    fn tuple(&mut self) -> Result<Vec<(String, Doc)>> {
        self.expect(&Token::LBrace, "`{`")?;
        let mut entries: Vec<(String, Doc)> = Vec::new();
        if self.peek() == Some(&Token::RBrace) {
            return Err(Error::validation(
                "Statement wasn't well formed, can't be processed: an item needs at least one attribute",
            ));
        }
        loop {
            let Some(name) = self.try_attribute_name() else {
                return Err(self.unexpected("an attribute name"));
            };
            if entries.iter().any(|(existing, _)| *existing == name) {
                return Err(Error::validation(format!(
                    "Duplicate attribute name `{name}` in the item"
                )));
            }
            self.expect(&Token::Colon, "`:`")?;
            let value = self.document()?;
            entries.push((name, value));
            if self.peek() == Some(&Token::Comma) {
                self.position += 1;
                continue;
            }
            break;
        }
        self.expect(&Token::RBrace, "`}`")?;
        Ok(entries)
    }

    /// Parse one value of an `INSERT` tuple.
    fn document(&mut self) -> Result<Doc> {
        match self.peek().cloned() {
            Some(Token::Parameter) => Ok(Doc::Parameter(self.parameter())),
            Some(Token::LBrace) => self.tuple().map(Doc::Map),
            Some(Token::LBracket) => {
                self.position += 1;
                let mut elements = Vec::new();
                if self.peek() == Some(&Token::RBracket) {
                    self.position += 1;
                    return Ok(Doc::List(elements));
                }
                loop {
                    elements.push(self.document()?);
                    if self.peek() == Some(&Token::Comma) {
                        self.position += 1;
                        continue;
                    }
                    break;
                }
                self.expect(&Token::RBracket, "`]`")?;
                Ok(Doc::List(elements))
            }
            Some(_) => self.literal().map(Doc::Literal),
            None => Err(self.unexpected("a value")),
        }
    }

    /// Parse a literal value.
    fn literal(&mut self) -> Result<AttributeValue> {
        let negated = if self.peek() == Some(&Token::Minus) {
            self.position += 1;
            true
        } else {
            false
        };
        let value = match self.peek().cloned() {
            Some(Token::Text(text)) => AttributeValue::S(text),
            Some(Token::Number(text)) => {
                let text = if negated { format!("-{text}") } else { text };
                crate::number::validate(&text)?;
                AttributeValue::N(text)
            }
            Some(Token::Ident(word)) if !negated && word.eq_ignore_ascii_case("TRUE") => {
                AttributeValue::Bool(true)
            }
            Some(Token::Ident(word)) if !negated && word.eq_ignore_ascii_case("FALSE") => {
                AttributeValue::Bool(false)
            }
            Some(Token::Ident(word)) if !negated && word.eq_ignore_ascii_case("NULL") => {
                AttributeValue::Null(true)
            }
            _ => return Err(self.unexpected("a literal value")),
        };
        self.position += 1;
        Ok(value)
    }

    /// Parse a bare or quoted name — a table, an index or an attribute.
    fn name(&mut self) -> Result<String> {
        self.try_name().ok_or_else(|| self.unexpected("a name"))
    }

    /// Consume a bare or double-quoted name, if one is next.
    fn try_name(&mut self) -> Option<String> {
        match self.peek() {
            Some(Token::Ident(name) | Token::Quoted(name)) => {
                let name = name.clone();
                self.position += 1;
                Some(name)
            }
            _ => None,
        }
    }

    /// Consume an attribute name of an `INSERT` tuple, which may also be
    /// written in single quotes — that is how PartiQL spells a tuple key.
    fn try_attribute_name(&mut self) -> Option<String> {
        match self.peek() {
            Some(Token::Text(name)) => {
                let name = name.clone();
                self.position += 1;
                Some(name)
            }
            _ => self.try_name(),
        }
    }

    /// Parse a document path: `name`, `"name"`, `name.child`, `name[0]`.
    fn path(&mut self) -> Result<Path> {
        let mut path = vec![Segment::Name(self.name()?)];
        loop {
            match self.peek() {
                Some(Token::Dot) => {
                    self.position += 1;
                    path.push(Segment::Name(self.name()?));
                }
                Some(Token::LBracket) => {
                    self.position += 1;
                    let index = match self.advance() {
                        Some(Token::Number(text)) => text.parse::<usize>().map_err(|_| {
                            Error::validation(format!(
                                "Invalid document path: `[{text}]` is not a list index"
                            ))
                        })?,
                        _ => return Err(self.unexpected("a list index")),
                    };
                    self.expect(&Token::RBracket, "`]`")?;
                    path.push(Segment::Index(index));
                }
                _ => return Ok(path),
            }
        }
    }

    /// Parse a `WHERE` clause's `OR` chain.
    fn or_condition(&mut self) -> Result<Cond> {
        let mut terms = vec![self.and_condition()?];
        while self.at_keyword("OR") {
            self.position += 1;
            terms.push(self.and_condition()?);
        }
        if terms.len() == 1 {
            if let Some(single) = terms.pop() {
                return Ok(single);
            }
        }
        Ok(Cond::Or(terms))
    }

    /// Parse a `WHERE` clause's `AND` chain.
    fn and_condition(&mut self) -> Result<Cond> {
        let mut terms = vec![self.not_condition()?];
        while self.at_keyword("AND") {
            self.position += 1;
            terms.push(self.not_condition()?);
        }
        if terms.len() == 1 {
            if let Some(single) = terms.pop() {
                return Ok(single);
            }
        }
        Ok(Cond::And(terms))
    }

    /// Parse an optional `NOT` and the predicate it applies to.
    fn not_condition(&mut self) -> Result<Cond> {
        if self.at_keyword("NOT") {
            self.position += 1;
            return Ok(Cond::Not(Box::new(self.not_condition()?)));
        }
        self.predicate()
    }

    /// Parse one predicate.
    fn predicate(&mut self) -> Result<Cond> {
        if self.peek() == Some(&Token::LParen) {
            self.position += 1;
            let condition = self.or_condition()?;
            self.expect(&Token::RParen, "`)`")?;
            return Ok(condition);
        }
        if let Some(function) = self.function_call()? {
            return Ok(function);
        }
        let left = self.operand()?;
        match self.peek() {
            Some(Token::Eq) => self.comparison(left, CmpOp::Eq),
            Some(Token::Ne) => self.comparison(left, CmpOp::Ne),
            Some(Token::Lt) => self.comparison(left, CmpOp::Lt),
            Some(Token::Le) => self.comparison(left, CmpOp::Le),
            Some(Token::Gt) => self.comparison(left, CmpOp::Gt),
            Some(Token::Ge) => self.comparison(left, CmpOp::Ge),
            _ if self.at_keyword("BETWEEN") => {
                self.position += 1;
                let low = self.operand()?;
                self.expect_keyword("AND")?;
                let high = self.operand()?;
                Ok(Cond::Between(left, low, high))
            }
            _ if self.at_keyword("IN") => {
                self.position += 1;
                self.expect(&Token::LParen, "`(`")?;
                let mut members = vec![self.operand()?];
                while self.peek() == Some(&Token::Comma) {
                    self.position += 1;
                    members.push(self.operand()?);
                }
                self.expect(&Token::RParen, "`)`")?;
                Ok(Cond::In(left, members))
            }
            _ if self.at_keyword("IS") => {
                self.position += 1;
                let negated = if self.at_keyword("NOT") {
                    self.position += 1;
                    true
                } else {
                    false
                };
                if self.at_keyword("MISSING") {
                    self.position += 1;
                    return Ok(Cond::Missing {
                        value: left,
                        negated,
                    });
                }
                if self.at_keyword("NULL") {
                    self.position += 1;
                    return Ok(Cond::Null {
                        value: left,
                        negated,
                    });
                }
                Err(self.unexpected("MISSING or NULL"))
            }
            _ => Err(self.unexpected("a comparison operator")),
        }
    }

    /// Parse `<op> operand` after `left`.
    fn comparison(&mut self, left: Value, operator: CmpOp) -> Result<Cond> {
        self.position += 1;
        let right = self.operand()?;
        Ok(Cond::Compare(left, operator, right))
    }

    /// Parse a function call used as a predicate, if the next tokens are one.
    fn function_call(&mut self) -> Result<Option<Cond>> {
        let Some(Token::Ident(word)) = self.peek() else {
            return Ok(None);
        };
        if self.peek_second() != Some(&Token::LParen) {
            return Ok(None);
        }
        let function = if word.eq_ignore_ascii_case("begins_with") {
            Some(Function::BeginsWith)
        } else if word.eq_ignore_ascii_case("contains") {
            Some(Function::Contains)
        } else if word.eq_ignore_ascii_case("attribute_exists") {
            Some(Function::AttributeExists)
        } else if word.eq_ignore_ascii_case("attribute_not_exists") {
            Some(Function::AttributeNotExists)
        } else if word.eq_ignore_ascii_case("attribute_type") {
            Some(Function::AttributeType)
        } else {
            None
        };
        let Some(function) = function else {
            return Ok(None);
        };
        self.position += 2;
        let mut arguments = vec![self.operand()?];
        while self.peek() == Some(&Token::Comma) {
            self.position += 1;
            arguments.push(self.operand()?);
        }
        self.expect(&Token::RParen, "`)`")?;
        let arity = match function {
            Function::AttributeExists | Function::AttributeNotExists => 1,
            Function::BeginsWith | Function::Contains | Function::AttributeType => 2,
        };
        if arguments.len() != arity {
            return Err(Error::validation(format!(
                "Statement wasn't well formed, can't be processed: {function:?} takes {arity} argument(s), not {}",
                arguments.len()
            )));
        }
        Ok(Some(Cond::Function(function, arguments)))
    }

    /// Parse one operand of a comparison.
    fn operand(&mut self) -> Result<Value> {
        match self.peek() {
            Some(Token::Parameter) => Ok(Value::Parameter(self.parameter())),
            Some(Token::Ident(word)) if word.eq_ignore_ascii_case("size") => {
                if self.peek_second() != Some(&Token::LParen) {
                    return Ok(Value::Path(self.path()?));
                }
                self.position += 2;
                let path = self.path()?;
                self.expect(&Token::RParen, "`)`")?;
                Ok(Value::Size(path))
            }
            Some(Token::Ident(_) | Token::Quoted(_)) => Ok(Value::Path(self.path()?)),
            Some(_) => self.literal().map(Value::Literal),
            None => Err(self.unexpected("a value")),
        }
    }
}

/// Render a path for an error message, without expression aliases.
fn render_path(path: &Path) -> String {
    let mut text = String::new();
    for segment in path {
        match segment {
            Segment::Name(name) => {
                if !text.is_empty() {
                    text.push('.');
                }
                text.push_str(name);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ok(statement: &str) -> Parsed {
        parse(statement).unwrap()
    }

    fn expect_select(statement: Statement) -> Select {
        match statement {
            Statement::Select(select) => select,
            other => panic!("expected a SELECT, got {other:?}"),
        }
    }

    fn expect_insert(statement: Statement) -> Insert {
        match statement {
            Statement::Insert(insert) => insert,
            other => panic!("expected an INSERT, got {other:?}"),
        }
    }

    fn expect_update(statement: Statement) -> Update {
        match statement {
            Statement::Update(update) => update,
            other => panic!("expected an UPDATE, got {other:?}"),
        }
    }

    fn expect_delete(statement: Statement) -> Delete {
        match statement {
            Statement::Delete(delete) => delete,
            other => panic!("expected a DELETE, got {other:?}"),
        }
    }

    #[test]
    fn parses_a_select_with_projection_and_key_condition() {
        let parsed = parse_ok("SELECT \"Artist\", SongTitle FROM \"Music\" WHERE \"Artist\" = ? AND \"SongTitle\" > ?");
        assert_eq!(parsed.parameters, 2);
        let select = expect_select(parsed.statement);
        assert_eq!(select.table, "Music");
        assert_eq!(select.index, None);
        assert_eq!(select.projection.len(), 2);
        assert_eq!(
            select.projection[1],
            vec![Segment::Name("SongTitle".into())]
        );
        assert!(matches!(select.condition, Some(Cond::And(_))));
    }

    #[test]
    fn parses_star_and_index_reference() {
        let parsed = parse_ok("select * from \"Music\".\"AlbumTitle-Index\" where \"Artist\" = ?");
        let select = expect_select(parsed.statement);
        assert!(select.projection.is_empty());
        assert_eq!(select.index.as_deref(), Some("AlbumTitle-Index"));
    }

    #[test]
    fn parses_insert_with_nested_documents() {
        let parsed = parse_ok(
            "INSERT INTO \"Music\" VALUE {'Artist': ?, 'tags': ['a', 'b'], 'meta': {'year': -1970, 'ok': true}}",
        );
        assert_eq!(parsed.parameters, 1);
        let insert = expect_insert(parsed.statement);
        assert_eq!(insert.item.len(), 3);
        assert_eq!(insert.item[0].0, "Artist");
        assert!(matches!(insert.item[0].1, Doc::Parameter(0)));
        assert!(matches!(insert.item[1].1, Doc::List(_)));
    }

    #[test]
    fn parses_update_and_delete() {
        let parsed = parse_ok("UPDATE \"Music\" SET \"Plays\" = \"Plays\" + ?, extra = 1 REMOVE legacy WHERE \"Artist\" = ?");
        let update = expect_update(parsed.statement);
        assert_eq!(update.assignments.len(), 2);
        assert_eq!(update.assignments[0].1.len(), 2);
        assert_eq!(update.remove.len(), 1);
        assert_eq!(parsed.parameters, 2);

        let parsed = parse_ok("DELETE FROM \"Music\" WHERE \"Artist\" = ?");
        let delete = expect_delete(parsed.statement);
        assert_eq!(delete.table, "Music");
        assert_eq!(parsed.parameters, 1);
    }

    #[test]
    fn parses_every_predicate_form() {
        for statement in [
            "DELETE FROM t WHERE a BETWEEN ? AND ?",
            "DELETE FROM t WHERE a IN (?, ?, ?)",
            "DELETE FROM t WHERE a IS MISSING",
            "DELETE FROM t WHERE a IS NOT NULL",
            "DELETE FROM t WHERE begins_with(a, ?) AND contains(b, 'x')",
            "DELETE FROM t WHERE attribute_exists(a) OR NOT attribute_type(a, 'S')",
            "DELETE FROM t WHERE (a = ? OR b = ?) AND size(c) > 2",
        ] {
            assert!(parse(statement).is_ok(), "{statement}");
        }
    }

    #[test]
    fn rejects_malformed_statements() {
        for statement in [
            "",
            "SELECT",
            "SELECT * FROM",
            "SELECT * FROM t WHERE",
            "SELECT * FROM t WHERE a",
            "SELECT * FROM t WHERE a = ",
            "INSERT INTO t VALUE {}",
            "INSERT INTO t VALUE {'a' 1}",
            "INSERT INTO t VALUE {'a': 1} WHERE a = 1",
            "UPDATE t SET a = 1 WHERE a = 1 AND",
            "UPDATE t SET a = 1, a = 2 WHERE a = 1",
            "SELECT * FROM t;",
            "PATCH t",
            "DELETE FROM t.",
        ] {
            assert!(parse(statement).is_err(), "{statement}");
        }
    }

    #[test]
    fn numbers_are_validated_while_parsing() {
        assert!(parse("INSERT INTO t VALUE {'n': 1e400}").is_err());
        assert!(parse("INSERT INTO t VALUE {'n': 1e10}").is_ok());
    }
}
