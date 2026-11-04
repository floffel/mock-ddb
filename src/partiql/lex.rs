//! Tokenizer for the PartiQL subset DynamoDB accepts.
//!
//! PartiQL's lexical grammar differs from the expression language in
//! `expr::lex`: values and names are literals (`'text'`, `1.5`) or double-quoted
//! identifiers, positional `?` parameters stand in for `:placeholder`s, and
//! there are no `#aliases`. The token set is therefore its own, hand-written
//! for the same reasons recorded above `mod lex` in [`crate::expr`]: the error
//! strings are the contract and the size limit has to run before any
//! allocation.

use crate::{Error, Result};

/// The service's ceiling on one PartiQL statement, in characters.
///
/// `ExecuteStatement.Statement` is documented as "Minimum length of 1. Maximum
/// length of 8192", so an oversized statement is rejected before it is copied
/// into a `Vec<char>` — a hostile statement costs one count, not an allocation.
pub(crate) const MAX_STATEMENT_BYTES: usize = 8192;

/// One lexical token of a PartiQL statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Token {
    /// A bare word: a keyword or an unquoted attribute name.
    Ident(String),
    /// `"name"` — a quoted table, index or attribute name, without the quotes.
    Quoted(String),
    /// `'text'` — a string literal, with `''` already collapsed to `'`.
    Text(String),
    /// A numeric literal, kept verbatim so `1.0` and `1e0` stay distinct text
    /// until the number layer validates and normalises them.
    Number(String),
    /// `?` — a positional parameter.
    Parameter,
    /// `,`
    Comma,
    /// `.`
    Dot,
    /// `:`
    Colon,
    /// `(`
    LParen,
    /// `)`
    RParen,
    /// `[`
    LBracket,
    /// `]`
    RBracket,
    /// `{`
    LBrace,
    /// `}`
    RBrace,
    /// `=`
    Eq,
    /// `<>` or `!=`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
    /// `+`
    Plus,
    /// `-`
    Minus,
    /// `*`
    Star,
}

impl Token {
    /// How the token is named in a parse error.
    pub(crate) fn describe(&self) -> String {
        match self {
            Self::Ident(word) | Self::Quoted(word) => format!("`{word}`"),
            Self::Text(value) => format!("'{value}'"),
            Self::Number(value) => format!("`{value}`"),
            Self::Parameter => "`?`".to_string(),
            Self::Comma => "`,`".to_string(),
            Self::Dot => "`.`".to_string(),
            Self::Colon => "`:`".to_string(),
            Self::LParen => "`(`".to_string(),
            Self::RParen => "`)`".to_string(),
            Self::LBracket => "`[`".to_string(),
            Self::RBracket => "`]`".to_string(),
            Self::LBrace => "`{`".to_string(),
            Self::RBrace => "`}`".to_string(),
            Self::Eq => "`=`".to_string(),
            Self::Ne => "`<>`".to_string(),
            Self::Lt => "`<`".to_string(),
            Self::Le => "`<=`".to_string(),
            Self::Gt => "`>`".to_string(),
            Self::Ge => "`>=`".to_string(),
            Self::Plus => "`+`".to_string(),
            Self::Minus => "`-`".to_string(),
            Self::Star => "`*`".to_string(),
        }
    }
}

/// Split a statement into tokens, rejecting an empty or oversized one.
pub(crate) fn tokenize(input: &str) -> Result<Vec<Token>> {
    if input.is_empty() {
        return Err(Error::validation(
            "1 validation error detected: Value at 'statement' failed to satisfy constraint: Member must have length greater than or equal to 1",
        ));
    }
    if input.chars().count() > MAX_STATEMENT_BYTES {
        return Err(Error::validation(format!(
            "1 validation error detected: Value at 'statement' failed to satisfy constraint: Member must have length less than or equal to {MAX_STATEMENT_BYTES}"
        )));
    }
    let characters: Vec<char> = input.chars().collect();
    let mut tokens = Vec::new();
    let mut position = 0;
    while position < characters.len() {
        let character = characters[position];
        if character.is_whitespace() {
            position += 1;
            continue;
        }
        if let Some((token, width)) = punctuation(&characters, position) {
            tokens.push(token);
            position += width;
            continue;
        }
        let (token, width) = match character {
            '\'' => quoted_text(&characters, position)?,
            '"' => quoted_name(&characters, position)?,
            '?' => (Token::Parameter, 1),
            character if character.is_ascii_digit() => number(&characters, position)?,
            _ => bare_word(&characters, position)?,
        };
        tokens.push(token);
        position += width;
    }
    Ok(tokens)
}

/// Scan a single- or two-character operator, if the character starts one.
fn punctuation(characters: &[char], position: usize) -> Option<(Token, usize)> {
    let next = characters.get(position + 1).copied();
    let token = match characters[position] {
        ',' => (Token::Comma, 1),
        '.' => (Token::Dot, 1),
        ':' => (Token::Colon, 1),
        '(' => (Token::LParen, 1),
        ')' => (Token::RParen, 1),
        '[' => (Token::LBracket, 1),
        ']' => (Token::RBracket, 1),
        '{' => (Token::LBrace, 1),
        '}' => (Token::RBrace, 1),
        '+' => (Token::Plus, 1),
        '-' => (Token::Minus, 1),
        '*' => (Token::Star, 1),
        '=' => (Token::Eq, 1),
        '<' => match next {
            Some('=') => (Token::Le, 2),
            Some('>') => (Token::Ne, 2),
            _ => (Token::Lt, 1),
        },
        '>' => match next {
            Some('=') => (Token::Ge, 2),
            _ => (Token::Gt, 1),
        },
        '!' if next == Some('=') => (Token::Ne, 2),
        _ => return None,
    };
    Some(token)
}

/// Scan `'text'`, where a doubled quote stands for one quote character.
fn quoted_text(characters: &[char], position: usize) -> Result<(Token, usize)> {
    let mut cursor = position + 1;
    let mut text = String::new();
    loop {
        let Some(character) = characters.get(cursor).copied() else {
            return Err(Error::validation(
                "Statement wasn't well formed, can't be processed: Unterminated string literal",
            ));
        };
        cursor += 1;
        if character != '\'' {
            text.push(character);
            continue;
        }
        if characters.get(cursor) == Some(&'\'') {
            text.push('\'');
            cursor += 1;
            continue;
        }
        return Ok((Token::Text(text), cursor - position));
    }
}

/// Scan `"name"`, where a doubled quote stands for one quote character.
fn quoted_name(characters: &[char], position: usize) -> Result<(Token, usize)> {
    let mut cursor = position + 1;
    let mut name = String::new();
    loop {
        let Some(character) = characters.get(cursor).copied() else {
            return Err(Error::validation(
                "Statement wasn't well formed, can't be processed: Unterminated quoted name",
            ));
        };
        cursor += 1;
        if character != '"' {
            name.push(character);
            continue;
        }
        if characters.get(cursor) == Some(&'"') {
            name.push('"');
            cursor += 1;
            continue;
        }
        if name.is_empty() {
            return Err(Error::validation(
                "Statement wasn't well formed, can't be processed: Empty quoted name",
            ));
        }
        return Ok((Token::Quoted(name), cursor - position));
    }
}

/// Scan a numeric literal: digits, an optional fraction, an optional exponent.
fn number(characters: &[char], position: usize) -> Result<(Token, usize)> {
    let mut cursor = position;
    let mut text = String::new();
    while characters.get(cursor).is_some_and(char::is_ascii_digit) {
        text.push(characters[cursor]);
        cursor += 1;
    }
    if characters.get(cursor) == Some(&'.') {
        text.push('.');
        cursor += 1;
        let start = cursor;
        while characters.get(cursor).is_some_and(char::is_ascii_digit) {
            text.push(characters[cursor]);
            cursor += 1;
        }
        if cursor == start {
            return Err(Error::validation(format!(
                "Statement wasn't well formed, can't be processed: Syntax error; token: `{text}`"
            )));
        }
    }
    if characters
        .get(cursor)
        .is_some_and(|character| *character == 'e' || *character == 'E')
    {
        let mantissa = text.clone();
        let mut exponent = String::from("e");
        cursor += 1;
        if characters
            .get(cursor)
            .is_some_and(|character| *character == '+' || *character == '-')
        {
            exponent.push(characters[cursor]);
            cursor += 1;
        }
        let start = cursor;
        while characters.get(cursor).is_some_and(char::is_ascii_digit) {
            exponent.push(characters[cursor]);
            cursor += 1;
        }
        if cursor == start {
            return Err(Error::validation(format!(
                "Statement wasn't well formed, can't be processed: Syntax error; token: `{mantissa}e`"
            )));
        }
        text.push_str(&exponent);
    }
    Ok((Token::Number(text), cursor - position))
}

/// Scan a bare word: a keyword or an unquoted attribute name.
fn bare_word(characters: &[char], position: usize) -> Result<(Token, usize)> {
    let mut cursor = position;
    let mut word = String::new();
    while characters
        .get(cursor)
        .is_some_and(|character| character.is_alphanumeric() || *character == '_')
    {
        word.push(characters[cursor]);
        cursor += 1;
    }
    if word.is_empty() {
        return Err(Error::validation(format!(
            "Statement wasn't well formed, can't be processed: Syntax error; token: `{}` at position {position}",
            characters[position]
        )));
    }
    Ok((Token::Ident(word), cursor - position))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenizes_a_select() {
        let tokens = tokenize("SELECT * FROM \"Music\" WHERE \"Artist\" = ?").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Ident("SELECT".into()),
                Token::Star,
                Token::Ident("FROM".into()),
                Token::Quoted("Music".into()),
                Token::Ident("WHERE".into()),
                Token::Quoted("Artist".into()),
                Token::Eq,
                Token::Parameter,
            ]
        );
    }

    #[test]
    fn tokenizes_text_numbers_and_keywords() {
        let tokens = tokenize("VALUE {'a': 'it''s', 'n': -1.5e+3}").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Ident("VALUE".into()),
                Token::LBrace,
                Token::Text("a".into()),
                Token::Colon,
                Token::Text("it's".into()),
                Token::Comma,
                Token::Text("n".into()),
                Token::Colon,
                Token::Minus,
                Token::Number("1.5e+3".into()),
                Token::RBrace,
            ]
        );
    }

    #[test]
    fn rejects_empty_unterminated_and_oversized_statements() {
        assert!(tokenize("").is_err());
        assert!(tokenize("SELECT 'oops").is_err());
        assert!(tokenize("SELECT \"oops").is_err());
        assert!(tokenize("SELECT \"\"").is_err());
        assert!(tokenize(&"a".repeat(MAX_STATEMENT_BYTES)).is_ok());
        assert!(tokenize(&"a".repeat(MAX_STATEMENT_BYTES + 1)).is_err());
    }

    #[test]
    fn rejects_stray_characters_and_broken_numbers() {
        assert!(tokenize("SELECT $ FROM t").is_err());
        assert!(tokenize("VALUE 1.").is_err());
        assert!(tokenize("VALUE 1e").is_err());
    }

    #[test]
    fn describes_every_token() {
        let tokens = [
            Token::Ident("a".into()),
            Token::Quoted("a".into()),
            Token::Text("a".into()),
            Token::Number("1".into()),
            Token::Parameter,
            Token::Comma,
            Token::Dot,
            Token::Colon,
            Token::LParen,
            Token::RParen,
            Token::LBracket,
            Token::RBracket,
            Token::LBrace,
            Token::RBrace,
            Token::Eq,
            Token::Ne,
            Token::Lt,
            Token::Le,
            Token::Gt,
            Token::Ge,
            Token::Plus,
            Token::Minus,
            Token::Star,
        ];
        for token in tokens {
            assert_ne!(token.describe(), "");
        }
        assert_eq!(Token::Parameter.describe(), "`?`");
        assert_eq!(Token::Text("x".into()).describe(), "'x'");
    }
}
