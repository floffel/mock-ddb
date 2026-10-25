//! Tokenizer for DynamoDB expression syntax.
//!
//! The grammar has no literals: every value is a `:placeholder`, every aliased
//! name a `#placeholder`, and everything else is an attribute path, a function
//! name or a keyword.

use crate::{Error, Result};

/// One lexical token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Token {
    /// A bare word: an attribute name, a function name or a keyword.
    Ident(String),
    /// `#name` — an expression attribute name placeholder, without the `#`.
    Name(String),
    /// `:name` — an expression attribute value placeholder, without the `:`.
    Value(String),
    /// `,`
    Comma,
    /// `.`
    Dot,
    /// `(`
    LParen,
    /// `)`
    RParen,
    /// `[`
    LBracket,
    /// `]`
    RBracket,
    /// `=`
    Eq,
    /// `<>` (and the tolerated `!=`)
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
}

impl Token {
    /// Rendering used in parse errors.
    pub(crate) fn describe(&self) -> String {
        match self {
            Self::Ident(name) => format!("`{name}`"),
            Self::Name(name) => format!("`#{name}`"),
            Self::Value(name) => format!("`:{name}`"),
            Self::Comma => "`,`".to_string(),
            Self::Dot => "`.`".to_string(),
            Self::LParen => "`(`".to_string(),
            Self::RParen => "`)`".to_string(),
            Self::LBracket => "`[`".to_string(),
            Self::RBracket => "`]`".to_string(),
            Self::Eq => "`=`".to_string(),
            Self::Ne => "`<>`".to_string(),
            Self::Lt => "`<`".to_string(),
            Self::Le => "`<=`".to_string(),
            Self::Gt => "`>`".to_string(),
            Self::Ge => "`>=`".to_string(),
            Self::Plus => "`+`".to_string(),
            Self::Minus => "`-`".to_string(),
        }
    }
}

/// Whether a character can only ever be punctuation in an expression.
fn is_delimiter(character: char) -> bool {
    character.is_whitespace()
        || matches!(
            character,
            ',' | '.' | '(' | ')' | '[' | ']' | '<' | '>' | '=' | '!' | '+' | '-' | '#' | ':'
        )
}

/// Whether a character can appear in a placeholder name.
fn is_alias_character(character: char) -> bool {
    character.is_alphanumeric() || character == '_'
}

/// Split an expression into tokens.
pub(crate) fn tokenize(input: &str) -> Result<Vec<Token>> {
    let characters: Vec<char> = input.chars().collect();
    let mut tokens = Vec::new();
    let mut position = 0;
    while position < characters.len() {
        let character = characters[position];
        if character.is_whitespace() {
            position += 1;
            continue;
        }
        if let Some((token, width)) = punctuation(&characters, position)? {
            tokens.push(token);
            position += width;
            continue;
        }
        let (token, width) = match character {
            '#' | ':' => placeholder(&characters, position)?,
            _ => bare_word(&characters, position)?,
        };
        tokens.push(token);
        position += width;
    }
    Ok(tokens)
}

/// Scan a single- or two-character operator, if the character starts one.
fn punctuation(characters: &[char], position: usize) -> Result<Option<(Token, usize)>> {
    let character = characters[position];
    let next = characters.get(position + 1).copied();
    let token = match character {
        ',' => Some((Token::Comma, 1)),
        '.' => Some((Token::Dot, 1)),
        '(' => Some((Token::LParen, 1)),
        ')' => Some((Token::RParen, 1)),
        '[' => Some((Token::LBracket, 1)),
        ']' => Some((Token::RBracket, 1)),
        '+' => Some((Token::Plus, 1)),
        '-' => Some((Token::Minus, 1)),
        '=' => Some((Token::Eq, 1)),
        '<' => match next {
            Some('=') => Some((Token::Le, 2)),
            Some('>') => Some((Token::Ne, 2)),
            _ => Some((Token::Lt, 1)),
        },
        '>' => match next {
            Some('=') => Some((Token::Ge, 2)),
            _ => Some((Token::Gt, 1)),
        },
        '!' => match next {
            Some('=') => Some((Token::Ne, 2)),
            _ => {
                return Err(Error::validation(format!(
                    "Invalid expression: unexpected character '!' at position {position}"
                )));
            }
        },
        _ => None,
    };
    Ok(token)
}

/// Scan `#name` or `:name`.
fn placeholder(characters: &[char], position: usize) -> Result<(Token, usize)> {
    let marker = characters[position];
    let mut cursor = position + 1;
    let mut name = String::new();
    while cursor < characters.len() && is_alias_character(characters[cursor]) {
        name.push(characters[cursor]);
        cursor += 1;
    }
    if name.is_empty() {
        return Err(Error::validation(format!(
            "Invalid expression: `{marker}` at position {position} must be followed by a name"
        )));
    }
    let token = if marker == '#' {
        Token::Name(name)
    } else {
        Token::Value(name)
    };
    Ok((token, cursor - position))
}

/// Scan a bare word: an attribute name, a function name or a keyword.
fn bare_word(characters: &[char], position: usize) -> Result<(Token, usize)> {
    let mut cursor = position;
    let mut name = String::new();
    while cursor < characters.len() && !is_delimiter(characters[cursor]) {
        name.push(characters[cursor]);
        cursor += 1;
    }
    if name.is_empty() {
        return Err(Error::validation(format!(
            "Invalid expression: unexpected character `{}` at position {position}",
            characters[position]
        )));
    }
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(Error::validation(format!(
            "Invalid expression: unexpected character in attribute name `{name}` at position {position}"
        )));
    }
    Ok((Token::Ident(name), cursor - position))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenizes_paths_placeholders_and_operators() {
        let tokens = tokenize("#n.#m[0] = :v AND s <> :w").unwrap();
        assert_eq!(tokens[0], Token::Name("n".into()));
        assert_eq!(tokens[1], Token::Dot);
        assert_eq!(tokens[3], Token::LBracket);
        assert_eq!(tokens[4], Token::Ident("0".into()));
        assert!(tokens.contains(&Token::Ne));
        assert!(tokens.contains(&Token::Ident("AND".into())));
    }

    #[test]
    fn tolerates_unicode_attribute_names_and_extra_whitespace() {
        let tokens = tokenize("  größe\t=\n:v  ").unwrap();
        assert_eq!(tokens[0], Token::Ident("größe".into()));
        assert_eq!(tokens[1], Token::Eq);
    }

    #[test]
    fn describes_every_token_for_error_messages() {
        let tokens = [
            Token::Ident("a".into()),
            Token::Name("n".into()),
            Token::Value("v".into()),
            Token::Comma,
            Token::Dot,
            Token::LParen,
            Token::RParen,
            Token::LBracket,
            Token::RBracket,
            Token::Eq,
            Token::Ne,
            Token::Lt,
            Token::Le,
            Token::Gt,
            Token::Ge,
            Token::Plus,
            Token::Minus,
        ];
        for token in tokens {
            assert_ne!(token.describe(), "");
        }
        assert_eq!(Token::Ident("a".into()).describe(), "`a`");
        assert_eq!(Token::Name("n".into()).describe(), "`#n`");
        assert_eq!(Token::Value("v".into()).describe(), "`:v`");
        assert_eq!(Token::Dot.describe(), "`.`");
        assert_eq!(Token::LBracket.describe(), "`[`");
        assert_eq!(Token::RBracket.describe(), "`]`");
        assert_eq!(Token::Ne.describe(), "`<>`");
        assert_eq!(Token::Lt.describe(), "`<`");
        assert_eq!(Token::Le.describe(), "`<=`");
        assert_eq!(Token::Ge.describe(), "`>=`");
        assert_eq!(Token::Plus.describe(), "`+`");
        assert_eq!(Token::Minus.describe(), "`-`");
        assert_eq!(Token::Comma.describe(), "`,`");
        assert_eq!(Token::LParen.describe(), "`(`");
        assert_eq!(Token::RParen.describe(), "`)`");
        assert_eq!(Token::Gt.describe(), "`>`");
        assert_eq!(Token::Eq.describe(), "`=`");
    }

    #[test]
    fn tokenizes_alternative_not_equal_and_rejects_symbols() {
        assert!(tokenize("a != :v").unwrap().contains(&Token::Ne));
        assert!(tokenize("a% = :v").is_err());
        assert!(tokenize("a ; b").is_err());
    }

    #[test]
    fn rejects_stray_punctuation_and_empty_placeholders() {
        assert!(tokenize("a @ b").is_err());
        assert!(tokenize("#").is_err());
        assert!(tokenize(": = 1").is_err());
        assert!(tokenize("a ! b").is_err());
    }
}
