// SPDX-License-Identifier: LGPL-3.0-or-later
//! Token definitions for the TaQL lexer.
//!
//! The [`Token`] enum is annotated with [`logos`] attributes so that a
//! zero-allocation lexer is generated at compile time. Keywords are
//! case-insensitive (via `ignore(ascii_case)`).
//!
//! # C++ reference
//!
//! `TaQLNodeDer.h`, `TaQLNode.h` — the C++ grammar defines the same
//! keyword set. TaQL comments begin with `#` and extend to end-of-line.

use logos::Logos;

/// A single lexical token produced by the TaQL lexer.
///
/// All keyword variants are matched case-insensitively. Whitespace and
/// `#`-comments are skipped automatically.
#[derive(Logos, Debug, Clone, PartialEq)]
#[logos(skip r"[ \t\r\n\f]+")] // skip whitespace
#[logos(skip r"#[^\n]*")] // skip # comments to EOL
pub enum Token {
    // ── Keywords ────────────────────────────────────────────────────
    #[token("SELECT", ignore(ascii_case))]
    Select,
    #[token("FROM", ignore(ascii_case))]
    From,
    #[token("WHERE", ignore(ascii_case))]
    Where,
    #[token("ORDER", ignore(ascii_case))]
    Order,
    #[token("BY", ignore(ascii_case))]
    By,
    #[token("GROUP", ignore(ascii_case))]
    Group,
    #[token("HAVING", ignore(ascii_case))]
    Having,
    #[token("LIMIT", ignore(ascii_case))]
    Limit,
    #[token("OFFSET", ignore(ascii_case))]
    Offset,
    #[token("DISTINCT", ignore(ascii_case))]
    Distinct,
    #[token("AS", ignore(ascii_case))]
    As,
    #[token("AND", ignore(ascii_case))]
    And,
    #[token("OR", ignore(ascii_case))]
    Or,
    #[token("NOT", ignore(ascii_case))]
    Not,
    #[token("IN", ignore(ascii_case))]
    In,
    #[token("BETWEEN", ignore(ascii_case))]
    Between,
    #[token("LIKE", ignore(ascii_case))]
    Like,
    #[token("ILIKE", ignore(ascii_case))]
    Ilike,
    #[token("IS", ignore(ascii_case))]
    Is,
    #[token("NULL", ignore(ascii_case))]
    Null,
    #[token("TRUE", ignore(ascii_case))]
    True,
    #[token("FALSE", ignore(ascii_case))]
    False,
    #[token("ASC", ignore(ascii_case))]
    Asc,
    #[token("DESC", ignore(ascii_case))]
    Desc,
    #[token("UPDATE", ignore(ascii_case))]
    Update,
    #[token("SET", ignore(ascii_case))]
    Set,
    #[token("INSERT", ignore(ascii_case))]
    Insert,
    #[token("INTO", ignore(ascii_case))]
    Into,
    #[token("VALUES", ignore(ascii_case))]
    Values,
    #[token("DELETE", ignore(ascii_case))]
    Delete,
    #[token("JOIN", ignore(ascii_case))]
    Join,
    #[token("INNER", ignore(ascii_case))]
    Inner,
    #[token("LEFT", ignore(ascii_case))]
    Left,
    #[token("RIGHT", ignore(ascii_case))]
    Right,
    #[token("CROSS", ignore(ascii_case))]
    Cross,
    #[token("ON", ignore(ascii_case))]
    On,
    #[token("COUNT", ignore(ascii_case))]
    Count,
    #[token("SUM", ignore(ascii_case))]
    Sum,
    #[token("AVG", ignore(ascii_case))]
    Avg,
    #[token("MIN", ignore(ascii_case))]
    Min,
    #[token("MAX", ignore(ascii_case))]
    Max,
    #[token("ROWID", ignore(ascii_case))]
    Rowid,
    #[token("CALC", ignore(ascii_case))]
    Calc,
    #[token("ALTER", ignore(ascii_case))]
    Alter,
    #[token("TABLE", ignore(ascii_case))]
    Table,
    #[token("COLUMN", ignore(ascii_case))]
    Column,
    #[token("RENAME", ignore(ascii_case))]
    Rename,
    #[token("ADD", ignore(ascii_case))]
    Add,
    #[token("DROP", ignore(ascii_case))]
    Drop,
    #[token("KEYWORD", ignore(ascii_case))]
    Keyword,
    #[token("TO", ignore(ascii_case))]
    To,
    #[token("ROW", ignore(ascii_case))]
    Row,
    #[token("USING", ignore(ascii_case))]
    Using,
    #[token("STYLE", ignore(ascii_case))]
    Style,
    #[token("CREATE", ignore(ascii_case))]
    Create,
    #[token("GIVING", ignore(ascii_case))]
    Giving,
    // ── Operators ───────────────────────────────────────────────────
    #[token("+")]
    Plus,
    #[token("-")]
    Minus,
    #[token("*")]
    Star,
    #[token("/")]
    Slash,
    #[token("%")]
    Percent,
    #[token("**")]
    DoubleStar,
    #[token("=")]
    Eq,
    #[token("==")]
    EqEq,
    #[token("!=")]
    Ne,
    #[token("<>")]
    LtGt,
    #[token("<")]
    Lt,
    #[token("<=")]
    Le,
    #[token(">")]
    Gt,
    #[token(">=")]
    Ge,
    #[token("&&")]
    AmpAmp,
    #[token("||")]
    PipePipe,
    #[token("!")]
    Bang,
    #[token("~")]
    Tilde,
    #[token("&")]
    Amp,
    #[token("|")]
    Pipe,
    #[token("^")]
    Caret,
    #[token(":")]
    Colon,
    /// Regex match operator `=~`
    #[token("=~")]
    EqTilde,
    /// Negated regex match operator `!~`
    #[token("!~")]
    BangTilde,

    // ── Delimiters ──────────────────────────────────────────────────
    #[token("(")]
    LParen,
    #[token(")")]
    RParen,
    #[token("[")]
    LBracket,
    #[token("]")]
    RBracket,
    #[token(",")]
    Comma,
    #[token(".")]
    Dot,

    // ── Literals ────────────────────────────────────────────────────
    /// Integer literal (decimal digits).
    #[regex(r"[0-9]+", priority = 3)]
    IntLiteral,

    /// Floating-point literal: digits with decimal point and/or exponent.
    #[regex(r"[0-9]+\.[0-9]*([eE][+-]?[0-9]+)?", priority = 4)]
    #[regex(r"[0-9]+[eE][+-]?[0-9]+", priority = 4)]
    #[regex(r"\.[0-9]+([eE][+-]?[0-9]+)?", priority = 4)]
    FloatLiteral,

    /// String literal: single- or double-quoted.
    #[regex(r#"'[^']*'"#)]
    #[regex(r#""[^"]*""#)]
    StringLiteral,

    /// Regex literal: `p/pattern/flags` or `m/pattern/flags`.
    ///
    /// C++ reference: `TaQLRegexNode`, `StringUtil::regex()`.
    #[regex(r"[pm]/[^/]*/[ig]*", priority = 5)]
    RegexLiteral,

    /// Identifier or unquoted column/table name.
    #[regex(r"[a-zA-Z_][a-zA-Z0-9_]*", priority = 1)]
    Ident,
}

impl Token {
    /// Returns true if this token is a keyword that could also be an identifier.
    pub fn is_keyword(&self) -> bool {
        !matches!(
            self,
            Token::Plus
                | Token::Minus
                | Token::Star
                | Token::Slash
                | Token::Percent
                | Token::DoubleStar
                | Token::Eq
                | Token::EqEq
                | Token::Ne
                | Token::LtGt
                | Token::Lt
                | Token::Le
                | Token::Gt
                | Token::Ge
                | Token::AmpAmp
                | Token::PipePipe
                | Token::Bang
                | Token::Tilde
                | Token::Amp
                | Token::Pipe
                | Token::Caret
                | Token::Colon
                | Token::EqTilde
                | Token::BangTilde
                | Token::LParen
                | Token::RParen
                | Token::LBracket
                | Token::RBracket
                | Token::Comma
                | Token::Dot
                | Token::IntLiteral
                | Token::FloatLiteral
                | Token::StringLiteral
                | Token::RegexLiteral
                | Token::Ident
        )
    }
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Token::Select => "SELECT",
            Token::From => "FROM",
            Token::Where => "WHERE",
            Token::Order => "ORDER",
            Token::By => "BY",
            Token::Group => "GROUP",
            Token::Having => "HAVING",
            Token::Limit => "LIMIT",
            Token::Offset => "OFFSET",
            Token::Distinct => "DISTINCT",
            Token::As => "AS",
            Token::And => "AND",
            Token::Or => "OR",
            Token::Not => "NOT",
            Token::In => "IN",
            Token::Between => "BETWEEN",
            Token::Like => "LIKE",
            Token::Ilike => "ILIKE",
            Token::Is => "IS",
            Token::Null => "NULL",
            Token::True => "TRUE",
            Token::False => "FALSE",
            Token::Asc => "ASC",
            Token::Desc => "DESC",
            Token::Update => "UPDATE",
            Token::Set => "SET",
            Token::Insert => "INSERT",
            Token::Into => "INTO",
            Token::Values => "VALUES",
            Token::Delete => "DELETE",
            Token::Join => "JOIN",
            Token::Inner => "INNER",
            Token::Left => "LEFT",
            Token::Right => "RIGHT",
            Token::Cross => "CROSS",
            Token::On => "ON",
            Token::Count => "COUNT",
            Token::Sum => "SUM",
            Token::Avg => "AVG",
            Token::Min => "MIN",
            Token::Max => "MAX",
            Token::Rowid => "ROWID",
            Token::Calc => "CALC",
            Token::Alter => "ALTER",
            Token::Table => "TABLE",
            Token::Column => "COLUMN",
            Token::Rename => "RENAME",
            Token::Add => "ADD",
            Token::Drop => "DROP",
            Token::Keyword => "KEYWORD",
            Token::To => "TO",
            Token::Row => "ROW",
            Token::Using => "USING",
            Token::Style => "STYLE",
            Token::Create => "CREATE",
            Token::Giving => "GIVING",
            Token::Plus => "+",
            Token::Minus => "-",
            Token::Star => "*",
            Token::Slash => "/",
            Token::Percent => "%",
            Token::DoubleStar => "**",
            Token::Eq => "=",
            Token::EqEq => "==",
            Token::Ne => "!=",
            Token::LtGt => "<>",
            Token::Lt => "<",
            Token::Le => "<=",
            Token::Gt => ">",
            Token::Ge => ">=",
            Token::AmpAmp => "&&",
            Token::PipePipe => "||",
            Token::Bang => "!",
            Token::Tilde => "~",
            Token::Amp => "&",
            Token::Pipe => "|",
            Token::Caret => "^",
            Token::Colon => ":",
            Token::EqTilde => "=~",
            Token::BangTilde => "!~",
            Token::LParen => "(",
            Token::RParen => ")",
            Token::LBracket => "[",
            Token::RBracket => "]",
            Token::Comma => ",",
            Token::Dot => ".",
            Token::IntLiteral => "<int>",
            Token::FloatLiteral => "<float>",
            Token::StringLiteral => "<string>",
            Token::RegexLiteral => "<regex>",
            Token::Ident => "<ident>",
        };
        f.write_str(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_all_tokens() {
        // Exercises every Display match arm to ensure coverage.
        let tokens = vec![
            (Token::Select, "SELECT"),
            (Token::From, "FROM"),
            (Token::Where, "WHERE"),
            (Token::Order, "ORDER"),
            (Token::By, "BY"),
            (Token::Group, "GROUP"),
            (Token::Having, "HAVING"),
            (Token::Limit, "LIMIT"),
            (Token::Offset, "OFFSET"),
            (Token::Distinct, "DISTINCT"),
            (Token::As, "AS"),
            (Token::And, "AND"),
            (Token::Or, "OR"),
            (Token::Not, "NOT"),
            (Token::In, "IN"),
            (Token::Between, "BETWEEN"),
            (Token::Like, "LIKE"),
            (Token::Ilike, "ILIKE"),
            (Token::Is, "IS"),
            (Token::Null, "NULL"),
            (Token::True, "TRUE"),
            (Token::False, "FALSE"),
            (Token::Asc, "ASC"),
            (Token::Desc, "DESC"),
            (Token::Update, "UPDATE"),
            (Token::Set, "SET"),
            (Token::Insert, "INSERT"),
            (Token::Into, "INTO"),
            (Token::Values, "VALUES"),
            (Token::Delete, "DELETE"),
            (Token::Join, "JOIN"),
            (Token::Inner, "INNER"),
            (Token::Left, "LEFT"),
            (Token::Right, "RIGHT"),
            (Token::Cross, "CROSS"),
            (Token::On, "ON"),
            (Token::Count, "COUNT"),
            (Token::Sum, "SUM"),
            (Token::Avg, "AVG"),
            (Token::Min, "MIN"),
            (Token::Max, "MAX"),
            (Token::Rowid, "ROWID"),
            (Token::Calc, "CALC"),
            (Token::Alter, "ALTER"),
            (Token::Table, "TABLE"),
            (Token::Column, "COLUMN"),
            (Token::Rename, "RENAME"),
            (Token::Add, "ADD"),
            (Token::Drop, "DROP"),
            (Token::Keyword, "KEYWORD"),
            (Token::To, "TO"),
            (Token::Row, "ROW"),
            (Token::Using, "USING"),
            (Token::Style, "STYLE"),
            (Token::Create, "CREATE"),
            (Token::Giving, "GIVING"),
            (Token::Plus, "+"),
            (Token::Minus, "-"),
            (Token::Star, "*"),
            (Token::Slash, "/"),
            (Token::Percent, "%"),
            (Token::DoubleStar, "**"),
            (Token::Eq, "="),
            (Token::EqEq, "=="),
            (Token::Ne, "!="),
            (Token::LtGt, "<>"),
            (Token::Lt, "<"),
            (Token::Le, "<="),
            (Token::Gt, ">"),
            (Token::Ge, ">="),
            (Token::AmpAmp, "&&"),
            (Token::PipePipe, "||"),
            (Token::Bang, "!"),
            (Token::Tilde, "~"),
            (Token::Amp, "&"),
            (Token::Pipe, "|"),
            (Token::Caret, "^"),
            (Token::Colon, ":"),
            (Token::EqTilde, "=~"),
            (Token::BangTilde, "!~"),
            (Token::LParen, "("),
            (Token::RParen, ")"),
            (Token::LBracket, "["),
            (Token::RBracket, "]"),
            (Token::Comma, ","),
            (Token::Dot, "."),
            (Token::IntLiteral, "<int>"),
            (Token::FloatLiteral, "<float>"),
            (Token::StringLiteral, "<string>"),
            (Token::RegexLiteral, "<regex>"),
            (Token::Ident, "<ident>"),
        ];
        for (tok, expected) in tokens {
            assert_eq!(tok.to_string(), expected);
        }
    }

    #[test]
    fn is_keyword_classification() {
        // Keywords should return true
        assert!(Token::Select.is_keyword());
        assert!(Token::Calc.is_keyword());
        assert!(Token::Alter.is_keyword());
        assert!(Token::Row.is_keyword());

        // Non-keywords should return false
        assert!(!Token::Plus.is_keyword());
        assert!(!Token::LParen.is_keyword());
        assert!(!Token::IntLiteral.is_keyword());
        assert!(!Token::Ident.is_keyword());
        assert!(!Token::Amp.is_keyword());
        assert!(!Token::Pipe.is_keyword());
        assert!(!Token::RegexLiteral.is_keyword());
    }
}
