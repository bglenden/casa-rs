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
    /// The `SELECT` keyword.
    #[token("SELECT", ignore(ascii_case))]
    Select,
    /// The `FROM` keyword.
    #[token("FROM", ignore(ascii_case))]
    From,
    /// The `WHERE` keyword.
    #[token("WHERE", ignore(ascii_case))]
    Where,
    /// The `ORDER` keyword.
    #[token("ORDER", ignore(ascii_case))]
    Order,
    /// The `BY` keyword.
    #[token("BY", ignore(ascii_case))]
    By,
    /// The `GROUP` keyword.
    #[token("GROUP", ignore(ascii_case))]
    Group,
    /// The `HAVING` keyword.
    #[token("HAVING", ignore(ascii_case))]
    Having,
    /// The `LIMIT` keyword.
    #[token("LIMIT", ignore(ascii_case))]
    Limit,
    /// The `OFFSET` keyword.
    #[token("OFFSET", ignore(ascii_case))]
    Offset,
    /// The `DISTINCT` keyword.
    #[token("DISTINCT", ignore(ascii_case))]
    Distinct,
    /// The `AS` keyword.
    #[token("AS", ignore(ascii_case))]
    As,
    /// The `AND` keyword.
    #[token("AND", ignore(ascii_case))]
    And,
    /// The `OR` keyword.
    #[token("OR", ignore(ascii_case))]
    Or,
    /// The `NOT` keyword.
    #[token("NOT", ignore(ascii_case))]
    Not,
    /// The `IN` keyword.
    #[token("IN", ignore(ascii_case))]
    In,
    /// The `BETWEEN` keyword.
    #[token("BETWEEN", ignore(ascii_case))]
    Between,
    /// The `LIKE` keyword.
    #[token("LIKE", ignore(ascii_case))]
    Like,
    /// The `ILIKE` keyword (case-insensitive LIKE).
    #[token("ILIKE", ignore(ascii_case))]
    Ilike,
    /// The `IS` keyword.
    #[token("IS", ignore(ascii_case))]
    Is,
    /// The `NULL` keyword.
    #[token("NULL", ignore(ascii_case))]
    Null,
    /// The `TRUE` keyword.
    #[token("TRUE", ignore(ascii_case))]
    True,
    /// The `FALSE` keyword.
    #[token("FALSE", ignore(ascii_case))]
    False,
    /// The `ASC` keyword.
    #[token("ASC", ignore(ascii_case))]
    Asc,
    /// The `DESC` keyword.
    #[token("DESC", ignore(ascii_case))]
    Desc,
    /// The `UPDATE` keyword.
    #[token("UPDATE", ignore(ascii_case))]
    Update,
    /// The `SET` keyword.
    #[token("SET", ignore(ascii_case))]
    Set,
    /// The `INSERT` keyword.
    #[token("INSERT", ignore(ascii_case))]
    Insert,
    /// The `INTO` keyword.
    #[token("INTO", ignore(ascii_case))]
    Into,
    /// The `VALUES` keyword.
    #[token("VALUES", ignore(ascii_case))]
    Values,
    /// The `DELETE` keyword.
    #[token("DELETE", ignore(ascii_case))]
    Delete,
    /// The `JOIN` keyword.
    #[token("JOIN", ignore(ascii_case))]
    Join,
    /// The `INNER` keyword.
    #[token("INNER", ignore(ascii_case))]
    Inner,
    /// The `LEFT` keyword.
    #[token("LEFT", ignore(ascii_case))]
    Left,
    /// The `RIGHT` keyword.
    #[token("RIGHT", ignore(ascii_case))]
    Right,
    /// The `CROSS` keyword.
    #[token("CROSS", ignore(ascii_case))]
    Cross,
    /// The `ON` keyword.
    #[token("ON", ignore(ascii_case))]
    On,
    /// The `COUNT` keyword.
    #[token("COUNT", ignore(ascii_case))]
    Count,
    /// The `SUM` keyword.
    #[token("SUM", ignore(ascii_case))]
    Sum,
    /// The `AVG` keyword.
    #[token("AVG", ignore(ascii_case))]
    Avg,
    /// The `MIN` keyword.
    #[token("MIN", ignore(ascii_case))]
    Min,
    /// The `MAX` keyword.
    #[token("MAX", ignore(ascii_case))]
    Max,
    /// The `ROWID` keyword.
    #[token("ROWID", ignore(ascii_case))]
    Rowid,
    /// The `CALC` keyword.
    #[token("CALC", ignore(ascii_case))]
    Calc,
    /// The `ALTER` keyword.
    #[token("ALTER", ignore(ascii_case))]
    Alter,
    /// The `TABLE` keyword.
    #[token("TABLE", ignore(ascii_case))]
    Table,
    /// The `COLUMN` keyword.
    #[token("COLUMN", ignore(ascii_case))]
    Column,
    /// The `RENAME` keyword.
    #[token("RENAME", ignore(ascii_case))]
    Rename,
    /// The `ADD` keyword.
    #[token("ADD", ignore(ascii_case))]
    Add,
    /// The `DROP` keyword.
    #[token("DROP", ignore(ascii_case))]
    Drop,
    /// The `KEYWORD` keyword.
    #[token("KEYWORD", ignore(ascii_case))]
    Keyword,
    /// The `TO` keyword.
    #[token("TO", ignore(ascii_case))]
    To,
    /// The `ROW` keyword.
    #[token("ROW", ignore(ascii_case))]
    Row,
    /// The `USING` keyword.
    #[token("USING", ignore(ascii_case))]
    Using,
    /// The `STYLE` keyword.
    #[token("STYLE", ignore(ascii_case))]
    Style,
    /// The `CREATE` keyword.
    #[token("CREATE", ignore(ascii_case))]
    Create,
    /// The `GIVING` keyword.
    #[token("GIVING", ignore(ascii_case))]
    Giving,
    // ── Operators ───────────────────────────────────────────────────
    /// Addition operator `+`.
    #[token("+")]
    Plus,
    /// Subtraction operator `-`.
    #[token("-")]
    Minus,
    /// Multiplication operator `*`.
    #[token("*")]
    Star,
    /// Division operator `/`.
    #[token("/")]
    Slash,
    /// Modulo operator `%`.
    #[token("%")]
    Percent,
    /// Exponentiation operator `**`.
    #[token("**")]
    DoubleStar,
    /// Assignment or equality operator `=`.
    #[token("=")]
    Eq,
    /// Equality operator `==`.
    #[token("==")]
    EqEq,
    /// Inequality operator `!=`.
    #[token("!=")]
    Ne,
    /// Inequality operator `<>`.
    #[token("<>")]
    LtGt,
    /// Less-than operator `<`.
    #[token("<")]
    Lt,
    /// Less-than-or-equal operator `<=`.
    #[token("<=")]
    Le,
    /// Greater-than operator `>`.
    #[token(">")]
    Gt,
    /// Greater-than-or-equal operator `>=`.
    #[token(">=")]
    Ge,
    /// Logical AND operator `&&`.
    #[token("&&")]
    AmpAmp,
    /// Logical OR operator `||`.
    #[token("||")]
    PipePipe,
    /// Logical NOT operator `!`.
    #[token("!")]
    Bang,
    /// Bitwise NOT operator `~`.
    #[token("~")]
    Tilde,
    /// Bitwise AND operator `&`.
    #[token("&")]
    Amp,
    /// Bitwise OR operator `|`.
    #[token("|")]
    Pipe,
    /// Bitwise XOR operator `^`.
    #[token("^")]
    Caret,
    /// Colon `:` (used in slice syntax).
    #[token(":")]
    Colon,
    /// Regex match operator `=~`
    #[token("=~")]
    EqTilde,
    /// Negated regex match operator `!~`
    #[token("!~")]
    BangTilde,

    // ── Delimiters ──────────────────────────────────────────────────
    /// Left parenthesis `(`.
    #[token("(")]
    LParen,
    /// Right parenthesis `)`.
    #[token(")")]
    RParen,
    /// Left bracket `[`.
    #[token("[")]
    LBracket,
    /// Right bracket `]`.
    #[token("]")]
    RBracket,
    /// Comma `,`.
    #[token(",")]
    Comma,
    /// Dot `.` (used for qualified column references).
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
