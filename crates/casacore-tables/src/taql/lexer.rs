// SPDX-License-Identifier: LGPL-3.0-or-later
//! Peekable wrapper around the logos-generated lexer.
//!
//! [`Lexer`] adds single-token lookahead, span tracking, and
//! source-position computation (line/column) for error messages.

use logos::Logos;
use std::ops::Range;

use super::error::{SourcePos, TaqlError};
use super::token::Token;

/// A peekable wrapper around the logos-generated lexer.
///
/// Provides `peek()`, `next_token()`, `expect()`, `eat_if()` and
/// position tracking for parser error messages.
pub struct Lexer<'src> {
    inner: logos::Lexer<'src, Token>,
    peeked: Option<Option<(Token, Range<usize>)>>,
    source: &'src str,
}

impl<'src> Lexer<'src> {
    /// Creates a new lexer over the given TaQL source string.
    pub fn new(source: &'src str) -> Self {
        Self {
            inner: Token::lexer(source),
            peeked: None,
            source,
        }
    }

    /// Peeks at the next token without consuming it.
    ///
    /// Returns `None` at end-of-input or on a lex error.
    pub fn peek(&mut self) -> Option<&Token> {
        if self.peeked.is_none() {
            self.peeked = Some(self.advance());
        }
        self.peeked.as_ref().unwrap().as_ref().map(|(t, _)| t)
    }

    /// Consumes and returns the next token and its span.
    pub fn next_token(&mut self) -> Option<(Token, Range<usize>)> {
        if let Some(peeked) = self.peeked.take() {
            peeked
        } else {
            self.advance()
        }
    }

    /// Returns the slice of source text for the given span.
    pub fn slice(&self, span: &Range<usize>) -> &'src str {
        &self.source[span.clone()]
    }

    /// Consumes the next token if it matches `expected`, returning its span.
    pub fn eat_if(&mut self, expected: &Token) -> Option<Range<usize>> {
        if self.peek() == Some(expected) {
            self.next_token().map(|(_, s)| s)
        } else {
            None
        }
    }

    /// Consumes the next token, requiring it to match `expected`.
    pub fn expect(&mut self, expected: &Token) -> Result<Range<usize>, TaqlError> {
        match self.next_token() {
            Some((ref tok, span)) if tok == expected => Ok(span),
            Some((tok, span)) => Err(TaqlError::parse(
                self.position(span.start),
                format!("expected {expected}, found {tok}"),
            )),
            None => Err(TaqlError::unexpected_end(format!("expected {expected}"))),
        }
    }

    /// Returns true if the lexer has been exhausted.
    pub fn is_eof(&mut self) -> bool {
        self.peek().is_none()
    }

    /// Computes (1-based) line and column for a byte offset.
    pub fn position(&self, offset: usize) -> SourcePos {
        let prefix = &self.source[..offset.min(self.source.len())];
        let line = prefix.matches('\n').count() + 1;
        let col = prefix.rfind('\n').map_or(offset, |nl| offset - nl - 1) + 1;
        SourcePos { line, col }
    }

    /// Advances the inner logos lexer by one token.
    fn advance(&mut self) -> Option<(Token, Range<usize>)> {
        let result = self.inner.next()?;
        let span = self.inner.span();
        match result {
            Ok(token) => Some((token, span)),
            Err(()) => {
                // logos skip failures: return None to signal a lex error.
                // The parser will interpret this as unexpected EOF or error.
                None
            }
        }
    }
}
