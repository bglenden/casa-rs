// SPDX-License-Identifier: LGPL-3.0-or-later
//! Path operands and their source spans in CASA lattice expressions.

use std::ops::Range;

use super::{ExprParseError, Lexer, Token};

/// Return the distinct image operands in first-use order.
///
/// Both bare and quoted names are included. Function names and Boolean
/// literals are excluded.
///
/// # Errors
/// Returns a tokenizer error if the expression is lexically invalid.
pub fn referenced_images(expression: &str) -> Result<Vec<String>, ExprParseError> {
    let mut names = Vec::new();
    visit_operands(expression, |name, _| {
        if !names.iter().any(|value| value == name) {
            names.push(name.to_owned());
        }
    })?;
    Ok(names)
}

/// Replace path operands with the paths provided by the caller. Tokens
/// outside the operands retain their original spelling.
///
/// # Errors
/// Returns a tokenizer error if the expression is lexically invalid.
pub fn substitute_image_operands(
    expression: &str,
    mut replacement: impl FnMut(&str) -> Option<String>,
) -> Result<String, ExprParseError> {
    let mut output = String::new();
    let mut offset = 0;
    visit_operands(expression, |name, span| {
        if let Some(path) = replacement(name)
            && let Some(quote) = ['"', '\''].into_iter().find(|quote| !path.contains(*quote))
        {
            output.push_str(&expression[offset..span.start]);
            output.push(quote);
            output.push_str(&path);
            output.push(quote);
            offset = span.end;
        }
    })?;
    output.push_str(&expression[offset..]);
    Ok(output)
}

fn visit_operands(
    expression: &str,
    mut visit: impl FnMut(&str, Range<usize>),
) -> Result<(), ExprParseError> {
    let mut lexer = Lexer::new(expression);
    let (mut current, mut start) = lexer.next_token()?;
    while current != Token::Eof {
        let end = lexer.pos;
        let (next, position) = lexer.next_token()?;
        if matches!(current, Token::QuotedPath(_)) && next == Token::Colon {
            let selector = expression[position + 1..]
                .split(|character: char| {
                    !character.is_ascii_alphanumeric() && !matches!(character, '_' | '.')
                })
                .next()
                .unwrap_or_default();
            return Err(ExprParseError {
                message: format!("Parse error at or near ':{selector}'"),
                position,
            });
        }
        match &current {
            Token::QuotedPath(name) => visit(name, start..end),
            Token::Ident(name) if !matches!(name.as_str(), "T" | "F") && next != Token::LParen => {
                visit(name, start..end);
            }
            _ => {}
        }
        current = next;
        start = position;
    }
    Ok(())
}
