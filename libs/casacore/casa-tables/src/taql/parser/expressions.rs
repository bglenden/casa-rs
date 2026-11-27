// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

impl<'src> Parser<'src> {
    /// Parse an expression at minimum binding power 0.
    pub fn parse_expr(&mut self) -> Result<Expr, TaqlError> {
        self.parse_expr_bp(0)
    }

    pub(super) fn parse_expr_list(&mut self) -> Result<Vec<Expr>, TaqlError> {
        let mut exprs = vec![self.parse_expr()?];
        while self.lexer.eat_if(&Token::Comma).is_some() {
            exprs.push(self.parse_expr()?);
        }
        Ok(exprs)
    }

    /// Pratt parser: parse an expression with minimum binding power `min_bp`.
    pub(super) fn parse_expr_bp(&mut self, min_bp: u8) -> Result<Expr, TaqlError> {
        let mut lhs = self.parse_prefix()?;

        loop {
            // Check for postfix / infix operators
            lhs = match self.lexer.peek() {
                // Postfix: IS [NOT] NULL
                Some(Token::Is) => {
                    let bp = 6; // between AND and comparison
                    if bp < min_bp {
                        break;
                    }
                    self.lexer.next_token();
                    let negated = self.lexer.eat_if(&Token::Not).is_some();
                    self.lexer.expect(&Token::Null)?;
                    Expr::IsNull {
                        expr: Box::new(lhs),
                        negated,
                    }
                }
                // Postfix: [NOT] BETWEEN / IN / LIKE / ILIKE
                Some(Token::Not) => {
                    let bp = 6;
                    if bp < min_bp {
                        break;
                    }
                    // Peek ahead: NOT BETWEEN, NOT IN, NOT LIKE, NOT ILIKE
                    self.lexer.next_token();
                    match self.lexer.peek() {
                        Some(Token::Between) => self.parse_between_tail(lhs, true)?,
                        Some(Token::In) => self.parse_in_tail(lhs, true)?,
                        Some(Token::Like) => {
                            self.lexer.next_token();
                            let pattern = self.parse_expr_bp(10)?;
                            Expr::Like {
                                expr: Box::new(lhs),
                                pattern: Box::new(pattern),
                                negated: true,
                                case_insensitive: false,
                            }
                        }
                        Some(Token::Ilike) => {
                            self.lexer.next_token();
                            let pattern = self.parse_expr_bp(10)?;
                            Expr::Like {
                                expr: Box::new(lhs),
                                pattern: Box::new(pattern),
                                negated: true,
                                case_insensitive: true,
                            }
                        }
                        _ => {
                            // This is NOT as a prefix operator that was already consumed.
                            // This shouldn't happen in valid TaQL but handle gracefully.
                            return Err(TaqlError::unexpected_end(
                                "expected BETWEEN, IN, LIKE, or ILIKE after NOT",
                            ));
                        }
                    }
                }
                Some(Token::Between) => {
                    let bp = 6;
                    if bp < min_bp {
                        break;
                    }
                    self.parse_between_tail(lhs, false)?
                }
                Some(Token::In) => {
                    let bp = 6;
                    if bp < min_bp {
                        break;
                    }
                    self.parse_in_tail(lhs, false)?
                }
                Some(Token::Like) => {
                    let bp = 6;
                    if bp < min_bp {
                        break;
                    }
                    self.lexer.next_token();
                    let pattern = self.parse_expr_bp(10)?;
                    Expr::Like {
                        expr: Box::new(lhs),
                        pattern: Box::new(pattern),
                        negated: false,
                        case_insensitive: false,
                    }
                }
                Some(Token::Ilike) => {
                    let bp = 6;
                    if bp < min_bp {
                        break;
                    }
                    self.lexer.next_token();
                    let pattern = self.parse_expr_bp(10)?;
                    Expr::Like {
                        expr: Box::new(lhs),
                        pattern: Box::new(pattern),
                        negated: false,
                        case_insensitive: true,
                    }
                }
                // Regex match: =~ or !~
                Some(Token::EqTilde) | Some(Token::BangTilde) => {
                    let bp = 10; // same as comparison operators
                    if bp < min_bp {
                        break;
                    }
                    let negated = self.lexer.peek() == Some(&Token::BangTilde);
                    self.lexer.next_token();
                    let pattern = self.parse_expr_bp(11)?;
                    Expr::RegexMatch {
                        expr: Box::new(lhs),
                        pattern: Box::new(pattern),
                        negated,
                    }
                }
                // Tilde as binary infix = regex match (C++ TaQL compatibility)
                Some(Token::Tilde) => {
                    let bp = 10;
                    if bp < min_bp {
                        break;
                    }
                    self.lexer.next_token();
                    let pattern = self.parse_expr_bp(11)?;
                    Expr::RegexMatch {
                        expr: Box::new(lhs),
                        pattern: Box::new(pattern),
                        negated: false,
                    }
                }
                // Array indexing: expr[...]
                Some(Token::LBracket) => {
                    let bp = 20; // highest precedence (postfix)
                    if bp < min_bp {
                        break;
                    }
                    self.lexer.next_token();
                    let indices = self.parse_index_elements()?;
                    self.lexer.expect(&Token::RBracket)?;
                    Expr::ArrayIndex {
                        array: Box::new(lhs),
                        indices,
                    }
                }
                // Infix operators
                Some(tok) => {
                    if let Some((l_bp, r_bp)) = infix_binding_power(tok) {
                        if l_bp < min_bp {
                            break;
                        }
                        let op = infix_op(tok);
                        self.lexer.next_token();
                        let rhs = self.parse_expr_bp(r_bp)?;
                        Expr::Binary {
                            left: Box::new(lhs),
                            op,
                            right: Box::new(rhs),
                        }
                    } else {
                        break;
                    }
                }
                None => break,
            };
        }

        Ok(lhs)
    }

    /// Parse a prefix expression (atom or unary operator).
    pub(super) fn parse_prefix(&mut self) -> Result<Expr, TaqlError> {
        match self.lexer.peek() {
            Some(Token::IntLiteral) => {
                let (_, span) = self.lexer.next_token().unwrap();
                let s = self.lexer.slice(&span);
                let n: i64 = s.parse().map_err(|_| {
                    TaqlError::parse(self.lexer.position(span.start), "invalid integer literal")
                })?;
                match self.parse_optional_quantity_unit(span.end)? {
                    Some(NumericSuffix::Unit(unit)) => Ok(Expr::Literal(Literal::Quantity {
                        value: n as f64,
                        unit,
                    })),
                    Some(NumericSuffix::Imaginary) => Ok(Expr::Literal(Literal::Complex(
                        num_complex::Complex64::new(0., n as f64),
                    ))),
                    None => Ok(Expr::Literal(Literal::Int(n))),
                }
            }
            Some(Token::FloatLiteral) => {
                let (_, span) = self.lexer.next_token().unwrap();
                let s = self.lexer.slice(&span);
                let v: f64 = s.parse().map_err(|_| {
                    TaqlError::parse(self.lexer.position(span.start), "invalid float literal")
                })?;
                match self.parse_optional_quantity_unit(span.end)? {
                    Some(NumericSuffix::Unit(unit)) => {
                        Ok(Expr::Literal(Literal::Quantity { value: v, unit }))
                    }
                    Some(NumericSuffix::Imaginary) => Ok(Expr::Literal(Literal::Complex(
                        num_complex::Complex64::new(0., v),
                    ))),
                    None => Ok(Expr::Literal(Literal::Float(v))),
                }
            }
            Some(Token::LBracket) => {
                self.lexer.next_token();
                if self.peek_starts_subquery() {
                    return self.parse_subquery_after_lbracket();
                }
                let mut values = Vec::new();
                if self.lexer.peek() != Some(&Token::RBracket) {
                    loop {
                        values.push(self.parse_expr()?);
                        if self.lexer.eat_if(&Token::Comma).is_none() {
                            break;
                        }
                    }
                }
                self.lexer.expect(&Token::RBracket)?;
                Ok(Expr::ArrayLiteral(values))
            }
            Some(Token::StringLiteral) => {
                let (_, span) = self.lexer.next_token().unwrap();
                let s = self.lexer.slice(&span);
                // Strip quotes.
                let inner = &s[1..s.len() - 1];
                Ok(Expr::Literal(Literal::String(inner.to_string())))
            }
            Some(Token::True) => {
                self.lexer.next_token();
                Ok(Expr::Literal(Literal::Bool(true)))
            }
            Some(Token::False) => {
                self.lexer.next_token();
                Ok(Expr::Literal(Literal::Bool(false)))
            }
            Some(Token::Null) => {
                self.lexer.next_token();
                Ok(Expr::Literal(Literal::Null))
            }
            Some(Token::Star) => {
                self.lexer.next_token();
                Ok(Expr::Star)
            }
            Some(Token::Rowid) => {
                let (_, span) = self.lexer.next_token().unwrap();
                if self.lexer.eat_if(&Token::LParen).is_some() {
                    self.lexer.expect(&Token::RParen)?;
                    Ok(Expr::RowNumber)
                } else {
                    Ok(Expr::ColumnRef(ColumnRef {
                        table: None,
                        column: self.lexer.slice(&span).to_owned(),
                    }))
                }
            }
            // Unary minus
            Some(Token::Minus) => {
                self.lexer.next_token();
                let operand = self.parse_expr_bp(19)?; // high precedence for unary
                Ok(Expr::Unary {
                    op: UnaryOp::Negate,
                    operand: Box::new(operand),
                })
            }
            // Unary NOT
            Some(Token::Not) | Some(Token::Bang) => {
                self.lexer.next_token();
                let operand = self.parse_expr_bp(9)?;
                Ok(Expr::Unary {
                    op: UnaryOp::Not,
                    operand: Box::new(operand),
                })
            }
            // Bitwise NOT
            Some(Token::Tilde) => {
                self.lexer.next_token();
                let operand = self.parse_expr_bp(19)?;
                Ok(Expr::Unary {
                    op: UnaryOp::BitNot,
                    operand: Box::new(operand),
                })
            }
            // Regex literal: p/pattern/flags or m/pattern/flags
            Some(Token::RegexLiteral) => {
                let (_, span) = self.lexer.next_token().unwrap();
                let s = self.lexer.slice(&span);
                // Parse: {p|m}/pattern/flags
                let inner = &s[2..]; // skip "p/" or "m/"
                let last_slash = inner.rfind('/').unwrap();
                let pattern = inner[..last_slash].to_string();
                let flags = inner[last_slash + 1..].to_string();
                Ok(Expr::Literal(Literal::Regex { pattern, flags }))
            }
            // Parenthesized expression or subquery
            Some(Token::LParen) => {
                self.lexer.next_token();
                if self.lexer.peek() == Some(&Token::Select) {
                    // Subquery: (SELECT ...)
                    let sel = self.parse_select()?;
                    self.lexer.expect(&Token::RParen)?;
                    Ok(Expr::Subquery(Box::new(sel)))
                } else {
                    let expr = self.parse_expr()?;
                    self.lexer.expect(&Token::RParen)?;
                    Ok(expr)
                }
            }
            // Aggregate functions: COUNT, SUM, AVG, MIN, MAX
            Some(Token::Count) => self.parse_aggregate(AggregateFunc::Count),
            Some(Token::Sum) => self.parse_aggregate(AggregateFunc::Sum),
            Some(Token::Avg) => self.parse_aggregate(AggregateFunc::Avg),
            Some(Token::Min) => self.parse_aggregate(AggregateFunc::Min),
            Some(Token::Max) => self.parse_aggregate(AggregateFunc::Max),
            // Identifier: column ref or function call
            Some(Token::Ident) => self.parse_ident_expr(),
            // Some keywords can be used as identifiers in column positions
            Some(tok) if tok.is_keyword() && is_allowed_column_keyword(tok) => {
                self.parse_ident_expr()
            }
            Some(tok) => {
                let msg = format!("unexpected token in expression: {tok}");
                let (_, span) = self.lexer.next_token().unwrap();
                Err(TaqlError::parse(self.lexer.position(span.start), msg))
            }
            None => Err(TaqlError::unexpected_end("expected an expression")),
        }
    }

    pub(super) fn parse_aggregate(&mut self, func: AggregateFunc) -> Result<Expr, TaqlError> {
        self.lexer.next_token(); // consume the aggregate keyword
        self.lexer.expect(&Token::LParen)?;
        let arg = if self.lexer.peek() == Some(&Token::Star) {
            self.lexer.next_token();
            Expr::Star
        } else {
            self.parse_expr()?
        };
        if matches!(func, AggregateFunc::Min | AggregateFunc::Max)
            && self.lexer.eat_if(&Token::Comma).is_some()
        {
            let mut args = vec![arg];
            loop {
                args.push(self.parse_expr()?);
                if self.lexer.eat_if(&Token::Comma).is_none() {
                    break;
                }
            }
            self.lexer.expect(&Token::RParen)?;
            return Ok(Expr::FunctionCall {
                name: if func == AggregateFunc::Min {
                    "min"
                } else {
                    "max"
                }
                .into(),
                args,
            });
        }
        self.lexer.expect(&Token::RParen)?;
        Ok(Expr::Aggregate {
            func,
            arg: Box::new(arg),
        })
    }

    pub(super) fn parse_optional_quantity_unit(
        &mut self,
        number_end: usize,
    ) -> Result<Option<NumericSuffix>, TaqlError> {
        let Some(token) = self.lexer.peek().cloned() else {
            return Ok(None);
        };
        let (unit, start) = match token {
            Token::Ident => {
                let (_, span) = self.lexer.next_token().unwrap();
                if span.start == number_end && matches!(self.lexer.slice(&span), "i" | "j") {
                    return Ok(Some(NumericSuffix::Imaginary));
                }
                (self.lexer.slice(&span).to_string(), span.start)
            }
            Token::StringLiteral => {
                let (_, span) = self.lexer.next_token().unwrap();
                let raw = self.lexer.slice(&span);
                (raw[1..raw.len() - 1].to_string(), span.start)
            }
            _ => return Ok(None),
        };
        Unit::new(&unit).map_err(|_| {
            TaqlError::parse(
                self.lexer.position(start),
                format!("invalid unit literal '{unit}'"),
            )
        })?;
        Ok(Some(NumericSuffix::Unit(unit)))
    }

    /// Parse an identifier expression: column reference or function call.
    pub(super) fn parse_ident_expr(&mut self) -> Result<Expr, TaqlError> {
        let name = self.parse_ident_string()?;

        // Check for g-prefixed aggregate function names (e.g. gmin, gcount)
        if self.lexer.peek() == Some(&Token::LParen)
            && let Some(agg) = aggregate_from_name(&name)
        {
            self.lexer.next_token(); // consume (
            let arg = if self.lexer.peek() == Some(&Token::RParen) {
                // Zero-arg aggregate: gcount(), growid(), etc.
                Expr::Star
            } else if self.lexer.peek() == Some(&Token::Star) {
                self.lexer.next_token();
                Expr::Star
            } else {
                self.parse_expr()?
            };
            self.lexer.expect(&Token::RParen)?;
            return Ok(Expr::Aggregate {
                func: agg,
                arg: Box::new(arg),
            });
        }

        // Function call: name(...)
        if self.lexer.peek() == Some(&Token::LParen) {
            self.lexer.next_token();
            let args = if self.lexer.peek() == Some(&Token::RParen) {
                vec![]
            } else {
                self.parse_expr_list()?
            };
            self.lexer.expect(&Token::RParen)?;
            return Ok(Expr::FunctionCall { name, args });
        }

        // Qualified column: table.column — or dotted function call: meas.epoch(...)
        if self.lexer.peek() == Some(&Token::Dot) {
            self.lexer.next_token();
            let suffix = self.parse_ident_string()?;
            if self.lexer.peek() == Some(&Token::LParen) {
                // Dotted function call: meas.epoch(...)
                let dotted_name = format!("{name}.{suffix}");
                self.lexer.next_token(); // consume (
                let args = if self.lexer.peek() == Some(&Token::RParen) {
                    vec![]
                } else {
                    self.parse_expr_list()?
                };
                self.lexer.expect(&Token::RParen)?;
                return Ok(Expr::FunctionCall {
                    name: dotted_name,
                    args,
                });
            }
            return Ok(Expr::ColumnRef(ColumnRef {
                table: Some(name),
                column: suffix,
            }));
        }

        // Simple column reference.
        Ok(Expr::ColumnRef(ColumnRef {
            table: None,
            column: name,
        }))
    }

    pub(super) fn parse_between_tail(
        &mut self,
        lhs: Expr,
        negated: bool,
    ) -> Result<Expr, TaqlError> {
        self.lexer.expect(&Token::Between)?;
        let low = self.parse_expr_bp(10)?;
        self.lexer.expect(&Token::And)?;
        let high = self.parse_expr_bp(10)?;
        Ok(Expr::Between {
            expr: Box::new(lhs),
            low: Box::new(low),
            high: Box::new(high),
            negated,
        })
    }

    pub(super) fn parse_subquery_after_lbracket(&mut self) -> Result<Expr, TaqlError> {
        let style = self.parse_using_style()?;
        let mut sel = self.parse_select()?;
        if style != IndexStyle::Glish {
            sel.style = style;
        }
        self.lexer.expect(&Token::RBracket)?;
        Ok(Expr::Subquery(Box::new(sel)))
    }

    pub(super) fn parse_in_tail(&mut self, lhs: Expr, negated: bool) -> Result<Expr, TaqlError> {
        self.lexer.expect(&Token::In)?;
        // Bracket syntax: IN [a, b, c] or IN [a:b] or IN [a:b:s] or IN [SELECT ...]
        if self.lexer.eat_if(&Token::LBracket).is_some() {
            if self.peek_starts_subquery() {
                let sub = self.parse_subquery_after_lbracket()?;
                return Ok(Expr::In {
                    expr: Box::new(lhs),
                    values: vec![sub],
                    negated,
                });
            }
            let elements = self.parse_in_set_elements()?;
            self.lexer.expect(&Token::RBracket)?;
            return Ok(Expr::InSet {
                expr: Box::new(lhs),
                elements,
                negated,
            });
        }
        // Parenthesized syntax: IN (a, b, c) or IN (SELECT ...)
        self.lexer.expect(&Token::LParen)?;
        if self.lexer.peek() == Some(&Token::Select) {
            // Subquery: IN (SELECT ...)
            let sel = self.parse_select()?;
            self.lexer.expect(&Token::RParen)?;
            return Ok(Expr::In {
                expr: Box::new(lhs),
                values: vec![Expr::Subquery(Box::new(sel))],
                negated,
            });
        }
        let values = self.parse_expr_list()?;
        self.lexer.expect(&Token::RParen)?;
        Ok(Expr::In {
            expr: Box::new(lhs),
            values,
            negated,
        })
    }

    /// Parse comma-separated IN set elements (values and/or ranges).
    pub(super) fn parse_in_set_elements(&mut self) -> Result<Vec<InSetElement>, TaqlError> {
        let mut elements = vec![self.parse_in_set_element()?];
        while self.lexer.eat_if(&Token::Comma).is_some() {
            elements.push(self.parse_in_set_element()?);
        }
        Ok(elements)
    }

    /// Parse a single IN set element: either a value or a range (`start:end[:step]`).
    pub(super) fn parse_in_set_element(&mut self) -> Result<InSetElement, TaqlError> {
        // Check for leading colon (open-start range)
        if self.lexer.peek() == Some(&Token::Colon) {
            self.lexer.next_token();
            let end = self.parse_expr_bp(10)?;
            let step = if self.lexer.eat_if(&Token::Colon).is_some() {
                Some(self.parse_expr_bp(10)?)
            } else {
                None
            };
            return Ok(InSetElement::Range {
                start: None,
                end: Some(end),
                step,
            });
        }

        let start = self.parse_expr_bp(10)?;
        if self.lexer.eat_if(&Token::Colon).is_some() {
            // Range: start:end or start:end:step
            let end = if self.lexer.peek() == Some(&Token::Colon)
                || self.lexer.peek() == Some(&Token::Comma)
                || self.lexer.peek() == Some(&Token::RBracket)
            {
                None
            } else {
                Some(self.parse_expr_bp(10)?)
            };
            let step = if self.lexer.eat_if(&Token::Colon).is_some() {
                Some(self.parse_expr_bp(10)?)
            } else {
                None
            };
            Ok(InSetElement::Range {
                start: Some(start),
                end,
                step,
            })
        } else {
            Ok(InSetElement::Value(start))
        }
    }

    /// Parse comma-separated index elements for array subscripts.
    pub(super) fn parse_index_elements(&mut self) -> Result<Vec<IndexElement>, TaqlError> {
        let mut elements = vec![self.parse_index_element()?];
        while self.lexer.eat_if(&Token::Comma).is_some() {
            // Empty dimension after comma (e.g. `[1:2,]`) means "all elements".
            if self.lexer.peek() == Some(&Token::RBracket)
                || self.lexer.peek() == Some(&Token::Comma)
            {
                elements.push(IndexElement::Slice {
                    start: None,
                    end: None,
                    step: None,
                });
            } else {
                elements.push(self.parse_index_element()?);
            }
        }
        Ok(elements)
    }

    /// Parse a single index element: value or slice (`start:end[:step]`).
    pub(super) fn parse_index_element(&mut self) -> Result<IndexElement, TaqlError> {
        // Check for leading colon (open-start slice)
        if self.lexer.peek() == Some(&Token::Colon) {
            self.lexer.next_token();
            let end = if self.lexer.peek() != Some(&Token::Colon)
                && self.lexer.peek() != Some(&Token::Comma)
                && self.lexer.peek() != Some(&Token::RBracket)
            {
                Some(self.parse_expr_bp(10)?)
            } else {
                None
            };
            let step = if self.lexer.eat_if(&Token::Colon).is_some() {
                Some(self.parse_expr_bp(10)?)
            } else {
                None
            };
            return Ok(IndexElement::Slice {
                start: None,
                end,
                step,
            });
        }

        let start = self.parse_expr_bp(10)?;
        if self.lexer.eat_if(&Token::Colon).is_some() {
            let end = if self.lexer.peek() != Some(&Token::Colon)
                && self.lexer.peek() != Some(&Token::Comma)
                && self.lexer.peek() != Some(&Token::RBracket)
            {
                Some(self.parse_expr_bp(10)?)
            } else {
                None
            };
            let step = if self.lexer.eat_if(&Token::Colon).is_some() {
                Some(self.parse_expr_bp(10)?)
            } else {
                None
            };
            Ok(IndexElement::Slice {
                start: Some(start),
                end,
                step,
            })
        } else {
            Ok(IndexElement::Single(start))
        }
    }
}
