// SPDX-License-Identifier: LGPL-3.0-or-later
//! Recursive-descent + Pratt expression parser for TaQL.
//!
//! Statement-level grammar is handled by dedicated `parse_select`,
//! `parse_update`, `parse_insert`, `parse_delete` methods. Expressions
//! are parsed by a Pratt parser (`parse_expr_bp`) with the following
//! binding-power table:
//!
//! | Precedence | Operators             | Assoc |
//! |------------|-----------------------|-------|
//! | 2/3        | OR, `\|\|`            | left  |
//! | 4/5        | AND, `&&`             | left  |
//! | 8/9        | NOT (prefix)          | right |
//! | 10/11      | `=` `!=` `<` `<=` `>` `>=` | left |
//! | 12/13      | `+` `-`               | left  |
//! | 14/15      | `*` `/` `%`           | left  |
//! | 17/16      | `**` (power)          | right |
//! | 19         | unary `-` `~`         | right |
//!
//! # C++ reference
//!
//! `TaQLNodeDer.cc` — `TaQLNode::parse()`, `TaQLMultiNode`, etc.

use super::ast::*;
use super::error::TaqlError;
use super::lexer::Lexer;
use super::token::Token;

/// TaQL parser — recursive descent with Pratt expression parsing.
pub struct Parser<'src> {
    lexer: Lexer<'src>,
}

impl<'src> Parser<'src> {
    /// Creates a new parser for the given TaQL source.
    pub fn new(source: &'src str) -> Self {
        Self {
            lexer: Lexer::new(source),
        }
    }

    /// Parses a complete TaQL statement.
    pub fn parse_statement(&mut self) -> Result<Statement, TaqlError> {
        // Optional leading `USING STYLE GLISH|PYTHON`.
        let style = self.parse_using_style()?;

        let stmt = match self.lexer.peek() {
            Some(Token::Select) => {
                let mut s = self.parse_select()?;
                s.style = style;
                Statement::Select(s)
            }
            Some(Token::Count) => {
                // COUNT SELECT ... — returns row count of a SELECT.
                self.lexer.next_token(); // consume COUNT
                if self.lexer.peek() != Some(&Token::Select) {
                    return Err(TaqlError::unexpected_end("expected SELECT after COUNT"));
                }
                let mut s = self.parse_select()?;
                s.style = style;
                Statement::CountSelect(s)
            }
            Some(Token::Update) => Statement::Update(self.parse_update()?),
            Some(Token::Insert) => Statement::Insert(self.parse_insert()?),
            Some(Token::Delete) => Statement::Delete(self.parse_delete()?),
            Some(Token::Calc) => Statement::Calc(self.parse_calc()?),
            Some(Token::Create) => Statement::CreateTable(self.parse_create_table()?),
            Some(Token::Drop) => {
                // DROP TABLE or DROP COLUMN (but DROP COLUMN is ALTER TABLE syntax)
                self.lexer.next_token(); // consume DROP
                self.lexer.expect(&Token::Table)?;
                let table_name = self.parse_ident_string()?;
                Statement::DropTable(DropTableStatement { table_name })
            }
            Some(Token::Alter) => Statement::AlterTable(self.parse_alter_table()?),
            Some(tok) => {
                let tok_str = tok.to_string();
                let (_, span) = self.lexer.next_token().unwrap();
                return Err(TaqlError::parse(
                    self.lexer.position(span.start),
                    format!(
                        "expected SELECT, UPDATE, INSERT, DELETE, CALC, COUNT, CREATE, DROP, or ALTER; found {tok_str}"
                    ),
                ));
            }
            None => {
                return Err(TaqlError::unexpected_end("expected a TaQL statement"));
            }
        };
        // Allow (but don't require) a trailing semicolon.
        // (not a real token — just skip any leftover)
        if !self.lexer.is_eof() {
            if let Some((tok, span)) = self.lexer.next_token() {
                return Err(TaqlError::parse(
                    self.lexer.position(span.start),
                    format!("unexpected token after statement: {tok}"),
                ));
            }
        }
        Ok(stmt)
    }
    /// Parse optional `USING STYLE GLISH|PYTHON` prefix.
    fn parse_using_style(&mut self) -> Result<IndexStyle, TaqlError> {
        if self.lexer.eat_if(&Token::Using).is_some() {
            self.lexer.expect(&Token::Style)?;
            let name = self.parse_ident_string()?;
            match name.to_uppercase().as_str() {
                "GLISH" => Ok(IndexStyle::Glish),
                "PYTHON" => Ok(IndexStyle::Python),
                other => Err(TaqlError::unexpected_end(format!(
                    "expected GLISH or PYTHON after USING STYLE, got {other}"
                ))),
            }
        } else {
            Ok(IndexStyle::Glish)
        }
    }

    // ── SELECT ─────────────────────────────────────────────────────

    fn parse_select(&mut self) -> Result<SelectStatement, TaqlError> {
        self.lexer.expect(&Token::Select)?;

        let distinct = self.lexer.eat_if(&Token::Distinct).is_some();

        let columns = self.parse_select_columns()?;

        let from = if self.lexer.eat_if(&Token::From).is_some() {
            Some(self.parse_table_ref()?)
        } else {
            None
        };

        let joins = self.parse_joins()?;

        let where_clause = if self.lexer.eat_if(&Token::Where).is_some() {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.lexer.eat_if(&Token::Group).is_some() {
            self.lexer.expect(&Token::By)?;
            self.parse_expr_list()?
        } else {
            vec![]
        };

        let having = if self.lexer.eat_if(&Token::Having).is_some() {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let order_by = if self.lexer.eat_if(&Token::Order).is_some() {
            self.lexer.expect(&Token::By)?;
            self.parse_order_by_list()?
        } else {
            vec![]
        };

        let limit = if self.lexer.eat_if(&Token::Limit).is_some() {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let offset = if self.lexer.eat_if(&Token::Offset).is_some() {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let giving = if self.lexer.eat_if(&Token::Giving).is_some() {
            let table_name = self.parse_ident_string()?;
            let output_type = if self.lexer.eat_if(&Token::As).is_some() {
                Some(self.parse_ident_string()?)
            } else {
                None
            };
            Some(GivingClause {
                table_name,
                output_type,
            })
        } else {
            None
        };

        Ok(SelectStatement {
            columns,
            from,
            joins,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            distinct,
            style: IndexStyle::default(),
            giving,
        })
    }

    fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>, TaqlError> {
        // SELECT * (possibly aliased, but typically not)
        if self.lexer.peek() == Some(&Token::Star) {
            self.lexer.next_token();
            // Check if there is an alias
            let alias = if self.lexer.eat_if(&Token::As).is_some() {
                Some(self.parse_ident_string()?)
            } else {
                None
            };
            if alias.is_some() {
                return Ok(vec![SelectColumn {
                    expr: Expr::Star,
                    alias,
                }]);
            }
            return Ok(vec![]);
        }

        let mut columns = vec![self.parse_select_column()?];
        while self.lexer.eat_if(&Token::Comma).is_some() {
            columns.push(self.parse_select_column()?);
        }
        Ok(columns)
    }

    fn parse_select_column(&mut self) -> Result<SelectColumn, TaqlError> {
        let expr = self.parse_expr()?;
        let alias = if self.lexer.eat_if(&Token::As).is_some() {
            Some(self.parse_ident_string()?)
        } else {
            None
        };
        Ok(SelectColumn { expr, alias })
    }

    fn parse_table_ref(&mut self) -> Result<TableRef, TaqlError> {
        let name = self.parse_ident_string()?;
        let alias = if self.lexer.eat_if(&Token::As).is_some()
            || (self.peek_is_ident_like() && !self.peek_is_keyword_clause())
        {
            Some(self.parse_ident_string()?)
        } else {
            None
        };
        Ok(TableRef { name, alias })
    }

    fn parse_joins(&mut self) -> Result<Vec<JoinClause>, TaqlError> {
        let mut joins = Vec::new();
        loop {
            let join_type = match self.lexer.peek() {
                Some(Token::Join) => {
                    self.lexer.next_token();
                    JoinType::Inner
                }
                Some(Token::Inner) => {
                    self.lexer.next_token();
                    self.lexer.expect(&Token::Join)?;
                    JoinType::Inner
                }
                Some(Token::Left) => {
                    self.lexer.next_token();
                    self.lexer.expect(&Token::Join)?;
                    JoinType::Left
                }
                Some(Token::Right) => {
                    self.lexer.next_token();
                    self.lexer.expect(&Token::Join)?;
                    JoinType::Right
                }
                Some(Token::Cross) => {
                    self.lexer.next_token();
                    self.lexer.expect(&Token::Join)?;
                    JoinType::Cross
                }
                _ => break,
            };
            let table = self.parse_table_ref()?;
            let on = if join_type != JoinType::Cross && self.lexer.eat_if(&Token::On).is_some() {
                Some(self.parse_expr()?)
            } else {
                None
            };
            joins.push(JoinClause {
                join_type,
                table,
                on,
            });
        }
        Ok(joins)
    }

    fn parse_order_by_list(&mut self) -> Result<Vec<OrderBySpec>, TaqlError> {
        let mut specs = vec![self.parse_order_by_spec()?];
        while self.lexer.eat_if(&Token::Comma).is_some() {
            specs.push(self.parse_order_by_spec()?);
        }
        Ok(specs)
    }

    fn parse_order_by_spec(&mut self) -> Result<OrderBySpec, TaqlError> {
        let expr = self.parse_expr()?;
        let ascending = if self.lexer.eat_if(&Token::Desc).is_some() {
            false
        } else {
            self.lexer.eat_if(&Token::Asc);
            true
        };
        Ok(OrderBySpec { expr, ascending })
    }

    // ── UPDATE ─────────────────────────────────────────────────────

    fn parse_update(&mut self) -> Result<UpdateStatement, TaqlError> {
        self.lexer.expect(&Token::Update)?;

        let table = if self.lexer.peek() != Some(&Token::Set) {
            Some(self.parse_table_ref()?)
        } else {
            None
        };

        self.lexer.expect(&Token::Set)?;

        let mut assignments = vec![self.parse_assignment()?];
        while self.lexer.eat_if(&Token::Comma).is_some() {
            assignments.push(self.parse_assignment()?);
        }

        let where_clause = if self.lexer.eat_if(&Token::Where).is_some() {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let limit = if self.lexer.eat_if(&Token::Limit).is_some() {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(UpdateStatement {
            table,
            assignments,
            where_clause,
            limit,
        })
    }

    fn parse_assignment(&mut self) -> Result<Assignment, TaqlError> {
        let column = self.parse_ident_string()?;
        self.lexer.expect(&Token::Eq)?;
        let value = self.parse_expr()?;
        Ok(Assignment { column, value })
    }

    // ── INSERT ─────────────────────────────────────────────────────

    fn parse_insert(&mut self) -> Result<InsertStatement, TaqlError> {
        self.lexer.expect(&Token::Insert)?;
        self.lexer.expect(&Token::Into)?;

        // Optional table name (may be absent if operating on the current table).
        let table = if self.peek_is_ident_like() && self.lexer.peek() != Some(&Token::Values) {
            // Check if this looks like a table name vs column list
            if self.lexer.peek() != Some(&Token::LParen) {
                Some(self.parse_table_ref()?)
            } else {
                None
            }
        } else {
            None
        };

        // Optional column list: (col1, col2, ...)
        let columns = if self.lexer.eat_if(&Token::LParen).is_some() {
            let mut cols = vec![self.parse_ident_string()?];
            while self.lexer.eat_if(&Token::Comma).is_some() {
                cols.push(self.parse_ident_string()?);
            }
            self.lexer.expect(&Token::RParen)?;
            cols
        } else {
            vec![]
        };

        self.lexer.expect(&Token::Values)?;

        let mut values = vec![self.parse_value_row()?];
        while self.lexer.eat_if(&Token::Comma).is_some() {
            values.push(self.parse_value_row()?);
        }

        Ok(InsertStatement {
            table,
            columns,
            values,
        })
    }

    fn parse_value_row(&mut self) -> Result<Vec<Expr>, TaqlError> {
        self.lexer.expect(&Token::LParen)?;
        let exprs = self.parse_expr_list()?;
        self.lexer.expect(&Token::RParen)?;
        Ok(exprs)
    }

    // ── DELETE ─────────────────────────────────────────────────────

    fn parse_delete(&mut self) -> Result<DeleteStatement, TaqlError> {
        self.lexer.expect(&Token::Delete)?;
        self.lexer.expect(&Token::From)?;

        let table = if self.peek_is_ident_like()
            && self.lexer.peek() != Some(&Token::Where)
            && self.lexer.peek() != Some(&Token::Limit)
        {
            Some(self.parse_table_ref()?)
        } else {
            None
        };

        let where_clause = if self.lexer.eat_if(&Token::Where).is_some() {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let limit = if self.lexer.eat_if(&Token::Limit).is_some() {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(DeleteStatement {
            table,
            where_clause,
            limit,
        })
    }

    // ── CALC statement ────────────────────────────────────────────

    fn parse_calc(&mut self) -> Result<CalcStatement, TaqlError> {
        self.lexer.expect(&Token::Calc)?;
        let expr = self.parse_expr()?;
        let from = if self.lexer.eat_if(&Token::From).is_some() {
            Some(self.parse_table_ref()?)
        } else {
            None
        };
        Ok(CalcStatement { expr, from })
    }

    // ── ALTER TABLE statement ─────────────────────────────────────

    fn parse_create_table(&mut self) -> Result<CreateTableStatement, TaqlError> {
        self.lexer.expect(&Token::Create)?;
        self.lexer.expect(&Token::Table)?;
        let table_name = self.parse_ident_string()?;
        self.lexer.expect(&Token::LParen)?;
        let mut columns = Vec::new();
        loop {
            let name = self.parse_ident_string()?;
            let data_type = self.parse_ident_string()?;
            columns.push(ColumnDef { name, data_type });
            if self.lexer.eat_if(&Token::Comma).is_none() {
                break;
            }
        }
        self.lexer.expect(&Token::RParen)?;
        Ok(CreateTableStatement {
            table_name,
            columns,
        })
    }

    fn parse_alter_table(&mut self) -> Result<AlterTableStatement, TaqlError> {
        self.lexer.expect(&Token::Alter)?;
        self.lexer.expect(&Token::Table)?;

        // Optional table name
        let table = if self.peek_is_ident_like()
            && self.lexer.peek() != Some(&Token::Add)
            && self.lexer.peek() != Some(&Token::Drop)
            && self.lexer.peek() != Some(&Token::Rename)
            && self.lexer.peek() != Some(&Token::Set)
        {
            Some(self.parse_table_ref()?)
        } else {
            None
        };

        let operation = match self.lexer.peek() {
            Some(Token::Add) => {
                self.lexer.next_token(); // consume ADD
                match self.lexer.peek() {
                    Some(Token::Column) => {
                        self.lexer.next_token(); // consume COLUMN
                        let name = self.parse_ident_string()?;
                        let data_type = self.parse_ident_string()?;
                        AlterOperation::AddColumn { name, data_type }
                    }
                    Some(Token::Row) => {
                        self.lexer.next_token(); // consume ROW
                        let count = if self.lexer.peek().is_some()
                            && !self.lexer.is_eof()
                            && self.lexer.peek() != Some(&Token::RParen)
                        {
                            self.parse_expr().ok()
                        } else {
                            None
                        };
                        AlterOperation::AddRow { count }
                    }
                    _ => {
                        return Err(TaqlError::unexpected_end(
                            "expected COLUMN or ROW after ADD",
                        ));
                    }
                }
            }
            Some(Token::Drop) => {
                self.lexer.next_token(); // consume DROP
                self.lexer.expect(&Token::Column)?;
                let name = self.parse_ident_string()?;
                AlterOperation::DropColumn { name }
            }
            Some(Token::Rename) => {
                self.lexer.next_token(); // consume RENAME
                self.lexer.expect(&Token::Column)?;
                let old_name = self.parse_ident_string()?;
                self.lexer.expect(&Token::To)?;
                let new_name = self.parse_ident_string()?;
                AlterOperation::RenameColumn { old_name, new_name }
            }
            Some(Token::Set) => {
                self.lexer.next_token(); // consume SET
                self.lexer.expect(&Token::Keyword)?;
                let name = self.parse_ident_string()?;
                self.lexer.expect(&Token::Eq)?;
                let value = self.parse_expr()?;
                AlterOperation::SetKeyword { name, value }
            }
            _ => {
                return Err(TaqlError::unexpected_end(
                    "expected ADD, DROP, RENAME, or SET after ALTER TABLE",
                ));
            }
        };

        Ok(AlterTableStatement { table, operation })
    }

    // ── Expression parser (Pratt) ──────────────────────────────────

    /// Parse an expression at minimum binding power 0.
    pub fn parse_expr(&mut self) -> Result<Expr, TaqlError> {
        self.parse_expr_bp(0)
    }

    fn parse_expr_list(&mut self) -> Result<Vec<Expr>, TaqlError> {
        let mut exprs = vec![self.parse_expr()?];
        while self.lexer.eat_if(&Token::Comma).is_some() {
            exprs.push(self.parse_expr()?);
        }
        Ok(exprs)
    }

    /// Pratt parser: parse an expression with minimum binding power `min_bp`.
    fn parse_expr_bp(&mut self, min_bp: u8) -> Result<Expr, TaqlError> {
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
    fn parse_prefix(&mut self) -> Result<Expr, TaqlError> {
        match self.lexer.peek() {
            Some(Token::IntLiteral) => {
                let (_, span) = self.lexer.next_token().unwrap();
                let s = self.lexer.slice(&span);
                let n: i64 = s.parse().map_err(|_| {
                    TaqlError::parse(self.lexer.position(span.start), "invalid integer literal")
                })?;
                Ok(Expr::Literal(Literal::Int(n)))
            }
            Some(Token::FloatLiteral) => {
                let (_, span) = self.lexer.next_token().unwrap();
                let s = self.lexer.slice(&span);
                let v: f64 = s.parse().map_err(|_| {
                    TaqlError::parse(self.lexer.position(span.start), "invalid float literal")
                })?;
                Ok(Expr::Literal(Literal::Float(v)))
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
                self.lexer.next_token();
                // Optional parentheses: ROWID or ROWID()
                if self.lexer.eat_if(&Token::LParen).is_some() {
                    self.lexer.expect(&Token::RParen)?;
                }
                Ok(Expr::RowNumber)
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

    fn parse_aggregate(&mut self, func: AggregateFunc) -> Result<Expr, TaqlError> {
        self.lexer.next_token(); // consume the aggregate keyword
        self.lexer.expect(&Token::LParen)?;
        let arg = if self.lexer.peek() == Some(&Token::Star) {
            self.lexer.next_token();
            Expr::Star
        } else {
            self.parse_expr()?
        };
        self.lexer.expect(&Token::RParen)?;
        Ok(Expr::Aggregate {
            func,
            arg: Box::new(arg),
        })
    }

    /// Parse an identifier expression: column reference or function call.
    fn parse_ident_expr(&mut self) -> Result<Expr, TaqlError> {
        let name = self.parse_ident_string()?;

        // Check for g-prefixed aggregate function names (e.g. gmin, gcount)
        if self.lexer.peek() == Some(&Token::LParen) {
            if let Some(agg) = aggregate_from_name(&name) {
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

        // Qualified column: table.column
        if self.lexer.peek() == Some(&Token::Dot) {
            self.lexer.next_token();
            let column = self.parse_ident_string()?;
            return Ok(Expr::ColumnRef(ColumnRef {
                table: Some(name),
                column,
            }));
        }

        // Simple column reference.
        Ok(Expr::ColumnRef(ColumnRef {
            table: None,
            column: name,
        }))
    }

    fn parse_between_tail(&mut self, lhs: Expr, negated: bool) -> Result<Expr, TaqlError> {
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

    fn parse_in_tail(&mut self, lhs: Expr, negated: bool) -> Result<Expr, TaqlError> {
        self.lexer.expect(&Token::In)?;
        // Bracket syntax: IN [a, b, c] or IN [a:b] or IN [a:b:s]
        if self.lexer.eat_if(&Token::LBracket).is_some() {
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
    fn parse_in_set_elements(&mut self) -> Result<Vec<InSetElement>, TaqlError> {
        let mut elements = vec![self.parse_in_set_element()?];
        while self.lexer.eat_if(&Token::Comma).is_some() {
            elements.push(self.parse_in_set_element()?);
        }
        Ok(elements)
    }

    /// Parse a single IN set element: either a value or a range (start:end[:step]).
    fn parse_in_set_element(&mut self) -> Result<InSetElement, TaqlError> {
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
    fn parse_index_elements(&mut self) -> Result<Vec<IndexElement>, TaqlError> {
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

    /// Parse a single index element: value or slice (start:end[:step]).
    fn parse_index_element(&mut self) -> Result<IndexElement, TaqlError> {
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

    // ── Helpers ────────────────────────────────────────────────────

    /// Parse an identifier token and return its string value.
    /// Accepts both Ident tokens and certain keywords used as identifiers.
    fn parse_ident_string(&mut self) -> Result<String, TaqlError> {
        match self.lexer.next_token() {
            Some((Token::Ident, span)) => Ok(self.lexer.slice(&span).to_string()),
            // Allow keywords to be used as identifiers in certain positions
            Some((ref tok, span)) if tok.is_keyword() => Ok(self.lexer.slice(&span).to_string()),
            Some((tok, span)) => Err(TaqlError::parse(
                self.lexer.position(span.start),
                format!("expected identifier, found {tok}"),
            )),
            None => Err(TaqlError::unexpected_end("expected identifier")),
        }
    }

    /// Returns true if the next token looks like an identifier.
    fn peek_is_ident_like(&mut self) -> bool {
        match self.lexer.peek() {
            Some(Token::Ident) => true,
            Some(tok) => tok.is_keyword(),
            None => false,
        }
    }

    /// Returns true if the peeked token is a clause keyword (WHERE, ORDER, etc.)
    fn peek_is_keyword_clause(&mut self) -> bool {
        matches!(
            self.lexer.peek(),
            Some(
                Token::Where
                    | Token::Order
                    | Token::Group
                    | Token::Having
                    | Token::Limit
                    | Token::Offset
                    | Token::Join
                    | Token::Inner
                    | Token::Left
                    | Token::Right
                    | Token::Cross
                    | Token::On
                    | Token::Set
            )
        )
    }
}

/// Returns (left_bp, right_bp) for infix operators, or None.
fn infix_binding_power(tok: &Token) -> Option<(u8, u8)> {
    match tok {
        Token::Or | Token::PipePipe => Some((2, 3)),
        Token::And | Token::AmpAmp => Some((4, 5)),
        // Bitwise operators between logical and comparison
        Token::Pipe => Some((3, 4)), // bitwise OR — just above logical OR
        Token::Caret => Some((5, 6)), // bitwise XOR
        Token::Amp => Some((7, 8)),  // bitwise AND — just below comparison
        Token::Eq
        | Token::EqEq
        | Token::Ne
        | Token::LtGt
        | Token::Lt
        | Token::Le
        | Token::Gt
        | Token::Ge => Some((10, 11)),
        Token::Plus | Token::Minus => Some((12, 13)),
        Token::Star | Token::Slash | Token::Percent => Some((14, 15)),
        Token::DoubleStar => Some((17, 16)), // right-associative
        _ => None,
    }
}

/// Maps an infix token to a `BinaryOp`.
fn infix_op(tok: &Token) -> BinaryOp {
    match tok {
        Token::Plus => BinaryOp::Add,
        Token::Minus => BinaryOp::Sub,
        Token::Star => BinaryOp::Mul,
        Token::Slash => BinaryOp::Div,
        Token::Percent => BinaryOp::Modulo,
        Token::DoubleStar => BinaryOp::Power,
        Token::Eq | Token::EqEq => BinaryOp::Eq,
        Token::Ne | Token::LtGt => BinaryOp::Ne,
        Token::Lt => BinaryOp::Lt,
        Token::Le => BinaryOp::Le,
        Token::Gt => BinaryOp::Gt,
        Token::Ge => BinaryOp::Ge,
        Token::And | Token::AmpAmp => BinaryOp::And,
        Token::Or | Token::PipePipe => BinaryOp::Or,
        Token::Amp => BinaryOp::BitAnd,
        Token::Pipe => BinaryOp::BitOr,
        Token::Caret => BinaryOp::BitXor,
        _ => unreachable!("infix_op called with non-infix token: {tok}"),
    }
}

/// Returns true if a keyword can also appear as a column/table name.
fn is_allowed_column_keyword(tok: &Token) -> bool {
    // Most keywords can be used as identifiers in column positions,
    // except structural keywords that start clauses.
    !matches!(
        tok,
        Token::Select
            | Token::From
            | Token::Where
            | Token::Order
            | Token::Group
            | Token::Having
            | Token::Limit
            | Token::Offset
            | Token::Distinct
            | Token::Update
            | Token::Set
            | Token::Insert
            | Token::Into
            | Token::Values
            | Token::Delete
            | Token::Join
            | Token::Inner
            | Token::Left
            | Token::Right
            | Token::Cross
            | Token::On
            | Token::As
            | Token::And
            | Token::Or
            | Token::Not
            | Token::True
            | Token::False
            | Token::Null
            | Token::Using
            | Token::Style
            | Token::Create
            | Token::Giving
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(query: &str) -> Statement {
        Parser::new(query).parse_statement().unwrap()
    }

    fn parse_err(query: &str) -> TaqlError {
        Parser::new(query).parse_statement().unwrap_err()
    }

    /// Parse → Display → parse → assert ASTs equal.
    fn roundtrip(query: &str) {
        let ast1 = parse(query);
        let displayed = ast1.to_string();
        let ast2 = parse(&displayed);
        assert_eq!(
            ast1, ast2,
            "round-trip failed:\n  input:     {query}\n  displayed: {displayed}"
        );
    }

    // ── SELECT tests ──

    #[test]
    fn select_star() {
        let stmt = parse("SELECT *");
        match stmt {
            Statement::Select(s) => {
                assert!(s.columns.is_empty());
                assert!(!s.distinct);
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn select_columns() {
        let stmt = parse("SELECT col1, col2");
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.columns.len(), 2);
                assert_eq!(
                    s.columns[0].expr,
                    Expr::ColumnRef(ColumnRef {
                        table: None,
                        column: "col1".to_string()
                    })
                );
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn select_alias() {
        let stmt = parse("SELECT col1 AS c1");
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.columns[0].alias, Some("c1".to_string()));
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn select_where() {
        let stmt = parse("SELECT * WHERE flux > 1.0");
        match stmt {
            Statement::Select(s) => {
                assert!(s.where_clause.is_some());
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn select_compound_where() {
        let stmt = parse("SELECT * WHERE flux > 1.0 AND id < 10 OR name = 'test'");
        match stmt {
            Statement::Select(s) => {
                // Should parse as: (flux > 1.0 AND id < 10) OR (name = 'test')
                assert!(matches!(
                    s.where_clause.as_ref().unwrap(),
                    Expr::Binary {
                        op: BinaryOp::Or,
                        ..
                    }
                ));
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn select_like() {
        let stmt = parse("SELECT * WHERE name LIKE 'test%'");
        match stmt {
            Statement::Select(s) => {
                assert!(matches!(
                    s.where_clause.as_ref().unwrap(),
                    Expr::Like {
                        negated: false,
                        case_insensitive: false,
                        ..
                    }
                ));
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn select_ilike() {
        let stmt = parse("SELECT * WHERE name ILIKE 'test%'");
        match stmt {
            Statement::Select(s) => {
                assert!(matches!(
                    s.where_clause.as_ref().unwrap(),
                    Expr::Like {
                        negated: false,
                        case_insensitive: true,
                        ..
                    }
                ));
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn select_between() {
        let stmt = parse("SELECT * WHERE id BETWEEN 1 AND 10");
        match stmt {
            Statement::Select(s) => {
                assert!(matches!(
                    s.where_clause.as_ref().unwrap(),
                    Expr::Between { negated: false, .. }
                ));
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn select_in() {
        let stmt = parse("SELECT * WHERE id IN (1, 2, 3)");
        match stmt {
            Statement::Select(s) => match s.where_clause.as_ref().unwrap() {
                Expr::In {
                    negated, values, ..
                } => {
                    assert!(!negated);
                    assert_eq!(values.len(), 3);
                }
                other => panic!("expected IN, got {other:?}"),
            },
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn select_is_null() {
        let stmt = parse("SELECT * WHERE name IS NULL");
        match stmt {
            Statement::Select(s) => {
                assert!(matches!(
                    s.where_clause.as_ref().unwrap(),
                    Expr::IsNull { negated: false, .. }
                ));
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn select_is_not_null() {
        let stmt = parse("SELECT * WHERE name IS NOT NULL");
        match stmt {
            Statement::Select(s) => {
                assert!(matches!(
                    s.where_clause.as_ref().unwrap(),
                    Expr::IsNull { negated: true, .. }
                ));
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn select_order_by() {
        let stmt = parse("SELECT * ORDER BY id ASC, flux DESC");
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.order_by.len(), 2);
                assert!(s.order_by[0].ascending);
                assert!(!s.order_by[1].ascending);
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn select_limit_offset() {
        let stmt = parse("SELECT * LIMIT 10 OFFSET 5");
        match stmt {
            Statement::Select(s) => {
                assert!(s.limit.is_some());
                assert!(s.offset.is_some());
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn select_distinct() {
        let stmt = parse("SELECT DISTINCT col1");
        match stmt {
            Statement::Select(s) => {
                assert!(s.distinct);
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn select_expression_column() {
        let stmt = parse("SELECT flux * 2.0 AS double_flux");
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.columns.len(), 1);
                assert!(matches!(
                    &s.columns[0].expr,
                    Expr::Binary {
                        op: BinaryOp::Mul,
                        ..
                    }
                ));
                assert_eq!(s.columns[0].alias, Some("double_flux".to_string()));
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn operator_precedence_add_mul() {
        // 1 + 2 * 3 should parse as 1 + (2 * 3)
        let stmt = parse("SELECT 1 + 2 * 3 AS x");
        match stmt {
            Statement::Select(s) => match &s.columns[0].expr {
                Expr::Binary {
                    op: BinaryOp::Add,
                    right,
                    ..
                } => {
                    assert!(matches!(
                        **right,
                        Expr::Binary {
                            op: BinaryOp::Mul,
                            ..
                        }
                    ));
                }
                other => panic!("expected Add, got {other:?}"),
            },
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn operator_precedence_power_right_assoc() {
        // 2 ** 3 ** 4 should parse as 2 ** (3 ** 4)
        let stmt = parse("SELECT 2 ** 3 ** 4 AS x");
        match stmt {
            Statement::Select(s) => match &s.columns[0].expr {
                Expr::Binary {
                    op: BinaryOp::Power,
                    right,
                    ..
                } => {
                    assert!(matches!(
                        **right,
                        Expr::Binary {
                            op: BinaryOp::Power,
                            ..
                        }
                    ));
                }
                other => panic!("expected Power, got {other:?}"),
            },
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn select_not_in() {
        let stmt = parse("SELECT * WHERE id NOT IN (1, 2)");
        match stmt {
            Statement::Select(s) => {
                assert!(matches!(
                    s.where_clause.as_ref().unwrap(),
                    Expr::In { negated: true, .. }
                ));
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn select_function_call() {
        let stmt = parse("SELECT sqrt(flux) AS root_flux");
        match stmt {
            Statement::Select(s) => {
                assert!(
                    matches!(&s.columns[0].expr, Expr::FunctionCall { name, args } if name == "sqrt" && args.len() == 1)
                );
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn select_aggregate() {
        let stmt = parse("SELECT COUNT(*), SUM(flux), AVG(flux)");
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.columns.len(), 3);
                assert!(matches!(
                    &s.columns[0].expr,
                    Expr::Aggregate {
                        func: AggregateFunc::Count,
                        ..
                    }
                ));
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn select_group_by_having() {
        let stmt = parse("SELECT category, COUNT(*) GROUP BY category HAVING COUNT(*) > 1");
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.group_by.len(), 1);
                assert!(s.having.is_some());
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn select_rowid() {
        let stmt = parse("SELECT ROWID(), col1");
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.columns.len(), 2);
                assert!(matches!(&s.columns[0].expr, Expr::RowNumber));
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn select_qualified_column() {
        let stmt = parse("SELECT t.col1 FROM mytable AS t");
        match stmt {
            Statement::Select(s) => {
                assert!(matches!(
                    &s.columns[0].expr,
                    Expr::ColumnRef(ColumnRef { table: Some(t), column: c })
                    if t == "t" && c == "col1"
                ));
            }
            _ => panic!("expected SELECT"),
        }
    }

    // ── UPDATE tests ──

    #[test]
    fn update_basic() {
        let stmt = parse("UPDATE SET flux = 1.0 WHERE id = 1");
        match stmt {
            Statement::Update(u) => {
                assert_eq!(u.assignments.len(), 1);
                assert_eq!(u.assignments[0].column, "flux");
                assert!(u.where_clause.is_some());
            }
            _ => panic!("expected UPDATE"),
        }
    }

    #[test]
    fn update_expression_rhs() {
        let stmt = parse("UPDATE SET flux = flux * 2.0");
        match stmt {
            Statement::Update(u) => {
                assert!(matches!(
                    &u.assignments[0].value,
                    Expr::Binary {
                        op: BinaryOp::Mul,
                        ..
                    }
                ));
            }
            _ => panic!("expected UPDATE"),
        }
    }

    // ── INSERT tests ──

    #[test]
    fn insert_basic() {
        let stmt = parse("INSERT INTO VALUES (1, 2.0, 'hello')");
        match stmt {
            Statement::Insert(i) => {
                assert_eq!(i.values.len(), 1);
                assert_eq!(i.values[0].len(), 3);
            }
            _ => panic!("expected INSERT"),
        }
    }

    #[test]
    fn insert_with_columns() {
        let stmt = parse("INSERT INTO (id, flux) VALUES (1, 2.0)");
        match stmt {
            Statement::Insert(i) => {
                assert_eq!(i.columns, vec!["id", "flux"]);
                assert_eq!(i.values.len(), 1);
            }
            _ => panic!("expected INSERT"),
        }
    }

    #[test]
    fn insert_multiple_rows() {
        let stmt = parse("INSERT INTO VALUES (1, 'a'), (2, 'b')");
        match stmt {
            Statement::Insert(i) => {
                assert_eq!(i.values.len(), 2);
            }
            _ => panic!("expected INSERT"),
        }
    }

    // ── DELETE tests ──

    #[test]
    fn delete_basic() {
        let stmt = parse("DELETE FROM WHERE id > 5");
        match stmt {
            Statement::Delete(d) => {
                assert!(d.where_clause.is_some());
            }
            _ => panic!("expected DELETE"),
        }
    }

    #[test]
    fn delete_with_limit() {
        let stmt = parse("DELETE FROM WHERE id > 5 LIMIT 3");
        match stmt {
            Statement::Delete(d) => {
                assert!(d.limit.is_some());
            }
            _ => panic!("expected DELETE"),
        }
    }

    // ── JOIN tests ──

    #[test]
    fn select_inner_join() {
        let stmt = parse("SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id");
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.joins.len(), 1);
                assert_eq!(s.joins[0].join_type, JoinType::Inner);
                assert!(s.joins[0].on.is_some());
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn select_cross_join() {
        let stmt = parse("SELECT * FROM t1 CROSS JOIN t2");
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.joins.len(), 1);
                assert_eq!(s.joins[0].join_type, JoinType::Cross);
                assert!(s.joins[0].on.is_none());
            }
            _ => panic!("expected SELECT"),
        }
    }

    // ── Round-trip tests ──

    #[test]
    fn roundtrip_select_star() {
        roundtrip("SELECT *");
    }

    #[test]
    fn roundtrip_select_columns() {
        roundtrip("SELECT col1, col2");
    }

    #[test]
    fn roundtrip_select_where() {
        roundtrip("SELECT * WHERE (flux > 1.0)");
    }

    #[test]
    fn roundtrip_select_order_by() {
        roundtrip("SELECT * ORDER BY id ASC, flux DESC");
    }

    #[test]
    fn roundtrip_update() {
        roundtrip("UPDATE SET flux = 1.0 WHERE (id = 1)");
    }

    #[test]
    fn roundtrip_insert() {
        roundtrip("INSERT INTO VALUES (1, 2.0, 'hello')");
    }

    #[test]
    fn roundtrip_delete() {
        roundtrip("DELETE FROM WHERE (id > 5)");
    }

    // ── Error tests ──

    #[test]
    fn error_empty() {
        let err = parse_err("");
        assert!(matches!(err, TaqlError::UnexpectedEnd { .. }));
    }

    #[test]
    fn error_unexpected_token() {
        let err = parse_err("FROBNICATE");
        assert!(matches!(err, TaqlError::ParseError { .. }));
    }

    #[test]
    fn error_trailing_token() {
        let err = parse_err("SELECT * WHERE flux > 1.0 EXTRA");
        assert!(matches!(err, TaqlError::ParseError { .. }));
    }

    // ── Comment test ──

    #[test]
    fn comment_skipped() {
        let stmt = parse("# this is a comment\nSELECT *");
        assert!(matches!(stmt, Statement::Select(_)));
    }

    #[test]
    fn case_insensitive_keywords() {
        let stmt = parse("select * where flux > 1.0");
        assert!(matches!(stmt, Statement::Select(_)));
    }

    #[test]
    fn unary_negate() {
        let stmt = parse("SELECT -1 AS neg");
        match stmt {
            Statement::Select(s) => {
                assert!(matches!(
                    &s.columns[0].expr,
                    Expr::Unary {
                        op: UnaryOp::Negate,
                        ..
                    }
                ));
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn not_between() {
        let stmt = parse("SELECT * WHERE id NOT BETWEEN 1 AND 10");
        match stmt {
            Statement::Select(s) => {
                assert!(matches!(
                    s.where_clause.as_ref().unwrap(),
                    Expr::Between { negated: true, .. }
                ));
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn select_not_like() {
        let stmt = parse("SELECT * WHERE name NOT LIKE 'test%'");
        match stmt {
            Statement::Select(s) => {
                assert!(matches!(
                    s.where_clause.as_ref().unwrap(),
                    Expr::Like {
                        negated: true,
                        case_insensitive: false,
                        ..
                    }
                ));
            }
            _ => panic!("expected SELECT"),
        }
    }

    // ── CALC statement tests ──

    #[test]
    fn calc_simple_expr() {
        let stmt = parse("CALC 1 + 2");
        match stmt {
            Statement::Calc(c) => {
                assert!(c.from.is_none());
                assert!(matches!(c.expr, Expr::Binary { .. }));
            }
            _ => panic!("expected Calc"),
        }
    }

    #[test]
    fn calc_with_from() {
        let stmt = parse("CALC SUM(col) FROM my_table");
        match stmt {
            Statement::Calc(c) => {
                assert!(c.from.is_some());
                assert_eq!(c.from.unwrap().name, "my_table");
            }
            _ => panic!("expected Calc"),
        }
    }

    // ── ALTER TABLE tests ──

    #[test]
    fn alter_add_column() {
        let stmt = parse("ALTER TABLE ADD COLUMN new_col INT");
        match stmt {
            Statement::AlterTable(a) => {
                assert!(matches!(
                    a.operation,
                    AlterOperation::AddColumn { ref name, ref data_type }
                    if name == "new_col" && data_type == "INT"
                ));
            }
            _ => panic!("expected AlterTable"),
        }
    }

    #[test]
    fn alter_drop_column() {
        let stmt = parse("ALTER TABLE DROP COLUMN old_col");
        match stmt {
            Statement::AlterTable(a) => {
                assert!(matches!(
                    a.operation,
                    AlterOperation::DropColumn { ref name } if name == "old_col"
                ));
            }
            _ => panic!("expected AlterTable"),
        }
    }

    #[test]
    fn alter_rename_column() {
        let stmt = parse("ALTER TABLE RENAME COLUMN a TO b");
        match stmt {
            Statement::AlterTable(a) => {
                assert!(matches!(
                    a.operation,
                    AlterOperation::RenameColumn { ref old_name, ref new_name }
                    if old_name == "a" && new_name == "b"
                ));
            }
            _ => panic!("expected AlterTable"),
        }
    }

    #[test]
    fn alter_add_row() {
        let stmt = parse("ALTER TABLE ADD ROW");
        match stmt {
            Statement::AlterTable(a) => {
                assert!(matches!(
                    a.operation,
                    AlterOperation::AddRow { count: None }
                ));
            }
            _ => panic!("expected AlterTable"),
        }
    }

    #[test]
    fn alter_set_keyword() {
        let stmt = parse("ALTER TABLE SET KEYWORD my_key = 42");
        match stmt {
            Statement::AlterTable(a) => {
                assert!(matches!(
                    a.operation,
                    AlterOperation::SetKeyword { ref name, .. }
                    if name == "my_key"
                ));
            }
            _ => panic!("expected AlterTable"),
        }
    }

    // ── g-prefix aggregate tests ──

    #[test]
    fn gmin_aggregate_in_select() {
        let stmt = parse("SELECT GMIN(col) FROM t GROUP BY grp");
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.columns.len(), 1);
                match &s.columns[0].expr {
                    Expr::Aggregate { func, .. } => {
                        assert_eq!(*func, AggregateFunc::Min);
                    }
                    other => panic!("expected Aggregate, got {other:?}"),
                }
            }
            _ => panic!("expected SELECT"),
        }
    }

    // ── Wave 2: Bitwise operators ──

    #[test]
    fn bitwise_and() {
        let stmt = parse("SELECT x & 255 AS masked");
        match stmt {
            Statement::Select(s) => match &s.columns[0].expr {
                Expr::Binary {
                    op: BinaryOp::BitAnd,
                    ..
                } => {}
                other => panic!("expected BitAnd, got {other:?}"),
            },
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn bitwise_or_xor() {
        let stmt = parse("SELECT x | y ^ z");
        match stmt {
            Statement::Select(s) => {
                // Should parse; exact shape depends on precedence
                assert_eq!(s.columns.len(), 1);
            }
            _ => panic!("expected SELECT"),
        }
    }

    // ── Wave 2: Regex literals and matching ──

    #[test]
    fn regex_literal() {
        let stmt = parse("SELECT * WHERE name =~ p/foo.*/i");
        match stmt {
            Statement::Select(s) => match s.where_clause.as_ref().unwrap() {
                Expr::RegexMatch {
                    negated, pattern, ..
                } => {
                    assert!(!negated);
                    assert!(matches!(
                        pattern.as_ref(),
                        Expr::Literal(Literal::Regex {
                            pattern: p,
                            flags: f,
                        }) if p == "foo.*" && f == "i"
                    ));
                }
                other => panic!("expected RegexMatch, got {other:?}"),
            },
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn regex_not_match() {
        let stmt = parse("SELECT * WHERE name !~ p/bad/");
        match stmt {
            Statement::Select(s) => {
                assert!(matches!(
                    s.where_clause.as_ref().unwrap(),
                    Expr::RegexMatch { negated: true, .. }
                ));
            }
            _ => panic!("expected SELECT"),
        }
    }

    // ── Wave 2: IN with bracket ranges ──

    #[test]
    fn in_bracket_values() {
        let stmt = parse("SELECT * WHERE x IN [1, 2, 3]");
        match stmt {
            Statement::Select(s) => match s.where_clause.as_ref().unwrap() {
                Expr::InSet {
                    elements, negated, ..
                } => {
                    assert!(!negated);
                    assert_eq!(elements.len(), 3);
                    assert!(matches!(&elements[0], InSetElement::Value(_)));
                }
                other => panic!("expected InSet, got {other:?}"),
            },
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn in_bracket_range() {
        let stmt = parse("SELECT * WHERE x IN [1:10]");
        match stmt {
            Statement::Select(s) => match s.where_clause.as_ref().unwrap() {
                Expr::InSet { elements, .. } => {
                    assert_eq!(elements.len(), 1);
                    assert!(matches!(
                        &elements[0],
                        InSetElement::Range {
                            start: Some(_),
                            end: Some(_),
                            step: None,
                        }
                    ));
                }
                other => panic!("expected InSet, got {other:?}"),
            },
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn in_bracket_range_with_step() {
        let stmt = parse("SELECT * WHERE x IN [0:10:2]");
        match stmt {
            Statement::Select(s) => match s.where_clause.as_ref().unwrap() {
                Expr::InSet { elements, .. } => {
                    assert_eq!(elements.len(), 1);
                    assert!(matches!(
                        &elements[0],
                        InSetElement::Range {
                            start: Some(_),
                            end: Some(_),
                            step: Some(_),
                        }
                    ));
                }
                other => panic!("expected InSet, got {other:?}"),
            },
            _ => panic!("expected SELECT"),
        }
    }

    // ── Wave 2: Array indexing (basic parsing, full impl in Wave 3) ──

    #[test]
    fn array_index_single() {
        let stmt = parse("SELECT arr[0]");
        match stmt {
            Statement::Select(s) => {
                assert!(matches!(&s.columns[0].expr, Expr::ArrayIndex { .. }));
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn array_index_slice() {
        let stmt = parse("SELECT arr[1:3]");
        match stmt {
            Statement::Select(s) => match &s.columns[0].expr {
                Expr::ArrayIndex { indices, .. } => {
                    assert_eq!(indices.len(), 1);
                    assert!(matches!(&indices[0], IndexElement::Slice { .. }));
                }
                other => panic!("expected ArrayIndex, got {other:?}"),
            },
            _ => panic!("expected SELECT"),
        }
    }

    // ── Wave 3: USING STYLE and multi-dim indexing ──

    #[test]
    fn using_style_python() {
        let stmt = parse("USING STYLE PYTHON SELECT arr[0]");
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.style, IndexStyle::Python);
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn using_style_glish() {
        let stmt = parse("USING STYLE GLISH SELECT arr[1]");
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.style, IndexStyle::Glish);
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn using_style_case_insensitive() {
        let stmt = parse("using style python SELECT arr[0]");
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.style, IndexStyle::Python);
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn no_using_style_defaults_glish() {
        let stmt = parse("SELECT arr[1]");
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.style, IndexStyle::Glish);
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn multi_dim_index() {
        let stmt = parse("SELECT arr[1, 2]");
        match stmt {
            Statement::Select(s) => match &s.columns[0].expr {
                Expr::ArrayIndex { indices, .. } => {
                    assert_eq!(indices.len(), 2);
                    assert!(matches!(&indices[0], IndexElement::Single(_)));
                    assert!(matches!(&indices[1], IndexElement::Single(_)));
                }
                other => panic!("expected ArrayIndex, got {other:?}"),
            },
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn multi_dim_slice() {
        let stmt = parse("SELECT arr[1:3, 2:4]");
        match stmt {
            Statement::Select(s) => match &s.columns[0].expr {
                Expr::ArrayIndex { indices, .. } => {
                    assert_eq!(indices.len(), 2);
                    assert!(matches!(&indices[0], IndexElement::Slice { .. }));
                    assert!(matches!(&indices[1], IndexElement::Slice { .. }));
                }
                other => panic!("expected ArrayIndex, got {other:?}"),
            },
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn slice_with_step() {
        let stmt = parse("SELECT arr[1:10:2]");
        match stmt {
            Statement::Select(s) => match &s.columns[0].expr {
                Expr::ArrayIndex { indices, .. } => {
                    assert_eq!(indices.len(), 1);
                    match &indices[0] {
                        IndexElement::Slice { start, end, step } => {
                            assert!(start.is_some());
                            assert!(end.is_some());
                            assert!(step.is_some());
                        }
                        _ => panic!("expected Slice"),
                    }
                }
                other => panic!("expected ArrayIndex, got {other:?}"),
            },
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn using_style_python_roundtrip() {
        roundtrip("USING STYLE PYTHON SELECT arr[0]");
    }

    #[test]
    fn gmean_aggregate_in_select() {
        let stmt = parse("SELECT GMEAN(col) FROM t GROUP BY grp");
        match stmt {
            Statement::Select(s) => match &s.columns[0].expr {
                Expr::Aggregate { func, .. } => {
                    assert_eq!(*func, AggregateFunc::Avg);
                }
                other => panic!("expected Aggregate, got {other:?}"),
            },
            _ => panic!("expected SELECT"),
        }
    }

    // ── Wave 8: COUNT SELECT parsing ─────────────────────────────

    #[test]
    fn count_select_basic() {
        let stmt = parse("COUNT SELECT *");
        match stmt {
            Statement::CountSelect(s) => {
                assert!(s.columns.is_empty()); // * = empty
                assert!(s.where_clause.is_none());
            }
            _ => panic!("expected CountSelect"),
        }
    }

    #[test]
    fn count_select_with_where() {
        let stmt = parse("COUNT SELECT * WHERE x > 5");
        match stmt {
            Statement::CountSelect(s) => {
                assert!(s.where_clause.is_some());
            }
            _ => panic!("expected CountSelect"),
        }
    }

    #[test]
    fn count_select_roundtrip() {
        let stmt = parse("COUNT SELECT *");
        let displayed = stmt.to_string();
        assert!(displayed.starts_with("COUNT "));
        let reparsed = parse(&displayed);
        assert!(matches!(reparsed, Statement::CountSelect(_)));
    }
}
