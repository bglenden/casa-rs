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
//! | 14/15      | `*` `/` `//` `%`           | left  |
//! | 17/16      | `**` (power)          | right |
//! | 19         | unary `-` `~`         | right |
//!
//! # C++ reference
//!
//! `TaQLNodeDer.cc` — `TaQLNode::parse()`, `TaQLMultiNode`, etc.

use casa_core::quanta::Unit;

use super::ast::*;
use super::error::TaqlError;
use super::lexer::Lexer;
use super::token::Token;

mod expressions;

/// TaQL parser — recursive descent with Pratt expression parsing.
pub struct Parser<'src> {
    lexer: Lexer<'src>,
}

enum NumericSuffix {
    Unit(String),
    Imaginary,
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
                self.lexer.next_token(); // consume COUNT
                if self.lexer.peek() == Some(&Token::Select) {
                    let mut s = self.parse_select()?;
                    s.style = style;
                    Statement::CountSelect(s)
                } else {
                    Statement::Count(self.parse_count(style)?)
                }
            }
            Some(Token::Update) => {
                let mut u = self.parse_update()?;
                u.style = style;
                Statement::Update(u)
            }
            Some(Token::Insert) => {
                let mut ins = self.parse_insert()?;
                ins.style = style;
                Statement::Insert(ins)
            }
            Some(Token::Delete) => {
                let mut del = self.parse_delete()?;
                del.style = style;
                Statement::Delete(del)
            }
            Some(Token::Calc) => {
                let mut c = self.parse_calc()?;
                c.style = style;
                Statement::Calc(c)
            }
            Some(Token::Create) => Statement::CreateTable(self.parse_create_table()?),
            Some(Token::Drop) => {
                // DROP TABLE or DROP COLUMN (but DROP COLUMN is ALTER TABLE syntax)
                self.lexer.next_token(); // consume DROP
                self.lexer.expect(&Token::Table)?;
                let table_name = self.parse_table_name()?;
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
        if !self.lexer.is_eof()
            && let Some((tok, span)) = self.lexer.next_token()
        {
            return Err(TaqlError::parse(
                self.lexer.position(span.start),
                format!("unexpected token after statement: {tok}"),
            ));
        }
        Ok(stmt)
    }
    /// Parse optional `USING [STYLE] ...` / `USINGSTYLE ...` prefix.
    fn parse_using_style(&mut self) -> Result<IndexStyle, TaqlError> {
        let is_usingstyle = self.lexer.peek_with_span().is_some_and(|(tok, span)| {
            tok == Token::Ident && self.lexer.slice(&span).eq_ignore_ascii_case("USINGSTYLE")
        });
        if is_usingstyle {
            self.lexer.next_token();
        } else if self.lexer.eat_if(&Token::Using).is_some() {
            self.lexer.expect(&Token::Style)?;
        } else {
            return Ok(IndexStyle::Glish);
        }

        let (mut one_based, mut end_inclusive, mut fortran_order) = (true, true, true);
        loop {
            let pos = self
                .lexer
                .peek_with_span()
                .map_or(0, |(_, span)| span.start);
            let flag = self.parse_ident_string()?;
            match flag.to_uppercase().as_str() {
                "GLISH" => (one_based, end_inclusive, fortran_order) = (true, true, true),
                "PYTHON" => (one_based, end_inclusive, fortran_order) = (false, false, false),
                "BASE0" => one_based = false,
                "BASE1" => one_based = true,
                "ENDINCL" => end_inclusive = true,
                "ENDEXCL" => end_inclusive = false,
                "FORTRANORDER" | "FORTRAN" => fortran_order = true,
                "CORDER" | "C" => fortran_order = false,
                _ => {
                    return Err(TaqlError::parse(
                        self.lexer.position(pos),
                        format!("{flag} is an invalid TaQL STYLE value"),
                    ));
                }
            }
            if self.lexer.eat_if(&Token::Comma).is_none() {
                break;
            }
        }
        Ok(IndexStyle::from_flags(
            one_based,
            end_inclusive,
            fortran_order,
        ))
    }

    fn parse_count(&mut self, style: IndexStyle) -> Result<CountStatement, TaqlError> {
        let columns = if self.lexer.eat_if(&Token::LParen).is_some() {
            let cols = if self.lexer.peek() == Some(&Token::RParen) {
                vec![]
            } else {
                self.parse_expr_list()?
            };
            self.lexer.expect(&Token::RParen)?;
            cols
        } else if self.lexer.peek() == Some(&Token::From) || self.lexer.is_eof() {
            vec![]
        } else {
            self.parse_expr_list()?
        };
        if columns.is_empty() {
            return Err(TaqlError::TypeError {
                message: "No COUNT columns given".to_string(),
            });
        }
        let from = if self.lexer.eat_if(&Token::From).is_some() {
            Some(self.parse_table_ref()?)
        } else {
            None
        };
        let where_clause = if self.lexer.eat_if(&Token::Where).is_some() {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(CountStatement {
            columns,
            from,
            where_clause,
            style,
        })
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

        let group_by = if let Some(span) = self.lexer.eat_if(&Token::Group) {
            if self.lexer.slice(&span).eq_ignore_ascii_case("GROUP") {
                self.lexer.expect(&Token::By)?;
            }
            self.parse_expr_list()?
        } else {
            vec![]
        };

        let having = if self.lexer.eat_if(&Token::Having).is_some() {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let (order_by, order_distinct) = if let Some(span) = self.lexer.eat_if(&Token::Order) {
            if self.lexer.slice(&span).eq_ignore_ascii_case("ORDER") {
                self.lexer.expect(&Token::By)?;
            }
            self.parse_order_by_list()?
        } else {
            (vec![], false)
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

        let mut giving = None;
        let mut giving_expr = None;
        if self.lexer.eat_if(&Token::Giving).is_some() {
            if self.lexer.eat_if(&Token::LBracket).is_some() {
                let expr = self.parse_expr()?;
                self.lexer.expect(&Token::RBracket)?;
                giving_expr = Some(expr);
            } else {
                giving = Some(self.parse_giving_destination()?);
            }
        }

        Ok(SelectStatement {
            columns,
            from,
            joins,
            where_clause,
            group_by,
            having,
            order_by,
            order_distinct,
            limit,
            offset,
            distinct,
            style: IndexStyle::default(),
            giving,
            giving_expr,
        })
    }

    fn parse_giving_destination(&mut self) -> Result<GivingClause, TaqlError> {
        let table_name = if self.lexer.peek() == Some(&Token::As) {
            String::new()
        } else {
            self.parse_table_name()?
        };
        let mut clause = GivingClause {
            table_name,
            output_type: None,
            output_options: None,
        };
        if self.lexer.eat_if(&Token::As).is_none() {
            return Ok(clause);
        }
        if self.lexer.eat_if(&Token::LBracket).is_none() {
            clause.output_type = Some(self.parse_ident_string()?);
            return Ok(clause);
        }
        let mut fields = Vec::new();
        loop {
            let name = self.parse_ident_string()?;
            self.lexer.expect(&Token::Eq)?;
            let value = self.parse_expr()?;
            let data_type = self
                .lexer
                .eat_if(&Token::As)
                .map(|_| self.parse_ident_string())
                .transpose()?;
            fields.push(GivingOption {
                name,
                value,
                data_type,
            });
            if self.lexer.eat_if(&Token::Comma).is_none() {
                break;
            }
        }
        self.lexer.expect(&Token::RBracket)?;
        clause.output_options = Some(fields);
        Ok(clause)
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
        if self.lexer.is_eof()
            || self.peek_is_keyword_clause()
            || matches!(self.lexer.peek(), Some(Token::RParen | Token::RBracket))
        {
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
        let name = self.parse_table_name()?;
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

    fn parse_order_by_list(&mut self) -> Result<(Vec<OrderBySpec>, bool), TaqlError> {
        let mut distinct = self.lexer.eat_if(&Token::Distinct).is_some();
        if !distinct && let Some((Token::Ident, span)) = self.lexer.peek_with_span() {
            let word = self.lexer.slice(&span);
            if word.eq_ignore_ascii_case("NODUPLICATES") || word.eq_ignore_ascii_case("UNIQUE") {
                self.lexer.next_token();
                distinct = true;
            }
        }
        let default_asc = if self.lexer.eat_if(&Token::Desc).is_some() {
            false
        } else {
            self.lexer.eat_if(&Token::Asc);
            true
        };
        let mut specs = vec![self.parse_order_by_spec(default_asc)?];
        while self.lexer.eat_if(&Token::Comma).is_some() {
            specs.push(self.parse_order_by_spec(default_asc)?);
        }
        Ok((specs, distinct))
    }

    fn parse_order_by_spec(&mut self, default_asc: bool) -> Result<OrderBySpec, TaqlError> {
        let expr = self.parse_expr()?;
        let ascending = if self.lexer.eat_if(&Token::Desc).is_some() {
            false
        } else if self.lexer.eat_if(&Token::Asc).is_some() {
            true
        } else {
            default_asc
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
            style: IndexStyle::default(),
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
        let table = if self.peek_is_table_name() && self.lexer.peek() != Some(&Token::Values) {
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
            style: IndexStyle::default(),
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

        let table = if self.peek_is_table_name()
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
            style: IndexStyle::default(),
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
        Ok(CalcStatement {
            expr,
            from,
            style: IndexStyle::default(),
        })
    }

    // ── ALTER TABLE statement ─────────────────────────────────────

    fn parse_create_table(&mut self) -> Result<CreateTableStatement, TaqlError> {
        self.lexer.expect(&Token::Create)?;
        self.lexer.expect(&Token::Table)?;
        let table_name = self.parse_table_name()?;
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
        let table = if self.peek_is_table_name()
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

    // ── Helpers ────────────────────────────────────────────────────

    /// Parse a table name or path: quoted string or contiguous path tokens.
    fn parse_table_name(&mut self) -> Result<String, TaqlError> {
        if let Some(Token::StringLiteral) = self.lexer.peek() {
            let (_, span) = self.lexer.next_token().unwrap();
            let s = self.lexer.slice(&span);
            return Ok(s[1..s.len() - 1].to_string());
        }
        let mut out = String::new();
        let mut prev_end = match self.lexer.peek_with_span() {
            Some((ref tok, ref span))
                if matches!(tok, Token::Ident | Token::Slash | Token::Dot | Token::Colon)
                    || tok.is_keyword() =>
            {
                let end = span.end;
                let text = self.lexer.slice(span).to_string();
                self.lexer.next_token();
                out.push_str(&text);
                end
            }
            Some((tok, span)) => {
                let msg = format!("expected table name, found {tok}");
                let pos = self.lexer.position(span.start);
                self.lexer.next_token();
                return Err(TaqlError::parse(pos, msg));
            }
            None => return Err(TaqlError::unexpected_end("expected table name")),
        };
        while let Some((ref tok, ref span)) = self.lexer.peek_with_span() {
            if span.start != prev_end {
                break;
            }
            let is_path_part = matches!(
                tok,
                Token::Ident
                    | Token::IntLiteral
                    | Token::FloatLiteral
                    | Token::Slash
                    | Token::Dot
                    | Token::Minus
                    | Token::Colon
            ) || tok.is_keyword();
            if !is_path_part {
                break;
            }
            prev_end = span.end;
            let text = self.lexer.slice(span).to_string();
            self.lexer.next_token();
            out.push_str(&text);
        }
        Ok(out)
    }

    fn peek_is_table_name(&mut self) -> bool {
        match self.lexer.peek() {
            Some(
                Token::Ident | Token::StringLiteral | Token::Slash | Token::Dot | Token::Colon,
            ) => true,
            Some(tok) => tok.is_keyword(),
            None => false,
        }
    }

    fn peek_starts_subquery(&mut self) -> bool {
        matches!(self.lexer.peek(), Some(Token::Select | Token::Using))
            || self.lexer.peek_with_span().is_some_and(|(tok, span)| {
                tok == Token::Ident && self.lexer.slice(&span).eq_ignore_ascii_case("USINGSTYLE")
            })
    }

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
                Token::From
                    | Token::Where
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
                    | Token::Giving
                    | Token::Into
                    | Token::Values
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
        Token::Star | Token::Slash | Token::DoubleSlash | Token::Percent => Some((14, 15)),
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
        Token::DoubleSlash => BinaryOp::IntDiv,
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
mod tests;
