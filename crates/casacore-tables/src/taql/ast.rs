// SPDX-License-Identifier: LGPL-3.0-or-later
//! Abstract syntax tree types for TaQL.
//!
//! The AST is produced by the parser and consumed by the
//! expression evaluator and execution engine. All types implement
//! [`std::fmt::Display`] for round-trip testing (parse → Display → parse → assert equal).
//!
//! # C++ reference
//!
//! `TaQLNodeDer.h` — `TaQLSelectNodeRep`, `TaQLUpdateNodeRep`,
//! `TaQLInsertNodeRep`, `TaQLDeleteNodeRep`.

use std::fmt;

use num_complex::Complex64;

/// A complete TaQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    Update(UpdateStatement),
    Insert(InsertStatement),
    Delete(DeleteStatement),
}

/// A SELECT statement.
///
/// ```text
/// SELECT [DISTINCT] columns
///   [FROM table]
///   [JOIN ...]
///   [WHERE expr]
///   [GROUP BY exprs [HAVING expr]]
///   [ORDER BY specs]
///   [LIMIT n] [OFFSET n]
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    /// Column expressions to project (empty = `SELECT *`).
    pub columns: Vec<SelectColumn>,
    /// Optional FROM table reference.
    pub from: Option<TableRef>,
    /// Optional JOIN clauses.
    pub joins: Vec<JoinClause>,
    /// Optional WHERE filter.
    pub where_clause: Option<Expr>,
    /// GROUP BY expressions.
    pub group_by: Vec<Expr>,
    /// Optional HAVING filter (only with GROUP BY).
    pub having: Option<Expr>,
    /// ORDER BY specifications.
    pub order_by: Vec<OrderBySpec>,
    /// Optional LIMIT.
    pub limit: Option<Expr>,
    /// Optional OFFSET.
    pub offset: Option<Expr>,
    /// Whether DISTINCT was specified.
    pub distinct: bool,
}

/// A column in a SELECT list.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectColumn {
    /// The expression for this column.
    pub expr: Expr,
    /// Optional alias (`AS name`).
    pub alias: Option<String>,
}

/// A table reference (name or path).
#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    /// Table name or path.
    pub name: String,
    /// Optional alias.
    pub alias: Option<String>,
}

/// An ORDER BY specification.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderBySpec {
    /// The expression to sort by.
    pub expr: Expr,
    /// Sort direction (true = ascending).
    pub ascending: bool,
}

/// An UPDATE statement.
///
/// ```text
/// UPDATE [table] SET col = expr [, ...] [WHERE expr] [LIMIT n]
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    /// Optional table reference.
    pub table: Option<TableRef>,
    /// Column assignments.
    pub assignments: Vec<Assignment>,
    /// Optional WHERE filter.
    pub where_clause: Option<Expr>,
    /// Optional LIMIT.
    pub limit: Option<Expr>,
}

/// A SET assignment in an UPDATE.
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    /// Column name.
    pub column: String,
    /// Value expression.
    pub value: Expr,
}

/// An INSERT statement.
///
/// ```text
/// INSERT INTO [table] [(col, ...)] VALUES (expr, ...) [, ...]
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    /// Optional table reference.
    pub table: Option<TableRef>,
    /// Optional column list.
    pub columns: Vec<String>,
    /// Rows of value expressions.
    pub values: Vec<Vec<Expr>>,
}

/// A DELETE statement.
///
/// ```text
/// DELETE FROM [table] [WHERE expr] [LIMIT n]
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    /// Optional table reference.
    pub table: Option<TableRef>,
    /// Optional WHERE filter.
    pub where_clause: Option<Expr>,
    /// Optional LIMIT.
    pub limit: Option<Expr>,
}

/// A JOIN clause.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    /// The type of join.
    pub join_type: JoinType,
    /// The table to join with.
    pub table: TableRef,
    /// The ON condition (None for CROSS JOIN).
    pub on: Option<Expr>,
}

/// Type of a JOIN operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Cross,
}

/// An expression node.
///
/// Expressions appear in SELECT lists, WHERE/HAVING filters, ORDER BY,
/// assignment values, and function arguments.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A literal value.
    Literal(Literal),
    /// A column reference, possibly qualified (`table.column`).
    ColumnRef(ColumnRef),
    /// A unary operation.
    Unary { op: UnaryOp, operand: Box<Expr> },
    /// A binary operation.
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    /// A function call.
    FunctionCall { name: String, args: Vec<Expr> },
    /// `expr BETWEEN low AND high`
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    /// `expr [NOT] IN (val, ...)`
    In {
        expr: Box<Expr>,
        values: Vec<Expr>,
        negated: bool,
    },
    /// `expr [NOT] LIKE pattern` or `expr [NOT] ILIKE pattern`
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
        case_insensitive: bool,
    },
    /// `expr IS [NOT] NULL`
    IsNull { expr: Box<Expr>, negated: bool },
    /// An aggregate function: COUNT, SUM, AVG, MIN, MAX.
    Aggregate { func: AggregateFunc, arg: Box<Expr> },
    /// `SELECT *` — all columns wildcard.
    Star,
    /// ROWID pseudo-column (0-based row number).
    RowNumber,
}

/// A literal value in an expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Complex(Complex64),
    Null,
}

/// A column reference, possibly qualified with a table alias.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRef {
    /// Optional table alias or name.
    pub table: Option<String>,
    /// Column name.
    pub column: String,
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    IntDiv,
    Modulo,
    Power,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Negate,
    Not,
    BitNot,
}

/// Aggregate function names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

// ── Display implementations for round-trip testing ────────────────────

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Select(s) => write!(f, "{s}"),
            Self::Update(s) => write!(f, "{s}"),
            Self::Insert(s) => write!(f, "{s}"),
            Self::Delete(s) => write!(f, "{s}"),
        }
    }
}

impl fmt::Display for SelectStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SELECT ")?;
        if self.distinct {
            write!(f, "DISTINCT ")?;
        }
        if self.columns.is_empty() {
            write!(f, "*")?;
        } else {
            for (i, col) in self.columns.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{col}")?;
            }
        }
        if let Some(ref from) = self.from {
            write!(f, " FROM {from}")?;
        }
        for join in &self.joins {
            write!(f, " {join}")?;
        }
        if let Some(ref w) = self.where_clause {
            write!(f, " WHERE {w}")?;
        }
        if !self.group_by.is_empty() {
            write!(f, " GROUP BY ")?;
            for (i, g) in self.group_by.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{g}")?;
            }
        }
        if let Some(ref h) = self.having {
            write!(f, " HAVING {h}")?;
        }
        if !self.order_by.is_empty() {
            write!(f, " ORDER BY ")?;
            for (i, o) in self.order_by.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{o}")?;
            }
        }
        if let Some(ref l) = self.limit {
            write!(f, " LIMIT {l}")?;
        }
        if let Some(ref o) = self.offset {
            write!(f, " OFFSET {o}")?;
        }
        Ok(())
    }
}

impl fmt::Display for SelectColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        if let Some(ref alias) = self.alias {
            write!(f, " AS {alias}")?;
        }
        Ok(())
    }
}

impl fmt::Display for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if let Some(ref alias) = self.alias {
            write!(f, " AS {alias}")?;
        }
        Ok(())
    }
}

impl fmt::Display for OrderBySpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        if self.ascending {
            write!(f, " ASC")
        } else {
            write!(f, " DESC")
        }
    }
}

impl fmt::Display for UpdateStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UPDATE")?;
        if let Some(ref t) = self.table {
            write!(f, " {t}")?;
        }
        write!(f, " SET ")?;
        for (i, a) in self.assignments.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{a}")?;
        }
        if let Some(ref w) = self.where_clause {
            write!(f, " WHERE {w}")?;
        }
        if let Some(ref l) = self.limit {
            write!(f, " LIMIT {l}")?;
        }
        Ok(())
    }
}

impl fmt::Display for Assignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", self.column, self.value)
    }
}

impl fmt::Display for InsertStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "INSERT INTO")?;
        if let Some(ref t) = self.table {
            write!(f, " {t}")?;
        }
        if !self.columns.is_empty() {
            write!(f, " (")?;
            for (i, c) in self.columns.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{c}")?;
            }
            write!(f, ")")?;
        }
        write!(f, " VALUES ")?;
        for (i, row) in self.values.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "(")?;
            for (j, v) in row.iter().enumerate() {
                if j > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{v}")?;
            }
            write!(f, ")")?;
        }
        Ok(())
    }
}

impl fmt::Display for DeleteStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DELETE FROM")?;
        if let Some(ref t) = self.table {
            write!(f, " {t}")?;
        }
        if let Some(ref w) = self.where_clause {
            write!(f, " WHERE {w}")?;
        }
        if let Some(ref l) = self.limit {
            write!(f, " LIMIT {l}")?;
        }
        Ok(())
    }
}

impl fmt::Display for JoinClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} JOIN {}", self.join_type, self.table)?;
        if let Some(ref on) = self.on {
            write!(f, " ON {on}")?;
        }
        Ok(())
    }
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inner => write!(f, "INNER"),
            Self::Left => write!(f, "LEFT"),
            Self::Right => write!(f, "RIGHT"),
            Self::Cross => write!(f, "CROSS"),
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Literal(l) => write!(f, "{l}"),
            Self::ColumnRef(c) => write!(f, "{c}"),
            Self::Unary { op, operand } => write!(f, "({op}{operand})"),
            Self::Binary { left, op, right } => write!(f, "({left} {op} {right})"),
            Self::FunctionCall { name, args } => {
                write!(f, "{name}(")?;
                for (i, a) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{a}")?;
                }
                write!(f, ")")
            }
            Self::Between {
                expr,
                low,
                high,
                negated,
            } => {
                write!(f, "({expr}")?;
                if *negated {
                    write!(f, " NOT")?;
                }
                write!(f, " BETWEEN {low} AND {high})")
            }
            Self::In {
                expr,
                values,
                negated,
            } => {
                write!(f, "({expr}")?;
                if *negated {
                    write!(f, " NOT")?;
                }
                write!(f, " IN (")?;
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{v}")?;
                }
                write!(f, "))")
            }
            Self::Like {
                expr,
                pattern,
                negated,
                case_insensitive,
            } => {
                write!(f, "({expr}")?;
                if *negated {
                    write!(f, " NOT")?;
                }
                if *case_insensitive {
                    write!(f, " ILIKE {pattern})")
                } else {
                    write!(f, " LIKE {pattern})")
                }
            }
            Self::IsNull { expr, negated } => {
                write!(f, "({expr} IS")?;
                if *negated {
                    write!(f, " NOT")?;
                }
                write!(f, " NULL)")
            }
            Self::Aggregate { func, arg } => write!(f, "{func}({arg})"),
            Self::Star => write!(f, "*"),
            Self::RowNumber => write!(f, "ROWID()"),
        }
    }
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int(n) => write!(f, "{n}"),
            Self::Float(v) => {
                if v.fract() == 0.0 && v.is_finite() {
                    write!(f, "{v:.1}")
                } else {
                    write!(f, "{v}")
                }
            }
            Self::String(s) => write!(f, "'{s}'"),
            Self::Bool(b) => {
                if *b {
                    write!(f, "TRUE")
                } else {
                    write!(f, "FALSE")
                }
            }
            Self::Complex(c) => write!(f, "({} + {}i)", c.re, c.im),
            Self::Null => write!(f, "NULL"),
        }
    }
}

impl fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref table) = self.table {
            write!(f, "{table}.{}", self.column)
        } else {
            write!(f, "{}", self.column)
        }
    }
}

impl fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::IntDiv => "//",
            Self::Modulo => "%",
            Self::Power => "**",
            Self::Eq => "=",
            Self::Ne => "!=",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Gt => ">",
            Self::Ge => ">=",
            Self::And => "AND",
            Self::Or => "OR",
        };
        f.write_str(s)
    }
}

impl fmt::Display for UnaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Negate => write!(f, "-"),
            Self::Not => write!(f, "NOT "),
            Self::BitNot => write!(f, "~"),
        }
    }
}

impl fmt::Display for AggregateFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Count => write!(f, "COUNT"),
            Self::Sum => write!(f, "SUM"),
            Self::Avg => write!(f, "AVG"),
            Self::Min => write!(f, "MIN"),
            Self::Max => write!(f, "MAX"),
        }
    }
}
