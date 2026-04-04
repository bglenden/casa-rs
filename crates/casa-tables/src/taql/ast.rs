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

/// Index style for array subscripts.
///
/// TaQL supports two indexing conventions:
/// - **Glish** (default): 1-based, end-inclusive ranges.
/// - **Python**: 0-based, end-exclusive ranges.
///
/// Selected via `USING STYLE GLISH` or `USING STYLE PYTHON`.
///
/// C++ reference: `TaQLStyle`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndexStyle {
    /// 1-based indexing with inclusive end (Fortran/Glish convention). This is the default.
    #[default]
    Glish,
    /// 0-based indexing with exclusive end (Python/C convention).
    Python,
}

impl fmt::Display for IndexStyle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Glish => write!(f, "GLISH"),
            Self::Python => write!(f, "PYTHON"),
        }
    }
}

/// A complete TaQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// A `SELECT` query.
    Select(SelectStatement),
    /// An `UPDATE` statement.
    Update(UpdateStatement),
    /// An `INSERT` statement.
    Insert(InsertStatement),
    /// A `DELETE` statement.
    Delete(DeleteStatement),
    /// `CALC expr` — evaluate an expression and return the result.
    Calc(CalcStatement),
    /// `ALTER TABLE ... ADD COLUMN | DROP COLUMN | RENAME COLUMN | ADD ROW`
    AlterTable(AlterTableStatement),
    /// `COUNT SELECT ...` — return the number of rows matching a SELECT.
    ///
    /// C++ reference: `TaQLCountNodeRep`.
    CountSelect(SelectStatement),
    /// `CREATE TABLE name (col type, ...)`
    ///
    /// C++ reference: `TaQLCreTabNodeRep`.
    CreateTable(CreateTableStatement),
    /// `DROP TABLE name`
    ///
    /// C++ reference: `TaQLDropTabNodeRep`.
    DropTable(DropTableStatement),
}

/// A CREATE TABLE statement.
///
/// C++ reference: `TaQLCreTabNodeRep`.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStatement {
    /// Table name or path.
    pub table_name: String,
    /// Column definitions.
    pub columns: Vec<ColumnDef>,
}

/// A column definition in CREATE TABLE.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
    /// Column data type as a string (e.g. "INT32", "FLOAT64", "STRING").
    pub data_type: String,
}

/// A DROP TABLE statement.
///
/// C++ reference: `TaQLDropTabNodeRep`.
#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStatement {
    /// Table name or path.
    pub table_name: String,
}

/// The GIVING clause specifies how to store results.
///
/// C++ reference: `TaQLGivingNodeRep`.
#[derive(Debug, Clone, PartialEq)]
pub struct GivingClause {
    /// Output table name or path.
    pub table_name: String,
    /// Optional output type (e.g., "MEMORY", "PLAIN", "SCRATCH").
    pub output_type: Option<String>,
}

/// A CALC statement.
///
/// ```text
/// CALC expr [FROM table]
/// ```
///
/// Evaluates `expr` in the context of an optional table and returns
/// the result as a scalar value.
///
/// C++ reference: `TaQLCalcNodeRep`.
#[derive(Debug, Clone, PartialEq)]
pub struct CalcStatement {
    /// The expression to evaluate.
    pub expr: Expr,
    /// Optional table context.
    pub from: Option<TableRef>,
}

/// An ALTER TABLE statement.
///
/// ```text
/// ALTER TABLE [table] ADD COLUMN name type
/// ALTER TABLE [table] DROP COLUMN name
/// ALTER TABLE [table] RENAME COLUMN old TO new
/// ALTER TABLE [table] ADD ROW [count]
/// ALTER TABLE [table] SET KEYWORD name = value
/// ```
///
/// C++ reference: `TaQLAlterTabNodeRep`.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableStatement {
    /// The table to alter.
    pub table: Option<TableRef>,
    /// The alteration to perform.
    pub operation: AlterOperation,
}

/// An individual ALTER TABLE operation.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterOperation {
    /// `ADD COLUMN name type`
    AddColumn { name: String, data_type: String },
    /// `DROP COLUMN name`
    DropColumn { name: String },
    /// `RENAME COLUMN old TO new`
    RenameColumn { old_name: String, new_name: String },
    /// `ADD ROW [count]`
    AddRow { count: Option<Expr> },
    /// `SET KEYWORD name = expr`
    SetKeyword { name: String, value: Expr },
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
    /// Index style (`USING STYLE GLISH` or `USING STYLE PYTHON`).
    pub style: IndexStyle,
    /// Optional GIVING clause for output materialization.
    pub giving: Option<GivingClause>,
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
    /// Inner join — rows matching in both tables.
    Inner,
    /// Left outer join — all rows from the left table.
    Left,
    /// Right outer join — all rows from the right table.
    Right,
    /// Cross join — Cartesian product of both tables.
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
    /// `expr =~ regex` or `expr !~ regex` — regex match.
    ///
    /// C++ reference: `TableExprNodeRegex`.
    RegexMatch {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
    },
    /// `expr IN [a, b, c]` or `expr IN [a:b]` — set/range membership with bracket syntax.
    ///
    /// C++ reference: `TaQLMultiNode` with set elements.
    InSet {
        expr: Box<Expr>,
        elements: Vec<InSetElement>,
        negated: bool,
    },
    /// Array indexing: `expr[i]`, `expr[i,j]`, `expr[s:e]`, `expr[s:e:step]`.
    ///
    /// C++ reference: `TableExprNodeArrayPart`.
    ArrayIndex {
        array: Box<Expr>,
        indices: Vec<IndexElement>,
    },
    /// `SELECT *` — all columns wildcard.
    Star,
    /// ROWID pseudo-column (0-based row number).
    RowNumber,
    /// A subquery expression: `(SELECT ...)`.
    ///
    /// C++ reference: `TaQLSubqueryNodeRep`.
    Subquery(Box<SelectStatement>),
}

/// An element in an IN set: either a discrete value or a range.
///
/// C++ reference: `TaQLMultiNode` with `TaQLRangeNodeRep`.
#[derive(Debug, Clone, PartialEq)]
pub enum InSetElement {
    /// A single value.
    Value(Expr),
    /// A range `start:end` or `start:end:step`.
    Range {
        start: Option<Expr>,
        end: Option<Expr>,
        step: Option<Expr>,
    },
}

/// An index element for array subscripts.
#[derive(Debug, Clone, PartialEq)]
pub enum IndexElement {
    /// A single index value.
    Single(Expr),
    /// A slice `start:end` or `start:end:step`.
    Slice {
        start: Option<Expr>,
        end: Option<Expr>,
        step: Option<Expr>,
    },
}

/// A literal value in an expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    /// Integer value.
    Int(i64),
    /// Floating-point value.
    Float(f64),
    /// String value.
    String(String),
    /// Boolean value.
    Bool(bool),
    /// Complex number value.
    Complex(Complex64),
    /// Regex pattern literal: `p/pattern/flags` or `m/pattern/flags`.
    ///
    /// C++ reference: `TaQLRegexNode`.
    Regex { pattern: String, flags: String },
    /// SQL NULL value.
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
    /// Addition `+`.
    Add,
    /// Subtraction `-`.
    Sub,
    /// Multiplication `*`.
    Mul,
    /// Division `/`.
    Div,
    /// Integer division `//`.
    IntDiv,
    /// Modulo `%`.
    Modulo,
    /// Exponentiation `**`.
    Power,
    /// Equality `=`.
    Eq,
    /// Inequality `!=`.
    Ne,
    /// Less than `<`.
    Lt,
    /// Less than or equal `<=`.
    Le,
    /// Greater than `>`.
    Gt,
    /// Greater than or equal `>=`.
    Ge,
    /// Logical AND.
    And,
    /// Logical OR.
    Or,
    /// Bitwise AND (`&`).
    BitAnd,
    /// Bitwise OR (`|`).
    BitOr,
    /// Bitwise XOR (`^`).
    BitXor,
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    /// Arithmetic negation `-`.
    Negate,
    /// Logical NOT.
    Not,
    /// Bitwise NOT `~`.
    BitNot,
}

/// Aggregate function names.
///
/// The `G`-prefixed variants (e.g. `GMin`) are TaQL aliases for the
/// standard SQL aggregates used in GROUP BY queries. Both spellings
/// are accepted by the parser.
///
/// C++ reference: `TableExprGroupFunc*.h`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    /// First non-NULL value in group.
    First,
    /// Last non-NULL value in group.
    Last,
    /// Product of values.
    Product,
    /// Sum of squares.
    SumSqr,
    /// Population variance (N denominator).
    Variance,
    /// Sample variance (N−1 denominator).
    SampleVariance,
    /// Population standard deviation.
    StdDev,
    /// Sample standard deviation.
    SampleStdDev,
    /// Root mean square.
    Rms,
    /// Any TRUE value (boolean OR).
    Any,
    /// All TRUE values (boolean AND).
    All,
    /// Count of TRUE values.
    NTrue,
    /// Count of FALSE values.
    NFalse,
    /// Median value.
    Median,
    /// Fractile (percentile); takes a second argument for the fraction.
    Fractile,
    /// Collect all group values into an array.
    ///
    /// C++ reference: `TableExprGroupAggr`.
    Aggr,
    /// Collect all group row IDs into an array.
    ///
    /// C++ reference: `TableExprGroupRowid`.
    RowId,
    /// Histogram of group values.
    ///
    /// C++ reference: `TableExprGroupHist`.
    Hist,
}

/// Try to parse a string as an aggregate function name.
///
/// Accepts both standard SQL names and TaQL `g`-prefixed aliases
/// (e.g. `"gmin"` → `Some(AggregateFunc::Min)`).
pub fn aggregate_from_name(name: &str) -> Option<AggregateFunc> {
    match name.to_lowercase().as_str() {
        "count" | "gcount" | "countall" => Some(AggregateFunc::Count),
        "sum" | "gsum" => Some(AggregateFunc::Sum),
        "avg" | "mean" | "gmean" | "gavg" => Some(AggregateFunc::Avg),
        "min" | "gmin" => Some(AggregateFunc::Min),
        "max" | "gmax" => Some(AggregateFunc::Max),
        "gfirst" => Some(AggregateFunc::First),
        "glast" => Some(AggregateFunc::Last),
        "gproduct" => Some(AggregateFunc::Product),
        "gsumsqr" | "gsumsquare" => Some(AggregateFunc::SumSqr),
        "gvariance" => Some(AggregateFunc::Variance),
        "gsamplevariance" => Some(AggregateFunc::SampleVariance),
        "gstddev" => Some(AggregateFunc::StdDev),
        "gsamplestddev" => Some(AggregateFunc::SampleStdDev),
        "grms" => Some(AggregateFunc::Rms),
        "gany" => Some(AggregateFunc::Any),
        "gall" => Some(AggregateFunc::All),
        "gntrue" => Some(AggregateFunc::NTrue),
        "gnfalse" => Some(AggregateFunc::NFalse),
        "gmedian" => Some(AggregateFunc::Median),
        "gfractile" => Some(AggregateFunc::Fractile),
        "gaggr" => Some(AggregateFunc::Aggr),
        "growid" => Some(AggregateFunc::RowId),
        "ghist" => Some(AggregateFunc::Hist),
        _ => None,
    }
}

// ── Display implementations for round-trip testing ────────────────────

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Select(s) => write!(f, "{s}"),
            Self::Update(s) => write!(f, "{s}"),
            Self::Insert(s) => write!(f, "{s}"),
            Self::Delete(s) => write!(f, "{s}"),
            Self::Calc(s) => write!(f, "{s}"),
            Self::AlterTable(s) => write!(f, "{s}"),
            Self::CountSelect(s) => write!(f, "COUNT {s}"),
            Self::CreateTable(s) => {
                write!(f, "CREATE TABLE {} (", s.table_name)?;
                for (i, col) in s.columns.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{} {}", col.name, col.data_type)?;
                }
                write!(f, ")")
            }
            Self::DropTable(s) => write!(f, "DROP TABLE {}", s.table_name),
        }
    }
}

impl fmt::Display for CalcStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CALC {}", self.expr)?;
        if let Some(ref from) = self.from {
            write!(f, " FROM {from}")?;
        }
        Ok(())
    }
}

impl fmt::Display for AlterTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ALTER TABLE")?;
        if let Some(ref t) = self.table {
            write!(f, " {t}")?;
        }
        write!(f, " {}", self.operation)
    }
}

impl fmt::Display for AlterOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AddColumn { name, data_type } => {
                write!(f, "ADD COLUMN {name} {data_type}")
            }
            Self::DropColumn { name } => write!(f, "DROP COLUMN {name}"),
            Self::RenameColumn { old_name, new_name } => {
                write!(f, "RENAME COLUMN {old_name} TO {new_name}")
            }
            Self::AddRow { count } => {
                write!(f, "ADD ROW")?;
                if let Some(c) = count {
                    write!(f, " {c}")?;
                }
                Ok(())
            }
            Self::SetKeyword { name, value } => {
                write!(f, "SET KEYWORD {name} = {value}")
            }
        }
    }
}

impl fmt::Display for SelectStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.style != IndexStyle::Glish {
            write!(f, "USING STYLE {} ", self.style)?;
        }
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
        if let Some(ref g) = self.giving {
            write!(f, " GIVING {}", g.table_name)?;
            if let Some(ref ot) = g.output_type {
                write!(f, " AS {ot}")?;
            }
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
            Self::RegexMatch {
                expr,
                pattern,
                negated,
            } => {
                if *negated {
                    write!(f, "({expr} !~ {pattern})")
                } else {
                    write!(f, "({expr} =~ {pattern})")
                }
            }
            Self::InSet {
                expr,
                elements,
                negated,
            } => {
                write!(f, "({expr}")?;
                if *negated {
                    write!(f, " NOT")?;
                }
                write!(f, " IN [")?;
                for (i, elem) in elements.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{elem}")?;
                }
                write!(f, "])")
            }
            Self::ArrayIndex { array, indices } => {
                write!(f, "{array}[")?;
                for (i, idx) in indices.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{idx}")?;
                }
                write!(f, "]")
            }
            Self::Star => write!(f, "*"),
            Self::RowNumber => write!(f, "ROWID()"),
            Self::Subquery(s) => write!(f, "({s})"),
        }
    }
}

impl fmt::Display for InSetElement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(e) => write!(f, "{e}"),
            Self::Range { start, end, step } => {
                if let Some(s) = start {
                    write!(f, "{s}")?;
                }
                write!(f, ":")?;
                if let Some(e) = end {
                    write!(f, "{e}")?;
                }
                if let Some(st) = step {
                    write!(f, ":{st}")?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for IndexElement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Single(e) => write!(f, "{e}"),
            Self::Slice { start, end, step } => {
                if let Some(s) = start {
                    write!(f, "{s}")?;
                }
                write!(f, ":")?;
                if let Some(e) = end {
                    write!(f, "{e}")?;
                }
                if let Some(st) = step {
                    write!(f, ":{st}")?;
                }
                Ok(())
            }
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
            Self::Regex { pattern, flags } => write!(f, "p/{pattern}/{flags}"),
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
            Self::BitAnd => "&",
            Self::BitOr => "|",
            Self::BitXor => "^",
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
            Self::First => write!(f, "GFIRST"),
            Self::Last => write!(f, "GLAST"),
            Self::Product => write!(f, "GPRODUCT"),
            Self::SumSqr => write!(f, "GSUMSQR"),
            Self::Variance => write!(f, "GVARIANCE"),
            Self::SampleVariance => write!(f, "GSAMPLEVARIANCE"),
            Self::StdDev => write!(f, "GSTDDEV"),
            Self::SampleStdDev => write!(f, "GSAMPLESTDDEV"),
            Self::Rms => write!(f, "GRMS"),
            Self::Any => write!(f, "GANY"),
            Self::All => write!(f, "GALL"),
            Self::NTrue => write!(f, "GNTRUE"),
            Self::NFalse => write!(f, "GNFALSE"),
            Self::Median => write!(f, "GMEDIAN"),
            Self::Fractile => write!(f, "GFRACTILE"),
            Self::Aggr => write!(f, "GAGGR"),
            Self::RowId => write!(f, "GROWID"),
            Self::Hist => write!(f, "GHIST"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_from_name_basic() {
        assert_eq!(aggregate_from_name("gcount"), Some(AggregateFunc::Count));
        assert_eq!(aggregate_from_name("GMIN"), Some(AggregateFunc::Min));
        assert_eq!(aggregate_from_name("gmax"), Some(AggregateFunc::Max));
        assert_eq!(aggregate_from_name("gmean"), Some(AggregateFunc::Avg));
        assert_eq!(aggregate_from_name("gfirst"), Some(AggregateFunc::First));
        assert_eq!(aggregate_from_name("glast"), Some(AggregateFunc::Last));
        assert_eq!(
            aggregate_from_name("gproduct"),
            Some(AggregateFunc::Product)
        );
        assert_eq!(aggregate_from_name("gsumsqr"), Some(AggregateFunc::SumSqr));
        assert_eq!(aggregate_from_name("grms"), Some(AggregateFunc::Rms));
        assert_eq!(aggregate_from_name("gany"), Some(AggregateFunc::Any));
        assert_eq!(aggregate_from_name("gall"), Some(AggregateFunc::All));
        assert_eq!(aggregate_from_name("gntrue"), Some(AggregateFunc::NTrue));
        assert_eq!(aggregate_from_name("gnfalse"), Some(AggregateFunc::NFalse));
        assert_eq!(aggregate_from_name("gmedian"), Some(AggregateFunc::Median));
        assert_eq!(
            aggregate_from_name("gfractile"),
            Some(AggregateFunc::Fractile)
        );
    }

    #[test]
    fn aggregate_from_name_aliases() {
        assert_eq!(aggregate_from_name("gavg"), Some(AggregateFunc::Avg));
        assert_eq!(
            aggregate_from_name("gvariance"),
            Some(AggregateFunc::Variance)
        );
        assert_eq!(
            aggregate_from_name("gsamplevariance"),
            Some(AggregateFunc::SampleVariance)
        );
        assert_eq!(aggregate_from_name("gstddev"), Some(AggregateFunc::StdDev));
        assert_eq!(
            aggregate_from_name("gsamplestddev"),
            Some(AggregateFunc::SampleStdDev)
        );
    }

    #[test]
    fn aggregate_from_name_rejects_non_aggregates() {
        assert_eq!(aggregate_from_name("foo"), None);
        assert_eq!(aggregate_from_name("g"), None);
        assert_eq!(aggregate_from_name(""), None);
        assert_eq!(aggregate_from_name("gfoo"), None);
        assert_eq!(aggregate_from_name("gabcdef"), None);
    }

    #[test]
    fn aggregate_from_name_accepts_unprefixed() {
        // The original 5 aggregates are recognized without g-prefix
        assert_eq!(aggregate_from_name("count"), Some(AggregateFunc::Count));
        assert_eq!(aggregate_from_name("sum"), Some(AggregateFunc::Sum));
        assert_eq!(aggregate_from_name("avg"), Some(AggregateFunc::Avg));
        assert_eq!(aggregate_from_name("min"), Some(AggregateFunc::Min));
        assert_eq!(aggregate_from_name("max"), Some(AggregateFunc::Max));
    }

    #[test]
    fn aggregate_display_roundtrip() {
        for func in [
            AggregateFunc::Count,
            AggregateFunc::Sum,
            AggregateFunc::Avg,
            AggregateFunc::Min,
            AggregateFunc::Max,
            AggregateFunc::First,
            AggregateFunc::Last,
            AggregateFunc::Product,
            AggregateFunc::SumSqr,
            AggregateFunc::Variance,
            AggregateFunc::SampleVariance,
            AggregateFunc::StdDev,
            AggregateFunc::SampleStdDev,
            AggregateFunc::Rms,
            AggregateFunc::Any,
            AggregateFunc::All,
            AggregateFunc::NTrue,
            AggregateFunc::NFalse,
            AggregateFunc::Median,
            AggregateFunc::Fractile,
            AggregateFunc::Aggr,
            AggregateFunc::RowId,
            AggregateFunc::Hist,
        ] {
            let displayed = func.to_string();
            let parsed = aggregate_from_name(&displayed);
            assert_eq!(parsed, Some(func), "roundtrip failed for {displayed}");
        }
    }
}
