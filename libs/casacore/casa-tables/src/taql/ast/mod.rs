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
use std::sync::Arc;

use casa_core::quanta::Unit;
use num_complex::Complex64;

use super::eval::ExprValue;

/// Index and axis style for array subscripts and `ROWNUMBER()`.
///
/// TaQL supports fine-grained style flags via `USING STYLE`:
/// - **Glish** (default): 1-based indexing, inclusive slice end, Fortran axis order.
/// - **Python**: 0-based indexing, exclusive slice end, C axis order.
/// - **Custom**: arbitrary combinations such as `base0, endincl, fortranorder`.
///
/// C++ reference: `TaQLStyle`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndexStyle {
    /// 1-based indexing with inclusive end and Fortran axis order (default).
    #[default]
    Glish,
    /// 0-based indexing with exclusive end and C axis order.
    Python,
    /// Explicit combination of base, slice end inclusivity, and axis order.
    Custom {
        /// Whether index expressions and row numbering start at one.
        one_based: bool,
        /// Whether a slice's stop coordinate is included.
        end_inclusive: bool,
        /// Whether subscripts follow first-axis-fastest Fortran order.
        fortran_order: bool,
    },
}

impl IndexStyle {
    /// Builds an [`IndexStyle`] from explicit flags, collapsing to [`Self::Glish`]
    /// or [`Self::Python`] when the tuple matches a standard convention.
    #[must_use]
    pub const fn from_flags(one_based: bool, end_inclusive: bool, fortran_order: bool) -> Self {
        match (one_based, end_inclusive, fortran_order) {
            (true, true, true) => Self::Glish,
            (false, false, false) => Self::Python,
            _ => Self::Custom {
                one_based,
                end_inclusive,
                fortran_order,
            },
        }
    }

    /// Returns `true` when indices and `ROWNUMBER()` are 1-based (`base1` / `glish`).
    #[must_use]
    pub const fn is_one_based(self) -> bool {
        match self {
            Self::Glish => true,
            Self::Python => false,
            Self::Custom { one_based, .. } => one_based,
        }
    }

    /// Returns `true` when slice upper bounds are inclusive (`endincl` / `glish`).
    #[must_use]
    pub const fn is_end_inclusive(self) -> bool {
        match self {
            Self::Glish => true,
            Self::Python => false,
            Self::Custom { end_inclusive, .. } => end_inclusive,
        }
    }

    /// Returns `true` when array indices are ordered Fortran-first (`fortranorder`).
    #[must_use]
    pub const fn is_fortran_order(self) -> bool {
        match self {
            Self::Glish => true,
            Self::Python => false,
            Self::Custom { fortran_order, .. } => fortran_order,
        }
    }
}

impl fmt::Display for IndexStyle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Glish => write!(f, "GLISH"),
            Self::Python => write!(f, "PYTHON"),
            Self::Custom {
                one_based,
                end_inclusive,
                fortran_order,
            } => write!(
                f,
                "{}, {}, {}",
                if *one_based { "BASE1" } else { "BASE0" },
                if *end_inclusive { "ENDINCL" } else { "ENDEXCL" },
                if *fortran_order {
                    "FORTRANORDER"
                } else {
                    "CORDER"
                }
            ),
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
    /// `COUNT [(cols)] FROM ... [WHERE ...]` — return grouped row counts.
    Count(CountStatement),
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
    /// Ordered fields from the optional `AS [name=value, ...]` record.
    pub output_options: Option<Vec<GivingOption>>,
}

/// A constant field of a TaQL output options record.
#[derive(Debug, Clone, PartialEq)]
pub struct GivingOption {
    /// Original field name.
    pub name: String,
    /// Expression to evaluate before the output table is created.
    pub value: Expr,
    /// Optional casacore type code following `AS`.
    pub data_type: Option<String>,
}

/// A `COUNT [(cols)] FROM table [WHERE expr]` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CountStatement {
    /// Columns/expressions to group and count.
    pub columns: Vec<Expr>,
    /// Optional table reference.
    pub from: Option<TableRef>,
    /// Optional WHERE filter.
    pub where_clause: Option<Expr>,
    /// Index style (`USING STYLE ...`).
    pub style: IndexStyle,
}

/// A CALC statement.
///
/// ```text
/// CALC expr [FROM table]
/// ```
///
/// Evaluates `expr` in the context of an optional table and returns
/// the values.
///
/// C++ reference: `TaQLCalcNodeRep`.
#[derive(Debug, Clone, PartialEq)]
pub struct CalcStatement {
    /// The expression to evaluate.
    pub expr: Expr,
    /// Optional table context.
    pub from: Option<TableRef>,
    /// Index style (`USING STYLE ...`).
    pub style: IndexStyle,
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
    AddColumn {
        /// New column name.
        name: String,
        /// TaQL type name as written in the statement.
        data_type: String,
    },
    /// `DROP COLUMN name`
    DropColumn {
        /// Name of the existing column to remove.
        name: String,
    },
    /// `RENAME COLUMN old TO new`
    RenameColumn {
        /// Existing column name.
        old_name: String,
        /// Replacement column name.
        new_name: String,
    },
    /// `ADD ROW [count]`
    AddRow {
        /// Expression yielding the row count, with one implied when omitted.
        count: Option<Expr>,
    },
    /// `SET KEYWORD name = expr`
    SetKeyword {
        /// Name of the table keyword.
        name: String,
        /// Expression furnishing the new keyword value.
        value: Expr,
    },
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
    /// Whether `ORDERBY NODUPLICATES|UNIQUE|DISTINCT` was requested.
    pub order_distinct: bool,
    /// Optional LIMIT.
    pub limit: Option<Expr>,
    /// Optional OFFSET.
    pub offset: Option<Expr>,
    /// Whether DISTINCT was specified.
    pub distinct: bool,
    /// Index style (`USING STYLE ...`).
    pub style: IndexStyle,
    /// Optional GIVING clause for output materialization.
    pub giving: Option<GivingClause>,
    /// Optional `GIVING [expr]` subquery projection.
    pub giving_expr: Option<Expr>,
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
    /// Index style (`USING STYLE ...`).
    pub style: IndexStyle,
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
    /// Index style (`USING STYLE ...`).
    pub style: IndexStyle,
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
    /// Index style (`USING STYLE ...`).
    pub style: IndexStyle,
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
    /// A constant value prepared from a table-specific expression.
    PreparedConstant(Arc<ExprValue>),
    /// A column reference, possibly qualified (`table.column`).
    ColumnRef(ColumnRef),
    /// A unary operation.
    Unary {
        /// Prefix operator.
        op: UnaryOp,
        /// Expression to which the operator applies.
        operand: Box<Expr>,
    },
    /// A binary operation.
    Binary {
        /// First operand.
        left: Box<Expr>,
        /// Infix operator.
        op: BinaryOp,
        /// Second operand.
        right: Box<Expr>,
    },
    /// A function call.
    FunctionCall {
        /// TaQL function identifier.
        name: String,
        /// Arguments in call order.
        args: Vec<Expr>,
    },
    /// A zero-based row lookup bound to a metadata column when preparing a query.
    BoundRowLookup {
        /// Original expression text retained for display.
        original: Arc<String>,
        /// Expression yielding a zero-based metadata row index.
        index: Box<Expr>,
        /// Prepared metadata values in row order, shared across query rows.
        values: Arc<[ExprValue]>,
        /// Value returned for a missing lookup when supplied.
        missing: Option<ExprValue>,
    },
    /// A row computation with metadata prepared for this query.
    BoundRowFunction(super::BoundRowFunction),
    /// A vector expression such as `[ANTENNA1, ANTENNA2]`.
    ArrayLiteral(Vec<Expr>),
    /// `expr BETWEEN low AND high`
    Between {
        /// Value tested against the bounds.
        expr: Box<Expr>,
        /// Lower bound.
        low: Box<Expr>,
        /// Upper bound.
        high: Box<Expr>,
        /// Whether to invert the range test.
        negated: bool,
    },
    /// `expr [NOT] IN (val, ...)`
    In {
        /// Value whose membership is tested.
        expr: Box<Expr>,
        /// Candidate expressions tested for equality.
        values: Vec<Expr>,
        /// Whether to invert the membership result.
        negated: bool,
    },
    /// `expr [NOT] LIKE pattern` or `expr [NOT] ILIKE pattern`
    Like {
        /// Text value to test.
        expr: Box<Expr>,
        /// SQL wildcard pattern expression.
        pattern: Box<Expr>,
        /// Whether to invert the pattern test.
        negated: bool,
        /// Whether to ignore case when matching.
        case_insensitive: bool,
    },
    /// `expr IS [NOT] NULL`
    IsNull {
        /// Value to test for null.
        expr: Box<Expr>,
        /// Whether this is an IS NOT NULL test.
        negated: bool,
    },
    /// An aggregate function: COUNT, SUM, AVG, MIN, MAX.
    Aggregate {
        /// Requested group reduction.
        func: AggregateFunc,
        /// Expression evaluated over group rows.
        arg: Box<Expr>,
    },
    /// `expr =~ regex` or `expr !~ regex` — regex match.
    ///
    /// C++ reference: `TableExprNodeRegex`.
    RegexMatch {
        /// Text value to test.
        expr: Box<Expr>,
        /// Pattern expression, including any regular-expression flags.
        pattern: Box<Expr>,
        /// Whether this is a negative match.
        negated: bool,
    },
    /// `expr IN [a, b, c]` or `expr IN [a:b]` — set/range membership with bracket syntax.
    ///
    /// C++ reference: `TaQLMultiNode` with set elements.
    InSet {
        /// Value whose membership is tested.
        expr: Box<Expr>,
        /// Discrete expressions and ranges in the set.
        elements: Vec<InSetElement>,
        /// Whether to invert the membership result.
        negated: bool,
    },
    /// Array indexing: `expr[i]`, `expr[i,j]`, `expr[s:e]`, `expr[s:e:step]`.
    ///
    /// C++ reference: `TableExprNodeArrayPart`.
    ArrayIndex {
        /// Expression producing the indexed array.
        array: Box<Expr>,
        /// Subscripts or slices in the active indexing style's axis order.
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
        /// Optional lower endpoint of the range.
        start: Option<Expr>,
        /// Optional upper endpoint of the range.
        end: Option<Expr>,
        /// Optional spacing of values in a discrete range.
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
        /// Optional initial subscript in the active indexing style.
        start: Option<Expr>,
        /// Optional stop subscript, with inclusion determined by the active style.
        end: Option<Expr>,
        /// Optional stride between successive array indices.
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
    /// Quantity literal with a numeric value and unit string.
    ///
    /// Parsed from TaQL forms like `2rad`, `200km`, or `1 'km/s'`.
    Quantity {
        /// Numeric magnitude in the declared unit.
        value: f64,
        /// Unit expression, such as km/s.
        unit: String,
    },
    /// String value.
    String(String),
    /// Boolean value.
    Bool(bool),
    /// Complex number value.
    Complex(Complex64),
    /// Regex pattern literal: `p/pattern/flags` or `m/pattern/flags`.
    ///
    /// C++ reference: `TaQLRegexNode`.
    Regex {
        /// Raw regular-expression text.
        pattern: String,
        /// Match flags attached to the pattern.
        flags: String,
    },
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
    /// Floor division `//` (a real result, as CASA's `floor(a/b)`).
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
    /// Count of group values or rows for a wildcard argument.
    Count,
    /// Arithmetic total of the group values.
    Sum,
    /// Arithmetic mean of the group values.
    Avg,
    /// Smallest value in the group.
    Min,
    /// Largest value in the group.
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
            Self::Count(c) => write!(f, "{c}"),
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

fn fmt_table_name(f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
    let bare = !name.is_empty()
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        && !name.as_bytes()[0].is_ascii_digit();
    if bare {
        write!(f, "{name}")
    } else {
        write!(f, "\"{name}\"")
    }
}

impl fmt::Display for CountStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.style != IndexStyle::Glish {
            write!(f, "USING STYLE {} ", self.style)?;
        }
        write!(f, "COUNT ")?;
        for (i, col) in self.columns.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{col}")?;
        }
        if let Some(ref from) = self.from {
            write!(f, " FROM {from}")?;
        }
        if let Some(ref w) = self.where_clause {
            write!(f, " WHERE {w}")?;
        }
        Ok(())
    }
}

impl fmt::Display for CalcStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.style != IndexStyle::Glish {
            write!(f, "USING STYLE {} ", self.style)?;
        }
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
            if self.order_distinct {
                write!(f, "NODUPLICATES ")?;
            }
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
            write!(f, " GIVING")?;
            if !g.table_name.is_empty() {
                write!(f, " ")?;
                fmt_table_name(f, &g.table_name)?;
            }
            if let Some(ref ot) = g.output_type {
                write!(f, " AS {ot}")?;
            } else if let Some(ref options) = g.output_options {
                write!(f, " AS [")?;
                for (index, field) in options.iter().enumerate() {
                    if index > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}={}", field.name, field.value)?;
                    if let Some(data_type) = &field.data_type {
                        write!(f, " AS {data_type}")?;
                    }
                }
                write!(f, "]")?;
            }
        } else if let Some(ref expr) = self.giving_expr {
            write!(f, " GIVING [{expr}]")?;
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
        fmt_table_name(f, &self.name)?;
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
        if self.style != IndexStyle::Glish {
            write!(f, "USING STYLE {} ", self.style)?;
        }
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
        if self.style != IndexStyle::Glish {
            write!(f, "USING STYLE {} ", self.style)?;
        }
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
        if self.style != IndexStyle::Glish {
            write!(f, "USING STYLE {} ", self.style)?;
        }
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
            Self::PreparedConstant(value) => write!(f, "{value}"),
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
            Self::BoundRowLookup { original, .. } => write!(f, "{original}"),
            Self::BoundRowFunction(function) => write!(f, "{function}"),
            Self::ArrayLiteral(values) => {
                write!(f, "[")?;
                for (index, value) in values.iter().enumerate() {
                    if index != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{value}")?;
                }
                write!(f, "]")
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
            Self::Quantity { value, unit } => {
                let numeric = if value.fract() == 0.0 && value.is_finite() {
                    format!("{value:.1}")
                } else {
                    value.to_string()
                };
                if Unit::new(unit)
                    .map(|u| u.name().contains('/'))
                    .unwrap_or(false)
                {
                    write!(f, "{numeric} '{unit}'")
                } else {
                    write!(f, "{numeric}{unit}")
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
mod tests;
