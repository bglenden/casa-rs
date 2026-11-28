// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical MeasurementSet selection intent and resolved row-block planning.
//!
//! [`MsSelection`] lets callers build up selection criteria (field, spectral
//! window, antenna, time range, scan, observation, array). A
//! [`MeasurementSet`] resolves that intent into bounded physical row blocks.
//!
//! # Example
//!
//! ```rust
//! use casa_ms::selection::MsSelection;
//!
//! let sel = MsSelection::new()
//!     .field(&[0, 1])
//!     .spw(&[0])
//!     .state(&[0])
//!     .scan(&[1, 2, 3]);
//! assert!(sel.to_taql().contains("FIELD_ID"));
//! ```
//!
//! Cf. C++ `MSSelection`.

use crate::error::{MsError, MsResult};
use crate::ms::MeasurementSet;
use crate::selection::syntax::{ChannelSelection, parse_spw_selector};
#[cfg(test)]
use casa_core::ArrayValue;
use casa_tables::Table;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::time::Instant;
use thiserror::Error;

pub mod antenna_expr;
pub(crate) mod array_expr;
mod columns;
pub use columns::load_uvw_column;
use columns::{SelectedRowColumns, load_f64_column, load_field_ids, load_i32_column};
mod edit;
mod feed_expr;
mod intent;
pub mod parser;
mod row_grammar;
pub use row_grammar::first_unflagged_time_row;
pub use row_grammar::flagdata_c_string;
pub use row_grammar::flagdata_channel_ranges;
pub mod spw_expr;
pub mod syntax;
mod taql_geometry;
pub mod taql_selection;
pub mod time_expr;
mod tool_indices;
mod uv_expr;
pub use uv_expr::UvDistanceSelection;

pub use antenna_expr::{
    AntennaRow, BaselineSelection, resolve_antenna_lists, resolve_antenna_selector,
};
pub use edit::{MsSelectorEditContext, MsSelectorKind, validate_ms_selector_edit};
pub use parser::{FieldSelectorRow, field_selector_workspace_bytes, resolve_field_selector_rows};
pub use spw_expr::{
    ChannelFrequencyRange, ChannelRange, DataDescriptionRow, SpectralWindowRow, SpwSelection,
    resolve_spw_selector,
};
pub use time_expr::{
    LogicalRowFilter, TimeCondition, TimeDefaults, TimeListEntry, TimeSelection, TimeSelector,
    TimeSelectorDefaults, TimeTerm, msselection_first_logical_row, resolve_time_selector,
};
pub use tool_indices::{MsSelectionIndices, ParsedMsToolSelection, ToolCorrelationSelection};

mod row_planning;
mod structured;

pub use row_planning::resolve_selected_row_facts;
pub use row_planning::system_physical_memory_bytes;
use row_planning::*;
use structured::*;

#[derive(Clone, Copy, PartialEq, Eq)]
/// Select the CASA row-selection grammar and task-specific time defaults.
pub enum RowGrammar {
    /// Use the native typed row-selection rules.
    Standard,
    /// Use the public CASA MSSelection grammar.
    Casa,
    /// Use the transform task’s row-selection conventions.
    Transform,
    /// Use flag-agent selection and its reference integration.
    FlagData {
        /// MAIN row supplying default date and time components in the agent grammar.
        agent_time_default_row: Option<usize>,
    },
}

/// Builder for MS row selection criteria.
///
/// Each method adds a constraint. Constraints are ANDed together when
/// converted to TaQL. Call [`to_taql`](MsSelection::to_taql) for diagnostics
/// or [`MeasurementSet::resolve_selection`] for physical execution.
///
/// Cf. C++ `MSSelection`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MsSelection {
    /// Enable the structured selectors below.
    pub selectdata: bool,
    /// FIELD selection expression supplied by a task or application surface.
    pub field: Option<String>,
    /// SPW/channel selection expression supplied by a task or application surface.
    pub spw: Option<String>,
    /// TIMERANGE selection expression supplied by a task or application surface.
    pub timerange: Option<String>,
    /// UVRANGE selection expression supplied by a task or application surface.
    pub uvrange: Option<String>,
    /// ANTENNA/baseline selection expression supplied by a task or application surface.
    pub antenna: Option<String>,
    /// SCAN selection expression supplied by a task or application surface.
    pub scan: Option<String>,
    /// CORRELATION selection expression supplied by a task or application surface.
    pub correlation: Option<String>,
    /// ARRAY selection expression supplied by a task or application surface.
    pub array: Option<String>,
    /// OBSERVATION selection expression supplied by a task or application surface.
    pub observation: Option<String>,
    /// INTENT selection expression supplied by a task or application surface.
    pub intent: Option<String>,
    /// FEED selection expression supplied by a task or application surface.
    pub feed: Option<String>,
    /// Explicit `DATA_DESC_ID` selection used by physical visibility consumers.
    pub data_description: Option<String>,
    /// Explicit `STATE_ID` selection used by physical visibility consumers.
    pub state: Option<String>,
    /// Raw TaQL/MSSelection expression supplied by a task or application surface.
    pub msselect: Option<String>,
}

impl Default for MsSelection {
    fn default() -> Self {
        Self {
            selectdata: true,
            field: None,
            spw: None,
            timerange: None,
            uvrange: None,
            antenna: None,
            scan: None,
            correlation: None,
            array: None,
            observation: None,
            intent: None,
            feed: None,
            data_description: None,
            state: None,
            msselect: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CompiledMsSelection {
    pub(crate) field_ids: Option<Vec<i32>>,
    pub(crate) spw_ids: Vec<i32>,
    spw_channel_selections: BTreeMap<i32, ChannelSelection>,
    spw_data_desc_ids: Option<Vec<i32>>,
    data_desc_ids: Vec<i32>,
    antenna_ids: Vec<i32>,
    antenna_names: Vec<String>,
    baselines: Vec<(i32, i32)>,
    casa_baseline: Option<BaselineSelection>,
    time_range: Option<(f64, f64)>,
    casa_time: Option<TimeSelection>,
    scan_numbers: Vec<i32>,
    casa_scan_ranges: Option<Vec<(i32, i32)>>,
    state_ids: Vec<i32>,
    observation_ids: Vec<i32>,
    casa_observation_ranges: Option<Vec<(i32, i32)>>,
    array_ids: Vec<i32>,
    casa_array_ranges: Option<Vec<(i32, i32)>>,
    casa_feed: Option<feed_expr::FeedSelection>,
    casa_uv: Option<uv_expr::UvDistanceSelection>,
    uv_ranges: Vec<UvSelectionRange>,
    uv_taql_exprs: Vec<String>,
    taql_exprs: Vec<String>,
    casa_taql_exprs: Vec<String>,
    correlation_types: Vec<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum UvBoundOp {
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct UvBound {
    pub(crate) value: f64,
    pub(crate) op: UvBoundOp,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum UvUnit {
    Meters(f64),
    Lambda(f64),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct UvSelectionRange {
    pub(crate) lower: Option<UvBound>,
    pub(crate) upper: Option<UvBound>,
    pub(crate) unit: UvUnit,
}

/// One bound of a [`UvDistanceInterval`]: its value, scaled to metres or
/// wavelengths, and whether the interval holds it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UvDistanceLimit {
    /// The bound's value.
    pub value: f64,
    /// Whether a distance equal to the bound is selected.
    pub inclusive: bool,
}

/// One interval of a CASA `uvrange` selector, as its syntax states it:
/// each bound, if stated, in metres or (when `wavelengths`) in wavelengths.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UvDistanceInterval {
    /// The lower bound.
    pub lower: Option<UvDistanceLimit>,
    /// The upper bound.
    pub upper: Option<UvDistanceLimit>,
    /// Whether the bounds are in wavelengths, not metres.
    pub wavelengths: bool,
}

/// The intervals of the CASA `uvrange` selector `text`, unit prefixes
/// applied (`klambda`, `km`, …).
///
/// # Errors
///
/// [`MsError::InvalidInput`] for text that is not `uvrange` syntax.
pub fn parse_uv_distance_intervals(text: &str) -> MsResult<Vec<UvDistanceInterval>> {
    Ok(parser::parse_uvrange_selector(text)?
        .into_iter()
        .map(|range| {
            let (scale, wavelengths) = match range.unit {
                UvUnit::Meters(scale) => (scale, false),
                UvUnit::Lambda(scale) => (scale, true),
            };
            let limit = |bound: UvBound| UvDistanceLimit {
                value: bound.value * scale,
                inclusive: matches!(bound.op, UvBoundOp::GreaterEqual | UvBoundOp::LessEqual),
            };
            UvDistanceInterval {
                lower: range.lower.map(limit),
                upper: range.upper.map(limit),
                wavelengths,
            }
        })
        .collect())
}

/// Explicit resource inputs for deterministic selected-row block planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MsSelectionIoBudget {
    /// Total bytes available to all simultaneously live row blocks.
    pub available_bytes: usize,
    /// Maximum number of row blocks simultaneously owned by the operation.
    pub maximum_live_blocks: usize,
    /// Exact projected bytes retained for one selected row.
    pub requested_bytes_per_row: usize,
    /// Optional storage tile or bucket row span used for alignment.
    pub storage_alignment_rows: Option<usize>,
}

impl MsSelectionIoBudget {
    /// Build an explicit read budget from a conservative share of physical memory.
    ///
    /// This is a resource-policy source, not a row-count fallback: physical row
    /// counts still come exclusively from the requested per-row byte shape.
    pub fn from_system_memory(
        maximum_live_blocks: usize,
        requested_bytes_per_row: usize,
        storage_alignment_rows: Option<usize>,
    ) -> Result<Self, MsSelectionError> {
        let physical_bytes = system_physical_memory_bytes().ok_or_else(|| {
            MsSelectionError::InvalidBudget(
                "physical memory is unavailable; supply an explicit I/O budget".to_string(),
            )
        })?;
        let available_bytes = physical_bytes / 8;
        Ok(Self {
            available_bytes,
            maximum_live_blocks,
            requested_bytes_per_row,
            storage_alignment_rows,
        })
    }
}

/// Pure checked plan for a bounded row scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MsReadPlan {
    /// Total rows in the workload.
    pub row_count: usize,
    /// Rows in each full physical block.
    pub rows_per_block: usize,
    /// Bytes retained by each full block.
    pub bytes_per_block: usize,
    /// Maximum simultaneously live blocks used to derive the plan.
    pub maximum_live_blocks: usize,
}

impl MsReadPlan {
    /// Derive a row scan from workload geometry and an explicit resource budget.
    pub fn new(row_count: usize, io: MsSelectionIoBudget) -> Result<Self, MsSelectionError> {
        validate_io_budget(io)?;
        if row_count == 0 {
            return Ok(Self {
                row_count,
                rows_per_block: 0,
                bytes_per_block: 0,
                maximum_live_blocks: io.maximum_live_blocks,
            });
        }
        let rows_per_block = rows_per_block_for_budget(io)?.min(row_count);
        let bytes_per_block = rows_per_block
            .checked_mul(io.requested_bytes_per_row)
            .ok_or(MsSelectionError::ByteOverflow)?;
        Ok(Self {
            row_count,
            rows_per_block,
            bytes_per_block,
            maximum_live_blocks: io.maximum_live_blocks,
        })
    }
}

/// One immutable selected-row block in MeasurementSet row order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MsSelectionRowBlock {
    /// First selected-row slot represented by this block.
    pub selected_row_start: usize,
    /// Number of selected-row slots represented by this block.
    pub selected_row_count: usize,
    /// Planned resident bytes for this block.
    pub planned_bytes: usize,
}

/// Canonically resolved physical facts for one selected MAIN row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ResolvedMsSelectionRow {
    /// Physical MAIN-table row index.
    pub row_index: usize,
    /// Resolved `FIELD_ID`.
    pub field_id: i32,
    /// Resolved `DATA_DESC_ID`.
    pub data_desc_id: i32,
    /// Spectral-window id resolved through `DATA_DESCRIPTION`.
    pub spectral_window_id: i32,
    /// Polarization id resolved through `DATA_DESCRIPTION`.
    pub polarization_id: i32,
    /// Resolved `ANTENNA1`.
    pub antenna1_id: i32,
    /// Resolved `ANTENNA2`.
    pub antenna2_id: i32,
    /// Resolved row-level flag.
    pub flag_row: bool,
    /// Resolved MAIN-table time in MJD seconds.
    pub time_mjd_seconds: f64,
}

/// Canonically resolved MeasurementSet selection and physical read plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ResolvedMsSelection {
    /// Original application-level intent retained for diagnostics and provenance.
    pub request: MsSelection,
    /// Selected physical MAIN-table rows and resolved facts in deterministic order.
    pub selected_rows: Vec<ResolvedMsSelectionRow>,
    /// Bounded physical row blocks derived from the explicit I/O budget.
    pub row_blocks: Vec<MsSelectionRowBlock>,
    /// Retained per-SPW channel selections for array-cell consumers.
    pub channel_selections: BTreeMap<i32, ChannelSelection>,
    /// Requested Stokes/correlation codes in canonical integer form.
    pub correlation_types: Vec<i32>,
    /// Canonical physical predicate used to obtain the selected MAIN rows.
    pub taql_predicate: String,
    /// Exact projected bytes retained per selected row.
    pub requested_bytes_per_row: usize,
}

impl ResolvedMsSelection {
    /// Return the physical row indices without allocating a second row list.
    pub fn row_indices(&self) -> impl ExactSizeIterator<Item = usize> + '_ {
        self.selected_rows.iter().map(|row| row.row_index)
    }

    /// Borrow the resolved rows represented by one planned block.
    pub fn rows_for_block(
        &self,
        block: &MsSelectionRowBlock,
    ) -> Result<&[ResolvedMsSelectionRow], MsSelectionError> {
        let end = block
            .selected_row_start
            .checked_add(block.selected_row_count)
            .ok_or(MsSelectionError::ByteOverflow)?;
        self.selected_rows
            .get(block.selected_row_start..end)
            .ok_or_else(|| {
                MsSelectionError::InvalidBudget(
                    "selected-row block lies outside the resolved selection".to_string(),
                )
            })
    }
}

/// Failure while resolving or physically planning a MeasurementSet selection.
#[derive(Debug, Error)]
pub enum MsSelectionError {
    /// Selection syntax, metadata resolution, or table access failed.
    #[error(transparent)]
    Domain(#[from] MsError),
    /// The supplied resource inputs are invalid.
    #[error("invalid MeasurementSet selection I/O budget: {0}")]
    InvalidBudget(String),
    /// One projected row cannot fit in the per-block budget.
    #[error(
        "one selected row requires {requested_bytes_per_row} bytes but the per-block budget is {per_block_budget_bytes} bytes"
    )]
    InsufficientBudget {
        /// Exact projected bytes retained per row.
        requested_bytes_per_row: usize,
        /// Bytes available to one live block.
        per_block_budget_bytes: usize,
    },
    /// Checked byte accounting overflowed.
    #[error("MeasurementSet selection byte accounting overflowed")]
    ByteOverflow,
}

impl MsSelection {
    /// Create an empty selection (selects all rows).
    pub fn new() -> Self {
        Self::default()
    }

    /// Return whether this intent contains any surface-supplied text selector.
    pub fn has_text_selection(&self) -> bool {
        self.field.is_some()
            || self.spw.is_some()
            || self.timerange.is_some()
            || self.uvrange.is_some()
            || self.antenna.is_some()
            || self.scan.is_some()
            || self.correlation.is_some()
            || self.array.is_some()
            || self.observation.is_some()
            || self.intent.is_some()
            || self.feed.is_some()
            || self.data_description.is_some()
            || self.state.is_some()
            || self.msselect.is_some()
    }

    /// Project this canonical intent into the listobs-style operation options.
    pub fn to_summary_options(&self) -> crate::listobs::ListObsOptions {
        crate::listobs::ListObsOptions {
            verbose: true,
            selectdata: self.selectdata,
            field: self.field.clone(),
            spw: self.spw.clone(),
            antenna: self.antenna.clone(),
            scan: self.scan.clone(),
            observation: self.observation.clone(),
            array: self.array.clone(),
            timerange: self.timerange.clone(),
            uvrange: self.uvrange.clone(),
            correlation: self.correlation.clone(),
            intent: self.intent.clone(),
            msselect: self.msselect.clone(),
            feed: self.feed.clone(),
            listunfl: false,
            cachesize_mb: None,
        }
    }

    /// Build a row-selection request carrying only `listobs` selection fields.
    pub fn from_summary_options(options: &crate::listobs::ListObsOptions) -> Self {
        Self {
            selectdata: options.selectdata,
            field: options.field.clone(),
            spw: options.spw.clone(),
            timerange: options.timerange.clone(),
            uvrange: options.uvrange.clone(),
            antenna: options.antenna.clone(),
            scan: options.scan.clone(),
            correlation: options.correlation.clone(),
            array: options.array.clone(),
            observation: options.observation.clone(),
            intent: options.intent.clone(),
            feed: options.feed.clone(),
            data_description: None,
            state: None,
            msselect: options.msselect.clone(),
        }
    }

    pub(crate) fn compile(&self, ms: &MeasurementSet) -> MsResult<CompiledMsSelection> {
        self.compile_with_row_grammar(ms, RowGrammar::Standard, None)
    }

    fn compile_with_row_grammar(
        &self,
        ms: &MeasurementSet,
        grammar: RowGrammar,
        rows_per_block: Option<usize>,
    ) -> MsResult<CompiledMsSelection> {
        if !self.selectdata && !self.has_text_selection() {
            return Ok(CompiledMsSelection::default());
        }
        if !self.selectdata {
            return Err(MsError::VersionError(
                "selectdata=false cannot be combined with MeasurementSet selectors".to_string(),
            ));
        }
        let mut compiled = CompiledMsSelection::new();
        if let Some(value) = self.data_description.as_deref() {
            compiled = compiled.data_description(
                &crate::selection::syntax::parse_numeric_id_selector(value, "data-description")?,
            );
        }
        if let Some(value) = self.state.as_deref() {
            compiled = compiled.state(&crate::selection::syntax::parse_numeric_id_selector(
                value, "state",
            )?);
        }
        parser::selection_from_options(
            ms,
            &self.to_summary_options(),
            compiled,
            grammar,
            rows_per_block,
        )
    }

    /// Select rows matching the given FIELD_IDs.
    pub fn field(mut self, ids: &[i32]) -> Self {
        append_csv(&mut self.field, &int_list(ids));
        self
    }

    /// Select rows whose spectral-window id matches one of `ids`.
    pub fn spw(mut self, ids: &[i32]) -> Self {
        append_csv(&mut self.spw, &int_list(ids));
        self
    }

    /// Add a CASA spectral-window and optional channel selector.
    pub fn spw_selector(mut self, selector: &str) -> MsResult<Self> {
        parse_spw_selector(selector)?;
        append_csv(&mut self.spw, selector);
        Ok(self)
    }

    /// Return all channel ranges for a window in the numeric SPW selector,
    /// in expression order, retaining overlaps between ranges.
    ///
    /// A matching clause without a channel range selects the whole window
    /// and returns `None`. A missing or unparseable selection also returns
    /// `None`.
    pub fn channel_selection_for_spw(&self, spw_id: i32) -> Option<ChannelSelection> {
        let clauses = parse_spw_selector(self.spw.as_deref()?).ok()?;
        let mut matching = clauses.into_iter().filter(|clause| clause.spw_id == spw_id);
        let mut selection = matching.next()?.channels?;
        for clause in matching {
            selection.segments.extend(clause.channels?.segments);
        }
        Some(selection)
    }

    /// Select rows matching the given DATA_DESC_IDs directly.
    pub fn data_description(mut self, ids: &[i32]) -> Self {
        append_csv(&mut self.data_description, &int_list(ids));
        self
    }

    /// Select rows where ANTENNA1 or ANTENNA2 matches any of the given IDs.
    pub fn antenna(mut self, ids: &[i32]) -> Self {
        append_csv(&mut self.antenna, &int_list(ids));
        self
    }

    /// Select rows involving any named antenna.
    pub fn antenna_name(mut self, names: &[&str]) -> Self {
        append_csv(&mut self.antenna, &names.join(","));
        self
    }

    /// Select rows matching specific antenna pairs.
    pub fn baseline(mut self, pairs: &[(i32, i32)]) -> Self {
        let selector = pairs
            .iter()
            .map(|(left, right)| format!("{left}&&{right}"))
            .collect::<Vec<_>>()
            .join(",");
        append_csv(&mut self.antenna, &selector);
        self
    }

    /// Select rows within a TIME range `[start, end]` in MJD seconds.
    pub fn time_range(self, start: f64, end: f64) -> Self {
        self.taql(&format!("TIME>={start} AND TIME<={end}"))
    }

    /// Select rows matching the given SCAN_NUMBERs.
    pub fn scan(mut self, numbers: &[i32]) -> Self {
        append_csv(&mut self.scan, &int_list(numbers));
        self
    }

    /// Select rows matching the given STATE_IDs.
    pub fn state(mut self, ids: &[i32]) -> Self {
        append_csv(&mut self.state, &int_list(ids));
        self
    }

    /// Select rows matching the given OBSERVATION_IDs.
    pub fn observation(mut self, ids: &[i32]) -> Self {
        append_csv(&mut self.observation, &int_list(ids));
        self
    }

    /// Select rows matching the given ARRAY_IDs.
    pub fn array(mut self, ids: &[i32]) -> Self {
        append_csv(&mut self.array, &int_list(ids));
        self
    }

    /// Add a raw TaQL WHERE expression, ANDed with any existing expression.
    pub fn taql(mut self, expr: &str) -> Self {
        self.msselect = Some(match self.msselect.take() {
            Some(existing) => format!("({existing}) AND ({expr})"),
            None => expr.to_string(),
        });
        self
    }

    /// Render the directly representable numeric/raw portion as TaQL.
    pub fn to_taql(&self) -> String {
        let mut clauses = Vec::new();
        if let Some(field) = &self.field {
            clauses.push(format!("FIELD_ID IN [{field}]"));
        }
        if let Some(spw) = &self.spw
            && let Ok(selectors) = parse_spw_selector(spw)
        {
            clauses.push(format!(
                "DATA_DESC_ID IN [{}]",
                selectors
                    .iter()
                    .map(|selector| selector.spw_id.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        if let Some(scan) = &self.scan {
            clauses.push(format!("SCAN_NUMBER IN [{scan}]"));
        }
        if let Some(observation) = &self.observation {
            clauses.push(format!("OBSERVATION_ID IN [{observation}]"));
        }
        if let Some(array) = &self.array {
            clauses.push(format!("ARRAY_ID IN [{array}]"));
        }
        if let Some(data_description) = &self.data_description {
            clauses.push(format!("DATA_DESC_ID IN [{data_description}]"));
        }
        if let Some(state) = &self.state {
            clauses.push(format!("STATE_ID IN [{state}]"));
        }
        if let Some(antenna) = &self.antenna {
            let mut ids = Vec::new();
            let mut baselines = Vec::new();
            for token in antenna.split(',').map(str::trim) {
                if let Some((left, right)) = token.split_once("&&") {
                    baselines.push(format!(
                        "(ANTENNA1=={} AND ANTENNA2=={})",
                        left.trim(),
                        right.trim()
                    ));
                } else if token.parse::<i32>().is_ok() {
                    ids.push(token);
                }
            }
            if !ids.is_empty() {
                let ids = ids.join(",");
                clauses.push(format!("(ANTENNA1 IN [{ids}] OR ANTENNA2 IN [{ids}])"));
            }
            if !baselines.is_empty() {
                clauses.push(format!("({})", baselines.join(" OR ")));
            }
        }
        if let Some(msselect) = &self.msselect {
            clauses.push(format!("({msselect})"));
        }
        if clauses.is_empty() {
            "SELECT *".to_string()
        } else {
            format!("SELECT * WHERE {}", clauses.join(" AND "))
        }
    }
}

/// Resolve MS main-table row indices using the standard grammar and system memory budget.
pub fn resolve_row_indices_with_system_budget(
    ms: &MeasurementSet,
    request: &MsSelection,
) -> MsResult<Vec<usize>> {
    resolve_row_indices_with_grammar(ms, request, RowGrammar::Standard)
}

/// Resolve MS main-table row indices using CASA selection grammar and system memory budget.
pub fn resolve_casa_row_indices(
    ms: &MeasurementSet,
    request: &MsSelection,
) -> MsResult<Vec<usize>> {
    resolve_casa_selection_with_system_budget(ms, request)
        .map(|selection| selection.row_indices().collect())
}

/// Resolve full MS selection metadata using CASA grammar and system memory budget.
pub fn resolve_casa_selection_with_system_budget(
    ms: &MeasurementSet,
    request: &MsSelection,
) -> MsResult<ResolvedMsSelection> {
    resolve_selection_with_grammar(ms, request, RowGrammar::Casa)
}

/// Compile the flag agent’s time expression with defaults taken from the selected MAIN row.
pub fn compile_flagdata_agent_time(
    ms: &MeasurementSet,
    request: &MsSelection,
    initial_default_row: usize,
) -> MsResult<Option<TimeSelection>> {
    let rows_per_block = rows_per_block_for_budget(default_selection_budget()?)
        .map_err(|error| MsError::InvalidInput(error.to_string()))?;
    request
        .compile_with_row_grammar(
            ms,
            RowGrammar::FlagData {
                agent_time_default_row: Some(initial_default_row),
            },
            Some(rows_per_block),
        )
        .map(|compiled| compiled.casa_time)
}

/// Resolve flag-agent selection in bounded row blocks using the available memory budget.
pub fn resolve_flagdata_selection_with_system_budget(
    ms: &MeasurementSet,
    request: &MsSelection,
    agent_time_default_row: Option<usize>,
) -> MsResult<ResolvedMsSelection> {
    resolve_selection_with_grammar(
        ms,
        request,
        RowGrammar::FlagData {
            agent_time_default_row,
        },
    )
}

fn resolve_row_indices_with_grammar(
    ms: &MeasurementSet,
    request: &MsSelection,
    grammar: RowGrammar,
) -> MsResult<Vec<usize>> {
    resolve_selection_with_grammar(ms, request, grammar)
        .map(|resolved| resolved.row_indices().collect())
}

fn resolve_selection_with_grammar(
    ms: &MeasurementSet,
    request: &MsSelection,
    grammar: RowGrammar,
) -> MsResult<ResolvedMsSelection> {
    ms.resolve_rows(request, default_selection_budget()?, grammar)
        .map(|(resolved, _)| resolved)
        .map_err(|error| match error {
            MsSelectionError::Domain(error) => error,
            other => MsError::InvalidInput(other.to_string()),
        })
}

fn default_selection_budget() -> MsResult<MsSelectionIoBudget> {
    MsSelectionIoBudget::from_system_memory(2, std::mem::size_of::<ResolvedMsSelectionRow>(), None)
        .map_err(|error| MsError::InvalidInput(error.to_string()))
}

impl CompiledMsSelection {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Set the FIELD predicate: `None` selects all; `Some([])` selects no rows.
    pub(crate) fn field(mut self, ids: Option<Vec<i32>>) -> Self {
        self.field_ids = ids;
        self
    }

    /// Select rows matching a CASA spectral-window selector and retain any
    /// channel ranges for consumers that operate inside row array cells.
    pub(crate) fn spw_selector(mut self, selector: &str) -> MsResult<Self> {
        for parsed in parse_spw_selector(selector)? {
            self.spw_ids.push(parsed.spw_id);
            if let Some(channels) = parsed.channels {
                self.spw_channel_selections.insert(parsed.spw_id, channels);
            }
        }
        Ok(self)
    }

    /// Select rows matching the given DATA_DESC_IDs directly.
    pub(crate) fn data_description(mut self, ids: &[i32]) -> Self {
        self.data_desc_ids.extend_from_slice(ids);
        self
    }

    /// Select rows where ANTENNA1 or ANTENNA2 matches any of the given IDs.
    pub(crate) fn antenna(mut self, ids: &[i32]) -> Self {
        self.antenna_ids.extend_from_slice(ids);
        self
    }

    /// Select by antenna name (resolved via the ANTENNA subtable).
    pub(crate) fn antenna_name(mut self, names: &[&str]) -> Self {
        self.antenna_names
            .extend(names.iter().map(|s| s.to_string()));
        self
    }

    /// Select rows matching specific antenna pairs (baselines).
    pub(crate) fn baseline(mut self, pairs: &[(i32, i32)]) -> Self {
        self.baselines.extend_from_slice(pairs);
        self
    }

    /// Select rows matching the given SCAN_NUMBERs.
    pub(crate) fn scan(mut self, numbers: &[i32]) -> Self {
        self.scan_numbers.extend_from_slice(numbers);
        self
    }

    /// Select rows matching the given STATE_IDs.
    pub(crate) fn state(mut self, ids: &[i32]) -> Self {
        self.state_ids.extend_from_slice(ids);
        self
    }

    /// Select rows matching the given OBSERVATION_IDs.
    pub(crate) fn observation(mut self, ids: &[i32]) -> Self {
        self.observation_ids.extend_from_slice(ids);
        self
    }

    /// Select rows matching the given ARRAY_IDs.
    pub(crate) fn array(mut self, ids: &[i32]) -> Self {
        self.array_ids.extend_from_slice(ids);
        self
    }

    /// Add a raw TaQL WHERE expression (ANDed with other criteria).
    pub(crate) fn taql(mut self, expr: &str) -> Self {
        self.taql_exprs.push(expr.to_string());
        self
    }

    /// Convert the selection to a TaQL SELECT statement.
    ///
    /// Returns a string like `SELECT * WHERE FIELD_ID IN [0,1] AND ...`.
    pub(crate) fn to_taql(&self) -> String {
        let clauses = self.where_clauses();
        if clauses.is_empty() {
            "SELECT *".to_string()
        } else {
            format!("SELECT * WHERE {}", clauses.join(" AND "))
        }
    }

    fn resolved_to_taql(&self, ms: &MeasurementSet) -> MsResult<String> {
        let mut resolved = self.clone();
        if !resolved.antenna_names.is_empty() {
            let ids = resolve_antenna_names(ms, &resolved.antenna_names)?;
            resolved.antenna_ids.extend(ids);
            resolved.antenna_names.clear();
        }
        if !resolved.spw_ids.is_empty() {
            let ddids = resolve_spw_to_ddid(ms, &resolved.spw_ids)?;
            resolved.data_desc_ids.extend(ddids);
            resolved.spw_ids.clear();
        }
        Ok(resolved.to_taql())
    }

    /// Apply this selection to a MeasurementSet, returning the selected row indices.
    ///
    /// If antenna names were specified, they are resolved against the ANTENNA
    /// subtable first.
    pub(crate) fn apply(&self, ms: &MeasurementSet) -> MsResult<Vec<usize>> {
        self.apply_internal(ms, None, false)
    }

    fn first_match(
        &self,
        ms: &MeasurementSet,
        rows_per_block: Option<usize>,
    ) -> MsResult<Option<usize>> {
        self.apply_internal(ms, rows_per_block, true)
            .map(|rows| rows.first().copied())
    }

    fn apply_internal(
        &self,
        ms: &MeasurementSet,
        structured_rows_per_block: Option<usize>,
        first_only: bool,
    ) -> MsResult<Vec<usize>> {
        let apply_started_at = Instant::now();
        let mut sel = self.clone();

        // Resolve antenna names to IDs
        let resolve_antenna_started_at = Instant::now();
        if !sel.antenna_names.is_empty() {
            let resolved = resolve_antenna_names(ms, &sel.antenna_names)?;
            sel.antenna_ids.extend(resolved);
            sel.antenna_names.clear();
        }
        let resolve_antenna_ns = resolve_antenna_started_at.elapsed().as_nanos() as u64;

        // Resolve SPW IDs to DATA_DESC_IDs
        let mut effective_ddids = Vec::new();
        let resolve_spw_started_at = Instant::now();
        if !sel.spw_ids.is_empty() {
            effective_ddids = resolve_spw_to_ddid(ms, &sel.spw_ids)?;
        }
        let resolve_spw_ns = resolve_spw_started_at.elapsed().as_nanos() as u64;

        // gaincal supplies this predicate even when there is no user TaQL.
        // It only needs two scalar columns, so keep the bounded scan path.
        let exclude_autocorrelations = sel.taql_exprs == ["ANTENNA1!=ANTENNA2"];
        if sel.taql_exprs.is_empty() || exclude_autocorrelations {
            let structured_started_at = Instant::now();
            let rows = apply_structured_selection(
                &sel,
                ms,
                &effective_ddids,
                structured_rows_per_block,
                exclude_autocorrelations,
                first_only,
            )?;
            log_selection_profile(
                "apply_structured",
                apply_started_at.elapsed().as_nanos() as u64,
                Some(format!(
                    "rows={} resolve_antenna={:.3}s resolve_spw={:.3}s structured={:.3}s",
                    rows.len(),
                    resolve_antenna_ns as f64 / 1_000_000_000.0,
                    resolve_spw_ns as f64 / 1_000_000_000.0,
                    structured_started_at.elapsed().as_nanos() as f64 / 1_000_000_000.0,
                )),
            );
            return Ok(rows);
        }

        let taql = if sel.spw_ids.is_empty() {
            sel.to_taql()
        } else {
            // Replace spw_ids with direct DATA_DESC_ID filter
            let mut sel2 = sel.clone();
            sel2.spw_ids.clear();
            let mut clauses = sel2.where_clauses();
            if effective_ddids.is_empty() {
                clauses.push("FALSE".to_string());
            } else {
                clauses.push(format!(
                    "DATA_DESC_ID IN [{}]",
                    effective_ddids
                        .iter()
                        .map(|id| id.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                ));
            }
            if clauses.is_empty() {
                "SELECT *".to_string()
            } else {
                format!("SELECT * WHERE {}", clauses.join(" AND "))
            }
        };

        let taql_started_at = Instant::now();
        let rows = taql_selection::select_rows(ms, &taql, structured_rows_per_block, first_only)?;
        log_selection_profile(
            "apply_taql",
            apply_started_at.elapsed().as_nanos() as u64,
            Some(format!(
                "rows={} resolve_antenna={:.3}s resolve_spw={:.3}s taql={:.3}s",
                rows.len(),
                resolve_antenna_ns as f64 / 1_000_000_000.0,
                resolve_spw_ns as f64 / 1_000_000_000.0,
                taql_started_at.elapsed().as_nanos() as f64 / 1_000_000_000.0,
            )),
        );
        Ok(rows)
    }

    fn where_clauses(&self) -> Vec<String> {
        let mut clauses = Vec::new();

        if let Some(ids) = &self.field_ids {
            clauses.push(if ids.is_empty() {
                "FALSE".to_string()
            } else {
                format!("FIELD_ID IN [{}]", int_list(ids))
            });
        }

        if !self.spw_ids.is_empty() {
            // Direct DATA_DESC_ID filter (simple case)
            clauses.push(format!("DATA_DESC_ID IN [{}]", int_list(&self.spw_ids)));
        }

        if let Some(ids) = &self.spw_data_desc_ids {
            clauses.push(if ids.is_empty() {
                "FALSE".to_string()
            } else {
                format!("DATA_DESC_ID IN [{}]", int_list(ids))
            });
        }

        if !self.data_desc_ids.is_empty() {
            clauses.push(format!(
                "DATA_DESC_ID IN [{}]",
                int_list(&self.data_desc_ids)
            ));
        }

        if !self.antenna_ids.is_empty() {
            let ids = int_list(&self.antenna_ids);
            clauses.push(format!("(ANTENNA1 IN [{ids}] OR ANTENNA2 IN [{ids}])"));
        }

        if !self.baselines.is_empty() {
            let pairs: Vec<String> = self
                .baselines
                .iter()
                .map(|(a1, a2)| format!("(ANTENNA1=={a1} AND ANTENNA2=={a2})"))
                .collect();
            clauses.push(format!("({})", pairs.join(" OR ")));
        }

        if let Some((start, end)) = self.time_range {
            clauses.push(format!("TIME>={start} AND TIME<={end}"));
        }

        if !self.scan_numbers.is_empty() {
            clauses.push(format!("SCAN_NUMBER IN [{}]", int_list(&self.scan_numbers)));
        }

        if !self.state_ids.is_empty() {
            clauses.push(format!("STATE_ID IN [{}]", int_list(&self.state_ids)));
        }

        if !self.observation_ids.is_empty() {
            clauses.push(format!(
                "OBSERVATION_ID IN [{}]",
                int_list(&self.observation_ids)
            ));
        }

        if !self.array_ids.is_empty() {
            clauses.push(format!("ARRAY_ID IN [{}]", int_list(&self.array_ids)));
        }

        for expr in &self.uv_taql_exprs {
            clauses.push(format!("({expr})"));
        }

        for expr in &self.casa_taql_exprs {
            clauses.push(format!("({expr})"));
        }

        for expr in &self.taql_exprs {
            clauses.push(format!("({expr})"));
        }

        clauses
    }
}

impl MeasurementSet {
    /// Resolve one canonical selection intent into deterministic bounded row blocks.
    pub fn resolve_selection(
        &self,
        request: &MsSelection,
        io: MsSelectionIoBudget,
    ) -> Result<ResolvedMsSelection, MsSelectionError> {
        self.resolve_rows(request, io, RowGrammar::Standard)
            .map(|(rows, _)| rows)
    }

    /// Resolve CASA's full row and channel selection grammars.
    ///
    /// Consumers use the returned channel selections for array-cell access.
    pub fn resolve_casa_selection(
        &self,
        request: &MsSelection,
        io: MsSelectionIoBudget,
    ) -> Result<ResolvedMsSelection, MsSelectionError> {
        self.resolve_rows(request, io, RowGrammar::Casa)
            .map(|(rows, _)| rows)
    }

    /// Resolve the admitted MAIN rows and compiled time selection under the supplied grammar and I/O budget.
    pub fn resolve_selection_with_time(
        &self,
        request: &MsSelection,
        io: MsSelectionIoBudget,
        grammar: RowGrammar,
    ) -> Result<(ResolvedMsSelection, Option<TimeSelection>), MsSelectionError> {
        self.resolve_rows(request, io, grammar)
    }

    fn resolve_rows(
        &self,
        request: &MsSelection,
        io: MsSelectionIoBudget,
        grammar: RowGrammar,
    ) -> Result<(ResolvedMsSelection, Option<TimeSelection>), MsSelectionError> {
        if io.requested_bytes_per_row < std::mem::size_of::<ResolvedMsSelectionRow>() {
            return Err(MsSelectionError::InvalidBudget(format!(
                "requested_bytes_per_row must include the {}-byte resolved row contract",
                std::mem::size_of::<ResolvedMsSelectionRow>()
            )));
        }
        let rows_per_block = Some(rows_per_block_for_budget(io)?);
        let compiled = request.compile_with_row_grammar(self, grammar, rows_per_block)?;
        let taql_predicate = compiled.resolved_to_taql(self)?;
        let selected_row_indices = compiled.apply_internal(self, rows_per_block, false)?;
        let row_blocks = plan_selected_row_blocks(&selected_row_indices, io)?;
        let selected_rows = resolve_selected_row_facts(self, &selected_row_indices, &row_blocks)?;
        Ok((
            ResolvedMsSelection {
                request: request.clone(),
                selected_rows,
                row_blocks,
                channel_selections: compiled.spw_channel_selections,
                correlation_types: compiled.correlation_types,
                taql_predicate,
                requested_bytes_per_row: io.requested_bytes_per_row,
            },
            compiled.casa_time,
        ))
    }
}

pub use array_expr::ArraySelection;

#[cfg(test)]
mod tests;
