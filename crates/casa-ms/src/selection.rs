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
use casa_tables::Table;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::sync::OnceLock;
use std::time::Instant;
use thiserror::Error;

mod edit;
pub(crate) mod parser;
pub mod syntax;

pub use edit::{MsSelectorEditContext, validate_ms_selector_edit};

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
    field_ids: Vec<i32>,
    spw_ids: Vec<i32>,
    spw_channel_selections: BTreeMap<i32, ChannelSelection>,
    data_desc_ids: Vec<i32>,
    antenna_ids: Vec<i32>,
    antenna_names: Vec<String>,
    baselines: Vec<(i32, i32)>,
    time_range: Option<(f64, f64)>,
    scan_numbers: Vec<i32>,
    state_ids: Vec<i32>,
    observation_ids: Vec<i32>,
    array_ids: Vec<i32>,
    taql_exprs: Vec<String>,
    correlation_types: Vec<i32>,
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

    pub(crate) fn from_summary_options(options: &crate::listobs::ListObsOptions) -> Self {
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

    fn compile(&self, ms: &MeasurementSet) -> MsResult<CompiledMsSelection> {
        if !self.selectdata && !self.has_text_selection() {
            return Ok(CompiledMsSelection::default());
        }
        if !self.selectdata {
            return Err(MsError::VersionError(
                "selectdata=false cannot be combined with MeasurementSet selectors".to_string(),
            ));
        }
        let mut compiled = parser::selection_from_options(ms, &self.to_summary_options())?;
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
        Ok(compiled)
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

    /// Return the retained channel selector for a spectral window, if present.
    pub fn channel_selection_for_spw(&self, spw_id: i32) -> Option<ChannelSelection> {
        self.spw
            .as_deref()
            .and_then(|selector| parse_spw_selector(selector).ok())
            .and_then(|selectors| {
                selectors
                    .into_iter()
                    .find(|selector| selector.spw_id == spw_id)
                    .and_then(|selector| selector.channels)
            })
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

pub(crate) fn resolve_row_indices_with_system_budget(
    ms: &MeasurementSet,
    request: &MsSelection,
) -> MsResult<Vec<usize>> {
    let budget = MsSelectionIoBudget::from_system_memory(
        2,
        std::mem::size_of::<ResolvedMsSelectionRow>(),
        None,
    )
    .map_err(|error| MsError::InvalidInput(error.to_string()))?;
    ms.resolve_selection(request, budget)
        .map(|resolved| resolved.row_indices().collect())
        .map_err(|error| match error {
            MsSelectionError::Domain(error) => error,
            other => MsError::InvalidInput(other.to_string()),
        })
}

impl CompiledMsSelection {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Select rows matching the given FIELD_IDs.
    pub(crate) fn field(mut self, ids: &[i32]) -> Self {
        self.field_ids.extend_from_slice(ids);
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
        self.apply_internal(ms, None)
    }

    fn apply_with_budget(
        &self,
        ms: &MeasurementSet,
        io: MsSelectionIoBudget,
    ) -> Result<Vec<usize>, MsSelectionError> {
        let rows_per_block = rows_per_block_for_budget(io)?;
        self.apply_internal(ms, Some(rows_per_block))
            .map_err(MsSelectionError::Domain)
    }

    fn apply_internal(
        &self,
        ms: &MeasurementSet,
        structured_rows_per_block: Option<usize>,
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

        if sel.taql_exprs.is_empty() {
            let structured_started_at = Instant::now();
            let rows =
                apply_structured_selection(&sel, ms, &effective_ddids, structured_rows_per_block)?;
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
        let view = ms.main_table().query(&taql)?;
        let rows = view.row_numbers().to_vec();
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

        if !self.field_ids.is_empty() {
            clauses.push(format!("FIELD_ID IN [{}]", int_list(&self.field_ids)));
        }

        if !self.spw_ids.is_empty() {
            // Direct DATA_DESC_ID filter (simple case)
            clauses.push(format!("DATA_DESC_ID IN [{}]", int_list(&self.spw_ids)));
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
        if io.requested_bytes_per_row < std::mem::size_of::<ResolvedMsSelectionRow>() {
            return Err(MsSelectionError::InvalidBudget(format!(
                "requested_bytes_per_row must include the {}-byte resolved row contract",
                std::mem::size_of::<ResolvedMsSelectionRow>()
            )));
        }
        let compiled = request.compile(self)?;
        let taql_predicate = compiled.resolved_to_taql(self)?;
        let selected_row_indices = compiled.apply_with_budget(self, io)?;
        let row_blocks = plan_selected_row_blocks(&selected_row_indices, io)?;
        let selected_rows = resolve_selected_row_facts(self, &selected_row_indices, &row_blocks)?;
        Ok(ResolvedMsSelection {
            request: request.clone(),
            selected_rows,
            row_blocks,
            channel_selections: compiled.spw_channel_selections,
            correlation_types: compiled.correlation_types,
            taql_predicate,
            requested_bytes_per_row: io.requested_bytes_per_row,
        })
    }
}

fn plan_selected_row_blocks(
    selected_rows: &[usize],
    io: MsSelectionIoBudget,
) -> Result<Vec<MsSelectionRowBlock>, MsSelectionError> {
    validate_io_budget(io)?;
    if selected_rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut rows_per_block = rows_per_block_for_budget(io)?;
    rows_per_block = rows_per_block.min(selected_rows.len().max(1));

    selected_rows
        .chunks(rows_per_block)
        .enumerate()
        .map(|(block_index, rows)| {
            let selected_row_start = block_index
                .checked_mul(rows_per_block)
                .ok_or(MsSelectionError::ByteOverflow)?;
            let planned_bytes = rows
                .len()
                .checked_mul(io.requested_bytes_per_row)
                .ok_or(MsSelectionError::ByteOverflow)?;
            Ok(MsSelectionRowBlock {
                selected_row_start,
                selected_row_count: rows.len(),
                planned_bytes,
            })
        })
        .collect()
}

fn resolve_selected_row_facts(
    ms: &MeasurementSet,
    selected_row_indices: &[usize],
    row_blocks: &[MsSelectionRowBlock],
) -> Result<Vec<ResolvedMsSelectionRow>, MsSelectionError> {
    let data_description = ms.data_description()?;
    let mut ddid_to_spw_pol = Vec::with_capacity(data_description.row_count());
    for row in 0..data_description.row_count() {
        ddid_to_spw_pol.push((
            data_description.spectral_window_id(row)?,
            data_description.polarization_id(row)?,
        ));
    }

    let table = ms.main_table();
    let mut resolved = Vec::with_capacity(selected_row_indices.len());
    for block in row_blocks {
        let end = block
            .selected_row_start
            .checked_add(block.selected_row_count)
            .ok_or(MsSelectionError::ByteOverflow)?;
        let rows = selected_row_indices
            .get(block.selected_row_start..end)
            .ok_or_else(|| {
                MsSelectionError::InvalidBudget(
                    "selected-row block lies outside the resolved selection".to_string(),
                )
            })?;
        let field_ids = load_i32_column(table, "FIELD_ID", rows)?;
        let data_desc_ids = load_i32_column(table, "DATA_DESC_ID", rows)?;
        let antenna1_ids = load_i32_column(table, "ANTENNA1", rows)?;
        let antenna2_ids = load_i32_column(table, "ANTENNA2", rows)?;
        let flag_rows = load_bool_column(table, "FLAG_ROW", rows)?;
        let times = load_f64_column(table, "TIME", rows)?;

        for slot in 0..rows.len() {
            let data_desc_id = data_desc_ids[slot];
            let (spectral_window_id, polarization_id) = usize::try_from(data_desc_id)
                .ok()
                .and_then(|ddid| ddid_to_spw_pol.get(ddid).copied())
                .ok_or_else(|| {
                    MsError::InvalidInput(format!(
                        "DATA_DESC_ID {data_desc_id} at MAIN row {} has no DATA_DESCRIPTION row",
                        rows[slot]
                    ))
                })?;
            resolved.push(ResolvedMsSelectionRow {
                row_index: rows[slot],
                field_id: field_ids[slot],
                data_desc_id,
                spectral_window_id,
                polarization_id,
                antenna1_id: antenna1_ids[slot],
                antenna2_id: antenna2_ids[slot],
                flag_row: flag_rows[slot],
                time_mjd_seconds: times[slot],
            });
        }
    }
    Ok(resolved)
}

fn rows_per_block_for_budget(io: MsSelectionIoBudget) -> Result<usize, MsSelectionError> {
    validate_io_budget(io)?;
    let per_block_budget_bytes = io.available_bytes / io.maximum_live_blocks;
    let mut rows_per_block = per_block_budget_bytes / io.requested_bytes_per_row;
    if rows_per_block == 0 {
        return Err(MsSelectionError::InsufficientBudget {
            requested_bytes_per_row: io.requested_bytes_per_row,
            per_block_budget_bytes,
        });
    }
    if let Some(alignment) = io.storage_alignment_rows
        && rows_per_block >= alignment
    {
        rows_per_block -= rows_per_block % alignment;
    }
    Ok(rows_per_block)
}

fn validate_io_budget(io: MsSelectionIoBudget) -> Result<(), MsSelectionError> {
    if io.maximum_live_blocks == 0 {
        return Err(MsSelectionError::InvalidBudget(
            "maximum_live_blocks must be positive".to_string(),
        ));
    }
    if io.requested_bytes_per_row == 0 {
        return Err(MsSelectionError::InvalidBudget(
            "requested_bytes_per_row must be positive".to_string(),
        ));
    }
    if matches!(io.storage_alignment_rows, Some(0)) {
        return Err(MsSelectionError::InvalidBudget(
            "storage_alignment_rows must be positive when supplied".to_string(),
        ));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn system_physical_memory_bytes() -> Option<usize> {
    let mut value = 0u64;
    let mut size = std::mem::size_of::<u64>() as libc::size_t;
    let result = unsafe {
        libc::sysctlbyname(
            c"hw.memsize".as_ptr(),
            (&mut value as *mut u64).cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    (result == 0 && value > 0).then_some(value as usize)
}

#[cfg(all(unix, not(target_os = "macos")))]
fn system_physical_memory_bytes() -> Option<usize> {
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if pages <= 0 || page_size <= 0 {
        return None;
    }
    (pages as usize).checked_mul(page_size as usize)
}

#[cfg(not(unix))]
fn system_physical_memory_bytes() -> Option<usize> {
    None
}

fn apply_structured_selection(
    sel: &CompiledMsSelection,
    ms: &MeasurementSet,
    effective_ddids: &[i32],
    rows_per_block: Option<usize>,
) -> MsResult<Vec<usize>> {
    let setup_started_at = Instant::now();
    let field_ids: HashSet<i32> = sel.field_ids.iter().copied().collect();
    let spw_ddids: HashSet<i32> = effective_ddids.iter().copied().collect();
    let data_desc_ids: HashSet<i32> = sel.data_desc_ids.iter().copied().collect();
    let antenna_ids: HashSet<i32> = sel.antenna_ids.iter().copied().collect();
    let baselines: HashSet<(i32, i32)> = sel.baselines.iter().copied().collect();
    let scan_numbers: HashSet<i32> = sel.scan_numbers.iter().copied().collect();
    let state_ids: HashSet<i32> = sel.state_ids.iter().copied().collect();
    let observation_ids: HashSet<i32> = sel.observation_ids.iter().copied().collect();
    let array_ids: HashSet<i32> = sel.array_ids.iter().copied().collect();

    if !sel.spw_ids.is_empty() && spw_ddids.is_empty() {
        return Ok(Vec::new());
    }

    let table = ms.main_table();
    let column_request = StructuredSelectionColumnRequest {
        field: !field_ids.is_empty(),
        data_desc: !sel.spw_ids.is_empty() || !data_desc_ids.is_empty(),
        antenna1: !antenna_ids.is_empty() || !baselines.is_empty(),
        antenna2: !antenna_ids.is_empty() || !baselines.is_empty(),
        time: sel.time_range.is_some(),
        scan: !scan_numbers.is_empty(),
        state: !state_ids.is_empty(),
        observation: !observation_ids.is_empty(),
        array: !array_ids.is_empty(),
    };
    let rows_per_block = rows_per_block.unwrap_or_else(|| table.row_count().max(1));
    let mut load_columns_ns = 0u64;
    let mut filter_rows_ns = 0u64;
    let mut rows = Vec::new();
    for start_row in (0..table.row_count()).step_by(rows_per_block) {
        let end_row = start_row
            .saturating_add(rows_per_block)
            .min(table.row_count());
        let block_rows = (start_row..end_row).collect::<Vec<_>>();
        let load_columns_started_at = Instant::now();
        let columns = load_structured_selection_columns(table, column_request, &block_rows)?;
        load_columns_ns =
            load_columns_ns.saturating_add(load_columns_started_at.elapsed().as_nanos() as u64);
        let filter_rows_started_at = Instant::now();
        for (slot, row) in block_rows.into_iter().enumerate() {
            if structured_row_matches(
                slot,
                sel,
                &columns,
                &field_ids,
                &spw_ddids,
                &data_desc_ids,
                &antenna_ids,
                &baselines,
                &scan_numbers,
                &state_ids,
                &observation_ids,
                &array_ids,
            ) {
                rows.push(row);
            }
        }
        filter_rows_ns =
            filter_rows_ns.saturating_add(filter_rows_started_at.elapsed().as_nanos() as u64);
    }

    log_selection_profile(
        "structured_selection",
        setup_started_at.elapsed().as_nanos() as u64,
        Some(format!(
            "rows={} row_count={} load_columns={:.3}s filter_rows={:.3}s requested=field:{} data_desc:{} antenna:{} time:{} scan:{} state:{} observation:{} array:{}",
            rows.len(),
            table.row_count(),
            load_columns_ns as f64 / 1_000_000_000.0,
            filter_rows_ns as f64 / 1_000_000_000.0,
            !field_ids.is_empty(),
            !spw_ddids.is_empty() || !data_desc_ids.is_empty(),
            !antenna_ids.is_empty() || !baselines.is_empty(),
            sel.time_range.is_some(),
            !scan_numbers.is_empty(),
            !state_ids.is_empty(),
            !observation_ids.is_empty(),
            !array_ids.is_empty(),
        )),
    );

    Ok(rows)
}

#[allow(clippy::too_many_arguments)]
fn structured_row_matches(
    slot: usize,
    sel: &CompiledMsSelection,
    columns: &StructuredSelectionColumns,
    field_ids: &HashSet<i32>,
    spw_ddids: &HashSet<i32>,
    data_desc_ids: &HashSet<i32>,
    antenna_ids: &HashSet<i32>,
    baselines: &HashSet<(i32, i32)>,
    scan_numbers: &HashSet<i32>,
    state_ids: &HashSet<i32>,
    observation_ids: &HashSet<i32>,
    array_ids: &HashSet<i32>,
) -> bool {
    if columns
        .field
        .as_ref()
        .is_some_and(|column| !field_ids.contains(&column[slot]))
    {
        return false;
    }
    if let Some(column) = columns.data_desc.as_ref() {
        let data_desc_id = column[slot];
        if (!spw_ddids.is_empty() && !spw_ddids.contains(&data_desc_id))
            || (!data_desc_ids.is_empty() && !data_desc_ids.contains(&data_desc_id))
        {
            return false;
        }
    }
    if let (Some(antenna1), Some(antenna2)) = (columns.antenna1.as_ref(), columns.antenna2.as_ref())
    {
        let pair = (antenna1[slot], antenna2[slot]);
        if (!antenna_ids.is_empty()
            && !antenna_ids.contains(&pair.0)
            && !antenna_ids.contains(&pair.1))
            || (!baselines.is_empty() && !baselines.contains(&pair))
        {
            return false;
        }
    }
    if let (Some((start, end)), Some(column)) = (sel.time_range, columns.time.as_ref())
        && !(start..=end).contains(&column[slot])
    {
        return false;
    }
    columns
        .scan
        .as_ref()
        .is_none_or(|column| scan_numbers.contains(&column[slot]))
        && columns
            .state
            .as_ref()
            .is_none_or(|column| state_ids.contains(&column[slot]))
        && columns
            .observation
            .as_ref()
            .is_none_or(|column| observation_ids.contains(&column[slot]))
        && columns
            .array
            .as_ref()
            .is_none_or(|column| array_ids.contains(&column[slot]))
}

fn selection_profile_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("CASA_RS_SELECTION_PROFILE") {
        Ok(value) => {
            let trimmed = value.trim();
            !trimmed.is_empty()
                && trimmed != "0"
                && !trimmed.eq_ignore_ascii_case("false")
                && !trimmed.eq_ignore_ascii_case("off")
        }
        Err(_) => false,
    })
}

fn log_selection_profile(phase: &str, total_ns: u64, detail: Option<String>) {
    if !selection_profile_enabled() {
        return;
    }
    let mut line = format!(
        "[casa-ms selection] phase={phase} dt={:.3}s",
        total_ns as f64 / 1_000_000_000.0
    );
    if let Some(detail) = detail
        && !detail.is_empty()
    {
        line.push(' ');
        line.push_str(&detail);
    }
    eprintln!("{line}");
}

#[derive(Clone, Copy, Debug, Default)]
struct StructuredSelectionColumnRequest {
    field: bool,
    data_desc: bool,
    antenna1: bool,
    antenna2: bool,
    time: bool,
    scan: bool,
    state: bool,
    observation: bool,
    array: bool,
}

#[derive(Debug, Default)]
struct StructuredSelectionColumns {
    field: Option<Vec<i32>>,
    data_desc: Option<Vec<i32>>,
    antenna1: Option<Vec<i32>>,
    antenna2: Option<Vec<i32>>,
    time: Option<Vec<f64>>,
    scan: Option<Vec<i32>>,
    state: Option<Vec<i32>>,
    observation: Option<Vec<i32>>,
    array: Option<Vec<i32>>,
}

fn load_structured_selection_columns(
    table: &Table,
    request: StructuredSelectionColumnRequest,
    rows: &[usize],
) -> MsResult<StructuredSelectionColumns> {
    let mut columns = StructuredSelectionColumns::default();

    if rows.is_empty() {
        if request.field {
            columns.field = Some(Vec::new());
        }
        if request.data_desc {
            columns.data_desc = Some(Vec::new());
        }
        if request.antenna1 {
            columns.antenna1 = Some(Vec::new());
        }
        if request.antenna2 {
            columns.antenna2 = Some(Vec::new());
        }
        if request.time {
            columns.time = Some(Vec::new());
        }
        if request.scan {
            columns.scan = Some(Vec::new());
        }
        if request.state {
            columns.state = Some(Vec::new());
        }
        if request.observation {
            columns.observation = Some(Vec::new());
        }
        if request.array {
            columns.array = Some(Vec::new());
        }
        return Ok(columns);
    }

    if request.field {
        columns.field = Some(load_i32_column(table, "FIELD_ID", rows)?);
    }
    if request.data_desc {
        columns.data_desc = Some(load_i32_column(table, "DATA_DESC_ID", rows)?);
    }
    if request.antenna1 {
        columns.antenna1 = Some(load_i32_column(table, "ANTENNA1", rows)?);
    }
    if request.antenna2 {
        columns.antenna2 = Some(load_i32_column(table, "ANTENNA2", rows)?);
    }
    if request.time {
        columns.time = Some(load_f64_column(table, "TIME", rows)?);
    }
    if request.scan {
        columns.scan = Some(load_i32_column(table, "SCAN_NUMBER", rows)?);
    }
    if request.state {
        columns.state = Some(load_i32_column(table, "STATE_ID", rows)?);
    }
    if request.observation {
        columns.observation = Some(load_i32_column(table, "OBSERVATION_ID", rows)?);
    }
    if request.array {
        columns.array = Some(load_i32_column(table, "ARRAY_ID", rows)?);
    }

    Ok(columns)
}

fn load_i32_column(table: &Table, column: &str, rows: &[usize]) -> MsResult<Vec<i32>> {
    table
        .column_accessor(column)?
        .scalar_cells_owned_for_rows(rows)?
        .into_iter()
        .zip(rows.iter().copied())
        .map(|(value, row)| match value {
            Some(casa_types::ScalarValue::Int32(v)) => Ok(v),
            Some(other) => Err(MsError::ColumnTypeMismatch {
                column: column.to_string(),
                table: "MAIN".to_string(),
                expected: "Int32".to_string(),
                found: format!("{:?}", other.primitive_type()),
            }),
            None => Err(MsError::MissingColumn {
                column: format!("{column}[row={row}]"),
                table: "MAIN".to_string(),
            }),
        })
        .collect()
}

fn load_f64_column(table: &Table, column: &str, rows: &[usize]) -> MsResult<Vec<f64>> {
    table
        .column_accessor(column)?
        .scalar_cells_owned_for_rows(rows)?
        .into_iter()
        .zip(rows.iter().copied())
        .map(|(value, row)| match value {
            Some(casa_types::ScalarValue::Float64(v)) => Ok(v),
            Some(other) => Err(MsError::ColumnTypeMismatch {
                column: column.to_string(),
                table: "MAIN".to_string(),
                expected: "Float64".to_string(),
                found: format!("{:?}", other.primitive_type()),
            }),
            None => Err(MsError::MissingColumn {
                column: format!("{column}[row={row}]"),
                table: "MAIN".to_string(),
            }),
        })
        .collect()
}

fn load_bool_column(table: &Table, column: &str, rows: &[usize]) -> MsResult<Vec<bool>> {
    table
        .column_accessor(column)?
        .scalar_cells_owned_for_rows(rows)?
        .into_iter()
        .zip(rows.iter().copied())
        .map(|(value, row)| match value {
            Some(casa_types::ScalarValue::Bool(v)) => Ok(v),
            Some(other) => Err(MsError::ColumnTypeMismatch {
                column: column.to_string(),
                table: "MAIN".to_string(),
                expected: "Bool".to_string(),
                found: format!("{:?}", other.primitive_type()),
            }),
            None => Err(MsError::MissingColumn {
                column: format!("{column}[row={row}]"),
                table: "MAIN".to_string(),
            }),
        })
        .collect()
}

fn int_list(ids: &[i32]) -> String {
    ids.iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn append_csv(target: &mut Option<String>, value: &str) {
    if value.is_empty() {
        return;
    }
    match target {
        Some(existing) if !existing.is_empty() => {
            existing.push(',');
            existing.push_str(value);
        }
        _ => *target = Some(value.to_string()),
    }
}

/// Resolve antenna names to IDs using the ANTENNA subtable.
fn resolve_antenna_names(ms: &MeasurementSet, names: &[String]) -> MsResult<Vec<i32>> {
    let ant = ms.antenna()?;
    let mut ids = Vec::new();
    for row in 0..ant.row_count() {
        let name = ant.name(row)?;
        if names.iter().any(|n| n == &name) {
            ids.push(row as i32);
        }
    }
    if ids.is_empty() {
        return Err(MsError::MissingColumn {
            column: format!("antenna names {names:?}"),
            table: "ANTENNA".to_string(),
        });
    }
    Ok(ids)
}

/// Resolve spectral window IDs to DATA_DESC_IDs using the DATA_DESCRIPTION subtable.
fn resolve_spw_to_ddid(ms: &MeasurementSet, spw_ids: &[i32]) -> MsResult<Vec<i32>> {
    let dd = ms.data_description()?;
    let mut ddids = Vec::new();
    for row in 0..dd.row_count() {
        let spw_id = dd.spectral_window_id(row)?;
        if spw_ids.contains(&spw_id) {
            ddids.push(row as i32);
        }
    }
    Ok(ddids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SubtableId;
    use crate::test_helpers::{default_value, default_value_for_def};

    fn apply_for_test(selection: &MsSelection, ms: &MeasurementSet) -> Vec<usize> {
        selection.compile(ms).unwrap().apply(ms).unwrap()
    }

    #[test]
    fn empty_selection_selects_all() {
        let sel = MsSelection::new();
        assert_eq!(sel.to_taql(), "SELECT *");
    }

    #[test]
    fn row_block_budget_rejects_one_byte_below_one_row() {
        let error = plan_selected_row_blocks(
            &[3],
            MsSelectionIoBudget {
                available_bytes: 7,
                maximum_live_blocks: 1,
                requested_bytes_per_row: 8,
                storage_alignment_rows: None,
            },
        )
        .expect_err("one row cannot fit");
        assert!(matches!(error, MsSelectionError::InsufficientBudget { .. }));
    }

    #[test]
    fn row_block_budget_accepts_at_and_above_one_row() {
        for available_bytes in [8, 9] {
            let blocks = plan_selected_row_blocks(
                &[3, 5],
                MsSelectionIoBudget {
                    available_bytes,
                    maximum_live_blocks: 1,
                    requested_bytes_per_row: 8,
                    storage_alignment_rows: None,
                },
            )
            .expect("one-row blocks fit");
            assert_eq!(blocks.len(), 2);
            assert!(blocks.iter().all(|block| block.planned_bytes == 8));
        }
    }

    #[test]
    fn row_block_budget_aligns_without_exceeding_budget() {
        let blocks = plan_selected_row_blocks(
            &(0..9).collect::<Vec<_>>(),
            MsSelectionIoBudget {
                available_bytes: 100,
                maximum_live_blocks: 1,
                requested_bytes_per_row: 10,
                storage_alignment_rows: Some(4),
            },
        )
        .expect("aligned blocks");
        assert_eq!(
            blocks
                .iter()
                .map(|block| block.selected_row_count)
                .collect::<Vec<_>>(),
            vec![8, 1]
        );
        assert!(blocks.iter().all(|block| block.planned_bytes <= 100));
    }

    #[test]
    fn empty_selection_needs_no_row_budget() {
        let blocks = plan_selected_row_blocks(
            &[],
            MsSelectionIoBudget {
                available_bytes: 0,
                maximum_live_blocks: 1,
                requested_bytes_per_row: usize::MAX,
                storage_alignment_rows: None,
            },
        )
        .expect("empty selection");
        assert!(blocks.is_empty());
    }

    #[test]
    fn maximum_row_size_uses_checked_one_row_blocks() {
        let blocks = plan_selected_row_blocks(
            &[usize::MAX],
            MsSelectionIoBudget {
                available_bytes: usize::MAX,
                maximum_live_blocks: 1,
                requested_bytes_per_row: usize::MAX,
                storage_alignment_rows: None,
            },
        )
        .expect("maximum-sized row budget");
        assert_eq!(blocks[0].planned_bytes, usize::MAX);
    }

    #[test]
    fn read_plan_covers_zero_one_wide_and_overflow_workloads() {
        let zero = MsReadPlan::new(
            0,
            MsSelectionIoBudget {
                available_bytes: 0,
                maximum_live_blocks: 1,
                requested_bytes_per_row: 8,
                storage_alignment_rows: None,
            },
        )
        .expect("zero-row plan");
        assert_eq!(zero.rows_per_block, 0);

        let one = MsReadPlan::new(
            1,
            MsSelectionIoBudget {
                available_bytes: 8,
                maximum_live_blocks: 1,
                requested_bytes_per_row: 8,
                storage_alignment_rows: None,
            },
        )
        .expect("one-row plan");
        assert_eq!(one.rows_per_block, 1);
        assert_eq!(one.bytes_per_block, 8);

        let constrained = MsReadPlan::new(
            1_000,
            MsSelectionIoBudget {
                available_bytes: 1_024,
                maximum_live_blocks: 2,
                requested_bytes_per_row: 128,
                storage_alignment_rows: Some(2),
            },
        )
        .expect("constrained plan");
        assert_eq!(constrained.rows_per_block, 4);

        let error = MsReadPlan::new(
            usize::MAX,
            MsSelectionIoBudget {
                available_bytes: usize::MAX,
                maximum_live_blocks: 1,
                requested_bytes_per_row: usize::MAX / 2 + 1,
                storage_alignment_rows: None,
            },
        )
        .expect("one row fits without overflow");
        assert_eq!(error.rows_per_block, 1);
    }

    #[test]
    fn field_selection() {
        let sel = MsSelection::new().field(&[0, 1]);
        assert_eq!(sel.to_taql(), "SELECT * WHERE FIELD_ID IN [0,1]");
    }

    #[test]
    fn combined_selection() {
        let sel = MsSelection::new().field(&[0]).scan(&[1, 2, 3]);
        let taql = sel.to_taql();
        assert!(taql.contains("FIELD_ID IN [0]"));
        assert!(taql.contains("SCAN_NUMBER IN [1,2,3]"));
        assert!(taql.contains(" AND "));
    }

    #[test]
    fn time_range_selection() {
        let sel = MsSelection::new().time_range(1000.0, 2000.0);
        let taql = sel.to_taql();
        assert!(taql.contains("TIME>=1000"));
        assert!(taql.contains("TIME<=2000"));
    }

    #[test]
    fn antenna_selection() {
        let sel = MsSelection::new().antenna(&[0, 1]);
        let taql = sel.to_taql();
        assert!(taql.contains("ANTENNA1 IN [0,1]"));
        assert!(taql.contains("ANTENNA2 IN [0,1]"));
    }

    #[test]
    fn baseline_selection() {
        let sel = MsSelection::new().baseline(&[(0, 1), (2, 3)]);
        let taql = sel.to_taql();
        assert!(taql.contains("ANTENNA1==0 AND ANTENNA2==1"));
        assert!(taql.contains("ANTENNA1==2 AND ANTENNA2==3"));
    }

    #[test]
    fn raw_taql_expression() {
        let sel = MsSelection::new().taql("UVW[0] > 100").taql("TIME > 0");
        let taql = sel.to_taql();
        assert!(taql.contains("(UVW[0] > 100)"));
        assert!(taql.contains("(TIME > 0)"));
    }

    #[test]
    fn data_description_selection() {
        let sel = MsSelection::new().data_description(&[2, 4]);
        let taql = sel.to_taql();
        assert!(taql.contains("DATA_DESC_ID IN [2,4]"));
    }

    #[test]
    fn state_selection() {
        let sel = MsSelection::new().state(&[1, 5]);
        let taql = sel.to_taql();
        assert!(taql.contains("STATE_ID IN [1,5]"));
    }

    #[test]
    fn apply_field_selection() {
        use crate::builder::MeasurementSetBuilder;
        use crate::test_helpers::default_value;
        use casa_types::{RecordField, RecordValue, ScalarValue, Value};

        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();

        // Add rows with different FIELD_IDs
        let schema = ms.main_table().schema().unwrap().clone();
        for field_id in [0, 0, 1, 1, 2] {
            let fields: Vec<RecordField> = schema
                .columns()
                .iter()
                .map(|col| {
                    if col.name() == "FIELD_ID" {
                        RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(field_id)))
                    } else {
                        RecordField::new(col.name(), default_value(col.name()))
                    }
                })
                .collect();
            ms.main_table_mut()
                .add_row(RecordValue::new(fields))
                .unwrap();
        }

        let sel = MsSelection::new().field(&[0, 1]);
        let rows = apply_for_test(&sel, &ms);
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn apply_time_range_selection() {
        use crate::builder::MeasurementSetBuilder;
        use crate::test_helpers::default_value;
        use casa_types::{RecordField, RecordValue, ScalarValue, Value};

        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();

        let schema = ms.main_table().schema().unwrap().clone();
        for time in [100.0, 200.0, 300.0, 400.0, 500.0] {
            let fields: Vec<RecordField> = schema
                .columns()
                .iter()
                .map(|col| {
                    if col.name() == "TIME" {
                        RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(time)))
                    } else {
                        RecordField::new(col.name(), default_value(col.name()))
                    }
                })
                .collect();
            ms.main_table_mut()
                .add_row(RecordValue::new(fields))
                .unwrap();
        }

        let sel = MsSelection::new().time_range(200.0, 400.0);
        let rows = apply_for_test(&sel, &ms);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn apply_antenna_name_selection() {
        use crate::builder::MeasurementSetBuilder;
        use casa_types::{RecordField, RecordValue, ScalarValue, Value};

        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();

        // Add antennas
        {
            let mut ant = ms.antenna_mut().unwrap();
            ant.add_antenna(
                "VLA01",
                "N01",
                "GROUND-BASED",
                "ALT-AZ",
                [0.0; 3],
                [0.0; 3],
                25.0,
            )
            .unwrap();
            ant.add_antenna(
                "VLA02",
                "N02",
                "GROUND-BASED",
                "ALT-AZ",
                [0.0; 3],
                [0.0; 3],
                25.0,
            )
            .unwrap();
            ant.add_antenna(
                "VLA03",
                "N03",
                "GROUND-BASED",
                "ALT-AZ",
                [0.0; 3],
                [0.0; 3],
                25.0,
            )
            .unwrap();
        }

        // Add rows referencing different antennas
        let schema = ms.main_table().schema().unwrap().clone();
        for (ant1, ant2) in [(0, 1), (0, 2), (1, 2)] {
            let fields: Vec<RecordField> = schema
                .columns()
                .iter()
                .map(|col| match col.name() {
                    "ANTENNA1" => {
                        RecordField::new("ANTENNA1", Value::Scalar(ScalarValue::Int32(ant1)))
                    }
                    "ANTENNA2" => {
                        RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(ant2)))
                    }
                    name => RecordField::new(name, default_value(name)),
                })
                .collect();
            ms.main_table_mut()
                .add_row(RecordValue::new(fields))
                .unwrap();
        }

        let sel = MsSelection::new().antenna_name(&["VLA01"]);
        let rows = apply_for_test(&sel, &ms);
        // VLA01 is antenna 0, which appears in rows 0 (ant1=0) and 1 (ant1=0)
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn apply_spw_selection_without_matching_ddid_returns_no_rows() {
        use crate::builder::MeasurementSetBuilder;
        use crate::schema;
        use casa_types::{RecordField, RecordValue, ScalarValue, Value};

        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();

        let dd_fields: Vec<RecordField> = schema::data_description::REQUIRED_COLUMNS
            .iter()
            .map(|col| match col.name {
                "SPECTRAL_WINDOW_ID" => {
                    RecordField::new(col.name, Value::Scalar(ScalarValue::Int32(0)))
                }
                "POLARIZATION_ID" => {
                    RecordField::new(col.name, Value::Scalar(ScalarValue::Int32(0)))
                }
                "FLAG_ROW" => RecordField::new(col.name, Value::Scalar(ScalarValue::Bool(false))),
                _ => RecordField::new(col.name, default_value_for_def(col)),
            })
            .collect();
        ms.subtable_mut(SubtableId::DataDescription)
            .unwrap()
            .add_row(RecordValue::new(dd_fields))
            .unwrap();

        let schema = ms.main_table().schema().unwrap().clone();
        let row_fields: Vec<RecordField> = schema
            .columns()
            .iter()
            .map(|col| match col.name() {
                "DATA_DESC_ID" => {
                    RecordField::new(col.name(), Value::Scalar(ScalarValue::Int32(7)))
                }
                name => RecordField::new(name, default_value(name)),
            })
            .collect();
        ms.main_table_mut()
            .add_row(RecordValue::new(row_fields))
            .unwrap();

        let rows = apply_for_test(&MsSelection::new().spw(&[7]), &ms);
        assert!(rows.is_empty());
    }

    #[test]
    fn spw_selector_preserves_channel_selection_for_array_cell_consumers() {
        let selection = MsSelection::new().spw_selector("0:82~89,2").unwrap();

        assert!(selection.channel_selection_for_spw(2).is_none());
        let channels = selection
            .channel_selection_for_spw(0)
            .expect("spw 0 channel selector")
            .indices(166)
            .expect("expand channels");
        assert_eq!(channels, (82..=89).collect::<Vec<_>>());
    }

    #[test]
    fn apply_field_and_spw_selection_returns_only_intersection() {
        use crate::builder::MeasurementSetBuilder;
        use casa_types::{RecordField, RecordValue, ScalarValue, Value};

        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();

        ms.subtable_mut(SubtableId::DataDescription)
            .unwrap()
            .add_row(RecordValue::new(vec![
                RecordField::new("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("POLARIZATION_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ]))
            .unwrap();
        ms.subtable_mut(SubtableId::DataDescription)
            .unwrap()
            .add_row(RecordValue::new(vec![
                RecordField::new("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new("POLARIZATION_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ]))
            .unwrap();

        let schema = ms.main_table().schema().unwrap().clone();
        for (field_id, data_desc_id) in [(0, 0), (0, 1), (1, 0), (1, 1)] {
            let row_fields: Vec<RecordField> = schema
                .columns()
                .iter()
                .map(|col| match col.name() {
                    "FIELD_ID" => {
                        RecordField::new(col.name(), Value::Scalar(ScalarValue::Int32(field_id)))
                    }
                    "DATA_DESC_ID" => RecordField::new(
                        col.name(),
                        Value::Scalar(ScalarValue::Int32(data_desc_id)),
                    ),
                    name => RecordField::new(name, default_value(name)),
                })
                .collect();
            ms.main_table_mut()
                .add_row(RecordValue::new(row_fields))
                .unwrap();
        }

        let rows = apply_for_test(&MsSelection::new().field(&[0]).spw(&[1]), &ms);
        assert_eq!(rows, vec![1]);
    }
}
