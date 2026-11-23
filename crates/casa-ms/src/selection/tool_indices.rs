// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA MSSelection side records for the Python MeasurementSet tool.

use super::antenna_expr::AntennaSelectionIndices;
use super::syntax::{row_id_interval, trim_row_id_space};
use super::{
    CompiledMsSelection, MsSelection, RowGrammar, default_selection_budget, int_list,
    rows_per_block_for_budget,
};
use crate::{MeasurementSet, MsError, MsResult};

/// The table identifiers and channel ranges recorded while parsing a CASA MS
/// selection. These are the clause results, independently of the rows selected
/// by applying the complete predicate.
#[derive(Default)]
pub struct MsSelectionIndices {
    /// FIELD IDs, in the FIELD selector's canonical order.
    pub field: Vec<i32>,
    /// SCAN_NUMBERs, in their requested order, with repeats.
    pub scan: Vec<i32>,
    /// The requested identifiers present in OBSERVATION.
    pub observation: Vec<i32>,
    /// STATE IDs resolved from the intent.
    pub state: Vec<i32>,
    /// First-side antenna identifiers, signed for negated terms.
    pub antenna1: Vec<i32>,
    /// Second-side antenna identifiers, signed for negated pairs.
    pub antenna2: Vec<i32>,
    /// Baselines in the order the antenna grammar produced them.
    pub baselines: Vec<[i32; 2]>,
    /// Spectral window identifiers in the order the SPW grammar emits.
    pub spw: Vec<i32>,
    /// Inclusive `[window, first channel, last channel, stride]` ranges.
    pub channel: Vec<[i32; 4]>,
    /// DATA_DESCRIPTION IDs matching the SPW clause.
    pub spwdd: Vec<i32>,
    /// DATA_DESCRIPTION IDs matching the polarization clause.
    pub poldd: Vec<i32>,
}

impl MsSelectionIndices {
    /// Resolve the data descriptions accepted by both applicable clauses.
    pub fn descriptions(&self) -> Vec<i32> {
        match (self.spwdd.is_empty(), self.poldd.is_empty()) {
            (_, true) => self.spwdd.clone(),
            (true, _) => self.poldd.clone(),
            _ => {
                let mut ids = self
                    .poldd
                    .iter()
                    .copied()
                    .filter(|id| self.spwdd.contains(id))
                    .collect::<Vec<_>>();
                ids.sort_unstable();
                ids.dedup();
                ids
            }
        }
    }
}

/// A parsed MeasurementSet tool selection with its auxiliary index records
/// and the corresponding row predicate.
pub struct ParsedMsToolSelection {
    /// Identifier and channel lists reported by CASA's tool.
    pub indices: MsSelectionIndices,
    compiled: CompiledMsSelection,
    rows_per_block: usize,
}

impl ParsedMsToolSelection {
    /// Whether an expression constrains the rows of the supplied view.
    pub fn has_row_expression(&self) -> bool {
        !self.compiled.where_clauses().is_empty()
    }

    /// Validate a user TaQL clause even when no row view is requested.
    ///
    /// # Errors
    /// The supplied predicate is invalid for this MS.
    pub fn validate_taql(&self, ms: &MeasurementSet) -> MsResult<()> {
        if !self.compiled.taql_exprs.is_empty() {
            self.compiled.first_match(ms, Some(self.rows_per_block))?;
        }
        Ok(())
    }

    /// Apply the predicate and return matching indices in the supplied view.
    ///
    /// # Errors
    /// A selection column or predicate cannot be evaluated.
    pub fn select_rows(&self, ms: &MeasurementSet) -> MsResult<Vec<usize>> {
        self.compiled
            .apply_internal(ms, Some(self.rows_per_block), false)
    }
}

impl MeasurementSet {
    /// Parse the auxiliary identifier lists for a CASA MS tool selection.
    ///
    /// # Errors
    /// A supplied selector cannot be parsed or names invalid metadata.
    pub fn parse_ms_tool_selection(
        &self,
        request: &MsSelection,
    ) -> MsResult<ParsedMsToolSelection> {
        let budget = default_selection_budget()?;
        let rows_per_block = rows_per_block_for_budget(budget)
            .map_err(|error| MsError::InvalidInput(error.to_string()))?;
        let poldd = nonempty(request.correlation.as_deref())
            .map(|text| polarization_indices(self, text))
            .transpose()?
            .unwrap_or_default();
        let mut rows = request.clone();
        rows.correlation = None;
        if !poldd.is_empty() {
            rows.data_description = Some(int_list(&poldd));
        }
        let compiled =
            rows.compile_with_row_grammar(self, RowGrammar::Casa, Some(rows_per_block))?;
        let field = compiled.field_ids.clone().unwrap_or_default();
        if field.is_empty()
            && let Some(text) = nonempty(request.field.as_deref())
        {
            return Err(MsError::InvalidInput(format!(
                "field selector {text:?} did not match any existing fields"
            )));
        }
        let mut table = MsSelectionIndices {
            field,
            state: compiled.state_ids.clone(),
            poldd,
            ..MsSelectionIndices::default()
        };
        if let Some(text) = nonempty(request.spw.as_deref()) {
            let spw = self.resolve_spw_selector(text)?;
            table
                .spw
                .extend(spw.spw_ids().iter().map(|&value| value as i32));
            table
                .spwdd
                .extend(spw.data_description_ids().iter().map(|&value| value as i32));
            table.channel.extend(
                spw.channel_list()
                    .iter()
                    .map(|row| [row.spw_id as i32, row.start, row.stop, row.step.max(1)]),
            );
        }
        if let Some(text) = nonempty(request.antenna.as_deref()) {
            let AntennaSelectionIndices {
                first,
                second,
                baselines,
            } = self.antenna_selection_indices(text)?;
            table.antenna1 = first;
            table.antenna2 = second;
            table.baselines = baselines;
        }
        if let Some(text) = nonempty(request.scan.as_deref()) {
            table.scan = scan_indices(text, rows_per_block)?;
        }
        if let Some(text) = nonempty(request.observation.as_deref()) {
            table.observation = observation_indices(text, self.observation()?.row_count())?;
        }
        Ok(ParsedMsToolSelection {
            indices: table,
            compiled,
            rows_per_block,
        })
    }
}

fn nonempty(text: Option<&str>) -> Option<&str> {
    text.filter(|text| !text.is_empty())
}

/// CASA's auxiliary SCAN_NUMBER list uses a 1000-scan bound for an
/// open-ended greater-than expression. The row predicate stays unbounded.
const CASA_MAX_SCAN: i64 = 1000;

fn scan_indices(text: &str, max_ids: usize) -> MsResult<Vec<i32>> {
    let mut values = Vec::new();
    for item in text.split(',').map(trim_row_id_space) {
        let (first, last) = selector_interval(item, "scan")?;
        let first = first.max(0);
        let last = if last == i64::from(i32::MAX) && item.starts_with('>') {
            CASA_MAX_SCAN + i64::from(!item.starts_with(">="))
        } else {
            last
        };
        if first > last {
            continue;
        }
        let count = usize::try_from(last - first + 1)
            .map_err(|_| MsError::InvalidInput("scan index list is too large".into()))?;
        if count > max_ids.saturating_sub(values.len()) {
            return Err(MsError::InvalidInput(
                "scan index list exceeds the selection memory budget".into(),
            ));
        }
        values.extend((first..=last).map(|value| value as i32));
    }
    Ok(values)
}

fn observation_indices(text: &str, table_rows: usize) -> MsResult<Vec<i32>> {
    let intervals = text
        .split(',')
        .map(trim_row_id_space)
        .map(|item| selector_interval(item, "observation"))
        .collect::<MsResult<Vec<_>>>()?;
    Ok((0..table_rows)
        .filter(|&row| {
            intervals
                .iter()
                .any(|&(first, last)| (first..=last).contains(&(row as i64)))
        })
        .map(|row| row as i32)
        .collect())
}

fn selector_interval(text: &str, label: &str) -> MsResult<(i64, i64)> {
    row_id_interval(text)
        .ok_or_else(|| MsError::InvalidInput(format!("invalid {label} selector term {text:?}")))
}

/// CASA's polarization parser accepts semicolon-separated correlation terms.
/// A term may name its own spectral windows before the colon.
fn polarization_indices(ms: &MeasurementSet, text: &str) -> MsResult<Vec<i32>> {
    use std::collections::HashSet;

    let data = ms.data_description()?;
    let polarization = ms.polarization()?;
    let mut result = Vec::new();
    let mut seen = HashSet::new();
    let descriptions = (0..data.row_count())
        .filter_map(|row| match data.flag_row(row) {
            Ok(true) => None,
            Ok(false) => Some(Ok(row)),
            Err(error) => Some(Err(error)),
        })
        .map(|row| {
            let row = row?;
            Ok((
                row,
                data.spectral_window_id(row)?,
                data.polarization_id(row)?,
            ))
        })
        .collect::<MsResult<Vec<_>>>()?;
    for term in text.split(';').filter(|term| !term.trim().is_empty()) {
        let (window, correlation) = term.split_once(':').unwrap_or(("*", term));
        let codes = super::parser::parse_correlation_selector(correlation)?;
        let spectral_windows = ms.resolve_spw_selector(window)?;
        let mut matches = Vec::new();
        for pol_id in 0..polarization.row_count() {
            let available = polarization.corr_type(pol_id)?;
            if !codes
                .iter()
                .all(|code| available.iter().any(|value| value == code))
            {
                continue;
            }
            for &window in spectral_windows.spw_ids() {
                if let Some(&(row, _, _)) = descriptions
                    .iter()
                    .find(|&&(_, spw, pol)| spw == window as i32 && pol == pol_id as i32)
                {
                    matches.push(row as i32);
                }
            }
        }
        if matches.is_empty() {
            return Err(MsError::VersionError(format!(
                "Poln. expression error: No match for polarization ID(s) (named {correlation})"
            )));
        }
        result.extend(matches.into_iter().filter(|&id| seen.insert(id)));
    }
    Ok(result)
}
