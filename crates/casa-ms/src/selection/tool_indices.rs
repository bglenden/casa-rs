// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA MSSelection side records for the Python MeasurementSet tool.

use casa_coordinates::StokesType;
use casa_tables::taql::RecordExpressionType;

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
    /// Baselines in the order the antenna grammar produced them. A supplied
    /// antenna clause has two columns even when it names no pairs.
    pub baselines: Option<Vec<[i32; 2]>>,
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

/// The compiled row predicate of a MeasurementSet tool selection.
pub struct ParsedMsToolSelection {
    compiled: CompiledMsSelection,
    rows_per_block: usize,
    taql_is_row_predicate: bool,
    correlations: Vec<ToolCorrelationSelection>,
}

/// Stored visibility correlations accepted by a CASA polarization clause.
pub struct ToolCorrelationSelection {
    /// DATA_DESCRIPTION row to which this projection applies.
    pub description_id: i32,
    /// Indices in the stored correlation axis, in requested order.
    pub indices: Vec<usize>,
    /// The CASA types in the projected correlation axis.
    pub types: Vec<StokesType>,
}

impl ParsedMsToolSelection {
    /// Whether an expression constrains the rows of the supplied view.
    pub fn has_row_expression(&self) -> bool {
        !self.compiled.where_clauses().is_empty()
    }

    /// Take the in-row correlation projections from this selection.
    pub fn into_correlations(self) -> Vec<ToolCorrelationSelection> {
        self.correlations
    }

    /// Apply the predicate and return matching indices in the supplied view.
    ///
    /// # Errors
    /// A selection column or predicate cannot be evaluated.
    pub fn select_rows(&self, ms: &MeasurementSet) -> MsResult<Vec<usize>> {
        if !self.taql_is_row_predicate {
            return Err(MsError::InvalidInput(
                "TaQL row predicate is not a Boolean scalar".into(),
            ));
        }
        self.compiled
            .apply_internal(ms, Some(self.rows_per_block), false)
    }
}

impl MeasurementSet {
    /// Compile one CASA MS tool selection and update its auxiliary lists.
    /// An omitted antenna, field, window, or polarization clause retains
    /// its previous list. The compiled predicate describes only this request.
    ///
    /// # Errors
    /// A supplied selector cannot be parsed or names invalid metadata.
    pub fn parse_ms_tool_selection(
        &self,
        request: &MsSelection,
        indices: &mut MsSelectionIndices,
    ) -> MsResult<ParsedMsToolSelection> {
        let budget = default_selection_budget()?;
        let rows_per_block = rows_per_block_for_budget(budget)
            .map_err(|error| MsError::InvalidInput(error.to_string()))?;
        indices.update_antenna_field_and_spw(self, request)?;
        indices.scan.clear();
        if let Some(text) = nonempty(request.scan.as_deref()) {
            indices.scan = scan_indices(text, rows_per_block)?;
        }
        let mut rows = request.clone();
        rows.correlation = None;
        let mut correlations = Vec::new();
        if let Some(text) = nonempty(request.correlation.as_deref()) {
            correlations = polarization_projections(self, text)?;
            indices.poldd = correlations
                .iter()
                .map(|plan| plan.description_id)
                .collect();
            if !indices.poldd.is_empty() {
                rows.data_description = Some(int_list(&indices.poldd));
            }
        }
        let taql = rows.msselect.take();
        let taql_type = if let Some(text) = nonempty(taql.as_deref().map(str::trim)) {
            super::taql_selection::validate_predicate(
                self,
                &format!("SELECT * WHERE ({text})"),
                Some(rows_per_block),
            )?
        } else {
            RecordExpressionType::PossibleRowPredicate
        };
        let taql_is_row_predicate = taql_type == RecordExpressionType::PossibleRowPredicate;
        if (taql_type == RecordExpressionType::NonBoolean && rows.has_text_selection())
            || (!taql_is_row_predicate && rows.timerange.is_some())
        {
            return Err(MsError::InvalidInput(
                "TaQL expression cannot be combined with the other row selectors".into(),
            ));
        }
        rows.msselect = taql;
        let compiled =
            rows.compile_with_row_grammar(self, RowGrammar::Transform, Some(rows_per_block))?;
        indices.state.clone_from(&compiled.state_ids);
        indices.observation.clear();
        if let Some(text) = nonempty(request.observation.as_deref()) {
            indices.observation = observation_indices(text, self.observation()?.row_count())?;
        }
        Ok(ParsedMsToolSelection {
            compiled,
            rows_per_block,
            taql_is_row_predicate,
            correlations,
        })
    }
}

impl MsSelectionIndices {
    fn update_antenna_field_and_spw(
        &mut self,
        ms: &MeasurementSet,
        request: &MsSelection,
    ) -> MsResult<()> {
        if let Some(text) = nonempty(request.antenna.as_deref()) {
            self.antenna1.clear();
            self.antenna2.clear();
            self.baselines = Some(Vec::new());
            let AntennaSelectionIndices {
                first,
                second,
                baselines,
            } = ms.antenna_selection_indices(text)?;
            self.antenna1 = first;
            self.antenna2 = second;
            self.baselines = Some(baselines);
        }
        if let Some(text) = nonempty(request.field.as_deref()) {
            self.field.clear();
            self.field = ms.resolve_field_selector(text)?;
            if self.field.is_empty() {
                return Err(MsError::InvalidInput(format!(
                    "field selector {text:?} did not match any existing fields"
                )));
            }
        }
        if let Some(text) = nonempty(request.spw.as_deref()) {
            self.spw.clear();
            let spw = ms.resolve_spw_selector(text)?;
            self.spw = spw.spw_ids().iter().map(|&value| value as i32).collect();
            self.spwdd = spw
                .data_description_ids()
                .iter()
                .map(|&value| value as i32)
                .collect();
            self.channel = spw
                .channel_list()
                .iter()
                .map(|row| [row.spw_id as i32, row.start, row.stop, row.step.max(1)])
                .collect();
        }
        Ok(())
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
fn polarization_projections(
    ms: &MeasurementSet,
    text: &str,
) -> MsResult<Vec<ToolCorrelationSelection>> {
    let data = ms.data_description()?;
    let polarization = ms.polarization()?;
    let mut result = Vec::new();
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
    for term in text
        .split(';')
        .map(str::trim)
        .filter(|term| !term.is_empty())
    {
        let fields = term
            .split(':')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>();
        let (window, correlation) = match fields.as_slice() {
            [correlation] => ("*", *correlation),
            [window, correlation] => (*window, *correlation),
            _ => {
                return Err(MsError::InvalidInput(format!(
                    "invalid polarization term {term:?}"
                )));
            }
        };
        let codes = correlation
            .split(',')
            .map(str::trim)
            .filter(|code| !code.is_empty())
            .map(|code| {
                StokesType::from_name(code)
                    .filter(|stokes| *stokes != StokesType::Undefined)
                    .ok_or_else(|| MsError::InvalidInput(format!("invalid polarization {code:?}")))
            })
            .collect::<MsResult<Vec<_>>>()?;
        if codes.is_empty() {
            return Err(MsError::InvalidInput(
                "polarization selector was empty".into(),
            ));
        }
        let spectral_windows = ms.resolve_spw_selector(window)?;
        let mut matches = Vec::new();
        for pol_id in 0..polarization.row_count() {
            let available = polarization.corr_type(pol_id)?;
            let Some(positions) = codes
                .iter()
                .map(|code| available.iter().position(|value| *value == code.code()))
                .collect::<Option<Vec<_>>>()
            else {
                continue;
            };
            for &window in spectral_windows.spw_ids() {
                if let Some(&(row, _, _)) = descriptions
                    .iter()
                    .find(|&&(_, spw, pol)| spw == window as i32 && pol == pol_id as i32)
                {
                    matches.push((row as i32, positions.clone()));
                }
            }
        }
        if matches.is_empty() {
            return Err(MsError::VersionError(format!(
                "Poln. expression error: No match for polarization ID(s) (named {correlation})"
            )));
        }
        for (description_id, positions) in matches {
            append_correlations(&mut result, description_id, positions, &codes);
        }
    }
    Ok(result)
}

fn append_correlations(
    projections: &mut Vec<ToolCorrelationSelection>,
    description_id: i32,
    positions: Vec<usize>,
    codes: &[StokesType],
) {
    let index = projections
        .iter()
        .position(|plan| plan.description_id == description_id)
        .unwrap_or_else(|| {
            projections.push(ToolCorrelationSelection {
                description_id,
                indices: Vec::new(),
                types: Vec::new(),
            });
            projections.len() - 1
        });
    let plan = &mut projections[index];
    for (position, &code) in positions.into_iter().zip(codes) {
        if !plan.indices.contains(&position) {
            plan.indices.push(position);
            plan.types.push(code);
        }
    }
}
