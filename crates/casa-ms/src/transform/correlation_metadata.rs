// SPDX-License-Identifier: LGPL-3.0-or-later
//! Identify the selected correlation axes in each polarization setup.

use std::collections::{BTreeMap, BTreeSet};

use casa_tables::TableError;
use casa_types::{ArrayValue, Correlation, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, Axis};

use crate::selection::MsSelection;
use crate::selection::syntax::parse_numeric_id_selector;
use crate::{MeasurementSet, MsError, MsResult, SubtableId};

use super::{MsTransformError, MsTransformRequest, rewrite_table_rows_with_variable_arrays};

#[derive(Clone)]
pub(super) struct CorrelationSelection {
    by_polarization: Vec<Vec<usize>>,
    by_ddid: BTreeMap<i32, usize>,
}

impl CorrelationSelection {
    pub(super) fn resolve(ms: &MeasurementSet, text: &str) -> MsResult<Self> {
        let pol = ms.polarization()?;
        let dd = ms.data_description()?;
        let pol_codes = (0..pol.row_count())
            .map(|row| pol.corr_type(row))
            .collect::<MsResult<Vec<_>>>()?;
        let mut descriptions = BTreeMap::new();
        for row in 0..dd.row_count() {
            let id = dd.polarization_id(row)?;
            let spw = dd.spectral_window_id(row)?;
            if !dd.flag_row(row)? && id >= 0 && spw >= 0 {
                descriptions
                    .entry((id as usize, spw as usize))
                    .or_insert(row as i32);
            }
        }
        let mut by_polarization = vec![Vec::new(); pol.row_count()];
        let mut by_ddid = BTreeMap::new();
        for term in selector_tokens(text, ';') {
            let mut fields = selector_tokens(term, ':');
            let first = fields.next().ok_or_else(|| no_match(term))?;
            let (windows, labels) = fields.next().map_or(("*", first), |labels| (first, labels));
            if fields.next().is_some() {
                return Err(MsError::InvalidInput(
                    "Poln. expression error: Too many ':'s. Channel specification is not allowed."
                        .to_string(),
                ));
            }
            let codes = selector_tokens(labels, ',')
                .map(|label| {
                    Correlation::ALL
                        .iter()
                        .find(|corr| corr.label().eq_ignore_ascii_case(label))
                        .map(|corr| corr.casacore_code())
                        .ok_or_else(|| no_match(labels))
                })
                .collect::<MsResult<Vec<_>>>()?;
            if codes.is_empty() {
                return Err(no_match(labels));
            }
            let windows = ms.resolve_spw_selector(windows)?;
            let selected_windows = windows.spw_ids().iter().copied().collect::<BTreeSet<_>>();
            let mut found = false;
            for (id, stored) in pol_codes.iter().enumerate() {
                let axes = codes
                    .iter()
                    .map(|code| stored.iter().position(|candidate| candidate == code))
                    .collect::<Option<Vec<_>>>();
                let Some(axes) = axes else { continue };
                for (&(_, spw), &ddid) in descriptions.range((id, 0)..=(id, usize::MAX)) {
                    if selected_windows.contains(&spw) {
                        by_ddid.insert(ddid, id);
                        by_polarization[id].extend(axes.iter().copied());
                        found = true;
                    }
                }
            }
            if !found {
                return Err(no_match(labels));
            }
        }
        if by_ddid.is_empty() {
            return Err(no_match(text));
        }
        // The arrays and their polarization metadata must use the same
        // order, including when a selector lists the products in reverse.
        for axes in &mut by_polarization {
            axes.sort_unstable();
            axes.dedup();
        }
        Ok(Self {
            by_polarization,
            by_ddid,
        })
    }

    pub(super) fn constrain(&self, selection: &mut MsSelection) -> MsResult<bool> {
        let explicit = selection
            .data_description
            .take()
            .map(|text| parse_numeric_id_selector(&text, "data-description"))
            .transpose()?;
        let ids = self
            .by_ddid
            .keys()
            .copied()
            .filter(|ddid| explicit.as_ref().is_none_or(|ids| ids.contains(ddid)))
            .map(|ddid| ddid.to_string())
            .collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(false);
        }
        selection.data_description = Some(ids.join(","));
        Ok(true)
    }

    pub(super) fn axes(&self, ddid: i32) -> Option<&[usize]> {
        self.by_ddid
            .get(&ddid)
            .map(|&pol| self.by_polarization[pol].as_slice())
    }

    pub(super) fn write_polarizations(
        &self,
        input: &MeasurementSet,
        request: &MsTransformRequest,
    ) -> Result<(), MsTransformError> {
        let name = SubtableId::Polarization.name();
        let rows = (0..self.by_polarization.len()).collect::<Vec<_>>();
        rewrite_table_rows_with_variable_arrays(
            super::subtable_copy::input_subtable_path(input, SubtableId::Polarization)?,
            &request.output_ms.join(name),
            &rows,
            &["CORR_TYPE", "CORR_PRODUCT"],
            |row, metadata| {
                self.rewrite_polarization(row, metadata).map_err(|source| {
                    MsTransformError::MutateMeasurementSet {
                        path: request.output_ms.join(name).display().to_string(),
                        source: Box::new(source),
                    }
                })
            },
        )
    }

    fn rewrite_polarization(
        &self,
        row: usize,
        mut metadata: RecordValue,
    ) -> Result<RecordValue, TableError> {
        let axes = &self.by_polarization[row];
        for (name, axis, rank) in [("CORR_TYPE", Axis(0), 1), ("CORR_PRODUCT", Axis(1), 2)] {
            let array = match metadata.get(name) {
                Some(Value::Array(ArrayValue::Int32(array))) if array.ndim() == rank => array,
                _ => {
                    return Err(TableError::Storage(format!(
                        "POLARIZATION row {row} has an invalid {name} array"
                    )));
                }
            };
            let selected = select_axis(array, axis, axes, name)?;
            metadata.upsert(name, Value::Array(ArrayValue::Int32(selected)));
        }
        metadata.upsert(
            "NUM_CORR",
            Value::Scalar(ScalarValue::Int32(axes.len() as i32)),
        );
        Ok(metadata)
    }
}

pub(super) fn parallel_hands(
    ms: &MeasurementSet,
    selection: Option<&CorrelationSelection>,
    descriptions: &[i32],
) -> MsResult<BTreeMap<i32, [usize; 2]>> {
    let pol = ms.polarization()?;
    let dd = ms.data_description()?;
    let descriptions = descriptions.iter().copied().collect::<BTreeSet<_>>();
    let mut hands = BTreeMap::new();
    for id in descriptions {
        let codes = pol.corr_type(dd.polarization_id(id as usize)? as usize)?;
        let selected = selection.and_then(|selection| selection.axes(id));
        let codes = if let Some(axes) = selected {
            axes.iter()
                .filter_map(|&axis| codes.get(axis).copied())
                .collect()
        } else {
            codes
        };
        let position = |linear: Correlation, circular: Correlation| {
            codes.iter().rposition(|&code| {
                code == linear.casacore_code() || code == circular.casacore_code()
            })
        };
        if let (Some(first), Some(second)) = (
            position(Correlation::XX, Correlation::RR),
            position(Correlation::YY, Correlation::LL),
        ) {
            hands.insert(id, [first, second]);
        }
    }
    Ok(hands)
}

fn no_match(labels: &str) -> MsError {
    MsError::InvalidInput(format!(
        "Poln. expression error: No match for polarization ID(s) (named {labels})"
    ))
}

fn selector_tokens(text: &str, separator: char) -> impl Iterator<Item = &str> {
    text.split(separator)
        .filter(|part| !part.is_empty())
        .enumerate()
        .map(|(index, part)| {
            if index == 0 {
                part
            } else {
                part.trim_matches(' ')
            }
        })
}

pub(super) fn select_visibility_axes(
    value: ArrayValue,
    axes: Option<&[usize]>,
    name: &str,
) -> Result<ArrayValue, TableError> {
    let Some(axes) = axes else { return Ok(value) };
    let axis = Axis(0);
    if let Some(&length) = value.shape().first()
        && axes.len() == length
        && axes.iter().copied().eq(0..length)
    {
        return Ok(value);
    }
    match value {
        ArrayValue::Complex32(array) => {
            select_axis(&array, axis, axes, name).map(ArrayValue::Complex32)
        }
        ArrayValue::Float32(array) => {
            select_axis(&array, axis, axes, name).map(ArrayValue::Float32)
        }
        ArrayValue::Bool(array) => select_axis(&array, axis, axes, name).map(ArrayValue::Bool),
        _ => Err(TableError::Storage(format!(
            "cannot select correlations in {name}"
        ))),
    }
}

fn select_axis<T: Copy>(
    array: &ArrayD<T>,
    axis: Axis,
    indices: &[usize],
    name: &str,
) -> Result<ArrayD<T>, TableError> {
    if let Some(&count) = array.shape().get(axis.index())
        && indices.iter().all(|&index| index < count)
    {
        Ok(array.select(axis, indices))
    } else {
        Err(TableError::Storage(format!(
            "selected correlation index is out of range for {name} with shape {:?}",
            array.shape()
        )))
    }
}
