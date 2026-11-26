// SPDX-License-Identifier: LGPL-3.0-or-later
//! Publish CASA's virtual source recipes or their sampled scratch visibilities.

pub mod image;
mod imager;
mod imager_definition;
mod imager_sources;
pub(crate) mod legacy_aw;
pub(crate) mod legacy_pbw;
pub(crate) mod moving_source;
pub(crate) mod predictcomp;
pub mod setjy;
pub(crate) mod settings;
pub(crate) mod single_dish;
pub mod virtual_publication;
mod voltage_pattern;

pub use imager::{ImagerImageDefinition, ImagerPrediction, ImagerPredictionModel};
pub use imager_definition::{ImagerDefinitionRequest, ImagerPhaseCentre, ImagerSpectralAxis};
pub use voltage_pattern::ImagerVoltagePattern;
pub(crate) use voltage_pattern::{ComponentVoltageCache, PreparedVoltagePattern};

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use casa_coordinates::SpectralCoordinate;
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::virtual_model::selected_model_combinations;
use casa_ms::write_session::{
    MeasurementSetMutationBatch, MeasurementSetWriteError, MeasurementSetWriteSession,
};
use casa_ms::{
    MeasurementSet, MsError, MsResult, ResolvedMsSelection, ResolvedMsSelectionRow,
    VirtualModelEntry,
};
use casa_tables::TableWriteLock;
use casa_types::{ArrayValue, Complex32};
use ndarray::{ArrayD, Axis, IxDyn};

use crate::model_columns::{
    ScratchDestination, ScratchWriteMode, observed_column, scratch_write_plan,
};
use crate::solve::grouping::VisibilityModels;

const MODEL: VisibilityDataColumn = VisibilityDataColumn::ModelData;

/// Restore the measures provider after reading a saved conversion frame.
/// The CASA record carries the epoch, direction, and observatory only.
pub(crate) fn spectral_with_runtime(
    spectral: &SpectralCoordinate,
) -> MsResult<Cow<'_, SpectralCoordinate>> {
    match (
        spectral.conversion_frequency_ref(),
        spectral.conversion_frame(),
    ) {
        (Some(target), Some(frame)) if target != spectral.frequency_ref() => {
            let measures = casa_ms::open_measures_runtime().map_err(invalid)?;
            Ok(Cow::Owned(spectral.clone().with_conversion(
                target,
                frame.clone().with_measures(measures),
            )))
        }
        _ => Ok(Cow::Borrowed(spectral)),
    }
}

pub fn model_entry(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    kind: casa_ms::VirtualModelKind,
    container: casa_types::RecordValue,
) -> MsResult<VirtualModelEntry> {
    let windows: BTreeSet<_> = selected
        .selected_rows
        .iter()
        .map(|row| row.spectral_window_id)
        .collect();
    let spectral_window = ms.spectral_window()?;
    let mut channel_selection = Vec::new();
    for spw in windows {
        match selected.channel_selections.get(&spw) {
            None => channel_selection.push([spw, 0, spectral_window.num_chan(spw as usize)?, 1]),
            Some(selection) => {
                // The old VI uses one stride for a single range, and the
                // inclusive envelope when the request contains several.
                let (span, step) = channel_span(selection);
                let count = span.len().div_ceil(step);
                channel_selection.push([spw, span.start as i32, count as i32, step as i32]);
            }
        }
    }
    Ok(VirtualModelEntry {
        kind,
        index_combination: selected_model_combinations(
            ms,
            &selected.row_indices().collect::<Vec<_>>(),
        )?
        .index_combination,
        channel_selection,
        container,
    })
}

pub fn write_model_scratch(
    ms: &mut MeasurementSet,
    selected: &ResolvedMsSelection,
    entries: &[VirtualModelEntry],
    incremental: bool,
    clear_virtual: bool,
    prepared: Option<VisibilityModels>,
    lock: &mut TableWriteLock,
) -> MsResult<()> {
    if entries.is_empty() || selected.selected_rows.is_empty() {
        return Ok(());
    }
    let models = match prepared {
        Some(models) => models,
        None => {
            VisibilityModels::from_entries(ms, &selected.selected_rows, entries).map_err(invalid)?
        }
    };
    let polarizations = ms.polarization()?;
    let correlations = (0..polarizations.row_count())
        .map(|id| Ok((id as i32, polarizations.corr_type(id)?)))
        .collect::<MsResult<_>>()?;
    let (plan, destinations) = scratch_write_plan(
        ms,
        selected.row_indices().collect(),
        &[MODEL],
        observed_column(ms),
        ScratchWriteMode::Replace,
        clear_virtual,
        lock,
    )?;
    let batch_rows = plan.batch_rows().max(1);
    let destination = &destinations[0];
    let channels: BTreeMap<_, _> = selected
        .channel_selections
        .iter()
        .map(|(&spw, selection)| (spw, channel_span(selection)))
        .collect();
    let prediction = Prediction {
        models,
        destination,
        channels,
        correlations,
        incremental,
    };
    MeasurementSetWriteSession::mutate_selected_rows_with_lock(ms, plan, lock, |session, ms| {
        for rows in selected.selected_rows.chunks(batch_rows) {
            let batch = prediction
                .batch(ms, rows)
                .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
            session.write_mutation_batch(ms, batch)?;
        }
        Ok(())
    })
    .map_err(invalid)?;
    Ok(())
}

fn prepare_mosaic_models(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    entries: &[VirtualModelEntry],
    scratch: bool,
) -> MsResult<VisibilityModels> {
    let models =
        VisibilityModels::from_entries(ms, &selected.selected_rows, entries).map_err(invalid)?;
    let polarizations = ms.polarization()?;
    let flags = ms.main_table().column_accessor("FLAG")?;
    let mut layouts = BTreeSet::new();
    for row in &selected.selected_rows {
        if layouts.insert((row.field_id, row.data_desc_id)) {
            let correlations = polarizations.corr_type(row.polarization_id as usize)?;
            let Some(ArrayValue::Bool(flags)) = flags
                .array_cells_owned_uncached(&[row.row_index])?
                .pop()
                .flatten()
            else {
                return Err(invalid("FLAG is not a defined Bool array"));
            };
            if scratch {
                models.predict_scratch(ms, row.row_index, &correlations, &flags, None, None)
            } else {
                models.predict(ms, row.row_index, &correlations, &flags)
            }
            .map_err(invalid)?;
        }
    }
    Ok(models)
}

struct Prediction<'a> {
    models: VisibilityModels,
    destination: &'a ScratchDestination<'a>,
    channels: BTreeMap<i32, (Range<usize>, usize)>,
    correlations: BTreeMap<i32, Vec<i32>>,
    incremental: bool,
}

impl Prediction<'_> {
    fn batch(
        &self,
        ms: &MeasurementSet,
        rows: &[ResolvedMsSelectionRow],
    ) -> MsResult<MeasurementSetMutationBatch> {
        let row_indices: Vec<_> = rows.iter().map(|row| row.row_index).collect();
        let main = ms.main_table();
        // CASA can store UVW in IncrementalStMan, where a single-cell read
        // decodes its whole storage bucket. Read each selected batch together.
        let uvw = main
            .column_accessor("UVW")?
            .array_cells_owned_uncached(&row_indices)?;
        let mut existing = (self.incremental
            || rows
                .iter()
                .any(|row| self.channels.contains_key(&row.spectral_window_id)))
        .then(|| {
            main.column_accessor(MODEL.name())?
                .array_cells_owned_uncached(&row_indices)
        })
        .transpose()?;
        let flags = rows
            .iter()
            .any(|row| self.models.uses_fourier(row.field_id))
            .then(|| {
                main.column_accessor("FLAG")?
                    .array_cells_owned_uncached(&row_indices)
            })
            .transpose()?;
        let mut ranges = BTreeMap::new();
        let mut values = Vec::with_capacity(rows.len());
        for (offset, row) in rows.iter().enumerate() {
            let correlation = &self.correlations[&row.polarization_id];
            let no_flags = ArrayD::from_elem(IxDyn(&[0, 0]), false);
            let flags = match flags.as_ref().and_then(|flags| flags[offset].as_ref()) {
                Some(ArrayValue::Bool(flags)) => flags,
                None => &no_flags,
                _ => return Err(invalid("FLAG is not a Bool array")),
            };
            let slice = self.channels.get(&row.spectral_window_id);
            let previous = if self.incremental || slice.is_some() {
                let Some(ArrayValue::Complex32(previous)) =
                    existing.as_mut().and_then(|cells| cells[offset].take())
                else {
                    return Err(invalid("MODEL_DATA is not a defined Complex array"));
                };
                Some(previous)
            } else {
                None
            };
            let starting = if self.incremental {
                previous.clone()
            } else {
                None
            };
            let sampled = self
                .models
                .predict_scratch(
                    ms,
                    row.row_index,
                    correlation,
                    flags,
                    starting,
                    uvw[offset].as_ref(),
                )
                .map_err(invalid)?
                .ok_or_else(|| invalid("the selected row is absent from the prediction"))?;
            let value = if let Some((span, step)) = slice {
                let mut previous = previous.expect("selected channels retain previous cells");
                ranges.insert(row.spectral_window_id, span.clone());
                update_channels(&mut previous, &sampled, span, *step)?;
                previous
            } else {
                sampled
            };
            values.push(ArrayValue::Complex32(value));
        }
        Ok(MeasurementSetMutationBatch {
            columns: self
                .destination
                .batch(ms, &row_indices, rows, &ranges, values)?,
            row_indices,
        })
    }
}

pub(super) fn channel_span(selection: &casa_ms::ChannelSelection) -> (Range<usize>, usize) {
    let first = selection
        .segments
        .iter()
        .map(|part| part.start.min(part.end))
        .min()
        .unwrap_or(0);
    let last = selection
        .segments
        .iter()
        .map(|part| {
            let lower = part.start.min(part.end);
            let upper = part.start.max(part.end);
            let stride = part.stride as i32;
            lower + (upper - lower) / stride * stride
        })
        .max()
        .unwrap_or(-1);
    let step = selection
        .segments
        .first()
        .map_or(1, |segment| segment.stride);
    if last < first {
        return (0..0, step);
    }
    // The old iterator spans the outer selected samples at the first
    // channel range's stride, rounding the count up over the envelope.
    let end = first as usize + ((last - first) as usize).div_ceil(step) * step + 1;
    (first as usize..end, step)
}

fn update_channels(
    previous: &mut ArrayD<Complex32>,
    current: &ArrayD<Complex32>,
    span: &Range<usize>,
    stride: usize,
) -> MsResult<()> {
    if previous.shape() != current.shape() || span.end > current.shape()[1] {
        return Err(invalid(
            "visibility channel selection does not fit MODEL_DATA",
        ));
    }
    for channel in span.clone().step_by(stride) {
        let mut destination = previous.index_axis_mut(Axis(1), channel);
        let source = current.index_axis(Axis(1), channel);
        destination.assign(&source);
    }
    Ok(())
}

fn invalid(error: impl std::fmt::Display) -> MsError {
    MsError::InvalidInput(error.to_string())
}
