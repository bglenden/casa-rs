// SPDX-License-Identifier: LGPL-3.0-or-later

//! The selection a request names, resolved against its MeasurementSet: the
//! data descriptions, spectral windows and channels, and one pass over the
//! selected MAIN rows for the facts the later stages need.

use std::collections::{BTreeMap, BTreeSet};

use casa_imaging_model::{
    CorrelationProduct, CorrelationSelection, CorrelationType, ObservationSelection,
    SelectedMainRow, SelectedRows, SelectedRowsBuilder, SpectralWindowSelection,
    VisibilityColumn as OwnerVisibilityColumn,
};
use casa_ms::{
    MeasurementSet, SelectedObservationContentBudget, SelectedObservationRow,
    SelectedObservationRowSelection, SelectedObservationSpectralEnvelope,
    SelectedObservationSpectralEnvelopeReducer, SelectedObservationSpectralWindow,
    VisibilityDataColumn, parse_spw_selector, resolve_channel_selector_selection,
};
use casa_types::measures::frequency::FrequencyRef;

use super::PrepareError;
use crate::{DataColumn, ImagingRequest, SpecMode};

/// One selected spectral window's frequency axis.
pub(super) struct SourceSpectralWindow {
    pub(super) spw_id: usize,
    pub(super) frequency_reference: FrequencyRef,
    pub(super) frequencies_hz: Vec<f64>,
    pub(super) channel_widths_hz: Vec<f64>,
}

/// The `|w|` envelope of the selected rows, in metres.
#[derive(Clone, Copy)]
pub(super) struct WRange {
    pub(super) maximum_abs_m: f64,
    pub(super) minimum_abs_m: f64,
    pub(super) sum_squares_m2: f64,
    pub(super) rows: u64,
}

/// Selected source channels per spectral window id.
pub(super) type WindowChannels = BTreeMap<usize, Vec<usize>>;

/// What one pass over the selected rows establishes.
pub(super) struct Survey {
    pub(super) row_selection: SelectedObservationRowSelection,
    pub(super) rows: SelectedRows,
    /// `(spectral window, polarization)` of each selected data description.
    pub(super) bindings: Vec<(usize, usize)>,
    /// The selected spectral windows, in one source frame.
    pub(super) spectral_windows: Vec<SourceSpectralWindow>,
    pub(super) source_frequency_reference: FrequencyRef,
    pub(super) fields: BTreeSet<i32>,
    pub(super) observation_ids: BTreeSet<i32>,
    pub(super) first_time_mjd_seconds: f64,
    pub(super) time_bounds_mjd_seconds: [f64; 2],
    /// The first unflagged cross-correlation row (native AW reads its
    /// parallactic angle).
    pub(super) first_cross_row: Option<SelectedObservationRow>,
    pub(super) w: WRange,
    pub(super) weight_spectrum_complete: bool,
    /// A continuum run's selected channels per window and their envelope.
    pub(super) continuum: Option<(WindowChannels, SelectedObservationSpectralEnvelope)>,
}

/// Per-row facts gathered by the pass.
struct RowFacts {
    rows: SelectedRowsBuilder,
    rows_error: Option<casa_imaging_model::SelectedRowSequenceError>,
    ddids: BTreeSet<i32>,
    fields: BTreeSet<i32>,
    observation_ids: BTreeSet<i32>,
    first_time_mjd_seconds: Option<f64>,
    time_bounds_mjd_seconds: [f64; 2],
    first_cross_row: Option<SelectedObservationRow>,
    w: WRange,
}

impl RowFacts {
    /// Nothing observed yet, over `row_count` MAIN rows and `ddids`
    /// candidate data descriptions.
    fn new(row_count: usize, ddids: usize) -> Self {
        Self {
            rows: SelectedRowsBuilder::with_data_description_capacity(
                u64::try_from(row_count).expect("a MAIN row count fits u64"),
                ddids,
            ),
            rows_error: None,
            ddids: BTreeSet::new(),
            fields: BTreeSet::new(),
            observation_ids: BTreeSet::new(),
            first_time_mjd_seconds: None,
            time_bounds_mjd_seconds: [f64::INFINITY, f64::NEG_INFINITY],
            first_cross_row: None,
            w: WRange {
                maximum_abs_m: 0.0,
                minimum_abs_m: f64::INFINITY,
                sum_squares_m2: 0.0,
                rows: 0,
            },
        }
    }

    fn observe(&mut self, row: SelectedObservationRow) {
        self.ddids.insert(row.data_description_id());
        self.fields.insert(row.field_id());
        self.observation_ids.insert(row.observation_id());
        self.first_time_mjd_seconds
            .get_or_insert(row.time_mjd_seconds());
        if !row.flag_row() && row.antenna1() != row.antenna2() {
            self.first_cross_row.get_or_insert(row);
        }
        self.time_bounds_mjd_seconds[0] =
            self.time_bounds_mjd_seconds[0].min(row.time_mjd_seconds());
        self.time_bounds_mjd_seconds[1] =
            self.time_bounds_mjd_seconds[1].max(row.time_mjd_seconds());
        let [_, _, w_m] = row.uvw_m();
        self.w.maximum_abs_m = self.w.maximum_abs_m.max(w_m.abs());
        self.w.minimum_abs_m = self.w.minimum_abs_m.min(w_m.abs());
        self.w.sum_squares_m2 += w_m * w_m;
        self.w.rows += 1;
        if self.rows_error.is_none() {
            self.rows_error = self
                .rows
                .push(SelectedMainRow::new(
                    u64::try_from(row.physical_row()).expect("row bounded by MS row count"),
                    u32::try_from(row.data_description_id()).expect("validated nonnegative DDID"),
                ))
                .err();
        }
    }
}

/// Resolve the request's selection and survey its rows in one pass.
pub(super) fn survey(
    request: &ImagingRequest,
    ms: &MeasurementSet,
    budget: SelectedObservationContentBudget,
    frame_engine: &casa_ms::derived::engine::MsCalEngine,
) -> Result<Survey, PrepareError> {
    let data_description = ms.data_description()?;
    let ddids = selected_data_descriptions(request, &data_description)?;
    let row_selection = ms.selected_observation_row_selection(
        &ddids,
        request.field.as_deref(),
        request.uvrange.as_deref(),
        request.intent.as_deref(),
    )?;
    let mut spectral_windows = candidate_windows(ms, &data_description, &ddids)?;
    let mut continuum = continuum_envelope(
        request,
        ms,
        &row_selection,
        &spectral_windows,
        frame_engine,
        budget,
    )?;
    let mut io = budget.row_io_budget();
    io.available_bytes = io
        .available_bytes
        .checked_sub(
            continuum
                .as_ref()
                .map_or(0, |(_, reducer)| reducer.retained_bytes()),
        )
        .ok_or(PrepareError::SpectralEnvelopeBudget)?;
    let mut facts = RowFacts::new(ms.row_count(), ddids.len());
    let main_table = ms.main_table();
    let mut weight_spectrum_complete = main_table.column_accessor("WEIGHT_SPECTRUM").is_ok();
    let mut row_error = None;
    ms.visit_selected_observation_rows(&row_selection, io, |row| {
        if row_error.is_none()
            && let Some((_, reducer)) = continuum.as_mut()
        {
            row_error = reducer.observe(row).err();
        }
        if weight_spectrum_complete {
            match main_table
                .column_accessor("WEIGHT_SPECTRUM")
                .and_then(|column| column.array_cell_is_defined_uncached(row.physical_row()))
            {
                Ok(defined) => weight_spectrum_complete = defined,
                Err(error) => row_error = Some(error.into()),
            }
        }
        facts.observe(row);
    })?;
    if let Some(error) = facts.rows_error {
        return Err(error.into());
    }
    if let Some(error) = row_error {
        return Err(error.into());
    }
    let rows = facts.rows.finish();
    if rows.selected_row_count() == 0 {
        return Err(PrepareError::NoSelectedRows);
    }
    let bindings = facts
        .ddids
        .iter()
        .map(|ddid| data_description_binding(&data_description, *ddid))
        .collect::<Result<Vec<_>, _>>()?;
    let spw_ids = bindings
        .iter()
        .map(|(spw_id, _)| *spw_id)
        .collect::<BTreeSet<_>>();
    spectral_windows.retain(|window| spw_ids.contains(&window.spw_id));
    let source_frequency_reference = one_source_frame(&spectral_windows)?;
    let continuum = continuum
        .map(|(mut selected, reducer)| {
            selected.retain(|spw_id, _| spw_ids.contains(spw_id));
            reducer.finish().map(|envelope| (selected, envelope))
        })
        .transpose()?;
    Ok(Survey {
        row_selection,
        rows,
        bindings,
        spectral_windows,
        source_frequency_reference,
        fields: facts.fields,
        observation_ids: facts.observation_ids,
        first_time_mjd_seconds: facts
            .first_time_mjd_seconds
            .expect("nonempty selected row traversal"),
        time_bounds_mjd_seconds: facts.time_bounds_mjd_seconds,
        first_cross_row: facts.first_cross_row,
        w: facts.w,
        weight_spectrum_complete,
        continuum,
    })
}

/// A continuum run's selected channels per candidate window, with the
/// reducer of their LSRK envelope that the row pass feeds; `None` for a
/// cube.
fn continuum_envelope<'a>(
    request: &ImagingRequest,
    ms: &'a MeasurementSet,
    row_selection: &'a SelectedObservationRowSelection,
    spectral_windows: &[SourceSpectralWindow],
    frame_engine: &'a casa_ms::derived::engine::MsCalEngine,
    budget: SelectedObservationContentBudget,
) -> Result<
    Option<(
        WindowChannels,
        SelectedObservationSpectralEnvelopeReducer<'a>,
    )>,
    PrepareError,
> {
    if request.specmode != SpecMode::Mfs {
        return Ok(None);
    }
    let selected = spectral_windows
        .iter()
        .map(|window| {
            Ok((
                window.spw_id,
                selected_channels(request, window.spw_id, &window.frequencies_hz)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, PrepareError>>()?;
    let reducer = ms.selected_observation_spectral_envelope_reducer(
        row_selection,
        spectral_windows.iter().map(|window| {
            SelectedObservationSpectralWindow::borrow_selected(
                u32::try_from(window.spw_id).expect("nonnegative i32 SPW fits u32"),
                window.frequency_reference,
                &window.frequencies_hz,
                &window.channel_widths_hz,
                selected
                    .get(&window.spw_id)
                    .expect("selected every candidate spectral window"),
            )
        }),
        FrequencyRef::LSRK,
        frame_engine,
        budget.available_bytes(),
    )?;
    Ok(Some((selected, reducer)))
}

/// The spectral windows of the candidate data descriptions.
fn candidate_windows(
    ms: &MeasurementSet,
    data_description: &casa_ms::MsDataDescription<'_>,
    ddids: &[i32],
) -> Result<Vec<SourceSpectralWindow>, PrepareError> {
    let spectral_window = ms.spectral_window()?;
    let spw_ids = ddids
        .iter()
        .map(|ddid| data_description_binding(data_description, *ddid).map(|(spw_id, _)| spw_id))
        .collect::<Result<BTreeSet<_>, _>>()?;
    spw_ids
        .into_iter()
        .map(|spw_id| {
            let code = spectral_window.meas_freq_ref(spw_id)?;
            Ok(SourceSpectralWindow {
                spw_id,
                frequency_reference: FrequencyRef::from_casacore_code(code)
                    .ok_or(PrepareError::UnknownSourceFrame { spw_id, code })?,
                frequencies_hz: spectral_window.chan_freq(spw_id)?,
                channel_widths_hz: spectral_window.chan_width(spw_id)?,
            })
        })
        .collect()
}

/// The one source frame every selected window shares.
fn one_source_frame(windows: &[SourceSpectralWindow]) -> Result<FrequencyRef, PrepareError> {
    let mut frames = windows.iter().map(|window| window.frequency_reference);
    let first = frames
        .next()
        .expect("the selected rows' data descriptions bind a candidate spectral window");
    if frames.any(|frame| frame != first) {
        return Err(PrepareError::MixedSourceFrames);
    }
    Ok(first)
}

/// The one selected spectral window of a stage that images one; `stage`
/// names it in the error.
pub(super) fn one_window<'a>(
    windows: &'a [SourceSpectralWindow],
    stage: &'static str,
) -> Result<&'a SourceSpectralWindow, PrepareError> {
    match windows {
        [window] => Ok(window),
        _ => Err(PrepareError::OneSpectralWindow {
            stage,
            selected: windows.len(),
        }),
    }
}

fn selected_data_descriptions(
    request: &ImagingRequest,
    table: &casa_ms::MsDataDescription<'_>,
) -> Result<Vec<i32>, PrepareError> {
    if let Some(ddid) = request.ddid {
        return Ok(vec![ddid]);
    }
    let selected_spws = request
        .spw
        .as_deref()
        .map(parse_spw_selector)
        .transpose()?
        .unwrap_or_default()
        .into_iter()
        .map(|selector| selector.spw_id)
        .collect::<BTreeSet<_>>();
    let mut ddids = Vec::new();
    for row in 0..table.row_count() {
        let spw = table.spectral_window_id(row)?;
        let polarization = table.polarization_id(row)?;
        if spw >= 0
            && polarization >= 0
            && (selected_spws.is_empty() || selected_spws.contains(&spw))
        {
            ddids.push(i32::try_from(row).expect("DATA_DESCRIPTION rows are addressed by i32 ids"));
        }
    }
    if ddids.is_empty() {
        return Err(PrepareError::NoSelectedDataDescriptions);
    }
    Ok(ddids)
}

fn data_description_binding(
    table: &casa_ms::MsDataDescription<'_>,
    ddid: i32,
) -> Result<(usize, usize), PrepareError> {
    let row = usize::try_from(ddid).map_err(|_| PrepareError::InvalidDataDescription { ddid })?;
    let spw = table.spectral_window_id(row)?;
    let polarization = table.polarization_id(row)?;
    match (usize::try_from(spw), usize::try_from(polarization)) {
        (Ok(spw), Ok(polarization)) => Ok((spw, polarization)),
        _ => Err(PrepareError::InvalidDataDescription { ddid }),
    }
}

/// The channels of `spw_id` the request selects: those its `spw`
/// selector names, else `channel_start` and `channel_count`.
pub(super) fn selected_channels(
    request: &ImagingRequest,
    spw_id: usize,
    frequencies: &[f64],
) -> Result<Vec<usize>, PrepareError> {
    if let Some(channels) = explicit_spw_channels(request, spw_id, frequencies)? {
        return Ok(channels);
    }
    let start = request.channel_start.unwrap_or(0);
    let count = request
        .channel_count
        .unwrap_or_else(|| frequencies.len().saturating_sub(start));
    match start.checked_add(count) {
        Some(end) if count > 0 && end <= frequencies.len() => Ok((start..end).collect()),
        _ => Err(PrepareError::ChannelRange {
            spw_id,
            start,
            count,
            channels: frequencies.len(),
        }),
    }
}

/// The channels of `spw_id` the request's `spw` selector names, if it
/// names any.
pub(super) fn explicit_spw_channels(
    request: &ImagingRequest,
    spw_id: usize,
    frequencies: &[f64],
) -> Result<Option<Vec<usize>>, PrepareError> {
    let Some(text) = request.spw.as_deref() else {
        return Ok(None);
    };
    let Some(selector) = parse_spw_selector(text)?
        .into_iter()
        .find(|selector| usize::try_from(selector.spw_id).ok() == Some(spw_id))
        .and_then(|selector| selector.channels)
    else {
        return Ok(None);
    };
    Ok(Some(
        resolve_channel_selector_selection(frequencies, &selector)?.indices,
    ))
}

/// The observation selection: the surveyed rows, `channels` per window and
/// every correlation of the selected polarization setups.
pub(super) fn observation_selection(
    ms: &MeasurementSet,
    survey: Survey,
    channels: &WindowChannels,
) -> Result<ObservationSelection, PrepareError> {
    let polarization = ms.polarization()?;
    let spectral_windows = channels
        .iter()
        .map(|(spw_id, channels)| {
            SpectralWindowSelection::new(
                u32::try_from(*spw_id).expect("SPW ids are nonnegative stored i32 values"),
                channels
                    .iter()
                    .map(|channel| u32::try_from(*channel).expect("a CHAN_FREQ index fits u32"))
                    .collect(),
            )
        })
        .collect::<Vec<_>>();
    let correlations = survey
        .bindings
        .iter()
        .map(|(_, polarization_id)| *polarization_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|polarization_id| {
            let correlations = polarization
                .corr_type(polarization_id)?
                .iter()
                .enumerate()
                .map(|(index, code)| {
                    Ok(CorrelationProduct::new(
                        u32::try_from(index).expect("a CORR_TYPE index fits u32"),
                        correlation_type(*code)?,
                    ))
                })
                .collect::<Result<Vec<_>, PrepareError>>()?;
            Ok(CorrelationSelection::new(
                u32::try_from(polarization_id)
                    .expect("polarization ids are nonnegative stored i32 values"),
                correlations,
            ))
        })
        .collect::<Result<Vec<_>, PrepareError>>()?;
    Ok(ObservationSelection::new(
        survey.rows,
        survey.row_selection.rows().clone(),
        survey.row_selection.data_descriptions().to_vec(),
        spectral_windows,
        correlations,
    ))
}

/// The visibility column imaged: the requested one, else `CORRECTED_DATA`
/// when present, else `DATA`.
pub(super) fn visibility_column(
    ms: &MeasurementSet,
    requested: Option<DataColumn>,
) -> Result<OwnerVisibilityColumn, PrepareError> {
    Ok(match requested {
        Some(DataColumn::Data) => OwnerVisibilityColumn::Data,
        Some(DataColumn::Corrected) => OwnerVisibilityColumn::CorrectedData,
        None if ms.data_column(VisibilityDataColumn::CorrectedData).is_ok() => {
            OwnerVisibilityColumn::CorrectedData
        }
        None if ms.data_column(VisibilityDataColumn::Data).is_ok() => OwnerVisibilityColumn::Data,
        None => return Err(PrepareError::NoVisibilityColumn),
    })
}

fn correlation_type(code: i32) -> Result<CorrelationType, PrepareError> {
    use CorrelationType::*;
    Ok(match code {
        1 => StokesI,
        2 => StokesQ,
        3 => StokesU,
        4 => StokesV,
        5 => CircularRr,
        6 => CircularRl,
        7 => CircularLr,
        8 => CircularLl,
        9 => LinearXx,
        10 => LinearXy,
        11 => LinearYx,
        12 => LinearYy,
        13 => MixedRx,
        14 => MixedRy,
        15 => MixedLx,
        16 => MixedLy,
        17 => MixedXr,
        18 => MixedXl,
        19 => MixedYr,
        20 => MixedYl,
        21 => QuasiOrthogonalPp,
        22 => QuasiOrthogonalPq,
        23 => QuasiOrthogonalQp,
        24 => QuasiOrthogonalQq,
        25 => RightCircular,
        26 => LeftCircular,
        27 => Linear,
        28 => PolarizedIntensity,
        29 => LinearPolarizedIntensity,
        30 => FractionalPolarizedIntensity,
        31 => FractionalLinearPolarizedIntensity,
        32 => PolarizationAngle,
        _ => return Err(PrepareError::UnsupportedCorrelation { code }),
    })
}

#[cfg(test)]
mod tests {
    use casa_ms::MeasurementSetBuilder;
    use serde_json::json;

    use super::*;
    use crate::request::tests::request;

    #[test]
    fn channels_past_the_window_are_refused() {
        let request = request(json!({ "channel_start": 1, "channel_count": 2 }));
        assert!(matches!(
            selected_channels(&request, 0, &[1.0e9, 1.1e9]),
            Err(PrepareError::ChannelRange {
                spw_id: 0,
                start: 1,
                count: 2,
                channels: 2,
            })
        ));
    }

    #[test]
    fn a_measurement_set_without_visibilities_has_no_column_to_image() {
        let ms = MeasurementSet::create_memory(MeasurementSetBuilder::new())
            .expect("a MeasurementSet with neither DATA nor CORRECTED_DATA");
        assert!(matches!(
            visibility_column(&ms, None),
            Err(PrepareError::NoVisibilityColumn)
        ));
    }
}
