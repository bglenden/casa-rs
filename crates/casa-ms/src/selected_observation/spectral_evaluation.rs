// SPDX-License-Identifier: LGPL-3.0-or-later
//! Storage-owned source and output-frame spectral evaluation.

use casa_imaging_model::{
    CompiledProblem, FrequencyFrame, SelectedInputWeightGroup, SelectedObservationRunChannel,
    SelectedObservationRunCorrelation, SelectedObservationRunRow, SelectedObservationSampleView,
    SelectedSpectralEvaluation, SelectedSpectralInterval, SpectralWindowSelection,
};

use casa_types::measures::{direction::MDirection, frame::MeasFrame, frequency::FrequencyRef};

use crate::{
    MeasurementSet, MsError, MsResult, MsSelectionIoBudget, SelectedObservationEphemeris,
    derived::engine::{MsCalEngine, raw_field_phase_direction},
    spectral_selection::{PreparedFrequencyFrameConversion, convert_frequency_to_frame_with_frame},
};

use super::{BoundObservationSourceError, SelectedObservationRowSelection};

/// Bounded metadata measurements from one selected spectral-range reduction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedObservationSpectralRangeMeasurements {
    selected_rows: u64,
    edge_evaluations: u64,
}

impl SelectedObservationSpectralRangeMeasurements {
    /// Return the selected MAIN rows observed by the metadata traversal.
    #[must_use]
    pub const fn selected_rows(self) -> u64 {
        self.selected_rows
    }

    /// Return the distinct consecutive TIME-by-DDID edge evaluations.
    #[must_use]
    pub const fn edge_evaluations(self) -> u64 {
        self.edge_evaluations
    }
}

/// Output-frame selected-channel edge extrema and reference-epoch axis bounds.
///
/// The selected range is reduced directly from bounded MAIN-row metadata and
/// never retains a row- or time-sized payload. The reference range evaluates
/// the same selected source edges at the output-axis epoch and direction.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedObservationSpectralRange {
    selected_edges_hz: [f64; 2],
    reference_edges_hz: [f64; 2],
    measurements: SelectedObservationSpectralRangeMeasurements,
}

impl SelectedObservationSpectralRange {
    /// Return the global lower and upper selected-channel edges in the output frame.
    #[must_use]
    pub const fn selected_edges_hz(self) -> [f64; 2] {
        self.selected_edges_hz
    }

    /// Return the selected-channel edges at the output-axis reference frame.
    #[must_use]
    pub const fn reference_edges_hz(self) -> [f64; 2] {
        self.reference_edges_hz
    }

    /// Return bounded metadata traversal measurements.
    #[must_use]
    pub const fn measurements(self) -> SelectedObservationSpectralRangeMeasurements {
        self.measurements
    }
}

#[derive(Debug, Clone, Copy)]
struct SelectedWindowEdges {
    spectral_window_id: u32,
    source_frequency_reference: FrequencyRef,
    lower_hz: f64,
    upper_hz: f64,
}

/// Borrowed native spectral-window coordinates used to plan an exact selected envelope.
#[derive(Debug, Clone, Copy)]
pub struct SelectedObservationSpectralWindow<'a> {
    spectral_window_id: u32,
    source_frequency_reference: FrequencyRef,
    frequencies_hz: &'a [f64],
    channel_widths_hz: &'a [f64],
    selected_channels: &'a [usize],
}

impl<'a> SelectedObservationSpectralWindow<'a> {
    /// Bind one selected native spectral window without copying its coordinate arrays.
    #[must_use]
    pub const fn borrow_selected(
        spectral_window_id: u32,
        source_frequency_reference: FrequencyRef,
        frequencies_hz: &'a [f64],
        channel_widths_hz: &'a [f64],
        selected_channels: &'a [usize],
    ) -> Self {
        Self {
            spectral_window_id,
            source_frequency_reference,
            frequencies_hz,
            channel_widths_hz,
            selected_channels,
        }
    }
}

fn selected_window_edges(
    window: SelectedObservationSpectralWindow<'_>,
) -> MsResult<SelectedWindowEdges> {
    if window.frequencies_hz.len() != window.channel_widths_hz.len() {
        return Err(MsError::InvalidInput(format!(
            "SPECTRAL_WINDOW_ID {} frequency/width lengths differ",
            window.spectral_window_id
        )));
    }
    let mut lower_hz = f64::INFINITY;
    let mut upper_hz = f64::NEG_INFINITY;
    for &channel in window.selected_channels {
        let frequency_hz = *window.frequencies_hz.get(channel).ok_or_else(|| {
            MsError::InvalidInput(format!(
                "selected channel {channel} is outside SPECTRAL_WINDOW_ID {}",
                window.spectral_window_id
            ))
        })?;
        let width_hz = window.channel_widths_hz[channel];
        if !(frequency_hz.is_finite() && width_hz.is_finite() && width_hz != 0.0) {
            return Err(MsError::InvalidInput(format!(
                "selected channel {channel} in SPECTRAL_WINDOW_ID {} has invalid frequency metadata",
                window.spectral_window_id
            )));
        }
        let half_width_hz = width_hz.abs() / 2.0;
        lower_hz = lower_hz.min(frequency_hz - half_width_hz);
        upper_hz = upper_hz.max(frequency_hz + half_width_hz);
    }
    if !(lower_hz.is_finite() && upper_hz.is_finite() && upper_hz > lower_hz) {
        return Err(MsError::InvalidInput(format!(
            "SPECTRAL_WINDOW_ID {} selects no finite channel interval",
            window.spectral_window_id
        )));
    }
    Ok(SelectedWindowEdges {
        spectral_window_id: window.spectral_window_id,
        source_frequency_reference: window.source_frequency_reference,
        lower_hz,
        upper_hz,
    })
}

/// Global selected channel-edge envelope after per-row frame conversion.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedObservationSpectralEnvelope {
    edges_hz: [f64; 2],
}

impl SelectedObservationSpectralEnvelope {
    /// Return the global lower and upper selected-channel edges in the output frame.
    #[must_use]
    pub const fn edges_hz(self) -> [f64; 2] {
        self.edges_hz
    }

    /// Return CASA's MFS/Taylor reference frequency: the selected edge-envelope midpoint.
    #[must_use]
    pub fn midpoint_hz(self) -> f64 {
        (self.edges_hz[0] + self.edges_hz[1]) / 2.0
    }
}

/// Bounded reducer that observes the canonical selected-row traversal in place.
pub struct SelectedObservationSpectralEnvelopeReducer<'a> {
    measurement_set: &'a MeasurementSet,
    row_selection: &'a SelectedObservationRowSelection,
    geometry_engine: &'a MsCalEngine,
    output_frequency_reference: FrequencyRef,
    windows: Vec<SelectedWindowEdges>,
    selected_edges_hz: [f64; 2],
    last_key: Option<(u64, i32, i32)>,
    last_transform: Option<(EnvelopeTransformKey, PreparedFrequencyFrameConversion)>,
    source_frame: Option<(i32, u64, MeasFrame)>,
    field_direction: Option<(i32, MDirection)>,
    retained_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EnvelopeTransformKey {
    field_id: i32,
    time_mjd_seconds_bits: u64,
    source_ref: FrequencyRef,
    output_ref: FrequencyRef,
}

impl<'a> SelectedObservationSpectralEnvelopeReducer<'a> {
    /// Plan a bounded exact envelope reduction from borrowed native coordinates.
    pub(crate) fn new<'w>(
        measurement_set: &'a MeasurementSet,
        row_selection: &'a SelectedObservationRowSelection,
        spectral_windows: impl IntoIterator<Item = SelectedObservationSpectralWindow<'w>>,
        output_frequency_reference: FrequencyRef,
        geometry_engine: &'a MsCalEngine,
        available_bytes: usize,
    ) -> MsResult<Self> {
        let mut windows = Vec::new();
        for window in spectral_windows {
            if windows.iter().any(|candidate: &SelectedWindowEdges| {
                candidate.spectral_window_id == window.spectral_window_id
            }) {
                return Err(MsError::InvalidInput(format!(
                    "selected spectral envelope repeats SPECTRAL_WINDOW_ID {}",
                    window.spectral_window_id
                )));
            }
            windows.push(selected_window_edges(window)?);
        }
        if windows.is_empty() {
            return Err(MsError::InvalidInput(
                "selected spectral envelope requires at least one spectral window".to_string(),
            ));
        }
        for description in row_selection.data_descriptions() {
            if !windows
                .iter()
                .any(|window| window.spectral_window_id == description.spectral_window_id())
            {
                return Err(MsError::InvalidInput(format!(
                    "selected DATA_DESC_ID {} references an unselected spectral window",
                    description.data_description_id()
                )));
            }
        }
        let retained_bytes = windows
            .capacity()
            .checked_mul(std::mem::size_of::<SelectedWindowEdges>())
            .ok_or_else(|| {
                MsError::InvalidInput("spectral-envelope residency overflows usize".to_string())
            })?;
        if retained_bytes > available_bytes {
            return Err(MsError::InvalidInput(format!(
                "selected spectral envelope requires {retained_bytes} retained bytes but the budget has {available_bytes} bytes"
            )));
        }
        Ok(Self {
            measurement_set,
            row_selection,
            geometry_engine,
            output_frequency_reference,
            windows,
            selected_edges_hz: [f64::INFINITY, f64::NEG_INFINITY],
            last_key: None,
            last_transform: None,
            source_frame: None,
            field_direction: None,
            retained_bytes,
        })
    }

    /// Return the exact heap capacity retained concurrently with row blocks.
    #[must_use]
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Observe one row from the canonical selected-row traversal.
    pub fn observe(&mut self, row: super::SelectedObservationRow) -> MsResult<()> {
        let key = (
            row.time_mjd_seconds().to_bits(),
            row.field_id(),
            row.data_description_id(),
        );
        if self.last_key == Some(key) {
            return Ok(());
        }
        self.last_key = Some(key);
        let data_description_id = u32::try_from(row.data_description_id()).map_err(|_| {
            MsError::InvalidInput(
                "selected spectral envelope observed a negative DATA_DESC_ID".to_string(),
            )
        })?;
        let description = self
            .row_selection
            .data_descriptions()
            .iter()
            .find(|description| description.data_description_id() == data_description_id)
            .ok_or_else(|| {
                MsError::InvalidInput(format!(
                    "selected DATA_DESC_ID {data_description_id} has no storage binding"
                ))
            })?;
        let window = self
            .windows
            .iter()
            .find(|window| window.spectral_window_id == description.spectral_window_id())
            .expect("constructor validates every selected data description");
        let source_ref = window.source_frequency_reference;
        let time_mjd_seconds = row.time_mjd_seconds();
        let time_bits = time_mjd_seconds.to_bits();
        let transform_key = EnvelopeTransformKey {
            field_id: row.field_id(),
            time_mjd_seconds_bits: time_bits,
            source_ref,
            output_ref: self.output_frequency_reference,
        };
        let conversion = if let Some((cached_key, conversion)) = self.last_transform
            && cached_key == transform_key
        {
            conversion
        } else if source_ref == self.output_frequency_reference {
            PreparedFrequencyFrameConversion::new(
                source_ref,
                self.output_frequency_reference,
                None,
                None,
            )?
        } else {
            let field_id = usize::try_from(row.field_id()).map_err(|_| {
                MsError::InvalidInput(
                    "selected spectral envelope observed a negative FIELD_ID".to_string(),
                )
            })?;
            let source_frame = match &mut self.source_frame {
                Some((cached_field, cached_time, frame))
                    if *cached_field == row.field_id() && *cached_time == time_bits =>
                {
                    frame
                }
                slot => {
                    let direction = match &mut self.field_direction {
                        Some((cached_field, direction)) if *cached_field == row.field_id() => {
                            direction.clone()
                        }
                        slot => {
                            let direction =
                                raw_field_phase_direction(self.measurement_set, field_id)?;
                            *slot = Some((row.field_id(), direction.clone()));
                            direction
                        }
                    };
                    let frame = self
                        .geometry_engine
                        .spectral_frame_observatory_direction(time_mjd_seconds, direction)?;
                    *slot = Some((row.field_id(), time_bits, frame));
                    &slot.as_ref().expect("source frame was inserted").2
                }
            };
            PreparedFrequencyFrameConversion::new(
                source_ref,
                self.output_frequency_reference,
                Some(source_frame),
                Some(source_frame),
            )?
        };
        self.last_transform = Some((transform_key, conversion));
        let first_hz = conversion.convert_hz(window.lower_hz);
        let second_hz = conversion.convert_hz(window.upper_hz);
        self.selected_edges_hz[0] = self.selected_edges_hz[0].min(first_hz.min(second_hz));
        self.selected_edges_hz[1] = self.selected_edges_hz[1].max(first_hz.max(second_hz));
        Ok(())
    }

    /// Complete the reduction after the shared selected-row pass.
    pub fn finish(self) -> MsResult<SelectedObservationSpectralEnvelope> {
        if !self.selected_edges_hz[0].is_finite()
            || !self.selected_edges_hz[1].is_finite()
            || self.selected_edges_hz[1] <= self.selected_edges_hz[0]
        {
            return Err(MsError::InvalidInput(
                "selected spectral envelope produced no finite output interval".to_string(),
            ));
        }
        Ok(SelectedObservationSpectralEnvelope {
            edges_hz: self.selected_edges_hz,
        })
    }
}

impl MeasurementSet {
    /// Plan a bounded spectral-envelope observer for the canonical selected-row traversal.
    #[allow(clippy::too_many_arguments)]
    pub fn selected_observation_spectral_envelope_reducer<'a, 'w>(
        &'a self,
        row_selection: &'a SelectedObservationRowSelection,
        spectral_windows: impl IntoIterator<Item = SelectedObservationSpectralWindow<'w>>,
        output_frequency_reference: FrequencyRef,
        geometry_engine: &'a MsCalEngine,
        available_bytes: usize,
    ) -> MsResult<SelectedObservationSpectralEnvelopeReducer<'a>> {
        SelectedObservationSpectralEnvelopeReducer::new(
            self,
            row_selection,
            spectral_windows,
            output_frequency_reference,
            geometry_engine,
            available_bytes,
        )
    }

    /// Reduce selected native-channel edges over every selected TIME-by-DDID row.
    ///
    /// This is the storage-owned equivalent of CASA `advisechansel` frequency-
    /// range evaluation. It streams canonical selected-row metadata under the
    /// caller's I/O budget and retains only extrema plus one consecutive-key
    /// cache. `reference_time_mjd_seconds` and `reference_direction` describe
    /// the output cube frame used to clamp an edge-valued requested start. MVC
    /// follows CASA's single-field range contract and rejects a selected row
    /// whose FIELD_ID differs from `field_id`.
    #[allow(clippy::too_many_arguments)]
    pub fn selected_observation_spectral_range(
        &self,
        row_selection: &SelectedObservationRowSelection,
        spectral_selection: &[SpectralWindowSelection],
        source_frequency_reference: FrequencyRef,
        output_frequency_reference: FrequencyRef,
        field_id: usize,
        reference_time_mjd_seconds: f64,
        reference_direction: MDirection,
        moving_phase_centre: Option<&SelectedObservationEphemeris>,
        geometry_engine: &MsCalEngine,
        io: MsSelectionIoBudget,
    ) -> MsResult<SelectedObservationSpectralRange> {
        if spectral_selection.is_empty() {
            return Err(MsError::InvalidInput(
                "selected spectral range requires at least one spectral window".to_string(),
            ));
        }
        let spectral_window = self.spectral_window()?;
        let mut windows = Vec::with_capacity(spectral_selection.len());
        for selection in spectral_selection {
            let spectral_window_id = selection.spectral_window_id();
            if windows
                .iter()
                .any(|window: &SelectedWindowEdges| window.spectral_window_id == spectral_window_id)
            {
                return Err(MsError::InvalidInput(format!(
                    "selected spectral range repeats SPECTRAL_WINDOW_ID {spectral_window_id}"
                )));
            }
            let row = usize::try_from(spectral_window_id).expect("u32 fits usize");
            let frequencies_hz = spectral_window.chan_freq(row)?;
            let widths_hz = spectral_window.chan_width(row)?;
            if frequencies_hz.len() != widths_hz.len() {
                return Err(MsError::InvalidInput(format!(
                    "SPECTRAL_WINDOW_ID {spectral_window_id} frequency/width lengths differ"
                )));
            }
            let mut lower_hz = f64::INFINITY;
            let mut upper_hz = f64::NEG_INFINITY;
            for channel in selection.channel_indices() {
                let channel = usize::try_from(*channel).expect("u32 fits usize");
                let frequency_hz = *frequencies_hz.get(channel).ok_or_else(|| {
                    MsError::InvalidInput(format!(
                        "selected channel {channel} is outside SPECTRAL_WINDOW_ID {spectral_window_id}"
                    ))
                })?;
                let width_hz = *widths_hz
                    .get(channel)
                    .expect("frequency/width lengths match");
                if !(frequency_hz.is_finite() && width_hz.is_finite() && width_hz != 0.0) {
                    return Err(MsError::InvalidInput(format!(
                        "selected channel {channel} in SPECTRAL_WINDOW_ID {spectral_window_id} has invalid frequency metadata"
                    )));
                }
                let half_width_hz = width_hz.abs() / 2.0;
                lower_hz = lower_hz.min(frequency_hz - half_width_hz);
                upper_hz = upper_hz.max(frequency_hz + half_width_hz);
            }
            if !(lower_hz.is_finite() && upper_hz.is_finite() && upper_hz > lower_hz) {
                return Err(MsError::InvalidInput(format!(
                    "SPECTRAL_WINDOW_ID {spectral_window_id} selects no finite channel interval"
                )));
            }
            windows.push(SelectedWindowEdges {
                spectral_window_id,
                source_frequency_reference,
                lower_hz,
                upper_hz,
            });
        }

        let reference_frame = geometry_engine
            .spectral_frame_observatory_direction(reference_time_mjd_seconds, reference_direction)
            .map_err(|error| {
                MsError::VersionError(format!(
                    "build selected spectral-range reference frame: {error}"
                ))
            })?;
        let mut reference_edges_hz = [f64::INFINITY, f64::NEG_INFINITY];
        for window in &windows {
            reference_edges_hz[0] =
                reference_edges_hz[0].min(convert_frequency_to_frame_with_frame(
                    source_frequency_reference,
                    output_frequency_reference,
                    window.lower_hz,
                    Some(&reference_frame),
                )?);
            reference_edges_hz[1] =
                reference_edges_hz[1].max(convert_frequency_to_frame_with_frame(
                    source_frequency_reference,
                    output_frequency_reference,
                    window.upper_hz,
                    Some(&reference_frame),
                )?);
        }

        let mut selected_edges_hz = [f64::INFINITY, f64::NEG_INFINITY];
        let selected_direction = raw_field_phase_direction(self, field_id)?;
        let mut selected_rows = 0_u64;
        let mut edge_evaluations = 0_u64;
        let mut last_key = None;
        let mut traversal_error = None;
        self.visit_selected_observation_rows(row_selection, io, |row| {
            selected_rows = selected_rows.saturating_add(1);
            if traversal_error.is_some() {
                return;
            }
            let row_field_id = match usize::try_from(row.field_id()) {
                Ok(value) => value,
                Err(_) => {
                    traversal_error = Some(MsError::InvalidInput(
                        "selected spectral range observed a negative FIELD_ID".to_string(),
                    ));
                    return;
                }
            };
            if row_field_id != field_id {
                traversal_error = Some(MsError::InvalidInput(format!(
                    "MVC selected spectral range requires one FIELD_ID; expected {field_id}, observed {row_field_id}"
                )));
                return;
            }
            let data_description_id = match u32::try_from(row.data_description_id()) {
                Ok(value) => value,
                Err(_) => {
                    traversal_error = Some(MsError::InvalidInput(
                        "selected spectral range observed a negative DATA_DESC_ID".to_string(),
                    ));
                    return;
                }
            };
            let Some(description) = row_selection
                .data_descriptions()
                .iter()
                .find(|description| description.data_description_id() == data_description_id)
            else {
                traversal_error = Some(MsError::InvalidInput(format!(
                    "selected DATA_DESC_ID {data_description_id} has no storage binding"
                )));
                return;
            };
            let Some(window) = windows.iter().find(|window| {
                window.spectral_window_id == description.spectral_window_id()
            }) else {
                traversal_error = Some(MsError::InvalidInput(format!(
                    "selected DATA_DESC_ID {data_description_id} references an unselected spectral window"
                )));
                return;
            };
            let key = (row.time_mjd_seconds().to_bits(), data_description_id);
            if last_key == Some(key) {
                return;
            }
            last_key = Some(key);
            let row_direction = match moving_phase_centre {
                Some(ephemeris) => geometry_engine.tracked_field_direction_j2000(
                    row.time_mjd_seconds(),
                    field_id,
                    ephemeris,
                ),
                None => Ok(selected_direction.clone()),
            };
            let frame = match row_direction.and_then(|direction| {
                geometry_engine
                    .spectral_frame_observatory_direction(row.time_mjd_seconds(), direction)
            }) {
                Ok(frame) => frame,
                Err(error) => {
                    traversal_error = Some(error);
                    return;
                }
            };
            let converted = [window.lower_hz, window.upper_hz].map(|frequency_hz| {
                convert_frequency_to_frame_with_frame(
                    source_frequency_reference,
                    output_frequency_reference,
                    frequency_hz,
                    Some(&frame),
                )
            });
            let [Ok(lower_hz), Ok(upper_hz)] = converted else {
                traversal_error = converted.into_iter().find_map(Result::err);
                return;
            };
            selected_edges_hz[0] = selected_edges_hz[0].min(lower_hz.min(upper_hz));
            selected_edges_hz[1] = selected_edges_hz[1].max(lower_hz.max(upper_hz));
            edge_evaluations = edge_evaluations.saturating_add(1);
        })?;
        if let Some(error) = traversal_error {
            return Err(error);
        }
        if selected_rows == 0
            || !selected_edges_hz[0].is_finite()
            || !selected_edges_hz[1].is_finite()
            || selected_edges_hz[1] <= selected_edges_hz[0]
            || !reference_edges_hz[0].is_finite()
            || !reference_edges_hz[1].is_finite()
            || reference_edges_hz[1] <= reference_edges_hz[0]
        {
            return Err(MsError::InvalidInput(
                "selected spectral range produced no finite output interval".to_string(),
            ));
        }
        Ok(SelectedObservationSpectralRange {
            selected_edges_hz,
            reference_edges_hz,
            measurements: SelectedObservationSpectralRangeMeasurements {
                selected_rows,
                edge_evaluations,
            },
        })
    }
}

/// One owner-issued selected sample and its evaluated output spectral support.
///
/// This envelope is constructed only by retained [`BoundSelectedObservation`](super::BoundSelectedObservation)
/// traversal after the selected sample has passed compiled-problem validation.
/// It is not persisted and does not participate in selected-sample content
/// identity.
///
/// ```compile_fail
/// use casa_ms::SelectedObservationTraversalSample;
///
/// let _forged = SelectedObservationTraversalSample {};
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedObservationTraversalSample<'a> {
    sample: SelectedObservationSampleView<'a>,
    spectral_evaluation: SelectedSpectralEvaluation,
}

impl<'a> SelectedObservationTraversalSample<'a> {
    pub(super) fn with_spectral_evaluation(
        sample: SelectedObservationSampleView<'a>,
        spectral_evaluation: SelectedSpectralEvaluation,
    ) -> Self {
        Self {
            sample,
            spectral_evaluation,
        }
    }

    /// Return the selected sample validated by the compiled problem.
    #[must_use]
    pub const fn selected(&self) -> SelectedObservationSampleView<'a> {
        self.sample
    }

    /// Return source-backed native/output-frame intervals, flag validity, and effective weight.
    #[must_use]
    pub const fn spectral_evaluation(&self) -> SelectedSpectralEvaluation {
        self.spectral_evaluation
    }
}

/// One borrowed row/channel run validated in canonical correlation order.
///
/// The run borrows one row and reusable correlation/evaluation scratch while
/// keeping its channel inline. It is released before the source block can be
/// refilled or the scratch can be reused.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedObservationTraversalRun<'a> {
    row: &'a SelectedObservationRunRow,
    channel: SelectedObservationRunChannel,
    correlations: &'a [SelectedObservationRunCorrelation],
    evaluations: &'a [SelectedSpectralEvaluation],
}

impl<'a> SelectedObservationTraversalRun<'a> {
    pub(super) fn new(
        row: &'a SelectedObservationRunRow,
        channel: SelectedObservationRunChannel,
        correlations: &'a [SelectedObservationRunCorrelation],
        evaluations: &'a [SelectedSpectralEvaluation],
    ) -> Self {
        debug_assert_eq!(correlations.len(), evaluations.len());
        Self {
            row,
            channel,
            correlations,
            evaluations,
        }
    }

    /// Iterate through validated correlation members in canonical order.
    pub fn samples(&self) -> impl Iterator<Item = SelectedObservationTraversalSample<'_>> {
        let input_weight_group = self.correlations.first().map(|first| {
            SelectedInputWeightGroup::correlation_run(
                first.input_weight,
                self.correlations
                    .last()
                    .expect("nonempty correlation run")
                    .input_weight,
                self.correlations.len(),
            )
        });
        self.correlations
            .iter()
            .zip(self.evaluations.iter())
            .enumerate()
            .map(move |(ordinal, (correlation, evaluation))| {
                SelectedObservationTraversalSample::with_spectral_evaluation(
                    SelectedObservationSampleView::from_run(self.row, &self.channel, correlation)
                        .with_input_weight_group(
                            input_weight_group
                                .unwrap_or(SelectedInputWeightGroup::single(
                                    correlation.input_weight,
                                ))
                                .with_density_owner(ordinal == 0)
                                .with_terminal_member(ordinal + 1 == self.correlations.len()),
                        ),
                    *evaluation,
                )
            })
    }

    /// Return the number of correlations in this row/channel run.
    #[must_use]
    pub fn len(&self) -> usize {
        self.correlations.len()
    }

    /// Return whether the run contains no correlations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.correlations.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SpectralProjectionKey {
    measurement_set: casa_imaging_model::MeasurementSetIdentity,
    spectral_window_id: u32,
    channel_index: u32,
    frequency_centre_bits: u64,
    frequency_lower_bits: u64,
    frequency_upper_bits: u64,
    channel_width_bits: u64,
    frequency_frame: FrequencyFrame,
    field_id: i32,
    time_mjd_days_bits: u64,
}

impl SpectralProjectionKey {
    fn from_sample(sample: SelectedObservationSampleView<'_>) -> Self {
        let address = sample.address();
        let metadata = sample.metadata();
        let coordinates = sample.coordinates();
        Self {
            measurement_set: address.measurement_set,
            spectral_window_id: address.spectral_window_id,
            channel_index: address.channel_index,
            frequency_centre_bits: address.frequency_centre_hz.to_bits(),
            frequency_lower_bits: address.frequency_lower_hz.to_bits(),
            frequency_upper_bits: address.frequency_upper_hz.to_bits(),
            channel_width_bits: address.channel_width_hz.to_bits(),
            frequency_frame: address.frequency_frame,
            field_id: metadata.field_id,
            time_mjd_days_bits: coordinates.time.mjd_days().to_bits(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SpectralTransformKey {
    measurement_set: casa_imaging_model::MeasurementSetIdentity,
    field_id: i32,
    time_mjd_seconds_bits: u64,
    source_ref: FrequencyRef,
    output_ref: FrequencyRef,
}

/// Retains frame-conversion state across the canonical correlation-major stream.
///
/// Spectral assignment is independent of correlation and visibility value. A
/// row/channel's parallel-hand samples therefore reuse the exact same derived
/// evaluation, while a row, channel interval, field, time, frame, weight, flag, SPW, or source
/// change forces a fresh owner evaluation.
pub(super) struct SpectralEvaluationProjector {
    last_source: Option<casa_imaging_model::MeasurementSetIdentity>,
    source_frame: Option<(i32, u64, MeasFrame)>,
    last_transform: Option<(SpectralTransformKey, PreparedFrequencyFrameConversion)>,
    last_projection: Option<(
        SpectralProjectionKey,
        SelectedSpectralInterval,
        SelectedSpectralInterval,
    )>,
}

impl SpectralEvaluationProjector {
    pub(super) const fn new() -> Self {
        Self {
            last_source: None,
            source_frame: None,
            last_transform: None,
            last_projection: None,
        }
    }

    pub(super) fn project<'a>(
        &mut self,
        problem: &CompiledProblem,
        sample: SelectedObservationSampleView<'a>,
        geometry_engine: &MsCalEngine,
    ) -> Result<SelectedObservationTraversalSample<'a>, BoundObservationSourceError> {
        let key = SpectralProjectionKey::from_sample(sample);
        let address = sample.address();
        if self.last_source != Some(address.measurement_set) {
            self.last_source = Some(address.measurement_set);
            self.source_frame = None;
            self.last_transform = None;
            self.last_projection = None;
        }
        let (native, output) = if let Some((cached_key, native, output)) = self.last_projection
            && cached_key == key
        {
            (native, output)
        } else {
            let intervals = derive_spectral_intervals_cached(
                problem,
                sample,
                geometry_engine,
                &mut self.source_frame,
                &mut self.last_transform,
            )?;
            self.last_projection = Some((key, intervals.0, intervals.1));
            intervals
        };
        let input_weight = sample.input_weight();
        let valid = !sample.channel_flag()
            && !sample.parallel_hand_group_flag()
            && !sample.row_flag()
            && input_weight.is_finite()
            && input_weight > 0.0;
        let evaluation = SelectedSpectralEvaluation::new(
            native,
            output,
            if valid { f64::from(input_weight) } else { 0.0 },
            valid,
        )
        .ok_or(BoundObservationSourceError::SpectralContributionMismatch)?;
        Ok(SelectedObservationTraversalSample::with_spectral_evaluation(sample, evaluation))
    }
}

fn derive_spectral_intervals_cached(
    problem: &CompiledProblem,
    sample: SelectedObservationSampleView<'_>,
    geometry_engine: &MsCalEngine,
    source_frame: &mut Option<(i32, u64, MeasFrame)>,
    last_transform: &mut Option<(SpectralTransformKey, PreparedFrequencyFrameConversion)>,
) -> Result<(SelectedSpectralInterval, SelectedSpectralInterval), BoundObservationSourceError> {
    let address = sample.address();
    let native_boundaries = if address.channel_width_hz >= 0.0 {
        [address.frequency_lower_hz, address.frequency_upper_hz]
    } else {
        [address.frequency_upper_hz, address.frequency_lower_hz]
    };
    let native = SelectedSpectralInterval::new(
        address.frequency_centre_hz,
        native_boundaries[0],
        native_boundaries[1],
    )
    .ok_or(BoundObservationSourceError::SpectralContributionMismatch)?;
    let conversion = prepared_frequency_conversion_cached(
        problem,
        sample,
        geometry_engine,
        source_frame,
        last_transform,
    )?;
    let output_centre_hz = conversion.convert_hz(native.centre_hz());
    let output_first_hz = conversion.convert_hz(native_boundaries[0]);
    let output_second_hz = conversion.convert_hz(native_boundaries[1]);
    let output = SelectedSpectralInterval::new(output_centre_hz, output_first_hz, output_second_hz)
        .ok_or(BoundObservationSourceError::SpectralContributionMismatch)?;
    Ok((native, output))
}

fn prepared_frequency_conversion_cached(
    problem: &CompiledProblem,
    sample: SelectedObservationSampleView<'_>,
    geometry_engine: &MsCalEngine,
    source_frame_cache: &mut Option<(i32, u64, MeasFrame)>,
    last_transform: &mut Option<(SpectralTransformKey, PreparedFrequencyFrameConversion)>,
) -> Result<PreparedFrequencyFrameConversion, BoundObservationSourceError> {
    let spectral = problem.geometry().spectral();
    let address = sample.address();
    let metadata = sample.metadata();
    let coordinates = sample.coordinates();
    let source_ref = frequency_ref(address.frequency_frame);
    let output_ref = frequency_ref(spectral.output_frame());
    let time_mjd_seconds = coordinates.time.mjd_days() * 86_400.0;
    let time_bits = time_mjd_seconds.to_bits();
    let key = SpectralTransformKey {
        measurement_set: address.measurement_set,
        field_id: metadata.field_id,
        time_mjd_seconds_bits: time_bits,
        source_ref,
        output_ref,
    };
    if let Some((cached_key, conversion)) = *last_transform
        && cached_key == key
    {
        return Ok(conversion);
    }
    if source_ref == output_ref {
        let conversion = PreparedFrequencyFrameConversion::new(source_ref, output_ref, None, None)
            .map_err(BoundObservationSourceError::from)?;
        *last_transform = Some((key, conversion));
        return Ok(conversion);
    }
    let field_id = usize::try_from(metadata.field_id)
        .map_err(|_| BoundObservationSourceError::SpectralContributionMismatch)?;
    let source_frame = match source_frame_cache {
        Some((cached_field, cached_time, frame))
            if *cached_field == metadata.field_id && *cached_time == time_bits =>
        {
            frame
        }
        slot => {
            // Frequency conversion follows the native FIELD direction, as CASA's
            // visibility iterator does. The selected phase direction may already
            // be rephased to the common imaging centre for mosaic gridding.
            let phase = geometry_engine.observation_direction_j2000(time_mjd_seconds, field_id)?;
            let mut frame =
                geometry_engine.spectral_frame_observatory_direction(time_mjd_seconds, phase)?;
            if let Some(velocity) =
                geometry_engine.moving_radial_velocity(time_mjd_seconds, field_id)?
            {
                frame = frame.with_radial_velocity(velocity);
            }
            *slot = Some((metadata.field_id, time_bits, frame));
            &slot.as_ref().expect("source frame was inserted").2
        }
    };
    let moving_rest_frame;
    let target_frame = if output_ref == FrequencyRef::REST {
        if source_frame.radial_velocity().is_none() {
            return Err(BoundObservationSourceError::SpectralContributionMismatch);
        }
        moving_rest_frame = source_frame.clone();
        &moving_rest_frame
    } else {
        // CASA requests the target reference type from the visibility iterator;
        // it does not substitute the image-axis anchor as a second measures frame.
        source_frame
    };
    let conversion = PreparedFrequencyFrameConversion::new(
        source_ref,
        output_ref,
        Some(source_frame),
        Some(target_frame),
    )
    .map_err(BoundObservationSourceError::from)?;
    *last_transform = Some((key, conversion));
    Ok(conversion)
}

const fn frequency_ref(frame: FrequencyFrame) -> FrequencyRef {
    match frame {
        FrequencyFrame::Rest => FrequencyRef::REST,
        FrequencyFrame::Topocentric => FrequencyRef::TOPO,
        FrequencyFrame::Barycentric => FrequencyRef::BARY,
        FrequencyFrame::Lsrk => FrequencyRef::LSRK,
    }
}

#[cfg(test)]
mod tests {
    use casa_types::measures::frequency::FrequencyRef;

    use super::{SelectedObservationSpectralWindow, selected_window_edges};

    #[test]
    fn selected_envelope_uses_exact_nonuniform_subselected_channel_edges() {
        let edges = selected_window_edges(SelectedObservationSpectralWindow::borrow_selected(
            7,
            FrequencyRef::TOPO,
            &[100.0, 113.0, 151.0, 220.0],
            &[8.0, -10.0, 14.0, 20.0],
            &[1, 3],
        ))
        .expect("nonuniform selected edges");

        assert_eq!(edges.lower_hz, 108.0);
        assert_eq!(edges.upper_hz, 230.0);
        assert_eq!(
            (edges.lower_hz + edges.upper_hz) / 2.0,
            169.0,
            "the Taylor reference is the selected edge-envelope midpoint, not the mean channel centre"
        );
    }
}
