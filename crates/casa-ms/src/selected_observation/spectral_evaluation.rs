// SPDX-License-Identifier: LGPL-3.0-or-later
//! Storage-owned source and output-frame spectral evaluation.

use casa_imaging_model::FrequencyFrame;

use casa_types::measures::{direction::MDirection, frame::MeasFrame, frequency::FrequencyRef};

use crate::{
    MeasurementSet, MsError, MsResult, MsSelectionIoBudget,
    derived::engine::{MsCalEngine, raw_field_phase_direction},
    spectral_selection::PreparedFrequencyFrameConversion,
};

use super::{BoundObservationSourceError, SelectedObservationRowSelection};

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
    /// Derive CASA's nominal Briggs-cube density padding for one native SPW
    /// (`BriggsCubeWeightor::estimateSwingChanPad`, per side: half its
    /// `swingpad`).
    ///
    /// Each field's requested image-centre interval is converted back to native
    /// frequency over the selected row epochs, then matched against the complete
    /// native SPW with CASA's strict channel-edge overlap. The padding is the
    /// swing of the matched native range in image channels, rounded up, plus
    /// `max(min(4, nchan/10), 1)`, so it is at least one plane even when the
    /// source and image frames agree. Explicit selected-channel interpolation
    /// support does not redefine this nominal density domain. No visibility
    /// payload is read.
    #[allow(clippy::too_many_arguments)]
    pub fn selected_observation_cube_density_padding(
        &self,
        row_selection: &SelectedObservationRowSelection,
        window: SelectedObservationSpectralWindow<'_>,
        fields: impl IntoIterator<Item = i32>,
        output_frequency_reference: FrequencyRef,
        output_centres: [f64; 2],
        output_channels: usize,
        geometry_engine: &MsCalEngine,
        io: MsSelectionIoBudget,
    ) -> MsResult<usize> {
        if row_selection
            .data_descriptions()
            .iter()
            .any(|description| description.spectral_window_id() != window.spectral_window_id)
            || window.frequencies_hz.is_empty()
            || window
                .frequencies_hz
                .iter()
                .any(|frequency| !frequency.is_finite() || *frequency <= 0.0)
            || window
                .channel_widths_hz
                .iter()
                .any(|width| !width.is_finite() || *width == 0.0)
        {
            return Err(MsError::InvalidInput(
                "cube density padding requires one valid native SPW".into(),
            ));
        }
        // CASA adds `max(min(4, nchan/10), 1)` planes beyond the swing, so a
        // frame-free or swing-free axis still pads by that much.
        let extra_padding = (output_channels / 10).clamp(1, 4);
        if window.source_frequency_reference == output_frequency_reference {
            return Ok(extra_padding);
        }
        let increment = (output_centres[1] - output_centres[0]).abs()
            / output_channels.saturating_sub(1).max(1) as f64;
        if output_channels < 2 || !increment.is_finite() || increment <= 0.0 {
            return Err(MsError::InvalidInput(
                "cube density padding requires a finite multi-channel axis".into(),
            ));
        }
        if window.frequencies_hz.len() != window.channel_widths_hz.len() {
            return Err(MsError::InvalidInput(
                "cube density native frequency/width lengths differ".into(),
            ));
        }
        let output = [
            output_centres[0].min(output_centres[1]),
            output_centres[0].max(output_centres[1]),
        ];
        let mut lower_centres = [f64::INFINITY, f64::NEG_INFINITY];
        let mut upper_centres = [f64::INFINITY, f64::NEG_INFINITY];
        for field in fields {
            let field_id = usize::try_from(field)
                .map_err(|_| MsError::InvalidInput("cube density field is negative".into()))?;
            let direction = raw_field_phase_direction(self, field_id)?;
            let mut interval = [f64::INFINITY, f64::NEG_INFINITY];
            let mut last_time = None;
            let mut error = None;
            self.visit_selected_observation_rows(row_selection, io, |row| {
                if row.field_id() != field
                    || last_time == Some(row.time_mjd_seconds().to_bits())
                    || error.is_some()
                {
                    return;
                }
                last_time = Some(row.time_mjd_seconds().to_bits());
                let result = (|| -> MsResult<()> {
                    let frame = geometry_engine.spectral_frame_observatory_direction(
                        row.time_mjd_seconds(),
                        direction.clone(),
                    )?;
                    let conversion = PreparedFrequencyFrameConversion::new(
                        output_frequency_reference,
                        window.source_frequency_reference,
                        Some(&frame),
                        Some(&frame),
                    )?;
                    let a = conversion.convert_hz(output[0]);
                    let b = conversion.convert_hz(output[1]);
                    interval[0] = interval[0].min(a.min(b));
                    interval[1] = interval[1].max(a.max(b));
                    Ok(())
                })();
                error = result.err();
            })?;
            if let Some(error) = error {
                return Err(error);
            }
            let native_min = window
                .frequencies_hz
                .iter()
                .copied()
                .fold(f64::INFINITY, f64::min);
            let native_max = window
                .frequencies_hz
                .iter()
                .copied()
                .fold(f64::NEG_INFINITY, f64::max);
            let lower = interval[0] - 0.5 * increment;
            let upper = interval[1] + 0.5 * increment;
            let partial = (lower > native_min && lower < native_max)
                || (upper > native_min && upper < native_max);
            let full = lower < native_min && upper > native_max;
            if !partial && !full {
                continue;
            }
            let mut first = None;
            let mut last = None;
            for (channel, (&centre, &width)) in window
                .frequencies_hz
                .iter()
                .zip(window.channel_widths_hz)
                .enumerate()
            {
                if full
                    || (centre + 0.5 * width.abs() > lower && centre - 0.5 * width.abs() < upper)
                {
                    first.get_or_insert(channel);
                    last = Some(channel);
                }
            }
            if let (Some(first), Some(last)) = (first, last) {
                let a = window.frequencies_hz[first];
                let b = window.frequencies_hz[last];
                lower_centres[0] = lower_centres[0].min(a.min(b));
                lower_centres[1] = lower_centres[1].max(a.min(b));
                upper_centres[0] = upper_centres[0].min(a.max(b));
                upper_centres[1] = upper_centres[1].max(a.max(b));
            }
        }
        if !lower_centres[0].is_finite() {
            return Ok(extra_padding);
        }
        // One SPW has one full-native origin, so CASA's first-channel shift is zero.
        let swing = (lower_centres[1] - lower_centres[0]).max(upper_centres[1] - upper_centres[0]);
        let swing_channels = (swing / increment).ceil();
        if !swing_channels.is_finite() || swing_channels >= usize::MAX as f64 {
            return Err(MsError::InvalidInput(
                "cube density padding overflows usize".into(),
            ));
        }
        (swing_channels as usize)
            .checked_add(extra_padding)
            .ok_or_else(|| MsError::InvalidInput("cube density padding overflows usize".into()))
    }

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
}

/// Prepare CASA's source-frame conversion for one row's channel lattice:
/// the native FIELD direction at the row time, as CASA's visibility iterator
/// converts (the selected phase direction may already be rephased to the
/// common imaging centre for mosaic gridding). Window planning calls this
/// before channelized visibility payloads are read, using only MAIN row
/// TIME/FIELD metadata.
pub(super) fn prepare_row_frequency_conversion(
    geometry_engine: &MsCalEngine,
    field_id: i32,
    time_mjd_seconds: f64,
    source_frame: FrequencyFrame,
    output_frame: FrequencyFrame,
) -> Result<PreparedFrequencyFrameConversion, BoundObservationSourceError> {
    let source_ref = frequency_ref(source_frame);
    let output_ref = frequency_ref(output_frame);
    if source_ref == output_ref {
        return PreparedFrequencyFrameConversion::new(source_ref, output_ref, None, None)
            .map_err(BoundObservationSourceError::from);
    }
    let field_id = usize::try_from(field_id)
        .map_err(|_| BoundObservationSourceError::SpectralContributionMismatch)?;
    let phase = geometry_engine.observation_direction_j2000(time_mjd_seconds, field_id)?;
    let mut native_frame =
        geometry_engine.spectral_frame_observatory_direction(time_mjd_seconds, phase)?;
    if let Some(velocity) = geometry_engine.moving_radial_velocity(time_mjd_seconds, field_id)? {
        native_frame = native_frame.with_radial_velocity(velocity);
    }
    let moving_rest_frame;
    let target_frame = if output_ref == FrequencyRef::REST {
        if native_frame.radial_velocity().is_none() {
            return Err(BoundObservationSourceError::SpectralContributionMismatch);
        }
        moving_rest_frame = native_frame.clone();
        &moving_rest_frame
    } else {
        &native_frame
    };
    PreparedFrequencyFrameConversion::new(
        source_ref,
        output_ref,
        Some(&native_frame),
        Some(target_frame),
    )
    .map_err(BoundObservationSourceError::from)
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
