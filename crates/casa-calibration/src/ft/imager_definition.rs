// SPDX-License-Identifier: LGPL-3.0-or-later
//! Construct the coordinate system for a legacy imager's model image.

use std::collections::{BTreeMap, BTreeSet};

use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, ObsInfo, Projection, ProjectionType, SpectralCoordinate,
    StokesCoordinate, StokesType,
};
use casa_ms::derived::engine::{MsCalEngine, raw_field_phase_direction};
use casa_ms::{
    CubeAxisConfig, CubeAxisValue, CubeSpectralSetup, MeasurementSet, MsResult,
    ResolvedMsSelection, combine_spectral_windows, convert_frequency_to_frame_with_frame,
};
use casa_types::ArrayValue;
use casa_types::measures::direction::MDirection;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::position::MPosition;

use super::{ImagerImageDefinition, invalid};

/// Direction of the pixel at the centre of the model image.
pub enum ImagerPhaseCentre {
    /// Use a phase direction from FIELD.
    Field(usize),
    /// Use a literal CASA direction measure.
    Direction(MDirection),
}

/// Spectral layout of the model image.
pub enum ImagerSpectralAxis {
    /// A single plane spanning the observed band.
    Continuum(FrequencyRef),
    /// Resample the selected windows on a configured spectral grid.
    Cube {
        /// The number of requested image channels.
        channels: usize,
        /// Reference written on the image axis.
        frame: FrequencyRef,
        /// Channel spacing and the requested frequencies or velocities.
        config: CubeAxisConfig,
    },
}

/// Geometry requested by the legacy imager before creating a model image.
pub struct ImagerDefinitionRequest {
    /// Direction-axis lengths in pixels.
    pub shape: [usize; 2],
    /// Signed angular increments for longitude and latitude, in radians.
    pub cell_rad: [f64; 2],
    /// Polarization planes in image order.
    pub stokes: Vec<StokesType>,
    /// The direction to use at the image centre.
    pub centre: ImagerPhaseCentre,
    /// The supported image projection.
    pub projection: ProjectionType,
    /// Spectral windows for the image grid. Empty uses the selected windows.
    pub windows: Vec<usize>,
    /// Explicit rest frequency or the MeasurementSet default.
    pub rest_frequency_hz: Option<f64>,
    /// The model image's spectral layout.
    pub spectral: ImagerSpectralAxis,
}

impl ImagerDefinitionRequest {
    pub(super) fn build_for_selection(
        &self,
        ms: &MeasurementSet,
        selected: &ResolvedMsSelection,
        location: Option<MPosition>,
    ) -> MsResult<ImagerImageDefinition> {
        let row = selected
            .selected_rows
            .first()
            .ok_or_else(|| invalid("an image needs a selected visibility"))?;
        let representative_field = usize::try_from(row.field_id).map_err(invalid)?;
        let (field_id, centre) = match &self.centre {
            ImagerPhaseCentre::Field(field) => (*field, raw_field_phase_direction(ms, *field)?),
            ImagerPhaseCentre::Direction(direction) => (representative_field, direction.clone()),
        };
        let engine = MsCalEngine::new(ms)?;
        let mut frame =
            engine.spectral_frame_observatory_direction(row.time_mjd_seconds, centre.clone())?;
        if let Some(position) = location {
            frame = frame.with_position(position);
        }
        let (lon, lat) = centre.as_angles();
        let telescope = ms.observation()?.string(0, "TELESCOPE_NAME")?;
        let projection = if self.projection == ProjectionType::SIN
            && matches!(telescope.as_str(), "ATCASCP" | "WSRT" | "DRAO")
            && lat.sin() != 0.
        {
            Projection::with_parameters(self.projection, vec![0., 1. / lat.tan()])
        } else {
            Projection::new(self.projection)
        };
        let direction = DirectionCoordinate::new(
            centre.refer(),
            projection,
            [lon.rem_euclid(std::f64::consts::TAU), lat],
            self.cell_rad,
            self.shape.map(|length| (length / 2) as f64),
        );
        let mut obs = ObsInfo::new(telescope)
            .with_observer(ms.observation()?.string(0, "OBSERVER")?)
            .with_pointing_center(lon, lat);
        if let Some(epoch) = frame.epoch() {
            obs = obs.with_date(epoch.clone());
        }
        if let Some(position) = frame.position() {
            obs = obs.with_telescope_position(position.clone());
        }

        let windows: Vec<_> = if self.windows.is_empty() {
            selected
                .selected_rows
                .iter()
                .map(|row| row.spectral_window_id as usize)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect()
        } else {
            self.windows.clone()
        };
        let window = *windows
            .first()
            .ok_or_else(|| invalid("an image needs a spectral window"))?;
        let spectral_window = ms.spectral_window()?;
        let native_frame = spectral_window.frequency_reference(window)?;
        let rest = self
            .rest_frequency_hz
            .or(rest_frequency(ms, representative_field, window)?)
            .unwrap_or(spectral_window.ref_frequency(window)?);
        let (spectral, channels) = match &self.spectral {
            ImagerSpectralAxis::Continuum(requested_frame) => {
                let chosen = if native_frame == FrequencyRef::REST {
                    *requested_frame
                } else {
                    FrequencyRef::LSRK
                };
                (continuum_axis(ms, selected, &engine, chosen, rest)?, 1)
            }
            ImagerSpectralAxis::Cube {
                channels,
                frame,
                config,
            } => {
                let mut config = config.clone();
                let report_frame = if native_frame == FrequencyRef::REST {
                    native_frame
                } else {
                    *frame
                };
                // With a single frequency plane CASA records the exact
                // frequency and spacing supplied in the definition.
                let single_frequency = match (&config.start, &config.width, channels) {
                    (
                        Some(CubeAxisValue::FrequencyHz { hz, .. }),
                        Some(CubeAxisValue::FrequencyHz { hz: delta, .. }),
                        1,
                    ) => Some((*hz, *delta)),
                    _ => None,
                };
                config.rest_frequency_hz = Some(rest);
                let data = windows
                    .into_iter()
                    .map(|id| {
                        Ok((
                            spectral_window.chan_freq(id)?,
                            spectral_window.chan_width(id)?,
                        ))
                    })
                    .collect::<MsResult<Vec<_>>>()?;
                let bands = data
                    .iter()
                    .map(|(f, w)| (f.as_slice(), w.as_slice()))
                    .collect::<Vec<_>>();
                let (frequencies, widths) = combine_spectral_windows(&bands)?;
                let spectral = CubeSpectralSetup::for_casa_cube_axis(
                    native_frame,
                    &frequencies,
                    &widths,
                    Some(*channels),
                    &config,
                    row.time_mjd_seconds,
                    field_id,
                    Some(centre),
                    &engine,
                )?;
                let values = &spectral.output_channel_frequencies_hz;
                let Some(&first) = values.first() else {
                    return Err(invalid("the model image has no spectral channels"));
                };
                let width = values
                    .get(1)
                    .map_or(spectral.output_channel_widths_hz[0], |next| next - first);
                let (first, increment) = single_frequency.unwrap_or((first, width));
                let axis = if values.len() > 1
                    && matches!(config.start, Some(CubeAxisValue::VelocityMs { .. }))
                    && config.veltype != casa_types::measures::doppler::DopplerRef::RADIO
                {
                    SpectralCoordinate::from_tabular(
                        report_frame,
                        (0..values.len()).map(|i| i as f64).collect(),
                        values.clone(),
                        first,
                        increment,
                        0.,
                        rest,
                    )
                    .map_err(invalid)?
                } else {
                    SpectralCoordinate::new(report_frame, first, increment, 0., rest)
                };
                (axis, values.len())
            }
        };
        let spectral = if native_frame == FrequencyRef::REST {
            spectral
        } else {
            spectral.with_conversion(FrequencyRef::LSRK, frame)
        };
        let shape = [self.shape[0], self.shape[1], self.stokes.len(), channels];
        let mut coordinates = CoordinateSystem::new().with_obs_info(obs);
        coordinates.add_coordinate(direction);
        coordinates.add_coordinate(StokesCoordinate::new(self.stokes.clone()));
        coordinates.add_coordinate(spectral);
        Ok(ImagerImageDefinition {
            shape,
            coordinates,
            field_id,
        })
    }
}

fn continuum_axis(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    engine: &MsCalEngine,
    chosen: FrequencyRef,
    rest: f64,
) -> MsResult<SpectralCoordinate> {
    let spectral_window = ms.spectral_window()?;
    let mut epochs = BTreeMap::<_, [f64; 2]>::new();
    for row in &selected.selected_rows {
        let time = row.time_mjd_seconds;
        let [first, last] = epochs
            .entry((row.spectral_window_id, row.field_id))
            .or_insert([time, time]);
        *first = first.min(time);
        *last = last.max(time);
    }
    let mut lower = f64::INFINITY;
    let mut upper = f64::NEG_INFINITY;
    for ((spw, field), times) in epochs {
        let id = spw as usize;
        let frequencies = spectral_window.chan_freq(id)?;
        let widths = spectral_window.chan_width(id)?;
        let selected_channels = match selected.channel_selections.get(&spw) {
            Some(selection) => selection.indices(frequencies.len())?,
            None => (0..frequencies.len()).collect(),
        };
        let frequency_frame = spectral_window.frequency_reference(id)?;
        let mut low = f64::INFINITY;
        let mut high = f64::NEG_INFINITY;
        for channel in selected_channels {
            let frequency = frequencies[channel];
            let half_width = 0.5 * widths[channel].abs();
            low = low.min(frequency - half_width);
            high = high.max(frequency + half_width);
        }
        for time in times {
            let data_frame = engine.spectral_frame_observatory(time, field as usize)?;
            let ratio = convert_frequency_to_frame_with_frame(
                frequency_frame,
                chosen,
                1.,
                Some(&data_frame),
            )?;
            lower = lower.min(low * ratio);
            upper = upper.max(high * ratio);
        }
    }
    Ok(SpectralCoordinate::new(
        chosen,
        (lower + upper) * 0.5,
        upper - lower,
        0.,
        rest,
    ))
}

fn rest_frequency(ms: &MeasurementSet, field: usize, window: usize) -> MsResult<Option<f64>> {
    let Some(table) = ms.subtable(casa_ms::SubtableId::Source) else {
        return Ok(None);
    };
    let source = casa_ms::MsSource::new(table);
    let id = ms.field()?.source_id(field)?;
    for row in 0..source.row_count() {
        let definition_window = source.i32(row, "SPECTRAL_WINDOW_ID")?;
        if source.i32(row, "SOURCE_ID")? == id
            && (definition_window == -1 || definition_window == window as i32)
            && let Some(ArrayValue::Float64(values)) =
                source.optional_array(row, "REST_FREQUENCY")?
            && let Some(value) = values.iter().next()
        {
            return Ok(Some(*value));
        }
    }
    Ok(None)
}
