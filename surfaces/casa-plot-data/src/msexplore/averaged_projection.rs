// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) struct AveragedProjectionInputs<'a> {
    pub(super) ms: &'a MeasurementSet,
    pub(super) spec: &'a MsPlotSpec,
    pub(super) calibration: Option<&'a PlotMsInput<'a>>,
    pub(super) time_bins: Option<&'a PlotTimeGroups>,
    pub(super) phase_shift: Option<plotms_phasecenter::PhaseCenterShift<'a>>,
    pub(super) averaged_row_numbers: Option<&'a [usize]>,
    pub(super) data_description: MsDataDescription<'a>,
    pub(super) selected_data_desc_ids: &'a [i32],
    pub(super) selected_field_ids: &'a [i32],
    pub(super) selected_times: &'a [f64],
    pub(super) derived_engine: Option<MsCalEngine>,
    pub(super) requested_freqframe: Option<FrequencyRef>,
    pub(super) time_reporting_frame: Option<FrequencyRef>,
    pub(super) time_bounds: HashMap<usize, plotms_ordinates::VisibilityTimeBounds>,
    pub(super) scalar_time_bounds: HashMap<usize, (f64, f64)>,
    pub(super) time_spectra: Option<HashMap<i32, TimePlotSpectrum>>,
    pub(super) uv_selections: HashMap<usize, (Vec<usize>, Vec<ChannelBin>)>,
    pub(super) clear_uv_channels: std::collections::HashSet<(usize, usize)>,
    pub(super) collapsed_uv: bool,
    pub(super) uv_channels: bool,
    pub(super) scalar_intervals: bool,
    pub(super) scalar_visibility: bool,
    pub(super) weighted_time_uvw: bool,
    pub(super) time_wavelengths: bool,
    pub(super) time_reporting_axes: bool,
    pub(super) average_statistics: bool,
}

pub(super) struct AveragedProjection<'a> {
    pub(super) inputs: AveragedProjectionInputs<'a>,
    pub(super) time_baseline_plan: Option<plotms_time_baselines::TimeBaselinePlan>,
    pub(super) buffer_integrations: Option<Vec<Option<plotms_time_baselines::Integration>>>,
    pub(super) time_uvws: Option<HashMap<usize, [f64; 3]>>,
    pub(super) time_statistics: Option<HashMap<(usize, usize), plotms_columns::AveragedWeightAxes>>,
    pub(super) report_time_frequencies: bool,
    pub(super) time_doppler: Option<DopplerRef>,
    pub(super) time_rest_frequency: Option<f64>,
    pub(super) x_projection_axis: MsAxis,
    pub(super) converted_time_frequencies: HashMap<(i32, usize, usize, u64), f64>,
    pub(super) time_spectral_engine: Option<casa_ms::MsResult<MsCalEngine>>,
    pub(super) shifted_time_row: Option<(usize, [f64; 3])>,
}

#[derive(Default)]
struct RowFrequencies {
    wavelength: Option<f64>,
    reporting: Option<f64>,
    velocity: Option<f64>,
}

struct PointGeometry {
    representative: MsScatterPointRef,
    display_channel: usize,
    shifted_time: Option<([f64; 3], casa_core::Complex32)>,
    planned_time: Option<f64>,
}

pub(super) struct ProjectedPoint {
    pub(super) point: (f64, f64),
    pub(super) representative: MsScatterPointRef,
    pub(super) flagged: bool,
    pub(super) reporting_frequency: Option<f64>,
    pub(super) metadata: Option<MsScatterPlotMsMetadata>,
}

impl AveragedProjection<'_> {
    fn row_frequencies(
        &mut self,
        group: Option<usize>,
        channel_ordinal: usize,
        planned_time: Option<f64>,
    ) -> Result<RowFrequencies, String> {
        let Self {
            inputs:
                AveragedProjectionInputs {
                    time_spectra,
                    time_reporting_frame,
                    time_bins,
                    selected_data_desc_ids,
                    selected_field_ids,
                    derived_engine,
                    ms,
                    selected_times,
                    time_bounds,
                    time_wavelengths,
                    ..
                },
            converted_time_frequencies,
            time_spectral_engine,
            time_doppler,
            time_rest_frequency,
            ..
        } = self;
        Ok(
            if let (Some(spectra), Some(frame), Some(bins), Some(group)) =
                (time_spectra, *time_reporting_frame, *time_bins, group)
            {
                let slot = bins.first_row_slot(group);
                let ddid = selected_data_desc_ids[slot];
                let spectrum = spectra
                    .get(&ddid)
                    .ok_or("Missing selected frequency grid for PlotMS time averaging")?;
                let mut frequency_at = |channel: usize, time: f64| -> Result<f64, String> {
                    let frequency = *spectrum
                        .frequencies_hz
                        .get(channel)
                        .ok_or("Missing selected channel frequency for PlotMS time averaging")?;
                    if spectrum.frame == frame {
                        return Ok(frequency);
                    }
                    use std::collections::hash_map::Entry;
                    let field = usize::try_from(selected_field_ids[slot])
                        .map_err(|_| "Invalid FIELD_ID for PlotMS time spectral conversion")?;
                    let key = (ddid, field, channel, time.to_bits());
                    match converted_time_frequencies.entry(key) {
                        Entry::Occupied(entry) => Ok(*entry.get()),
                        Entry::Vacant(entry) => {
                            let engine = match derived_engine {
                                Some(engine) => engine,
                                None => time_spectral_engine
                                    .get_or_insert_with(|| MsCalEngine::for_calibration(ms))
                                    .as_ref()
                                    .map_err(|error| error.to_string())?,
                            };
                            Ok(*entry.insert(
                                convert_frequency_to_frame(
                                    spectrum.frame,
                                    frame,
                                    frequency,
                                    time,
                                    field,
                                    engine,
                                )
                                .map_err(|error| error.to_string())?,
                            ))
                        }
                    }
                };
                let buffer_time = planned_time.unwrap_or(selected_times[slot]);
                let frequency = frequency_at(channel_ordinal, buffer_time)?;
                // CASA projects a wavelength at the individual baseline epoch;
                // Frequency, Velocity and verbose metadata use buffer row zero.
                let row_time = time_bounds
                    .get(&group)
                    .map_or(buffer_time, |bounds| bounds.buffer_integration().time);
                let wavelength_frequency = if *time_wavelengths && row_time != buffer_time {
                    frequency_at(channel_ordinal, row_time)?
                } else {
                    frequency
                };
                let velocity = time_doppler
                    .map(|definition| {
                        let rest = match *time_rest_frequency {
                            Some(rest) => rest,
                            None => frequency_at(spectrum.frequencies_hz.len() / 2, buffer_time)?,
                        };
                        velocity_ms_from_frequency_hz(frequency, rest, definition)
                            .map(|velocity| velocity / 1000.0)
                            .map_err(|error| error.to_string())
                    })
                    .transpose()?;
                RowFrequencies {
                    wavelength: Some(wavelength_frequency),
                    reporting: Some(frequency),
                    velocity,
                }
            } else {
                RowFrequencies::default()
            },
        )
    }

    fn point_geometry(
        &mut self,
        group: Option<usize>,
        channel_ordinal: usize,
        accumulator: &AveragedPointAccumulator,
    ) -> Result<Option<PointGeometry>, String> {
        let Self {
            inputs:
                AveragedProjectionInputs {
                    collapsed_uv,
                    uv_channels,
                    time_bounds,
                    time_bins,
                    selected_data_desc_ids,
                    uv_selections,
                    clear_uv_channels,
                    phase_shift,
                    data_description,
                    selected_field_ids,
                    selected_times,
                    ms,
                    scalar_intervals,
                    scalar_time_bounds,
                    ..
                },
            shifted_time_row,
            time_uvws,
            time_baseline_plan,
            buffer_integrations,
            ..
        } = self;
        if !*collapsed_uv && !accumulator.has_usable_weight() {
            return Ok(None);
        }
        let Some(mut representative) = accumulator.representative.clone() else {
            return Ok(None);
        };
        let mut display_channel = accumulator.display_channel;
        if *collapsed_uv {
            let group = group.expect("collapsed UV uses time groups");
            if time_bounds
                .get(&group)
                .and_then(plotms_ordinates::VisibilityTimeBounds::midpoint)
                .is_none()
            {
                return Ok(None);
            }
            if *uv_channels && !clear_uv_channels.contains(&(group, channel_ordinal)) {
                return Ok(None);
            }
            let time = time_bins.expect("collapsed UV uses a buffer plan");
            let ddid = selected_data_desc_ids[time.first_row_slot(group)] as usize;
            let (correlations, channels) = &uv_selections[&ddid];
            let (bins, channel) = if *uv_channels {
                (channels.len(), channel_ordinal)
            } else {
                (1, 0)
            };
            let index = plotms_row_axes::averaged_uvw_cache_index(time, group, bins, channel);
            let channel = &channels[index % channels.len()];
            representative.corr = correlations[index % correlations.len()];
            representative.chan_start = channel.start;
            representative.chan_end = channel.end;
            display_channel = channel.display_channel();
        }
        let channel_start = representative.chan_start;
        // The phase-shifting TVI is above AveragingTvi2. Use the
        // averaged baseline before projecting its complex visibilities.
        let shifted_time = if let Some(shift) = phase_shift {
            let group = group.ok_or("Missing PlotMS phasecenter time group")?;
            let bins = time_bins.expect("phasecenter time groups");
            let slot = bins.first_row_slot(group);
            let uvw = match *shifted_time_row {
                Some((previous, uvw)) if previous == group => uvw,
                _ => {
                    let ddid = usize::try_from(selected_data_desc_ids[slot])
                        .map_err(|_| "Invalid DATA_DESC_ID for PlotMS phasecenter")?;
                    let spw = data_description
                        .spectral_window_id(ddid)
                        .map_err(|error| error.to_string())?;
                    let spw = usize::try_from(spw)
                        .map_err(|_| "Invalid SPECTRAL_WINDOW_ID for PlotMS phasecenter")?;
                    let reference = bins
                        .phase_reference_slots()
                        .expect("phasecenter time reference slots")[slot];
                    let uvw = shift.prepare_row(
                        ms,
                        selected_field_ids[slot],
                        spw,
                        selected_times[reference],
                        time_uvws.as_ref().expect("phasecenter averaged UVW")[&group],
                    )?;
                    *shifted_time_row = Some((group, uvw));
                    uvw
                }
            };
            Some((uvw, shift.phase(channel_start)?))
        } else {
            None
        };
        let planned_time = group.and_then(|group| {
            if *scalar_intervals {
                scalar_time_bounds
                    .get(&group)
                    .map(|(start, end)| start + (end - start) / 2.0)
            } else if let Some(plan) = &time_baseline_plan {
                plan.center(group)
            } else if let Some(integrations) = &buffer_integrations {
                integrations[group].map(|integration| integration.time)
            } else {
                time_bounds
                    .get(&group)
                    .and_then(plotms_ordinates::VisibilityTimeBounds::midpoint)
            }
        });

        Ok(Some(PointGeometry {
            representative,
            display_channel,
            shifted_time,
            planned_time,
        }))
    }

    pub(super) fn project_point(
        &mut self,
        group: Option<usize>,
        order: Option<usize>,
        spectral_channel: Option<&plotms_spw::SpectralChannel>,
        channel_ordinal: usize,
        accumulator: AveragedPointAccumulator,
        y_projection_axis: MsAxis,
    ) -> Result<Option<ProjectedPoint>, String> {
        let Some(PointGeometry {
            representative,
            display_channel,
            shifted_time,
            planned_time,
        }) = self.point_geometry(group, channel_ordinal, &accumulator)?
        else {
            return Ok(None);
        };
        let channel_start = representative.chan_start;
        let RowFrequencies {
            wavelength: wavelength_frequency,
            reporting: time_reporting_frequency,
            velocity: time_reporting_velocity,
        } = self.row_frequencies(group, channel_ordinal, planned_time)?;
        let Self {
            inputs:
                AveragedProjectionInputs {
                    time_bins,
                    time_reporting_axes,
                    scalar_intervals,
                    averaged_row_numbers,
                    spec,
                    average_statistics,
                    weighted_time_uvw,
                    scalar_visibility,
                    calibration,
                    requested_freqframe,
                    ..
                },
            time_statistics,
            buffer_integrations,
            time_uvws,
            x_projection_axis,
            report_time_frequencies,
            ..
        } = self;
        let statistics = time_statistics
            .as_ref()
            .and_then(|statistics| statistics.get(&(group?, representative.corr)));
        let project = |axis: MsAxis, x_axis| {
            if *time_reporting_axes && axis == MsAxis::Frequency {
                time_reporting_frequency
            } else if *time_reporting_axes && axis == MsAxis::Velocity {
                time_reporting_velocity
            } else if let Some(channel) = spectral_channel
                && matches!(axis, MsAxis::Channel | MsAxis::Frequency)
            {
                Some(if axis == MsAxis::Channel {
                    channel.number as f64
                } else {
                    channel.frequency_hz
                })
            } else if axis == MsAxis::Time && (time_bins.is_some() || *scalar_intervals) {
                planned_time
            } else if axis == MsAxis::Row {
                Some(*(*averaged_row_numbers)?.get(group?)? as f64)
            } else if axis == MsAxis::Interval {
                Some(buffer_integrations.as_ref()?[group?]?.interval)
            } else if axis == MsAxis::WeightedAmplitude {
                let value = finalize_averaged_axis_value(
                    MsAxis::Amplitude,
                    &accumulator,
                    spec.averaging.scalar,
                    spec.averaging.avgtime.is_some(),
                    x_axis,
                    time_bins.is_some(),
                )?;
                Some(weight_amplitude(value, statistics?.weight))
            } else if *average_statistics && axis.is_weight_axis() {
                statistics?.value(axis, channel_start)
            } else if let Some((_, phase)) = shifted_time
                && axis.is_visibility_math()
            {
                let samples = if x_axis {
                    &accumulator.x_visibility
                } else {
                    &accumulator.y_visibility
                };
                samples.project_shifted_time(axis, phase)
            } else if *weighted_time_uvw && axis.uses_uvw() {
                let uvw = shifted_time
                    .map(|(uvw, _)| uvw)
                    .or_else(|| time_uvws.as_ref()?.get(&order?).copied())?;
                let metres = uvw_metres_coordinate(axis, uvw);
                if axis.uses_wavelengths() {
                    Some(metres_to_wavelengths(metres, wavelength_frequency?))
                } else {
                    Some(metres)
                }
            } else if *scalar_visibility && axis.is_visibility_math() {
                let samples = if x_axis {
                    &accumulator.x_visibility
                } else {
                    &accumulator.y_visibility
                };
                samples.project_scalar(axis)
            } else {
                finalize_averaged_axis_value(
                    axis,
                    &accumulator,
                    spec.averaging.scalar,
                    spec.averaging.avgtime.is_some(),
                    x_axis,
                    time_bins.is_some(),
                )
            }
        };
        let Some(x_value) = project(*x_projection_axis, true) else {
            return Ok(None);
        };
        let Some(y_value) = project(y_projection_axis, false) else {
            return Ok(None);
        };

        let (reporting_frequency, metadata) = if calibration.is_some() {
            let reporting_frequency = if *report_time_frequencies {
                time_reporting_frequency
            } else if requested_freqframe.is_some() && *x_projection_axis == MsAxis::Frequency {
                Some(x_value)
            } else if requested_freqframe.is_some() && y_projection_axis == MsAxis::Frequency {
                Some(y_value)
            } else {
                None
            };
            let metadata = MsScatterPlotMsMetadata {
                channel: spectral_channel.map_or(display_channel, |channel| channel.number),
                average_time: spec
                    .averaging
                    .avgtime
                    .and_then(|_| project(MsAxis::Time, false)),
                averaging_group: order,
            };
            (reporting_frequency, Some(metadata))
        } else {
            (None, None)
        };
        Ok(Some(ProjectedPoint {
            point: (x_value, y_value),
            representative,
            flagged: accumulator.visibility_flagged == Some(true),
            reporting_frequency,
            metadata,
        }))
    }
}
