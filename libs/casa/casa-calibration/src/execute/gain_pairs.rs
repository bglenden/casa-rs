// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) struct ApplyBuffer<'a> {
    pub(super) data_desc_id: i32,
    pub(super) correlation_types: &'a [i32],
    pub(super) data_channels: DataChannelGrid<'a>,
    pub(super) corrected: &'a mut ArrayD<Complex32>,
    pub(super) flags: &'a mut ArrayD<bool>,
    pub(super) propagate_flags: bool,
    pub(super) newly_flagged_samples: &'a mut usize,
}

pub(super) enum FastGainPair {
    Complex(Arc<CalibrationGrid>, Arc<CalibrationGrid>),
    Antpos {
        delay_delta_ns: f64,
        flagged: bool,
    },
    GainCurve {
        ant1: FastReceptorGains,
        ant2: FastReceptorGains,
    },
    Opacity {
        denom: Complex32,
        flagged: bool,
    },
}

#[derive(Clone, Copy)]
pub(super) struct FastReceptorGains {
    pub(super) receptor_count: usize,
    pub(super) values: [f32; FAST_GAIN_RECEPTOR_LIMIT],
    pub(super) flags: [bool; FAST_GAIN_RECEPTOR_LIMIT],
}

#[derive(Clone, Copy)]
pub(super) struct FastGainSample {
    pub(super) denom: Complex32,
    pub(super) flagged: bool,
}

impl FastReceptorGains {
    fn sample(self, receptor: usize) -> (f32, bool) {
        let receptor = receptor.min(self.receptor_count.saturating_sub(1));
        let (value, flag) = correcting_gain(
            Complex32::new(self.values[receptor], 0.),
            self.flags[receptor],
        );
        (value.re, flag)
    }
}

impl FastGainPair {
    fn requires_data_frequencies(&self) -> bool {
        matches!(self, Self::Antpos { .. })
    }

    fn scalar_sample(&self, receptors: (usize, usize)) -> Option<FastGainSample> {
        match self {
            Self::Complex(ant1, ant2) => {
                let (CalibrationGrid::Complex(ant1), CalibrationGrid::Complex(ant2)) =
                    (ant1.as_ref(), ant2.as_ref())
                else {
                    return None;
                };
                if ant1.channel_count > 1 || ant2.channel_count > 1 {
                    return None;
                }
                let receptor1 = receptors.0.min(ant1.receptor_count.saturating_sub(1));
                let receptor2 = receptors.1.min(ant2.receptor_count.saturating_sub(1));
                let (gain1, flag1) =
                    correcting_gain(ant1.value_at(receptor1, 0), ant1.flag_at(receptor1, 0));
                let (gain2, flag2) =
                    correcting_gain(ant2.value_at(receptor2, 0), ant2.flag_at(receptor2, 0));
                Some(FastGainSample {
                    denom: gain1 * gain2.conj(),
                    flagged: flag1 || flag2,
                })
            }
            Self::GainCurve { ant1, ant2 } => {
                let (gain1, flag1) = ant1.sample(receptors.0);
                let (gain2, flag2) = ant2.sample(receptors.1);
                Some(FastGainSample {
                    denom: Complex32::new(gain1 * gain2, 0.0),
                    flagged: flag1 || flag2,
                })
            }
            Self::Opacity { denom, flagged } => Some(FastGainSample {
                denom: *denom,
                flagged: *flagged,
            }),
            Self::Antpos { .. } => None,
        }
    }

    fn channel_sample(
        &self,
        receptors: (usize, usize),
        chan_index: usize,
        data_channels: DataChannelGrid<'_>,
    ) -> Option<FastGainSample> {
        match self {
            Self::Complex(ant1, ant2) => {
                let (CalibrationGrid::Complex(ant1), CalibrationGrid::Complex(ant2)) =
                    (ant1.as_ref(), ant2.as_ref())
                else {
                    return None;
                };
                if ant1.channel_count <= 1 && ant2.channel_count <= 1 {
                    return None;
                }
                let receptor1 = receptors.0.min(ant1.receptor_count.saturating_sub(1));
                let receptor2 = receptors.1.min(ant2.receptor_count.saturating_sub(1));
                let ant1_chan = if ant1.channel_count <= 1 {
                    0
                } else {
                    data_channels
                        .original_index(chan_index)
                        .min(ant1.channel_count.saturating_sub(1))
                };
                let ant2_chan = if ant2.channel_count <= 1 {
                    0
                } else {
                    data_channels
                        .original_index(chan_index)
                        .min(ant2.channel_count.saturating_sub(1))
                };
                let (gain1, flag1) = correcting_gain(
                    ant1.value_at(receptor1, ant1_chan),
                    ant1.flag_at(receptor1, ant1_chan),
                );
                let (gain2, flag2) = correcting_gain(
                    ant2.value_at(receptor2, ant2_chan),
                    ant2.flag_at(receptor2, ant2_chan),
                );
                Some(FastGainSample {
                    denom: gain1 * gain2.conj(),
                    flagged: flag1 || flag2,
                })
            }
            Self::Antpos {
                delay_delta_ns,
                flagged,
            } => {
                let frequency_hz = data_channels.frequencies_hz[chan_index];
                let phase_rad =
                    2.0 * std::f64::consts::PI * *delay_delta_ns * (frequency_hz / 1.0e9);
                Some(FastGainSample {
                    denom: Complex32::new(phase_rad.cos() as f32, phase_rad.sin() as f32),
                    flagged: *flagged,
                })
            }
            Self::GainCurve { .. } | Self::Opacity { .. } => None,
        }
    }
}

pub(super) fn apply_complex_gain_pairs_fast(
    pairs: &[FastGainPair],
    ctx: &mut ApplyBuffer<'_>,
) -> Result<(), ApplyExecutionError> {
    if pairs.is_empty() {
        return Ok(());
    }
    let shape = ctx.corrected.shape();
    let corr_count = shape[0];
    let channel_count = shape[1];
    ensure_fast_pair_frequency_coverage(pairs, ctx.data_channels.frequencies_hz, channel_count)?;
    let corrected_is_fortran = ctx.corrected.ndim() == 2 && ctx.corrected.strides()[0] == 1;
    let flags_are_fortran =
        !ctx.propagate_flags || (ctx.flags.ndim() == 2 && ctx.flags.strides()[0] == 1);
    if corrected_is_fortran
        && flags_are_fortran
        && let Some(corrected) = ctx.corrected.as_slice_memory_order_mut()
    {
        let flags = (ctx.propagate_flags)
            .then(|| ctx.flags.as_slice_memory_order_mut())
            .flatten();
        return apply_complex_gain_pairs_fast_fortran(
            pairs,
            ctx.data_desc_id,
            ctx.correlation_types,
            ctx.data_channels,
            corr_count,
            channel_count,
            corrected,
            flags,
            ctx.newly_flagged_samples,
        );
    }

    let receptors_by_corr =
        fast_pair_receptors(ctx.data_desc_id, ctx.correlation_types, corr_count)?;

    for (corr_index, &receptors) in receptors_by_corr.iter().enumerate().take(corr_count) {
        let mut scalar_denom = Complex32::new(1.0, 0.0);
        for pair in pairs {
            let Some(sample) = pair.scalar_sample(receptors) else {
                continue;
            };
            if fast_gain_sample_invalid(sample) && ctx.propagate_flags {
                flag_all_channels_generic(
                    ctx.flags,
                    corr_index,
                    channel_count,
                    ctx.newly_flagged_samples,
                );
            }
            if sample.denom != Complex32::new(0., 0.) {
                scalar_denom *= sample.denom;
            }
        }

        for chan_index in 0..channel_count {
            let mut denom = scalar_denom;
            for pair in pairs {
                let Some(sample) = pair.channel_sample(receptors, chan_index, ctx.data_channels)
                else {
                    continue;
                };
                if fast_gain_sample_invalid(sample)
                    && ctx.propagate_flags
                    && !ctx.flags[[corr_index, chan_index]]
                {
                    ctx.flags[[corr_index, chan_index]] = true;
                    *ctx.newly_flagged_samples += 1;
                }
                if sample.denom != Complex32::new(0., 0.) {
                    denom *= sample.denom;
                }
            }
            ctx.corrected[[corr_index, chan_index]] /= denom;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn apply_complex_gain_pairs_fast_fortran(
    pairs: &[FastGainPair],
    data_desc_id: i32,
    correlation_types: &[i32],
    data_channels: DataChannelGrid<'_>,
    corr_count: usize,
    channel_count: usize,
    corrected: &mut [Complex32],
    mut flags: Option<&mut [bool]>,
    newly_flagged_samples: &mut usize,
) -> Result<(), ApplyExecutionError> {
    let receptors_by_corr = fast_pair_receptors(data_desc_id, correlation_types, corr_count)?;

    for (corr_index, &receptors) in receptors_by_corr.iter().enumerate().take(corr_count) {
        let mut scalar_denom = Complex32::new(1.0, 0.0);
        for pair in pairs {
            let Some(sample) = pair.scalar_sample(receptors) else {
                continue;
            };
            if fast_gain_sample_invalid(sample)
                && let Some(flags) = flags.as_deref_mut()
            {
                flag_all_channels_fortran(
                    flags,
                    corr_index,
                    corr_count,
                    channel_count,
                    newly_flagged_samples,
                );
            }
            if sample.denom != Complex32::new(0., 0.) {
                scalar_denom *= sample.denom;
            }
        }

        for chan_index in 0..channel_count {
            let offset = corr_index + chan_index * corr_count;
            let mut denom = scalar_denom;
            for pair in pairs {
                let Some(sample) = pair.channel_sample(receptors, chan_index, data_channels) else {
                    continue;
                };
                if fast_gain_sample_invalid(sample)
                    && let Some(flags) = flags.as_deref_mut()
                    && !flags[offset]
                {
                    flags[offset] = true;
                    *newly_flagged_samples += 1;
                }
                if sample.denom != Complex32::new(0., 0.) {
                    denom *= sample.denom;
                }
            }
            corrected[offset] /= denom;
        }
    }

    Ok(())
}

pub(super) fn fast_pair_receptors(
    data_desc_id: i32,
    correlation_types: &[i32],
    corr_count: usize,
) -> Result<Vec<(usize, usize)>, ApplyExecutionError> {
    (0..corr_count)
        .map(|corr_index| {
            correlation_receptors(correlation_types[corr_index]).ok_or_else(|| {
                ApplyExecutionError::UnsupportedCorrelationLayout {
                    data_desc_id,
                    correlation_types: correlation_types
                        .iter()
                        .map(|code| stokes_name(*code).to_string())
                        .collect(),
                }
            })
        })
        .collect::<Result<Vec<_>, _>>()
}

pub(super) fn ensure_fast_pair_frequency_coverage(
    pairs: &[FastGainPair],
    data_frequencies_hz: &[f64],
    channel_count: usize,
) -> Result<(), ApplyExecutionError> {
    if pairs.iter().any(FastGainPair::requires_data_frequencies)
        && data_frequencies_hz.len() < channel_count
    {
        return Err(ApplyExecutionError::UnsupportedInterpolation {
            path: "<measurement-set SPECTRAL_WINDOW>".to_string(),
            interp: ApplyInterpolationMode::Nearest,
            reason: "data channel index is outside the MeasurementSet SPW grid".to_string(),
        });
    }
    Ok(())
}

pub(super) fn fast_gain_sample_invalid(sample: FastGainSample) -> bool {
    sample.flagged || sample.denom == Complex32::new(0.0, 0.0)
}

pub(super) fn flag_all_channels_generic(
    flags: &mut ArrayD<bool>,
    corr_index: usize,
    channel_count: usize,
    newly_flagged_samples: &mut usize,
) {
    for flag_chan_index in 0..channel_count {
        if !flags[[corr_index, flag_chan_index]] {
            flags[[corr_index, flag_chan_index]] = true;
            *newly_flagged_samples += 1;
        }
    }
}

pub(super) fn flag_all_channels_fortran(
    flags: &mut [bool],
    corr_index: usize,
    corr_count: usize,
    channel_count: usize,
    newly_flagged_samples: &mut usize,
) {
    for flag_chan_index in 0..channel_count {
        let flag_offset = corr_index + flag_chan_index * corr_count;
        if !flags[flag_offset] {
            flags[flag_offset] = true;
            *newly_flagged_samples += 1;
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_fast_gain_pair(
    table_index: usize,
    ant1: &Arc<CalibrationGrid>,
    ant2: &Arc<CalibrationGrid>,
    field_id: i32,
    antenna1_id: i32,
    antenna2_id: i32,
    time_seconds: f64,
    table: &LoadedCalibrationTable,
    geometry_cache: Option<&mut RowGeometryCache>,
) -> Result<Option<FastGainPair>, ApplyExecutionError> {
    let engine = table.engine.as_deref();
    let path = &table.path;
    let mut geometry_cache = geometry_cache;
    match (ant1.as_ref(), ant2.as_ref()) {
        (CalibrationGrid::Complex(_), CalibrationGrid::Complex(_)) => Ok(Some(
            FastGainPair::Complex(Arc::clone(ant1), Arc::clone(ant2)),
        )),
        (CalibrationGrid::Antpos(ant1), CalibrationGrid::Antpos(ant2)) => {
            let delay1_ns = fast_antpos_delay_ns(
                table_index,
                ant1,
                engine,
                field_id,
                antenna1_id,
                time_seconds,
                path,
                table.antpos_tropo.as_ref(),
                reborrow_geometry_cache(&mut geometry_cache),
            )?;
            let delay2_ns = fast_antpos_delay_ns(
                table_index,
                ant2,
                engine,
                field_id,
                antenna2_id,
                time_seconds,
                path,
                table.antpos_tropo.as_ref(),
                reborrow_geometry_cache(&mut geometry_cache),
            )?;
            Ok(Some(FastGainPair::Antpos {
                delay_delta_ns: if ant1.flagged { 0. } else { delay1_ns }
                    - if ant2.flagged { 0. } else { delay2_ns },
                flagged: ant1.flagged || ant2.flagged,
            }))
        }
        (CalibrationGrid::GainCurve(ant1), CalibrationGrid::GainCurve(ant2)) => {
            let Some(ant1) = fast_gain_curve_values(
                ant1,
                engine,
                field_id,
                antenna1_id,
                time_seconds,
                path,
                reborrow_geometry_cache(&mut geometry_cache),
            )?
            else {
                return Ok(None);
            };
            let Some(ant2) = fast_gain_curve_values(
                ant2,
                engine,
                field_id,
                antenna2_id,
                time_seconds,
                path,
                reborrow_geometry_cache(&mut geometry_cache),
            )?
            else {
                return Ok(None);
            };
            Ok(Some(FastGainPair::GainCurve { ant1, ant2 }))
        }
        (CalibrationGrid::Opacity(ant1), CalibrationGrid::Opacity(ant2)) => {
            let gain1 = fast_opacity_gain(
                ant1,
                engine,
                field_id,
                antenna1_id,
                time_seconds,
                path,
                reborrow_geometry_cache(&mut geometry_cache),
            )?;
            let gain2 = fast_opacity_gain(
                ant2,
                engine,
                field_id,
                antenna2_id,
                time_seconds,
                path,
                reborrow_geometry_cache(&mut geometry_cache),
            )?;
            let (gain1, flag1) = correcting_gain(Complex32::new(gain1, 0.), ant1.flagged);
            let (gain2, flag2) = correcting_gain(Complex32::new(gain2, 0.), ant2.flagged);
            Ok(Some(FastGainPair::Opacity {
                denom: gain1 * gain2,
                flagged: flag1 || flag2,
            }))
        }
        _ => Ok(None),
    }
}

pub(super) fn reborrow_geometry_cache<'a>(
    geometry_cache: &'a mut Option<&mut RowGeometryCache>,
) -> Option<&'a mut RowGeometryCache> {
    geometry_cache.as_mut().map(|cache| &mut **cache)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn fast_antpos_delay_ns(
    table_index: usize,
    grid: &AntposGrid,
    engine: Option<&MsCalEngine>,
    field_id: i32,
    antenna_id: i32,
    time_seconds: f64,
    path: &Path,
    tropo: Option<&TropoDelay>,
    geometry_cache: Option<&mut RowGeometryCache>,
) -> Result<f64, ApplyExecutionError> {
    let mut geometry_cache = geometry_cache;
    let Some(engine) = engine else {
        return Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: "KAntPos Jones apply requires MeasurementSet geometry".to_string(),
        });
    };
    let offset_m = [
        f64::from(grid.offsets_m[0]),
        f64::from(grid.offsets_m[1]),
        f64::from(grid.offsets_m[2]),
    ];
    let offset_bits = [
        offset_m[0].to_bits(),
        offset_m[1].to_bits(),
        offset_m[2].to_bits(),
    ];
    let antenna_index = usize::try_from(antenna_id).ok();
    if let Some(cache) = reborrow_geometry_cache(&mut geometry_cache) {
        cache.ensure_fast_term_context(time_seconds, field_id);
        if let Some(antenna_index) = antenna_index
            && let Some(delay_ns) =
                cache.cached_fast_antpos_delay(table_index, antenna_index, offset_bits)
        {
            return Ok(delay_ns);
        }
    }
    let delay_ns = antpos_delay_ns(
        engine,
        field_id,
        antenna_id,
        time_seconds,
        offset_m,
        path,
        tropo,
        reborrow_geometry_cache(&mut geometry_cache),
    )?;
    if let (Some(cache), Some(antenna_index)) =
        (reborrow_geometry_cache(&mut geometry_cache), antenna_index)
    {
        cache.remember_fast_antpos_delay(table_index, antenna_index, offset_bits, delay_ns);
    }
    Ok(delay_ns)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn antpos_delay_ns(
    engine: &MsCalEngine,
    field: i32,
    antenna: i32,
    time_seconds: f64,
    offset_m: [f64; 3],
    path: &Path,
    tropo: Option<&TropoDelay>,
    cache: Option<&mut RowGeometryCache>,
) -> Result<f64, ApplyExecutionError> {
    let field_id = usize::try_from(field).unwrap_or(usize::MAX);
    let antenna_id = usize::try_from(antenna).unwrap_or(usize::MAX);
    let failed = |source| ApplyExecutionError::MutateMeasurementSet {
        path: path.display().to_string(),
        source,
    };
    let uvw = match cache {
        Some(cache) => {
            cache.project_itrf_offset_to_uvw(engine, time_seconds, field, antenna, offset_m)
        }
        None => engine.project_itrf_offset_to_uvw(time_seconds, field_id, antenna_id, offset_m),
    }
    .map_err(failed)?;
    let tropo_m = tropo
        .map(|tropo| tropo.metres(engine, time_seconds, field_id, antenna_id))
        .transpose()
        .map_err(failed)?
        .unwrap_or(0.0);
    Ok((uvw[2] + tropo_m) / SPEED_OF_LIGHT_M_PER_S * 1.0e9)
}

pub(super) fn fast_gain_curve_values(
    grid: &GainCurveGrid,
    engine: Option<&MsCalEngine>,
    field_id: i32,
    antenna_id: i32,
    time_seconds: f64,
    path: &Path,
    geometry_cache: Option<&mut RowGeometryCache>,
) -> Result<Option<FastReceptorGains>, ApplyExecutionError> {
    if grid.receptor_count > FAST_GAIN_RECEPTOR_LIMIT {
        return Ok(None);
    }
    let Some(engine) = engine else {
        return Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: "EGainCurve apply requires MeasurementSet geometry".to_string(),
        });
    };
    // The sampled coefficients vary by calibration window and slice.
    // Only cache the geometry, which is independent of those coefficients.
    let elevation = if let Some(cache) = geometry_cache {
        cache.elevation(engine, time_seconds, field_id, antenna_id)
    } else {
        engine
            .azel(
                time_seconds,
                usize::try_from(field_id).unwrap_or(usize::MAX),
                usize::try_from(antenna_id).unwrap_or(usize::MAX),
            )
            .map(|(_az, elevation)| elevation)
    }
    .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
        path: path.display().to_string(),
        source,
    })?;
    let za_degrees = (std::f64::consts::FRAC_PI_2 - elevation).to_degrees();
    let mut values = [0.0_f32; FAST_GAIN_RECEPTOR_LIMIT];
    let mut flags = [false; FAST_GAIN_RECEPTOR_LIMIT];
    for receptor in 0..grid.receptor_count {
        (values[receptor], flags[receptor]) = grid.evaluate(receptor, za_degrees);
    }
    let gains = FastReceptorGains {
        receptor_count: grid.receptor_count,
        values,
        flags,
    };
    Ok(Some(gains))
}

pub(super) fn fast_opacity_gain(
    grid: &OpacityGrid,
    engine: Option<&MsCalEngine>,
    field_id: i32,
    antenna_id: i32,
    time_seconds: f64,
    path: &Path,
    geometry_cache: Option<&mut RowGeometryCache>,
) -> Result<f32, ApplyExecutionError> {
    let elevation = opacity_elevation(
        engine,
        field_id,
        antenna_id,
        time_seconds,
        path,
        geometry_cache,
    )?;
    Ok(opacity_gain(grid.tau, elevation))
}

pub(super) fn opacity_elevation(
    engine: Option<&MsCalEngine>,
    field_id: i32,
    antenna_id: i32,
    time_seconds: f64,
    path: &Path,
    geometry_cache: Option<&mut RowGeometryCache>,
) -> Result<f64, ApplyExecutionError> {
    let Some(engine) = engine else {
        return Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: "TOpac apply requires MeasurementSet geometry".to_string(),
        });
    };
    let elevation = if let Some(cache) = geometry_cache {
        cache.elevation(engine, time_seconds, field_id, antenna_id)
    } else {
        engine
            .azel(
                time_seconds,
                usize::try_from(field_id).unwrap_or(usize::MAX),
                usize::try_from(antenna_id).unwrap_or(usize::MAX),
            )
            .map(|(_az, elevation)| elevation)
    }
    .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
        path: path.display().to_string(),
        source,
    })?;
    Ok(elevation)
}
