// SPDX-License-Identifier: LGPL-3.0-or-later
//! Per-window atmospheric emission at the target and reference elevations.

use casa_atmosphere::{
    AbsorptionProfile, AtmosphereProfile, ChannelTotals, ProfileParameters, Sky, SkyColumn,
    SpectralGrid, absorption::channel_totals,
};

use super::error_text;

const TDM_RESOLUTION: usize = 5;
const TDM_HANNING: [f64; 29] = [
    -0.00098041,
    -0.00202866,
    -0.00265951,
    -0.00222265,
    0.0,
    0.00465696,
    0.01217214,
    0.02260546,
    0.03556241,
    0.05017949,
    0.06519771,
    0.07911911,
    0.09042184,
    0.09779662,
    0.10035898,
    0.09779662,
    0.09042184,
    0.07911911,
    0.06519771,
    0.05017949,
    0.03556241,
    0.02260546,
    0.01217214,
    0.00465696,
    0.0,
    -0.00222265,
    -0.00265951,
    -0.00202866,
    -0.00098041,
];

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct GroundConditions {
    pub(super) pressure_hpa: f64,
    pub(super) temperature_k: f64,
    pub(super) humidity_percent: f64,
    pub(super) water_m: f64,
}

#[derive(Clone, Copy)]
pub(super) enum SpectralResponse {
    Channel,
    Triangle,
    Tdm,
}

pub(super) struct WindowModel {
    grid: SpectralGrid,
    response: SpectralResponse,
    gain: f64,
    threads: usize,
    columns: Option<AtmosphericColumns>,
    correction: Option<(f64, Vec<f64>)>,
}

struct AtmosphericColumns {
    conditions: GroundConditions,
    by_channel: Vec<SkyColumn>,
    zenith_opacity: Vec<f64>,
}

impl WindowModel {
    pub(super) fn new(
        frequencies: &[f64],
        width: f64,
        response: SpectralResponse,
        gain: f64,
        threads: usize,
    ) -> Result<Self, String> {
        if frequencies.is_empty() {
            return Err("Atmospheric correction requires a nonempty spectral window.".into());
        }
        if !gain.is_finite() {
            return Err("Atmospheric correction requires a finite gain factor.".into());
        }
        let mut channels = frequencies.len();
        let mut reference = (channels - 1) / 2;
        let mut center = frequencies[reference];
        let mut spacing = if channels == 1 {
            width
        } else {
            (frequencies[channels - 1] - frequencies[0]) / (channels - 1) as f64
        };
        if matches!(response, SpectralResponse::Tdm) {
            spacing /= TDM_RESOLUTION as f64;
            channels *= TDM_RESOLUTION;
            let offset = (TDM_RESOLUTION - 1) / 2;
            reference = reference * TDM_RESOLUTION + offset;
            center += offset as f64 * spacing;
        }
        let mut grid = SpectralGrid::new();
        grid.add_regular(channels, reference, center, spacing)
            .map_err(error_text)?;
        Ok(Self {
            grid,
            response,
            gain,
            threads,
            columns: None,
            correction: None,
        })
    }

    pub(super) fn correction(
        &mut self,
        time: f64,
        conditions: GroundConditions,
        profile: &ProfileParameters,
        layers: &[(f64, f64)],
        elevation: [f64; 2],
    ) -> Result<&[f64], String> {
        if self
            .correction
            .as_ref()
            .is_none_or(|(cached, _)| *cached != time)
        {
            self.update_correction(time, conditions, profile, layers, elevation)?;
        }
        Ok(&self.correction.as_ref().expect("computed correction").1)
    }

    fn update_correction(
        &mut self,
        time: f64,
        conditions: GroundConditions,
        profile: &ProfileParameters,
        layers: &[(f64, f64)],
        [on, off]: [f64; 2],
    ) -> Result<(), String> {
        if self
            .columns
            .as_ref()
            .is_none_or(|cached| cached.conditions != conditions)
        {
            self.columns = Some(AtmosphericColumns::build(
                conditions,
                profile,
                layers,
                &self.grid,
                self.threads,
            )?);
        }
        let columns = self.columns.as_ref().expect("prepared atmospheric columns");
        let on_airmass = 1. / on.sin();
        let off_airmass = 1. / off.sin();
        if !(on_airmass.is_finite()
            && on_airmass >= 1.
            && off_airmass.is_finite()
            && off_airmass >= 1.)
        {
            return Err("Atmospheric correction requires elevations above the horizon.".into());
        }
        let raw = columns
            .by_channel
            .iter()
            .zip(&columns.zenith_opacity)
            .map(|(column, &opacity)| {
                (column.trj(on_airmass) - column.trj(off_airmass)) * (opacity * off_airmass).exp()
            })
            .collect::<Vec<_>>();
        let mut correction = match self.response {
            SpectralResponse::Channel => raw,
            SpectralResponse::Triangle => (0..raw.len())
                .map(|i| response_at(&raw, &[0.25, 0.5, 0.25], i))
                .collect(),
            SpectralResponse::Tdm => (0..raw.len())
                .step_by(TDM_RESOLUTION)
                .map(|i| response_at(&raw, &TDM_HANNING, i))
                .collect(),
        };
        for value in &mut correction {
            *value *= self.gain;
            if !value.is_finite() || value.abs() > f64::from(f32::MAX) {
                return Err("Atmospheric correction is outside the finite Float32 range.".into());
            }
        }
        self.correction = Some((time, correction));
        Ok(())
    }
}

impl AtmosphericColumns {
    fn build(
        conditions: GroundConditions,
        base: &ProfileParameters,
        layers: &[(f64, f64)],
        grid: &SpectralGrid,
        threads: usize,
    ) -> Result<Self, String> {
        let profile = AtmosphereProfile::with_temperature_levels(
            &ProfileParameters {
                ground_pressure_hpa: conditions.pressure_hpa,
                ground_temperature_k: conditions.temperature_k,
                relative_humidity_percent: conditions.humidity_percent,
                ..base.clone()
            },
            layers,
        )
        .map_err(error_text)?;
        let sky = Sky::zenith(conditions.water_m);
        let optical = prepare_absorption(profile, grid, threads)?;
        let by_channel = (0..grid.total_channels())
            .map(|channel| optical.sky_column(channel, &sky))
            .collect::<Result<Vec<_>, _>>()
            .map_err(error_text)?;
        let zenith_opacity = by_channel.iter().map(SkyColumn::zenith_opacity).collect();
        Ok(Self {
            conditions,
            by_channel,
            zenith_opacity,
        })
    }
}

fn prepare_absorption(
    profile: AtmosphereProfile,
    grid: &SpectralGrid,
    threads: usize,
) -> Result<AbsorptionProfile<ChannelTotals>, String> {
    // Independent channel calculations preserve the grid and summation order.
    let threads = threads.min(grid.total_channels().div_ceil(128)).max(1);
    if threads == 1 {
        return Ok(AbsorptionProfile::new_totals(profile, grid.clone()));
    }
    let frequencies = grid.frequencies_hz().collect::<Vec<_>>();
    let channels = std::thread::scope(|scope| {
        let profile = &profile;
        frequencies
            .chunks(frequencies.len().div_ceil(threads))
            .map(|points| {
                scope.spawn(move || {
                    points
                        .iter()
                        .map(|&frequency| channel_totals(profile, frequency))
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|task| task.join().map_err(|_| "Atmosphere channel worker failed."))
            .collect::<Result<Vec<_>, _>>()
    })?
    .into_iter()
    .flatten()
    .collect();
    AbsorptionProfile::from_channels(profile, grid.clone(), channels).map_err(error_text)
}

fn response_at(values: &[f64], kernel: &[f64], center: usize) -> f64 {
    let half = kernel.len() / 2;
    let start = center.saturating_sub(half);
    let stop = (center + half + 1).min(values.len());
    let weights = &kernel[start + half - center..stop + half - center];
    let result = values[start..stop]
        .iter()
        .zip(weights)
        .map(|(value, weight)| value * weight)
        .sum::<f64>();
    if weights.len() == kernel.len() {
        result
    } else {
        result / weights.iter().sum::<f64>()
    }
}
