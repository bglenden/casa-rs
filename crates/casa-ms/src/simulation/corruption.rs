// SPDX-License-Identifier: LGPL-3.0-or-later
//! Deterministic corruption of a synthetic observation's predicted
//! visibilities ([`SyntheticCorruptionState`]): per-antenna, per-receptor
//! gains (`setgain`), bandpass gains, polarization leakage (`setleakage`)
//! and additive noise (`setnoise`), each drawn from the request's seed.

use num_complex::Complex32;

use super::*;

pub(super) struct SyntheticCorruptionState {
    seed: u64,
    simplenoise_jy: f32,
    gains_by_sample: Vec<Vec<[Complex32; 2]>>,
    /// Per-antenna gains over the observation's concatenated channels.
    bandpass_gains: Vec<Vec<Complex32>>,
    total_channel_count: usize,
    leakage_terms: Vec<[Complex32; 2]>,
}

impl SyntheticCorruptionState {
    pub(super) fn new(
        config: &SyntheticCorruptionConfig,
        antenna_count: usize,
        total_channel_count: usize,
        sample_count: usize,
    ) -> Self {
        let mut rng = DeterministicRng::new(config.seed);
        let gains_by_sample = build_gain_terms(
            config.gain.as_ref(),
            config.seed,
            antenna_count,
            sample_count,
        );
        let bandpass_gains = (0..antenna_count)
            .map(|_| {
                (0..total_channel_count)
                    .map(|_| {
                        if let Some(bandpass) = &config.bandpass {
                            let amplitude = 1.0 + rng.gaussian_f32() * bandpass.amplitude[0];
                            let phase = rng.gaussian_f32() * bandpass.amplitude[1];
                            Complex32::new(amplitude * phase.cos(), amplitude * phase.sin())
                        } else {
                            Complex32::new(1.0, 0.0)
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        let leakage_terms = (0..antenna_count)
            .map(|_| {
                if let Some(leakage) = &config.leakage {
                    casa_like_leakage_terms(&mut rng, leakage)
                } else {
                    [Complex32::new(0.0, 0.0); 2]
                }
            })
            .collect();
        Self {
            seed: config.seed,
            simplenoise_jy: config
                .noise
                .as_ref()
                .map(|noise| noise.simplenoise_jy)
                .unwrap_or(0.0),
            gains_by_sample,
            bandpass_gains,
            total_channel_count,
            leakage_terms,
        }
    }

    /// Corrupt one row of `window`. Bandpass gains and noise draws are keyed
    /// by the observation-wide channel, so every window sees independent noise.
    pub(super) fn apply(
        &self,
        values: &mut [Complex32],
        antenna1: usize,
        antenna2: usize,
        window: ObservedWindow<'_>,
        sample_index: usize,
    ) {
        let channel_count = window.setup.channel_count;
        let gains = &self.gains_by_sample[sample_index % self.gains_by_sample.len()];
        let Some(correlation_count) = values.len().checked_div(channel_count) else {
            return;
        };
        if correlation_count == 0 {
            return;
        }
        for (index, value) in values.iter_mut().enumerate() {
            let channel = window.first_channel + index / correlation_count;
            let correlation = index % correlation_count;
            let logical_index = correlation * self.total_channel_count + channel;
            let baseline_gain = gains[antenna1][correlation]
                * gains[antenna2][correlation].conj()
                * self.bandpass_gains[antenna1][channel]
                * self.bandpass_gains[antenna2][channel].conj();
            *value *= baseline_gain;
            if self.simplenoise_jy > 0.0 {
                let mut rng = DeterministicRng::new(row_noise_seed(
                    self.seed,
                    sample_index,
                    antenna1,
                    antenna2,
                    logical_index,
                ));
                value.re += rng.gaussian_f32() * self.simplenoise_jy;
                value.im += rng.gaussian_f32() * self.simplenoise_jy;
            }
        }
        if channel_count > 0 && values.len() == 2 * channel_count {
            let rr_leakage =
                self.leakage_terms[antenna1][0] * self.leakage_terms[antenna2][0].conj();
            let ll_leakage =
                self.leakage_terms[antenna1][1] * self.leakage_terms[antenna2][1].conj();
            if rr_leakage != Complex32::new(0.0, 0.0) || ll_leakage != Complex32::new(0.0, 0.0) {
                for channel in 0..channel_count {
                    let rr_index = ms_data_index(0, channel, 2);
                    let ll_index = ms_data_index(1, channel, 2);
                    let rr = values[rr_index];
                    let ll = values[ll_index];
                    values[rr_index] = rr + rr_leakage * ll;
                    values[ll_index] = ll + ll_leakage * rr;
                }
            }
        }
    }
}

fn row_noise_seed(
    seed: u64,
    sample_index: usize,
    antenna1: usize,
    antenna2: usize,
    value_index: usize,
) -> u64 {
    let mut mixed = seed ^ 0x9E37_79B9_7F4A_7C15;
    mixed = mix_u64(mixed ^ sample_index as u64);
    mixed = mix_u64(mixed ^ ((antenna1 as u64) << 32) ^ antenna2 as u64);
    mix_u64(mixed ^ value_index as u64)
}

fn mix_u64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

fn build_gain_terms(
    gain: Option<&SyntheticGainCorruption>,
    seed: u64,
    antenna_count: usize,
    sample_count: usize,
) -> Vec<Vec<[Complex32; 2]>> {
    let slot_count = sample_count.max(2);
    let Some(gain) = gain else {
        return vec![vec![[Complex32::new(1.0, 0.0); 2]; antenna_count]; slot_count];
    };
    match gain.mode {
        SyntheticGainMode::Fbm => build_fbm_gain_terms(gain, seed, antenna_count, slot_count),
        SyntheticGainMode::Random => build_random_gain_terms(gain, seed, antenna_count, slot_count),
    }
}

fn build_fbm_gain_terms(
    gain: &SyntheticGainCorruption,
    seed: u64,
    antenna_count: usize,
    slot_count: usize,
) -> Vec<Vec<[Complex32; 2]>> {
    let scale = gain_amplitude_scale(gain.amplitude).min(0.9);
    let mut by_sample = vec![vec![[Complex32::new(1.0, 0.0); 2]; antenna_count]; slot_count];
    if scale == 0.0 {
        return by_sample;
    }
    let series_by_antenna = (0..antenna_count)
        .map(|antenna| {
            (0..2)
                .map(|correlation| {
                    let casa_seed = seed
                        .wrapping_add(antenna as u64)
                        .wrapping_add(correlation as u64);
                    (
                        fbm_like_series(casa_seed, slot_count, 1.1),
                        fbm_like_series(casa_seed.wrapping_mul(100), slot_count, 1.1),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    for (sample, row) in by_sample.iter_mut().enumerate() {
        for (antenna_terms, correlation_series) in row.iter_mut().zip(series_by_antenna.iter()) {
            for (term, (amplitude_series, phase_series)) in
                antenna_terms.iter_mut().zip(correlation_series.iter())
            {
                let amplitude = 1.0 + amplitude_series[sample] * scale;
                let phase = phase_series[sample] * scale * std::f32::consts::PI;
                *term = Complex32::new(amplitude * phase.cos(), amplitude * phase.sin());
            }
        }
    }
    by_sample
}

fn build_random_gain_terms(
    gain: &SyntheticGainCorruption,
    seed: u64,
    antenna_count: usize,
    slot_count: usize,
) -> Vec<Vec<[Complex32; 2]>> {
    let mut rng = DeterministicRng::new(seed);
    (0..slot_count)
        .map(|_| {
            (0..antenna_count)
                .map(|_| {
                    [
                        Complex32::new(
                            rng.gaussian_f32() * gain.amplitude[0],
                            rng.gaussian_f32() * gain.amplitude[1],
                        ),
                        Complex32::new(
                            rng.gaussian_f32() * gain.amplitude[0],
                            rng.gaussian_f32() * gain.amplitude[1],
                        ),
                    ]
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn fbm_like_series(seed: u64, sample_count: usize, beta: f32) -> Vec<f32> {
    let count = sample_count.max(2);
    let mut rng = DeterministicRng::new(seed);
    let mut series = vec![0.0_f32; count];
    let harmonics = (count / 2).max(1);
    for harmonic in 1..=harmonics {
        let phase = std::f32::consts::TAU * rng.next_unit_f64() as f32;
        let amplitude = (harmonic as f32).powf(-0.5 * beta) * rng.gaussian_f32();
        for (sample, value) in series.iter_mut().enumerate() {
            let angle =
                std::f32::consts::TAU * harmonic as f32 * sample as f32 / count as f32 + phase;
            *value += amplitude * angle.cos();
        }
    }
    let mean = series.iter().sum::<f32>() / count as f32;
    let rms = (series
        .iter()
        .map(|value| {
            let centered = *value - mean;
            centered * centered
        })
        .sum::<f32>()
        / count as f32)
        .sqrt();
    if rms > 0.0 {
        for value in &mut series {
            *value /= rms;
        }
    }
    series
}

fn gain_amplitude_scale(amplitude: [f32; 2]) -> f32 {
    (amplitude[0] * amplitude[0] + amplitude[1] * amplitude[1]).sqrt()
}

fn casa_like_leakage_terms(
    rng: &mut DeterministicRng,
    leakage: &SyntheticPolarizationLeakageCorruption,
) -> [Complex32; 2] {
    let random = Complex32::new(
        rng.gaussian_f32() * leakage.amplitude[0],
        rng.gaussian_f32() * leakage.amplitude[1],
    );
    let offset = Complex32::new(leakage.offset[0], leakage.offset[1]);
    [
        random + offset,
        random + Complex32::new(-offset.re, offset.im),
    ]
}

struct DeterministicRng {
    state: u64,
    spare_gaussian: Option<f32>,
}

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self {
            state: seed.max(1),
            spare_gaussian: None,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }

    fn next_unit_f64(&mut self) -> f64 {
        let value = self.next_u64() >> 11;
        (value as f64) * (1.0 / ((1u64 << 53) as f64))
    }

    fn gaussian_f32(&mut self) -> f32 {
        if let Some(value) = self.spare_gaussian.take() {
            return value;
        }
        let u1 = self.next_unit_f64().clamp(f64::MIN_POSITIVE, 1.0);
        let u2 = self.next_unit_f64();
        let radius = (-2.0 * u1.ln()).sqrt();
        let theta = 2.0 * std::f64::consts::PI * u2;
        let z0 = radius * theta.cos();
        let z1 = radius * theta.sin();
        self.spare_gaussian = Some(z1 as f32);
        z0 as f32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_noise_seed_varies_by_row_coordinates() {
        let base = row_noise_seed(42, 0, 0, 1, 0);
        assert_ne!(base, row_noise_seed(43, 0, 0, 1, 0));
        assert_ne!(base, row_noise_seed(42, 1, 0, 1, 0));
        assert_ne!(base, row_noise_seed(42, 0, 1, 2, 0));
        assert_ne!(base, row_noise_seed(42, 0, 0, 1, 1));
        assert_eq!(base, row_noise_seed(42, 0, 0, 1, 0));
    }
}
