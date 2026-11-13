// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reuse the saved-noise kernel for a supplied A Noise table. The table's
//! ordinary interpolator is evaluated before any DATA operation is recorded.
use super::*;
use crate::corrupt::offline::OfflineNoise;
use crate::corrupt::{NoiseApplication, NoiseLevel};

impl Simulator {
    /// M corrects the zero side of the simulator equation. Its interpolation
    /// still runs, so malformed axes/parameters fail before recording DATA.
    pub(super) fn check_baseline_identity(
        &self,
        table: &AppliedTable,
        scan_index: usize,
        time: f64,
        output_hz: &[f64],
        phase_frequency: Option<f64>,
    ) -> SimulatorResult<()> {
        let Lookup::Sampled(lookup) = &table.lookup.lookup else {
            unreachable!("M is sampled")
        };
        let scan = &self.scans[scan_index];
        let window = table
            .calibration_window(scan.spectral_window_id)
            .expect("covered table");
        for p in 0..self.antennas.len() {
            for q in p..self.antennas.len() {
                let Some(baseline) = lookup.baseline_index(p, q) else {
                    continue;
                };
                let Some(solution) = lookup
                    .simulation_solution(
                        scan.observation_id,
                        scan.scan_number,
                        window as i32,
                        baseline,
                        time,
                        phase_frequency,
                    )
                    .map_err(raised)?
                else {
                    continue;
                };
                if solution.receptors != 2 {
                    return Err(raised("M Mueller requires two parallel-hand parameters"));
                }
                if let Some(mode) = table.frequency {
                    let input_hz = table
                        .lookup
                        .frequencies
                        .get(window)
                        .map(Vec::as_slice)
                        .unwrap_or(&[]);
                    solution
                        .resample_frequency(input_hz, output_hz, mode)
                        .map_err(raised)?;
                }
            }
        }
        Ok(())
    }

    pub(in crate::session) fn applied_noise(
        &self,
        table: &AppliedTable,
        covered: &impl Fn(usize) -> bool,
    ) -> SimulatorResult<NoiseApplication> {
        Ok(NoiseApplication {
            domain: self.noise_domain(false)?,
            seed: 0,
            level: NoiseLevel::Fixed(self.applied_baseline_field(table, covered)?),
            ordinal: 0,
            baseline: Vec::new(),
        })
    }

    pub(in crate::session) fn applied_baseline_field(
        &self,
        table: &AppliedTable,
        covered: &impl Fn(usize) -> bool,
    ) -> SimulatorResult<Arc<OfflineNoise>> {
        let Lookup::Sampled(lookup) = &table.lookup.lookup else {
            unreachable!("A is sampled")
        };
        let inverse = table.kind == AppliedKind::BaselineIdentity;
        let (mut counts, _) = self.integration_counts();
        counts.resize(self.spectral_windows.len(), 0);
        for (window, count) in counts.iter_mut().enumerate() {
            if !covered(window) {
                *count = 0;
            }
        }
        let channels: Vec<_> = self
            .spectral_windows
            .iter()
            .map(|w| w.channel_count)
            .collect();
        let domain = self.noise_domain(false)?;
        let reservation = self.admit_term(TermBytes {
            kept: OfflineNoise::bytes(self.antennas.len(), &counts, &channels),
            forming: (table.lookup.max_cell_values * 128
                + channels.iter().copied().max().unwrap_or(0) * 64) as u64,
        })?;
        let mut field = OfflineNoise::new(
            self.antennas.len(),
            &counts,
            &channels,
            reservation,
            domain.metadata(),
        );
        for (index, scan) in self.scans.iter().enumerate() {
            let data_window = scan.spectral_window_id;
            if counts[data_window] == 0 {
                continue;
            }
            let window = table
                .calibration_window(data_window)
                .expect("covered table");
            let output_hz = self.spectral_windows[data_window].channel_frequencies_hz();
            let input_hz = table
                .lookup
                .frequencies
                .get(window)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            for time in self.integration_times_s(index) {
                field.push_with(
                    data_window,
                    time,
                    scan.field_id as i32,
                    index,
                    |p, q, values| {
                        if inverse {
                            values.fill(Complex32::new(1., 0.));
                        }
                        let Some(baseline) = lookup.baseline_index(p, q) else {
                            return Ok::<_, SimulatorError>(());
                        };
                        let Some(solution) = lookup
                            .simulation_solution(
                                scan.observation_id,
                                scan.scan_number,
                                window as i32,
                                baseline,
                                time,
                                None,
                            )
                            .map_err(raised)?
                        else {
                            return Ok::<_, SimulatorError>(());
                        };
                        if solution.receptors != if inverse { 2 } else { 4 } {
                            return Err(raised("invalid baseline parameter count"));
                        }
                        let solution = if let Some(mode) = table.frequency {
                            solution
                                .resample_frequency(input_hz, &output_hz, mode)
                                .map_err(raised)?
                        } else {
                            solution
                        };
                        for channel in 0..output_hz.len() {
                            for correlation in 0..4 {
                                if inverse && (correlation == 1 || correlation == 2) {
                                    // MuellerDiag2::apply zeros cross hands.
                                    values[channel * 4 + correlation] = Complex32::new(0., 0.);
                                    continue;
                                }
                                let parameter = if inverse {
                                    correlation / 3
                                } else {
                                    correlation
                                };
                                let input_channel =
                                    if solution.channels == 1 { 0 } else { channel };
                                if !solution.flags[parameter * solution.channels + input_channel] {
                                    let value = solution.value(parameter, input_channel);
                                    values[channel * 4 + correlation] = if inverse {
                                        if value.norm_sqr() == 0. {
                                            Complex32::new(1., 0.)
                                        } else {
                                            1. / value
                                        }
                                    } else {
                                        value
                                    };
                                }
                            }
                        }
                        Ok(())
                    },
                )?;
            }
        }
        Ok(Arc::new(field.sorted()))
    }
}
