// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA DelayFFT's coherent sum over solve buffers.

use std::collections::BTreeMap;
use std::f64::consts::TAU;

use crate::solve::{GainSolveError, GainSolveMode, control::SolveControl};

use super::{Complex32, DelaySolutions, DelaySpectrum, DelayWindow, Fft2, constrain, delay_error};

type Buffer<'a> = (DelayWindow, Vec<DelaySpectrum<'a>>);

pub(super) fn solve(
    buffers: Vec<Buffer<'_>>,
    reference: i32,
    mode: GainSolveMode,
    cross_hand: bool,
    legacy: bool,
    control: SolveControl<'_>,
) -> Result<DelaySolutions, GainSolveError> {
    control.check()?;
    let layout = CombinedSpectrum::new(&buffers, cross_hand || legacy)?;
    if cross_hand {
        let mut spectrum = layout.empty_spectrum()?;
        for (_index, (window, rows)) in buffers.iter().enumerate() {
            control.check()?;
            layout.add(
                *window,
                cross_samples(*window, rows, mode, control)?,
                &mut spectrum,
            )?;
            #[cfg(feature = "solve-test-hooks")]
            control.observe(crate::solve_test_hooks::Checkpoint::DelayBufferFinished(
                _index,
            ));
        }
        control.check()?;
        return Ok(BTreeMap::from([(
            0,
            [Some(layout.peak(&spectrum).unwrap_or(0.)), Some(0.)],
        )]));
    }
    let mut spectra = BTreeMap::new();
    for (window, rows) in &buffers {
        for row in rows.iter().filter(|row| row.parallel_hand) {
            control.check()?;
            let (a, b) = row.antennas;
            let antenna = match (a == reference, b == reference) {
                (true, false) => b,
                (false, true) => a,
                _ => continue,
            };
            let target = match spectra.entry((antenna, row.receptor)) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(layout.empty_spectrum()?)
                }
            };
            let samples = row.channels().map(|(value, weight)| {
                let (mean, weight) = constrain(value, weight, mode);
                if legacy {
                    let amplitude = mean.norm();
                    if amplitude < f32::EPSILON {
                        mean
                    } else {
                        mean / amplitude
                    }
                } else {
                    mean * weight
                }
            });
            layout.add(*window, samples, target)?;
        }
    }
    let mut results = BTreeMap::from([(reference, [Some(0.); 2])]);
    for ((antenna, receptor), spectrum) in spectra {
        control.check()?;
        let sign = if antenna > reference { -1. } else { 1. };
        results.entry(antenna).or_default()[receptor] =
            layout.peak(&spectrum).map(|delay| sign * delay);
    }
    Ok(results)
}

fn cross_samples(
    window: DelayWindow,
    rows: &[DelaySpectrum<'_>],
    mode: GainSolveMode,
    control: SolveControl<'_>,
) -> Result<std::vec::IntoIter<Complex32>, GainSolveError> {
    let mut pairs = BTreeMap::<_, [Option<&DelaySpectrum<'_>>; 2]>::new();
    for row in rows.iter().filter(|row| !row.parallel_hand) {
        pairs.entry(row.antennas).or_default()[usize::from(row.correlation != 1)] = Some(row);
    }
    let mut output = vec![Complex32::default(); window.channels];
    for [xy, yx] in pairs.into_values() {
        control.check()?;
        let (Some(xy), Some(yx)) = (xy, yx) else {
            continue;
        };
        // CrossDelayFFT balances flags across the two cross hands.
        for (out, ((a, wa), (b, wb))) in output.iter_mut().zip(xy.channels().zip(yx.channels())) {
            if wa > 0. && wb > 0. {
                let (a, wa) = constrain(a, wa, mode);
                let (b, wb) = constrain(b, wb, mode);
                *out += a * wa + (b * wb).conj();
            }
        }
    }
    Ok(output.into_iter())
}

struct CombinedSpectrum {
    origin_ghz: f64,
    spacing_ghz: f64,
    padded_ghz: f64,
    size: usize,
    wrap_before_shift: bool,
}

impl CombinedSpectrum {
    fn new(buffers: &[Buffer<'_>], wrap_before_shift: bool) -> Result<Self, GainSolveError> {
        let spacing = buffers.iter().map(|(window, _)| window.spacing_ghz);
        let bandwidth = if wrap_before_shift {
            let low = buffers
                .iter()
                .map(|(window, _)| window.start_ghz)
                .fold(f64::INFINITY, f64::min);
            let high = buffers
                .iter()
                .map(|(window, _)| window.start_ghz + window.channels as f64 * window.spacing_ghz)
                .fold(f64::NEG_INFINITY, f64::max);
            high - low
        } else {
            let high = buffers
                .iter()
                .map(|(window, _)| window.highest_ghz)
                .fold(f64::NEG_INFINITY, f64::max);
            let low = buffers
                .iter()
                .map(|(window, _)| window.lowest_ghz)
                .fold(f64::INFINITY, f64::min);
            let widest = spacing.clone().map(f64::abs).fold(0., f64::max);
            (2. + (high - low) / widest).floor() * widest
        };
        if !wrap_before_shift
            && spacing
                .clone()
                .any(|step| step.signum() != buffers[0].0.spacing_ghz.signum())
        {
            return Err(delay_error(
                "multi-window antenna delays require matching sidebands",
            ));
        }
        let spacing_ghz = if wrap_before_shift {
            spacing.fold(f64::INFINITY, f64::min)
        } else {
            spacing
                .min_by(|lhs, rhs| lhs.abs().total_cmp(&rhs.abs()))
                .expect("solve buffers")
        };
        let padded_ghz = bandwidth * 8.;
        let size = fft_size(padded_ghz, spacing_ghz)?;
        Ok(Self {
            origin_ghz: buffers[0].0.start_ghz,
            spacing_ghz,
            padded_ghz,
            size,
            wrap_before_shift,
        })
    }

    fn empty_spectrum(&self) -> Result<Vec<Complex32>, GainSolveError> {
        zeros(self.size)
    }

    fn add(
        &self,
        window: DelayWindow,
        samples: impl Iterator<Item = Complex32>,
        target: &mut [Complex32],
    ) -> Result<(), GainSolveError> {
        let size = fft_size(self.padded_ghz, window.spacing_ghz)?;
        if size < window.channels
            || size > self.size
            || !self.wrap_before_shift && !self.size.is_multiple_of(size)
        {
            return Err(delay_error(
                "combined delay windows have incompatible channel widths",
            ));
        }
        let mut input = zeros(size)?;
        for (bin, value) in input[..window.channels].iter_mut().zip(samples) {
            *bin = value;
        }
        Fft2::with_threads([size, 1], 1)
            .map_err(delay_error)?
            .with_estimated_plan()
            .transform(&mut input, false)
            .map_err(delay_error)?;
        let shift = TAU * (self.origin_ghz - window.start_ghz) / window.spacing_ghz / size as f64;
        for (index, total) in target.iter_mut().enumerate() {
            let mut phase_index = if self.wrap_before_shift {
                index % size
            } else {
                index
            } as f64;
            if !self.wrap_before_shift && index >= self.size / 2 {
                phase_index -= self.size as f64;
            }
            let phase = shift * phase_index;
            let rotation = Complex32::new(phase.cos() as f32, phase.sin() as f32);
            *total += input[index % size] * rotation;
        }
        Ok(())
    }

    fn peak(&self, spectrum: &[Complex32]) -> Option<f32> {
        let amp = |bin: usize| spectrum[bin].norm();
        let bin = (0..self.size)
            .max_by(|&lhs, &rhs| amp(lhs).total_cmp(&amp(rhs)).then_with(|| rhs.cmp(&lhs)))?;
        let middle = amp(bin);
        let left = amp((bin + self.size - 1) % self.size);
        let right = amp((bin + 1) % self.size);
        let curvature = left - 2. * middle + right;
        if middle <= 0. || curvature == 0. {
            return None;
        }
        let mut cycles = (bin as f32 + 0.5 - (right - middle) / curvature) / self.size as f32;
        if cycles > 0.5 {
            cycles -= 1.;
        }
        Some(cycles / self.spacing_ghz as f32)
    }
}

fn fft_size(padded_ghz: f64, spacing_ghz: f64) -> Result<usize, GainSolveError> {
    let size = (padded_ghz / spacing_ghz).abs().round();
    if !(1.0..=f64::from(i32::MAX)).contains(&size) {
        return Err(delay_error(
            "combined delay spectrum has an invalid FFT extent",
        ));
    }
    Ok(size as usize)
}

fn zeros(size: usize) -> Result<Vec<Complex32>, GainSolveError> {
    let mut values = Vec::new();
    values.try_reserve_exact(size).map_err(delay_error)?;
    values.resize(size, Complex32::default());
    Ok(values)
}

#[cfg(all(test, feature = "solve-test-hooks"))]
mod cancellation_tests {
    use super::*;
    use crate::solve_test_hooks::{Checkpoint, with_observer};
    use casa_core::cancellation::Cancellation;
    use std::sync::{Arc, Mutex};

    #[test]
    fn a_cancelled_cross_hand_fit_does_not_transform_the_next_buffer() {
        let window = DelayWindow {
            start_ghz: 1.0,
            spacing_ghz: 0.001,
            lowest_ghz: 1.0,
            highest_ghz: 1.003,
            channels: 4,
            correlations: 4,
        };
        let cancellation = Cancellation::new();
        let signal = cancellation.clone();
        let finished = Arc::new(Mutex::new(Vec::new()));
        let observed = finished.clone();
        let result = with_observer(
            move |event| {
                if let Checkpoint::DelayBufferFinished(index) = event {
                    observed.lock().unwrap().push(index);
                    signal.cancel();
                }
            },
            false,
            || {
                solve(
                    vec![(window, Vec::new()), (window, Vec::new())],
                    0,
                    GainSolveMode::AmplitudePhase,
                    true,
                    false,
                    SolveControl(Some(&cancellation)),
                )
            },
        );
        assert!(matches!(result, Err(GainSolveError::Interrupted)));
        assert_eq!(*finished.lock().unwrap(), [0]);
    }
}
