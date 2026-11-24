// SPDX-License-Identifier: LGPL-3.0-or-later
//! Least-squares fitting and residual clipping shared by the baseline families.

use casa_numerics::LeastSquaresDesign;

pub(super) struct Fit {
    pub(super) coefficients: Vec<f64>,
    pub(super) residuals: Vec<f32>,
    pub(super) included: Vec<bool>,
    pub(super) rms: f32,
    pub(super) boundaries: Vec<usize>,
}

pub(super) struct LinearFit {
    pub(super) basis: Vec<Vec<f64>>,
    pub(super) terms: usize,
    // First endpoint after each segment. The sample immediately before
    // this index stays eligible in Sakura's clipping pass.
    pub(super) segment_ends: Vec<usize>,
    pub(super) failure: &'static str,
}

impl LinearFit {
    fn prepare(&self, channels: usize, included: &[bool]) -> Option<LeastSquaresDesign> {
        let basis = self
            .basis
            .iter()
            .take(channels)
            .zip(included)
            .filter_map(|(terms, &use_channel)| use_channel.then_some(terms.as_slice()));
        LeastSquaresDesign::unweighted(basis, self.terms)
    }

    pub(super) fn solve(
        &self,
        spectrum: &[f32],
        included: &[bool],
        clip_sigma: f64,
        max_fits: usize,
        mut first_design: Option<&mut Option<LeastSquaresDesign>>,
    ) -> Result<Option<Fit>, String> {
        let mut included = included.to_vec();
        for iteration in 0..max_fits {
            let observed = self
                .basis
                .iter()
                .zip(spectrum)
                .zip(&included)
                .filter_map(|((_, &value), &use_channel)| use_channel.then_some(f64::from(value)))
                .collect::<Vec<_>>();
            if observed.len() < self.terms {
                return if iteration == 0 {
                    Ok(None)
                } else {
                    Err(self.failure.into())
                };
            }
            let mut uncached = None;
            let design = first_design.take().unwrap_or(&mut uncached);
            if design.is_none() && observed.iter().all(|value| value.is_finite()) {
                *design = self.prepare(spectrum.len(), &included);
            }
            let coefficients = design
                .as_ref()
                .and_then(|design| design.solve(&observed))
                .ok_or(self.failure)?;
            let residuals = self
                .basis
                .iter()
                .zip(spectrum)
                .map(|(basis, &value)| {
                    let fit = basis
                        .iter()
                        .zip(&coefficients)
                        .map(|(value, coefficient)| value * coefficient)
                        .sum::<f64>() as f32;
                    value - fit
                })
                .collect::<Vec<_>>();
            let (sum, power) = residuals
                .iter()
                .zip(&included)
                .filter(|&(_, &use_channel)| use_channel)
                .fold((0., 0.), |(sum, power), (&value, _)| {
                    let value = f64::from(value);
                    (sum + value, power + value * value)
                });
            let count = observed.len() as f64;
            let mean = sum / count;
            let rms = (power / count - mean * mean).abs().sqrt();
            if iteration + 1 < max_fits
                && self.clip(&residuals, &mut included, mean, clip_sigma * rms)
            {
                continue;
            }
            return Ok(Some(Fit {
                coefficients,
                residuals,
                included,
                rms: rms as f32,
                boundaries: Vec::new(),
            }));
        }
        Err(self.failure.into())
    }

    fn clip(&self, residuals: &[f32], included: &mut [bool], mean: f64, radius: f64) -> bool {
        let radius = f64::from(radius as f32);
        let lower = (mean - radius) as f32;
        let upper = (mean + radius) as f32;
        let mut changed = false;
        let mut endpoints = self.segment_ends.iter().copied();
        let mut next = endpoints.next();
        for (channel, (&value, use_channel)) in residuals.iter().zip(included).enumerate() {
            if next == Some(channel + 1) {
                next = endpoints.next();
            } else if *use_channel && (value - lower) * (upper - value) < 0. {
                *use_channel = false;
                changed = true;
            }
        }
        changed
    }
}
