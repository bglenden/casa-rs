// SPDX-License-Identifier: LGPL-3.0-or-later

use super::curve::Curve;
use super::models::{LineEstimate, LineKind, ProfileModels};
use super::solver::{Sample, solve_channel_profiles};

/// A spectral line model evaluated in channel coordinates.
#[derive(Debug, Clone, Copy)]
pub enum ChannelLineShape {
    /// Gaussian amplitude, center, and full width at half maximum.
    Gaussian,
    /// Lorentzian amplitude, center, and full width at half maximum.
    Lorentzian,
}

/// Fitted spectral components in the same order as the initial estimates.
pub struct ChannelLineFit {
    /// Amplitude, center and full width at half maximum for each line.
    pub parameters: Vec<[f64; 3]>,
    /// Standard errors for each fitted parameter.
    pub errors: Vec<[f64; 3]>,
    /// The nonlinear solver reached its convergence threshold.
    pub converged: bool,
}

/// Fit a sum of line profiles on the unmasked channels of a spectrum.
pub fn fit_channel_lines(
    spectrum: &[f32],
    mask: &[bool],
    shape: ChannelLineShape,
    estimates: &[[f64; 3]],
) -> Option<ChannelLineFit> {
    if spectrum.len() != mask.len() || estimates.is_empty() {
        return None;
    }
    let kind = match shape {
        ChannelLineShape::Gaussian => LineKind::Gaussian,
        ChannelLineShape::Lorentzian => LineKind::Lorentzian,
    };
    let lines = estimates
        .iter()
        .map(|&values| LineEstimate {
            kind,
            values,
            fixed: [false; 3],
        })
        .collect::<Vec<_>>();
    let curve = Curve::new(&ProfileModels::default(), &lines, None);
    let samples = spectrum
        .iter()
        .zip(mask)
        .enumerate()
        .filter_map(|(channel, (&value, &include))| {
            include.then_some(Sample {
                x: channel as f64,
                y: f64::from(value),
                weight: 1.,
            })
        })
        .collect::<Vec<_>>();
    let fit = solve_channel_profiles(&curve, &samples);
    Some(fit.map_or_else(
        || ChannelLineFit {
            parameters: vec![[f64::NAN; 3]; estimates.len()],
            errors: vec![[f64::NAN; 3]; estimates.len()],
            converged: false,
        },
        |fit| ChannelLineFit {
            parameters: fit.values.as_chunks::<3>().0.to_vec(),
            errors: fit.errors.as_chunks::<3>().0.to_vec(),
            converged: fit.converged,
        },
    ))
}
