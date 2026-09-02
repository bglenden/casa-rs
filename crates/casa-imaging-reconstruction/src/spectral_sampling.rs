// SPDX-License-Identifier: LGPL-3.0-or-later

//! Reconstruction-owned compilation of spectral prediction stencils and
//! bounded row-local data resampling geometry.

use casa_imaging_model::{
    CompiledProblem, ReconstructionBasis, SelectedObservationSampleView,
    SelectedSpectralContribution, SelectedSpectralContributions, SelectedSpectralEvaluation,
    SpectralCovariance, SpectralEdgePolicy, SpectralKernel,
};
use smallvec::SmallVec;
use thiserror::Error;

/// One CASA fine-channel interpolation point between adjacent native channels.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct CasaLinearSample {
    output_channel: usize,
    frequency_hz: f64,
    left_factor: f64,
    right_factor: f64,
}

impl CasaLinearSample {
    pub(crate) const fn output_channel(self) -> usize {
        self.output_channel
    }

    pub(crate) const fn frequency_hz(self) -> f64 {
        self.frequency_hz
    }

    pub(crate) const fn factors(self) -> [f64; 2] {
        [self.left_factor, self.right_factor]
    }
}

/// One direct CASA row-vector interpolation point.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct CasaDirectLinearSample {
    output_channel: usize,
    left_native_channel: usize,
    right_native_channel: usize,
    frequency_hz: f64,
    left_factor: f64,
    right_factor: f64,
}

#[cfg(test)]
impl CasaDirectLinearSample {
    pub(crate) const fn output_channel(self) -> usize {
        self.output_channel
    }

    pub(crate) const fn native_channels(self) -> [usize; 2] {
        [self.left_native_channel, self.right_native_channel]
    }

    pub(crate) const fn factors(self) -> [f64; 2] {
        [self.left_factor, self.right_factor]
    }
}

#[cfg(test)]
impl From<CasaDirectLinearSample> for CasaLinearSample {
    fn from(sample: CasaDirectLinearSample) -> Self {
        Self {
            output_channel: sample.output_channel,
            frequency_hz: sample.frequency_hz,
            left_factor: sample.left_factor,
            right_factor: sample.right_factor,
        }
    }
}

/// CASA's bounded row-local frequency grid for linear cube interpolation.
///
/// Direct interpolation uses the image-channel centres. When image channels
/// are wider than native channels, CASA first constructs a synchronized fine
/// grid. A streaming consumer retains one adjacent native channel pair and a
/// cursor, so visibility, weight, and flag arrays remain row-local.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct CasaLinearGrid {
    fine_start_hz: f64,
    fine_increment_hz: f64,
    fine_channels_per_output: usize,
    output_channels: usize,
    output_increment_hz: f64,
}

impl CasaLinearGrid {
    /// Compile CASA's direct or synchronized-fine interpolation grid.
    pub(crate) fn compile(
        output_centres_hz: &[f64],
        first_native_frequency_hz: f64,
        second_native_frequency_hz: f64,
    ) -> Option<Self> {
        if output_centres_hz.len() < 2 {
            return None;
        }
        let output_increment_hz = output_centres_hz[1] - output_centres_hz[0];
        let native_increment_hz = second_native_frequency_hz - first_native_frequency_hz;
        if !output_increment_hz.is_finite()
            || output_increment_hz == 0.0
            || !native_increment_hz.is_finite()
            || native_increment_hz == 0.0
        {
            return None;
        }
        let width = output_increment_hz.abs() / native_increment_hz.abs();
        if !width.is_finite() || width <= 0.0 {
            return None;
        }
        let output_last_hz = *output_centres_hz.last()?;
        if width <= 1.0 {
            let fine_increment_hz = output_increment_hz.abs().copysign(native_increment_hz);
            let fine_start_hz = if fine_increment_hz.signum() == output_increment_hz.signum() {
                output_centres_hz[0]
            } else {
                output_last_hz
            };
            return Some(Self {
                fine_start_hz,
                fine_increment_hz,
                fine_channels_per_output: 1,
                output_channels: output_centres_hz.len(),
                output_increment_hz,
            });
        }
        let fine_channels_per_output = width.floor() as usize;
        let fine_increment_abs = output_increment_hz.abs() / fine_channels_per_output as f64;
        let first_edge_hz = output_centres_hz[0] - output_increment_hz / 2.0;
        let last_edge_hz = output_last_hz + output_increment_hz / 2.0;
        let low_edge_hz = first_edge_hz.min(last_edge_hz);
        let high_edge_hz = first_edge_hz.max(last_edge_hz);
        let fine_increment_hz = fine_increment_abs.copysign(native_increment_hz);
        let fine_start_hz = if fine_increment_hz > 0.0 {
            low_edge_hz + fine_increment_abs / 2.0
        } else {
            high_edge_hz - fine_increment_abs / 2.0
        };
        Some(Self {
            fine_start_hz,
            fine_increment_hz,
            fine_channels_per_output,
            output_channels: output_centres_hz.len(),
            output_increment_hz,
        })
    }

    pub(crate) const fn fine_channel_count(self) -> usize {
        self.fine_channels_per_output * self.output_channels
    }

    fn fine_frequency_hz(self, ordinal: usize) -> f64 {
        self.fine_start_hz + ordinal as f64 * self.fine_increment_hz
    }

    fn output_channel(self, fine_ordinal: usize) -> usize {
        let output_ordinal = fine_ordinal / self.fine_channels_per_output;
        if self.fine_increment_hz.signum() == self.output_increment_hz.signum() {
            output_ordinal
        } else {
            self.output_channels - 1 - output_ordinal
        }
    }

    /// Consume all still-unseen fine points bracketed by one ordered adjacent
    /// native pair. The cursor makes the boundary rule single-valued when a
    /// fine point lies exactly on a native centre.
    pub(crate) fn consume_pair(
        self,
        next_fine_channel: &mut usize,
        left_frequency_hz: f64,
        right_frequency_hz: f64,
    ) -> Result<SmallVec<[CasaLinearSample; 2]>, SpectralStencilError> {
        let native_increment_hz = right_frequency_hz - left_frequency_hz;
        if !left_frequency_hz.is_finite()
            || !right_frequency_hz.is_finite()
            || native_increment_hz == 0.0
            || native_increment_hz.signum() != self.fine_increment_hz.signum()
        {
            return Err(SpectralStencilError::InvalidOutputGeometry);
        }
        let mut samples = SmallVec::new();
        while *next_fine_channel < self.fine_channel_count() {
            let fine_ordinal = *next_fine_channel;
            let frequency_hz = self.fine_frequency_hz(fine_ordinal);
            let before_left = if native_increment_hz > 0.0 {
                frequency_hz < left_frequency_hz
            } else {
                frequency_hz > left_frequency_hz
            };
            if before_left {
                *next_fine_channel += 1;
                continue;
            }
            let after_right = if native_increment_hz > 0.0 {
                frequency_hz > right_frequency_hz
            } else {
                frequency_hz < right_frequency_hz
            };
            if after_right {
                break;
            }
            let right_factor =
                ((frequency_hz - left_frequency_hz) / native_increment_hz).clamp(0.0, 1.0);
            samples.push(CasaLinearSample {
                output_channel: self.output_channel(fine_ordinal),
                frequency_hz,
                left_factor: 1.0 - right_factor,
                right_factor,
            });
            *next_fine_channel += 1;
        }
        Ok(samples)
    }
}

/// Compile CASA's direct `interpVisFreq=imageFreq` row-vector samples.
///
/// CASA's direct linear branch keeps the image-frequency vector in image
/// channel order, calls `InterpolateArray1D` with `extrapolate=false`, and
/// flags out-of-range output channels. This helper preserves that vector
/// semantics while retaining only one row's selected spectral vector.
#[cfg(test)]
pub(crate) fn casa_direct_linear_samples(
    output_centres_hz: &[f64],
    input_frequencies_hz: &[f64],
) -> Result<Vec<CasaDirectLinearSample>, SpectralStencilError> {
    if output_centres_hz.is_empty()
        || input_frequencies_hz.len() < 2
        || output_centres_hz
            .iter()
            .chain(input_frequencies_hz)
            .any(|frequency| !frequency.is_finite())
    {
        return Err(SpectralStencilError::InvalidOutputGeometry);
    }
    let direction = input_frequencies_hz[input_frequencies_hz.len() - 1] - input_frequencies_hz[0];
    if direction == 0.0
        || input_frequencies_hz.windows(2).any(|pair| {
            let increment = pair[1] - pair[0];
            increment == 0.0 || increment.signum() != direction.signum()
        })
    {
        return Err(SpectralStencilError::InvalidOutputGeometry);
    }
    let mut samples = Vec::with_capacity(output_centres_hz.len());
    for (output_channel, frequency_hz) in output_centres_hz.iter().copied().enumerate() {
        let Some(mut upper) = casa_bracket(input_frequencies_hz, frequency_hz) else {
            continue;
        };
        if upper == 0 {
            upper = 1;
        }
        let lower = upper - 1;
        let lower_frequency_hz = input_frequencies_hz[lower];
        let upper_frequency_hz = input_frequencies_hz[upper];
        if (lower_frequency_hz - upper_frequency_hz).abs() <= f64::EPSILON {
            return Err(SpectralStencilError::InvalidOutputGeometry);
        }
        let right_factor = ((frequency_hz - lower_frequency_hz)
            / (upper_frequency_hz - lower_frequency_hz))
            .clamp(0.0, 1.0);
        samples.push(CasaDirectLinearSample {
            output_channel,
            left_native_channel: lower,
            right_native_channel: upper,
            frequency_hz,
            left_factor: 1.0 - right_factor,
            right_factor,
        });
    }
    Ok(samples)
}

#[cfg(test)]
fn casa_bracket(input_frequencies_hz: &[f64], frequency_hz: f64) -> Option<usize> {
    let mut lower = 0_usize;
    let mut upper = input_frequencies_hz.len() - 1;
    let mut middle = 0_usize;
    let ascending = input_frequencies_hz[upper] >= input_frequencies_hz[lower];
    while lower <= upper {
        middle = (upper + lower) / 2;
        let midval = input_frequencies_hz[middle];
        let to_left = if ascending {
            frequency_hz < midval
        } else {
            frequency_hz > midval
        };
        if to_left {
            if middle == 0 {
                break;
            }
            upper = middle - 1;
        } else {
            let to_right = if ascending {
                frequency_hz > midval
            } else {
                frequency_hz < midval
            };
            if to_right {
                middle += 1;
                lower = middle;
            } else {
                if middle == 0 {
                    return Some(0);
                }
                upper = middle - 1;
            }
        }
    }
    if middle == input_frequencies_hz.len()
        || (middle == 0 && frequency_hz != input_frequencies_hz[0])
    {
        None
    } else {
        Some(middle)
    }
}

/// Why a selected sample did or did not acquire output spectral support.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpectralStencilValidity {
    /// The valid source sample has one or more output terms.
    Mapped,
    /// Source flags or effective weight rejected the sample.
    Flagged,
    /// The valid source sample is outside the law's edge support.
    Unmapped,
}

/// A compiled sparse native-sample stencil.
///
/// Ordinary kernels use this for prediction and adjoint evaluation. CASA
/// linear cube data use the separate bounded row resampler above, which
/// interpolates visibilities, weights, and flags before gridding.
#[derive(Debug, Clone, PartialEq)]
pub struct SpectralStencilReceipt {
    contributions: SelectedSpectralContributions,
    evaluation: SelectedSpectralEvaluation,
    validity: SpectralStencilValidity,
    covariance: SpectralCovariance,
}

impl SpectralStencilReceipt {
    /// Return sparse coefficients in output-channel order.
    #[must_use]
    pub const fn contributions(&self) -> &SelectedSpectralContributions {
        &self.contributions
    }

    /// Return the source-backed trace used to compile the stencil.
    #[must_use]
    pub const fn evaluation(&self) -> SelectedSpectralEvaluation {
        self.evaluation
    }

    /// Return mapped, flagged, or edge-unmapped validity.
    #[must_use]
    pub const fn validity(&self) -> SpectralStencilValidity {
        self.validity
    }

    /// Return the covariance propagation declaration.
    #[must_use]
    pub const fn covariance(&self) -> SpectralCovariance {
        self.covariance
    }
}

/// A spectral law or geometry could not produce a coherent sparse stencil.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SpectralStencilError {
    /// Output WCS centres or boundaries were invalid.
    #[error("compiled output spectral geometry is invalid")]
    InvalidOutputGeometry,
    /// Identity sampling could not locate the selected source channel.
    #[error("identity sampling could not locate the selected source channel")]
    IdentitySourceMismatch,
    /// Integration exceeded its planner-declared sparse term bound.
    #[error("spectral integration exceeded its planner term bound")]
    PlannerTermBound,
    /// Generated terms were not a valid sparse contribution set.
    #[error("compiled spectral coefficients are invalid")]
    InvalidCoefficients,
}

/// Compile one sparse native-sample stencil from a source trace and sampling law.
pub fn compile_spectral_stencil<'a>(
    problem: &CompiledProblem,
    sample: impl Into<SelectedObservationSampleView<'a>>,
    evaluation: SelectedSpectralEvaluation,
) -> Result<SpectralStencilReceipt, SpectralStencilError> {
    let sample = sample.into();
    let law = problem.science().spectral().sampling();
    let frequency_hz = evaluation.output_frame().centre_hz();
    // Preserve paired forward support even when flags force the adjoint weight
    // to zero: MODEL_DATA refreshes every selected cell, including flagged
    // cells. A constant-basis model evaluates the same coefficient at every
    // selected frequency rather than treating source-channel ordinal as a
    // model coefficient.
    let terms = match problem.reconstruction().basis() {
        ReconstructionBasis::Constant | ReconstructionBasis::Taylor { .. } => {
            one_term(0, 1.0, frequency_hz)?
        }
        ReconstructionBasis::TaylorViaChannelMajor { .. }
        | ReconstructionBasis::ChannelLocal { .. }
        | ReconstructionBasis::JointContinuumLine { .. } => {
            channel_local_terms(problem, sample, evaluation, frequency_hz)?
        }
    };
    let contributions = SelectedSpectralContributions::new(terms)
        .ok_or(SpectralStencilError::InvalidCoefficients)?;
    let validity = if !evaluation.is_valid() {
        SpectralStencilValidity::Flagged
    } else if contributions.is_empty() {
        SpectralStencilValidity::Unmapped
    } else {
        SpectralStencilValidity::Mapped
    };
    Ok(receipt(
        contributions,
        evaluation,
        validity,
        law.covariance(),
    ))
}

fn channel_local_terms(
    problem: &CompiledProblem,
    sample: SelectedObservationSampleView<'_>,
    evaluation: SelectedSpectralEvaluation,
    frequency_hz: f64,
) -> Result<SmallVec<[SelectedSpectralContribution; 4]>, SpectralStencilError> {
    let law = problem.science().spectral().sampling();
    let spectral = problem.geometry().spectral();
    let centres = (0..spectral.output_channels())
        .map(|channel| spectral.channel_centre_hz(channel))
        .collect::<Option<Vec<_>>>()
        .ok_or(SpectralStencilError::InvalidOutputGeometry)?;
    let boundaries = (0..=spectral.output_channels())
        .map(|boundary| spectral.channel_boundary_hz(boundary))
        .collect::<Option<Vec<_>>>()
        .ok_or(SpectralStencilError::InvalidOutputGeometry)?;
    validate_axis(&centres, &boundaries)?;
    match law.kernel() {
        SpectralKernel::Identity => identity_terms(problem, sample, frequency_hz),
        SpectralKernel::Nearest => Ok(nearest_terms(&centres, &boundaries, frequency_hz)),
        SpectralKernel::Linear => Ok(linear_terms(
            &centres,
            &boundaries,
            evaluation.output_frame().boundaries_hz(),
            frequency_hz,
        )),
        SpectralKernel::Cubic => Ok(cubic_terms(&centres, frequency_hz)),
        SpectralKernel::ChannelIntegration { maximum_terms } => integration_terms(
            &boundaries,
            evaluation.output_frame().boundaries_hz(),
            law.edge_policy(),
            maximum_terms,
            frequency_hz,
        ),
    }
}

fn receipt(
    contributions: SelectedSpectralContributions,
    evaluation: SelectedSpectralEvaluation,
    validity: SpectralStencilValidity,
    covariance: SpectralCovariance,
) -> SpectralStencilReceipt {
    SpectralStencilReceipt {
        contributions,
        evaluation,
        validity,
        covariance,
    }
}

fn validate_axis(centres: &[f64], boundaries: &[f64]) -> Result<(), SpectralStencilError> {
    if centres.is_empty()
        || boundaries.len() != centres.len() + 1
        || centres
            .iter()
            .chain(boundaries)
            .any(|value| !value.is_finite() || *value <= 0.0)
    {
        return Err(SpectralStencilError::InvalidOutputGeometry);
    }
    let direction = (centres[centres.len() - 1] - centres[0]).signum();
    if centres.len() > 1
        && (direction == 0.0
            || centres
                .windows(2)
                .any(|pair| (pair[1] - pair[0]).signum() != direction))
    {
        return Err(SpectralStencilError::InvalidOutputGeometry);
    }
    Ok(())
}

fn identity_terms(
    problem: &CompiledProblem,
    sample: SelectedObservationSampleView<'_>,
    frequency_hz: f64,
) -> Result<SmallVec<[SelectedSpectralContribution; 4]>, SpectralStencilError> {
    let address = sample.address();
    if let Some(transform) = problem.visibility_transform()
        && let Some(rule) = transform.rule(sample.metadata().field_id, address.spectral_window_id)
    {
        let ordinal = rule
            .channels()
            .iter()
            .filter(|channel| channel.use_role().contributes_to_output())
            .position(|channel| channel.channel_index() == address.channel_index)
            .ok_or(SpectralStencilError::IdentitySourceMismatch)?;
        if ordinal >= problem.geometry().spectral().output_channels() {
            return Ok(SmallVec::new());
        }
        return one_term(ordinal, 1.0, frequency_hz);
    }
    let source = problem
        .inputs()
        .observation_snapshot()
        .sources()
        .iter()
        .find(|source| source.identity() == address.measurement_set)
        .ok_or(SpectralStencilError::IdentitySourceMismatch)?;
    let selection = source
        .selection()
        .spectral_windows()
        .iter()
        .find(|selection| selection.spectral_window_id() == address.spectral_window_id)
        .ok_or(SpectralStencilError::IdentitySourceMismatch)?;
    let ordinal = selection
        .channel_indices()
        .iter()
        .position(|channel| *channel == address.channel_index)
        .ok_or(SpectralStencilError::IdentitySourceMismatch)?;
    if ordinal >= problem.geometry().spectral().output_channels() {
        return Ok(SmallVec::new());
    }
    one_term(ordinal, 1.0, frequency_hz)
}

fn nearest_terms(
    centres: &[f64],
    boundaries: &[f64],
    frequency_hz: f64,
) -> SmallVec<[SelectedSpectralContribution; 4]> {
    let Some((index, _)) = centres.iter().enumerate().min_by(|(_, left), (_, right)| {
        (*left - frequency_hz)
            .abs()
            .total_cmp(&(*right - frequency_hz).abs())
    }) else {
        return SmallVec::new();
    };
    let low = boundaries[index].min(boundaries[index + 1]);
    let high = boundaries[index].max(boundaries[index + 1]);
    if !(low..=high).contains(&frequency_hz) {
        return SmallVec::new();
    }
    one_term(index, 1.0, frequency_hz).unwrap_or_default()
}

fn linear_terms(
    centres: &[f64],
    boundaries: &[f64],
    source_boundaries_hz: [f64; 2],
    frequency_hz: f64,
) -> SmallVec<[SelectedSpectralContribution; 4]> {
    if centres.len() == 1 {
        return nearest_terms(centres, boundaries, frequency_hz);
    }
    if let Some(terms) = casa_wide_channel_linear_terms(centres, source_boundaries_hz, frequency_hz)
    {
        return terms;
    }
    interpolation_interval(centres, frequency_hz)
        .and_then(|index| {
            let first = centres[index];
            let second = centres[index + 1];
            let upper = (frequency_hz - first) / (second - first);
            sparse_terms([(index, 1.0 - upper), (index + 1, upper)], frequency_hz)
        })
        .unwrap_or_default()
}

/// Compile CASA's coarse-image to native-visibility prediction stencil.
///
/// Wide-channel prediction repeats each coarse model plane across CASA's fine
/// grid and then linearly interpolates that fine grid to the native frequency.
/// It is intentionally distinct from data gridding, which interpolates
/// adjacent native visibilities, weights, and flags onto the fine grid first.
fn casa_wide_channel_linear_terms(
    output_centres_hz: &[f64],
    source_boundaries_hz: [f64; 2],
    source_frequency_hz: f64,
) -> Option<SmallVec<[SelectedSpectralContribution; 4]>> {
    let source_increment_hz = source_boundaries_hz[1] - source_boundaries_hz[0];
    let output_increment_hz = output_centres_hz.get(1)? - output_centres_hz[0];
    if output_increment_hz.abs() / source_increment_hz.abs() <= 1.0 {
        return None;
    }
    let grid = CasaLinearGrid::compile(
        output_centres_hz,
        source_frequency_hz,
        source_frequency_hz + source_increment_hz,
    )?;
    let fine_channel_count = grid.fine_channel_count();
    if fine_channel_count < 2 {
        return None;
    }
    let fine_pixel = (source_frequency_hz - grid.fine_start_hz) / grid.fine_increment_hz;
    let left = if fine_pixel <= 0.0 {
        0
    } else if fine_pixel >= (fine_channel_count - 1) as f64 {
        fine_channel_count - 2
    } else {
        fine_pixel.floor() as usize
    };
    let right = left + 1;
    let left_frequency_hz = grid.fine_frequency_hz(left);
    let right_factor = (source_frequency_hz - left_frequency_hz) / grid.fine_increment_hz;
    let mut by_output = SmallVec::<[(usize, f64); 2]>::new();
    for (fine_channel, factor) in [(left, 1.0 - right_factor), (right, right_factor)] {
        if factor == 0.0 {
            continue;
        }
        let output_channel = grid.output_channel(fine_channel);
        if let Some((_, accumulated)) = by_output
            .iter_mut()
            .find(|(channel, _)| *channel == output_channel)
        {
            *accumulated += factor;
        } else {
            by_output.push((output_channel, factor));
        }
    }

    Some(
        by_output
            .into_iter()
            .filter(|(_, factor)| *factor != 0.0)
            .filter_map(|(output_channel, factor)| {
                SelectedSpectralContribution::new(
                    u32::try_from(output_channel).ok()?,
                    factor,
                    source_frequency_hz,
                )
            })
            .collect(),
    )
}

fn cubic_terms(centres: &[f64], frequency_hz: f64) -> SmallVec<[SelectedSpectralContribution; 4]> {
    if centres.len() < 4 {
        return SmallVec::new();
    }
    let Some(interval) = interpolation_interval(centres, frequency_hz) else {
        return SmallVec::new();
    };
    let bracket = interval + 1;
    let start = if bracket > 1 && bracket < centres.len() - 1 {
        bracket - 2
    } else if bracket <= 1 {
        0
    } else {
        centres.len() - 4
    };
    let mut terms = SmallVec::<[(usize, f64); 4]>::new();
    for index in start..start + 4 {
        let mut coefficient = 1.0;
        for other in start..start + 4 {
            if other != index {
                coefficient *= (frequency_hz - centres[other]) / (centres[index] - centres[other]);
            }
        }
        terms.push((index, coefficient));
    }
    sparse_terms(terms, frequency_hz).unwrap_or_default()
}

fn integration_terms(
    output_boundaries_hz: &[f64],
    source_boundaries_hz: [f64; 2],
    edge_policy: SpectralEdgePolicy,
    maximum_terms: usize,
    frequency_hz: f64,
) -> Result<SmallVec<[SelectedSpectralContribution; 4]>, SpectralStencilError> {
    let source_low = source_boundaries_hz[0].min(source_boundaries_hz[1]);
    let source_high = source_boundaries_hz[0].max(source_boundaries_hz[1]);
    let axis_end = output_boundaries_hz[output_boundaries_hz.len() - 1];
    let axis_low = output_boundaries_hz[0].min(axis_end);
    let axis_high = output_boundaries_hz[0].max(axis_end);
    if edge_policy == SpectralEdgePolicy::CompleteSupport
        && (source_low < axis_low || source_high > axis_high)
    {
        return Ok(SmallVec::new());
    }
    let mut terms = SmallVec::<[SelectedSpectralContribution; 4]>::new();
    for (output_channel, pair) in output_boundaries_hz.windows(2).enumerate() {
        let output_low = pair[0].min(pair[1]);
        let output_high = pair[0].max(pair[1]);
        let overlap = source_high.min(output_high) - source_low.max(output_low);
        if overlap > 0.0 {
            if terms.len() == maximum_terms {
                return Err(SpectralStencilError::PlannerTermBound);
            }
            terms.push(
                SelectedSpectralContribution::new(
                    u32::try_from(output_channel)
                        .map_err(|_| SpectralStencilError::InvalidCoefficients)?,
                    overlap / (output_high - output_low),
                    frequency_hz,
                )
                .ok_or(SpectralStencilError::InvalidCoefficients)?,
            );
        }
    }
    Ok(terms)
}

fn interpolation_interval(centres: &[f64], frequency_hz: f64) -> Option<usize> {
    centres.windows(2).position(|pair| {
        pair[0].min(pair[1]) <= frequency_hz && frequency_hz <= pair[0].max(pair[1])
    })
}

fn one_term(
    output_channel: usize,
    factor: f64,
    frequency_hz: f64,
) -> Result<SmallVec<[SelectedSpectralContribution; 4]>, SpectralStencilError> {
    sparse_terms([(output_channel, factor)], frequency_hz)
        .ok_or(SpectralStencilError::InvalidCoefficients)
}

fn sparse_terms(
    terms: impl IntoIterator<Item = (usize, f64)>,
    frequency_hz: f64,
) -> Option<SmallVec<[SelectedSpectralContribution; 4]>> {
    terms
        .into_iter()
        .filter(|(_, factor)| *factor != 0.0)
        .map(|(output_channel, factor)| {
            SelectedSpectralContribution::new(
                u32::try_from(output_channel).ok()?,
                factor,
                frequency_hz,
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t35_one_channel_identity_matches_constant_basis_and_is_exactly_paired() {
        let stencil = one_term(0, 1.0, 1.4e9).expect("identity stencil");
        assert_eq!(
            stencil
                .iter()
                .map(|term| (term.output_channel(), term.factor()))
                .collect::<Vec<_>>(),
            vec![(0, 1.0)]
        );

        let model = 2.5_f64;
        let visibility = -3.0_f64;
        let weight = 4.0_f64;
        let channel_prediction = stencil
            .iter()
            .map(|term| term.factor() * model)
            .sum::<f64>();
        let channel_dirty = stencil
            .iter()
            .map(|term| term.factor() * weight * visibility)
            .sum::<f64>();
        let channel_psf = stencil
            .iter()
            .map(|term| term.factor() * weight * term.factor())
            .sum::<f64>();
        let channel_sum_weight = weight;
        assert_eq!(channel_prediction, model);
        assert_eq!(channel_dirty, weight * visibility);
        assert_eq!(channel_psf, weight);
        assert_eq!(channel_sum_weight, weight);

        let lhs = channel_prediction * visibility;
        let adjoint_visibility = stencil
            .iter()
            .map(|term| term.factor() * visibility)
            .sum::<f64>();
        let rhs = model * adjoint_visibility;
        assert_eq!(lhs, rhs, "prediction and adjoint must use the same stencil");
    }

    #[test]
    fn t35_dense_oracle_matches_sparse_linear_and_signed_cubic_stencils() {
        let centres = [10.0, 20.0, 30.0, 40.0];
        let linear = linear_terms(&centres, &[5.0, 15.0, 25.0, 35.0, 45.0], [20.0, 30.0], 25.0);
        assert_eq!(
            linear
                .iter()
                .map(|term| (term.output_channel(), term.factor()))
                .collect::<Vec<_>>(),
            vec![(1, 0.5), (2, 0.5)]
        );

        let cubic = cubic_terms(&centres, 25.0);
        let dense = cubic.iter().fold([0.0; 4], |mut dense, term| {
            dense[term.output_channel() as usize] = term.factor();
            dense
        });
        assert_eq!(dense, [-0.0625, 0.5625, 0.5625, -0.0625]);
        for degree in 0_i32..=3 {
            let reconstructed = dense
                .iter()
                .zip(centres)
                .map(|(coefficient, centre)| coefficient * centre.powi(degree))
                .sum::<f64>();
            assert!((reconstructed - 25.0_f64.powi(degree)).abs() < 1.0e-9);
        }
    }

    #[test]
    fn t40_one_channel_linear_sampling_degenerates_to_its_exact_constant_stencil() {
        let inside = linear_terms(
            &[44.001e9],
            &[44.0005e9, 44.0015e9],
            [44.0005e9, 44.0015e9],
            44.001e9,
        );
        assert_eq!(
            inside
                .iter()
                .map(|term| (term.output_channel(), term.factor()))
                .collect::<Vec<_>>(),
            vec![(0, 1.0)]
        );
        assert!(
            linear_terms(
                &[44.001e9],
                &[44.0005e9, 44.0015e9],
                [44.0015e9, 44.0025e9],
                44.002e9,
            )
            .is_empty()
        );
    }

    #[test]
    fn t41_wide_linear_channel_interior_is_not_splatted_between_coarse_centres() {
        let terms = linear_terms(
            &[132.0, 196.0],
            &[100.0, 164.0, 228.0],
            [132.0, 133.0],
            132.5,
        );
        assert_eq!(
            terms
                .iter()
                .map(|term| (term.output_channel(), term.factor()))
                .collect::<Vec<_>>(),
            vec![(0, 1.0)]
        );

        let edge = linear_terms(
            &[132.0, 196.0],
            &[100.0, 164.0, 228.0],
            [163.5, 164.5],
            164.0,
        );
        assert_eq!(
            edge.iter()
                .map(|term| (term.output_channel(), term.factor()))
                .collect::<Vec<_>>(),
            vec![(0, 0.5), (1, 0.5)]
        );
    }

    #[test]
    fn t41_wide_linear_1024_to_16_resampling_matches_casa_row_laws() {
        let output_centres = (0..16)
            .map(|channel| 1_031.75 + 64.0 * channel as f64)
            .collect::<Vec<_>>();
        let grid =
            CasaLinearGrid::compile(&output_centres, 1_000.0, 1_001.0).expect("wide CASA grid");
        assert_eq!(grid.fine_channel_count(), 1_024);

        let mut cursor = 0;
        let mut counts = [0_usize; 16];
        let mut sum_weights = [0.0_f64; 16];
        let mut dirty_numerators = [0.0_f64; 16];
        let mut flagged = 0;
        let mut first = None;
        let mut last = None;
        for native in 0..1_023 {
            for sample in grid
                .consume_pair(
                    &mut cursor,
                    1_000.0 + native as f64,
                    1_001.0 + native as f64,
                )
                .expect("ordered native pair")
            {
                first.get_or_insert(sample);
                last = Some(sample);
                let [left, right] = sample.factors();
                let left_weight = 2.0 + native as f64;
                let right_weight = left_weight + 1.0;
                let left_visibility = 3.0 * native as f64 - 7.0;
                let right_visibility = left_visibility + 3.0;
                let output = sample.output_channel();
                let is_flagged = (native == 16 || native == 17) && left > 0.0 && right > 0.0;
                if is_flagged {
                    flagged += 1;
                    continue;
                }
                let weight = left * left_weight + right * right_weight;
                let visibility = left * left_visibility + right * right_visibility;
                counts[output] += 1;
                sum_weights[output] += weight;
                dirty_numerators[output] += weight * visibility;
            }
        }

        assert_eq!(cursor, 1_023, "last out-of-support fine point stays absent");
        assert_eq!(flagged, 2, "both neighbors flag an interior interpolation");
        assert_eq!(counts[0], 62);
        assert!(counts[1..15].iter().all(|count| *count == 64));
        assert_eq!(counts[15], 63);
        assert_eq!(first.expect("first fine point").frequency_hz(), 1_000.25);
        assert_eq!(first.expect("first fine point").factors(), [0.75, 0.25]);
        assert_eq!(last.expect("last fine point").frequency_hz(), 2_022.25);

        for output in 0..16 {
            let range_end = if output == 15 { 63 } else { 64 };
            let expected = (0..range_end)
                .map(|within| 64 * output + within)
                .filter(|fine| *fine != 16 && *fine != 17)
                .fold((0.0, 0.0), |(sum_weight, dirty), fine| {
                    let weight = 2.25 + fine as f64;
                    let visibility = 3.0 * fine as f64 - 6.25;
                    (sum_weight + weight, dirty + weight * visibility)
                });
            assert_eq!(sum_weights[output], expected.0);
            assert_eq!(dirty_numerators[output], expected.1);
        }
    }

    #[test]
    fn t47_direct_linear_grid_streams_adjacent_pairs_onto_image_centres() {
        let grid = CasaLinearGrid::compile(&[1_000.0, 1_001.0, 1_002.0], 1_000.8, 1_001.8)
            .expect("direct CASA grid");
        assert_eq!(grid.fine_channel_count(), 3);
        let mut cursor = 0;
        let first = grid
            .consume_pair(&mut cursor, 1_000.8, 1_001.8)
            .expect("first native pair");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].output_channel(), 1);
        assert_eq!(first[0].frequency_hz(), 1_001.0);
        let [left, right] = first[0].factors();
        assert!((left - 0.8).abs() < 1.0e-12);
        assert!((right - 0.2).abs() < 1.0e-12);
        assert!((left + right - 1.0).abs() < f64::EPSILON);

        let second = grid
            .consume_pair(&mut cursor, 1_001.8, 1_002.8)
            .expect("second native pair");
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].output_channel(), 2);
    }

    #[test]
    fn t47_direct_linear_row_samples_match_casa_vector_edge_rules() {
        let ascending =
            casa_direct_linear_samples(&[5.0, 10.0, 15.0, 30.0, 35.0], &[10.0, 20.0, 30.0])
                .expect("ascending direct samples");
        assert_eq!(
            ascending
                .iter()
                .map(|sample| (
                    sample.output_channel(),
                    sample.native_channels(),
                    sample.factors()
                ))
                .collect::<Vec<_>>(),
            vec![
                (1, [0, 1], [1.0, 0.0]),
                (2, [0, 1], [0.5, 0.5]),
                (3, [1, 2], [0.0, 1.0]),
            ]
        );

        let descending =
            casa_direct_linear_samples(&[35.0, 30.0, 25.0, 10.0, 5.0], &[30.0, 20.0, 10.0])
                .expect("descending direct samples");
        assert_eq!(
            descending
                .iter()
                .map(|sample| (
                    sample.output_channel(),
                    sample.native_channels(),
                    sample.factors()
                ))
                .collect::<Vec<_>>(),
            vec![
                (1, [0, 1], [1.0, 0.0]),
                (2, [0, 1], [0.5, 0.5]),
                (3, [1, 2], [0.0, 1.0]),
            ]
        );
    }

    #[test]
    fn t41_wide_linear_prediction_is_explicitly_coarse_fine_to_native() {
        let output_centres = (0..16)
            .map(|channel| 1_031.75 + 64.0 * channel as f64)
            .collect::<Vec<_>>();
        let output_boundaries = (0..=16)
            .map(|boundary| 999.75 + 64.0 * boundary as f64)
            .collect::<Vec<_>>();
        let model = (0..16)
            .map(|channel| 10.0 + channel as f64)
            .collect::<Vec<_>>();
        for native in 0..1_024 {
            let frequency_hz = 1_000.0 + native as f64;
            let predicted = linear_terms(
                &output_centres,
                &output_boundaries,
                [frequency_hz - 0.5, frequency_hz + 0.5],
                frequency_hz,
            )
            .iter()
            .map(|term| term.factor() * model[term.output_channel() as usize])
            .sum::<f64>();

            let fine_pixel = frequency_hz - 1_000.25;
            let left = if fine_pixel <= 0.0 {
                0
            } else if fine_pixel >= 1_023.0 {
                1_022
            } else {
                fine_pixel.floor() as usize
            };
            let right = left + 1;
            let right_factor = fine_pixel - left as f64;
            let expected =
                model[left / 64] * (1.0 - right_factor) + model[right / 64] * right_factor;
            assert!((predicted - expected).abs() < 1.0e-12);
        }
    }

    #[test]
    fn t36_descending_partial_overlap_and_planner_bounds_are_explicit() {
        let descending = [45.0, 35.0, 25.0, 15.0, 5.0];
        let terms = integration_terms(
            &descending,
            [42.0, 28.0],
            SpectralEdgePolicy::PartialOverlap,
            2,
            35.0,
        )
        .unwrap();
        assert_eq!(
            terms
                .iter()
                .map(|term| (term.output_channel(), term.factor()))
                .collect::<Vec<_>>(),
            vec![(0, 0.7), (1, 0.7)]
        );
        assert_eq!(
            integration_terms(
                &descending,
                [42.0, 18.0],
                SpectralEdgePolicy::PartialOverlap,
                2,
                30.0,
            ),
            Err(SpectralStencilError::PlannerTermBound)
        );
    }

    #[test]
    fn t36_edge_validity_and_source_channel_order_are_deterministic() {
        assert!(
            linear_terms(
                &[30.0, 20.0, 10.0],
                &[35.0, 25.0, 15.0, 5.0],
                [30.0, 40.0],
                35.0,
            )
            .is_empty()
        );
        assert_eq!(
            linear_terms(
                &[30.0, 20.0, 10.0],
                &[35.0, 25.0, 15.0, 5.0],
                [20.0, 30.0],
                25.0,
            )
            .iter()
            .map(|term| term.output_channel())
            .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert!(cubic_terms(&[10.0, 20.0, 30.0], 15.0).is_empty());
    }

    #[cfg(feature = "cpp-interop-tests")]
    #[test]
    fn t36_nearest_linear_cubic_edge_and_covariance_match_casacore_oracles() {
        use casa_imaging_model::SpectralSamplingLaw;
        use casa_test_support::spectral_interop::{
            SpectralInterpolationMethod, SpectralInterpolationOracle,
        };

        let centres = [10.0, 20.0, 30.0, 40.0];
        let boundaries = [5.0, 15.0, 25.0, 35.0, 45.0];
        let cases = [
            (
                SpectralInterpolationMethod::Nearest,
                nearest_terms(&centres, &boundaries, 26.0),
                26.0,
            ),
            (
                SpectralInterpolationMethod::Linear,
                linear_terms(&centres, &boundaries, [20.0, 30.0], 25.0),
                25.0,
            ),
            (
                SpectralInterpolationMethod::Cubic,
                cubic_terms(&centres, 25.0),
                25.0,
            ),
        ];
        for (method, rust, coordinate) in cases {
            let casa = SpectralInterpolationOracle::coefficients(&centres, coordinate, method)
                .expect("CASA/casacore spectral coefficient oracle");
            assert!(casa.valid);
            let dense = rust.iter().fold([0.0; 4], |mut dense, term| {
                dense[term.output_channel() as usize] = term.factor();
                dense
            });
            assert_eq!(dense.as_slice(), casa.coefficients.as_slice());
        }

        for method in [
            SpectralInterpolationMethod::Nearest,
            SpectralInterpolationMethod::Linear,
            SpectralInterpolationMethod::Cubic,
        ] {
            let edge = SpectralInterpolationOracle::coefficients(&centres, 45.1, method)
                .expect("CASA/casacore edge oracle");
            assert!(!edge.valid);
            assert!(
                edge.coefficients
                    .iter()
                    .all(|coefficient| *coefficient == 0.0)
            );
        }
        assert!(nearest_terms(&centres, &boundaries, 45.1).is_empty());
        assert!(linear_terms(&centres, &boundaries, [40.1, 50.1], 45.1).is_empty());
        assert!(cubic_terms(&centres, 45.1).is_empty());

        let integrated = integration_terms(
            &boundaries,
            [12.0, 28.0],
            SpectralEdgePolicy::PartialOverlap,
            3,
            20.0,
        )
        .expect("bounded partial-overlap integration");
        assert_eq!(
            integrated
                .iter()
                .map(|term| (term.output_channel(), term.factor()))
                .collect::<Vec<_>>(),
            vec![(0, 0.3), (1, 1.0), (2, 0.3)]
        );
        assert_eq!(
            SpectralSamplingLaw::NEAREST.covariance(),
            SpectralCovariance::PropagateIndependentSourceNoise
        );
        assert_eq!(
            SpectralSamplingLaw::LINEAR.covariance(),
            SpectralCovariance::PropagateIndependentSourceNoise
        );
        assert_eq!(
            SpectralSamplingLaw::CUBIC.covariance(),
            SpectralCovariance::PropagateIndependentSourceNoise
        );
        assert_eq!(
            SpectralSamplingLaw::channel_integration(3).covariance(),
            SpectralCovariance::PropagateIndependentSourceNoise
        );
    }
}
