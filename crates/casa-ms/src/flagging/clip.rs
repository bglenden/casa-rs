// SPDX-License-Identifier: LGPL-3.0-or-later
//! Numerical clip tests for stored correlations and derived Stokes products.

use casa_types::{ArrayValue, Complex32, Complex64};
use ndarray::{ArrayD, Ix2};

use super::correlation::{ClipMetric, ClipProjection, StokesCombination};
use super::{FlaggingError, is_casa_clip, is_casa_clip_complex32, is_casa_clip_complex64};

#[derive(Debug, Clone, Copy)]
pub(super) struct ClipOptions<'a> {
    pub(super) zeros: bool,
    pub(super) bounds: Option<[f64; 2]>,
    pub(super) outside: bool,
    pub(super) projections: Option<&'a [ClipProjection]>,
    pub(super) metric: Option<ClipMetric>,
}

impl ClipMetric {
    fn apply32(self, value: Complex32) -> f32 {
        match self {
            Self::Amplitude => value.norm(),
            Self::Real => value.re,
            Self::Imaginary => value.im,
            Self::Phase => value.arg(),
            Self::SquaredMagnitude => value.norm_sqr(),
        }
    }

    fn apply64(self, value: Complex64) -> f64 {
        match self {
            Self::Amplitude => value.norm(),
            Self::Real => value.re,
            Self::Imaginary => value.im,
            Self::Phase => value.arg(),
            Self::SquaredMagnitude => value.norm_sqr(),
        }
    }
}

impl StokesCombination {
    fn apply32(self, left: Complex32, right: Complex32) -> Complex32 {
        match self {
            Self::Sum => (left + right) / 2.0,
            Self::Difference => (left - right) / 2.0,
            // Scale first to avoid overflowing the division's numerator.
            // Complex division preserves signed zero at the phase boundary.
            Self::ImaginaryDifference => ((left - right) / 2.0) / Complex32::i(),
        }
    }

    fn apply64(self, left: Complex64, right: Complex64) -> Complex64 {
        match self {
            Self::Sum => (left + right) / 2.0,
            Self::Difference => (left - right) / 2.0,
            Self::ImaginaryDifference => ((left - right) / 2.0) / Complex64::i(),
        }
    }
}

pub(super) fn apply_clip_array(
    data: &ArrayValue,
    flags: &ArrayD<bool>,
    channels: Option<&[usize]>,
    options: ClipOptions<'_>,
    path: &str,
    row: usize,
    changed: &mut Vec<(usize, usize)>,
) -> Result<(), FlaggingError> {
    let flags = flags
        .view()
        .into_dimensionality::<Ix2>()
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.to_string(),
            reason: format!("FLAG row {row} must be rank-2: {source}"),
        })?;
    let ClipOptions {
        zeros,
        bounds,
        outside,
        projections,
        metric,
    } = options;
    let matches_range = |value: f64, [lower, upper]: [f64; 2]| {
        if outside {
            value < lower || value > upper
        } else {
            value >= lower && value <= upper
        }
    };
    let clip_scalar = |value: f64| {
        is_casa_clip(value.abs(), zeros)
            || bounds.is_some_and(|bounds| matches_range(value, bounds))
    };
    match data {
        ArrayValue::Complex32(values) => clip_cells(
            values,
            flags,
            channels,
            projections,
            changed,
            |value| {
                if let Some(metric) = metric {
                    clip_scalar(f64::from(metric.apply32(value)))
                } else {
                    is_casa_clip_complex32(value, zeros)
                        || bounds
                            .is_some_and(|bounds| matches_range(f64::from(value.norm()), bounds))
                }
            },
            StokesCombination::apply32,
        ),
        ArrayValue::Complex64(values) => clip_cells(
            values,
            flags,
            channels,
            projections,
            changed,
            |value| {
                if let Some(metric) = metric {
                    clip_scalar(metric.apply64(value))
                } else {
                    is_casa_clip_complex64(value, zeros)
                        || bounds.is_some_and(|bounds| matches_range(value.norm(), bounds))
                }
            },
            StokesCombination::apply64,
        ),
        ArrayValue::Float32(values) => clip_cells(
            values,
            flags,
            channels,
            projections,
            changed,
            |value| clip_scalar(f64::from(value)),
            |combination, left, right| combination.apply32(left.into(), right.into()).re,
        ),
        ArrayValue::Float64(values) => clip_cells(
            values,
            flags,
            channels,
            projections,
            changed,
            clip_scalar,
            |combination, left, right| combination.apply64(left.into(), right.into()).re,
        ),
        _ => Ok(()),
    }
    .map_err(|source| FlaggingError::MutateFlags {
        path: path.to_string(),
        reason: format!("DATA row {row} must be rank-2: {source}"),
    })
}

/// Each unflagged stored plane affected by a matching product.
fn clip_cells<T: Copy>(
    values: &ArrayD<T>,
    flags: ndarray::ArrayView2<'_, bool>,
    channels: Option<&[usize]>,
    projections: Option<&[ClipProjection]>,
    changed: &mut Vec<(usize, usize)>,
    clip: impl Fn(T) -> bool,
    combine: impl Fn(StokesCombination, T, T) -> T,
) -> Result<(), ndarray::ShapeError> {
    let values = values.view().into_dimensionality::<Ix2>()?;
    let mut visit = |projection: ClipProjection, chan: usize| match projection {
        ClipProjection::Stored(plane) => {
            if !flags[(plane, chan)] && clip(values[(plane, chan)]) {
                changed.push((plane, chan));
            }
        }
        ClipProjection::Stokes(left, right, combination) => {
            let left_flagged = flags[(left, chan)];
            let right_flagged = flags[(right, chan)];
            if (!left_flagged || !right_flagged)
                && clip(combine(
                    combination,
                    values[(left, chan)],
                    values[(right, chan)],
                ))
            {
                if !left_flagged {
                    changed.push((left, chan));
                }
                if !right_flagged {
                    changed.push((right, chan));
                }
            }
        }
    };
    let mut visit_channels = |projection| {
        let plane_count = values.shape()[0];
        let in_bounds = match projection {
            ClipProjection::Stored(plane) => plane < plane_count,
            ClipProjection::Stokes(left, right, _) => left < plane_count && right < plane_count,
        };
        if !in_bounds {
            return;
        }
        match channels {
            Some(channels) => channels.iter().for_each(|&chan| visit(projection, chan)),
            None => (0..values.shape()[1]).for_each(|chan| visit(projection, chan)),
        }
    };
    match projections {
        Some(projections) => projections.iter().copied().for_each(visit_channels),
        None => {
            for plane in 0..values.shape()[0] {
                visit_channels(ClipProjection::Stored(plane));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::array;

    #[test]
    fn clip_skips_products_requiring_a_missing_plane() {
        let values = array![[0.0_f32, 0.0], [0.0, 0.0]].into_dyn();
        let flags = array![[false, false], [false, false]];
        let projections = [
            ClipProjection::Stored(2),
            ClipProjection::Stokes(0, 2, StokesCombination::Sum),
            ClipProjection::Stored(0),
        ];
        let mut changed = Vec::new();
        clip_cells(
            &values,
            flags.view(),
            None,
            Some(&projections),
            &mut changed,
            |value| value == 0.0,
            |combination, left, right| combination.apply32(left.into(), right.into()).re,
        )
        .unwrap();
        assert_eq!(changed, [(0, 0), (0, 1)]);
    }
}
