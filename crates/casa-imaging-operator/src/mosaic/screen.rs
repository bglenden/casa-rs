// SPDX-License-Identifier: LGPL-3.0-or-later
//! A mosaic set's screens and cells: each dish pair's beam products on the
//! screen and their transforms (`HetArrayConvFunc::findConvFunction`), the
//! weight plane's support (`supportAndNormalizeLatt`), and the crop,
//! normalisation and Lanczos resampling (`resample`, `interpLanczos`) that
//! make the cells.

use casa_numerics::{BeamPattern, FrequencyPattern};
use num_complex::{Complex32, Complex64};

use super::MOSAIC_OVERSAMPLING;
use crate::convolution::DenseCell;
use crate::dense::{OversampledPlane, dense_cell};
use crate::error::OperatorError;
use crate::fft::PlaneFft;

/// `HetArrayConvFunc::supportAndNormalizeLatt`: the support ends where the
/// weight plane falls below this fraction of its peak along both axes.
const SUPPORT_CUT_LEVEL: f32 = 2.5e-2;
/// `HetArrayConvFunc::interpLanczos`: the Lanczos window order.
pub(super) const LANCZOS_A: i64 = 3;

/// The quarter-cropped imaging and weight lattices of one pair at one
/// frequency, `side × side`, x fastest, origin at `side/2`.
#[derive(Debug)]
pub(super) struct PairLattices {
    pub(super) imaging: Vec<Complex32>,
    pub(super) weight: Vec<Complex32>,
}

/// `findConvFunction`'s screens for one dish pair at `frequency_hz`:
/// `VP_k · VP_j` and `PB_k · PB_j` over the centred screen
/// (`PBMath1D::apply` with `iPower` 1 and 2), transformed and
/// quarter-cropped to `convSize/4`. With conjugate beams at
/// `conjugate_hz` (`findConvFunction` on a one-channel image with
/// `getConjConvFunc`) the weight screen is the imaging one times the
/// voltage patterns there, `VP_k(ν) VP_j(ν) VP_k(ν_c) VP_j(ν_c)`.
pub(super) fn screen_pair(
    fft: &mut PlaneFft<f64>,
    screen: &mut [Complex64],
    conv_size: usize,
    increment_deg: [f64; 2],
    dishes: [&(BeamPattern, f64); 2],
    frequency_hz: f64,
    conjugate_hz: Option<f64>,
) -> Result<PairLattices, OperatorError> {
    let centre = conv_size / 2;
    // `PBMath1D::apply`: squared pixel offsets in degrees² held as Float.
    let axis = |increment: f64| {
        (0..conv_size)
            .map(|index| (increment * (index as f64 - centre as f64)).powi(2) as f32)
            .collect::<Vec<_>>()
    };
    let rx2 = axis(increment_deg[0]);
    let ry2 = axis(increment_deg[1]);
    // `PBMath1DEVLA` takes the fit of each beam frequency
    // (`nearestVPArray`); an Airy table serves every frequency.
    let at = |hz: f64| dishes.map(|dish| PatternAt::new(dish, hz));
    let [first, second] = at(frequency_hz);
    let conjugate = conjugate_hz.map(at);
    let mut imaging = vec![Complex32::default(); conv_size * conv_size];
    let mut weight = vec![Complex32::default(); conv_size * conv_size];
    for (iy, ry2) in ry2.iter().enumerate() {
        for (ix, rx2) in rx2.iter().enumerate() {
            let r2 = rx2 + ry2;
            let vk = first.voltage(r2);
            let vj = second.voltage(r2);
            let index = iy * conv_size + ix;
            imaging[index] = Complex32::new(vk * vj, 0.0);
            weight[index] = Complex32::new(
                match &conjugate {
                    None => (vk * vk) * (vj * vj),
                    Some([ck, cj]) => ((vk * vj) * ck.voltage(r2)) * cj.voltage(r2),
                },
                0.0,
            );
        }
    }
    let lattice = conv_size / 4;
    let quarter = lattice * 3 / 2;
    let mut transform = |values: &[Complex32]| -> Result<Vec<Complex32>, OperatorError> {
        for (target, value) in screen.iter_mut().zip(values) {
            *target = Complex64::new(f64::from(value.re), f64::from(value.im));
        }
        fft.transform(screen, false)?;
        let mut cropped = Vec::with_capacity(lattice * lattice);
        for y in 0..lattice {
            for x in 0..lattice {
                let value = screen[(quarter + y) * conv_size + quarter + x];
                cropped.push(Complex32::new(value.re as f32, value.im as f32));
            }
        }
        Ok(cropped)
    };
    Ok(PairLattices {
        imaging: transform(&imaging)?,
        weight: transform(&weight)?,
    })
}

/// A dish's voltage pattern at one frequency, as `PBMath1D::apply` looks it
/// up on a screen.
struct PatternAt<'p> {
    pattern: FrequencyPattern<'p>,
    /// `(r_max · GHz / (60 · ν/GHz))²`, in degrees².
    rmax2: f64,
    /// `60 · ν/GHz`: arcmin·GHz per degree.
    factor: f64,
}

impl<'p> PatternAt<'p> {
    fn new(
        (pattern, maximum_radius_arcmin_ghz): &'p (BeamPattern, f64),
        frequency_hz: f64,
    ) -> Self {
        let factor = 60.0 * frequency_hz / 1.0e9;
        Self {
            pattern: pattern.at_frequency(frequency_hz),
            rmax2: (maximum_radius_arcmin_ghz / factor).powi(2),
            factor,
        }
    }

    /// The voltage at the squared offset `r2`, in degrees² held as `Float`.
    fn voltage(&self, r2: f32) -> f32 {
        if f64::from(r2) > self.rmax2 {
            0.0
        } else {
            let radius = (f64::from(r2).sqrt() * self.factor) as f32;
            self.pattern.voltage(f64::from(radius))
        }
    }
}

/// `HetArrayConvFunc::supportAndNormalizeLatt` at screen sampling 1: the
/// first radius from the weight plane's peak where both axes fall below
/// the cut level, plus one; at least six when the lattice allows; capped
/// below half the lattice.
pub(super) fn weight_plane_support(
    plane: &[Complex32],
    side: usize,
) -> Result<usize, OperatorError> {
    let sampling = 1_usize;
    let (mut max_abs, mut min_abs, mut max_pos) = (f32::NEG_INFINITY, f32::INFINITY, [0, 0]);
    for y in 0..side {
        for x in 0..side {
            let value = plane[y * side + x].norm();
            if value > max_abs {
                max_abs = value;
                max_pos = [x, y];
            }
            min_abs = min_abs.min(value);
        }
    }
    let cut = SUPPORT_CUT_LEVEL * max_abs;
    let mut found = false;
    let mut trial = 0;
    // CASA walks from the peak towards the origin; a peak nearer the
    // origin than the walk ends it early.
    let limit = side
        .saturating_sub(max_pos[0].max(max_pos[1]) + 2)
        .min(max_pos[0].min(max_pos[1]) + 1);
    while trial < limit {
        if plane[max_pos[1] * side + max_pos[0] - trial].norm() < cut
            && plane[(max_pos[1] - trial) * side + max_pos[0]].norm() < cut
        {
            found = true;
            break;
        }
        trial += 1;
    }
    // `supportAndNormalizeLatt`'s fallbacks reach `side/2 − 4·sampling`;
    // a screen too small for that has no usable beam.
    let fallback =
        (side / 2)
            .checked_sub(4 * sampling)
            .ok_or(OperatorError::ConvolutionFunction {
                reason: "the mosaic screen is too small for its beam support",
            })?;
    if !found {
        if max_abs - min_abs > cut {
            found = true;
        }
        trial = fallback;
    }
    if !found {
        return Err(OperatorError::ConvolutionFunction {
            reason: "the mosaic weight convolution function has no support",
        });
    }
    if trial < 5 * sampling {
        trial = if 10 * sampling < side {
            5 * sampling
        } else {
            fallback
        };
    }
    let mut support = (0.5 + trial as f64 / sampling as f64) as usize + 1;
    if support * sampling >= side / 2 {
        support = side / 2 / sampling - 1;
    }
    Ok(support)
}

/// The simulator's `HetArrayConvFunc::supportAndNormalizeLatt`
/// (`TransformMachines/`, [`MosaicRules::Simulator`](super::MosaicRules::Simulator)) at screen sampling 1:
/// walking in from `side/2 − 2` towards the centre, the first offset `t`
/// where the weight plane exceeds the cut level at `(c − t − 1, c − 1)` or
/// `(c − 1, c − t − 1)`, with `c = side/2`, is widened to `⌊√(2t²)⌋`; no
/// such offset takes the same fallbacks as tclean's search. At least five,
/// plus one, and capped below half the lattice.
pub(super) fn simulator_weight_plane_support(
    plane: &[Complex32],
    side: usize,
) -> Result<usize, OperatorError> {
    let sampling = 1_usize;
    let (mut max_abs, mut min_abs) = (f32::NEG_INFINITY, f32::INFINITY);
    for value in plane.iter().map(|value| value.norm()) {
        max_abs = max_abs.max(value);
        min_abs = min_abs.min(value);
    }
    let cut = SUPPORT_CUT_LEVEL * max_abs;
    let centre = side / 2;
    let mut found = false;
    let mut trial = 0_usize;
    if centre >= 2 {
        for candidate in (1..=centre - 2).rev() {
            let along_x = plane[(centre - 1) * side + centre - candidate - 1].norm();
            let along_y = plane[(centre - candidate - 1) * side + centre - 1].norm();
            if along_x > cut || along_y > cut {
                found = true;
                // `Int(sqrt(2.0*Float(trial*trial)))`, in double precision.
                trial = (2.0 * f64::from((candidate * candidate) as f32)).sqrt() as usize;
                break;
            }
        }
    }
    let fallback =
        (side / 2)
            .checked_sub(4 * sampling)
            .ok_or(OperatorError::ConvolutionFunction {
                reason: "the mosaic screen is too small for its beam support",
            })?;
    let floor = || {
        if 10 * sampling < side {
            5 * sampling
        } else {
            fallback
        }
    };
    if !found {
        if max_abs - min_abs > cut {
            found = true;
        }
        trial = floor();
    }
    if !found {
        return Err(OperatorError::ConvolutionFunction {
            reason: "the mosaic weight convolution function has no support",
        });
    }
    if trial < 5 * sampling {
        trial = floor();
    }
    let mut support = (0.5 + trial as f64 / sampling as f64) as usize + 1;
    if support * sampling >= side / 2 {
        support = side / 2 / sampling - 1;
    }
    Ok(support)
}

/// One pair's cells at one beam frequency: imaging, weight and, with
/// conjugate beams, the imaging cell at the conjugate frequency, which
/// gridding takes in place of the imaging one.
pub(super) struct PairCells {
    pub(super) imaging: DenseCell,
    pub(super) weight: DenseCell,
    pub(super) conjugate: Option<DenseCell>,
}

/// `supportAndNormalizeLatt`'s normalisation over `support`,
/// `findConvFunction`'s crop to `crop` and the Lanczos resample to
/// [`MOSAIC_OVERSAMPLING`], then the dense cells of `taps` taps on each side
/// of the centre; with conjugate beams of frequency ratio `ν_c/ν`, also the
/// imaging cell of the resampled plane scaled by it ([`conjugate_plane`]).
pub(super) fn finish_pair(
    lattices: &PairLattices,
    side: usize,
    (support, crop, taps): (usize, usize, usize),
    conjugate_ratio: Option<f64>,
) -> Result<PairCells, OperatorError> {
    let half = u16::try_from(taps).expect("support fits u16");
    let out_side = crop * usize::from(MOSAIC_OVERSAMPLING);
    let cell = |values: &[Complex32]| {
        let plane = OversampledPlane {
            values,
            side: out_side,
            centre: [out_side / 2; 2],
            sampling: MOSAIC_OVERSAMPLING,
        };
        dense_cell(&[plane], [half, half])
    };
    let resample = |plane: &[Complex32]| -> Result<Vec<Complex32>, OperatorError> {
        let integral = block_sum_re(plane, side, support);
        if !integral.is_finite() || integral <= 0.0 {
            return Err(OperatorError::ConvolutionFunction {
                reason: "the mosaic convolution function integral is not positive",
            });
        }
        let scale = (1.0 / integral) as f32;
        let offset = side / 2 - crop / 2;
        let mut cropped = Vec::with_capacity(crop * crop);
        for y in 0..crop {
            for x in 0..crop {
                cropped.push(plane[(offset + y) * side + offset + x] * scale);
            }
        }
        Ok(lanczos_resample(&cropped, crop, out_side))
    };
    // One resampled plane at a time ([`super::MosaicScreens::kernel_bytes`]).
    let (imaging, conjugate) = {
        let plane = resample(&lattices.imaging)?;
        let conjugate =
            conjugate_ratio.map(|ratio| cell(&conjugate_plane(&plane, out_side, ratio)));
        (cell(&plane), conjugate)
    };
    Ok(PairCells {
        imaging,
        weight: cell(&resample(&lattices.weight)?),
        conjugate,
    })
}

/// The side of a `side`-cell plane resampled by `ratio`: `Int(n · r/2) · 2`
/// (`HetArrayConvFunc::resample`).
pub(super) fn conjugate_side(side: usize, ratio: f64) -> usize {
    (side as f64 * ratio / 2.0) as usize * 2
}

/// `HetArrayConvFunc::fillConjConvFunc` on one oversampled imaging plane of
/// `side × side` cells: the plane resampled by `ratio = ν_c/ν`
/// ([`conjugate_side`]), its centre `side` cells kept when `ν_c > ν` and
/// set in the centre of a zero plane otherwise, and scaled by the inverse
/// of the area ratio `(resampled side / side)²`.
pub(super) fn conjugate_plane(plane: &[Complex32], side: usize, ratio: f64) -> Vec<Complex32> {
    let scaled = conjugate_side(side, ratio);
    let resampled = lanczos_resample(plane, side, scaled);
    let area = scaled as f64 / side as f64;
    let renorm = Complex32::new((1.0 / (area * area)) as f32, 0.0);
    let mut out = vec![Complex32::default(); side * side];
    if ratio > 1.0 {
        let begin = (scaled - side) / 2;
        for (y, row) in out.chunks_exact_mut(side).enumerate() {
            row.copy_from_slice(&resampled[(begin + y) * scaled + begin..][..side]);
        }
    } else {
        let begin = (side - scaled) / 2;
        for (y, row) in resampled.chunks_exact(scaled).enumerate() {
            out[(begin + y) * side + begin..][..scaled].copy_from_slice(row);
        }
    }
    for value in &mut out {
        *value *= renorm;
    }
    out
}

/// `Re Σ` over the `[−support, support]²` block about the lattice centre.
fn block_sum_re(plane: &[Complex32], side: usize, support: usize) -> f64 {
    let centre = side / 2;
    let mut sum = 0.0;
    for y in centre - support..=centre + support {
        for x in centre - support..=centre + support {
            sum += f64::from(plane[y * side + x].re);
        }
    }
    sum
}

/// `HetArrayConvFunc::resample`: the `side × side` plane resampled to
/// `out_side` per axis, output pixel `j` reading input coordinate
/// `j · side / out_side` through the Lanczos-3 window on the real and
/// imaginary parts, zero within three input pixels of the edges.
fn lanczos_resample(input: &[Complex32], side: usize, out_side: usize) -> Vec<Complex32> {
    let real = input.iter().map(|value| value.re).collect::<Vec<_>>();
    let imag = input.iter().map(|value| value.im).collect::<Vec<_>>();
    let mut out = Vec::with_capacity(out_side * out_side);
    for k in 0..out_side {
        let y = k as f64 / out_side as f64 * side as f64;
        for j in 0..out_side {
            let x = j as f64 / out_side as f64 * side as f64;
            out.push(Complex32::new(
                interp_lanczos(x, y, side, &real),
                interp_lanczos(x, y, side, &imag),
            ));
        }
    }
    out
}

/// `HetArrayConvFunc::interpLanczos` with `a = 3`.
fn interp_lanczos(x: f64, y: f64, side: usize, data: &[f32]) -> f32 {
    let floor_x = x.floor() as i64;
    let floor_y = y.floor() as i64;
    let n = side as i64;
    if floor_x < LANCZOS_A
        || floor_x >= n - LANCZOS_A
        || floor_y < LANCZOS_A
        || floor_y >= n - LANCZOS_A
    {
        return 0.0;
    }
    let a = LANCZOS_A as f64;
    let mut result = 0.0_f64;
    for i in floor_x - LANCZOS_A + 1..=floor_x + LANCZOS_A {
        let wx = sinc(x - i as f64) * sinc((x - i as f64) / a);
        for j in floor_y - LANCZOS_A + 1..=floor_y + LANCZOS_A {
            let wy = sinc(y - j as f64) * sinc((y - j as f64) / a);
            result += f64::from(data[(j * n + i) as usize]) * wx * wy;
        }
    }
    result as f32
}

/// `HetArrayConvFunc::sinc`: `sin(πx)/(πx)`, one at zero.
fn sinc(x: f64) -> f64 {
    if x == 0.0 {
        1.0
    } else {
        let px = std::f64::consts::PI * x;
        px.sin() / px
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lanczos_resample_reproduces_a_smooth_plane_away_from_the_edges() {
        let side = 16;
        let gaussian = |x: f64, y: f64| (-((x - 8.0).powi(2) + (y - 8.0).powi(2)) / 18.0).exp();
        let input = (0..side * side)
            .map(|index| {
                Complex32::new(
                    gaussian((index % side) as f64, (index / side) as f64) as f32,
                    0.0,
                )
            })
            .collect::<Vec<_>>();
        let out = lanczos_resample(&input, side, 4 * side);
        let out_side = side * 4;
        assert_eq!(out.len(), out_side * out_side);
        // Output pixel (j, k) samples input (j/4, k/4): exact on input
        // pixels, and within the Lanczos-3 window's gain droop (its
        // weights sum to 0.994 at a half-pixel offset, as in CASA)
        // between them.
        assert_eq!(f64::from(out[32 * out_side + 32].re), gaussian(8.0, 8.0));
        for (j, k) in [(30, 35), (25, 40), (40, 20)] {
            let want = gaussian(j as f64 / 4.0, k as f64 / 4.0);
            let got = f64::from(out[k * out_side + j].re);
            assert!(
                (got - want).abs() < 1.5e-2 * want,
                "({j}, {k}): {got} vs {want}"
            );
        }
        // Within three input pixels of an edge the window is zero.
        assert_eq!(out[32 * out_side + 11].re, 0.0);
        assert_eq!(out[32 * out_side + (side - 3) * 4].re, 0.0);
    }

    #[test]
    fn weight_support_is_the_cut_radius_plus_one_within_the_rules() {
        let side = 64;
        let centre = side / 2;
        // A Gaussian weight plane: |w| < 2.5 % of peak at radius ≈ 2.72 σ.
        let sigma = 4.0;
        let plane = (0..side * side)
            .map(|index| {
                let dx = (index % side) as f64 - centre as f64;
                let dy = (index / side) as f64 - centre as f64;
                Complex32::new(
                    (-(dx * dx + dy * dy) / (2.0 * sigma * sigma)).exp() as f32,
                    0.0,
                )
            })
            .collect::<Vec<_>>();
        // trial 11: exp(−121/32) = 0.0228 < 0.025 → support 12.
        assert_eq!(weight_plane_support(&plane, side).expect("support"), 12);
        // A narrow plane stops at the minimum of five, plus one.
        let narrow = (0..side * side)
            .map(|index| {
                let dx = (index % side) as f64 - centre as f64;
                let dy = (index / side) as f64 - centre as f64;
                Complex32::new((-(dx * dx + dy * dy) / 0.5).exp() as f32, 0.0)
            })
            .collect::<Vec<_>>();
        assert_eq!(weight_plane_support(&narrow, side).expect("support"), 6);
    }
}
