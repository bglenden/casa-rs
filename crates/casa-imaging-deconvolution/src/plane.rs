// SPDX-License-Identifier: LGPL-3.0-or-later
//! Plane layout, the cleanable support, and CASA's peak searches.

/// The shape of one image plane.
///
/// Pixels are stored x-major: pixel `(x, y)` is at `x * ny + y`. That is the
/// layout of the normal state, masks and model the minor cycle is fed from,
/// so no plane is transposed on the way in or out. casacore scans arrays
/// x fastest; the peak searches below reproduce its tie order explicitly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlaneShape {
    /// Pixels along x (the first image axis).
    pub nx: usize,
    /// Pixels along y (the second image axis).
    pub ny: usize,
}

impl PlaneShape {
    /// A plane of `nx` by `ny` pixels.
    #[must_use]
    pub const fn new(nx: usize, ny: usize) -> Self {
        Self { nx, ny }
    }

    /// Number of pixels.
    #[must_use]
    pub const fn len(self) -> usize {
        self.nx * self.ny
    }

    /// Whether the plane has no pixels.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.len() == 0
    }

    /// Storage index of pixel `(x, y)`.
    #[must_use]
    pub const fn index(self, x: usize, y: usize) -> usize {
        x * self.ny + y
    }

    /// Pixel `[x, y]` of a storage index.
    #[must_use]
    pub const fn pixel(self, index: usize) -> [usize; 2] {
        [index / self.ny, index % self.ny]
    }

    /// The image centre `(nx/2, ny/2)`, where CASA centres PSFs and scale
    /// functions.
    #[must_use]
    pub const fn centre(self) -> [usize; 2] {
        [self.nx / 2, self.ny / 2]
    }

    /// Whether storage index `a` comes before `b` in casacore's x-fastest
    /// scan order.
    #[must_use]
    pub const fn scans_before(self, a: usize, b: usize) -> bool {
        let [ax, ay] = self.pixel(a);
        let [bx, by] = self.pixel(b);
        ay < by || (ay == by && ax < bx)
    }
}

/// The pixels a component may be placed on: the clean mask and the valid
/// model support (the primary-beam limit) together.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Support {
    shape: PlaneShape,
    pixels: Vec<bool>,
    count: usize,
}

impl Support {
    /// Support from one flag per pixel, in storage order.
    ///
    /// # Panics
    ///
    /// When the flags do not cover the plane.
    #[must_use]
    pub fn new(shape: PlaneShape, pixels: Vec<bool>) -> Self {
        assert_eq!(pixels.len(), shape.len(), "support covers its plane");
        let count = pixels.iter().filter(|pixel| **pixel).count();
        Self {
            shape,
            pixels,
            count,
        }
    }

    /// Every pixel of the plane.
    #[must_use]
    pub fn full(shape: PlaneShape) -> Self {
        Self::new(shape, vec![true; shape.len()])
    }

    /// The plane this support covers.
    #[must_use]
    pub const fn shape(&self) -> PlaneShape {
        self.shape
    }

    /// Whether storage index `index` is supported.
    #[must_use]
    pub fn contains(&self, index: usize) -> bool {
        self.pixels[index]
    }

    /// The flags in storage order.
    #[must_use]
    pub fn as_slice(&self) -> &[bool] {
        &self.pixels
    }

    /// Number of supported pixels (CASA's mask sum).
    #[must_use]
    pub const fn count(&self) -> usize {
        self.count
    }

    /// Whether no pixel is supported.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }
}

/// Relative difference within which two values the minor cycle compares
/// are a tie.
///
/// The planes a solver searches come from double-precision FFT
/// convolutions, whose rounding is about 1e-14 of the plane's peak at
/// 4096². A point-symmetric sky gives mirror pixels whose values agree only
/// to that rounding, and which mirror wins an exact comparison then depends
/// on the FFT's rounding pattern (and, with measured FFT plans, on the
/// run). casacore compares exactly, so CASA takes the first in x-fastest
/// order whenever its arithmetic keeps the tie. Values closer than this are
/// treated as equal and resolved in casacore's order; no meaningful flux
/// difference is this small.
pub(crate) const TIE_TOLERANCE: f64 = 1.0e-12;

/// The values `value` ties with: `[value − tol·|value|, value + tol·|value|]`,
/// or `value` alone when it is infinite.
fn tie_band(value: f64) -> (f64, f64) {
    let margin = TIE_TOLERANCE * value.abs();
    if margin.is_finite() {
        (value - margin, value + margin)
    } else {
        (value, value)
    }
}

/// Whether `value` exceeds `best` by more than a tie.
pub(crate) fn exceeds(value: f64, best: f64) -> bool {
    value - best > TIE_TOLERANCE * value.abs().max(best.abs())
}

/// The supported pixel of largest absolute value, as the Fortran `hclean`
/// peak search finds it: a pixel replaces the best only when larger, so
/// among equal magnitudes the first in x-fastest order wins. Magnitudes
/// within [`TIE_TOLERANCE`] of each other are equal.
/// Returns the index and the signed value; `None` when nothing is supported.
/// The first supported NaN is returned as soon as it is met, so a solver
/// can refuse the plane.
#[must_use]
pub fn first_peak(values: &[f64], support: &Support) -> Option<(usize, f64)> {
    let shape = support.shape();
    let mut best: Option<(usize, f64)> = None;
    let (mut lower, mut upper) = (f64::NEG_INFINITY, f64::NEG_INFINITY);
    for (index, (&value, &supported)) in values.iter().zip(support.as_slice()).enumerate() {
        let magnitude = value.abs();
        if !supported || magnitude < lower {
            continue;
        }
        if magnitude.is_nan() {
            return Some((index, value));
        }
        if best.is_none_or(|(at, _)| magnitude > upper || shape.scans_before(index, at)) {
            best = Some((index, value));
            (lower, upper) = tie_band(magnitude);
        }
    }
    best
}

/// casacore's `findMaxAbsMask`: the signed extreme of largest magnitude
/// over the supported pixels, taken from `minMax`. The maximum and the
/// minimum are each the first occurrence in x-fastest order, and the
/// minimum wins only when larger in magnitude. Values within
/// [`TIE_TOLERANCE`] of each other are equal.
///
/// Unsupported pixels take part as zeros (casacore multiplies by the mask),
/// so with no supported pixel the result is `(0, 0.0)`.
#[must_use]
pub fn casacore_max_abs(values: &[f64], support: &Support) -> (usize, f64) {
    let shape = support.shape();
    let mut maximum = (0, f64::NEG_INFINITY);
    let mut minimum = (0, f64::INFINITY);
    let (mut maximum_low, mut maximum_high) = (f64::NEG_INFINITY, f64::NEG_INFINITY);
    let (mut minimum_low, mut minimum_high) = (f64::INFINITY, f64::INFINITY);
    for (index, (&value, &supported)) in values.iter().zip(support.as_slice()).enumerate() {
        let value = if supported { value } else { 0.0 };
        if value >= maximum_low && (value > maximum_high || shape.scans_before(index, maximum.0)) {
            maximum = (index, value);
            (maximum_low, maximum_high) = tie_band(value);
        }
        if value <= minimum_high && (value < minimum_low || shape.scans_before(index, minimum.0)) {
            minimum = (index, value);
            (minimum_low, minimum_high) = tie_band(value);
        }
    }
    if exceeds(minimum.1.abs(), maximum.1.abs()) {
        minimum
    } else {
        maximum
    }
}

/// The largest absolute value over the supported pixels; zero when none is.
#[must_use]
pub fn peak_magnitude(values: &[f64], support: &Support) -> f64 {
    values
        .iter()
        .zip(support.as_slice())
        .filter(|(_, supported)| **supported)
        .fold(0.0_f64, |peak, (value, _)| peak.max(value.abs()))
}

/// The PSF peak as CASA's `MatrixCleaner::findPSFMaxAbs` finds it: the
/// largest magnitude inside the central `findBeamPatch(0, nx, ny, 4, 20)`
/// box, scanned x fastest with strictly-greater replacement. The box's
/// upper bounds are exclusive, as in casacore, and an axis no longer than
/// the patch is searched from 0 to its last pixel exclusive.
#[must_use]
pub fn psf_peak(psf: &[f64], shape: PlaneShape) -> Option<usize> {
    let support = beam_patch(0.0, shape);
    let bounds = |extent: usize| {
        if extent > support {
            (extent / 2 - support / 2, extent / 2 + support / 2)
        } else {
            (0, extent.saturating_sub(1))
        }
    };
    let (x0, x1) = bounds(shape.nx);
    let (y0, y1) = bounds(shape.ny);
    let mut best: Option<(usize, f64)> = None;
    for y in y0..y1 {
        for x in x0..x1 {
            let index = shape.index(x, y);
            let magnitude = psf[index].abs();
            if best.is_none_or(|(_, peak)| magnitude > peak) {
                best = Some((index, magnitude));
            }
        }
    }
    best.map(|(index, _)| index)
}

/// `MatrixCleaner::findBeamPatch(maxScaleSize, nx, ny, 4, 20)`: the even
/// side of the PSF patch the multi-term cleaner updates within, at least
/// 80 pixels and at most the smaller image axis.
#[must_use]
pub fn beam_patch(maximum_scale_px: f64, shape: PlaneShape) -> usize {
    const PSF_BEAM: f64 = 4.0;
    const BEAMS: f64 = 20.0;
    let mut support =
        ((PSF_BEAM * PSF_BEAM + maximum_scale_px * maximum_scale_px).sqrt() * BEAMS) as usize;
    if (support as f64) < PSF_BEAM * BEAMS {
        support = (PSF_BEAM * BEAMS) as usize;
    }
    if support > shape.nx || support > shape.ny {
        support = shape.nx.min(shape.ny);
    }
    if !support.is_multiple_of(2) {
        support -= 1;
    }
    support
}

/// CASA's robust noise of one plane (`SIImageStore::calcRobustRMS`): the
/// median of the supported values and 1.4826 times their median absolute
/// deviation. `None` when nothing is supported.
#[must_use]
pub fn robust_noise(values: &[f64], support: &Support) -> Option<RobustNoise> {
    let mut samples = values
        .iter()
        .zip(support.as_slice())
        .filter(|(_, supported)| **supported)
        .map(|(value, _)| *value)
        .collect::<Vec<_>>();
    if samples.is_empty() {
        return None;
    }
    let middle = samples.len() / 2;
    let median = *samples.select_nth_unstable_by(middle, f64::total_cmp).1;
    for sample in &mut samples {
        *sample = (*sample - median).abs();
    }
    let deviation = *samples.select_nth_unstable_by(middle, f64::total_cmp).1;
    Some(RobustNoise {
        median,
        rms: 1.482_602_218_505_602 * deviation,
    })
}

/// The robust statistics of one residual plane.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RobustNoise {
    /// Median of the supported values.
    pub median: f64,
    /// 1.4826 times the median absolute deviation.
    pub rms: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hclean_ties_go_to_the_first_pixel_in_x_fastest_order() {
        let shape = PlaneShape::new(3, 4);
        let mut values = vec![0.0; shape.len()];
        // (2, 1) precedes (0, 3) in x-fastest order although it is stored later.
        values[shape.index(0, 3)] = -2.0;
        values[shape.index(2, 1)] = 2.0;
        let support = Support::full(shape);
        assert_eq!(
            first_peak(&values, &support),
            Some((shape.index(2, 1), 2.0))
        );
        values[shape.index(1, 0)] = -2.0;
        assert_eq!(
            first_peak(&values, &support),
            Some((shape.index(1, 0), -2.0))
        );
        let masked = Support::new(
            shape,
            (0..shape.len()).map(|i| i != shape.index(1, 0)).collect(),
        );
        assert_eq!(first_peak(&values, &masked), Some((shape.index(2, 1), 2.0)));
        assert_eq!(
            first_peak(&values, &Support::new(shape, vec![false; 12])),
            None
        );
    }

    #[test]
    fn casacore_max_abs_prefers_the_maximum_on_equal_magnitude() {
        let shape = PlaneShape::new(4, 3);
        let mut values = vec![0.5; shape.len()];
        values[shape.index(0, 0)] = -3.0;
        values[shape.index(3, 2)] = 3.0;
        let support = Support::full(shape);
        assert_eq!(
            casacore_max_abs(&values, &support),
            (shape.index(3, 2), 3.0)
        );
        values[shape.index(1, 1)] = -3.5;
        assert_eq!(
            casacore_max_abs(&values, &support),
            (shape.index(1, 1), -3.5)
        );
        // Masked pixels count as zeros.
        let only_low = Support::new(shape, (0..12).map(|i| values[i] == 0.5).collect());
        assert_eq!(casacore_max_abs(&values, &only_low).1, 0.5);
        let none = Support::new(shape, vec![false; 12]);
        assert_eq!(casacore_max_abs(&values, &none), (0, 0.0));
    }

    /// Rounding never chooses between mirror pixels: values a few ulps apart
    /// resolve to the first in x-fastest order whichever is larger and in
    /// either sign, and a real difference still picks the larger.
    #[test]
    fn rounding_ties_resolve_in_scan_order() {
        let shape = PlaneShape::new(5, 4);
        let support = Support::full(shape);
        let first = shape.index(3, 1);
        let later = shape.index(1, 2);
        let nudged = |value: f64, ulps: i64| f64::from_bits((value.to_bits() as i64 + ulps) as u64);
        for sign in [1.0, -1.0] {
            let peak = sign * 1.25e-3;
            for ulps in [-3, -1, 0, 1, 3] {
                let mut values = vec![sign * 1.0e-4; shape.len()];
                values[first] = peak;
                values[later] = nudged(peak, ulps);
                assert_eq!(first_peak(&values, &support), Some((first, peak)));
                assert_eq!(casacore_max_abs(&values, &support), (first, peak));
            }
            let mut values = vec![sign * 1.0e-4; shape.len()];
            values[first] = peak;
            values[later] = peak * (1.0 + 1.0e-9);
            assert_eq!(first_peak(&values, &support).unwrap().0, later);
            assert_eq!(casacore_max_abs(&values, &support).0, later);
        }
        // Between the maximum and the minimum a tie goes to the maximum.
        let mut values = vec![0.0; shape.len()];
        values[first] = -2.0;
        values[later] = nudged(2.0, -2);
        assert_eq!(casacore_max_abs(&values, &support).0, later);
        values[later] = 2.0 * (1.0 - 1.0e-9);
        assert_eq!(casacore_max_abs(&values, &support).0, first);
    }

    #[test]
    fn the_psf_peak_is_searched_in_the_central_patch_with_exclusive_bounds() {
        // 100 pixels: patch 80, box 10..90 on both axes.
        let shape = PlaneShape::new(100, 100);
        let mut psf = vec![0.0; shape.len()];
        psf[shape.index(50, 50)] = 1.0;
        psf[shape.index(5, 50)] = 2.0;
        psf[shape.index(50, 90)] = 2.0;
        assert_eq!(psf_peak(&psf, shape), Some(shape.index(50, 50)));
        // An axis no longer than the patch ends one short of its last pixel.
        let small = PlaneShape::new(9, 7);
        let mut psf = vec![0.0; small.len()];
        psf[small.index(8, 3)] = 5.0;
        psf[small.index(4, 3)] = 1.0;
        assert_eq!(psf_peak(&psf, small), Some(small.index(4, 3)));
    }

    #[test]
    fn beam_patch_is_even_and_bounded_by_the_image() {
        assert_eq!(beam_patch(0.0, PlaneShape::new(512, 512)), 80);
        assert_eq!(beam_patch(10.0, PlaneShape::new(512, 512)), 214);
        assert_eq!(beam_patch(0.0, PlaneShape::new(100, 75)), 74);
        assert_eq!(beam_patch(0.0, PlaneShape::new(64, 64)), 64);
    }

    #[test]
    fn robust_noise_is_the_scaled_median_absolute_deviation_of_the_support() {
        let shape = PlaneShape::new(1, 5);
        let values = [1.0, 2.0, 3.0, 100.0, -50.0];
        let support = Support::new(shape, vec![true, true, true, false, false]);
        let noise = robust_noise(&values, &support).expect("support");
        assert_eq!(noise.median, 2.0);
        assert!((noise.rms - 1.482_602_218_505_602).abs() < 1e-15);
        assert_eq!(
            robust_noise(&values, &Support::new(shape, vec![false; 5])),
            None
        );
    }
}
