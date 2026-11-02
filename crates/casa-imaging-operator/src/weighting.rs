// SPDX-License-Identifier: LGPL-3.0-or-later
//! Global visibility weighting: natural and radial weights, and uniform,
//! superuniform and Briggs (normalized, absolute and bandwidth-tapered)
//! density weights, with an optional Gaussian taper, pinned to CASA
//! (`VisImagingWeight`, `BriggsCubeWeightor`).

use crate::error::OperatorError;
use crate::sample::{Placement, SampleBlock};

/// Gaussian uv taper in baseline half-widths at half maximum.
pub type Taper = casa_imaging_model::UvTaper;

/// Which CASA cell arithmetic a density grid uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DensityCellRule {
    /// `VisImagingWeight`: single-precision coordinates truncated toward
    /// zero, `v` not mirrored.
    Standard,
    /// `BriggsCubeWeightor`: rounded coordinates of the negated `u` and `v`,
    /// built in double and looked up in single precision.
    Cube,
}

/// A sample's density-cell coordinates in wavelengths, rounded the way CASA
/// forms them before it picks a density cell (`VisImagingWeight::weightUniform`
/// and its accumulation loop, `BriggsCubeWeightor::getWeightUniform`):
/// `Float f = ν/c`, then `Float u = uvw_u·f` and `Float v = uvw_v·f`. At
/// large image sizes one single-precision ulp is a visible fraction of a
/// cell, so the rounding order decides which cell a boundary sample uses.
/// Gridding and the uv taper keep the double coordinates of the
/// [`Placement`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DensityUv {
    /// u in wavelengths.
    pub u: f32,
    /// v in wavelengths.
    pub v: f32,
    /// CASA's `Float f = ν/c`, in wavelengths per metre.
    pub wavelengths_per_m: f32,
    /// The baseline's u and v in metres.
    pub uv_m: [f64; 2],
}

impl DensityUv {
    /// CASA's single-precision coordinates of baseline `uvw_m` at
    /// `frequency_hz`.
    #[must_use]
    pub fn casa(uvw_m: [f64; 3], frequency_hz: f64) -> Self {
        let per_m = (frequency_hz / SPEED_OF_LIGHT_M_PER_S) as f32;
        let wavelengths_per_m = f64::from(per_m);
        Self {
            u: (uvw_m[0] * wavelengths_per_m) as f32,
            v: (uvw_m[1] * wavelengths_per_m) as f32,
            wavelengths_per_m: per_m,
            uv_m: [uvw_m[0], uvw_m[1]],
        }
    }

    /// The uv distance in wavelengths that CASA's radial weighting scales a
    /// weight by (`VisImagingWeight::weightRadial`):
    /// `f·sqrt(square(u) + square(v))`, the metre coordinates in double and
    /// `f` the single-precision `ν/c`.
    #[must_use]
    pub fn radial_distance(self) -> f64 {
        let [u, v] = self.uv_m;
        f64::from(self.wavelengths_per_m) * (u * u + v * v).sqrt()
    }
}

/// CASA `C::c`.
pub(crate) const SPEED_OF_LIGHT_M_PER_S: f64 = 299_792_458.0;

/// Shape of a density grid: image-sized cells per plane.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DensityGridShape {
    /// Cells along u (the image width).
    pub width: usize,
    /// Cells along v (the image height).
    pub height: usize,
    /// Density planes: 1 for a global generation; for a per-channel
    /// generation the output channel count plus `padding` on each side; for
    /// a per-field generation the FIELD_IDs from `first_field` on.
    pub planes: usize,
    /// Per-channel generation: density planes on each side of the output
    /// axis (CASA `BriggsCubeWeightor::estimateSwingChanPad`), so native
    /// channels just beyond the image still find their own plane. 0 for a
    /// global generation.
    pub padding: u32,
    /// Half-width `b` of the box of `(2b + 1)²` cells about its own that
    /// each sample adds to: CASA `superuniform`'s `uBox = vBox`
    /// (`VisImagingWeight`) and a cube density's `superUniformBox`
    /// (`BriggsCubeWeightor`), both from `npixels`. 0 adds to one cell.
    pub box_half_width: usize,
    /// Signed cell increments `[Δx, Δy]` in radians: the image's, or for
    /// a square `npixels` grid CASA's `(n·Δ)/n` (`SynthesisImagerVi2::weight`).
    pub increment_rad: [f64; 2],
    /// Cell arithmetic.
    pub rule: DensityCellRule,
    /// A per-field generation (tclean's `mosweight`, `VisImagingWeight`'s
    /// `multiField`), on [`DensityCellRule::Standard`] cells: plane `p` is
    /// the density of FIELD_ID `first_field + p`, gridded from that field's
    /// samples alone, and a sample reads its own field's plane. `None` for a
    /// global or per-channel generation.
    pub first_field: Option<u32>,
}

impl DensityGridShape {
    /// Cells over every plane.
    #[must_use]
    pub const fn cells(&self) -> usize {
        self.width * self.height * self.planes
    }

    /// Bytes a [`DensityGrid`] of this shape holds: an `f64` per cell and a
    /// weight sum per plane. A weighting generation keeps its grid for the
    /// whole run.
    #[must_use]
    pub const fn bytes(&self) -> u64 {
        ((self.cells() + self.planes) * size_of::<f64>()) as u64
    }

    /// The cell increment CASA gives `VisImagingWeight` for a uniform or
    /// Briggs density of `cells` cells along an image axis of increment
    /// `increment_rad`: `SynthesisImagerVi2::weight` forms the field of view
    /// `cells·Δ` and divides it by `cells` again, in double, whether `cells`
    /// is `npixels` or the image's own size.
    #[must_use]
    pub fn visimagingweight_increment(cells: usize, increment_rad: f64) -> f64 {
        (cells as f64 * increment_rad) / cells as f64
    }

    /// The density plane a sample on output plane `plane` from FIELD_ID
    /// `field` grids into and reads: its field's for a per-field generation
    /// (`None` outside its span), else `plane`, which `plane_of`
    /// resolves.
    #[must_use]
    pub fn density_plane(&self, plane: u32, field: u32) -> Option<u32> {
        match self.first_field {
            Some(first) => field
                .checked_sub(first)
                .filter(|plane| (*plane as usize) < self.planes),
            None => Some(plane),
        }
    }

    /// Density plane of a placement: plane 0 for a global generation.
    fn plane_of(&self, placement_plane: u32) -> Option<usize> {
        if self.planes == 1 {
            Some(0)
        } else {
            let plane = placement_plane as usize;
            (plane < self.planes).then_some(plane)
        }
    }

    /// CASA's integer cell origin `[nx/2, ny/2]` (`uorigin_p`, `vorigin_p`;
    /// `GridFT`'s `offset_p` likewise): integer division, so an odd size
    /// puts the origin on the lower of the two middle cells.
    fn origin(&self) -> [usize; 2] {
        [self.width / 2, self.height / 2]
    }

    /// CASA's `Float` cell scales `uscale_p = nx·Δx`, `vscale_p = ny·Δy`,
    /// each rounded once from the double product
    /// (`VisImagingWeight::VisImagingWeight`, `BriggsCubeWeightor::init`).
    fn scale(&self) -> [f32; 2] {
        [
            (self.width as f64 * self.increment_rad[0]) as f32,
            (self.height as f64 * self.increment_rad[1]) as f32,
        ]
    }

    /// The first cell of the box a density placement adds to, the
    /// `(2b + 1)²` cells about its own cell, when the whole box is accepted
    /// (`VisImagingWeight`: `(ucell − uBox) > 0 && (ucell + uBox) < nx`, and
    /// likewise in v; `GridFT`'s `onmygrid` likewise keeps a sample only
    /// when its whole support lies on the grid). Standard placements carry
    /// [`DensityUv`] coordinates; cube placements carry the double
    /// coordinates CASA grids its weight density with (`GridFT::put`'s
    /// `locuvw`: `nint(scale·u + offset + 1)` in Fortran indexing, of the
    /// `u` and `v` that `FTMachine::negateUV` negated).
    fn build_box(&self, plane: usize, u: f64, v: f64) -> Option<usize> {
        let [x, y] = match self.rule {
            DensityCellRule::Standard => self.standard_xy(u as f32, v as f32),
            DensityCellRule::Cube => {
                let [origin_x, origin_y] = self.origin().map(|origin| origin as f64);
                let x = (-u * self.width as f64 * self.increment_rad[0] + origin_x + 1.0).round()
                    as isize
                    - 1;
                let y = (-v * self.height as f64 * self.increment_rad[1] + origin_y + 1.0).round()
                    as isize
                    - 1;
                [x, y]
            }
        };
        let half = isize::try_from(self.box_half_width).unwrap_or(isize::MAX);
        self.cell_index(plane, x.saturating_add(half), y.saturating_add(half))?;
        self.cell_index(plane, x.saturating_sub(half), y.saturating_sub(half))
    }

    /// The share of a sample's weight each cell of its box takes.
    /// `VisImagingWeight` adds the whole weight to every cell of the box;
    /// `BriggsCubeWeightor` grids through `GridFT` with a convolution
    /// function of ones over the box, which `sectggridd` divides by its sum
    /// `(2b + 1)²`.
    fn box_share(&self) -> f64 {
        match self.rule {
            DensityCellRule::Standard => 1.0,
            DensityCellRule::Cube => {
                let side = (2 * self.box_half_width + 1) as f64;
                1.0 / (side * side)
            }
        }
    }

    /// The cell a lookup reads: `BriggsCubeWeightor::getWeightUniform`
    /// rounds `uscale·u + uorigin` in `Float` with `u = −uvw_u·f` and
    /// `v = −uvw_v·f`, as its grid was built. Evaluated in a mirrored
    /// orientation, an interior cell differs only at an exact `Float` tie
    /// (`k + ½`), where rounding half away from zero is not mirror-symmetric;
    /// and on an odd width the accepted cells `0 < x < width` are not
    /// mirror-symmetric about the origin `width / 2`, so the outermost
    /// column would be read where CASA reads none, or the reverse.
    fn lookup_cell(&self, plane: usize, uv: DensityUv) -> Option<usize> {
        match self.rule {
            DensityCellRule::Standard => self.standard_cell(plane, uv.u, uv.v),
            DensityCellRule::Cube => {
                let [scale_x, scale_y] = self.scale();
                let [origin_x, origin_y] = self.origin().map(|origin| origin as f32);
                let x = (scale_x * -uv.u + origin_x).round();
                let y = (scale_y * -uv.v + origin_y).round();
                self.cell_index(plane, x as isize, y as isize)
            }
        }
    }

    /// `VisImagingWeight`'s cell.
    fn standard_cell(&self, plane: usize, u: f32, v: f32) -> Option<usize> {
        let [x, y] = self.standard_xy(u, v);
        self.cell_index(plane, x, y)
    }

    /// `VisImagingWeight`: `Int(uscale·u + uorigin)` in `Float`, truncated
    /// toward zero.
    fn standard_xy(&self, u: f32, v: f32) -> [isize; 2] {
        let [scale_x, scale_y] = self.scale();
        let [origin_x, origin_y] = self.origin().map(|origin| origin as f32);
        [
            (scale_x * u + origin_x) as isize,
            (scale_y * v + origin_y) as isize,
        ]
    }

    /// CASA accepts `0 < cell < extent` only.
    fn cell_index(&self, plane: usize, x: isize, y: isize) -> Option<usize> {
        if x <= 0 || y <= 0 || x >= self.width as isize || y >= self.height as isize {
            return None;
        }
        Some((plane * self.height + y as usize) * self.width + x as usize)
    }
}

/// Gridded weight density per plane, the input to uniform and Briggs
/// weights.
#[derive(Clone, Debug, PartialEq)]
pub struct DensityGrid {
    shape: DensityGridShape,
    cells: Vec<f64>,
    sum_weights: Vec<f64>,
}

impl DensityGrid {
    /// An empty grid of `shape`, ready to [`DensityGrid::accumulate`] blocks.
    #[must_use]
    pub fn new(shape: DensityGridShape) -> Self {
        Self {
            cells: vec![0.0; shape.cells()],
            sum_weights: vec![0.0; shape.planes],
            shape,
        }
    }

    /// Add one block of density samples.
    ///
    /// Blocks carry one polarization whose weight is CASA's unpolarized input
    /// weight; each sample adds its weight's box share
    /// ([`DensityGridShape::box_half_width`]) to every cell of the box about
    /// its cell and of the box about the conjugate cell, each when that whole
    /// box lies inside the grid (CASA tests the two independently), and its
    /// weight once to the plane's weight sum when its own box does.
    pub fn accumulate(&mut self, block: &SampleBlock<'_>) {
        assert_eq!(block.npol, 1, "density blocks carry one unpolarized weight");
        let share = self.shape.box_share();
        let side = 2 * self.shape.box_half_width + 1;
        for (placement, weight) in block.placements.iter().zip(block.weights) {
            if *weight <= 0.0 {
                continue;
            }
            let Some(plane) = self.shape.plane_of(placement.plane) else {
                continue;
            };
            let weight = f64::from(*weight);
            let primary = self.shape.build_box(plane, placement.u, placement.v);
            let conjugate = self.shape.build_box(plane, -placement.u, -placement.v);
            for first in primary.into_iter().chain(conjugate) {
                // CASA's loops: v outer, u inner.
                for row in 0..side {
                    let start = first + row * self.shape.width;
                    for cell in &mut self.cells[start..start + side] {
                        *cell += weight * share;
                    }
                }
            }
            if primary.is_some() {
                self.sum_weights[plane] += weight;
            }
        }
    }

    /// The shape.
    #[must_use]
    pub const fn shape(&self) -> &DensityGridShape {
        &self.shape
    }

    /// Every cell, plane-major then `[y][x]`.
    #[must_use]
    pub fn cells(&self) -> &[f64] {
        &self.cells
    }

    /// Cells of one plane.
    #[must_use]
    pub fn plane(&self, plane: usize) -> &[f64] {
        let cells = self.shape.width * self.shape.height;
        &self.cells[plane * cells..(plane + 1) * cells]
    }

    /// Input weight summed once per sample whose own box lies inside the
    /// grid, per plane.
    #[must_use]
    pub fn sum_weights(&self) -> &[f64] {
        &self.sum_weights
    }

    /// Density at the lookup cell of `uv` on `plane`; `None` outside the
    /// grid.
    #[must_use]
    pub fn lookup(&self, plane: usize, uv: DensityUv) -> Option<f64> {
        self.shape
            .lookup_cell(plane, uv)
            .map(|cell| self.cells[cell])
    }

    /// Briggs factors `f² = (5·10^{−robust})² / (Σd² / Σd)` per plane, with
    /// `Σd` the density sum (standard) or twice the input weight sum (cube),
    /// and `d² = 1` (`rmode = "norm"`).
    #[must_use]
    pub fn robust_factors(&self, robust: f64) -> RobustFactors {
        let cells = self.shape.width * self.shape.height;
        let f2 = (0..self.shape.planes)
            .map(|plane| {
                let density = &self.cells[plane * cells..(plane + 1) * cells];
                let square_sum = density.iter().map(|value| value * value).sum::<f64>();
                let sum = match self.shape.rule {
                    DensityCellRule::Standard => density.iter().sum::<f64>(),
                    DensityCellRule::Cube => 2.0 * self.sum_weights[plane],
                };
                if sum > 0.0 && square_sum > 0.0 {
                    (5.0 * 10_f64.powf(-robust)).powi(2) / (square_sum / sum)
                } else {
                    0.0
                }
            })
            .collect();
        RobustFactors {
            robust,
            f2,
            d2: 1.0,
        }
    }

    /// Absolute Briggs factors (CASA `briggsabs`, `rmode = "abs"`):
    /// `f² = robust²` and `d² = 2·noise²` on every plane, each held in
    /// `Float` (`VisImagingWeight`'s `f2_p` and `d2_p`; `BriggsCubeWeightor`
    /// likewise).
    #[must_use]
    pub fn absolute_factors(&self, robust: f64, noise_jy: f64) -> RobustFactors {
        let f2 = f64::from((robust * robust) as f32);
        RobustFactors {
            robust,
            f2: vec![f2; self.shape.planes],
            d2: f64::from((2.0 * (noise_jy * noise_jy)) as f32),
        }
    }
}

/// CASA's Briggs factors for one robustness: `f²` per density plane and the
/// offset `d²`, so that a sample whose cell has density `D` weighs
/// `w / (D·f² + d²)`.
#[derive(Clone, Debug, PartialEq)]
pub struct RobustFactors {
    robust: f64,
    f2: Vec<f64>,
    d2: f64,
}

impl RobustFactors {
    /// The robustness parameter.
    #[must_use]
    pub const fn robust(&self) -> f64 {
        self.robust
    }

    /// `f²` of `plane`.
    #[must_use]
    pub fn factor(&self, plane: usize) -> f64 {
        self.f2[plane]
    }

    /// `d²`: 1 for normalized Briggs, `2·noise²` for absolute Briggs.
    #[must_use]
    pub const fn offset(&self) -> f64 {
        self.d2
    }
}

/// The density grid of every block from `source` (see
/// [`DensityGrid::accumulate`]).
pub fn build_density_grid<'a>(
    source: impl Iterator<Item = SampleBlock<'a>>,
    shape: DensityGridShape,
) -> DensityGrid {
    let mut grid = DensityGrid::new(shape);
    for block in source {
        grid.accumulate(&block);
    }
    grid
}

/// CASA `briggsbwtaper` (CAS-13021): the fractional bandwidth
/// `2(ν_last − ν_first)/(ν_last + ν_first)` of the image's spectral axis,
/// which turns each sample's uv distance in cells into a divisor of the
/// Briggs density term so every cube channel is weighted as if its uv
/// coverage had the continuum's radial smearing.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BandwidthTaper {
    fractional_bandwidth: f64,
}

impl BandwidthTaper {
    /// From the first and last channel centres of the image's spectral axis
    /// (`SynthesisImagerVi2::weight`); the two must be finite, positive and
    /// distinct.
    pub fn from_frequency_range(first_hz: f64, last_hz: f64) -> Result<Self, OperatorError> {
        let low = first_hz.min(last_hz);
        let high = first_hz.max(last_hz);
        if !(low.is_finite() && high.is_finite()) || low <= 0.0 || high <= low {
            return Err(OperatorError::Weighting {
                reason: "the bandwidth taper needs two distinct positive frequencies",
            });
        }
        Ok(Self {
            fractional_bandwidth: 2.0 * (high - low) / (high + low),
        })
    }

    /// `2(ν_last − ν_first)/(ν_last + ν_first)`.
    #[must_use]
    pub const fn fractional_bandwidth(self) -> f64 {
        self.fractional_bandwidth
    }

    /// CASA `uvDistanceFactor`: `n + 0.5`, or `(4 − n)/(4 − 2n)` when that is
    /// below 1.5, with `n` the fractional bandwidth times the sample's
    /// distance from the uv origin in grid cells.
    fn uv_distance_factor(self, shape: &DensityGridShape, u: f64, v: f64) -> f64 {
        let u_cells = u * shape.width as f64 * shape.increment_rad[0];
        let v_cells = v * shape.height as f64 * shape.increment_rad[1];
        let cells = self.fractional_bandwidth * u_cells.hypot(v_cells);
        let factor = cells + 0.5;
        if factor < 1.5 {
            (4.0 - cells) / (4.0 - 2.0 * cells)
        } else {
            factor
        }
    }
}

/// The imaging-weight rule of one run.
#[derive(Clone, Debug, PartialEq)]
pub enum WeightingGeneration {
    /// Input weight, optionally tapered.
    Natural {
        /// Gaussian uv taper.
        taper: Option<Taper>,
    },
    /// Input weight times the sample's uv distance in wavelengths
    /// ([`DensityUv::radial_distance`], CASA `radial`), optionally tapered.
    Radial {
        /// Gaussian uv taper.
        taper: Option<Taper>,
    },
    /// Uniform (`robust` absent), Briggs, absolute Briggs or Briggs
    /// bandwidth-taper density weighting.
    Density {
        /// Gridded weight density.
        grid: DensityGrid,
        /// Briggs factors; `None` for uniform weighting.
        robust: Option<RobustFactors>,
        /// CASA `briggsbwtaper`; needs `robust`.
        bandwidth: Option<BandwidthTaper>,
        /// Gaussian uv taper.
        taper: Option<Taper>,
    },
}

impl WeightingGeneration {
    /// Uniform, Briggs or Briggs bandwidth-taper weighting over `grid`.
    pub fn density(
        grid: DensityGrid,
        robust: Option<f64>,
        bandwidth: Option<BandwidthTaper>,
        taper: Option<Taper>,
    ) -> Result<Self, OperatorError> {
        let robust = match robust {
            Some(robust) if !robust.is_finite() => {
                return Err(OperatorError::Weighting {
                    reason: "robustness is not finite",
                });
            }
            Some(robust) => Some(grid.robust_factors(robust)),
            None => None,
        };
        if bandwidth.is_some() && robust.is_none() {
            return Err(OperatorError::Weighting {
                reason: "the bandwidth taper is a Briggs mode and needs a robustness",
            });
        }
        Ok(Self::Density {
            grid,
            robust,
            bandwidth,
            taper,
        })
    }

    /// CASA `briggsabs` over `grid`: `w / (D·robust² + 2·noise²)`
    /// ([`DensityGrid::absolute_factors`]). Refused when both terms vanish,
    /// as `SynthesisImagerVi2::weight` refuses `robust = 0` with `noise = 0`.
    pub fn absolute_density(
        grid: DensityGrid,
        robust: f64,
        noise_jy: f64,
        taper: Option<Taper>,
    ) -> Result<Self, OperatorError> {
        if !(robust.is_finite() && noise_jy.is_finite()) {
            return Err(OperatorError::Weighting {
                reason: "absolute Briggs robustness and noise must be finite",
            });
        }
        let factors = grid.absolute_factors(robust, noise_jy);
        if factors.d2 == 0.0 && factors.f2.iter().all(|f2| *f2 == 0.0) {
            return Err(OperatorError::Weighting {
                reason: "absolute Briggs with robust and noise both zero divides by zero",
            });
        }
        Ok(Self::Density {
            grid,
            robust: Some(factors),
            bandwidth: None,
            taper,
        })
    }

    /// The padding of a per-channel density generation (CASA
    /// `BriggsCubeWeightor`), whose output samples take the nearest native
    /// channel's weight; `None` when native weights are interpolated
    /// linearly (`VisImagingWeight`).
    pub(crate) fn cube_padding(&self) -> Option<u32> {
        match self {
            Self::Density { grid, .. } if grid.shape.rule == DensityCellRule::Cube => {
                Some(grid.shape.padding)
            }
            Self::Natural { .. } | Self::Radial { .. } | Self::Density { .. } => None,
        }
    }

    /// The density plane a sample on output plane `plane` from FIELD_ID
    /// `field` reads ([`DensityGridShape::density_plane`]); `plane` for a
    /// weighting without a density.
    #[must_use]
    pub fn density_plane(&self, plane: u32, field: u32) -> Option<u32> {
        match self {
            Self::Density { grid, .. } => grid.shape.density_plane(plane, field),
            Self::Natural { .. } | Self::Radial { .. } => Some(plane),
        }
    }

    /// Imaging weight of one placement from its unpolarized input weight;
    /// `density_uv` picks the density cell on the density plane
    /// `placement.plane` (ignored by a global generation; a field's plane
    /// for a per-field one, [`Self::density_plane`]) and gives radial
    /// weighting its uv distance. Pure; CASA's cell rules live in
    /// [`DensityGrid::lookup`].
    #[must_use]
    pub fn imaging_weight(
        &self,
        placement: &Placement,
        density_uv: DensityUv,
        input_weight: f32,
    ) -> f32 {
        if input_weight <= 0.0 {
            return 0.0;
        }
        let input = f64::from(input_weight);
        let (weighted, taper) = match self {
            Self::Natural { taper } => (input, *taper),
            Self::Radial { taper } => (density_uv.radial_distance() * input, *taper),
            Self::Density {
                grid,
                robust,
                bandwidth,
                taper,
            } => {
                let Some(plane) = grid.shape.plane_of(placement.plane) else {
                    return 0.0;
                };
                // CASA weighs a sample whose density cell is empty or off
                // the grid at zero, under uniform and Briggs alike
                // (`VisImagingWeight::weightUniform` requires `a_gwt > 0`;
                // `BriggsCubeWeightor::getWeightUniform` likewise).
                let Some(density) = grid
                    .lookup(plane, density_uv)
                    .filter(|density| *density > 0.0)
                else {
                    return 0.0;
                };
                let weighted = match robust {
                    None => input / density,
                    Some(factors) => {
                        let distance = bandwidth.map_or(1.0, |bandwidth| {
                            bandwidth.uv_distance_factor(&grid.shape, placement.u, placement.v)
                        });
                        input / (density * factors.factor(plane) / distance + factors.offset())
                    }
                };
                (weighted, *taper)
            }
        };
        (weighted * gaussian_taper(taper, placement.u, placement.v)) as f32
    }
}

/// CASA `VisImagingWeight::filter`: an elliptical Gaussian in the uv plane.
fn gaussian_taper(taper: Option<Taper>, u: f64, v: f64) -> f64 {
    let Some(taper) = taper else {
        return 1.0;
    };
    let sine = taper.position_angle_rad().sin();
    let cosine = taper.position_angle_rad().cos();
    let rotated_u = sine * u + cosine * v;
    let rotated_v = -cosine * u + sine * v;
    let major = std::f64::consts::LN_2 / taper.major_lambda().powi(2);
    let minor = std::f64::consts::LN_2 / taper.minor_lambda().powi(2);
    (-major * rotated_u.powi(2) - minor * rotated_v.powi(2)).exp()
}
