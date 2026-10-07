// SPDX-License-Identifier: LGPL-3.0-or-later
//! Global visibility weighting: natural, uniform and Briggs density
//! weights with an optional Gaussian taper, pinned to CASA
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
    /// `BriggsCubeWeightor`: rounded coordinates with `v` mirrored, built in
    /// double and looked up in single precision.
    Cube,
}

/// Shape of a density grid: image-sized cells per plane.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DensityGridShape {
    /// Cells along u (the image width).
    pub width: usize,
    /// Cells along v (the image height).
    pub height: usize,
    /// Density planes: 1 for a global generation, the output channel count
    /// for a per-channel generation.
    pub planes: usize,
    /// Signed image increments `[Δx, Δy]` in radians.
    pub increment_rad: [f64; 2],
    /// Cell arithmetic.
    pub rule: DensityCellRule,
}

impl DensityGridShape {
    /// Cells over every plane.
    #[must_use]
    pub const fn cells(&self) -> usize {
        self.width * self.height * self.planes
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

    fn build_cell(&self, plane: usize, u: f64, v: f64) -> Option<usize> {
        match self.rule {
            DensityCellRule::Standard => self.standard_cell(plane, u, v),
            DensityCellRule::Cube => {
                let width = self.width as f64;
                let height = self.height as f64;
                let x =
                    (u * width * self.increment_rad[0] + width / 2.0 + 1.0).round() as isize - 1;
                let y =
                    (-v * height * self.increment_rad[1] + height / 2.0 + 1.0).round() as isize - 1;
                self.cell_index(plane, x, y)
            }
        }
    }

    fn lookup_cell(&self, plane: usize, u: f64, v: f64) -> Option<usize> {
        match self.rule {
            DensityCellRule::Standard => self.standard_cell(plane, u, v),
            DensityCellRule::Cube => {
                let width = self.width as f32;
                let height = self.height as f32;
                let x = ((u as f32) * width * (self.increment_rad[0] as f32) + width / 2.0).round();
                let y =
                    (-(v as f32) * height * (self.increment_rad[1] as f32) + height / 2.0).round();
                self.cell_index(plane, x as isize, y as isize)
            }
        }
    }

    /// CASA stores uv coordinates and scales in `Float` and truncates the
    /// cell coordinate toward zero.
    fn standard_cell(&self, plane: usize, u: f64, v: f64) -> Option<usize> {
        let width = self.width as f32;
        let height = self.height as f32;
        let x = ((u as f32) * width * (self.increment_rad[0] as f32) + width / 2.0) as isize;
        let y = ((v as f32) * height * (self.increment_rad[1] as f32) + height / 2.0) as isize;
        self.cell_index(plane, x, y)
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
    /// weight; each sample adds its weight at its cell and at the conjugate
    /// cell, each when that cell lies inside the grid (CASA tests the two
    /// independently), and once to the plane's weight sum when its own cell
    /// does.
    pub fn accumulate(&mut self, block: &SampleBlock<'_>) {
        assert_eq!(block.npol, 1, "density blocks carry one unpolarized weight");
        for (placement, weight) in block.placements.iter().zip(block.weights) {
            if *weight <= 0.0 {
                continue;
            }
            let Some(plane) = self.shape.plane_of(placement.plane) else {
                continue;
            };
            let weight = f64::from(*weight);
            let primary = self.shape.build_cell(plane, placement.u, placement.v);
            let conjugate = self.shape.build_cell(plane, -placement.u, -placement.v);
            if let Some(cell) = primary {
                self.cells[cell] += weight;
                self.sum_weights[plane] += weight;
            }
            if let Some(cell) = conjugate {
                self.cells[cell] += weight;
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

    /// Input weight summed once per sample, per plane.
    #[must_use]
    pub fn sum_weights(&self) -> &[f64] {
        &self.sum_weights
    }

    /// Density at the lookup cell of `(u, v)` on `plane`; `None` outside
    /// the grid.
    #[must_use]
    pub fn lookup(&self, plane: usize, u: f64, v: f64) -> Option<f64> {
        self.shape
            .lookup_cell(plane, u, v)
            .map(|cell| self.cells[cell])
    }

    /// Briggs factors `f² = (5·10^{−robust})² / (Σd² / Σd)` per plane, with
    /// `Σd` the density sum (standard) or twice the input weight sum (cube).
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
        RobustFactors { robust, f2 }
    }
}

/// Briggs `f²` per density plane for one robustness.
#[derive(Clone, Debug, PartialEq)]
pub struct RobustFactors {
    robust: f64,
    f2: Vec<f64>,
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
    /// Uniform (`robust` absent), Briggs or Briggs bandwidth-taper density
    /// weighting.
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

    /// Imaging weight of one placement from its unpolarized input weight.
    /// Pure; CASA's cell rules live in [`DensityGrid::lookup`].
    #[must_use]
    pub fn imaging_weight(&self, placement: &Placement, input_weight: f32) -> f32 {
        if input_weight <= 0.0 {
            return 0.0;
        }
        let input = f64::from(input_weight);
        let (weighted, taper) = match self {
            Self::Natural { taper } => (input, *taper),
            Self::Density {
                grid,
                robust,
                bandwidth,
                taper,
            } => {
                let Some(plane) = grid.shape.plane_of(placement.plane) else {
                    return 0.0;
                };
                let Some(density) = grid.lookup(plane, placement.u, placement.v) else {
                    return 0.0;
                };
                let weighted = match robust {
                    None => {
                        if density > 0.0 {
                            input / density
                        } else {
                            0.0
                        }
                    }
                    Some(factors) => {
                        if grid.shape.rule == DensityCellRule::Cube && density <= 0.0 {
                            return 0.0;
                        }
                        let distance = bandwidth.map_or(1.0, |bandwidth| {
                            bandwidth.uv_distance_factor(&grid.shape, placement.u, placement.v)
                        });
                        input / (density * factors.factor(plane) / distance + 1.0)
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
