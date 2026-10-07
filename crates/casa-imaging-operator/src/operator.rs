// SPDX-License-Identifier: LGPL-3.0-or-later
//! The measurement operator: `A = Degrid ∘ Fft ∘ Correction ∘ PolBasis⁻¹ ∘
//! ModelPrescale` and `A* = PolBasis ∘ Correction ∘ Fft⁻¹ ∘ Grid`.

use ndarray::Array2;
use num_complex::{Complex, Complex64};

use crate::accumulator::{
    AccumulatorLayout, GridAccumulator, GridPrecision, GridScalar, GridStorage, Mode, ModeSet,
    PlaneRange, Tile,
};
use crate::backend::PreparedModelGrids;
use crate::convolution::ConvolutionFunctionSet;
use crate::error::OperatorError;
use crate::fft::PlaneFft;
use crate::geometry::GridGeometry;
use crate::polarization::PolarizationRouting;

/// Frequency-domain model basis.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Basis {
    /// One plane shared across frequency.
    Constant,
    /// One independent plane per output channel.
    ChannelLocal {
        /// Number of output channels.
        planes: u32,
    },
    /// Taylor polynomial in `(ν − ν₀)/ν₀` on one plane.
    Taylor {
        /// Number of Taylor terms.
        terms: u32,
        /// Reference frequency `ν₀` in Hz.
        reference_hz: f64,
    },
}

impl Basis {
    /// Grid planes.
    #[must_use]
    pub const fn planes(self) -> u32 {
        match self {
            Self::Constant | Self::Taylor { .. } => 1,
            Self::ChannelLocal { planes } => planes,
        }
    }

    /// Data (residual) terms per plane.
    #[must_use]
    pub const fn data_terms(self) -> usize {
        match self {
            Self::Constant | Self::ChannelLocal { .. } => 1,
            Self::Taylor { terms, .. } => terms as usize,
        }
    }

    /// PSF terms per plane: `2·N_t − 1` for a Taylor basis.
    #[must_use]
    pub const fn psf_terms(self) -> usize {
        2 * self.data_terms() - 1
    }

    /// The Taylor expansion variable `(ν − ν₀)/ν₀` at `freq_hz`; 0 for
    /// other bases.
    #[must_use]
    pub fn spectral(self, freq_hz: f64) -> f32 {
        match self {
            Self::Taylor { reference_hz, .. } => ((freq_hz - reference_hz) / reference_hz) as f32,
            Self::Constant | Self::ChannelLocal { .. } => 0.0,
        }
    }
}

/// Model coefficient images for a range of planes, in requested
/// polarization coordinates.
#[derive(Clone, Debug, PartialEq)]
pub struct ModelImages {
    /// Grid plane of `planes[0]`.
    pub first_plane: u32,
    /// One entry per plane.
    pub planes: Vec<ModelPlane>,
}

/// Model images of one plane: `images[term * pols + pol]`, each `[y][x]`
/// over the image extent.
#[derive(Clone, Debug, PartialEq)]
pub struct ModelPlane {
    /// Term-major, polarization-minor images.
    pub images: Vec<Array2<f32>>,
}

/// Image-domain factor applied to the model before correction (the primary
/// beam when the product contract is `flatnoise`).
#[derive(Clone, Copy, Debug)]
pub enum ModelPrescale<'a> {
    /// No factor.
    Unit,
    /// One `[y][x]` factor per model plane, shared by every term and polarization.
    Planes(&'a [Array2<f32>]),
}

/// Unnormalised image-domain results of one accumulator, in requested
/// polarization coordinates.
#[derive(Clone, Debug, PartialEq)]
pub struct NormalImages {
    /// Grid plane of `planes[0]`.
    pub first_plane: u32,
    /// One entry per accumulated plane.
    pub planes: Vec<NormalPlane>,
}

/// Images of one plane, one section per accumulated mode.
#[derive(Clone, Debug, PartialEq)]
pub struct NormalPlane {
    /// Dirty or residual terms.
    pub data: Option<NormalSection>,
    /// Point-spread-function terms.
    pub psf: Option<NormalSection>,
    /// Sensitivity image.
    pub weight: Option<NormalSection>,
}

/// Images of one mode: `images[term * pols + pol]` with the matching
/// `sumwt`, each image `[y][x]` over the image extent.
#[derive(Clone, Debug, PartialEq)]
pub struct NormalSection {
    /// Term-major, polarization-minor images.
    pub images: Vec<Array2<f32>>,
    /// `sumwt` per image (CASA: a Stokes plane carries grid plane 0's).
    pub sumwt: Vec<f64>,
    /// Terms.
    pub terms: usize,
    /// Requested polarizations.
    pub pols: usize,
}

impl NormalSection {
    /// Image of `(term, pol)`.
    #[must_use]
    pub fn image(&self, term: usize, pol: usize) -> &Array2<f32> {
        &self.images[term * self.pols + pol]
    }

    /// `sumwt` of `(term, pol)`.
    #[must_use]
    pub fn sumwt_of(&self, term: usize, pol: usize) -> f64 {
        self.sumwt[term * self.pols + pol]
    }
}

/// The measurement operator for one geometry, basis, polarization routing,
/// kernel set and precision.
pub struct MeasurementOperator {
    geometry: GridGeometry,
    basis: Basis,
    polarization: PolarizationRouting,
    cf: Box<dyn ConvolutionFunctionSet>,
    precision: GridPrecision,
}

impl MeasurementOperator {
    /// Assemble an operator. The kernel set's routing must match
    /// `polarization` and its correction the padded grid.
    #[must_use]
    pub fn new(
        geometry: GridGeometry,
        basis: Basis,
        polarization: PolarizationRouting,
        cf: Box<dyn ConvolutionFunctionSet>,
        precision: GridPrecision,
    ) -> Self {
        let mueller = cf.mueller();
        assert_eq!(
            mueller.visibility_pols(),
            polarization.correlations().len(),
            "kernel set routing must cover the selected correlations"
        );
        assert_eq!(
            mueller.grid_pols(),
            polarization.grid_pols(),
            "kernel set routing must cover the grid polarizations"
        );
        let correction = cf.image_correction();
        let [nx, ny] = geometry.grid_shape();
        assert_eq!(
            correction.x().len(),
            nx,
            "correction x axis must span the grid"
        );
        assert_eq!(
            correction.y().len(),
            ny,
            "correction y axis must span the grid"
        );
        Self {
            geometry,
            basis,
            polarization,
            cf,
            precision,
        }
    }

    /// The grid geometry.
    #[must_use]
    pub const fn geometry(&self) -> &GridGeometry {
        &self.geometry
    }

    /// The model basis.
    #[must_use]
    pub const fn basis(&self) -> Basis {
        self.basis
    }

    /// The polarization routing.
    #[must_use]
    pub const fn polarization(&self) -> &PolarizationRouting {
        &self.polarization
    }

    /// The kernel set.
    #[must_use]
    pub fn cf(&self) -> &dyn ConvolutionFunctionSet {
        self.cf.as_ref()
    }

    /// The grid precision.
    #[must_use]
    pub const fn precision(&self) -> GridPrecision {
        self.precision
    }

    /// A zeroed accumulator holding `modes` over `planes`, covering `tile`
    /// or the whole grid.
    #[must_use]
    pub fn accumulator(
        &self,
        planes: PlaneRange,
        tile: Option<Tile>,
        modes: ModeSet,
    ) -> GridAccumulator {
        assert!(
            planes.end <= self.basis.planes(),
            "plane range exceeds the basis"
        );
        let layout = AccumulatorLayout::new(
            self.geometry.clone(),
            planes,
            self.polarization.grid_pols(),
            modes,
            self.basis.data_terms(),
            self.basis.psf_terms(),
            tile,
        );
        GridAccumulator::new(layout, self.precision)
    }

    /// Prepare `model` for degridding: expand requested polarizations to
    /// grid planes, multiply by `prescale` and the correction, embed in the
    /// padded grid and transform forward.
    pub fn prepare_model(
        &self,
        model: &ModelImages,
        prescale: ModelPrescale<'_>,
    ) -> Result<PreparedModelGrids, OperatorError> {
        let planes = PlaneRange::new(
            model.first_plane,
            model.first_plane + model.planes.len() as u32,
        );
        if planes.end > self.basis.planes() {
            return Err(OperatorError::ModelShape {
                reason: "plane range exceeds the basis",
            });
        }
        let terms = self.basis.data_terms();
        let requested = self.polarization.requested().len();
        let image_shape = self.geometry.image().shape;
        let expected = [image_shape[1], image_shape[0]];
        for plane in &model.planes {
            if plane.images.len() != terms * requested {
                return Err(OperatorError::ModelShape {
                    reason: "images per plane must be terms × requested polarizations",
                });
            }
            if plane.images.iter().any(|image| image.shape() != expected) {
                return Err(OperatorError::ModelShape {
                    reason: "image shape must be [ny, nx] of the image extent",
                });
            }
        }
        if let ModelPrescale::Planes(factors) = prescale {
            if factors.len() != model.planes.len() {
                return Err(OperatorError::ModelShape {
                    reason: "one prescale image per model plane",
                });
            }
            if factors.iter().any(|factor| factor.shape() != expected) {
                return Err(OperatorError::ModelShape {
                    reason: "prescale image shape must be [ny, nx] of the image extent",
                });
            }
        }
        let layout = AccumulatorLayout::new(
            self.geometry.clone(),
            planes,
            self.polarization.grid_pols(),
            ModeSet::DATA,
            terms,
            self.basis.psf_terms(),
            None,
        );
        let storage = match self.precision {
            GridPrecision::F32 => {
                GridStorage::F32(self.model_grids::<f32>(&layout, model, prescale)?)
            }
            GridPrecision::F64 => {
                GridStorage::F64(self.model_grids::<f64>(&layout, model, prescale)?)
            }
        };
        Ok(PreparedModelGrids::new(layout, storage))
    }

    fn model_grids<T: GridScalar>(
        &self,
        layout: &AccumulatorLayout,
        model: &ModelImages,
        prescale: ModelPrescale<'_>,
    ) -> Result<Vec<Complex<T>>, OperatorError> {
        let [nx, ny] = self.geometry.grid_shape();
        let [ix, iy] = self.geometry.image_origin();
        let image_shape = self.geometry.image().shape;
        let correction = self.cf.image_correction();
        let requested = self.polarization.requested().len();
        let gpols = self.polarization.grid_pols();
        let terms = layout.terms();
        let mut fft = PlaneFft::<T>::new([nx, ny])?;
        let mut cells = vec![Complex::<T>::default(); layout.cells()];
        for (plane_local, plane) in model.planes.iter().enumerate() {
            let factor = match prescale {
                ModelPrescale::Unit => None,
                ModelPrescale::Planes(factors) => Some(&factors[plane_local]),
            };
            for term in 0..terms {
                for gpol in 0..gpols {
                    let offset = layout.block_offset(plane_local, gpol, term);
                    let grid = &mut cells[offset..offset + layout.block_cells()];
                    for y in 0..image_shape[1] {
                        for x in 0..image_shape[0] {
                            let mut value = Complex64::default();
                            for pol in 0..requested {
                                let coefficient = self.polarization.from_requested(gpol, pol);
                                if coefficient != Complex64::default() {
                                    value += coefficient
                                        * f64::from(plane.images[term * requested + pol][(y, x)]);
                                }
                            }
                            if let Some(factor) = factor {
                                value *= f64::from(factor[(y, x)]);
                            }
                            value *= correction.at(ix + x, iy + y);
                            grid[(iy + y) * nx + ix + x] =
                                Complex::new(T::from_f64(value.re), T::from_f64(value.im));
                        }
                    }
                    fft.transform(grid, false)?;
                }
            }
        }
        Ok(cells)
    }

    /// Transform an accumulator to unnormalised images: inverse FFT,
    /// correction, crop to the image extent and conversion to the requested
    /// polarization coordinates, with `sumwt` per image.
    ///
    /// The accumulator must cover the whole grid; merge tiles first.
    pub fn finish(&self, acc: GridAccumulator) -> Result<NormalImages, OperatorError> {
        if !acc.layout().is_full_grid() {
            return Err(OperatorError::TiledAccumulator);
        }
        match acc.precision() {
            GridPrecision::F32 => self.finish_planes::<f32>(&acc),
            GridPrecision::F64 => self.finish_planes::<f64>(&acc),
        }
    }

    fn finish_planes<T: GridScalar>(
        &self,
        acc: &GridAccumulator,
    ) -> Result<NormalImages, OperatorError> {
        let layout = acc.layout();
        let [nx, ny] = self.geometry.grid_shape();
        let mut fft = PlaneFft::<T>::new([nx, ny])?;
        let mut work = vec![Complex::<T>::default(); nx * ny];
        let gpols = layout.pols();
        let mut gpol_images = vec![Vec::<Complex64>::new(); gpols];
        let mut planes = Vec::with_capacity(layout.planes().len());
        for plane_local in 0..layout.planes().len() {
            let mut section = |mode: Mode| -> Result<Option<NormalSection>, OperatorError> {
                let Some(terms) = layout.term_range(mode) else {
                    return Ok(None);
                };
                let mut images =
                    Vec::with_capacity(terms.len() * self.polarization.requested().len());
                let mut sumwt = Vec::with_capacity(images.capacity());
                for term in terms.clone() {
                    for (gpol, image) in gpol_images.iter_mut().enumerate() {
                        work.copy_from_slice(acc.block::<T>(plane_local, gpol, term));
                        fft.transform(&mut work, true)?;
                        *image = self.cropped_image(&work);
                    }
                    for pol in 0..self.polarization.requested().len() {
                        images.push(self.requested_image(pol, &gpol_images));
                        sumwt.push(acc.sumwt_at(
                            plane_local,
                            self.polarization.sumwt_source(pol),
                            term,
                        ));
                    }
                }
                Ok(Some(NormalSection {
                    images,
                    sumwt,
                    terms: terms.len(),
                    pols: self.polarization.requested().len(),
                }))
            };
            let data = section(Mode::Data)?;
            let psf = section(Mode::Psf)?;
            let weight = section(Mode::Weight)?;
            planes.push(NormalPlane { data, psf, weight });
        }
        Ok(NormalImages {
            first_plane: layout.planes().start,
            planes,
        })
    }

    /// Correction and crop of one transformed grid plane, `[y][x]` over the
    /// image extent.
    fn cropped_image<T: GridScalar>(&self, grid: &[Complex<T>]) -> Vec<Complex64> {
        let [nx, _] = self.geometry.grid_shape();
        let [ix, iy] = self.geometry.image_origin();
        let [width, height] = self.geometry.image().shape;
        let correction = self.cf.image_correction();
        let mut image = Vec::with_capacity(width * height);
        for y in 0..height {
            for x in 0..width {
                let cell = grid[(iy + y) * nx + ix + x];
                let factor = correction.at(ix + x, iy + y);
                image.push(Complex64::new(
                    cell.re.into_f64() * factor,
                    cell.im.into_f64() * factor,
                ));
            }
        }
        image
    }

    /// Real part of the polarization basis conversion of the cropped grid
    /// planes into requested plane `pol`.
    fn requested_image(&self, pol: usize, gpol_images: &[Vec<Complex64>]) -> Array2<f32> {
        let [width, height] = self.geometry.image().shape;
        let mut image = Array2::<f32>::zeros((height, width));
        for (gpol, grid_image) in gpol_images.iter().enumerate() {
            let coefficient = self.polarization.to_requested(pol, gpol);
            if coefficient == Complex64::default() {
                continue;
            }
            for (target, value) in image.iter_mut().zip(grid_image) {
                *target += (coefficient * value).re as f32;
            }
        }
        image
    }
}
