// SPDX-License-Identifier: LGPL-3.0-or-later
//! Visibility prediction from an external sky model, as CASA's simulator
//! tool predicts it (`sm.predict` with its default `GridFT`).
//!
//! CASA's `Simulator::predict` degrids the model through a `GridFT`
//! FTMachine:
//! - The grid is padded by `setoptions(padding=…)`, 1.3 by default.
//! - The tangent point is the direction of pixel `(nx/2, ny/2)`, whatever
//!   the image's reference pixel is (`FTMachine::initMaps`).
//! - Each MeasurementSet channel takes the nearest model plane, with no
//!   frequency-frame conversion.
//! - After `setvp(dovp=True)`, a `VPSkyJones` multiplies the model by the
//!   primary beam's power pattern in the image plane, centred on the
//!   pointing.
//!
//! [`ModelPredictor`] does the same on the imaging measurement operator:
//! - the spheroidal kernel set on a [`GridPadding::CasaComposite`] grid;
//! - a channel-local basis with nearest mapping;
//! - the beam as a [`ModelPrescale::Planes`] factor.
//!
//! Gaps remain:
//! - Autocorrelations predict zero. CASA's `GridFT` predicts them, but
//!   `simobserve` turns them off with `setauto(0)`.
//! - The model must be Stokes I.
//! - Rows must already be in GridFT's uvw convention ([`PredictRow`]).

use std::path::Path;

use casa_coordinates::{CoordinateError, CoordinateType};
use casa_images::{ImageError, PagedImage};
use casa_imaging_model::{CorrelationType, PolarizationCoordinate};
use casa_imaging_operator::{
    CpuBackend, GridGeometry, GridPadding, GridPrecision, ImageExtent, MeasurementOperator,
    ModelImages, ModelPlane, ModelPrescale, NativeRow, OperatorError, PaddingFactor,
    PolarizationRouting, PredictionScratch, PreparedModelGrids, RowContext, SpectralAxis,
    SpectralKernel, SpectralResampler, Spheroidal,
};
use casa_simulation_synthesis::{AiryPrimaryBeam, AiryVoltagePattern};
use ndarray::{Array2, IxDyn};
use num_complex::Complex32;
use thiserror::Error;

/// A failure to read a sky model or to predict from it.
#[derive(Debug, Error)]
pub enum PredictError {
    /// The model image could not be read.
    #[error("model image {path}: {source}")]
    Image {
        /// The image path.
        path: String,
        /// The image error.
        #[source]
        source: ImageError,
    },
    /// A pixel could not be converted to a world coordinate, or back.
    #[error("model image coordinates: {0}")]
    Coordinates(#[from] CoordinateError),
    /// The model image has a shape or coordinate system this predictor does
    /// not support yet.
    #[error("unsupported model image: {0}")]
    UnsupportedModel(&'static str),
    /// The measurement operator refused the model or a row.
    #[error(transparent)]
    Operator(#[from] OperatorError),
}

/// A CASA model image as `sm.predict` reads it: Stokes I planes in
/// Jy/pixel, with the direction of pixel `(nx/2, ny/2)` and the frequency of
/// every plane.
#[derive(Clone, Debug)]
pub struct SkyModel {
    /// Model planes, each `[y][x]`.
    planes: Vec<Array2<f32>>,
    /// Signed radians per pixel `[Δx, Δy]`.
    increment_rad: [f64; 2],
    /// J2000 direction of pixel `(nx/2, ny/2)`, CASA's tangent point.
    centre_rad: [f64; 2],
    /// Frequency of plane 0 in Hz.
    first_frequency_hz: f64,
    /// Frequency step between planes in Hz.
    frequency_increment_hz: f64,
    /// The image's coordinate system, which places the pointing (the beam
    /// centre) on the model's pixels.
    coordinates: casa_coordinates::CoordinateSystem,
}

impl SkyModel {
    /// Read a CASA image with axes `[direction x, direction y, Stokes,
    /// spectral]`, one Stokes plane (I), as `simobserve` writes its sky
    /// model.
    ///
    /// # Errors
    ///
    /// An unreadable image, or one whose axes are not those above.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, PredictError> {
        let path = path.as_ref();
        let image = PagedImage::<f32>::open(path).map_err(|source| PredictError::Image {
            path: path.display().to_string(),
            source,
        })?;
        let shape = image.shape().to_vec();
        let coordinates = image.coordinates().clone();
        if shape.len() != 4
            || coordinates.find_coordinate(CoordinateType::Direction) != Some(0)
            || coordinates.find_coordinate(CoordinateType::Stokes) != Some(1)
            || coordinates.find_coordinate(CoordinateType::Spectral) != Some(2)
        {
            return Err(PredictError::UnsupportedModel(
                "axes must be direction, Stokes and spectral, in that order",
            ));
        }
        if shape[2] != 1 || coordinates.coordinate(1).to_world(&[0.0])?[0] != 1.0 {
            return Err(PredictError::UnsupportedModel(
                "the model must be Stokes I only",
            ));
        }
        let [nx, ny, nplanes] = [shape[0], shape[1], shape[3]];
        let pixels =
            image
                .get_slice(&[0, 0, 0, 0], &shape)
                .map_err(|source| PredictError::Image {
                    path: path.display().to_string(),
                    source,
                })?;
        let planes = (0..nplanes)
            .map(|plane| Array2::from_shape_fn((ny, nx), |(y, x)| pixels[IxDyn(&[x, y, 0, plane])]))
            .collect();
        let increment = coordinates.coordinate(0).increment();
        let centre = coordinates.to_world(&[(nx / 2) as f64, (ny / 2) as f64, 0.0, 0.0])?;
        Ok(Self {
            planes,
            increment_rad: [increment[0], increment[1]],
            centre_rad: [centre[0], centre[1]],
            first_frequency_hz: centre[3],
            frequency_increment_hz: coordinates.coordinate(2).increment()[0],
            coordinates,
        })
    }

    /// `[nx, ny]` pixels.
    #[must_use]
    pub fn shape(&self) -> [usize; 2] {
        let [ny, nx] = [self.planes[0].nrows(), self.planes[0].ncols()];
        [nx, ny]
    }

    /// J2000 direction of pixel `(nx/2, ny/2)`, CASA's tangent point.
    #[must_use]
    pub fn centre_rad(&self) -> [f64; 2] {
        self.centre_rad
    }

    /// Number of model planes.
    #[must_use]
    pub fn planes(&self) -> usize {
        self.planes.len()
    }

    /// Frequency of each model plane, in Hz.
    fn plane_frequency_hz(&self, plane: usize) -> f64 {
        self.first_frequency_hz + plane as f64 * self.frequency_increment_hz
    }

    /// Fractional pixel `[x, y]` of a J2000 direction.
    fn pixel_of(&self, direction_rad: [f64; 2]) -> Result<[f64; 2], PredictError> {
        let pixel = self.coordinates.to_pixel(&[
            direction_rad[0],
            direction_rad[1],
            1.0,
            self.first_frequency_hz,
        ])?;
        Ok([pixel[0], pixel[1]])
    }
}

/// The primary-beam voltage pattern `setvp(dovp=True)` applies.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PrimaryBeam {
    /// CASA `PBMath1DAiry`: an Airy pattern for a dish with a central
    /// blockage.
    Airy {
        /// Effective dish diameter in metres.
        dish_diameter_m: f64,
        /// Blockage diameter in metres.
        blockage_diameter_m: f64,
    },
}

impl PrimaryBeam {
    /// ALMA 12 m antennas as CASA's voltage-pattern manager models them:
    /// 10.7 m effective diameter, 0.75 m blockage.
    pub const ALMA: Self = Self::Airy {
        dish_diameter_m: 10.7,
        blockage_diameter_m: 0.75,
    };

    /// The power pattern on each model plane, `[y][x]`, centred on the
    /// pixel `centre`, at the plane's frequency (`BeamSkyJones::apply`
    /// squares the voltage pattern).
    fn power_planes(self, model: &SkyModel, centre: [f64; 2]) -> Vec<Array2<f32>> {
        let Self::Airy {
            dish_diameter_m,
            blockage_diameter_m,
        } = self;
        let pattern = AiryVoltagePattern::new(AiryPrimaryBeam {
            dish_diameter_m,
            blockage_diameter_m,
        });
        let [nx, ny] = model.shape();
        (0..model.planes())
            .map(|plane| {
                let frequency_hz = model.plane_frequency_hz(plane);
                Array2::from_shape_fn((ny, nx), |(y, x)| {
                    let l = (x as f64 - centre[0]) * model.increment_rad[0];
                    let m = (y as f64 - centre[1]) * model.increment_rad[1];
                    let voltage = pattern.evaluate_offsets(l, m, frequency_hz);
                    voltage * voltage
                })
            })
            .collect()
    }
}

/// How [`ModelPredictor`] grids, as `sm.setoptions` and `sm.setvp` choose.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PredictOptions {
    /// Grid padding (`setoptions(padding=…)`).
    pub padding: PaddingFactor,
    /// The primary beam, when `setvp(dovp=True)`.
    pub primary_beam: Option<PrimaryBeam>,
    /// Grid arithmetic. CASA's simulator `GridFT` runs in `Float`.
    pub precision: GridPrecision,
}

impl Default for PredictOptions {
    fn default() -> Self {
        Self {
            padding: PaddingFactor::SIMULATOR,
            primary_beam: None,
            precision: GridPrecision::F32,
        }
    }
}

/// One MeasurementSet row to predict.
#[derive(Clone, Copy, Debug)]
pub struct PredictRow<'a> {
    /// Baseline coordinates in metres towards the model's tangent point, in
    /// GridFT's convention (the MeasurementSet's own `UVW` when the row's
    /// phase centre is the tangent point).
    pub uvw_m: [f64; 3],
    /// GridFT's phase shift in metres (`−dphase`); zero when the row's phase
    /// centre is the tangent point.
    pub phase_shift_m: f64,
    /// Channel frequencies in Hz.
    pub frequencies_hz: &'a [f64],
    /// Sample flags, `[channel][correlation]`.
    pub flags: &'a [bool],
    /// The row flag.
    pub row_flag: bool,
    /// Antenna pair.
    pub antennas: [u32; 2],
}

/// Reusable per-thread buffers for [`ModelPredictor::predict_row`].
#[derive(Debug, Default)]
pub struct PredictionWorkspace {
    scratch: PredictionScratch,
    backend: CpuBackend,
    values: Vec<Complex32>,
    weights: Vec<f32>,
}

/// A model prepared for prediction on one correlation set.
pub struct ModelPredictor {
    operator: MeasurementOperator,
    resampler: SpectralResampler,
    grids: PreparedModelGrids,
}

impl ModelPredictor {
    /// Prepare `model` for rows with `correlations`, its primary beam (when
    /// requested) centred on `pointing_rad`.
    ///
    /// # Errors
    ///
    /// A geometry the grid cannot hold, correlations the Stokes I model
    /// cannot feed, or a pointing outside the model's coordinate system.
    pub fn new(
        model: &SkyModel,
        correlations: &[CorrelationType],
        pointing_rad: [f64; 2],
        options: &PredictOptions,
    ) -> Result<Self, PredictError> {
        let [nx, ny] = model.shape();
        let geometry = GridGeometry::new(
            ImageExtent {
                shape: [nx, ny],
                increment_rad: model.increment_rad,
                reference_pixel: [nx / 2, ny / 2],
            },
            GridPadding::CasaComposite(options.padding),
        )?;
        let polarization =
            PolarizationRouting::compile(correlations, &[PolarizationCoordinate::StokesI])?;
        let planes = u32::try_from(model.planes())
            .map_err(|_| PredictError::UnsupportedModel("too many model planes"))?;
        let axis = SpectralAxis::new(
            model.first_frequency_hz,
            model.frequency_increment_hz,
            planes,
        )?;
        let resampler = SpectralResampler::channel_local(axis, SpectralKernel::Nearest);
        let kernels = Spheroidal::new(&geometry, &polarization);
        let operator = MeasurementOperator::new(
            geometry,
            resampler.basis(),
            polarization,
            Box::new(kernels),
            options.precision,
        );
        let images = ModelImages {
            first_plane: 0,
            planes: model
                .planes
                .iter()
                .map(|plane| ModelPlane {
                    images: vec![plane.clone()],
                })
                .collect(),
        };
        let beam = match options.primary_beam {
            Some(beam) => Some(beam.power_planes(model, model.pixel_of(pointing_rad)?)),
            None => None,
        };
        let prescale = beam
            .as_deref()
            .map_or(ModelPrescale::Unit, ModelPrescale::Planes);
        let grids = operator.prepare_model(&images, prescale)?;
        Ok(Self {
            operator,
            resampler,
            grids,
        })
    }

    /// Correlations per sample.
    #[must_use]
    pub fn correlations(&self) -> usize {
        self.operator.polarization().correlations().len()
    }

    /// The model visibility of every sample of `row`, written to `out` as
    /// `[channel][correlation]`. Flagged samples, flagged rows and
    /// autocorrelations predict zero.
    ///
    /// # Errors
    ///
    /// A row inconsistent with the predictor, or an `out` of the wrong
    /// length.
    pub fn predict_row(
        &self,
        workspace: &mut PredictionWorkspace,
        row: &PredictRow<'_>,
        out: &mut [Complex32],
    ) -> Result<(), PredictError> {
        let cells = row.frequencies_hz.len() * self.correlations();
        workspace.values.resize(cells, Complex32::new(0.0, 0.0));
        workspace.weights.resize(cells, 1.0);
        let native = NativeRow {
            uvw_m: row.uvw_m,
            phase_shift_m: row.phase_shift_m,
            pointing_offset_rad: [0.0; 2],
            frequencies_hz: row.frequencies_hz,
            values: &workspace.values,
            weights: &workspace.weights,
            flags: row.flags,
            row_flag: row.row_flag,
            context: RowContext {
                original_uvw_m: None,
                time_s: 0.0,
                antennas: row.antennas,
                antenna_types: [0, 0],
                parallactic_angle_rad: [0.0, 0.0],
                field: 0,
                spectral_window: 0,
            },
        };
        self.resampler.predict_row(
            &self.operator,
            &mut workspace.backend,
            &self.grids,
            &native,
            &mut workspace.scratch,
            out,
        )?;
        Ok(())
    }
}
