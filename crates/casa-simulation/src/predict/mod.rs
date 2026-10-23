// SPDX-License-Identifier: LGPL-3.0-or-later
//! Visibility prediction from an external sky model, as CASA's simulator
//! tool predicts it (`sm.predict` with its default `GridFT`, or with
//! `WProjectFT` after `setoptions(ftmachine='wproject')`).
//!
//! CASA's `Simulator::predict` degrids the model through the FTMachine
//! `setoptions` chose ([`FtMachine`]):
//! - `GridFT`'s grid is padded by `setoptions(padding=…)`, 1.3 by default.
//!   `WProjectFT`'s is padded by 1, its constructor's default, and degrids
//!   through `wprojplanes` W-projection planes.
//! - The tangent point is the direction of pixel `(nx/2, ny/2)`, whatever
//!   the image's reference pixel is (`FTMachine::initMaps`), and every row is
//!   rotated to it (`FTMachine::rotateUVW`).
//! - Each MeasurementSet channel takes the nearest model plane, with no
//!   frequency-frame conversion.
//! - `GridFT` predicts autocorrelations, as the model's zero spacing (its
//!   `usezero` is on by default); `WProjectFT` is built with `usezero` off
//!   and predicts them as zero.
//! - After `setvp(dovp=True)`, a `VPSkyJones` multiplies the model by the
//!   primary beam's power pattern in the image plane, centred on the
//!   pointing.
//!
//! The model is prepared on the imaging measurement operator, one plane at
//! a time ([`SkyModel`] reads a plane, the beam multiplies it, the operator
//! transforms it into the plane's grids):
//! - the spheroidal or W-projection kernel set on a
//!   [`GridPadding::CasaComposite`] grid;
//! - a channel-local basis with nearest mapping, predicting
//!   autocorrelations through `GridFT`;
//! - the beam as the plane's prescale factor.
//!
//! The session predicts the rows it writes from the same preparation, in
//! blocks on the worker team; [`ModelPredictor`] predicts one row at a time
//! and is the reference the session is tested against.
//!
//! The model must be Stokes I, J2000 and in the axis order `simobserve`
//! writes.

mod beam;
mod components;
mod model;

pub(crate) use beam::BeamPattern;
pub use beam::PrimaryBeam;
pub(crate) use components::{
    ComponentPrediction, ComponentRefusal, ComponentTargets, ComponentWindow,
};
pub use model::SkyModel;

use casa_coordinates::CoordinateError;
use casa_images::ImageError;
use casa_imaging_model::{CorrelationType, PolarizationCoordinate};
use casa_imaging_operator::{
    ConvolutionFunctionSet, CpuBackend, GridGeometry, GridPadding, GridPrecision, ImageExtent,
    MeasurementOperator, ModeSet, ModelPlane, ModelPlaneGrids, NativeRow, OperatorError,
    PaddingFactor, PlaneRange, PolarizationRouting, PredictionScratch, PreparedModelGrids,
    RowContext, SpectralAxis, SpectralKernel, SpectralResampler, Spheroidal, WPlaneCount, WPlanes,
};
use casa_imaging_runtime::pass::WorkerTeam;
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
    UnsupportedModel(String),
    /// The measurement operator refused the model or a row.
    #[error(transparent)]
    Operator(#[from] OperatorError),
}

/// The FTMachine a prediction degrids through (`sm.setoptions(ftmachine=…)`,
/// `Simulator::createSkyEquation`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FtMachine {
    /// `GridFT` (`ftmachine='ft'`, the default): the spheroidal kernel on a
    /// grid padded by `setoptions(padding=…)`, predicting autocorrelations
    /// as the model's zero spacing.
    Grid(PaddingFactor),
    /// `WProjectFT` (`ftmachine='wproject'`) with `wprojplanes` planes, at
    /// least two, reaching `|w| = 0.25/|Δx|`. CASA's simulator builds it
    /// with its constructor's padding of 1, whatever `setoptions(padding=…)`
    /// says, and with `usezero` false, so autocorrelations predict zero.
    WProject(u32),
}

impl FtMachine {
    /// Whether the Metal backend predicts through this machine: `GridFT`'s
    /// spheroidal set only.
    #[must_use]
    pub const fn runs_on_metal(self) -> bool {
        matches!(self, Self::Grid(_))
    }
}

/// The grid, polarization routing and spectral resampler of a model's
/// operator for one FTMachine, before its kernels are built: the session
/// admits the kernels from it ([`Self::geometry`]) and builds them in stages,
/// as imaging does; [`Self::kernels`] builds them in one.
pub(crate) struct ModelLayout {
    machine: FtMachine,
    geometry: GridGeometry,
    polarization: PolarizationRouting,
    resampler: SpectralResampler,
}

impl ModelLayout {
    /// The layout for `model` on rows with `correlations` through `machine`.
    ///
    /// # Errors
    ///
    /// A geometry the grid cannot hold, or correlations the Stokes I model
    /// cannot feed.
    pub(crate) fn new(
        model: &SkyModel,
        correlations: &[CorrelationType],
        machine: FtMachine,
    ) -> Result<Self, PredictError> {
        let [nx, ny] = model.shape();
        let padding = match machine {
            FtMachine::Grid(padding) => padding,
            FtMachine::WProject(_) => PaddingFactor::SIMULATOR_WPROJECT,
        };
        let geometry = GridGeometry::new(
            ImageExtent {
                shape: [nx, ny],
                increment_rad: model.increment_rad,
                reference_pixel: [nx / 2, ny / 2],
            },
            GridPadding::CasaComposite(padding),
        )?;
        let polarization =
            PolarizationRouting::compile(correlations, &[PolarizationCoordinate::StokesI])?;
        let planes = u32::try_from(model.planes())
            .map_err(|_| PredictError::UnsupportedModel("too many model planes".into()))?;
        let axis = SpectralAxis::new(
            model.first_frequency_hz,
            model.frequency_increment_hz,
            planes,
        )?;
        let resampler = SpectralResampler::channel_local(axis, SpectralKernel::Nearest);
        let resampler = match machine {
            FtMachine::Grid(_) => resampler.predicting_autocorrelations(),
            FtMachine::WProject(_) => resampler,
        };
        Ok(Self {
            machine,
            geometry,
            polarization,
            resampler,
        })
    }

    /// The FTMachine.
    pub(crate) const fn machine(&self) -> FtMachine {
        self.machine
    }

    /// The padded grid the kernels are built for.
    pub(crate) const fn geometry(&self) -> &GridGeometry {
        &self.geometry
    }

    /// The polarization routing the kernels are built for.
    pub(crate) const fn polarization(&self) -> &PolarizationRouting {
        &self.polarization
    }

    /// The machine's kernel set, built in one step.
    ///
    /// # Errors
    ///
    /// The W-projection planes cannot be built.
    pub(crate) fn kernels(&self) -> Result<Box<dyn ConvolutionFunctionSet>, PredictError> {
        Ok(match self.machine {
            FtMachine::Grid(_) => Box::new(Spheroidal::new(&self.geometry, &self.polarization)),
            FtMachine::WProject(planes) => Box::new(WPlanes::new(
                &self.geometry,
                &self.polarization,
                WPlaneCount::Fixed(planes),
            )?),
        })
    }
}

/// The measurement operator and spectral resampler that predict one model
/// on rows of one correlation set, as CASA's `GridFT` or `WProjectFT` does,
/// and the preparation of the model's grids. The session and
/// [`ModelPredictor`] share it, so both predict from the same grids.
pub(crate) struct ModelOperator {
    pub(crate) operator: MeasurementOperator,
    pub(crate) resampler: SpectralResampler,
    machine: FtMachine,
}

impl ModelOperator {
    /// The operator of `layout` with `kernels` (its machine's set), in
    /// `precision`.
    pub(crate) fn new(
        layout: ModelLayout,
        kernels: Box<dyn ConvolutionFunctionSet>,
        precision: GridPrecision,
    ) -> Self {
        let ModelLayout {
            machine,
            geometry,
            polarization,
            resampler,
        } = layout;
        let operator = MeasurementOperator::new(
            geometry,
            resampler.basis(),
            polarization,
            kernels,
            precision,
        );
        Self {
            operator,
            resampler,
            machine,
        }
    }

    /// The FTMachine the operator degrids through.
    pub(crate) const fn machine(&self) -> FtMachine {
        self.machine
    }

    /// The planes of the model's grids: every plane of the basis.
    fn planes(&self) -> PlaneRange {
        PlaneRange::new(0, self.operator.basis().planes())
    }

    /// Bytes one set of the model's grids holds
    /// ([`PreparedModelGrids::bytes`]), known before they are allocated.
    pub(crate) fn grid_bytes(&self) -> u64 {
        self.operator
            .accumulator_layout(self.planes(), None, ModeSet::DATA)
            .bytes(self.operator.precision()) as u64
    }

    /// Bytes one worker holds while it prepares a plane, beside the grids:
    /// the plane read and its slice, the beam's power on it, and the
    /// transform's padded plane.
    pub(crate) fn plane_scratch_bytes(&self, model: &SkyModel) -> u64 {
        let [nx, ny] = model.shape();
        let [gx, gy] = self.operator.geometry().grid_shape();
        let cell = match self.operator.precision() {
            GridPrecision::F32 => size_of::<Complex32>(),
            GridPrecision::F64 => 2 * size_of::<Complex32>(),
        };
        model.plane_bytes() + (nx * ny * size_of::<f32>() + 2 * gx * gy * cell) as u64
    }

    /// The model's grids: each plane read, multiplied by `beam`'s power
    /// pattern centred on its pixel when there is one, and transformed into
    /// the plane's grids. Planes are prepared on `team`'s workers, one plane
    /// per job, or in turn on the caller without one; the grids do not
    /// depend on which.
    ///
    /// # Errors
    ///
    /// A plane that cannot be read, or the operator's refusal.
    pub(crate) fn prepare(
        &self,
        model: &SkyModel,
        beam: Option<(&BeamPattern, [f64; 2])>,
        team: Option<&WorkerTeam>,
    ) -> Result<PreparedModelGrids, PredictError> {
        let mut grids = self.operator.empty_model_grids(self.planes())?;
        let mut jobs: Vec<Option<ModelPlaneGrids<'_>>> =
            grids.planes_mut().into_iter().map(Some).collect();
        let prepare = |plane: usize, target: &mut Option<ModelPlaneGrids<'_>>| {
            let Some(target) = target.take() else {
                return Ok(());
            };
            let images = vec![model.read_plane(plane)?];
            let factor = beam.map(|(pattern, centre)| pattern.power_plane(model, plane, centre));
            self.operator
                .fill_model_plane(&ModelPlane { images }, factor.as_ref(), target)?;
            Ok::<_, PredictError>(())
        };
        match team {
            Some(team) => team.for_each_mut(&mut jobs, prepare)?,
            None => jobs
                .iter_mut()
                .enumerate()
                .try_for_each(|(plane, target)| prepare(plane, target))?,
        }
        drop(jobs);
        debug_assert_eq!(grids.bytes(), self.grid_bytes());
        Ok(grids)
    }
}

/// How [`ModelPredictor`] grids, as `sm.setoptions` and `sm.setvp` choose.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PredictOptions {
    /// The FTMachine, with `GridFT`'s padding (`setoptions(ftmachine=…,
    /// padding=…, wprojplanes=…)`).
    pub ft_machine: FtMachine,
    /// The primary beam, when `setvp(dovp=True)`.
    pub primary_beam: Option<PrimaryBeam>,
    /// Grid arithmetic. CASA's simulator FTMachines run in `Float`.
    pub precision: GridPrecision,
}

impl Default for PredictOptions {
    fn default() -> Self {
        Self {
            ft_machine: FtMachine::Grid(PaddingFactor::SIMULATOR),
            primary_beam: None,
            precision: GridPrecision::F32,
        }
    }
}

/// One MeasurementSet row to predict.
#[derive(Clone, Copy, Debug)]
pub struct PredictRow<'a> {
    /// Baseline coordinates in metres towards the model's tangent point, in
    /// GridFT's convention: the row's `UVW` rotated by
    /// [`casa_ms::derived::GridFtProjection`].
    pub uvw_m: [f64; 3],
    /// GridFT's phase shift in metres (`−dphase`), from the same rotation.
    pub phase_shift_m: f64,
    /// Channel frequencies in Hz.
    pub frequencies_hz: &'a [f64],
    /// Sample flags, `[channel][correlation]`.
    pub flags: &'a [bool],
    /// The row flag.
    pub row_flag: bool,
    /// Antenna pair; an autocorrelation predicts the model's zero spacing
    /// through `GridFT`, and zero through `WProjectFT`.
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

/// A model prepared for prediction on one correlation set, predicting a row
/// at a time on the CPU: the reference the session's block prediction is
/// tested against.
pub struct ModelPredictor {
    model: ModelOperator,
    grids: PreparedModelGrids,
}

impl ModelPredictor {
    /// Prepare `model` for rows with `correlations`, its primary beam (when
    /// requested) centred on `pointing_rad`.
    ///
    /// # Errors
    ///
    /// A geometry the grid cannot hold, correlations the Stokes I model
    /// cannot feed, a pointing outside the model's coordinate system, or a
    /// plane that cannot be read.
    pub fn new(
        model: &SkyModel,
        correlations: &[CorrelationType],
        pointing_rad: [f64; 2],
        options: &PredictOptions,
    ) -> Result<Self, PredictError> {
        let layout = ModelLayout::new(model, correlations, options.ft_machine)?;
        let kernels = layout.kernels()?;
        let operator = ModelOperator::new(layout, kernels, options.precision);
        let pattern = options.primary_beam.map(PrimaryBeam::pattern);
        let beam = match &pattern {
            Some(pattern) => Some((pattern, model.pixel_of(pointing_rad)?)),
            None => None,
        };
        let grids = operator.prepare(model, beam, None)?;
        Ok(Self {
            model: operator,
            grids,
        })
    }

    /// Correlations per sample.
    #[must_use]
    pub fn correlations(&self) -> usize {
        self.model.operator.polarization().correlations().len()
    }

    /// The model visibility of every sample of `row`, written to `out` as
    /// `[channel][correlation]`. Flagged samples and flagged rows predict
    /// zero; an autocorrelation predicts the model's zero spacing through
    /// `GridFT` and zero through `WProjectFT`.
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
        self.model.resampler.predict_row(
            &self.model.operator,
            &mut workspace.backend,
            &self.grids,
            &native,
            &mut workspace.scratch,
            out,
        )?;
        Ok(())
    }
}
