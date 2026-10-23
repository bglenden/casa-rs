// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.predict`, `sm.setoptions` and `sm.setvp`: the model DATA is predicted
//! from.
//!
//! As in CASA's `Simulator`:
//! - `setoptions` and `setvp` only record; `predict` reads them.
//! - `predict` reads the model image when it is called and predicts it into
//!   the rows observed so far: replacing their DATA, or adding to it when
//!   `incremental`. Scans observed later are not predicted by it.
//! - The primary beam (`setvp(dovp=True)`) is the default voltage pattern of
//!   the telescope of the first `observe` (OBSERVATION row 0), centred on
//!   each field.
//!
//! The model is prepared when `predict` is called (read, multiplied by the
//! beam and transformed, one set of grids per field when the beam is on), so
//! a model changed or deleted afterwards predicts as in CASA and errors
//! surface at the call. The grids are admitted against the session's memory
//! budget and held until the step is replaced or the session closes; `close`
//! degrids them into the rows it writes.
//!
//! Divergences: CASA rewrites the model image's spectral units and frame in
//! place; casa-rs never writes it (`docs/CASA (C++) bugs.md`). MODEL_DATA
//! and CORRECTED_DATA are not written (plan decision 6), even for an
//! incremental predict, whose sum CASA forms through MODEL_DATA.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_imaging_model::CorrelationType;
use casa_imaging_operator::{
    ConvolutionFunctionSet, GridPrecision, MAX_W_PLANES, OperatorError, PaddingFactor,
    PreparedModelGrids, Spheroidal, WPlaneCount, WPlanes,
};
use casa_imaging_runtime::pass::WorkerTeam;
use casa_imaging_runtime::{Demand, Reservation, admit, charge_process_caches, free_memory};

use super::error::{SimulatorError, SimulatorResult};
use super::{BackendRequest, Simulator};
use crate::predict::{
    BeamPattern, FtMachine, ModelLayout, ModelOperator, PredictError, PrimaryBeam,
    SingleDishKernel, SkyModel,
};

/// The phase a model's grids are admitted as.
pub(crate) const MODEL_PHASE: &str = "the simulator's model grids";

/// The phase the workers preparing a model are admitted as.
const PREPARATION_PHASE: &str = "the simulator's model preparation";

/// The phase the W-projection screens are admitted as while the planes are
/// transformed on them.
const W_SCREENS_PHASE: &str = "the simulator's W-projection screens";

/// The phase a model's kernels are admitted as.
const KERNEL_PHASE: &str = "the simulator's convolution functions";

/// The FTMachines CASA's simulator builds other than `GridFT`,
/// `WProjectFT` and `SDGrid`.
const OTHER_FTMACHINES: [&str; 3] = ["both", "mosaic", "pbwproject"];

/// The arguments of `sm.predict`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PredictSpec {
    /// `imagename`: the model images; empty names are skipped, as CASA skips
    /// them.
    pub images: Vec<PathBuf>,
    /// `complist`: a component list, or `None` for `''`.
    pub component_list: Option<PathBuf>,
    /// `incremental`: add the prediction to DATA rather than replace it.
    pub incremental: bool,
}

impl PredictSpec {
    /// The model images named, without the empty names.
    fn named_images(&self) -> Vec<&Path> {
        self.images
            .iter()
            .map(PathBuf::as_path)
            .filter(|path| !path.as_os_str().is_empty())
            .collect()
    }

    /// The check casatools' binding makes before `Simulator::predict`, even
    /// before `open`: every named image exists and opens as an image
    /// (`ImageFactory::fromFile`). Here it must open as the simulator reads
    /// it, a CASA image of `Float` pixels: CASA's binding also accepts FITS
    /// and complex images, which its simulator then refuses with `False`
    /// (`docs/CASA (C++) bugs.md`).
    ///
    /// # Errors
    ///
    /// Raised: "File … does not exist." or "Unable to open image …".
    pub fn check_images(&self) -> SimulatorResult<()> {
        for path in self.named_images() {
            if !path.exists() {
                return Err(SimulatorError::raised(format!(
                    "File {} does not exist.",
                    path.display()
                )));
            }
            if SkyModel::open(path).is_err_and(|error| matches!(error, PredictError::Image { .. }))
            {
                return Err(SimulatorError::raised(format!(
                    "Unable to open image {}.",
                    path.display()
                )));
            }
        }
        Ok(())
    }
}

/// The arguments of `sm.setoptions` that casa-rs reads. `cache`, `tile`,
/// `location`, `facets` and `maxdata` do not change what `GridFT` and
/// `WProjectFT` predict; the session's memory budget stands for `cache`.
#[derive(Debug, Clone, PartialEq)]
pub struct PredictionOptions {
    /// `ftmachine`: `WProjectFT` for `wproject`, `SDGrid` for `sd`, and
    /// `GridFT` unless `both`, `mosaic` or `pbwproject` (case-insensitive).
    pub ftmachine: String,
    /// `gridfunction`: `SF`, the spheroid, for `GridFT` and `WProjectFT`;
    /// `PB`, `BOX` or `SF` for `SDGrid`.
    pub grid_function: String,
    /// `padding`, `GridFT`'s; factors below 1 are ignored, as CASA ignores
    /// them. `WProjectFT` pads by 1 whatever it is.
    pub padding: f64,
    /// `wprojplanes`, `WProjectFT`'s planes.
    pub w_planes: i64,
}

impl Default for PredictionOptions {
    /// CASA's binding defaults: `ft`, `SF`, 1.3 and one plane.
    fn default() -> Self {
        Self {
            ftmachine: "ft".to_string(),
            grid_function: "SF".to_string(),
            padding: 1.3,
            w_planes: 1,
        }
    }
}

/// The arguments of `sm.setvp` that casa-rs reads. `skyposthreshold` and
/// `pblimit` do not change a symmetric Airy beam's prediction through
/// `GridFT`.
#[derive(Debug, Clone, PartialEq)]
pub struct VoltagePatternSpec {
    /// `dovp`: multiply the model by the primary beam.
    pub dovp: bool,
    /// `usedefaultvp`: the default voltage patterns. Without them and
    /// without a table CASA uses its voltage-pattern manager's patterns,
    /// which are the defaults in a fresh session.
    pub use_default_vp: bool,
    /// `vptable`: a voltage-pattern table, read when not `usedefaultvp`.
    pub vp_table: String,
    /// `dosquint`: beam squint, which a symmetric Airy beam has none of.
    pub do_squint: bool,
    /// `parangleinc`, degrees (CASA's 360 for `[]`), logged only.
    pub parallactic_angle_increment_deg: f64,
}

impl Default for VoltagePatternSpec {
    /// CASA's binding defaults.
    fn default() -> Self {
        Self {
            dovp: true,
            use_default_vp: true,
            vp_table: String::new(),
            do_squint: true,
            parallactic_angle_increment_deg: 360.0,
        }
    }
}

/// A predict step's prepared model: the operator and grids `close` degrids
/// the covered scans' rows from.
pub(crate) struct Prediction {
    pub(crate) model: ModelOperator,
    /// The model's tangent point (pixel `(nx/2, ny/2)`), J2000 radians.
    pub(crate) tangent_rad: [f64; 2],
    /// The grids: one set for every field without a beam, else one set per
    /// field, the beam centred on it.
    grids: Vec<(Option<usize>, PreparedModelGrids)>,
    /// The grids' memory, admitted at `predict` and released with them.
    _reservation: Reservation,
    /// What the operator's kernels keep, admitted as they were built and
    /// released with them.
    _kernels: Reservation,
}

impl std::fmt::Debug for Prediction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Prediction")
            .field("tangent_rad", &self.tangent_rad)
            .field(
                "fields",
                &self
                    .grids
                    .iter()
                    .map(|(field, _)| *field)
                    .collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

impl Prediction {
    /// The grids rows of `field` are predicted from.
    pub(crate) fn grids_for(&self, field: usize) -> Option<&PreparedModelGrids> {
        self.grids
            .iter()
            .find(|(grid_field, _)| grid_field.is_none_or(|grid_field| grid_field == field))
            .map(|(_, grids)| grids)
    }

    /// Bytes every set of grids holds.
    pub(crate) fn grid_bytes(&self) -> u64 {
        self.grids.iter().map(|(_, grids)| grids.bytes()).sum()
    }
}

impl Simulator {
    /// `sm.setoptions(...)`: record the FTMachine `predict` degrids through,
    /// `GridFT`'s padding and `WProjectFT`'s planes. Each call sets the
    /// FTMachine anew, as CASA's does.
    ///
    /// # Errors
    ///
    /// Not yet implemented: an FTMachine other than `GridFT`, `WProjectFT`
    /// and `SDGrid` (`both`, `mosaic`, `pbwproject`), a grid function other
    /// than `SF` for `GridFT` and `WProjectFT`, or other than `PB`, `BOX`
    /// and `SF` for `SDGrid`. Nothing is recorded then.
    pub fn set_options(&mut self, options: &PredictionOptions) -> SimulatorResult<()> {
        let ftmachine = options.ftmachine.to_lowercase();
        if OTHER_FTMACHINES.contains(&ftmachine.as_str()) {
            let phase = if ftmachine == "both" || ftmachine == "pbwproject" {
                "#841"
            } else {
                "simulation Phase 2"
            };
            return Err(SimulatorError::not_yet_implemented(format!(
                "setoptions(ftmachine='{ftmachine}') is not yet implemented in casa-rs ({phase})"
            )));
        }
        let single_dish = if ftmachine == "sd" {
            Some(
                SingleDishKernel::from_grid_function(&options.grid_function).ok_or_else(|| {
                    SimulatorError::not_yet_implemented(format!(
                        "setoptions(ftmachine='sd', gridfunction='{}') is not yet implemented \
                         in casa-rs (#841); 'PB', 'BOX' and 'SF' are",
                        options.grid_function
                    ))
                })?,
            )
        } else {
            if options.grid_function.to_lowercase() != "sf" {
                return Err(SimulatorError::not_yet_implemented(format!(
                    "setoptions(gridfunction='{}') is not yet implemented in casa-rs (#841); \
                     'SF' is",
                    options.grid_function
                )));
            }
            None
        };
        tracing::info!("Setting processing options");
        // CASA keeps a padding of at least 1 and ignores the rest.
        let padding = options.padding as f32;
        if padding >= 1.0 {
            self.padding = PaddingFactor::new(padding)
                .map_err(|error| SimulatorError::raised(format!("padding {padding}: {error}")))?;
        }
        self.w_projection_planes = (ftmachine == "wproject").then_some(options.w_planes);
        self.single_dish = single_dish;
        Ok(())
    }

    /// The FTMachine `predict` degrids through, as `setoptions` chose it.
    ///
    /// # Errors
    ///
    /// - CASA's `False`: `WProjectFT` with one plane, whose convolution
    ///   function CASA's `WPConvFunc` cannot normalise.
    /// - Raised: `WProjectFT` with no planes or fewer, where CASA's
    ///   simulator crashes (`docs/CASA (C++) bugs.md`), or more than
    ///   casa-rs's [`MAX_W_PLANES`] (65,536).
    /// - Not yet implemented: `WProjectFT` on the Metal backend.
    fn ft_machine(&self) -> SimulatorResult<FtMachine> {
        let Some(planes) = self.w_projection_planes else {
            return Ok(FtMachine::Grid(self.padding));
        };
        match planes {
            ..=0 => Err(SimulatorError::raised(format!(
                "setoptions(ftmachine='wproject', wprojplanes={planes}): W projection needs at \
                 least two planes"
            ))),
            1 => Err(SimulatorError::refused(
                "Caught exception: Convolution function integral is not positive",
            )),
            _ => {
                let planes = u32::try_from(planes)
                    .ok()
                    .filter(|&planes| planes as usize <= MAX_W_PLANES)
                    .ok_or_else(|| {
                        SimulatorError::raised(format!(
                            "setoptions(ftmachine='wproject', wprojplanes={planes}): casa-rs \
                             builds at most 65,536 W planes"
                        ))
                    })?;
                if self.options.resources.backend() == BackendRequest::Metal {
                    return Err(SimulatorError::not_yet_implemented(
                        "predict with setoptions(ftmachine='wproject') on the Metal backend is \
                         not yet implemented in casa-rs (#841); backend 'auto' or 'cpu' \
                         predicts it on the CPU",
                    ));
                }
                Ok(FtMachine::WProject(planes))
            }
        }
    }

    /// `sm.setvp(...)`: record whether `predict` applies the primary beam.
    ///
    /// # Errors
    ///
    /// Not yet implemented: a voltage-pattern table (`vptable` without
    /// `usedefaultvp`). Nothing is recorded then.
    pub fn set_vp(&mut self, spec: &VoltagePatternSpec) -> SimulatorResult<()> {
        if !spec.use_default_vp && !spec.vp_table.is_empty() {
            return Err(SimulatorError::not_yet_implemented(format!(
                "setvp(vptable='{}') is not yet implemented in casa-rs (#841); the default \
                 voltage patterns are",
                spec.vp_table
            )));
        }
        tracing::info!("Setting voltage pattern parameters");
        if spec.use_default_vp {
            tracing::info!("Using system default voltage patterns for each telescope");
        }
        if spec.do_squint {
            tracing::info!("Beam Squint will be included in the VP model");
            tracing::info!(
                "and the parallactic angle increment is {} degrees",
                spec.parallactic_angle_increment_deg
            );
        }
        self.voltage_pattern = spec.dovp;
        Ok(())
    }

    /// `sm.predict(imagename, complist, incremental)`: predict the model
    /// into the rows observed so far, replacing their DATA or, when
    /// `incremental`, adding to it.
    ///
    /// # Errors
    ///
    /// - Raised, as casatools' binding raises before predicting: a named
    ///   image that does not exist or does not open
    ///   ([`PredictSpec::check_images`]).
    /// - Not yet implemented: a component list, more than one image, a
    ///   model layout CASA reads and casa-rs does not (other than Stokes I,
    ///   J2000, direction–Stokes–spectral axes), a near-field distance on the
    ///   last field, scans whose windows differ in correlations, or
    ///   `setvp(dovp=True)` for a telescope without an Airy default pattern
    ///   (VLA and EVLA among them) or for pointings `observemany` moved off
    ///   their fields.
    /// - CASA's `False`: no model image ("No model images found"), or no
    ///   rows observed yet.
    /// - Raised: a scan of 3 correlations ("No such VisVector type"), or
    ///   model grids or kernels that do not fit the session's memory budget
    ///   ([`SimulatorError::admission_refused`]).
    /// - As the FTMachine `setoptions` chose refuses: `WProjectFT` with
    ///   fewer than two planes, or on the Metal backend.
    pub fn predict(&mut self, spec: &PredictSpec) -> SimulatorResult<()> {
        spec.check_images()?;
        let images = spec.named_images();
        tracing::info!(
            "Predicting visibilities using model: {images:?} and componentList: {}",
            spec.component_list
                .as_deref()
                .map_or(String::new(), |path| path.display().to_string())
        );
        if spec.incremental {
            tracing::info!("The data column will be incremented");
        } else {
            tracing::info!("The data column will be replaced");
        }
        if let Some(list) = &spec.component_list {
            return Err(SimulatorError::not_yet_implemented(format!(
                "predict(complist='{}') is not yet implemented in casa-rs (simulation Phase 2)",
                list.display()
            )));
        }
        if images.len() > 1 {
            return Err(SimulatorError::not_yet_implemented(format!(
                "predict from {} model images is not yet implemented in casa-rs (simulation \
                 Phase 2); one image is",
                images.len()
            )));
        }
        let Some(path) = images.first() else {
            return Err(SimulatorError::refused(
                "Failed to create SkyEquation: No model images found",
            ));
        };
        if let Some(field) = self.fields.last()
            && field.distance_m != 0.0
        {
            return Err(SimulatorError::not_yet_implemented(format!(
                "predict refocused to the distance {} m of field {} is not yet implemented in \
                 casa-rs (#841)",
                field.distance_m, field.name
            )));
        }
        let model = SkyModel::open(path).map_err(model_error)?;
        if self.planned_rows() == 0 {
            return Err(SimulatorError::refused(
                "Failed to create SkyEquation: Caught exception: MSIter::construct - Input \
                 MeasurementSet.has zero selected rows",
            ));
        }
        let correlations = self.predicted_correlations()?;
        let beam = self.voltage_pattern_beam()?;
        if beam.is_some() {
            self.check_pointings_on_fields()?;
        }
        if let Some(kernel) = self.single_dish {
            return self.predict_single_dish(&model, kernel, beam, spec.incremental);
        }
        let layout = ModelLayout::new(&model, &correlations, self.ft_machine()?)
            .map_err(|error| correlation_error(error, &correlations))?;
        let (kernels, kernel_reservation) = self.admitted_kernels(&layout)?;
        let operator = ModelOperator::new(layout, kernels, GridPrecision::F32);
        let prediction = self.prepare(&model, operator, beam, kernel_reservation)?;
        if !spec.incremental {
            // Every step so far covers a prefix of the scans this one covers.
            self.data_plan.clear();
        }
        self.data_plan
            .predict(Arc::new(prediction), self.scans.len());
        Ok(())
    }

    /// The correlations of the scans observed so far, which one operator
    /// predicts.
    fn predicted_correlations(&self) -> SimulatorResult<Vec<CorrelationType>> {
        self.check_visibility_correlations("predicted")?;
        let window = |scan: &super::observe::Scan| &self.spectral_windows[scan.spectral_window_id];
        let first = &window(&self.scans[0]).correlations;
        if self
            .scans
            .iter()
            .any(|scan| window(scan).correlations != *first)
        {
            return Err(SimulatorError::not_yet_implemented(
                "predicting scans whose spectral windows have different correlations is not yet \
                 implemented in casa-rs (#841)",
            ));
        }
        first
            .iter()
            .map(|&code| {
                CorrelationType::from_casa_code(code).ok_or_else(|| {
                    SimulatorError::not_yet_implemented(format!(
                        "predicting correlation code {code} is not yet implemented in casa-rs \
                         (#841)"
                    ))
                })
            })
            .collect()
    }

    /// The primary beam `setvp` asks for: the default pattern of OBSERVATION
    /// row 0's telescope.
    fn voltage_pattern_beam(&self) -> SimulatorResult<Option<PrimaryBeam>> {
        if !self.voltage_pattern {
            return Ok(None);
        }
        let telescope = self.observing_telescope();
        let beam = PrimaryBeam::for_telescope(telescope).ok_or_else(|| {
            let phase = if telescope == "VLA" || telescope == "EVLA" {
                "simulation Phase 2"
            } else {
                "#841"
            };
            SimulatorError::not_yet_implemented(format!(
                "setvp(dovp=True) for telescope {telescope} is not yet implemented in casa-rs \
                 ({phase}); ALMA's and ACA's default patterns are"
            ))
        })?;
        Ok(Some(beam))
    }

    /// OBSERVATION row 0's telescope, whose patterns CASA's `VPSkyJones`
    /// takes (`msc.observation().telescopeName()(0)`).
    pub(super) fn observing_telescope(&self) -> &str {
        self.observations
            .first()
            .map_or(self.telescope.as_str(), |observation| {
                observation.telescope.as_str()
            })
    }

    /// That every scan points at its field, as a primary beam centred on
    /// each field needs. CASA centres the beam on each pointing's direction
    /// (from POINTING), which casa-rs does not yet do for the pointings
    /// `observemany` moves off their field.
    fn check_pointings_on_fields(&self) -> SimulatorResult<()> {
        let moved = self
            .scans
            .iter()
            .find(|scan| scan.phase_center_rad != self.fields[scan.field_id].direction_rad);
        match moved {
            Some(scan) => Err(SimulatorError::not_yet_implemented(format!(
                "predict with setvp(dovp=True) for scan {} pointed away from its field {} is \
                 not yet implemented in casa-rs (simulation Phase 2); pointings at their \
                 fields' centres are",
                scan.scan_number, self.fields[scan.field_id].name
            ))),
            None => Ok(()),
        }
    }

    /// The fields of the scans observed so far, each once, in the order
    /// first observed.
    pub(super) fn observed_fields(&self) -> Vec<usize> {
        let mut fields = Vec::new();
        for scan in &self.scans {
            if !fields.contains(&scan.field_id) {
                fields.push(scan.field_id);
            }
        }
        fields
    }

    /// Build `layout`'s kernels, admitted as they are built, as imaging
    /// admits them: the spheroidal set's bytes, or the W-projection screens
    /// ([`WPlanes::screens_bytes`]) and then the kernels cropped from them
    /// ([`WScreens::kernel_bytes`](casa_imaging_operator::WScreens::kernel_bytes)),
    /// the screens released once the kernels are built. The reservation
    /// returned keeps what the kernels hold.
    fn admitted_kernels(
        &self,
        layout: &ModelLayout,
    ) -> SimulatorResult<(Box<dyn ConvolutionFunctionSet>, Reservation)> {
        let resources = &self.options.resources;
        let (host, policy) = (resources.host(), resources.policy());
        charge_process_caches(host, policy).map_err(SimulatorError::admission)?;
        let admitted = |phase, memory| {
            admit(host, policy, &Demand { phase, memory }).map_err(SimulatorError::admission)
        };
        let (geometry, polarization) = (layout.geometry(), layout.polarization());
        let raised =
            |error: OperatorError| SimulatorError::raised(format!("Caught exception: {error}"));
        Ok(match layout.machine() {
            FtMachine::Grid(_) => (
                Box::new(Spheroidal::new(geometry, polarization)),
                admitted(KERNEL_PHASE, Spheroidal::bytes(geometry))?,
            ),
            FtMachine::WProject(planes) => {
                let count = WPlaneCount::Fixed(planes);
                let screening = admitted(
                    W_SCREENS_PHASE,
                    WPlanes::screens_bytes(geometry, count).map_err(raised)?,
                )?;
                let screens = WPlanes::screens(geometry, polarization, count).map_err(raised)?;
                let mut kernels = admitted(KERNEL_PHASE, screens.kernel_bytes())?;
                let set = screens.finish().map_err(raised)?;
                drop(screening);
                kernels.retain(set.resident_bytes());
                tracing::info!(
                    planes,
                    screen = set.screen_size(),
                    bytes = set.resident_bytes(),
                    "W projection using {planes} planes"
                );
                (Box::new(set), kernels)
            }
        })
    }

    /// Admit and prepare `model`'s grids: one set, or one per observed field
    /// with `beam` centred on it. The planes are prepared on as many of the
    /// policy's workers as fit their scratch beside the grids. The
    /// prediction keeps `kernels`, the reservation of the operator's
    /// kernels.
    fn prepare(
        &self,
        model: &SkyModel,
        operator: ModelOperator,
        beam: Option<PrimaryBeam>,
        kernels: Reservation,
    ) -> SimulatorResult<Prediction> {
        let resources = &self.options.resources;
        let (host, policy) = (resources.host(), resources.policy());
        charge_process_caches(host, policy).map_err(SimulatorError::admission)?;
        let fields: Vec<Option<usize>> = match beam {
            Some(_) => self.observed_fields().into_iter().map(Some).collect(),
            None => vec![None],
        };
        let pattern_bytes = if beam.is_some() {
            BeamPattern::BYTES
        } else {
            0
        };
        let reservation = admit(
            host,
            policy,
            &Demand {
                phase: MODEL_PHASE,
                memory: operator.grid_bytes() * fields.len() as u64 + pattern_bytes,
            },
        )
        .map_err(SimulatorError::admission)?;
        let scratch = operator.plane_scratch_bytes(model);
        let free = free_memory(host, policy);
        let workers = (1..=resources.cores().workers.max(1))
            .rev()
            .find(|workers| *workers as u64 * scratch <= free)
            .unwrap_or(1);
        let _preparation = admit(
            host,
            policy,
            &Demand {
                phase: PREPARATION_PHASE,
                memory: workers as u64 * scratch,
            },
        )
        .map_err(SimulatorError::admission)?;
        let team = WorkerTeam::new(workers)
            .map_err(|error| SimulatorError::raised(format!("the worker team: {error}")))?;
        let pattern = beam.map(PrimaryBeam::pattern);
        let mut grids = Vec::with_capacity(fields.len());
        for field in fields {
            let centre = match (&pattern, field) {
                (Some(pattern), Some(field)) => {
                    let pointing = self.fields[field].direction_rad;
                    Some((pattern, model.pixel_of(pointing).map_err(model_error)?))
                }
                _ => None,
            };
            let prepared = operator
                .prepare(model, centre, Some(&team))
                .map_err(model_error)?;
            grids.push((field, prepared));
        }
        tracing::info!(
            planes = model.planes(),
            grids = grids.len(),
            workers,
            bytes = operator.grid_bytes() * grids.len() as u64,
            "prepared the model {}",
            model.path().display()
        );
        Ok(Prediction {
            model: operator,
            tangent_rad: model.centre_rad(),
            grids,
            _reservation: reservation,
            _kernels: kernels,
        })
    }
}

/// A model that cannot be read or prepared, as CASA reports it: a layout
/// CASA reads and casa-rs does not is not yet implemented; the rest is
/// raised.
fn model_error(error: PredictError) -> SimulatorError {
    match error {
        PredictError::UnsupportedModel(reason) => SimulatorError::not_yet_implemented(format!(
            "predict from this model is not yet implemented in casa-rs (#841): {reason}"
        )),
        PredictError::Image { path, .. } => {
            SimulatorError::raised(format!("Unable to open image {path}."))
        }
        other => SimulatorError::raised(format!("Caught exception: {other}")),
    }
}

/// An operator refusal for `correlations`: a correlation set the Stokes I
/// model cannot feed is not yet implemented; the rest is raised.
fn correlation_error(error: PredictError, correlations: &[CorrelationType]) -> SimulatorError {
    match error {
        PredictError::Operator(OperatorError::Polarization { reason }) => {
            SimulatorError::not_yet_implemented(format!(
                "predicting a Stokes I model into the correlations {correlations:?} is not yet \
                 implemented in casa-rs (#841): {reason}"
            ))
        }
        other => model_error(other),
    }
}
