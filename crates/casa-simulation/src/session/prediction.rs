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
//! The model is read when `predict` is called, so a model changed or
//! deleted afterwards predicts as in CASA and errors surface at the call:
//! - Without the beam, or under it with one field observed, it is prepared
//!   then (multiplied by the beam and transformed into one set of grids).
//!   The grids are admitted against the session's memory budget and held
//!   until the step is replaced or the session closes.
//! - Under the beam with several fields, each field's grids have the beam
//!   centred on that field, and one set per field may not fit the budget
//!   (a 106-pointing mosaic of a 1873² model needed 5.3 GB). The step holds
//!   the model's planes instead, and `close` prepares each field's grids as
//!   its write reaches the field, keeping as many as its budget allows and
//!   preparing a field again when it returns, as CASA's `GridFT` transforms
//!   the model again whenever the field changes.
//!
//! `close` degrids the grids into the rows it writes.
//!
//! Divergences: CASA rewrites the model image's spectral units and frame in
//! place; casa-rs never writes it (`docs/CASA (C++) bugs.md`). MODEL_DATA
//! and CORRECTED_DATA are not created (plan decision 6), even for an
//! incremental predict, whose sum CASA forms through MODEL_DATA; a
//! MeasurementSet `openfromms` opened keeps those it has in step with DATA.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_components::ComponentList;
use casa_imaging_operator::{
    ConvolutionFunctionSet, GridPrecision, MAX_W_PLANES, MosaicPb, MosaicRules, OperatorError,
    PaddingFactor, PreparedModelGrids, Spheroidal, WPlaneCount, WPlanes,
};
use casa_imaging_runtime::pass::WorkerTeam;
use casa_imaging_runtime::{Demand, Reservation, admit, charge_process_caches, free_memory};
use casa_types::Correlation;

use super::error::{SimulatorError, SimulatorResult};
use super::resources::SessionResources;
use super::{BackendRequest, Simulator};
use crate::predict::{
    BeamPattern, ComponentPrediction, ComponentRefusal, ComponentTargets, ComponentWindow,
    FtMachine, ModelLayout, ModelOperator, PredictError, PrimaryBeam, SingleDishKernel, SkyModel,
};

/// The phase a model's grids are admitted as.
pub(crate) const MODEL_PHASE: &str = "the simulator's model grids";

/// The phase a component list's prepared fluxes and beams are admitted as.
const COMPONENTS_PHASE: &str = "the simulator's component list";

/// The phase the workers preparing a model are admitted as.
const PREPARATION_PHASE: &str = "the simulator's model preparation";

/// The phase the W-projection screens are admitted as while the planes are
/// transformed on them.
const W_SCREENS_PHASE: &str = "the simulator's W-projection screens";

/// The phase a model's kernels are admitted as.
const KERNEL_PHASE: &str = "the simulator's convolution functions";

/// The phase the mosaic screens are admitted as while the beams are
/// transformed on them.
const MOSAIC_SCREENS_PHASE: &str = "the simulator's mosaic screens";

/// The phase the mosaic machine's pointings, dish classes and beams'
/// inputs are admitted as.
const MOSAIC_STATE_PHASE: &str = "the simulator's mosaic pointings and dish classes";

/// The FTMachines CASA's simulator builds other than `GridFT`,
/// `WProjectFT`, `MosaicFTNew` and `SDGrid`.
const OTHER_FTMACHINES: [&str; 2] = ["both", "pbwproject"];

/// The arguments of `sm.predict`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PredictSpec {
    /// `imagename`: the model images; empty names are skipped, as CASA skips
    /// them.
    pub images: Vec<PathBuf>,
    /// `complist`: the component lists, predicted together; empty names are
    /// skipped. CASA's binding takes one list, `''` for none. `simobserve`
    /// predicts its model's list and its calibrator's at once, which CASA
    /// cannot (`docs/CASA (C++) bugs.md`).
    pub component_lists: Vec<PathBuf>,
    /// `incremental`: add the prediction to DATA rather than replace it.
    pub incremental: bool,
}

impl PredictSpec {
    /// The model images named, without the empty names.
    fn named_images(&self) -> Vec<&Path> {
        named(&self.images)
    }

    /// The component lists named, without the empty names.
    fn named_lists(&self) -> Vec<&Path> {
        named(&self.component_lists)
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
/// `pblimit` do not change a radially symmetric beam's prediction
/// (`PBMath1D::apply` uses the cutoff only to divide by the beam).
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
    /// `dosquint`: beam squint, which CASA's VLA and EVLA patterns have but
    /// which does not reach a Stokes I model, the only one predicted
    /// ([`PrimaryBeam`]).
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
    /// The grids, or what `close` prepares each field's from.
    grids: StepGrids,
    /// The mosaic machine's pointing offsets, from the model's pixel
    /// `(nx/2, ny/2)`, by phase centre (its bits), sorted by the bits; empty
    /// for the others.
    pointings: Vec<([u64; 2], [f64; 2])>,
    /// The mosaic machine's dish class of each antenna; empty for the
    /// others.
    antenna_types: Vec<u8>,
    /// The memory of the grids, or of the planes held, admitted at
    /// `predict` and released with them.
    _reservation: Reservation,
    /// What the operator's kernels keep, and the mosaic machine's pointings
    /// and dish classes, admitted as they were built and released with
    /// them.
    _kernels: Reservation,
}

/// The grids a step predicts from.
enum StepGrids {
    /// Prepared at `predict`: one set for every field without a beam
    /// (`None`), or for the one field observed under it.
    Prepared(Option<usize>, Box<PreparedModelGrids>),
    /// One set per observed field under the beam, centred on the field,
    /// which `close` prepares ([`Prediction::prepare_field`]).
    PerField(Box<FieldModel>),
}

/// What `close` prepares a field's grids from: the model with its planes
/// held, the beam's power pattern, and each observed field's beam centre
/// on the model's pixels, by field.
struct FieldModel {
    model: SkyModel,
    pattern: BeamPattern,
    centres: Vec<(usize, [f64; 2])>,
}

/// Where the grids of a field come from: a set prepared at `predict`, or
/// one `close` prepares ([`Prediction::prepare_field`]).
pub(crate) enum FieldGrids<'p> {
    Prepared(&'p PreparedModelGrids),
    PerField,
}

impl std::fmt::Debug for Prediction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fields: Vec<Option<usize>> = match &self.grids {
            StepGrids::Prepared(field, _) => vec![*field],
            StepGrids::PerField(held) => {
                held.centres.iter().map(|(field, _)| Some(*field)).collect()
            }
        };
        formatter
            .debug_struct("Prediction")
            .field("tangent_rad", &self.tangent_rad)
            .field("fields", &fields)
            .field("per_field", &matches!(self.grids, StepGrids::PerField(_)))
            .finish_non_exhaustive()
    }
}

impl Prediction {
    /// Where the grids rows of `field` are predicted from come from; `None`
    /// for a field the step did not observe.
    pub(crate) fn grids_for(&self, field: usize) -> Option<FieldGrids<'_>> {
        match &self.grids {
            StepGrids::Prepared(grid_field, grids) => grid_field
                .is_none_or(|grid_field| grid_field == field)
                .then_some(FieldGrids::Prepared(grids)),
            StepGrids::PerField(held) => held
                .centres
                .iter()
                .any(|(observed, _)| *observed == field)
                .then_some(FieldGrids::PerField),
        }
    }

    /// Bytes the grids prepared at `predict` hold: one set, or none when
    /// `close` prepares each field's.
    pub(crate) fn prepared_bytes(&self) -> u64 {
        match &self.grids {
            StepGrids::Prepared(_, grids) => grids.bytes(),
            StepGrids::PerField(_) => 0,
        }
    }

    /// Whether `close` prepares each field's grids
    /// ([`Self::prepare_field`]).
    pub(crate) fn per_field(&self) -> bool {
        matches!(self.grids, StepGrids::PerField(_))
    }

    /// Bytes one set of grids holds ([`PreparedModelGrids::bytes`]).
    pub(crate) fn set_bytes(&self) -> u64 {
        self.model.grid_bytes()
    }

    /// Bytes preparing a field's grids holds beside them on `workers`
    /// workers: a plane's scratch for each worker that has a plane
    /// ([`ModelOperator::plane_scratch_bytes`]); none when `close` prepares
    /// no grids.
    pub(crate) fn preparation_bytes(&self, workers: usize) -> u64 {
        match &self.grids {
            StepGrids::Prepared(..) => 0,
            StepGrids::PerField(held) => {
                workers.clamp(1, held.model.planes().max(1)) as u64
                    * self.model.plane_scratch_bytes(&held.model)
            }
        }
    }

    /// Prepare the grids of `field`, which the step observed under the
    /// beam, on `team`: the planes held, multiplied by the beam's power
    /// centred on the field and transformed, as `predict` prepares one
    /// field's ([`ModelOperator::prepare`]); the same grids whatever the
    /// workers.
    ///
    /// # Errors
    ///
    /// A raised error for a field the step did not observe or a step whose
    /// grids were prepared at `predict`, and the operator's refusal, which
    /// for a model `predict` read is only a transform's failure.
    pub(crate) fn prepare_field(
        &self,
        field: usize,
        team: &WorkerTeam,
    ) -> SimulatorResult<PreparedModelGrids> {
        let StepGrids::PerField(held) = &self.grids else {
            return Err(SimulatorError::raised(
                "the step's grids were prepared at predict",
            ));
        };
        let centre = held
            .centres
            .iter()
            .find(|(observed, _)| *observed == field)
            .map(|(_, centre)| *centre)
            .ok_or_else(|| {
                SimulatorError::raised(format!("predict did not observe field {field}"))
            })?;
        self.model
            .prepare(&held.model, Some((&held.pattern, centre)), Some(team))
            .map_err(model_error)
    }

    /// The offset of rows pointed at `centre_rad`, which the mosaic
    /// machine's kernels ramp by; zero for the other machines.
    pub(crate) fn pointing_offset_rad(&self, centre_rad: [f64; 2]) -> [f64; 2] {
        let key = centre_rad.map(f64::to_bits);
        let found = self
            .pointings
            .binary_search_by_key(&key, |(known, _)| *known)
            .ok()
            .map(|index| &self.pointings[index]);
        // A mosaic step measured every scan it covers.
        debug_assert!(
            found.is_some() || self.pointings.is_empty(),
            "a mosaic step predicts rows pointed at {centre_rad:?}, which it did not measure"
        );
        found.map_or([0.0; 2], |(_, offset)| *offset)
    }

    /// Each antenna's dish class, which keys the mosaic machine's kernels;
    /// empty for the other machines.
    pub(crate) fn antenna_types(&self) -> &[u8] {
        &self.antenna_types
    }
}

impl Simulator {
    /// `sm.setoptions(...)`: record the FTMachine `predict` degrids through,
    /// `GridFT`'s padding and `WProjectFT`'s planes. Each call sets the
    /// FTMachine anew, as CASA's does.
    ///
    /// # Errors
    ///
    /// Not yet implemented: an FTMachine other than `GridFT`, `WProjectFT`,
    /// `MosaicFTNew` and `SDGrid` (`both`, `pbwproject`), a grid function
    /// other than `SF` for the interferometric machines, or other than `PB`,
    /// `BOX` and `SF` for `SDGrid`. Nothing is recorded then.
    pub fn set_options(&mut self, options: &PredictionOptions) -> SimulatorResult<()> {
        let ftmachine = options.ftmachine.to_lowercase();
        if OTHER_FTMACHINES.contains(&ftmachine.as_str()) {
            return Err(SimulatorError::not_yet_implemented(format!(
                "setoptions(ftmachine='{ftmachine}') is not yet implemented in casa-rs (#841)"
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
        self.mosaic = ftmachine == "mosaic";
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
        if self.mosaic {
            return Ok(FtMachine::Mosaic);
        }
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
        self.squint = spec.do_squint;
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
    /// - Not yet implemented: more than one image, a model layout CASA reads
    ///   and casa-rs does not (other than Stokes I, J2000,
    ///   direction–Stokes–spectral axes), a component casa-rs does not
    ///   predict (a direction not in J2000, or a flux, spectrum or shape
    ///   casa-components does not evaluate), a near-field distance on the
    ///   last field, scans whose windows differ in correlations, or
    ///   `setvp(dovp=True)` for a telescope whose default pattern casa-rs
    ///   does not have (ALMA's, ACA's, the VLA's and the EVLA's it has) or
    ///   for pointings `observemany` moved off their fields.
    /// - CASA's `False`: no model image ("No model images found"), or no
    ///   rows observed yet.
    /// - Raised: a scan of 3 correlations ("No such VisVector type"), or
    ///   model grids or kernels, or a component list's read or preparation,
    ///   that do not fit the session's memory budget
    ///   ([`SimulatorError::admission_refused`]).
    /// - As the FTMachine `setoptions` chose refuses: `WProjectFT` with
    ///   fewer than two planes, or on the Metal backend.
    pub fn predict(&mut self, spec: &PredictSpec) -> SimulatorResult<()> {
        spec.check_images()?;
        let images = spec.named_images();
        let lists = spec.named_lists();
        tracing::info!(
            "Predicting visibilities using model: {images:?} and componentList: {}",
            list_label(&lists)
        );
        if spec.incremental {
            tracing::info!("The data column will be incremented");
        } else {
            tracing::info!("The data column will be replaced");
        }
        if images.len() > 1 {
            return Err(SimulatorError::not_yet_implemented(format!(
                "predict from {} model images is not yet implemented in casa-rs (simulation \
                 Phase 2); one image is",
                images.len()
            )));
        }
        // The plan corrupts by gains and leakage after every prediction
        // (`session::plan`).
        if spec.incremental && self.data_plan.corrupts_by_jones() {
            return Err(SimulatorError::not_yet_implemented(
                "an incremental predict after a corrupt with setgain or setleakage terms is not \
                 yet implemented in casa-rs (simulation Phase 3)",
            ));
        }
        if images.is_empty() && lists.is_empty() {
            return Err(SimulatorError::refused(
                "Failed to create SkyEquation: No model images found",
            ));
        }
        if let Some(field) = self.fields.last()
            && field.distance_m != 0.0
        {
            return Err(SimulatorError::not_yet_implemented(format!(
                "predict refocused to the distance {} m of field {} is not yet implemented in \
                 casa-rs (#841)",
                field.distance_m, field.name
            )));
        }
        // `createSkyEquation` reads the component list before the images.
        let list = read_component_lists(&lists, &self.options.resources)?;
        let model = images
            .first()
            .map(|path| SkyModel::open(path).map_err(model_error))
            .transpose()?;
        if self.planned_rows() == 0 {
            return Err(SimulatorError::refused(
                "Failed to create SkyEquation: Caught exception: MSIter::construct - Input \
                 MeasurementSet.has zero selected rows",
            ));
        }
        let correlations = self.predicted_correlations()?;
        let beam = self.voltage_pattern_beam()?;
        // Each of these points a scan at its phase centre, which on an
        // opened MeasurementSet POINTING must agree with.
        if self.mosaic && model.is_some() {
            self.require_pointings_at_phase_centres("setoptions(ftmachine='mosaic')")?;
        }
        if beam.is_some() {
            self.require_pointings_at_phase_centres("setvp(dovp=True)")?;
        }
        if self.single_dish.is_some() {
            self.require_pointings_at_phase_centres("setoptions(ftmachine='sd')")?;
        }
        // A primary beam, over a model image, a component list or total
        // power, and the mosaic machine's kernels are centred on each
        // scan's field here, which a pointing `observemany` moved does not
        // point at (#884).
        if model.is_some() && self.mosaic {
            self.check_pointings_on_fields("setoptions(ftmachine='mosaic')")?;
        }
        if beam.is_some() {
            self.check_pointings_on_fields("setvp(dovp=True)")?;
        }
        if let Some(kernel) = self.single_dish {
            // Total power samples a model image; `SDGrid` with a component
            // list is not yet here.
            let Some(model) = model.filter(|_| list.is_none()) else {
                return Err(SimulatorError::not_yet_implemented(
                    "predict(complist=…) after setoptions(ftmachine='sd') is not yet \
                     implemented in casa-rs (#841); a model image is",
                ));
            };
            return self.predict_single_dish(model, kernel, beam, spec.incremental);
        }
        let components = list
            .map(|(label, list, held)| self.prepare_components(&label, (list, held), beam))
            .transpose()?;
        let prediction = model
            .map(|model| self.prepare_image(model, &correlations, beam))
            .transpose()?;
        if !spec.incremental {
            // Every step so far covers a prefix of the scans this one covers.
            self.data_plan.clear();
        }
        let scans = self.scans.len();
        if let Some(components) = components {
            self.data_plan
                .predict_components(Arc::new(components), scans);
        }
        if let Some(prediction) = prediction {
            self.data_plan.predict(Arc::new(prediction), scans);
        }
        Ok(())
    }

    /// `model` admitted and prepared for rows with `correlations` through
    /// the FTMachine `setoptions` chose: its kernels, then its grids, with
    /// `beam` in the image plane except through the mosaic machine, whose
    /// kernels carry the beams and which keeps each scan's pointing and
    /// each antenna's dish class. The model, with its image's tile cache,
    /// is dropped once it is read ([`Self::prepare`]).
    fn prepare_image(
        &self,
        model: SkyModel,
        correlations: &[Correlation],
        beam: Option<PrimaryBeam>,
    ) -> SimulatorResult<Prediction> {
        let machine = self.ft_machine()?;
        let mut mosaic_state = None;
        let (beams, antenna_types, pointings) = match machine {
            FtMachine::Mosaic => {
                // What the machine keeps per pointing and antenna, and its
                // beams' inputs until the kernels are built, admitted before
                // they are formed.
                let (kept, building) = self.mosaic_state_bytes();
                let resources = &self.options.resources;
                let reservation = admit(
                    resources.host(),
                    resources.policy(),
                    &Demand {
                        phase: MOSAIC_STATE_PHASE,
                        memory: kept + building,
                    },
                )
                .map_err(SimulatorError::admission)?;
                mosaic_state = Some((reservation, kept));
                let (beams, antenna_types) = self.mosaic_beams()?;
                (Some(beams), antenna_types, self.mosaic_pointings(&model)?)
            }
            FtMachine::Grid(_) | FtMachine::WProject(_) => {
                if self
                    .antennas
                    .windows(2)
                    .any(|pair| pair[0].diameter_m != pair[1].diameter_m)
                {
                    tracing::warn!(
                        "Heterogeneous array is only supported for ALMA,ACA,OVRO telescopes, \
                         and only with option ftmachine=mosaic."
                    );
                }
                (None, Vec::new(), Vec::new())
            }
        };
        let layout = ModelLayout::new(&model, correlations, machine, beams)
            .map_err(|error| correlation_error(error, correlations))?;
        let (kernels, mut kernel_reservation) = self.admitted_kernels(&layout)?;
        // The operator drops the layout's beams.
        let operator = ModelOperator::new(layout, kernels, GridPrecision::F32);
        if let Some((mut reservation, kept)) = mosaic_state {
            debug_assert_eq!(
                (pointings.capacity() * size_of::<([u64; 2], [f64; 2])>()
                    + antenna_types.capacity()) as u64,
                kept,
                "the mosaic state keeps what was admitted for it"
            );
            reservation.retain(kept);
            kernel_reservation.join(reservation);
        }
        let image_beam = beam.filter(|_| machine != FtMachine::Mosaic);
        let mut prediction = self.prepare(model, operator, image_beam, kernel_reservation)?;
        prediction.pointings = pointings;
        prediction.antenna_types = antenna_types;
        Ok(prediction)
    }

    /// `list` prepared for the session's windows, with `beam` per observed
    /// field, and admitted. The admission is made when the list has been
    /// read: what the prediction will hold, known from the list's length,
    /// and its preparation's scratch. The list's own share of the read's
    /// admission, `held_list`, is released when the list is dropped here.
    fn prepare_components(
        &self,
        label: &str,
        (list, held_list): (ComponentList, Reservation),
        beam: Option<PrimaryBeam>,
    ) -> SimulatorResult<ComponentPrediction> {
        // CASA squints the VLA's and EVLA's beams for a component list (RR
        // and LL beams apart); casa-rs's patterns are unsquinted.
        if self.squint
            && let Some(beam) = beam.filter(|beam| beam.squints())
        {
            return Err(SimulatorError::not_yet_implemented(format!(
                "predict(complist='{label}') under the {beam:?} primary beam with beam squint \
                 is not yet implemented in casa-rs (#841): CASA squints the RR and LL beams \
                 for components; setvp(dosquint=False) predicts them unsquinted"
            )));
        }
        if self
            .antennas
            .windows(2)
            .any(|pair| pair[0].diameter_m != pair[1].diameter_m)
        {
            tracing::warn!(
                "Heterogeneous array is not supported for simulation from components.  \
                 Primary beam will be applied by telescope name."
            );
        }
        let windows: Vec<ComponentWindow> = self
            .spectral_windows
            .iter()
            .map(|window| ComponentWindow {
                first_hz: window.start_frequency_hz,
                step_hz: window.channel_width_hz,
                channels: window.channel_count,
                correlations: window.correlations.clone(),
            })
            .collect();
        let pattern = beam.map(PrimaryBeam::pattern);
        // A scan is phased on its field's phase centre, as CASA's
        // `VisBuffer` phases it, and the beam centred there: a beam on a
        // pointing `observemany` moved was refused above (#884). Scans at
        // one centre share a pointing.
        let mut pointings: Vec<(usize, [f64; 2])> = Vec::new();
        let pointing_of_scan: Vec<usize> = self
            .scans
            .iter()
            .map(|scan| {
                let centre = scan.phase_center_rad.map(f64::to_bits);
                pointings
                    .iter()
                    .position(|(_, known)| known.map(f64::to_bits) == centre)
                    .unwrap_or_else(|| {
                        pointings.push((pointings.len(), scan.phase_center_rad));
                        pointings.len() - 1
                    })
            })
            .collect();
        let targets = ComponentTargets {
            windows: &windows,
            pointings: &pointings,
            pointing_of_scan: &pointing_of_scan,
            beam: pattern.as_ref(),
        };
        let resources = &self.options.resources;
        let preparation = ComponentTargets::preparation_bytes(list.len());
        let mut reservation = admit(
            resources.host(),
            resources.policy(),
            &Demand {
                phase: COMPONENTS_PHASE,
                memory: targets.bytes(list.len()) + preparation,
            },
        )
        .map_err(SimulatorError::admission)?;
        let held_preparation = reservation.split(preparation);
        let prediction = ComponentPrediction::new(&list, targets, reservation).map_err(
            |ComponentRefusal::NotYetImplemented(reason)| {
                SimulatorError::not_yet_implemented(format!(
                    "predict(complist='{label}'): {reason} (#841)"
                ))
            },
        )?;
        drop(held_preparation);
        tracing::info!(
            components = list.len(),
            bytes = prediction.bytes(),
            "prepared the component list {label}"
        );
        drop(list);
        drop(held_list);
        Ok(prediction)
    }

    /// The correlations of the scans observed so far, which one operator
    /// predicts.
    fn predicted_correlations(&self) -> SimulatorResult<Vec<Correlation>> {
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
                Correlation::from_casacore_code(code).ok_or_else(|| {
                    SimulatorError::not_yet_implemented(format!(
                        "predicting correlation code {code} is not yet implemented in casa-rs \
                         (#841)"
                    ))
                })
            })
            .collect()
    }

    /// The primary beam `setvp` asks for: the default pattern of OBSERVATION
    /// row 0's telescope at spectral window 0's REF_FREQUENCY
    /// (`VPSkyJones`).
    fn voltage_pattern_beam(&self) -> SimulatorResult<Option<PrimaryBeam>> {
        if !self.voltage_pattern {
            return Ok(None);
        }
        let telescope = self.observing_telescope();
        let beam = PrimaryBeam::for_telescope(telescope, self.reference_frequency_hz())
            .ok_or_else(|| {
                SimulatorError::not_yet_implemented(format!(
                    "setvp(dovp=True) for telescope {telescope} is not yet implemented in \
                     casa-rs (#841); the default patterns of ALMA, ACA, VLA and EVLA are"
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

    /// Spectral window 0's REF_FREQUENCY, which picks the VLA's pattern by
    /// band (`VPSkyJones`), Hz.
    pub(super) fn reference_frequency_hz(&self) -> f64 {
        self.spectral_windows
            .first()
            .map_or(0.0, |window| window.reference_frequency_hz)
    }

    /// That every scan points at its field, as `what` needs here: a primary
    /// beam, or the mosaic machine's kernels, centred on each scan's field.
    /// CASA's beam on a pointing `observemany` moved off its field is
    /// centred where the first row of the field it visits points, which
    /// `BeamSkyJones` keeps for the field's other rows (#884, `docs/CASA
    /// (C++) bugs.md`); casa-rs does not follow it yet.
    fn check_pointings_on_fields(&self, what: &str) -> SimulatorResult<()> {
        let moved = self
            .scans
            .iter()
            .find(|scan| scan.pointing_rad != scan.phase_center_rad);
        match moved {
            Some(scan) => Err(SimulatorError::not_yet_implemented(format!(
                "predict with {what} for scan {} pointed away from its field {} is not yet \
                 implemented in casa-rs (simulation Phase 2, #884); pointings at their \
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
            FtMachine::Mosaic => {
                let beams = layout.beams().expect("a mosaic layout has its beams");
                let (dishes, windows) = (&beams.dishes, &beams.windows);
                let (rules, frequency_hz) = (MosaicRules::Simulator, layout.image_frequency_hz());
                tracing::info!("Performing Mosaic gridding for model images (uv convolution)");
                let screening = admitted(
                    MOSAIC_SCREENS_PHASE,
                    MosaicPb::screens_bytes(rules, geometry, frequency_hz, dishes, windows)
                        .map_err(raised)?,
                )?;
                let screens =
                    MosaicPb::screens(rules, geometry, polarization, frequency_hz, dishes, windows)
                        .map_err(raised)?;
                let mut kernels = admitted(KERNEL_PHASE, screens.kernel_bytes())?;
                let set = screens.finish().map_err(raised)?;
                drop(screening);
                kernels.retain(set.resident_bytes());
                tracing::info!(
                    dishes = set.dishes(),
                    screen = set.screen_size(),
                    bytes = set.resident_bytes(),
                    "convolution function support: {}",
                    set.max_half_support()[0]
                );
                (Box::new(set), kernels)
            }
        })
    }

    /// Admit and prepare `model`'s grids: one set, without `beam` or with
    /// one field observed (the beam centred on it). With several fields
    /// under the beam, the model's planes are held for `close` to prepare
    /// each field's grids ([`Self::hold_for_fields`]). The model, and with
    /// it its image's tile cache, which the preparation's or the read's
    /// admission covers, is dropped before that admission is released. The
    /// prediction keeps `kernels`, the reservation of the operator's
    /// kernels.
    fn prepare(
        &self,
        model: SkyModel,
        operator: ModelOperator,
        beam: Option<PrimaryBeam>,
        kernels: Reservation,
    ) -> SimulatorResult<Prediction> {
        let resources = &self.options.resources;
        let (host, policy) = (resources.host(), resources.policy());
        charge_process_caches(host, policy).map_err(SimulatorError::admission)?;
        let fields = self.observed_fields();
        let pattern = beam.map(PrimaryBeam::pattern);
        let tangent_rad = model.centre_rad();
        let (grids, reservation) = match pattern {
            Some(pattern) if fields.len() > 1 => {
                self.hold_for_fields(model, &operator, pattern, &fields)?
            }
            pattern => {
                let field = pattern.as_ref().map(|_| fields[0]);
                let reservation = admit(
                    host,
                    policy,
                    &Demand {
                        phase: MODEL_PHASE,
                        memory: operator.grid_bytes()
                            + pattern.as_ref().map_or(0, |_| BeamPattern::BYTES),
                    },
                )
                .map_err(SimulatorError::admission)?;
                let scratch = operator.plane_scratch_bytes(&model);
                let free = free_memory(host, policy);
                let workers = (1..=resources.cores().workers.max(1))
                    .rev()
                    .find(|workers| *workers as u64 * scratch <= free)
                    .unwrap_or(1);
                let preparation = admit(
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
                let centre = match (&pattern, field) {
                    (Some(pattern), Some(field)) => {
                        let pointing = self.fields[field].direction_rad;
                        Some((pattern, model.pixel_of(pointing).map_err(model_error)?))
                    }
                    _ => None,
                };
                let prepared = operator
                    .prepare(&model, centre, Some(&team))
                    .map_err(model_error)?;
                tracing::info!(
                    planes = model.planes(),
                    grids = 1,
                    workers,
                    bytes = operator.grid_bytes(),
                    "prepared the model {}",
                    model.path().display()
                );
                drop(model);
                drop(preparation);
                (StepGrids::Prepared(field, Box::new(prepared)), reservation)
            }
        };
        Ok(Prediction {
            model: operator,
            tangent_rad,
            grids,
            pointings: Vec::new(),
            antenna_types: Vec::new(),
            _reservation: reservation,
            _kernels: kernels,
        })
    }

    /// `model`'s planes read and held for `close` to prepare the grids of
    /// each of `fields` with `pattern` centred on it, each field's centre
    /// placed on the model's pixels: the planes and the pattern admitted
    /// and kept, the read's scratch (a plane, and the image's tile cache)
    /// admitted while it reads. One field's grids and their preparation on
    /// one worker, the least `close` holds, are admitted and released
    /// again, so a model too large for the budget is refused here, as when
    /// `predict` prepared every field's grids.
    fn hold_for_fields(
        &self,
        model: SkyModel,
        operator: &ModelOperator,
        pattern: BeamPattern,
        fields: &[usize],
    ) -> SimulatorResult<(StepGrids, Reservation)> {
        let resources = &self.options.resources;
        let (host, policy) = (resources.host(), resources.policy());
        let admitted = |phase, memory| {
            admit(host, policy, &Demand { phase, memory }).map_err(SimulatorError::admission)
        };
        let centres = fields
            .iter()
            .map(|&field| {
                let pointing = self.fields[field].direction_rad;
                Ok((field, model.pixel_of(pointing).map_err(model_error)?))
            })
            .collect::<SimulatorResult<Vec<_>>>()?;
        let kept = model.held_bytes()
            + BeamPattern::BYTES
            + (size_of::<FieldModel>() + centres.capacity() * size_of::<(usize, [f64; 2])>())
                as u64;
        let reservation = admitted(MODEL_PHASE, kept)?;
        drop(admitted(
            MODEL_PHASE,
            operator.grid_bytes() + operator.plane_scratch_bytes(&model),
        )?);
        let reading = admitted(PREPARATION_PHASE, model.plane_bytes() + model.cache_bytes())?;
        let held = model.held().map_err(model_error)?;
        drop(model);
        drop(reading);
        tracing::info!(
            planes = held.planes(),
            fields = fields.len(),
            bytes = held.held_bytes(),
            grid_bytes = operator.grid_bytes(),
            "held the model {} for each field's grids",
            held.path().display()
        );
        Ok((
            StepGrids::PerField(Box::new(FieldModel {
                model: held,
                pattern,
                centres,
            })),
            reservation,
        ))
    }
}

/// The names in `paths` that are not empty.
fn named(paths: &[PathBuf]) -> Vec<&Path> {
    paths
        .iter()
        .map(PathBuf::as_path)
        .filter(|path| !path.as_os_str().is_empty())
        .collect()
}

/// Component list names as one label, comma-separated as CASA's
/// `simobserve` joins them.
fn list_label(paths: &[&Path]) -> String {
    paths
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(",")
}

/// The component lists at `paths` as one list, their components in order,
/// with the label naming them and what [`read_component_list`] holds for
/// each; `None` for no lists.
fn read_component_lists(
    paths: &[&Path],
    resources: &SessionResources,
) -> SimulatorResult<Option<(String, ComponentList, Reservation)>> {
    let mut joined: Option<(ComponentList, Reservation)> = None;
    for path in paths {
        let (list, held) = read_component_list(path, resources)?;
        match &mut joined {
            None => joined = Some((list, held)),
            Some((components, reservation)) => {
                for component in list.into_components() {
                    components.push(component);
                }
                reservation.join(held);
            }
        }
    }
    Ok(joined.map(|(list, held)| (list_label(paths), list, held)))
}

/// The component list at `path`, as `createSkyEquation` reads it: CASA's
/// `False` when the path is not a readable table ("ComponentList … not
/// readable"), and when the table is not a component list CASA reads
/// (`ComponentList`'s constructor throws, and `predict` catches it). The
/// read is admitted within `resources` before it starts; once the list is
/// formed, the list's share of that admission is returned with it and the
/// rest released.
fn read_component_list(
    path: &Path,
    resources: &SessionResources,
) -> SimulatorResult<(ComponentList, Reservation)> {
    if !path.join("table.dat").is_file() {
        return Err(SimulatorError::refused(format!(
            "Failed to create SkyEquation: ComponentList {} not readable",
            path.display()
        )));
    }
    let refused = |error| SimulatorError::refused(format!("Caught exception: {error}"));
    let reading_bytes = ComponentList::read_table_bytes(path).map_err(refused)?;
    let mut reading = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: COMPONENTS_PHASE,
            memory: reading_bytes,
        },
    )
    .map_err(SimulatorError::admission)?;
    let list = ComponentList::read_table(path).map_err(refused)?;
    let held = reading.split(list.bytes().min(reading_bytes));
    Ok((list, held))
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
fn correlation_error(error: PredictError, correlations: &[Correlation]) -> SimulatorError {
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
