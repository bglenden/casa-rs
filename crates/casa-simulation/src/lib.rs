// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! CASA-compatible MeasurementSet simulation.
//!
//! This crate is the Rust side of CASA's `simulator` tool (`sm`) and of the
//! `simobserve`, `simanalyze` and `simalma` tasks
//! (`docs/simulation/simulation-plan-20261020.md`, #841). Prediction goes
//! through the imaging measurement operator, so simulation and imaging share
//! one forward model.
//!
//! | CASA | casa-rs |
//! |---|---|
//! | the `sm` tool's session (`open`, the setters, `observe`, `close`) | [`session::Simulator`] |
//! | `sm.predict` with `GridFT` (`setoptions(ftmachine='ft')`) | [`Simulator::predict`], [`Simulator::set_options`]; one row at a time, [`predict::ModelPredictor`] |
//! | the model image `sm.predict` reads | [`predict::SkyModel`] |
//! | `sm.setvp(dovp=True)` with an Airy voltage pattern | [`Simulator::set_vp`], [`predict::PrimaryBeam`] |
//! | `sm.setseed`, `sm.setnoise(mode='simplenoise')`, `sm.corrupt`, `sm.reset` | [`Simulator::set_seed`], [`Simulator::set_noise`], [`Simulator::corrupt`], [`Simulator::reset`] |
//! | `simutil.readantenna(antab)` | [`readantenna::read_antenna_config`] |
//!
//! CASA's `sm` has no execution controls; a session's memory budget, workers
//! and backend ([`ExecutionControls`], [`SessionResources`]) are the Python
//! tool's constructor arguments. `close` predicts a model on the CPU or the
//! host's Metal device and draws noise on the CPU. Where the session departs
//! from CASA is recorded in `docs/CASA (C++) bugs.md`.

mod corrupt;
pub mod predict;
pub mod readantenna;
pub mod session;
mod write;

pub use session::array::{ArrayConfig, CoordinateSystem};
pub use session::error::{CasaOutcome, SimulatorError, SimulatorResult};
pub use session::observe::{ObserveSpec, StateSpec};
pub use session::spectral::SpectralWindowSpec;
pub use session::{
    BackendRequest, DEFAULT_SEED, ExecutionControls, NoiseSpec, PredictSpec, PredictionOptions,
    SessionResources, Simulator, SimulatorOptions, UnknownBackend, VoltagePatternSpec,
};
