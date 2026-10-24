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
//! | `sm.openfromms`: `predict` and `corrupt` on an existing MeasurementSet | [`Simulator::open_from_ms`] |
//! | `sm.predict` with `GridFT` or `WProjectFT` (`setoptions(ftmachine='ft' or 'wproject')`) | [`Simulator::predict`], [`Simulator::set_options`], [`predict::FtMachine`]; one row at a time, [`predict::ModelPredictor`] |
//! | `sm.predict` with `MosaicFTNew` for ALMA and ACA (`setoptions(ftmachine='mosaic')`) | [`Simulator::predict`], [`predict::FtMachine::Mosaic`] |
//! | the model image `sm.predict` reads | [`predict::SkyModel`] |
//! | `sm.setvp(dovp=True)` with an Airy voltage pattern | [`Simulator::set_vp`], [`predict::PrimaryBeam`] |
//! | `sm.setseed`, `sm.setnoise(mode='simplenoise')`, `sm.corrupt`, `sm.reset` | [`Simulator::set_seed`], [`Simulator::set_noise`], [`Simulator::corrupt`], [`Simulator::reset`] |
//! | `sm.setnoise(mode='tsys-manual')`: σ per row from a system temperature (CASA's `AtmosCorruptor`) | [`Simulator::set_noise`], [`NoiseSpec::tsys_manual`], [`TsysParameters`] |
//! | `sm.setgain(mode='fbm' or 'random')`, `sm.setleakage`, and the calibration tables they write | [`Simulator::set_gain`], [`Simulator::set_leakage`] |
//! | `sm.setapply` of a `G`, `T` or `D` table | [`Simulator::set_apply`] |
//! | `simutil.readantenna(antab)` | [`readantenna::read_antenna_config`] |
//!
//! CASA's `sm` has no execution controls; a session's memory budget, workers
//! and backend ([`ExecutionControls`], [`SessionResources`]) are the Python
//! tool's constructor arguments. `close` predicts a model on the CPU or the
//! host's Metal device (on the CPU only through `WProjectFT`), and draws
//! noise and applies gains and leakage on the CPU. Where the session departs
//! from CASA is recorded in `docs/CASA (C++) bugs.md`.

mod corrupt;
pub mod predict;
pub mod readantenna;
pub mod session;
mod write;

pub use session::array::{ArrayConfig, CoordinateSystem};
pub use session::error::{CasaOutcome, SimulatorError, SimulatorResult};
pub use session::observe::{ObserveManySpec, ObserveSpec, Pointing, StateSpec};
pub use session::spectral::SpectralWindowSpec;
pub use session::{
    ApplySpec, BackendRequest, DEFAULT_SEED, ExecutionControls, GainSpec, LeakageSpec, NoiseSpec,
    PredictSpec, PredictionOptions, SessionResources, Simulator, SimulatorOptions, TsysParameters,
    UnknownBackend, VoltagePatternSpec,
};
