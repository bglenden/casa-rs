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
//! | `sm.predict` with `GridFT` (`setoptions(ftmachine='ft')`) | [`predict::ModelPredictor`] |
//! | the model image `sm.predict` reads | [`predict::SkyModel`] |
//! | `sm.setvp(dovp=True)` with an Airy voltage pattern | [`predict::PrimaryBeam`] |

pub mod predict;
pub mod session;
mod write;

pub use session::array::{ArrayConfig, CoordinateSystem};
pub use session::error::{CasaOutcome, SimulatorError, SimulatorResult};
pub use session::observe::{ObserveSpec, StateSpec};
pub use session::spectral::SpectralWindowSpec;
pub use session::{Simulator, SimulatorOptions};
