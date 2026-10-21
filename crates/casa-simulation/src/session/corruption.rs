// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.setseed`, `sm.setnoise`, `sm.corrupt` and `sm.reset`: thermal noise.
//!
//! As in CASA's `Simulator`:
//! - `setnoise` adds a noise term with the seed in force at the call; terms
//!   accumulate, duplicates included, until `reset`.
//! - `corrupt` applies every term to the rows observed so far. Scans
//!   observed later are not corrupted by it.
//! - `reset` removes the terms. Corruptions already applied stay, as they
//!   are already in CASA's DATA; the seed stays too.
//!
//! The draws are not CASA's (the crate's `corrupt` module describes the
//! generator and what is statistically the same). Two terms set with the same seed add
//! coherently in one `corrupt`, as CASA's identical streams do, and a term
//! applied again draws afresh.

use super::Simulator;
use super::error::{SimulatorError, SimulatorResult};
use super::plan::{DataStep, NoiseTerm};
use crate::corrupt::{MAXIMUM_APPLICATIONS, NoiseApplication};

/// CASA's `Simulator` seed until `setseed` (`seed_p(11111)`). The Python
/// `setseed()` default, 185349251, applies only when `setseed` is called.
pub const DEFAULT_SEED: i32 = 11_111;

/// The arguments of `sm.setnoise` that casa-rs reads.
#[derive(Debug, Clone, PartialEq)]
pub struct NoiseSpec {
    /// `mode`: `simplenoise` is implemented; `tsys-manual` and `tsys-atm`
    /// are later phases' (#841).
    pub mode: String,
    /// `simplenoise`: σ of each real and imaginary draw, Jy.
    pub simple_noise_jy: f64,
    /// `OTF`: corrupt on the fly. CASA writes no calibration table then,
    /// whatever `table` says; without it, it writes one, which casa-rs does
    /// not yet.
    pub on_the_fly: bool,
}

impl Simulator {
    /// `sm.setseed(seed)`: the seed of noise terms set from now on.
    pub fn set_seed(&mut self, seed: i32) {
        self.seed = seed;
    }

    /// `sm.setnoise(...)`: add a noise term with the current seed.
    ///
    /// # Errors
    ///
    /// - A mode CASA does not know: CASA logs "unsupported mode … in
    ///   setnoise()" and returns `False`.
    /// - `tsys-manual` or `tsys-atm`, or `OTF=False`: not yet implemented.
    /// - No rows observed yet: CASA cannot size the term on an empty
    ///   MeasurementSet and returns `False`, adding no term.
    pub fn set_noise(&mut self, spec: &NoiseSpec) -> SimulatorResult<()> {
        match spec.mode.as_str() {
            "simplenoise" => {}
            "tsys-manual" => {
                return Err(SimulatorError::not_yet_implemented(
                    "setnoise(mode='tsys-manual') is not yet implemented in casa-rs \
                     (simulation Phase 3)",
                ));
            }
            "tsys-atm" => {
                return Err(SimulatorError::not_yet_implemented(
                    "setnoise(mode='tsys-atm') is not yet implemented in casa-rs \
                     (simulation Phase 4)",
                ));
            }
            other => {
                return Err(SimulatorError::refused(format!(
                    "Caught exception: unsupported mode {other} in setnoise()"
                )));
            }
        }
        if !spec.on_the_fly {
            return Err(SimulatorError::not_yet_implemented(
                "setnoise(OTF=False), which writes a calibration table, is not yet implemented \
                 in casa-rs (simulation Phase 3)",
            ));
        }
        tracing::info!(
            "Using simple noise model with noise level of {} Jy",
            spec.simple_noise_jy
        );
        if self.planned_rows() == 0 {
            return Err(SimulatorError::refused(
                "Caught exception: Error in Simulator::create_corrupt: MSIter::construct - \
                 Input MeasurementSet.has zero selected rows",
            ));
        }
        tracing::info!("Creating A NOISE Calibration structure for data corruption.");
        self.noise_terms.push(NoiseTerm {
            seed: self.seed,
            sigma_jy: spec.simple_noise_jy as f32,
            applications: 0,
        });
        Ok(())
    }

    /// `sm.corrupt()`: apply every noise term to the rows observed so far.
    /// With no terms it does nothing.
    ///
    /// # Errors
    ///
    /// A scan observed so far with a correlation count other than 1, 2 or 4,
    /// which CASA cannot corrupt ("No such VisVector type"), or a term
    /// applied 2²⁴ times.
    pub fn corrupt(&mut self) -> SimulatorResult<()> {
        if self.noise_terms.is_empty() {
            return Ok(());
        }
        self.check_visibility_correlations("corrupted")?;
        if self
            .noise_terms
            .iter()
            .any(|term| term.applications >= MAXIMUM_APPLICATIONS)
        {
            return Err(SimulatorError::raised(format!(
                "a noise term cannot be applied more than {MAXIMUM_APPLICATIONS} times"
            )));
        }
        tracing::info!("Doing visibility corruption.");
        let applications = self
            .noise_terms
            .iter_mut()
            .map(|term| {
                let application = NoiseApplication {
                    seed: term.seed,
                    sigma_jy: term.sigma_jy,
                    ordinal: term.applications,
                };
                term.applications += 1;
                application
            })
            .collect();
        self.data_plan.push(DataStep::Corrupt {
            applications,
            scans: self.scans.len(),
        });
        Ok(())
    }

    /// `sm.reset()`: remove every noise term. Corruptions already applied,
    /// and the seed, stay.
    pub fn reset(&mut self) {
        tracing::info!("Resetting all visibility corruption components");
        tracing::info!("Reset all image-plane corruption components");
        self.noise_terms.clear();
    }

    /// MAIN rows of the scans observed so far.
    pub(super) fn planned_rows(&self) -> usize {
        self.scans.iter().map(|scan| scan.row_count()).sum()
    }

    /// CASA's visibility vectors hold 1, 2 or 4 correlations: a scan
    /// observed so far with another count cannot be `what` (corrupted,
    /// predicted).
    ///
    /// # Errors
    ///
    /// Raised: "No such VisVector type".
    pub(super) fn check_visibility_correlations(&self, what: &str) -> SimulatorResult<()> {
        for scan in &self.scans {
            let correlations = self.spectral_windows[scan.spectral_window_id]
                .correlations
                .len();
            if !matches!(correlations, 1 | 2 | 4) {
                return Err(SimulatorError::raised(format!(
                    "No such VisVector type: scan {} has {correlations} correlations; \
                     only 1, 2 or 4 can be {what}",
                    scan.scan_number
                )));
            }
        }
        Ok(())
    }
}
