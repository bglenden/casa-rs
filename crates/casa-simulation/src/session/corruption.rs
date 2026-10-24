// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.setseed`, `sm.setnoise`, `sm.corrupt` and `sm.reset`: thermal noise,
//! and `corrupt`'s gains and leakage (`setgain`, `setleakage`: the `gains`
//! module).
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
//!
//! `setnoise(mode='simplenoise')` gives every draw one σ;
//! `mode='tsys-manual'` gives each row the σ of a system-temperature model
//! (the `corrupt` module's `tsys`). Each term adds its own noise: CASA
//! instead scales the noise of every term by every `tsys-manual` term's
//! σ, recorded with casa-rs's other departures in
//! `docs/CASA (C++) bugs.md`.

use std::sync::Arc;

use super::Simulator;
use super::error::{SimulatorError, SimulatorResult};
use super::plan::NoiseTerm;
pub use crate::corrupt::TsysParameters;
use crate::corrupt::jones::{HeldTerm, JonesKind};
use crate::corrupt::{MAXIMUM_APPLICATIONS, NoiseApplication, NoiseLevel, TsysManual};

/// CASA's `Simulator` seed until `setseed` (`seed_p(11111)`). The Python
/// `setseed()` default, 185349251, applies only when `setseed` is called.
pub const DEFAULT_SEED: i32 = 11_111;

/// A precipitable water vapour above this, mm, is refused: CASA takes it
/// for a value given without units, which default to metres.
const LARGEST_WATER_VAPOUR_MM: f64 = 100.0;

/// A zenith Tsys above this, K, or not positive, is refused
/// (`AtmosCorruptor::initialize`).
const LARGEST_ZENITH_TSYS_K: f32 = 1e6;

/// The arguments of `sm.setnoise` that casa-rs reads.
#[derive(Debug, Clone, PartialEq)]
pub struct NoiseSpec {
    /// `mode`: `simplenoise` and `tsys-manual` are implemented; `tsys-atm`
    /// is a later phase's (#841).
    pub mode: String,
    /// `simplenoise`: σ of each real and imaginary draw, Jy.
    pub simple_noise_jy: f64,
    /// `table`: the calibration table's name. CASA writes no table for a
    /// `simplenoise` term set with `OTF`; for `tsys-manual` it writes the
    /// `T` term's to `<table>.T.cal`, which casa-rs does not yet.
    pub table: String,
    /// `pwv`, mm: in the Tsys modes, a value above 100 mm is refused, as
    /// CASA refuses it (`tsys-manual` uses no water vapour).
    pub water_vapour_mm: f64,
    /// `tsys-manual`'s atmosphere, antenna and receiver parameters.
    pub tsys: TsysParameters,
    /// `OTF`: corrupt on the fly. CASA writes no calibration table then,
    /// whatever `table` says; without it, it writes one, which casa-rs does
    /// not yet.
    pub on_the_fly: bool,
}

impl Default for NoiseSpec {
    /// `simplenoise` of 0 Jy with CASA's other `setnoise` defaults.
    fn default() -> Self {
        Self {
            mode: "simplenoise".to_string(),
            simple_noise_jy: 0.0,
            table: String::new(),
            water_vapour_mm: 0.0,
            tsys: TsysParameters::default(),
            on_the_fly: true,
        }
    }
}

impl NoiseSpec {
    /// `setnoise(mode='simplenoise', simplenoise=σ)`, σ in Jy.
    #[must_use]
    pub fn simple(sigma_jy: f64) -> Self {
        Self {
            simple_noise_jy: sigma_jy,
            ..Self::default()
        }
    }

    /// `setnoise(mode='tsys-manual', ...)` with `tsys`'s parameters.
    #[must_use]
    pub fn tsys_manual(tsys: TsysParameters) -> Self {
        Self {
            mode: "tsys-manual".to_string(),
            tsys,
            ..Self::default()
        }
    }
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
    /// - `tsys-atm`, or `OTF=False`: not yet implemented; so is
    ///   `tsys-manual` with a `table`, which CASA writes.
    /// - No rows observed yet: CASA cannot size the term on an empty
    ///   MeasurementSet and returns `False`, adding no term.
    /// - `tsys-manual` with a water vapour above 100 mm, or a zenith Tsys
    ///   that is not positive or above 10⁶ K: CASA returns `False`. It has
    ///   added a unit-σ noise term by then, which casa-rs does not
    ///   (`docs/CASA (C++) bugs.md`).
    pub fn set_noise(&mut self, spec: &NoiseSpec) -> SimulatorResult<()> {
        let tsys = match spec.mode.as_str() {
            "simplenoise" => false,
            "tsys-manual" => true,
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
        };
        if !spec.on_the_fly {
            return Err(SimulatorError::not_yet_implemented(
                "setnoise(OTF=False), which writes a calibration table, is not yet implemented \
                 in casa-rs (simulation Phase 3)",
            ));
        }
        if !tsys {
            tracing::info!(
                "Using simple noise model with noise level of {} Jy",
                spec.simple_noise_jy
            );
        }
        if self.planned_rows() == 0 {
            return Err(SimulatorError::refused(
                "Caught exception: Error in Simulator::create_corrupt: MSIter::construct - \
                 Input MeasurementSet.has zero selected rows",
            ));
        }
        let level = if tsys {
            NoiseLevel::TsysManual(self.tsys_manual(spec)?)
        } else {
            NoiseLevel::Simple(spec.simple_noise_jy as f32)
        };
        tracing::info!("Creating A NOISE Calibration structure for data corruption.");
        if tsys {
            tracing::info!("Creating T Calibration structure for data corruption.");
        }
        self.noise_terms.push(NoiseTerm {
            seed: self.seed,
            level,
            applications: 0,
        });
        Ok(())
    }

    /// The `tsys-manual` model of `spec`, checked as CASA checks it.
    fn tsys_manual(&self, spec: &NoiseSpec) -> SimulatorResult<TsysManual> {
        let parameters = &spec.tsys;
        if parameters.receiver_type > 0 {
            tracing::info!("User has requested Double Sideband Receiver");
        } else if parameters.receiver_type < 0 {
            tracing::info!("User has not set Rx type, using 2SB");
        }
        if parameters.sensitivity > 0.0 {
            tracing::info!(
                "adding noise with the sensitivity constant of {}",
                parameters.sensitivity as f32
            );
        } else {
            tracing::info!("adding noise with the sensitivity constant of 1/sqrt(2)");
        }
        if spec.water_vapour_mm > LARGEST_WATER_VAPOUR_MM {
            return Err(SimulatorError::refused(format!(
                "Caught exception:  you input PWV={}mm. The most common reason for this error \
                 is forgetting to set units, which default to meters",
                spec.water_vapour_mm
            )));
        }
        if let Some(table) = calibration_table_root(&spec.table) {
            return Err(SimulatorError::not_yet_implemented(format!(
                "setnoise(mode='tsys-manual', table=...), which writes the T calibration table \
                 {table}.T.cal, is not yet implemented in casa-rs (simulation Phase 3)"
            )));
        }
        let model = TsysManual::new(parameters);
        // CASA checks the zenith Tsys at the centre of the first window.
        let window = self
            .spectral_windows
            .first()
            .ok_or_else(|| SimulatorError::raised("tsys-manual noise needs a spectral window"))?;
        let zenith = model.zenith_tsys(
            window.reference_frequency_hz,
            window.total_bandwidth_hz,
            window.channel_count,
        );
        tracing::info!(
            "manual tau set to {} Zenith Tsys at center of first Spectral Window = {zenith} \
             tground={} spillover={}; tauscale={}",
            model.tau(),
            parameters.ground_k as f32,
            parameters.spillover_efficiency as f32,
            model.tau()
        );
        if zenith > LARGEST_ZENITH_TSYS_K || zenith <= 0.0 {
            return Err(SimulatorError::refused(format!(
                "Caught exception: error in ATM setup - Tsys {zenith} poorly defined - check \
                 inputs"
            )));
        }
        Ok(model)
    }

    /// `sm.corrupt()`: apply every term to the rows observed so far: the
    /// gains and leakages (`setgain`, `setleakage`), then the noise. With
    /// no terms it does nothing.
    ///
    /// # Errors
    ///
    /// - A scan observed so far with a correlation count other than 1, 2 or
    ///   4, which CASA cannot corrupt ("No such VisVector type"); leakage on
    ///   a scan without all four correlations (CASA's `JonesGenLin` raises);
    ///   or a noise term applied 2²⁴ times.
    /// - A scan in a spectral window a gain or leakage term has no solution
    ///   for, which CASA leaves uncorrupted: not yet implemented.
    pub fn corrupt(&mut self) -> SimulatorResult<()> {
        if self.noise_terms.is_empty() && self.jones_terms.is_empty() {
            return Ok(());
        }
        self.check_visibility_correlations("corrupted")?;
        // A `setapply` table is evaluated over the scans observed by now.
        let terms = self
            .jones_terms
            .iter()
            .map(|source| self.jones_term(source))
            .collect::<SimulatorResult<Vec<_>>>()?;
        self.check_jones_coverage(&terms)?;
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
        let applications = self.noise_terms.iter_mut().map(|term| {
            let application = NoiseApplication {
                seed: term.seed,
                level: term.level,
                ordinal: term.applications,
            };
            term.applications += 1;
            application
        });
        // CASA corrupts the model from the sky side in: leakage before
        // gains, and of one kind the term set last first
        // (`VisEquation::collapseForSim`).
        let mut jones: Vec<(usize, Arc<HeldTerm>)> = terms.into_iter().enumerate().collect();
        jones.sort_by(|a, b| b.1.kind.cmp(&a.1.kind).then(b.0.cmp(&a.0)));
        let jones: Vec<Arc<HeldTerm>> = jones.into_iter().map(|(_, term)| term).collect();
        self.data_plan
            .corrupt(applications, jones, self.scans.len());
        Ok(())
    }

    /// Refuse gains or leakage on a scan observed so far that they cannot
    /// corrupt as CASA does: leakage without all four correlations, which
    /// CASA's `JonesGenLin` raises on, and a spectral window a term has no
    /// solution for, which CASA leaves wholly uncorrupted.
    fn check_jones_coverage(&self, terms: &[Arc<HeldTerm>]) -> SimulatorResult<()> {
        for scan in &self.scans {
            let window = &self.spectral_windows[scan.spectral_window_id];
            let products = window.correlation_products();
            // The linearised D takes the parallel hands into the cross
            // hands: CASA's 4-vector apply needs all four.
            let full = [[0, 0], [0, 1], [1, 0], [1, 1]]
                .iter()
                .all(|pair| products.contains(pair));
            for term in terms {
                if term.kind == JonesKind::Leakage && !full {
                    return Err(SimulatorError::raised(
                        "JonesGenLin matrix apply (J::aR) incompatible with VisVector.",
                    ));
                }
                if !term.covers(scan.spectral_window_id) {
                    return Err(SimulatorError::not_yet_implemented(format!(
                        "corrupt of spectral window {}, observed after a setgain or setleakage \
                         term was set, is not yet implemented in casa-rs (simulation Phase 3): \
                         CASA leaves it uncorrupted",
                        window.name
                    )));
                }
            }
        }
        Ok(())
    }

    /// `sm.reset()`: remove every noise, gain and leakage term.
    /// Corruptions already applied, and the seed, stay.
    pub fn reset(&mut self) {
        tracing::info!("Resetting all visibility corruption components");
        tracing::info!("Reset all image-plane corruption components");
        self.noise_terms.clear();
        self.jones_terms.clear();
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

/// The root of the calibration table `setnoise` writes for `table`, as
/// `Simulator::setnoise` trims it (blanks, then a trailing `cal`, `.` and
/// `_`), or `None` when it writes none (a root of at most one character).
fn calibration_table_root(table: &str) -> Option<&str> {
    let mut root = table.trim();
    if root.len() > 3 {
        root = root.strip_suffix("cal").unwrap_or(root);
    }
    for suffix in [".", "_"] {
        if root.len() > 1 {
            root = root.strip_suffix(suffix).unwrap_or(root);
        }
    }
    (root.len() > 1).then_some(root)
}

#[cfg(test)]
mod tests {
    use super::calibration_table_root;

    #[test]
    fn calibration_table_names_are_trimmed_as_casa_trims_them() {
        assert_eq!(calibration_table_root(""), None);
        assert_eq!(calibration_table_root(" x "), None);
        assert_eq!(calibration_table_root("noisy.cal"), Some("noisy"));
        assert_eq!(calibration_table_root("noisy_.cal"), Some("noisy"));
        assert_eq!(calibration_table_root("noisy_"), Some("noisy"));
        assert_eq!(calibration_table_root("cal"), Some("cal"));
        assert_eq!(calibration_table_root("a.cal"), None);
    }
}
