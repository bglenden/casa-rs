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
//! (the `corrupt` module's `tsys`), and `mode='tsys-atm'` each row and
//! channel the σ of one whose atmosphere is ATM's (`tsys_atm`). Each term
//! adds its own noise: CASA instead scales the noise of every term by every
//! Tsys term's σ, recorded with casa-rs's other departures in
//! `docs/CASA (C++) bugs.md`.

use std::sync::Arc;

use super::Simulator;
use super::error::{SimulatorError, SimulatorResult};
use super::plan::NoiseTerm;
use crate::corrupt::jones::{HeldTerm, JonesKind};
pub use crate::corrupt::{AtmosphereSpec, TsysParameters};
use crate::corrupt::{MAXIMUM_APPLICATIONS, NoiseApplication, NoiseLevel, TsysManual};

/// CASA's `Simulator` seed until `setseed` (`seed_p(11111)`). The Python
/// `setseed()` default, 185349251, applies only when `setseed` is called.
pub const DEFAULT_SEED: i32 = 11_111;

/// A precipitable water vapour above this, mm, is refused: CASA takes it
/// for a value given without units, which default to metres.
const LARGEST_WATER_VAPOUR_MM: f64 = 100.0;

/// A zenith Tsys above this, K, or not positive, is refused
/// (`AtmosCorruptor::initialize`).
pub(super) const LARGEST_ZENITH_TSYS_K: f32 = 1e6;

/// The water vapour `tsys-atm` takes when `pwv` is not positive, mm.
const DEFAULT_WATER_VAPOUR_MM: f64 = 1.0;

/// The arguments of `sm.setnoise` that casa-rs reads.
#[derive(Debug, Clone, PartialEq)]
pub struct NoiseSpec {
    /// `mode`: `simplenoise`, `tsys-manual` or `tsys-atm`.
    pub mode: String,
    /// `simplenoise`: σ of each real and imaginary draw, Jy.
    pub simple_noise_jy: f64,
    /// `table`: the calibration-table root. Tsys modes write T/Tf noise-scale
    /// parameters to `<table>.T.cal` at close (CASA writes at `setnoise`).
    /// A `simplenoise` term with `OTF=True` writes no table.
    pub table: String,
    /// `pwv`, mm: in the Tsys modes, a value above 100 mm is refused, as
    /// CASA refuses it. `tsys-atm` sees the sky through this column of
    /// water, 1 mm when not positive; `tsys-manual` uses none.
    pub water_vapour_mm: f64,
    /// The Tsys modes' antenna and receiver parameters, and
    /// `tsys-manual`'s atmosphere (`tatmos`, `tau`).
    pub tsys: TsysParameters,
    /// `tsys-atm`'s site and atmosphere (`pground`, `relhum`, `altitude`,
    /// `waterheight`); the ground's temperature is `tsys`'s `tground`.
    pub atmosphere: AtmosphereSpec,
    /// `OTF`: draw fresh additive noise at each corrupt. False reuses a
    /// precomputed A Noise field. Tsys modes scale its unit draws by their
    /// noise model. A named field is saved as `<root>.A.cal`. This is separate
    /// from the optional Tsys noise-scale table.
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
            atmosphere: AtmosphereSpec::default(),
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

    /// `setnoise(mode='tsys-atm', pwv=…, ...)` with `tsys`'s antenna and
    /// receiver parameters, `water_vapour_mm` of water vapour and the site
    /// `atmosphere`.
    #[must_use]
    pub fn tsys_atm(
        tsys: TsysParameters,
        water_vapour_mm: f64,
        atmosphere: AtmosphereSpec,
    ) -> Self {
        Self {
            mode: "tsys-atm".to_string(),
            water_vapour_mm,
            tsys,
            atmosphere,
            ..Self::default()
        }
    }
}

/// The noise modes `setnoise` knows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NoiseMode {
    Simple,
    TsysManual,
    TsysAtm,
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
    /// - Offline noise on observations added later remains unfinished
    ///   (#1292); corruption refuses it before DATA changes. Named output
    ///   tables are written at close from this call's snapshots.
    /// - No rows observed yet: CASA cannot size the term on an empty
    ///   MeasurementSet and returns `False`, adding no term.
    /// - A Tsys mode with a water vapour above 100 mm, or a zenith Tsys at
    ///   the centre of the first spectral window that is not positive or is
    ///   above 10⁶ K: CASA returns `False`. It has added a unit-σ noise term
    ///   by then, which casa-rs does not (`docs/CASA (C++) bugs.md`).
    ///   casa-rs also refuses a `tsys-atm` zenith Tsys that is not a number,
    ///   which CASA's check lets through.
    /// - `tsys-atm` with site values ATM cannot build an atmosphere from, or
    ///   whose atmosphere the session's memory budget cannot hold.
    pub fn set_noise(&mut self, spec: &NoiseSpec) -> SimulatorResult<()> {
        let mode = match spec.mode.as_str() {
            "simplenoise" => NoiseMode::Simple,
            "tsys-manual" => NoiseMode::TsysManual,
            "tsys-atm" => NoiseMode::TsysAtm,
            other => {
                return Err(SimulatorError::refused(format!(
                    "Caught exception: unsupported mode {other} in setnoise()"
                )));
            }
        };
        if mode == NoiseMode::Simple {
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
        let level = match mode {
            NoiseMode::Simple => NoiseLevel::Simple(spec.simple_noise_jy as f32),
            NoiseMode::TsysManual => NoiseLevel::TsysManual(self.tsys_manual(spec)?),
            NoiseMode::TsysAtm => {
                check_tsys_spec(spec)?;
                let water_vapour_mm = if spec.water_vapour_mm > 0.0 {
                    spec.water_vapour_mm
                } else {
                    tracing::info!("User has not set PWV, using 1mm");
                    DEFAULT_WATER_VAPOUR_MM
                };
                NoiseLevel::TsysAtm(Arc::new(self.tsys_atm(spec, water_vapour_mm)?))
            }
        };
        let table = self.prepare_noise_table(&level, &spec.table)?;
        tracing::info!("Creating A NOISE Calibration structure for data corruption.");
        match mode {
            NoiseMode::Simple => {}
            NoiseMode::TsysManual => {
                tracing::info!("Creating T Calibration structure for data corruption.");
            }
            NoiseMode::TsysAtm => {
                tracing::info!("Creating TF NOISE Calibration structure for data corruption.");
            }
        }
        if spec.on_the_fly {
            self.noise_terms.push(NoiseTerm {
                seed: self.seed,
                level,
                applications: 0,
            });
        } else {
            self.set_offline_noise(spec, level)?;
        }
        if let Some(table) = table {
            self.noise_tables.retain(|old| old.path != table.path);
            self.noise_tables.push(table);
        }
        Ok(())
    }

    /// The `tsys-manual` model of `spec`, checked as CASA checks it.
    fn tsys_manual(&self, spec: &NoiseSpec) -> SimulatorResult<TsysManual> {
        let parameters = &spec.tsys;
        check_tsys_spec(spec)?;
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
    /// - A covered scan defined after a `tsys-atm` term was set: not yet
    ///   implemented. A window any Jones term has no solution for skips
    ///   the entire step, including noise and corrected-data updates.
    pub fn corrupt(&mut self) -> SimulatorResult<()> {
        if self.noise_terms.is_empty() && self.jones_terms.is_empty() {
            return Ok(());
        }
        self.check_visibility_correlations("corrupted")?;
        // CASA gates the complete step before evaluating any term. The
        // predicate intersects simulated and applied metadata without a
        // per-window allocation; an excluded window forms no table grid.
        let covered = |window| self.jones_terms.iter().all(|source| source.covers(window));
        // A `setapply` table is evaluated over the covered scans observed now.
        let terms = self
            .jones_terms
            .iter()
            .map(|source| self.jones_term(source, &covered))
            .collect::<SimulatorResult<Vec<_>>>()?;
        self.check_jones_coverage(&terms)?;
        self.check_noise_coverage(&terms)?;
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
                level: term.level.clone(),
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

    /// Validate the correlations of covered scans. CASA gates the entire
    /// corruption step before checking or applying any Jones operator.
    fn check_jones_coverage(&self, terms: &[Arc<HeldTerm>]) -> SimulatorResult<()> {
        for scan in &self.scans {
            // VisEquation::spwOK gates the entire step: no term is
            // applied when any Jones term has no solution in this window.
            if terms
                .iter()
                .any(|term| !term.covers(scan.spectral_window_id))
            {
                continue;
            }
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
                if term.kind == JonesKind::General && !full {
                    return Err(SimulatorError::raised(
                        "Jones matrix apply (J::aR) incompatible with VisVector.",
                    ));
                }
            }
        }
        Ok(())
    }

    /// Refuse a scan observed so far in a spectral window defined after a
    /// `tsys-atm` term was set: as CASA's corruptor, the term built its
    /// atmosphere for the windows defined then.
    fn check_noise_coverage(&self, terms: &[Arc<HeldTerm>]) -> SimulatorResult<()> {
        for term in &self.noise_terms {
            if let NoiseLevel::Fixed(field) | NoiseLevel::ScaledFixed { field, .. } = &term.level {
                if self.scans.iter().enumerate().any(|(index, scan)| {
                    terms
                        .iter()
                        .all(|term| term.covers(scan.spectral_window_id))
                        && (index >= field.scans || !field.covers(scan.spectral_window_id))
                }) {
                    return Err(SimulatorError::not_yet_implemented(
                        "OTF=False noise on observations added after setnoise is not yet implemented (#1292)",
                    ));
                }
                continue;
            }
            let NoiseLevel::TsysAtm(model) = &term.level else {
                continue;
            };
            if let Some(scan) = self.scans.iter().find(|scan| {
                terms
                    .iter()
                    .all(|term| term.covers(scan.spectral_window_id))
                    && !model.covers(scan.spectral_window_id)
            }) {
                return Err(SimulatorError::not_yet_implemented(format!(
                    "corrupt of spectral window {}, defined after a setnoise(mode='tsys-atm') \
                     term was set, is not yet implemented in casa-rs (#1053): \
                     the term has no atmosphere for it",
                    self.spectral_windows[scan.spectral_window_id].name
                )));
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

/// The checks, and log lines, `setnoise` makes of both Tsys modes'
/// arguments before it builds the model.
fn check_tsys_spec(spec: &NoiseSpec) -> SimulatorResult<()> {
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
            "Caught exception:  you input PWV={}mm. The most common reason for this error is \
             forgetting to set units, which default to meters",
            spec.water_vapour_mm
        )));
    }
    Ok(())
}

/// The root of the calibration table `setnoise` writes for `table`, as
/// `Simulator::setnoise` trims it (blanks, then a trailing `cal`, `.` and
/// `_`), or `None` when it writes none (a root of at most one character).
pub(super) fn calibration_table_root(table: &str) -> Option<&str> {
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
