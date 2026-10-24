// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.setgain` and `sm.setleakage`: antenna gains and polarization
//! leakage, which `corrupt` applies (#841 Phase 3).
//!
//! As in CASA's `Simulator`, each call simulates its term over the rows
//! observed so far, with the seed in force (`sm.setseed`), and adds it to
//! the terms every later `corrupt` applies, until `reset`. The simulation
//! and its differences from CASA are the `corrupt::jones` module's.

use std::path::PathBuf;
use std::sync::Arc;

use casa_calibration::{SimulatedJones, SimulatedSolution, write_simulated_jones_table};
use casa_ms::MeasurementSet;
use num_complex::Complex64;

use super::Simulator;
use super::error::{SimulatorError, SimulatorResult};
use crate::corrupt::jones::{
    Chunks, JonesKind, JonesTerm, MAXIMUM_GAIN_SCALE, fbm_gains, leakages, random_gains,
};

/// The arguments of `sm.setgain`.
#[derive(Debug, Clone, PartialEq)]
pub struct GainSpec {
    /// `mode`: `fbm`, gains drifting as fractional Brownian motion, or
    /// `random`, gains drawn afresh each integration about one (CASA's
    /// scatter about zero, `docs/CASA (C++) bugs.md`); `table` CASA
    /// refuses.
    pub mode: String,
    /// `table`: a calibration table CASA writes the simulated gains to;
    /// casa-rs does not write it yet.
    pub table: String,
    /// `amplitude`: one value `a` for the complex scale `(a, a)`, or two,
    /// `(a₀, a₁)`. The fluctuations' scale is its modulus.
    pub amplitude: Vec<f64>,
}

/// The arguments of `sm.setleakage`.
#[derive(Debug, Clone, PartialEq)]
pub struct LeakageSpec {
    /// `table`: a calibration table CASA writes the simulated leakages to;
    /// casa-rs does not write it yet. (`mode` CASA does not read.)
    pub table: String,
    /// `amplitude`: one value `a` for the complex scale `(a, a)` of the
    /// leakages' real and imaginary parts, or two.
    pub amplitude: Vec<f64>,
    /// `offset`: one value `o` for the offset `(o, o)` added to them, or
    /// two.
    pub offset: Vec<f64>,
}

/// CASA's complex pair from a one- or two-element vector (`setgain`,
/// `setleakage`).
fn pair(values: &[f64], what: &str) -> SimulatorResult<Complex64> {
    match values {
        [value] => Ok(Complex64::new(*value, *value)),
        [real, imaginary, ..] => Ok(Complex64::new(*real, *imaginary)),
        [] => Err(SimulatorError::refused(format!(
            "Caught exception: {what} needs at least one value"
        ))),
    }
}

impl Simulator {
    /// `sm.setgain(...)`: simulate antenna gains over the rows observed so
    /// far and add them to the terms `corrupt` applies.
    ///
    /// # Errors
    ///
    /// - `mode='table'`: CASA logs "Cannot yet read from table" and returns
    ///   `False`; another unknown mode is refused as CASA's "unsupported
    ///   mode".
    /// - No rows observed yet, or no `amplitude`: refused.
    /// - A `table` path that cannot be made absolute: raised.
    pub fn set_gain(&mut self, spec: &GainSpec) -> SimulatorResult<()> {
        let random = match spec.mode.as_str() {
            "fbm" => false,
            "random" => true,
            "table" => {
                return Err(SimulatorError::refused("Cannot yet read from table"));
            }
            other => {
                return Err(SimulatorError::refused(format!(
                    "Caught exception: unsupported mode {other} in setgain()"
                )));
            }
        };
        let table = table_path(&spec.table)?;
        let camp = pair(&spec.amplitude, "setgain's amplitude")?;
        tracing::info!("Gain corruption with complex RMS amplitude = {camp}");
        self.require_rows()?;
        tracing::info!("Creating G JONES Calibration structure for data corruption.");
        let chunks = self.integration_chunks();
        let antennas = self.antennas.len();
        let term = if random {
            random_gains(self.seed, camp, antennas, chunks)
        } else {
            let mut scale = camp.norm();
            if scale >= MAXIMUM_GAIN_SCALE {
                tracing::warn!(" decreasing gain fluctuations from {scale} to 0.9 ");
                scale = MAXIMUM_GAIN_SCALE;
            }
            fbm_gains(self.seed, scale, antennas, chunks)
        };
        self.add_jones_term(term, table);
        Ok(())
    }

    /// `sm.setleakage(...)`: simulate polarization leakages over the rows
    /// observed so far and add them to the terms `corrupt` applies.
    ///
    /// # Errors
    ///
    /// No rows observed yet, or no `amplitude` or `offset`: refused. A
    /// `table` path that cannot be made absolute: raised.
    pub fn set_leakage(&mut self, spec: &LeakageSpec) -> SimulatorResult<()> {
        let table = table_path(&spec.table)?;
        let camp = pair(&spec.amplitude, "setleakage's amplitude")?;
        let offset = pair(&spec.offset, "setleakage's offset")?;
        self.require_rows()?;
        tracing::info!("Creating D Calibration structure for data corruption.");
        let chunks = self.window_chunks();
        let term = leakages(self.seed, (camp, offset), self.antennas.len(), chunks);
        self.add_jones_term(term, table);
        Ok(())
    }

    /// Add `term` to those `corrupt` applies, and to the calibration
    /// tables `close` writes when it names one. CASA writes the table at
    /// once; casa-rs writes it at `close`, beside the MeasurementSet whose
    /// subtables it copies. `reset` does not remove it.
    fn add_jones_term(&mut self, term: JonesTerm, table: Option<PathBuf>) {
        let term = Arc::new(term);
        if let Some(path) = table {
            tracing::info!("Writing calTable = {} at close", path.display());
            self.jones_tables.push((path, Arc::clone(&term)));
        }
        self.jones_terms.push(term);
    }

    /// CASA's `create_corrupt` cannot size a term on an empty
    /// MeasurementSet.
    fn require_rows(&self) -> SimulatorResult<()> {
        if self.planned_rows() == 0 {
            return Err(SimulatorError::refused(
                "Caught exception: Error in Simulator::create_corrupt: MSIter::construct - \
                 Input MeasurementSet.has zero selected rows",
            ));
        }
        Ok(())
    }

    /// The heap of the gain and leakage terms the session holds through
    /// `close` (set, applied by a step, or to be written as a table), each
    /// counted once: they grow with the integrations or windows times the
    /// antennas, and `close` admits them.
    pub(crate) fn jones_term_bytes(&self) -> u64 {
        let mut seen = std::collections::HashSet::new();
        self.jones_terms
            .iter()
            .chain(self.data_plan().jones_terms())
            .chain(self.jones_tables.iter().map(|(_, term)| term))
            .filter(|term| seen.insert(Arc::as_ptr(term)))
            .map(|term| term.bytes())
            .sum()
    }

    /// Write the calibration tables `setgain` and `setleakage` named, from
    /// the MeasurementSet `close` wrote: one row per solution and antenna,
    /// by spectral window, time and antenna, as CASA's
    /// `NewCalTable::writeToDisk` sorts them.
    pub(super) fn write_jones_tables(&self) -> SimulatorResult<()> {
        if self.jones_tables.is_empty() {
            return Ok(());
        }
        let ms = MeasurementSet::open(self.path()).map_err(|error| {
            SimulatorError::raised(format!("reopen the MeasurementSet: {error}"))
        })?;
        for (path, term) in &self.jones_tables {
            let kind = match term.kind {
                JonesKind::Gain => SimulatedJones::Gain,
                JonesKind::Leakage => SimulatedJones::Leakage,
            };
            let solutions: Vec<SimulatedSolution> =
                term.solutions()
                    .flat_map(|(window, solution, values)| {
                        values.iter().enumerate().map(move |(antenna, &receptors)| {
                            SimulatedSolution {
                                time_s: solution.time_s,
                                field_id: solution.field as i32,
                                spectral_window_id: window as i32,
                                antenna: antenna as i32,
                                receptors,
                            }
                        })
                    })
                    .collect();
            write_simulated_jones_table(&ms, path, kind, &solutions).map_err(|error| {
                SimulatorError::raised(format!(
                    "write the calibration table {}: {error}",
                    path.display()
                ))
            })?;
        }
        Ok(())
    }

    /// Scan `scan`'s integrations, their TIMEs in order: as planned, or as
    /// the opened MeasurementSet has them.
    fn integration_times_s(&self, scan: usize) -> Vec<f64> {
        if let Some(planned) = self.scans[scan].planned() {
            return planned.integration_times_s().collect();
        }
        self.opened
            .as_ref()
            .and_then(|opened| opened.integration_times_s.get(scan))
            .cloned()
            .unwrap_or_default()
    }

    /// The solution chunks of a gain term: one per integration of each scan
    /// observed so far, at its TIME, in its spectral window and field.
    fn integration_chunks(&self) -> Chunks {
        let mut chunks = Chunks::default();
        for (index, scan) in self.scans.iter().enumerate() {
            for time_s in self.integration_times_s(index) {
                chunks.push(scan.spectral_window_id, time_s, scan.field_id);
            }
        }
        chunks.sorted()
    }

    /// The solution chunks of a leakage term: one per spectral window
    /// observed so far, at its earliest TIME and on that integration's
    /// field. CASA's infinite solution interval spans every scan and field
    /// of a window: its table has one solution per window and antenna.
    fn window_chunks(&self) -> Chunks {
        let mut first: Vec<Option<(f64, usize)>> = Vec::new();
        for (index, scan) in self.scans.iter().enumerate() {
            let Some(&time_s) = self.integration_times_s(index).first() else {
                continue;
            };
            let window = scan.spectral_window_id;
            if first.len() <= window {
                first.resize(window + 1, None);
            }
            if first[window].is_none_or(|(earlier, _)| time_s < earlier) {
                first[window] = Some((time_s, scan.field_id));
            }
        }
        let mut chunks = Chunks::default();
        for (window, first) in first.into_iter().enumerate() {
            if let Some((time_s, field)) = first {
                chunks.push(window, time_s, field);
            }
        }
        chunks
    }
}

/// `table` as the absolute path `close` writes it to, or `None` for none.
fn table_path(table: &str) -> SimulatorResult<Option<PathBuf>> {
    if table.is_empty() {
        return Ok(None);
    }
    std::path::absolute(table)
        .map(Some)
        .map_err(|error| SimulatorError::raised(format!("table {table}: {error}")))
}
