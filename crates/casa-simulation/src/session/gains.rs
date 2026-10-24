// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.setgain` and `sm.setleakage`: antenna gains and polarization
//! leakage, which `corrupt` applies (#841 Phase 3).
//!
//! As in CASA's `Simulator`, each call simulates its term over the rows
//! observed so far, with the seed in force (`sm.setseed`), and adds it to
//! the terms every later `corrupt` applies, until `reset`. The simulation
//! and its differences from CASA are the `corrupt::jones` module's.

use std::sync::Arc;

use num_complex::Complex64;

use super::Simulator;
use super::error::{SimulatorError, SimulatorResult};
use crate::corrupt::jones::{Chunks, MAXIMUM_GAIN_SCALE, fbm_gains, leakages};

/// The arguments of `sm.setgain`.
#[derive(Debug, Clone, PartialEq)]
pub struct GainSpec {
    /// `mode`: `fbm` is implemented; `random` is not yet, and `table` CASA
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
    /// - `mode='random'`, a `table` to write, or a session `openfromms`
    ///   opened: not yet implemented.
    /// - No rows observed yet, or no `amplitude`: refused.
    pub fn set_gain(&mut self, spec: &GainSpec) -> SimulatorResult<()> {
        match spec.mode.as_str() {
            "fbm" => {}
            "table" => {
                return Err(SimulatorError::refused("Cannot yet read from table"));
            }
            "random" => {
                return Err(SimulatorError::not_yet_implemented(
                    "setgain(mode='random') is not yet implemented in casa-rs (simulation \
                     Phase 3): CASA draws those gains about zero rather than one \
                     (docs/CASA (C++) bugs.md); mode='fbm' is implemented",
                ));
            }
            other => {
                return Err(SimulatorError::refused(format!(
                    "Caught exception: unsupported mode {other} in setgain()"
                )));
            }
        }
        self.refuse_after_openfromms("setgain")?;
        Self::refuse_table("setgain", &spec.table)?;
        let camp = pair(&spec.amplitude, "setgain's amplitude")?;
        tracing::info!("Gain corruption with complex RMS amplitude = {camp}");
        self.require_rows()?;
        let mut scale = camp.norm();
        if scale >= MAXIMUM_GAIN_SCALE {
            tracing::warn!(" decreasing gain fluctuations from {scale} to 0.9 ");
            scale = MAXIMUM_GAIN_SCALE;
        }
        tracing::info!("Creating G JONES Calibration structure for data corruption.");
        let chunks = self.integration_chunks();
        let term = fbm_gains(self.seed, scale, self.antennas.len(), chunks);
        self.jones_terms.push(Arc::new(term));
        Ok(())
    }

    /// `sm.setleakage(...)`: simulate polarization leakages over the rows
    /// observed so far and add them to the terms `corrupt` applies.
    ///
    /// # Errors
    ///
    /// A `table` to write, or a session `openfromms` opened: not yet
    /// implemented. No rows observed yet, or no `amplitude` or `offset`:
    /// refused.
    pub fn set_leakage(&mut self, spec: &LeakageSpec) -> SimulatorResult<()> {
        self.refuse_after_openfromms("setleakage")?;
        Self::refuse_table("setleakage", &spec.table)?;
        let camp = pair(&spec.amplitude, "setleakage's amplitude")?;
        let offset = pair(&spec.offset, "setleakage's offset")?;
        self.require_rows()?;
        tracing::info!("Creating D Calibration structure for data corruption.");
        let chunks = self.scan_chunks();
        let term = leakages(self.seed, (camp, offset), self.antennas.len(), chunks);
        self.jones_terms.push(Arc::new(term));
        Ok(())
    }

    /// Refuse `call` writing its simulated term to `table`, which casa-rs
    /// does not yet do.
    fn refuse_table(call: &str, table: &str) -> SimulatorResult<()> {
        if table.is_empty() {
            return Ok(());
        }
        Err(SimulatorError::not_yet_implemented(format!(
            "{call}(table='{table}'), which writes the simulated term as a calibration table, is \
             not yet implemented in casa-rs (simulation Phase 3); table='' is"
        )))
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

    /// The solution chunks of a gain term: one per integration of each scan
    /// observed so far, at its TIME, in its spectral window.
    fn integration_chunks(&self) -> Chunks {
        let mut chunks = Chunks::default();
        for scan in &self.scans {
            let Some(planned) = scan.planned() else {
                continue;
            };
            for time_s in planned.integration_times_s() {
                chunks.push(scan.spectral_window_id, time_s);
            }
        }
        sorted(chunks)
    }

    /// The solution chunks of a leakage term: one per scan observed so
    /// far, at the middle of its first and last TIME, in its spectral
    /// window.
    fn scan_chunks(&self) -> Chunks {
        let mut chunks = Chunks::default();
        for scan in &self.scans {
            let Some(planned) = scan.planned() else {
                continue;
            };
            let mut times = planned.integration_times_s();
            let Some(first) = times.next() else {
                continue;
            };
            let last = times.last().unwrap_or(first);
            chunks.push(scan.spectral_window_id, 0.5 * (first + last));
        }
        sorted(chunks)
    }
}

/// `chunks` with each window's in time order, as `corrupt` looks them up.
fn sorted(mut chunks: Chunks) -> Chunks {
    for times in &mut chunks.by_window {
        times.sort_by(|a, b| a.0.total_cmp(&b.0));
    }
    chunks
}
