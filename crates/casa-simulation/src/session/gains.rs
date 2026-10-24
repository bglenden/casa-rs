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
use crate::corrupt::jones::{Chunks, MAXIMUM_GAIN_SCALE, fbm_gains, leakages, random_gains};

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
    /// - A `table` to write, or a session `openfromms` opened: not yet
    ///   implemented.
    /// - No rows observed yet, or no `amplitude`: refused.
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
        self.refuse_after_openfromms("setgain")?;
        Self::refuse_table("setgain", &spec.table)?;
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
        let chunks = self.window_chunks();
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

    /// The solution chunks of a leakage term: one per spectral window
    /// observed so far, at its earliest TIME. CASA's infinite solution
    /// interval spans every scan and field of a window: its table has one
    /// solution per window and antenna.
    fn window_chunks(&self) -> Chunks {
        let mut first: Vec<Option<f64>> = Vec::new();
        for scan in &self.scans {
            let Some(time_s) = scan
                .planned()
                .and_then(|planned| planned.integration_times_s().next())
            else {
                continue;
            };
            let window = scan.spectral_window_id;
            if first.len() <= window {
                first.resize(window + 1, None);
            }
            first[window] = Some(first[window].map_or(time_s, |earlier| earlier.min(time_s)));
        }
        let mut chunks = Chunks::default();
        for (window, time_s) in first.into_iter().enumerate() {
            if let Some(time_s) = time_s {
                chunks.push(window, time_s);
            }
        }
        chunks
    }
}

/// `chunks` with each window's in time order, as `corrupt` looks them up.
fn sorted(mut chunks: Chunks) -> Chunks {
    for times in &mut chunks.by_window {
        times.sort_by(|a, b| a.0.total_cmp(&b.0));
    }
    chunks
}
