// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.setgain` and `sm.setleakage`: antenna gains and polarization
//! leakage, which `corrupt` applies (#841 Phase 3).
//!
//! As in CASA's `Simulator`, each call simulates its term over the rows
//! observed so far, with the seed in force (`sm.setseed`), and adds it to
//! the terms every later `corrupt` applies, until `reset`. The simulation
//! and its differences from CASA are the `corrupt::jones` module's.
//!
//! A term grows with the integrations observed, so each call counts them
//! first, admits what the term keeps and what forming it holds against the
//! session's resource policy, and only then forms it; the term keeps its
//! share for as long as it, or a corruption recorded with it, lives.

use std::sync::Arc;

use casa_imaging_runtime::{Demand, Reservation, admit};
use num_complex::Complex64;

use super::Simulator;
use super::error::{SimulatorError, SimulatorResult};
use crate::corrupt::jones::{
    Chunks, HeldTerm, JonesTerm, MAXIMUM_GAIN_SCALE, TermBytes, fbm_gains, leakages, random_gains,
    screen_slots,
};

/// The phase a gain or leakage term is admitted as, at `setgain` or
/// `setleakage`: what it keeps for the session, and what forming it holds.
const JONES_TERM_PHASE: &str = "a simulated gain or leakage term";

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
    /// - The term's memory does not fit the session's resource policy: an
    ///   admission error, before anything is formed.
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
        let antennas = self.antennas.len();
        let (per_window, span) = self.integration_counts();
        let bytes = if random {
            TermBytes::per_chunk(&per_window, antennas)
        } else {
            TermBytes::fbm(&per_window, screen_slots(span), antennas)
        };
        let reservation = self.admit_term(bytes)?;
        let chunks = self.integration_chunks(&per_window);
        drop(per_window);
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
        self.hold_term(term, reservation, bytes);
        Ok(())
    }

    /// Admit a term's memory before it is formed.
    fn admit_term(&self, bytes: TermBytes) -> SimulatorResult<Reservation> {
        let resources = &self.options.resources;
        admit(
            resources.host(),
            resources.policy(),
            &Demand {
                phase: JONES_TERM_PHASE,
                memory: bytes.kept + bytes.forming,
            },
        )
        .map_err(SimulatorError::admission)
    }

    /// Add `term` to the terms `corrupt` applies, keeping of `reservation`
    /// what it keeps, for as long as the term lives.
    fn hold_term(&mut self, term: JonesTerm, mut reservation: Reservation, bytes: TermBytes) {
        reservation.retain(bytes.kept);
        self.jones_terms
            .push(Arc::new(HeldTerm::new(term, reservation)));
    }

    /// `sm.setleakage(...)`: simulate polarization leakages over the rows
    /// observed so far and add them to the terms `corrupt` applies.
    ///
    /// # Errors
    ///
    /// A `table` to write, or a session `openfromms` opened: not yet
    /// implemented. No rows observed yet, or no `amplitude` or `offset`:
    /// refused. The term's memory does not fit the session's resource
    /// policy: an admission error, before anything is formed.
    pub fn set_leakage(&mut self, spec: &LeakageSpec) -> SimulatorResult<()> {
        self.refuse_after_openfromms("setleakage")?;
        Self::refuse_table("setleakage", &spec.table)?;
        let camp = pair(&spec.amplitude, "setleakage's amplitude")?;
        let offset = pair(&spec.offset, "setleakage's offset")?;
        self.require_rows()?;
        tracing::info!("Creating D Calibration structure for data corruption.");
        let antennas = self.antennas.len();
        let first = self.first_times();
        let per_window: Vec<usize> = first
            .iter()
            .map(|time| usize::from(time.is_some()))
            .collect();
        let bytes = TermBytes::per_chunk(&per_window, antennas);
        let reservation = self.admit_term(bytes)?;
        let chunks = window_chunks(&first, &per_window);
        let term = leakages(self.seed, (camp, offset), antennas, chunks);
        self.hold_term(term, reservation, bytes);
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

    /// The integrations of the scans observed so far in each spectral
    /// window, and the span of their times: what a gain term's chunks
    /// number, counted before they are formed.
    fn integration_counts(&self) -> (Vec<usize>, Option<[f64; 2]>) {
        let mut per_window = Vec::new();
        let mut span: Option<[f64; 2]> = None;
        for scan in &self.scans {
            let Some(planned) = scan.planned() else {
                continue;
            };
            let window = scan.spectral_window_id;
            if per_window.len() <= window {
                per_window.resize(window + 1, 0);
            }
            for time_s in planned.integration_times_s() {
                per_window[window] += 1;
                span = Some(span.map_or([time_s, time_s], |[low, high]| {
                    [low.min(time_s), high.max(time_s)]
                }));
            }
        }
        (per_window, span)
    }

    /// The solution chunks of a gain term: one per integration of each scan
    /// observed so far, at its TIME, in its spectral window, counted
    /// `per_window` ([`Self::integration_counts`]).
    fn integration_chunks(&self, per_window: &[usize]) -> Chunks {
        let mut chunks = Chunks::with_capacity(per_window);
        for scan in &self.scans {
            let Some(planned) = scan.planned() else {
                continue;
            };
            for time_s in planned.integration_times_s() {
                chunks.push(scan.spectral_window_id, time_s);
            }
        }
        // Each window's in time order, as `corrupt` looks them up; equal
        // times keep the order they were observed in. Sorting in place
        // holds nothing beside the chunks.
        for times in &mut chunks.by_window {
            times.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        }
        chunks
    }

    /// Each spectral window's earliest TIME among the scans observed so
    /// far, if any. CASA's infinite leakage solution interval spans every
    /// scan and field of a window: its table has one solution per window
    /// and antenna.
    fn first_times(&self) -> Vec<Option<f64>> {
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
        first
    }
}

/// The solution chunks of a leakage term: one per spectral window observed
/// so far, at its earliest TIME (`first`), counted `per_window`.
fn window_chunks(first: &[Option<f64>], per_window: &[usize]) -> Chunks {
    let mut chunks = Chunks::with_capacity(per_window);
    for (window, time_s) in first.iter().enumerate() {
        if let Some(time_s) = *time_s {
            chunks.push(window, time_s);
        }
    }
    chunks
}
