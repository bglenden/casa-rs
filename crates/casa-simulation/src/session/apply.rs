// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.setapply`: corrupt by a calibration table (#841 Phase 3).
//!
//! CASA's `Simulator::setapply` adds the table's term to those `corrupt`
//! applies; the term corrupts the model as a simulated one of its type
//! does (`VisEquation::collapseForSim`: a `G` table multiplies each
//! correlation by `g_p·conj(g_q)`, a `D` table leaks into the cross hands),
//! with the table's solutions found and interpolated in time as
//! `calibrater` applies them, at each row's time, scans observed after the
//! `setapply` included (measured on CASA 6.7.6: `interp='linear'`
//! interpolates amplitude and phase apart, `'nearest'` takes the nearest).
//! casa-rs loads the table at `setapply`, as CASA does, and keeps it; at
//! each `corrupt` it finds the table's solutions through `casa-calibration`'s
//! [`CalibrationTableLookup`] for each integration observed by then and
//! antenna, and applies them as the terms `setgain` and `setleakage`
//! simulate. A table changed or moved after `setapply` does not change the
//! term (#893 review).
//!
//! CASA's `setapply` ignores its `field`, and its interpolation does not
//! partition the table by field: a row takes the solutions of every field
//! as one series in time ([`FieldSelection::AnyField`]; measured on CASA
//! 6.7.6, #893 review).
//!
//! Loading a table is admitted against the session's resource policy
//! before it is read, and the loaded table keeps its share for as long as
//! the term does; each `corrupt` admits the term it evaluates, as
//! `setgain` admits one it simulates.

use std::path::PathBuf;
use std::sync::Arc;

use casa_calibration::{ApplyInterpolationMode, CalibrationTableLookup, FieldSelection};
use casa_imaging_runtime::{Demand, Reservation, admit};
use num_complex::Complex32;

use super::Simulator;
use super::error::{SimulatorError, SimulatorResult};
use crate::corrupt::jones::{Chunks, HeldTerm, JonesKind, TermBytes, applied};

/// The phase loading a `setapply` table is admitted as: the table as it is
/// read, and what the loaded lookup keeps.
const LOOKUP_PHASE: &str = "loading a setapply calibration table";

/// The arguments of `sm.setapply`.
#[derive(Debug, Clone, PartialEq)]
pub struct ApplySpec {
    /// `table`: the calibration table to corrupt by.
    pub table: String,
    /// `type`: its calibration type (`G` or `D`); empty takes the table's.
    pub kind: String,
    /// `t`: the interpolation interval, seconds; casa-rs takes 0 only.
    pub interval_s: f64,
    /// `interp`: the time interpolation (`linear` or `nearest`), optionally
    /// followed by `,` and a frequency interpolation; empty is `linear`.
    pub interp: String,
    /// `calwt`: whether to calibrate the weights; casa-rs does not yet.
    pub calwt: bool,
    /// `spwmap`: each data window's table window; −1, or a window past
    /// its end, is the data window itself.
    pub spwmap: Vec<i64>,
    /// `opacity` (for `type='TOPAC'`); casa-rs takes 0 only.
    pub opacity: f64,
}

/// A gain or leakage term `corrupt` applies: simulated when it was set
/// (`setgain`, `setleakage`), or a calibration table's (`setapply`),
/// evaluated over the scans observed at each `corrupt`.
#[derive(Debug, Clone)]
pub(crate) enum JonesSource {
    Simulated(Arc<HeldTerm>),
    Applied(AppliedTable),
}

/// A table `setapply` set: the table as it was loaded, and how to apply it.
#[derive(Clone)]
pub(crate) struct AppliedTable {
    path: PathBuf,
    lookup: Arc<HeldLookup>,
    kind: JonesKind,
    spwmap: Vec<i64>,
}

impl std::fmt::Debug for AppliedTable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AppliedTable")
            .field("path", &self.path)
            .field("kind", &self.kind)
            .field("spwmap", &self.spwmap)
            .finish_non_exhaustive()
    }
}

/// A loaded table with the memory admitted for what it keeps.
struct HeldLookup {
    lookup: CalibrationTableLookup,
    _reservation: Reservation,
}

/// `Simulator::setapply`'s refusal: CASA logs the reason and raises.
fn raised(reason: impl std::fmt::Display) -> SimulatorError {
    SimulatorError::raised(format!(
        "{reason} Check inputs and try again. Error in Simulator::setapply."
    ))
}

fn not_yet(what: impl std::fmt::Display) -> SimulatorError {
    SimulatorError::not_yet_implemented(format!(
        "setapply {what} is not yet implemented in casa-rs (simulation Phase 3)"
    ))
}

/// The calibration type a table's subtype names: `G Jones` is `G`.
fn table_type(subtype: &str) -> String {
    subtype
        .strip_suffix(" Jones")
        .unwrap_or(subtype)
        .to_uppercase()
}

impl Simulator {
    /// `sm.setapply(...)`: corrupt by the calibration table `spec.table`
    /// at each later `corrupt`, its solutions taken at each integration
    /// observed by then.
    ///
    /// # Errors
    ///
    /// - The table cannot be read, its type is not the `type` asked for,
    ///   or `interp` is CASA's deprecated `aipslin`: raised, as CASA
    ///   raises.
    /// - A table of other than `G` or `D` solutions; `calwt`, a nonzero `t`
    ///   or `opacity`, or an interpolation other than `linear` or
    ///   `nearest`: not yet implemented. (A table without a solution, or
    ///   with a flagged one, for an antenna and integration is refused at
    ///   `corrupt`.)
    /// - No rows observed yet: refused, as `setgain` refuses.
    pub fn set_apply(&mut self, spec: &ApplySpec) -> SimulatorResult<()> {
        let interp = apply_interpolation(&spec.interp)?;
        if spec.calwt {
            return Err(not_yet("with calwt=True"));
        }
        if spec.interval_s != 0.0 {
            return Err(not_yet(format!("with t={}", spec.interval_s)));
        }
        if spec.opacity != 0.0 {
            return Err(not_yet(format!("with opacity={}", spec.opacity)));
        }
        if spec.table.is_empty() {
            return Err(raised("No calibration table given."));
        }
        let path = std::path::absolute(&spec.table).map_err(raised)?;
        let lookup = self.load_table(&path, interp)?;
        let table_kind = table_type(lookup.lookup.subtype());
        let kind = match spec.kind.to_uppercase() {
            requested if requested.is_empty() || requested == table_kind => table_kind,
            requested => {
                return Err(raised(format!(
                    "{} is a {} table, not {requested}.",
                    spec.table,
                    lookup.lookup.subtype()
                )));
            }
        };
        let kind = match kind.as_str() {
            "G" => JonesKind::Gain,
            "D" => JonesKind::Leakage,
            other => return Err(not_yet(format!("of a {other} table"))),
        };
        self.require_rows()?;
        tracing::info!(
            "Arranging to CORRUPT with: {} ({})",
            spec.table,
            lookup.lookup.subtype()
        );
        self.jones_terms.push(JonesSource::Applied(AppliedTable {
            path,
            lookup: Arc::new(lookup),
            kind,
            spwmap: spec.spwmap.clone(),
        }));
        Ok(())
    }

    /// Load the table at `path` to look its solutions up across its fields
    /// with `interp`, its memory admitted before it is read: what reading
    /// it holds, and what the lookup keeps, which it holds on to.
    fn load_table(
        &self,
        path: &std::path::Path,
        interp: ApplyInterpolationMode,
    ) -> SimulatorResult<HeldLookup> {
        let rows = CalibrationTableLookup::rows_on_disk(path).map_err(raised)?;
        let kept = CalibrationTableLookup::kept_bytes(rows);
        let resources = &self.options.resources;
        let mut reservation = admit(
            resources.host(),
            resources.policy(),
            &Demand {
                phase: LOOKUP_PHASE,
                memory: CalibrationTableLookup::loading_bytes(rows) + kept,
            },
        )
        .map_err(SimulatorError::admission)?;
        let lookup = CalibrationTableLookup::open_with(path, interp, FieldSelection::AnyField)
            .map_err(raised)?;
        reservation.retain(kept);
        Ok(HeldLookup {
            lookup,
            _reservation: reservation,
        })
    }

    /// The term `source` is at this `corrupt`: a simulated one as it was
    /// set, a table's evaluated over the scans observed so far. An
    /// evaluated term is admitted as a simulated one is, before it is
    /// formed, and keeps its share while a step applies it.
    pub(super) fn jones_term(&self, source: &JonesSource) -> SimulatorResult<Arc<HeldTerm>> {
        match source {
            JonesSource::Simulated(term) => Ok(Arc::clone(term)),
            JonesSource::Applied(table) => {
                let antennas = self.antennas.len();
                let (per_window, _) = self.integration_counts();
                let bytes = TermBytes::per_chunk(&per_window, antennas);
                let mut reservation = self.admit_term(bytes)?;
                let (chunks, values) =
                    self.applied_solutions(&table.lookup.lookup, &table.spwmap, &per_window)?;
                reservation.retain(bytes.kept);
                Ok(Arc::new(HeldTerm::new(
                    applied(table.kind, antennas, chunks, values),
                    reservation,
                )))
            }
        }
    }

    /// Each integration observed so far as a chunk, counted `per_window`,
    /// with each antenna's solution from `lookup` at its time, field and
    /// window (mapped by `spwmap`).
    fn applied_solutions(
        &self,
        lookup: &CalibrationTableLookup,
        spwmap: &[i64],
        per_window: &[usize],
    ) -> SimulatorResult<(Chunks, Vec<[Complex32; 2]>)> {
        let table_window = |window: usize| {
            spwmap
                .get(window)
                .and_then(|&mapped| usize::try_from(mapped).ok())
                .unwrap_or(window)
        };
        let mut chunks = Chunks::with_capacity(per_window);
        let mut values = Vec::with_capacity(per_window.iter().sum::<usize>() * self.antennas.len());
        for (index, scan) in self.scans.iter().enumerate() {
            let window = table_window(scan.spectral_window_id);
            for time_s in self.integration_times_s(index) {
                chunks.push(scan.spectral_window_id, time_s, scan.field_id);
                for antenna in 0..self.antennas.len() {
                    let solution = lookup
                        .solution(scan.field_id as i32, window as i32, antenna as i32, time_s)
                        .map_err(raised)?
                        .ok_or_else(|| {
                            not_yet(format!(
                                "of a table without a solution for antenna {antenna} in spectral \
                                 window {window} (CASA leaves the window uncorrupted)"
                            ))
                        })?;
                    if solution.channels != 1 {
                        return Err(not_yet("of a table of solutions per channel"));
                    }
                    if solution.flags.iter().any(|&flag| flag) {
                        return Err(not_yet("of a table with flagged solutions"));
                    }
                    if solution.receptors != 2 {
                        return Err(not_yet(format!(
                            "of {}-receptor solutions",
                            solution.receptors
                        )));
                    }
                    values.push([solution.value(0, 0), solution.value(1, 0)]);
                }
            }
        }
        Ok((chunks.sorted(), values))
    }
}

/// `interp`'s time interpolation (`linear[,…]` or `nearest[,…]`; empty is
/// `linear`), refusing CASA's deprecated `aipslin` as CASA does.
fn apply_interpolation(interp: &str) -> SimulatorResult<ApplyInterpolationMode> {
    if interp.contains("aips") {
        return Err(raised(
            "The (peculiar) 'aipslin' interpolation type was deprecated in CASA v3.4; use \
             'linear'.",
        ));
    }
    match interp.split(',').next().unwrap_or("").trim() {
        "" | "linear" => Ok(ApplyInterpolationMode::Linear),
        "nearest" => Ok(ApplyInterpolationMode::Nearest),
        other => Err(not_yet(format!("with interp='{other}'"))),
    }
}
