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

use casa_calibration::{
    ApplyInterpolationMode, CalibrationTableLookup, FieldSelection, FrequencyInterpolation,
};
use casa_imaging_runtime::{Demand, Reservation, admit};
use num_complex::Complex32;

use super::Simulator;
use super::error::{SimulatorError, SimulatorResult};
use crate::corrupt::jones::{Chunks, HeldTerm, JonesKind, TermBytes, applied, applied_frequency};

/// The phase loading a `setapply` table is admitted as: the table as it is
/// read, and what the loaded lookup keeps.
const LOOKUP_PHASE: &str = "loading a setapply calibration table";

/// The arguments of `sm.setapply`.
#[derive(Debug, Clone, PartialEq)]
pub struct ApplySpec {
    /// `table`: the calibration table to corrupt by.
    pub table: String,
    /// `type`: its calibration type (`G`, `D` or scalar `T`); empty takes the table's.
    pub kind: String,
    /// `t`: CASA records this interval, but G/D/T table application still
    /// evaluates the requested interpolation at each data time.
    pub interval_s: f64,
    /// `interp`: the time interpolation (`linear` or `nearest`), optionally
    /// followed by `,` and a frequency interpolation; empty is `linear`.
    pub interp: String,
    /// `calwt`: accepted as in CASA; corruption explicitly disables weight
    /// calibration, so both settings preserve WEIGHT and SIGMA.
    pub calwt: bool,
    /// `spwmap`: each data window's table window; −1, or a window past
    /// its end, is the data window itself.
    pub spwmap: Vec<i64>,
    /// `opacity`: used by CASA's TOPAC term; ignored for the supported G/D/T
    /// tables, as their application ignores it in CASA.
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

/// A loaded table's mathematical operator. A scalar T parameter expands
/// to the same gain on both receptors; it shares the gain kernel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppliedKind {
    Gain,
    Leakage,
    Scalar,
}

impl AppliedKind {
    fn operator(self) -> JonesKind {
        match self {
            Self::Gain | Self::Scalar => JonesKind::Gain,
            Self::Leakage => JonesKind::Leakage,
        }
    }

    fn receptors(self) -> usize {
        if self == Self::Scalar { 1 } else { 2 }
    }
}

/// A table `setapply` set: the table as it was loaded, and how to apply it.
#[derive(Clone)]
pub(crate) struct AppliedTable {
    path: PathBuf,
    lookup: Arc<HeldLookup>,
    kind: AppliedKind,
    frequency: Option<FrequencyInterpolation>,
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
    frequencies: Vec<Vec<f64>>,
    max_cell_values: usize,
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
        "setapply {what} is not yet implemented in casa-rs (#1053)"
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
    ///   raises. A table whose parameter count disagrees with its type
    ///   raises at `corrupt`, as CASA does.
    /// - Other than `G`, `D` or scalar `T` solutions, or interpolation
    ///   other than `linear` or `nearest`: not yet implemented.
    /// - No rows observed yet: refused, as `setgain` refuses.
    ///
    /// Flagged G/T parameters and missing G/T antennas use unit gains,
    /// as in CASA. A scalar T parameter applies equally to all correlations.
    /// For D tables an invalid receptor or missing antenna uses neutral leakage (both coefficients zero), correcting CASA's unit
    /// leakage substitution. Input data flags, weights and sigma persist.
    pub fn set_apply(&mut self, spec: &ApplySpec) -> SimulatorResult<()> {
        let interp = apply_interpolation(&spec.interp)?;
        // VisCal::corrupt turns weight calibration off. G/D/T table lookup
        // uses each row's time regardless of t, and only TOPAC reads opacity.
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
        let frequency = matches!(kind.as_str(), "B" | "TF" | "DF")
            .then(|| frequency_interpolation(&spec.interp))
            .transpose()?;
        let kind = match kind.as_str() {
            "G" | "B" => AppliedKind::Gain,
            "D" | "DF" => AppliedKind::Leakage,
            "T" | "TF" => AppliedKind::Scalar,
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
            frequency,
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
        let metadata_bytes =
            CalibrationTableLookup::metadata_bytes_on_disk(path).map_err(raised)?;
        let resources = &self.options.resources;
        // Admit headers before inspecting shapes; no numerical array is
        // materialized by layout_on_disk. Only scalar counts survive it.
        let metadata = admit(
            resources.host(),
            resources.policy(),
            &Demand {
                phase: LOOKUP_PHASE,
                memory: metadata_bytes,
            },
        )
        .map_err(SimulatorError::admission)?;
        let layout = CalibrationTableLookup::layout_on_disk(path).map_err(raised)?;
        drop(metadata);
        let kept = layout.kept_bytes();
        let mut reservation = admit(
            resources.host(),
            resources.policy(),
            &Demand {
                phase: LOOKUP_PHASE,
                memory: layout.loading_bytes().saturating_add(kept),
            },
        )
        .map_err(SimulatorError::admission)?;
        let lookup = CalibrationTableLookup::open_with(path, interp, FieldSelection::AnyField)
            .map_err(raised)?;
        let frequencies = if layout.frequency_values > 0 {
            CalibrationTableLookup::frequency_grids_on_disk(path).map_err(raised)?
        } else {
            Vec::new()
        };
        reservation.retain(kept);
        Ok(HeldLookup {
            lookup,
            frequencies,
            max_cell_values: layout.max_cell_values,
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
                let channels = self
                    .spectral_windows
                    .iter()
                    .map(|window| window.channel_count)
                    .collect::<Vec<_>>();
                let bytes = if table.frequency.is_some() {
                    TermBytes::frequency(
                        &per_window,
                        &channels,
                        antennas,
                        table.lookup.max_cell_values,
                    )
                } else {
                    TermBytes::per_chunk(&per_window, antennas)
                };
                let mut reservation = self.admit_term(bytes)?;
                let (chunks, values, offsets) = self.applied_solutions(table, &per_window)?;
                reservation.retain(bytes.kept);
                let term = if table.frequency.is_some() {
                    applied_frequency(table.kind.operator(), antennas, chunks, values, offsets)
                } else {
                    applied(table.kind.operator(), antennas, chunks, values)
                };
                Ok(Arc::new(HeldTerm::new(term, reservation)))
            }
        }
    }

    /// Each integration observed so far as a chunk, counted `per_window`,
    /// with each antenna's solution from `lookup` at its time, field and
    /// window (mapped by `spwmap`).
    fn applied_solutions(
        &self,
        table: &AppliedTable,
        per_window: &[usize],
    ) -> SimulatorResult<(Chunks, Vec<[Complex32; 2]>, Vec<usize>)> {
        let kind = table.kind;
        let lookup = &table.lookup.lookup;
        let spwmap = &table.spwmap;
        let table_window = |window: usize| {
            spwmap
                .get(window)
                .and_then(|&mapped| usize::try_from(mapped).ok())
                .unwrap_or(window)
        };
        // JonesDiag::setMatByOk substitutes unit gains per receptor.
        // JonesGenLin instead substitutes one for BOTH leakage values;
        // correct that data-corrupting defect with the neutral zero pair.
        let neutral = Complex32::new(
            if kind.operator() == JonesKind::Gain {
                1.0
            } else {
                0.0
            },
            0.0,
        );
        let mut chunks = Chunks::with_capacity(per_window);
        let value_count = per_window
            .iter()
            .enumerate()
            .map(|(window, count)| {
                count
                    * self.antennas.len()
                    * if table.frequency.is_some() {
                        self.spectral_windows[window].channel_count
                    } else {
                        1
                    }
            })
            .sum();
        let mut values = Vec::with_capacity(value_count);
        let mut offsets = if table.frequency.is_some() {
            Vec::with_capacity(per_window.iter().sum::<usize>() + 1)
        } else {
            Vec::new()
        };
        for (index, scan) in self.scans.iter().enumerate() {
            let window = table_window(scan.spectral_window_id);
            let output_hz = if table.frequency.is_some() {
                self.spectral_windows[scan.spectral_window_id].channel_frequencies_hz()
            } else {
                Vec::new()
            };
            let channels = if table.frequency.is_some() {
                output_hz.len()
            } else {
                1
            };
            for time_s in self.integration_times_s(index) {
                if table.frequency.is_some() {
                    offsets.push(values.len());
                }
                chunks.push(scan.spectral_window_id, time_s, scan.field_id);
                for antenna in 0..self.antennas.len() {
                    let Some(solution) = lookup
                        .solution(scan.field_id as i32, window as i32, antenna as i32, time_s)
                        .map_err(raised)?
                    else {
                        values.extend(std::iter::repeat_n([neutral; 2], channels));
                        continue;
                    };
                    if solution.channels != 1 && table.frequency.is_none() {
                        return Err(not_yet("of a table of solutions per channel"));
                    }
                    if solution.receptors != kind.receptors() {
                        return Err(raised(format!(
                            "Calibration table has {} parameters per channel; {:?} requires {}",
                            solution.receptors,
                            kind,
                            kind.receptors()
                        )));
                    }
                    let solution = if let Some(mode) = table.frequency {
                        let input_hz = table
                            .lookup
                            .frequencies
                            .get(window)
                            .map(Vec::as_slice)
                            .unwrap_or(&[]);
                        solution
                            .resample_frequency(input_hz, &output_hz, mode)
                            .map_err(raised)?
                    } else {
                        solution
                    };
                    for channel in 0..channels {
                        let invalid_leakage = kind == AppliedKind::Leakage
                            && (0..solution.receptors)
                                .any(|receptor| solution.flags[receptor * channels + channel]);
                        values.push(std::array::from_fn(|receptor| {
                            let receptor = if kind == AppliedKind::Scalar {
                                0
                            } else {
                                receptor
                            };
                            if invalid_leakage || solution.flags[receptor * channels + channel] {
                                neutral
                            } else {
                                solution.value(receptor, channel)
                            }
                        }));
                    }
                }
            }
        }
        if table.frequency.is_some() {
            offsets.push(values.len());
        }
        Ok((chunks.sorted(), values, offsets))
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

/// CASA defaults frequency interpolation to linear for frequency-dependent parameters.
fn frequency_interpolation(interp: &str) -> SimulatorResult<FrequencyInterpolation> {
    let mode = interp.split(',').nth(1).unwrap_or("linear");
    match mode {
        "nearest" => Ok(FrequencyInterpolation::Nearest),
        "" | "linear" => Ok(FrequencyInterpolation::Linear),
        other => Err(not_yet(format!("with frequency interp='{other}'"))),
    }
}
