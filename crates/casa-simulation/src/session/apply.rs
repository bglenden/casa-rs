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
//! as one series in time (measured on CASA
//! 6.7.6, #893 review).
//!
//! Loading a table is admitted against the session's resource policy
//! before it is read, and the loaded table keeps its share for as long as
//! the term does; each `corrupt` admits the term it evaluates, as
//! `setgain` admits one it simulates.

use std::path::PathBuf;
use std::sync::Arc;

use casa_calibration::{
    CalibrationTableLookup, FrequencyInterpolation, TimeInterpolation, TimeInterpolationOptions,
};
use casa_imaging_runtime::{Demand, Reservation, admit};
use num_complex::Complex32;

use super::Simulator;
use super::error::{SimulatorError, SimulatorResult};
use crate::corrupt::jones::{Chunks, HeldTerm, JonesKind, TermBytes, applied, applied_frequency};
#[path = "apply_physical.rs"]
mod physical;

/// The phase loading a `setapply` table is admitted as: the table as it is
/// read, and what the loaded lookup keeps.
const LOOKUP_PHASE: &str = "loading a setapply calibration table";

/// The arguments of `sm.setapply`.
#[derive(Debug, Clone, PartialEq)]
pub struct ApplySpec {
    /// `table`: the calibration table to corrupt by.
    pub table: String,
    /// `type`: its calibration type (`G`, `D`, scalar `T`, `B`, `Tf`, `Df`); empty takes the table's.
    pub kind: String,
    /// `t`: CASA records this interval, but G/D/T table application still
    /// evaluates the requested interpolation at each data time.
    pub interval_s: f64,
    /// `interp`: time interpolation (`nearest`, `linear`, `cubic`, `spline`), optionally
    /// followed by `,` and a frequency interpolation; empty is `linear`.
    pub interp: String,
    /// `calwt`: accepted as in CASA; corruption explicitly disables weight
    /// calibration, so both settings preserve WEIGHT and SIGMA.
    pub calwt: bool,
    /// `spwmap`: empty or [-1] preserves data-window ids. One nonnegative
    /// id broadcasts to every data window; a longer list maps a prefix.
    /// [-999] broadcasts the first window with solutions. Other negative
    /// or out-of-range ids raise.
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

impl JonesSource {
    /// Metadata-only coverage, before any applied solutions are formed.
    pub(super) fn covers(&self, window: usize) -> bool {
        match self {
            Self::Simulated(term) => term.covers(window),
            Self::Applied(table) => table.calibration_window(window).is_some(),
        }
    }
}

/// A loaded table's mathematical operator. A scalar T parameter expands
/// to the same gain on both receptors; it shares the gain kernel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppliedKind {
    Gain,
    Leakage,
    Scalar,
    Delay,
    AntennaPosition,
    Opacity,
    GainCurve,
    SystemTemperature,
}

impl AppliedKind {
    fn operator(self) -> JonesKind {
        match self {
            Self::Gain | Self::Scalar | Self::Delay | Self::AntennaPosition | Self::Opacity => {
                JonesKind::Gain
            }
            Self::Leakage => JonesKind::Leakage,
            Self::GainCurve | Self::SystemTemperature => JonesKind::SkyGain,
        }
    }

    fn receptors(self) -> usize {
        match self {
            Self::Scalar | Self::Opacity => 1,
            Self::AntennaPosition => 3,
            Self::GainCurve => 8,
            _ => 2,
        }
    }
}

/// A table `setapply` set: the table as it was loaded, and how to apply it.
#[derive(Clone)]
pub(crate) struct AppliedTable {
    path: PathBuf,
    lookup: Arc<HeldLookup>,
    kind: AppliedKind,
    frequency: Option<FrequencyInterpolation>,
    spwmap: Vec<usize>,
    phase_delay: bool,
}

impl AppliedTable {
    fn channel_dependent(&self) -> bool {
        self.frequency.is_some() || self.kind == AppliedKind::Delay
    }

    fn calibration_window(&self, window: usize) -> Option<usize> {
        let mapped = *self.spwmap.get(window)?;
        self.lookup
            .lookup
            .has_spectral_window(mapped as i32)
            .then_some(mapped)
    }
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

/// Evaluated solutions; channel offsets are present only for spectral tables.
struct AppliedSolutions {
    chunks: Chunks,
    values: Vec<[Complex32; 2]>,
    offsets: Vec<usize>,
}

/// A loaded table with the memory admitted for what it keeps.
struct HeldLookup {
    lookup: CalibrationTableLookup,
    frequencies: Vec<Vec<f64>>,
    max_cell_values: usize,
    windows: usize,
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
    /// - Other than `G`, `D`, scalar `T`, `B`, `Tf` and `Df`, or phase-delay
    ///   and per-observation/scan interpolation modifiers: not yet implemented.
    /// - Time interpolation supports nearest, linear, cubic and natural spline.
    ///   Cubic interior queries require four times; exact and endpoint queries
    ///   accept shorter axes. Spline corrects CASA's setup crash (#1053).
    /// - Frequency interpolation supports nearest, linear, cubic and spline,
    ///   their flag-retaining forms, and CASA's default linear fallback.
    ///   Cubic interior interpolation with fewer than four valid input
    ///   channels raises at `corrupt`, as CASA does.
    /// - No rows observed yet: refused, as `setgain` refuses.
    ///
    /// The term snapshots its spectral-window map. A window added later,
    /// or mapped to a calibration window with no solutions, skips the whole
    /// corruption step, including noise and corrected-data updates, as in CASA.
    ///
    /// Flagged G/T parameters and missing G/T antennas use unit gains,
    /// as in CASA. A scalar T parameter applies equally to all correlations.
    /// For D tables an invalid receptor or missing antenna uses neutral leakage (both coefficients zero), correcting CASA's unit
    /// leakage substitution. Input data flags, weights and sigma persist.
    pub fn set_apply(&mut self, spec: &ApplySpec) -> SimulatorResult<()> {
        let mut interp = apply_interpolation(&spec.interp)?;
        interp.observation_count = self.observations.len();
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
        let frequency = matches!(kind.as_str(), "B" | "TF" | "DF" | "B TSYS")
            .then(|| frequency_interpolation(&spec.interp));
        let kind = match kind.as_str() {
            "G" | "B" => AppliedKind::Gain,
            "D" | "DF" => AppliedKind::Leakage,
            "T" | "TF" => AppliedKind::Scalar,
            "K" => AppliedKind::Delay,
            "KANTPOS" => AppliedKind::AntennaPosition,
            "TOPAC" => AppliedKind::Opacity,
            "EGAINCURVE" => AppliedKind::GainCurve,
            "B TSYS" => AppliedKind::SystemTemperature,
            other => return Err(not_yet(format!("of a {other} table"))),
        };
        let spwmap = mapped_windows(&spec.spwmap, &lookup, self.spectral_windows.len())?;
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
            spwmap,
            phase_delay: interp.phase_delay,
        }));
        Ok(())
    }

    /// Load the table at `path` to look its solutions up across its fields
    /// with `interp`, its memory admitted before it is read: what reading
    /// it holds, and what the lookup keeps, which it holds on to.
    fn load_table(
        &self,
        path: &std::path::Path,
        interp: TimeInterpolationOptions,
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
        let layout = CalibrationTableLookup::layout_for_simulation(path, interp).map_err(raised)?;
        drop(metadata);
        let kept = layout
            .kept_bytes()
            .saturating_add(layout.time_kept_bytes(interp.mode))
            .saturating_add(layout.context_kept_bytes(interp));
        let mut reservation = admit(
            resources.host(),
            resources.policy(),
            &Demand {
                phase: LOOKUP_PHASE,
                memory: layout
                    .loading_bytes()
                    .saturating_add(layout.time_loading_bytes(interp.mode))
                    .saturating_add(layout.context_loading_bytes(interp))
                    .saturating_add(kept),
            },
        )
        .map_err(SimulatorError::admission)?;
        let lookup = CalibrationTableLookup::open_for_simulation(path, interp).map_err(raised)?;
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
            windows: layout.frequency_windows,
            _reservation: reservation,
        })
    }

    /// The term `source` is at this `corrupt`: a simulated one as it was
    /// set, a table's evaluated over the scans observed so far. An
    /// evaluated term is admitted as a simulated one is, before it is
    /// formed, and keeps its share while a step applies it.
    pub(super) fn jones_term(
        &self,
        source: &JonesSource,
        covered: &impl Fn(usize) -> bool,
    ) -> SimulatorResult<Arc<HeldTerm>> {
        match source {
            JonesSource::Simulated(term) => Ok(Arc::clone(term)),
            JonesSource::Applied(table) => {
                let antennas = self.antennas.len();
                let (mut per_window, _) = self.integration_counts();
                // Definitions without observed rows still own a window slot.
                per_window.resize(self.spectral_windows.len(), 0);
                for (window, count) in per_window.iter_mut().enumerate() {
                    if !covered(window) {
                        *count = 0;
                    }
                }
                let channels = self
                    .spectral_windows
                    .iter()
                    .enumerate()
                    .map(|(index, window)| {
                        if per_window[index] == 0 {
                            0
                        } else {
                            window.channel_count
                        }
                    })
                    .collect::<Vec<_>>();
                let mut bytes = if table.channel_dependent() {
                    TermBytes::frequency(
                        &per_window,
                        &channels,
                        antennas,
                        table.lookup.max_cell_values,
                    )
                } else {
                    TermBytes::per_chunk(&per_window, antennas)
                };
                if matches!(table.kind, AppliedKind::Opacity | AppliedKind::GainCurve) {
                    bytes.forming += (antennas * size_of::<f64>()) as u64;
                }
                let mut reservation = self.admit_term(bytes)?;
                let evaluated = self.applied_solutions(table, &per_window)?;
                reservation.retain(bytes.kept);
                let term = if table.channel_dependent() {
                    applied_frequency(
                        table.kind.operator(),
                        antennas,
                        evaluated.chunks,
                        evaluated.values,
                        evaluated.offsets,
                    )
                } else {
                    applied(
                        table.kind.operator(),
                        antennas,
                        evaluated.chunks,
                        evaluated.values,
                    )
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
    ) -> SimulatorResult<AppliedSolutions> {
        let kind = table.kind;
        let lookup = &table.lookup.lookup;
        // JonesDiag::setMatByOk substitutes unit gains per receptor.
        // JonesGenLin instead substitutes one for BOTH leakage values;
        // correct that data-corrupting defect with the neutral zero pair.
        let neutral = Complex32::new(
            if kind.operator() != JonesKind::Leakage {
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
                    * if table.channel_dependent() {
                        self.spectral_windows[window].channel_count
                    } else {
                        1
                    }
            })
            .sum();
        let mut values = Vec::with_capacity(value_count);
        let mut offsets = if table.channel_dependent() {
            Vec::with_capacity(per_window.iter().sum::<usize>() + 1)
        } else {
            Vec::new()
        };
        for (index, scan) in self.scans.iter().enumerate() {
            if per_window[scan.spectral_window_id] == 0 {
                continue;
            }
            let Some(window) = table.calibration_window(scan.spectral_window_id) else {
                continue;
            };
            let output_hz = if table.channel_dependent() {
                self.spectral_windows[scan.spectral_window_id].channel_frequencies_hz()
            } else {
                Vec::new()
            };
            let channels = if table.channel_dependent() {
                output_hz.len()
            } else {
                1
            };
            let phase_frequency = (table.phase_delay && table.frequency.is_none()).then(|| {
                let data = &self.spectral_windows[scan.spectral_window_id];
                data.start_frequency_hz
                    + (data.channel_count - 1) as f64 * data.channel_width_hz / 2.
            });
            for time_s in self.integration_times_s(index) {
                if table.channel_dependent() {
                    offsets.push(values.len());
                }
                chunks.push(scan.spectral_window_id, time_s, scan.field_id);
                let geometry = physical::Geometry::new(self, kind, scan.field_id, time_s)?;
                for antenna in 0..self.antennas.len() {
                    let Some(solution) = lookup
                        .simulation_solution(
                            scan.observation_id,
                            scan.scan_number,
                            window as i32,
                            antenna as i32,
                            time_s,
                            phase_frequency,
                        )
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
                    let reference = table
                        .lookup
                        .frequencies
                        .get(window)
                        .and_then(|axis| axis.get(axis.len() / 2))
                        .copied()
                        .unwrap_or(0.);
                    for channel in 0..channels {
                        if let Some(pair) = geometry.values(
                            kind,
                            &solution,
                            antenna,
                            (channel, output_hz.get(channel).copied().unwrap_or(0.)),
                            reference,
                        ) {
                            values.push(pair);
                            continue;
                        }
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
        if table.channel_dependent() {
            offsets.push(values.len());
        }
        Ok(AppliedSolutions {
            chunks: chunks.sorted(),
            values,
            offsets,
        })
    }
}

/// The simulator's time interpolation, refusing deprecated and unknown modes
/// as CASA does; unsupported modifiers remain explicit refusals (#1053).
fn apply_interpolation(interp: &str) -> SimulatorResult<TimeInterpolationOptions> {
    if interp.contains("aips") {
        return Err(raised(
            "The (peculiar) 'aipslin' interpolation type was deprecated in CASA v3.4; use \
             'linear'.",
        ));
    }
    let requested = interp.split(',').next().unwrap_or("").trim();
    let mode = if requested.contains("nearest") {
        TimeInterpolation::Nearest
    } else if requested.is_empty() || requested.contains("linear") {
        TimeInterpolation::Linear
    } else if requested.contains("cubic") {
        TimeInterpolation::Cubic
    } else if requested.contains("spline") {
        TimeInterpolation::Spline
    } else {
        return Err(raised(format!(
            "Unrecognized time interpolation type '{requested}'"
        )));
    };
    Ok(TimeInterpolationOptions {
        mode,
        phase_delay: requested.contains("PD"),
        per_observation: requested.contains("perobs"),
        per_scan: requested.contains("perscan"),
        observation_count: 1,
    })
}

/// CASA defaults frequency interpolation to linear for frequency-dependent parameters.
fn frequency_interpolation(interp: &str) -> FrequencyInterpolation {
    let mode = interp.split(',').nth(1).unwrap_or("linear");
    let retain_flags = mode.contains("flag");
    if mode.contains("nearest") {
        if retain_flags {
            FrequencyInterpolation::NearestFlag
        } else {
            FrequencyInterpolation::Nearest
        }
    } else if mode.contains("cubic") {
        if retain_flags {
            FrequencyInterpolation::CubicFlag
        } else {
            FrequencyInterpolation::Cubic
        }
    } else if mode.contains("spline") {
        if retain_flags {
            FrequencyInterpolation::SplineFlag
        } else {
            FrequencyInterpolation::Spline
        }
    } else if retain_flags {
        FrequencyInterpolation::LinearFlag
    } else {
        FrequencyInterpolation::Linear
    }
}

/// CTPatchedInterp::setSpwMap: identity, single-window broadcast, explicit
/// prefix, or the first available solution window (-999).
fn mapped_windows(
    spec: &[i64],
    lookup: &HeldLookup,
    windows: usize,
) -> SimulatorResult<Vec<usize>> {
    if spec.is_empty() || spec == [-1] {
        return Ok((0..windows).collect());
    }
    if spec.len() > windows {
        return Err(raised(format!(
            "Specified spwmap has more elements ({}) than the number of spectral windows in the MS ({windows}).",
            spec.len()
        )));
    }
    if spec[0] == -999 {
        let first = lookup.lookup.first_spectral_window().ok_or_else(|| {
            raised("No calibration spectral window is available for automatic fanout")
        })?;
        return Ok(vec![first as usize; windows]);
    }
    if spec.iter().any(|&window| window < 0) {
        return Err(raised("Please specify positive indices in spwmap."));
    }
    if spec
        .iter()
        .any(|&window| window as u64 >= lookup.windows as u64)
    {
        return Err(raised(format!(
            "Please specify spw indices <= maximum available ({})",
            lookup.windows.saturating_sub(1)
        )));
    }
    Ok((0..windows)
        .map(|window| {
            spec.get(if spec.len() == 1 { 0 } else { window })
                .map(|&window| window as usize)
                .unwrap_or(window)
        })
        .collect())
}
