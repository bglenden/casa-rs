// SPDX-License-Identifier: LGPL-3.0-or-later
//! Pulse calibration aligned to a reference fringe solution.

mod fringe;
mod pulse;
mod selection;
mod table;

use std::collections::BTreeSet;
use std::f64::consts::{PI, TAU};
use std::path::PathBuf;

use casa_ms::MeasurementSet;
use thiserror::Error;

use self::fringe::FringeSolution;
use self::pulse::PulseSeries;
use self::selection::Selection;

/// Request to execute pulse-calibration alignment against a fringe solution.
pub struct PccorRequest {
    /// Input MeasurementSet.
    pub vis: PathBuf,
    /// Output calibration table.
    pub output: PathBuf,
    /// Reference antenna selector.
    pub refant: String,
    /// Time range selector.
    pub timerange: String,
    /// Scan selector.
    pub scan: String,
    /// Spectral window selector.
    pub spw: String,
    /// Antenna selector.
    pub antenna: String,
    /// Apply cable-cal correction.
    pub cable: bool,
    /// Optional fringe-fit calibration table.
    pub fringe_table: Option<PathBuf>,
}

/// Errors returned by `run_pccor`.
#[derive(Debug, Error)]
pub enum PccorError {
    /// Invalid selection.
    #[error("{0}")]
    Selection(String),
    /// Invalid or missing fringe-fit table.
    #[error("{0}")]
    FringeTable(String),
    /// Index failure.
    #[error("{0}")]
    Index(String),
    /// Execution failure.
    #[error("{0}")]
    Execution(String),
    /// MeasurementSet error.
    #[error(transparent)]
    MeasurementSet(#[from] casa_ms::MsError),
    /// Table error.
    #[error(transparent)]
    Table(#[from] casa_tables::TableError),
}

/// Per-window aligned pulse-calibration report for one antenna.
pub struct PccorWindowReport {
    /// Spectral window id.
    pub window: i32,
    /// Sample timestamps.
    pub time: Vec<f64>,
    /// Scan numbers (`NaN` when unavailable).
    pub scan: Vec<f64>,
    /// Field ids (`NaN` when unavailable).
    pub field_id: Vec<f64>,
    /// Aligned phase values shaped `[2, samples]` row-major by hand.
    pub phase: Vec<f64>,
    /// Aligned delay values shaped `[2, samples]` row-major by hand.
    pub delay: Vec<f64>,
    /// Wrapped fringe phase `[hand 0, hand 1]`.
    pub ff_phase: Vec<f64>,
    /// Fringe delay `[hand 0, hand 1]`.
    pub ff_delay: Vec<f64>,
    /// Mean pulse-cal phase tones per hand.
    pub pc_mean_phase: [Vec<f64>; 2],
    /// Mean pulse-cal delay `[hand 0, hand 1]`.
    pub pc_mean_delay: Vec<f64>,
    /// Reference antenna mean pulse-cal phase tones per hand.
    pub pc_refant_phase: [Vec<f64>; 2],
    /// Reference antenna mean pulse-cal delay `[hand 0, hand 1]`.
    pub pc_refant_delay: Vec<f64>,
}

/// Per-antenna pulse-calibration report.
pub struct PccorAntennaReport {
    /// Antenna name.
    pub name: String,
    /// Per-window reports.
    pub windows: Vec<PccorWindowReport>,
}

/// Output of `run_pccor`.
pub struct PccorReport {
    /// Antenna reports in table-return order.
    pub antennas: Vec<PccorAntennaReport>,
    /// Number of rows written to `output`.
    pub rows_written: usize,
}

/// Execute `pccor` alignment and write the calibration table.
pub fn run_pccor(request: &PccorRequest) -> Result<PccorReport, PccorError> {
    let open = crate::timing::Phase::start("pccor_select_read");
    let ms = MeasurementSet::open(&request.vis)?;
    let selection = Selection::read(&ms, request)?;
    let pulses = pulse::read(&request.vis, &selection)?;
    drop(open);

    let fit = crate::timing::Phase::start("pccor_reference_fringe");
    let fringes = fringe::read_or_solve(&ms, request, &selection)?;
    drop(fit);
    if selection.windows.is_empty() {
        return Err(PccorError::Index("list index out of range".into()));
    }

    let align = crate::timing::Phase::start("pccor_align");
    let mut antennas = Vec::with_capacity(selection.antennas.len());
    let mut seen_antennas = BTreeSet::new();
    let mut table_rows = Vec::new();
    for &antenna in &selection.antennas {
        let first_antenna = seen_antennas.insert(antenna);
        let mut windows = Vec::with_capacity(selection.windows.len());
        let mut seen_windows = BTreeSet::new();
        for &window in &selection.windows {
            let pair = (antenna, window);
            if request.cable && pulses[&pair].missing_single_tone {
                return Err(PccorError::Selection(
                    "non-broadcastable output operand with shape (2,1) doesn't match the broadcast shape (2,0)".into(),
                ));
            }
            let options = Alignment {
                pulse: &pulses[&pair],
                reference: &pulses[&(selection.reference, window)],
                fringe: &fringes[&pair],
                cable: request.cable,
            };
            let aligned = options.align(&selection);
            table_rows.extend(aligned.iter().filter_map(|row| {
                row.scan.map(|scan| table::Row {
                    antenna,
                    window,
                    time: row.time,
                    field: scan.field,
                    scan: scan.number,
                    phase: row.phase,
                    delay: row.delay,
                })
            }));
            if first_antenna && seen_windows.insert(window) {
                windows.push(options.report(window, aligned));
            }
        }
        if first_antenna {
            let name = selection.antenna_names[antenna as usize].clone();
            antennas.push(PccorAntennaReport { name, windows });
        }
    }
    drop(align);
    let publish = crate::timing::Phase::start("pccor_write");
    let rows_written = table_rows.len();
    table::write(&ms, &request.output, selection.reference, table_rows)?;
    drop(publish);
    Ok(PccorReport {
        antennas,
        rows_written,
    })
}

struct Alignment<'a> {
    pulse: &'a PulseSeries,
    reference: &'a PulseSeries,
    fringe: &'a FringeSolution,
    cable: bool,
}

struct Aligned<'a> {
    time: f64,
    scan: Option<&'a selection::Scan>,
    phase: [f64; 2],
    delay: [f64; 2],
}

impl Alignment<'_> {
    fn align<'a>(&self, selection: &'a Selection) -> Vec<Aligned<'a>> {
        let fringe = self.fringe;
        let fallback = match self.pulse.samples.as_slice() {
            [] => true,
            [only] => only.delay[0] == 0.,
            _ => false,
        };
        if fallback {
            return vec![Aligned {
                time: fringe.time,
                scan: selection.scan_at(fringe.time),
                phase: fringe.phase.map(wrap_phase),
                delay: fringe.delay,
            }];
        }
        self.pulse
            .samples
            .iter()
            .map(|sample| {
                let cable = if self.cable { sample.cable } else { 0. };
                let phase = [0, 1].map(|hand| {
                    wrap_phase(
                        fringe.phase[hand] + sample.phase[hand] - self.pulse.mean_phase[hand][0],
                    )
                });
                let delay = [0, 1].map(|hand| {
                    sample.delay[hand] + fringe.delay[hand]
                        - (self.pulse.mean_delay[hand] - self.reference.mean_delay[hand])
                        + cable
                });
                Aligned {
                    time: sample.time,
                    scan: selection.scan_at(sample.time),
                    phase,
                    delay,
                }
            })
            .collect()
    }

    fn report(&self, window: i32, aligned: Vec<Aligned<'_>>) -> PccorWindowReport {
        PccorWindowReport {
            window,
            time: aligned.iter().map(|row| row.time).collect(),
            scan: aligned
                .iter()
                .map(|row| row.scan.map_or(f64::NAN, |scan| f64::from(scan.number)))
                .collect(),
            field_id: aligned
                .iter()
                .map(|row| row.scan.map_or(f64::NAN, |scan| f64::from(scan.field)))
                .collect(),
            phase: aligned.iter().flat_map(|row| row.phase).collect(),
            delay: aligned.iter().flat_map(|row| row.delay).collect(),
            ff_phase: self.fringe.phase.map(wrap_phase).to_vec(),
            ff_delay: self.fringe.delay.to_vec(),
            pc_mean_phase: self.pulse.mean_phase.clone(),
            pc_mean_delay: self.pulse.mean_delay.to_vec(),
            pc_refant_phase: self.reference.mean_phase.clone(),
            pc_refant_delay: self.reference.mean_delay.to_vec(),
        }
    }
}

fn wrap_phase(phase: f64) -> f64 {
    (phase + PI).rem_euclid(TAU) - PI
}
