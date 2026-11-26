// SPDX-License-Identifier: LGPL-3.0-or-later
//! Pulse calibration aligned to a reference fringe solution.

mod fringe;
mod pulse;
mod selection;
mod table;

use std::collections::BTreeSet;
use std::f64::consts::{PI, TAU};
use std::path::PathBuf;

use casa_contracts::{CasaFloat, CasaValue, FloatDtype};
use casa_ms::MeasurementSet;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};
use serde::Serialize;
use thiserror::Error;

use self::fringe::FringeSolution;
use self::pulse::PulseSeries;
use self::selection::Selection;

struct PccorRequest {
    vis: PathBuf,
    output: PathBuf,
    refant: String,
    timerange: String,
    scan: String,
    spw: String,
    antenna: String,
    cable: bool,
    fringe_table: Option<PathBuf>,
}

#[derive(Debug, Error)]
enum PccorError {
    #[error("{0}")]
    Selection(String),
    #[error("{0}")]
    FringeTable(String),
    #[error("{0}")]
    Index(String),
    #[error("{0}")]
    Execution(String),
    #[error(transparent)]
    MeasurementSet(#[from] casa_ms::MsError),
    #[error(transparent)]
    Table(#[from] casa_tables::TableError),
}

#[derive(Serialize)]
struct Summary {
    output_table: PathBuf,
    rows_written: usize,
}

pub(crate) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let fringe = task.required_text("ff_table")?;
    let request = PccorRequest {
        vis: task.input_path("vis")?,
        output: task.path("pccor_caltable")?,
        refant: task.required_text("refant")?,
        timerange: task.required_text("timerange")?,
        scan: task.required_text("scan")?,
        spw: task.required_text("spw")?,
        antenna: task.required_text("antenna")?,
        cable: task.flag("cablecal_correction")?,
        fringe_table: (fringe != "none").then(|| PathBuf::from(fringe)),
    };
    let (result, rows_written) = execute(&request).map_err(|error| match error {
        PccorError::Selection(message) => TaskFailure::execution(message).at("selection"),
        PccorError::FringeTable(message) => TaskFailure::execution(message).at("ff_table"),
        PccorError::Index(message) => TaskFailure::execution(message).at("pccor_index"),
        _ => TaskFailure::execution(error.to_string()),
    })?;
    TaskSuccess::new(
        result,
        &Summary {
            output_table: request.output,
            rows_written,
        },
    )
}

fn execute(request: &PccorRequest) -> Result<(CasaValue, usize), PccorError> {
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
                windows.push((format!("spw_{window}"), options.record(aligned)));
            }
        }
        if first_antenna {
            let name = &selection.antenna_names[antenna as usize];
            antennas.push((format!("ant_{name}"), CasaValue::dict(windows)));
        }
    }
    drop(align);
    let publish = crate::timing::Phase::start("pccor_write");
    let rows_written = table_rows.len();
    table::write(&ms, &request.output, selection.reference, table_rows)?;
    drop(publish);
    Ok((CasaValue::dict(antennas), rows_written))
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

    fn record(&self, aligned: Vec<Aligned<'_>>) -> CasaValue {
        let vector = CasaValue::float64_vector;
        CasaValue::dict([
            (
                "time",
                vector(aligned.iter().map(|row| row.time).collect::<Vec<_>>()),
            ),
            (
                "scan",
                vector(
                    aligned
                        .iter()
                        .map(|row| row.scan.map_or(f64::NAN, |scan| f64::from(scan.number)))
                        .collect(),
                ),
            ),
            (
                "field_id",
                vector(
                    aligned
                        .iter()
                        .map(|row| row.scan.map_or(f64::NAN, |scan| f64::from(scan.field)))
                        .collect(),
                ),
            ),
            (
                "phase",
                hand_array(aligned.len(), aligned.iter().flat_map(|row| row.phase)),
            ),
            (
                "delay",
                hand_array(aligned.len(), aligned.iter().flat_map(|row| row.delay)),
            ),
            (
                "ff_phase",
                vector(self.fringe.phase.map(wrap_phase).to_vec()),
            ),
            ("ff_delay", vector(self.fringe.delay.to_vec())),
            ("pc_mean_phase", tone_array(&self.pulse.mean_phase)),
            ("pc_mean_delay", vector(self.pulse.mean_delay.to_vec())),
            ("pc_refant_phase", tone_array(&self.reference.mean_phase)),
            (
                "pc_refant_delay",
                vector(self.reference.mean_delay.to_vec()),
            ),
        ])
    }
}

fn wrap_phase(phase: f64) -> f64 {
    (phase + PI).rem_euclid(TAU) - PI
}

fn hand_array(columns: usize, values: impl Iterator<Item = f64>) -> CasaValue {
    CasaValue::FloatArray {
        dtype: FloatDtype::Float64,
        shape: vec![2, columns],
        data: values.map(CasaFloat).collect(),
    }
}

fn tone_array(hands: &[Vec<f64>; 2]) -> CasaValue {
    hand_array(
        hands[0].len(),
        hands[0]
            .iter()
            .zip(&hands[1])
            .flat_map(|(&first, &second)| [first, second]),
    )
}
