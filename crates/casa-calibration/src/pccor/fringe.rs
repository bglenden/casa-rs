// SPDX-License-Identifier: LGPL-3.0-or-later
//! Use supplied fringe solutions or solve through the native fringe kernel.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_ms::{MeasurementSet, MsSelection};
use casa_tables::{Table, TableOptions};
use casa_types::ArrayValue;
use ndarray::Ix2;

use super::pulse::Pair;
use super::selection::Selection;
use super::{PccorError, PccorRequest};
use crate::solve::{get_f64, get_i32};
use crate::{
    CalibrationDataset, CalibrationSolveRequest, FringeControls, FringeCorrelation,
    GainNormalization, GainSolveMode, GainSolveModelSource, GainSolveRequest, GainType,
    RefAntSelector, solve_calibration,
};

pub(super) struct FringeSolution {
    pub(super) time: f64,
    pub(super) phase: [f64; 2],
    pub(super) delay: [f64; 2],
}

pub(super) fn read_or_solve(
    ms: &MeasurementSet,
    request: &PccorRequest,
    selection: &Selection,
) -> Result<BTreeMap<Pair, FringeSolution>, PccorError> {
    let generated;
    let path = if let Some(path) = &request.fringe_table {
        path
    } else {
        generated = solve(ms, request)?;
        &generated
    };
    read(path, selection)
}

fn solve(ms: &MeasurementSet, request: &PccorRequest) -> Result<PathBuf, PccorError> {
    let mut filename = request.vis.as_os_str().to_owned();
    filename.push(".mpc");
    let output_table = PathBuf::from(filename);
    let gain = GainSolveRequest {
        selection: MsSelection {
            scan: (request.scan != "none").then(|| request.scan.clone()),
            timerange: (request.timerange != "none").then(|| request.timerange.clone()),
            ..MsSelection::default()
        },
        output_table: output_table.clone(),
        gain_type: GainType::K,
        solve_mode: GainSolveMode::Phase,
        solve_interval: Default::default(),
        preaverage_seconds: 0.,
        correlation_dependent_flags: false,
        combine: Default::default(),
        refant: RefAntSelector::AntennaName(request.refant.clone()),
        prior_calibration_tables: Vec::new(),
        parang: false,
        model_source: GainSolveModelSource::FromMeasurementSet,
        normalize_average_amplitude: false,
        normalization_type: GainNormalization::Mean,
        robustness: Default::default(),
        min_snr: 3.,
        min_baselines_per_antenna: 0,
        smodel: [1., 0., 0., 0.],
    };
    solve_calibration(
        CalibrationDataset::Open(ms),
        CalibrationSolveRequest::Fringe {
            gain,
            controls: FringeControls {
                delay_window: [-1e6, 1e6],
                rate_window: [-1e6, 1e6],
                global_solve: true,
                iterations: 100,
                active_parameters: [true, true, false],
                zero_rates: true,
                correlation_combination: FringeCorrelation::Separate,
                concatenate_windows: true,
            },
        },
    )
    .map_err(|error| PccorError::Execution(error.to_string()))?;
    Ok(output_table)
}

fn read(path: &Path, selection: &Selection) -> Result<BTreeMap<Pair, FringeSolution>, PccorError> {
    let table = Table::open(TableOptions::new(path))?;
    let parameters = table.column_accessor("FPARAM")?;
    let malformed =
        |row| PccorError::Execution(format!("Invalid FPARAM cell in fringe fit row {row}"));
    let [start, stop] = selection.interval;
    let mut times_in_interval = false;
    let mut reference = None;
    let mut solutions = BTreeMap::<Pair, Vec<FringeSolution>>::new();
    for row in 0..table.row_count() {
        let antenna = get_i32(&table, row, "ANTENNA1").map_err(scalar_error)?;
        let row_reference = get_i32(&table, row, "ANTENNA2").map_err(scalar_error)?;
        let window = get_i32(&table, row, "SPECTRAL_WINDOW_ID").map_err(scalar_error)?;
        let time = get_f64(&table, row, "TIME").map_err(scalar_error)?;
        times_in_interval |= start < time && time <= stop;
        if reference
            .replace(row_reference)
            .is_some_and(|first| first != row_reference)
        {
            return Err(PccorError::FringeTable(
                "Multiple reference antennas not supported in fringe fit table".into(),
            ));
        }
        if !selection.antennas.contains(&antenna) || !selection.windows.contains(&window) {
            continue;
        }
        let ArrayValue::Float32(values) = parameters.array_cell(row)? else {
            return Err(malformed(row));
        };
        let values = values
            .view()
            .into_dimensionality::<Ix2>()
            .map_err(|_| malformed(row))?;
        if values.nrows() != 8 || values.ncols() == 0 {
            return Err(malformed(row));
        }
        solutions
            .entry((antenna, window))
            .or_default()
            .push(FringeSolution {
                time,
                phase: [f64::from(values[(0, 0)]), f64::from(values[(4, 0)])],
                delay: [
                    f64::from(values[(1, 0)]) * 1e-9,
                    f64::from(values[(5, 0)]) * 1e-9,
                ],
            });
    }
    if let Some(reference) = reference
        && reference != selection.reference
    {
        let requested = &selection.antenna_names[selection.reference as usize];
        let found = selection
            .antenna_names
            .get(reference as usize)
            .map_or("unknown", String::as_str);
        return Err(PccorError::FringeTable(format!(
            "Different reference antenna between pccor ({requested}) and fringe fit data ({found})",
        )));
    }
    if !times_in_interval {
        return Err(PccorError::Execution(
            "Fringe fit solutions not available for specified times".into(),
        ));
    }
    let mut selected = BTreeMap::new();
    for &antenna in &selection.antennas {
        for &window in &selection.windows {
            let pair = (antenna, window);
            if let Some(mut values) = solutions.remove(&pair)
                && values.len() == 1
            {
                selected.insert(pair, values.pop().expect("checked single solution"));
                continue;
            }
            return Err(PccorError::FringeTable(
                "Expected a single fringe fit solution for the time interval".into(),
            ));
        }
    }
    Ok(selected)
}

fn scalar_error(error: crate::GainSolveError) -> PccorError {
    PccorError::Execution(error.to_string())
}
