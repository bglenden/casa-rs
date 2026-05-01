// SPDX-License-Identifier: LGPL-3.0-or-later
//! Env-gated gain-solver parity traces.

use std::fs::OpenOptions;
use std::io::Write;

use serde::Serialize;
use serde_json::json;

use super::grouping::{SolveAccumulator, SolveBaseKey, SolveBucketKey};
use super::kernel::{PhaseSolverTraceEvent, SolutionRow};
use super::{GainSolveMode, GainSolveRequest, GainType};

const TRACE_ENV: &str = "CASA_RS_GAIN_TRACE";

#[cfg(test)]
thread_local! {
    static TEST_TRACE_PATH: std::cell::RefCell<Option<std::path::PathBuf>> = const {
        std::cell::RefCell::new(None)
    };
}

fn trace_path() -> Option<std::path::PathBuf> {
    #[cfg(test)]
    if let Some(path) = TEST_TRACE_PATH.with(|path| path.borrow().clone()) {
        return Some(path);
    }

    let value = std::env::var_os(TRACE_ENV)?;
    let path = std::path::PathBuf::from(value);
    (!path.as_os_str().is_empty()).then_some(path)
}

fn write_event(event: impl Serialize) {
    let Some(path) = trace_path() else {
        return;
    };
    let result = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .and_then(|mut file| {
            serde_json::to_writer(&mut file, &event).map_err(std::io::Error::other)?;
            file.write_all(b"\n")
        });
    if let Err(error) = result {
        eprintln!(
            "[casa-calibration gain trace] failed to write {}: {error}",
            path.display()
        );
    }
}

pub(crate) fn trace_group(
    base_key: &SolveBaseKey,
    bucket_key: &SolveBucketKey,
    group: &SolveAccumulator,
    request: &GainSolveRequest,
) {
    if trace_path().is_none() {
        return;
    }
    let receptors =
        group
            .receptor_graphs
            .iter()
            .zip(group.receptor_weights.iter())
            .zip(group.receptor_stats.iter())
            .enumerate()
            .map(|(receptor, ((graph, weights), stats))| {
                let mut edges = graph
                .iter()
                .filter_map(|(&(antenna1, antenna2), value)| {
                    if antenna1 > antenna2 {
                        return None;
                    }
                    let stat = stats.get(&(antenna1, antenna2)).copied().unwrap_or_default();
                    Some(json!({
                        "antenna1": antenna1,
                        "antenna2": antenna2,
                        "sum_re": value.re,
                        "sum_im": value.im,
                        "weight": weights.get(&(antenna1, antenna2)).copied().unwrap_or_default(),
                        "sample_count": stat.sample_count,
                        "collapsed_count": stat.collapsed_count,
                        "weighted_sample_power": stat.weighted_sample_power,
                        "raw_weighted_sample_power": stat.raw_weighted_sample_power,
                    }))
                })
                .collect::<Vec<_>>();
                edges.sort_by_key(|edge| {
                    (
                        edge["antenna1"].as_i64().unwrap_or_default(),
                        edge["antenna2"].as_i64().unwrap_or_default(),
                    )
                });
                json!({
                    "receptor": receptor,
                    "edge_count": edges.len(),
                    "edges": edges,
                })
            })
            .collect::<Vec<_>>();

    write_event(json!({
        "event": "rust_group",
        "base_key": base_key_json(base_key),
        "bucket_key": bucket_key_json(bucket_key),
        "gain_type": gain_type_name(request.gain_type),
        "solve_mode": solve_mode_name(request.solve_mode),
        "field_id": group.field_id,
        "spw_id": group.spw_id,
        "observation_id": group.observation_id,
        "min_time": group.min_time,
        "max_time": group.max_time,
        "averaged_time": group.aggregate_time_centroid(),
        "fallback_time": group.aggregate_time(),
        "total_interval": group.total_interval,
        "sample_rows": group.sample_rows,
        "scan_numbers": group.scan_numbers,
        "antenna_ids": group.antenna_ids,
        "receptors": receptors,
    }));
}

pub(crate) fn trace_solution_rows(
    base_key: &SolveBaseKey,
    bucket_key: &SolveBucketKey,
    rows: &[SolutionRow],
    request: &GainSolveRequest,
    refant_id: i32,
) {
    if trace_path().is_none() {
        return;
    }
    let mut solutions = rows
        .iter()
        .map(|row| {
            let gains = row
                .gains
                .iter()
                .enumerate()
                .map(|(receptor, gain)| {
                    json!({
                        "receptor": receptor,
                        "re": gain.re,
                        "im": gain.im,
                        "amplitude": gain.norm(),
                        "phase": gain.im.atan2(gain.re),
                        "flag": row.flags.get(receptor).copied().unwrap_or(true),
                        "param_error": row.param_errors.get(receptor).copied().unwrap_or_default(),
                        "snr": row.snrs.get(receptor).copied().unwrap_or_default(),
                        "weight": row.weights.get(receptor).copied().unwrap_or_default(),
                    })
                })
                .collect::<Vec<_>>();
            json!({
                "time": row.time_seconds,
                "interval": row.interval_seconds,
                "field_id": row.field_id,
                "spw_id": row.spw_id,
                "scan_number": row.scan_number,
                "observation_id": row.observation_id,
                "antenna_id": row.antenna_id,
                "gains": gains,
            })
        })
        .collect::<Vec<_>>();
    solutions.sort_by_key(|row| row["antenna_id"].as_i64().unwrap_or_default());

    write_event(json!({
        "event": "rust_solution",
        "base_key": base_key_json(base_key),
        "bucket_key": bucket_key_json(bucket_key),
        "gain_type": gain_type_name(request.gain_type),
        "solve_mode": solve_mode_name(request.solve_mode),
        "refant_id": refant_id,
        "solutions": solutions,
    }));
}

pub(crate) fn trace_phase_solver_iteration(event: PhaseSolverTraceEvent<'_>) {
    if trace_path().is_none() {
        return;
    }

    let mut antenna_ids = event.antenna_ids.iter().copied().collect::<Vec<_>>();
    antenna_ids.sort_unstable();
    let tracked_antennas = antenna_ids
        .iter()
        .copied()
        .map(|antenna_id| {
            let gain = event
                .gains
                .get(&antenna_id)
                .copied()
                .unwrap_or_else(|| num_complex::Complex64::new(0.0, 0.0));
            let last_gain = event
                .last_gains
                .get(&antenna_id)
                .copied()
                .unwrap_or_else(|| num_complex::Complex64::new(0.0, 0.0));
            let gradient = event
                .gradient
                .get(&antenna_id)
                .copied()
                .unwrap_or_else(|| num_complex::Complex64::new(0.0, 0.0));
            let delta = event
                .delta
                .get(&antenna_id)
                .copied()
                .unwrap_or_else(|| num_complex::Complex64::new(0.0, 0.0));
            json!({
                "antenna_id": antenna_id,
                "gain_re": gain.re,
                "gain_im": gain.im,
                "last_gain_re": last_gain.re,
                "last_gain_im": last_gain.im,
                "gradient_re": gradient.re,
                "gradient_im": gradient.im,
                "hessian": event.hessian.get(&antenna_id).copied().unwrap_or_default(),
                "delta_re": delta.re,
                "delta_im": delta.im,
            })
        })
        .collect::<Vec<_>>();

    write_event(json!({
        "event": "rust_phase_solver_iteration",
        "iteration": event.iteration,
        "refant_id": event.refant_id,
        "chi_square": event.chi_square,
        "last_chi_square": event.last_chi_square,
        "delta_chi_square": event.delta_chi_square,
        "fractional_delta": event.fractional_delta,
        "convergence_count": event.convergence_count,
        "line_search": {
            "x0": event.step.x0,
            "x1": event.step.x1,
            "x2": event.step.x2,
            "step": event.step.step,
            "opt_factor": event.step.opt_factor,
            "expanded": event.step.expanded,
            "iterations": event.step.iterations,
        },
        "antennas": tracked_antennas,
    }));
}

fn base_key_json(key: &SolveBaseKey) -> serde_json::Value {
    json!({
        "field_id": key.field_id,
        "spw_id": key.spw_id,
        "observation_id": key.observation_id,
        "scan_number": key.scan_number,
    })
}

fn bucket_key_json(key: &SolveBucketKey) -> serde_json::Value {
    match key {
        SolveBucketKey::Infinite => json!({"kind": "infinite"}),
        SolveBucketKey::Integration {
            time_bits,
            interval_bits,
        } => json!({
            "kind": "integration",
            "time_bits": time_bits,
            "interval_bits": interval_bits,
        }),
        SolveBucketKey::Seconds(index) => json!({"kind": "seconds", "index": index}),
    }
}

fn gain_type_name(gain_type: GainType) -> &'static str {
    match gain_type {
        GainType::G => "G",
        GainType::T => "T",
    }
}

fn solve_mode_name(solve_mode: GainSolveMode) -> &'static str {
    match solve_mode {
        GainSolveMode::Phase => "p",
        GainSolveMode::AmplitudePhase => "ap",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};

    use casa_ms::selection::MsSelection;
    use casa_types::Complex32;
    use num_complex::Complex64;
    use tempfile::tempdir;

    use super::*;
    use crate::solve::grouping::SolveEdgeStats;
    use crate::solve::kernel::PhaseStepTrace;
    use crate::solve::{GainSolveCombine, GainSolveInterval, GainSolveModelSource, RefAntSelector};

    fn test_request(output_table: impl Into<std::path::PathBuf>) -> GainSolveRequest {
        GainSolveRequest {
            selection: MsSelection::new(),
            output_table: output_table.into(),
            gain_type: GainType::G,
            solve_mode: GainSolveMode::Phase,
            solve_interval: GainSolveInterval::Integration,
            combine: GainSolveCombine::default(),
            refant: RefAntSelector::AntennaId(0),
            prior_calibration_tables: Vec::new(),
            parang: false,
            model_source: GainSolveModelSource::PointSource,
            normalize_average_amplitude: false,
            min_snr: 0.0,
            min_baselines_per_antenna: 1,
            smodel: [1.0, 0.0, 0.0, 0.0],
        }
    }

    #[test]
    fn trace_writers_emit_group_solution_and_phase_iteration_jsonl() {
        let dir = tempdir().expect("tempdir");
        let trace_file = dir.path().join("gain-trace.jsonl");
        TEST_TRACE_PATH.with(|path| *path.borrow_mut() = Some(trace_file.clone()));

        let base_key = SolveBaseKey {
            field_id: 3,
            spw_id: 4,
            observation_id: 5,
            scan_number: 6,
        };
        let bucket_key = SolveBucketKey::Integration {
            time_bits: 12.5_f64.to_bits(),
            interval_bits: 2.0_f64.to_bits(),
        };
        let mut group = SolveAccumulator::new(3, 4, 5);
        group.min_time = 10.0;
        group.max_time = 20.0;
        group.unique_time_sum = 30.0;
        group.unique_time_count = 2;
        group.time_centroid_weighted_sum = 61.0;
        group.time_centroid_weight = 4.0;
        group.total_interval = 8.0;
        group.sample_rows = 2;
        group.scan_numbers.insert(6);
        group.antenna_ids.extend([0, 1]);
        group.receptor_graphs = vec![HashMap::from([
            ((0, 1), Complex32::new(2.0, -1.0)),
            ((1, 0), Complex32::new(2.0, 1.0)),
        ])];
        group.receptor_weights = vec![HashMap::from([((0, 1), 3.5), ((1, 0), 3.5)])];
        group.receptor_stats = vec![HashMap::from([(
            (0, 1),
            SolveEdgeStats {
                weighted_sample_power: 7.0,
                raw_weighted_sample_power: 8.0,
                sample_count: 9,
                collapsed_count: 10,
            },
        )])];
        let request = test_request(dir.path().join("phase.cal"));

        trace_group(&base_key, &bucket_key, &group, &request);
        trace_solution_rows(
            &base_key,
            &SolveBucketKey::Seconds(2),
            &[
                SolutionRow {
                    time_seconds: 20.0,
                    interval_seconds: 4.0,
                    field_id: 3,
                    spw_id: 4,
                    antenna_id: 2,
                    scan_number: 6,
                    observation_id: 5,
                    gains: vec![Complex32::new(0.5, 0.25)],
                    flags: vec![false],
                    param_errors: vec![0.1],
                    snrs: vec![12.0],
                    weights: vec![99.0],
                },
                SolutionRow {
                    time_seconds: 20.0,
                    interval_seconds: 4.0,
                    field_id: 3,
                    spw_id: 4,
                    antenna_id: 1,
                    scan_number: 6,
                    observation_id: 5,
                    gains: vec![Complex32::new(1.0, 0.0)],
                    flags: vec![true],
                    param_errors: vec![0.0],
                    snrs: vec![0.0],
                    weights: vec![0.0],
                },
            ],
            &request,
            0,
        );

        let antenna_ids = BTreeSet::from([0, 1]);
        let step = PhaseStepTrace {
            x0: 0.0,
            x1: 0.5,
            x2: 1.0,
            step: 0.25,
            opt_factor: 0.75,
            expanded: true,
            iterations: 3,
        };
        let gains = HashMap::from([(1, Complex64::new(0.0, 1.0))]);
        let last_gains = HashMap::from([(1, Complex64::new(1.0, 0.0))]);
        let gradient = HashMap::from([(1, Complex64::new(0.1, -0.2))]);
        let hessian = HashMap::from([(1, 2.0)]);
        let delta = HashMap::from([(1, Complex64::new(-0.01, 0.02))]);
        trace_phase_solver_iteration(PhaseSolverTraceEvent {
            iteration: 7,
            refant_id: 0,
            chi_square: 1.0,
            last_chi_square: 2.0,
            delta_chi_square: -1.0,
            fractional_delta: -0.5,
            convergence_count: 2,
            step: &step,
            antenna_ids: &antenna_ids,
            last_gains: &last_gains,
            gains: &gains,
            gradient: &gradient,
            hessian: &hessian,
            delta: &delta,
        });

        TEST_TRACE_PATH.with(|path| *path.borrow_mut() = None);

        let lines = std::fs::read_to_string(&trace_file).expect("trace file");
        assert!(lines.contains("\"event\":\"rust_group\""));
        assert!(lines.contains("\"kind\":\"integration\""));
        assert!(lines.contains("\"sample_count\":9"));
        assert!(lines.contains("\"event\":\"rust_solution\""));
        assert!(lines.contains("\"kind\":\"seconds\""));
        assert!(
            lines.find("\"antenna_id\":1").expect("antenna 1")
                < lines.find("\"antenna_id\":2").expect("antenna 2")
        );
        assert!(lines.contains("\"event\":\"rust_phase_solver_iteration\""));
        assert!(lines.contains("\"expanded\":true"));
        assert!(lines.contains("\"gradient_im\":-0.2"));
    }

    #[test]
    fn trace_name_helpers_cover_public_request_enums() {
        assert_eq!(gain_type_name(GainType::G), "G");
        assert_eq!(gain_type_name(GainType::T), "T");
        assert_eq!(solve_mode_name(GainSolveMode::Phase), "p");
        assert_eq!(solve_mode_name(GainSolveMode::AmplitudePhase), "ap");
        assert_eq!(
            base_key_json(&SolveBaseKey {
                field_id: 1,
                spw_id: 2,
                observation_id: 3,
                scan_number: 4,
            })["scan_number"],
            4
        );
        assert_eq!(
            bucket_key_json(&SolveBucketKey::Infinite)["kind"],
            "infinite"
        );
    }
}
