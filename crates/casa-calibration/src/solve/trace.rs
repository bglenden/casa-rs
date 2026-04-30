// SPDX-License-Identifier: LGPL-3.0-or-later
//! Env-gated gain-solver parity traces.

use std::fs::OpenOptions;
use std::io::Write;
use std::sync::OnceLock;

use serde::Serialize;
use serde_json::json;

use super::grouping::{SolveAccumulator, SolveBaseKey, SolveBucketKey};
use super::kernel::{PhaseSolverTraceEvent, SolutionRow};
use super::{GainSolveMode, GainSolveRequest, GainType};

const TRACE_ENV: &str = "CASA_RS_GAIN_TRACE";

fn trace_path() -> Option<&'static std::path::PathBuf> {
    static PATH: OnceLock<Option<std::path::PathBuf>> = OnceLock::new();
    PATH.get_or_init(|| {
        let value = std::env::var_os(TRACE_ENV)?;
        let path = std::path::PathBuf::from(value);
        if path.as_os_str().is_empty() {
            None
        } else {
            Some(path)
        }
    })
    .as_ref()
}

fn write_event(event: impl Serialize) {
    let Some(path) = trace_path() else {
        return;
    };
    let result = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
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
