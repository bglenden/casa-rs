// SPDX-License-Identifier: LGPL-3.0-or-later
//! Numerical solve kernel for limited `gaincal`.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::f64::consts::PI;

use casa_types::Complex32;

use super::{GainSolveError, GainSolveMode, GainType};
use crate::solve::grouping::SolveAccumulator;

#[derive(Debug, Clone)]
pub(crate) struct SolutionRow {
    pub(crate) time_seconds: f64,
    pub(crate) interval_seconds: f64,
    pub(crate) field_id: i32,
    pub(crate) spw_id: i32,
    pub(crate) antenna_id: i32,
    pub(crate) scan_number: i32,
    pub(crate) observation_id: i32,
    pub(crate) gains: Vec<Complex32>,
    pub(crate) flags: Vec<bool>,
}

#[derive(Debug, Clone)]
pub(crate) struct SolvedPhaseGraph {
    pub(crate) gains: HashMap<i32, Complex32>,
    pub(crate) reachable: BTreeSet<i32>,
}

pub(crate) fn solve_group(
    group: SolveAccumulator,
    available_antennas: &BTreeSet<i32>,
    gain_type: GainType,
    solve_mode: GainSolveMode,
    refant_id: i32,
) -> Result<Vec<SolutionRow>, GainSolveError> {
    let averaged_time = (group.min_time + group.max_time) / 2.0;
    let averaged_interval = group.total_interval / group.sample_rows.max(1) as f64;
    let scan_number = *group.scan_numbers.iter().next().unwrap_or(&0);
    let solved = group
        .receptor_graphs
        .iter()
        .zip(group.receptor_weights.iter())
        .map(|(graph, weights)| {
            solve_graph(
                graph,
                weights,
                solve_mode,
                refant_id,
                group.field_id,
                group.spw_id,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut antenna_ids = available_antennas.clone();
    antenna_ids.extend(group.antenna_ids);
    antenna_ids.insert(refant_id);

    let solution_rows = antenna_ids
        .into_iter()
        .map(|antenna_id| {
            let gains = match gain_type {
                GainType::G => solved
                    .iter()
                    .map(|per_receptor| {
                        *per_receptor
                            .gains
                            .get(&antenna_id)
                            .unwrap_or(&Complex32::new(1.0, 0.0))
                    })
                    .collect::<Vec<_>>(),
                GainType::T => vec![
                    *solved[0]
                        .gains
                        .get(&antenna_id)
                        .unwrap_or(&Complex32::new(1.0, 0.0)),
                ],
            };
            let flags = match gain_type {
                GainType::G => solved
                    .iter()
                    .map(|per_receptor| {
                        antenna_id != refant_id && !per_receptor.reachable.contains(&antenna_id)
                    })
                    .collect(),
                GainType::T => {
                    vec![antenna_id != refant_id && !solved[0].reachable.contains(&antenna_id)]
                }
            };
            SolutionRow {
                time_seconds: averaged_time,
                interval_seconds: averaged_interval,
                field_id: group.field_id,
                spw_id: group.spw_id,
                antenna_id,
                scan_number,
                observation_id: group.observation_id,
                gains,
                flags,
            }
        })
        .collect();

    Ok(solution_rows)
}

pub(crate) fn solve_graph(
    graph: &HashMap<(i32, i32), Complex32>,
    weights: &HashMap<(i32, i32), f32>,
    solve_mode: GainSolveMode,
    refant_id: i32,
    field_id: i32,
    spw_id: i32,
) -> Result<SolvedPhaseGraph, GainSolveError> {
    let mut antenna_ids = BTreeSet::new();
    for (antenna1, antenna2) in graph.keys() {
        antenna_ids.insert(*antenna1);
        antenna_ids.insert(*antenna2);
    }
    antenna_ids.insert(refant_id);
    let reachable = reachable_antennas(graph, refant_id);

    for antenna_id in &antenna_ids {
        if *antenna_id != refant_id && !reachable.contains(antenna_id) {
            return Err(GainSolveError::UnsolvableAntenna {
                antenna_id: *antenna_id,
                field_id,
                spw_id,
            });
        }
    }

    let initial_phases = initial_phases(graph, refant_id, &reachable);
    let mut gains = antenna_ids
        .iter()
        .copied()
        .map(|antenna_id| {
            let phase = *initial_phases.get(&antenna_id).unwrap_or(&0.0);
            (
                antenna_id,
                Complex32::new(phase.cos() as f32, phase.sin() as f32),
            )
        })
        .collect::<HashMap<_, _>>();

    for _ in 0..512 {
        let mut changed = false;
        for antenna_id in antenna_ids.iter().copied() {
            if !reachable.contains(&antenna_id)
                || (matches!(solve_mode, GainSolveMode::Phase) && antenna_id == refant_id)
            {
                continue;
            }
            let mut accumulator = Complex32::new(0.0, 0.0);
            let mut total_weight = 0.0_f32;
            for other_id in antenna_ids.iter().copied() {
                if antenna_id == other_id || !reachable.contains(&other_id) {
                    continue;
                }
                if let Some(edge) = graph.get(&(antenna_id, other_id)) {
                    accumulator += *edge * gains[&other_id];
                    total_weight += weights
                        .get(&(antenna_id, other_id))
                        .copied()
                        .unwrap_or_default()
                        * gains[&other_id].norm_sqr();
                }
            }
            if total_weight <= f32::EPSILON {
                continue;
            }
            let candidate = accumulator / Complex32::new(total_weight, 0.0);
            let updated = match solve_mode {
                GainSolveMode::Phase => {
                    let norm = candidate.norm();
                    if norm <= f32::EPSILON {
                        continue;
                    }
                    candidate / Complex32::new(norm, 0.0)
                }
                GainSolveMode::AmplitudePhase => {
                    if candidate.norm() <= f32::EPSILON {
                        continue;
                    }
                    candidate
                }
            };
            let delta = updated * gains[&antenna_id].conj();
            let delta_complex = f64::from((updated - gains[&antenna_id]).norm());
            let delta_phase = f64::from(delta.im).atan2(f64::from(delta.re)).abs();
            if delta_phase > 1.0e-10 || delta_complex > 1.0e-7 {
                changed = true;
            }
            gains.insert(antenna_id, updated);
        }
        rereference_gains(&mut gains, refant_id, solve_mode);
        if !changed {
            break;
        }
    }

    Ok(SolvedPhaseGraph { gains, reachable })
}

fn rereference_gains(
    gains: &mut HashMap<i32, Complex32>,
    refant_id: i32,
    solve_mode: GainSolveMode,
) {
    let Some(ref_gain) = gains.get(&refant_id).copied() else {
        gains.insert(refant_id, Complex32::new(1.0, 0.0));
        return;
    };
    let ref_norm = ref_gain.norm();
    if ref_norm <= f32::EPSILON {
        gains.insert(refant_id, Complex32::new(1.0, 0.0));
        return;
    }
    let anchor = match solve_mode {
        GainSolveMode::Phase | GainSolveMode::AmplitudePhase => {
            ref_gain / Complex32::new(ref_norm, 0.0)
        }
    };
    for gain in gains.values_mut() {
        *gain /= anchor;
    }
    let anchored_ref = match solve_mode {
        GainSolveMode::Phase => Complex32::new(1.0, 0.0),
        GainSolveMode::AmplitudePhase => Complex32::new(ref_norm, 0.0),
    };
    gains.insert(refant_id, anchored_ref);
}

pub(crate) fn accumulate_edge(
    graph: &mut HashMap<(i32, i32), Complex32>,
    weights: &mut HashMap<(i32, i32), f32>,
    antenna1: i32,
    antenna2: i32,
    weight: f32,
    normalized: Complex32,
) {
    graph
        .entry((antenna1, antenna2))
        .and_modify(|value| *value += normalized)
        .or_insert(normalized);
    weights
        .entry((antenna1, antenna2))
        .and_modify(|value| *value += weight)
        .or_insert(weight);
    graph
        .entry((antenna2, antenna1))
        .and_modify(|value| *value += normalized.conj())
        .or_insert(normalized.conj());
    weights
        .entry((antenna2, antenna1))
        .and_modify(|value| *value += weight)
        .or_insert(weight);
}

fn reachable_antennas(graph: &HashMap<(i32, i32), Complex32>, refant_id: i32) -> BTreeSet<i32> {
    let mut reachable = BTreeSet::new();
    let mut queue = VecDeque::from([refant_id]);
    reachable.insert(refant_id);

    while let Some(antenna_id) = queue.pop_front() {
        for (from, to) in graph.keys() {
            if *from == antenna_id && reachable.insert(*to) {
                queue.push_back(*to);
            }
        }
    }

    reachable
}

fn initial_phases(
    graph: &HashMap<(i32, i32), Complex32>,
    refant_id: i32,
    reachable: &BTreeSet<i32>,
) -> HashMap<i32, f64> {
    let mut phases = HashMap::new();
    let mut queue = VecDeque::from([refant_id]);
    phases.insert(refant_id, 0.0);

    while let Some(antenna_id) = queue.pop_front() {
        let phase = phases[&antenna_id];
        for (from, to) in graph.keys() {
            if *from != antenna_id || !reachable.contains(to) || phases.contains_key(to) {
                continue;
            }
            let measurement = phase_angle(graph[&(*from, *to)]);
            phases.insert(*to, wrap_phase(phase - measurement));
            queue.push_back(*to);
        }
    }

    for antenna_id in reachable {
        phases.entry(*antenna_id).or_insert(0.0);
    }

    phases
}

fn phase_angle(value: Complex32) -> f64 {
    f64::from(value.im).atan2(f64::from(value.re))
}

fn wrap_phase(value: f64) -> f64 {
    let mut wrapped = value;
    while wrapped > PI {
        wrapped -= 2.0 * PI;
    }
    while wrapped <= -PI {
        wrapped += 2.0 * PI;
    }
    wrapped
}
