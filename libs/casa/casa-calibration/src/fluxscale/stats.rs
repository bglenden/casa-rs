// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's per-antenna/polarization time means and median gain-ratio estimate.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use super::{FluxScaleError, FluxScaleEstimate, GainSampleRow};

#[derive(Default)]
struct MeanGain {
    sum: f64,
    count: usize,
}

/// `SolvableVisJones::fluxscale` first time-averages amplitudes with unit
/// weights, then matches each antenna/polarization to its reference mean.
pub(super) fn derive_selected_estimates(
    rows: &[GainSampleRow],
    reference_fields: &BTreeSet<i32>,
    transfer_fields: &BTreeSet<i32>,
    threshold: Option<f64>,
    refspwmap: &[i32],
    masks: &super::selection::SampleMasks,
) -> Result<HashMap<(i32, i32), FluxScaleEstimate>, FluxScaleError> {
    let mut timestamps = HashSet::with_capacity(rows.len());
    for (row, selected) in rows.iter().zip(&masks.logical) {
        if *selected
            && !timestamps.insert((
                row.field_id,
                row.spw_id,
                row.antenna_id,
                row.time_seconds.to_bits(),
            ))
        {
            return Err(FluxScaleError::UnsupportedInput {
                path: "fluxscale input".into(),
                reason: format!(
                    "multiple gain solutions in one timestamp for field={} spw={} antenna={}",
                    row.field_id, row.spw_id, row.antenna_id
                ),
            });
        }
    }
    let means = time_mean_gains(rows, threshold, masks);
    // Reference fields carry equal weight after their own time averages.
    let mut references = BTreeMap::<(i32, i32, usize), MeanGain>::new();
    for (&(field, spw, antenna, polarization), mean) in &means {
        if reference_fields.contains(&field) {
            let reference = references.entry((spw, antenna, polarization)).or_default();
            reference.sum += mean.sum / mean.count as f64;
            reference.count += 1;
        }
    }
    let mut ratios = BTreeMap::<(i32, i32), Vec<f64>>::new();
    for (&(field, spw, antenna, polarization), mean) in &means {
        if !transfer_fields.contains(&field) {
            continue;
        }
        let reference_spw = mapped_reference_spw(spw, refspwmap);
        if let Some(reference) = references.get(&(reference_spw, antenna, polarization)) {
            let reference = reference.sum / reference.count as f64;
            if reference > 0.0 {
                ratios
                    .entry((field, spw))
                    .or_default()
                    .push((mean.sum / mean.count as f64) / reference);
            }
        }
    }
    let mut estimates = HashMap::new();
    for (key @ (field_id, spw_id), mut values) in ratios {
        values.sort_by(f64::total_cmp);
        let ratio = median(&values);
        if !(ratio.is_finite() && ratio > 0.0) {
            return Err(FluxScaleError::UnstableScaleFactor { field_id, spw_id });
        }
        let count = values.len();
        let error = if count > 1 {
            let mean = values.iter().sum::<f64>() / count as f64;
            let variance = values
                .iter()
                .map(|value| (value - mean).powi(2))
                .sum::<f64>()
                / (count - 1) as f64;
            // CASA's reported flux error is 2*stddev(ratio)/sqrt(nPA-1).
            2.0 * variance.sqrt() / ((count - 1) as f64).sqrt()
        } else {
            0.0
        };
        estimates.insert(
            key,
            FluxScaleEstimate {
                flux_density_jy: ratio * ratio,
                flux_density_error_jy: error,
                correction_factor: (1.0 / ratio) as f32,
                num_sol: count,
            },
        );
    }
    if estimates.is_empty() {
        let field_id = transfer_fields.iter().copied().next().unwrap_or(-1);
        let spw_id = rows
            .iter()
            .find(|row| row.field_id == field_id)
            .map_or(-1, |row| row.spw_id);
        return Err(FluxScaleError::MissingGainSamples { field_id, spw_id });
    }
    Ok(estimates)
}

fn time_mean_gains(
    rows: &[GainSampleRow],
    threshold: Option<f64>,
    masks: &super::selection::SampleMasks,
) -> BTreeMap<(i32, i32, i32, usize), MeanGain> {
    let threshold = threshold.filter(|threshold| *threshold >= 0.);
    let mut amplitudes = BTreeMap::<(i32, i32), Vec<f64>>::new();
    for (row, selected) in rows.iter().zip(&masks.threshold) {
        if !selected || threshold.is_none() {
            continue;
        }
        for (gain, flagged) in row.gains.iter().zip(&row.flags) {
            if !flagged {
                amplitudes
                    .entry((row.field_id, row.spw_id))
                    .or_default()
                    .push(f64::from(gain.norm()));
            }
        }
    }
    let medians = amplitudes
        .into_iter()
        .map(|(key, mut values)| {
            values.sort_by(f64::total_cmp);
            (key, f64::from(median(&values) as f32))
        })
        .collect::<BTreeMap<_, _>>();
    let mut means = BTreeMap::<(i32, i32, i32, usize), MeanGain>::new();
    for (row, selected) in rows.iter().zip(&masks.data) {
        if !selected {
            continue;
        }
        for (polarization, (gain, flagged)) in row.gains.iter().zip(&row.flags).enumerate() {
            let amplitude = f64::from(gain.norm());
            if *flagged
                || threshold.is_some_and(|threshold| {
                    !medians
                        .get(&(row.field_id, row.spw_id))
                        .is_some_and(|median| within_threshold(amplitude, *median, threshold))
                })
            {
                continue;
            }
            let mean = means
                .entry((row.field_id, row.spw_id, row.antenna_id, polarization))
                .or_default();
            mean.sum += amplitude;
            mean.count += 1;
        }
    }
    means
}

fn within_threshold(amplitude: f64, median: f64, threshold: f64) -> bool {
    let threshold = threshold as f32;
    let lower = if threshold < 1. { 1. - threshold } else { 0. };
    amplitude >= f64::from(lower * median as f32)
        && amplitude <= (1. + f64::from(threshold)) * median
}

fn median(values: &[f64]) -> f64 {
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    }
}

pub(super) fn mapped_reference_spw(spw: i32, mapping: &[i32]) -> i32 {
    if mapping.first().is_none_or(|id| *id < 0) {
        return spw;
    }
    if mapping.len() == 1 {
        return mapping[0];
    }
    usize::try_from(spw)
        .ok()
        .and_then(|index| mapping.get(index))
        .copied()
        .filter(|id| *id >= 0)
        .unwrap_or(spw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_core::Complex32;
    use ndarray::{ArrayD, IxDyn};

    #[derive(serde::Deserialize)]
    struct Fixture {
        rows: Vec<Row>,
        expected: Expected,
    }
    #[derive(serde::Deserialize)]
    struct Row {
        field_id: i32,
        spw_id: i32,
        antenna_id: i32,
        time: f64,
        scan: i32,
        gains: Vec<[f32; 2]>,
        flags: Vec<bool>,
    }
    #[derive(serde::Deserialize)]
    struct Expected {
        flux_density_jy: f64,
        flux_density_error_jy: f64,
        num_sol: usize,
    }

    #[test]
    fn casa_interval_solutions_match_flux_error_and_antenna_polarization_count() {
        let fixture: Fixture =
            serde_json::from_str(include_str!("../../tests/fixtures/fluxscale-original.json"))
                .unwrap();
        let rows = fixture
            .rows
            .into_iter()
            .enumerate()
            .map(|(row_index, row)| {
                let shape = IxDyn(&[row.gains.len(), 1]);
                GainSampleRow {
                    row_index,
                    field_id: row.field_id,
                    spw_id: row.spw_id,
                    antenna_id: row.antenna_id,
                    antenna2_id: 0,
                    time_seconds: row.time,
                    interval_seconds: 0.0,
                    scan_number: row.scan,
                    gains: ArrayD::from_shape_vec(
                        shape.clone(),
                        row.gains
                            .into_iter()
                            .map(|[re, im]| Complex32::new(re, im))
                            .collect(),
                    )
                    .unwrap()
                    .into(),
                    flags: ArrayD::from_shape_vec(shape, row.flags).unwrap().into(),
                }
            })
            .collect::<Vec<_>>();
        let estimates = derive_selected_estimates(
            &rows,
            &BTreeSet::from([0]),
            &BTreeSet::from([1]),
            None,
            &[],
            &super::super::selection::SampleMasks::all(rows.len()),
        )
        .unwrap();
        let result = &estimates[&(1, 0)];
        assert!((result.flux_density_jy - fixture.expected.flux_density_jy).abs() < 1e-6);
        assert!(
            (result.flux_density_error_jy - fixture.expected.flux_density_error_jy).abs() < 1e-6
        );
        assert_eq!(result.num_sol, fixture.expected.num_sol);
    }
}
