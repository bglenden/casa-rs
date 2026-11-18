// SPDX-License-Identifier: LGPL-3.0-or-later
//! Fit antenna polynomials to weighted baseline spectra.

use std::collections::{BTreeMap, BTreeSet};
use std::f64::consts::{PI, TAU};

use casa_numerics::invert_symmetric_positive_definite;
use casa_types::Complex32;
use num_complex::Complex64;

use super::legacy_bpoly_basis;
use super::{BPolySolutionRow, BandpassSolveCuts, BandpassSolveError, BandpassSolveRequest};
use crate::solve::grouping::SolveAccumulator;

struct VisibilitySample {
    antennas: [usize; 2],
    channel: usize,
    visibility: Complex64,
    weight: f64,
}

pub(super) fn fit_interval(
    group: &SolveAccumulator,
    frequencies: &BTreeMap<i32, &[f64]>,
    antennas: &BTreeSet<i32>,
    refant: i32,
    request: &BandpassSolveRequest,
    cuts: BandpassSolveCuts,
) -> Result<Vec<BPolySolutionRow>, BandpassSolveError> {
    let error = |reason: &str| BandpassSolveError::FitPolynomial {
        antenna_id: refant,
        field_id: group.field_id,
        spw_id: group.spw_id,
        reason: reason.to_string(),
    };
    let [amp_count, phase_count] = [request.amplitude_degree + 1, request.phase_degree + 1];
    let count = amp_count.max(phase_count);
    let grid = FrequencyGrid::new(frequencies)
        .ok_or_else(|| error("at least two distinct channel frequencies are required"))?;
    let indices = frequencies
        .iter()
        .map(|(&id, &frequencies)| {
            (
                id,
                frequencies
                    .iter()
                    .map(|&value| grid.index(value))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if indices
        .values()
        .flatten()
        .copied()
        .collect::<BTreeSet<_>>()
        .len()
        < count
    {
        return Err(error(
            "not enough spectral channels for the requested degree",
        ));
    }

    let basis = (0..grid.count)
        .map(|channel| {
            let mut values = vec![0.0; count];
            let [first, last] = grid.domain();
            legacy_bpoly_basis(first, last, grid.center(channel), &mut values);
            values
        })
        .collect::<Vec<_>>();
    let antenna_indices = antennas
        .iter()
        .enumerate()
        .map(|(i, &id)| (id, i))
        .collect::<BTreeMap<_, _>>();
    let (spectra, participation) = collect_spectra(group, &indices, &grid, &antenna_indices, cuts);
    if spectra.iter().all(Vec::is_empty) {
        return Ok(Vec::new());
    }
    let time = group
        .polynomial
        .as_ref()
        .and_then(|accumulation| accumulation.time())
        .unwrap_or_else(|| group.aggregate_time());
    let mut rows = antennas
        .iter()
        .map(|&antenna_id| BPolySolutionRow {
            time_seconds: (time / 86_400.0) * 86_400.0,
            interval_seconds: 0.0,
            field_id: group.field_id,
            spw_id: group.spw_id,
            antenna_id,
            scan_number: -1,
            observation_id: -1,
            valid_domain_hz: grid.domain(),
            scale_factor: Complex32::new(1.0, 0.0),
            amp_coefficients: vec![vec![0.0; amp_count]; 2],
            phase_coefficients: vec![vec![0.0; phase_count]; 2],
        })
        .collect::<Vec<_>>();

    for (receptor, samples) in spectra.iter().enumerate() {
        if samples.is_empty() {
            continue;
        }
        let active = samples
            .iter()
            .flat_map(|sample| sample.antennas)
            .collect::<BTreeSet<_>>();
        let phase_reference = antenna_indices
            .get(&refant)
            .filter(|index| active.contains(index))
            .or_else(|| active.first())
            .copied();
        let amplitudes = fit_coefficients(samples, &basis, &active, amp_count, None)
            .ok_or_else(|| error("amplitude polynomial fit is singular"))?;
        let phases = fit_coefficients(samples, &basis, &active, phase_count, phase_reference)
            .ok_or_else(|| error("phase polynomial fit is singular"))?;
        for (&index, amplitude) in active.iter().zip(amplitudes.chunks(amp_count)) {
            rows[index].amp_coefficients[receptor].copy_from_slice(amplitude);
        }
        for (&index, phase) in active
            .iter()
            .filter(|&&index| Some(index) != phase_reference)
            .zip(phases.chunks(phase_count))
        {
            rows[index].phase_coefficients[receptor].copy_from_slice(phase);
        }
        if request.normalize_average_amplitude {
            for &index in &active {
                normalize_output(&mut rows[index], receptor, &basis, &participation[index]);
            }
        }
    }
    Ok(rows)
}

struct FrequencyGrid {
    first: f64,
    step: f64,
    count: usize,
}

impl FrequencyGrid {
    fn new(frequencies: &BTreeMap<i32, &[f64]>) -> Option<Self> {
        let step = frequencies
            .values()
            .filter_map(|channels| Some((channels.get(1)? - channels.first()?).abs()))
            .reduce(f64::max)?;
        let values = frequencies
            .values()
            .flat_map(|channels| channels.iter().copied());
        let first = values.clone().reduce(f64::min)?;
        let last = values.reduce(f64::max)?;
        (step.is_finite() && step > 0.0 && first.is_finite() && last.is_finite()).then(|| Self {
            first,
            step,
            count: ((last - first) / step + 1.5) as usize,
        })
    }

    fn index(&self, frequency: f64) -> usize {
        (((frequency - self.first) / self.step + 0.5) as usize).min(self.count - 1)
    }

    fn center(&self, channel: usize) -> f64 {
        self.first + channel as f64 * self.step
    }

    fn domain(&self) -> [f64; 2] {
        [self.first, self.center(self.count - 1)]
    }
}

fn collect_spectra(
    group: &SolveAccumulator,
    bins: &BTreeMap<i32, Vec<usize>>,
    grid: &FrequencyGrid,
    antennas: &BTreeMap<i32, usize>,
    cuts: BandpassSolveCuts,
) -> ([Vec<VisibilitySample>; 2], Vec<Vec<usize>>) {
    let mut sums = [BTreeMap::new(), BTreeMap::new()];
    let mut means = [BTreeMap::new(), BTreeMap::new()];
    let mut participation = vec![vec![0; grid.count]; antennas.len()];
    for (description, [a, b], receptor, spectrum) in
        group.polynomial.iter().flat_map(|data| data.spectra())
    {
        let bins = &bins[&description];
        let (i, j) = (antennas[&a], antennas[&b]);
        let weight = f64::from(spectrum.weight);
        for (channel, (&bin, &sample)) in
            bins.iter().zip(&spectrum.weighted_visibilities).enumerate()
        {
            if cuts.masks_polynomial_channel(channel, bins.len()) || sample.norm_sqr() == 0.0 {
                continue;
            }
            let sample = Complex64::new(f64::from(sample.re), f64::from(sample.im));
            let (sum, total) = sums[receptor]
                .entry((i, j, bin))
                .or_insert((Complex64::default(), 0.0));
            *sum += sample;
            *total += weight;
            if cuts.normalize_visibilities {
                let (sum, total) = means[receptor]
                    .entry((i, j))
                    .or_insert((Complex64::default(), 0.0));
                *sum += sample;
                *total += weight;
            }
            participation[i][bin] += 1;
            participation[j][bin] += 1;
        }
    }
    let [first, second] = sums;
    let [first_means, second_means] = means;
    let spectra = [(first, first_means), (second, second_means)].map(|(sum, means)| {
        sum.into_iter()
            .filter_map(|((a, b, channel), (visibility, weight))| {
                let visibility = if let Some(&(sum, norm_weight)) = means.get(&(a, b)) {
                    if sum.norm_sqr() == 0.0 {
                        return None;
                    }
                    visibility / (sum / norm_weight)
                } else {
                    visibility
                };
                (participation[a][channel] > 1
                    && participation[b][channel] > 1
                    && visibility.norm_sqr() > 0.0)
                    .then_some(VisibilitySample {
                        antennas: [a, b],
                        channel,
                        visibility: visibility / weight,
                        weight,
                    })
            })
            .collect::<Vec<_>>()
    });
    (spectra, participation)
}

fn fit_coefficients(
    samples: &[VisibilitySample],
    bases: &[Vec<f64>],
    active: &BTreeSet<usize>,
    count: usize,
    phase_reference: Option<usize>,
) -> Option<Vec<f64>> {
    let columns = active
        .iter()
        .filter(|&&index| Some(index) != phase_reference)
        .enumerate()
        .map(|(column, &index)| (index, column * count))
        .collect::<BTreeMap<_, _>>();
    let size = columns.len() * count;
    if size == 0 {
        return Some(Vec::new());
    }
    let mut normal = vec![0.0; size * size];
    let mut right = vec![0.0; size];
    let mut phase_history = BTreeMap::new();
    for sample in samples {
        let basis = &bases[sample.channel][..count];
        let signs = [1.0, if phase_reference.is_some() { -1.0 } else { 1.0 }];
        let terms = sample
            .antennas
            .into_iter()
            .zip(signs)
            .filter_map(|(antenna, sign)| columns.get(&antenna).map(|&offset| (offset, sign)));
        let target = if phase_reference.is_some() {
            let phase = sample.visibility.arg();
            let previous = phase_history.entry(sample.antennas).or_insert(phase);
            *previous += (phase - *previous + PI).rem_euclid(TAU) - PI;
            *previous
        } else {
            sample.visibility.norm().ln()
        };
        for (left, left_sign) in terms.clone() {
            for (i, &x) in basis.iter().enumerate() {
                let weighted = sample.weight * left_sign * x;
                right[left + i] += weighted * target;
                for (column, sign) in terms.clone() {
                    for (j, &y) in basis.iter().enumerate() {
                        normal[(left + i) * size + column + j] += weighted * sign * y;
                    }
                }
            }
        }
    }
    let inverse = invert_symmetric_positive_definite(&normal, size)?;
    Some(inverse.chunks(size).map(|row| dot(row, &right)).collect())
}

fn normalize_output(
    row: &mut BPolySolutionRow,
    receptor: usize,
    bases: &[Vec<f64>],
    participation: &[usize],
) {
    let amplitude = &mut row.amp_coefficients[receptor];
    let phase = &mut row.phase_coefficients[receptor];
    let mut total_amplitude = 0.0;
    let mut phasor = Complex64::default();
    let mut count = 0;
    for (basis, &coverage) in bases.iter().zip(participation) {
        if coverage > 3 {
            total_amplitude += dot(amplitude, basis).exp();
            phasor += Complex64::from_polar(1.0, dot(phase, basis));
            count += 1;
        }
    }
    if count > 0 {
        amplitude[0] -= 2.0 * (total_amplitude / f64::from(count)).ln();
        phase[0] -= 2.0 * phasor.arg();
    }
}

fn dot(first: &[f64], second: &[f64]) -> f64 {
    first.iter().zip(second).map(|(a, b)| a * b).sum()
}
