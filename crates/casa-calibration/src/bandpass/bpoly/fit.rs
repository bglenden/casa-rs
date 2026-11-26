// SPDX-License-Identifier: LGPL-3.0-or-later
//! Fit antenna polynomials to weighted baseline spectra.

use std::collections::{BTreeMap, BTreeSet};

use casa_core::Complex32;
use casa_numerics::invert_symmetric_positive_definite;
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
    frequencies: &BTreeMap<i32, (&[f64], f64)>,
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
    let degrees = [request.amplitude_degree, request.phase_degree];
    let [amp_count, phase_count] = std::array::from_fn(|index| {
        if cuts.omit_polynomial[index] {
            0
        } else {
            degrees[index] + 1
        }
    });
    let count = amp_count.max(phase_count);
    let grid = FrequencyGrid::new(frequencies)
        .ok_or_else(|| error("at least two distinct channel frequencies are required"))?;
    let mut indices = frequencies
        .iter()
        .map(|(&id, &(frequencies, _))| {
            (
                id,
                frequencies
                    .iter()
                    .map(|&value| grid.index(value))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let occupied = indices.values().flatten().copied().collect::<BTreeSet<_>>();
    if occupied.len() < count {
        return Err(error(
            "not enough spectral channels for the requested degree",
        ));
    }

    // The logical grid can span a wide gap between spectral windows.
    // Only store basis and participation entries for selected channels.
    let basis = occupied
        .iter()
        .copied()
        .map(|channel| {
            let mut values = vec![0.0; count];
            let [first, last] = grid.domain();
            legacy_bpoly_basis(first, last, grid.center(channel), &mut values);
            values
        })
        .collect::<Vec<_>>();
    let compact = occupied
        .into_iter()
        .enumerate()
        .map(|(index, slot)| (slot, index))
        .collect::<BTreeMap<_, _>>();
    for slot in indices.values_mut().flatten() {
        *slot = compact[slot];
    }
    let antenna_indices = antennas
        .iter()
        .enumerate()
        .map(|(i, &id)| (id, i))
        .collect::<BTreeMap<_, _>>();
    let (spectra, participation) =
        collect_spectra(group, &indices, basis.len(), &antenna_indices, cuts);
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
        if samples
            .iter()
            .map(|sample| sample.channel)
            .collect::<BTreeSet<_>>()
            .len()
            < count
        {
            tracing::warn!(
                "{}",
                error("too few unflagged frequencies for a polynomial fit")
            );
            return Ok(Vec::new());
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
        let Some((amplitudes, phases)) =
            fit_coefficients(samples, &basis, &active, amp_count, None).zip(fit_coefficients(
                samples,
                &basis,
                &active,
                phase_count,
                phase_reference,
            ))
        else {
            tracing::warn!("{}", error("normal equations are singular"));
            return Ok(Vec::new());
        };
        for (&index, amplitude) in active.iter().zip(amplitudes.chunks(amp_count.max(1))) {
            rows[index].amp_coefficients[receptor].copy_from_slice(amplitude);
        }
        for (&index, phase) in active
            .iter()
            .filter(|&&index| Some(index) != phase_reference)
            .zip(phases.chunks(phase_count.max(1)))
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
    fn new(frequencies: &BTreeMap<i32, (&[f64], f64)>) -> Option<Self> {
        let step = frequencies
            .values()
            .map(|&(channels, spacing)| {
                channels
                    .get(1)
                    .map_or(spacing, |second| (second - channels[0]).abs())
            })
            .reduce(f64::max)?;
        let values = frequencies
            .values()
            .flat_map(|(channels, _)| channels.iter().copied());
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
    occupied_channel_count: usize,
    antennas: &BTreeMap<i32, usize>,
    cuts: BandpassSolveCuts,
) -> ([Vec<VisibilitySample>; 2], Vec<Vec<usize>>) {
    let mut sums = [BTreeMap::new(), BTreeMap::new()];
    let mut means = [BTreeMap::new(), BTreeMap::new()];
    let mut participation = vec![vec![0; occupied_channel_count]; antennas.len()];
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
    let phase = phase_reference.is_some();
    let signs = [1.0, if phase { -1.0 } else { 1.0 }];
    let samples = samples
        .iter()
        .map(|sample| FitSample {
            basis: &bases[sample.channel][..count],
            terms: sample
                .antennas
                .map(|antenna| columns.get(&antenna).copied()),
            weight: sample.weight,
            observed: if phase {
                sample.visibility.arg()
            } else {
                sample.visibility.norm().ln()
            },
        })
        .collect::<Vec<_>>();
    let mut normal = vec![0.0; size * size];
    for sample in &samples {
        for (left, left_sign) in sample.terms(signs) {
            for (i, &x) in sample.basis.iter().enumerate() {
                let weighted = sample.weight * left_sign * x;
                for (column, sign) in sample.terms(signs) {
                    for (j, &y) in sample.basis.iter().enumerate() {
                        normal[(left + i) * size + column + j] += weighted * sign * y;
                    }
                }
            }
        }
    }
    let inverse = invert_symmetric_positive_definite(&normal, size)?;
    let mut coefficients = vec![0.0; size];
    let mut right = vec![0.0; size];
    // CLIC POLYANT starts phase coefficients at zero and uses the sine
    // of the baseline residual. Its normal matrix is constant across steps.
    for _ in 0..if phase { 100 } else { 1 } {
        right.fill(0.0);
        for sample in &samples {
            let residual = if phase {
                let predicted = sample
                    .terms(signs)
                    .map(|(offset, sign)| {
                        sign * dot(&coefficients[offset..offset + count], sample.basis)
                    })
                    .sum::<f64>();
                (sample.observed - predicted).sin()
            } else {
                sample.observed
            };
            for (offset, sign) in sample.terms(signs) {
                for (index, &basis) in sample.basis.iter().enumerate() {
                    right[offset + index] += sign * sample.weight * basis * residual;
                }
            }
        }
        let mut update_norm = 0.0;
        for (coefficient, row) in coefficients.iter_mut().zip(inverse.chunks(size)) {
            let update = dot(row, &right);
            *coefficient += update;
            update_norm += update * update;
        }
        if update_norm <= 1e-14 {
            break;
        }
    }
    Some(coefficients)
}

struct FitSample<'a> {
    basis: &'a [f64],
    terms: [Option<usize>; 2],
    weight: f64,
    observed: f64,
}

impl FitSample<'_> {
    fn terms(&self, signs: [f64; 2]) -> impl Iterator<Item = (usize, f64)> {
        self.terms
            .into_iter()
            .zip(signs)
            .filter_map(|(offset, sign)| offset.map(|offset| (offset, sign)))
    }
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
        if let Some(offset) = amplitude.first_mut() {
            *offset -= 2.0 * (total_amplitude / f64::from(count)).ln();
        }
        if let Some(offset) = phase.first_mut() {
            *offset -= 2.0 * phasor.arg();
        }
    }
}

fn dot(first: &[f64], second: &[f64]) -> f64 {
    first.iter().zip(second).map(|(a, b)| a * b).sum()
}
