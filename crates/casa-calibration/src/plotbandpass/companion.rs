// SPDX-License-Identifier: LGPL-3.0-or-later
//! Match secondary bandpass solutions to the selected spectrum.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::spectrum::{Curve, Ordinate};
use super::table::{SolutionRow, Solutions};
use super::{PlotRequest, SelectedRow, polynomial_degrees, spectral_window};

pub(super) struct Companion {
    source: Solutions,
    by_antenna_window: BTreeMap<(i32, i32), Vec<usize>>,
    epochs: Vec<f64>,
}

impl Companion {
    pub(super) fn open(path: &Path, vis: Option<&Path>, tolerance: f64) -> Result<Self, String> {
        let source = Solutions::open(path, vis)?;
        let mut by_antenna_window = BTreeMap::<_, Vec<_>>::new();
        for (index, row) in source.rows.iter().enumerate() {
            let original = spectral_window(&source, row.window)?.original_id;
            by_antenna_window
                .entry((row.antenna, original))
                .or_default()
                .push(index);
        }
        let mut available = source.rows.iter().map(|row| row.time).collect::<Vec<_>>();
        available.sort_unstable_by(f64::total_cmp);
        available.dedup();
        let mut epochs = Vec::new();
        for time in available {
            if epochs
                .last()
                .is_none_or(|previous: &f64| time - *previous >= tolerance)
            {
                epochs.push(time);
            }
        }
        Ok(Self {
            source,
            by_antenna_window,
            epochs,
        })
    }

    pub(super) fn polynomial(&self) -> bool {
        self.source.subtype == "BPOLY"
    }

    pub(super) fn degrees(&self) -> Option<[usize; 2]> {
        polynomial_degrees(&self.source)
    }

    pub(super) fn match_row(
        &self,
        primary: SolutionRow,
        original_window: i32,
        time_index: usize,
        tolerance: f64,
    ) -> Option<usize> {
        let epoch = if self.polynomial() {
            primary.time
        } else {
            *self.epochs.get(time_index)?
        };
        self.by_antenna_window
            .get(&(primary.antenna, original_window))?
            .iter()
            .copied()
            .filter(|&index| {
                let peer = self.source.rows[index];
                primary.field == peer.field
                    && (epoch == peer.time || (epoch - peer.time).abs() < tolerance)
            })
            .min_by(|&left, &right| {
                let difference = |index: usize| (self.source.rows[index].time - epoch).abs();
                difference(left).total_cmp(&difference(right))
            })
    }

    pub(super) fn check_time_count(&self, requested: usize) -> Result<(), String> {
        if !self.polynomial() {
            return Ok(());
        }
        let times = self
            .source
            .rows
            .iter()
            .map(|row| row.time.to_bits())
            .collect::<BTreeSet<_>>();
        if requested > times.len() {
            return Err(
                "cannot access local variable 'mytime' where it is not associated with a value"
                    .into(),
            );
        }
        Ok(())
    }

    pub(super) fn curve(
        &self,
        row: usize,
        receptor: usize,
        ordinate: Ordinate,
        primary: &Curve,
        request: &PlotRequest<'_>,
    ) -> Result<(Curve, usize), String> {
        let window = spectral_window(&self.source, self.source.rows[row].window)?;
        let frequencies = window
            .sky_frequencies_hz
            .iter()
            .map(|hz| hz * 1e-9)
            .collect::<Vec<_>>();
        let samples = self.source.spectrum(row)?;
        let [_, channels] = samples.shape();
        let mut curve =
            samples.curve(receptor, Some(&frequencies), ordinate, request.show_flagged)?;
        if self.polynomial() {
            normalize_polynomial(&mut curve, primary, ordinate);
        }
        if request.derivative_sigma > 0.0 && !curve.y.is_empty() {
            curve = curve
                .differentiate(ordinate == Ordinate::Amplitude, request.edge)
                .curve;
        }
        Ok((curve, channels))
    }
}

pub(super) fn requested_times(
    groups: impl Iterator<Item = SelectedRow>,
    request: &PlotRequest<'_>,
    available: usize,
) -> usize {
    if request.selectors.time.iter().all(String::is_empty) {
        return available;
    }
    groups
        .map(|row| row.time_index)
        .collect::<BTreeSet<_>>()
        .len()
}

fn normalize_polynomial(curve: &mut Curve, primary: &Curve, ordinate: Ordinate) {
    let mean = primary.y.iter().sum::<f64>() / primary.y.len() as f64;
    if ordinate == Ordinate::Phase {
        let first = primary.x[0];
        let last = primary.x[primary.x.len() - 1];
        let lower = first.min(last);
        let upper = first.max(last);
        let (real, imaginary) = curve
            .x
            .iter()
            .zip(&curve.y)
            .filter(|&(frequency, _)| *frequency > lower && *frequency < upper)
            .fold((0.0, 0.0), |(real, imaginary), (_, &phase)| {
                let (sin, cos) = phase.to_radians().sin_cos();
                (real + cos, imaginary + sin)
            });
        let offset = mean - imaginary.atan2(real).to_degrees();
        for value in &mut curve.y {
            *value += offset;
        }
    } else {
        for value in &mut curve.y {
            *value *= mean;
        }
    }
}
