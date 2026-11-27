// SPDX-License-Identifier: LGPL-3.0-or-later
//! Human-readable legacy fit summaries next to a GSPLINE calibration table.

use std::f64::consts::{PI, TAU};
use std::io::{self, Write};
use std::path::Path;

use super::{BaselineSeries, SampleBasis, SplineCoefficients, basis, usable};

pub(super) fn write_logs(
    output: &Path,
    times: &[f64],
    baselines: &[BaselineSeries],
    fit: &SplineCoefficients,
    hands: usize,
) {
    let sample_basis = times.iter().map(|&time| basis::sample(&fit.knots, time));
    let Some(samples) = sample_basis.collect::<Option<Vec<_>>>() else {
        return;
    };
    for (suffix, term, phase) in [
        ("AMP", fit.log_amplitude.as_ref(), false),
        ("PHASE", fit.phase.as_ref(), true),
    ] {
        let Some(term) = term else { continue };
        for (hand, coefficients) in term.iter().enumerate().take(hands) {
            let write =
                |file: &mut _| write_summary(file, &samples, baselines, coefficients, hand, phase);
            // CASA treats the sidecar as diagnostic. A failed log write does
            // not invalidate the calibration table, including on append.
            let _ = write_log(output, &format!(".{suffix}.pol{hand}.log"), write);
        }
    }
}

fn write_log(
    output: &Path,
    suffix: &str,
    write: impl FnOnce(&mut std::fs::File) -> io::Result<()>,
) -> io::Result<()> {
    let Some(name) = output.file_name() else {
        return Ok(());
    };
    let mut name = name.to_os_string();
    name.push(suffix);
    let path = output.with_file_name(name);
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    let mut staged = tempfile::NamedTempFile::new_in(directory.unwrap_or(Path::new(".")))?;
    write(staged.as_file_mut())?;
    staged.persist(path)?;
    Ok(())
}

fn write_summary(
    file: &mut impl Write,
    basis: &[SampleBasis],
    baselines: &[BaselineSeries],
    coefficients: &[Vec<f64>],
    hand: usize,
    phase: bool,
) -> io::Result<()> {
    let label = if phase { "PHASE" } else { "AMPLITUDE" };
    writeln!(file, "*****{label} summary********")?;
    writeln!(file, "Antenna based spline coefficients ")?;
    for (antenna, controls) in coefficients.iter().enumerate() {
        write!(file, "  {}   [", antenna + 1)?;
        for value in controls {
            write!(file, "{value:.8e}, ")?;
        }
        writeln!(file, "0, 0, 0, 0]")?;
    }
    writeln!(file, " RMS Fit per baseline ")?;
    writeln!(file, "Antenna1      Antenna2      rmsfit")?;
    let mut series = baselines.iter().peekable();
    for first in 0..coefficients.len() {
        for second in first + 1..coefficients.len() {
            let rms = series
                .next_if(|baseline| baseline.antennas == [first, second])
                .map_or(0., |baseline| {
                    rms_fit(
                        baseline,
                        basis,
                        [&coefficients[first], &coefficients[second]],
                        hand,
                        phase,
                    )
                });
            writeln!(
                file,
                "   {}           {}            {rms:.8e}",
                first + 1,
                second + 1
            )?;
        }
    }
    Ok(())
}

fn rms_fit(
    baseline: &BaselineSeries,
    basis: &[SampleBasis],
    controls: [&[f64]; 2],
    hand: usize,
    phase: bool,
) -> f64 {
    let mut squared = 0.;
    let mut weight = 0.;
    for (sample, basis) in baseline.samples.iter().map(|row| row[hand]).zip(basis) {
        if !usable(sample) {
            continue;
        }
        let measured = sample.value(phase);
        let sign = if phase { -1. } else { 1. };
        let predicted = basis
            .weights
            .iter()
            .enumerate()
            .map(|(offset, blend)| {
                let index = basis.start + offset;
                (sign * controls[0][index] + controls[1][index]) * blend
            })
            .sum::<f64>();
        let residual = (measured - predicted + PI).rem_euclid(TAU) - PI;
        squared += sample.weight * residual * residual;
        weight += sample.weight;
    }
    if weight > 0. {
        (squared / weight).sqrt()
    } else {
        0.
    }
}
