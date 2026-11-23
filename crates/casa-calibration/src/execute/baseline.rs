// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA M/Mf Mueller corrections have one two-hand response per antenna pair.

use super::*;

pub(crate) fn is_subtype(subtype: &str) -> bool {
    matches!(subtype, "M Mueller" | "Mf Mueller" | "A Mueller")
}

/// A stable triangular index, independent of MS antenna count and row order.
pub(crate) fn index(p: i32, q: i32) -> Option<i32> {
    let first = i64::from(p.min(q));
    let last = i64::from(p.max(q));
    if first < 0 {
        return None;
    }
    i32::try_from(last * (last + 1) / 2 + first).ok()
}

pub(crate) fn require_index(p: i32, q: i32, path: &Path) -> Result<i32, ApplyExecutionError> {
    index(p, q).ok_or_else(|| ApplyExecutionError::UnsupportedCalibrationTable {
        path: path.display().to_string(),
        reason: format!("baseline antenna coordinates ({p}, {q}) are out of range"),
    })
}

pub(super) fn prepare(
    solution: Option<fields::GridAtTime>,
    input: &[f64],
    output: &crate::plan::SpectralWindowPlan,
    cache: Option<&mut RowGeometryCache>,
    table: &LoadedCalibrationTable,
) -> Result<Option<Arc<CalibrationGrid>>, ApplyExecutionError> {
    let Some(solution) = solution else {
        return Ok(None);
    };
    match solution.grid.as_ref() {
        CalibrationGrid::Complex(grid) if grid.channel_count <= 1 => Ok(Some(solution.grid)),
        CalibrationGrid::Complex(_) => bandpass::prepare(
            solution.grid,
            input,
            output,
            cache,
            table,
            solution.interpolated,
        )
        .map(Some),
        _ => Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: table.path.display().to_string(),
            reason: "baseline calibration requires CPARAM".into(),
        }),
    }
}

pub(super) fn correlation_count(output: &ApplyBuffer<'_>) -> Result<usize, ApplyExecutionError> {
    let correlations = output.correlation_types.len();
    if !matches!(correlations, 1 | 2 | 4) {
        return Err(ApplyExecutionError::UnsupportedCorrelationLayout {
            data_desc_id: output.data_desc_id,
            correlation_types: output
                .correlation_types
                .iter()
                .map(|code| stokes_name(*code).to_string())
                .collect(),
        });
    }
    Ok(correlations)
}

pub(super) fn apply(
    grid: Option<&CalibrationGrid>,
    path: &Path,
    reversed: bool,
    forward_model: bool,
    output: &mut ApplyBuffer<'_>,
) -> Result<(), ApplyExecutionError> {
    let grid = grid.map(|grid| match grid {
        CalibrationGrid::Complex(grid) => grid,
        _ => unreachable!("baseline grid was prepared as CPARAM"),
    });
    let channels = output.corrected.shape()[1];
    let correlations = correlation_count(output)?;
    for corr in 0..correlations {
        if correlations == 4 && matches!(corr, 1 | 2) {
            // Diag2 Mueller has no cross-hand response.
            for chan in 0..channels {
                output.corrected[[corr, chan]] = Complex32::ZERO;
            }
            continue;
        }
        for chan in 0..channels {
            let sample = match grid {
                Some(grid) => grid.sample(
                    corr % 2,
                    chan,
                    output.data_channels,
                    &[],
                    ApplyInterpolationMode::Nearest,
                    path,
                )?,
                None => GainSample {
                    value: Complex32::ONE,
                    flagged: true,
                },
            };
            let (value, flagged) = if forward_model {
                gain_validity::forward_gain(sample.value, sample.flagged)
            } else {
                correcting_gain(sample.value, sample.flagged)
            };
            let value = if reversed { value.conj() } else { value };
            let data = &mut output.corrected[[corr, chan]];
            if forward_model {
                *data *= value;
            } else {
                *data /= value;
            }
            if flagged && output.propagate_flags && !output.flags[[corr, chan]] {
                output.flags[[corr, chan]] = true;
                *output.newly_flagged_samples += 1;
            }
        }
    }
    Ok(())
}
