// SPDX-License-Identifier: LGPL-3.0-or-later
//! Polarization calibration on visibility coherencies.

use super::*;

pub(super) fn is_cross_hand_phase(subtype: &str) -> bool {
    matches!(
        subtype,
        "X Jones" | "Xf Jones" | "Xparang Jones" | "Xfparang Jones"
    )
}

pub(super) fn apply_x_mueller(
    grid: Option<&CalibrationGrid>,
    path: &Path,
    forward: bool,
    output: &mut ApplyBuffer<'_>,
) -> Result<(), ApplyExecutionError> {
    let Some(grid) = grid else {
        return Ok(());
    };
    let CalibrationGrid::Complex(grid) = grid else {
        return Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: "X Mueller requires CPARAM".into(),
        });
    };
    // The legacy factory marks every computed matrix element usable.
    let value = if forward {
        grid.value_at(0, 0)
    } else {
        correcting_gain(grid.value_at(0, 0), false).0
    };
    for (index, &code) in output.correlation_types.iter().enumerate() {
        let gain = match code {
            6 | 10 => value,
            7 | 11 => value.conj(),
            _ => continue,
        };
        let mut cross_hand = output.corrected.index_axis_mut(ndarray::Axis(0), index);
        if forward {
            cross_hand.mapv_inplace(|sample| sample * gain);
        } else {
            cross_hand.mapv_inplace(|sample| sample / gain);
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(super) enum JonesMatrix {
    LinearizedLeakage,
    FullLeakage,
    General,
}

impl JonesMatrix {
    pub(super) fn from_subtype(subtype: &str) -> Option<Self> {
        match subtype {
            "D Jones" | "Df Jones" => Some(Self::LinearizedLeakage),
            "Dgen Jones" | "Dfgen Jones" | "DLLS Jones" | "DfLLS Jones" => Some(Self::FullLeakage),
            "J Jones" | "Jf Jones" => Some(Self::General),
            _ => None,
        }
    }

    pub(super) fn is_channelized(subtype: &str) -> bool {
        matches!(
            subtype,
            "Df Jones" | "Dfgen Jones" | "DfLLS Jones" | "Jf Jones"
        )
    }

    pub(super) fn parameter_shape(self) -> (usize, &'static str) {
        match self {
            Self::LinearizedLeakage | Self::FullLeakage => (2, "leakage"),
            Self::General => (4, "full Jones"),
        }
    }

    pub(super) fn apply(
        self,
        row: &ApplyRowPlan,
        grids: [&CalibrationGrid; 2],
        sampling: &CalibrationSamplingContext<'_>,
        output: &mut ApplyBuffer<'_>,
        forward: bool,
    ) -> Result<(), ApplyExecutionError> {
        let indices = coherency_order(row.data_desc_id, output.correlation_types)?;
        for channel in 0..output.corrected.shape()[1] {
            let sample = |side: usize, receptor| {
                grids[side].sample(
                    receptor,
                    channel,
                    row.field_id,
                    [row.antenna1, row.antenna2][side],
                    row.time_seconds,
                    sampling,
                )
            };
            let values = indices.map(|index| output.corrected[[index, channel]]);
            let propagate = output.propagate_flags;
            let flags = indices.map(|index| propagate && output.flags[[index, channel]]);
            let (values, flags) = match self {
                Self::LinearizedLeakage => linearized(
                    values,
                    flags,
                    [sample(0, 0)?, sample(0, 1)?],
                    [sample(1, 0)?, sample(1, 1)?],
                    forward,
                ),
                Self::FullLeakage => {
                    let unity = GainSample {
                        value: Complex32::ONE,
                        flagged: false,
                    };
                    full(
                        values,
                        flags,
                        [unity, sample(0, 0)?, sample(0, 1)?, unity],
                        [unity, sample(1, 0)?, sample(1, 1)?, unity],
                        forward,
                    )
                }
                Self::General => full(
                    values,
                    flags,
                    [sample(0, 0)?, sample(0, 1)?, sample(0, 2)?, sample(0, 3)?],
                    [sample(1, 0)?, sample(1, 1)?, sample(1, 2)?, sample(1, 3)?],
                    forward,
                ),
            };
            for (slot, index) in indices.into_iter().enumerate() {
                output.corrected[[index, channel]] = values[slot];
                if propagate && flags[slot] && !output.flags[[index, channel]] {
                    output.flags[[index, channel]] = true;
                    *output.newly_flagged_samples += 1;
                }
            }
        }
        Ok(())
    }
}

pub(super) fn apply_position_angle(
    row: &ApplyRowPlan,
    grids: [&CalibrationGrid; 2],
    sampling: &CalibrationSamplingContext<'_>,
    output: &mut ApplyBuffer<'_>,
    forward: bool,
) -> Result<(), ApplyExecutionError> {
    let correlations = output.correlation_types;
    let circular = correlations.iter().all(|code| matches!(code, 5..=8));
    let linear_order = (!circular)
        .then(|| coherency_order(row.data_desc_id, correlations))
        .transpose()?;
    let direction = if forward { 1.0 } else { -1.0 };
    let propagate = output.propagate_flags;
    for channel in 0..output.corrected.shape()[1] {
        let sample = |side: usize| {
            grids[side].sample(
                0,
                channel,
                row.field_id,
                [row.antenna1, row.antenna2][side],
                row.time_seconds,
                sampling,
            )
        };
        let [left, right] = [sample(0)?, sample(1)?];
        let angles = [left, right].map(|sample| {
            if sample.flagged {
                0.0
            } else {
                direction * sample.value.re
            }
        });
        let invalid = left.flagged || right.flagged;
        let input_flagged = propagate
            && linear_order.is_some_and(|indices| {
                indices
                    .into_iter()
                    .any(|index| output.flags[[index, channel]])
            });
        let mut flag = |index: usize| {
            if propagate && !output.flags[[index, channel]] {
                output.flags[[index, channel]] = true;
                *output.newly_flagged_samples += 1;
            }
        };
        if let Some(indices) = linear_order {
            let data = indices.map(|index| output.corrected[[index, channel]]);
            let rotated = parang::rotate_linear(data, angles.map(f32::sin_cos));
            for (index, value) in indices.into_iter().zip(rotated) {
                output.corrected[[index, channel]] = value;
                if invalid || input_flagged {
                    flag(index);
                }
            }
        } else {
            let diagonal = angles.map(|angle| {
                let phase = Complex32::from_polar(1.0, angle);
                [phase, phase.conj()]
            });
            for (index, &code) in correlations.iter().enumerate() {
                let (first, second) = if correlations.len() == 1 {
                    (0, 0)
                } else {
                    correlation_receptors(code).expect("circular correlation")
                };
                output.corrected[[index, channel]] *=
                    diagonal[0][first] * diagonal[1][second].conj();
                if invalid {
                    flag(index);
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn coherency_order(
    ddid: i32,
    correlations: &[i32],
) -> Result<[usize; 4], ApplyExecutionError> {
    for codes in [[5, 6, 7, 8], [9, 10, 11, 12]] {
        let indices = codes.map(|code| correlations.iter().position(|&value| value == code));
        if correlations.len() == 4
            && let [Some(a), Some(b), Some(c), Some(d)] = indices
        {
            return Ok([a, b, c, d]);
        }
    }
    Err(ApplyExecutionError::UnsupportedCorrelationLayout {
        data_desc_id: ddid,
        correlation_types: correlations
            .iter()
            .map(|&code| stokes_name(code).into())
            .collect(),
    })
}

fn linearized(
    [xx, mut xy, mut yx, yy]: [Complex32; 4],
    [fxx, fxy, fyx, fyy]: [bool; 4],
    left: [GainSample; 2],
    right: [GainSample; 2],
    forward: bool,
) -> ([Complex32; 4], [bool; 4]) {
    let terms = |pair: [GainSample; 2]| {
        if pair.iter().any(|sample| sample.flagged) {
            // JonesGenLin::setMatByOk substitutes unity for both elements.
            return [Complex32::new(1.0, 0.0); 2];
        }
        pair.map(|sample| if forward { sample.value } else { -sample.value })
    };
    let [lx, ly] = terms(left);
    let [rx, ry] = terms(right);
    xy += lx * yy;
    yx += ly * xx;
    xy += ry.conj() * xx;
    yx += rx.conj() * yy;
    let fxy = fxy || fxx || fyy || left[0].flagged || right[1].flagged;
    let fyx = fyx || fxx || fyy || left[1].flagged || right[0].flagged;
    ([xx, xy, yx, yy], [fxx, fxy, fyx, fyy])
}

fn full(
    data: [Complex32; 4],
    flags: [bool; 4],
    left: [GainSample; 4],
    right: [GainSample; 4],
    forward: bool,
) -> ([Complex32; 4], [bool; 4]) {
    let (left, left_flag) = full_matrix(left, forward);
    let (right, right_flag) = full_matrix(right, forward);
    let [a, b, c, d] = product(left, data);
    let [e, f, g, h] = right.map(|value| value.conj());
    let data = product([a, b, c, d], [e, g, f, h]);
    (
        data,
        [left_flag || right_flag || flags.into_iter().any(|flag| flag); 4],
    )
}

fn full_matrix(terms: [GainSample; 4], forward: bool) -> ([Complex32; 4], bool) {
    let values = terms.map(|sample| sample.value);
    let [a, b, c, d] = values;
    let det = a * d - b * c;
    let invertible = det.norm() > 0.0;
    if terms.into_iter().any(|sample| sample.flagged) || (!forward && !invertible) {
        (
            [
                Complex32::ONE,
                Complex32::ZERO,
                Complex32::ZERO,
                Complex32::ONE,
            ],
            true,
        )
    } else if forward {
        (values, false)
    } else {
        ([d / det, -b / det, -c / det, a / det], false)
    }
}

fn product([a, b, c, d]: [Complex32; 4], [e, f, g, h]: [Complex32; 4]) -> [Complex32; 4] {
    [a * e + b * g, a * f + b * h, c * e + d * g, c * f + d * h]
}
