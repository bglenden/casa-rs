// SPDX-License-Identifier: LGPL-3.0-or-later
//! Leakage Jones terms on four-product visibility matrices.

use super::*;

#[derive(Clone, Copy)]
pub(super) enum Leakage {
    Linearized,
    Full,
}

impl Leakage {
    pub(super) fn from_subtype(subtype: &str) -> Option<Self> {
        match subtype {
            "D Jones" | "Df Jones" => Some(Self::Linearized),
            "Dgen Jones" | "Dfgen Jones" | "DLLS Jones" | "DfLLS Jones" => Some(Self::Full),
            _ => None,
        }
    }

    pub(super) fn is_channelized(subtype: &str) -> bool {
        matches!(subtype, "Df Jones" | "Dfgen Jones" | "DfLLS Jones")
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
            let left = [sample(0, 0)?, sample(0, 1)?];
            let right = [sample(1, 0)?, sample(1, 1)?];
            let values = indices.map(|index| output.corrected[[index, channel]]);
            let propagate = output.apply_mode.writes_flags();
            let flags = indices.map(|index| propagate && output.flags[[index, channel]]);
            let (values, flags) = match self {
                Self::Linearized => linearized(values, flags, left, right, forward),
                Self::Full => full(values, flags, left, right, forward),
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

fn coherency_order(ddid: i32, correlations: &[i32]) -> Result<[usize; 4], ApplyExecutionError> {
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
    left: [GainSample; 2],
    right: [GainSample; 2],
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

fn full_matrix(terms: [GainSample; 2], forward: bool) -> ([Complex32; 4], bool) {
    let zero = Complex32::new(0.0, 0.0);
    let one = Complex32::new(1.0, 0.0);
    let [x, y] = terms.map(|sample| sample.value);
    let det = one - x * y;
    if terms.into_iter().any(|sample| sample.flagged) || (!forward && det.norm() == 0.0) {
        ([one, zero, zero, one], true)
    } else if forward {
        ([one, x, y, one], false)
    } else {
        ([one / det, -x / det, -y / det, one / det], false)
    }
}

fn product([a, b, c, d]: [Complex32; 4], [e, f, g, h]: [Complex32; 4]) -> [Complex32; 4] {
    [a * e + b * g, a * f + b * h, c * e + d * g, c * f + d * h]
}
