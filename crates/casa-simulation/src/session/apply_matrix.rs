// SPDX-License-Identifier: LGPL-3.0-or-later
//! Supplied full polarized Jones matrices and full (second-order) leakage.
use super::*;
use casa_calibration::ComplexSolution;

pub(super) const IDENTITY: [Complex32; 4] = [
    Complex32::new(1., 0.),
    Complex32::new(0., 0.),
    Complex32::new(0., 0.),
    Complex32::new(1., 0.),
];

pub(super) fn parameters(
    kind: AppliedKind,
    solution: &ComplexSolution,
    channel: usize,
) -> [Complex32; 4] {
    // Jones::setMatByOk substitutes the whole identity if any matrix
    // element is invalid. Dgen first expands its two off-diagonal values.
    if (0..solution.receptors)
        .any(|parameter| solution.flags[parameter * solution.channels + channel])
    {
        return IDENTITY;
    }
    if kind == AppliedKind::FullLeakage {
        [
            IDENTITY[0],
            solution.value(0, channel),
            solution.value(1, channel),
            IDENTITY[3],
        ]
    } else {
        std::array::from_fn(|parameter| solution.value(parameter, channel))
    }
}
