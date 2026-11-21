// SPDX-License-Identifier: LGPL-3.0-or-later
//! Full 2×2 D Jones application and differentiation.

use num_complex::Complex32;

type Coherency = [Complex32; 4];

pub(super) fn left(model: Coherency, [dx, dy]: [Complex32; 2]) -> Coherency {
    let [xx, xy, yx, yy] = model;
    [xx + dx * yx, xy + dx * yy, yx + dy * xx, yy + dy * xy]
}

pub(super) fn right(model: Coherency, [dx, dy]: [Complex32; 2]) -> Coherency {
    let [xx, xy, yx, yy] = model;
    let [dx, dy] = [dx.conj(), dy.conj()];
    [xx + dx * xy, xy + dy * xx, yx + dx * yy, yy + dy * yx]
}

pub(super) fn derivative(partial: Coherency, side: usize, receptor: usize) -> Coherency {
    let mut derivative = [Complex32::ZERO; 4];
    if side == 0 {
        let row = 2 * receptor;
        let other = 2 - row;
        derivative[row..row + 2].copy_from_slice(&partial[other..other + 2]);
    } else {
        for row in 0..2 {
            derivative[2 * row + receptor] = partial[2 * row + (1 - receptor)];
        }
    }
    derivative
}
