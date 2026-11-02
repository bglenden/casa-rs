// SPDX-License-Identifier: LGPL-3.0-or-later
//! Repeated compact contributions through every CPU spread loop (#991).
use super::*;
use crate::convolution::DenseCell;

fn repeated(taps: &TapLayout<'_>, gradient: [f32; 2]) {
    let tile = Tile::full([16, 16]);
    let location = CellLocation {
        x: 8,
        y: 8,
        ox: 0,
        oy: 0,
    };
    let mut grid = vec![Complex32::default(); 256];
    let mut low = vec![Complex32::default(); 256];
    let mut reference = vec![Complex64::default(); 256];
    for _ in 0..100_000 {
        spread::<f32, true>(
            &mut grid,
            &mut low,
            tile,
            location,
            taps,
            0,
            true,
            gradient,
            Complex32::new(0.3, -0.2),
        );
        spread::<f64, false>(
            &mut reference,
            &mut [],
            tile,
            location,
            taps,
            0,
            true,
            gradient,
            Complex64::new(f64::from(0.3_f32), f64::from(-0.2_f32)),
        );
    }
    let error: f64 = grid
        .iter()
        .zip(&reference)
        .map(|(got, expected)| {
            (Complex64::new(f64::from(got.re), f64::from(got.im)) - expected).norm_sqr()
        })
        .sum();
    let norm: f64 = reference.iter().map(|value| value.norm_sqr()).sum();
    assert!(
        (error / norm).sqrt() < 1.0e-6,
        "relative error {}",
        (error / norm).sqrt()
    );
}

#[test]
fn repeated_compact_separable_dynamic_and_dense_ramped_match_f64() {
    for support in [3, 7] {
        let rows = vec![0.137_f32; support * 2];
        repeated(
            &TapLayout::SeparableReal {
                rows: &rows,
                support: support as u16,
                oversampling: 1,
            },
            [0.0; 2],
        );
    }
    let dense = DenseCell::new(
        vec![Complex32::new(0.17, -0.13); 3 * 3 * 4].into_boxed_slice(),
        [3, 3],
        1,
        1,
    );
    for gradient in [[0.0, 0.0], [0.3, -0.2]] {
        repeated(&dense.layout(), gradient);
    }
}
