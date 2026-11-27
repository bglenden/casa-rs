// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_fft::Fft2;
use num_complex::Complex64;

#[test]
fn complex_fft_maps_an_origin_impulse_to_a_constant_spectrum() {
    let mut fft = Fft2::<f64>::with_threads([3, 4], 2)
        .unwrap()
        .with_estimated_plan();
    let mut plane = [Complex64::new(0.0, 0.0); 12];
    plane[0] = Complex64::new(2.0, -1.0);
    fft.transform(&mut plane, false).unwrap();
    for value in plane {
        assert!((value - Complex64::new(2.0, -1.0)).norm() < 1.0e-12);
    }
}

#[cfg(target_os = "macos")]
mod plan_memory;
