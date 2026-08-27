// SPDX-License-Identifier: LGPL-3.0-or-later
#![cfg(all(feature = "cpp-interop-tests", has_casacore_cpp))]

use casa_test_support::hogbom_interop::HogbomOracle;

#[test]
fn recentres_x_major_planes_and_runs_exactly_the_requested_iterations() {
    let shape = [6, 5];
    let component = [1, 3];
    let index = component[0] * shape[1] + component[1];
    let mut psf = vec![0.0_f32; shape[0] * shape[1]];
    let mut residual = vec![0.0_f32; shape[0] * shape[1]];
    psf[index] = 1.0;
    residual[index] = 1.0;

    let result = HogbomOracle::clean_minor_cycle_2d(&psf, &residual, shape, 0.5, 0.0, 2)
        .expect("casacore Högbom oracle");

    assert_eq!(result.iterdone, 2);
    assert_eq!(result.model[index], 0.75);
    assert_eq!(result.residual[index], 0.25);
    assert_eq!(result.peak_residual_jy_per_beam, 0.25);
    assert!(
        result
            .model
            .iter()
            .enumerate()
            .all(|(position, value)| position == index || *value == 0.0)
    );
    assert!(
        result
            .residual
            .iter()
            .enumerate()
            .all(|(position, value)| position == index || *value == 0.0)
    );
}
