// SPDX-License-Identifier: LGPL-3.0-or-later
//! Saved PSF beam publication laws.

use super::*;

/// A saved PSF beam grid must cover every plane it will publish.
#[test]
fn an_incomplete_saved_psf_beam_grid_cannot_be_published() {
    let problem = continuum_problem(81, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join);
    assert!(matches!(
        inputs.with_saved_psf_beams(&[]),
        Err(ProductsError::BeamGrid {
            beams: 0,
            channels: 1
        })
    ));
}
