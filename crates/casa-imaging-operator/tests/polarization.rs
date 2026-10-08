// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA correlation representation, polarization map and Stokes basis.

use casa_imaging_model::{CorrelationType as C, PolarizationCoordinate as P};
use casa_imaging_operator::{GridPolarization, OperatorError, PolarizationRouting};
use num_complex::Complex64;

fn c(re: f64, im: f64) -> Complex64 {
    Complex64::new(re, im)
}

#[test]
fn stokes_i_from_parallel_hands_sums_both_into_one_plane() {
    let routing =
        PolarizationRouting::compile(&[C::LinearXx, C::LinearYy], &[P::StokesI]).expect("routing");
    assert_eq!(routing.grid(), &[GridPolarization::StokesI]);
    assert_eq!(routing.pol_map(), &[Some(0), Some(0)]);
    assert_eq!(routing.to_requested(0, 0), c(1.0, 0.0));
    assert_eq!(routing.from_requested(0, 0), c(1.0, 0.0));
    assert_eq!(routing.sumwt_source(0), 0);
    let full = PolarizationRouting::compile(
        &[C::CircularRr, C::CircularRl, C::CircularLr, C::CircularLl],
        &[P::StokesI],
    )
    .expect("routing");
    assert_eq!(full.pol_map(), &[Some(0), None, None, Some(0)]);
}

#[test]
fn full_stokes_from_linear_feeds_uses_the_casacore_matrices() {
    let routing = PolarizationRouting::compile(
        &[C::LinearXx, C::LinearXy, C::LinearYx, C::LinearYy],
        &[P::StokesI, P::StokesQ, P::StokesU, P::StokesV],
    )
    .expect("routing");
    assert_eq!(routing.grid_pols(), 4);
    assert_eq!(routing.pol_map(), &[Some(0), Some(1), Some(2), Some(3)]);
    let to = |r: usize| {
        (0..4)
            .map(|g| routing.to_requested(r, g))
            .collect::<Vec<_>>()
    };
    assert_eq!(to(0), [c(0.5, 0.0), c(0.0, 0.0), c(0.0, 0.0), c(0.5, 0.0)]);
    assert_eq!(to(1), [c(0.5, 0.0), c(0.0, 0.0), c(0.0, 0.0), c(-0.5, 0.0)]);
    assert_eq!(to(2), [c(0.0, 0.0), c(0.5, 0.0), c(0.5, 0.0), c(0.0, 0.0)]);
    assert_eq!(to(3), [c(0.0, 0.0), c(0.0, -0.5), c(0.0, 0.5), c(0.0, 0.0)]);
    let from = |g: usize| {
        (0..4)
            .map(|r| routing.from_requested(g, r))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        from(0),
        [c(1.0, 0.0), c(1.0, 0.0), c(0.0, 0.0), c(0.0, 0.0)]
    );
    assert_eq!(
        from(1),
        [c(0.0, 0.0), c(0.0, 0.0), c(1.0, 0.0), c(0.0, 1.0)]
    );
    assert_eq!(
        from(2),
        [c(0.0, 0.0), c(0.0, 0.0), c(1.0, 0.0), c(0.0, -1.0)]
    );
    assert_eq!(
        from(3),
        [c(1.0, 0.0), c(-1.0, 0.0), c(0.0, 0.0), c(0.0, 0.0)]
    );
    // From ∘ To is the identity on Stokes vectors: Σ_g to[r][g]·from[g][s] = δ_rs.
    for r in 0..4 {
        for s in 0..4 {
            let product = (0..4)
                .map(|g| routing.to_requested(r, g) * routing.from_requested(g, s))
                .sum::<Complex64>();
            let expected = if r == s { c(1.0, 0.0) } else { c(0.0, 0.0) };
            assert!((product - expected).norm() < 1e-12, "({r},{s}) = {product}");
        }
    }
    for r in 0..4 {
        assert_eq!(routing.sumwt_source(r), 0);
    }
    // CASA `ToStokesPSF` with several correlations and more than two Stokes
    // parameters gives every plane the first parameter's PSF (Stokes I),
    // never the Q, U or V combinations, whose unit-visibility sums vanish.
    for r in 0..4 {
        let psf = (0..4)
            .map(|g| routing.to_requested_psf(r, g))
            .collect::<Vec<_>>();
        assert_eq!(psf, to(0), "PSF plane {r}");
    }
}

#[test]
fn circular_pairs_follow_the_changelabels_table() {
    let iv =
        PolarizationRouting::compile(&[C::CircularRr, C::CircularLl], &[P::StokesI, P::StokesV])
            .expect("routing");
    assert_eq!(
        iv.grid(),
        &[
            GridPolarization::Correlation(C::CircularRr),
            GridPolarization::Correlation(C::CircularLl)
        ]
    );
    assert_eq!(iv.to_requested(1, 0), c(0.5, 0.0));
    assert_eq!(iv.to_requested(1, 1), c(-0.5, 0.0));
    assert_eq!(iv.from_requested(0, 1), c(1.0, 0.0));
    assert_eq!(iv.from_requested(1, 1), c(-1.0, 0.0));
    let qu = PolarizationRouting::compile(
        &[C::CircularRr, C::CircularRl, C::CircularLr, C::CircularLl],
        &[P::StokesQ, P::StokesU],
    )
    .expect("routing");
    assert_eq!(
        qu.grid(),
        &[
            GridPolarization::Correlation(C::CircularRl),
            GridPolarization::Correlation(C::CircularLr)
        ]
    );
    assert_eq!(qu.pol_map(), &[None, Some(0), Some(1), None]);
    assert_eq!(qu.to_requested(1, 0), c(0.0, -0.5));
    assert_eq!(qu.to_requested(1, 1), c(0.0, 0.5));
    assert_eq!(qu.from_requested(0, 1), c(0.0, 1.0));
    assert_eq!(qu.from_requested(1, 1), c(0.0, -1.0));
    // I and Q on circular feeds need all four correlations.
    let iq = PolarizationRouting::compile(
        &[C::CircularRr, C::CircularRl, C::CircularLr, C::CircularLl],
        &[P::StokesI, P::StokesQ],
    )
    .expect("routing");
    assert_eq!(iq.grid_pols(), 4);
}

#[test]
fn requested_correlations_route_themselves_and_drop_the_rest() {
    let routing = PolarizationRouting::compile(
        &[C::LinearXx, C::LinearXy, C::LinearYx, C::LinearYy],
        &[P::LinearXx, P::LinearYy],
    )
    .expect("routing");
    assert_eq!(routing.pol_map(), &[Some(0), None, None, Some(1)]);
    assert_eq!(routing.to_requested(0, 0), c(1.0, 0.0));
    assert_eq!(routing.to_requested(0, 1), c(0.0, 0.0));
    assert_eq!(routing.sumwt_source(1), 1);
}

#[test]
fn unroutable_requests_are_typed_errors() {
    let q_from_parallel =
        PolarizationRouting::compile(&[C::CircularRr, C::CircularLl], &[P::StokesQ]);
    assert!(matches!(
        q_from_parallel,
        Err(OperatorError::Polarization { .. })
    ));
    let mixed =
        PolarizationRouting::compile(&[C::LinearXx, C::LinearYy], &[P::StokesI, P::LinearXx]);
    assert!(matches!(mixed, Err(OperatorError::Polarization { .. })));
    let unselected = PolarizationRouting::compile(&[C::LinearXx], &[P::LinearYy]);
    assert!(matches!(
        unselected,
        Err(OperatorError::Polarization { .. })
    ));
    let mixed_feeds = PolarizationRouting::compile(&[C::LinearXx, C::CircularLl], &[P::StokesI]);
    assert!(matches!(
        mixed_feeds,
        Err(OperatorError::Polarization { .. })
    ));
}
