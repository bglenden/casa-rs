// SPDX-License-Identifier: LGPL-3.0-or-later
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::{FrequencyRef, MFrequency};

use super::*;
use crate::{PowerLogPolynomial, SpectralIndex};

const ARCSEC: f64 = std::f64::consts::PI / 180.0 / 3600.0;
const GHZ_230: f64 = 230.0e9;

fn direction() -> MDirection {
    MDirection::from_angles(2.6, -0.5, DirectionRef::J2000)
}

fn component(flux: Flux, kind: ShapeKind, spectrum: SpectralKind) -> SkyComponent {
    SkyComponent::new(
        flux,
        ComponentShape::new(direction(), kind).expect("shape"),
        SpectralModel::new(MFrequency::new(GHZ_230, FrequencyRef::LSRK), spectrum),
    )
}

fn close(actual: f64, expected: f64, what: &str) {
    assert!(
        (actual - expected).abs() <= 1.0e-12 * expected.abs().max(1.0),
        "{what}: {actual} against {expected}"
    );
}

#[test]
fn stokes_reach_circular_and_linear_bases_as_casa_converts_them() {
    let iquv = [1.0, 0.1, 0.05, 0.02];
    let circular = stokes_in(Polarisation::Circular, iquv);
    assert_eq!(
        circular,
        [
            Complex64::new(1.02, 0.0),
            Complex64::new(0.1, 0.05),
            Complex64::new(0.1, -0.05),
            Complex64::new(0.98, 0.0),
        ]
    );
    let linear = stokes_in(Polarisation::Linear, iquv);
    assert_eq!(
        linear,
        [
            Complex64::new(1.1, 0.0),
            Complex64::new(0.05, 0.02),
            Complex64::new(0.05, -0.02),
            Complex64::new(0.9, 0.0),
        ]
    );
    // And back: a flux held in either basis reads as the same Stokes.
    for (basis, values) in [
        (Polarisation::Circular, circular),
        (Polarisation::Linear, linear),
    ] {
        let flux = Flux::new(values, "mJy", basis).expect("flux");
        let stokes = flux.stokes_jy_values().expect("Jy");
        for (actual, expected) in stokes.iter().zip(iquv) {
            close(*actual, expected * 1.0e-3, "Stokes in Jy");
        }
    }
    // A brightness unit is not a flux density.
    let brightness = Flux::new(
        [Complex64::new(1.0, 0.0); 4],
        "K.rad2",
        Polarisation::Stokes,
    )
    .expect("flux");
    assert!(brightness.stokes_jy_values().is_err());
}

#[test]
fn a_gaussian_is_the_textbook_transform_of_its_axes() {
    let major = 2.0 * ARCSEC;
    let minor = 0.8 * ARCSEC;
    let gaussian = |pa: f64| ShapeKind::Gaussian {
        axes: Ellipse::new(major, minor, pa).expect("axes"),
        errors: Default::default(),
    };
    let wavelength = 299_792_458.0 / GHZ_230;
    let (u, v) = (40.0, -25.0);
    // At PA 0 the major axis runs north-south, along v.
    let expected = (-(std::f64::consts::PI.powi(2) / (4.0 * std::f64::consts::LN_2))
        * ((major * v / wavelength).powi(2) + (minor * u / wavelength).powi(2)))
    .exp();
    let shape = ComponentShape::new(direction(), gaussian(0.0)).expect("shape");
    close(
        shape.visibility_scale([u, v, 0.0], GHZ_230).expect("vis"),
        expected,
        "PA 0",
    );
    // At PA 90° the axes swap.
    let swapped = (-(std::f64::consts::PI.powi(2) / (4.0 * std::f64::consts::LN_2))
        * ((major * u / wavelength).powi(2) + (minor * v / wavelength).powi(2)))
    .exp();
    let rotated =
        ComponentShape::new(direction(), gaussian(std::f64::consts::FRAC_PI_2)).expect("shape");
    close(
        rotated.visibility_scale([u, v, 0.0], GHZ_230).expect("vis"),
        swapped,
        "PA 90",
    );
    close(
        shape
            .visibility_scale([0.0, 0.0, 7.0], GHZ_230)
            .expect("vis"),
        1.0,
        "zero spacing",
    );
}

#[test]
fn a_disk_is_two_j1_over_r_and_one_where_u_plus_v_is_zero() {
    let diameter = 3.0 * ARCSEC;
    let disk = ComponentShape::new(
        direction(),
        ShapeKind::Disk {
            axes: Ellipse::new(diameter, diameter, 0.0).expect("axes"),
            errors: Default::default(),
        },
    )
    .expect("shape");
    let (u, v) = (120.0_f64, 35.0_f64);
    let r = std::f64::consts::PI * GHZ_230 / 299_792_458.0 * diameter * u.hypot(v);
    close(
        disk.visibility_scale([u, v, 0.0], GHZ_230).expect("vis"),
        2.0 * libm::j1(r) / r,
        "circular disk",
    );
    // CASA tests u + v, not u and v, against zero.
    assert_eq!(
        disk.visibility_scale([80.0, -80.0, 0.0], GHZ_230)
            .expect("vis"),
        1.0
    );
    let limb = ComponentShape::new(
        direction(),
        ShapeKind::LimbDarkenedDisk {
            axes: Ellipse::new(diameter, diameter, 0.0).expect("axes"),
            errors: Default::default(),
        },
    )
    .expect("shape");
    assert!(matches!(
        limb.visibility_scale([u, v, 0.0], GHZ_230),
        Err(ComponentError::Visibility { .. })
    ));
}

#[test]
fn spectral_models_scale_the_stokes_flux_as_casas_matrix_sampler() {
    let iquv = [2.0, 0.2, 0.1, 0.05];
    let nu = 1.1 * GHZ_230;
    let flux = Flux::stokes_jy(iquv[0], iquv[1], iquv[2], iquv[3]);
    // One index scales every Stokes alike.
    let single = component(
        flux.clone(),
        ShapeKind::Point,
        SpectralKind::SpectralIndex(SpectralIndex::new(-0.7)),
    );
    let sampled = single.spectrum().sample_stokes(nu, iquv).expect("sample");
    for (actual, expected) in sampled.iter().zip(iquv) {
        close(*actual, expected * 1.1_f64.powf(-0.7), "single index");
    }
    // Stokes indices: I by α₀, Q and U by α₀+α₁ and rotated by α₂, V by α₀+α₃.
    let full = component(
        flux.clone(),
        ShapeKind::Point,
        SpectralKind::SpectralIndex(
            SpectralIndex::new(-0.7).with_stokes_indices([-0.7, 0.3, 2.0, -0.2]),
        ),
    );
    let sampled = full.spectrum().sample_stokes(nu, iquv).expect("sample");
    let c = 299_792_458.0_f64;
    let two_alpha =
        2.0 * 2.0 * c * c * (GHZ_230 * GHZ_230 - nu * nu) / (nu * nu * GHZ_230 * GHZ_230);
    let (q, u) = (0.2 * 1.1_f64.powf(-0.4), 0.1 * 1.1_f64.powf(-0.4));
    close(sampled[0], 2.0 * 1.1_f64.powf(-0.7), "I");
    close(sampled[1], two_alpha.cos() * q - two_alpha.sin() * u, "Q");
    close(sampled[2], two_alpha.sin() * q + two_alpha.cos() * u, "U");
    close(sampled[3], 0.05 * 1.1_f64.powf(-0.9), "V");
    // A power-log-polynomial scales I only.
    let plp = component(
        flux.clone(),
        ShapeKind::Point,
        SpectralKind::PowerLogPolynomial(PowerLogPolynomial::new(vec![-0.5, 0.2, -0.1])),
    );
    let sampled = plp.spectrum().sample_stokes(nu, iquv).expect("sample");
    let x = 1.1_f64;
    close(
        sampled[0],
        2.0 * x.powf(-0.5 + 0.2 * x.ln() - 0.1 * x.ln() * x.ln()),
        "PLP I",
    );
    assert_eq!(sampled[1..], iquv[1..]);
}

#[test]
fn a_tabular_spectrum_interpolates_and_extrapolates_linearly_in_its_own_frame() {
    let table = TabularSpectrum::new(
        FrequencyRef::LSRK,
        vec![220.0e9, 230.0e9, 240.0e9],
        vec![
            [1.0, 0.0, 0.0, 0.0],
            [2.0, 0.2, 0.0, 0.0],
            [4.0, 0.0, 0.0, 0.0],
        ],
    )
    .expect("table");
    let tabular = component(
        Flux::stokes_jy(9.0, 0.0, 0.0, 0.0),
        ShapeKind::Point,
        SpectralKind::Tabular(table.clone()),
    );
    let at = |nu: f64| {
        tabular
            .spectrum()
            .sample_stokes(nu, [9.0, 0.0, 0.0, 0.0])
            .expect("sample")
    };
    // The table's values, not the component's flux.
    close(at(225.0e9)[0], 1.5, "between");
    close(at(225.0e9)[1], 0.1, "Q between");
    close(at(245.0e9)[0], 5.0, "extrapolated above");
    close(at(210.0e9)[0], 0.0, "extrapolated below");
    // Another reference frame than the tabulation's is refused.
    let mut other = tabular.clone();
    *other.spectrum_mut() = SpectralModel::new(
        MFrequency::new(GHZ_230, FrequencyRef::TOPO),
        SpectralKind::Tabular(table),
    );
    assert!(other.spectrum().sample_stokes(GHZ_230, [1.0; 4]).is_err());
}

#[test]
fn a_descending_tabulation_interpolates_between_the_same_pairs() {
    let at = |frequencies: Vec<f64>, stokes_i: Vec<f64>, nu: f64| {
        let table = TabularSpectrum::new(
            FrequencyRef::LSRK,
            frequencies,
            stokes_i.into_iter().map(|i| [i, 0.0, 0.0, 0.0]).collect(),
        )
        .expect("table");
        SpectralModel::new(
            MFrequency::new(GHZ_230, FrequencyRef::LSRK),
            SpectralKind::Tabular(table),
        )
        .sample_stokes(nu, [1.0; 4])
        .expect("sample")[0]
    };
    // casacore 3.7.1's `InterpolateArray1D::linear` gives 6.5 (#872 review).
    close(
        at(vec![3.0e9, 2.0e9, 1.0e9], vec![9.0, 4.0, 1.0], 2.5e9),
        6.5,
        "between",
    );
    let descending = |nu| at(vec![240.0e9, 230.0e9, 220.0e9], vec![4.0, 2.0, 1.0], nu);
    close(descending(225.0e9), 1.5, "between, low pair");
    close(descending(230.0e9), 2.0, "a knot");
    close(descending(240.0e9), 4.0, "the first knot");
    close(descending(245.0e9), 5.0, "extrapolated above");
    close(descending(210.0e9), 0.0, "extrapolated below");
}

#[test]
fn a_components_visibility_is_its_sampled_flux_times_its_shape() {
    let gaussian = ShapeKind::Gaussian {
        axes: Ellipse::new(1.0 * ARCSEC, 0.5 * ARCSEC, 0.4).expect("axes"),
        errors: Default::default(),
    };
    let component = component(
        Flux::stokes_jy(0.5, 0.05, 0.0, 0.0),
        gaussian,
        SpectralKind::SpectralIndex(SpectralIndex::new(-0.7)),
    );
    let uvw = [30.0, 12.0, 1.0];
    let nu = 231.0e9;
    let visibility = component
        .visibility(uvw, nu, Polarisation::Linear)
        .expect("visibility");
    let scale = component.shape().visibility_scale(uvw, nu).expect("scale");
    let flux = component.flux_in(nu, Polarisation::Linear).expect("flux");
    for (value, expected) in visibility.iter().zip(flux) {
        assert_eq!(*value, expected * scale);
    }
    close(
        flux[0].re,
        0.55 * (nu / GHZ_230).powf(-0.7),
        "XX = I + Q at the frequency",
    );
}
