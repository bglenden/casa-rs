// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::PathBuf;

use serde_json::json;

use super::*;

/// The values the catalog resolves from `overrides` over its defaults,
/// with a MeasurementSet, an image name and a cell.
pub(crate) fn resolved(
    overrides: Value,
) -> Result<serde_json::Map<String, Value>, casa_task_runtime::ResolveError> {
    let mut values = json!({ "vis": "unused.ms", "imagename": "unused", "cell": "1arcsec" });
    let fields = values.as_object_mut().expect("object");
    fields.extend(overrides.as_object().expect("overrides object").clone());
    casa_task_runtime::resolve_plain_values(
        casa_provider_contracts::builtin_surface_bundle("tclean").expect("tclean surface"),
        PathBuf::from("."),
        fields,
    )
}

/// The request tclean's `overrides` make, or its refusal.
pub(crate) fn read(overrides: Value) -> Result<ImagingRequest, crate::TcleanRefusal> {
    ImagingRequest::from_tclean(&resolved(overrides).expect("catalog values"))
}

/// A request resolved through the catalog from `overrides` over its
/// defaults, with a MeasurementSet, an image name and a cell.
pub(crate) fn request(overrides: Value) -> ImagingRequest {
    read(overrides).expect("imaging request")
}

fn refusal(overrides: Value) -> &'static str {
    request(overrides.clone())
        .validate()
        .err()
        .unwrap_or_else(|| panic!("{overrides} is a valid request"))
        .parameters
}

#[test]
fn the_catalog_defaults_validate() {
    let request = request(json!({}));
    request.validate().expect("defaults");
    assert_eq!(
        request.gridder,
        Gridder::Standard {
            facets: NonZeroUsize::MIN
        }
    );
    assert_eq!(request.backend, BackendRequest::Auto);
    assert_eq!(request.gridprecision, None);
    assert_eq!(
        request.diagnostics,
        Diagnostics {
            stage_timing: false,
            metal_stage_timing: false,
            science_trace: false,
        }
    );
    assert_eq!(
        request.resource_policy(),
        ResourcePolicy::Explicit {
            workers: 1,
            memory: u64::MAX,
        }
    );
}

/// CASA's `imsize` and `cell` take one value for both axes or one per
/// axis, x first.
#[test]
fn imsize_and_cell_take_one_value_or_one_per_axis() {
    let square = request(json!({ "imsize": 64, "cell": "2arcsec" }));
    assert_eq!((square.imsize, square.cell), ([64, 64], [2.0, 2.0]));
    // CASA's one-element lists are one value for both axes (#970 review).
    let listed = request(json!({ "imsize": [120], "cell": ["8arcsec"] }));
    assert_eq!((listed.imsize, listed.cell), ([120, 120], [8.0, 8.0]));
    let pairs = request(json!({ "imsize": [120, 80], "cell": ["8arcsec", "6arcsec"] }));
    assert_eq!((pairs.imsize, pairs.cell), ([120, 80], [8.0, 6.0]));
    pairs.validate().expect("a non-square image");
    assert!(resolved(json!({ "imsize": [64, 32, 16] })).is_err());
}

/// `uvtaper` as `VisImagingWeight::setFilter` reads it: an angle is the
/// image-plane FWHM θ, whose uv-plane HWHM is 2 ln 2 / (π θ), and
/// wavelengths are the uv-plane HWHM. CASA's
/// `π² θ² / (4 ln 2) = ln 2 / HWHM²`.
#[test]
fn uvtaper_takes_casas_forms_and_refuses_an_axis_without_a_unit() {
    use std::f64::consts::{LN_2, PI};
    let parsed = |value: Value| read(json!({ "uvtaper": value })).map(|request| request.uvtaper);
    let close = |actual: f64, expected: f64| {
        assert!(
            (actual - expected).abs() <= 1.0e-12 * expected,
            "{actual} against {expected}"
        );
    };
    assert_eq!(request(json!({})).uvtaper, None);
    assert_eq!(parsed(json!([])).expect("an empty taper"), None);
    let arcsec = PI / (180.0 * 3600.0);
    let circular = parsed(json!(["10arcsec"]))
        .expect("parsed")
        .expect("a taper");
    close(circular.major_lambda(), 2.0 * LN_2 / (PI * 10.0 * arcsec));
    close(circular.minor_lambda(), circular.major_lambda());
    assert_eq!(circular.position_angle_rad(), 0.0);
    let rbmaj = PI * PI * (10.0 * arcsec).powi(2) / (4.0 * LN_2);
    close(LN_2 / circular.major_lambda().powi(2), rbmaj);
    let uv = parsed(json!(["5klambda", "3klambda", "45deg"]))
        .expect("parsed")
        .expect("a taper");
    close(uv.major_lambda(), 5000.0);
    close(uv.minor_lambda(), 3000.0);
    close(uv.position_angle_rad(), PI / 4.0);
    let two = parsed(json!(["80arcsec", "40arcsec"]))
        .expect("parsed")
        .expect("a taper");
    close(two.minor_lambda(), 2.0 * two.major_lambda());
    // CASA reads a bare number as radians and tapers every weight to
    // zero; mixed axes and a unitless angle are refused too.
    for value in [
        json!(["500.0"]),
        json!(["10arcsec", "3klambda"]),
        json!(["10arcsec", "5arcsec", "30"]),
        json!(["1Jy"]),
    ] {
        assert!(parsed(value.clone()).is_err(), "{value}");
    }
}

#[test]
fn contradictory_parameters_are_refused_before_any_data_is_read() {
    for (overrides, parameters) in [
        (
            json!({ "deconvolver": "mtmfs", "nterms": 0 }),
            "nterms, deconvolver",
        ),
        (
            json!({ "deconvolver": "mtmfs", "nterms": 2, "specmode": "cube" }),
            "deconvolver, specmode",
        ),
        (json!({ "savemodel": "modelcolumn" }), "savemodel, niter"),
        (json!({ "savemodel": "virtual" }), "savemodel, niter"),
        (
            json!({ "savemodel": "virtual", "niter": 1, "gridder": "wproject", "wprojplanes": 8 }),
            "savemodel, gridder",
        ),
        (
            json!({ "savemodel": "virtual", "niter": 1, "stokes": "IV" }),
            "savemodel, stokes",
        ),
        (
            json!({ "savemodel": "virtual", "niter": 1, "outlierfile": "outliers.txt" }),
            "savemodel, outlierfile",
        ),
        (
            json!({ "save_continuum_residual": true }),
            "save_continuum_residual, fitspw",
        ),
        (json!({ "fitspw": "0:0~3" }), "fitspw, specmode"),
        (json!({ "pblimit": 1.0 }), "psfcutoff, pblimit"),
        (
            json!({ "deconvolver": "mtmfs", "nterms": 2, "startmodel": "deep.model" }),
            "startmodel, deconvolver",
        ),
    ] {
        assert_eq!(refusal(overrides), parameters);
    }
}

#[test]
fn aw_projection_uses_the_32_plane_evla_cache() {
    let aw = |planes: Value| {
        json!({
            "gridder": "awproject",
            "conjbeams": true,
            "cfcache": "cf",
            "wprojplanes": planes,
        })
    };
    assert_eq!(refusal(aw(json!(16))), "wprojplanes");
    assert_eq!(refusal(aw(json!(-1))), "wprojplanes");
    request(aw(json!(32))).validate().expect("32 planes");
    let native = json!({
        "gridder": "awproject",
        "conjbeams": true,
        "aw_cf_source": "native-evla",
        "wprojplanes": 32,
        "native_cf_cache": "cache",
        "evla_surface": "surface",
        "native_cf_working_size": 64,
        "native_cf_oversampling": 4,
        "native_cf_cache_bytes": 1_048_576,
        "native_cf_maximum_cells": 16,
    });
    assert_eq!(
        refusal(native),
        "native_cf_cache_bytes, native_cf_maximum_cells"
    );
}

/// A blank optional parameter is unset, as CASA's empty text is.
#[test]
fn blank_optional_values_are_unset() {
    let blank = request(json!({
        "phasecenter": "",
        "mask": "",
        "outlierfile": "",
        "restfreq": "",
        "scales": [],
        "field": "",
        "spw": "",
        "start": "",
        "startmodel": "",
    }));
    assert_eq!(blank, request(json!({})));
    assert_eq!(blank.datacolumn, None);
}

#[test]
fn stokes_names_stokes_parameters_or_correlations_in_casa_order() {
    use PolarizationCoordinate::*;
    for (text, coordinates) in [
        ("I", vec![StokesI]),
        ("VI", vec![StokesI, StokesV]),
        ("IQUV", vec![StokesI, StokesQ, StokesU, StokesV]),
        ("RRLL", vec![CircularRr, CircularLl]),
        ("XXYY", vec![LinearXx, LinearYy]),
        ("XXXYYXYY", vec![LinearXx, LinearXy, LinearYx, LinearYy]),
    ] {
        assert_eq!(
            request(json!({ "stokes": text })).stokes,
            coordinates,
            "{text}"
        );
    }
    for text in ["IXX", "XXRR", "IQZ", "X", ""] {
        let refused = resolved(json!({ "stokes": text }))
            .map_or(true, |values| ImagingRequest::from_tclean(&values).is_err());
        assert!(refused, "{text:?}");
    }
}
