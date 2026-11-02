// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{num::NonZeroUsize, path::PathBuf};

use serde_json::{Value, json};

use super::read;
use crate::request::tests::{request, resolved};
use crate::{DataColumn, Deconvolver, Gridder, ImagingRequest, SpecMode, TcleanRefusal};

/// tclean's refusal of `overrides`.
fn refusal(overrides: Value) -> TcleanRefusal {
    crate::request::tests::read(overrides).expect_err("a refused tclean value")
}

/// Overrides that reach every gridder, AW source and spectral mode.
fn modes() -> Vec<Value> {
    vec![
        json!({}),
        json!({ "specmode": "cube", "nchan": 4 }),
        json!({ "gridder": "wproject" }),
        json!({ "gridder": "mosaic", "mosweight": false }),
        json!({ "gridder": "awproject", "conjbeams": true, "cfcache": "cf", "wprojplanes": 32 }),
        json!({
            "gridder": "awproject",
            "conjbeams": true,
            "wprojplanes": 32,
            "aw_cf_source": "native-evla",
            "native_cf_cache": "cache",
            "evla_surface": "surface",
            "native_cf_working_size": 64,
            "native_cf_oversampling": 4,
            "native_cf_cache_bytes": 1_048_576,
            "native_cf_maximum_cells": 64,
        }),
        json!({ "deconvolver": "mtmfs", "nterms": 2, "scales": [0, 3] }),
        json!({ "usemask": "auto-multithresh" }),
        json!({ "weighting": "superuniform" }),
        json!({ "weighting": "radial", "specmode": "cube", "nchan": 4 }),
        json!({ "weighting": "briggsabs", "specmode": "cube", "nchan": 4 }),
    ]
}

/// Every tclean parameter is read, whatever the modes: one the request
/// ignored without reading would be dropped silently.
#[test]
fn every_tclean_parameter_is_read() {
    for overrides in modes() {
        let values = resolved(overrides.clone()).expect("catalog values");
        let (_, consumed) = read(&values).unwrap_or_else(|error| panic!("{overrides}: {error}"));
        let unread = values
            .keys()
            .filter(|name| !consumed.contains(name.as_str()))
            .collect::<Vec<_>>();
        assert!(unread.is_empty(), "{overrides}: unread {unread:?}");
    }
}

/// Every reader field is required or explicitly consumed internal provenance:
/// the flattened gridder rules out `deny_unknown_fields`, so silently dropped
/// fields need this law, including the optional empty-mask provenance flag.
#[test]
fn every_request_field_is_required_or_consumed_internal_provenance() {
    for overrides in modes() {
        let values = resolved(overrides.clone()).expect("catalog values");
        let (fields, _) = read(&values).expect("tclean values");
        serde_json::from_value::<ImagingRequest>(Value::Object(fields.clone()))
            .unwrap_or_else(|error| panic!("{overrides}: {error}"));
        for name in fields.keys() {
            let mut without = fields.clone();
            without.remove(name);
            if name == "explicit_empty_mask" {
                let default = serde_json::from_value::<ImagingRequest>(Value::Object(without))
                    .expect("internal mask provenance defaults to false");
                assert!(!default.explicit_empty_mask);
                let mut explicit = fields.clone();
                explicit.insert(name.clone(), Value::Bool(true));
                let explicit = serde_json::from_value::<ImagingRequest>(Value::Object(explicit))
                    .expect("internal mask provenance is read");
                assert!(explicit.explicit_empty_mask);
                assert_ne!(explicit, default);
                continue;
            }
            assert!(
                serde_json::from_value::<ImagingRequest>(Value::Object(without)).is_err(),
                "{overrides}: the request does not read {name}"
            );
        }
    }
}

/// A parameter the request's modes do not read is ignored, as CASA's
/// tclean ignores it.
#[test]
fn parameters_a_mode_does_not_read_are_ignored() {
    let plain = request(json!({}));
    for overrides in [
        json!({ "nterms": 5 }),
        json!({ "scales": [0, 5, 15] }),
        json!({ "start": "1.4GHz", "width": 2, "veltype": "ratio" }),
        json!({ "interpolation": "cubic", "nchan": 8 }),
        json!({ "wprojplanes": 64, "normtype": "pbsquare", "pointingoffsetsigdev": [300, 30] }),
        json!({ "aterm": false, "psterm": true, "wbawp": false, "conjbeams": true }),
        json!({ "mosweight": false, "cfcache": "cf" }),
    ] {
        assert_eq!(request(overrides.clone()), plain, "{overrides}");
    }
    // A mosaic reads no W planes.
    assert_eq!(
        request(json!({ "gridder": "mosaic", "wprojplanes": 8 })),
        request(json!({ "gridder": "mosaic" }))
    );
    // auto-multithresh reads no user mask.
    assert_eq!(
        request(json!({ "usemask": "auto-multithresh", "mask": "box[[1pix,1pix],[4pix,4pix]]" })),
        request(json!({ "usemask": "auto-multithresh" }))
    );
    // CASA's tclean selects whatever selectdata says: its parameter classes
    // never read it (probed: selectdata=False, field='99' is refused).
    let selected = json!({ "field": "3", "spw": "0:1~4", "intent": "*" });
    let mut unselected = selected.clone();
    unselected["selectdata"] = json!(false);
    assert_eq!(request(unselected), request(selected.clone()));
    assert_ne!(request(selected), plain);
}

/// What the catalog takes only at tclean's default, wherever CASA reads it.
#[test]
fn the_catalog_refuses_what_casa_reads_in_every_mode() {
    for overrides in [
        // CASA projects with any projection in every mode.
        json!({ "projection": "NCP" }),
        // CASA masks within the primary beam with pbmask in every mask mode.
        json!({ "pbmask": 0.5 }),
        // CASA reads psfphasecenter for every gridder named for a mosaic.
        json!({ "gridder": "ftmosaic", "mosweight": false, "psfphasecenter": "J2000 0rad 0rad" }),
    ] {
        let error = resolved(overrides.clone()).expect_err("refused at the catalog");
        assert!(error.to_string().contains("#980"), "{overrides}: {error}");
    }
}

#[test]
fn casa_values_become_the_request_casa_rs_runs() {
    let field = request(json!({ "phasecenter": 3 }));
    assert_eq!(
        (field.phasecenter.as_deref(), field.phasecenter_field),
        (None, Some(3))
    );
    let direction = request(json!({ "phasecenter": "J2000 0rad 0rad" }));
    assert_eq!(direction.phasecenter.as_deref(), Some("J2000 0rad 0rad"));
    assert_eq!(direction.phasecenter_field, None);

    let boxes = request(json!({
        "mask": ["box[[100pix,100pix],[150pix,150pix]]", "box [[9pix, 8pix], [2pix, 1pix]]"]
    }));
    assert_eq!(boxes.mask_box, [[100, 100, 150, 150], [2, 1, 9, 8]]);
    let legacy = request(json!({ "mask": "0,0,4,4;5,5,9,9" }));
    assert_eq!(legacy.mask_box, [[0, 0, 4, 4], [5, 5, 9, 9]]);
    let image = request(json!({ "mask": "clean.mask" }));
    assert_eq!(image.mask_image, Some(PathBuf::from("clean.mask")));

    // CASA's cycleniter=-1 is niter, and so is any cycleniter of zero or
    // less, or more than niter (`checkAndFixIterationPars`).
    assert_eq!(request(json!({ "niter": 2500 })).minor_cycle_length, 2500);
    assert_eq!(
        request(json!({ "niter": 2500, "cycleniter": 100 })).minor_cycle_length,
        100
    );
    for cycleniter in [0, -5, 50] {
        assert_eq!(
            request(json!({ "niter": 10, "cycleniter": cycleniter })).minor_cycle_length,
            10,
            "{cycleniter}"
        );
    }
    assert_eq!(request(json!({})).minor_cycle_length, 1);
    // CASA sets mosweight=False for the mosaicft and ftmosaic gridders, as
    // casa-rs's mosaic weighs density over all fields.
    assert_eq!(
        request(json!({ "gridder": "mosaicft", "weighting": "briggs" })).gridder,
        request(json!({ "gridder": "mosaic", "weighting": "briggs", "mosweight": false })).gridder
    );
    // A region file is read as its regions, as CASA reads it.
    let dir = tempfile::tempdir().expect("temporary directory");
    let region = dir.path().join("m.crtf");
    std::fs::write(&region, "#CRTF\nbox[[10pix,10pix],[20pix,20pix]]\n").expect("region file");
    assert_eq!(
        request(json!({ "mask": region.to_str().expect("utf-8") })).mask_box,
        [[10, 10, 20, 20]]
    );

    assert_eq!(request(json!({})).datacolumn, None);
    assert_eq!(
        request(json!({ "datacolumn": "DATA" })).datacolumn,
        Some(DataColumn::Data)
    );
    assert_eq!(
        request(json!({ "specmode": "cont" })).specmode,
        SpecMode::Mfs
    );
    let cube = request(
        json!({ "specmode": "cube", "nchan": 12, "veltype": "optical", "outframe": "bary" }),
    );
    assert_eq!(cube.channel_count, Some(12));
    assert_eq!(cube.veltype, casa_types::measures::doppler::DopplerRef::Z);
    assert_eq!(
        request(json!({ "gridder": "gridft" })).gridder,
        Gridder::Standard {
            facets: NonZeroUsize::MIN
        }
    );
    assert_eq!(
        request(json!({ "gridder": "widefield", "wprojplanes": -1 })).gridder,
        Gridder::Wproject { wprojplanes: None }
    );
    // CASA's wproject and widefield with tclean's one w-plane are gridft.
    assert_eq!(
        request(json!({ "gridder": "widefield", "facets": 2 })).gridder,
        Gridder::Standard {
            facets: NonZeroUsize::new(2).expect("two facets")
        }
    );
    assert_eq!(
        request(json!({ "gridder": "wproject" })).gridder,
        Gridder::Standard {
            facets: NonZeroUsize::MIN
        }
    );
    assert_eq!(
        request(json!({ "gridder": "wproject", "wprojplanes": 8 })).gridder,
        Gridder::Wproject {
            wprojplanes: NonZeroUsize::new(8)
        }
    );
    // Bare numbers are CASA's units: Jy and arcseconds.
    let bare = request(json!({ "threshold": 0.002, "cell": 0.5, "imsize": [64, 32] }));
    assert_eq!(
        (bare.threshold, bare.cell, bare.imsize),
        (0.002, [0.5, 0.5], [64, 32])
    );
    // nterms is read with mtmfs.
    let terms = request(json!({ "deconvolver": "mtmfs", "nterms": 3 }));
    assert_eq!((terms.deconvolver, terms.nterms), (Deconvolver::Mtmfs, 3));
}

/// A value casa-rs cannot honour is refused at its parameter, naming the
/// issue that tracks it.
#[test]
fn values_casa_rs_cannot_honour_are_refused() {
    for (overrides, parameter, issue) in [
        (json!({ "specmode": "mvc" }), "specmode", "#823"),
        (json!({ "deconvolver": "mem" }), "deconvolver", "#823"),
        (json!({ "deconvolver": "asp" }), "deconvolver", "#223"),
        (json!({ "usemask": "pb" }), "usemask", "#823"),
        (
            json!({ "usemask": "auto-multithresh", "stokes": "IQUV", "niter": 10 }),
            "usemask",
            "#980",
        ),
        (
            json!({ "usemask": "auto-multithresh", "specmode": "cube", "nchan": 4, "niter": 10 }),
            "usemask",
            "#980",
        ),
        (json!({ "savemodel": "virtual" }), "savemodel", "#823"),
        (
            json!({ "specmode": "cube", "interpolation": "cubic" }),
            "interpolation",
            "#823",
        ),
        (
            json!({ "specmode": "cube", "outframe": "GALACTO" }),
            "outframe",
            "#980",
        ),
        (
            json!({ "specmode": "cube", "veltype": "ratio" }),
            "veltype",
            "#980",
        ),
        (
            json!({ "gridder": "wproject", "wprojplanes": -1, "facets": 2 }),
            "facets",
            "#980",
        ),
        (json!({ "gridder": "awp2" }), "gridder", "#980"),
        (
            json!({ "gridder": "mosaic", "weighting": "briggs" }),
            "mosweight",
            "#818",
        ),
        (
            json!({ "gridder": "mosaic", "conjbeams": true }),
            "conjbeams",
            "#980",
        ),
        (
            json!({ "gridder": "awproject", "cfcache": "cf" }),
            "conjbeams",
            "#980",
        ),
        (
            json!({ "mask": "circle[[10pix,10pix],5pix]" }),
            "mask",
            "#980",
        ),
        (json!({ "mask": ["one.mask", "two.mask"] }), "mask", "#980"),
        (json!({ "gridder": "awprojectft" }), "gridder", "#980"),
        // An mfs image's REST axis grids the data's frequencies as they are.
        (json!({ "outframe": "REST" }), "outframe", "#980"),
        // CASA reads a blank rest frequency as zero.
        (
            json!({ "specmode": "cube", "restfreq": ["", "2GHz"] }),
            "restfreq",
            "#980",
        ),
        (
            json!({ "deconvolver": "mtmfs", "nterms": 1 }),
            "nterms",
            "#980",
        ),
        (
            json!({ "gridder": "awproject", "conjbeams": true, "cfcache": "cf",
                    "wprojplanes": 32, "facets": 2 }),
            "facets",
            "#980",
        ),
        // Astra's #1015 F1: CASA derives a cube's extent from the selected
        // channels, the start and the width (probed: spw='0:2~4' gives three
        // channels from the third; width=2 four of 2 MHz from the first
        // pair's centre); casa-rs's takes the window's channel count.
        (
            json!({ "specmode": "cubedata", "spw": "0:2~4" }),
            "nchan",
            "#980",
        ),
        (
            json!({ "specmode": "cubedata", "width": "2" }),
            "nchan",
            "#980",
        ),
        (
            json!({ "specmode": "cube", "start": "1.4GHz" }),
            "nchan",
            "#980",
        ),
        (
            json!({ "specmode": "cube", "spw": "0:2~4", "nchan": 3 }),
            "start",
            "#980",
        ),
    ] {
        let refused = refusal(overrides.clone());
        assert_eq!(refused.parameter, parameter, "{overrides}");
        assert!(
            refused.message.contains(issue),
            "{overrides}: {}",
            refused.message
        );
    }
    for (overrides, parameter) in [
        (json!({ "cell": [] }), "cell"),
        // CASA's tclean refuses these itself.
        (json!({ "weighting": "briggsbwtaper" }), "weighting"),
        (
            json!({ "gridder": "wproject", "wprojplanes": 0 }),
            "wprojplanes",
        ),
        (
            json!({ "gridder": "widefield", "wprojplanes": -3 }),
            "wprojplanes",
        ),
        (json!({ "gridder": "mosaic", "facets": 2 }), "facets"),
        (json!({ "datacolumn": "model" }), "datacolumn"),
        (json!({ "specmode": "cube", "nchan": 0 }), "nchan"),
        (
            json!({ "specmode": "cube", "nchan": 4, "channel_count": 4 }),
            "channel_count",
        ),
        (
            json!({ "gridder": "awproject", "conjbeams": true, "wprojplanes": 32 }),
            "cfcache",
        ),
    ] {
        assert_eq!(
            refusal(overrides.clone()).parameter,
            parameter,
            "{overrides}"
        );
    }
}

#[test]
fn signed_integer_pixel_boxes_preserve_bounds_and_normalize_reversed_endpoints() {
    let crtf = request(json!({"mask": "box[[3pix,4pix],[-2pix,-1pix]]"}));
    assert_eq!(format!("{:?}", crtf.mask_box), "[[-2, -1, 3, 4]]");
    let legacy = request(json!({"mask": "-2,-1,3,4"}));
    assert_eq!(format!("{:?}", legacy.mask_box), "[[-2, -1, 3, 4]]");
    // Integer-valued decimal CRTF syntax already worked and stays supported.
    let decimal = request(json!({"mask": "box[[-2.0pix,-1.0pix],[3.0pix,4.0pix]]"}));
    assert_eq!(format!("{:?}", decimal.mask_box), "[[-2, -1, 3, 4]]");
    for mask in [
        "box[[-2.5pix,0pix],[3pix,4pix]]",
        "box[[1deg,2deg],[3deg,4deg]]",
    ] {
        let error = refusal(json!({"mask": mask}));
        assert_eq!(error.parameter, "mask");
        assert!(error.message.contains("#980"));
    }
    let error = refusal(json!({"mask": ["one.mask", "two.mask"]}));
    assert_eq!(error.parameter, "mask");
    assert!(error.message.contains("#980"));
}

#[test]
fn tclean_accepts_zero_major_limit_without_changing_iteration_intent() {
    for niter in [0, 10] {
        let value = request(json!({"niter": niter, "nmajor": 0}));
        assert_eq!(value.niter, niter);
        assert_eq!(value.nmajor, Some(0));
    }
}

#[test]
fn zero_major_limit_is_valid_but_zero_minor_cycle_length_is_not() {
    let mut value = request(json!({"niter": 10, "nmajor": 1}));
    value.nmajor = Some(0);
    value
        .validate()
        .expect("the initial major pass alone may be requested");
    value.minor_cycle_length = 0;
    assert!(
        value.validate().is_err(),
        "a zero minor-cycle limit remains invalid"
    );
}

#[test]
fn scan_and_observation_selectors_keep_their_exact_text_with_either_selectdata() {
    for selectdata in [false, true] {
        let request = request(json!({
            "selectdata": selectdata, "scan": "1,3~5", "observation": "0",
        }));
        assert_eq!(request.scan.as_deref(), Some("1,3~5"));
        assert_eq!(request.observation.as_deref(), Some("0"));
    }
    // Only exact empty is unset: CASA's tclean refuses blanks and `none`
    // and accepts padded IDs, so their text reaches the selector's parser.
    for text in ["  ", "none", "None", " none ", " 1 ", ">1"] {
        let request = request(json!({"scan": text, "observation": text}));
        assert_eq!(request.scan.as_deref(), Some(text));
        assert_eq!(request.observation.as_deref(), Some(text));
    }
    let request = request(json!({"scan": "", "observation": ""}));
    assert_eq!((request.scan, request.observation), (None, None));
}

/// The request tclean's `overrides` make with `resolved` set over the
/// catalog's resolution, so the reader's own refusals are tested for values
/// whatever the catalog's domain admits.
fn after_resolution(
    overrides: Value,
    resolved_values: Value,
) -> Result<ImagingRequest, TcleanRefusal> {
    let mut values = resolved(overrides).expect("catalog values");
    values.extend(resolved_values.as_object().expect("an object").clone());
    ImagingRequest::from_tclean(&values)
}

/// `npixels` as `SynthesisImagerVi2::weight` reads it: superuniform's box,
/// 3 when zero or less; the size of a uniform or Briggs density; nothing
/// for natural and radial weighting. `task_tclean.py` images nothing for
/// briggsbwtaper with npixels ≠ 0, and a negative density grid fails.
#[test]
fn npixels_is_read_as_casas_weightings_read_it() {
    let npixels = |weighting: &str, npixels: i64| {
        after_resolution(
            json!({ "weighting": weighting, "specmode": "cube", "nchan": 4 }),
            json!({ "npixels": npixels }),
        )
        .map(|request| request.npixels)
    };
    for (weighting, given, read) in [
        ("superuniform", 0, 3),
        ("superuniform", -2, 3),
        ("superuniform", 5, 5),
        ("uniform", 0, 0),
        ("uniform", 16, 16),
        ("briggs", 2, 2),
        ("briggsabs", 7, 7),
        ("briggsbwtaper", 0, 0),
        ("natural", 9, 0),
        ("radial", -4, 0),
    ] {
        assert_eq!(npixels(weighting, given), Ok(read), "{weighting} {given}");
    }
    for (weighting, given) in [("briggsbwtaper", 4), ("uniform", -1), ("briggs", -3)] {
        let refused = npixels(weighting, given).expect_err("refused");
        assert_eq!(refused.parameter, "npixels", "{weighting} {given}");
    }
}

/// `noise` is read for briggsabs alone (`rmode = "abs"`), as a flux
/// density in Jy (`noise.get("Jy")`); robust 0 with noise 0 divides by
/// zero, which `SynthesisImagerVi2::weight` refuses for a cube.
#[test]
fn briggsabs_reads_noise_as_a_flux_density() {
    let read = |weighting: &str, robust: f64, noise: Value| {
        after_resolution(
            json!({ "weighting": weighting, "robust": robust }),
            json!({ "noise": noise }),
        )
    };
    let noise = |robust, text: &str| read("briggsabs", robust, json!(text)).map(|r| r.noise);
    assert_eq!(
        request(json!({ "weighting": "briggsabs" })).noise,
        Some(1.0)
    );
    assert_eq!(noise(0.5, "5mJy"), Ok(Some(0.005)));
    assert_eq!(noise(0.0, "0.1Jy"), Ok(Some(0.1)));
    assert_eq!(noise(0.5, "0Jy"), Ok(Some(0.0)));
    for (robust, text) in [(0.5, "0.5"), (0.5, "1K"), (0.5, "garbage"), (0.0, "0Jy")] {
        let refused = noise(robust, text).expect_err("refused");
        assert_eq!(refused.parameter, "noise", "{robust} {text}");
    }
    // Every other weighting reads no noise.
    for weighting in ["natural", "uniform", "briggs", "superuniform", "radial"] {
        let request = read(weighting, 0.0, json!("garbage")).expect(weighting);
        assert_eq!(request.noise, None, "{weighting}");
    }
    let request = request(json!({ "weighting": "briggsabs", "robust": -1.5 }));
    assert_eq!(
        (request.weighting, request.robust),
        (crate::Weighting::Briggsabs, -1.5)
    );
}

/// casa-rs's mosaic weighs density over all fields, so `mosweight=True` is
/// refused with every density weighting, superuniform among them; radial
/// weighting grids no density.
#[test]
fn per_field_density_is_refused_for_superuniform_and_not_for_radial() {
    let mosaic = |weighting: &str| json!({ "gridder": "mosaic", "weighting": weighting });
    assert_eq!(refusal(mosaic("superuniform")).parameter, "mosweight");
    assert_eq!(
        request(mosaic("radial")).weighting,
        crate::Weighting::Radial
    );
}

/// `restoringbeam` as `SynthesisParamsDeconv::fromRecord` reads it: one
/// text or a list of one, two or three quantities (a circle, an ellipse at
/// angle zero, or an ellipse at an angle); `''`, `[]` and any other count
/// restore with the fitted beams, and a zero axis is casacore's null beam,
/// which does too.
#[test]
fn restoringbeam_takes_casas_forms() {
    use casa_imaging_model::{FixedRestoringBeam, RestoringBeamPolicy};
    let arcsec = std::f64::consts::PI / (180.0 * 3600.0);
    let fixed = |major: f64, minor: f64, angle_rad: f64| {
        RestoringBeamPolicy::Fixed(
            FixedRestoringBeam::new(major * arcsec, minor * arcsec, angle_rad).expect("a beam"),
        )
    };
    let beam = |value: Value| request(json!({ "restoringbeam": value })).restoringbeam;
    for value in [json!(""), json!([]), json!([""])] {
        assert_eq!(
            beam(value.clone()),
            RestoringBeamPolicy::PerPlane,
            "{value}"
        );
    }
    for value in [json!("common"), json!(["common"])] {
        assert_eq!(beam(value.clone()), RestoringBeamPolicy::Common, "{value}");
    }
    for value in [json!("10arcsec"), json!(["10arcsec"]), json!(["10 arcsec"])] {
        assert_eq!(beam(value.clone()), fixed(10.0, 10.0, 0.0), "{value}");
    }
    assert_eq!(beam(json!(["10arcsec", "8arcsec"])), fixed(10.0, 8.0, 0.0));
    assert_eq!(
        beam(json!(["0.25arcmin", "8arcsec", "45deg"])),
        fixed(15.0, 8.0, std::f64::consts::FRAC_PI_4)
    );
    // CASA ignores a list of any other length, and a zero axis.
    for value in [
        json!(["10arcsec", "8arcsec", "45deg", "1arcsec"]),
        json!(["0arcsec"]),
        json!(["10arcsec", "0arcsec"]),
        json!(["0arcsec", "0arcsec", "30deg"]),
    ] {
        assert_eq!(
            beam(value.clone()),
            RestoringBeamPolicy::PerPlane,
            "{value}"
        );
    }
    // `GaussianBeam::setMajorMinor` and `setPA` throw for these, and CASA's
    // tclean stops ("Cannot construct a restoringbeam from supplied
    // parameters").
    for value in [
        json!(["8arcsec", "10arcsec"]),
        json!(["0arcsec", "1arcsec"]),
        json!("10"),
        json!(["10", "8"]),
        json!(["10arcsec", "8arcsec", "45"]),
        json!(["10Jy"]),
        json!(["-1arcsec"]),
        json!(["common", "10arcsec"]),
        json!(["10arcsec", ""]),
        json!("COMMON"),
    ] {
        let refused = refusal(json!({ "restoringbeam": value }));
        assert_eq!(refused.parameter, "restoringbeam", "{value}");
        assert!(
            refused.message.contains("Cannot construct a restoringbeam"),
            "{value}: {}",
            refused.message
        );
    }
}

/// tclean's `restoration`, which the catalog still holds at its default:
/// the reader reads it, and `restoration=False` restores nothing.
#[test]
fn restoration_is_read() {
    assert!(request(json!({})).restoration);
    let mut values = crate::request::tests::resolved(json!({})).expect("catalog values");
    values.insert("restoration".to_string(), json!(false));
    let request = ImagingRequest::from_tclean(&values).expect("a request");
    assert!(!request.restoration);
}

/// An mfs image's header takes its frame and rest frequencies from
/// `outframe` and `restfreq`, as a cube's does; every entry of `restfreq`
/// is read, the first the image's.
#[test]
fn mfs_reads_its_frame_and_rest_frequencies() {
    use casa_types::measures::frequency::FrequencyRef;
    let bary = request(json!({ "outframe": "bary", "restfreq": ["1.4GHz"] }));
    assert_eq!(bary.outframe, FrequencyRef::BARY);
    assert_eq!(bary.restfreq, [1.4e9]);
    assert_eq!(
        request(json!({ "outframe": "TOPO" })).outframe,
        FrequencyRef::TOPO
    );
    assert_eq!(request(json!({})).outframe, FrequencyRef::LSRK);
    assert!(request(json!({})).restfreq.is_empty());
    for overrides in [
        json!({ "restfreq": ["1.4GHz", "1.6GHz"] }),
        json!({ "specmode": "cube", "nchan": 2, "restfreq": ["1.4GHz", "1.6GHz"] }),
    ] {
        assert_eq!(
            request(overrides.clone()).restfreq,
            [1.4e9, 1.6e9],
            "{overrides}"
        );
    }
}

/// `reffreq` as `SynthesisParamsImage::fromRecord` reads it: a frequency
/// quantity in every mode, empty or zero for the band's midpoint; a value
/// without a frequency unit is refused as CASA's conversion to Hz fails.
#[test]
fn reffreq_is_read_as_a_frequency_in_every_mode() {
    assert_eq!(request(json!({})).reffreq, None);
    assert_eq!(request(json!({ "reffreq": "" })).reffreq, None);
    assert_eq!(request(json!({ "reffreq": "0Hz" })).reffreq, None);
    assert_eq!(request(json!({ "reffreq": "1.4GHz" })).reffreq, Some(1.4e9));
    assert_eq!(
        request(json!({ "reffreq": "1400 MHz", "deconvolver": "mtmfs" })).reffreq,
        Some(1.4e9)
    );
    // A cube reads it too, and its axis ignores it.
    assert_eq!(
        request(json!({ "specmode": "cube", "nchan": 4, "reffreq": "1.4GHz" })).reffreq,
        Some(1.4e9)
    );
    for text in ["1.4", "1Jy", "garbage", "-1GHz"] {
        assert_eq!(
            refusal(json!({ "reffreq": text })).parameter,
            "reffreq",
            "{text}"
        );
    }
}

/// tclean's auto-multithresh controls and noise estimate, read in every
/// mask mode: `SynthesisParamsDeconv::fromRecord` reads them whatever
/// `usemask` is, and `fastnoise` also sets the `nsigma` noise.
#[test]
fn automask_controls_and_the_noise_estimate_are_read() {
    let defaults = request(json!({}));
    assert_eq!(
        (
            defaults.smoothfactor,
            defaults.cutthreshold,
            defaults.dogrowprune,
            defaults.minpercentchange,
            defaults.verbose,
            defaults.fastnoise,
        ),
        (1.0, 0.01, true, -1.0, false, true)
    );
    let set = json!({
        "usemask": "auto-multithresh",
        "smoothfactor": 0.5,
        "cutthreshold": 0.2,
        "dogrowprune": false,
        "minpercentchange": 0.2,
        "verbose": true,
        "fastnoise": false,
    });
    let read = request(set.clone());
    assert_eq!(
        (
            read.smoothfactor,
            read.cutthreshold,
            read.dogrowprune,
            read.minpercentchange,
            read.verbose,
            read.fastnoise,
        ),
        (0.5, 0.2, false, 0.2, true, false)
    );
    // The noise estimate applies to nsigma without an automatic mask.
    assert!(!request(json!({ "nsigma": 3.0, "fastnoise": false })).fastnoise);
    // CASA's automatic mask fails on a beam of no positive width; without
    // one, or without cleaning, CASA never smooths.
    for factor in [0.0, -1.0] {
        let mut automatic = set.clone();
        automatic["smoothfactor"] = json!(factor);
        automatic["niter"] = json!(10);
        assert_eq!(refusal(automatic.clone()).parameter, "smoothfactor");
        automatic["niter"] = json!(0);
        assert_eq!(request(automatic).smoothfactor, factor);
        assert_eq!(
            request(json!({ "niter": 10, "smoothfactor": factor })).smoothfactor,
            factor
        );
    }
}
