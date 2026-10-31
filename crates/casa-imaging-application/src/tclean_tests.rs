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

/// Every field the reader makes is one the request requires: a field the
/// request does not read would be dropped silently (the flattened gridder
/// rules out `deny_unknown_fields`).
#[test]
fn every_request_field_is_required() {
    for overrides in modes() {
        let values = resolved(overrides.clone()).expect("catalog values");
        let (fields, _) = read(&values).expect("tclean values");
        serde_json::from_value::<ImagingRequest>(Value::Object(fields.clone()))
            .unwrap_or_else(|error| panic!("{overrides}: {error}"));
        for name in fields.keys() {
            let mut without = fields.clone();
            without.remove(name);
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
        // CASA selects these whatever selectdata is.
        json!({ "selectdata": false, "timerange": "10:00:00~11:00:00" }),
        // CASA parses reffreq in every mode ("Error in converting garbage
        // to a Quantity" in a cube).
        json!({ "specmode": "cube", "nchan": 4, "reffreq": "1.4GHz" }),
        // CASA weighs with npixels for uniform weighting too.
        json!({ "weighting": "uniform", "npixels": 4 }),
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
        (json!({ "weighting": "superuniform" }), "weighting", "#980"),
        (json!({ "usemask": "pb" }), "usemask", "#823"),
        (json!({ "savemodel": "virtual" }), "savemodel", "#823"),
        (
            json!({ "specmode": "cube", "interpolation": "cubic" }),
            "interpolation",
            "#823",
        ),
        (
            json!({ "specmode": "cube", "restfreq": ["1GHz", "2GHz"] }),
            "restfreq",
            "#980",
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
            json!({ "restoringbeam": ["10arcsec"] }),
            "restoringbeam",
            "#980",
        ),
        (
            json!({ "mask": "circle[[10pix,10pix],5pix]" }),
            "mask",
            "#980",
        ),
        (json!({ "mask": ["one.mask", "two.mask"] }), "mask", "#980"),
        (json!({ "gridder": "awprojectft" }), "gridder", "#980"),
        // mfs images take their frame and rest frequency from these.
        (json!({ "outframe": "BARY" }), "outframe", "#980"),
        (json!({ "restfreq": ["1GHz"] }), "restfreq", "#980"),
        (
            json!({ "deconvolver": "mtmfs", "nterms": 1 }),
            "nterms",
            "#980",
        ),
        (json!({ "niter": 10, "nmajor": 0 }), "nmajor", "#980"),
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
