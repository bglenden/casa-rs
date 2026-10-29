// SPDX-License-Identifier: LGPL-3.0-or-later

//! MSv4 imaging (#932 step 6c, owner decision 5): the store product law. A
//! request imaged on a T1 track and on its MSv4 conversion reads the same
//! visibilities, flags and weights, and its geometry is equal to 1e-12
//! (the store geometry law), so every product agrees to rounding and the
//! two runs take the same cycles. The conversion is casa-rs's own
//! (`to_msv4`), which XRADIO reads back (trial README, "Converter").

use std::path::PathBuf;

use casa_ms::observation_io::Msv4Codec;
use casa_ms::observation_io::convert::{ConversionOptions, LossKind, PaddingRows, to_msv4};
use serde_json::{Value, json};

use super::fixture::{Component, Observation};

/// The products' largest difference, relative to the MeasurementSet's RMS:
/// the blocking comparison's product threshold. The stores hold the same
/// values and their geometry agrees to 1e-12, so what differs is rounding
/// (the standard gridder's products are bit-equal; a mosaic's differ by
/// about 1e-9).
const PRODUCT_NRMS: f64 = 1.0e-6;

const POINT: Component = Component {
    offset_px: [24, 16],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};
const GAUSSIAN: Component = Component {
    offset_px: [-30, -26],
    flux_jy: 0.5,
    fwhm_px: Some(8.0),
    spectral_index: 0.0,
};

/// `observation`'s track converted to MSv4, every data group, the losses a
/// simulated track has accepted (its `SIGMA`, row flags and the subtables
/// MSv4 does not carry).
fn converted(observation: &Observation, name: &str) -> PathBuf {
    let path = observation.scratch(name);
    to_msv4(
        observation.measurement_set(),
        &path,
        &ConversionOptions {
            data_groups: None,
            accept: vec![
                LossKind::Sigma,
                LossKind::RowFlags,
                LossKind::FlagCategories,
                LossKind::Tables,
                LossKind::Columns,
            ],
            batch_bytes: 64 << 20,
            workers: 1,
            chunks: [0, 0, 0, 0],
            codec: Msv4Codec::Zstd,
            padding: PaddingRows::Flagged,
            creator: "casars-imager t1".to_string(),
            creation_date: "2026-10-29T00:00:00".to_string(),
        },
    )
    .expect("the track as MSv4");
    path
}

/// RMS of `values`.
fn rms(values: impl Iterator<Item = f64>) -> f64 {
    let (sum, count) = values.fold((0.0, 0usize), |(sum, count), value| {
        (sum + value * value, count + 1)
    });
    (sum / count.max(1) as f64).sqrt()
}

/// Image `observation` and `store`, its MSv4 conversion, under `controls`,
/// and hold the store product law: the same products, each within
/// [`PRODUCT_NRMS`] of the MeasurementSet's, the same restoring beam, and
/// the same minor iterations and major cycles.
fn assert_store_products(
    observation: &Observation,
    store: &std::path::Path,
    name: &str,
    controls: Value,
) {
    let (ours, measurement_set) = observation.image(&format!("{name}-msv2"), controls.clone());
    let (theirs, msv4) = observation
        .try_image_store(store, &format!("{name}-msv4"), controls)
        .expect("the MSv4 conversion images");
    assert_eq!(
        measurement_set.suffixes(),
        msv4.suffixes(),
        "{name}: products"
    );
    for suffix in measurement_set.suffixes() {
        let (expected, got) = (
            measurement_set.get(&suffix).values(),
            msv4.get(&suffix).values(),
        );
        assert_eq!(expected.shape(), got.shape(), "{name}{suffix}: shape");
        let scale = rms(expected.iter().map(|value| f64::from(*value)));
        let difference = rms(expected
            .iter()
            .zip(&got)
            .map(|(a, b)| f64::from(*a) - f64::from(*b)));
        let nrms = if scale > 0.0 {
            difference / scale
        } else {
            difference
        };
        eprintln!("T1 {name}{suffix}: MSv4 against MSv2 NRMS {nrms:.3e}");
        assert!(
            nrms <= PRODUCT_NRMS,
            "{name}{suffix}: the MSv4 product departs by NRMS {nrms:.3e}"
        );
    }
    if measurement_set.suffixes().contains(".image") {
        let beams = |products: &super::fixture::Products| {
            products
                .get(".image")
                .image
                .image_info()
                .expect("image info")
                .beam_set
        };
        let (a, b) = (beams(&measurement_set), beams(&msv4));
        assert_eq!(a.nchan(), b.nchan(), "{name}: restoring beams");
        for channel in 0..a.nchan() {
            let (a, b) = (a.beam(channel, 0), b.beam(channel, 0));
            for (x, y) in [
                (a.major, b.major),
                (a.minor, b.minor),
                (a.position_angle, b.position_angle),
            ] {
                assert!(
                    (x - y).abs() <= 1.0e-9 * x.abs().max(1.0e-12),
                    "{name}: channel {channel}'s restoring beam"
                );
            }
        }
    }
    assert_eq!(
        (ours.actual_minor_iterations, ours.major_cycles),
        (theirs.actual_minor_iterations, theirs.major_cycles),
        "{name}: cycles"
    );
}

/// The standard gridder: a dirty image and Hogbom and multiscale cleans of
/// the point and the Gaussian, natural and Briggs weighting.
#[test]
fn standard_mfs_images_the_msv4_conversion_as_the_measurement_set() {
    let observation = Observation::synthesise(&[("point", POINT), ("gaussian", GAUSSIAN)]);
    let store = converted(&observation, "mfs.ps.zarr");
    for (name, controls) in [
        ("mfs-dirty", json!({ "weighting": "natural", "niter": 0 })),
        (
            "mfs-hogbom",
            json!({ "weighting": "briggs", "robust": 0.5, "deconvolver": "hogbom",
                    "niter": 500, "threshold": "0.005Jy" }),
        ),
        (
            "mfs-multiscale",
            json!({ "weighting": "natural", "deconvolver": "multiscale", "scales": "0,6",
                    "niter": 300, "threshold": "0.005Jy" }),
        ),
    ] {
        assert_store_products(&observation, &store, name, controls);
    }
}

/// A cube in the LSRK frame and a two-term MTMFS over sixteen channels,
/// with the primary beam.
#[test]
fn a_cube_and_mtmfs_image_the_msv4_conversion_as_the_measurement_set() {
    let observation = Observation::synthesise_band(16, &[("point", POINT)]);
    let store = converted(&observation, "band.ps.zarr");
    for (name, controls) in [
        (
            "cube",
            json!({ "specmode": "cube", "channel_count": 16, "weighting": "natural",
                    "deconvolver": "hogbom", "niter": 500, "threshold": "0.01Jy",
                    "write_pb": true }),
        ),
        (
            "mtmfs",
            json!({ "weighting": "natural", "deconvolver": "mtmfs", "nterms": 2,
                    "niter": 300, "threshold": "0.005Jy", "write_pb": true,
                    "pblimit": 0.2 }),
        ),
    ] {
        assert_store_products(&observation, &store, name, controls);
    }
}

/// A three-pointing mosaic: each field's centre and the mosaic's primary
/// beams come from the MSv4's fields as from the MeasurementSet's.
#[test]
fn a_mosaic_images_the_msv4_conversion_as_the_measurement_set() {
    let (observation, controls) = super::mosaic::mosaic_run(false);
    let store = converted(&observation, "mosaic.ps.zarr");
    assert_store_products(&observation, &store, "mosaic", controls);
}

/// An MSv4 store cannot add a data group, so a run that saves the model
/// into an MSv4 without a `model` group is refused when its update session
/// opens, before it images; the MeasurementSet's run adds `MODEL_DATA`.
#[test]
fn saving_the_model_into_an_msv4_without_a_model_group_is_refused() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let store = converted(&observation, "no-model.ps.zarr");
    let Err(refusal) = observation.try_image_store(
        &store,
        "msv4-model-column",
        json!({ "weighting": "natural", "deconvolver": "hogbom", "niter": 100,
                    "threshold": "0.005Jy", "savemodel": "modelcolumn" }),
    ) else {
        panic!("an MSv4 without a model group cannot take the model");
    };
    assert!(
        refusal.contains("adding a data group to an MSv4 store"),
        "{refusal}"
    );
}

/// A field that follows an ephemeris, tracked (`TRACKFIELD`). Without a
/// `phasecenter` the image is centred on the field's reference direction,
/// the body at the FIELD row's `TIME`, which an MSv4 does not carry (the
/// conversion reports it lost): an MSv4 field is taken at its first
/// selected time instead, and the image moves with the body between the
/// two (imaging port 2.4, step 6c).
#[test]
fn a_tracked_moving_field_images_the_msv4_conversion_as_the_measurement_set() {
    let observation = super::ephemeris::moving_track();
    let store = converted(&observation, "moving.ps.zarr");
    assert_store_products(
        &observation,
        &store,
        "moving-trackfield",
        json!({ "weighting": "natural", "niter": 0, "phasecenter": "TRACKFIELD" }),
    );
}
