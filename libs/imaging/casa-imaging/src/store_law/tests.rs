// SPDX-License-Identifier: LGPL-3.0-or-later
//! The store law where the other store reverses a window (#1014): CASA's
//! ALMA test MeasurementSet (`mssel_test_small.ms`, its window 1's 64
//! channels descending at −31.25 MHz) and XRADIO's conversion of it, which
//! writes that window ascending (casa-ms's fixtures).

use std::path::{Path, PathBuf};

use serde_json::json;

use super::compare_stores;
use crate::{Cancel, HostResources, RunContext};

/// Unpack casa-ms fixture archive `archive` into `root`; the path of
/// `name` in it.
fn unpack(root: &Path, archive: &str, name: &str) -> PathBuf {
    let archive = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../libs/casacore/casa-ms/tests/fixtures")
        .join(archive);
    tar::Archive::new(flate2::read::GzDecoder::new(
        std::fs::File::open(archive).expect("fixture"),
    ))
    .unpack(root)
    .expect("unpack");
    root.join(name)
}

/// Every MAIN row of the MeasurementSet matches its sample in XRADIO's
/// conversion, and every row's geometry agrees to the law's tolerance,
/// each channel's frequency included: window 1's channel `c` is compared
/// with XRADIO's channel `63 - c`, the same channel, not the one at the
/// same position, which lies elsewhere in the band.
#[test]
fn a_window_another_store_reverses_holds_the_store_law() {
    let root = tempfile::tempdir().expect("root");
    let measurement_set = unpack(
        root.path(),
        "mssel_test_small.ms.tgz",
        "mssel_test_small.ms",
    );
    let converted = unpack(
        root.path(),
        "ms_trial/mssel_test_small.ps.zarr.tgz",
        "mssel_test_small.ps.zarr",
    );
    let request = crate::request::tests::request(json!({
        "vis": measurement_set,
        "imagename": root.path().join("law"),
        "imsize": [64, 64],
        "cell": "2arcsec",
        "backend": "cpu",
    }));
    let context = RunContext {
        host: HostResources::detect().expect("host resources"),
        policy: request.resource_policy(),
        cancel: Cancel::default(),
        summary: None,
    };
    let comparison = compare_stores(&request, &context, &converted).expect("both stores' geometry");
    assert_eq!(comparison.rows, 1058, "every MAIN row: {comparison:?}");
    assert_eq!(comparison.unmatched, 0, "{comparison:?}");
    assert!(comparison.holds(), "{comparison:?}");
    // 529 rows of 64 channels and 529 of one.
    assert_eq!(
        comparison.compared.get("frequency"),
        Some(&(529 * 64 + 529))
    );
}

#[test]
fn a_moving_field_keeps_its_geometry_in_msv4() {
    use casa_core::{ArrayValue, PrimitiveType, ScalarValue, Value};
    use casa_ms::{MeasurementSet, SubtableId};
    use casa_mstransform::convert::{ConversionOptions, LossKind, PaddingRows, to_msv4};
    use casa_simulation::synthetic::{
        AnalyticComponent, AnalyticSpectrum, CorruptionConfig, NoiseCorruption, NoiseMode,
        ObservationRequest, SkyModel, SpectralSetup, generate, tutorial_vla_a_antennas,
    };
    use casa_test_support::ephemeris_table::{
        EphemerisRow, write_ephemeris_table, write_field_ephemeris,
    };

    const PHASE_CENTER_RAD: [f64; 2] = [4.712_391_234_768_306, -0.401_423_788_703_971_4];
    const CELL_RAD: f64 = 0.012 * std::f64::consts::PI / (180.0 * 3_600.0);
    let body = || -> Vec<EphemerisRow> {
        let [ra, dec] = PHASE_CENTER_RAD.map(f64::to_degrees);
        let drift_deg = 1.0e-6_f64.to_degrees();
        (0..3)
            .map(|day| {
                let step = f64::from(day);
                [
                    56_656.5 + step,
                    ra + step * drift_deg,
                    dec - step * drift_deg,
                    30.0 + 1.0e-4 * step,
                    0.01,
                ]
            })
            .collect()
    };

    let root = tempfile::tempdir().expect("root");
    let ms_path = root.path().join("moving.ms");
    let mut sim_request = ObservationRequest::vla_ppdisk(
        root.path().join("unused.fits"),
        &ms_path,
        tutorial_vla_a_antennas(),
    );
    sim_request.field_name = "t1".to_string();
    sim_request.phase_center_rad = PHASE_CENTER_RAD;
    sim_request.duration_seconds = 3_600.0;
    sim_request.integration_seconds = 24.0;
    sim_request.start_time_mjd_seconds = 4_895_229_545.733_246;
    sim_request.spectral_windows = vec![SpectralSetup {
        name: "t1-qband".to_string(),
        start_frequency_hz: 44.0e9,
        channel_width_hz: 128.0e6,
        channel_count: 4,
    }];
    sim_request.model = Some(SkyModel::AnalyticComponents {
        path: None,
        schema_version: Some(1),
        name: Some("t1-sky".to_string()),
        components: vec![AnalyticComponent::Point {
            name: Some("point".to_string()),
            l_rad: -10.0 * CELL_RAD,
            m_rad: -6.0 * CELL_RAD,
            spectrum: AnalyticSpectrum {
                flux_jy: 1.0,
                spectral_index: 0.0,
                reference_frequency_hz: Some(45.0e9),
                line_peak_jy: 0.0,
                line_center_fraction: 0.5,
                line_sigma_fraction: 0.1,
                absorption_peak_jy: 0.0,
                absorption_center_fraction: 0.5,
                absorption_sigma_fraction: 0.1,
            },
        }],
    });
    sim_request.corruption = Some(CorruptionConfig {
        seed: 20_261_007,
        noise: Some(NoiseCorruption {
            mode: NoiseMode::SimpleNoise,
            simplenoise_jy: 0.65,
        }),
        gain: None,
        leakage: None,
    });
    generate(&sim_request).expect("synthesise moving MS");

    let mut ms = MeasurementSet::open(&ms_path).expect("open MS");
    let reference_time = ms
        .main_table()
        .cell_accessor(0, "TIME")
        .unwrap()
        .scalar()
        .unwrap()
        .clone();
    let field = ms.subtable_mut(SubtableId::Field).expect("FIELD");
    field
        .fill_column_range(
            "TIME",
            casa_tables::RowRange::new(0, 1),
            Value::Scalar(reference_time),
        )
        .unwrap();
    field
        .add_column(
            casa_tables::ColumnSchema::scalar("EPHEMERIS_ID", PrimitiveType::Int32),
            Some(Value::Scalar(ScalarValue::Int32(0))),
        )
        .expect("EPHEMERIS_ID");
    let offset = Value::Array(ArrayValue::Float64(
        ndarray::ArrayD::from_shape_vec(vec![2, 1], vec![1.0e-7, -5.0e-8]).expect("a direction"),
    ));
    for column in ["PHASE_DIR", "DELAY_DIR", "REFERENCE_DIR"] {
        field
            .fill_column_range(column, casa_tables::RowRange::new(0, 1), offset.clone())
            .expect("the offset");
    }
    ms.save().expect("save");
    drop(ms);
    let body_rows = body();
    write_field_ephemeris(&ms_path, 0, "T1BODY", &body_rows);
    let table = root.path().join("t1body.tab");
    write_ephemeris_table(&table, "T1BODY", &body_rows);

    let converted = root.path().join("moving.ps.zarr");
    to_msv4(
        &ms_path,
        &converted,
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
            codec: casa_ms::observation_io::Msv4Codec::Zstd,
            padding: PaddingRows::Flagged,
            creator: "casars-imager t1".to_string(),
            creation_date: "2026-10-29T00:00:00".to_string(),
        },
    )
    .expect("the track as MSv4");

    let [ra, dec] = PHASE_CENTER_RAD;
    for (name, phasecenter) in [
        ("msv4-trackfield", "TRACKFIELD".to_string()),
        (
            "msv4-table",
            table.to_str().expect("UTF-8 path").to_string(),
        ),
        (
            "msv4-fixed",
            format!("J2000 {}rad {}rad", ra + 2.0e-6, dec - 1.0e-6),
        ),
    ] {
        let req = crate::counting_heap::request(json!({
            "vis": ms_path,
            "imagename": root.path().join(name),
            "imsize": [256, 256],
            "cell": ["0.012arcsec", "0.012arcsec"],
            "phasecenter": phasecenter,
            "backend": "cpu",
        }));
        let comparison = compare_stores(
            &req,
            &crate::counting_heap::context(req.resource_policy()),
            &converted,
        )
        .expect("both stores' geometry");
        assert_eq!(comparison.unmatched, 0, "{name}: {comparison:?}");
        assert!(
            comparison.holds(),
            "{name}: the converted store's geometry departs: {comparison:?}"
        );
    }
}
