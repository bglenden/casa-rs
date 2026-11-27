// SPDX-License-Identifier: LGPL-3.0-or-later

//! Internal source-law checks moved out of external test crates so
//! `source_law` remains private to `casa-imaging`.

use std::path::{Path, PathBuf};

use casa_core::measures::frequency::FrequencyRef;
use casa_core::{ArrayValue, PrimitiveType, Value};
use casa_ms::{MeasurementSet, SubtableId};
use casa_simulation::synthetic::{
    AnalyticComponent, AnalyticSpectrum, ObservationRequest, SkyModel, SpectralSetup, generate,
    tutorial_vla_a_antennas,
};
use casa_tables::ColumnSchema;
use ndarray::ArrayD;
use serde_json::json;

use super::{gridding_layouts, ordinary_write_uses_parent_selection, prediction_layouts};
use crate::ImagingRequest;
use crate::counting_heap::{context, request as base_request};

fn drifting_track(root: &Path, width_hz: f64) -> PathBuf {
    let ms_path = root.join("drifting.ms");
    let mut request = ObservationRequest::vla_ppdisk(
        root.join("unused.fits"),
        &ms_path,
        tutorial_vla_a_antennas(),
    );
    request.field_name = "t1".to_string();
    request.phase_center_rad = [4.712_391_234_768_306, -0.401_423_788_703_971_4];
    request.duration_seconds = 3_600.0;
    request.integration_seconds = 24.0;
    request.start_time_mjd_seconds = 4_895_229_545.733_246;
    request.spectral_windows = vec![SpectralSetup {
        name: "t1-qband".to_string(),
        start_frequency_hz: 44.0e9,
        channel_width_hz: width_hz,
        channel_count: 40,
    }];
    request.model = Some(SkyModel::AnalyticComponents {
        path: None,
        schema_version: Some(1),
        name: Some("t1-sky".to_string()),
        components: vec![AnalyticComponent::Point {
            name: Some("point".to_string()),
            l_rad: 0.0,
            m_rad: 0.0,
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
    generate(&request).expect("synthesise drifting track");
    let mut ms = MeasurementSet::open(&ms_path).unwrap();
    ms.spectral_window_mut()
        .unwrap()
        .set_i32(0, "MEAS_FREQ_REF", FrequencyRef::TOPO.casacore_code())
        .unwrap();
    ms.save().unwrap();
    ms_path
}

fn track_request(ms: &Path, image: &Path, controls: serde_json::Value) -> ImagingRequest {
    let mut values = json!({
        "vis": ms,
        "imagename": image,
        "imsize": 256,
        "cell": "0.012arcsec",
        "backend": "cpu",
    });
    values
        .as_object_mut()
        .unwrap()
        .extend(controls.as_object().unwrap().clone());
    base_request(values)
}

#[test]
fn parent_prediction_rows_match_casas_drifting_channel_layouts() {
    let root = tempfile::tempdir().unwrap();
    let ms = drifting_track(root.path(), 2.0e3);
    let request = track_request(
        &ms,
        &root.path().join("compact-prediction-rows"),
        json!({
            "specmode": "cube", "spw": "0:20", "start": 20, "width": 1, "nchan": 1,
            "niter": 5, "nsigma": 0.01, "savemodel": "modelcolumn",
        }),
    );
    let layouts = prediction_layouts(&request, &context(request.resource_policy()))
        .expect("the parent prediction traversal");
    let mut actual = layouts
        .iter()
        .map(|layout| layout.channels.clone())
        .collect::<Vec<_>>();
    actual.sort();
    let mut casa = (15..=19)
        .flat_map(|first| {
            [
                (first..first + 6).collect::<Vec<_>>(),
                (first..first + 7).collect::<Vec<_>>(),
            ]
        })
        .chain(std::iter::once((20..26).collect::<Vec<_>>()))
        .collect::<Vec<_>>();
    casa.sort();
    assert_eq!(
        actual, casa,
        "CASA's physical parent selections over the drift"
    );
    assert!(
        layouts
            .iter()
            .all(|layout| layout.flags.iter().all(|flag| !flag)),
        "the selected unflagged cells remain unflagged: {layouts:?}"
    );
}

#[test]
fn parent_prediction_rows_include_casas_singleton_channel() {
    let root = tempfile::tempdir().unwrap();
    let ms = drifting_track(root.path(), 20_000.0);
    let request = track_request(
        &ms,
        &root.path().join("compact-singleton-rows"),
        json!({
            "specmode": "cube", "spw": "0:20", "start": 20, "width": 1, "nchan": 2,
            "niter": 5, "nsigma": 0.01, "savemodel": "modelcolumn",
        }),
    );
    let layouts = prediction_layouts(&request, &context(request.resource_policy()))
        .expect("the parent prediction traversal");
    let mut actual = layouts
        .iter()
        .map(|layout| layout.channels.clone())
        .collect::<Vec<_>>();
    actual.sort();
    assert_eq!(
        actual,
        [vec![19, 20], vec![20], vec![20, 21]],
        "CASA's parent selections"
    );
    assert!(
        layouts
            .iter()
            .all(|layout| layout.flags.iter().all(|flag| !flag))
    );
}

#[test]
fn continuum_fit_only_channels_do_not_enter_tuned_gridding_rows() {
    let root = tempfile::tempdir().unwrap();
    let ms = drifting_track(root.path(), 20_000.0);
    let request = track_request(
        &ms,
        &root.path().join("fitted-grid-layout"),
        json!({
            "specmode": "cube", "spw": "0:20", "start": 20, "width": 1, "nchan": 2,
            "niter": 0, "fitspw": "0:0;39",
        }),
    );
    let layouts = gridding_layouts(&request, &context(request.resource_policy()))
        .expect("the tuned gridding traversal");
    assert_eq!(
        layouts,
        [vec![20]],
        "fit-only channels cannot enter a gridding row"
    );
}

#[test]
fn final_prediction_excludes_continuum_fit_only_channels() {
    let root = tempfile::tempdir().unwrap();
    let ms = drifting_track(root.path(), 20_000.0);
    let request = track_request(
        &ms,
        &root.path().join("fitted-prediction-layout"),
        json!({
            "specmode": "cube", "spw": "0", "start": 20, "width": 1, "nchan": 2,
            "niter": 5, "nsigma": 0.01, "savemodel": "modelcolumn", "fitspw": "0:0;39",
        }),
    );
    let ctx = context(request.resource_policy());
    let mut tuned = gridding_layouts(&request, &ctx).expect("the tuned gridding traversal");
    let mut predicted = prediction_layouts(&request, &ctx)
        .expect("the final prediction traversal")
        .into_iter()
        .map(|layout| layout.channels)
        .collect::<Vec<_>>();
    tuned.sort();
    predicted.sort();
    assert_eq!(
        predicted, tuned,
        "prediction must not interpolate through fit-only or unrelated channels"
    );
    assert!(
        predicted
            .iter()
            .all(|channels| !channels.contains(&0) && !channels.contains(&39))
    );
}

#[test]
fn corrected_only_writer_uses_tuned_continuum_rows() {
    let root = tempfile::tempdir().unwrap();
    let ms_path = drifting_track(root.path(), 20_000.0);
    let mut ms = MeasurementSet::open(&ms_path).unwrap();
    let Some(Value::Array(ArrayValue::Complex32(data))) = ms
        .main_table()
        .column_accessor("DATA")
        .unwrap()
        .get(0)
        .unwrap()
        .cloned()
    else {
        panic!("DATA is a complex array");
    };
    let shape = data.shape().to_vec();
    ms.main_table_mut()
        .add_column(
            ColumnSchema::array_fixed("CORRECTED_DATA", PrimitiveType::Complex32, shape.clone()),
            Some(Value::Array(ArrayValue::Complex32(ArrayD::zeros(shape)))),
        )
        .unwrap();
    ms.save().unwrap();
    drop(ms);

    let request = track_request(
        &ms_path,
        &root.path().join("corrected-only-source"),
        json!({
            "specmode": "cube", "spw": "0:20", "start": 20, "width": 1, "nchan": 2,
            "niter": 0, "nsigma": 0.01, "fitspw": "0:0;39",
            "save_continuum_residual": true,
        }),
    );
    assert!(
        !ordinary_write_uses_parent_selection(&request, &context(request.resource_policy()))
            .expect("corrected-only writer"),
        "a corrected-only writer must request tuned rows",
    );
}

#[test]
fn fitted_gridding_layout_excludes_outer_fit_channels() {
    let root = tempfile::tempdir().unwrap();
    let ms_path = drifting_track(root.path(), 1.0e6);
    let mut ms = MeasurementSet::open(&ms_path).unwrap();
    let mut frequencies = (0..40)
        .map(|channel| 44.0e9 + f64::from(channel) * 1.0e6)
        .collect::<Vec<_>>();
    frequencies[1] = 44.00025e9;
    ms.subtable_mut(SubtableId::SpectralWindow)
        .unwrap()
        .row_accessor_mut()
        .set_cell(
            0,
            "CHAN_FREQ",
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(vec![40], frequencies).unwrap(),
            )),
        )
        .unwrap();
    ms.save().unwrap();
    drop(ms);

    let request = track_request(
        &ms_path,
        &root.path().join("fitted-layout"),
        json!({
            "channel_count": 2,
            "specmode": "cube",
            "outframe": "TOPO",
            "spw": "0",
            "start": "44.02025GHz",
            "width": "1MHz",
            "niter": 5,
            "nsigma": 0.01,
            "savemodel": "modelcolumn",
            "fitspw": "0:0;39",
            "fitorder": 0,
        }),
    );
    assert_eq!(
        gridding_layouts(&request, &context(request.resource_policy())).unwrap(),
        [(16..27).collect::<Vec<_>>()],
    );
}
