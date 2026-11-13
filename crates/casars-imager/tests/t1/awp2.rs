// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_ms::{
    MeasurementSet, SyntheticPolarizationBasis, SyntheticPolarizationSetup, SyntheticSpectralSetup,
    VisibilityDataColumn, tutorial_vla_a_antennas,
};
use casa_types::{ArrayValue, ScalarValue, Value};
use serde_json::json;

use super::fixture::{Component, Geometry, Observation, Setup};

/// A phase-centred point recovers its injected flux with the AWPLPG
/// operator and publishes the separately gridded sensitivity and PB.
#[test]
fn awp2_dirty_continuum_recovers_a_phase_centred_point() {
    let geometry = Geometry::square(100, 8.0);
    let (observation, surface) = l_band_point("VLA", geometry, Vec::new());
    let (run, products) = observation.image(
        "awp2",
        json!({
            "gridder": "awp2",
            "evla_surface": surface,
            "weighting": "natural",
            "niter": 0,
            "write_pb": true,
        }),
    );
    assert_eq!((run.major_cycles, run.minor_iterations), (1, 0));
    assert_eq!(
        products.suffixes().into_iter().collect::<Vec<_>>(),
        [
            ".image",
            ".model",
            ".pb",
            ".psf",
            ".residual",
            ".sumwt",
            ".weight"
        ]
    );
    let centre = geometry.centre();
    let psf = &products.get(".psf").pixels;
    let image = &products.get(".image").pixels;
    let beam = &products.get(".pb").pixels;
    assert!((psf[centre] - 1.0).abs() < 1e-5);
    assert!(
        (image[centre] - 1.0).abs() < 0.002,
        "phase-centre flux {}",
        image[centre]
    );
    let weight = &products.get(".weight").pixels;
    let peak_weight = weight.iter().copied().fold(0.0, f32::max);
    assert!(peak_weight > 0.0);
    let peak_beam = beam.iter().copied().fold(0.0, f32::max);
    assert!((peak_beam - 1.0).abs() < 1e-5);
    let off_axis = [10, 10];
    assert!(beam[centre] > beam[off_axis]);
    for (&pb, &sensitivity) in beam.iter().zip(weight) {
        let normalized = (sensitivity.abs() / peak_weight).sqrt();
        let expected = if normalized > 0.2 { normalized } else { 0.0 };
        assert!((pb - expected).abs() < 1e-5, "{pb} vs {expected}");
    }
}

/// The two in-image fields share one clean. A third field whose beam
/// overlaps the image has no CASA mosaic convolution function.
#[test]
fn awp2_cleans_and_predicts_the_in_image_fields() {
    let geometry = Geometry::square(512, 10.0);
    let cell_rad = geometry.cell_rad()[0];
    let fields = [0.0, 72.0, 270.0]
        .map(|cells| Setup::pointing([cells * cell_rad, 0.0]))
        .to_vec();
    let (observation, surface) = l_band_point("EVLA", geometry, fields);
    let mut controls = json!({
        "gridder": "awp2",
        "evla_surface": surface,
        "field": "0,1",
        "phasecenter": "0",
        "niter": 0,
        "write_pb": true,
    });
    let (_, dirty) = observation.image("awp2-two-fields", controls.clone());
    controls["field"] = json!("0,1,2");
    controls["niter"] = json!(30);
    controls["savemodel"] = json!("modelcolumn");
    let (run, clean) = observation.image("awp2-clean", controls);
    assert_eq!(run.minor_iterations, 30);
    assert_eq!(
        clean.get(".sumwt").channel_values(),
        dirty.get(".sumwt").channel_values()
    );
    let centre = geometry.centre();
    let dirty_residual = dirty.get(".residual").pixels[centre];
    assert!(clean.get(".model").pixels[centre] > 0.8 * dirty_residual);
    assert!(clean.get(".residual").pixels[centre].abs() < 0.1 * dirty_residual);

    let ms = MeasurementSet::open(observation.measurement_set()).unwrap();
    let model = ms.data_column(VisibilityDataColumn::ModelData).unwrap();
    let mut by_field = [0_usize; 3];
    for row in 0..ms.row_count() {
        let ScalarValue::Int32(field) = ms
            .main_table()
            .cell_accessor(row, "FIELD_ID")
            .unwrap()
            .scalar()
            .unwrap()
        else {
            panic!("expected an integer field ID");
        };
        let ArrayValue::Complex32(values) = model.get(row).unwrap() else {
            panic!("expected a complex MODEL_DATA cell");
        };
        by_field[*field as usize] += values.iter().filter(|v| v.norm_sqr() > 0.0).count();
    }
    assert!(by_field[0] > 0 && by_field[1] > 0, "{by_field:?}");
    assert_eq!(by_field[2], 0, "outside field received a model prediction");
}

/// AWPLPG includes an all-FLAG_ROW integration in the PA survey at
/// MJD zero. That epoch is outside the native Measures model's range.
#[test]
fn awp2_squint_refuses_an_integration_without_a_valid_cross_row() {
    let (observation, surface) = l_band_point("VLA", Geometry::square(100, 8.0), Vec::new());
    let mut ms = MeasurementSet::open(observation.measurement_set()).unwrap();
    let time = |ms: &MeasurementSet, row| {
        ms.main_table()
            .cell_accessor(row, "TIME")
            .unwrap()
            .scalar()
            .unwrap()
            .clone()
    };
    let first = time(&ms, 0);
    for row in 0..ms.row_count() {
        if time(&ms, row) == first {
            ms.main_table_mut()
                .row_accessor_mut()
                .set_cell(row, "FLAG_ROW", Value::Scalar(ScalarValue::Bool(true)))
                .unwrap();
        }
    }
    ms.save().unwrap();
    drop(ms);
    let error = observation
        .try_image(
            "flagged-squint",
            json!({
                "gridder": "awp2",
                "evla_surface": surface,
                "stokes": "IV",
                "wprojplanes": 2,
                "computepastep": 15.0,
                "niter": 0,
            }),
        )
        .err()
        .expect("an all-flagged PA epoch is outside the verified range");
    assert!(error.contains("MJD-zero PA in CASA"), "{error}");
}

fn l_band_point(
    telescope: &str,
    geometry: Geometry,
    fields: Vec<[f64; 2]>,
) -> (Observation, std::path::PathBuf) {
    let mut antennas = tutorial_vla_a_antennas();
    let origin = antennas[0].position_m;
    for antenna in &mut antennas {
        for (value, centre) in antenna.position_m.iter_mut().zip(origin) {
            *value = centre + (*value - centre) / 35.0;
        }
    }
    let observation = Observation::synthesise_polarized(
        Setup {
            telescope: telescope.into(),
            antennas,
            fields,
            spectral_windows: vec![SyntheticSpectralSetup {
                name: "awp2 L band".into(),
                start_frequency_hz: 1.25e9,
                channel_width_hz: 1e8,
                channel_count: 4,
            }],
            geometry,
            noise_jy: 0.0,
        },
        SyntheticPolarizationSetup {
            basis: SyntheticPolarizationBasis::Circular,
            correlation_count: 2,
            source_stokes_fractional_quv: [0.0; 3],
        },
        &[(
            "centre",
            Component {
                offset_px: [0, 0],
                flux_jy: 1.0,
                fwhm_px: None,
                spectral_index: 0.0,
            },
        )],
    );
    let mut ms = MeasurementSet::open(observation.measurement_set()).unwrap();
    ms.spectral_window_mut()
        .unwrap()
        .set_i32(
            0,
            "MEAS_FREQ_REF",
            casa_types::measures::frequency::FrequencyRef::TOPO.casacore_code(),
        )
        .unwrap();
    ms.save().unwrap();
    drop(ms);
    let surface = observation.scratch("vla.surface");
    std::fs::write(&surface, super::aw_projection::surface_text()).unwrap();
    (observation, surface)
}
