// SPDX-License-Identifier: LGPL-3.0-or-later
//! The developer fixture route uses native session prediction and persistence.
use casa_core::ArrayValue;
use casa_ms::{MeasurementSet, VisibilityDataColumn};
use casa_simulation::synthetic::*;

#[test]
fn storage_fixtures_explicitly_disable_geometric_observing_cuts() {
    let root = tempfile::tempdir().unwrap();
    let mut request = ObservationRequest::vla_ppdisk(
        "unused.fits",
        root.path().join("cut.ms"),
        tutorial_vla_a_antennas().into_iter().take(3).collect(),
    );
    request.predict_model = false;
    request.duration_seconds = 10.;
    request.integration_seconds = 10.;
    // The south celestial pole is below the horizon at the northern VLA.
    request.phase_center_rad = [0., -std::f64::consts::FRAC_PI_2];
    let cut = generate(&request).unwrap();
    assert_eq!(cut.flagged_row_count, cut.main_row_count);
    request.output_ms = root.path().join("uncut.ms");
    let uncut = generate_unflagged(&request).unwrap();
    assert_eq!(uncut.main_row_count, cut.main_row_count);
    assert_eq!(uncut.flagged_row_count, 0);
}

#[test]
fn two_windows_predict_a_phase_center_point_to_its_analytic_visibility() {
    let root = tempfile::tempdir().unwrap();
    let mut request = ObservationRequest::vla_ppdisk(
        "unused.fits",
        root.path().join("point.ms"),
        vec![
            Antenna::vla("A0", "P0", [-1_601_185.4, -5_041_977.5, 3_554_875.9]),
            Antenna::vla("A1", "P1", [-1_601_085.4, -5_041_977.5, 3_554_875.9]),
            Antenna::vla("A2", "P2", [-1_601_185.4, -5_041_877.5, 3_554_875.9]),
        ],
    );
    request.duration_seconds = 20.;
    request.integration_seconds = 10.;
    request.allow_below_elevation_limit = true;
    request.spectral_windows = [1e9, 2e9]
        .into_iter()
        .enumerate()
        .map(|(i, f)| SpectralSetup {
            name: format!("window-{i}"),
            start_frequency_hz: f,
            channel_width_hz: 1e6,
            channel_count: 2,
        })
        .collect();
    request.model = Some(SkyModel::AnalyticComponents {
        path: None,
        schema_version: None,
        name: None,
        components: vec![AnalyticComponent::Point {
            name: None,
            l_rad: 0.,
            m_rad: 0.,
            spectrum: AnalyticSpectrum {
                flux_jy: 1.25,
                spectral_index: 0.,
                reference_frequency_hz: Some(1e9),
                line_peak_jy: 0.,
                line_center_fraction: 0.5,
                line_sigma_fraction: 0.1,
                absorption_peak_jy: 0.,
                absorption_center_fraction: 0.5,
                absorption_sigma_fraction: 0.1,
            },
        }],
    });
    let report = generate(&request).unwrap();
    assert_eq!(report.main_row_count, 12);
    let ms = MeasurementSet::open(&report.output_ms).unwrap();
    assert!(ms.validate().unwrap().is_empty());
    let data = ms.data_column(VisibilityDataColumn::Data).unwrap();
    for row in 0..report.main_row_count {
        let ArrayValue::Complex32(values) = data.get(row).unwrap() else {
            panic!("complex DATA")
        };
        for value in values {
            assert!(
                (value.re - 1.25).abs() < 1e-5 && value.im.abs() < 1e-5,
                "{value:?}"
            );
        }
    }
}
