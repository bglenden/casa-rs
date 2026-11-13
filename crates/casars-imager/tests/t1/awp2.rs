// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_ms::{SyntheticSpectralSetup, tutorial_vla_a_antennas};
use serde_json::json;

use super::fixture::{Component, Geometry, Observation, Setup};

/// A phase-centred point recovers its injected flux with the AWPLPG
/// operator and publishes the separately gridded sensitivity and PB.
#[test]
fn awp2_dirty_continuum_recovers_a_phase_centred_point() {
    let mut antennas = tutorial_vla_a_antennas();
    let origin = antennas[0].position_m;
    for antenna in &mut antennas {
        for (value, centre) in antenna.position_m.iter_mut().zip(origin) {
            *value = centre + (*value - centre) / 35.0;
        }
    }
    let geometry = Geometry::square(100, 8.0);
    let observation = Observation::synthesise_setup(
        Setup {
            telescope: "VLA".into(),
            antennas,
            fields: Vec::new(),
            spectral_windows: vec![SyntheticSpectralSetup {
                name: "awp2 L band".into(),
                start_frequency_hz: 1.25e9,
                channel_width_hz: 1e8,
                channel_count: 4,
            }],
            geometry,
            noise_jy: 0.0,
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
    let mut ms = casa_ms::MeasurementSet::open(observation.measurement_set()).unwrap();
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
