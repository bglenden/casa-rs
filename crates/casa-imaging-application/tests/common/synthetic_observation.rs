// SPDX-License-Identifier: LGPL-3.0-or-later
//! A synthetic observation of a point and a Gaussian, which the admission
//! and worker-fit laws image.

use std::path::{Path, PathBuf};

use casa_ms::{
    SyntheticAnalyticComponent, SyntheticAnalyticSpectrum, SyntheticObservationRequest,
    SyntheticSkyModel, SyntheticSpectralSetup, generate_synthetic_observation_ms,
    tutorial_vla_a_antennas,
};

/// Cell of every image of the observation; the natural beam spans about
/// four.
pub const CELL_ARCSEC: f64 = 0.012;

/// A point and a Gaussian observed by a short VLA-A Q-band track in
/// `channels` 128 MHz channels.
pub fn observation(root: &Path, channels: usize) -> PathBuf {
    observation_as(root, channels, "VLA")
}

/// [`observation`] recorded as `telescope`'s, the ALMA one with 12 m dishes
/// on the VLA-A pads: the mosaic gridder's beams are ALMA's.
pub fn observation_as(root: &Path, channels: usize, telescope: &str) -> PathBuf {
    let measurement_set = root.join(format!("admission-{telescope}.ms"));
    let mut antennas = tutorial_vla_a_antennas();
    if telescope == "ALMA" {
        for antenna in &mut antennas {
            antenna.dish_diameter_m = 12.0;
        }
    }
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        root.join("unused.fits"),
        &measurement_set,
        antennas,
    );
    request.telescope_name = telescope.to_string();
    request.duration_seconds = 600.0;
    request.integration_seconds = 60.0;
    request.spectral_windows = vec![SyntheticSpectralSetup {
        name: "admission".to_string(),
        start_frequency_hz: 44.0e9,
        channel_width_hz: 128.0e6,
        channel_count: channels,
    }];
    let cell_rad = (CELL_ARCSEC / 3_600.0_f64).to_radians();
    let spectrum = |flux_jy: f64, spectral_index: f64| SyntheticAnalyticSpectrum {
        flux_jy,
        spectral_index,
        reference_frequency_hz: Some(44.2e9),
        line_peak_jy: 0.0,
        line_center_fraction: 0.5,
        line_sigma_fraction: 0.1,
        absorption_peak_jy: 0.0,
        absorption_center_fraction: 0.5,
        absorption_sigma_fraction: 0.1,
    };
    request.model = Some(SyntheticSkyModel::AnalyticComponents {
        path: None,
        schema_version: Some(1),
        name: Some("admission".to_string()),
        components: vec![
            SyntheticAnalyticComponent::Point {
                name: Some("point".to_string()),
                l_rad: -20.0 * cell_rad,
                m_rad: 12.0 * cell_rad,
                spectrum: spectrum(1.0, -0.7),
            },
            SyntheticAnalyticComponent::Gaussian {
                name: Some("gaussian".to_string()),
                l_rad: 30.0 * cell_rad,
                m_rad: -25.0 * cell_rad,
                major_fwhm_rad: 8.0 * cell_rad,
                minor_fwhm_rad: 8.0 * cell_rad,
                position_angle_rad: 0.0,
                spectrum: spectrum(0.5, 0.0),
            },
        ],
    });
    generate_synthetic_observation_ms(&request).expect("synthesise the admission MS");
    measurement_set
}
