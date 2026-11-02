// SPDX-License-Identifier: LGPL-3.0-or-later
//! Write the base MSv2 of the MSv2/MSv4 trial fixture
//! (`docs/measurement-set/trial/README.md`).
//!
//! Five VLA-A antennas observe two point sources for two minutes in 10 s
//! integrations: eight 128 MHz Q-band channels in LSRK, four linear
//! correlations, a slightly polarized source so the cross hands carry
//! signal, simple noise and random antenna gains that change every 60 s, so
//! a 60 s gain solve sees two solution intervals. Everything is seeded; the
//! output is the same on every run. `tools/ms-trial/make_fixtures.py` then
//! sets deterministic flags and weights and converts the result to MSv4
//! with XRADIO.
//!
//! ```text
//! cargo run -p casa-ms --example ms_trial_fixture -- OUTPUT.ms
//! ```

use std::path::PathBuf;

use casa_ms::{
    SyntheticAnalyticComponent, SyntheticAnalyticSpectrum, SyntheticObservationRequest,
    SyntheticPolarizationBasis, SyntheticPolarizationSetup, SyntheticSkyModel,
    SyntheticSpectralSetup, SyntheticWorkerPolicy, generate_synthetic_observation_ms,
    tutorial_vla_a_antennas,
};

/// Image cell the trial images use: about a quarter of the natural beam.
const CELL_ARCSEC: f64 = 0.012;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: ms_trial_fixture OUTPUT.ms")?;
    let antennas = tutorial_vla_a_antennas().into_iter().take(5).collect();
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        output.with_extension("unused.fits"),
        &output,
        antennas,
    );
    request.overwrite = true;
    request.project = "casa-rs-ms-trial".to_string();
    request.field_name = "trial".to_string();
    request.duration_seconds = 120.0;
    request.integration_seconds = 10.0;
    request.spectral_windows = vec![SyntheticSpectralSetup {
        name: "trial".to_string(),
        start_frequency_hz: 44.0e9,
        channel_width_hz: 128.0e6,
        channel_count: 8,
    }];
    request.polarization_setup = SyntheticPolarizationSetup {
        basis: SyntheticPolarizationBasis::Linear,
        correlation_count: 4,
        source_stokes_fractional_quv: [0.08, -0.05, 0.0],
    };
    let cell_rad = (CELL_ARCSEC / 3_600.0_f64).to_radians();
    let spectrum = |flux_jy: f64, spectral_index: f64| SyntheticAnalyticSpectrum {
        flux_jy,
        spectral_index,
        reference_frequency_hz: Some(44.448e9),
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
        name: Some("trial".to_string()),
        components: vec![
            SyntheticAnalyticComponent::Point {
                name: Some("centre".to_string()),
                l_rad: 0.0,
                m_rad: 0.0,
                spectrum: spectrum(1.0, 0.0),
            },
            SyntheticAnalyticComponent::Point {
                name: Some("offset".to_string()),
                l_rad: -14.0 * cell_rad,
                m_rad: 9.0 * cell_rad,
                spectrum: spectrum(0.1, -0.8),
            },
        ],
    });
    request.model_peak_jy_per_pixel = None;
    // The generator's corruption indexes antenna gains by correlation and
    // supports at most two correlations, even for noise alone, so
    // make_fixtures.py applies the noise and known per-receptor gains.
    request.corruption = None;
    request.worker_policy = SyntheticWorkerPolicy::Fixed;
    request.row_workers = Some(1);
    request.channel_workers = Some(1);
    let report = generate_synthetic_observation_ms(&request)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
