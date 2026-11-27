// SPDX-License-Identifier: LGPL-3.0-or-later
//! Baseline projection and visibility amplitudes for calibrator models.

use std::path::{Path, PathBuf};

use casa_components::{SkyComponent, SpectralKind};
use casa_core::measures::baseline::UvwFrame;
use casa_core::measures::direction::{DirectionRef, MDirection, RiseSetTimes};
use casa_core::measures::epoch::{EpochRef, MEpoch};
use casa_core::measures::frame::MeasFrame;
use casa_ms::open_measures_runtime;

use crate::readantenna::read_antenna_config;

/// Prepared plot data from one integration centred on the requested epoch.
pub struct CalibratorBaselines {
    /// Resolved array configuration.
    pub antenna_path: PathBuf,
    /// Topocentric azimuth and elevation.
    pub azel: MDirection,
    /// Rise and set times for the model direction.
    pub riseset: RiseSetTimes,
    /// Projected baseline lengths in metres, in antenna-index order.
    pub lengths_m: Vec<f64>,
    /// Stokes I visibility amplitudes, one row in Jy per frequency.
    pub amplitudes_jy: Vec<Vec<f64>>,
}

/// Project each cross-correlation baseline and sample the calibrated component.
///
/// # Errors
///
/// An invalid array configuration, missing Measures data, or an unsupported
/// component shape or spectrum.
pub fn calibrator_baselines(
    workspace: &Path,
    antenna_list: &str,
    component: &SkyComponent,
    epoch: &MEpoch,
    frequencies_hz: &[f64],
    data_roots: &[PathBuf],
) -> Result<CalibratorBaselines, String> {
    let path = resolve_config(workspace, antenna_list, data_roots)?;
    let measures = open_measures_runtime().map_err(error_text)?;
    let config = read_antenna_config(&path, measures.as_ref()).map_err(error_text)?;
    let direction = component.shape().reference_direction();
    let frame = MeasFrame::new()
        .with_measures(measures.clone())
        .with_epoch(epoch.clone())
        .with_position(config.observatory.clone())
        .with_direction(direction.clone());
    let azel = direction
        .convert_to(DirectionRef::AZEL, &frame)
        .map_err(error_text)?;
    let riseset = direction.rise_set_times(&frame).map_err(error_text)?;
    let mut plot = CalibratorBaselines {
        antenna_path: path,
        azel,
        riseset,
        lengths_m: Vec::new(),
        amplitudes_jy: vec![Vec::new(); frequencies_hz.len()],
    };
    if plot.azel.latitude_rad() <= 0. {
        return Ok(plot);
    }
    let direction = direction
        .convert_to(DirectionRef::J2000, &frame)
        .map_err(error_text)?;
    // The simulator interprets its stored UTC timestamp as UT1 for UVW.
    let epoch = epoch
        .convert_to(EpochRef::UTC, &frame)
        .map_err(error_text)?;
    let uvw_frame = UvwFrame::new(
        [direction.longitude_rad(), direction.latitude_rad()],
        MEpoch::from_mjd(epoch.value().as_mjd(), EpochRef::UT1),
        &config.observatory,
        measures,
    )
    .map_err(error_text)?;
    let antennas: Vec<_> = config
        .x
        .iter()
        .zip(&config.y)
        .zip(&config.z)
        .map(|((&x, &y), &z)| [x, y, z])
        .collect();
    let origin = config.observatory.as_itrf();
    let antenna_uvw: Vec<_> = antennas
        .iter()
        .map(|p| uvw_frame.baseline_uvw(std::array::from_fn(|i| origin[i] - p[i])))
        .collect();
    let transform = component.transform().map_err(error_text)?;
    let flux = flux_samples(component, frequencies_hz)?;
    for (i, first) in antenna_uvw.iter().enumerate() {
        for second in &antenna_uvw[i + 1..] {
            let uvw: [f64; 3] = std::array::from_fn(|axis| second[axis] - first[axis]);
            plot.lengths_m.push(uvw[0].hypot(uvw[1]));
            let sample = transform.at_baseline(uvw);
            for ((&frequency, &flux), values) in frequencies_hz
                .iter()
                .zip(&flux)
                .zip(&mut plot.amplitudes_jy)
            {
                values.push((sample.at_frequency(frequency).map_err(error_text)? * flux).abs());
            }
        }
    }
    Ok(plot)
}

fn flux_samples(component: &SkyComponent, frequencies_hz: &[f64]) -> Result<Vec<f64>, String> {
    let spectrum = component.spectrum();
    if let SpectralKind::Tabular(table) = spectrum.kind()
        && table.frequencies_hz() == frequencies_hz
    {
        return Ok(table.stokes_flux().iter().map(|stokes| stokes[0]).collect());
    }
    let stokes = component.flux().stokes_jy_values().map_err(error_text)?;
    frequencies_hz
        .iter()
        .map(|&hz| {
            spectrum
                .sample_stokes(hz, stokes)
                .map(|sample| sample[0])
                .map_err(error_text)
        })
        .collect()
}

fn resolve_config(workspace: &Path, name: &str, data_roots: &[PathBuf]) -> Result<PathBuf, String> {
    let given = workspace.join(name);
    if given.is_file() {
        return Ok(given);
    }
    data_roots
        .iter()
        .map(|root| root.join("alma/simmos").join(name))
        .find(|path| path.is_file())
        .ok_or_else(|| format!("Couldn't find antennalist: {name}"))
}

fn error_text(error: impl std::fmt::Display) -> String {
    error.to_string()
}
