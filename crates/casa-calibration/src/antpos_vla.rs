// SPDX-License-Identifier: LGPL-3.0-or-later
//! VLA semester 2016B online tropospheric delay correction for KAntPos.

use casa_ms::{MeasurementSet, MsResult, derived::engine::MsCalEngine};

pub(crate) const TROPO_KEYWORD: &str = "VLATrDelCorr";

pub(crate) fn only_vla_observations(ms: &MeasurementSet) -> MsResult<bool> {
    let observations = ms.observation()?;
    for row in 0..observations.row_count() {
        if !observations.string(row, "TELESCOPE_NAME")?.contains("VLA") {
            return Ok(false);
        }
    }
    Ok(observations.row_count() != 0)
}

pub(crate) struct TropoDelay {
    arms: Vec<Option<Arm>>,
}

struct Arm {
    azimuth_rad: f64,
    distance_m: f64,
}

impl TropoDelay {
    pub(crate) fn new(ms: &MeasurementSet, engine: &MsCalEngine, scale: f64) -> MsResult<Self> {
        let antennas = ms.antenna()?;
        let center = engine.observatory_position().as_itrf();
        let arms = (0..antennas.row_count())
            .map(|row| {
                let station = antennas.station(row)?;
                let stripped = station
                    .strip_prefix("EVLA:")
                    .or_else(|| station.strip_prefix("VLA:"));
                let azimuth = arm_azimuth(stripped.unwrap_or(&station), stripped.is_some());
                let Some(azimuth_rad) = azimuth else {
                    return Ok(None);
                };
                let position = antennas.position(row)?;
                let radius = position
                    .into_iter()
                    .zip(center)
                    .map(|(position, center)| (position - center).powi(2))
                    .sum::<f64>()
                    .sqrt();
                let distance_m =
                    radius * (-1.0e-15 * scale) * crate::execute::SPEED_OF_LIGHT_M_PER_S;
                Ok(Some(Arm {
                    azimuth_rad,
                    distance_m,
                }))
            })
            .collect::<MsResult<_>>()?;
        Ok(Self { arms })
    }

    pub(crate) fn metres(
        &self,
        engine: &MsCalEngine,
        time_seconds: f64,
        field: usize,
        antenna: usize,
    ) -> MsResult<f64> {
        let Some(Some(arm)) = self.arms.get(antenna) else {
            return Ok(0.0);
        };
        let (azimuth, elevation) = engine.azel(time_seconds, field, antenna)?;
        Ok((azimuth - arm.azimuth_rad).cos() / elevation.tan() / elevation.sin() * arm.distance_m)
    }
}

fn arm_azimuth(station: &str, has_prefix: bool) -> Option<f64> {
    [('N', 355.0_f64), ('E', 115.0), ('W', 236.0)]
        .into_iter()
        .find(|(arm, _)| {
            if has_prefix {
                station.starts_with(*arm)
            } else {
                station.contains(*arm)
            }
        })
        .map(|(_, degrees)| degrees.to_radians())
}
