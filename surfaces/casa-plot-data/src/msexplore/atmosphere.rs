// SPDX-License-Identifier: LGPL-3.0-or-later
//! Atmospheric transmission and sky-temperature curves for PlotMS.

use casa_atmosphere::{
    AbsorptionProfile, AtmosphereProfile, AtmosphereType, ProfileParameters, Sky, SpectralGrid,
};

#[derive(Clone, Copy)]
pub enum Overlay {
    Transmission,
    SkyTemperature,
}

pub struct AtmosphericConditions {
    pub profile: ProfileParameters,
    pub water_m: f64,
    pub airmass: f64,
}

impl AtmosphericConditions {
    pub fn new(telescope: &str, airmass: f64) -> Self {
        let alma = telescope == "ALMA";
        Self {
            profile: ProfileParameters {
                altitude_m: 5059.0,
                ground_pressure_hpa: if alma { 563.0 } else { 786.0 },
                ground_temperature_k: f64::from(273.15_f32),
                atmosphere: AtmosphereType::MidlatitudeWinter,
                ..ProfileParameters::default()
            },
            water_m: if alma { 0.001 } else { 0.005 },
            airmass,
        }
    }

    pub fn curve(
        &self,
        frequencies_hz: &[f64],
        overlay: Overlay,
    ) -> Result<Vec<f64>, casa_atmosphere::AtmosphereError> {
        let count = frequencies_hz.len();
        if count < 2 {
            return Ok(vec![0.0; count]);
        }

        let mut model_count = count;
        while model_count > 512 {
            model_count /= 2;
        }
        let mut curve = vec![-f64::NAN; count];
        let Some(grid) = regular_grid(frequencies_hz, model_count)? else {
            return Ok(curve);
        };
        let profile = AtmosphereProfile::new(&self.profile)?;
        let absorption = AbsorptionProfile::new(profile, grid);
        let sky = Sky::zenith(self.water_m);
        let stride = count / model_count;
        for channel in 0..model_count {
            let tau =
                absorption.dry_opacity(channel) + absorption.wet_opacity(channel, self.water_m);
            let slant_tau = self.airmass * tau;
            curve[channel * stride] = match overlay {
                Overlay::Transmission => 100.0 * (-slant_tau).exp(),
                Overlay::SkyTemperature => {
                    absorption.tebb_sky(channel, &sky)? * (-slant_tau).exp_m1() / (-tau).exp_m1()
                }
            };
        }
        Ok(curve)
    }
    pub fn image_curve(
        &self,
        frequencies_hz: &[f64],
        lo_hz: Option<f64>,
        overlay: Overlay,
    ) -> Result<Vec<f64>, casa_atmosphere::AtmosphereError> {
        let count = frequencies_hz.len();
        if count < 2 {
            return Ok(vec![0.0; count]);
        }
        let Some(lo) = lo_hz else {
            return Ok(vec![-f64::NAN; count]);
        };
        let reflected = frequencies_hz
            .iter()
            .map(|frequency| 2.0 * lo - frequency)
            .collect::<Vec<_>>();
        // CASA regularizes the image band before the ordinary atmospheric
        // calculation builds its own grid. Both passes affect odd channel counts.
        let Some(grid) = regular_grid(&reflected, count)? else {
            return Ok(vec![-f64::NAN; count]);
        };
        self.curve(&grid.frequencies_hz().collect::<Vec<_>>(), overlay)
    }
}

fn regular_grid(
    frequencies_hz: &[f64],
    model_count: usize,
) -> Result<Option<SpectralGrid>, casa_atmosphere::AtmosphereError> {
    let count = frequencies_hz.len();
    let step = (frequencies_hz[count - 1] - frequencies_hz[0]) / (model_count - 1) as f64;
    let middle = count / 2;
    let mut reference = (frequencies_hz[middle - 1] + frequencies_hz[middle]) * 0.5;
    if model_count.is_multiple_of(2) {
        reference -= step * 0.5;
    }
    if !reference.is_finite() || !step.is_finite() {
        return Ok(None);
    }
    let mut grid = SpectralGrid::new();
    grid.add_regular(model_count, (model_count - 1) / 2, reference, step)?;
    Ok(Some(grid))
}
