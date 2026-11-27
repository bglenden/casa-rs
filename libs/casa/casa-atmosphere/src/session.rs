// SPDX-License-Identifier: LGPL-3.0-or-later
//! The profile, windows and sky settings owned by one atmosphere tool.

use crate::{
    AbsorptionProfile, AtmosphereError, AtmosphereProfile, ProfileParameters, Sky, SpectralGrid,
};

/// Independent mutable state for the atmosphere tool.
#[derive(Debug, Default)]
pub struct AtmosphereSession {
    profile: Option<AtmosphereProfile>,
    spectrum: Option<AtmosphereSpectrum>,
}

/// The evaluated ATM channels and the current radiative-transfer settings.
#[derive(Debug)]
pub struct AtmosphereSpectrum {
    /// Absorption and refractivity at the ground-profile water column.
    pub absorption: AbsorptionProfile,
    /// User water column, airmass and background used by sky queries.
    pub sky: Sky,
}

impl AtmosphereSession {
    /// The attached atmospheric profile, when initialized.
    #[must_use]
    pub fn profile(&self) -> Option<&AtmosphereProfile> {
        self.profile.as_ref()
    }

    /// The configured spectral windows and sky settings, when initialized.
    #[must_use]
    pub fn spectrum(&self) -> Option<&AtmosphereSpectrum> {
        self.spectrum.as_ref()
    }

    /// Mutably access the configured sky for an explicit setting change.
    pub fn sky_mut(&mut self) -> Option<&mut Sky> {
        self.spectrum.as_mut().map(|spectrum| &mut spectrum.sky)
    }

    /// Clear this tool's profile, windows and sky settings.
    pub fn close(&mut self) {
        *self = Self::default();
    }

    /// Attach a new profile and clear the spectral state.
    pub fn set_profile(&mut self, profile: AtmosphereProfile) {
        self.profile = Some(profile);
        self.spectrum = None;
    }

    /// Replace the spectral windows and reset to the ground column at zenith.
    /// Return false when the tool has no atmospheric profile.
    pub fn set_grid(&mut self, grid: SpectralGrid) -> bool {
        let Some(profile) = &self.profile else {
            return false;
        };
        self.spectrum = Some(AtmosphereSpectrum {
            absorption: AbsorptionProfile::new(profile.clone(), grid),
            sky: Sky::zenith(profile.ground_water_column_m()),
        });
        true
    }

    /// Append one regular window and reset sky settings. Preserve all computed
    /// channels already in the spectrum. An unconfigured tool returns `None`.
    ///
    /// # Errors
    /// Invalid channel count or frequencies, as in [`SpectralGrid::add_regular`].
    pub fn append_window(
        &mut self,
        channels: usize,
        reference_channel: usize,
        reference_frequency_hz: f64,
        separation_hz: f64,
    ) -> Result<Option<usize>, AtmosphereError> {
        let Some(spectrum) = &mut self.spectrum else {
            return Ok(None);
        };
        let index = spectrum.absorption.append_regular_window(
            channels,
            reference_channel,
            reference_frequency_hz,
            separation_hz,
        )?;
        spectrum.sky = Sky::zenith(spectrum.absorption.ground_water_column_m());
        Ok(Some(index))
    }

    /// Apply the six mutable weather parameters using ATM's rebuild thresholds.
    /// A successful call resets user water even if the effective profile was
    /// already within the thresholds. The result says if the profile changed.
    ///
    /// # Errors
    /// A requested rebuild has invalid physical parameters.
    pub fn update_profile(
        &mut self,
        requested: &ProfileParameters,
    ) -> Result<bool, AtmosphereError> {
        let Some(profile) = &self.profile else {
            return Ok(false);
        };
        let current = profile.parameters();
        let changed = site_changed(current, requested);
        if changed {
            let effective = ProfileParameters {
                altitude_m: requested.altitude_m,
                ground_pressure_hpa: requested.ground_pressure_hpa,
                ground_temperature_k: requested.ground_temperature_k,
                lapse_rate_k_per_km: requested.lapse_rate_k_per_km,
                relative_humidity_percent: requested.relative_humidity_percent,
                water_vapour_scale_height_km: requested.water_vapour_scale_height_km,
                ..current.clone()
            };
            let updated = AtmosphereProfile::new(&effective)?;
            if let Some(spectrum) = &mut self.spectrum {
                spectrum.absorption =
                    AbsorptionProfile::new(updated.clone(), spectrum.absorption.grid().clone());
            }
            self.profile = Some(updated);
        }
        if let Some(spectrum) = &mut self.spectrum {
            spectrum.sky.water_column_m = spectrum.absorption.ground_water_column_m();
        }
        Ok(changed)
    }
}

fn site_changed(current: &ProfileParameters, requested: &ProfileParameters) -> bool {
    // ATM compares pressure in pascals, scale height in metres and
    // humidity as a fraction. Convert both operands before subtracting.
    (requested.altitude_m - current.altitude_m).abs() > 1.0
        || (requested.ground_pressure_hpa * 100.0 - current.ground_pressure_hpa * 100.0).abs()
            > 99.0
        || (requested.ground_temperature_k - current.ground_temperature_k).abs() > 0.3
        || (requested.lapse_rate_k_per_km - current.lapse_rate_k_per_km).abs() > 0.01
        || (requested.relative_humidity_percent / 100.0 - current.relative_humidity_percent / 100.0)
            .abs()
            > 1.0
        || (requested.water_vapour_scale_height_km * 1000.0
            - current.water_vapour_scale_height_km * 1000.0)
            .abs()
            > 20.0
}
