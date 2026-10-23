// SPDX-License-Identifier: LGPL-3.0-or-later
//! `setoptions(ftmachine='mosaic')`: what `predict` builds CASA's simulator
//! mosaic machine from.
//!
//! `Simulator::createSkyEquation` builds `MosaicFTNew` with the
//! heterogeneous-array beams of `TransformMachines/HetArrayConvFunc` when
//! OBSERVATION row 0's telescope has PBMath's UNKNOWN, OVRO, ACA or ALMA
//! beam. casa-rs builds it for ALMA and ACA, whose dishes
//! `findAntennaSizes` replaces by the 10.7 m and 6.25 m Airy apertures
//! ([`AiryDish::casa_alma_simulator`]). Other telescopes keep
//! `MosaicFTNew`'s own `SimplePBConvFunc` of their beam, not yet here.
//!
//! - The dish classes are the distinct dish diameters in ANTENNA order, a
//!   diameter compared as casacore's `String::toString` prints it, to six
//!   significant digits.
//! - Every channel of each window observed is selected.
//! - A scan is pointed at its phase centre, which casa-rs's POINTING rows
//!   hold; the kernels' phase ramp is its offset from the model's pixel
//!   `(nx/2, ny/2)`. CASA predicts nothing for a pointing outside the
//!   model image (`checkPBOfField`); casa-rs does not predict one yet.

use casa_imaging_operator::{AiryDish, MosaicWindow};

use super::Simulator;
use super::error::{SimulatorError, SimulatorResult};
use crate::predict::{MosaicBeams, PredictError, SkyModel};

/// The telescopes whose mosaic casa-rs builds.
const MOSAIC_TELESCOPES: [&str; 2] = ["ALMA", "ACA"];

/// A dish diameter as `String::toString(Double)` prints it, which keys
/// `HetArrayConvFunc`'s dish classes: six significant digits.
fn diameter_key(diameter_m: f64) -> String {
    format!("{diameter_m:.5e}")
}

impl Simulator {
    /// The mosaic machine's beams for the scans observed so far, and each
    /// antenna's dish class.
    ///
    /// # Errors
    ///
    /// Not yet implemented: a telescope other than ALMA and ACA, an antenna
    /// without a dish, or more than 255 dish diameters.
    pub(super) fn mosaic_beams(&self) -> SimulatorResult<(MosaicBeams, Vec<u8>)> {
        let telescope = self.observing_telescope();
        if !MOSAIC_TELESCOPES.contains(&telescope) {
            return Err(SimulatorError::not_yet_implemented(format!(
                "predict with setoptions(ftmachine='mosaic') for telescope {telescope} is not \
                 yet implemented in casa-rs (#841); ALMA's and ACA's heterogeneous-array beams \
                 are"
            )));
        }
        let mut keys: Vec<String> = Vec::new();
        let mut dishes = Vec::new();
        let mut classes = Vec::with_capacity(self.antennas.len());
        for antenna in &self.antennas {
            // `findAntennaSizes` gives a dish class only to `diameter > 0`.
            if antenna.diameter_m.is_nan() || antenna.diameter_m <= 0.0 {
                return Err(SimulatorError::not_yet_implemented(format!(
                    "predict with setoptions(ftmachine='mosaic') for antenna {} of diameter {} \
                     m is not yet implemented in casa-rs (#841)",
                    antenna.name, antenna.diameter_m
                )));
            }
            let key = diameter_key(antenna.diameter_m);
            let class = keys
                .iter()
                .position(|known| *known == key)
                .unwrap_or_else(|| {
                    tracing::info!(
                        "Overriding PB with Airy of diameter {} m starting with antenna {}",
                        AiryDish::casa_alma_simulator(antenna.diameter_m).aperture_m,
                        classes.len()
                    );
                    keys.push(key);
                    dishes.push(AiryDish::casa_alma_simulator(antenna.diameter_m));
                    keys.len() - 1
                });
            classes.push(u8::try_from(class).map_err(|_| {
                SimulatorError::not_yet_implemented(
                    "predict with setoptions(ftmachine='mosaic') for more than 255 dish \
                     diameters is not yet implemented in casa-rs (#841)",
                )
            })?);
        }
        let mut windows: Vec<MosaicWindow> = Vec::new();
        for scan in &self.scans {
            let id = scan.spectral_window_id;
            if windows
                .iter()
                .any(|window| window.spectral_window as usize == id)
            {
                continue;
            }
            let window = &self.spectral_windows[id];
            let frequencies = window.channel_frequencies_hz();
            windows.push(MosaicWindow {
                spectral_window: id as u32,
                window_frequencies_hz: frequencies.clone(),
                channel_width_hz: window.channel_width_hz,
                selected_frequencies_hz: frequencies,
            });
        }
        Ok((MosaicBeams { dishes, windows }, classes))
    }

    /// Each distinct phase centre of the scans observed so far (its bits)
    /// and its pointing offset in `model`
    /// ([`SkyModel::pointing_offset_rad`]).
    ///
    /// # Errors
    ///
    /// Not yet implemented: a pointing outside the model image. Raised: a
    /// pointing the model's coordinates cannot convert.
    pub(super) fn mosaic_pointings(
        &self,
        model: &SkyModel,
    ) -> SimulatorResult<Vec<([u64; 2], [f64; 2])>> {
        let mut pointings: Vec<([u64; 2], [f64; 2])> = Vec::new();
        for scan in &self.scans {
            let key = scan.phase_center_rad.map(f64::to_bits);
            if pointings.iter().any(|(known, _)| *known == key) {
                continue;
            }
            let offset = model
                .pointing_offset_rad(scan.phase_center_rad)
                .map_err(|error: PredictError| {
                    SimulatorError::raised(format!("Caught exception: {error}"))
                })?
                .ok_or_else(|| {
                    SimulatorError::not_yet_implemented(format!(
                        "predict with setoptions(ftmachine='mosaic') for field {} pointed \
                         outside the model image is not yet implemented in casa-rs (#841); \
                         CASA predicts nothing for it",
                        self.fields[scan.field_id].name
                    ))
                })?;
            pointings.push((key, offset));
        }
        Ok(pointings)
    }
}

#[cfg(test)]
mod tests {
    use super::diameter_key;

    #[test]
    fn diameters_compare_to_six_significant_digits() {
        assert_eq!(diameter_key(12.0), diameter_key(12.000_000_1));
        assert_ne!(diameter_key(12.0), diameter_key(12.000_1));
        assert_ne!(diameter_key(12.0), diameter_key(7.0));
    }
}
