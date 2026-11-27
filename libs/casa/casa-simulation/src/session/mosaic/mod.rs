// SPDX-License-Identifier: LGPL-3.0-or-later
//! `setoptions(ftmachine='mosaic')`: what `predict` builds CASA's simulator
//! mosaic machine from.
//!
//! `Simulator::createSkyEquation` builds `MosaicFTNew` with the
//! heterogeneous-array beams of `TransformMachines/HetArrayConvFunc` when
//! OBSERVATION row 0's telescope has PBMath's UNKNOWN, OVRO, ACA or ALMA
//! beam. casa-rs builds it for ALMA, ACA and OVRO. ALMA and ACA dishes
//! `findAntennaSizes` replaces by the 10.7 m and 6.25 m Airy apertures
//! ([`AiryDish::casa_alma_simulator`]); OVRO uses each physical diameter
//! and its scaled blockage. Other supported default telescope patterns use
//! `MosaicFTNew`'s `SimplePBConvFunc`, one common beam for all antennas.
//!
//! - The dish classes are the distinct dish diameters in ANTENNA order, a
//!   diameter compared as casacore's `String::toString` prints it, to six
//!   significant digits.
//! - Every channel of each window observed is selected.
//! - A scan's beam uses POINTING independently of its FIELD phase centre;
//!   the kernels' phase ramp is its offset from the model's pixel
//!   `(nx/2, ny/2)`. Heterogeneous-array beams predict nothing for a
//!   pointing outside the model image (`checkPBOfField`); common beams
//!   keep applying the phase ramp outside the image.

use casa_imaging_operator::{AiryDish, MosaicWindow};

use super::Simulator;
use super::error::{SimulatorError, SimulatorResult};
use crate::predict::{MosaicBeams, PredictError, PrimaryBeam, SkyModel};

/// The telescopes whose mosaic casa-rs builds.
const MOSAIC_TELESCOPES: [&str; 3] = ["ALMA", "ACA", "OVRO"];

/// A dish diameter as `String::toString(Double)` prints it, which keys
/// `HetArrayConvFunc`'s dish classes: six significant digits.
fn diameter_key(diameter_m: f64) -> String {
    format!("{diameter_m:.5e}")
}

/// The most a diameter key's text holds: `format!` grows its string from
/// nothing, to 16 bytes for its nine to twelve characters.
const DIAMETER_KEY_BYTES: usize = 32;

/// One pointing the mosaic machine keeps: a phase centre's bits and its
/// offset.
pub(super) type MosaicPointing = ([u64; 2], Option<[f64; 2]>);

mod time_order;

type FrameKey = (i32, usize, usize);

#[derive(Default)]
struct BufferClock {
    rows: usize,
    epoch_s: f64,
    antenna: usize,
    last_row: Option<u64>,
}

/// CASA's visibility-buffer clock, sampled before numerical workers run.
pub(super) struct MosaicGeometry {
    geometry: crate::predict::PointingGeometry,
    buffers: std::sync::Mutex<Vec<(FrameKey, BufferClock)>>,
    rows: usize,
    observer: Option<[f64; 3]>,
    time_order: Option<time_order::TimeOrder>,
}

impl MosaicGeometry {
    fn bytes_for(
        model: &SkyModel,
        centres: Option<&super::from_ms::polynomial::PhaseCentres>,
    ) -> u64 {
        model.pointing_geometry_bytes()
            + (size_of::<Self>() - size_of::<crate::predict::PointingGeometry>()
                + centres.map_or(0, |p| p.value.len()) * size_of::<(FrameKey, BufferClock)>())
                as u64
    }

    pub(super) fn new(
        model: &SkyModel,
        observer: Option<[f64; 3]>,
        bounded: bool,
        centres: Option<&super::from_ms::polynomial::PhaseCentres>,
        antennas: usize,
    ) -> Self {
        let first = super::from_ms::polynomial::first_chunk_rows(centres).min(101);
        // Simulator::setRowBlocking(100) includes both endpoints. SkyEquation
        // increases that blocking when the first buffer holds too few rows.
        let rows = if antennas > 1 && first < antennas * (antennas - 1) / 4 {
            antennas * (antennas + 1) / 2 + 1
        } else {
            101
        };
        let mut buffers = Vec::with_capacity(centres.map_or(0, |p| p.value.len()));
        if let Some(centres) = centres {
            buffers.extend(
                centres
                    .value
                    .iter()
                    .map(|(key, _)| (*key, BufferClock::default())),
            );
        }
        Self {
            geometry: model.pointing_geometry(bounded),
            observer,
            time_order: None,
            buffers: std::sync::Mutex::new(buffers),
            rows,
        }
    }

    pub(super) fn bytes(&self) -> u64 {
        self.geometry.bytes()
            + (size_of::<Self>() - size_of::<crate::predict::PointingGeometry>()
                + self.buffers.lock().expect("buffer metadata").capacity()
                    * size_of::<(FrameKey, BufferClock)>()) as u64
    }

    pub(super) fn sample(
        &self,
        scan: &super::observe::Scan,
        row: u64,
        time: f64,
        antenna: usize,
    ) -> SimulatorResult<(f64, usize)> {
        let key = (scan.array_id, scan.field_id, scan.data_description_id);
        if let Some(order) = &self.time_order {
            return order.sample(key, row, time);
        }
        let mut buffers = self
            .buffers
            .lock()
            .map_err(|e| SimulatorError::raised(e.to_string()))?;
        let index = buffers
            .binary_search_by_key(&key, |(key, _)| *key)
            .map_err(|_| SimulatorError::raised("missing mosaic frame chunk"))?;
        let clock = &mut buffers[index].1;
        // A private selection rendering or a retried transaction starts
        // another pass over the same captured physical rows.
        if clock.last_row.is_some_and(|previous| row <= previous) {
            clock.rows = 0;
        }
        if clock.rows == 0 {
            clock.epoch_s = time;
            clock.antenna = antenna;
        }
        clock.last_row = Some(row);
        clock.rows += 1;
        if clock.rows == self.rows {
            clock.rows = 0;
        }
        Ok((clock.epoch_s, clock.antenna))
    }

    pub(super) fn offset(
        &self,
        pointing: [f64; 2],
        frame: &casa_core::measures::MeasFrame,
    ) -> Result<Option<[f64; 2]>, PredictError> {
        let mut frame = frame.clone();
        if let Some([x, y, z]) = self.observer {
            frame = frame.with_position(casa_core::measures::MPosition::new_itrf(x, y, z));
        }
        self.geometry.offset(pointing, &frame)
    }
}

impl Simulator {
    /// Bytes the mosaic machine's state holds, known before it is formed:
    /// what the prediction keeps (fixed offsets or numeric direction geometry
    /// and a clock per selected chunk, plus each antenna's dish class), and what builds the beams
    /// until the kernels are built (a dish and a diameter key for each
    /// antenna at most, and every window's channel frequencies twice).
    pub(super) fn mosaic_state_bytes(
        &self,
        model: &SkyModel,
        centres: Option<&super::from_ms::polynomial::PhaseCentres>,
    ) -> (u64, u64) {
        let timed = !crate::predict::fixed_direction_frame(model.tangent_direction().refer());
        let geometry = if timed {
            MosaicGeometry::bytes_for(model, centres) as usize
        } else {
            self.scans.len() * size_of::<MosaicPointing>()
        };
        let kept = geometry + self.antennas.len() * size_of::<u8>();
        let antennas = self.antennas.len()
            * (size_of::<PrimaryBeam>() + size_of::<String>() + DIAMETER_KEY_BYTES);
        let windows = self.spectral_windows.len()
            * (size_of::<MosaicWindow>() + size_of::<(u32, f64)>())
            + self
                .spectral_windows
                .iter()
                .map(|window| 2 * window.channel_count * size_of::<f64>())
                .sum::<usize>();
        (
            kept as u64,
            (antennas + windows + if timed { 256 } else { 0 }) as u64,
        )
    }

    /// SimplePBConvFunc keeps the catalogue position only for VLA images
    /// on a VLA-labelled observation; other buffers use their first antenna.
    pub(super) fn mosaic_observer(&self, model: &SkyModel) -> SimulatorResult<Option<[f64; 3]>> {
        if !model.observation_telescope().contains("VLA")
            || !self.observing_telescope().contains("VLA")
        {
            return Ok(None);
        }
        casa_core::measures::MPosition::from_observatory_name(
            self.observing_telescope(),
            self.options.measures.as_ref(),
        )
        .map(|position| position.map(|p| p.as_itrf()))
        .map_err(|error| SimulatorError::raised(error.to_string()))
    }

    /// The mosaic machine's beams for the scans observed so far, and each
    /// antenna's dish class.
    ///
    /// # Errors
    ///
    /// Not yet implemented: a telescope without a supported default beam,
    /// a heterogeneous-array antenna without a dish, or more than 255 dish
    /// diameters.
    pub(super) fn mosaic_beams(
        &self,
        custom: Option<&super::voltage_patterns::BeamSource>,
    ) -> SimulatorResult<(MosaicBeams, Vec<u8>)> {
        let telescope = self.observing_telescope();
        let common = custom.is_some() || !MOSAIC_TELESCOPES.contains(&telescope);
        let default = match custom {
            Some(beam) => beam.configured(),
            None => PrimaryBeam::for_telescope(telescope, self.reference_frequency_hz()),
        };
        if common && default.is_none() && custom.is_none() {
            return Err(SimulatorError::refused(format!(
                "Failed to create SkyEquation: vpmanager does not have a beam for {telescope}"
            )));
        }
        // At most one class an antenna: the capacities are the bounds
        // `mosaic_state_bytes` admits.
        let mut keys: Vec<String> = Vec::with_capacity(self.antennas.len());
        let mut dishes = Vec::with_capacity(self.antennas.len());
        let mut classes = Vec::with_capacity(self.antennas.len());
        if common && let Some(default) = default {
            dishes.push(default);
        }
        for antenna in &self.antennas {
            if common {
                classes.push(0);
                continue;
            }
            // `findAntennaSizes` gives a dish class only to `diameter > 0`.
            if antenna.diameter_m.is_nan() || antenna.diameter_m <= 0.0 {
                return Err(SimulatorError::not_yet_implemented(format!(
                    "predict with setoptions(ftmachine='mosaic') for antenna {} of diameter {} \
                     m is not yet implemented in casa-rs (#1053)",
                    antenna.name, antenna.diameter_m
                )));
            }
            let key = diameter_key(antenna.diameter_m);
            let class = keys
                .iter()
                .position(|known| *known == key)
                .unwrap_or_else(|| {
                    let dish = if matches!(telescope, "ALMA" | "ACA") {
                        AiryDish::casa_alma_simulator(antenna.diameter_m)
                    } else {
                        AiryDish::casa_airy(antenna.diameter_m)
                    };
                    tracing::info!(
                        "Overriding PB with Airy of diameter {} m starting with antenna {}",
                        dish.aperture_m,
                        classes.len()
                    );
                    keys.push(key);
                    dishes.push(PrimaryBeam::from(dish));
                    keys.len() - 1
                });
            classes.push(u8::try_from(class).map_err(|_| {
                SimulatorError::not_yet_implemented(
                    "predict with setoptions(ftmachine='mosaic') for more than 255 dish \
                     diameters is not yet implemented in casa-rs (#1053)",
                )
            })?);
        }
        let mut windows: Vec<MosaicWindow> = Vec::with_capacity(self.spectral_windows.len());
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
                buffer_band: None,
            });
        }
        Ok((
            MosaicBeams {
                common,
                custom_pattern: custom.cloned(),
                image_angles_rad: Vec::new(),
                dishes,
                windows,
            },
            classes,
        ))
    }

    /// Each distinct pointing of the scans observed so far (its bits), where
    /// their antennas point, and its offset in `model`
    /// ([`SkyModel::pointing_offset_and_coverage`]), sorted by the bits. The capacity
    /// is a pointing for each scan, the bound `mosaic_state_bytes` admits.
    ///
    /// # Errors
    ///
    /// Raised: a pointing the model's coordinates cannot convert. A `None`
    /// offset records CASA's heterogeneous-array zero contribution outside
    /// the model image; common beams retain the offset.
    pub(super) fn mosaic_pointings(
        &self,
        model: &SkyModel,
        bounded: bool,
    ) -> SimulatorResult<Vec<MosaicPointing>> {
        let mut pointings: Vec<MosaicPointing> = Vec::with_capacity(self.scans.len());
        for scan in &self.scans {
            let key = scan.pointing_rad.map(f64::to_bits);
            let (offset, inside) = model
                .pointing_offset_and_coverage(scan.pointing_rad)
                .map_err(|error: PredictError| {
                    SimulatorError::raised(format!("Caught exception: {error}"))
                })?;
            pointings.push((key, (!bounded || inside).then_some(offset)));
        }
        pointings.sort_unstable_by_key(|(key, _)| *key);
        pointings.dedup_by_key(|(key, _)| *key);
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
