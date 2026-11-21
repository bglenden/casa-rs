// SPDX-License-Identifier: LGPL-3.0-or-later
//! Single-layer 183 GHz model for the ALMA water-vapour radiometer.

use std::f64::consts::PI;

use super::data::Temperatures;

const LO_GHZ: f64 = 183.31;
const WATER_LINE_GHZ: f64 = 183.310107;
const BAND_OFFSETS_GHZ: [f64; 4] = [1.25, 3.25, 5.5, 7.25];
const BAND_WIDTHS_GHZ: [f64; 4] = [1.5, 2.5, 2.0, 1.5];
const GAUSS_ABSCISSA: [f64; 5] = [
    -0.906179845938664,
    -0.5384693101056831,
    0.0,
    0.5384693101056831,
    0.906179845938664,
];
const GAUSS_WEIGHT: [f64; 5] = [
    0.2369268850561891,
    0.4786286704993665,
    0.5688888888888889,
    0.4786286704993665,
    0.2369268850561891,
];

// HITRAN 2004 H2(16)O partition sums for 250 through 296 K.
// Retrieval uses 250–295 K and scales the line strength from 296 K.
const WATER_PARTITION: [f64; 47] = [
    135.742004, 136.551441, 137.362522, 138.175246, 138.989607, 139.805604, 140.623233, 141.442490,
    142.263373, 143.085879, 143.910004, 144.735545, 145.562700, 146.391469, 147.221851, 148.053843,
    148.887444, 149.722655, 150.559472, 151.397894, 152.237922, 153.079552, 153.922785, 154.767618,
    155.614050, 156.462081, 157.311708, 158.162931, 159.015748, 159.870158, 160.726160, 161.583752,
    162.442933, 163.303702, 164.166058, 165.029999, 165.895590, 166.762764, 167.631518, 168.501850,
    169.373758, 170.247241, 171.122297, 171.998922, 172.877117, 173.756878, 174.638204,
];
const H_OVER_K_K_PER_GHZ: f64 = 0.047992375;
const COLUMN_PER_MM_PER_CM2: f64 = 3.3427e21;
const SKY_COUPLING: f64 = 0.98;
const TERMINATION_K: f64 = 275.0;
const REFERENCE_K: f64 = 296.0;
const CLOUD_K: f64 = 270.0;
const BACKGROUND_K: f64 = 2.7;

#[derive(Clone, Copy)]
pub(super) struct WaterLayer {
    pub precipitable_water_mm: f64,
    pub temperature_k: f64,
    pub pressure_hpa: f64,
    pub cloud_opacity: f64,
}

struct FrequencySample {
    frequency_ghz: f64,
    weight: f64,
    background_rj_k: f64,
    cloud_rj_k: f64,
}

pub(super) struct RadiometerModel {
    bands: [[FrequencySample; 10]; 4],
    airmass: f64,
}

impl RadiometerModel {
    pub fn new(elevation_rad: f64) -> Self {
        let bands = std::array::from_fn(|channel| {
            std::array::from_fn(|node| {
                let index = node % GAUSS_ABSCISSA.len();
                let side = if node < GAUSS_ABSCISSA.len() {
                    -1.0
                } else {
                    1.0
                };
                let frequency = LO_GHZ
                    + side * BAND_OFFSETS_GHZ[channel]
                    + 0.5 * BAND_WIDTHS_GHZ[channel] * GAUSS_ABSCISSA[index];
                FrequencySample {
                    frequency_ghz: frequency,
                    weight: 0.25 * GAUSS_WEIGHT[index],
                    background_rj_k: blackbody_rj_k(frequency, BACKGROUND_K),
                    cloud_rj_k: blackbody_rj_k(frequency, CLOUD_K),
                }
            })
        });
        Self {
            bands,
            airmass: 1.0 / elevation_rad.sin(),
        }
    }

    pub fn temperatures(&self, layer: WaterLayer) -> Temperatures {
        self.spectrum(layer, layer.precipitable_water_mm * self.airmass)
    }

    pub fn temperature_per_path(&self, layer: WaterLayer) -> [f64; 4] {
        const DELTA_MM: f64 = 0.001;
        let slant = self.airmass * layer.precipitable_water_mm;
        let high = self.spectrum(layer, slant + DELTA_MM);
        let low = self.spectrum(layer, slant - DELTA_MM);
        // ALMA Memo 496 gives the non-dispersive wet path per mm of water.
        let path_mm_per_mm =
            COLUMN_PER_MM_PER_CM2 * 18.0105646863 * 1.660538782e-27 * 1.742e7 / layer.temperature_k;
        let kelvin_per_meter_scale = 1_000.0 / (2.0 * DELTA_MM * path_mm_per_mm);
        std::array::from_fn(|channel| (high[channel] - low[channel]) * kelvin_per_meter_scale)
    }

    fn spectrum(&self, layer: WaterLayer, slant_water_mm: f64) -> Temperatures {
        let opacity = WaterOpacity::new(layer.temperature_k, layer.pressure_hpa);
        self.bands.each_ref().map(|band| {
            let brightness = band
                .iter()
                .map(|sample| {
                    let frequency = sample.frequency_ghz;
                    let mut background = sample.background_rj_k;
                    if layer.cloud_opacity != 0.0 {
                        let cloud_transmission =
                            (-layer.cloud_opacity * (frequency / 183.3).powi(2)).exp();
                        background = transmit(background, sample.cloud_rj_k, cloud_transmission);
                    }
                    let transmission = (-opacity.per_mm(frequency) * slant_water_mm).exp();
                    sample.weight
                        * transmit(
                            background,
                            blackbody_rj_k(frequency, layer.temperature_k),
                            transmission,
                        )
                })
                .sum::<f64>();
            SKY_COUPLING * brightness + (1.0 - SKY_COUPLING) * TERMINATION_K
        })
    }
}

struct WaterOpacity {
    center_squared: f64,
    pressure_width: f64,
    line_scale: f64,
    continuum_scale: f64,
}

impl WaterOpacity {
    fn new(temperature: f64, pressure: f64) -> Self {
        let center = WATER_LINE_GHZ - 5.0298e-5 * pressure;
        let pressure_width = 2.9114e-3 * pressure * (REFERENCE_K / temperature).powf(0.77);
        let level_population = (195.908398 * (1.0 / REFERENCE_K - 1.0 / temperature)).exp();
        let photon_energy = H_OVER_K_K_PER_GHZ * center;
        let stimulated_emission =
            (-photon_energy / temperature).exp_m1() / (-photon_energy / REFERENCE_K).exp_m1();
        let strength = 2.333884e-21 * level_population * stimulated_emission * WATER_PARTITION[46]
            / water_partition(temperature);
        let air_density_cm3 = 2.686763e19 * (pressure / 1013.25) * (273.15 / temperature);
        let continuum_scale =
            6.1e-48 * (300.0 / temperature).powf(2.6) * COLUMN_PER_MM_PER_CM2 * air_density_cm3;
        Self {
            center_squared: center * center,
            pressure_width,
            line_scale: 4.0 * strength * pressure_width * COLUMN_PER_MM_PER_CM2 / PI,
            continuum_scale,
        }
    }

    fn per_mm(&self, frequency_ghz: f64) -> f64 {
        let frequency_squared = frequency_ghz.powi(2);
        let offset_squared = (frequency_squared - self.center_squared).powi(2);
        let gross_denominator =
            offset_squared + 4.0 * self.pressure_width.powi(2) * frequency_squared;
        frequency_squared * (self.line_scale / gross_denominator + self.continuum_scale)
    }
}

fn water_partition(temperature: f64) -> f64 {
    let grid = temperature - 250.0;
    let lower = (grid as usize).min(WATER_PARTITION.len() - 2);
    let fraction = grid - lower as f64;
    WATER_PARTITION[lower] * (1.0 - fraction) + WATER_PARTITION[lower + 1] * fraction
}

fn blackbody_rj_k(frequency_ghz: f64, temperature_k: f64) -> f64 {
    let photon_energy = H_OVER_K_K_PER_GHZ * frequency_ghz;
    let intensity_to_rj = 1.4744992e-18 / 3.0723613e-17;
    intensity_to_rj * frequency_ghz / (photon_energy / temperature_k).exp_m1()
}

fn transmit(background: f64, slab: f64, transmission: f64) -> f64 {
    background * transmission + slab * (1.0 - transmission)
}
