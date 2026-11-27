// SPDX-License-Identifier: LGPL-3.0-or-later
//! VLA opacity interpolation for CASA's fixed 0–50 GHz reference model.

const CHANNELS: usize = 2000;
const START_HZ: f64 = 12_500_000.0;
const STEP_HZ: f64 = 25_000_000.0;
const COEFFICIENT_BYTES: usize = 2 * size_of::<f64>();

// The model profiles and spectral grid are identical on every CASA call.
// Regenerate the fixed coefficients with tools/casa-compat/generate_plotweather_opacity.py.
const COEFFICIENTS: &[u8; CHANNELS * COEFFICIENT_BYTES] =
    include_bytes!("vla_opacity_6_7_6_14.bin");

pub(super) fn windows(windows_ghz: &[f64], water_mm: f64) -> Result<Vec<f64>, String> {
    windows_ghz
        .iter()
        .map(|&frequency| interpolate(frequency, water_mm))
        .collect()
}

pub(super) fn frequencies() -> Vec<f64> {
    (0..CHANNELS).map(frequency_ghz).collect()
}

pub(super) fn spectrum(water_mm: f64) -> Vec<f64> {
    (0..CHANNELS)
        .map(|index| opacity(index, water_mm))
        .collect()
}

fn frequency_ghz(index: usize) -> f64 {
    (START_HZ + STEP_HZ * index as f64) / 1e9
}

fn interpolate(frequency: f64, water_mm: f64) -> Result<f64, String> {
    if !frequency.is_finite() {
        return Err("spectral-window frequency must be finite".into());
    }
    let sample = ((frequency * 1e9 - START_HZ) / STEP_HZ).clamp(0.0, (CHANNELS - 1) as f64);
    let lower = sample.floor() as usize;
    let upper = (lower + 1).min(CHANNELS - 1);
    // CASA's four-point slice starts two channels before the nearest
    // channel, with ties resolved in favour of the first index.
    let nearest = if frequency - frequency_ghz(lower) > frequency_ghz(upper) - frequency {
        upper
    } else {
        lower
    };
    if nearest < 2 {
        return Err("array of sample points is empty".into());
    }
    let start = opacity(lower, water_mm);
    if lower == upper || frequency == frequency_ghz(lower) {
        return Ok(start);
    }
    let spacing = frequency_ghz(upper) - frequency_ghz(lower);
    Ok(start + (frequency - frequency_ghz(lower)) * (opacity(upper, water_mm) - start) / spacing)
}

fn opacity(index: usize, water_mm: f64) -> f64 {
    let offset = index * COEFFICIENT_BYTES;
    let row = &COEFFICIENTS[offset..offset + COEFFICIENT_BYTES];
    let intercept = f64::from_le_bytes(row[..8].try_into().expect("fixed opacity intercept"));
    let slope = f64::from_le_bytes(row[8..].try_into().expect("fixed opacity slope"));
    intercept + slope * water_mm
}
