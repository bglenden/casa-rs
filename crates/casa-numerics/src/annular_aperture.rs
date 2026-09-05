// SPDX-License-Identifier: LGPL-3.0-or-later

use libm::j1;

const SAMPLE_COUNT: usize = 10_000;
const SAMPLE_COUNT_MINUS_ONE: f64 = (SAMPLE_COUNT - 1) as f64;

/// Cached CASA-compatible voltage table for an annular aperture.
///
/// The caller owns the physical meaning and units of `maximum_radius`; values
/// passed to [`Self::evaluate`] must use the same radial coordinate. The table
/// preserves CASA's 10,000 `Float` samples and truncated lookup indexing.
#[derive(Clone)]
pub struct AnnularApertureVoltageTable {
    maximum_radius: f64,
    values: Box<[f32]>,
}

impl AnnularApertureVoltageTable {
    /// Tabulate an aperture with the supplied diameter, blockage, and support.
    #[must_use]
    pub fn new(aperture_diameter: f64, blockage_diameter: f64, maximum_radius: f64) -> Self {
        let dimensionless_max_radius =
            maximum_radius * 7.016 / (1.566 * 60.0) * aperture_diameter / 24.5;
        let area_ratio = (aperture_diameter / blockage_diameter).powi(2);
        let length_ratio = aperture_diameter / blockage_diameter;
        let values = (0..SAMPLE_COUNT)
            .map(|index| {
                voltage_at_index(
                    index as f64,
                    dimensionless_max_radius,
                    area_ratio,
                    length_ratio,
                    blockage_diameter,
                )
            })
            .collect();
        Self {
            maximum_radius,
            values,
        }
    }

    /// Evaluate the tabulated voltage at a nonnegative radial coordinate.
    ///
    /// Coordinates beyond the caller-supplied support return zero. Values
    /// inside the support use CASA's lower-sample (floor) selection rather
    /// than interpolation.
    #[must_use]
    pub fn evaluate(&self, radius: f64) -> f32 {
        if !radius.is_finite() || radius < 0.0 || radius > self.maximum_radius {
            return 0.0;
        }
        let index = (radius * SAMPLE_COUNT_MINUS_ONE / self.maximum_radius)
            .floor()
            .clamp(0.0, SAMPLE_COUNT_MINUS_ONE) as usize;
        self.values[index]
    }

    /// Return the radial support supplied by the scientific owner.
    #[must_use]
    pub fn maximum_radius(&self) -> f64 {
        self.maximum_radius
    }

    /// Return the retained heap storage used by the sampled voltage values.
    #[must_use]
    pub const fn resident_bytes(&self) -> usize {
        Self::table_resident_bytes()
    }

    /// Return the retained heap storage of one table before construction.
    #[must_use]
    pub const fn table_resident_bytes() -> usize {
        SAMPLE_COUNT * std::mem::size_of::<f32>()
    }
}

fn voltage_at_index(
    index: f64,
    dimensionless_max_radius: f64,
    area_ratio: f64,
    length_ratio: f64,
    blockage_diameter: f64,
) -> f32 {
    let x = index * dimensionless_max_radius / SAMPLE_COUNT_MINUS_ONE;
    if x.abs() <= f64::EPSILON {
        return 1.0;
    }
    if blockage_diameter <= 0.0 {
        return (2.0 * j1(x) / x) as f32;
    }
    ((area_ratio * 2.0 * j1(x) / x - 2.0 * j1(x * length_ratio) / (x * length_ratio))
        / (area_ratio - 1.0)) as f32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_uses_explicit_support_and_casa_truncation() {
        let table = AnnularApertureVoltageTable::new(10.7, 0.75, 107.04);
        let bin_width = table.maximum_radius() / SAMPLE_COUNT_MINUS_ONE;

        assert_eq!(
            [0.0, 0.01, 1.0, 50.0, 107.04].map(|radius| table.evaluate(radius).to_bits()),
            [
                1_065_353_216,
                1_065_353_216,
                1_065_353_196,
                1_060_422_159,
                1_034_343_953
            ]
        );

        assert_eq!(table.evaluate(0.0), 1.0);
        assert_eq!(table.evaluate(bin_width * 12.25), table.values[12]);
        assert_eq!(table.evaluate(bin_width * 12.75), table.values[12]);
        assert_eq!(table.evaluate(bin_width * 13.25), table.values[13]);
        assert_eq!(table.evaluate(107.04 + bin_width), 0.0);
    }

    #[test]
    fn unblocked_aperture_is_supported() {
        let table = AnnularApertureVoltageTable::new(12.0, 0.0, 107.04);
        assert_eq!(table.evaluate(0.0), 1.0);
        assert!(table.evaluate(50.0).is_finite());
    }
}
