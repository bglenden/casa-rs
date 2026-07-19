// SPDX-License-Identifier: LGPL-3.0-or-later
mod aipsio_oracle;
pub mod gridder_interop;
pub mod hogbom_interop;
mod image_oracle;
mod image_oracle_impl;
mod lattice_oracle;
mod lattice_oracle_impl;
pub mod measures_interop;
mod measures_oracle;
pub mod ms_interop;
mod oracle_ffi;
mod oracle_runtime;
pub mod quanta_interop;
pub mod table_interop;
pub mod table_measures_interop;
mod table_oracle;
mod table_oracle_impl;
pub mod table_quantum_interop;
pub mod taql_interop;
mod test_data;

pub use aipsio_oracle::*;
use casa_aipsio::{Complex32, Complex64};
pub use image_oracle::ImageOracle;
pub use image_oracle_impl::{
    CppImageExprBinaryOp, CppImageExprCompareOp, CppImageExprUnaryOp, CppMaskLogicalOp,
    CppRegionStatistics, CppUnsupportedRegionKind,
};
pub use lattice_oracle::LatticeOracle;
pub use lattice_oracle_impl::CppLatticeStatisticsBenchResult;
pub use oracle_runtime::OracleError;
pub use table_oracle::TableOracle;
pub use table_oracle_impl::{
    BulkScalarIoBenchResult, CellSliceBenchParams, CellSliceBenchResult, CppTableFixture,
    DeepCopyBenchResult, SetAlgebraBenchResult,
};
pub use test_data::*;

/// Deterministic, filesystem-free measures inputs for cross-crate tests.
pub fn deterministic_measures_provider()
-> std::sync::Arc<dyn casa_types::measures::MeasuresProvider> {
    #[derive(Debug)]
    struct Provider;

    impl casa_types::measures::MeasuresProvider for Provider {
        fn eop_values(
            &self,
            _utc_mjd: f64,
        ) -> Result<Option<casa_types::measures::EopValues>, String> {
            Ok(Some(casa_types::measures::EopValues {
                dut1_seconds: 0.0,
                x_arcsec: 0.0,
                y_arcsec: 0.0,
                dx_mas: 0.0,
                dy_mas: 0.0,
                is_predicted: false,
            }))
        }

        fn tai_minus_utc_seconds(&self, _utc_mjd: f64) -> Result<f64, String> {
            Ok(32.0)
        }

        fn utc_from_tai_mjd(&self, tai_mjd: f64) -> Result<f64, String> {
            Ok(tai_mjd - 32.0 / 86_400.0)
        }

        fn igrf_coefficients(&self, decimal_year: f64) -> Result<(Vec<f64>, usize), String> {
            if !(1900.0..=2025.999).contains(&decimal_year) {
                return Err("test IGRF date is outside 1900 through 2025".to_string());
            }
            let mut coefficients = vec![0.0; 13 * 15];
            coefficients[0] = -29_440.0;
            coefficients[1] = -1_500.0;
            coefficients[2] = 4_650.0;
            Ok((coefficients, 13))
        }

        fn observatory(
            &self,
            name: &str,
        ) -> Result<Option<casa_types::measures::ObservatoryPosition>, String> {
            Ok(match name.to_ascii_uppercase().as_str() {
                "ALMA" | "ALMASD" | "ACA" => {
                    Some(casa_types::measures::ObservatoryPosition::Wgs84 {
                        longitude_rad: -67.754_929_f64.to_radians(),
                        latitude_rad: -23.022_886_f64.to_radians(),
                        height_m: 5056.8,
                    })
                }
                "VLA" => Some(casa_types::measures::ObservatoryPosition::Itrf {
                    x_m: -1_601_185.0,
                    y_m: -5_041_977.0,
                    z_m: 3_554_875.0,
                }),
                "WSRT" => Some(casa_types::measures::ObservatoryPosition::Itrf {
                    x_m: 3_826_577.0,
                    y_m: 461_022.0,
                    z_m: 5_064_892.0,
                }),
                _ => None,
            })
        }

        fn source(
            &self,
            name: &str,
        ) -> Result<Option<casa_types::measures::NamedSourceDirection>, String> {
            Ok(name.eq_ignore_ascii_case("CASA").then(|| {
                casa_types::measures::NamedSourceDirection {
                    reference: "J2000".to_string(),
                    longitude_rad: 6.123_487_680_622_104,
                    latitude_rad: 1.026_515_399_560_464_8,
                }
            }))
        }

        fn spectral_line_hz(&self, name: &str) -> Result<Option<f64>, String> {
            Ok(name.eq_ignore_ascii_case("HI").then_some(1.420_405_752e9))
        }
    }

    std::sync::Arc::new(Provider)
}

#[cfg(test)]
mod tests;
