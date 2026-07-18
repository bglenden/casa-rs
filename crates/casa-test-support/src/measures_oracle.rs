// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stable typed facade over the private casacore measures shim.

#[cfg(has_casacore_cpp)]
use crate::measures_interop::*;
use crate::oracle_runtime::{OracleError, oracle_operation};

macro_rules! measures_operation {
    ($operation:expr, $body:block) => {{ oracle_operation!($operation, $body) }};
}

/// Rust-facing access to casacore measures operations.
pub struct MeasuresOracle;

#[cfg_attr(not(has_casacore_cpp), allow(unused_variables))]
impl MeasuresOracle {
    #[allow(clippy::too_many_arguments)]
    pub fn epoch_convert(mjd_in: f64, ref_in: &str, ref_out: &str) -> Result<f64, OracleError> {
        measures_operation!("measures.epoch_convert", {
            cpp_epoch_convert(mjd_in, ref_in, ref_out).map_err(|message| OracleError::CppFailure {
                operation: "measures.epoch_convert",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn epoch_to_record(
        mjd_in: f64,
        ref_in: &str,
    ) -> Result<(f64, String, String), OracleError> {
        measures_operation!("measures.epoch_to_record", {
            cpp_epoch_to_record(mjd_in, ref_in).map_err(|message| OracleError::CppFailure {
                operation: "measures.epoch_to_record",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn position_convert(
        v0: f64,
        v1: f64,
        v2: f64,
        ref_in: &str,
        ref_out: &str,
    ) -> Result<(f64, f64, f64), OracleError> {
        measures_operation!("measures.position_convert", {
            cpp_position_convert(v0, v1, v2, ref_in, ref_out).map_err(|message| {
                OracleError::CppFailure {
                    operation: "measures.position_convert",
                    message,
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn position_to_record(x: f64, y: f64, z: f64) -> Result<(f64, f64, f64), OracleError> {
        measures_operation!("measures.position_to_record", {
            cpp_position_to_record(x, y, z).map_err(|message| OracleError::CppFailure {
                operation: "measures.position_to_record",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn position_to_wgs_xyz(
        v0: f64,
        v1: f64,
        v2: f64,
        ref_in: &str,
    ) -> Result<(f64, f64, f64), OracleError> {
        measures_operation!("measures.position_to_wgs_xyz", {
            cpp_position_to_wgs_xyz(v0, v1, v2, ref_in).map_err(|message| OracleError::CppFailure {
                operation: "measures.position_to_wgs_xyz",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn bench_position_convert(
        x_start: f64,
        y: f64,
        z: f64,
        count: i32,
        ref_in: &str,
        ref_out: &str,
        iterations: i32,
    ) -> Result<u64, OracleError> {
        measures_operation!("measures.bench_position_convert", {
            cpp_bench_position_convert(x_start, y, z, count, ref_in, ref_out, iterations).map_err(
                |message| OracleError::CppFailure {
                    operation: "measures.bench_position_convert",
                    message,
                },
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn bench_epoch_convert(
        mjd_start: f64,
        count: i32,
        ref_in: &str,
        ref_out: &str,
        iterations: i32,
    ) -> Result<u64, OracleError> {
        measures_operation!("measures.bench_epoch_convert", {
            cpp_bench_epoch_convert(mjd_start, count, ref_in, ref_out, iterations).map_err(
                |message| OracleError::CppFailure {
                    operation: "measures.bench_epoch_convert",
                    message,
                },
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn doppler_convert(value_in: f64, ref_in: &str, ref_out: &str) -> Result<f64, OracleError> {
        measures_operation!("measures.doppler_convert", {
            cpp_doppler_convert(value_in, ref_in, ref_out).map_err(|message| {
                OracleError::CppFailure {
                    operation: "measures.doppler_convert",
                    message,
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn bench_doppler_convert(
        value_start: f64,
        count: i32,
        ref_in: &str,
        ref_out: &str,
        iterations: i32,
    ) -> Result<u64, OracleError> {
        measures_operation!("measures.bench_doppler_convert", {
            cpp_bench_doppler_convert(value_start, count, ref_in, ref_out, iterations).map_err(
                |message| OracleError::CppFailure {
                    operation: "measures.bench_doppler_convert",
                    message,
                },
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn direction_convert(
        lon_in: f64,
        lat_in: f64,
        ref_in: &str,
        ref_out: &str,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
    ) -> Result<(f64, f64), OracleError> {
        measures_operation!("measures.direction_convert", {
            cpp_direction_convert(
                lon_in, lat_in, ref_in, ref_out, epoch_mjd, obs_lon, obs_lat, obs_h,
            )
            .map_err(|message| OracleError::CppFailure {
                operation: "measures.direction_convert",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn named_direction_convert(
        source_name: &str,
        ref_out: &str,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
    ) -> Result<(f64, f64), OracleError> {
        measures_operation!("measures.named_direction_convert", {
            cpp_named_direction_convert(source_name, ref_out, epoch_mjd, obs_lon, obs_lat, obs_h)
                .map_err(|message| OracleError::CppFailure {
                    operation: "measures.named_direction_convert",
                    message,
                })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn riseset(
        source_name: &str,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
    ) -> Result<(f64, f64), OracleError> {
        measures_operation!("measures.riseset", {
            cpp_riseset(source_name, epoch_mjd, obs_lon, obs_lat, obs_h).map_err(|message| {
                OracleError::CppFailure {
                    operation: "measures.riseset",
                    message,
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn line_frequency(line_name: &str) -> Result<f64, OracleError> {
        measures_operation!("measures.line_frequency", {
            cpp_line_frequency(line_name).map_err(|message| OracleError::CppFailure {
                operation: "measures.line_frequency",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn earthmag_convert_xyz(
        xyz_in: [f64; 3],
        ref_in: &str,
        ref_out: &str,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
    ) -> Result<[f64; 3], OracleError> {
        measures_operation!("measures.earthmag_convert_xyz", {
            cpp_earthmag_convert_xyz(xyz_in, ref_in, ref_out, epoch_mjd, obs_lon, obs_lat, obs_h)
                .map_err(|message| OracleError::CppFailure {
                    operation: "measures.earthmag_convert_xyz",
                    message,
                })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn earthmag_convert_angles(
        lon_in: f64,
        lat_in: f64,
        length_nt: f64,
        ref_in: &str,
        ref_out: &str,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
    ) -> Result<(f64, f64), OracleError> {
        measures_operation!("measures.earthmag_convert_angles", {
            cpp_earthmag_convert_angles(
                lon_in, lat_in, length_nt, ref_in, ref_out, epoch_mjd, obs_lon, obs_lat, obs_h,
            )
            .map_err(|message| OracleError::CppFailure {
                operation: "measures.earthmag_convert_angles",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn igrf_value(
        mode: &str,
        ref_out: Option<&str>,
        height_m: f64,
        dir_lon: f64,
        dir_lat: f64,
        dir_ref: &str,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
    ) -> Result<Vec<f64>, OracleError> {
        measures_operation!("measures.igrf_value", {
            cpp_igrf_value(
                mode, ref_out, height_m, dir_lon, dir_lat, dir_ref, epoch_mjd, obs_lon, obs_lat,
                obs_h,
            )
            .map_err(|message| OracleError::CppFailure {
                operation: "measures.igrf_value",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn iau2000_precession_matrix(epoch_mjd_tt: f64) -> Result<[[f64; 3]; 3], OracleError> {
        measures_operation!("measures.iau2000_precession_matrix", {
            cpp_iau2000_precession_matrix(epoch_mjd_tt).map_err(|message| OracleError::CppFailure {
                operation: "measures.iau2000_precession_matrix",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn direction_convert_iau2000a(
        lon_in: f64,
        lat_in: f64,
        ref_in: &str,
        ref_out: &str,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
    ) -> Result<(f64, f64), OracleError> {
        measures_operation!("measures.direction_convert_iau2000a", {
            cpp_direction_convert_iau2000a(
                lon_in, lat_in, ref_in, ref_out, epoch_mjd, obs_lon, obs_lat, obs_h,
            )
            .map_err(|message| OracleError::CppFailure {
                operation: "measures.direction_convert_iau2000a",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn earth_velocity(epoch_mjd_tdb: f64) -> Result<([f64; 3], [f64; 3]), OracleError> {
        measures_operation!("measures.earth_velocity", {
            cpp_earth_velocity(epoch_mjd_tdb).map_err(|message| OracleError::CppFailure {
                operation: "measures.earth_velocity",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn bench_direction_convert(
        lon_start: f64,
        lat: f64,
        count: i32,
        ref_in: &str,
        ref_out: &str,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        iterations: i32,
    ) -> Result<u64, OracleError> {
        measures_operation!("measures.bench_direction_convert", {
            cpp_bench_direction_convert(
                lon_start, lat, count, ref_in, ref_out, epoch_mjd, obs_lon, obs_lat, obs_h,
                iterations,
            )
            .map_err(|message| OracleError::CppFailure {
                operation: "measures.bench_direction_convert",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn frequency_convert(
        freq_hz: f64,
        ref_in: &str,
        ref_out: &str,
        dir_lon: f64,
        dir_lat: f64,
        dir_ref: &str,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
    ) -> Result<f64, OracleError> {
        measures_operation!("measures.frequency_convert", {
            cpp_frequency_convert(
                freq_hz, ref_in, ref_out, dir_lon, dir_lat, dir_ref, epoch_mjd, obs_lon, obs_lat,
                obs_h,
            )
            .map_err(|message| OracleError::CppFailure {
                operation: "measures.frequency_convert",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn frequency_convert_via_model(
        freq_hz: f64,
        ref_in: &str,
        ref_out: &str,
        dir_lon: f64,
        dir_lat: f64,
        dir_ref: &str,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
    ) -> Result<f64, OracleError> {
        measures_operation!("measures.frequency_convert_via_model", {
            cpp_frequency_convert_via_model(
                freq_hz, ref_in, ref_out, dir_lon, dir_lat, dir_ref, epoch_mjd, obs_lon, obs_lat,
                obs_h,
            )
            .map_err(|message| OracleError::CppFailure {
                operation: "measures.frequency_convert_via_model",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn frequency_convert_via_mutated_model(
        freq_hz: f64,
        ref_in: &str,
        ref_out: &str,
        dir_lon: f64,
        dir_lat: f64,
        dir_ref: &str,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
    ) -> Result<f64, OracleError> {
        measures_operation!("measures.frequency_convert_via_mutated_model", {
            cpp_frequency_convert_via_mutated_model(
                freq_hz, ref_in, ref_out, dir_lon, dir_lat, dir_ref, epoch_mjd, obs_lon, obs_lat,
                obs_h,
            )
            .map_err(|message| OracleError::CppFailure {
                operation: "measures.frequency_convert_via_mutated_model",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn frequency_convert_between_frames(
        freq_hz: f64,
        ref_in: &str,
        ref_out: &str,
        src_dir_lon: f64,
        src_dir_lat: f64,
        src_dir_ref: &str,
        src_epoch_mjd: f64,
        src_obs_lon: f64,
        src_obs_lat: f64,
        src_obs_h: f64,
        dst_dir_lon: f64,
        dst_dir_lat: f64,
        dst_dir_ref: &str,
        dst_epoch_mjd: f64,
        dst_obs_lon: f64,
        dst_obs_lat: f64,
        dst_obs_h: f64,
    ) -> Result<f64, OracleError> {
        measures_operation!("measures.frequency_convert_between_frames", {
            cpp_frequency_convert_between_frames(
                freq_hz,
                ref_in,
                ref_out,
                src_dir_lon,
                src_dir_lat,
                src_dir_ref,
                src_epoch_mjd,
                src_obs_lon,
                src_obs_lat,
                src_obs_h,
                dst_dir_lon,
                dst_dir_lat,
                dst_dir_ref,
                dst_epoch_mjd,
                dst_obs_lon,
                dst_obs_lat,
                dst_obs_h,
            )
            .map_err(|message| OracleError::CppFailure {
                operation: "measures.frequency_convert_between_frames",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn bench_frequency_convert(
        freq_start: f64,
        count: i32,
        ref_in: &str,
        ref_out: &str,
        dir_lon: f64,
        dir_lat: f64,
        dir_ref: &str,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        iterations: i32,
    ) -> Result<u64, OracleError> {
        measures_operation!("measures.bench_frequency_convert", {
            cpp_bench_frequency_convert(
                freq_start, count, ref_in, ref_out, dir_lon, dir_lat, dir_ref, epoch_mjd, obs_lon,
                obs_lat, obs_h, iterations,
            )
            .map_err(|message| OracleError::CppFailure {
                operation: "measures.bench_frequency_convert",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn radvel_convert(
        ms_in: f64,
        ref_in: &str,
        ref_out: &str,
        dir_lon: f64,
        dir_lat: f64,
        dir_ref: &str,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
    ) -> Result<f64, OracleError> {
        measures_operation!("measures.radvel_convert", {
            cpp_radvel_convert(
                ms_in, ref_in, ref_out, dir_lon, dir_lat, dir_ref, epoch_mjd, obs_lon, obs_lat,
                obs_h,
            )
            .map_err(|message| OracleError::CppFailure {
                operation: "measures.radvel_convert",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn bench_radvel_convert(
        ms_start: f64,
        count: i32,
        ref_in: &str,
        ref_out: &str,
        dir_lon: f64,
        dir_lat: f64,
        dir_ref: &str,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        iterations: i32,
    ) -> Result<u64, OracleError> {
        measures_operation!("measures.bench_radvel_convert", {
            cpp_bench_radvel_convert(
                ms_start, count, ref_in, ref_out, dir_lon, dir_lat, dir_ref, epoch_mjd, obs_lon,
                obs_lat, obs_h, iterations,
            )
            .map_err(|message| OracleError::CppFailure {
                operation: "measures.bench_radvel_convert",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn frequency_convert_with_rv(
        freq_hz: f64,
        ref_in: &str,
        ref_out: &str,
        dir_lon: f64,
        dir_lat: f64,
        dir_ref: &str,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        rv_ms: f64,
        rv_ref: &str,
    ) -> Result<f64, OracleError> {
        measures_operation!("measures.frequency_convert_with_rv", {
            cpp_frequency_convert_with_rv(
                freq_hz, ref_in, ref_out, dir_lon, dir_lat, dir_ref, epoch_mjd, obs_lon, obs_lat,
                obs_h, rv_ms, rv_ref,
            )
            .map_err(|message| OracleError::CppFailure {
                operation: "measures.frequency_convert_with_rv",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn frequency_rest_with_doppler(
        freq_hz: f64,
        ref_in: &str,
        doppler_value: f64,
        doppler_ref: &str,
    ) -> Result<f64, OracleError> {
        measures_operation!("measures.frequency_rest_with_doppler", {
            cpp_frequency_rest_with_doppler(freq_hz, ref_in, doppler_value, doppler_ref).map_err(
                |message| OracleError::CppFailure {
                    operation: "measures.frequency_rest_with_doppler",
                    message,
                },
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn frequency_shift_with_doppler(
        freq_hz: f64,
        ref_in: &str,
        doppler_value: f64,
        doppler_ref: &str,
    ) -> Result<f64, OracleError> {
        measures_operation!("measures.frequency_shift_with_doppler", {
            cpp_frequency_shift_with_doppler(freq_hz, ref_in, doppler_value, doppler_ref).map_err(
                |message| OracleError::CppFailure {
                    operation: "measures.frequency_shift_with_doppler",
                    message,
                },
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn epoch_convert_with_frame(
        mjd_in: f64,
        ref_in: &str,
        ref_out: &str,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        dut1: f64,
    ) -> Result<f64, OracleError> {
        measures_operation!("measures.epoch_convert_with_frame", {
            cpp_epoch_convert_with_frame(mjd_in, ref_in, ref_out, obs_lon, obs_lat, obs_h, dut1)
                .map_err(|message| OracleError::CppFailure {
                    operation: "measures.epoch_convert_with_frame",
                    message,
                })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn eop_query(mjd: f64) -> Result<(f64, f64, f64), OracleError> {
        measures_operation!("measures.eop_query", {
            cpp_eop_query(mjd).map_err(|message| OracleError::CppFailure {
                operation: "measures.eop_query",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn simulator_baseline_uvw(
        obs_itrf_m: [f64; 3],
        ant_itrf_m: [f64; 3],
        phase_center_rad: [f64; 2],
        epoch_mjd_ut1: f64,
    ) -> Result<([f64; 3], [f64; 3]), OracleError> {
        measures_operation!("measures.simulator_baseline_uvw", {
            cpp_simulator_baseline_uvw(obs_itrf_m, ant_itrf_m, phase_center_rad, epoch_mjd_ut1)
                .map_err(|message| OracleError::CppFailure {
                    operation: "measures.simulator_baseline_uvw",
                    message,
                })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn baseline_convert(
        obs_itrf_m: [f64; 3],
        ant_itrf_m: [f64; 3],
        phase_center_rad: [f64; 2],
        epoch_mjd_ut1: f64,
        ref_out: &str,
    ) -> Result<[f64; 3], OracleError> {
        measures_operation!("measures.baseline_convert", {
            cpp_baseline_convert(
                obs_itrf_m,
                ant_itrf_m,
                phase_center_rad,
                epoch_mjd_ut1,
                ref_out,
            )
            .map_err(|message| OracleError::CppFailure {
                operation: "measures.baseline_convert",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn last_rad_for_itrf(obs_itrf_m: [f64; 3], epoch_mjd_ut1: f64) -> Result<f64, OracleError> {
        measures_operation!("measures.last_rad_for_itrf", {
            cpp_last_rad_for_itrf(obs_itrf_m, epoch_mjd_ut1).map_err(|message| {
                OracleError::CppFailure {
                    operation: "measures.last_rad_for_itrf",
                    message,
                }
            })
        })
    }
}

#[cfg(test)]
mod tests {
    #[cfg(not(has_casacore_cpp))]
    use super::MeasuresOracle;

    #[cfg(not(has_casacore_cpp))]
    #[test]
    fn facade_is_stable_without_cpp() {
        assert!(matches!(
            MeasuresOracle::epoch_convert(51_544.5, "UTC", "TAI"),
            Err(crate::OracleError::Unavailable { .. })
        ));
    }
}
