// SPDX-License-Identifier: LGPL-3.0-or-later
//! C++ casacore's ordered frequency conversion: CASA's
//! `MSUtil::getFreqRangeInSpw` loop, one `MFrequency::Convert` over a
//! sequence of epochs.

use crate::measures_oracle::MeasuresOracle;
use crate::oracle_runtime::{OracleError, oracle_operation};

#[cfg(has_casacore_cpp)]
use crate::oracle_runtime::CasacoreOracleRuntime;

#[cfg(has_casacore_cpp)]
unsafe extern "C" {
    fn measures_shim_frequency_convert_sequence(
        freq_hz: *const f64,
        n_freq: i32,
        ref_in: *const std::ffi::c_char,
        ref_out: *const std::ffi::c_char,
        epoch_mjd: *const f64,
        n_epoch: i32,
        dir_lon: f64,
        dir_lat: f64,
        dir_ref: *const std::ffi::c_char,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        freq_out: *mut f64,
    ) -> i32;
}

/// One ordered conversion run: every frequency converted at each epoch in
/// turn, through one converter on one mutable frame.
#[derive(Debug, Clone, Copy)]
pub struct FrequencySequence<'a> {
    /// Frequencies to convert at every epoch, in Hz.
    pub frequencies_hz: &'a [f64],
    /// Source frame name, for example `"TOPO"`.
    pub ref_in: &'a str,
    /// Target frame name, for example `"LSRK"`.
    pub ref_out: &'a str,
    /// UTC epochs in MJD days, in the order they are applied.
    pub epochs_mjd: &'a [f64],
    /// Direction longitude and latitude in radians.
    pub direction_rad: [f64; 2],
    /// Direction frame name, for example `"J2000"`.
    pub direction_ref: &'a str,
    /// Observatory WGS84 longitude and latitude (radians) and height (m).
    pub observatory: [f64; 3],
}

impl MeasuresOracle {
    /// Run CASA's ordered conversion in C++ casacore. The result is
    /// epoch-major: `result[e * frequencies.len() + f]`.
    #[cfg_attr(not(has_casacore_cpp), allow(unused_variables))]
    pub fn frequency_convert_sequence(
        sequence: FrequencySequence<'_>,
    ) -> Result<Vec<f64>, OracleError> {
        oracle_operation!("measures.frequency_convert_sequence", {
            cpp_frequency_convert_sequence(sequence).map_err(|message| {
                OracleError::CppFailure {
                    operation: "measures.frequency_convert_sequence",
                    message,
                }
            })
        })
    }
}

#[cfg(has_casacore_cpp)]
fn cpp_frequency_convert_sequence(sequence: FrequencySequence<'_>) -> Result<Vec<f64>, String> {
    let c_string = |value| {
        CasacoreOracleRuntime::c_string("measures input", value).map_err(|error| error.to_string())
    };
    let (c_in, c_out, c_dir) = (
        c_string(sequence.ref_in)?,
        c_string(sequence.ref_out)?,
        c_string(sequence.direction_ref)?,
    );
    let count = |values: &[f64]| i32::try_from(values.len()).map_err(|error| error.to_string());
    let (n_freq, n_epoch) = (count(sequence.frequencies_hz)?, count(sequence.epochs_mjd)?);
    let mut out = vec![0.0; sequence.frequencies_hz.len() * sequence.epochs_mjd.len()];
    let [observatory_lon, observatory_lat, observatory_h] = sequence.observatory;
    // SAFETY: the pointers and lengths describe live slices and `out` holds
    // exactly `n_freq * n_epoch` values; the strings are NUL-terminated.
    let rc = unsafe {
        measures_shim_frequency_convert_sequence(
            sequence.frequencies_hz.as_ptr(),
            n_freq,
            c_in.as_ptr(),
            c_out.as_ptr(),
            sequence.epochs_mjd.as_ptr(),
            n_epoch,
            sequence.direction_rad[0],
            sequence.direction_rad[1],
            c_dir.as_ptr(),
            observatory_lon,
            observatory_lat,
            observatory_h,
            out.as_mut_ptr(),
        )
    };
    if rc == 0 {
        Ok(out)
    } else {
        Err(format!("C++ frequency_convert_sequence failed: rc={rc}"))
    }
}
