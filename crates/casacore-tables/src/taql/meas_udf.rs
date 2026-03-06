// SPDX-License-Identifier: LGPL-3.0-or-later
//! Measure UDF functions for TaQL (`meas.*`).
//!
//! Provides measure-aware conversion functions that can be called from TaQL
//! expressions, mirroring the C++ `meas.*` UDFs registered by
//! `register_meas()` in `meas/MeasUDF/Register.cc`.
//!
//! # Supported functions
//!
//! | Function | Description |
//! |----------|-------------|
//! | `meas.epoch` | Epoch (time scale) conversion |
//! | `meas.dir` / `meas.direction` | Sky direction conversion |
//! | `meas.pos` / `meas.position` | Position conversion |
//! | `meas.freq` / `meas.frequency` | Spectral frequency conversion |
//! | `meas.doppler` / `meas.redshift` | Doppler convention conversion |
//! | `meas.radvel` / `meas.radialvelocity` | Radial velocity conversion |
//! | `meas.j2000` | Shortcut: direction → J2000 |
//! | `meas.galactic` | Shortcut: direction → GALACTIC |
//! | `meas.b1950` | Shortcut: direction → B1950 |
//!
//! # C++ reference
//!
//! `meas/MeasUDF/Register.cc`, `EpochUDF.cc`, `DirectionUDF.cc`, etc.

use std::str::FromStr;

use casacore_types::measures::direction::{DirectionRef, MDirection};
use casacore_types::measures::doppler::{DopplerRef, MDoppler};
use casacore_types::measures::epoch::{EpochRef, MEpoch};
use casacore_types::measures::frame::MeasFrame;
use casacore_types::measures::frequency::{FrequencyRef, MFrequency};
use casacore_types::measures::position::{MPosition, PositionRef};
use casacore_types::measures::radial_velocity::{MRadialVelocity, RadialVelocityRef};

use super::error::TaqlError;
use super::eval::ExprValue;

/// Dispatch a `meas.*` function call.
///
/// Called from `call_function()` when the function name starts with `"meas."`.
pub(crate) fn call_meas_function(name: &str, args: &[ExprValue]) -> Result<ExprValue, TaqlError> {
    let suffix = &name[5..]; // strip "meas."
    match suffix {
        "epoch" => meas_epoch(args, name),
        "dir" | "direction" => meas_dir(args, name),
        "pos" | "position" => meas_pos(args, name),
        "freq" | "frequency" => meas_freq(args, name),
        "doppler" | "redshift" => meas_doppler(args, name),
        "radvel" | "radialvelocity" => meas_radvel(args, name),
        "j2000" => meas_dir_shortcut(args, "J2000", name),
        "galactic" => meas_dir_shortcut(args, "GALACTIC", name),
        "b1950" => meas_dir_shortcut(args, "B1950", name),
        _ => Err(TaqlError::UnknownFunction {
            name: name.to_string(),
        }),
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────

/// Parse a reference type string from an `ExprValue::String`.
fn parse_ref<R: FromStr>(val: &ExprValue, fn_name: &str) -> Result<R, TaqlError> {
    let s = val.to_string_val()?;
    R::from_str(&s).map_err(|_| TaqlError::TypeError {
        message: format!("{fn_name}: unknown reference type \"{s}\""),
    })
}

/// Convert a `MeasureError` into a `TaqlError`.
fn measure_err(fn_name: &str, e: casacore_types::measures::MeasureError) -> TaqlError {
    TaqlError::TypeError {
        message: format!("{fn_name}: {e}"),
    }
}

/// Check that `args.len()` is within `[min, max]`.
fn check_arity_range(
    name: &str,
    args: &[ExprValue],
    min: usize,
    max: usize,
) -> Result<(), TaqlError> {
    if args.len() < min || args.len() > max {
        Err(TaqlError::ArgumentCount {
            name: name.to_string(),
            expected: format!("{min}..{max}"),
            got: args.len(),
        })
    } else {
        Ok(())
    }
}

/// Return `Ok(true)` if any argument is null (for null propagation).
fn any_null(args: &[ExprValue]) -> bool {
    args.iter().any(|a| a.is_null())
}

/// Build a `MeasFrame` from optional epoch/position/direction float args.
///
/// `extra` is the slice of optional arguments after the required ones.
/// The interpretation depends on the calling function:
/// - For direction/frequency/radvel: `[epoch, px, py, pz]`
fn build_frame_with_epoch_pos(extra: &[ExprValue]) -> Result<MeasFrame, TaqlError> {
    let mut frame = MeasFrame::new();
    if !extra.is_empty() {
        let epoch_mjd = extra[0].to_float()?;
        frame = frame.with_epoch(MEpoch::from_mjd(epoch_mjd, EpochRef::UTC));
    }
    if extra.len() >= 4 {
        let px = extra[1].to_float()?;
        let py = extra[2].to_float()?;
        let pz = extra[3].to_float()?;
        frame = frame.with_position(MPosition::new_itrf(px, py, pz));
    }
    Ok(frame)
}

/// Build a full `MeasFrame` from optional direction + epoch + position args.
///
/// For frequency/radvel: `[dir_lon, dir_lat, epoch, px, py, pz]`
fn build_frame_with_dir_epoch_pos(extra: &[ExprValue]) -> Result<MeasFrame, TaqlError> {
    let mut frame = MeasFrame::new();
    if extra.len() >= 2 {
        let lon = extra[0].to_float()?;
        let lat = extra[1].to_float()?;
        frame = frame.with_direction(MDirection::from_angles(lon, lat, DirectionRef::J2000));
    }
    if extra.len() >= 3 {
        let epoch_mjd = extra[2].to_float()?;
        frame = frame.with_epoch(MEpoch::from_mjd(epoch_mjd, EpochRef::UTC));
    }
    if extra.len() >= 6 {
        let px = extra[3].to_float()?;
        let py = extra[4].to_float()?;
        let pz = extra[5].to_float()?;
        frame = frame.with_position(MPosition::new_itrf(px, py, pz));
    }
    Ok(frame)
}

// ── Engine functions ─────────────────────────────────────────────────────

/// `meas.epoch(target_ref, value [, src_ref])` — epoch conversion.
///
/// Converts a Modified Julian Date between time scales.
///
/// # Arguments
///
/// - `target_ref` — target reference type string (e.g. `"TAI"`, `"UTC"`)
/// - `value` — MJD as float
/// - `src_ref` (optional) — source reference type (default: `"UTC"`)
///
/// # Returns
///
/// `Float` — MJD in the target time scale.
fn meas_epoch(args: &[ExprValue], fn_name: &str) -> Result<ExprValue, TaqlError> {
    check_arity_range(fn_name, args, 2, 3)?;
    if any_null(args) {
        return Ok(ExprValue::Null);
    }
    let target: EpochRef = parse_ref(&args[0], fn_name)?;
    let mjd = args[1].to_float()?;
    let src: EpochRef = if args.len() == 3 {
        parse_ref(&args[2], fn_name)?
    } else {
        EpochRef::UTC
    };
    let epoch = MEpoch::from_mjd(mjd, src);
    let frame = MeasFrame::new();
    let converted = epoch
        .convert_to(target, &frame)
        .map_err(|e| measure_err(fn_name, e))?;
    Ok(ExprValue::Float(converted.value().as_mjd()))
}

/// `meas.dir(target_ref, lon, lat [, src_ref [, epoch, px, py, pz]])` — direction conversion.
///
/// Converts sky coordinates between reference frames.
///
/// # Arguments
///
/// - `target_ref` — target reference type string (e.g. `"GALACTIC"`, `"J2000"`)
/// - `lon` — longitude in radians
/// - `lat` — latitude in radians
/// - `src_ref` (optional) — source reference type (default: `"J2000"`)
/// - `epoch` (optional) — MJD for time-dependent conversions
/// - `px, py, pz` (optional) — ITRF observer position in metres
///
/// # Returns
///
/// `Array([lon, lat])` — direction in the target frame (radians).
fn meas_dir(args: &[ExprValue], fn_name: &str) -> Result<ExprValue, TaqlError> {
    check_arity_range(fn_name, args, 3, 8)?;
    if any_null(args) {
        return Ok(ExprValue::Null);
    }
    let target: DirectionRef = parse_ref(&args[0], fn_name)?;
    let lon = args[1].to_float()?;
    let lat = args[2].to_float()?;
    let src: DirectionRef = if args.len() >= 4 {
        parse_ref(&args[3], fn_name)?
    } else {
        DirectionRef::J2000
    };
    let frame = if args.len() > 4 {
        build_frame_with_epoch_pos(&args[4..])?
    } else {
        MeasFrame::new()
    };
    let dir = MDirection::from_angles(lon, lat, src);
    let converted = dir
        .convert_to(target, &frame)
        .map_err(|e| measure_err(fn_name, e))?;
    Ok(make_dir_result(&converted))
}

/// Direction shortcuts: `meas.j2000(lon, lat [, src_ref [, epoch, px, py, pz]])`
fn meas_dir_shortcut(
    args: &[ExprValue],
    target_name: &str,
    fn_name: &str,
) -> Result<ExprValue, TaqlError> {
    check_arity_range(fn_name, args, 2, 7)?;
    if any_null(args) {
        return Ok(ExprValue::Null);
    }
    let target: DirectionRef =
        DirectionRef::from_str(target_name).expect("hardcoded target must parse");
    let lon = args[0].to_float()?;
    let lat = args[1].to_float()?;
    let src: DirectionRef = if args.len() >= 3 {
        parse_ref(&args[2], fn_name)?
    } else {
        DirectionRef::J2000
    };
    let frame = if args.len() > 3 {
        build_frame_with_epoch_pos(&args[3..])?
    } else {
        MeasFrame::new()
    };
    let dir = MDirection::from_angles(lon, lat, src);
    let converted = dir
        .convert_to(target, &frame)
        .map_err(|e| measure_err(fn_name, e))?;
    Ok(make_dir_result(&converted))
}

/// `meas.pos(target_ref, x, y, z [, src_ref])` — position conversion.
///
/// Converts between ITRF (geocentric Cartesian) and WGS84 (geodetic).
///
/// # Arguments
///
/// - `target_ref` — target reference type string (`"ITRF"` or `"WGS84"`)
/// - `x, y, z` — coordinates (ITRF: metres; WGS84: lon_rad, lat_rad, height_m)
/// - `src_ref` (optional) — source reference type (default: `"ITRF"`)
///
/// # Returns
///
/// `Array([x, y, z])` — position in the target frame.
fn meas_pos(args: &[ExprValue], fn_name: &str) -> Result<ExprValue, TaqlError> {
    check_arity_range(fn_name, args, 4, 5)?;
    if any_null(args) {
        return Ok(ExprValue::Null);
    }
    let target: PositionRef = parse_ref(&args[0], fn_name)?;
    let x = args[1].to_float()?;
    let y = args[2].to_float()?;
    let z = args[3].to_float()?;
    let src: PositionRef = if args.len() == 5 {
        parse_ref(&args[4], fn_name)?
    } else {
        PositionRef::ITRF
    };
    let pos = match src {
        PositionRef::ITRF => MPosition::new_itrf(x, y, z),
        PositionRef::WGS84 => MPosition::new_wgs84(x, y, z),
    };
    let converted = pos
        .convert_to(target)
        .map_err(|e| measure_err(fn_name, e))?;
    let vals = converted.values();
    Ok(ExprValue::Array(super::eval::ArrayValue {
        shape: vec![3],
        data: vec![
            ExprValue::Float(vals[0]),
            ExprValue::Float(vals[1]),
            ExprValue::Float(vals[2]),
        ],
    }))
}

/// `meas.freq(target_ref, hz [, src_ref [, dir_lon, dir_lat, epoch, px, py, pz]])` — frequency conversion.
///
/// Converts spectral frequencies between reference frames.
///
/// # Arguments
///
/// - `target_ref` — target reference type string (e.g. `"BARY"`, `"LSRK"`)
/// - `hz` — frequency in Hz
/// - `src_ref` (optional) — source reference type (default: `"LSRK"`)
/// - `dir_lon, dir_lat` (optional) — J2000 direction in radians
/// - `epoch` (optional) — MJD
/// - `px, py, pz` (optional) — ITRF observer position in metres
///
/// # Returns
///
/// `Float` — frequency in Hz in the target frame.
fn meas_freq(args: &[ExprValue], fn_name: &str) -> Result<ExprValue, TaqlError> {
    check_arity_range(fn_name, args, 2, 9)?;
    if any_null(args) {
        return Ok(ExprValue::Null);
    }
    let target: FrequencyRef = parse_ref(&args[0], fn_name)?;
    let hz = args[1].to_float()?;
    let src: FrequencyRef = if args.len() >= 3 {
        parse_ref(&args[2], fn_name)?
    } else {
        FrequencyRef::LSRK
    };
    let frame = if args.len() > 3 {
        build_frame_with_dir_epoch_pos(&args[3..])?
    } else {
        MeasFrame::new()
    };
    let freq = MFrequency::new(hz, src);
    let converted = freq
        .convert_to(target, &frame)
        .map_err(|e| measure_err(fn_name, e))?;
    Ok(ExprValue::Float(converted.hz()))
}

/// `meas.doppler(target_ref, value [, src_ref])` — Doppler convention conversion.
///
/// Converts Doppler values between conventions (RADIO, Z, BETA, etc.).
///
/// # Arguments
///
/// - `target_ref` — target convention string (e.g. `"Z"`, `"RADIO"`)
/// - `value` — Doppler parameter value
/// - `src_ref` (optional) — source convention (default: `"RADIO"`)
///
/// # Returns
///
/// `Float` — Doppler value in the target convention.
fn meas_doppler(args: &[ExprValue], fn_name: &str) -> Result<ExprValue, TaqlError> {
    check_arity_range(fn_name, args, 2, 3)?;
    if any_null(args) {
        return Ok(ExprValue::Null);
    }
    let target: DopplerRef = parse_ref(&args[0], fn_name)?;
    let value = args[1].to_float()?;
    let src: DopplerRef = if args.len() == 3 {
        parse_ref(&args[2], fn_name)?
    } else {
        DopplerRef::RADIO
    };
    let doppler = MDoppler::new(value, src);
    let frame = MeasFrame::new();
    let converted = doppler
        .convert_to(target, &frame)
        .map_err(|e| measure_err(fn_name, e))?;
    Ok(ExprValue::Float(converted.value()))
}

/// `meas.radvel(target_ref, ms [, src_ref [, dir_lon, dir_lat, epoch, px, py, pz]])` — radial velocity conversion.
///
/// Converts radial velocities between reference frames.
///
/// # Arguments
///
/// - `target_ref` — target reference type string (e.g. `"BARY"`, `"LSRK"`)
/// - `ms` — velocity in m/s
/// - `src_ref` (optional) — source reference type (default: `"LSRK"`)
/// - `dir_lon, dir_lat` (optional) — J2000 direction in radians
/// - `epoch` (optional) — MJD
/// - `px, py, pz` (optional) — ITRF observer position in metres
///
/// # Returns
///
/// `Float` — velocity in m/s in the target frame.
fn meas_radvel(args: &[ExprValue], fn_name: &str) -> Result<ExprValue, TaqlError> {
    check_arity_range(fn_name, args, 2, 9)?;
    if any_null(args) {
        return Ok(ExprValue::Null);
    }
    let target: RadialVelocityRef = parse_ref(&args[0], fn_name)?;
    let ms = args[1].to_float()?;
    let src: RadialVelocityRef = if args.len() >= 3 {
        parse_ref(&args[2], fn_name)?
    } else {
        RadialVelocityRef::LSRK
    };
    let frame = if args.len() > 3 {
        build_frame_with_dir_epoch_pos(&args[3..])?
    } else {
        MeasFrame::new()
    };
    let rv = MRadialVelocity::new(ms, src);
    let converted = rv
        .convert_to(target, &frame)
        .map_err(|e| measure_err(fn_name, e))?;
    Ok(ExprValue::Float(converted.ms()))
}

/// Build a direction result as `Array([lon, lat])`.
fn make_dir_result(dir: &MDirection) -> ExprValue {
    ExprValue::Array(super::eval::ArrayValue {
        shape: vec![2],
        data: vec![
            ExprValue::Float(dir.longitude_rad()),
            ExprValue::Float(dir.latitude_rad()),
        ],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn s(val: &str) -> ExprValue {
        ExprValue::String(val.to_string())
    }
    fn f(val: f64) -> ExprValue {
        ExprValue::Float(val)
    }

    // ── Epoch ────────────────────────────────────────────────────────

    #[test]
    fn epoch_utc_to_tai() {
        // J2000.0 = MJD 51544.5 in UTC; TAI is 32 leap seconds ahead.
        let result = call_meas_function("meas.epoch", &[s("TAI"), f(51544.5)]).unwrap();
        let mjd = match result {
            ExprValue::Float(v) => v,
            other => panic!("expected Float, got {other:?}"),
        };
        let offset_s = (mjd - 51544.5) * 86400.0;
        assert!(
            (offset_s - 32.0).abs() < 0.01,
            "UTC→TAI offset should be ~32s, got {offset_s}"
        );
    }

    #[test]
    fn epoch_explicit_source() {
        let result = call_meas_function("meas.epoch", &[s("TAI"), f(51544.5), s("UTC")]).unwrap();
        let mjd = match result {
            ExprValue::Float(v) => v,
            other => panic!("expected Float, got {other:?}"),
        };
        let offset_s = (mjd - 51544.5) * 86400.0;
        assert!((offset_s - 32.0).abs() < 0.01);
    }

    #[test]
    fn epoch_tai_roundtrip() {
        // TAI→UTC→TAI should be identity.
        let tai_mjd = 51544.5 + 32.0 / 86400.0;
        let utc = call_meas_function("meas.epoch", &[s("UTC"), f(tai_mjd), s("TAI")]).unwrap();
        let utc_mjd = match utc {
            ExprValue::Float(v) => v,
            other => panic!("expected Float, got {other:?}"),
        };
        assert!(
            (utc_mjd - 51544.5).abs() < 1e-10,
            "roundtrip failed: {utc_mjd}"
        );
    }

    // ── Direction ────────────────────────────────────────────────────

    #[test]
    fn dir_j2000_to_galactic() {
        // Galactic center ≈ (l=0, b=0) maps to J2000 ≈ (RA=266.4°, Dec=-28.9°)
        // Convert J2000 (0,0) to GALACTIC — just check it produces a valid result.
        let result =
            call_meas_function("meas.dir", &[s("GALACTIC"), f(0.0), f(0.0), s("J2000")]).unwrap();
        match result {
            ExprValue::Array(arr) => {
                assert_eq!(arr.shape, vec![2]);
                assert_eq!(arr.data.len(), 2);
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn j2000_shortcut() {
        // Convert galactic center to J2000 via shortcut.
        let result = call_meas_function("meas.j2000", &[f(0.0), f(0.0), s("GALACTIC")]).unwrap();
        let (lon, lat) = extract_dir(&result);
        // Galactic center in J2000: RA ≈ 4.65 rad (266.4°), Dec ≈ -0.505 rad (-28.9°)
        assert!((lon - 4.65).abs() < 0.02, "expected RA≈4.65 rad, got {lon}");
        assert!(
            (lat - (-0.505)).abs() < 0.02,
            "expected Dec≈-0.505 rad, got {lat}"
        );
    }

    #[test]
    fn galactic_shortcut() {
        // RA=0, Dec=0 in J2000 → galactic
        let result = call_meas_function("meas.galactic", &[f(0.0), f(0.0)]).unwrap();
        let (lon, lat) = extract_dir(&result);
        // J2000 (0,0) → Galactic ≈ (l≈96.3°, b≈-60.2°) = (1.681, -1.050) rad
        assert!(
            (lon - 1.681).abs() < 0.02,
            "expected l≈1.681 rad, got {lon}"
        );
        assert!(
            (lat - (-1.050)).abs() < 0.02,
            "expected b≈-1.050 rad, got {lat}"
        );
    }

    #[test]
    fn dir_roundtrip_j2000_galactic() {
        let orig_lon = 1.0;
        let orig_lat = 0.5;
        // J2000 → GALACTIC
        let gal = call_meas_function(
            "meas.dir",
            &[s("GALACTIC"), f(orig_lon), f(orig_lat), s("J2000")],
        )
        .unwrap();
        let (glon, glat) = extract_dir(&gal);
        // GALACTIC → J2000
        let j2k =
            call_meas_function("meas.dir", &[s("J2000"), f(glon), f(glat), s("GALACTIC")]).unwrap();
        let (rlon, rlat) = extract_dir(&j2k);
        assert!(
            (rlon - orig_lon).abs() < 1e-10,
            "lon roundtrip: {rlon} vs {orig_lon}"
        );
        assert!(
            (rlat - orig_lat).abs() < 1e-10,
            "lat roundtrip: {rlat} vs {orig_lat}"
        );
    }

    // ── Position ─────────────────────────────────────────────────────

    #[test]
    fn pos_itrf_to_wgs84() {
        // VLA ≈ ITRF (-1601185, -5041977, 3554876) metres
        let result = call_meas_function(
            "meas.pos",
            &[
                s("WGS84"),
                f(-1601185.0),
                f(-5041977.0),
                f(3554876.0),
                s("ITRF"),
            ],
        )
        .unwrap();
        match &result {
            ExprValue::Array(arr) => {
                assert_eq!(arr.shape, vec![3]);
                let lon = match &arr.data[0] {
                    ExprValue::Float(v) => *v,
                    _ => panic!("expected float"),
                };
                let lat = match &arr.data[1] {
                    ExprValue::Float(v) => *v,
                    _ => panic!("expected float"),
                };
                // VLA ≈ lon -107.6° (-1.879 rad), lat 34.1° (0.595 rad)
                assert!(
                    (lon - (-1.879)).abs() < 0.01,
                    "expected lon≈-1.879, got {lon}"
                );
                assert!((lat - 0.595).abs() < 0.01, "expected lat≈0.595, got {lat}");
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn pos_wgs84_to_itrf_roundtrip() {
        let lon = -1.879_f64;
        let lat = 0.595_f64;
        let height = 2100.0_f64;
        // WGS84 → ITRF
        let itrf = call_meas_function(
            "meas.pos",
            &[s("ITRF"), f(lon), f(lat), f(height), s("WGS84")],
        )
        .unwrap();
        let vals = extract_array3(&itrf);
        // ITRF → WGS84
        let wgs84 = call_meas_function(
            "meas.pos",
            &[s("WGS84"), f(vals[0]), f(vals[1]), f(vals[2]), s("ITRF")],
        )
        .unwrap();
        let back = extract_array3(&wgs84);
        assert!((back[0] - lon).abs() < 1e-8, "lon roundtrip failed");
        assert!((back[1] - lat).abs() < 1e-8, "lat roundtrip failed");
        assert!((back[2] - height).abs() < 1.0, "height roundtrip failed");
    }

    // ── Doppler ──────────────────────────────────────────────────────

    #[test]
    fn doppler_radio_to_z() {
        // radio = 0.5 → z = 1/(1-0.5) - 1 = 1.0
        let result = call_meas_function("meas.doppler", &[s("Z"), f(0.5), s("RADIO")]).unwrap();
        let z = match result {
            ExprValue::Float(v) => v,
            other => panic!("expected Float, got {other:?}"),
        };
        assert!((z - 1.0).abs() < 1e-10, "expected z≈1.0, got {z}");
    }

    #[test]
    fn doppler_default_source() {
        // Default source is RADIO. Convert 0.0 to Z → 0.0
        let result = call_meas_function("meas.doppler", &[s("Z"), f(0.0)]).unwrap();
        let z = match result {
            ExprValue::Float(v) => v,
            other => panic!("expected Float, got {other:?}"),
        };
        assert!(z.abs() < 1e-10, "expected z≈0, got {z}");
    }

    // ── Frequency ────────────────────────────────────────────────────

    #[test]
    fn freq_same_frame() {
        // LSRK→LSRK should be identity.
        let result = call_meas_function("meas.freq", &[s("LSRK"), f(1.4e9), s("LSRK")]).unwrap();
        let hz = match result {
            ExprValue::Float(v) => v,
            other => panic!("expected Float, got {other:?}"),
        };
        assert!(
            (hz - 1.4e9).abs() < 1.0,
            "same-frame should be identity, got {hz}"
        );
    }

    // ── Radial velocity ──────────────────────────────────────────────

    #[test]
    fn radvel_same_frame() {
        // LSRK→LSRK should be identity.
        let result = call_meas_function("meas.radvel", &[s("LSRK"), f(1000.0), s("LSRK")]).unwrap();
        let ms = match result {
            ExprValue::Float(v) => v,
            other => panic!("expected Float, got {other:?}"),
        };
        assert!(
            (ms - 1000.0).abs() < 0.01,
            "same-frame should be identity, got {ms}"
        );
    }

    // ── NULL propagation ─────────────────────────────────────────────

    #[test]
    fn null_propagation() {
        let result = call_meas_function("meas.epoch", &[s("TAI"), ExprValue::Null]).unwrap();
        assert!(result.is_null());

        let result =
            call_meas_function("meas.dir", &[s("GALACTIC"), ExprValue::Null, f(0.0)]).unwrap();
        assert!(result.is_null());
    }

    // ── Error cases ──────────────────────────────────────────────────

    #[test]
    fn wrong_arity() {
        let err = call_meas_function("meas.epoch", &[s("TAI")]);
        assert!(matches!(err, Err(TaqlError::ArgumentCount { .. })));
    }

    #[test]
    fn invalid_ref_string() {
        let err = call_meas_function("meas.epoch", &[s("BOGUS"), f(51544.5)]);
        assert!(matches!(err, Err(TaqlError::TypeError { .. })));
    }

    #[test]
    fn unknown_meas_function() {
        let err = call_meas_function("meas.nonexistent", &[]);
        assert!(matches!(err, Err(TaqlError::UnknownFunction { .. })));
    }

    // ── B1950 shortcut ───────────────────────────────────────────────

    #[test]
    fn b1950_shortcut() {
        // Just verify it works without error and returns 2-element array.
        let result = call_meas_function("meas.b1950", &[f(0.0), f(0.0)]).unwrap();
        let (_lon, _lat) = extract_dir(&result);
    }

    // ── Alias coverage ───────────────────────────────────────────────

    #[test]
    fn direction_alias() {
        let result = call_meas_function(
            "meas.direction",
            &[s("GALACTIC"), f(0.0), f(0.0), s("J2000")],
        )
        .unwrap();
        let _ = extract_dir(&result);
    }

    #[test]
    fn position_alias() {
        let result = call_meas_function(
            "meas.position",
            &[s("ITRF"), f(-1.879), f(0.595), f(2100.0), s("WGS84")],
        )
        .unwrap();
        let _ = extract_array3(&result);
    }

    #[test]
    fn frequency_alias() {
        let result =
            call_meas_function("meas.frequency", &[s("LSRK"), f(1.4e9), s("LSRK")]).unwrap();
        assert!(matches!(result, ExprValue::Float(_)));
    }

    #[test]
    fn redshift_alias() {
        let result = call_meas_function("meas.redshift", &[s("Z"), f(0.0), s("RADIO")]).unwrap();
        assert!(matches!(result, ExprValue::Float(_)));
    }

    #[test]
    fn radialvelocity_alias() {
        let result =
            call_meas_function("meas.radialvelocity", &[s("LSRK"), f(0.0), s("LSRK")]).unwrap();
        assert!(matches!(result, ExprValue::Float(_)));
    }

    // ── Helpers ──────────────────────────────────────────────────────

    fn extract_dir(val: &ExprValue) -> (f64, f64) {
        match val {
            ExprValue::Array(arr) => {
                assert_eq!(arr.shape, vec![2]);
                let lon = match &arr.data[0] {
                    ExprValue::Float(v) => *v,
                    other => panic!("expected Float, got {other:?}"),
                };
                let lat = match &arr.data[1] {
                    ExprValue::Float(v) => *v,
                    other => panic!("expected Float, got {other:?}"),
                };
                (lon, lat)
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    fn extract_array3(val: &ExprValue) -> [f64; 3] {
        match val {
            ExprValue::Array(arr) => {
                assert_eq!(arr.shape, vec![3]);
                let mut out = [0.0; 3];
                for (i, v) in arr.data.iter().enumerate() {
                    out[i] = match v {
                        ExprValue::Float(f) => *f,
                        other => panic!("expected Float, got {other:?}"),
                    };
                }
                out
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }
}
