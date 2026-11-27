// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

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
pub(super) fn meas_epoch(
    args: &[ExprValue],
    fn_name: &str,
    measures: MeasuresRef<'_>,
) -> Result<ExprValue, TaqlError> {
    check_arity_range(fn_name, args, 2, 3)?;
    if any_null(args) {
        return Ok(ExprValue::Null);
    }
    let target: EpochRef = parse_ref(&args[0], fn_name)?;
    let mjd = expr_to_mjd_days(&args[1], fn_name)?;
    let src: EpochRef = if args.len() == 3 {
        parse_ref(&args[2], fn_name)?
    } else {
        EpochRef::UTC
    };
    let epoch = MEpoch::from_mjd(mjd, src);
    let frame = meas_frame(measures);
    let converted = epoch
        .convert_to(target, &frame)
        .map_err(|e| measure_err(fn_name, e))?;
    Ok(ExprValue::Float(converted.value().as_mjd()))
}

/// `meas.last(value, position)` / `meas.lst(value, position)` — local apparent sidereal time.
///
/// Converts a UTC MJD to LAST using the supplied observatory position.
///
/// Accepted position forms:
/// - `observatory_name`
/// - `x, y, z [, pos_ref]`
///
/// Returns the sidereal time as seconds into the local sidereal day, matching
/// C++ TaQL `meas.last` / `meas.lst`.
pub(super) fn meas_last(
    args: &[ExprValue],
    fn_name: &str,
    measures: MeasuresRef<'_>,
) -> Result<ExprValue, TaqlError> {
    if !(args.len() == 2 || args.len() == 4 || args.len() == 5) {
        return Err(TaqlError::ArgumentCount {
            name: fn_name.to_string(),
            expected: "2 or 4..5".to_string(),
            got: args.len(),
        });
    }
    if any_null(args) {
        return Ok(ExprValue::Null);
    }
    let mjd = expr_to_mjd_days(&args[0], fn_name)?;
    let position = parse_position_input(&args[1..], fn_name, measures)?;
    let epoch = MEpoch::from_mjd(mjd, EpochRef::UTC);
    let frame = meas_frame(measures)
        .with_position(position)
        .with_epoch(epoch.clone());
    let converted = epoch
        .convert_to(EpochRef::LAST, &frame)
        .map_err(|e| measure_err(fn_name, e))?;
    Ok(ExprValue::Float(converted.value().frac() * SECONDS_PER_DAY))
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
pub(super) fn meas_dir(
    args: &[ExprValue],
    fn_name: &str,
    measures: MeasuresRef<'_>,
) -> Result<ExprValue, TaqlError> {
    check_arity_range(fn_name, args, 2, 8)?;
    if any_null(args) {
        return Ok(ExprValue::Null);
    }
    let target: DirectionRef = parse_ref(&args[0], fn_name)?;
    if let ExprValue::String(name) = &args[1] {
        let frame = if args.len() > 2 {
            build_frame_with_epoch_pos(&args[2..], fn_name, measures)?
        } else {
            meas_frame(measures)
        };
        let dir =
            MDirection::from_source_name(name, &frame).map_err(|e| measure_err(fn_name, e))?;
        let converted = dir
            .convert_to(target, &frame)
            .map_err(|e| measure_err(fn_name, e))?;
        return Ok(make_dir_result(&converted));
    }

    check_arity_range(fn_name, args, 3, 8)?;
    let lon = expr_to_angle_rad(&args[1], fn_name)?;
    let lat = expr_to_angle_rad(&args[2], fn_name)?;
    let src: DirectionRef = if args.len() >= 4 {
        parse_ref(&args[3], fn_name)?
    } else {
        DirectionRef::J2000
    };
    let frame = if args.len() > 4 {
        build_frame_with_epoch_pos(&args[4..], fn_name, measures)?
    } else {
        meas_frame(measures)
    };
    let dir = MDirection::from_angles(lon, lat, src);
    let converted = dir
        .convert_to(target, &frame)
        .map_err(|e| measure_err(fn_name, e))?;
    Ok(make_dir_result(&converted))
}

/// `meas.dircos(target_ref, lon, lat [, src_ref [, epoch, px, py, pz]])`
/// returns direction cosines in the target frame.
pub(super) fn meas_dircos(
    args: &[ExprValue],
    fn_name: &str,
    measures: MeasuresRef<'_>,
) -> Result<ExprValue, TaqlError> {
    check_arity_range(fn_name, args, 2, 8)?;
    if any_null(args) {
        return Ok(ExprValue::Null);
    }
    let target: DirectionRef = parse_ref(&args[0], fn_name)?;
    if let ExprValue::String(name) = &args[1] {
        let frame = if args.len() > 2 {
            build_frame_with_epoch_pos(&args[2..], fn_name, measures)?
        } else {
            meas_frame(measures)
        };
        let dir =
            MDirection::from_source_name(name, &frame).map_err(|e| measure_err(fn_name, e))?;
        let converted = dir
            .convert_to(target, &frame)
            .map_err(|e| measure_err(fn_name, e))?;
        return Ok(make_array_result(&converted.cosines()));
    }

    check_arity_range(fn_name, args, 3, 8)?;
    let lon = expr_to_angle_rad(&args[1], fn_name)?;
    let lat = expr_to_angle_rad(&args[2], fn_name)?;
    let src: DirectionRef = if args.len() >= 4 {
        parse_ref(&args[3], fn_name)?
    } else {
        DirectionRef::J2000
    };
    let frame = if args.len() > 4 {
        build_frame_with_epoch_pos(&args[4..], fn_name, measures)?
    } else {
        meas_frame(measures)
    };
    let dir = MDirection::from_angles(lon, lat, src);
    let converted = dir
        .convert_to(target, &frame)
        .map_err(|e| measure_err(fn_name, e))?;
    Ok(make_array_result(&converted.cosines()))
}

/// Direction shortcuts: `meas.j2000(lon, lat [, src_ref [, epoch, px, py, pz]])`
pub(super) fn meas_dir_shortcut(
    args: &[ExprValue],
    target_name: &str,
    fn_name: &str,
    measures: MeasuresRef<'_>,
) -> Result<ExprValue, TaqlError> {
    check_arity_range(fn_name, args, 1, 7)?;
    if any_null(args) {
        return Ok(ExprValue::Null);
    }
    let target: DirectionRef =
        DirectionRef::from_str(target_name).expect("hardcoded target must parse");
    if let Some(ExprValue::String(name)) = args.first() {
        let frame = if args.len() > 1 {
            build_frame_with_epoch_pos(&args[1..], fn_name, measures)?
        } else {
            meas_frame(measures)
        };
        let dir =
            MDirection::from_source_name(name, &frame).map_err(|e| measure_err(fn_name, e))?;
        let converted = dir
            .convert_to(target, &frame)
            .map_err(|e| measure_err(fn_name, e))?;
        return Ok(make_dir_result(&converted));
    }

    check_arity_range(fn_name, args, 2, 7)?;
    let lon = expr_to_angle_rad(&args[0], fn_name)?;
    let lat = expr_to_angle_rad(&args[1], fn_name)?;
    let src: DirectionRef = if args.len() >= 3 {
        parse_ref(&args[2], fn_name)?
    } else {
        DirectionRef::J2000
    };
    let frame = if args.len() > 3 {
        build_frame_with_epoch_pos(&args[3..], fn_name, measures)?
    } else {
        meas_frame(measures)
    };
    let dir = MDirection::from_angles(lon, lat, src);
    let converted = dir
        .convert_to(target, &frame)
        .map_err(|e| measure_err(fn_name, e))?;
    Ok(make_dir_result(&converted))
}

/// `meas.riset(direction, epoch, position)` / `meas.riseset(...)` — rise and
/// set UTC datetimes for a source.
pub(super) fn meas_riseset(
    args: &[ExprValue],
    fn_name: &str,
    measures: MeasuresRef<'_>,
) -> Result<ExprValue, TaqlError> {
    if any_null(args) {
        return Ok(ExprValue::Null);
    }
    if args.len() < 2 {
        return Err(TaqlError::ArgumentCount {
            name: fn_name.to_string(),
            expected: "2..8".to_string(),
            got: args.len(),
        });
    }

    let riseset = if let ExprValue::String(name) = &args[0] {
        let frame = build_frame_with_epoch_pos(&args[1..], fn_name, measures)?;
        rise_set_times_from_name(name, &frame).map_err(|e| measure_err(fn_name, e))?
    } else {
        if args.len() < 3 {
            return Err(TaqlError::ArgumentCount {
                name: fn_name.to_string(),
                expected: "3..8".to_string(),
                got: args.len(),
            });
        }
        let lon = expr_to_angle_rad(&args[0], fn_name)?;
        let lat = expr_to_angle_rad(&args[1], fn_name)?;
        let (src, tail_start) = if args.len() >= 4 && matches!(args[2], ExprValue::String(_)) {
            (parse_ref(&args[2], fn_name)?, 3)
        } else {
            (DirectionRef::J2000, 2)
        };
        let frame = build_frame_with_epoch_pos(&args[tail_start..], fn_name, measures)?;
        let dir = MDirection::from_angles(lon, lat, src);
        dir.rise_set_times(&frame)
            .map_err(|e| measure_err(fn_name, e))?
    };

    Ok(make_datetime_array_result(&[
        riseset.rise_mjd,
        riseset.set_mjd,
    ]))
}

/// `meas.em*` — explicit Earth-magnetic vector conversion.
///
/// Explicit Earth-magnetic inputs follow casacore's unit-driven split between
/// XYZ flux-density triples and angle/angle/length triples when TaQL quantity
/// literals carry units.
pub(super) fn meas_earthmag(
    args: &[ExprValue],
    output: EarthMagneticOutput,
    fn_name: &str,
    measures: MeasuresRef<'_>,
) -> Result<ExprValue, TaqlError> {
    check_arity_range(fn_name, args, 1, 9)?;
    if any_null(args) {
        return Ok(ExprValue::Null);
    }

    let (target, start) = if let Some(ExprValue::String(value)) = args.first() {
        match EarthMagneticRef::from_str(value) {
            Ok(target) => {
                if target == EarthMagneticRef::IGRF {
                    return Err(TaqlError::TypeError {
                        message: format!(
                            "{fn_name}: IGRF cannot be used as an explicit target; use meas.igrf*"
                        ),
                    });
                }
                (target, 1)
            }
            Err(_) => (EarthMagneticRef::ITRF, 0),
        }
    } else {
        (EarthMagneticRef::ITRF, 0)
    };

    let (parsed, src, consumed) = parse_earthmag_xyz_input(&args[start..], fn_name)?;
    if src == EarthMagneticRef::IGRF {
        return Err(TaqlError::TypeError {
            message: format!(
                "{fn_name}: source EarthMagnetic reference cannot be IGRF; use meas.igrf*"
            ),
        });
    }

    let frame = if args.len() > start + consumed {
        build_frame_with_epoch_pos(&args[start + consumed..], fn_name, measures)?
    } else {
        meas_frame(measures)
    };
    let field = match parsed {
        EarthMagneticParsedInput::Xyz(xyz) => {
            MEarthMagnetic::from_xyz_nt(xyz[0], xyz[1], xyz[2], src)
        }
        EarthMagneticParsedInput::Angles {
            lon_rad,
            lat_rad,
            length_nt,
        } => MEarthMagnetic::from_angles(lon_rad, lat_rad, length_nt, src),
    };
    let converted = field
        .convert_to(target, &frame)
        .map_err(|e| measure_err(fn_name, e))?;
    Ok(make_earthmag_result(&converted, output))
}

/// `meas.igrf*` — IGRF model helpers.
pub(super) fn meas_igrf(
    args: &[ExprValue],
    output: IgrfOutput,
    fn_name: &str,
    measures: MeasuresRef<'_>,
) -> Result<ExprValue, TaqlError> {
    check_arity_range(fn_name, args, 2, 8)?;
    if any_null(args) {
        return Ok(ExprValue::Null);
    }

    let (target, start) = match output {
        IgrfOutput::Los | IgrfOutput::Long => {
            if let Some(ExprValue::String(value)) = args.first() {
                if EarthMagneticRef::from_str(value).is_ok() {
                    (EarthMagneticRef::ITRF, 1)
                } else {
                    (EarthMagneticRef::ITRF, 0)
                }
            } else {
                (EarthMagneticRef::ITRF, 0)
            }
        }
        _ => {
            if let Some(ExprValue::String(value)) = args.first() {
                match EarthMagneticRef::from_str(value) {
                    Ok(target) => {
                        if target == EarthMagneticRef::IGRF {
                            return Err(TaqlError::TypeError {
                                message: format!(
                                    "{fn_name}: IGRF cannot be used as an output frame"
                                ),
                            });
                        }
                        (target, 1)
                    }
                    Err(_) => (EarthMagneticRef::ITRF, 0),
                }
            } else {
                (EarthMagneticRef::ITRF, 0)
            }
        }
    };

    let height_m = expr_to_length_m(&args[start], fn_name)?;
    let (direction_input, consumed) = parse_direction_input(&args[start + 1..], fn_name)?;
    let frame = if args.len() > start + 1 + consumed {
        build_frame_with_epoch_pos(&args[start + 1 + consumed..], fn_name, measures)?
    } else {
        meas_frame(measures)
    };
    let direction = direction_input.materialize(&frame)?;
    let sample =
        calculate_igrf(height_m, &direction, &frame).map_err(|e| measure_err(fn_name, e))?;

    match output {
        IgrfOutput::Los => Ok(ExprValue::Float(sample.los_field_nt)),
        IgrfOutput::Long => Ok(ExprValue::Float(sample.longitude_rad)),
        _ => {
            let field = sample
                .field
                .convert_to(target, &frame)
                .map_err(|e| measure_err(fn_name, e))?;
            Ok(make_earthmag_result(
                &field,
                match output {
                    IgrfOutput::Xyz => EarthMagneticOutput::Xyz,
                    IgrfOutput::Angles => EarthMagneticOutput::Angles,
                    IgrfOutput::Length => EarthMagneticOutput::Length,
                    IgrfOutput::Los | IgrfOutput::Long => unreachable!(),
                },
            ))
        }
    }
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
pub(super) fn meas_pos(
    args: &[ExprValue],
    fn_name: &str,
    measures: MeasuresRef<'_>,
) -> Result<ExprValue, TaqlError> {
    check_arity_range(fn_name, args, 2, 5)?;
    if any_null(args) {
        return Ok(ExprValue::Null);
    }
    let target: PositionRef = parse_ref(&args[0], fn_name)?;
    let pos = parse_position_input(&args[1..], fn_name, measures)?;
    let converted = pos
        .convert_to(target)
        .map_err(|e| measure_err(fn_name, e))?;
    Ok(make_array_result(&converted.values()))
}

/// Position extraction helpers such as `meas.itrfllh` and `meas.wgsll`.
pub(super) fn meas_pos_extract(
    args: &[ExprValue],
    target: PositionRef,
    output: PositionOutput,
    fn_name: &str,
    measures: MeasuresRef<'_>,
) -> Result<ExprValue, TaqlError> {
    if !(args.len() == 1 || args.len() == 3 || args.len() == 4) {
        return Err(TaqlError::ArgumentCount {
            name: fn_name.to_string(),
            expected: "1 or 3..4".to_string(),
            got: args.len(),
        });
    }
    if any_null(args) {
        return Ok(ExprValue::Null);
    }
    let input = parse_position_input(args, fn_name, measures)?;
    let converted = input
        .convert_to(target)
        .map_err(|e| measure_err(fn_name, e))?;
    match (target, output) {
        (PositionRef::ITRF, PositionOutput::Xyz) => Ok(make_array_result(&converted.as_itrf())),
        (PositionRef::ITRF, PositionOutput::LonLat) => {
            let (lon, lat, _radius) = converted.as_spherical();
            Ok(make_array2_result(lon, lat))
        }
        (PositionRef::ITRF, PositionOutput::Height) => {
            let (_lon, _lat, radius) = converted.as_spherical();
            Ok(ExprValue::Float(radius))
        }
        (PositionRef::ITRF, PositionOutput::LonLatHeight) => {
            let (lon, lat, radius) = converted.as_spherical();
            Ok(make_array_result(&[lon, lat, radius]))
        }
        (PositionRef::WGS84, PositionOutput::Xyz) => {
            Ok(make_array_result(&casacore_wgs_xyz_values(&converted)))
        }
        (PositionRef::WGS84, PositionOutput::LonLat) => {
            let vals = converted.values();
            Ok(make_array2_result(vals[0], vals[1]))
        }
        (PositionRef::WGS84, PositionOutput::Height) => Ok(ExprValue::Float(converted.values()[2])),
        (PositionRef::WGS84, PositionOutput::LonLatHeight) => {
            Ok(make_array_result(&converted.values()))
        }
    }
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
pub(super) fn meas_freq(
    args: &[ExprValue],
    fn_name: &str,
    measures: MeasuresRef<'_>,
) -> Result<ExprValue, TaqlError> {
    check_arity_range(fn_name, args, 2, 11)?;
    if any_null(args) {
        return Ok(ExprValue::Null);
    }
    let target: FrequencyRef = parse_ref(&args[0], fn_name)?;
    let hz = expr_to_frequency_hz(&args[1], fn_name)?;
    let src: FrequencyRef = if args.len() >= 3 {
        parse_ref(&args[2], fn_name)?
    } else {
        FrequencyRef::LSRK
    };
    let frame = if target == FrequencyRef::REST || src == FrequencyRef::REST {
        if args.len() < 5 {
            return Err(TaqlError::ArgumentCount {
                name: fn_name.to_string(),
                expected: "5..11".to_string(),
                got: args.len(),
            });
        }
        let rv = parse_radvel_input(&args[3], &args[4], fn_name)?;
        let mut frame = if args.len() > 5 {
            build_frame_with_dir_epoch_pos(&args[5..], measures)?
        } else {
            meas_frame(measures)
        };
        frame = frame.with_radial_velocity(rv);
        frame
    } else if args.len() > 3 {
        build_frame_with_dir_epoch_pos(&args[3..], measures)?
    } else {
        meas_frame(measures)
    };
    let freq = MFrequency::new(hz, src);
    let converted = freq
        .convert_to(target, &frame)
        .map_err(|e| measure_err(fn_name, e))?;
    Ok(ExprValue::Float(converted.hz()))
}

/// `meas.rest(hz, src_ref, doppler [, doppler_ref])`
/// or `meas.rest(hz, src_ref, rv_ms, rv_ref [, dir_lon, dir_lat, epoch, px, py, pz])`.
pub(super) fn meas_rest(
    args: &[ExprValue],
    fn_name: &str,
    measures: MeasuresRef<'_>,
) -> Result<ExprValue, TaqlError> {
    check_arity_range(fn_name, args, 3, 10)?;
    if any_null(args) {
        return Ok(ExprValue::Null);
    }
    let hz = expr_to_frequency_hz(&args[0], fn_name)?;
    let src: FrequencyRef = parse_ref(&args[1], fn_name)?;
    if src == FrequencyRef::REST {
        return Err(TaqlError::TypeError {
            message: format!("{fn_name}: source frequency cannot already be REST"),
        });
    }

    if args.len() == 3 {
        let doppler = parse_doppler_input(&args[2], None, fn_name)?;
        return Ok(ExprValue::Float(doppler.rest_frequency_hz(hz)));
    }

    if let Ok(doppler_ref) = parse_ref::<DopplerRef>(&args[3], fn_name) {
        if args.len() != 4 {
            return Err(TaqlError::ArgumentCount {
                name: fn_name.to_string(),
                expected: "3..4 or 4..10".to_string(),
                got: args.len(),
            });
        }
        let doppler = MDoppler::new(args[2].to_float()?, doppler_ref);
        return Ok(ExprValue::Float(doppler.rest_frequency_hz(hz)));
    }

    let rv = parse_radvel_input(&args[2], &args[3], fn_name)?;
    let mut frame = if args.len() > 4 {
        build_frame_with_dir_epoch_pos(&args[4..], measures)?
    } else {
        meas_frame(measures)
    };
    frame = frame.with_radial_velocity(rv);

    let converted = MFrequency::new(hz, src)
        .convert_to(FrequencyRef::REST, &frame)
        .map_err(|e| measure_err(fn_name, e))?;
    Ok(ExprValue::Float(converted.hz()))
}

/// `meas.shift(hz, src_ref, doppler [, doppler_ref])` — apply a Doppler shift.
pub(super) fn meas_shift(
    args: &[ExprValue],
    fn_name: &str,
    _measures: MeasuresRef<'_>,
) -> Result<ExprValue, TaqlError> {
    check_arity_range(fn_name, args, 3, 4)?;
    if any_null(args) {
        return Ok(ExprValue::Null);
    }
    let hz = expr_to_frequency_hz(&args[0], fn_name)?;
    let _src: FrequencyRef = parse_ref(&args[1], fn_name)?;
    let doppler = parse_doppler_input(&args[2], args.get(3), fn_name)?;
    Ok(ExprValue::Float(doppler.shift_frequency_hz(hz)))
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
pub(super) fn meas_doppler(args: &[ExprValue], fn_name: &str) -> Result<ExprValue, TaqlError> {
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
pub(super) fn meas_radvel(
    args: &[ExprValue],
    fn_name: &str,
    measures: MeasuresRef<'_>,
) -> Result<ExprValue, TaqlError> {
    check_arity_range(fn_name, args, 2, 9)?;
    if any_null(args) {
        return Ok(ExprValue::Null);
    }
    let target: RadialVelocityRef = parse_ref(&args[0], fn_name)?;
    let ms = expr_to_radial_velocity_ms(&args[1], fn_name)?;
    let src: RadialVelocityRef = if args.len() >= 3 {
        parse_ref(&args[2], fn_name)?
    } else {
        RadialVelocityRef::LSRK
    };
    let frame = if args.len() > 3 {
        build_frame_with_dir_epoch_pos(&args[3..], measures)?
    } else {
        meas_frame(measures)
    };
    let rv = MRadialVelocity::new(ms, src);
    let converted = rv
        .convert_to(target, &frame)
        .map_err(|e| measure_err(fn_name, e))?;
    Ok(ExprValue::Float(converted.ms()))
}
