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
//! | `meas.last` / `meas.lst` | Local apparent sidereal time (seconds-of-day) |
//! | `meas.help` | Help/introspection text for the `meas.*` TaQL surface |
//! | `meas.dir` / `meas.direction` | Sky direction conversion |
//! | `meas.dircos` / `meas.directioncosine` | Direction conversion returning cosines |
//! | `meas.pos` / `meas.position` | Position conversion |
//! | `meas.itrfxyz` / `meas.itrfll` / `meas.itrfh` / `meas.itrfllh` | ITRF position extractors |
//! | `meas.wgs` / `meas.wgsxyz` | WGS84 raw-value extractor |
//! | `meas.wgsll` / `meas.wgsh` / `meas.wgsllh` | WGS84 position extractors |
//! | `meas.freq` / `meas.frequency` | Spectral frequency conversion |
//! | `meas.rest` / `meas.restfreq` / `meas.restfrequency` | Rest-frequency helpers |
//! | `meas.shift` / `meas.shiftfreq` / `meas.shiftfrequency` | Doppler shift helpers |
//! | `meas.doppler` / `meas.redshift` | Doppler convention conversion |
//! | `meas.radvel` / `meas.radialvelocity` | Radial velocity conversion |
//! | `meas.hadec` / `meas.azel` / `meas.app` | Direction shortcut conversions |
//! | `meas.j2000` | Shortcut: direction → J2000 |
//! | `meas.galactic` | Shortcut: direction → GALACTIC |
//! | `meas.b1950` | Shortcut: direction → B1950 |
//! | `meas.ecl` / `meas.ecliptic` | Shortcut: direction → ECLIPTIC |
//! | `meas.gal` | Shortcut: direction → GALACTIC |
//! | `meas.sgal` / `meas.supergal` / `meas.supergalactic` | Shortcut: direction → SUPERGAL |
//!
//! # C++ reference
//!
//! `meas/MeasUDF/Register.cc`, `EpochUDF.cc`, `DirectionUDF.cc`, etc.

use std::str::FromStr;

use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::doppler::{DopplerRef, MDoppler};
use casa_types::measures::epoch::{EpochRef, MEpoch};
use casa_types::measures::frame::MeasFrame;
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::measures::position::{MPosition, PositionRef};
use casa_types::measures::radial_velocity::{MRadialVelocity, RadialVelocityRef};

use super::error::TaqlError;
use super::eval::ExprValue;

const SECONDS_PER_DAY: f64 = 86_400.0;
const MEAS_HELP_URL: &str =
    "See also section 'Special Measures functions' at http://casacore.github.io/casacore-notes/199.html";

/// Dispatch a `meas.*` function call.
///
/// Called from `call_function()` when the function name starts with `"meas."`.
pub(crate) fn call_meas_function(name: &str, args: &[ExprValue]) -> Result<ExprValue, TaqlError> {
    let suffix = &name[5..]; // strip "meas."
    match suffix {
        "epoch" => meas_epoch(args, name),
        "last" | "lst" => meas_last(args, name),
        "help" => meas_help(args, name),
        "dir" | "direction" => meas_dir(args, name),
        "dircos" | "directioncosine" => meas_dircos(args, name),
        "pos" | "position" => meas_pos(args, name),
        "itrfxyz" => meas_pos_extract(args, PositionRef::ITRF, PositionOutput::Xyz, name),
        "itrfll" | "itrflonlat" => {
            meas_pos_extract(args, PositionRef::ITRF, PositionOutput::LonLat, name)
        }
        "itrfh" | "itrfheight" => {
            meas_pos_extract(args, PositionRef::ITRF, PositionOutput::Height, name)
        }
        "itrfllh" => meas_pos_extract(args, PositionRef::ITRF, PositionOutput::LonLatHeight, name),
        "wgs" | "wgsxyz" => meas_pos_extract(args, PositionRef::WGS84, PositionOutput::Xyz, name),
        "wgsll" | "wgslonlat" => {
            meas_pos_extract(args, PositionRef::WGS84, PositionOutput::LonLat, name)
        }
        "wgsh" | "wgsheight" => {
            meas_pos_extract(args, PositionRef::WGS84, PositionOutput::Height, name)
        }
        "wgsllh" => meas_pos_extract(args, PositionRef::WGS84, PositionOutput::LonLatHeight, name),
        "freq" | "frequency" => meas_freq(args, name),
        "rest" | "restfreq" | "restfrequency" => meas_rest(args, name),
        "shift" | "shiftfreq" | "shiftfrequency" => meas_shift(args, name),
        "doppler" | "redshift" => meas_doppler(args, name),
        "radvel" | "radialvelocity" => meas_radvel(args, name),
        "hadec" => meas_dir_shortcut(args, "HADEC", name),
        "azel" => meas_dir_shortcut(args, "AZEL", name),
        "app" | "apparent" => meas_dir_shortcut(args, "APP", name),
        "j2000" => meas_dir_shortcut(args, "J2000", name),
        "galactic" => meas_dir_shortcut(args, "GALACTIC", name),
        "b1950" => meas_dir_shortcut(args, "B1950", name),
        "ecl" | "ecliptic" => meas_dir_shortcut(args, "ECLIPTIC", name),
        "gal" => meas_dir_shortcut(args, "GALACTIC", name),
        "sgal" | "supergal" | "supergalactic" => meas_dir_shortcut(args, "SUPERGAL", name),
        "itrfd" | "itrfdir" | "itrfdirection" => meas_dir_shortcut(args, "ITRF", name),
        _ => Err(TaqlError::UnknownFunction {
            name: name.to_string(),
        }),
    }
}

#[derive(Clone, Copy)]
enum PositionOutput {
    Xyz,
    LonLat,
    Height,
    LonLatHeight,
}

fn meas_help(args: &[ExprValue], fn_name: &str) -> Result<ExprValue, TaqlError> {
    check_arity_range(fn_name, args, 0, 1)?;
    if any_null(args) {
        return Ok(ExprValue::Null);
    }

    let topic = match args {
        [] => None,
        [ExprValue::String(topic)] => Some(topic.to_ascii_lowercase()),
        [other] => {
            return Err(TaqlError::TypeError {
                message: format!(
                    "{fn_name}: expected optional string subtype, got {}",
                    other.type_name()
                ),
            });
        }
        _ => unreachable!("arity checked above"),
    };

    Ok(ExprValue::String(render_meas_help(topic.as_deref())))
}

fn render_meas_help(topic: Option<&str>) -> String {
    match topic {
        None | Some("") => with_help_link(
            [
                help_position(false),
                help_epoch(false),
                help_direction(false),
                help_earth_magnetic(false),
                help_frequency(false),
                help_radial_velocity(false),
                help_doppler(false),
            ]
            .join("\n\n"),
        ),
        Some("position") | Some("pos") => with_help_link(help_position(true)),
        Some("epoch") => with_help_link(help_epoch(true)),
        Some("direction") | Some("dir") => with_help_link(help_direction(true)),
        Some("earthmagnetic") | Some("em") => with_help_link(help_earth_magnetic(true)),
        Some("frequency") | Some("freq") => with_help_link(help_frequency(true)),
        Some("radialvelocity") | Some("radvel") | Some("rv") => {
            with_help_link(help_radial_velocity(true))
        }
        Some("doppler") => with_help_link(help_doppler(true)),
        Some(other) => format!(
            "{other} is an unknown meas subtype; use pos(ition), epoch, dir(ection), \
earthmagnetic (em), freq(uency) or radialvelocity (radvel)\n"
        ),
    }
}

fn with_help_link(mut section: String) -> String {
    section.push('\n');
    section.push_str(MEAS_HELP_URL);
    section.push('\n');
    section
}

fn help_position(show_types: bool) -> String {
    let mut out = String::from(
        "Position conversion functions:\n\
  MEAS.POS (type, position)                      convert to given type\n\
       POSITION is a synonym for POS\n\
  MEAS.ITRFXYZ (position)                        convert to ITRF XYZ coord\n\
  MEAS.ITRFLL (position)                         convert to ITRF LonLat\n\
       ITRFLONLAT is a synonym for ITRFLL\n\
  MEAS.ITRFH (position)                          convert to ITRF height\n\
       ITRFHEIGHT is a synonym for ITRFH\n\
  MEAS.ITRFLLH (position)                        convert to ITRF LonLatHeight\n\
  MEAS.WGS (position)                            convert to WGS84 XYZ coord\n\
       WGSXYZ is a synonym for WGS\n\
  MEAS.WGSLL (position)                          convert to WGS84 LonLat\n\
       WGSLONLAT is a synonym for WGSLL\n\
  MEAS.WGSH (position)                           convert to WGS84 height\n\
       WGSHEIGHT is a synonym for WGSH\n\
  MEAS.WGSLLH (position)                         convert to WGS84 LonLatHeight",
    );
    if show_types {
        out.push_str(
            "\n\nKnown observatory positions (names are case-insensitive):\n\
  Names from the bundled/runtime observatory catalog, for example ALMA and VLA.\n\n",
        );
        append_known_types(
            &mut out,
            "Known position types:",
            &[PositionRef::ITRF.as_str(), PositionRef::WGS84.as_str()],
        );
    }
    out
}

fn help_epoch(show_types: bool) -> String {
    let mut out = String::from(
        "Epoch conversion functions:\n\
  MEAS.EPOCH (type, epoch [,position])           convert to given type\n\
  MEAS.LAST (epoch, position)                    convert to local sidereal time\n\
       LST is a synonym for LAST",
    );
    if show_types {
        out.push_str("\n\n");
        let epoch_types: Vec<&str> = EpochRef::ALL.iter().map(|r| r.as_str()).collect();
        append_known_types(&mut out, "Known epoch types:", &epoch_types);
    }
    out
}

fn help_direction(show_types: bool) -> String {
    let mut out = String::from(
        "Direction conversion functions:\n\
  MEAS.DIR (type, direction [,epoch, position])  convert to given type\n\
       DIRECTION is a synonym for DIR\n\
  MEAS.HADEC (direction, epoch, position)        convert to Hourangle/Decl\n\
  MEAS.AZEL (direction, epoch, position)         convert to Azimuth/Elevation\n\
  MEAS.APP (direction, epoch, position)          convert to apparent\n\
       APPARENT is a synonym for APP\n\
  MEAS.J2000 (direction [,epoch, position])      convert to J2000\n\
  MEAS.B1950 (direction [,epoch, position])      convert to B1950\n\
  MEAS.ECL (direction [,epoch, position])        convert to Ecliptic\n\
       ECLIPTIC is a synonym for ECL\n\
  MEAS.GAL (direction [,epoch, position])        convert to Galactic\n\
       GALACTIC is a synonym for GAL\n\
  MEAS.SGAL (direction [,epoch, position])       convert to Supergalactic\n\
       SUPERGAL is a synonym for SGAL\n\
       SUPERGALACTIC is a synonym for SGAL\n\
  MEAS.ITRFD (direction [,epoch, position])      convert to ITRF\n\
       ITRFDIR is a synonym for ITRFD\n\
       ITRFDIRECTION is a synonym for ITRFD\n\
  MEAS.DIRCOS (type, direction [,epoch, position])\n\
       as DIR returning 3 direction cosines instead of 2 angles\n\
       DIRECTIONCOSINE is a synonym for DIRCOS\n\
  MEAS.RISET (direction, epoch, position)        not yet implemented in Rust TaQL\n\
       RISESET is a synonym for RISET",
    );
    if show_types {
        out.push_str(
            "\n\nKnown source directions (names are case-insensitive):\n\
  All sources in the bundled/runtime measures sources catalog\n\
  SUN   MOON  MERCURY  VENUS  MARS  JUPITER  SATURN  URANUS  NEPTUNE  PLUTO\n\
  CasA  CygA  HerA     HydA   PerA  TauA     VirA\n\
 In function RISET type SUN can have a suffix -XX where XX can be (default -UR):\n\
   C    center touches horizon             CR  center with refraction\n\
   U    upper edge touches horizon         UR  upper edge with refraction\n\
   L    lower edge touches horizon         LR  lower edge with refraction\n\
   CT   civil twilight darkness (-6 deg)   NT  nautical twilight darkness (-12)\n\
   AT   amateur astronomy twilight (-15)   ST  scientific astronomy twilight (-18)\n\
 The first 6 suffixes can also be used with MOON.\n\n",
        );
        let direction_types: Vec<&str> = DirectionRef::ALL.iter().map(|r| r.as_str()).collect();
        append_known_types(&mut out, "Known direction types:", &direction_types);
    }
    out
}

fn help_earth_magnetic(show_types: bool) -> String {
    let mut out = String::from(
        "EarthMagnetic conversion functions:\n\
  MEAS.EM (type, em, epoch, position)            planned, not yet implemented in Rust TaQL\n\
       EARTHMAGNETIC and EMXYZ are synonyms for EM\n\
  MEAS.EMANG (type, em, epoch, position)         planned, not yet implemented in Rust TaQL\n\
       EMANGLES is a synonym for EMANG\n\
  MEAS.EMLEN (type, em, epoch, position)         planned, not yet implemented in Rust TaQL\n\
       EMLENGTH is a synonym for EMLEN\n\
  MEAS.IGRF (type, height, direction, epoch, position)\n\
       planned, not yet implemented in Rust TaQL\n\
       IGRFXYZ is a synonym for IGRF\n\
  MEAS.IGRFANG (t, h, d, e, p)                   planned, not yet implemented in Rust TaQL\n\
       IGRFANGLES is a synonym for IGRFANG\n\
  MEAS.IGRFLEN (t, h, d, e, p)                   planned, not yet implemented in Rust TaQL\n\
       IGRFLENGTH is a synonym for IGRFLEN\n\
  MEAS.IGRFLOS (h, d, e, p)                      planned, not yet implemented in Rust TaQL\n\
  MEAS.IGRFLONG (h, d, e, p)                     planned, not yet implemented in Rust TaQL",
    );
    if show_types {
        out.push_str(
            "\n\nEarth-magnetic and IGRF helpers remain deferred in the Rust measures layer.\n\
Their names are listed here for CASA/casacore discoverability, but they do not evaluate yet.",
        );
    }
    out
}

fn help_frequency(show_types: bool) -> String {
    let mut out = String::from(
        "Frequency conversion functions:\n\
  MEAS.FREQ (type, freq, radvel, direction, epoch, position)   convert to given type\n\
           Instead of freq, a period or wavelength can be given (requires a unit)\n\
           radvel is only needed when converting to/from rest frequencies\n\
       FREQUENCY is a synonym for FREQ\n\
  MEAS.REST (freq, radvel, direction, epoch, position)         convert to rest freq\n\
  MEAS.REST (freq, doppler)                                    convert to rest freq\n\
       RESTFREQ and RESTFREQUENCY are synonyms for REST\n\
  MEAS.SHIFTFREQ (freq, doppler)                               shift frequencies\n\
       SHIFT and SHIFTFREQUENCY are synonyms for SHIFTFREQ\n\
       It can also be used to shift rest frequencies",
    );
    if show_types {
        out.push_str("\n\n");
        let frequency_types: Vec<&str> = FrequencyRef::ALL.iter().map(|r| r.as_str()).collect();
        append_known_types(&mut out, "Known frequency types:", &frequency_types);
    }
    out
}

fn help_radial_velocity(show_types: bool) -> String {
    let mut out = String::from(
        "RadialVelocity conversion functions:\n\
  MEAS.RADVEL (type, radvel, direction, epoch, position)    convert to given type\n\
  MEAS.RADVEL (type, doppler)                               calc from doppler\n\
       RV and RADIALVELOCITY are synonyms for RADVEL",
    );
    if show_types {
        out.push_str("\n\n");
        let rv_types: Vec<&str> = RadialVelocityRef::ALL.iter().map(|r| r.as_str()).collect();
        append_known_types(&mut out, "Known radial-velocity types:", &rv_types);
    }
    out
}

fn help_doppler(show_types: bool) -> String {
    let mut out = String::from(
        "Doppler conversion functions:\n\
  MEAS.DOPPLER (type, doppler)               convert to given type\n\
  MEAS.DOPPLER (type, radvel)                calc from radial velocity\n\
  MEAS.DOPPLER (type, freq, restfreq)        calc from frequency\n\
       REDSHIFT is a synonym for DOPPLER",
    );
    if show_types {
        out.push_str("\n\n");
        let doppler_types: Vec<&str> = DopplerRef::ALL.iter().map(|r| r.as_str()).collect();
        append_known_types(&mut out, "Known doppler types:", &doppler_types);
    }
    out
}

fn append_known_types(out: &mut String, header: &str, types: &[&str]) {
    out.push_str(header);
    out.push('\n');
    out.push_str("  ");
    out.push_str(&types.join("  "));
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
fn measure_err(fn_name: &str, e: casa_types::measures::MeasureError) -> TaqlError {
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
        frame = frame
            .with_epoch(MEpoch::from_mjd(epoch_mjd, EpochRef::UTC))
            .with_bundled_eop();
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
        frame = frame
            .with_epoch(MEpoch::from_mjd(epoch_mjd, EpochRef::UTC))
            .with_bundled_eop();
    }
    if extra.len() >= 6 {
        let px = extra[3].to_float()?;
        let py = extra[4].to_float()?;
        let pz = extra[5].to_float()?;
        frame = frame.with_position(MPosition::new_itrf(px, py, pz));
    }
    Ok(frame)
}

fn parse_position_input(args: &[ExprValue], fn_name: &str) -> Result<MPosition, TaqlError> {
    match args {
        [name] => {
            let observatory = name.to_string_val()?;
            MPosition::from_observatory_name(&observatory).ok_or(TaqlError::TypeError {
                message: format!("{fn_name}: unknown observatory \"{observatory}\""),
            })
        }
        [x, y, z] => Ok(MPosition::new_itrf(
            x.to_float()?,
            y.to_float()?,
            z.to_float()?,
        )),
        [x, y, z, src_ref] => {
            let src: PositionRef = parse_ref(src_ref, fn_name)?;
            let x = x.to_float()?;
            let y = y.to_float()?;
            let z = z.to_float()?;
            Ok(match src {
                PositionRef::ITRF => MPosition::new_itrf(x, y, z),
                PositionRef::WGS84 => MPosition::new_wgs84(x, y, z),
            })
        }
        _ => Err(TaqlError::ArgumentCount {
            name: fn_name.to_string(),
            expected: "1 or 3..4".to_string(),
            got: args.len(),
        }),
    }
}

fn parse_doppler_input(
    value_arg: &ExprValue,
    ref_arg: Option<&ExprValue>,
    fn_name: &str,
) -> Result<MDoppler, TaqlError> {
    let value = value_arg.to_float()?;
    let refer = if let Some(arg) = ref_arg {
        parse_ref::<DopplerRef>(arg, fn_name)?
    } else {
        DopplerRef::RADIO
    };
    Ok(MDoppler::new(value, refer))
}

fn parse_radvel_input(
    value_arg: &ExprValue,
    ref_arg: &ExprValue,
    fn_name: &str,
) -> Result<MRadialVelocity, TaqlError> {
    Ok(MRadialVelocity::new(
        value_arg.to_float()?,
        parse_ref::<RadialVelocityRef>(ref_arg, fn_name)?,
    ))
}

fn casacore_mvposition_length(length_m: f64) -> f64 {
    let mut adjusted = length_m;
    if adjusted < 0.0 && adjusted > -7.0e6 {
        adjusted = adjusted / 1.0e7 + 743.569;
    } else if adjusted > 743.568 && adjusted < 743.569 {
        adjusted += 0.001;
    }
    if adjusted == 0.0 { 1.0e-6 } else { adjusted }
}

/// casacore `meas.wgs` / `meas.wgsxyz` return `MVPosition::getValue()` after
/// conversion to `WGS84`, not the cleaner lon/lat/height triplet.
fn casacore_wgs_xyz_values(position: &MPosition) -> [f64; 3] {
    let converted = if position.refer() == PositionRef::WGS84 {
        position.clone()
    } else {
        position
            .convert_to(PositionRef::WGS84)
            .expect("conversion to WGS84 should already have succeeded")
    };
    let [lon, lat, height] = converted.values();
    let scale = casacore_mvposition_length(height);
    let clat = lat.cos();
    [
        lon.cos() * clat * scale,
        lon.sin() * clat * scale,
        lat.sin() * scale,
    ]
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
fn meas_last(args: &[ExprValue], fn_name: &str) -> Result<ExprValue, TaqlError> {
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
    let mjd = args[0].to_float()?;
    let position = parse_position_input(&args[1..], fn_name)?;
    let epoch = MEpoch::from_mjd(mjd, EpochRef::UTC);
    let frame = MeasFrame::new()
        .with_position(position)
        .with_bundled_eop()
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

/// `meas.dircos(target_ref, lon, lat [, src_ref [, epoch, px, py, pz]])`
/// returns direction cosines in the target frame.
fn meas_dircos(args: &[ExprValue], fn_name: &str) -> Result<ExprValue, TaqlError> {
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
    Ok(make_array_result(&converted.cosines()))
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
    check_arity_range(fn_name, args, 2, 5)?;
    if any_null(args) {
        return Ok(ExprValue::Null);
    }
    let target: PositionRef = parse_ref(&args[0], fn_name)?;
    let pos = parse_position_input(&args[1..], fn_name)?;
    let converted = pos
        .convert_to(target)
        .map_err(|e| measure_err(fn_name, e))?;
    Ok(make_array_result(&converted.values()))
}

/// Position extraction helpers such as `meas.itrfllh` and `meas.wgsll`.
fn meas_pos_extract(
    args: &[ExprValue],
    target: PositionRef,
    output: PositionOutput,
    fn_name: &str,
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
    let input = parse_position_input(args, fn_name)?;
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
fn meas_freq(args: &[ExprValue], fn_name: &str) -> Result<ExprValue, TaqlError> {
    check_arity_range(fn_name, args, 2, 11)?;
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
            build_frame_with_dir_epoch_pos(&args[5..])?
        } else {
            MeasFrame::new()
        };
        frame = frame.with_radial_velocity(rv);
        frame
    } else if args.len() > 3 {
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

/// `meas.rest(hz, src_ref, doppler [, doppler_ref])`
/// or `meas.rest(hz, src_ref, rv_ms, rv_ref [, dir_lon, dir_lat, epoch, px, py, pz])`.
fn meas_rest(args: &[ExprValue], fn_name: &str) -> Result<ExprValue, TaqlError> {
    check_arity_range(fn_name, args, 3, 10)?;
    if any_null(args) {
        return Ok(ExprValue::Null);
    }
    let hz = args[0].to_float()?;
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
        build_frame_with_dir_epoch_pos(&args[4..])?
    } else {
        MeasFrame::new()
    };
    frame = frame.with_radial_velocity(rv);

    let converted = MFrequency::new(hz, src)
        .convert_to(FrequencyRef::REST, &frame)
        .map_err(|e| measure_err(fn_name, e))?;
    Ok(ExprValue::Float(converted.hz()))
}

/// `meas.shift(hz, src_ref, doppler [, doppler_ref])` — apply a Doppler shift.
fn meas_shift(args: &[ExprValue], fn_name: &str) -> Result<ExprValue, TaqlError> {
    check_arity_range(fn_name, args, 3, 4)?;
    if any_null(args) {
        return Ok(ExprValue::Null);
    }
    let hz = args[0].to_float()?;
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
    make_array2_result(dir.longitude_rad(), dir.latitude_rad())
}

fn make_array2_result(v0: f64, v1: f64) -> ExprValue {
    make_array_result(&[v0, v1])
}

fn make_array_result(values: &[f64]) -> ExprValue {
    ExprValue::Array(super::eval::ArrayValue {
        shape: vec![values.len()],
        data: values.iter().copied().map(ExprValue::Float).collect(),
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

    #[test]
    fn last_with_position() {
        let epoch = MEpoch::from_mjd(50217.625, EpochRef::UTC);
        let frame = MeasFrame::new()
            .with_position(MPosition::new_wgs84(
                6.60417_f64.to_radians(),
                52.8_f64.to_radians(),
                10.0,
            ))
            .with_bundled_eop();
        let expected = epoch
            .convert_to(EpochRef::LAST, &frame)
            .unwrap()
            .value()
            .frac()
            * SECONDS_PER_DAY;

        let result = call_meas_function(
            "meas.last",
            &[
                f(50217.625),
                f(6.60417_f64.to_radians()),
                f(52.8_f64.to_radians()),
                f(10.0),
                s("WGS84"),
            ],
        )
        .unwrap();
        let actual = match result {
            ExprValue::Float(v) => v,
            other => panic!("expected Float, got {other:?}"),
        };
        assert!((actual - expected).abs() < 1e-6);
    }

    #[test]
    fn lst_alias() {
        let result = call_meas_function(
            "meas.lst",
            &[
                f(50217.625),
                f(6.60417_f64.to_radians()),
                f(52.8_f64.to_radians()),
                f(10.0),
                s("WGS84"),
            ],
        )
        .unwrap();
        assert!(matches!(result, ExprValue::Float(_)));
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
    fn dircos_returns_unit_vector() {
        let lon = 1.0;
        let lat = 0.5;
        let result =
            call_meas_function("meas.dircos", &[s("J2000"), f(lon), f(lat), s("J2000")]).unwrap();
        let cosines = extract_array3(&result);
        let clat = lat.cos();
        let expected = [lon.cos() * clat, lon.sin() * clat, lat.sin()];
        for i in 0..3 {
            assert!((cosines[i] - expected[i]).abs() < 1e-12);
        }
    }

    #[test]
    fn app_hadec_and_azel_shortcuts_work() {
        let pos = MPosition::new_wgs84(-1.878_283_2, 0.595_370_3, 2124.0)
            .convert_to(PositionRef::ITRF)
            .unwrap();
        let [px, py, pz] = pos.values();
        let args = &[
            f(0.185_948_8),
            f(0.722_777_4),
            s("J2000"),
            f(51544.5),
            f(px),
            f(py),
            f(pz),
        ];
        let _ = extract_dir(&call_meas_function("meas.app", args).unwrap());
        let _ = extract_dir(&call_meas_function("meas.hadec", args).unwrap());
        let _ = extract_dir(&call_meas_function("meas.azel", args).unwrap());
    }

    #[test]
    fn ecliptic_and_supergalactic_aliases_work() {
        let _ = extract_dir(&call_meas_function("meas.ecl", &[f(1.0), f(0.5)]).unwrap());
        let _ = extract_dir(&call_meas_function("meas.ecliptic", &[f(1.0), f(0.5)]).unwrap());
        let _ = extract_dir(&call_meas_function("meas.gal", &[f(1.0), f(0.5)]).unwrap());
        let _ = extract_dir(&call_meas_function("meas.sgal", &[f(1.0), f(0.5)]).unwrap());
        let _ = extract_dir(&call_meas_function("meas.supergal", &[f(1.0), f(0.5)]).unwrap());
        let _ = extract_dir(&call_meas_function("meas.supergalactic", &[f(1.0), f(0.5)]).unwrap());
    }

    #[test]
    fn itrf_direction_aliases_work() {
        let pos = MPosition::new_wgs84(-1.878_283_2, 0.595_370_3, 2124.0)
            .convert_to(PositionRef::ITRF)
            .unwrap();
        let [px, py, pz] = pos.values();
        let args = &[
            f(0.185_948_8),
            f(0.722_777_4),
            s("J2000"),
            f(51544.5),
            f(px),
            f(py),
            f(pz),
        ];
        let _ = extract_dir(&call_meas_function("meas.itrfd", args).unwrap());
        let _ = extract_dir(&call_meas_function("meas.itrfdir", args).unwrap());
        let _ = extract_dir(&call_meas_function("meas.itrfdirection", args).unwrap());
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

    #[test]
    fn itrf_extractors_match_spherical_record_components() {
        let source = MPosition::new_wgs84(-1.879, 0.595, 2100.0)
            .convert_to(PositionRef::ITRF)
            .unwrap();
        let xyz = source.values();
        let expected = source.as_spherical();

        let ll = call_meas_function("meas.itrfll", &[f(xyz[0]), f(xyz[1]), f(xyz[2]), s("ITRF")])
            .unwrap();
        let h = call_meas_function("meas.itrfh", &[f(xyz[0]), f(xyz[1]), f(xyz[2]), s("ITRF")])
            .unwrap();
        let llh = call_meas_function(
            "meas.itrfllh",
            &[f(xyz[0]), f(xyz[1]), f(xyz[2]), s("ITRF")],
        )
        .unwrap();

        let ll_vals = extract_array2(&ll);
        let h_val = match h {
            ExprValue::Float(v) => v,
            other => panic!("expected Float, got {other:?}"),
        };
        let llh_vals = extract_array3(&llh);

        assert!((ll_vals[0] - expected.0).abs() < 1e-12);
        assert!((ll_vals[1] - expected.1).abs() < 1e-12);
        assert!((h_val - expected.2).abs() < 1e-6);
        assert!((llh_vals[0] - expected.0).abs() < 1e-12);
        assert!((llh_vals[1] - expected.1).abs() < 1e-12);
        assert!((llh_vals[2] - expected.2).abs() < 1e-6);
    }

    #[test]
    fn wgs_extractors_match_meas_pos() {
        let result = call_meas_function(
            "meas.wgsllh",
            &[f(-1601185.0), f(-5041977.0), f(3554876.0), s("ITRF")],
        )
        .unwrap();
        let ll = call_meas_function(
            "meas.wgsll",
            &[f(-1601185.0), f(-5041977.0), f(3554876.0), s("ITRF")],
        )
        .unwrap();
        let h = call_meas_function(
            "meas.wgsh",
            &[f(-1601185.0), f(-5041977.0), f(3554876.0), s("ITRF")],
        )
        .unwrap();
        let expected = extract_array3(
            &call_meas_function(
                "meas.pos",
                &[
                    s("WGS84"),
                    f(-1601185.0),
                    f(-5041977.0),
                    f(3554876.0),
                    s("ITRF"),
                ],
            )
            .unwrap(),
        );
        let ll_vals = extract_array2(&ll);
        let h_val = match h {
            ExprValue::Float(v) => v,
            other => panic!("expected Float, got {other:?}"),
        };
        let llh_vals = extract_array3(&result);
        assert!((ll_vals[0] - expected[0]).abs() < 1e-12);
        assert!((ll_vals[1] - expected[1]).abs() < 1e-12);
        assert!((h_val - expected[2]).abs() < 1e-6);
        assert_eq!(llh_vals, expected);
    }

    #[test]
    fn wgsxyz_alias_matches_casacore_raw_value_convention() {
        let lon = -1.879;
        let lat = 0.595;
        let height = 2100.0;
        let expected = casacore_wgs_xyz_values(&MPosition::new_wgs84(lon, lat, height));
        let result =
            call_meas_function("meas.wgsxyz", &[f(lon), f(lat), f(height), s("WGS84")]).unwrap();
        let alias =
            call_meas_function("meas.wgs", &[f(lon), f(lat), f(height), s("WGS84")]).unwrap();
        let vals = extract_array3(&result);
        let alias_vals = extract_array3(&alias);
        for i in 0..3 {
            assert!((vals[i] - expected[i]).abs() < 1e-12);
            assert!((alias_vals[i] - expected[i]).abs() < 1e-12);
        }
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

    #[test]
    fn freq_rest_with_radvel_matches_rest_helper() {
        let rest = call_meas_function(
            "meas.rest",
            &[f(1.4e9), s("LSRK"), f(50_000.0), s("LSRK"), f(1.0), f(0.5)],
        )
        .unwrap();
        let via_freq = call_meas_function(
            "meas.freq",
            &[
                s("REST"),
                f(1.4e9),
                s("LSRK"),
                f(50_000.0),
                s("LSRK"),
                f(1.0),
                f(0.5),
            ],
        )
        .unwrap();
        let rest_hz = match rest {
            ExprValue::Float(v) => v,
            other => panic!("expected Float, got {other:?}"),
        };
        let via_freq_hz = match via_freq {
            ExprValue::Float(v) => v,
            other => panic!("expected Float, got {other:?}"),
        };
        assert!((rest_hz - via_freq_hz).abs() < 1e-6);
    }

    #[test]
    fn rest_and_shift_with_doppler_radio() {
        let rest = call_meas_function("meas.rest", &[f(2.0e8), s("LSRK"), f(0.5)]).unwrap();
        let shift = call_meas_function("meas.shift", &[f(2.0e8), s("LSRK"), f(0.5)]).unwrap();
        let rest_hz = match rest {
            ExprValue::Float(v) => v,
            other => panic!("expected Float, got {other:?}"),
        };
        let shift_hz = match shift {
            ExprValue::Float(v) => v,
            other => panic!("expected Float, got {other:?}"),
        };
        assert!((rest_hz - 4.0e8).abs() < 1e-3);
        assert!((shift_hz - 1.0e8).abs() < 1e-3);
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

    fn extract_array2(val: &ExprValue) -> [f64; 2] {
        match val {
            ExprValue::Array(arr) => {
                assert_eq!(arr.shape, vec![2]);
                let mut out = [0.0; 2];
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
