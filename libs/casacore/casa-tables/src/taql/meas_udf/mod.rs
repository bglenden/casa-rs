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
//! | `meas.riset` / `meas.riseset` | Rise/set UTC datetimes for a source |
//! | `meas.em` / `meas.earthmagnetic` / `meas.emxyz` | Earth-magnetic vector conversion |
//! | `meas.emang` / `meas.emangles` | Earth-magnetic conversion as angles |
//! | `meas.emlen` / `meas.emlength` | Earth-magnetic conversion as field strength |
//! | `meas.igrf` / `meas.igrfxyz` | IGRF model field vector |
//! | `meas.igrfang` / `meas.igrfangles` | IGRF model field as angles |
//! | `meas.igrflen` / `meas.igrflength` | IGRF model field strength |
//! | `meas.igrflos` / `meas.igrflong` | IGRF line-of-sight and point-longitude helpers |
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
use std::sync::Arc;

use casa_core::measures::MeasuresProvider;
use casa_core::measures::direction::{DirectionRef, MDirection, rise_set_times_from_name};
use casa_core::measures::doppler::{DopplerRef, MDoppler};
use casa_core::measures::earth_magnetic::{EarthMagneticRef, MEarthMagnetic, calculate_igrf};
use casa_core::measures::epoch::{EpochRef, MEpoch};
use casa_core::measures::frame::MeasFrame;
use casa_core::measures::frequency::{FrequencyRef, MFrequency};
use casa_core::measures::position::{MPosition, PositionRef};
use casa_core::measures::radial_velocity::{MRadialVelocity, RadialVelocityRef};
use casa_core::quanta::{Quantity, Unit};

use super::error::TaqlError;
use super::eval::ExprValue;

const SECONDS_PER_DAY: f64 = 86_400.0;
const MEAS_HELP_URL: &str = "See also section 'Special Measures functions' at http://casacore.github.io/casacore-notes/199.html";

/// Dispatch a `meas.*` function call.
///
/// Called from `call_function()` when the function name starts with `"meas."`.
type MeasuresRef<'a> = Option<&'a Arc<dyn MeasuresProvider>>;

#[cfg(test)]
pub(crate) fn call_meas_function(name: &str, args: &[ExprValue]) -> Result<ExprValue, TaqlError> {
    let measures: Arc<dyn MeasuresProvider> = Arc::new(TestMeasuresProvider);
    call_meas_function_with_provider(name, args, Some(&measures))
}

#[cfg(test)]
#[derive(Debug)]
struct TestMeasuresProvider;

#[cfg(test)]
impl MeasuresProvider for TestMeasuresProvider {
    fn eop_values(&self, _utc_mjd: f64) -> Result<Option<casa_core::measures::EopValues>, String> {
        Ok(Some(casa_core::measures::EopValues {
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
        Ok(tai_mjd - 32.0 / SECONDS_PER_DAY)
    }

    fn igrf_coefficients(&self, _decimal_year: f64) -> Result<(Vec<f64>, usize), String> {
        let mut coefficients = vec![0.0; 13 * 15];
        coefficients[0] = -29_440.0;
        coefficients[1] = -1_500.0;
        coefficients[2] = 4_650.0;
        Ok((coefficients, 13))
    }

    fn observatory(
        &self,
        name: &str,
    ) -> Result<Option<casa_core::measures::ObservatoryPosition>, String> {
        Ok(match name.to_ascii_uppercase().as_str() {
            "WSRT" => Some(casa_core::measures::ObservatoryPosition::Itrf {
                x_m: 3_826_577.0,
                y_m: 461_022.0,
                z_m: 5_064_892.0,
            }),
            "VLA" => Some(casa_core::measures::ObservatoryPosition::Itrf {
                x_m: -1_601_185.0,
                y_m: -5_041_977.0,
                z_m: 3_554_875.0,
            }),
            _ => None,
        })
    }

    fn source(
        &self,
        name: &str,
    ) -> Result<Option<casa_core::measures::NamedSourceDirection>, String> {
        Ok(name
            .eq_ignore_ascii_case("CasA")
            .then_some(casa_core::measures::NamedSourceDirection {
                reference: "J2000".to_string(),
                longitude_rad: 6.123_487_680_622_104,
                latitude_rad: 1.026_515_399_560_464_8,
            }))
    }
}

pub(crate) fn call_meas_function_with_provider(
    name: &str,
    args: &[ExprValue],
    measures: MeasuresRef<'_>,
) -> Result<ExprValue, TaqlError> {
    let suffix = &name[5..]; // strip "meas."
    match suffix {
        "epoch" => meas_epoch(args, name, measures),
        "last" | "lst" => meas_last(args, name, measures),
        "help" => meas_help(args, name),
        "dir" | "direction" => meas_dir(args, name, measures),
        "dircos" | "directioncosine" => meas_dircos(args, name, measures),
        "riset" | "riseset" => meas_riseset(args, name, measures),
        "em" | "earthmagnetic" | "emxyz" => {
            meas_earthmag(args, EarthMagneticOutput::Xyz, name, measures)
        }
        "emang" | "emangles" => meas_earthmag(args, EarthMagneticOutput::Angles, name, measures),
        "emlen" | "emlength" => meas_earthmag(args, EarthMagneticOutput::Length, name, measures),
        "igrf" | "igrfxyz" => meas_igrf(args, IgrfOutput::Xyz, name, measures),
        "igrfang" | "igrfangles" => meas_igrf(args, IgrfOutput::Angles, name, measures),
        "igrflen" | "igrflength" => meas_igrf(args, IgrfOutput::Length, name, measures),
        "igrflos" => meas_igrf(args, IgrfOutput::Los, name, measures),
        "igrflong" => meas_igrf(args, IgrfOutput::Long, name, measures),
        "pos" | "position" => meas_pos(args, name, measures),
        "itrfxyz" => meas_pos_extract(args, PositionRef::ITRF, PositionOutput::Xyz, name, measures),
        "itrfll" | "itrflonlat" => meas_pos_extract(
            args,
            PositionRef::ITRF,
            PositionOutput::LonLat,
            name,
            measures,
        ),
        "itrfh" | "itrfheight" => meas_pos_extract(
            args,
            PositionRef::ITRF,
            PositionOutput::Height,
            name,
            measures,
        ),
        "itrfllh" => meas_pos_extract(
            args,
            PositionRef::ITRF,
            PositionOutput::LonLatHeight,
            name,
            measures,
        ),
        "wgs" | "wgsxyz" => meas_pos_extract(
            args,
            PositionRef::WGS84,
            PositionOutput::Xyz,
            name,
            measures,
        ),
        "wgsll" | "wgslonlat" => meas_pos_extract(
            args,
            PositionRef::WGS84,
            PositionOutput::LonLat,
            name,
            measures,
        ),
        "wgsh" | "wgsheight" => meas_pos_extract(
            args,
            PositionRef::WGS84,
            PositionOutput::Height,
            name,
            measures,
        ),
        "wgsllh" => meas_pos_extract(
            args,
            PositionRef::WGS84,
            PositionOutput::LonLatHeight,
            name,
            measures,
        ),
        "freq" | "frequency" => meas_freq(args, name, measures),
        "rest" | "restfreq" | "restfrequency" => meas_rest(args, name, measures),
        "shift" | "shiftfreq" | "shiftfrequency" => meas_shift(args, name, measures),
        "doppler" | "redshift" => meas_doppler(args, name),
        "radvel" | "radialvelocity" => meas_radvel(args, name, measures),
        "hadec" => meas_dir_shortcut(args, "HADEC", name, measures),
        "azel" => meas_dir_shortcut(args, "AZEL", name, measures),
        "app" | "apparent" => meas_dir_shortcut(args, "APP", name, measures),
        "j2000" => meas_dir_shortcut(args, "J2000", name, measures),
        "galactic" => meas_dir_shortcut(args, "GALACTIC", name, measures),
        "b1950" => meas_dir_shortcut(args, "B1950", name, measures),
        "ecl" | "ecliptic" => meas_dir_shortcut(args, "ECLIPTIC", name, measures),
        "gal" => meas_dir_shortcut(args, "GALACTIC", name, measures),
        "sgal" | "supergal" | "supergalactic" => {
            meas_dir_shortcut(args, "SUPERGAL", name, measures)
        }
        "itrfd" | "itrfdir" | "itrfdirection" => meas_dir_shortcut(args, "ITRF", name, measures),
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

#[derive(Clone, Copy)]
enum EarthMagneticOutput {
    Xyz,
    Angles,
    Length,
}

#[derive(Clone, Copy)]
enum IgrfOutput {
    Xyz,
    Angles,
    Length,
    Los,
    Long,
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
  MEAS.RISET (direction, epoch, position)        rise and set UTC datetimes\n\
       RISESET is a synonym for RISET",
    );
    if show_types {
        out.push_str(
            "\n\nKnown source directions (names are case-insensitive):\n\
  Built-in source names resolved by the Rust measures layer\n\
  SUN   MOON  MERCURY  VENUS  MARS  JUPITER  SATURN  URANUS  NEPTUNE  PLUTO\n\
  CasA  CygA  HerA     HydA   PerA  TauA     VirA\n\
  ZENITH returns the local zenith in AZEL.\n\
 In function RISET type SUN can have a suffix -XX where XX can be (default -UR):\n\
   C    center touches horizon             CR  center with refraction\n\
   U    upper edge touches horizon         UR  upper edge with refraction\n\
   L    lower edge touches horizon         LR  lower edge with refraction\n\
   CT   civil twilight darkness (-6 deg)   NT  nautical twilight darkness (-12)\n\
   AT   amateur astronomy twilight (-15)   ST  scientific astronomy twilight (-18)\n\
 The first 6 suffixes can also be used with MOON.\n\
 External measures source-catalog names remain deferred in Rust.\n\n",
        );
        let direction_types: Vec<&str> = DirectionRef::ALL.iter().map(|r| r.as_str()).collect();
        append_known_types(&mut out, "Known direction types:", &direction_types);
    }
    out
}

fn help_earth_magnetic(show_types: bool) -> String {
    let mut out = String::from(
        "EarthMagnetic conversion functions:\n\
  MEAS.EM (type, em, epoch, position)            convert em value to given type as xyz\n\
       EARTHMAGNETIC and EMXYZ are synonyms for EM\n\
  MEAS.EMANG (type, em, epoch, position)         convert and return as angles\n\
       EMANGLES is a synonym for EMANG\n\
  MEAS.EMLEN (type, em, epoch, position)         convert and return as flux density\n\
       EMLENGTH is a synonym for EMLEN\n\
  MEAS.IGRF (type, height, direction, epoch, position)\n\
       IGRF model value\n\
       IGRFXYZ is a synonym for IGRF\n\
  MEAS.IGRFANG (t, h, d, e, p)                   IGRF model angles in ITRF\n\
       IGRFANGLES is a synonym for IGRFANG\n\
  MEAS.IGRFLEN (t, h, d, e, p)                   IGRF model flux density\n\
       IGRFLENGTH is a synonym for IGRFLEN\n\
  MEAS.IGRFLOS (h, d, e, p)                      IGRF value along line-of-sight\n\
  MEAS.IGRFLONG (h, d, e, p)                     longitude of calculation point",
    );
    if show_types {
        out.push_str("\n\nKnown EarthMagnetic types:\n");
        let earthmag_types: Vec<&str> = EarthMagneticRef::ALL
            .iter()
            .filter(|r| **r != EarthMagneticRef::IGRF)
            .map(|r| r.as_str())
            .collect();
        append_known_types(&mut out, "Known EarthMagnetic types:", &earthmag_types);
        out.push_str(
            "\n\nExplicit EarthMagnetic values can be given as numeric XYZ vectors or as\n\
angle, angle, length scalars when TaQL quantities provide units (for example 0.3rad,\n\
-0.2rad, 50000nT). Compound units should use quoted quantity syntax such as 1 'km/s'.",
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
fn measure_err(fn_name: &str, e: casa_core::measures::MeasureError) -> TaqlError {
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
/// - For direction UDFs: `[epoch, position]`
/// - For numeric helpers: `[epoch, px, py, pz]`
fn meas_frame(measures: MeasuresRef<'_>) -> MeasFrame {
    measures.cloned().map_or_else(MeasFrame::new, |provider| {
        MeasFrame::new().with_measures(provider)
    })
}

fn build_frame_with_epoch_pos(
    extra: &[ExprValue],
    fn_name: &str,
    measures: MeasuresRef<'_>,
) -> Result<MeasFrame, TaqlError> {
    let mut frame = meas_frame(measures);
    if !extra.is_empty() {
        let epoch_mjd = expr_to_mjd_days(&extra[0], fn_name)?;
        frame = frame.with_epoch(MEpoch::from_mjd(epoch_mjd, EpochRef::UTC));
    }
    if extra.len() > 1 {
        frame = frame.with_position(parse_position_input(&extra[1..], fn_name, measures)?);
    }
    Ok(frame)
}

/// Build a full `MeasFrame` from optional direction + epoch + position args.
///
/// For frequency/radvel: `[dir_lon, dir_lat, epoch, px, py, pz]`
fn build_frame_with_dir_epoch_pos(
    extra: &[ExprValue],
    measures: MeasuresRef<'_>,
) -> Result<MeasFrame, TaqlError> {
    let mut frame = meas_frame(measures);
    if extra.len() >= 2 {
        let lon = expr_to_angle_rad(&extra[0], "meas frame direction")?;
        let lat = expr_to_angle_rad(&extra[1], "meas frame direction")?;
        frame = frame.with_direction(MDirection::from_angles(lon, lat, DirectionRef::J2000));
    }
    if extra.len() >= 3 {
        let epoch_mjd = expr_to_mjd_days(&extra[2], "meas frame epoch")?;
        frame = frame.with_epoch(MEpoch::from_mjd(epoch_mjd, EpochRef::UTC));
    }
    if extra.len() > 3 {
        frame = frame.with_position(parse_position_input(&extra[3..], "meas frame", measures)?);
    }
    Ok(frame)
}

fn expr_as_quantity(
    value: &ExprValue,
    default_unit: &str,
    fn_name: &str,
) -> Result<Quantity, TaqlError> {
    match value {
        ExprValue::Quantity(q) => Ok(q.clone()),
        _ => Quantity::new(value.to_float()?, default_unit).map_err(|e| TaqlError::TypeError {
            message: format!("{fn_name}: {e}"),
        }),
    }
}

fn expr_to_unit_value(
    value: &ExprValue,
    default_unit: &str,
    target_unit: &str,
    what: &str,
    fn_name: &str,
) -> Result<f64, TaqlError> {
    let quantity = expr_as_quantity(value, default_unit, fn_name)?;
    let target = Unit::new(target_unit).map_err(|e| TaqlError::TypeError {
        message: format!("{fn_name}: {e}"),
    })?;
    quantity
        .convert(&target)
        .map(|converted| converted.value())
        .map_err(|_| TaqlError::TypeError {
            message: format!(
                "{fn_name}: expected {what} with units conformant to {target_unit}, got {}",
                quantity.unit().name()
            ),
        })
}

fn expr_to_angle_rad(value: &ExprValue, fn_name: &str) -> Result<f64, TaqlError> {
    expr_to_unit_value(value, "rad", "rad", "angle", fn_name)
}

fn expr_to_length_m(value: &ExprValue, fn_name: &str) -> Result<f64, TaqlError> {
    expr_to_unit_value(value, "m", "m", "length", fn_name)
}

fn expr_to_flux_nt(value: &ExprValue, fn_name: &str) -> Result<f64, TaqlError> {
    expr_to_unit_value(value, "nT", "nT", "flux density", fn_name)
}

fn expr_to_frequency_hz(value: &ExprValue, fn_name: &str) -> Result<f64, TaqlError> {
    expr_to_unit_value(value, "Hz", "Hz", "frequency", fn_name)
}

fn expr_to_radial_velocity_ms(value: &ExprValue, fn_name: &str) -> Result<f64, TaqlError> {
    expr_to_unit_value(value, "m/s", "m/s", "radial velocity", fn_name)
}

fn expr_to_mjd_days(value: &ExprValue, fn_name: &str) -> Result<f64, TaqlError> {
    expr_to_unit_value(value, "d", "d", "epoch", fn_name)
}

fn parse_position_input(
    args: &[ExprValue],
    fn_name: &str,
    measures: MeasuresRef<'_>,
) -> Result<MPosition, TaqlError> {
    match args {
        [name] => {
            let observatory = name.to_string_val()?;
            let provider = measures.ok_or_else(|| TaqlError::TypeError {
                message: format!("{fn_name}: observatory name {observatory:?} requires an explicit measures provider"),
            })?;
            MPosition::from_observatory_name(&observatory, provider.as_ref())
                .map_err(|error| measure_err(fn_name, error))?
                .ok_or_else(|| TaqlError::TypeError {
                    message: format!("{fn_name}: unknown observatory {observatory:?}"),
                })
        }
        [x, y, z] => Ok(MPosition::new_itrf(
            expr_to_length_m(x, fn_name)?,
            expr_to_length_m(y, fn_name)?,
            expr_to_length_m(z, fn_name)?,
        )),
        [x, y, z, src_ref] => {
            let src: PositionRef = parse_ref(src_ref, fn_name)?;
            Ok(match src {
                PositionRef::ITRF => MPosition::new_itrf(
                    expr_to_length_m(x, fn_name)?,
                    expr_to_length_m(y, fn_name)?,
                    expr_to_length_m(z, fn_name)?,
                ),
                PositionRef::WGS84 => MPosition::new_wgs84(
                    expr_to_angle_rad(x, fn_name)?,
                    expr_to_angle_rad(y, fn_name)?,
                    expr_to_length_m(z, fn_name)?,
                ),
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
    let value = match value_arg {
        ExprValue::Quantity(q) => {
            let target = Unit::new("m/s").expect("valid unit");
            q.convert(&target)
                .map(|converted| converted.value())
                .map_err(|_| TaqlError::TypeError {
                    message: format!(
                        "{fn_name}: quantity doppler input must be a radial velocity, got {}",
                        q.unit().name()
                    ),
                })?
        }
        _ => value_arg.to_float()?,
    };
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
        expr_to_radial_velocity_ms(value_arg, fn_name)?,
        parse_ref::<RadialVelocityRef>(ref_arg, fn_name)?,
    ))
}

enum ParsedDirectionInput {
    Named(String),
    Angles {
        lon: f64,
        lat: f64,
        refer: DirectionRef,
    },
    Cosines {
        xyz: [f64; 3],
        refer: DirectionRef,
    },
}

impl ParsedDirectionInput {
    fn materialize(&self, frame: &MeasFrame) -> Result<MDirection, TaqlError> {
        match self {
            Self::Named(name) => {
                MDirection::from_source_name(name, frame).map_err(|e| TaqlError::TypeError {
                    message: e.to_string(),
                })
            }
            Self::Angles { lon, lat, refer } => Ok(MDirection::from_angles(*lon, *lat, *refer)),
            Self::Cosines { xyz, refer } => Ok(MDirection::from_cosines(*xyz, *refer)),
        }
    }
}

fn parse_direction_input(
    args: &[ExprValue],
    fn_name: &str,
) -> Result<(ParsedDirectionInput, usize), TaqlError> {
    let Some(first) = args.first() else {
        return Err(TaqlError::ArgumentCount {
            name: fn_name.to_string(),
            expected: "direction argument".to_string(),
            got: 0,
        });
    };

    if let ExprValue::String(name) = first {
        return Ok((ParsedDirectionInput::Named(name.clone()), 1));
    }

    if let ExprValue::Array(arr) = first {
        let vals = extract_float_array(arr, fn_name)?;
        let refer = if args.len() > 1 && matches!(args[1], ExprValue::String(_)) {
            parse_ref(&args[1], fn_name)?
        } else {
            DirectionRef::J2000
        };
        return match vals.as_slice() {
            [lon, lat] => Ok((
                ParsedDirectionInput::Angles {
                    lon: *lon,
                    lat: *lat,
                    refer,
                },
                if args.len() > 1 && matches!(args[1], ExprValue::String(_)) {
                    2
                } else {
                    1
                },
            )),
            [x, y, z] => Ok((
                ParsedDirectionInput::Cosines {
                    xyz: [*x, *y, *z],
                    refer,
                },
                if args.len() > 1 && matches!(args[1], ExprValue::String(_)) {
                    2
                } else {
                    1
                },
            )),
            _ => Err(TaqlError::TypeError {
                message: format!("{fn_name}: direction array must contain 2 or 3 values"),
            }),
        };
    }

    if args.len() < 2 {
        return Err(TaqlError::ArgumentCount {
            name: fn_name.to_string(),
            expected: "2..3 direction scalars".to_string(),
            got: args.len(),
        });
    }
    let lon = expr_to_angle_rad(&args[0], fn_name)?;
    let lat = expr_to_angle_rad(&args[1], fn_name)?;
    let refer = if args.len() > 2 && matches!(args[2], ExprValue::String(_)) {
        parse_ref(&args[2], fn_name)?
    } else {
        DirectionRef::J2000
    };
    Ok((
        ParsedDirectionInput::Angles { lon, lat, refer },
        if args.len() > 2 && matches!(args[2], ExprValue::String(_)) {
            3
        } else {
            2
        },
    ))
}

fn parse_earthmag_xyz_input(
    args: &[ExprValue],
    fn_name: &str,
) -> Result<(EarthMagneticParsedInput, EarthMagneticRef, usize), TaqlError> {
    if args.is_empty() {
        return Err(TaqlError::ArgumentCount {
            name: fn_name.to_string(),
            expected: "earth-magnetic vector".to_string(),
            got: 0,
        });
    }

    let (parsed, consumed) = match &args[0] {
        ExprValue::Array(arr) => {
            let vals = extract_float_array(arr, fn_name)?;
            match vals.as_slice() {
                [x, y, z] => (EarthMagneticParsedInput::Xyz([*x, *y, *z]), 1),
                _ => {
                    return Err(TaqlError::TypeError {
                        message: format!(
                            "{fn_name}: earth-magnetic vector array must contain 3 values"
                        ),
                    });
                }
            }
        }
        _ => {
            if args.len() < 3 {
                return Err(TaqlError::ArgumentCount {
                    name: fn_name.to_string(),
                    expected: "3 vector scalars".to_string(),
                    got: args.len(),
                });
            }
            parse_earthmag_scalars(args, fn_name)?
        }
    };

    let src = if args.len() > consumed && matches!(args[consumed], ExprValue::String(_)) {
        parse_ref(&args[consumed], fn_name)?
    } else {
        EarthMagneticRef::ITRF
    };
    Ok((
        parsed,
        src,
        consumed
            + if args.len() > consumed && matches!(args[consumed], ExprValue::String(_)) {
                1
            } else {
                0
            },
    ))
}

enum EarthMagneticParsedInput {
    Xyz([f64; 3]),
    Angles {
        lon_rad: f64,
        lat_rad: f64,
        length_nt: f64,
    },
}

fn parse_earthmag_scalars(
    args: &[ExprValue],
    fn_name: &str,
) -> Result<(EarthMagneticParsedInput, usize), TaqlError> {
    let mode = match &args[0] {
        ExprValue::Quantity(q) => {
            let flux = Unit::new("T").expect("valid unit");
            let angle = Unit::new("rad").expect("valid unit");
            if q.unit().conformant(&flux) {
                Some(true)
            } else if q.unit().conformant(&angle) {
                Some(false)
            } else {
                None
            }
        }
        _ => None,
    };

    let parsed = match mode {
        Some(true) => EarthMagneticParsedInput::Xyz([
            expr_to_flux_nt(&args[0], fn_name)?,
            expr_to_flux_nt(&args[1], fn_name)?,
            expr_to_flux_nt(&args[2], fn_name)?,
        ]),
        Some(false) => EarthMagneticParsedInput::Angles {
            lon_rad: expr_to_angle_rad(&args[0], fn_name)?,
            lat_rad: expr_to_angle_rad(&args[1], fn_name)?,
            length_nt: expr_to_flux_nt(&args[2], fn_name)?,
        },
        None => EarthMagneticParsedInput::Xyz([
            args[0].to_float()?,
            args[1].to_float()?,
            args[2].to_float()?,
        ]),
    };

    Ok((parsed, 3))
}

fn extract_float_array(
    arr: &super::eval::ArrayValue,
    fn_name: &str,
) -> Result<Vec<f64>, TaqlError> {
    arr.data
        .iter()
        .map(|value| {
            value.to_float().map_err(|_| TaqlError::TypeError {
                message: format!("{fn_name}: expected numeric array values"),
            })
        })
        .collect()
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

mod evaluate;

use evaluate::*;

/// Build a direction result as `Array([lon, lat])`.
fn make_dir_result(dir: &MDirection) -> ExprValue {
    make_array2_result(dir.longitude_rad(), dir.latitude_rad())
}

fn make_earthmag_result(field: &MEarthMagnetic, output: EarthMagneticOutput) -> ExprValue {
    match output {
        EarthMagneticOutput::Xyz => make_array_result(&field.xyz_nt()),
        EarthMagneticOutput::Angles => {
            let (lon, lat) = field.angles_rad();
            make_array2_result(lon, lat)
        }
        EarthMagneticOutput::Length => ExprValue::Float(field.length_nt()),
    }
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

fn make_datetime_array_result(values: &[f64]) -> ExprValue {
    ExprValue::Array(super::eval::ArrayValue {
        shape: vec![values.len()],
        data: values.iter().copied().map(ExprValue::DateTime).collect(),
    })
}

#[cfg(test)]
mod tests;
