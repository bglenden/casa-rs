// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[derive(Debug, Clone)]
pub(super) enum NamedDirectionKind {
    Fixed(MDirection),
    SolarSystem(SolarSystemBody),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SolarSystemBody {
    Mercury,
    Venus,
    Mars,
    Jupiter,
    Saturn,
    Uranus,
    Neptune,
    Pluto,
    Sun,
    Moon,
}

#[derive(Debug, Clone)]
pub(super) struct ParsedNamedDirection {
    pub(super) kind: NamedDirectionKind,
    pub(super) rise_set_horizon_rad: f64,
}

pub(super) fn parse_named_direction(
    source_name: &str,
    frame: &MeasFrame,
) -> Result<ParsedNamedDirection, MeasureError> {
    let original = source_name.trim();
    let upper = original.to_ascii_uppercase();
    if upper.is_empty() {
        return Err(MeasureError::UnknownSourceName {
            input: source_name.to_string(),
        });
    }

    if upper == "ZENITH" {
        return Ok(ParsedNamedDirection {
            kind: NamedDirectionKind::Fixed(MDirection::from_angles(
                0.0,
                std::f64::consts::FRAC_PI_2,
                DirectionRef::AZEL,
            )),
            rise_set_horizon_rad: 0.0,
        });
    }

    if let Some(parsed) = parse_solar_system_name(original, &upper)? {
        return Ok(parsed);
    }

    let runtime_source = match frame.measures() {
        Some(provider) => provider
            .source(original)
            .map_err(|reason| MeasureError::ModelError {
                model: "Sources",
                reason,
            })?,
        None => None,
    };
    if let Some(entry) = runtime_source {
        let refer = DirectionRef::from_str(&entry.reference).map_err(|_| {
            MeasureError::InvalidSourceName {
                input: source_name.to_string(),
                reason: format!(
                    "unsupported direction type {:?} in ephemerides/Sources",
                    entry.reference
                ),
            }
        })?;
        return Ok(ParsedNamedDirection {
            kind: NamedDirectionKind::Fixed(MDirection::from_angles(
                entry.longitude_rad,
                entry.latitude_rad,
                refer,
            )),
            rise_set_horizon_rad: 0.0,
        });
    }

    let fixed = match upper.as_str() {
        "CASA" => Some((6.123_487_680_622_104, 1.026_515_399_560_464_8)),
        "CYGA" => Some((5.233_686_575_770_755, 0.710_940_958_218_079_1)),
        "TAUA" => Some((1.459_674_849_373_091_3, 0.384_225_023_359_212_94)),
        "VIRA" => Some((3.276_086_511_413_598, 0.216_265_895_335_673_78)),
        "HERA" => Some((4.411_908_733_038_216, 0.087_135_562_905_816_9)),
        "HYDA" => Some((2.435_146_6, -0.211_107_06)),
        "PERA" => Some((0.871_803_63, 0.724_515_80)),
        _ => None,
    };

    if let Some((lon_rad, lat_rad)) = fixed {
        return Ok(ParsedNamedDirection {
            kind: NamedDirectionKind::Fixed(MDirection::from_angles(
                lon_rad,
                lat_rad,
                DirectionRef::J2000,
            )),
            rise_set_horizon_rad: 0.0,
        });
    }

    Err(MeasureError::UnknownSourceName {
        input: source_name.to_string(),
    })
}

/// Pluto, which casacore names but no model here resolves.
pub(super) fn pluto_unmodelled() -> MeasureError {
    MeasureError::NotYetImplemented {
        route: "moving source name PLUTO".to_string(),
    }
}

pub(super) fn parse_solar_system_name(
    original: &str,
    upper: &str,
) -> Result<Option<ParsedNamedDirection>, MeasureError> {
    const DEG_TO_RAD: f64 = std::f64::consts::PI / 180.0;

    let parse_horizon = |input: &str, ext: &str, is_sun: bool| -> Result<f64, MeasureError> {
        let value_deg = match ext {
            "" => -0.833,
            "-C" => 0.0,
            "-U" => -0.25,
            "-L" => 0.25,
            "-CR" => -0.583,
            "-UR" => -0.833,
            "-LR" => -0.333,
            "-CT" if is_sun => -6.0,
            "-NT" if is_sun => -12.0,
            "-AT" if is_sun => -15.0,
            "-ST" if is_sun => -18.0,
            _ => {
                let reason = if is_sun {
                    "use -C, -U, -L, -CR, -UR, -LR, -CT, -NT, -AT, or -ST"
                } else {
                    "use -C, -U, -L, -CR, -UR, or -LR"
                };
                return Err(MeasureError::InvalidSourceName {
                    input: input.to_string(),
                    reason: reason.to_string(),
                });
            }
        };
        Ok(value_deg * DEG_TO_RAD)
    };

    macro_rules! simple_body {
        ($name:literal, $body:expr) => {
            if upper == $name {
                return Ok(Some(ParsedNamedDirection {
                    kind: NamedDirectionKind::SolarSystem($body),
                    rise_set_horizon_rad: 0.0,
                }));
            }
        };
    }

    if let Some(ext) = upper.strip_prefix("SUN") {
        return Ok(Some(ParsedNamedDirection {
            kind: NamedDirectionKind::SolarSystem(SolarSystemBody::Sun),
            rise_set_horizon_rad: parse_horizon(original, ext, true)?,
        }));
    }
    if let Some(ext) = upper.strip_prefix("MOON") {
        return Ok(Some(ParsedNamedDirection {
            kind: NamedDirectionKind::SolarSystem(SolarSystemBody::Moon),
            rise_set_horizon_rad: parse_horizon(original, ext, false)?,
        }));
    }

    simple_body!("MERCURY", SolarSystemBody::Mercury);
    simple_body!("VENUS", SolarSystemBody::Venus);
    simple_body!("MARS", SolarSystemBody::Mars);
    simple_body!("JUPITER", SolarSystemBody::Jupiter);
    simple_body!("SATURN", SolarSystemBody::Saturn);
    simple_body!("URANUS", SolarSystemBody::Uranus);
    simple_body!("NEPTUNE", SolarSystemBody::Neptune);
    simple_body!("PLUTO", SolarSystemBody::Pluto);

    Ok(None)
}

pub(super) fn default_rise_set_horizon_rad(kind: &NamedDirectionKind) -> f64 {
    match kind {
        NamedDirectionKind::Fixed(_) => 0.0,
        NamedDirectionKind::SolarSystem(SolarSystemBody::Sun)
        | NamedDirectionKind::SolarSystem(SolarSystemBody::Moon) => -0.833_f64.to_radians(),
        NamedDirectionKind::SolarSystem(_) => 0.0,
    }
}

pub(super) fn resolve_named_direction(
    parsed: &ParsedNamedDirection,
    frame: &MeasFrame,
) -> Result<MDirection, MeasureError> {
    match &parsed.kind {
        NamedDirectionKind::Fixed(direction) => Ok(direction.clone()),
        NamedDirectionKind::SolarSystem(body) => resolve_solar_system_body(*body, frame),
    }
}

pub(super) fn resolve_solar_system_body(
    body: SolarSystemBody,
    frame: &MeasFrame,
) -> Result<MDirection, MeasureError> {
    let epoch = frame.epoch().ok_or(MeasureError::MissingFrameData {
        what: "epoch (for moving source name)",
    })?;
    let tt = epoch.convert_to(EpochRef::TT, frame)?;
    let (tt1, tt2) = tt.value().as_jd_pair();
    let direction = match body {
        SolarSystemBody::Sun => {
            let (earth_helio, _) =
                sofars::eph::epv00(tt1, tt2).ok_or(MeasureError::SofarsError { code: -1 })?;
            [-earth_helio[0][0], -earth_helio[0][1], -earth_helio[0][2]]
        }
        SolarSystemBody::Moon => {
            let moon = sofars::eph::moon98(tt1, tt2);
            moon[0]
        }
        SolarSystemBody::Mercury
        | SolarSystemBody::Venus
        | SolarSystemBody::Mars
        | SolarSystemBody::Jupiter
        | SolarSystemBody::Saturn
        | SolarSystemBody::Uranus
        | SolarSystemBody::Neptune => {
            let planet_index = match body {
                SolarSystemBody::Mercury => 1,
                SolarSystemBody::Venus => 2,
                SolarSystemBody::Mars => 4,
                SolarSystemBody::Jupiter => 5,
                SolarSystemBody::Saturn => 6,
                SolarSystemBody::Uranus => 7,
                SolarSystemBody::Neptune => 8,
                _ => unreachable!(),
            };
            let (planet, _status) = sofars::eph::plan94(tt1, tt2, planet_index)
                .map_err(|code| MeasureError::SofarsError { code })?;
            let (earth_helio, _) =
                sofars::eph::epv00(tt1, tt2).ok_or(MeasureError::SofarsError { code: -1 })?;
            [
                planet[0][0] - earth_helio[0][0],
                planet[0][1] - earth_helio[0][1],
                planet[0][2] - earth_helio[0][2],
            ]
        }
        SolarSystemBody::Pluto => return Err(pluto_unmodelled()),
    };

    Ok(MDirection::from_cosines(direction, DirectionRef::ICRS))
}

pub(super) fn calc_rise_set(
    resolver: &dyn Fn(&MeasFrame) -> Result<MDirection, MeasureError>,
    horizon_rad: f64,
    frame: &MeasFrame,
) -> Result<RiseSetTimes, MeasureError> {
    let position = frame.position().ok_or(MeasureError::MissingFrameData {
        what: "position (for rise/set)",
    })?;
    let epoch_utc = frame
        .epoch()
        .ok_or(MeasureError::MissingFrameData {
            what: "epoch (for rise/set)",
        })?
        .convert_to(EpochRef::UTC, frame)?;
    let start = (epoch_utc.value().as_mjd() + 1.0e-6).floor();
    let latitude = position.latitude_rad();

    let mut rise = 0.0;
    let mut set = 0.0;
    let above_below = fill_rise_set(
        start + 0.5,
        latitude,
        horizon_rad,
        start,
        frame,
        resolver,
        Some(&mut rise),
        Some(&mut set),
    )?;

    if above_below > 0 {
        return Ok(RiseSetTimes {
            rise_mjd: start + 1.0,
            set_mjd: start,
        });
    }
    if above_below < 0 {
        return Ok(RiseSetTimes {
            rise_mjd: start,
            set_mjd: start + 1.0,
        });
    }

    if rise < start {
        rise += 1.0 - 236.0 / 86_400.0;
    }
    if set < start {
        set += 1.0 - 236.0 / 86_400.0;
    }
    if set < rise {
        set += 1.0;
    }

    for _ in 0..2 {
        fill_rise_set(
            rise,
            latitude,
            horizon_rad,
            start,
            frame,
            resolver,
            Some(&mut rise),
            None,
        )?;
        if rise < start {
            rise += 1.0 - 236.0 / 86_400.0;
        }

        fill_rise_set(
            set,
            latitude,
            horizon_rad,
            start,
            frame,
            resolver,
            None,
            Some(&mut set),
        )?;
        if set < start {
            set += 1.0 - 236.0 / 86_400.0;
        }
        if set < rise {
            set += 1.0;
        }
    }

    Ok(RiseSetTimes {
        rise_mjd: rise,
        set_mjd: set,
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn fill_rise_set(
    trial_epoch_mjd: f64,
    latitude_rad: f64,
    horizon_rad: f64,
    start_day_mjd: f64,
    base_frame: &MeasFrame,
    resolver: &dyn Fn(&MeasFrame) -> Result<MDirection, MeasureError>,
    rise_out: Option<&mut f64>,
    set_out: Option<&mut f64>,
) -> Result<i32, MeasureError> {
    let trial_frame = base_frame
        .clone()
        .with_epoch(MEpoch::from_mjd(trial_epoch_mjd, EpochRef::UTC));
    let direction = resolver(&trial_frame)?;
    let hadec = direction.convert_to(DirectionRef::HADEC, &trial_frame)?;
    let dec = hadec.latitude_rad();
    let ct =
        (horizon_rad.sin() - dec.sin() * latitude_rad.sin()) / (dec.cos() * latitude_rad.cos());
    if ct >= 1.0 {
        return Ok(1);
    }
    if ct <= -1.0 {
        return Ok(-1);
    }

    let ct = ct.acos();
    let app = direction.convert_to(DirectionRef::APP, &trial_frame)?;
    let norm_ra = sofars::vm::anp(app.longitude_rad());

    if let Some(rise) = rise_out {
        *rise = last_angle_to_utc_mjd(start_day_mjd, norm_ra - ct, &trial_frame)?;
    }
    if let Some(set) = set_out {
        *set = last_angle_to_utc_mjd(start_day_mjd, norm_ra + ct, &trial_frame)?;
    }
    Ok(0)
}

/// The UTC of a local apparent sidereal time on the sidereal day that starts
/// the UTC day `start_day_mjd`, as casacore's `DirectionEngine::fillRiseSet`
/// offsets it: the start of the day converted to LAST and razed to whole
/// sidereal days.
pub(super) fn last_angle_to_utc_mjd(
    start_day_mjd: f64,
    last_angle_rad: f64,
    frame: &MeasFrame,
) -> Result<f64, MeasureError> {
    let sidereal_day = MEpoch::from_mjd(start_day_mjd, EpochRef::UTC)
        .convert_to(EpochRef::LAST, frame)?
        .value()
        .day();
    let last = MjdHighPrec::new(sidereal_day, last_angle_rad / std::f64::consts::TAU);
    Ok(MEpoch::new(last, EpochRef::LAST)
        .convert_to(EpochRef::UTC, frame)?
        .value()
        .as_mjd())
}
