// SPDX-License-Identifier: LGPL-3.0-or-later
//! Mosaic pointings: `simutil.calc_pointings2` and the pointing files
//! `simobserve` reads and writes (`<project>.ptg.txt`).
//!
//! A pointing is a direction string as `simutil` writes them:
//! `J2000 18:59:57.29543 -023.00.38.805516`.

use std::path::Path;

use casa_types::quanta::Quantity;

use super::direction::Directions;
use super::{SimutilError, qa};
use crate::readantenna::parse::{python_float, python_split};

/// `simobserve`'s `pbcoeff`: the primary beam is 1.13 λ/D. `calc_pointings2`'s
/// ALMA tiling uses it for its default beam, where CASA reads an undefined
/// name.
pub const PBCOEFF: f64 = 1.13;

/// The most pointings [`calc_pointings`] lays out. CASA lays out a map of
/// any size, and with a spacing tiny beside the map runs for hours or out
/// of memory; casa-rs refuses such a map before laying any of it out.
pub const MOST_POINTINGS: i64 = 10_000_000;

/// The layouts `calc_pointings2` knows, by the first three characters of
/// `maptype` in upper case (`hex`, `hexagonal`, `ALMA-OT`, `alma`, ...).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MapType {
    /// `HEX`: rows `0.866025404 × spacing` apart, alternate rows shifted by
    /// half a spacing.
    Hexagonal,
    /// `SQU`: a square lattice.
    Square,
    /// `ALM`: the ALMA Observing Tool's triangular tiling (Cycle 1 on).
    Alma,
    /// `ALMA2012`: the Cycle 0 tiling.
    Alma2012,
    /// Anything else: no pointings are laid out, and the centre is used.
    Unknown,
}

impl MapType {
    /// The layout CASA's `maptype` string names.
    pub fn from_casa(maptype: &str) -> Self {
        let prefix = |n: usize| maptype.chars().take(n).collect::<String>().to_uppercase();
        match prefix(3).as_str() {
            "HEX" => Self::Hexagonal,
            "SQU" => Self::Square,
            "ALM" if prefix(8) == "ALMA2012" => Self::Alma2012,
            "ALM" => Self::Alma,
            _ => Self::Unknown,
        }
    }
}

/// `simutil.calc_pointings2(spacing, size, maptype, direction, relmargin,
/// beam)`: the pointings of a map `size` (x, y) with pointings `spacing`
/// apart about `direction`, starting at the top row. `relmargin` is the
/// margin in spacings left at each edge (hexagonal and square maps);
/// `beam_arcsec` is the primary beam the ALMA tiling fills to (`spacing ×
/// 1.13 × √3` when not positive).
///
/// As in CASA, the direction is checked first ([`Directions::split`]); a
/// list of more than one is then returned as it is. A list of one is
/// centred at its mean, which CASA writes at precision 5 and reads back, so
/// its pointings can differ from those of the same direction as a string
/// in the last digit. A map too small for any pointing, or a layout CASA
/// does not know, gives the direction alone, as it was given.
///
/// # Errors
///
/// A direction that does not split; a zero spacing, or a count of rows or
/// columns that is not finite, which CASA refuses with Python's
/// `ZeroDivisionError`, `OverflowError` or `ValueError` (their messages);
/// a map of more than [`MOST_POINTINGS`] pointings.
pub fn calc_pointings(
    spacing: &Quantity,
    size: [&Quantity; 2],
    maptype: &str,
    direction: &Directions,
    relmargin: f64,
    beam_arcsec: f64,
) -> Result<Vec<String>, SimutilError> {
    let centre = direction.split()?;
    let direction = match direction {
        Directions::List(directions) if directions.len() > 1 => return Ok(directions.clone()),
        // A list that splits is not empty.
        Directions::List(directions) => &directions[0],
        Directions::One(direction) => direction,
    };
    let layout = MapType::from_casa(maptype);
    let mut pointings = match layout {
        MapType::Hexagonal | MapType::Square => lattice(
            layout,
            spacing,
            size,
            (&centre.epoch, &centre.longitude, &centre.latitude),
            relmargin,
        )?,
        MapType::Alma | MapType::Alma2012 => {
            let spacing = qa::value_in(spacing, "arcsec")?;
            // CASA's tilings divide by the spacing as a Python float.
            if spacing == 0.0 {
                return Err(SimutilError::Arithmetic("float division by zero".into()));
            }
            let (width, height) = (
                qa::value_in(size[0], "arcsec")?,
                qa::value_in(size[1], "arcsec")?,
            );
            let offsets = if layout == MapType::Alma2012 {
                triangle_points(width, height, 0.0, spacing)?
            } else {
                let beam = if beam_arcsec <= 0.0 {
                    spacing * PBCOEFF * 3.0_f64.sqrt()
                } else {
                    beam_arcsec
                };
                triangular_tiling(width, height, 0.0, spacing, beam)?
            };
            tiled(
                &offsets,
                (&centre.epoch, &centre.longitude, &centre.latitude),
            )?
        }
        MapType::Unknown => Vec::new(),
    };
    if pointings.is_empty() {
        pointings.push(direction.to_string());
    }
    tracing::info!("using {} generated pointing(s)", pointings.len());
    Ok(pointings)
}

/// A centre: the epoch (with its space) and its angles in degrees.
type Centre<'a> = (&'a str, &'a Quantity, &'a Quantity);

/// The hexagonal and square maps, row by row from the top, each row from
/// the east (CASA starts at the top because it sets first in the south).
fn lattice(
    layout: MapType,
    spacing: &Quantity,
    size: [&Quantity; 2],
    (epoch, centre_x, centre_y): Centre<'_>,
    relmargin: f64,
) -> Result<Vec<String>, SimutilError> {
    let hexagonal = layout == MapType::Hexagonal;
    let y_spacing = if hexagonal {
        qa::mul(&qa::number(0.866025404), spacing)?
    } else {
        spacing.clone()
    };
    let row_margin = if hexagonal { 2.309401077 } else { 2.0 };
    let rows =
        1 + count(qa::value_in(&qa::div(size[1], &y_spacing)?, "")? - row_margin * relmargin)?;
    let available = 1.0 + qa::value_in(&qa::div(size[0], spacing)?, "")? - 2.0 * relmargin;
    let columns = count(available)?;
    bounded(rows, columns)?;
    // Hexagonal maps: the even rows are shifted half a spacing ahead, and
    // possibly shorter, so the top and bottom rows of an odd count are short.
    let (even_columns, to_first) = if !hexagonal {
        (columns, 0.5 * (columns - 1) as f64)
    } else if available - columns as f64 >= 0.5 && rows > 1 {
        (columns, 0.5 * (columns as f64 - 0.5))
    } else {
        (columns - 1, 0.5 * (columns - 1) as f64)
    };
    let mut pointings = Vec::new();
    let mut y = qa::add(
        centre_y,
        &qa::mul(&qa::number(0.5 * (rows - 1) as f64), &y_spacing)?,
    )?;
    for row in 0..rows.max(0) {
        let x_spacing = qa::mul(&qa::number(1.0 / qa::value_in(&y, "rad")?.cos()), spacing)?;
        let latitude = qa::dms(&y)?;
        // A hexagonal map's even rows start half a spacing ahead.
        let (shift, count) = if hexagonal && row % 2 == 0 {
            (to_first - 0.5, even_columns)
        } else {
            (to_first, columns)
        };
        let x_min = qa::sub(centre_x, &qa::mul(&qa::number(shift), &x_spacing)?)?;
        for column in 0..count.max(0) {
            let x = qa::add(&x_min, &qa::mul(&qa::number(column as f64), &x_spacing)?)?;
            pointings.push(format!("{epoch}{} {latitude}", qa::hms(&x)?));
        }
        y = qa::sub(&y, &y_spacing)?;
    }
    Ok(pointings)
}

/// The ALMA tilings' offsets placed about the centre, from the last offset
/// to the first. As CASA does, each offset in arcsec is written as Python
/// writes a float, with `arcsec` after it, and read back.
fn tiled(
    offsets: &[(f64, f64)],
    (epoch, centre_x, centre_y): Centre<'_>,
) -> Result<Vec<String>, SimutilError> {
    let arcsec = |value: f64| qa::read(&format!("{}arcsec", qa::python_float(value)));
    let mut pointings = Vec::with_capacity(offsets.len());
    for &(x, y) in offsets.iter().rev() {
        let latitude = qa::add(centre_y, &arcsec(y)?)?;
        let cos_latitude = qa::value_in(&latitude, "rad")?.cos();
        let longitude = qa::add(centre_x, &arcsec(x / cos_latitude)?)?;
        pointings.push(format!(
            "{epoch}{} {}",
            qa::hms(&longitude)?,
            qa::dms(&latitude)?
        ));
    }
    Ok(pointings)
}

/// Python's `num % 2 == 0`, which is floor modulo for negative numbers.
fn even(number: i64) -> bool {
    number.rem_euclid(2) == 0
}

/// `simutil.applyRotate`.
fn rotate(x: f64, y: f64, cos_angle: f64, sin_angle: f64) -> (f64, f64) {
    (cos_angle * x - sin_angle * y, sin_angle * x + cos_angle * y)
}

fn trig(angle_deg: f64) -> (f64, f64) {
    let radians = angle_deg * std::f64::consts::PI / 180.0;
    (radians.cos(), radians.sin())
}

/// Python's `int(floor(value))` for a count of rows or columns: NaN and
/// the infinities are refused with Python's messages, and a count above
/// [`MOST_POINTINGS`] as too many. A count below −[`MOST_POINTINGS`] is
/// held there; like any negative count it lays out nothing.
fn count(value: f64) -> Result<i64, SimutilError> {
    if value.is_nan() {
        return Err(SimutilError::Arithmetic(
            "cannot convert float NaN to integer".into(),
        ));
    }
    if value.is_infinite() {
        return Err(SimutilError::Arithmetic(
            "cannot convert float infinity to integer".into(),
        ));
    }
    let floor = value.floor();
    if floor > MOST_POINTINGS as f64 {
        return Err(SimutilError::TooManyPointings);
    }
    Ok(floor.max(-(MOST_POINTINGS as f64)) as i64)
}

/// Refuse a layout of `rows` rows of at most `columns` pointings each that
/// may hold more than [`MOST_POINTINGS`], before it is laid out.
fn bounded(rows: i64, columns: i64) -> Result<(), SimutilError> {
    if rows.max(0) * columns.max(0) > MOST_POINTINGS {
        return Err(SimutilError::TooManyPointings);
    }
    Ok(())
}

/// `simutil.getTrianglePoints`: the Cycle 0 tiling, offsets in arcsec.
///
/// # Errors
///
/// A count of rows or columns that is not finite, or more than
/// [`MOST_POINTINGS`] points.
pub fn triangle_points(
    width: f64,
    height: f64,
    angle_deg: f64,
    spacing: f64,
) -> Result<Vec<(f64, f64)>, SimutilError> {
    let (cos_angle, sin_angle) = trig(angle_deg);
    let mut points = Vec::new();
    let mut push = |x: f64, y: f64| points.push(rotate(x, y, cos_angle, sin_angle));
    if width >= height {
        let (w_spacing, h_spacing) = (spacing, (3.0_f64.sqrt() / 2.0) * spacing);
        let w_even = count((width / 2.0) / w_spacing)?;
        let w_odd = count((width / 2.0) / w_spacing + 0.5)?;
        let h = count((height / 2.0) / h_spacing)?;
        bounded(2 * h + 1, 2 * w_even.max(w_odd) + 1)?;
        for ih in -h..=h {
            if even(ih) {
                for iw in -w_even..=w_even {
                    push(iw as f64 * w_spacing, ih as f64 * h_spacing);
                }
            } else {
                for iw in -w_odd..=w_odd {
                    push((iw as f64 + 0.5) * w_spacing, ih as f64 * h_spacing);
                }
            }
        }
    } else {
        let (w_spacing, h_spacing) = ((3.0_f64.sqrt() / 2.0) * spacing, spacing);
        let w = count((width / 2.0) / w_spacing)?;
        let h_even = count((height / 2.0) / h_spacing)?;
        let h_odd = count((height / 2.0) / h_spacing + 0.5)?;
        bounded(2 * w + 1, 2 * h_even.max(h_odd) + 1)?;
        for iw in -w..=w {
            if even(iw) {
                for ih in -h_even..=h_even {
                    push(iw as f64 * w_spacing, ih as f64 * h_spacing);
                }
            } else {
                for ih in -h_odd..=h_odd {
                    push(iw as f64 * w_spacing, (ih as f64 + 0.5) * h_spacing);
                }
            }
        }
    }
    Ok(points)
}

/// `simutil.getTriangularTiling`: the Observing Tool's tiling (Cycle 1 on),
/// laid along the map's longer side, offsets in arcsec. An odd number of
/// rows across the short side takes the base tiling, an even number the
/// shifted one.
///
/// # Errors
///
/// As [`triangle_points`].
pub fn triangular_tiling(
    long_length: f64,
    lat_length: f64,
    angle_deg: f64,
    spacing: f64,
    beam: f64,
) -> Result<Vec<(f64, f64)>, SimutilError> {
    // The OT's landscape test is `>=`; CASA's is `>`.
    let (width, height, angle) = if long_length > lat_length {
        (long_length, lat_length, angle_deg)
    } else {
        (lat_length, long_length, angle_deg + 90.0)
    };
    let v_spacing = (3.0_f64.sqrt() / 2.0) * spacing;
    let rows = (height / v_spacing).ceil();
    if rows % 2.0 == 0.0 {
        shifted_tiling(width, height, angle, spacing, beam)
    } else {
        base_tiling(width, height, angle, spacing, beam)
    }
}

/// `simutil.needsFiller`: one more pointing when the row of `points` leaves
/// more than a beam of `length` uncovered.
fn needs_filler(length: f64, spacing: f64, beam: f64, points: i64) -> i64 {
    i64::from(length > spacing * (points - 1) as f64 + beam)
}

/// `simutil.getBaseTriangularTiling` (`y` negated, as in CASA).
fn base_tiling(
    width: f64,
    height: f64,
    angle: f64,
    spacing: f64,
    beam: f64,
) -> Result<Vec<(f64, f64)>, SimutilError> {
    let (cos_angle, sin_angle) = trig(angle);
    let (w_spacing, h_spacing) = (spacing, (3.0_f64.sqrt() / 2.0) * spacing);
    let mut w_even = count((width / 2.0) / w_spacing)?;
    w_even += needs_filler(width, w_spacing, beam, w_even * 2 + 1);
    let mut w_odd = count((width / 2.0) / w_spacing + 0.5)?;
    w_odd += needs_filler(width, w_spacing, beam, w_odd * 2);
    let mut h = count((height / 2.0) / h_spacing)?;
    h += needs_filler(height, h_spacing, beam, h * 2 + 1);
    bounded(2 * h + 1, 2 * w_even.max(w_odd) + 1)?;
    let mut points = Vec::new();
    for ih in -h..=h {
        let columns: Vec<f64> = if even(ih) {
            (-w_even..=w_even).map(|iw| iw as f64 * w_spacing).collect()
        } else {
            (-w_odd..w_odd)
                .map(|iw| (iw as f64 + 0.5) * w_spacing)
                .collect()
        };
        for x in columns {
            let (x, y) = rotate(x, ih as f64 * h_spacing, cos_angle, sin_angle);
            points.push((x, -y));
        }
    }
    Ok(points)
}

/// `simutil.getShiftTriangularTiling` (`y` negated, as in CASA).
fn shifted_tiling(
    width: f64,
    height: f64,
    angle: f64,
    spacing: f64,
    beam: f64,
) -> Result<Vec<(f64, f64)>, SimutilError> {
    let (cos_angle, sin_angle) = trig(angle);
    let (w_spacing, h_spacing) = (spacing, (3.0_f64.sqrt() / 2.0) * spacing);
    let mut w_even = count((width / 2.0) / w_spacing + 0.5)?;
    w_even += needs_filler(width, w_spacing, beam, w_even * 2);
    let mut w_odd = count((width / 2.0) / w_spacing)?;
    w_odd += needs_filler(width, w_spacing, beam, w_odd * 2 + 1);
    let mut h = count((height - h_spacing) / 2.0 / h_spacing + 1.0)?;
    h += needs_filler(height, h_spacing, beam, h * 2);
    bounded(2 * h, 2 * w_even.max(w_odd) + 1)?;
    let mut points = Vec::new();
    for ih in -h..h {
        let columns: Vec<f64> = if even(ih) {
            (-w_even..w_even)
                .map(|iw| (iw as f64 + 0.5) * w_spacing)
                .collect()
        } else {
            (-w_odd..=w_odd).map(|iw| iw as f64 * w_spacing).collect()
        };
        for x in columns {
            let (x, y) = rotate(x, (ih as f64 + 0.5) * h_spacing, cos_angle, sin_angle);
            points.push((x, -y));
        }
    }
    Ok(points)
}

/// A pointing file's pointings and their times in seconds (0 where a line
/// gives none).
#[derive(Debug, Clone, PartialEq)]
pub struct PointingFile {
    /// The pointings, as `simutil` writes directions.
    pub pointings: Vec<String>,
    /// Each line's time. A line that fails after its time is read keeps
    /// the time without a pointing, as in CASA.
    pub times: Vec<f64>,
}

/// `simutil.read_pointings(filename)`.
pub fn read_pointings(path: &Path) -> Result<PointingFile, SimutilError> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| SimutilError::Arithmetic(format!("{}: {error}", path.display())))?;
    parse_pointings(&text)
}

/// `simutil.read_pointings` of a file's text. Lines starting `#`, `RA` or
/// `--` are skipped, as are lines with fewer than three fields. A line is
/// either `epoch ra dec [time]` or an Observing Tool line,
/// `ra, dec, SEXAGESIMAL, ..., time`, whose direction is taken as ICRS (and
/// whose declination, given with colons, is read as hours and divided by
/// 15). As in CASA, reading stops silently at the first line that fails;
/// a file with no pointings is refused.
pub fn parse_pointings(text: &str) -> Result<PointingFile, SimutilError> {
    let mut file = PointingFile {
        pointings: Vec::new(),
        times: Vec::new(),
    };
    // Python's text mode turns `\r\n` and `\r` into `\n`.
    let text = text.replace("\r\n", "\n").replace('\r', "\n");
    for line in text.split_inclusive('\n') {
        if read_pointing_line(line, &mut file).is_err() {
            break;
        }
    }
    if file.pointings.is_empty() {
        return Err(SimutilError::NoPointings);
    }
    tracing::info!("read in {} pointing(s) from file", file.pointings.len());
    Ok(file)
}

fn read_pointing_line(line: &str, file: &mut PointingFile) -> Result<(), SimutilError> {
    if line.starts_with('#') || line.starts_with("RA") || line.starts_with("--") {
        return Ok(());
    }
    let time = |field: Option<&str>| match field {
        Some(field) => python_float(field).ok_or_else(|| SimutilError::Quantity(field.to_string())),
        None => Ok(0.0),
    };
    if line.find("SEXAGESIMAL").is_some_and(|at| at > 0) {
        let fields: Vec<&str> = line.split(',').collect();
        file.times.push(time(fields.get(4).copied())?);
        let longitude = qa::hms(&qa::read(fields[0])?)?;
        // CASA's `splitline[1]` raises IndexError, which stops the reading.
        let declination = fields
            .get(1)
            .copied()
            .ok_or_else(|| SimutilError::Direction(line.to_string()))?;
        let latitude = if declination.contains(':') {
            qa::dms(&qa::div(&qa::read(declination)?, &qa::number(15.0))?)?
        } else {
            qa::dms(&qa::read(declination)?)?
        };
        file.pointings.push(format!("ICRS {longitude} {latitude}"));
        return Ok(());
    }
    let fields: Vec<&str> = python_split(line).collect();
    if fields.len() > 2 {
        file.times.push(time(fields.get(3).copied())?);
        let longitude = qa::hms(&qa::read(fields[1])?)?;
        let latitude = qa::dms(&qa::read(fields[2])?)?;
        file.pointings
            .push(format!("{} {longitude} {latitude}", fields[0]));
    }
    Ok(())
}

/// `simutil.write_pointings(filename, pointings, time)`'s text: a header,
/// then each pointing, two spaces and its time. `times` are written as
/// given (Python's `str` of each); one time is used for every pointing.
pub fn format_pointings(pointings: &[String], times: &[String]) -> Result<String, SimutilError> {
    if times.len() != 1 && times.len() < pointings.len() {
        return Err(SimutilError::Times {
            pointings: pointings.len(),
            times: times.len(),
        });
    }
    let mut text = String::from("#Epoch     RA          DEC      TIME[sec]\n");
    for (index, pointing) in pointings.iter().enumerate() {
        let time = if times.len() == 1 {
            &times[0]
        } else {
            &times[index]
        };
        text.push_str(&format!("{pointing}  {time}\n"));
    }
    Ok(text)
}

/// [`format_pointings`] written to `path`, each time a float as Python
/// writes it.
pub fn write_pointings(
    path: &Path,
    pointings: &[String],
    times: &[f64],
) -> Result<(), SimutilError> {
    let times: Vec<String> = times.iter().map(|&time| qa::python_float(time)).collect();
    let text = format_pointings(pointings, &times)?;
    std::fs::write(path, text)
        .map_err(|error| SimutilError::Arithmetic(format!("{}: {error}", path.display())))
}

#[cfg(test)]
mod tests {
    use super::{Directions, MapType, SimutilError, calc_pointings, qa};

    const CENTRE: &str = "J2000 19h00m00 -23d00m00";

    /// `calc_pointings2(spacing, [size, size], maptype, CENTRE, relmargin,
    /// beam 25 arcsec)`.
    fn laid_out(
        spacing: &str,
        size: &str,
        maptype: &str,
        relmargin: f64,
    ) -> Result<Vec<String>, SimutilError> {
        let (spacing, size) = (qa::read(spacing).unwrap(), qa::read(size).unwrap());
        calc_pointings(
            &spacing,
            [&size, &size],
            maptype,
            &Directions::One(CENTRE.to_string()),
            relmargin,
            25.0,
        )
    }

    /// Where CASA's integer conversions raise (#913 review), casa-rs refuses
    /// with Python's message before it converts, allocates or loops: a zero
    /// spacing on every layout, and a margin that is not finite. A negative
    /// spacing gives the centre alone, as in CASA.
    #[test]
    fn spacings_casa_cannot_count_are_refused() {
        let refused = |spacing: &str, size: &str, maptype: &str, relmargin: f64| match laid_out(
            spacing, size, maptype, relmargin,
        ) {
            Err(SimutilError::Arithmetic(message)) => message,
            other => panic!("{spacing} {size} {maptype} {relmargin}: {other:?}"),
        };
        let infinity = "cannot convert float infinity to integer";
        assert_eq!(refused("0arcsec", "30arcsec", "hexagonal", 0.5), infinity);
        assert_eq!(refused("0arcsec", "30arcsec", "square", 0.5), infinity);
        assert_eq!(
            refused("0arcsec", "0arcsec", "hexagonal", 0.5),
            "cannot convert float NaN to integer"
        );
        for maptype in ["ALMA", "ALMA2012"] {
            assert_eq!(
                refused("0arcsec", "30arcsec", maptype, 0.5),
                "float division by zero"
            );
        }
        assert_eq!(
            refused("10arcsec", "30arcsec", "hexagonal", f64::INFINITY),
            infinity
        );
        assert_eq!(
            refused("10arcsec", "30arcsec", "square", f64::NAN),
            "cannot convert float NaN to integer"
        );
        for maptype in ["hexagonal", "square", "ALMA", "ALMA2012"] {
            assert_eq!(
                laid_out("-10arcsec", "30arcsec", maptype, 0.5).unwrap(),
                [CENTRE]
            );
        }
    }

    /// A spacing tiny beside the map, which CASA lays out for hours, is
    /// refused before any pointing is laid out.
    #[test]
    fn maps_of_too_many_pointings_are_refused() {
        for maptype in ["hexagonal", "square", "ALMA", "ALMA2012"] {
            assert_eq!(
                laid_out("1e-300arcsec", "30arcsec", maptype, 0.5),
                Err(SimutilError::TooManyPointings),
                "{maptype}"
            );
            // Rows and columns each within the bound, their product beyond.
            assert_eq!(
                laid_out("0.005arcsec", "30arcsec", maptype, 0.5),
                Err(SimutilError::TooManyPointings),
                "{maptype}"
            );
        }
    }

    /// A list of more than one direction is checked, then returned as it is.
    #[test]
    fn listed_directions_are_checked_and_returned() {
        let spacing = qa::read("10arcsec").unwrap();
        let size = qa::read("30arcsec").unwrap();
        let lay_out = |directions: &[&str]| {
            calc_pointings(
                &spacing,
                [&size, &size],
                "hexagonal",
                &Directions::List(directions.iter().map(ToString::to_string).collect()),
                0.5,
                0.0,
            )
        };
        let listed = ["J2000 10h00m00 -30d00m00", "J2000 10h00m01 -30d00m00"];
        assert_eq!(lay_out(&listed).unwrap(), listed);
        assert_eq!(
            lay_out(&["not-a-direction", "also-not-a-direction"]),
            Err(SimutilError::Direction("not-a-direction".to_string()))
        );
        assert!(lay_out(&[listed[0], "also-not-a-direction"]).is_err());
        assert!(lay_out(&[]).is_err());
    }

    #[test]
    fn map_types_go_by_their_first_characters() {
        for (name, layout) in [
            ("hexagonal", MapType::Hexagonal),
            ("hex", MapType::Hexagonal),
            ("HEXAGONAL", MapType::Hexagonal),
            ("square", MapType::Square),
            ("ALMA", MapType::Alma),
            ("alma", MapType::Alma),
            ("ALMA-OT", MapType::Alma),
            ("ALMA2012", MapType::Alma2012),
            ("alma2012", MapType::Alma2012),
            ("triangle", MapType::Unknown),
            ("", MapType::Unknown),
        ] {
            assert_eq!(MapType::from_casa(name), layout, "{name}");
        }
    }
}
