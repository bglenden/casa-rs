// SPDX-License-Identifier: LGPL-3.0-or-later
//! Directions as `simutil` writes them: `'J2000 18:00:00.03100
//! -022.59.59.600000'`, an epoch, a longitude in `qa.formxxx`'s `hms` and a
//! latitude in its `dms`, both at precision 5.

use casa_types::quanta::Quantity;

use super::{SimutilError, qa};
use crate::readantenna::parse::python_split;

/// A direction as `simutil.direction_splitter` splits it.
#[derive(Debug, Clone, PartialEq)]
pub struct SplitDirection {
    /// The epoch with a space after it (`"J2000 "`), or empty when the
    /// direction has not exactly three fields.
    pub epoch: String,
    /// The longitude, in degrees.
    pub longitude: Quantity,
    /// The latitude, in degrees.
    pub latitude: Quantity,
}

/// `simutil.direction_splitter(direction)`: the epoch and the two angles of
/// a direction string, in degrees.
///
/// As CASA does, the longitude is the second field and the latitude the
/// third, whatever the number of fields, and the epoch is kept only when
/// there are exactly three. The latitude's three numbers are found by
/// CASA's regular expression `([+-]?\d+).(\d+).(\d+\.?\d*)` and read back as
/// `<d>d<m>m<s>s`, since casacore reads a string with colons as hours. A
/// direction with fewer than three fields (CASA raises `IndexError`) or a
/// latitude the expression does not match (CASA raises `AttributeError`) is
/// refused.
pub fn split_direction(direction: &str) -> Result<SplitDirection, SimutilError> {
    let refused = || SimutilError::Direction(direction.to_string());
    let fields: Vec<&str> = python_split(direction).collect();
    if fields.len() < 3 {
        return Err(refused());
    }
    let epoch = if fields.len() == 3 {
        format!("{} ", fields[0])
    } else {
        String::new()
    };
    let longitude = qa::to_angle(&qa::read(fields[1])?)?;
    let (degrees, minutes, seconds) = latitude_fields(fields[2]).ok_or_else(refused)?;
    let (degrees, minutes, seconds) = (&degrees, &minutes, &seconds);
    let latitude = qa::to_angle(&qa::read(&format!("{degrees}d{minutes}m{seconds}s"))?)?;
    Ok(SplitDirection {
        epoch,
        longitude: qa::convert(&longitude, "deg")?,
        latitude: qa::convert(&latitude, "deg")?,
    })
}

/// Python's `re.match('([+-]?\d+).(\d+).(\d+\.?\d*)', text).groups()`,
/// tried in the order Python's backtracking tries it: the first group as
/// long as possible, then the second. `.` is any one character but a
/// newline.
fn latitude_fields(text: &str) -> Option<(String, String, String)> {
    let chars: Vec<char> = text.chars().collect();
    let digits_from = |start: usize| {
        chars[start.min(chars.len())..]
            .iter()
            .take_while(|c| c.is_ascii_digit())
            .count()
    };
    let any_at = |at: usize| chars.get(at).is_some_and(|c| *c != '\n');
    let group = |from: usize, to: usize| chars[from..to].iter().collect::<String>();
    let sign = usize::from(matches!(chars.first(), Some('+' | '-')));
    for first_end in (sign + 1..=sign + digits_from(sign)).rev() {
        if !any_at(first_end) {
            continue;
        }
        let second = first_end + 1;
        for second_end in (second + 1..=second + digits_from(second)).rev() {
            if !any_at(second_end) {
                continue;
            }
            let third = second_end + 1;
            let mut third_end = third + digits_from(third);
            if third_end == third {
                continue;
            }
            if chars.get(third_end) == Some(&'.') {
                third_end += 1 + digits_from(third_end + 1);
            }
            return Some((
                group(0, first_end),
                group(second, second_end),
                group(third, third_end),
            ));
        }
    }
    None
}

/// `simutil.isdirection(text)`: whether `text` splits as a direction.
/// casatools' `me.direction` accepts any reference code it is then given
/// (an unknown one falls back to J2000), so splitting is the whole test.
pub fn is_direction(text: &str) -> bool {
    split_direction(text).is_ok()
}

/// A direction as `simutil`'s methods take one (`direction`, which CASA's
/// tasks pass as a string or a list): a string, or a list of strings.
#[derive(Debug, Clone, PartialEq)]
pub enum Directions {
    /// One direction string.
    One(String),
    /// A list of direction strings, even of one.
    List(Vec<String>),
}

impl Directions {
    /// `simutil.direction_splitter(direction)`: a string split as it is, or
    /// a list, even of one direction, split at its mean
    /// ([`average_direction`]), which CASA writes at precision 5 and reads
    /// back. Every direction of a list is split, so one that does not split
    /// refuses the list, as `simutil.isdirection` does.
    ///
    /// # Errors
    ///
    /// A direction that does not split, or an empty list.
    pub fn split(&self) -> Result<SplitDirection, SimutilError> {
        match self {
            Self::One(direction) => split_direction(direction),
            Self::List(directions) => split_direction(&average_direction(directions)?.centre),
        }
    }
}

/// `simutil.dir_m2s`: a direction measure's reference code and its
/// longitude and latitude as `simutil` writes them.
pub fn format_direction(
    refer: &str,
    longitude: &Quantity,
    latitude: &Quantity,
) -> Result<String, SimutilError> {
    Ok(format!(
        "{refer} {} {}",
        qa::hms(longitude)?,
        qa::dms(latitude)?
    ))
}

/// `simutil.wrapang(ang, target, period)`: `angle` moved by whole periods
/// to within half a period of `target`.
pub fn wrap_angle(angle: f64, target: f64, period: f64) -> f64 {
    let difference = angle - target;
    let period = period.abs();
    if difference.abs() > 0.5 * period {
        let wraps = (0.5 + difference / period).floor();
        angle - wraps * period
    } else {
        angle
    }
}

/// A direction with the offsets of the directions it summarises, in
/// degrees on the sky: `[(longitude − centre) × cos(centre latitude),
/// latitude − centre]`.
#[derive(Debug, Clone, PartialEq)]
pub struct CentredDirections {
    /// The centre, as `simutil` writes directions.
    pub centre: String,
    /// Each direction's offset from the centre.
    pub offsets: Vec<[f64; 2]>,
}

/// One direction's angles in degrees, as `qa.convert(x, 'deg')['value']`.
fn degrees(direction: &str) -> Result<(String, f64, f64), SimutilError> {
    let split = split_direction(direction)?;
    Ok((
        split.epoch,
        qa::value_in(&split.longitude, "deg")?,
        qa::value_in(&split.latitude, "deg")?,
    ))
}

/// `simutil.average_direction(directions)`: the running mean of the
/// longitudes (each wrapped to within 180° of the mean so far) and of the
/// latitudes, with the first direction's epoch. A direction in another
/// epoch is averaged as it is (CASA warns that precession is not handled).
pub fn average_direction(directions: &[String]) -> Result<CentredDirections, SimutilError> {
    let epoch = first_epoch(directions)?;
    let (mut longitude, mut latitude) = (0.0, 0.0);
    for (count, direction) in directions.iter().enumerate() {
        let (other, x, y) = degrees(direction)?;
        warn_other_epoch(&epoch, &other);
        let x = wrap_angle(x, longitude, 360.0);
        let n = (count + 1) as f64;
        longitude += (x - longitude) / n;
        latitude += (y - latitude) / n;
    }
    centred(directions, &epoch, longitude, latitude)
}

/// `simutil.median_direction(directions)`: NumPy's median of the longitudes
/// and of the latitudes. As in CASA, the longitudes are first wrapped to
/// within 180° of 0°, not of each other.
pub fn median_direction(directions: &[String]) -> Result<CentredDirections, SimutilError> {
    let epoch = first_epoch(directions)?;
    let (mut longitudes, mut latitudes) = (Vec::new(), Vec::new());
    for direction in directions {
        let (other, x, y) = degrees(direction)?;
        warn_other_epoch(&epoch, &other);
        longitudes.push(wrap_angle(x, 0.0, 360.0));
        latitudes.push(y);
    }
    centred(directions, &epoch, median(longitudes), median(latitudes))
}

fn first_epoch(directions: &[String]) -> Result<String, SimutilError> {
    let first = directions
        .first()
        .ok_or_else(|| SimutilError::Direction(String::new()))?;
    Ok(split_direction(first)?.epoch)
}

fn warn_other_epoch(epoch: &str, other: &str) {
    if other != epoch {
        tracing::warn!("[simutil] WARN: precession not handled by average_direction()");
    }
}

/// NumPy's `median`: the middle value, or the mean of the two middle ones.
fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len() % 2 == 1 {
        values[middle]
    } else {
        (values[middle - 1] + values[middle]) / 2.0
    }
}

/// The centre's string and each direction's offset from it. CASA writes
/// the centre's angles with `'%17.12fdeg'` and reads them back before
/// formatting them.
fn centred(
    directions: &[String],
    epoch: &str,
    longitude: f64,
    latitude: f64,
) -> Result<CentredDirections, SimutilError> {
    let cos_latitude = (latitude * std::f64::consts::PI / 180.0).cos();
    let mut offsets = Vec::with_capacity(directions.len());
    for direction in directions {
        let (_, x, y) = degrees(direction)?;
        let x = wrap_angle(x, longitude, 360.0);
        offsets.push([(x - longitude) * cos_latitude, y - latitude]);
    }
    let reread = |degrees: f64| qa::to_angle(&qa::read(&format!("{degrees:17.12}deg"))?);
    let centre = format!(
        "{epoch}{} {}",
        qa::hms(&reread(longitude)?)?,
        qa::dms(&reread(latitude)?)?
    );
    Ok(CentredDirections { centre, offsets })
}

#[cfg(test)]
mod tests {
    use super::{Directions, latitude_fields, split_direction};

    fn fields(text: &str) -> Option<(String, String, String)> {
        latitude_fields(text)
    }

    fn owned(a: &str, b: &str, c: &str) -> Option<(String, String, String)> {
        Some((a.to_string(), b.to_string(), c.to_string()))
    }

    #[test]
    fn latitudes_split_as_casas_expression_matches() {
        assert_eq!(
            fields("-023.00.38.805516"),
            owned("-023", "00", "38.805516")
        );
        assert_eq!(fields("-22d59m59.6s"), owned("-22", "59", "59.6"));
        assert_eq!(fields("+20d00m00"), owned("+20", "00", "00"));
        assert_eq!(fields("-30°15′20.5″"), owned("-30", "15", "20.5"));
        // Python backtracks into the digits when there is no separator.
        assert_eq!(fields("123456"), owned("12", "4", "6"));
        assert_eq!(fields("-23d00m"), None);
        assert_eq!(fields("20.0deg"), None);
    }

    /// A list of one direction is split at its mean, written at precision
    /// 5 and read back, so it differs from the string in the last digits
    /// (#913 review); CASA's values, from `direction_splitter`.
    #[test]
    fn a_list_of_one_direction_splits_at_its_rounded_mean() {
        let direction = "J2000 18h00m00.123456789s -23d00m00.123456789s";
        let one = Directions::One(direction.to_string()).split().unwrap();
        let listed = Directions::List(vec![direction.to_string()])
            .split()
            .unwrap();
        assert_eq!(one, split_direction(direction).unwrap());
        assert_eq!(listed.epoch, "J2000 ");
        assert!((one.longitude.value() - 270.000_514_403_287_5).abs() < 1e-12);
        assert!((listed.longitude.value() - 270.000_514_416_666_65).abs() < 1e-12);
        assert!((listed.latitude.value() + 23.000_034_293_611_11).abs() < 1e-12);
        assert!(Directions::List(Vec::new()).split().is_err());
        assert!(
            Directions::List(vec![direction.to_string(), "not-a-direction".to_string()])
                .split()
                .is_err()
        );
    }
}
