// SPDX-License-Identifier: LGPL-3.0-or-later
//! Decode the observer table and its MeasComet metadata in the Horizons reply.

use casa_core::quanta::format::{YMD, format_time};
use casa_core::quanta::{Quantity, Unit};
use casa_core::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use casa_tables::{ColumnSchema, Table, TableSchema};

const COLUMNS: [(&str, &str, &str); 14] = [
    ("MJD", "d", "Date__(UT)"),
    ("RA", "deg", "R.A.___(ICRF)___DEC"),
    ("DEC", "deg", "R.A.___(ICRF)___DEC"),
    ("Rho", "AU", "delta"),
    ("RadVel", "AU/d", "deldot"),
    ("NP_ang", "deg", "NP.ang"),
    ("NP_dist", "arcsec", "NP.dist"),
    ("DiskLong", "deg", "ObsSub-LON"),
    ("DiskLat", "deg", "ObsSub-LAT"),
    ("Sl_lon", "deg", "SunSub-LON"),
    ("Sl_lat", "deg", "SunSub-LAT"),
    ("r", "AU", "r"),
    ("rdot", "km/s", "rdot"),
    ("phang", "deg", "S-T-O"),
];

pub(super) fn ephemeris_table(response: &str) -> Result<Table, String> {
    let (preamble, body) = response
        .split_once("$$SOE")
        .ok_or("no ephemeris data found")?;
    let (body, _) = body
        .split_once("$$EOE")
        .ok_or("ephemeris data has no end marker")?;
    let heading = preamble
        .lines()
        .find(|line| line.trim_start().starts_with("Date__(UT)"))
        .ok_or("no ephemeris data header was found")?;
    let names: Vec<_> = heading.split_whitespace().collect();
    let indexes = column_indexes(&names)?;
    let radial_velocity_scale = Unit::new("AU/d")
        .and_then(|unit| Quantity::new(1., "km/s")?.get_value_in(&unit))
        .map_err(|error| error.to_string())?;
    let mut rows: Vec<[f64; COLUMNS.len()]> = Vec::new();
    for line in body.lines().filter(|line| !line.trim().is_empty()) {
        let mut row = parse_row(line, &indexes, names.len() + 2)?;
        row[4] *= radial_velocity_scale;
        if rows.last().is_some_and(|previous| previous[0] == row[0]) {
            return Err(format!(
                "duplicated timestamp in ephemeris data: {}",
                row[0]
            ));
        }
        rows.push(row);
    }
    let first = rows.first().ok_or("no ephemeris data found")?[0];
    let last = rows.last().expect("nonempty ephemeris")[0];
    let metadata = metadata(preamble, &rows, first, last)?;
    let schema = TableSchema::new(
        COLUMNS
            .iter()
            .map(|(column, _, _)| ColumnSchema::scalar(*column, PrimitiveType::Float64))
            .collect(),
    )
    .map_err(|error| error.to_string())?;
    let mut table = Table::with_schema(schema);
    for row in rows {
        table
            .add_row(RecordValue::new(
                COLUMNS
                    .iter()
                    .zip(row)
                    .map(|((column, _, _), value)| RecordField::new(*column, number(value)))
                    .collect(),
            ))
            .map_err(|error| error.to_string())?;
    }
    *table.keywords_mut() = metadata;
    for (column, unit, _) in COLUMNS {
        table.set_column_keywords(
            column,
            RecordValue::new(vec![RecordField::new(
                "QuantumUnits",
                Value::Array(ArrayValue::from_string_vec(vec![unit.into()])),
            )]),
        );
    }
    table.set_info(casa_tables::TableInfo {
        table_type: "IERS".to_owned(),
        sub_type: "Comet".to_owned(),
        readme: vec!["Derivied by jplhorizons-query from JPL-Horizons API (https://ssd.jpl.nasa.gov/api/horizons.api)".to_owned()],
    });
    Ok(table)
}

fn column_indexes(names: &[&str]) -> Result<[usize; COLUMNS.len()], String> {
    let mut indexes = [0; COLUMNS.len()];
    for (index, (column, _, heading)) in COLUMNS.into_iter().enumerate().skip(1) {
        let position = names
            .iter()
            .position(|name| *name == heading)
            .ok_or_else(|| format!("missing Horizons column {heading}"))?;
        indexes[index] = position
            + match column {
                "RA" => 1,
                _ => 2,
            };
    }
    Ok(indexes)
}

fn parse_row(
    line: &str,
    indexes: &[usize; COLUMNS.len()],
    expected: usize,
) -> Result<[f64; COLUMNS.len()], String> {
    let fields: Vec<_> = line.split_whitespace().collect();
    // Topocentric data may carry a solar/lunar presence code immediately
    // after its calendar date. It is optional even within the same table.
    if !matches!(fields.len().checked_sub(expected), Some(0 | 1)) {
        return Err(format!(
            "unexpected number of Horizons data items: {} (expected {expected})",
            fields.len()
        ));
    }
    let offset = fields.len() - expected;
    let mut row = [0.; COLUMNS.len()];
    row[0] = calendar(fields[0], fields[1]).ok_or("invalid Horizons calendar row")?;
    for (index, &source) in indexes.iter().enumerate().skip(1) {
        let value = fields
            .get(source + offset)
            .ok_or("short Horizons data row")?;
        row[index] = if *value == "n.a." {
            -999.
        } else {
            value
                .parse()
                .map_err(|_| format!("invalid Horizons data item {value}"))?
        };
    }
    Ok(row)
}

fn metadata(
    preamble: &str,
    rows: &[[f64; COLUMNS.len()]],
    first: f64,
    last: f64,
) -> Result<RecordValue, String> {
    let mut keys = RecordValue::new(Vec::new());
    keys.upsert("VS_VERSION", text("0004.0001"));
    keys.upsert("VS_TYPE", text("Table of comet/planetary positions"));
    let whole_minutes = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_secs()
        / 60;
    let now = format_time(whole_minutes as f64 / 1_440. + 40_587., YMD, 6);
    keys.upsert(
        "VS_DATE",
        text(now.rsplit_once(':').map_or(&now, |(minute, _)| minute)),
    );
    keys.upsert("posrefsys", text("ICRS"));
    let mut start = first;
    let mut stop = last;
    let mut step = None;
    let mut synchronous = false;
    for line in preamble.lines().map(str::trim_start) {
        if line.starts_with("Ephemeris / ") {
            let words: Vec<_> = line.split_whitespace().collect();
            if let [_, _, _, _, month, day, time, year, ..] = words.as_slice()
                && let (Some(month), Ok(day), Some(time)) =
                    (month_index(month), day.parse::<u32>(), time.get(..5))
            {
                keys.upsert(
                    "VS_CREATE",
                    text(&format!("{year}/{month:02}/{day:02}/{time}")),
                );
            }
        } else if let Some((label, value)) = line.trim_start_matches('>').split_once(':') {
            let value = value.trim();
            match label.trim() {
                "Target body name" => {
                    let (name, source) = object_name(value);
                    keys.upsert("NAME", text(name));
                    if let Some(source) = source {
                        keys.upsert("ephemeris_source", text(source));
                    }
                }
                "Start time" => start = header_epoch(value).unwrap_or(start),
                "Stop  time" => stop = header_epoch(value).unwrap_or(stop),
                "Step-size" => step = Some(value),
                "Center-site name" => {
                    let name = if value.contains("ALMA") {
                        "ALMA"
                    } else if value.contains("VLA") {
                        "VLA"
                    } else if value.contains("Green Bank") {
                        "GBT"
                    } else {
                        value
                    };
                    keys.upsert("obsloc", text(name));
                }
                "Center geodetic" => {
                    if let Some([lon, lat, distance]) =
                        triplet(value.split('{').next().unwrap_or(value))
                    {
                        keys.upsert("GeoLong", number(lon));
                        keys.upsert("GeoLat", number(lat));
                        keys.upsert("GeoDist", number(distance));
                    }
                }
                "Target radii" => {
                    if let Some(radii) = value.split_once("km").and_then(|(radii, _)| {
                        triplet(radii)
                            .or_else(|| radii.trim().parse().ok().map(|radius| [radius; 3]))
                    }) {
                        let eq = (radii[0] + radii[1]) / 2.;
                        let mean = rows
                            .iter()
                            .map(|row| {
                                // This uses CASA's historical latitude scaling.
                                let latitude = if row[8] == -999. {
                                    0.
                                } else {
                                    row[8].to_degrees()
                                };
                                (eq * (eq * latitude.sin()).hypot(radii[2] * latitude.cos())).sqrt()
                            })
                            .sum::<f64>()
                            / rows.len() as f64;
                        keys.upsert(
                            "radii",
                            quantity("km", Value::Array(ArrayValue::from_f64_vec(radii.to_vec()))),
                        );
                        keys.upsert("meanrad", quantity("km", number(mean)));
                    }
                }
                _ => {}
            }
        }
        if keys.get("orb_per").is_none()
            && let Some((value, unit)) = period(
                line,
                &["Orbital period", "orb. per.,", "orb per", "orbit period"],
            )
        {
            keys.upsert("orb_per", quantity(&unit, number(value)));
        }
        if line.contains("Rotational period") && line.contains("Synchronous") {
            synchronous = true;
        } else if let Some((value, unit)) = period(
            &line.to_ascii_lowercase(),
            &["rot. period", "rot. per.", "rotational period", "rotper"],
        ) {
            keys.upsert("rot_per", quantity(&unit, number(value)));
        }
        if let Some((value, _)) = period(line, &["Mean Temperature", "Mean temperature"]) {
            keys.upsert("T_mean", quantity("K", number(value)));
        }
    }
    if synchronous {
        let period = keys
            .get("orb_per")
            .cloned()
            .unwrap_or_else(|| text("Synchronous"));
        keys.upsert("rot_per", period);
    }
    let step = step
        .map(|step| step_days(step, start, stop))
        .transpose()?
        .unwrap_or_else(|| {
            if rows.len() > 1 {
                rows[1][0] - first
            } else {
                0.
            }
        });
    if step <= 0. || !step.is_finite() {
        return Err("Horizons ephemeris step must be positive".into());
    }
    keys.upsert("dMJD", number(step));
    keys.upsert("MJD0", number(start - step));
    keys.upsert("earliest", epoch(first));
    keys.upsert("latest", epoch(last));
    Ok(keys)
}

fn object_name(value: &str) -> (&str, Option<&str>) {
    let (name, source) = value
        .split_once("{source:")
        .map_or((value, None), |(name, source)| {
            (name, Some(source.trim().trim_end_matches('}').trim()))
        });
    let mut name = name.trim();
    if let Some((first, second)) = name.trim_end_matches(')').rsplit_once(" (") {
        name = if second.trim().parse::<f64>().is_ok() {
            first.trim()
        } else {
            second.trim()
        };
    }
    (name, source)
}

fn period(line: &str, labels: &[&str]) -> Option<(f64, String)> {
    let label = labels
        .iter()
        .find_map(|label| line.split_once(label).map(|(_, tail)| tail))?;
    let (prelude, value) = label.split_once(['=', '~'])?;
    let value = value.trim();
    if labels.contains(&"rotper")
        && let Some(hours) = rotational_clock_hours(value)
    {
        return Some((hours, "h".into()));
    }
    let end = value
        .find(|ch: char| !matches!(ch, '0'..='9' | '.' | '-'))
        .unwrap_or(value.len());
    let number = value[..end].parse().ok()?;
    let unit = value[end..]
        .split_whitespace()
        .next()
        .filter(|unit| unit.chars().all(char::is_alphabetic))
        .or_else(|| {
            prelude
                .trim()
                .chars()
                .all(char::is_alphabetic)
                .then_some(prelude.trim())
                .filter(|unit| !unit.is_empty())
        })
        .unwrap_or(if labels.contains(&"rotper") { "h" } else { "d" });
    Some((number, unit.to_owned()))
}

fn rotational_clock_hours(value: &str) -> Option<f64> {
    let (hours, rest) = value.split_once('h')?;
    let (minutes, rest) = rest.split_once('m')?;
    let (seconds, _) = rest.split_once('s')?;
    let parse = |value: &str| value.trim().parse::<f64>().ok();
    Some(parse(hours)? + parse(minutes)? / 60. + parse(seconds)? / 3_600.)
}

fn step_days(value: &str, start: f64, stop: f64) -> Result<f64, String> {
    let mut words = value.split_whitespace();
    let count: f64 = words
        .next()
        .and_then(|value| value.parse().ok())
        .ok_or("invalid Horizons step size")?;
    let unit = words.next().ok_or("no Horizons step unit")?;
    match unit {
        "minutes" => Ok(count / 1_440.),
        "hours" => Ok(count / 24.),
        "days" => Ok(count),
        "steps" => Ok((stop - start) / count),
        "calendar" => Err("Unit of Step-size in calendar month or year is not supported".into()),
        _ => Err(format!("Unit of Step-size, {unit} is not recognized")),
    }
}

fn triplet(value: &str) -> Option<[f64; 3]> {
    let mut fields = value
        .split([',', 'x'])
        .map(|value| value.trim().parse::<f64>());
    Some([
        fields.next()?.ok()?,
        fields.next()?.ok()?,
        fields.next()?.ok()?,
    ])
}

fn header_epoch(value: &str) -> Option<f64> {
    let mut fields = value.split_whitespace();
    fields.next()?; // A.D.
    calendar(fields.next()?, fields.next()?)
}

fn calendar(date: &str, time: &str) -> Option<f64> {
    let (year_month, day) = date.rsplit_once('-')?;
    let (year, month) = year_month.rsplit_once('-')?;
    let month = month_index(month)?;
    Quantity::read(&format!("{year}/{month:02}/{day}/{time}"))
        .ok()
        .map(|epoch| epoch.value())
}

fn month_index(month: &str) -> Option<u32> {
    month.parse().ok().or_else(|| {
        [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
        ]
        .iter()
        .position(|name| name.eq_ignore_ascii_case(month))
        .map(|index| index as u32 + 1)
    })
}

fn number(value: f64) -> Value {
    Value::Scalar(ScalarValue::Float64(value))
}

fn text(value: &str) -> Value {
    Value::Scalar(ScalarValue::String(value.into()))
}

fn quantity(unit: &str, value: Value) -> Value {
    Value::Record(RecordValue::new(vec![
        RecordField::new("unit", text(unit)),
        RecordField::new("value", value),
    ]))
}

fn epoch(mjd: f64) -> Value {
    Value::Record(RecordValue::new(vec![
        RecordField::new("m0", quantity("d", number(mjd))),
        RecordField::new("refer", text("UTC")),
        RecordField::new("type", text("epoch")),
    ]))
}
