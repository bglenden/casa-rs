// SPDX-License-Identifier: LGPL-3.0-or-later
//! Restore image history from FITS HISTORY cards as casacore does.

use std::io::{BufReader, Read};
use std::path::Path;

use crate::{
    GaussianBeam, ImageError,
    image::{ImageHistoryEntry, ImageHistoryRow},
};

pub(super) fn restoring_beam_from_history(rows: &[ImageHistoryRow]) -> Option<GaussianBeam> {
    let keys = ["BMAJ", "BMIN", "BPA"];
    let line = rows
        .iter()
        .map(|row| row.entry.message.as_str())
        .find(|line| keys.iter().all(|key| line.contains(key)))?;
    let [Some(major), Some(minor), Some(pa)] = keys.map(|key| {
        line.split_whitespace()
            .skip_while(|field| !field.contains(key))
            .nth(1)?
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
    }) else {
        return None;
    };
    (major >= minor && minor > 0.0)
        .then(|| GaussianBeam::new(major.to_radians(), minor.to_radians(), pa.to_radians()))
}

pub(crate) fn history_rows(path: &Path) -> Result<Vec<ImageHistoryRow>, ImageError> {
    let cards = read_cards(path)?;
    Ok(history_from_cards(&cards, false))
}

pub(super) fn history_from_cards(
    cards: &[String],
    preserve_casacore_location: bool,
) -> Vec<ImageHistoryRow> {
    let cards = cards.iter().filter_map(|card| {
        card.strip_prefix("HISTORY ")
            .map(|line| line.trim_end().to_string())
    });
    let mut entries = Vec::new();
    let mut group = String::new();
    let mut lines: Vec<String> = Vec::new();
    for line in cards {
        let normalized = line.trim_start();
        let mut words = normalized.split_whitespace();
        let first = words.next().unwrap_or("");
        let second = words.next().unwrap_or("");
        if first.eq_ignore_ascii_case("CASA") && second.eq_ignore_ascii_case("START") {
            append_group(&mut entries, &group, &lines, preserve_casacore_location);
            lines.clear();
            group = words.collect::<String>();
        } else if first.eq_ignore_ascii_case("CASA") && second.eq_ignore_ascii_case("END") {
            append_group(&mut entries, &group, &lines, preserve_casacore_location);
            lines.clear();
            group.clear();
        } else if let Some(continuation) = line.strip_prefix('>') {
            if let Some(previous) = lines.last_mut() {
                previous.push_str(continuation);
            } else {
                lines.push(line);
            }
        } else {
            lines.push(line);
        }
    }
    append_group(&mut entries, &group, &lines, preserve_casacore_location);
    entries
}

pub fn read_cards(path: &Path) -> Result<Vec<String>, ImageError> {
    let mut file = BufReader::new(
        std::fs::File::open(path).map_err(|error| ImageError::Io(error.to_string()))?,
    );
    let mut cards = Vec::new();
    loop {
        let mut card = [0; 80];
        file.read_exact(&mut card)
            .map_err(|error| ImageError::Io(error.to_string()))?;
        if &card[..8] == b"END     " {
            break;
        }
        cards.push(String::from_utf8_lossy(&card).into_owned());
    }
    Ok(cards)
}

fn append_group(
    entries: &mut Vec<ImageHistoryRow>,
    group: &str,
    lines: &[String],
    preserve_casacore_location: bool,
) {
    if group == "LOGTABLE" && lines.len().is_multiple_of(2) {
        for pair in lines.as_chunks::<2>().0 {
            let header = &pair[0];
            let mut fields = header.split_whitespace();
            let date = fields.next().unwrap_or("");
            let priority = fields
                .next()
                .filter(|value| !value.contains('='))
                .unwrap_or("INFO");
            let quoted = |prefix, consume_suffix| {
                header
                    .split_once(prefix)
                    .and_then(|(_, rest)| {
                        if consume_suffix {
                            rest.rsplit_once('\'')
                        } else {
                            rest.split_once('\'')
                        }
                    })
                    .map_or("", |(value, _)| value)
            };
            entries.push(ImageHistoryRow {
                entry: ImageHistoryEntry {
                    time_mjd_seconds: parse_time(date).unwrap_or(-1.0),
                    priority: priority.into(),
                    location: quoted("SRCCODE='", preserve_casacore_location).into(),
                    message: pair[1].clone(),
                },
                object_id: quoted("OBJID='", false).into(),
            });
        }
    } else if group.is_empty() || group == "LOGTABLE" {
        entries.extend(lines.iter().map(|line| {
            ImageHistoryRow::informational(ImageHistoryEntry {
                time_mjd_seconds: -1.0,
                priority: "INFO".into(),
                location: String::new(),
                message: line.clone(),
            })
        }));
    }
}

fn parse_time(value: &str) -> Option<f64> {
    let (date, clock) = value.split_once('T').unwrap_or((value, "00:00:00"));
    let mut date = date.split('-');
    let (year, month, day) = (
        date.next()?.parse().ok()?,
        date.next()?.parse().ok()?,
        date.next()?.parse().ok()?,
    );
    let mut clock = clock.split(':');
    let (hour, minute, second) = (
        clock.next()?.parse().ok()?,
        clock.next()?.parse().ok()?,
        clock.next()?.parse().ok()?,
    );
    casa_core::quanta::time::MvTime::from_ymd_hms_utc(year, month, day, hour, minute, second)
        .map(|time| time.as_mjd_seconds())
}
