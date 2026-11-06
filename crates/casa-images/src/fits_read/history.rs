// SPDX-License-Identifier: LGPL-3.0-or-later
//! Restore image history from FITS HISTORY cards as casacore does.

use std::io::{BufReader, Read};
use std::path::Path;

use crate::{ImageError, image::ImageHistoryEntry};

pub(crate) fn history_entries(path: &Path) -> Result<Vec<ImageHistoryEntry>, ImageError> {
    let cards = read_cards(path)?;
    let cards = cards.into_iter().filter_map(|card| {
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
            append_group(&mut entries, &group, &lines);
            lines.clear();
            group = words.collect::<String>();
        } else if first.eq_ignore_ascii_case("CASA") && second.eq_ignore_ascii_case("END") {
            append_group(&mut entries, &group, &lines);
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
    append_group(&mut entries, &group, &lines);
    Ok(entries)
}

pub(crate) fn read_cards(path: &Path) -> Result<Vec<String>, ImageError> {
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

fn append_group(entries: &mut Vec<ImageHistoryEntry>, group: &str, lines: &[String]) {
    if group == "LOGTABLE" && lines.len().is_multiple_of(2) {
        for pair in lines.as_chunks::<2>().0 {
            let header = &pair[0];
            let date = header.split_whitespace().next().unwrap_or("");
            let location = header
                .split_once("SRCCODE='")
                .and_then(|(_, rest)| rest.split_once('\''))
                .map_or("", |(location, _)| location);
            entries.push(ImageHistoryEntry {
                time_mjd_seconds: parse_time(date).unwrap_or(-1.0),
                location: location.to_string(),
                message: pair[1].clone(),
            });
        }
    } else if group.is_empty() || group == "LOGTABLE" {
        entries.extend(lines.iter().map(|line| ImageHistoryEntry {
            time_mjd_seconds: -1.0,
            location: String::new(),
            message: line.clone(),
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
    casa_types::quanta::time::MvTime::from_ymd_hms_utc(year, month, day, hour, minute, second)
        .map(|time| time.as_mjd_seconds())
}
