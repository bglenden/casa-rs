// SPDX-License-Identifier: LGPL-3.0-or-later
//! Source and field identities from ASAP scan-table rows.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use casa_core::{ArrayValue, RecordValue, Value};
use casa_tables::Table;

use super::asap_row::{doubles, integer, invalid, number, string};
use super::{DAY, direction, tag_rows};
use crate::single_dish_import::{Field, Source};
use casa_ms::MsResult;

pub(super) fn split_sources(
    rows: &[&RecordValue],
    molecules: &Table,
) -> MsResult<(Vec<Source>, HashMap<String, i32>)> {
    let molecule_rows = tag_rows(molecules)?;
    let mut sources = BTreeMap::new();
    for &row in rows {
        let key = (string(row, "SRCNAME")?, integer(row, "IFNO")?);
        let time = number(row, "TIME")?;
        let interval = number(row, "INTERVAL")?;
        let times = sources
            .entry(key)
            .or_insert((row, time, time, interval, interval));
        if time < times.1 {
            times.1 = time;
            times.3 = interval;
        }
        if time > times.2 {
            times.2 = time;
            times.4 = interval;
        }
    }
    let mut ids = HashMap::new();
    let mut result = Vec::with_capacity(sources.len());
    for ((name, window), (row, first, last, first_interval, last_interval)) in sources {
        let next_id = ids.len() as i32;
        let id = *ids.entry(name.to_string()).or_insert(next_id);
        let before = first * DAY - 0.5 * first_interval;
        let after = last * DAY + 0.5 * last_interval;
        let molecule = molecule_rows.get(&integer(row, "MOLECULE_ID")?);
        let rest_frequencies = molecule
            .and_then(|value| doubles(value, "RESTFREQUENCY").ok())
            .unwrap_or_default();
        let transitions = match molecule.and_then(|value| value.get("NAME")) {
            Some(Value::Array(ArrayValue::String(values))) => values.iter().cloned().collect(),
            _ => Vec::new(),
        };
        result.push(Source {
            id,
            name: name.into(),
            window,
            time: 0.5 * (before + after),
            interval: after - before,
            direction: doubles(row, "SRCDIRECTION")?
                .try_into()
                .map_err(|_| invalid("SRCDIRECTION"))?,
            proper_motion: doubles(row, "SRCPROPERMOTION")?
                .try_into()
                .map_err(|_| invalid("SRCPROPERMOTION"))?,
            rest_frequencies,
            transitions,
            velocity: number(row, "SRCVELOCITY")?,
        });
    }
    Ok((result, ids))
}

pub(super) fn fields(
    rows: &[&RecordValue],
    sources: &HashMap<String, i32>,
) -> MsResult<(Vec<Field>, HashMap<String, i32>)> {
    let mut by_name = BTreeMap::new();
    for &row in rows {
        by_name.entry(string(row, "FIELDNAME")?).or_insert(row);
    }
    let mut available = (0..by_name.len() as i32).collect::<BTreeSet<_>>();
    let mut ids = HashMap::new();
    let mut result = Vec::with_capacity(by_name.len());
    for (label, row) in by_name {
        let (name, claim) = label.split_once("__").unwrap_or((label, ""));
        let requested = claim.parse::<i32>().ok().filter(|id| available.remove(id));
        let id = requested
            .or_else(|| available.pop_first())
            .ok_or_else(|| invalid("FIELDNAME"))?;
        ids.insert(label.to_string(), id);
        result.push(Field {
            id,
            name: name.into(),
            source_id: sources[string(row, "SRCNAME")?],
            time: number(row, "TIME")? * DAY,
            direction: direction(row, "SRCDIRECTION")?,
        });
    }
    result.sort_unstable_by_key(|field| field.id);
    Ok((result, ids))
}
