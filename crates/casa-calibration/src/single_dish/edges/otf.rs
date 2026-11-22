// SPDX-License-Identifier: LGPL-3.0-or-later
//! Select reference integrations at the spatial boundary of each OTF map.

use std::collections::BTreeMap;

use casa_ms::MeasurementSet;
use casa_ms::derived::engine::MsCalEngine;
use casa_types::measures::direction::{DirectionRef, MDirection};

use super::{Integration, error_text, mask};

pub(super) fn reference_rows<'a>(
    ms: &MeasurementSet,
    integrations: &'a [Integration],
    fraction: f64,
) -> Result<Vec<&'a Integration>, String> {
    let mut groups = BTreeMap::<_, Vec<_>>::new();
    for sample in integrations {
        if sample.antenna == sample.other_antenna {
            groups
                .entry((sample.field, sample.antenna, sample.window))
                .or_default()
                .push(sample);
        }
    }
    let mut groups = groups.into_values().collect::<Vec<_>>();
    for group in &mut groups {
        group.sort_by(|left, right| left.time.total_cmp(&right.time));
    }
    let engine = MsCalEngine::new(ms)
        .and_then(|engine| engine.with_field_ephemerides(ms))
        .map_err(error_text)?;
    let field = ms.field().map_err(error_text)?;
    let source = ms.source().map_err(error_text)?;
    let mut names = BTreeMap::new();
    for row in 0..source.row_count() {
        let id = source.i32(row, "SOURCE_ID").map_err(error_text)?;
        if let std::collections::btree_map::Entry::Vacant(entry) = names.entry(id) {
            entry.insert(source.string(row, "NAME").map_err(error_text)?);
        }
    }
    let samples = groups
        .iter()
        .flatten()
        .map(|row| (row.antenna, row.time))
        .collect::<Vec<_>>();
    let directions = ms
        .pointing()
        .and_then(|pointing| pointing.scan_directions(&samples, &engine))
        .map_err(error_text)?;
    let mut directions = directions.into_iter();
    let mut off = Vec::new();
    for group in groups {
        let field_id = group[0].field as usize;
        let source_id = field.source_id(field_id).map_err(error_text)?;
        let name = names
            .get(&source_id)
            .ok_or("No SOURCE row for selected map field.")?
            .to_ascii_uppercase();
        let mut pointings = group
            .iter()
            .zip(directions.by_ref())
            .map(|(row, direction)| {
                let (lon, lat) = direction.as_angles();
                let Some((source_lon, source_lat)) = body_direction(&engine, row, &name)? else {
                    return Ok([lon, lat]);
                };
                Ok([lon - source_lon, lat - source_lat])
            })
            .collect::<Result<Vec<_>, String>>()?;
        let mask = mask::select(&mut pointings, fraction)?;
        off.extend(
            group
                .into_iter()
                .zip(mask)
                .filter_map(|(reference, keep)| keep.then_some(reference)),
        );
    }
    Ok(off)
}

fn body_direction(
    engine: &MsCalEngine,
    row: &Integration,
    name: &str,
) -> Result<Option<(f64, f64)>, String> {
    if name == "COMET" {
        return engine
            .tracked_body_direction_j2000(row.time, row.field as usize)
            .map(|direction| Some(direction.as_angles()))
            .map_err(error_text);
    }
    if !matches!(
        name,
        "SUN"
            | "MOON"
            | "MERCURY"
            | "VENUS"
            | "MARS"
            | "JUPITER"
            | "SATURN"
            | "URANUS"
            | "NEPTUNE"
            | "PLUTO"
    ) {
        return Ok(None);
    }
    let frame = engine
        .antenna_frame(row.time, row.antenna as usize)
        .map_err(error_text)?;
    let direction = MDirection::from_source_name(name, &frame)
        .and_then(|direction| direction.convert_to(DirectionRef::J2000, &frame))
        .map_err(error_text)?;
    Ok(Some(direction.as_angles()))
}
