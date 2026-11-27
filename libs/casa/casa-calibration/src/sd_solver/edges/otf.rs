// SPDX-License-Identifier: LGPL-3.0-or-later
//! Select reference integrations at the spatial boundary of each OTF map.

use std::collections::BTreeMap;

use crate::sd_solver::map_geometry::{MapTargets, SourceRow};
use casa_ms::MeasurementSet;
use casa_ms::derived::engine::MsCalEngine;

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
    let mut targets = MapTargets::read(ms)?;
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
        let target = targets.field(field_id, SourceRow::Required)?;
        let mut pointings = group
            .iter()
            .zip(directions.by_ref())
            .map(|(row, direction)| {
                let (lon, lat) = direction.as_angles();
                let Some((source_lon, source_lat)) =
                    target.direction(&engine, row.time, row.antenna as usize, field_id)?
                else {
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
