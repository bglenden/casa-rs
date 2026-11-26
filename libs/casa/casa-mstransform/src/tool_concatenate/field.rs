// SPDX-License-Identifier: LGPL-3.0-or-later
//! FIELD direction identity, SOURCE remapping, and ephemeris transfers.

use std::collections::BTreeMap;
use std::path::PathBuf;

use casa_core::{
    ArrayValue, RecordValue, ScalarValue, Value,
    measures::{
        MeasFrame,
        direction::{DirectionRef, MDirection},
    },
};
use ndarray::{ArrayD, IxDyn};

use casa_ms::find_ephemeris_in_field;
use casa_ms::subtables::{FieldDirectionColumn, MsField, SubTable, get_f64, get_i32, get_string};
use casa_ms::{MeasurementSet, MsError, MsResult};

pub(super) struct FieldRows {
    pub(super) additions: Vec<RecordValue>,
    pub(super) ids: Vec<i32>,
    pub(super) matched: bool,
    pub(super) ephemerides: Vec<(PathBuf, String)>,
    pub(super) add_ephemeris_column: bool,
}

pub(super) fn reconcile(
    target: &MeasurementSet,
    donor: &MeasurementSet,
    source_ids: &BTreeMap<i32, i32>,
    direction_rad: f64,
    respect_name: bool,
) -> MsResult<FieldRows> {
    let target_field = target.field()?;
    let donor_field = donor.field()?;
    let target_rows = target_field.table().rows()?;
    let donor_rows = donor_field.table().rows()?;
    let mut known = (0..target_rows.len())
        .map(|index| candidate(&target_field, index, false))
        .collect::<MsResult<Vec<_>>>()?;
    let existing_eph = known.iter().filter_map(|row| row.ephemeris).max();
    let mut next_eph = existing_eph.unwrap_or(-1);
    let donor_bounds = existing_eph
        .map(|_| main_time_range(donor))
        .transpose()?
        .flatten();
    let target_reference = fixed_reference(target_field.table());
    let target_ephemeris_column = target_field
        .table()
        .schema()
        .is_some_and(|schema| schema.contains_column("EPHEMERIS_ID"));
    let mut output = FieldRows {
        additions: Vec::new(),
        ids: Vec::with_capacity(donor_rows.len()),
        matched: false,
        ephemerides: Vec::new(),
        add_ephemeris_column: donor_field
            .table()
            .schema()
            .is_some_and(|schema| schema.contains_column("EPHEMERIS_ID"))
            && !target_ephemeris_column,
    };
    for (index, record) in donor_rows.iter().enumerate() {
        let entry = candidate(&donor_field, index, true)?;
        let epoch = get_f64(donor_field.table(), index, "TIME")?;
        let mut direction = directions(&donor_field, index, 0.0)?;
        let mut converted = false;
        if let Some(reference) = target_reference {
            for value in &mut direction {
                if value.refer() != reference && entry.ephemeris.is_none() {
                    *value = value.convert_to(reference, &MeasFrame::new())?;
                    converted = true;
                }
            }
        }
        let by_name = known.iter().rposition(|other| other.name == entry.name);
        let order = by_name.into_iter().chain(
            (0..known.len())
                .rev()
                .filter(|&candidate| Some(candidate) != by_name),
        );
        let mut matched = None;
        for id in order {
            let other = &known[id];
            if other.flagged || other.polynomial != 0 {
                continue;
            }
            let field = if other.donor {
                &donor_field
            } else {
                &target_field
            };
            let Ok(at_time) = directions(field, other.row, epoch) else {
                continue;
            };
            if equivalent(&at_time, &direction, direction_rad) {
                matched = Some(id);
                break;
            }
        }
        if let Some(id) = matched {
            let other = &known[id];
            let field = if other.donor {
                &donor_field
            } else {
                &target_field
            };
            let same_kind = other.ephemeris.is_some() == entry.ephemeris.is_some();
            let covers = other.ephemeris.is_none()
                || donor_bounds.is_none_or(|(start, end)| {
                    [start, end].into_iter().all(|time| {
                        field
                            .metadata_direction_at_seconds(
                                other.row,
                                FieldDirectionColumn::Phase,
                                time,
                            )
                            .is_ok()
                    })
                });
            if same_kind && covers && (!respect_name || other.name == entry.name) {
                output.ids.push(super::spectral::row_id(id)?);
                output.matched = true;
                continue;
            }
        }
        let mut record = record.clone();
        if converted {
            if entry.polynomial != 0 {
                return Err(MsError::InvalidInput(
                    "cannot convert a polynomial FIELD between direction frames".into(),
                ));
            }
            for (column, value) in ["REFERENCE_DIR", "DELAY_DIR", "PHASE_DIR"]
                .into_iter()
                .zip(direction)
            {
                let [x, y, _] = value.cosines();
                let cell =
                    ArrayD::from_shape_vec(IxDyn(&[2, 1]), vec![y.atan2(x), value.latitude_rad()])
                        .expect("two direction angles");
                record.upsert(column, Value::Array(ArrayValue::Float64(cell)));
            }
        }
        let source = get_i32(donor_field.table(), index, "SOURCE_ID")?;
        if let Some(id) = source_ids.get(&source) {
            record.upsert("SOURCE_ID", integer(*id));
        }
        if let Some(id) = entry.ephemeris {
            let directory = donor_field
                .table()
                .path()
                .ok_or_else(|| MsError::InvalidInput("FIELD ephemeris has no table path".into()))?;
            let path = find_ephemeris_in_field(directory, id)
                .map_err(|error| MsError::InvalidInput(error.to_string()))?;
            next_eph = next_eph
                .checked_add(1)
                .ok_or_else(|| MsError::InvalidInput("EPHEMERIS_ID exceeds int32".into()))?;
            let comment = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.split_once('_'))
                .map(|(_, comment)| comment)
                .ok_or_else(|| MsError::InvalidInput("invalid FIELD ephemeris name".into()))?;
            output
                .ephemerides
                .push((path.clone(), format!("EPHEM{next_eph}_{comment}")));
            record.upsert("EPHEMERIS_ID", integer(next_eph));
        } else if target_ephemeris_column || output.add_ephemeris_column {
            record.upsert("EPHEMERIS_ID", integer(-1));
        }
        output.ids.push(super::spectral::row_id(known.len())?);
        known.push(entry);
        output.additions.push(record);
    }
    Ok(output)
}

struct Candidate {
    donor: bool,
    row: usize,
    name: String,
    flagged: bool,
    polynomial: i32,
    ephemeris: Option<i32>,
}

fn candidate(field: &MsField<'_>, row: usize, donor: bool) -> MsResult<Candidate> {
    Ok(Candidate {
        donor,
        row,
        name: get_string(field.table(), row, "NAME")?,
        flagged: casa_ms::subtables::get_bool(field.table(), row, "FLAG_ROW")?,
        polynomial: field.num_poly(row)?,
        ephemeris: field.ephemeris_id(row)?.filter(|&id| id >= 0),
    })
}

fn fixed_reference(table: &casa_tables::Table) -> Option<DirectionRef> {
    let Value::Record(metadata) = table.column_keywords("REFERENCE_DIR")?.get("MEASINFO")? else {
        return None;
    };
    let Value::Scalar(ScalarValue::String(reference)) = metadata.get("Ref")? else {
        return None;
    };
    reference.parse().ok()
}

fn directions(field: &MsField<'_>, row: usize, time: f64) -> MsResult<[MDirection; 3]> {
    let direction = |column| {
        Ok::<_, MsError>(
            field
                .metadata_direction_at_seconds(row, column, time)?
                .direction,
        )
    };
    Ok([
        direction(FieldDirectionColumn::Reference)?,
        direction(FieldDirectionColumn::Delay)?,
        direction(FieldDirectionColumn::Phase)?,
    ])
}

fn equivalent(known: &[MDirection; 3], incoming: &[MDirection; 3], tolerance: f64) -> bool {
    let frame = known[0].refer();
    let solar = frame.casacore_code() >= DirectionRef::MERCURY.casacore_code()
        && frame.casacore_code() <= DirectionRef::SUN.casacore_code();
    incoming.iter().all(|direction| direction.refer() == frame)
        && (solar
            || known.iter().zip(incoming).all(|(a, b)| {
                let (a, b) = (a.cosines(), b.cosines());
                let squared = a.iter().zip(b).map(|(&x, y)| (x - y).powi(2)).sum::<f64>();
                2.0 * (squared.sqrt() / 2.0).min(1.0).asin() < tolerance
            }))
}

fn main_time_range(donor: &MeasurementSet) -> MsResult<Option<(f64, f64)>> {
    if donor.row_count() == 0 {
        return Ok(None);
    }
    let table = donor.main_table();
    let mut range = (f64::INFINITY, f64::NEG_INFINITY);
    for row in 0..table.row_count() {
        let value = get_f64(table, row, "TIME")?;
        range.0 = range.0.min(value);
        range.1 = range.1.max(value);
    }
    Ok(Some(range))
}

fn integer(value: i32) -> Value {
    Value::Scalar(ScalarValue::Int32(value))
}
