// SPDX-License-Identifier: LGPL-3.0-or-later
//! MAIN column reconciliation for physical concatenation.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use casa_core::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use casa_tables::{ArrayShapeContract, ColumnType, EncodedArrayAppend, RecordAppendOptions, Table};
use ndarray::{ArrayD, Axis, IxDyn};
use rayon::prelude::*;

use super::{
    MainRowMap, MetadataPlan, required_table,
    scan::{ScanBounds, ScanOffsets},
    spectral::row_id,
};
use crate::write_session::VisibilityWriteDestination;
use crate::{MeasurementSet, MsError, MsResult, SubtableId, subtables::get_array};

const SCALARS: &[&str] = &[
    "ARRAY_ID",
    "EXPOSURE",
    "FLAG_ROW",
    "INTERVAL",
    "TIME",
    "TIME_CENTROID",
    "ANTENNA1",
    "ANTENNA2",
    "DATA_DESC_ID",
    "FEED1",
    "FEED2",
    "FIELD_ID",
    "OBSERVATION_ID",
    "PROCESSOR_ID",
    "SCAN_NUMBER",
    "STATE_ID",
];

pub(super) struct MainAppend {
    pub(super) records: Vec<RecordValue>,
    encoded: EncodedArrayAppend,
    reset_state: Option<ScalarValue>,
    channel_selection: Option<Value>,
}

impl MainAppend {
    pub(super) fn options(&self) -> RecordAppendOptions<'_> {
        RecordAppendOptions {
            encoded_arrays: Some(&self.encoded),
            existing_scalar: self.reset_state.as_ref().map(|value| ("STATE_ID", value)),
            column_keyword: self
                .channel_selection
                .as_ref()
                .map(|value| ("MODEL_DATA", "CHANNEL_SELECTION", value)),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct RowOrder {
    pub(super) first: i32,
    pub(super) second: i32,
    pub(super) description: i32,
    polarization: usize,
    pub(super) flipped: bool,
    pub(super) reverse: bool,
}

pub(super) fn prepare(
    attached: &MeasurementSet,
    donor: &MeasurementSet,
    destination: &Table,
    plan: &MetadataPlan<'_>,
    scale: f32,
) -> MsResult<Option<MainAppend>> {
    let map = &plan.row_map;
    let source = donor.main_table();
    if source.row_count() == 0 {
        return Ok(None);
    }
    let read = || scalar_columns(source, SCALARS);
    let (scalars, attached_scan) = super::join_with_tracing(read, || {
        if destination.row_count() == 0 {
            Ok(None)
        } else {
            scalar_columns(destination, &["OBSERVATION_ID", "SCAN_NUMBER"]).map(Some)
        }
    });
    let mut scalars = scalars?;
    let order = row_orders(source.row_count(), &scalars, map, true)?;
    let kernels = ArrayTransform::new(donor, &order, scale)?;
    let array_names = array_names(attached.main_table(), source, destination, |_| true)?;
    let channel_selection = if array_names.contains(&"MODEL_DATA") {
        Some(model_channel_selection(attached, Some(plan))?)
    } else {
        None
    };
    let passthrough = array_names
        .iter()
        .copied()
        .filter(|column| !kernels.changes(column))
        .collect::<Vec<_>>();
    let encoded = destination.plan_encoded_array_append(source, &passthrough)?;
    let materialized = array_names
        .into_iter()
        .filter(|name| !encoded.columns().any(|column| name == &column))
        .collect::<Vec<_>>();
    let bounds = attached_scan?
        .as_ref()
        .map(ScanBounds::from_main)
        .transpose()?
        .unwrap_or_default();
    let scans = bounds.offsets(&scalars, &map.observations)?;
    let rows = (0..source.row_count()).collect::<Vec<_>>();
    let dispatch = tracing::dispatcher::get_default(Clone::clone);
    let arrays = materialized
        .par_iter()
        .map(|&name| {
            tracing::dispatcher::with_default(&dispatch, || {
                let cells = source
                    .column_accessor(name)?
                    .array_cells_owned_uncached(&rows)?;
                Ok((
                    VisibilityWriteDestination::for_append(destination, name)?,
                    cells.into_iter(),
                ))
            })
        })
        .collect::<MsResult<Vec<_>>>()?;
    let mut arrays = arrays;
    let mut records = Vec::with_capacity(rows.len());
    for (row, order) in order.into_iter().enumerate() {
        let mut fields = Vec::with_capacity(SCALARS.len() + arrays.len());
        append_scalars(&mut fields, &mut scalars, row, order, map, &scans)?;
        for (column, cells) in &mut arrays {
            if let Some(Some(array)) = cells.next() {
                let array = kernels.apply(column.name(), order, array)?;
                column.append_cell(array, &mut fields)?;
            }
        }
        records.push(RecordValue::new(fields));
    }
    let reset_state =
        (destination.row_count() > 0 && map.attached_has_state && !map.donor_has_state)
            .then_some(ScalarValue::Int32(-1));
    Ok(Some(MainAppend {
        records,
        encoded,
        reset_state,
        channel_selection,
    }))
}

pub(super) type ScalarColumns = HashMap<String, Vec<Option<ScalarValue>>>;

pub(super) fn scalar_columns(table: &Table, columns: &[&str]) -> MsResult<ScalarColumns> {
    Ok(table.scalar_columns_owned(columns)?)
}

pub(super) fn row_orders(
    rows: usize,
    scalar: &ScalarColumns,
    map: &MainRowMap,
    reorder: bool,
) -> MsResult<Vec<RowOrder>> {
    (0..rows)
        .map(|row| {
            row_order(
                map,
                integer(scalar, "ANTENNA1", row)?,
                integer(scalar, "ANTENNA2", row)?,
                integer(scalar, "DATA_DESC_ID", row)?,
                reorder,
            )
        })
        .collect()
}

pub(super) fn row_order(
    map: &MainRowMap,
    first: i32,
    second: i32,
    description: i32,
    reorder: bool,
) -> MsResult<RowOrder> {
    let a = row_id(*lookup(&map.antennas, first, "ANTENNA1")?)?;
    let b = row_id(*lookup(&map.antennas, second, "ANTENNA2")?)?;
    let description = lookup(&map.descriptions, description, "DATA_DESC_ID")?;
    let flipped = reorder && a > b;
    Ok(RowOrder {
        first: if flipped { b } else { a },
        second: if flipped { a } else { b },
        description: description.id,
        polarization: description.polarization,
        flipped,
        reverse: description.reverse,
    })
}

fn append_scalars(
    fields: &mut Vec<RecordField>,
    scalar: &mut ScalarColumns,
    row: usize,
    order: RowOrder,
    map: &MainRowMap,
    scans: &ScanOffsets,
) -> MsResult<()> {
    let old_observation = integer(scalar, "OBSERVATION_ID", row)?;
    let observation = sparse_id(&map.observations, old_observation)?;
    let scan = scans.remap(
        old_observation,
        observation,
        integer(scalar, "SCAN_NUMBER", row)?,
    )?;
    let (feed1, feed2) = if order.flipped {
        ("FEED2", "FEED1")
    } else {
        ("FEED1", "FEED2")
    };
    let state = match (map.attached_has_state, map.donor_has_state) {
        (false, false) => integer(scalar, "STATE_ID", row)?,
        (true, true) => optional_id(&map.states, integer(scalar, "STATE_ID", row)?, "STATE_ID")?,
        _ => -1,
    };
    let ids = [
        ("ANTENNA1", order.first),
        ("ANTENNA2", order.second),
        ("DATA_DESC_ID", order.description),
        ("FEED1", integer(scalar, feed1, row)?),
        ("FEED2", integer(scalar, feed2, row)?),
        (
            "FIELD_ID",
            *lookup(&map.fields, integer(scalar, "FIELD_ID", row)?, "FIELD_ID")?,
        ),
        ("OBSERVATION_ID", observation),
        (
            "PROCESSOR_ID",
            sparse_id(&map.processors, integer(scalar, "PROCESSOR_ID", row)?)?,
        ),
        ("SCAN_NUMBER", scan),
        ("STATE_ID", state),
    ];
    fields.extend(
        ids.into_iter()
            .map(|(name, id)| RecordField::new(name, Value::Scalar(ScalarValue::Int32(id)))),
    );
    for &name in &SCALARS[..6] {
        let value = scalar
            .get_mut(name)
            .and_then(|cells| cells.get_mut(row))
            .and_then(Option::take)
            .ok_or_else(|| invalid(format!("missing MAIN {name} at row {row}")))?;
        fields.push(RecordField::new(name, Value::Scalar(value)));
    }
    Ok(())
}

pub(super) fn integer(columns: &ScalarColumns, name: &str, row: usize) -> MsResult<i32> {
    match columns.get(name).and_then(|cells| cells.get(row)) {
        Some(Some(ScalarValue::Int32(value))) => Ok(*value),
        _ => Err(invalid(format!(
            "missing or non-int32 MAIN {name} at row {row}"
        ))),
    }
}

pub(super) fn lookup<'a, T>(indices: &'a [T], id: i32, column: &str) -> MsResult<&'a T> {
    usize::try_from(id)
        .ok()
        .and_then(|id| indices.get(id))
        .ok_or_else(|| invalid(format!("MAIN {column} index {id} is outside its subtable")))
}

pub(super) fn sparse_id(indices: &[usize], id: i32) -> MsResult<i32> {
    // Older MeasurementSets can use zero with an empty PROCESSOR subtable.
    // CASA remaps only IDs found in the observation and processor maps.
    match usize::try_from(id)
        .ok()
        .and_then(|index| indices.get(index))
    {
        Some(&mapped) => row_id(mapped),
        None => Ok(id),
    }
}

pub(super) fn optional_id(indices: &[usize], id: i32, column: &str) -> MsResult<i32> {
    if id == -1 {
        Ok(id)
    } else {
        row_id(*lookup(indices, id, column)?)
    }
}

pub(super) fn model_channel_selection(
    attached: &MeasurementSet,
    plan: Option<&MetadataPlan<'_>>,
) -> MsResult<Value> {
    use crate::subtables::{SubTable, get_i32};
    let windows = attached.spectral_window()?;
    let mut channel_count = (0..windows.table().row_count())
        .map(|row| get_i32(windows.table(), row, "NUM_CHAN"))
        .collect::<MsResult<Vec<_>>>()?;
    if let Some(change) = plan.and_then(|plan| {
        plan.changes
            .iter()
            .find(|change| change.table == SubtableId::SpectralWindow)
    }) {
        for row in &change.rows.appended {
            let Some(Value::Scalar(ScalarValue::Int32(channels))) = row.get("NUM_CHAN") else {
                return Err(invalid("SPECTRAL_WINDOW has no channel count"));
            };
            channel_count.push(*channels);
        }
    }
    let selection = ArrayD::from_shape_fn(IxDyn(&[2, channel_count.len()]), |position| {
        if position[0] == 0 {
            0
        } else {
            channel_count[position[1]]
        }
    });
    Ok(Value::Array(ArrayValue::Int32(selection)))
}

pub(super) fn array_names(
    attached: &Table,
    source: &Table,
    destination: &Table,
    needs_optional: impl Fn(&str) -> bool,
) -> MsResult<Vec<&'static str>> {
    let contains = |table: &Table, name| {
        table
            .schema()
            .is_some_and(|schema| schema.contains_column(name))
    };
    let floats = contains(attached, "FLOAT_DATA");
    let visibility = if floats { "FLOAT_DATA" } else { "DATA" };
    let mut columns = vec![visibility, "FLAG", "WEIGHT", "SIGMA", "UVW"];
    for name in ["MODEL_DATA", "CORRECTED_DATA"] {
        if contains(attached, name) {
            if !contains(source, name) {
                return Err(invalid(format!(
                    "incoming MeasurementSet is missing {name}"
                )));
            }
            columns.push(name);
        }
    }
    if !contains(source, visibility) || !contains(destination, visibility) {
        return Err(invalid(format!("MeasurementSet is missing {visibility}")));
    }
    for name in ["FLAG_CATEGORY", "WEIGHT_SPECTRUM", "SIGMA_SPECTRUM"] {
        if needs_optional(name)
            && contains(destination, name)
            && contains(source, name)
            && (std::ptr::eq(source, destination)
                || (first_output_cell_defined(destination, name)?
                    && first_array_is_defined(source, name)?))
        {
            columns.push(name);
        }
    }
    Ok(columns)
}

fn first_output_cell_defined(table: &Table, name: &str) -> MsResult<bool> {
    if table.row_count() != 0 {
        return first_array_is_defined(table, name);
    }
    // Fixed-shape cells are already defined after CASA allocates its
    // destination rows, including a newly created named output.
    Ok(matches!(
        table
            .schema()
            .and_then(|schema| schema.column(name))
            .map(|column| column.column_type()),
        Some(ColumnType::Array(ArrayShapeContract::Fixed { .. }))
    ))
}

fn first_array_is_defined(table: &Table, column: &str) -> MsResult<bool> {
    let value = table
        .column_accessor(column)?
        .array_cells_owned_uncached(&[0])?;
    Ok(value.first().is_some_and(Option::is_some))
}

fn polarization_swaps(
    donor: &MeasurementSet,
    order: &[RowOrder],
) -> MsResult<BTreeMap<usize, Vec<usize>>> {
    let table = required_table(donor, SubtableId::Polarization)?;
    let ids = order
        .iter()
        .filter(|row| row.flipped)
        .map(|row| row.polarization)
        .collect::<BTreeSet<_>>();
    ids.into_iter()
        .map(|id| {
            let ArrayValue::Int32(products) = get_array(table, id, "CORR_PRODUCT")? else {
                return Err(invalid("POLARIZATION.CORR_PRODUCT must hold integers"));
            };
            if products.ndim() != 2 || products.shape()[0] != 2 {
                return Err(invalid(
                    "POLARIZATION.CORR_PRODUCT must be a two-receptor matrix",
                ));
            }
            let n = products.shape()[1];
            let partners = (0..n)
                .map(|i| {
                    (0..n)
                        .find(|&j| {
                            products[[0, i]] == products[[1, j]]
                                && products[[1, i]] == products[[0, j]]
                        })
                        .ok_or_else(|| {
                            invalid("baseline reversal has no matching correlation product")
                        })
                })
                .collect::<MsResult<Vec<_>>>()?;
            Ok((id, partners))
        })
        .collect()
}

pub(super) struct ArrayTransform {
    polarizations: BTreeMap<usize, Vec<usize>>,
    pub(super) any_flipped: bool,
    any_reversed: bool,
    scale: f32,
}

impl ArrayTransform {
    pub(super) fn new(donor: &MeasurementSet, order: &[RowOrder], scale: f32) -> MsResult<Self> {
        Ok(Self {
            polarizations: polarization_swaps(donor, order)?,
            any_flipped: order.iter().any(|row| row.flipped),
            any_reversed: order.iter().any(|row| row.reverse),
            scale: if scale > 0.0 { scale } else { 1.0 },
        })
    }

    pub(super) fn changes(&self, name: &str) -> bool {
        (self.any_flipped && name != "FLOAT_DATA")
            || (self.any_reversed && channel_axis(name))
            || (self.scale != 1.0 && weight_column(name))
    }

    pub(super) fn apply(
        &self,
        name: &str,
        row: RowOrder,
        mut value: ArrayValue,
    ) -> MsResult<ArrayValue> {
        if name == "UVW" {
            if row.flipped {
                let ArrayValue::Float64(uvw) = &mut value else {
                    return Err(invalid("UVW must hold doubles"));
                };
                uvw.mapv_inplace(|pixel| -pixel);
            }
            return Ok(value);
        }
        let permute = if row.flipped && name != "FLOAT_DATA" {
            Some(self.polarizations[&row.polarization].as_slice())
        } else {
            None
        };
        let reverse = row.reverse && channel_axis(name);
        let scale = match name {
            "WEIGHT" | "WEIGHT_SPECTRUM" => self.scale,
            "SIGMA" | "SIGMA_SPECTRUM" => self.scale.sqrt().recip(),
            _ => 1.0,
        };
        if permute.is_none() && !reverse && scale == 1.0 {
            return Ok(value);
        }
        Ok(match value {
            ArrayValue::Complex32(array) => {
                let mut array = reorder(array, permute, reverse)?;
                if row.flipped {
                    array.mapv_inplace(|sample| sample.conj());
                }
                ArrayValue::Complex32(array)
            }
            ArrayValue::Float32(array) => {
                let mut array = reorder(array, permute, reverse)?;
                if scale != 1.0 {
                    array.mapv_inplace(|sample| sample * scale);
                }
                ArrayValue::Float32(array)
            }
            ArrayValue::Bool(array) => ArrayValue::Bool(reorder(array, permute, reverse)?),
            _ => {
                return Err(invalid(format!(
                    "unsupported {name} array for concatenation"
                )));
            }
        })
    }
}

fn reorder<T: Clone>(
    mut cell: ArrayD<T>,
    permutation: Option<&[usize]>,
    reverse: bool,
) -> MsResult<ArrayD<T>> {
    if let Some(permutation) = permutation {
        if cell.shape().first() != Some(&permutation.len()) {
            return Err(invalid("correlation axis does not match POLARIZATION"));
        }
        if permutation.iter().copied().ne(0..permutation.len()) {
            cell = cell.select(Axis(0), permutation);
        }
    }
    if reverse {
        if cell.ndim() < 2 {
            return Err(invalid("visibility array has no channel axis"));
        }
        cell.invert_axis(Axis(1));
    }
    Ok(cell)
}

fn channel_axis(name: &str) -> bool {
    !matches!(name, "UVW" | "WEIGHT" | "SIGMA")
}

fn weight_column(name: &str) -> bool {
    matches!(
        name,
        "WEIGHT" | "SIGMA" | "WEIGHT_SPECTRUM" | "SIGMA_SPECTRUM"
    )
}

fn invalid(message: impl Into<String>) -> MsError {
    MsError::InvalidInput(message.into())
}
