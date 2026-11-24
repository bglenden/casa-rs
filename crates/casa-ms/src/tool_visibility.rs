// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column reads for the legacy MeasurementSet tool visibility record.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use casa_tables::{
    RequiredScalarColumnValues, SelectedArray1DCells, Table, TableColumn, TablePartNames,
};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use num_complex::Complex32;

use crate::{MeasurementSet, MsError, MsResult};

/// Determine whether CASA's implicit initialization needs to narrow this view
/// to data description zero. The iterator's row shape comes from its logical
/// spectral window and polarization tables.
pub fn heterogeneous_row_shapes(ms: &MeasurementSet) -> MsResult<Option<Vec<usize>>> {
    let dd = ms.data_description()?;
    if dd.row_count() <= 1 || ms.row_count() <= 1 {
        return Ok(None);
    }
    let rows = (0..ms.row_count()).collect::<Vec<_>>();
    let values = ms
        .main_table()
        .column_accessor("DATA_DESC_ID")?
        .scalar_cells_owned_for_rows(&rows)?;
    let RequiredScalarColumnValues::Int32(ids) = typed_scalar_column("DATA_DESC_ID", values)?
    else {
        return Err(input("DATA_DESC_ID must contain an integer"));
    };
    let used = ids.iter().copied().collect::<BTreeSet<_>>();
    let spw = ms.spectral_window()?;
    let pol = ms.polarization()?;
    let mut uniform = None;
    for id in used {
        let id = usize::try_from(id).map_err(|_| input("negative DATA_DESC_ID"))?;
        let window = usize::try_from(dd.spectral_window_id(id)?)
            .map_err(|_| input("negative SPECTRAL_WINDOW_ID"))?;
        let correlation = usize::try_from(dd.polarization_id(id)?)
            .map_err(|_| input("negative POLARIZATION_ID"))?;
        let shape = (pol.num_corr(correlation)?, spw.num_chan(window)?);
        if uniform.is_some_and(|uniform| uniform != shape) {
            let first_description = rows
                .into_iter()
                .zip(ids)
                .filter_map(|(row, id)| (id == 0).then_some(row));
            return Ok(Some(first_description.collect()));
        }
        uniform = Some(shape);
    }
    Ok(None)
}

/// Read the ordinary row-axis visibility record for a logical set of rows.
/// Complex and float operations retain the precision of the stored cells.
/// # Errors
/// A requested column cannot be read as a rectangular array.
pub fn read_tool_visibility(
    ms: &MeasurementSet,
    rows: &[usize],
    items: &[String],
) -> MsResult<RecordValue> {
    let names = items
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let mut read = VisibilityRead::new(ms.main_table(), rows, &names)?;
    let mut fields = BTreeMap::new();
    if names.contains("axis_info") {
        let RequiredScalarColumnValues::Int32(ids) = &read.scalar_columns["DATA_DESC_ID"] else {
            return Err(input("DATA_DESC_ID must contain an integer"));
        };
        fields.insert("axis_info".into(), Value::Record(axis_info(ms, ids)?));
    }
    read.scalars(&names, &mut fields)?;
    read.arrays(&names, &mut fields)?;
    read.products(&names, &mut fields)?;
    Ok(RecordValue::new(
        fields
            .into_iter()
            .map(|(name, value)| RecordField { name, value })
            .collect(),
    ))
}

struct VisibilityRead<'a> {
    main: &'a Table,
    rows: &'a [usize],
    plain: bool,
    scalar_columns: HashMap<String, RequiredScalarColumnValues>,
}

const SCALARS: [(&str, &str); 10] = [
    ("antenna1", "ANTENNA1"),
    ("antenna2", "ANTENNA2"),
    ("array_id", "ARRAY_ID"),
    ("data_desc_id", "DATA_DESC_ID"),
    ("feed1", "FEED1"),
    ("feed2", "FEED2"),
    ("field_id", "FIELD_ID"),
    ("flag_row", "FLAG_ROW"),
    ("scan_number", "SCAN_NUMBER"),
    ("time", "TIME"),
];

impl<'a> VisibilityRead<'a> {
    fn new(main: &'a Table, rows: &'a [usize], names: &BTreeSet<String>) -> MsResult<Self> {
        let mut columns = SCALARS
            .into_iter()
            .filter_map(|(name, column)| {
                (names.contains(name)
                    || (matches!(name, "antenna1" | "antenna2") && names.contains("ifr_number")))
                .then_some(column)
            })
            .collect::<Vec<_>>();
        if names.contains("axis_info") && !columns.contains(&"DATA_DESC_ID") {
            columns.push("DATA_DESC_ID");
        }
        let plain = if let Some(path) = main.path() {
            matches!(
                Table::immediate_part_names(path, path)?,
                TablePartNames::Plain(_)
            )
        } else {
            false
        };
        let scalar_columns = if columns.is_empty() {
            HashMap::new()
        } else if plain && !rows.is_empty() {
            main.required_scalar_columns_owned_for_rows(&columns, rows)?
        } else {
            columns
                .into_iter()
                .map(|column| {
                    let values = main
                        .column_accessor(column)?
                        .scalar_cells_owned_for_rows(rows)?;
                    typed_scalar_column(column, values).map(|values| (column.to_owned(), values))
                })
                .collect::<MsResult<_>>()?
        };
        Ok(Self {
            main,
            rows,
            plain,
            scalar_columns,
        })
    }

    fn scalars(
        &mut self,
        names: &BTreeSet<String>,
        fields: &mut BTreeMap<String, Value>,
    ) -> MsResult<()> {
        if names.contains("ifr_number") {
            let (
                RequiredScalarColumnValues::Int32(first),
                RequiredScalarColumnValues::Int32(second),
            ) = (
                &self.scalar_columns["ANTENNA1"],
                &self.scalar_columns["ANTENNA2"],
            )
            else {
                return Err(input("invalid antenna ID column type"));
            };
            let ifrs = first
                .iter()
                .zip(second)
                .map(|(&left, &right)| left.wrapping_mul(1000).wrapping_add(right))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            fields.insert(
                "ifr_number".into(),
                Value::Array(ArrayValue::Int32(vector(ifrs))),
            );
        }
        for (name, column) in SCALARS {
            if !names.contains(name) {
                continue;
            }
            let value = match self
                .scalar_columns
                .remove(column)
                .ok_or_else(|| input(format!("missing column {column}")))?
            {
                RequiredScalarColumnValues::Bool(values) => ArrayValue::Bool(vector(values)),
                RequiredScalarColumnValues::Int32(values) => ArrayValue::Int32(vector(values)),
                RequiredScalarColumnValues::Float32(values) => ArrayValue::Float32(vector(values)),
                RequiredScalarColumnValues::Float64(values) => ArrayValue::Float64(vector(values)),
            };
            fields.insert(name.into(), Value::Array(value));
        }
        Ok(())
    }

    fn arrays(
        &self,
        names: &BTreeSet<String>,
        fields: &mut BTreeMap<String, Value>,
    ) -> MsResult<()> {
        for (name, column) in [("weight", "WEIGHT"), ("sigma", "SIGMA")] {
            if names.contains(name) {
                fields.insert(
                    name.into(),
                    Value::Array(read_vector_cells(self.main, column, self.rows, self.plain)?),
                );
            }
        }
        if names.contains("flag") || names.contains("flag_sum") {
            let flags = read_matrix_cells(self.main, "FLAG", self.rows, false)?;
            if names.contains("flag_sum") {
                let ArrayValue::Bool(flag) = &flags else {
                    return Err(input("FLAG cells must be Boolean"));
                };
                let (shape, rows) = match flag.shape() {
                    [correlations, channels, rows] => ([*correlations, *channels], *rows),
                    _ if self.rows.is_empty() => ([0, 0], 0),
                    _ => return Err(input("FLAG column has no row axis")),
                };
                let samples = shape.iter().product();
                let mut sums = vec![0_i64; samples];
                let memory = flag
                    .as_slice_memory_order()
                    .ok_or_else(|| input("FLAG block is not contiguous"))?;
                for row in memory.chunks_exact(samples.max(1)).take(rows) {
                    for (sum, &flagged) in sums.iter_mut().zip(row) {
                        *sum += i64::from(flagged);
                    }
                }
                fields.insert(
                    "flag_sum".into(),
                    Value::Array(ArrayValue::Int64(shaped(&shape, sums)?)),
                );
            }
            if names.contains("flag") {
                fields.insert("flag".into(), Value::Array(flags));
            }
        }
        if names
            .iter()
            .any(|name| matches!(name.as_str(), "uvw" | "u" | "v" | "w" | "uvdist"))
        {
            let uvw = read_vector_cells(self.main, "UVW", self.rows, self.plain)?;
            let ArrayValue::Float64(values) = &uvw else {
                return Err(input("UVW cells must be double precision"));
            };
            if !self.rows.is_empty() && values.shape().first() != Some(&3) {
                return Err(input("UVW cells must have three components"));
            }
            let memory = values
                .as_slice_memory_order()
                .ok_or_else(|| input("UVW block is not contiguous"))?;
            let (uvw_rows, _) = memory.as_chunks::<3>();
            for (axis, name) in ["u", "v", "w"].into_iter().enumerate() {
                if names.contains(name) {
                    let values = uvw_rows.iter().map(|row| row[axis]).collect();
                    fields.insert(
                        name.into(),
                        Value::Array(ArrayValue::Float64(vector(values))),
                    );
                }
            }
            if names.contains("uvdist") {
                let meters = uvw_rows
                    .iter()
                    .map(|row| (row[0] * row[0] + row[1] * row[1]).sqrt())
                    .collect();
                fields.insert(
                    "uvdist".into(),
                    Value::Array(ArrayValue::Float64(vector(meters))),
                );
            }
            if names.contains("uvw") {
                fields.insert("uvw".into(), Value::Array(uvw));
            }
        }
        Ok(())
    }

    fn products(
        &self,
        names: &BTreeSet<String>,
        fields: &mut BTreeMap<String, Value>,
    ) -> MsResult<()> {
        for (prefix, source, model) in [
            ("", "DATA", false),
            ("corrected_", "CORRECTED_DATA", false),
            ("model_", "MODEL_DATA", false),
            ("ratio_", "CORRECTED_DATA", true),
            ("residual_", "CORRECTED_DATA", true),
            ("obs_residual_", "DATA", true),
        ] {
            let requested = ["data", "amplitude", "phase", "real", "imaginary"].map(|name| {
                let key = format!("{prefix}{name}");
                names.contains(&key).then_some(key)
            });
            if requested.iter().all(Option::is_none) {
                continue;
            }
            let has_column = |column| {
                self.main
                    .schema()
                    .is_some_and(|schema| schema.column(column).is_some())
            };
            if (source != "DATA" && !has_column(source)) || (model && !has_column("MODEL_DATA")) {
                for (index, key) in requested
                    .into_iter()
                    .enumerate()
                    .filter_map(|(index, key)| key.map(|key| (index, key)))
                {
                    let values = if index == 0 {
                        ArrayValue::Complex32(vector(Vec::new()))
                    } else {
                        ArrayValue::Float32(vector(Vec::new()))
                    };
                    fields.insert(key, Value::Array(values));
                }
                continue;
            }
            let [raw, amplitude, phase, real, imaginary] = requested;
            let only_raw = !model
                && [&amplitude, &phase, &real, &imaginary]
                    .into_iter()
                    .all(Option::is_none);
            let data = read_matrix_cells(self.main, source, self.rows, only_raw)?;
            let data = if model {
                let model_values = read_matrix_cells(self.main, "MODEL_DATA", self.rows, false)?;
                let (ArrayValue::Complex32(mut observed), ArrayValue::Complex32(expected)) =
                    (data, model_values)
                else {
                    return Err(input("visibility columns must contain Complex values"));
                };
                if observed.shape() != expected.shape() {
                    return Err(input("visibility columns have different shapes"));
                }
                if prefix == "ratio_" {
                    observed.zip_mut_with(&expected, |observed, expected| *observed /= *expected);
                } else {
                    observed.zip_mut_with(&expected, |observed, expected| *observed -= *expected);
                }
                ArrayValue::Complex32(observed)
            } else {
                data
            };
            if !only_raw {
                let ArrayValue::Complex32(complex) = &data else {
                    return Err(input("visibility columns must contain Complex values"));
                };
                let projections: [fn(Complex32) -> f32; 4] = [
                    |sample| sample.norm(),
                    |sample| sample.arg(),
                    |sample| sample.re,
                    |sample| sample.im,
                ];
                let memory = complex
                    .as_slice_memory_order()
                    .ok_or_else(|| input("visibility block is not contiguous"))?;
                for (name, project) in [amplitude, phase, real, imaginary]
                    .into_iter()
                    .zip(projections)
                    .filter_map(|(key, project)| key.map(|key| (key, project)))
                {
                    let values = shaped(
                        complex.shape(),
                        memory.iter().copied().map(project).collect(),
                    )?;
                    fields.insert(name, Value::Array(ArrayValue::Float32(values)));
                }
            }
            if let Some(name) = raw {
                fields.insert(name, Value::Array(data));
            }
        }
        if names.contains("float_data") {
            let values = if self
                .main
                .schema()
                .is_some_and(|schema| schema.column("FLOAT_DATA").is_some())
            {
                read_matrix_cells(self.main, "FLOAT_DATA", self.rows, false)?
            } else {
                ArrayValue::Float32(vector(Vec::new()))
            };
            fields.insert("float_data".into(), Value::Array(values));
        }
        Ok(())
    }
}

fn read_vector_cells(
    main: &Table,
    name: &str,
    rows: &[usize],
    plain: bool,
) -> MsResult<ArrayValue> {
    let column = main.column_accessor(name)?;
    if rows.is_empty() {
        return empty_array(main, name);
    }
    if !plain {
        return pack_cells(column, rows);
    }
    let packed = column.array_cells_1d_typed_uncached(rows)?;
    let shape = [packed.axis0_count(), packed.row_count()];
    match packed {
        SelectedArray1DCells::Float32(values) => {
            Ok(ArrayValue::Float32(shaped(&shape, values.into_values())?))
        }
        SelectedArray1DCells::Float64(values) => {
            Ok(ArrayValue::Float64(shaped(&shape, values.into_values())?))
        }
        _ => Err(input(format!("invalid floating-point array column {name}"))),
    }
}

fn read_matrix_cells(
    main: &Table,
    name: &str,
    rows: &[usize],
    widen_complex: bool,
) -> MsResult<ArrayValue> {
    let column = main.column_accessor(name)?;
    if rows.is_empty() {
        return empty_array(main, name);
    }
    let block = if widen_complex {
        column.packed_2d_cells_uncached_with_complex_promotion(rows)?
    } else {
        column.packed_2d_cells_uncached(rows)?
    };
    if let Some(block) = block {
        return Ok(block);
    }
    pack_cells(column, rows)
}

fn empty_array(main: &Table, name: &str) -> MsResult<ArrayValue> {
    use casa_types::PrimitiveType;
    let data_type = main
        .schema()
        .and_then(|schema| schema.column(name))
        .and_then(|column| column.data_type());
    match data_type {
        Some(PrimitiveType::Bool) => Ok(ArrayValue::Bool(vector(Vec::new()))),
        Some(PrimitiveType::Float32) => Ok(ArrayValue::Float32(vector(Vec::new()))),
        Some(PrimitiveType::Float64) => Ok(ArrayValue::Float64(vector(Vec::new()))),
        Some(PrimitiveType::Complex32) => Ok(ArrayValue::Complex32(vector(Vec::new()))),
        Some(PrimitiveType::Complex64) => Ok(ArrayValue::Complex64(vector(Vec::new()))),
        _ => Err(input(format!("unsupported array column type in {name}"))),
    }
}

fn typed_scalar_column(
    column: &str,
    values: Vec<Option<ScalarValue>>,
) -> MsResult<RequiredScalarColumnValues> {
    macro_rules! typed {
        ($variant:ident) => {
            values
                .into_iter()
                .map(|value| match value {
                    Some(ScalarValue::$variant(value)) => Ok(value),
                    _ => Err(input(format!("undefined or wrongly typed {column} cell"))),
                })
                .collect::<MsResult<Vec<_>>>()
                .map(RequiredScalarColumnValues::$variant)
        };
    }
    match column {
        "TIME" => typed!(Float64),
        "FLAG_ROW" => typed!(Bool),
        _ => typed!(Int32),
    }
}

fn pack_cells(column: TableColumn<'_>, rows: &[usize]) -> MsResult<ArrayValue> {
    let name = column.name();
    let cells = column
        .array_cells_owned_uncached(rows)?
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| input(format!("undefined array cell in {name}")))?;
    let first = cells
        .first()
        .ok_or_else(|| input(format!("no array cells in {name}")))?;
    let cell_shape = first.shape();
    if cells.iter().any(|cell| cell.shape() != cell_shape) {
        return Err(input(format!(
            "array column {name} has variable cell shapes"
        )));
    }
    let mut shape = cell_shape.to_vec();
    shape.push(cells.len());
    macro_rules! stack {
        ($variant:ident) => {{
            let mut values = Vec::with_capacity(shape.iter().product());
            for cell in cells {
                let ArrayValue::$variant(cell) = cell else {
                    return Err(input(format!(
                        "array column {name} has inconsistent element types"
                    )));
                };
                values.extend(cell.reversed_axes());
            }
            Ok(ArrayValue::$variant(shaped(&shape, values)?))
        }};
    }
    match first {
        ArrayValue::Bool(_) => stack!(Bool),
        ArrayValue::Float32(_) => stack!(Float32),
        ArrayValue::Float64(_) => stack!(Float64),
        ArrayValue::Complex32(_) => stack!(Complex32),
        ArrayValue::Complex64(_) => stack!(Complex64),
        _ => Err(input(format!("unexpected array element type in {name}"))),
    }
}

fn axis_info(ms: &MeasurementSet, ids: &[i32]) -> MsResult<RecordValue> {
    use casa_coordinates::StokesType;

    let indices = ids
        .iter()
        .copied()
        .map(|id| usize::try_from(id).map_err(|_| input("negative DATA_DESC_ID")));
    let dd = ms.data_description()?;
    let first = indices
        .clone()
        .next()
        .transpose()?
        .ok_or_else(|| input("no data descriptions in selection"))?;
    let pol_id = usize::try_from(dd.polarization_id(first)?)
        .map_err(|_| input("negative POLARIZATION_ID"))?;
    let correlations = ms
        .polarization()?
        .corr_type(pol_id)?
        .into_iter()
        .map(|code| {
            StokesType::from_code(code)
                .map_or("??", StokesType::name)
                .to_owned()
        })
        .collect();
    let descriptions = indices.collect::<MsResult<BTreeSet<_>>>()?;
    let windows = descriptions
        .into_iter()
        .map(|id| dd.spectral_window_id(id))
        .collect::<MsResult<BTreeSet<_>>>()?;
    let spw = ms.spectral_window()?;
    let mut lengths = None;
    let mut frequencies = Vec::new();
    let mut resolution = Vec::new();
    for &window in &windows {
        let index = usize::try_from(window).map_err(|_| input("negative SPECTRAL_WINDOW_ID"))?;
        let freq = spw.chan_freq(index)?;
        let res = spw.resolution(index)?;
        if freq.len() != res.len() || lengths.is_some_and(|n| n != freq.len()) {
            return Err(input("spectral windows have different channel counts"));
        }
        lengths = Some(freq.len());
        frequencies.extend(freq);
        resolution.extend(res);
    }
    let shape = [lengths.unwrap_or(0), windows.len()];
    let freq = RecordValue::new(vec![
        RecordField::new(
            "chan_freq",
            Value::Array(ArrayValue::Float64(shaped(&shape, frequencies)?)),
        ),
        RecordField::new(
            "resolution",
            Value::Array(ArrayValue::Float64(shaped(&shape, resolution)?)),
        ),
    ]);
    Ok(RecordValue::new(vec![
        RecordField::new(
            "corr_axis",
            Value::Array(ArrayValue::String(vector(correlations))),
        ),
        RecordField::new("freq_axis", Value::Record(freq)),
    ]))
}

fn vector<T>(values: Vec<T>) -> ArrayD<T> {
    ndarray::Array1::from_vec(values).into_dyn()
}

fn shaped<T>(shape: &[usize], values: Vec<T>) -> MsResult<ArrayD<T>> {
    ArrayD::from_shape_vec(IxDyn(shape).f(), values).map_err(|error| input(error.to_string()))
}

fn input(message: impl Into<String>) -> MsError {
    MsError::InvalidInput(message.into())
}
