// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column reads for the legacy MeasurementSet tool visibility record.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use casa_tables::{
    PlainReferenceRead, RequiredScalarColumnValues, SelectedArray1DCells, Table, TableColumn,
    TablePartNames,
};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use num_complex::Complex32;

use crate::{MeasurementSet, MsError, MsResult};

mod averaging;
mod concatenation;
use averaging::RowAverage;
use concatenation::{MemberRows, concatenate_arrays, concatenate_scalars, member_rows};

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
/// With `average`, visibility products are reduced across rows using flags and
/// the weight of each row and correlation before amplitude or phase projection.
/// Contiguous groups of logical rows can reuse open plain members supplied in
/// concatenation order. Other views use the table's persisted row projection.
/// # Errors
/// A requested column cannot be read as a rectangular array.
pub fn read_tool_visibility(
    main: &Table,
    members: &[Table],
    reference: Option<&PlainReferenceRead>,
    rows: &[usize],
    items: &[String],
    metadata: Option<&MeasurementSet>,
    average: bool,
) -> MsResult<RecordValue> {
    let mut names = items
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    if average {
        // CASA interprets this item as a visibility product with the averaging
        // prefix, then discards it because no such product exists.
        names.remove("data_desc_id");
    }
    let mut read = VisibilityRead::new(main, members, reference, rows, &names)?;
    let mut fields = BTreeMap::new();
    if names.contains("axis_info") {
        let RequiredScalarColumnValues::Int32(ids) = &read.scalar_columns["DATA_DESC_ID"] else {
            return Err(input("DATA_DESC_ID must contain an integer"));
        };
        let ms = metadata.ok_or_else(|| input("axis_info requires MeasurementSet metadata"))?;
        fields.insert("axis_info".into(), Value::Record(axis_info(ms, ids)?));
    }
    read.scalars(&names, &mut fields, average)?;
    if average {
        let reduced = read.products(&names, &mut fields, true, items)?;
        read.arrays(&names, &mut fields, true, reduced)?;
    } else {
        read.arrays(&names, &mut fields, false, None)?;
        read.products(&names, &mut fields, false, items)?;
    }
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
    members: Vec<MemberRows<'a>>,
    reference: Option<ReferenceRows<'a>>,
    scalar_columns: HashMap<String, RequiredScalarColumnValues>,
}

struct ReferenceRows<'a> {
    read: &'a PlainReferenceRead,
    rows: Vec<usize>,
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
    fn new(
        main: &'a Table,
        members: &'a [Table],
        reference: Option<&'a PlainReferenceRead>,
        rows: &'a [usize],
        names: &BTreeSet<String>,
    ) -> MsResult<Self> {
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
        let plain = if let Some(path) = main
            .path()
            .filter(|_| members.is_empty() && reference.is_none())
        {
            matches!(
                Table::immediate_part_names(path, path)?,
                TablePartNames::Plain(_)
            )
        } else {
            false
        };
        let members = member_rows(members, main.row_count(), rows);
        let reference = reference
            .map(|read| {
                read.parent_rows(rows)
                    .map(|rows| ReferenceRows { read, rows })
            })
            .transpose()?;
        let scalar_columns = if columns.is_empty() {
            HashMap::new()
        } else if !members.is_empty() {
            concatenate_scalars(&members, &columns)?
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
            members,
            reference,
            scalar_columns,
        })
    }

    fn vector_cells(&self, name: &str) -> MsResult<ArrayValue> {
        if self.members.is_empty() {
            return read_vector_cells(self.main, name, self.rows, self.plain);
        }
        concatenate_arrays(&self.members, |table, rows| {
            read_vector_cells(table, name, rows, true)
        })
    }

    fn matrix_cells(&self, name: &str, widen_complex: bool) -> MsResult<ArrayValue> {
        if let Some(reference) = &self.reference {
            return read_matrix_cells(
                reference.read.parent(),
                reference.read.parent_column(name)?,
                &reference.rows,
                widen_complex,
            );
        }
        if self.members.is_empty() {
            return read_matrix_cells(self.main, name, self.rows, widen_complex);
        }
        concatenate_arrays(&self.members, |table, rows| {
            read_matrix_cells(table, name, rows, false)
        })
    }

    fn scalars(
        &mut self,
        names: &BTreeSet<String>,
        fields: &mut BTreeMap<String, Value>,
        average: bool,
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
            let mut ifrs = first
                .iter()
                .zip(second)
                .map(|(&left, &right)| left.wrapping_mul(1000).wrapping_add(right))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            if average {
                averaging::common_id(&mut ifrs);
            }
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
                RequiredScalarColumnValues::Int32(mut values) => {
                    if average {
                        averaging::common_id(&mut values);
                    }
                    ArrayValue::Int32(vector(values))
                }
                RequiredScalarColumnValues::Float32(values) => ArrayValue::Float32(vector(values)),
                RequiredScalarColumnValues::Float64(values) if average && name == "time" => {
                    let mean = values.iter().sum::<f64>() / values.len() as f64;
                    ArrayValue::Float64(vector(vec![mean]))
                }
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
        average: bool,
        reduced: Option<RowAverage>,
    ) -> MsResult<()> {
        let (raw_flags, reduced_flags) = if let Some(reduced) = reduced {
            let (raw, flags, weights) = reduced.into_fields();
            let name = if names.contains("weight") {
                "weight"
            } else {
                "avgweight"
            };
            fields.insert(name.into(), Value::Array(weights));
            (Some(ArrayValue::Bool(raw)), Some(flags))
        } else {
            if names.contains("weight") {
                fields.insert("weight".into(), Value::Array(self.vector_cells("WEIGHT")?));
            }
            (None, None)
        };
        if names.contains("sigma") {
            let values = self.vector_cells("SIGMA")?;
            let values = if average {
                averaging::sigma(values)?
            } else {
                values
            };
            fields.insert("sigma".into(), Value::Array(values));
        }
        if names.contains("flag") || names.contains("flag_sum") {
            let flags = match raw_flags {
                Some(flags) => flags,
                None => self.matrix_cells("FLAG", false)?,
            };
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
                let flags = reduced_flags.map_or(flags, ArrayValue::Bool);
                fields.insert("flag".into(), Value::Array(flags));
            }
        }
        if names
            .iter()
            .any(|name| matches!(name.as_str(), "uvw" | "u" | "v" | "w" | "uvdist"))
        {
            let uvw = self.vector_cells("UVW")?;
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
        average: bool,
        items: &[String],
    ) -> MsResult<Option<RowAverage>> {
        let has_column = |column| {
            self.main
                .schema()
                .is_some_and(|schema| schema.column(column).is_some())
        };
        let has_model = has_column("MODEL_DATA");
        let has_corrected = has_column("CORRECTED_DATA");
        let has_float = has_column("FLOAT_DATA");
        let mut aliases: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for name in names {
            let missing_float = !has_float && name.contains("float");
            let missing_complex = (!has_model
                && ["model", "ratio", "residual"]
                    .iter()
                    .any(|&item| name.contains(item)))
                || (!has_corrected
                    && (name.contains("corrected")
                        || name.contains("ratio")
                        || (name.contains("residual") && !name.contains("obs"))));
            if missing_float || missing_complex {
                // CASA checks optional-column substrings before recognizing item names.
                let empty = if !missing_float && name.contains("data") {
                    ArrayValue::Complex32(vector(Vec::new()))
                } else {
                    ArrayValue::Float32(vector(Vec::new()))
                };
                fields.insert(name.clone(), Value::Array(empty));
            } else if let Some(derived) = name.strip_prefix("avg_") {
                if average
                    && ["data", "amplitude", "phase", "real", "imaginary"]
                        .iter()
                        .any(|&part| name.contains(part))
                {
                    // CASA adds another averaging prefix and does not recognize it.
                    continue;
                }
                if average
                    && matches!(
                        derived,
                        "antenna1" | "antenna2" | "feed1" | "feed2" | "sigma" | "time"
                    )
                    && !names.contains(derived)
                {
                    return Err(input(format!(
                        "RecordInterface: field {derived} is unknown"
                    )));
                }
                // The ordinary CASA reader accepts this prefix and returns its
                // underlying visibility column without applying a projection.
                let base = derived
                    .rsplit_once('_')
                    .map_or_else(|| "data".to_owned(), |(prefix, _)| format!("{prefix}_data"));
                aliases.entry(base).or_default().push(name.clone());
            }
        }
        let mut reduction = None;
        for (prefix, source, uses_model) in [
            ("", "DATA", false),
            ("corrected_", "CORRECTED_DATA", false),
            ("model_", "MODEL_DATA", false),
            ("ratio_", "CORRECTED_DATA", true),
            ("residual_", "CORRECTED_DATA", true),
            ("obs_residual_", "DATA", true),
        ] {
            let requested = ["data", "amplitude", "phase", "real", "imaginary"].map(|name| {
                let key = format!("{prefix}{name}");
                (names.contains(&key) && !fields.contains_key(&key)).then_some(key)
            });
            let mut raw_aliases = aliases.remove(&format!("{prefix}data"));
            let explicit = requested.iter().any(Option::is_some);
            if !explicit && raw_aliases.is_none() {
                continue;
            }
            let reduce = average && explicit;
            if reduce {
                for key in requested.iter().flatten() {
                    require_single_average(items, key)?;
                }
            }
            let [raw, amplitude, phase, real, imaginary] = requested;
            let only_raw = !uses_model
                && [&amplitude, &phase, &real, &imaginary]
                    .into_iter()
                    .all(Option::is_none);
            let data = self.matrix_cells(source, only_raw && !reduce)?;
            let data = if uses_model {
                let model_values = self.matrix_cells("MODEL_DATA", false)?;
                let (ArrayValue::Complex32(mut observed), ArrayValue::Complex32(expected)) =
                    (data, model_values)
                else {
                    return Err(input("visibility columns must contain Complex values"));
                };
                if observed.shape() != expected.shape() {
                    return Err(input("visibility columns have different shapes"));
                }
                if prefix == "ratio_" {
                    observed.zip_mut_with(&expected, |observed, expected| {
                        *observed = divide_visibility(*observed, *expected);
                    });
                } else {
                    observed.zip_mut_with(&expected, |observed, expected| *observed -= *expected);
                }
                ArrayValue::Complex32(observed)
            } else {
                data
            };
            let data = if reduce {
                let means = self.average_product(&mut reduction, &data)?;
                if let Some(aliases) = raw_aliases.take() {
                    insert_array_aliases(fields, aliases.into_iter(), data);
                }
                means
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
            insert_array_aliases(
                fields,
                raw.into_iter().chain(raw_aliases.into_iter().flatten()),
                data,
            );
        }
        let raw_float = names
            .contains("float_data")
            .then(|| "float_data".to_owned());
        let mut float_aliases = aliases.remove("float_data");
        if has_float && (raw_float.is_some() || float_aliases.is_some()) {
            let values = self.matrix_cells("FLOAT_DATA", false)?;
            let values = if average && raw_float.is_some() {
                require_single_average(items, "float_data")?;
                let means = self.average_product(&mut reduction, &values)?;
                if let Some(aliases) = float_aliases.take() {
                    insert_array_aliases(fields, aliases.into_iter(), values);
                }
                means
            } else {
                values
            };
            insert_array_aliases(
                fields,
                raw_float
                    .into_iter()
                    .chain(float_aliases.into_iter().flatten()),
                values,
            );
        }
        Ok(reduction)
    }

    fn average_product(
        &self,
        reduction: &mut Option<RowAverage>,
        values: &ArrayValue,
    ) -> MsResult<ArrayValue> {
        if reduction.is_none() {
            *reduction = Some(RowAverage::new(
                self.matrix_cells("FLAG", false)?,
                self.vector_cells("WEIGHT")?,
            )?);
        }
        reduction
            .as_ref()
            .expect("averaging input exists")
            .reduce(values)
    }
}

fn require_single_average(items: &[String], name: &str) -> MsResult<()> {
    if items
        .iter()
        .filter(|item| item.eq_ignore_ascii_case(name))
        .nth(1)
        .is_some()
    {
        return Err(input(format!(
            "RecordInterface: field avg_{name} is unknown"
        )));
    }
    Ok(())
}

fn insert_array_aliases(
    fields: &mut BTreeMap<String, Value>,
    names: impl Iterator<Item = String>,
    values: ArrayValue,
) {
    let mut names = names.peekable();
    while let Some(name) = names.next() {
        if names.peek().is_none() {
            fields.insert(name, Value::Array(values));
            break;
        }
        fields.insert(name, Value::Array(values.clone()));
    }
}

fn divide_visibility(sample: Complex32, model: Complex32) -> Complex32 {
    let [real, imaginary, model_real, model_imaginary] =
        [sample.re, sample.im, model.re, model.im].map(f64::from);
    // Every finite Float32 product, including two subnormal values, fits
    // in Float64. Widened arithmetic avoids a spurious zero or infinite norm.
    let norm = model_real * model_real + model_imaginary * model_imaginary;
    let observed_dot = real * model_real + imaginary * model_imaginary;
    let observed_cross = imaginary * model_real - real * model_imaginary;
    let quotient = (observed_dot / norm, observed_cross / norm);
    let signed_unit_for_infinity = |value: f64| {
        let magnitude: f64 = if value.is_infinite() { 1.0 } else { 0.0 };
        magnitude.copysign(value)
    };
    let (result_real, result_imaginary) = if !quotient.0.is_nan() || !quotient.1.is_nan() {
        quotient
    } else if norm == 0.0 {
        let infinite = f64::INFINITY.copysign(model_real);
        (infinite * real, infinite * imaginary)
    } else if (real.is_infinite() || imaginary.is_infinite())
        && model_real.is_finite()
        && model_imaginary.is_finite()
    {
        let real = signed_unit_for_infinity(real);
        let imaginary = signed_unit_for_infinity(imaginary);
        (
            f64::INFINITY * (real * model_real + imaginary * model_imaginary),
            f64::INFINITY * (imaginary * model_real - real * model_imaginary),
        )
    } else if (model_real.is_infinite() || model_imaginary.is_infinite())
        && real.is_finite()
        && imaginary.is_finite()
    {
        let model_real = signed_unit_for_infinity(model_real);
        let model_imaginary = signed_unit_for_infinity(model_imaginary);
        (
            0.0 * (real * model_real + imaginary * model_imaginary),
            0.0 * (imaginary * model_real - real * model_imaginary),
        )
    } else {
        quotient
    };
    Complex32::new(result_real as f32, result_imaginary as f32)
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
    let has_virtual_columns = main.schema().is_some_and(|schema| {
        schema
            .columns()
            .iter()
            .any(|desc| main.is_virtual_column(desc.name()))
    });
    let float_in_incremental_storage = name != "UVW"
        && main.data_manager_info().iter().any(|manager| {
            manager.dm_type == "IncrementalStMan"
                && manager.columns.iter().any(|column| column == name)
        });
    if !plain || has_virtual_columns || float_in_incremental_storage {
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
        .collect::<MsResult<Vec<_>>>()?;
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
