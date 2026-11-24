// SPDX-License-Identifier: LGPL-3.0-or-later
//! Metadata and visibility ranges over the public MS tool's selected rows.

use std::collections::{BTreeMap, BTreeSet};

use casa_coordinates::StokesType;
use casa_tables::{PlainReferenceRead, RequiredScalarColumnValues, Table};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{Array1, ArrayD, IxDyn, ShapeBuilder};

use crate::tool_visibility::{VisibilityReadOptions, read_reference_scalars, read_tool_visibility};
use crate::{MeasurementSet, MsError, MsResult};

const SPECTRAL_ITEMS: &[&str] = &[
    "chan_freq",
    "corr_names",
    "corr_types",
    "num_chan",
    "num_corr",
    "ref_frequency",
];

/// A Python MS range request in normalized CASA field names.
pub struct ToolRangeRequest {
    names: BTreeSet<String>,
    samples: Vec<String>,
    use_flags: bool,
    block_bytes: usize,
}

impl ToolRangeRequest {
    /// Parse item names and set the visibility block budget in decimal MB.
    /// A nonpositive size keeps CASA's 10 MB default.
    pub fn new(items: &[String], use_flags: bool, blocksize: i64) -> Self {
        let names = items
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        let samples = names
            .iter()
            .filter(|name| is_sample_item(name))
            .cloned()
            .collect();
        let megabytes = if blocksize <= 0 {
            10
        } else {
            blocksize as usize
        };
        Self {
            names,
            samples,
            use_flags,
            block_bytes: megabytes.saturating_mul(1_000_000),
        }
    }

    /// Whether this request uses metadata from the linked subtables.
    pub fn needs_metadata(&self) -> bool {
        self.has_spectral_items()
            || !self.samples.is_empty()
            || ["antennas", "fields", "phase_dir"]
                .iter()
                .any(|&name| self.names.contains(name))
    }

    fn has_spectral_items(&self) -> bool {
        SPECTRAL_ITEMS.iter().any(|&name| self.names.contains(name))
    }

    /// Read ranges over logical MAIN rows in the supplied view.
    /// In-row channel and polarization selections do not affect these ranges.
    ///
    /// # Errors
    /// A requested column, descriptor, or subtable cannot be read.
    pub fn read(
        &self,
        main: &Table,
        members: &[Table],
        reference: Option<&PlainReferenceRead>,
        rows: &[usize],
        metadata: Option<&MeasurementSet>,
    ) -> MsResult<RecordValue> {
        if rows.is_empty() {
            return Ok(RecordValue::new(Vec::new()));
        }
        let mut result = BTreeMap::new();
        scalar_ranges(main, members, reference, rows, &self.names, &mut result)?;
        if self.needs_metadata() {
            let ms = metadata
                .ok_or_else(|| MsError::InvalidInput("range requires linked MS metadata".into()))?;
            name_ranges(ms, &self.names, &mut result)?;
            if self.has_spectral_items() || !self.samples.is_empty() {
                let descriptions = selected_descriptions(main, reference, rows)?;
                let axes = RangeSpectralAxes::new(ms, descriptions)?;
                axes.selected_items(ms, &self.names, &mut result)?;
                if axes.has_common_shape() && !self.samples.is_empty() {
                    let rows_per_block =
                        self.block_bytes / axes.bytes_per_row(self.samples.len(), self.use_flags);
                    result.extend(visibility_ranges(
                        main,
                        members,
                        reference,
                        rows,
                        &self.samples,
                        self.use_flags,
                        rows_per_block,
                    )?);
                }
            }
        }
        Ok(RecordValue::new(
            result
                .into_iter()
                .map(|(name, value)| RecordField { name, value })
                .collect(),
        ))
    }
}

fn is_sample_item(name: &str) -> bool {
    if matches!(name, "float_data" | "weight" | "sigma") {
        return true;
    }
    let quantity = [
        "corrected_",
        "model_",
        "ratio_",
        "obs_residual_",
        "residual_",
    ]
    .into_iter()
    .find_map(|prefix| name.strip_prefix(prefix))
    .unwrap_or(name);
    matches!(quantity, "amplitude" | "phase" | "real" | "imaginary")
}

fn selected_descriptions(
    main: &Table,
    reference: Option<&PlainReferenceRead>,
    rows: &[usize],
) -> MsResult<BTreeSet<i32>> {
    let column = "DATA_DESC_ID";
    let mut values = if let Some(reference) = reference {
        read_reference_scalars(reference, &[column], &reference.parent_rows(rows)?)?
    } else {
        main.required_scalar_columns_owned_for_rows(&[column], rows)?
    };
    let Some(RequiredScalarColumnValues::Int32(ids)) = values.remove(column) else {
        return Err(MsError::InvalidInput(
            "DATA_DESC_ID must contain an integer".into(),
        ));
    };
    Ok(ids.into_iter().collect())
}

#[derive(Clone, Copy, Default)]
struct Extrema(Option<[f64; 2]>);

impl Extrema {
    fn add(&mut self, value: f64) {
        let [low, high] = self.0.get_or_insert([value, value]);
        if value < *low {
            *low = value;
        }
        if value > *high {
            *high = value;
        }
    }

    fn array(self) -> ArrayValue {
        ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[2]), self.0.unwrap_or_default().to_vec())
                .expect("two extrema"),
        )
    }
}

fn visibility_ranges(
    main: &Table,
    members: &[Table],
    reference: Option<&PlainReferenceRead>,
    rows: &[usize],
    products: &[String],
    flags: bool,
    rows_per_block: usize,
) -> MsResult<BTreeMap<String, Value>> {
    if products.is_empty() || rows.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut names = products.to_vec();
    if flags
        && products
            .iter()
            .any(|name| !matches!(name.as_str(), "weight" | "sigma"))
    {
        names.push("flag".into());
    }
    let mut ranges = BTreeMap::new();
    for block in rows.chunks(rows_per_block.max(1)) {
        let record = read_tool_visibility(
            main,
            members,
            reference,
            block,
            &names,
            None,
            VisibilityReadOptions::default(),
        )?;
        accumulate_block(&record, products, &mut ranges)?;
    }
    Ok(ranges
        .into_iter()
        .map(|(name, range): (String, Extrema)| (name, Value::Array(range.array())))
        .collect())
}

fn accumulate_block(
    record: &RecordValue,
    products: &[String],
    ranges: &mut BTreeMap<String, Extrema>,
) -> MsResult<()> {
    let flags = match record.get("flag") {
        Some(Value::Array(ArrayValue::Bool(flags))) => Some(flags),
        _ => None,
    };
    for name in products {
        let Some(Value::Array(array)) = record.get(name) else {
            continue;
        };
        if array.is_empty() {
            continue;
        }
        let range = ranges.entry(name.clone()).or_default();
        let mask = if matches!(name.as_str(), "weight" | "sigma") {
            None
        } else {
            flags
        };
        match array {
            ArrayValue::Float32(values) => accumulate(values, mask, range)?,
            ArrayValue::Float64(values) => accumulate(values, mask, range)?,
            _ => {
                return Err(MsError::InvalidInput(format!(
                    "invalid range data type for {name}"
                )));
            }
        }
    }
    Ok(())
}

fn accumulate<T>(
    values: &ArrayD<T>,
    flags: Option<&ArrayD<bool>>,
    range: &mut Extrema,
) -> MsResult<()>
where
    T: Copy + Into<f64>,
{
    if let Some(flags) = flags {
        if values.shape() != flags.shape() {
            return Err(MsError::InvalidInput(
                "FLAG and visibility range axes differ".into(),
            ));
        }
        for (&value, &flagged) in values.iter().zip(flags.iter()) {
            if !flagged {
                range.add(value.into());
            }
        }
    } else {
        for &value in values {
            range.add(value.into());
        }
    }
    Ok(())
}

const INTEGER_ITEMS: &[&str] = &[
    "antenna1",
    "antenna2",
    "array_id",
    "data_desc_id",
    "feed1",
    "feed2",
    "field_id",
    "ifr_number",
    "scan_number",
];

const COORDINATE_ITEMS: &[&str] = &["u", "v", "w", "uvdist"];

fn scalar_ranges(
    main: &Table,
    members: &[Table],
    reference: Option<&PlainReferenceRead>,
    rows: &[usize],
    want: &BTreeSet<String>,
    result: &mut BTreeMap<String, Value>,
) -> MsResult<()> {
    let mut names = INTEGER_ITEMS
        .iter()
        .chain(COORDINATE_ITEMS)
        .filter(|&&name| want.contains(name))
        .map(|&name| name.to_owned())
        .collect::<Vec<_>>();
    let want_time = want.contains("time");
    let want_times = want.contains("times");
    if want_time || want_times {
        names.push("time".into());
    }
    if !names.is_empty() {
        let record = read_tool_visibility(
            main,
            members,
            reference,
            rows,
            &names,
            None,
            VisibilityReadOptions::default(),
        )?;
        project_scalar_ranges(&record, want_time, want_times, result);
    }
    if want.contains("rows") {
        let rows: Vec<i64> = if let Some(reference) = reference {
            reference
                .parent_rows(rows)?
                .into_iter()
                .map(|row| row as i64)
                .collect()
        } else {
            let root = main.root_row_numbers()?;
            rows.iter().map(|&row| root[row] as i64).collect()
        };
        result.insert("rows".into(), Value::Array(ArrayValue::Int64(vector(rows))));
    }
    Ok(())
}

fn project_scalar_ranges(
    record: &RecordValue,
    want_time: bool,
    want_times: bool,
    result: &mut BTreeMap<String, Value>,
) {
    for &name in INTEGER_ITEMS {
        if let Some(Value::Array(ArrayValue::Int32(values))) = record.get(name) {
            let unique = values.iter().copied().collect::<BTreeSet<_>>();
            result.insert(
                name.into(),
                Value::Array(ArrayValue::Int32(vector(unique.into_iter().collect()))),
            );
        }
    }
    for &name in COORDINATE_ITEMS {
        if let Some(Value::Array(ArrayValue::Float64(values))) = record.get(name) {
            let mut range = Extrema::default();
            values.iter().for_each(|&value| range.add(value));
            result.insert(name.into(), Value::Array(range.array()));
        }
    }
    if let Some(Value::Array(ArrayValue::Float64(values))) = record.get("time") {
        if want_time {
            let mut range = Extrema::default();
            values.iter().for_each(|&value| range.add(value));
            result.insert("time".into(), Value::Array(range.array()));
        }
        if want_times {
            let mut values = values.iter().copied().collect::<Vec<_>>();
            values.sort_unstable_by(f64::total_cmp);
            values.dedup_by(|left, right| *left == *right);
            result.insert(
                "times".into(),
                Value::Array(ArrayValue::Float64(vector(values))),
            );
        }
    }
}

fn name_ranges(
    ms: &MeasurementSet,
    want: &BTreeSet<String>,
    result: &mut BTreeMap<String, Value>,
) -> MsResult<()> {
    if want.contains("antennas") {
        let antenna = ms.antenna()?;
        let names = (0..antenna.row_count())
            .map(|id| antenna.name(id))
            .collect::<MsResult<_>>()?;
        result.insert(
            "antennas".into(),
            Value::Array(ArrayValue::String(vector(names))),
        );
    }
    if want.contains("fields") {
        let field = ms.field()?;
        let names = (0..field.row_count())
            .map(|id| field.name(id))
            .collect::<MsResult<_>>()?;
        result.insert(
            "fields".into(),
            Value::Array(ArrayValue::String(vector(names))),
        );
    }
    if want.contains("phase_dir") {
        let field = ms.field()?;
        let rows = field.row_count();
        let mut directions = Vec::with_capacity(2 * rows);
        let mut reference = None;
        for id in 0..rows {
            let direction = field
                .metadata_direction_at_seconds(
                    id,
                    crate::subtables::FieldDirectionColumn::Phase,
                    0.0,
                )?
                .direction;
            reference.get_or_insert(direction.refer().as_str());
            let (longitude, latitude) = direction.as_angles();
            directions.extend([longitude, latitude]);
        }
        let angles = ArrayD::from_shape_vec(IxDyn(&[2, rows]).f(), directions)
            .map_err(|error| MsError::InvalidInput(error.to_string()))?;
        let epoch =
            reference.ok_or_else(|| MsError::InvalidInput("no FIELD phase direction".into()))?;
        let record = RecordValue::new(vec![
            RecordField::new("direction", Value::Array(ArrayValue::Float64(angles))),
            RecordField::new(
                "epoch",
                Value::Scalar(ScalarValue::String(epoch.to_owned())),
            ),
        ]);
        result.insert("phase_dir".into(), Value::Record(record));
    }
    Ok(())
}

fn description_index(id: i32, label: &str) -> MsResult<usize> {
    usize::try_from(id).map_err(|_| MsError::InvalidInput(format!("negative {label}")))
}

struct RangeSpectralAxes {
    windows: Vec<usize>,
    polarizations: Vec<usize>,
    channel_counts: Vec<i32>,
    correlation_counts: Vec<i32>,
}

impl RangeSpectralAxes {
    fn new(ms: &MeasurementSet, descriptions: impl IntoIterator<Item = i32>) -> MsResult<Self> {
        let dd = ms.data_description()?;
        let spw = ms.spectral_window()?;
        let pol = ms.polarization()?;
        let mut axes = Self {
            windows: Vec::new(),
            polarizations: Vec::new(),
            channel_counts: Vec::new(),
            correlation_counts: Vec::new(),
        };
        for id in descriptions {
            let row = description_index(id, "DATA_DESC_ID")?;
            let window = description_index(dd.spectral_window_id(row)?, "SPECTRAL_WINDOW_ID")?;
            let polarization = description_index(dd.polarization_id(row)?, "POLARIZATION_ID")?;
            axes.windows.push(window);
            axes.polarizations.push(polarization);
            axes.channel_counts.push(spw.num_chan(window)?);
            axes.correlation_counts.push(pol.num_corr(polarization)?);
        }
        Ok(axes)
    }

    fn has_common_shape(&self) -> bool {
        self.channel_counts.windows(2).all(|w| w[0] == w[1])
            && self.correlation_counts.windows(2).all(|w| w[0] == w[1])
    }

    fn bytes_per_row(&self, products: usize, flags: bool) -> usize {
        let correlations = self.correlation_counts.first().copied().unwrap_or(1).max(1) as usize;
        let channels = self.channel_counts.first().copied().unwrap_or(1).max(1) as usize;
        // Two complex inputs suffice for ratio and residual, with space for
        // projected output blocks and the optional Boolean mask.
        correlations
            .saturating_mul(channels)
            .saturating_mul(16 + 8 * products + usize::from(flags))
    }

    fn selected_items(
        &self,
        ms: &MeasurementSet,
        names: &BTreeSet<String>,
        result: &mut BTreeMap<String, Value>,
    ) -> MsResult<()> {
        let spw = ms.spectral_window()?;
        let pol = ms.polarization()?;
        for name in names {
            let value = match name.as_str() {
                "num_chan" => ArrayValue::Int32(vector(self.channel_counts.clone())),
                "num_corr" => ArrayValue::Int32(vector(self.correlation_counts.clone())),
                "ref_frequency" => ArrayValue::Float64(vector(
                    self.windows
                        .iter()
                        .map(|&id| spw.ref_frequency(id))
                        .collect::<MsResult<_>>()?,
                )),
                "chan_freq" if self.has_common_shape() => {
                    ArrayValue::Float64(self.matrix(&self.windows, |id| spw.chan_freq(id))?)
                }
                "corr_types" if self.has_common_shape() => {
                    ArrayValue::Int32(self.matrix(&self.polarizations, |id| pol.corr_type(id))?)
                }
                "corr_names" if self.has_common_shape() => ArrayValue::String(
                    self.matrix(&self.polarizations, |id| pol.corr_type(id))?
                        .mapv(|code| {
                            StokesType::from_code(code)
                                .map_or("??", StokesType::name)
                                .to_owned()
                        }),
                ),
                _ => continue,
            };
            result.insert(name.clone(), Value::Array(value));
        }
        Ok(())
    }

    fn matrix<T>(
        &self,
        ids: &[usize],
        values: impl Fn(usize) -> MsResult<Vec<T>>,
    ) -> MsResult<ArrayD<T>> {
        let mut length = None;
        let mut columns = Vec::new();
        for &id in ids {
            let column = values(id)?;
            if length.is_some_and(|expected| expected != column.len()) {
                return Err(MsError::InvalidInput(
                    "inconsistent ranges across spectral descriptions".into(),
                ));
            }
            length = Some(column.len());
            columns.extend(column);
        }
        ArrayD::from_shape_vec(IxDyn(&[length.unwrap_or(0), ids.len()]).f(), columns)
            .map_err(|err| MsError::InvalidInput(err.to_string()))
    }
}

fn vector<T>(values: Vec<T>) -> ArrayD<T> {
    Array1::from_vec(values).into_dyn()
}
