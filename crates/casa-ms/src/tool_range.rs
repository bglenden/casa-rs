// SPDX-License-Identifier: LGPL-3.0-or-later
//! Metadata and visibility ranges over the public MS tool's selected rows.

use casa_coordinates::StokesType;
use casa_tables::{PlainReferenceRead, RequiredScalarColumnValues, Table};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{Array1, ArrayD, IxDyn, ShapeBuilder};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Deref;

use crate::subtables::{
    FieldDirectionColumn, MsAntenna, MsDataDescription, MsField, MsPolarization, MsSpectralWindow,
    linked_subtable_row,
};
use crate::tool_visibility::{
    VisibilityReadOptions, concatenate_scalars, member_rows, read_reference_scalars,
    read_tool_visibility,
};
use crate::{MsError, MsResult, SubtableId};

mod samples;
use samples::RangeSamples;

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
    wants_samples: bool,
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
        let wants_samples = names.iter().any(|name| is_sample_item(name));
        let megabytes = if blocksize <= 0 {
            10
        } else {
            blocksize as usize
        };
        Self {
            names,
            wants_samples,
            use_flags,
            block_bytes: megabytes.saturating_mul(1_000_000),
        }
    }

    fn has_spectral_items(&self) -> bool {
        SPECTRAL_ITEMS.iter().any(|&name| self.names.contains(name))
    }

    /// Read ranges over logical MAIN rows in the supplied view.
    /// In-row channel and polarization selections do not affect these ranges.
    ///
    /// # Errors
    /// A requested column, descriptor, or subtable cannot be read.
    pub fn read<T: Deref<Target = [Table]>>(
        &self,
        main: &Table,
        members: &[Table],
        reference: Option<&PlainReferenceRead>,
        rows: &[usize],
        read_linked: impl FnOnce(&[SubtableId]) -> MsResult<HashMap<SubtableId, T>>,
    ) -> MsResult<RecordValue> {
        if rows.is_empty() {
            return Ok(RecordValue::new(Vec::new()));
        }
        let mut result = BTreeMap::new();
        scalar_ranges(main, members, reference, rows, &self.names, &mut result)?;
        let spectral = self.has_spectral_items() || self.wants_samples;
        let mut ids = Vec::new();
        if self.names.contains("antennas") {
            ids.push(SubtableId::Antenna);
        }
        if self.names.contains("fields") || self.names.contains("phase_dir") {
            ids.push(SubtableId::Field);
        }
        if spectral {
            ids.extend([
                SubtableId::DataDescription,
                SubtableId::SpectralWindow,
                SubtableId::Polarization,
            ]);
        }
        let linked = read_linked(&ids)?;
        let linked_subtable = |id: SubtableId| -> MsResult<&[Table]> {
            linked
                .get(&id)
                .map(Deref::deref)
                .ok_or_else(|| MsError::MissingSubtable(id.name().into()))
        };
        name_ranges(&linked_subtable, &self.names, &mut result)?;
        if spectral {
            let descriptions = selected_descriptions(main, members, reference, rows)?;
            let description = linked_subtable(SubtableId::DataDescription)?;
            let windows = linked_subtable(SubtableId::SpectralWindow)?;
            let polarization = linked_subtable(SubtableId::Polarization)?;
            let axes = RangeSpectralAxes::new(description, windows, polarization, descriptions)?;
            axes.selected_items(&self.names, &mut result)?;
            if axes.has_common_shape() && self.wants_samples {
                result.extend(RangeSamples::new(main, &self.names, self.use_flags).read(
                    main,
                    members,
                    reference,
                    rows,
                    axes.shape(),
                    self.block_bytes,
                )?);
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
    members: &[Table],
    reference: Option<&PlainReferenceRead>,
    rows: &[usize],
) -> MsResult<BTreeSet<i32>> {
    let column = "DATA_DESC_ID";
    let member_rows = member_rows(members, main.row_count(), rows);
    let mut values = if !member_rows.is_empty() {
        concatenate_scalars(&member_rows, &[column])?
    } else if let Some(reference) = reference {
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

fn name_ranges<'a>(
    linked_subtable: &impl Fn(SubtableId) -> MsResult<&'a [Table]>,
    want: &BTreeSet<String>,
    result: &mut BTreeMap<String, Value>,
) -> MsResult<()> {
    if want.contains("antennas") {
        let tables = linked_subtable(SubtableId::Antenna)?;
        let mut names = Vec::new();
        for table in tables.iter() {
            let antenna = MsAntenna::new(table);
            for row in 0..antenna.row_count() {
                names.push(antenna.name(row)?);
            }
        }
        result.insert(
            "antennas".into(),
            Value::Array(ArrayValue::String(vector(names))),
        );
    }
    let want_fields = want.contains("fields");
    let want_directions = want.contains("phase_dir");
    if !want_fields && !want_directions {
        return Ok(());
    }
    let tables = linked_subtable(SubtableId::Field)?;
    let mut names = Vec::new();
    let mut coordinates = Vec::new();
    let mut frame = None;
    for table in tables.iter() {
        let field = MsField::new(table);
        for row in 0..field.row_count() {
            if want_fields {
                names.push(field.name(row)?);
            }
            if want_directions {
                let direction = field
                    .metadata_direction_at_seconds(row, FieldDirectionColumn::Phase, 0.0)?
                    .direction;
                frame.get_or_insert(direction.refer().as_str());
                let [x, y, _] = direction.cosines();
                coordinates.extend([y.atan2(x), direction.latitude_rad()]);
            }
        }
    }
    if want_fields {
        result.insert(
            "fields".into(),
            Value::Array(ArrayValue::String(vector(names))),
        );
    }
    if want_directions {
        let angles = ArrayD::from_shape_vec(IxDyn(&[2, coordinates.len() / 2]).f(), coordinates)
            .map_err(|error| MsError::InvalidInput(error.to_string()))?;
        let epoch =
            frame.ok_or_else(|| MsError::InvalidInput("no FIELD phase direction".into()))?;
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

struct RangeSpectralAxes<'a> {
    window_tables: &'a [Table],
    polarization_tables: &'a [Table],
    windows: Vec<usize>,
    polarizations: Vec<usize>,
    channel_counts: Vec<i32>,
    correlation_counts: Vec<i32>,
}

impl<'a> RangeSpectralAxes<'a> {
    fn new(
        descriptions_tables: &[Table],
        window_tables: &'a [Table],
        polarization_tables: &'a [Table],
        descriptions: impl IntoIterator<Item = i32>,
    ) -> MsResult<Self> {
        let mut axes = Self {
            window_tables,
            polarization_tables,
            windows: Vec::new(),
            polarizations: Vec::new(),
            channel_counts: Vec::new(),
            correlation_counts: Vec::new(),
        };
        for id in descriptions {
            let row = description_index(id, "DATA_DESC_ID")?;
            let (table, row) =
                linked_subtable_row(descriptions_tables, row, SubtableId::DataDescription)?;
            let description = MsDataDescription::new(table);
            let window =
                description_index(description.spectral_window_id(row)?, "SPECTRAL_WINDOW_ID")?;
            let polarization =
                description_index(description.polarization_id(row)?, "POLARIZATION_ID")?;
            let (window_table, window_row) = axes.window(window)?;
            let (pol_table, pol_row) = axes.polarization(polarization)?;
            axes.windows.push(window);
            axes.polarizations.push(polarization);
            axes.channel_counts.push(window_table.num_chan(window_row)?);
            axes.correlation_counts.push(pol_table.num_corr(pol_row)?);
        }
        Ok(axes)
    }

    fn window(&self, id: usize) -> MsResult<(MsSpectralWindow<'a>, usize)> {
        let (table, row) = linked_subtable_row(self.window_tables, id, SubtableId::SpectralWindow)?;
        Ok((MsSpectralWindow::new(table), row))
    }

    fn polarization(&self, id: usize) -> MsResult<(MsPolarization<'a>, usize)> {
        let (table, row) =
            linked_subtable_row(self.polarization_tables, id, SubtableId::Polarization)?;
        Ok((MsPolarization::new(table), row))
    }

    fn correlation_codes(&self, id: usize) -> MsResult<Vec<i32>> {
        let (pol, row) = self.polarization(id)?;
        pol.corr_type(row)
    }

    fn has_common_shape(&self) -> bool {
        self.channel_counts.windows(2).all(|w| w[0] == w[1])
            && self.correlation_counts.windows(2).all(|w| w[0] == w[1])
    }

    fn shape(&self) -> (usize, usize) {
        let correlations = self.correlation_counts.first().copied().unwrap_or(0).max(0) as usize;
        let channels = self.channel_counts.first().copied().unwrap_or(0).max(0) as usize;
        (correlations, channels)
    }

    fn selected_items(
        &self,
        names: &BTreeSet<String>,
        result: &mut BTreeMap<String, Value>,
    ) -> MsResult<()> {
        for name in names {
            let value = match name.as_str() {
                "num_chan" => ArrayValue::Int32(vector(self.channel_counts.clone())),
                "num_corr" => ArrayValue::Int32(vector(self.correlation_counts.clone())),
                "ref_frequency" => ArrayValue::Float64(vector(
                    self.windows
                        .iter()
                        .map(|&id| {
                            let (window, row) = self.window(id)?;
                            window.ref_frequency(row)
                        })
                        .collect::<MsResult<_>>()?,
                )),
                "chan_freq" if self.has_common_shape() => {
                    ArrayValue::Float64(self.matrix(&self.windows, |id| {
                        let (window, row) = self.window(id)?;
                        window.chan_freq(row)
                    })?)
                }
                "corr_types" if self.has_common_shape() => ArrayValue::Int32(
                    self.matrix(&self.polarizations, |id| self.correlation_codes(id))?,
                ),
                "corr_names" if self.has_common_shape() => ArrayValue::String(
                    self.matrix(&self.polarizations, |id| self.correlation_codes(id))?
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
