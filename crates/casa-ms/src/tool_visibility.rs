// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column reads for the legacy MeasurementSet tool visibility record.

use std::borrow::Cow;
use std::cell::OnceCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, OnceLock};

use casa_tables::{
    PlainReferenceRead, RequiredScalarColumnValues, SelectedArray1DCells, Table, TableColumn,
    TablePartNames,
};
use casa_types::measures::MeasuresProvider;
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use num_complex::Complex32;

use crate::{MeasurementSet, MsError, MsResult};

mod averaging;
mod baseline_axis;
mod channel_axis;
mod concatenation;
mod polarization_axis;
mod projection;
mod vi1;
mod write_record;
mod write_rows;
use averaging::RowAverage;
use baseline_axis::{BaselineGrid, rows_by_time};
use channel_axis::axis_info;
use channel_axis::{ChannelWeightKind, ChannelWeights};
pub use channel_axis::{LegacyVisibilityAxes, VisibilityChannels};
use concatenation::{MemberRows, concatenate_arrays};
pub(crate) use concatenation::{concatenate_scalars, member_rows};
pub use polarization_axis::VisibilityPolarization;
pub(crate) use projection::read_reference_scalars;
pub use vi1::read_vi1_visibility;
pub use write_rows::{
    LegacyVisibilityWriteGrid, LegacyVisibilityWriteOptions, VisibilityWriteOptions,
    VisibilityWriteRecord,
};

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

/// Restrict the current MS tool view to one data description in row order.
/// # Errors
/// The selected rows or their required DATA_DESC_ID scalar cannot be read.
pub fn select_tool_data_description(
    main: &Table,
    members: &[Table],
    reference: Option<&PlainReferenceRead>,
    active: Option<&[usize]>,
    description: i64,
) -> MsResult<Vec<usize>> {
    let rows = active.map_or_else(
        || Cow::Owned((0..main.row_count()).collect()),
        Cow::Borrowed,
    );
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let column = reference
        .map(|reference| reference.parent_column("DATA_DESC_ID"))
        .transpose()?
        .unwrap_or("DATA_DESC_ID");
    let parts = member_rows(members, main.row_count(), &rows);
    let mut ids = if !parts.is_empty() {
        concatenate_scalars(&parts, &[column])?
    } else if let Some(reference) = reference {
        reference
            .parent()
            .required_scalar_columns_owned_for_rows(&[column], &reference.parent_rows(&rows)?)?
    } else {
        main.required_scalar_columns_owned_for_rows(&[column], &rows)?
    };
    let Some(RequiredScalarColumnValues::Int32(ids)) = ids.remove(column) else {
        return Err(input("DATA_DESC_ID must contain an integer"));
    };
    Ok(rows
        .iter()
        .copied()
        .zip(ids)
        .filter_map(|(row, id)| (i64::from(id) == description).then_some(row))
        .collect())
}

/// Smallest spectral-window channel count in the active row view.
/// # Errors
/// A data description or its spectral window cannot be read.
pub fn selected_channel_count(ms: &MeasurementSet) -> MsResult<usize> {
    let dd = ms.data_description()?;
    let spw = ms.spectral_window()?;
    let used = if dd.row_count() == 1 {
        BTreeSet::from([0])
    } else {
        let rows = (0..ms.row_count()).collect::<Vec<_>>();
        let values = ms
            .main_table()
            .column_accessor("DATA_DESC_ID")?
            .scalar_cells_owned_for_rows(&rows)?;
        let RequiredScalarColumnValues::Int32(ids) = typed_scalar_column("DATA_DESC_ID", values)?
        else {
            return Err(input("DATA_DESC_ID must contain an integer"));
        };
        ids.into_iter().collect()
    };
    let mut smallest = None;
    for id in used {
        let id = usize::try_from(id).map_err(|_| input("negative DATA_DESC_ID"))?;
        let window = usize::try_from(dd.spectral_window_id(id)?)
            .map_err(|_| input("negative SPECTRAL_WINDOW_ID"))?;
        let count =
            usize::try_from(spw.num_chan(window)?).map_err(|_| input("negative NUM_CHAN"))?;
        smallest = Some(smallest.map_or(count, |old: usize| old.min(count)));
    }
    smallest.ok_or_else(|| input("no selected spectral windows"))
}

/// Layout and reduction options for a visibility record.
#[derive(Clone, Copy, Default)]
pub struct VisibilityReadOptions<'a> {
    /// Reduce visibility products across rows or within each baseline over time.
    pub average: bool,
    /// Place rows on sorted baseline and integration axes with this many gaps
    /// between first-antenna groups. A negative gap is treated as zero.
    pub baseline_gap: Option<i32>,
    /// Use these antenna pairs for the baseline axis. An iterator reads only
    /// the current chunk while retaining the attachment's baseline coverage.
    pub baseline_pairs: Option<&'a [(i32, i32)]>,
    /// Read a positioned visibility buffer, preserving its iteration order
    /// and applying the visibility iterator's observed-data conventions.
    pub iterator_read: bool,
    /// Apply the legacy selector's conventions and initialized axis metadata.
    pub legacy_axes: Option<LegacyVisibilityAxes<'a>>,
    /// Channel selection applied on each logical row before visibility products.
    pub channels: Option<VisibilityChannels>,
    /// Correlation selection or Stokes projection on the active row view.
    pub polarization: Option<&'a VisibilityPolarization>,
    /// Reuse the attachment's Measures provider for HA and LAST coordinates.
    pub time_measures: Option<&'a OnceLock<Arc<dyn MeasuresProvider>>>,
}

/// Read a visibility record for a logical set of rows.
/// Complex and float operations retain the precision of the stored cells.
/// With `average`, visibility products are reduced using flags and each row
/// and correlation's weight before amplitude or phase projection. With a
/// baseline axis, each antenna pair is reduced over time.
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
    options: VisibilityReadOptions<'_>,
) -> MsResult<RecordValue> {
    let VisibilityReadOptions {
        average,
        baseline_gap,
        baseline_pairs,
        iterator_read,
        time_measures,
        ..
    } = options;
    let legacy = options.legacy_axes.is_some();
    let ordered_rows = if !legacy
        && !iterator_read
        && (options.channels.is_some() || options.polarization.is_some())
        && baseline_gap.is_none()
    {
        rows_by_time(main, rows)?
    } else {
        Cow::Borrowed(rows)
    };
    let mut names = items
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    if legacy {
        names.remove("array_id");
    } else if average {
        // CASA interprets this item as a visibility product with the averaging
        // prefix, then discards it because no such product exists.
        names.remove("data_desc_id");
    }
    let mut read = VisibilityRead::new(
        main,
        members,
        reference,
        &ordered_rows,
        rows.first().copied(),
        &names,
        options,
    )?;
    let grid = baseline_gap
        .map(|gap| {
            if legacy {
                BaselineGrid::legacy(&read.scalar_columns, baseline_pairs, gap)
            } else {
                BaselineGrid::new(&read.scalar_columns, baseline_pairs, gap)
            }
        })
        .transpose()?;
    let mut fields = BTreeMap::new();
    if names.contains("axis_info") {
        let RequiredScalarColumnValues::Int32(ids) = &read.scalar_columns["DATA_DESC_ID"] else {
            return Err(input("DATA_DESC_ID must contain an integer"));
        };
        let ms = metadata.ok_or_else(|| input("axis_info requires MeasurementSet metadata"))?;
        let first_row = grid.as_ref().and_then(BaselineGrid::first_row).unwrap_or(0);
        let mut axes = axis_info(ms, ids, first_row, options)?;
        if let Some(grid) = &grid {
            let first_selected = rows.first().copied().unwrap_or(0);
            let (baseline_axis, time_axis) = grid.axes(
                ms,
                &names,
                &read.scalar_columns,
                time_measures,
                (main, first_selected),
                average,
            )?;
            axes.push(RecordField::new("ifr_axis", Value::Record(baseline_axis)));
            axes.push(RecordField::new("time_axis", Value::Record(time_axis)));
        }
        fields.insert("axis_info".into(), Value::Record(axes));
    }
    if let Some(grid) = &grid
        && names.remove("ifr_number")
    {
        fields.insert("ifr_number".into(), Value::Array(grid.numbers()?));
    }
    read.scalars(&names, &mut fields, average && grid.is_none())?;
    let reduced = if average {
        read.products(&names, &mut fields, true, items, grid.as_ref())?
    } else {
        None
    };
    let reduced_flags = reduced.is_some();
    read.arrays(&names, &mut fields, average, reduced, grid.as_ref())?;
    if !average {
        read.products(&names, &mut fields, false, items, grid.as_ref())?;
    }
    if let Some(grid) = grid {
        grid.remap(&mut fields, average, reduced_flags)?;
    }
    Ok(RecordValue::new(
        fields
            .into_iter()
            .map(|(name, value)| RecordField { name, value })
            .collect(),
    ))
}

/// Return the sorted distinct baselines of the given MAIN row selection.
/// Antenna pairs retain the order of the recorded baseline.
///
/// # Errors
/// The ANTENNA1 and ANTENNA2 columns must be readable integer columns.
pub fn tool_baseline_pairs(
    main: &Table,
    reference: Option<&PlainReferenceRead>,
    rows: Option<&[usize]>,
) -> MsResult<Vec<(i32, i32)>> {
    let names = ["ANTENNA1", "ANTENNA2"];
    let mut columns = match (reference, rows) {
        (Some(reference), rows) => {
            let all_rows;
            let rows = if let Some(rows) = rows {
                rows
            } else {
                all_rows = (0..main.row_count()).collect::<Vec<_>>();
                &all_rows
            };
            read_reference_scalars(reference, &names, &reference.parent_rows(rows)?)?
        }
        (None, Some(rows)) => main.required_scalar_columns_owned_for_rows(&names, rows)?,
        (None, None) => main.required_scalar_columns_owned(&names)?,
    };
    let (
        Some(RequiredScalarColumnValues::Int32(first)),
        Some(RequiredScalarColumnValues::Int32(second)),
    ) = (columns.remove(names[0]), columns.remove(names[1]))
    else {
        return Err(input("invalid antenna ID column type"));
    };
    Ok(first
        .into_iter()
        .zip(second)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

pub(crate) struct VisibilityRead<'a> {
    main: &'a Table,
    rows: &'a [usize],
    first_row: Option<usize>,
    plain: bool,
    members: Vec<MemberRows<'a>>,
    reference: Option<ReferenceRows<'a>>,
    scalar_columns: HashMap<String, RequiredScalarColumnValues>,
    baseline_axis: bool,
    iterator_read: bool,
    legacy: bool,
    channels: Option<VisibilityChannels>,
    polarization: Option<&'a VisibilityPolarization>,
    raw_flags: OnceCell<ArrayD<bool>>,
    selected_flags: OnceCell<ArrayD<bool>>,
    converted_channel_flags: OnceCell<ArrayD<bool>>,
    sigma_weights: OnceCell<ChannelWeights>,
    corrected_weights: OnceCell<ChannelWeights>,
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

pub(crate) const COMPLEX_PRODUCTS: [(&str, &str, bool); 6] = [
    ("", "DATA", false),
    ("corrected_", "CORRECTED_DATA", false),
    ("model_", "MODEL_DATA", false),
    ("ratio_", "CORRECTED_DATA", true),
    ("residual_", "CORRECTED_DATA", true),
    ("obs_residual_", "DATA", true),
];
const PRODUCT_NAMES: [&str; 5] = ["data", "amplitude", "phase", "real", "imaginary"];

impl<'a> VisibilityRead<'a> {
    pub(crate) fn new(
        main: &'a Table,
        members: &'a [Table],
        reference: Option<&'a PlainReferenceRead>,
        rows: &'a [usize],
        first_row: Option<usize>,
        names: &BTreeSet<String>,
        options: VisibilityReadOptions<'a>,
    ) -> MsResult<Self> {
        let baseline_axis = options.baseline_gap.is_some();
        let legacy = options.legacy_axes.is_some();
        let channels = options.channels;
        let hour_angle = baseline_axis && names.contains("axis_info") && names.contains("ha");
        let mut columns = SCALARS
            .into_iter()
            .filter_map(|(name, column)| {
                if name == "flag_row"
                    && !legacy
                    && channels.is_some_and(|channels| channels.width() > 1)
                    && !rows.is_empty()
                {
                    return None;
                }
                (names.contains(name)
                    || (matches!(name, "antenna1" | "antenna2")
                        && (baseline_axis || names.contains("ifr_number")))
                    || (baseline_axis && name == "time")
                    || (hour_angle && name == "field_id"))
                    .then_some(column)
            })
            .collect::<Vec<_>>();
        if (names.contains("axis_info")
            || options.polarization.is_some()
            || (legacy && baseline_axis))
            && !columns.contains(&"DATA_DESC_ID")
        {
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
        } else if let Some(reference) = &reference {
            read_reference_scalars(reference.read, &columns, &reference.rows)?
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
            first_row,
            plain,
            members,
            reference,
            scalar_columns,
            baseline_axis,
            iterator_read: options.iterator_read,
            legacy,
            channels,
            polarization: options.polarization,
            raw_flags: OnceCell::new(),
            selected_flags: OnceCell::new(),
            converted_channel_flags: OnceCell::new(),
            sigma_weights: OnceCell::new(),
            corrected_weights: OnceCell::new(),
        })
    }

    pub(crate) fn vector_cells(&self, name: &str) -> MsResult<ArrayValue> {
        let is_row_weight = matches!(name, "WEIGHT" | "SIGMA");
        let values = if let Some(channels) = self.channels
            && !self.legacy
            && channels.width() > 1
            && !self.rows.is_empty()
            && is_row_weight
        {
            let sigma = name == "SIGMA";
            let kind = if sigma {
                ChannelWeightKind::Observed
            } else {
                ChannelWeightKind::Corrected
            };
            ArrayValue::Float32(channels.statistic(
                self.raw_flags()?,
                self.channel_weights(kind)?,
                sigma,
            )?)
        } else {
            self.raw_vector_cells(name)?
        };
        if self.legacy && is_row_weight {
            if let Some(plan) = self.polarization.filter(|plan| plan.is_conversion()) {
                plan.legacy_weight(values, self.polarization_ids()?, name == "SIGMA")
            } else {
                Ok(values)
            }
        } else if is_row_weight {
            self.gather_correlations(values)
        } else {
            Ok(values)
        }
    }

    fn raw_vector_cells(&self, name: &str) -> MsResult<ArrayValue> {
        if let Some(reference) = &self.reference {
            return read_vector_cells(
                reference.read.parent(),
                reference.read.parent_column(name)?,
                &reference.rows,
                true,
            );
        }
        if self.members.is_empty() {
            return read_vector_cells(self.main, name, self.rows, self.plain);
        }
        concatenate_arrays(&self.members, |table, rows| {
            read_vector_cells(table, name, rows, true)
        })
    }

    fn matrix_cells(&self, name: &str, widen_complex: bool) -> MsResult<ArrayValue> {
        let Some(channels) = self.channels.filter(|_| !self.rows.is_empty()) else {
            return self.gather_correlations(self.raw_matrix_cells(name, widen_complex)?);
        };
        if name == "FLAG" {
            return Ok(ArrayValue::Bool(self.channel_flags(channels)?.clone()));
        }
        let values = self.raw_matrix_cells(name, widen_complex && channels.width() == 1)?;
        self.gather_correlations(self.select_channels(channels, name, &values)?)
    }

    fn polarization_ids(&self) -> MsResult<&[i32]> {
        match self.scalar_columns.get("DATA_DESC_ID") {
            Some(RequiredScalarColumnValues::Int32(ids)) => Ok(ids),
            _ => Err(input("DATA_DESC_ID must contain an integer")),
        }
    }

    fn gather_correlations(&self, values: ArrayValue) -> MsResult<ArrayValue> {
        if let Some(polarization) = self.polarization.filter(|_| !self.rows.is_empty()) {
            polarization.gather(values, self.polarization_ids()?)
        } else {
            Ok(values)
        }
    }

    pub(crate) fn raw_matrix_cells(&self, name: &str, widen_complex: bool) -> MsResult<ArrayValue> {
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

    pub(crate) fn range_matrix_cells(&self, name: &str, convert: bool) -> MsResult<ArrayValue> {
        if !self.legacy {
            return self.raw_matrix_cells(name, false);
        }
        let values = self.matrix_cells(name, false)?;
        let Some(plan) = self
            .polarization
            .filter(|plan| convert && plan.is_conversion())
        else {
            return Ok(values);
        };
        match values {
            ArrayValue::Bool(flags) => plan
                .converted_flags(&flags, self.polarization_ids()?)
                .map(ArrayValue::Bool),
            ArrayValue::Complex32(_) => plan.apply_conversion(values, self.polarization_ids()?),
            _ => Ok(values),
        }
    }

    fn raw_flags(&self) -> MsResult<&ArrayD<bool>> {
        if let Some(flags) = self.raw_flags.get() {
            return Ok(flags);
        }
        let ArrayValue::Bool(flags) = self.raw_matrix_cells("FLAG", false)? else {
            return Err(input("FLAG cells must be Boolean"));
        };
        Ok(self.raw_flags.get_or_init(|| flags))
    }

    fn channel_flags(&self, channels: VisibilityChannels) -> MsResult<&ArrayD<bool>> {
        if let Some(flags) = self.selected_flags.get() {
            return Ok(flags);
        }
        let flags = ArrayValue::Bool(channels.flags(self.raw_flags()?)?);
        let ArrayValue::Bool(flags) = self.gather_correlations(flags)? else {
            return Err(input("FLAG cells must be Boolean"));
        };
        Ok(self.selected_flags.get_or_init(|| flags))
    }

    fn channel_weights(&self, kind: ChannelWeightKind) -> MsResult<&ChannelWeights> {
        let cache = match kind {
            ChannelWeightKind::Observed => &self.sigma_weights,
            ChannelWeightKind::Corrected => &self.corrected_weights,
        };
        if let Some(weights) = cache.get() {
            return Ok(weights);
        }
        let column = kind.spectrum();
        let spectral = if let Some(row) = self.first_row
            && self
                .main
                .schema()
                .is_some_and(|schema| schema.column(column).is_some())
        {
            // CASA tests the first cell of the selected table for content,
            // including variable-shape columns with an empty first cell.
            self.main
                .array_shape_uncached(row, column)?
                .is_some_and(|shape| !shape.is_empty() && !shape.contains(&0))
        } else {
            false
        };
        let values = if spectral {
            self.raw_matrix_cells(column, false)?
        } else {
            self.raw_vector_cells(kind.fallback())?
        };
        let ArrayValue::Float32(values) = values else {
            return Err(input("channel weights must contain Float values"));
        };
        Ok(cache.get_or_init(|| ChannelWeights::new(values, spectral, kind)))
    }

    fn select_channels(
        &self,
        channels: VisibilityChannels,
        column: &str,
        values: &ArrayValue,
    ) -> MsResult<ArrayValue> {
        if channels.width() == 1 {
            return match values {
                ArrayValue::Float32(values) => Ok(ArrayValue::Float32(channels.selects(values)?)),
                ArrayValue::Complex32(values) => {
                    Ok(ArrayValue::Complex32(channels.selects(values)?))
                }
                ArrayValue::Complex64(values) => {
                    Ok(ArrayValue::Complex64(channels.selects(values)?))
                }
                _ => Err(input("unexpected channel-selected visibility type")),
            };
        }
        let flags = self.raw_flags()?;
        let weights = match (self.legacy, column) {
            (true, _) | (_, "MODEL_DATA") => None,
            (_, "DATA") => Some(self.channel_weights(ChannelWeightKind::Observed)?),
            _ => Some(self.channel_weights(ChannelWeightKind::Corrected)?),
        };
        match values {
            ArrayValue::Float32(values) => {
                Ok(ArrayValue::Float32(channels.means(values, flags, weights)?))
            }
            ArrayValue::Complex32(values) => Ok(ArrayValue::Complex32(
                channels.means(values, flags, weights)?,
            )),
            _ => Err(input("unexpected channel-averaged visibility type")),
        }
    }

    fn observed_float(&self, raw: &ArrayValue) -> MsResult<ArrayValue> {
        let selected = self
            .channels
            .filter(|_| !self.rows.is_empty())
            .map(|channels| self.select_channels(channels, "DATA", raw))
            .transpose()?;
        let ArrayValue::Float32(samples) = selected.as_ref().unwrap_or(raw) else {
            return Err(input("FLOAT_DATA cells must contain Float values"));
        };
        self.gather_correlations(ArrayValue::Complex32(
            samples.mapv(|sample| Complex32::new(sample, 0.0)),
        ))
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
                .collect::<Vec<_>>();
            if !self.legacy
                && !self.iterator_read
                && self.channels.is_none()
                && self.polarization.is_none()
            {
                ifrs.sort_unstable();
                ifrs.dedup();
            }
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
            if name == "data_desc_id" && self.polarization.is_some() {
                let mut ids = self.polarization_ids()?.to_vec();
                if average {
                    averaging::common_id(&mut ids);
                }
                fields.insert(name.into(), Value::Array(ArrayValue::Int32(vector(ids))));
                continue;
            }
            if name == "flag_row"
                && !self.legacy
                && let Some(channels) = self.channels.filter(|channels| channels.width() > 1)
                && !self.rows.is_empty()
            {
                let flags = self.channel_flags(channels)?;
                let cell_width = flags.shape()[0] * flags.shape()[1];
                let memory = flags
                    .as_slice_memory_order()
                    .ok_or_else(|| input("FLAG block is not contiguous"))?;
                let row_flags = memory
                    .chunks_exact(cell_width)
                    .map(|row| row.iter().all(|&flagged| flagged))
                    .collect();
                fields.insert(
                    name.into(),
                    Value::Array(ArrayValue::Bool(vector(row_flags))),
                );
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
        reduced: Option<RowAverage<'_>>,
        grid: Option<&BaselineGrid>,
    ) -> MsResult<()> {
        let (raw_flags, reduced_flags) = if let Some(reduced) = reduced {
            let (raw, flags, weights) = reduced.into_fields();
            let converted = self
                .polarization
                .is_some_and(VisibilityPolarization::is_conversion);
            let raw = if self.legacy {
                if names.contains("weight") {
                    let weights = averaging::sum_weights(self.vector_cells("WEIGHT")?, grid)?;
                    fields.insert("weight".into(), Value::Array(weights));
                }
                if converted && names.contains("flag_sum") {
                    ArrayValue::Bool(self.legacy_flags()?)
                } else {
                    ArrayValue::Bool(raw)
                }
            } else {
                let (raw, weights) = if converted {
                    (
                        self.matrix_cells("FLAG", false)?,
                        self.vector_cells("WEIGHT")?,
                    )
                } else {
                    (ArrayValue::Bool(raw), weights)
                };
                let name = if names.contains("weight") {
                    "weight"
                } else {
                    "avgweight"
                };
                fields.insert(name.into(), Value::Array(weights));
                raw
            };
            (Some(raw), Some(flags))
        } else {
            if names.contains("weight") {
                let weights = self.vector_cells("WEIGHT")?;
                let weights = if self.legacy && average {
                    averaging::sum_weights(weights, grid)?
                } else {
                    weights
                };
                fields.insert("weight".into(), Value::Array(weights));
            }
            (None, None)
        };
        if names.contains("sigma") {
            let values = self.vector_cells("SIGMA")?;
            let values = if average {
                averaging::sigma(values, grid)?
            } else {
                values
            };
            fields.insert("sigma".into(), Value::Array(values));
        }
        if names.contains("flag") || names.contains("flag_sum") {
            let flags = match raw_flags {
                Some(flags) => flags,
                None if self.legacy => ArrayValue::Bool(self.legacy_flags()?),
                None => self.matrix_cells("FLAG", false)?,
            };
            if names.contains("flag_sum") {
                let ArrayValue::Bool(flag) = &flags else {
                    return Err(input("FLAG cells must be Boolean"));
                };
                let sums = if let Some(grid) = grid {
                    grid.flag_sums(flag)?
                } else {
                    averaging::row_flag_sums(flag, self.rows.is_empty())?
                };
                fields.insert("flag_sum".into(), Value::Array(ArrayValue::Int64(sums)));
            }
            if names.contains("flag") {
                let flags = if self.legacy && average && reduced_flags.is_none() {
                    ArrayValue::Bool(vector(Vec::new()))
                } else {
                    reduced_flags.map_or(flags, ArrayValue::Bool)
                };
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

    fn products<'grid>(
        &self,
        names: &BTreeSet<String>,
        fields: &mut BTreeMap<String, Value>,
        average: bool,
        items: &[String],
        grid: Option<&'grid BaselineGrid>,
    ) -> MsResult<Option<RowAverage<'grid>>> {
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
            if self.legacy && (missing_float || missing_complex) {
                continue;
            } else if missing_float || missing_complex {
                // CASA checks optional-column substrings before recognizing item names.
                let empty = if !missing_float && name.contains("data") {
                    ArrayValue::Complex32(vector(Vec::new()))
                } else {
                    ArrayValue::Float32(vector(Vec::new()))
                };
                fields.insert(name.clone(), Value::Array(empty));
            } else if !self.legacy
                && let Some((base, alias)) =
                    raw_visibility_alias(name, names, average, self.baseline_axis)?
            {
                aliases.entry(base).or_default().push(alias);
            }
        }
        let mut reduction = None;
        let mut float_cells = None;
        for (prefix, source, uses_model) in COMPLEX_PRODUCTS {
            let requested = PRODUCT_NAMES.map(|name| {
                let key = format!("{prefix}{name}");
                (names.contains(&key) && !fields.contains_key(&key)).then_some(key)
            });
            if self.legacy && (!has_column(source) || (uses_model && !has_model)) {
                for (item, key) in PRODUCT_NAMES.into_iter().zip(requested) {
                    if let Some(key) = key {
                        let empty = if item == "data" {
                            ArrayValue::Complex32(vector(Vec::new()))
                        } else {
                            ArrayValue::Float32(vector(Vec::new()))
                        };
                        fields.insert(key, Value::Array(empty));
                    }
                }
                continue;
            }
            let mut raw_aliases = aliases.remove(&format!("{prefix}data"));
            let explicit = requested.iter().any(Option::is_some);
            if !explicit && raw_aliases.is_none() {
                continue;
            }
            let reduce = average && explicit;
            if reduce && !self.legacy {
                for key in requested.iter().flatten() {
                    require_single_average(items, key)?;
                }
            }
            let [raw, amplitude, phase, real, imaginary] = requested;
            let only_raw = !uses_model
                && [&amplitude, &phase, &real, &imaginary]
                    .into_iter()
                    .all(Option::is_none);
            let convert = self.polarization.filter(|plan| plan.is_conversion());
            let data = if !self.legacy
                && (self.iterator_read
                    || self.baseline_axis
                    || self.channels.is_some()
                    || self.polarization.is_some())
                && has_float
                && source == "DATA"
            {
                // The CASA visibility iterator uses FLOAT_DATA for observed
                // products whenever that column exists.
                if float_cells.is_none() {
                    float_cells = Some(self.raw_matrix_cells("FLOAT_DATA", false)?);
                }
                self.observed_float(float_cells.as_ref().expect("FLOAT_DATA was read"))?
            } else {
                self.matrix_cells(source, only_raw && !reduce && convert.is_none())?
            };
            let data = if uses_model {
                self.model_product(data, prefix, average, convert)?
            } else {
                data
            };
            let data = if let Some(polarization) = convert.filter(|_| !self.legacy || !uses_model) {
                self.convert_visibility_product(data, polarization, !average && !self.legacy)?
            } else {
                data
            };
            let data = if reduce {
                let means = self.average_product(&mut reduction, &data, grid, convert)?;
                if let Some(aliases) = raw_aliases.take() {
                    insert_array_aliases(fields, aliases.into_iter(), data);
                }
                means
            } else {
                data
            };
            if !only_raw {
                insert_visibility_components(fields, &data, [amplitude, phase, real, imaginary])?;
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
            let values = match (float_cells, self.channels.filter(|_| !self.rows.is_empty())) {
                (Some(values), Some(channels)) => self.gather_correlations(
                    self.select_channels(channels, "FLOAT_DATA", &values)?,
                )?,
                (Some(values), None) => self.gather_correlations(values)?,
                (None, _) => self.matrix_cells("FLOAT_DATA", false)?,
            };
            let values = if average && raw_float.is_some() {
                if !self.legacy {
                    require_single_average(items, "float_data")?;
                }
                let mut float_average = None;
                let input = if self
                    .polarization
                    .is_some_and(VisibilityPolarization::is_conversion)
                    && reduction.is_some()
                {
                    &mut float_average
                } else {
                    &mut reduction
                };
                let means = self.average_product(input, &values, grid, None)?;
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

    fn convert_visibility_product(
        &self,
        data: ArrayValue,
        polarization: &VisibilityPolarization,
        mask_binned: bool,
    ) -> MsResult<ArrayValue> {
        let ids = self.polarization_ids()?;
        let mut data = polarization.apply_conversion(data, ids)?;
        if let Some(channels) = self
            .channels
            .filter(|channels| mask_binned && channels.width() > 1)
        {
            let flags = match self.converted_channel_flags.get() {
                Some(flags) => flags,
                None => {
                    let flags = polarization.converted_flags(self.channel_flags(channels)?, ids)?;
                    self.converted_channel_flags.get_or_init(|| flags)
                }
            };
            let ArrayValue::Complex32(samples) = &mut data else {
                return Err(input("Stokes projection requires Complex visibility data"));
            };
            if samples.shape() != flags.shape() {
                return Err(input("converted FLAG and visibility dimensions differ"));
            }
            samples.zip_mut_with(flags, |sample, &flagged| {
                if flagged {
                    *sample = Complex32::ZERO;
                }
            });
        }
        Ok(data)
    }

    fn average_product<'grid>(
        &self,
        reduction: &mut Option<RowAverage<'grid>>,
        values: &ArrayValue,
        grid: Option<&'grid BaselineGrid>,
        convert: Option<&VisibilityPolarization>,
    ) -> MsResult<ArrayValue> {
        if reduction.is_none() {
            let flags = self.matrix_cells("FLAG", false)?;
            let weights = if self.legacy {
                self.gather_correlations(self.raw_vector_cells("WEIGHT")?)?
            } else {
                self.vector_cells("WEIGHT")?
            };
            let (flags, weights) = if let Some(polarization) = convert {
                polarization.averaging_inputs(flags, weights, self.polarization_ids()?)?
            } else {
                (flags, weights)
            };
            *reduction = Some(RowAverage::new(flags, weights, grid)?);
        }
        reduction
            .as_mut()
            .expect("averaging input exists")
            .reduce(values)
    }

    fn legacy_flags(&self) -> MsResult<ArrayD<bool>> {
        let ArrayValue::Bool(flags) = self.range_matrix_cells("FLAG", true)? else {
            return Err(input("FLAG cells must be Boolean"));
        };
        Ok(flags)
    }
}

fn insert_visibility_components(
    fields: &mut BTreeMap<String, Value>,
    data: &ArrayValue,
    names: [Option<String>; 4],
) -> MsResult<()> {
    let ArrayValue::Complex32(complex) = data else {
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
    for (name, project) in names.into_iter().zip(projections) {
        if let Some(name) = name {
            let values = shaped(
                complex.shape(),
                memory.iter().copied().map(project).collect(),
            )?;
            fields.insert(name, Value::Array(ArrayValue::Float32(values)));
        }
    }
    Ok(())
}

fn visibility_product(name: &str) -> bool {
    name == "float_data"
        || COMPLEX_PRODUCTS.iter().any(|&(prefix, _, _)| {
            name.strip_prefix(prefix)
                .is_some_and(|part| PRODUCT_NAMES.contains(&part))
        })
}

fn raw_visibility_alias(
    name: &str,
    names: &BTreeSet<String>,
    average: bool,
    baseline_axis: bool,
) -> MsResult<Option<(String, String)>> {
    let (alias, derived) = if average && PRODUCT_NAMES.iter().any(|&part| name.contains(part)) {
        if visibility_product(name) {
            return Ok(None);
        }
        // CASA adds the prefix before recognizing the item. An unrecognized
        // item can still resolve to a raw visibility column under that name.
        (format!("avg_{name}"), name)
    } else if let Some(derived) = name.strip_prefix("avg_") {
        let implicit_time = derived == "time" && baseline_axis && names.contains("axis_info");
        if average
            && matches!(
                derived,
                "antenna1" | "antenna2" | "feed1" | "feed2" | "sigma" | "time"
            )
            && !names.contains(derived)
            && !implicit_time
        {
            return Err(input(format!(
                "RecordInterface: field {derived} is unknown"
            )));
        }
        (name.to_owned(), derived)
    } else {
        return Ok(None);
    };
    let base = derived
        .rsplit_once('_')
        .map_or_else(|| "data".to_owned(), |(prefix, _)| format!("{prefix}_data"));
    Ok(Some((base, alias)))
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

pub(crate) fn divide_visibility(sample: Complex32, model: Complex32) -> Complex32 {
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

fn vector<T>(values: Vec<T>) -> ArrayD<T> {
    ndarray::Array1::from_vec(values).into_dyn()
}

fn shaped<T>(shape: &[usize], values: Vec<T>) -> MsResult<ArrayD<T>> {
    ArrayD::from_shape_vec(IxDyn(shape).f(), values).map_err(|error| input(error.to_string()))
}

fn input(message: impl Into<String>) -> MsError {
    MsError::InvalidInput(message.into())
}
