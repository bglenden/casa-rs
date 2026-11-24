// SPDX-License-Identifier: LGPL-3.0-or-later
//! Visibility statistics for the public MeasurementSet tool.

use std::collections::BTreeMap;

use casa_types::{RecordField, RecordValue, Value};

use crate::selection::{ResolvedMsSelection, resolve_casa_selection_with_system_budget};
use crate::{MeasurementSet, MsError, MsResult, MsSelection};

mod cube;
mod record;
mod reduction;
mod rows;
mod scalar;
mod spec;
mod time_average;

use cube::{CubeReduction, SampleAxes};
use record::{legacy_record, modern_record};
use reduction::Moments;
use rows::{Row, read_rows};
use spec::{Axis, Column, Weighting};
use time_average::TimeAverage;

/// Selection, projection and reporting options for a public MS tool reduction.
pub struct ToolStatisticsOptions {
    /// CASA column name or alias.
    pub column: String,
    /// Projection for complex values: real, imaginary, amplitude or phase.
    pub complex_value: String,
    /// Omit flagged values according to the column's mask.
    pub use_flags: bool,
    /// Use per-visibility weights when the selected column supports them.
    pub use_weights: bool,
    /// CASA selection expressions applied to the current logical attachment.
    pub selection: MsSelection,
    /// Comma-separated axes for the returned statistics records.
    pub reporting_axes: String,
    /// Average visibility vectors in time before projecting to real samples.
    pub time_average: bool,
    /// CASA duration quantity for the averaging bins.
    pub time_bin: String,
    /// Whether the bins are allowed to span scan or state boundaries.
    pub time_span: String,
    /// Maximum separation in metres between baselines in the same bin.
    pub max_uvw_distance: f64,
    /// Compute median, median absolute deviation and the requested quartiles.
    pub quantiles: bool,
    /// Use the deprecated statisticsold projection, masking and record format.
    pub legacy: bool,
}

/// Compute statistics for the logical rows and spectral axes seen by the MS tool.
///
/// # Errors
/// Returns an error for invalid selection, shapes or physical column metadata.
pub fn tool_statistics(
    ms: &MeasurementSet,
    options: &ToolStatisticsOptions,
) -> MsResult<RecordValue> {
    let selection = if options.selection.has_text_selection() {
        Some(resolve_casa_selection_with_system_budget(
            ms,
            &options.selection,
        )?)
    } else {
        None
    };
    let (label, column) = Column::parse(&options.column, &options.complex_value, options.legacy)?;
    let Some(column) = column else {
        return Ok(RecordValue::default());
    };
    let time_average = options
        .time_average
        .then(|| TimeAverage::new(ms, options, column))
        .transpose()?;
    let selected = selection
        .as_ref()
        .map(|selection| selection.row_indices().collect::<Vec<_>>());
    let mut rows = read_rows(ms.main_table(), selected.as_deref())?;
    if rows.is_empty() {
        return Err(MsError::InvalidInput(if options.legacy {
            "No data could be found!".into()
        } else {
            "MSIter::construct - Input MeasurementSet.has zero selected rows".into()
        }));
    }
    let mut reporting = Axis::parse_axes(&options.reporting_axes);
    if time_average.is_some() {
        TimeAverage::reporting_axes(&mut reporting, &options.time_span);
    }
    sort_rows(&mut rows, &sort_order(&reporting));
    let mut reducer = Reduction {
        ms,
        selection: selection.as_ref(),
        grid: BTreeMap::new(),
        cube: CubeReduction {
            use_flags: options.use_flags,
            weighting: column.weighting(options.use_weights),
            legacy: options.legacy,
        },
        column,
        time_average,
    };
    if options.legacy {
        return reducer.legacy(rows, &label);
    }

    let mut record = RecordValue::default();
    let mut remaining = rows.as_mut_slice();
    while !remaining.is_empty() {
        let count = match &reducer.time_average {
            Some(average) => average.report_count(remaining, &reporting),
            None => remaining
                .iter()
                .take_while(|row| same_group(&remaining[0], row, &reporting))
                .count(),
        };
        let (group, rest) = remaining.split_at_mut(count);
        let key = reporting
            .iter()
            .map(|axis| group[0].group_item(*axis))
            .collect::<Vec<_>>()
            .join(",");
        if options.time_average {
            sort_rows(group, &CHUNK_AXES);
        }
        let mut moments = Moments::new(options.quantiles);
        reducer.reduce(group, &mut moments)?;
        let stats = modern_record(
            moments,
            options.use_flags && !options.time_average,
            reducer.cube.weighting != Weighting::Unweighted && !options.time_average,
            options.quantiles,
            column.is_boolean(),
        );
        record.upsert(key, Value::Record(stats));
        remaining = rest;
    }
    record
        .fields_mut()
        .sort_unstable_by(|a, b| a.name.cmp(&b.name));
    Ok(record)
}

const CHUNK_AXES: [Axis; 4] = [
    Axis::Array,
    Axis::Field,
    Axis::DataDescription,
    Axis::Integration,
];

fn sort_order(reporting: &[Axis]) -> Vec<Axis> {
    let mut axes = reporting.to_vec();
    axes.extend(
        CHUNK_AXES
            .iter()
            .copied()
            .filter(|axis| !reporting.contains(axis)),
    );
    axes
}

fn sort_rows(rows: &mut [Row], axes: &[Axis]) {
    rows.sort_by(|left, right| {
        axes.iter()
            .map(|axis| left.compare(right, *axis))
            .find(|order| order.is_ne())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
}

fn same_group(left: &Row, right: &Row, axes: &[Axis]) -> bool {
    axes.iter().all(|axis| left.compare(right, *axis).is_eq())
}

struct Reduction<'a> {
    ms: &'a MeasurementSet,
    selection: Option<&'a ResolvedMsSelection>,
    grid: BTreeMap<i32, SampleAxes>,
    cube: CubeReduction,
    column: Column,
    time_average: Option<TimeAverage>,
}

impl Reduction<'_> {
    fn reduce(&mut self, rows: &[Row], moments: &mut Moments) -> MsResult<()> {
        let axes = if self.time_average.is_some() {
            &CHUNK_AXES[..CHUNK_AXES.len() - 1]
        } else {
            &CHUNK_AXES[..]
        };
        for (dataset, chunk) in rows
            .chunk_by(|left, right| same_group(left, right, axes))
            .enumerate()
        {
            match self.column {
                Column::Complex(_, _) | Column::FloatCube(_) | Column::Flag => {
                    let id = chunk[0].data_description;
                    let axes = match self.grid.entry(id) {
                        std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(SampleAxes::for_description(self.ms, self.selection, id)?)
                        }
                    };
                    let table = self.ms.main_table();
                    if let Some(average) = self.time_average.as_ref() {
                        average.reduce(table, chunk, axes, self.column, dataset, moments)?;
                    } else {
                        self.cube
                            .reduce(table, chunk, axes, self.column, dataset, moments)?;
                    }
                }
                column => scalar::reduce(
                    self.ms.main_table(),
                    chunk,
                    column,
                    self.cube.use_flags,
                    self.cube.legacy,
                    dataset,
                    moments,
                )?,
            }
        }
        Ok(())
    }

    fn legacy(mut self, rows: Vec<Row>, label: &str) -> MsResult<RecordValue> {
        let mut result = RecordValue::default();
        if let Column::RowVector(column) = self.column {
            let moments = scalar::reduce_vector(self.ms, self.selection, &rows, column)?;
            for (index, moments) in moments.into_iter().enumerate() {
                result.push(RecordField::new(
                    format!("{label}_{index}"),
                    Value::Record(legacy_record(moments)?),
                ));
            }
        } else {
            let mut moments = Moments::new(true);
            self.reduce(&rows, &mut moments)?;
            result.push(RecordField::new(
                label,
                Value::Record(legacy_record(moments)?),
            ));
        }
        Ok(result)
    }
}
