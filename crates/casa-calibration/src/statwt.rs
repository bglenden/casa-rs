// SPDX-License-Identifier: LGPL-3.0-or-later
//! Estimate visibility weights from the scatter within baseline and data bins.

mod classic;

use std::collections::BTreeMap;

use casa_ms::{MeasurementSet, MsError, MsResult, ResolvedMsSelection, ResolvedMsSelectionRow};
use casa_tables::{RequiredScalarColumnValues, Table, TableWriteLock};
use casa_types::{ArrayValue, Complex32, ScalarValue, Value};
use ndarray::{Array1, ArrayD, Axis};

use classic::{Moments, VisibilityMoments};

/// Main-table data column contributing samples to a classical weight estimate.
#[derive(Clone, Copy)]
pub enum StatWtDataColumn {
    /// Read the input visibilities from DATA.
    Data,
    /// Read the calibrated visibilities from CORRECTED_DATA.
    Corrected,
    /// Subtract MODEL_DATA from DATA.
    ResidualData,
    /// Subtract MODEL_DATA from CORRECTED_DATA.
    ResidualCorrected,
}

impl StatWtDataColumn {
    fn visibility_column(self) -> &'static str {
        match self {
            Self::Data | Self::ResidualData => "DATA",
            Self::Corrected | Self::ResidualCorrected => "CORRECTED_DATA",
        }
    }

    fn residual(self) -> bool {
        matches!(self, Self::ResidualData | Self::ResidualCorrected)
    }

    fn output_column(self) -> &'static str {
        match self {
            Self::Data | Self::ResidualData => "SIGMA",
            Self::Corrected | Self::ResidualCorrected => "WEIGHT",
        }
    }
}

/// Options for the classical scalar-weight workflow in one-integration bins.
#[derive(Clone, Copy)]
pub struct ClassicScalarStatWt {
    /// The main-table data to estimate weights from.
    pub data_column: StatWtDataColumn,
    /// The minimum number of unflagged samples in a bin.
    pub minimum_samples: usize,
    /// Aggregate all correlations in each baseline bin.
    pub combine_correlations: bool,
    /// Optional inclusive bounds on acceptable weights.
    pub weight_range: Option<[f32; 2]>,
}

/// Statistical summary of the estimated weights over previously unflagged data.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize)]
pub struct StatWtSummary {
    /// Mean of the estimated weights, including zeros for insufficient samples.
    pub mean: f64,
    /// Sample variance of the estimated weights.
    pub variance: f64,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct BaselineGroup {
    array: i32,
    scan: i32,
    state: i32,
    field: i32,
    data_description: i32,
    antenna1: i32,
    antenna2: i32,
}

/// Estimate classical scalar weights in one-integration time bins.
///
/// The caller supplies the MeasurementSet selection held by either the task
/// or the existing MS tool. Pass a MAIN write lock to persist weights and
/// flags. Without a write lock this reports the same statistics in preview.
pub fn classical_scalar_statwt(
    ms: &mut MeasurementSet,
    selection: &ResolvedMsSelection,
    options: ClassicScalarStatWt,
    mut lock: Option<&mut TableWriteLock>,
) -> MsResult<StatWtSummary> {
    let table = ms.main_table_mut();
    let rows = selection.row_indices().collect::<Vec<_>>();
    let groups = baseline_groups(table, selection, &rows)?;
    let mut summary = Moments::default();
    let mut pending = Vec::new();
    let mut pending_bytes = 0;
    let column = options.data_column.output_column();
    for group in groups.values() {
        for integration in
            group.chunk_by(|left, right| left.time_mjd_seconds == right.time_mjd_seconds)
        {
            let (samples, channel_indices) =
                read_integration(table, selection, options.data_column, integration)?;
            let weights = estimate_scalar_weights(
                &samples,
                &channel_indices,
                options.minimum_samples,
                options.combine_correlations,
            );
            for (row, sample) in integration.iter().zip(samples) {
                for (correlation, &weight) in weights.iter().enumerate() {
                    if sample.has_unflagged(correlation, &channel_indices) {
                        summary.add(f64::from(weight));
                    }
                }
                if let Some(lock) = &mut lock {
                    pending_bytes += sample.flags.len() + weights.len() * size_of::<f32>();
                    pending.push(RowUpdate::new(
                        row.row_index,
                        sample,
                        &weights,
                        &channel_indices,
                        column == "SIGMA",
                        options.weight_range,
                    ));
                    if pending_bytes >= casa_ms::write_session::TARGET_BATCH_BYTES
                        || pending.len() == 65_536
                    {
                        persist_updates(table, column, &mut pending, lock)?;
                        pending_bytes = 0;
                    }
                }
            }
        }
    }
    if let Some(lock) = lock {
        persist_updates(table, column, &mut pending, lock)?;
    }
    Ok(StatWtSummary {
        mean: summary.mean(),
        variance: summary.sample_variance(),
    })
}

struct RowUpdate {
    row: usize,
    values: ArrayValue,
    flags: ArrayValue,
    row_flagged: bool,
}

impl RowUpdate {
    fn new(
        row: usize,
        mut sample: Samples,
        weights: &[f32],
        channels: &[usize],
        sigma: bool,
        range: Option<[f32; 2]>,
    ) -> Self {
        for (correlation, &weight) in weights.iter().enumerate() {
            if weight == 0.0 || range.is_some_and(|[low, high]| weight < low || weight > high) {
                for &channel in channels {
                    sample.flags[[correlation, channel]] = true;
                }
            }
        }
        let values = weights
            .iter()
            .map(|&weight| {
                if !sigma {
                    weight
                } else if weight > 0.0 {
                    weight.sqrt().recip()
                } else {
                    -1.0
                }
            })
            .collect();
        Self {
            row,
            values: ArrayValue::Float32(Array1::from_vec(values).into_dyn()),
            row_flagged: sample.flags.iter().all(|&flag| flag),
            flags: ArrayValue::Bool(sample.flags),
        }
    }
}

fn persist_updates(
    table: &mut Table,
    column: &str,
    pending: &mut Vec<RowUpdate>,
    lock: &mut TableWriteLock,
) -> MsResult<()> {
    if pending.is_empty() {
        return Ok(());
    }
    pending.sort_unstable_by_key(|update| update.row);
    let columns = [column, "FLAG", "FLAG_ROW"];
    let rows: Vec<_> = pending.iter().map(|update| update.row).collect();
    let mut writer = table.row_accessor_mut().prepare(&columns)?;
    for update in pending.drain(..) {
        writer.seek(update.row)?;
        writer.set_value_at(0, Value::Array(update.values))?;
        writer.set_value_at(1, Value::Array(update.flags))?;
        writer.set_value_at(2, Value::Scalar(ScalarValue::Bool(update.row_flagged)))?;
    }
    writer.flush()?;
    drop(writer);
    lock.record_write();
    table.prepare_write().save_selected_rows(&columns, &rows)?;
    table.discard_persisted_cell_updates(&columns, &rows);
    Ok(())
}

fn baseline_groups<'a>(
    table: &Table,
    selection: &'a ResolvedMsSelection,
    rows: &[usize],
) -> MsResult<BTreeMap<BaselineGroup, Vec<&'a ResolvedMsSelectionRow>>> {
    let mut metadata = table
        .required_scalar_columns_owned_for_rows(&["ARRAY_ID", "SCAN_NUMBER", "STATE_ID"], rows)?;
    let mut take_column = |name: &str| match metadata.remove(name) {
        Some(RequiredScalarColumnValues::Int32(values)) => Ok(values),
        _ => Err(MsError::InvalidInput(format!("{name} must be int32"))),
    };
    let arrays = take_column("ARRAY_ID")?;
    let scans = take_column("SCAN_NUMBER")?;
    let states = take_column("STATE_ID")?;
    let mut groups = BTreeMap::<_, Vec<_>>::new();
    for (index, row) in selection.selected_rows.iter().enumerate() {
        let key = BaselineGroup {
            array: arrays[index],
            scan: scans[index],
            state: states[index],
            field: row.field_id,
            data_description: row.data_desc_id,
            antenna1: row.antenna1_id.min(row.antenna2_id),
            antenna2: row.antenna1_id.max(row.antenna2_id),
        };
        groups.entry(key).or_default().push(row);
    }
    for group in groups.values_mut() {
        group.sort_unstable_by(|left, right| {
            left.time_mjd_seconds.total_cmp(&right.time_mjd_seconds)
        });
    }
    Ok(groups)
}

struct Samples {
    values: ArrayD<Complex32>,
    flags: ArrayD<bool>,
}

impl Samples {
    fn has_unflagged(&self, correlation: usize, channels: &[usize]) -> bool {
        channels
            .iter()
            .any(|&channel| !self.flags[[correlation, channel]])
    }
}

fn read_integration(
    table: &Table,
    selection: &ResolvedMsSelection,
    column: StatWtDataColumn,
    integration: &[&ResolvedMsSelectionRow],
) -> MsResult<(Vec<Samples>, Vec<usize>)> {
    let indices = integration
        .iter()
        .map(|row| row.row_index)
        .collect::<Vec<_>>();
    let read = |name| {
        table
            .column_accessor(name)?
            .array_cells_owned_uncached(&indices)
    };
    let data = read(column.visibility_column())?;
    let flags = read("FLAG")?;
    let models = column.residual().then(|| read("MODEL_DATA")).transpose()?;
    let mut samples = Vec::with_capacity(integration.len());
    for (index, (values, flags)) in data.into_iter().zip(flags).enumerate() {
        let row = integration[index];
        let (Some(ArrayValue::Complex32(mut values)), Some(ArrayValue::Bool(flags))) =
            (values, flags)
        else {
            return Err(MsError::InvalidInput(format!(
                "visibility or flag cell is undefined at row {}",
                row.row_index
            )));
        };
        if values.ndim() != 2 || values.shape() != flags.shape() {
            return Err(MsError::InvalidInput(format!(
                "visibility and flag shapes disagree at row {}",
                row.row_index
            )));
        }
        if let Some(models) = &models {
            let Some(ArrayValue::Complex32(model)) = &models[index] else {
                return Err(MsError::InvalidInput(format!(
                    "MODEL_DATA is undefined at row {}",
                    row.row_index
                )));
            };
            if model.shape() != values.shape() {
                return Err(MsError::InvalidInput(format!(
                    "MODEL_DATA has a different shape at row {}",
                    row.row_index
                )));
            }
            values -= model;
        }
        samples.push(Samples { values, flags });
    }
    let channels = samples[0].values.len_of(Axis(1));
    if samples
        .iter()
        .any(|row| row.values.shape() != samples[0].values.shape())
    {
        return Err(MsError::InvalidInput(
            "rows in one integration have incompatible visibility shapes".into(),
        ));
    }
    let selected_channels = selection
        .channel_selections
        .get(&integration[0].spectral_window_id)
        .map_or_else(
            || Ok((0..channels).collect()),
            |selection| selection.indices(channels),
        )?;
    Ok((samples, selected_channels))
}

fn estimate_scalar_weights(
    samples: &[Samples],
    channels: &[usize],
    minimum_samples: usize,
    combine_correlations: bool,
) -> Vec<f32> {
    let correlations = samples[0].values.len_of(Axis(0));
    let mut moments = vec![
        VisibilityMoments::default();
        if combine_correlations {
            1
        } else {
            correlations
        }
    ];
    for row in samples {
        for correlation in 0..correlations {
            let bin = if combine_correlations { 0 } else { correlation };
            for &channel in channels {
                if !row.flags[[correlation, channel]] {
                    moments[bin].add(row.values[[correlation, channel]]);
                }
            }
        }
    }
    (0..correlations)
        .map(|correlation| {
            let bin = if combine_correlations { 0 } else { correlation };
            moments[bin].weight(minimum_samples)
        })
        .collect()
}
