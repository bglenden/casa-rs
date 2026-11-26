// SPDX-License-Identifier: LGPL-3.0-or-later
//! The fixed-integration reweighter used by the legacy MS tool.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use casa_ms::schema::main_table::initial_model_visibility;
use casa_ms::selection::MsSelectionIndices;
use casa_ms::{MeasurementSet, MsError, MsResult, MsSelection, ResolvedMsSelectionRow};
use casa_tables::TableWriteLock;
use casa_types::{ArrayValue, Complex32};
use ndarray::{ArrayD, Axis, IxDyn};

use super::StatWtMs;
use crate::solve::grouping::{VisibilityModels, correlation_types_for_ddid};

/// Arguments to the original MS reweighter, with CASA's selector syntax.
pub struct LegacyStatWtOptions {
    /// Use the squared samples about zero instead of the estimated mean.
    pub rms: bool,
    /// Smallest number of unflagged channels used for an estimate.
    pub minimum_samples: u32,
    /// Visibility column selector, such as `data` or `corrected_data`.
    pub data_column: String,
    /// The channels and windows used to estimate visibility scatter.
    pub fit_spw: String,
    /// Clauses that move selected axes after integration time in the sort.
    pub combine: String,
    /// Rows and output spectral windows on which to persist estimates.
    pub selection: MsSelection,
}

/// Replace WEIGHT and SIGMA on the selected rows. The legacy iterator
/// forms one group from consecutive chunks at the same integration time,
/// and fits independent scatter estimates for every recorded baseline and
/// correlation. Input flags participate in the fit and remain unchanged.
///
/// Returns `false` when the selection contains no visibility rows. The caller
/// holds the MAIN-table write locks throughout selection, fitting, and write.
/// An invalid selector, incompatible shape, or storage failure is an error.
pub fn reweight_legacy(
    ms: &mut StatWtMs,
    options: &LegacyStatWtOptions,
    locks: &mut BTreeMap<PathBuf, TableWriteLock>,
) -> MsResult<bool> {
    let mut selection = options.selection.clone();
    let plan = Windows::resolve(ms.metadata(), &options.fit_spw, selection.spw.take())?;
    if let Some(correlation) = selection.correlation.take().filter(|text| text != "*") {
        let request = MsSelection {
            correlation: Some(correlation),
            ..Default::default()
        };
        ms.metadata()
            .parse_ms_tool_selection(&request, &mut MsSelectionIndices::default())?;
    }
    let mut rows = ms.select(&selection).map_err(invalid)?;
    rows.retain(|row| plan.has_window(row.spectral_window_id));
    if rows.is_empty() {
        return Ok(false);
    }
    let mut setup = HashMap::new();
    for row in &rows {
        if let std::collections::hash_map::Entry::Vacant(entry) = setup.entry(row.data_desc_id) {
            let (_, physical, _) = ms.by_row(row.row_index);
            entry.insert(correlation_types_for_ddid(physical, row.data_desc_id).map_err(invalid)?);
        }
    }
    let column = column_name(&options.data_column)?;
    let models = (column == "MODEL_DATA")
        .then(|| ms.models(&rows))
        .transpose()?;
    let order = ordered_rows(ms, &rows, &options.combine)?;
    let minimum_samples = (options.minimum_samples as usize).max(if options.rms { 1 } else { 2 });
    let mut output = Vec::new();
    let request = ReadRequest {
        ms,
        rows: &rows,
        windows: &plan,
        setup: &setup,
        models: models.as_deref(),
        column,
    };
    for groups in order.chunks(16) {
        let mut samples = request.read(groups)?;
        for group in groups {
            fit_group(
                group,
                &request,
                &mut samples,
                options.rms,
                minimum_samples,
                &mut output,
            )?;
        }
    }
    output.sort_unstable_by_key(|(row, _)| *row);
    ms.persist_legacy(output, locks)?;
    Ok(true)
}

struct Windows {
    fit: BTreeMap<i32, Vec<usize>>,
    output: Vec<i32>,
}

impl Windows {
    fn resolve(ms: &MeasurementSet, fit: &str, output: Option<String>) -> MsResult<Self> {
        let fit = ms.resolve_spw_selector(if fit.is_empty() { "*" } else { fit })?;
        let output = ms.resolve_spw_selector(output.as_deref().unwrap_or("*"))?;
        let mut channels = BTreeMap::<i32, Vec<usize>>::new();
        for range in fit.channel_list() {
            if let Some(selected) = range.channels() {
                channels
                    .entry(range.spw_id as i32)
                    .or_default()
                    .extend(selected);
            }
        }
        for channels in channels.values_mut() {
            channels.sort_unstable();
            channels.dedup();
        }
        Ok(Self {
            fit: channels,
            output: output.spw_ids().iter().map(|&id| id as i32).collect(),
        })
    }

    fn has_window(&self, window: i32) -> bool {
        self.fit.contains_key(&window) || self.output.contains(&window)
    }
}

fn ordered_rows(
    ms: &StatWtMs,
    rows: &[ResolvedMsSelectionRow],
    combine: &str,
) -> MsResult<Vec<Vec<usize>>> {
    let metadata = ms.legacy_boundaries(rows)?;
    let combine = combine.to_ascii_lowercase();
    let watch = ["scan", "state", "spw", "obs"].map(|axis| !combine.contains(axis));
    let mut order = (0..rows.len()).collect::<Vec<_>>();
    let key = |slot: usize| {
        let [array, scan, state, observation] = metadata[slot];
        let row = &rows[slot];
        let watched = |axis: usize, value| if watch[axis] { value } else { 0 };
        let combined = |axis: usize, value| if watch[axis] { 0 } else { value };
        (
            [
                array,
                watched(0, scan),
                watched(1, state),
                row.field_id,
                watched(2, row.data_desc_id),
            ],
            row.time_mjd_seconds,
            [
                watched(3, observation),
                combined(0, scan),
                combined(1, state),
                combined(2, row.data_desc_id),
                combined(3, observation),
            ],
            row.row_index,
        )
    };
    order.sort_unstable_by(|&left, &right| {
        let left = key(left);
        let right = key(right);
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.total_cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
            .then_with(|| left.3.cmp(&right.3))
    });
    Ok(order
        .chunk_by(|&first, &last| rows[first].time_mjd_seconds == rows[last].time_mjd_seconds)
        .map(<[usize]>::to_vec)
        .collect())
}

pub(super) type LegacyOutputRow = (usize, [ArrayValue; 2]);

type Cells = HashMap<usize, (Option<ArrayValue>, Option<ArrayValue>)>;

struct ReadRequest<'a> {
    ms: &'a StatWtMs,
    rows: &'a [ResolvedMsSelectionRow],
    windows: &'a Windows,
    setup: &'a HashMap<i32, Vec<i32>>,
    models: Option<&'a [VisibilityModels]>,
    column: &'static str,
}

impl ReadRequest<'_> {
    fn read(&self, groups: &[Vec<usize>]) -> MsResult<Cells> {
        let mut slots = groups
            .iter()
            .flatten()
            .copied()
            .filter(|&slot| {
                let row = &self.rows[slot];
                !row.flag_row && self.windows.fit.contains_key(&row.spectral_window_id)
            })
            .collect::<Vec<_>>();
        slots.sort_unstable();
        let ids = slots
            .iter()
            .map(|&slot| self.rows[slot].row_index)
            .collect::<Vec<_>>();
        let flags = self.ms.array_cells("FLAG", &ids)?;
        let values = self.ms.array_cells(self.column, &ids)?;
        Ok(slots
            .into_iter()
            .zip(values.into_iter().zip(flags))
            .collect())
    }

    fn fit_row(
        &self,
        slot: usize,
        cells: &mut Cells,
        estimates: &mut [IntegrationScatter],
        rms: bool,
    ) -> MsResult<()> {
        let Some((data, Some(ArrayValue::Bool(flags)))) = cells.remove(&slot) else {
            return Err(invalid(
                "legacy statwt needs a defined visibility flag cell",
            ));
        };
        let row = &self.rows[slot];
        let codes = &self.setup[&row.data_desc_id];
        let data = self.visibility(row, data, &flags, codes)?;
        let channels = &self.windows.fit[&row.spectral_window_id];
        if flags.ndim() != 2
            || flags.len_of(Axis(0)) != estimates.len()
            || data
                .as_ref()
                .is_some_and(|data| data.shape() != flags.shape())
            || channels
                .last()
                .is_some_and(|&last| last >= flags.len_of(Axis(1)))
        {
            return Err(invalid(format!(
                "invalid visibility shape at row {}",
                row.row_index
            )));
        }
        for (correlation, estimate) in estimates.iter_mut().enumerate() {
            for &channel in channels {
                if !flags[[correlation, channel]] {
                    let value = data.as_ref().map_or_else(
                        || initial_model_visibility(codes[correlation]),
                        |data| data[[correlation, channel]],
                    );
                    estimate.add(value, rms);
                }
            }
        }
        Ok(())
    }

    fn visibility(
        &self,
        row: &ResolvedMsSelectionRow,
        data: Option<ArrayValue>,
        flags: &ArrayD<bool>,
        codes: &[i32],
    ) -> MsResult<Option<ArrayD<Complex32>>> {
        if let Some(models) = self.models {
            let (part, physical, physical_row) = self.ms.by_row(row.row_index);
            let model = &models[part];
            if !model.reads_column(row.field_id) {
                return model
                    .predict(physical, physical_row, codes, flags)
                    .map_err(invalid);
            }
            if data.is_none() {
                return Ok(None);
            }
        }
        match data {
            Some(ArrayValue::Complex32(data)) => Ok(Some(data)),
            _ => Err(invalid(format!(
                "{} is undefined at row {}",
                self.column, row.row_index
            ))),
        }
    }
}

fn fit_group(
    group: &[usize],
    request: &ReadRequest<'_>,
    cells: &mut Cells,
    rms: bool,
    minimum_samples: usize,
    output: &mut Vec<LegacyOutputRow>,
) -> MsResult<()> {
    let correlation = |slot: usize| &request.setup[&request.rows[slot].data_desc_id];
    let setup = correlation(group[0]);
    if group.iter().any(|&slot| correlation(slot) != setup) {
        return Err(invalid(
            "legacy statwt cannot combine different correlation setups",
        ));
    }
    let baseline = |slot: usize| {
        let row = &request.rows[slot];
        (row.antenna1_id, row.antenna2_id)
    };
    let mut estimates = HashMap::new();
    for &slot in group {
        if cells.contains_key(&slot) {
            let moments = estimates
                .entry(baseline(slot))
                .or_insert_with(|| vec![IntegrationScatter::default(); setup.len()]);
            request.fit_row(slot, cells, moments, rms)?;
        }
    }
    for &slot in group {
        let row = &request.rows[slot];
        if request.windows.output.contains(&row.spectral_window_id) {
            let moments = estimates.get(&baseline(slot));
            let variances = (0..setup.len()).map(|correlation| {
                moments.map_or(0.0, |moments| {
                    moments[correlation].variance(minimum_samples)
                })
            });
            let (weight, sigma): (Vec<_>, Vec<_>) = variances
                .map(|variance| {
                    if variance > 0.0 {
                        (variance.recip() as f32, variance.sqrt() as f32)
                    } else {
                        (0.0, -1.0)
                    }
                })
                .unzip();
            output.push((
                row.row_index,
                [weight, sigma].map(|values| {
                    ArrayValue::Float32(
                        ArrayD::from_shape_vec(IxDyn(&[values.len()]), values).expect("one axis"),
                    )
                }),
            ));
        }
    }
    Ok(())
}

/// Running complex mean in the visibility's stored precision and a 64-bit
/// sum of the products of the deviations before and after updating the mean.
#[derive(Clone, Copy, Default)]
struct IntegrationScatter {
    mean: Complex32,
    squared_deviation: f64,
    count: usize,
}

impl IntegrationScatter {
    fn add(&mut self, value: Complex32, rms: bool) {
        self.count += 1;
        let previous = value - self.mean;
        if !rms {
            self.mean += previous / self.count as f32;
        }
        let current = value - self.mean;
        self.squared_deviation += f64::from(previous.re * current.re + previous.im * current.im);
    }

    fn variance(self, minimum_samples: usize) -> f64 {
        if self.count < minimum_samples {
            return 0.0;
        }
        self.squared_deviation / (2.0 * self.count as f64 - 1.0)
    }
}

fn column_name(column: &str) -> MsResult<&'static str> {
    match column.to_ascii_uppercase().as_str() {
        "CORRECTED" | "CORRECTED_DATA" => Ok("CORRECTED_DATA"),
        "MODEL" | "MODEL_DATA" => Ok("MODEL_DATA"),
        "FLOAT" | "FLOAT_DATA" => Err(invalid("FLOAT_DATA is not supported as a data Cube.")),
        "LAG" | "LAG_DATA" => Err(invalid("LAG_DATA is not supported as a data Cube.")),
        _ => Ok("DATA"),
    }
}

fn invalid(error: impl std::fmt::Display) -> MsError {
    MsError::InvalidInput(error.to_string())
}
