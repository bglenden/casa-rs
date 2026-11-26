// SPDX-License-Identifier: LGPL-3.0-or-later
//! Polynomial continuum fits written into a MeasurementSet's scratch columns.

mod fit;
mod transform;

pub use transform::LegacyContinuumTransform;

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;

use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::write_session::{MeasurementSetMutationBatch, MeasurementSetWriteSession};
use casa_ms::{MeasurementSet, MsError, MsResult, MsSelection, ResolvedMsSelectionRow};
use casa_tables::{RequiredScalarColumnValues, TableWriteLock};
use casa_types::{ArrayValue, Complex32, Correlation};
use ndarray::{ArrayD, Ix2};

use crate::model_columns::{
    ScratchWriteMode, initialize_calibrater_columns_holding, observed_column, scratch_write_plan,
};
use crate::session::resolve_casa_calibration_selection;
use crate::solve::grouping::correlation_types_for_ddid;
use fit::{BaselineAverage, FitGrid};

const CORRECTED: VisibilityDataColumn = VisibilityDataColumn::CorrectedData;
const MODEL: VisibilityDataColumn = VisibilityDataColumn::ModelData;

/// The operations common to the public `continuumsub` and `continuumsubold`.
pub struct ContinuumScratchOptions {
    /// Field expression in CASA selection syntax.
    pub field: Option<String>,
    /// Spectral windows and channels contributing to the fit.
    pub fit_spw: String,
    /// Channels on which to apply a fit, or an empty selector for all.
    pub apply_spw: String,
    /// Zero fits one scan at a time, a negative value fits one integration,
    /// and a positive value sets the length of a bin in seconds.
    pub solution_interval: f64,
    /// Degree of the complex polynomial.
    pub order: usize,
    /// `model`, `subtract` (or `sub`), or `replace`.
    pub mode: String,
    /// Select `continuumsubold`'s binning and treatment of all-flagged samples.
    pub old_iterator: bool,
}

/// Model the continuum in corrected visibilities within each time bin and
/// baseline. Only parallel hands and cross baselines contribute or change.
/// The MAIN write lock must protect `ms` through initialization and publication.
/// Missing scratch columns are initialized over the full attached MS.
///
/// # Errors
/// An invalid selector, incompatible visibility shape, or failed table update.
pub fn fit_continuum_scratch_holding(
    ms: &mut MeasurementSet,
    options: &ContinuumScratchOptions,
    lock: &mut TableWriteLock,
) -> MsResult<()> {
    let windows = windows(ms, options)?;
    let selection = MsSelection {
        field: options.field.clone(),
        msselect: Some("ANTENNA1!=ANTENNA2".into()),
        ..Default::default()
    };
    let mut rows = resolve_casa_calibration_selection(ms, &selection)
        .map_err(invalid)?
        .selected_rows;
    rows.retain(|row| windows.contains_key(&row.spectral_window_id));
    let columns: &[_] = match options.mode.as_str() {
        "subtract" | "sub" => &[MODEL, CORRECTED],
        "model" => &[MODEL],
        "replace" => &[CORRECTED],
        _ => &[],
    };
    initialize_calibrater_columns_holding(ms, false, true, true, lock)?;
    if rows.is_empty() || columns.is_empty() {
        return Ok(());
    }
    let context = ScratchFit::new(ms, rows, windows, options)?;
    let observed = observed_column(ms);
    let (plan, destinations) = scratch_write_plan(
        ms,
        context.rows.iter().map(|row| row.row_index).collect(),
        columns,
        observed,
        ScratchWriteMode::Replace,
        false,
        lock,
    )?;
    let batch_size = plan.batch_rows().max(1);
    let empty_ranges = BTreeMap::<i32, Range<usize>>::new();
    MeasurementSetWriteSession::mutate_selected_rows(
        ms,
        plan,
        Some(lock),
        invalid,
        |session, ms| {
            for (first, rows) in context.rows.chunks(batch_size).enumerate() {
                let ids = rows.iter().map(|row| row.row_index).collect::<Vec<_>>();
                let models = &context.models[first * batch_size..][..rows.len()];
                let mut batches = Vec::new();
                for (&column, destination) in columns.iter().zip(&destinations) {
                    let values = context.output(ms, rows, &ids, models, column, &options.mode)?;
                    batches.extend(destination.batch(ms, &ids, rows, &empty_ranges, values)?);
                }
                session
                    .write_mutation_batch(
                        ms,
                        MeasurementSetMutationBatch {
                            row_indices: ids,
                            columns: batches,
                        },
                    )
                    .map_err(invalid)?;
            }
            Ok(())
        },
    )?;
    Ok(())
}

fn windows(
    ms: &MeasurementSet,
    options: &ContinuumScratchOptions,
) -> MsResult<BTreeMap<i32, FitGrid>> {
    let fit = ms.resolve_spw_selector(if options.fit_spw.is_empty() {
        "*"
    } else {
        &options.fit_spw
    })?;
    let apply = ms.resolve_spw_selector(&options.apply_spw)?;
    let ranges = |ranges: &[casa_ms::selection::ChannelRange], window: usize| {
        let mut channels = ranges
            .iter()
            .filter(|range| range.spw_id == window)
            .filter(|range| range.start >= 0)
            .flat_map(|range| range.start as usize..=range.stop as usize)
            .collect::<Vec<_>>();
        channels.sort_unstable();
        channels.dedup();
        channels
    };
    let spectral = ms.spectral_window()?;
    let mut result = BTreeMap::new();
    for &window in fit.spw_ids() {
        let count = usize::try_from(spectral.num_chan(window)?).map_err(invalid)?;
        let fit_channels = ranges(fit.channel_list(), window);
        let mut apply_channels = ranges(apply.channel_list(), window);
        if apply_channels.is_empty() {
            apply_channels.extend(0..count);
        }
        result.insert(
            window as i32,
            FitGrid::new(options.order, fit_channels, apply_channels, count),
        );
    }
    Ok(result)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct BaselineKey {
    group: usize,
    first: i32,
    second: i32,
}

struct ScratchFit {
    rows: Vec<ResolvedMsSelectionRow>,
    hands: HashMap<i32, Vec<usize>>,
    models: Vec<BaselineKey>,
    fits: HashMap<BaselineKey, ndarray::Array2<Complex32>>,
}

impl ScratchFit {
    fn new(
        ms: &MeasurementSet,
        rows: Vec<ResolvedMsSelectionRow>,
        windows: BTreeMap<i32, FitGrid>,
        options: &ContinuumScratchOptions,
    ) -> MsResult<Self> {
        let mut hands = HashMap::new();
        for row in &rows {
            if let std::collections::hash_map::Entry::Vacant(entry) = hands.entry(row.data_desc_id)
            {
                let correlations =
                    correlation_types_for_ddid(ms, row.data_desc_id).map_err(invalid)?;
                entry.insert(
                    correlations
                        .iter()
                        .enumerate()
                        .filter_map(|(index, &code)| {
                            matches!(
                                Correlation::from_casacore_code(code),
                                Some(
                                    Correlation::RR
                                        | Correlation::LL
                                        | Correlation::XX
                                        | Correlation::YY
                                )
                            )
                            .then_some(index)
                        })
                        .collect::<Vec<_>>(),
                );
            }
        }
        let groups = groups(ms, &rows, options.solution_interval, options.old_iterator)?;
        let mut models = vec![
            BaselineKey {
                group: 0,
                first: 0,
                second: 0
            };
            rows.len()
        ];
        let mut fits = HashMap::new();
        let has_spectrum =
            !options.old_iterator && ms.main_table().column_accessor("WEIGHT_SPECTRUM").is_ok();
        for (group_index, group) in groups.iter().enumerate() {
            let mut averages = HashMap::new();
            for &slot in group {
                let row = &rows[slot];
                models[slot] = BaselineKey {
                    group: group_index,
                    first: row.antenna1_id,
                    second: row.antenna2_id,
                };
            }
            let mut selected = group.clone();
            selected.sort_unstable();
            for batch in selected.chunks(1024) {
                let ids = batch
                    .iter()
                    .map(|&slot| rows[slot].row_index)
                    .collect::<Vec<_>>();
                let data = arrays(ms, CORRECTED.name(), &ids)?;
                let flags = arrays(ms, "FLAG", &ids)?;
                let weights = arrays(ms, "WEIGHT", &ids)?;
                let spectrum = if has_spectrum {
                    arrays(ms, "WEIGHT_SPECTRUM", &ids)?
                } else {
                    vec![None; ids.len()]
                };
                for ((((&slot, data), flags), weights), spectrum) in
                    batch.iter().zip(data).zip(flags).zip(weights).zip(spectrum)
                {
                    let row = &rows[slot];
                    let hands = &hands[&row.data_desc_id];
                    accumulate(
                        &mut averages,
                        models[slot],
                        data,
                        flags,
                        weights,
                        spectrum,
                        hands,
                        options.old_iterator,
                    )?;
                }
            }
            let grid = &windows[&rows[group[0]].spectral_window_id];
            fits.extend(
                averages
                    .into_iter()
                    .map(|(baseline, average)| (baseline, average.model(grid))),
            );
        }
        Ok(Self {
            rows,
            hands,
            models,
            fits,
        })
    }

    fn output(
        &self,
        ms: &MeasurementSet,
        rows: &[ResolvedMsSelectionRow],
        ids: &[usize],
        models: &[BaselineKey],
        column: VisibilityDataColumn,
        mode: &str,
    ) -> MsResult<Vec<ArrayValue>> {
        arrays(ms, column.name(), ids)?
            .into_iter()
            .zip(rows)
            .zip(models)
            .map(|((data, row), key)| {
                let mut data = complex(data, column.name())?;
                if let Some(fit) = self.fits.get(key) {
                    for (hand, &correlation) in self.hands[&row.data_desc_id].iter().enumerate() {
                        for channel in 0..fit.ncols() {
                            let sample = &mut data[[correlation, channel]];
                            *sample = if column == CORRECTED && mode != "replace" {
                                *sample - fit[[hand, channel]]
                            } else {
                                fit[[hand, channel]]
                            };
                        }
                    }
                }
                Ok(ArrayValue::Complex32(data))
            })
            .collect()
    }
}

fn accumulate(
    averages: &mut HashMap<BaselineKey, BaselineAverage>,
    key: BaselineKey,
    data: Option<ArrayValue>,
    flags: Option<ArrayValue>,
    weights: Option<ArrayValue>,
    spectrum: Option<ArrayValue>,
    hands: &[usize],
    old_iterator: bool,
) -> MsResult<()> {
    let data = complex(data, "CORRECTED_DATA")?
        .into_dimensionality::<Ix2>()
        .map_err(invalid)?;
    let (Some(ArrayValue::Bool(flags)), Some(ArrayValue::Float32(weights))) = (flags, weights)
    else {
        return Err(invalid("continuum fits require FLAG and WEIGHT"));
    };
    let flags = flags.into_dimensionality::<Ix2>().map_err(invalid)?;
    let weights = weights
        .as_slice_memory_order()
        .ok_or_else(|| invalid("noncontiguous WEIGHT"))?;
    let spectrum = match spectrum {
        Some(ArrayValue::Float32(spectrum)) if !spectrum.is_empty() => {
            Some(spectrum.into_dimensionality::<Ix2>().map_err(invalid)?)
        }
        None | Some(ArrayValue::Float32(_)) => None,
        _ => return Err(invalid("WEIGHT_SPECTRUM must be float32")),
    };
    if data.shape() != flags.shape()
        || weights.len() != data.nrows()
        || spectrum.as_ref().is_some_and(|w| w.shape() != data.shape())
        || hands.iter().any(|&hand| hand >= weights.len())
    {
        return Err(invalid(
            "CORRECTED_DATA does not agree with FLAG, WEIGHT or POLARIZATION",
        ));
    }
    let average = averages
        .entry(key)
        .or_insert_with(|| BaselineAverage::new(hands.len(), data.ncols(), old_iterator));
    average.accumulate(
        data.view(),
        flags.view(),
        weights,
        spectrum.as_ref().map(|w| w.view()),
        hands,
    );
    Ok(())
}

fn groups(
    ms: &MeasurementSet,
    rows: &[ResolvedMsSelectionRow],
    mut interval: f64,
    old_iterator: bool,
) -> MsResult<Vec<Vec<usize>>> {
    let ids = rows.iter().map(|row| row.row_index).collect::<Vec<_>>();
    let table = ms.main_table();
    if interval < 0.0 && !old_iterator {
        let mut first = table.required_scalar_columns_owned_for_rows(&["INTERVAL"], &ids[..1])?;
        let Some(RequiredScalarColumnValues::Float64(seconds)) = first.remove("INTERVAL") else {
            return Err(invalid("INTERVAL must be float64"));
        };
        interval = seconds[0];
    }
    let mut metadata =
        table.required_scalar_columns_owned_for_rows(&["ARRAY_ID", "SCAN_NUMBER"], &ids)?;
    let (
        Some(RequiredScalarColumnValues::Int32(arrays)),
        Some(RequiredScalarColumnValues::Int32(scans)),
    ) = (metadata.remove("ARRAY_ID"), metadata.remove("SCAN_NUMBER"))
    else {
        return Err(invalid("ARRAY_ID and SCAN_NUMBER must be int32"));
    };
    let mut buckets = BTreeMap::<_, Vec<_>>::new();
    for (slot, row) in rows.iter().enumerate() {
        buckets
            .entry((arrays[slot], row.data_desc_id, scans[slot], row.field_id))
            .or_default()
            .push(slot);
    }
    let mut groups = Vec::new();
    for rows_in_scan in buckets.values_mut() {
        rows_in_scan.sort_by(|&a, &b| {
            rows[a]
                .time_mjd_seconds
                .total_cmp(&rows[b].time_mjd_seconds)
        });
        let mut pending = rows_in_scan.as_slice();
        while let Some(first) = pending.first() {
            let start = rows[*first].time_mjd_seconds;
            let end = if interval == 0.0 {
                pending.len()
            } else if interval < 0.0 {
                pending.partition_point(|&slot| rows[slot].time_mjd_seconds == start)
            } else {
                pending
                    .partition_point(|&slot| rows[slot].time_mjd_seconds - start < interval - 0.01)
                    .max(1)
            };
            groups.push(pending[..end].to_vec());
            pending = &pending[end..];
        }
    }
    Ok(groups)
}

fn arrays(ms: &MeasurementSet, column: &str, rows: &[usize]) -> MsResult<Vec<Option<ArrayValue>>> {
    Ok(ms
        .main_table()
        .column_accessor(column)?
        .array_cells_owned_uncached(rows)?)
}

fn complex(cell: Option<ArrayValue>, name: &str) -> MsResult<ArrayD<Complex32>> {
    match cell {
        Some(ArrayValue::Complex32(data)) if data.ndim() == 2 => Ok(data),
        _ => Err(invalid(format!("missing or invalid {name} cell"))),
    }
}

fn invalid(error: impl std::fmt::Display) -> MsError {
    MsError::InvalidInput(error.to_string())
}
