// SPDX-License-Identifier: LGPL-3.0-or-later
//! Polynomial continuum fits written into a MeasurementSet's scratch columns.

mod fit;
mod transform;

pub use transform::LegacyContinuumTransform;

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;

use casa_core::quanta::{Quantity, Unit};
use casa_core::{ArrayValue, Complex32, Correlation};
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::{MeasurementSet, MsError, MsResult, MsSelection, ResolvedMsSelectionRow};
use casa_ms::{MeasurementSetMutationBatch, MeasurementSetWriteSession};
use casa_tables::{RequiredScalarColumnValues, TableWriteLock};
use ndarray::{ArrayD, Ix2};

use crate::model_columns::{
    ScratchWriteMode, initialize_projected_scratch_holding, scratch_write_plan,
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
    /// A duration, `int` for an integration or `inf` for a scan.
    pub solution_interval: String,
    /// Degree of the complex polynomial.
    pub order: usize,
    /// `model`, `subtract` (or `sub`), or `replace`.
    pub mode: String,
    /// Select `continuumsubold`'s binning and treatment of all-flagged samples.
    pub old_iterator: bool,
}

/// Initialize the fit inputs and destinations on the physical rows visible to
/// the current attachment. Existing scratch columns are preserved.
///
/// # Errors
/// A missing visibility column or failure to install a scratch column.
pub fn initialize_continuum_scratch_holding(
    ms: &mut MeasurementSet,
    rows: &[usize],
    observed: &str,
    scratch: [&str; 2],
    lock: &mut TableWriteLock,
) -> MsResult<()> {
    initialize_projected_scratch_holding(ms, rows, observed, scratch, lock)
}

/// Model corrected visibilities within each time bin and cross baseline. The
/// fit uses the attached logical rows, including across physical members.
/// Publication maps those rows back to each member under its MAIN write lock.
pub struct ContinuumScratchFit {
    fit: ScratchFit,
    columns: &'static [VisibilityDataColumn],
    mode: String,
}

impl ContinuumScratchFit {
    /// Prepare the complete logical fit after initialization of scratch data.
    ///
    /// # Errors
    /// A selector or solution interval is invalid, or an input cell is unreadable.
    pub fn prepare(ms: &MeasurementSet, options: &ContinuumScratchOptions) -> MsResult<Self> {
        let selection = MsSelection {
            field: options.field.clone(),
            msselect: Some("ANTENNA1!=ANTENNA2".into()),
            ..Default::default()
        };
        let mut rows = resolve_casa_calibration_selection(ms, &selection)
            .map_err(invalid)?
            .selected_rows;
        let windows = windows(ms, options)?;
        let interval = solution_interval(&options.solution_interval)?;
        rows.retain(|row| windows.contains_key(&row.spectral_window_id));
        let columns: &[_] = match options.mode.as_str() {
            "subtract" | "sub" => &[MODEL, CORRECTED],
            "model" => &[MODEL],
            "replace" => &[CORRECTED],
            _ => &[],
        };
        if columns.is_empty() {
            rows.clear();
        }
        let fit = ScratchFit::new(ms, rows, windows, interval, options.old_iterator)?;
        Ok(Self {
            fit,
            columns,
            mode: options.mode.clone(),
        })
    }

    /// Logical row IDs in the order used by [`Self::write_holding`].
    pub fn row_indices(&self) -> impl Iterator<Item = usize> + '_ {
        self.fit.rows.iter().map(|row| row.row_index)
    }

    /// Verify that each existing destination cell can accept the fitted data.
    /// Call for every constituent with its write lock held before updating any.
    ///
    /// # Errors
    /// A destination cell is missing, unreadable, or disagrees with the fit axes.
    pub fn check_destinations(
        &self,
        ms: &MeasurementSet,
        rows: &BTreeMap<usize, usize>,
        [corrected, model]: [&str; 2],
    ) -> MsResult<()> {
        let ids: Vec<_> = rows.keys().copied().collect();
        for &column in self.columns {
            let name = if column == CORRECTED {
                corrected
            } else {
                model
            };
            let shapes = scratch_shapes(ms.main_table(), name, &ids)?;
            for ((row, &logical), shape) in rows.iter().zip(shapes) {
                let source = &self.fit.rows[logical];
                let hands = &self.fit.hands[&source.data_desc_id];
                let fit = self.fit.fits.get(&self.fit.models[logical]);
                let valid = matches!(shape.as_deref(), Some([correlations, channels])
                    if hands.iter().all(|hand| hand < correlations)
                        && fit.is_none_or(|fit| fit.ncols() == *channels));
                if !valid {
                    return Err(invalid(format!(
                        "missing or invalid {name} cell at row {row}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Apply the model to one physical table. The row map pairs physical row
    /// numbers with positions in [`Self::row_indices`].
    ///
    /// # Errors
    /// A physical scratch column cannot be read or updated.
    pub fn write_holding(
        &self,
        ms: &mut MeasurementSet,
        rows: &BTreeMap<usize, usize>,
        [corrected, model]: [&str; 2],
        lock: &mut TableWriteLock,
    ) -> MsResult<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mode = ScratchWriteMode::Projected { corrected, model };
        let (plan, destinations) = scratch_write_plan(
            ms,
            rows.keys().copied().collect(),
            self.columns,
            corrected,
            mode,
            false,
            lock,
        )?;
        let batch_size = plan.batch_rows().max(1);
        let empty_ranges = BTreeMap::<i32, Range<usize>>::new();
        let ordered: Vec<_> = rows
            .iter()
            .map(|(&physical, &logical)| (physical, logical))
            .collect();
        MeasurementSetWriteSession::mutate_selected_rows(
            ms,
            plan,
            Some(lock),
            invalid,
            |session, ms| {
                for batch in ordered.chunks(batch_size) {
                    let ids = batch
                        .iter()
                        .map(|&(physical, _)| physical)
                        .collect::<Vec<_>>();
                    let logical_rows = batch
                        .iter()
                        .map(|&(_, logical)| self.fit.rows[logical].clone())
                        .collect::<Vec<_>>();
                    let models = batch
                        .iter()
                        .map(|&(_, logical)| self.fit.models[logical])
                        .collect::<Vec<_>>();
                    let mut batches = Vec::new();
                    for (&column, destination) in self.columns.iter().zip(&destinations) {
                        let name = if column == CORRECTED {
                            corrected
                        } else {
                            model
                        };
                        let values = self.fit.output(
                            ms,
                            &logical_rows,
                            &ids,
                            &models,
                            name,
                            column == CORRECTED && self.mode != "replace",
                        )?;
                        batches.extend(destination.batch(
                            ms,
                            &ids,
                            &logical_rows,
                            &empty_ranges,
                            values,
                        )?);
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
}

fn solution_interval(text: &str) -> MsResult<f64> {
    let folded = text.to_ascii_uppercase();
    if text.is_empty() || folded.contains("INF") {
        return Ok(0.0);
    }
    if folded.contains("INT") {
        return Ok(-1.0);
    }
    let quantity = Quantity::read(text).map_err(|_| invalid("Unrecognized units for solint."))?;
    let seconds = Unit::new("s").expect("seconds");
    Ok(f64::from(
        quantity
            .get_value_in(&seconds)
            .unwrap_or_else(|_| quantity.value()) as f32,
    ))
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
        interval: f64,
        old_iterator: bool,
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
        let groups = groups(ms, &rows, interval, old_iterator)?;
        let mut models = vec![
            BaselineKey {
                group: 0,
                first: 0,
                second: 0
            };
            rows.len()
        ];
        for (group_index, group) in groups.iter().enumerate() {
            for &slot in group {
                let row = &rows[slot];
                models[slot] = BaselineKey {
                    group: group_index,
                    first: row.antenna1_id,
                    second: row.antenna2_id,
                };
            }
        }
        let mut result = Self {
            rows,
            hands,
            models,
            fits: HashMap::new(),
        };
        result.fit_windows(ms, &groups, &windows, old_iterator)?;
        Ok(result)
    }

    fn fit_windows(
        &mut self,
        ms: &MeasurementSet,
        groups: &[Vec<usize>],
        windows: &BTreeMap<i32, FitGrid>,
        old_iterator: bool,
    ) -> MsResult<()> {
        const BATCH_ROWS: usize = 1024;
        let has_spectrum =
            !old_iterator && ms.main_table().column_accessor("WEIGHT_SPECTRUM").is_ok();
        let mut pending = groups;
        while let Some((first, following)) = pending.split_first() {
            // Adjacent short slots often use the same visibility tiles. Read
            // them together while capping the number of live row averages.
            let mut rows = first.len();
            let other_groups = following
                .iter()
                .take_while(|group| {
                    rows += group.len();
                    rows <= BATCH_ROWS
                })
                .count();
            let (wave, rest) = pending.split_at(other_groups + 1);
            pending = rest;
            let mut slots: Vec<_> = wave.iter().flatten().copied().collect();
            slots.sort_unstable();
            let mut averages = HashMap::new();
            for chunk in slots.chunks(BATCH_ROWS) {
                self.average_rows(ms, chunk, old_iterator, has_spectrum, &mut averages)?;
            }
            self.fits
                .extend(averages.into_iter().map(|(baseline, average)| {
                    let row = &self.rows[groups[baseline.group][0]];
                    (baseline, average.model(&windows[&row.spectral_window_id]))
                }));
        }
        Ok(())
    }

    fn average_rows(
        &self,
        ms: &MeasurementSet,
        slots: &[usize],
        old_iterator: bool,
        has_spectrum: bool,
        averages: &mut HashMap<BaselineKey, BaselineAverage>,
    ) -> MsResult<()> {
        let ids = slots
            .iter()
            .map(|&slot| self.rows[slot].row_index)
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
            slots.iter().zip(data).zip(flags).zip(weights).zip(spectrum)
        {
            let hands = &self.hands[&self.rows[slot].data_desc_id];
            accumulate(
                averages,
                self.models[slot],
                ScratchCells {
                    data,
                    flags,
                    weights,
                    spectrum,
                },
                hands,
                old_iterator,
            )?;
        }
        Ok(())
    }

    fn output(
        &self,
        ms: &MeasurementSet,
        rows: &[ResolvedMsSelectionRow],
        ids: &[usize],
        models: &[BaselineKey],
        column: &str,
        subtract: bool,
    ) -> MsResult<Vec<ArrayValue>> {
        arrays(ms, column, ids)?
            .into_iter()
            .zip(rows)
            .zip(models)
            .map(|((data, row), key)| {
                let mut data = complex(data, column)?;
                if let Some(fit) = self.fits.get(key) {
                    for (hand, &correlation) in self.hands[&row.data_desc_id].iter().enumerate() {
                        for channel in 0..fit.ncols() {
                            let sample = &mut data[[correlation, channel]];
                            *sample = if subtract {
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

struct ScratchCells {
    data: Option<ArrayValue>,
    flags: Option<ArrayValue>,
    weights: Option<ArrayValue>,
    spectrum: Option<ArrayValue>,
}

fn accumulate(
    averages: &mut HashMap<BaselineKey, BaselineAverage>,
    key: BaselineKey,
    ScratchCells {
        data,
        flags,
        weights,
        spectrum,
    }: ScratchCells,
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
    interval: f64,
    old_iterator: bool,
) -> MsResult<Vec<Vec<usize>>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let ids = rows.iter().map(|row| row.row_index).collect::<Vec<_>>();
    let table = ms.main_table();
    let per_description = if interval < 0.0 && !old_iterator {
        integration_intervals(table, rows)?
    } else {
        BTreeMap::new()
    };
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
    for ((_, description, _, _), rows_in_scan) in &mut buckets {
        let interval = per_description
            .get(description)
            .copied()
            .unwrap_or(interval);
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

fn integration_intervals(
    table: &casa_tables::Table,
    rows: &[ResolvedMsSelectionRow],
) -> MsResult<BTreeMap<i32, f64>> {
    let mut first = BTreeMap::new();
    for row in rows {
        first.entry(row.data_desc_id).or_insert(row.row_index);
    }
    let (descriptions, rows): (Vec<_>, Vec<_>) = first.into_iter().unzip();
    let mut intervals = table.required_scalar_columns_owned_for_rows(&["INTERVAL"], &rows)?;
    let Some(RequiredScalarColumnValues::Float64(seconds)) = intervals.remove("INTERVAL") else {
        return Err(invalid("INTERVAL must be float64"));
    };
    // VI2 receives the first interval of each selected description as a Float.
    Ok(descriptions
        .into_iter()
        .zip(seconds.into_iter().map(|value| f64::from(value as f32)))
        .collect())
}

fn arrays(ms: &MeasurementSet, column: &str, rows: &[usize]) -> MsResult<Vec<Option<ArrayValue>>> {
    Ok(ms
        .main_table()
        .column_accessor(column)?
        .array_cells_owned_uncached(rows)?)
}

fn scratch_shapes(
    table: &casa_tables::Table,
    name: &str,
    rows: &[usize],
) -> MsResult<Vec<Option<Vec<usize>>>> {
    let source = table.column_accessor(name)?;
    if table
        .schema()
        .and_then(|schema| schema.column(name))
        .and_then(|column| column.data_type())
        != Some(casa_core::PrimitiveType::Complex32)
    {
        return Err(invalid(format!("invalid {name} column type")));
    }
    if let Ok(shapes) = source.array_shapes_uncached(rows) {
        return Ok(shapes);
    }
    let mut shapes = Vec::with_capacity(rows.len());
    for batch in rows.chunks(128) {
        for cell in source.array_cells_owned_uncached(batch)? {
            shapes.push(match cell {
                Some(ArrayValue::Complex32(array)) => Some(array.shape().to_vec()),
                _ => None,
            });
        }
    }
    Ok(shapes)
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
