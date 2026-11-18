// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selection, bucketing, and accumulation for `gaincal` and `bandpass`.

mod samples;

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use casa_ms::ChannelSelection;
use casa_ms::MsError;
use casa_ms::ms::MeasurementSet;
use casa_ms::selection::MsSelection;
use casa_types::{ArrayValue, Complex32};
use ndarray::{ArrayD, ArrayView2, Axis, Ix2, IxDyn, ShapeBuilder};

use super::{
    GainSolveCombine, GainSolveError, GainSolveInterval, GainSolveMode, GainSolveModelSource,
    GainType, RefAntSelector, correlation_receptors, get_f64, get_i32, stokes_name,
};
use crate::ApplyCalibrationTableSpec;
use crate::execute::{EvaluatedApplyRow, evaluate_apply_rows};
use crate::plan::{ApplyPlanRequest, plan_apply};
use crate::session::resolve_calibration_selection;
use crate::solve::kernel::SolveGraph;

pub(crate) fn validate_solve_interval(
    solve_interval: GainSolveInterval,
) -> Result<(), GainSolveError> {
    match solve_interval {
        GainSolveInterval::Infinite | GainSolveInterval::Integration => Ok(()),
        GainSolveInterval::Seconds(seconds) if seconds > 0.0 => Ok(()),
        _ => Err(GainSolveError::UnsupportedSolveInterval { solve_interval }),
    }
}

pub(crate) fn validate_smodel(smodel: [f32; 4]) -> Result<(), GainSolveError> {
    if smodel[0] <= 0.0 || smodel[1] != 0.0 || smodel[2] != 0.0 || smodel[3] != 0.0 {
        return Err(GainSolveError::UnsupportedSkyModel { smodel });
    }
    Ok(())
}

pub(crate) fn load_preapplied_rows(
    ms: &MeasurementSet,
    selection: &MsSelection,
    prior_calibration_tables: &[ApplyCalibrationTableSpec],
    parang: bool,
) -> Result<Option<HashMap<usize, EvaluatedApplyRow>>, GainSolveError> {
    if prior_calibration_tables.is_empty() && !parang {
        return Ok(None);
    }
    let plan = plan_apply(
        ms,
        &ApplyPlanRequest {
            selection: selection.clone(),
            apply_mode: crate::ApplyMode::CalFlag,
            parang,
            calibration_tables: prior_calibration_tables.to_vec(),
        },
    )
    .map_err(|source| GainSolveError::PriorCalibrationPlan {
        source: Box::new(source),
    })?;
    evaluate_apply_rows(ms, &plan).map(Some).map_err(|source| {
        GainSolveError::PriorCalibrationApply {
            source: Box::new(source),
        }
    })
}

pub(crate) fn build_solve_groups(
    ms: &MeasurementSet,
    rows: &[SelectedSolveRow],
    preapplied_rows: Option<&HashMap<usize, EvaluatedApplyRow>>,
    options: SolveGroupOptions,
) -> Result<BTreeMap<(SolveBaseKey, SolveBucketKey), SolveAccumulator>, GainSolveError> {
    let mut groups = BTreeMap::<(SolveBaseKey, SolveBucketKey), SolveAccumulator>::new();
    let mut layouts_by_ddid = HashMap::new();
    let ordered = bucketed_rows(rows, options.solve_interval, options.combine);
    let pipelined = overlap_inputs(ordered.len());
    if std::env::var_os("CASA_RS_CALIBRATION_TIMING").is_some() {
        eprintln!("calibration_workers {}", if pipelined { 2 } else { 1 });
    }
    let read = |batch: &[BucketedRow<'_>]| {
        let _phase = crate::timing::Phase::start("solve_read_decode");
        let indices = batch
            .iter()
            .map(|(row, _, _)| row.row_index)
            .collect::<Vec<_>>();
        read_input_batch(
            ms,
            &indices,
            preapplied_rows.is_none(),
            options.model_source,
        )
    };
    let mut consume = |batch: &[BucketedRow<'_>], inputs: SolveInputBatch| {
        let _accumulate_phase = crate::timing::Phase::start("solve_accumulate");
        for (index, &(row, base_key, bucket_key)) in batch.iter().enumerate() {
            let group = groups.entry((base_key, bucket_key)).or_insert_with(|| {
                SolveAccumulator::new(row.field_id, row.data_spw_id, row.observation_id)
            });
            if options.combine.fields {
                // CalSolVi2Organizer orders the buffers of a grouped time
                // interval by FIELD_ID. SDBList::aggregateFld uses the first.
                group.field_id = group.field_id.min(row.field_id);
            }
            let preapplied = preapplied_rows.and_then(|rows| rows.get(&row.row_index));
            group.observe(
                ms,
                row,
                preapplied,
                &options,
                &mut layouts_by_ddid,
                (&inputs, index),
            )?;
        }
        Ok::<_, GainSolveError>(())
    };
    if pipelined {
        std::thread::scope(|scope| {
            let (sender, receiver) = std::sync::mpsc::sync_channel(1);
            let producer_rows = &ordered;
            scope.spawn(move || {
                for batch in producer_rows.chunks(256) {
                    let input = read(batch);
                    let failed = input.is_err();
                    if sender.send(input).is_err() || failed {
                        break;
                    }
                }
            });
            // Drop the receiver before scope joins the reader on an error,
            // unblocking a producer that is waiting to send its next batch.
            (|| {
                for (batch, input) in ordered.chunks(256).zip(receiver) {
                    consume(batch, input?)?;
                }
                Ok::<_, GainSolveError>(())
            })()
        })?;
    } else {
        for batch in ordered.chunks(256) {
            consume(batch, read(batch)?)?;
        }
    }

    Ok(groups)
}

type BucketedRow<'a> = (&'a SelectedSolveRow, SolveBaseKey, SolveBucketKey);

/// One reader and the calling accumulation thread, within the CPU budget.
fn overlap_inputs(rows: usize) -> bool {
    let available = std::thread::available_parallelism().map_or(1, usize::from);
    let requested = std::env::var("RAYON_NUM_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(available);
    rows > 256 && available.min(requested) >= 2
}

/// Array payloads live for one batch, without filling the table's row cache.
pub(crate) struct SolveInputBatch {
    columns: Vec<(&'static str, Vec<Option<ArrayValue>>)>,
}

/// Read selected arrays together so headers and tiles are shared by a batch;
/// release the payloads after accumulation instead of retaining the whole MS.
fn read_input_batch(
    ms: &MeasurementSet,
    rows: &[usize],
    read_visibilities: bool,
    model: GainSolveModelSource,
) -> Result<SolveInputBatch, GainSolveError> {
    let schema = ms.main_table().schema();
    let spectra = ["WEIGHT_SPECTRUM", "SIGMA_SPECTRUM"]
        .iter()
        .all(|name| schema.is_some_and(|schema| schema.contains_column(name)));
    let mut columns = vec![if spectra { "SIGMA_SPECTRUM" } else { "SIGMA" }];
    if read_visibilities {
        columns.extend(["DATA", "FLAG"]);
    }
    if model == GainSolveModelSource::ModelColumn {
        columns.push("MODEL_DATA");
    }
    let mut inputs = SolveInputBatch {
        columns: Vec::with_capacity(columns.len()),
    };
    for column in columns {
        let values = ms
            .main_table()
            .column_accessor(column)
            .and_then(|accessor| {
                read_column_inputs(
                    &accessor,
                    column,
                    schema
                        .and_then(|s| s.column(column))
                        .and_then(|c| c.data_type()),
                    rows,
                )
            })
            .map_err(|source| ms_read_error(ms, source))?;
        inputs.columns.push((column, values));
    }
    Ok(inputs)
}

/// Homogeneous cells use the typed selected reader, avoiding reconstruction
/// of complete tiled cubes. Mixed shapes and pending edits use the ordinary
/// owned-cell reader, preserving their existing validation and semantics.
fn read_column_inputs(
    column: &casa_tables::TableColumn<'_>,
    name: &str,
    kind: Option<casa_types::PrimitiveType>,
    rows: &[usize],
) -> Result<Vec<Option<ArrayValue>>, casa_tables::TableError> {
    if name != "SIGMA"
        && let Ok(shapes) = column.array_shapes_uncached(rows)
        && let Some(Some(shape)) = shapes.first()
        && shape.len() == 2
        && shape.iter().all(|n| *n > 0)
        && shapes.iter().all(|other| other.as_ref() == Some(shape))
    {
        use casa_tables::SelectedArray2DCellsMut;
        use casa_types::PrimitiveType;
        let values = match kind {
            Some(PrimitiveType::Complex32) => read_row_packed(
                column,
                rows,
                shape[1],
                |values| SelectedArray2DCellsMut::RowChannelComplex32(values),
                ArrayValue::Complex32,
            )?,
            Some(PrimitiveType::Bool) => read_row_packed(
                column,
                rows,
                shape[1],
                |values| SelectedArray2DCellsMut::RowChannelBool(values),
                ArrayValue::Bool,
            )?,
            Some(PrimitiveType::Float32) => read_row_packed(
                column,
                rows,
                shape[1],
                |values| SelectedArray2DCellsMut::RowChannelFloat32(values),
                ArrayValue::Float32,
            )?,
            _ => None,
        };
        if let Some(values) = values {
            return Ok(values);
        }
    }
    column.array_cells_owned_uncached(rows)
}

/// Request contiguous row cells directly, avoiding a channel/row transpose.
fn read_row_packed<T: Clone>(
    column: &casa_tables::TableColumn<'_>,
    rows: &[usize],
    channels: usize,
    destination: for<'a> fn(&'a mut Vec<T>) -> casa_tables::SelectedArray2DCellsMut<'a>,
    wrap: fn(ArrayD<T>) -> ArrayValue,
) -> Result<Option<Vec<Option<ArrayValue>>>, casa_tables::TableError> {
    let mut values = Vec::new();
    let Some(shape) = column.fill_array_cells_2d_channel_range_typed_uncached(
        rows,
        0,
        channels,
        destination(&mut values),
    )?
    else {
        return Ok(None);
    };
    let shape = [shape.axis0_count, shape.channel_count];
    let capacity = shape[0].checked_mul(shape[1]).ok_or_else(|| {
        casa_tables::TableError::Storage("solve input cell size overflows".to_string())
    })?;
    values
        .chunks_exact(capacity)
        .map(|row| {
            ArrayD::from_shape_vec(IxDyn(&shape).f(), row.to_vec())
                .map(|array| Some(wrap(array)))
                .map_err(|e| casa_tables::TableError::Storage(e.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn input_cell<'a>(
    ms: &'a MeasurementSet,
    row: usize,
    inputs: (&'a SolveInputBatch, usize),
    column: &str,
) -> Result<&'a ArrayValue, GainSolveError> {
    if let Some((_, values)) = inputs.0.columns.iter().find(|(name, _)| *name == column) {
        return values[inputs.1].as_ref().ok_or_else(|| {
            ms_read_error(
                ms,
                casa_tables::TableError::ColumnNotFound {
                    row_index: row,
                    column: column.to_string(),
                },
            )
        });
    }
    main_cell(ms, row, column)
}

fn ms_read_error(ms: &MeasurementSet, source: casa_tables::TableError) -> GainSolveError {
    GainSolveError::OpenMeasurementSet {
        path: ms
            .path()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "<in-memory>".to_string()),
        source: MsError::from(source),
    }
}

/// Each cross-correlation row of `rows`, in solve order, with the group and
/// interval it is solved in.
///
/// CASA's iteration sorts by the axes `combine` keeps apart and then by
/// time, so the rows of a group come in time order whatever their fields
/// and scans, and a `solint` interval starts at its group's earliest time.
fn bucketed_rows(
    rows: &[SelectedSolveRow],
    solve_interval: GainSolveInterval,
    combine: GainSolveCombine,
) -> Vec<(&SelectedSolveRow, SolveBaseKey, SolveBucketKey)> {
    let mut keyed = rows
        .iter()
        .filter(|row| row.antenna1 != row.antenna2)
        .map(|row| {
            let base_key = SolveBaseKey {
                field_id: if combine.fields { 0 } else { row.field_id },
                spw_id: row.data_spw_id,
                observation_id: row.observation_id,
                scan_number: if combine.scans { 0 } else { row.scan_number },
            };
            (row, base_key)
        })
        .collect::<Vec<_>>();
    keyed.sort_by(|(a, a_key), (b, b_key)| {
        a_key
            .cmp(b_key)
            .then(a.time_seconds.total_cmp(&b.time_seconds))
            .then(a.interval_seconds.total_cmp(&b.interval_seconds))
            .then(a.row_index.cmp(&b.row_index))
    });

    let mut bucket_starts = BTreeMap::<SolveBaseKey, f64>::new();
    keyed
        .into_iter()
        .map(|(row, base_key)| {
            let bucket_key = match solve_interval {
                GainSolveInterval::Infinite => SolveBucketKey::Infinite,
                GainSolveInterval::Integration => SolveBucketKey::Integration {
                    time_bits: row.time_seconds.to_bits(),
                    interval_bits: row.interval_seconds.to_bits(),
                },
                GainSolveInterval::Seconds(seconds) => {
                    let start = *bucket_starts.entry(base_key).or_insert(row.time_seconds);
                    let index = ((row.time_seconds - start) / seconds).floor().max(0.0);
                    SolveBucketKey::Seconds(index as u64)
                }
            };
            (row, base_key, bucket_key)
        })
        .collect()
}

/// Refuses `rows` when two of their data descriptions share a spectral
/// window, whatever their polarizations: CASA solves each description
/// apart, where casa-rs's groups and tables are keyed by window.
pub(crate) fn refuse_shared_spectral_windows(
    rows: &[SelectedSolveRow],
) -> Result<(), GainSolveError> {
    let mut descriptions = BTreeMap::<i32, BTreeSet<i32>>::new();
    for row in rows {
        descriptions
            .entry(row.data_spw_id)
            .or_default()
            .insert(row.data_desc_id);
    }
    match descriptions.into_iter().find(|(_, ids)| ids.len() > 1) {
        Some((spw_id, ids)) => Err(GainSolveError::SharedSpectralWindow {
            spw_id,
            data_desc_ids: ids.into_iter().collect(),
        }),
        None => Ok(()),
    }
}

pub(crate) fn all_antenna_ids(ms: &MeasurementSet) -> Result<BTreeSet<i32>, GainSolveError> {
    let antenna = ms
        .antenna()
        .map_err(|source| GainSolveError::OpenMeasurementSet {
            path: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            source,
        })?;
    Ok((0..antenna.row_count())
        .map(|row| i32::try_from(row).expect("antenna row count should fit in i32"))
        .collect())
}

pub(crate) fn resolve_refant(
    ms: &MeasurementSet,
    selector: &RefAntSelector,
) -> Result<i32, GainSolveError> {
    match selector {
        RefAntSelector::AntennaId(id) => Ok(*id),
        RefAntSelector::AntennaName(name) => {
            let antenna = ms
                .antenna()
                .map_err(|error| GainSolveError::ResolveRefAnt {
                    selector: name.clone(),
                    reason: error.to_string(),
                })?;
            for row in 0..antenna.row_count() {
                if antenna
                    .name(row)
                    .map_err(|error| GainSolveError::ResolveRefAnt {
                        selector: name.clone(),
                        reason: error.to_string(),
                    })?
                    == *name
                {
                    return Ok(row as i32);
                }
            }
            Err(GainSolveError::ResolveRefAnt {
                selector: name.clone(),
                reason: "no ANTENNA.NAME match".to_string(),
            })
        }
    }
}

pub(crate) fn collect_selected_rows(
    ms: &MeasurementSet,
    selection: &MsSelection,
) -> Result<Vec<SelectedSolveRow>, GainSolveError> {
    let selected_rows = resolve_calibration_selection(ms, selection).map_err(|error| {
        GainSolveError::OpenMeasurementSet {
            path: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            source: match error {
                casa_ms::MsSelectionError::Domain(source) => source,
                other => MsError::InvalidInput(other.to_string()),
            },
        }
    })?;
    if selected_rows.selected_rows.is_empty() {
        return Err(GainSolveError::EmptySelection);
    }

    selected_rows
        .selected_rows
        .into_iter()
        .map(|row| {
            Ok(SelectedSolveRow {
                row_index: row.row_index,
                field_id: row.field_id,
                observation_id: get_i32(ms.main_table(), row.row_index, "OBSERVATION_ID")?,
                data_desc_id: row.data_desc_id,
                data_spw_id: row.spectral_window_id,
                channel_selection: selection.channel_selection_for_spw(row.spectral_window_id),
                antenna1: row.antenna1_id,
                antenna2: row.antenna2_id,
                time_seconds: row.time_mjd_seconds,
                time_centroid_seconds: get_f64(ms.main_table(), row.row_index, "TIME_CENTROID")?,
                interval_seconds: get_f64(ms.main_table(), row.row_index, "INTERVAL")?,
                scan_number: get_i32(ms.main_table(), row.row_index, "SCAN_NUMBER")?,
                flag_row: row.flag_row,
            })
        })
        .collect()
}

pub(crate) fn correlation_types_for_ddid(
    ms: &MeasurementSet,
    data_desc_id: i32,
) -> Result<Vec<i32>, GainSolveError> {
    let dd = ms
        .data_description()
        .map_err(|source| GainSolveError::OpenMeasurementSet {
            path: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            source,
        })?;
    let pol = ms
        .polarization()
        .map_err(|source| GainSolveError::OpenMeasurementSet {
            path: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            source,
        })?;
    let row = usize::try_from(data_desc_id).map_err(|_| {
        GainSolveError::UnsupportedCorrelationLayout {
            data_desc_id,
            correlation_types: Vec::new(),
        }
    })?;
    let pol_id = dd
        .polarization_id(row)
        .map_err(|source| GainSolveError::OpenMeasurementSet {
            path: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            source,
        })?;
    pol.corr_type(usize::try_from(pol_id).unwrap_or(usize::MAX))
        .map_err(|source| GainSolveError::OpenMeasurementSet {
            path: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            source,
        })
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SolveGroupOptions {
    pub(crate) gain_type: GainType,
    pub(crate) model_source: GainSolveModelSource,
    pub(crate) stokes_i: f32,
    pub(crate) solve_interval: GainSolveInterval,
    pub(crate) combine: GainSolveCombine,
    /// Maximum time from the first sample in an average of one baseline.
    pub(crate) preaverage_max_span: Option<f64>,
    /// Solve each data channel apart (`bandpass`) rather than the selected
    /// channels together (`gaincal`).
    pub(crate) channelized: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct SelectedSolveRow {
    pub(crate) row_index: usize,
    pub(crate) field_id: i32,
    pub(crate) observation_id: i32,
    pub(crate) data_desc_id: i32,
    pub(crate) data_spw_id: i32,
    pub(crate) channel_selection: Option<ChannelSelection>,
    pub(crate) antenna1: i32,
    pub(crate) antenna2: i32,
    pub(crate) time_seconds: f64,
    pub(crate) time_centroid_seconds: f64,
    pub(crate) interval_seconds: f64,
    pub(crate) scan_number: i32,
    pub(crate) flag_row: bool,
}

/// What the accumulation reads of a sample's row: its identities, times and
/// scan, from a MAIN row or from any other source of samples.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SolveSampleRow {
    pub(crate) field_id: i32,
    pub(crate) data_desc_id: i32,
    pub(crate) antenna1: i32,
    pub(crate) antenna2: i32,
    pub(crate) time_seconds: f64,
    pub(crate) time_centroid_seconds: f64,
    pub(crate) interval_seconds: f64,
    pub(crate) scan_number: i32,
}

impl SolveSampleRow {
    fn of(row: &SelectedSolveRow) -> Self {
        Self {
            field_id: row.field_id,
            data_desc_id: row.data_desc_id,
            antenna1: row.antenna1,
            antenna2: row.antenna2,
            time_seconds: row.time_seconds,
            time_centroid_seconds: row.time_centroid_seconds,
            interval_seconds: row.interval_seconds,
            scan_number: row.scan_number,
        }
    }
}

/// One sample's arrays for [`SolveAccumulator::observe_selected_samples`], each
/// `[correlation, channel]` as casacore stores a MAIN cell: the data, the
/// flags the solve uses (row flags already folded in), the solve weights and
/// the model, if any.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SolveSamples<'a> {
    pub(crate) data: ArrayView2<'a, Complex32>,
    pub(crate) flags: ArrayView2<'a, bool>,
    pub(crate) weights: ArrayView2<'a, f32>,
    pub(crate) model: Option<ArrayView2<'a, Complex32>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SolveBaseKey {
    pub(crate) field_id: i32,
    pub(crate) spw_id: i32,
    pub(crate) observation_id: i32,
    pub(crate) scan_number: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SolveBucketKey {
    Infinite,
    Integration { time_bits: u64, interval_bits: u64 },
    Seconds(u64),
}

/// The samples of one solution interval, collapsed per baseline,
/// correlation and solution channel, and the interval's metadata.
#[derive(Debug, Clone)]
pub(crate) struct SolveAccumulator {
    pub(crate) field_id: i32,
    pub(crate) spw_id: i32,
    pub(crate) observation_id: i32,
    pub(crate) min_time: f64,
    pub(crate) max_time: f64,
    pub(crate) unique_time_sum: f64,
    pub(crate) unique_time_count: usize,
    pub(crate) total_interval: f64,
    pub(crate) sample_rows: usize,
    pub(crate) field_ids: BTreeSet<i32>,
    pub(crate) scan_numbers: BTreeSet<i32>,
    pub(crate) antenna_ids: BTreeSet<i32>,
    unique_time_bits: BTreeSet<u64>,
    /// Data channels per solution: 1 when the selected channels solve
    /// together, else the row's channel count.
    pub(crate) solution_channels: usize,
    /// `[solution channel][receptor]`, filled by [`Self::finalize_for_solve`].
    pub(crate) channel_graphs: Vec<Vec<SolveGraph>>,
    /// The solution's TIME, set by [`Self::finalize_for_solve`].
    pub(crate) solution_time: f64,
    /// Whether a sample entered the solve: CASA attempts a solution for
    /// such an interval (`SDBList::Ok`, positive unflagged weight after the
    /// model division), and sets its window's gain-table channel then,
    /// whether or not the solve succeeds.
    pub(crate) attempted: bool,
    baseline_centroids: HashMap<AveragedBaseline, WeightedTime>,
    preaverage_starts: HashMap<(i32, i32), f64>,
    receptor_accumulations: Vec<HashMap<SolveAccumulationKey, Vec<SolveEdgeAccumulation>>>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SolveEdgeStats {
    pub(crate) weighted_sample_power: f64,
    pub(crate) raw_weighted_sample_power: f64,
    pub(crate) sample_count: usize,
    pub(crate) collapsed_count: usize,
}

impl SolveEdgeStats {
    pub(crate) fn add(&mut self, other: Self) {
        self.weighted_sample_power += other.weighted_sample_power;
        self.raw_weighted_sample_power += other.raw_weighted_sample_power;
        self.sample_count += other.sample_count;
        self.collapsed_count += other.collapsed_count;
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct SolveEdgeAccumulation {
    weighted_sum: Complex32,
    weight: f32,
    weighted_sample_power: f64,
    sample_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SolveAccumulationKey {
    baseline: AveragedBaseline,
    correlation_index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct AveragedBaseline {
    antennas: (i32, i32),
    start_bits: u64,
}

/// A weighted mean of times.
#[derive(Debug, Clone, Copy, Default)]
struct WeightedTime {
    weighted_sum: f64,
    weight: f64,
}

/// A data description's row-invariant layout; shape/selection changes refresh
/// the plan, so variable-shaped cells keep the same validation.
struct SolveRowLayout {
    correlations: Vec<i32>,
    channel_count: usize,
    selection: Option<ChannelSelection>,
    channels: Vec<usize>,
    all_channels: bool,
}

/// One MAIN row's checked two-dimensional solve arrays. Scalar SIGMA weights
/// have one channel, broadcast to the data's channel count without copying.
struct SolveRowInputs<'a> {
    data: &'a ArrayD<Complex32>,
    flags: Cow<'a, ArrayD<bool>>,
    weights: ArrayD<f32>,
    model: Option<&'a ArrayD<Complex32>>,
}

impl SolveRowInputs<'_> {
    /// The arrays as [`SolveSamples`] views.
    fn samples(&self) -> SolveSamples<'_> {
        SolveSamples {
            data: two_dimensional(self.data),
            flags: two_dimensional(&self.flags),
            weights: self
                .weights
                .broadcast(self.data.raw_dim())
                .expect("checked receptor/channel weight shape")
                .into_dimensionality::<Ix2>()
                .expect("checked two-dimensional"),
            model: self.model.map(two_dimensional),
        }
    }
}

/// A MAIN cell checked two-dimensional, as a view.
fn two_dimensional<T>(cell: &ArrayD<T>) -> ArrayView2<'_, T> {
    cell.view()
        .into_dimensionality::<Ix2>()
        .expect("checked two-dimensional")
}

impl SolveAccumulator {
    pub(crate) fn new(field_id: i32, spw_id: i32, observation_id: i32) -> Self {
        Self {
            field_id,
            spw_id,
            observation_id,
            min_time: f64::INFINITY,
            max_time: f64::NEG_INFINITY,
            unique_time_sum: 0.0,
            unique_time_count: 0,
            total_interval: 0.0,
            sample_rows: 0,
            field_ids: BTreeSet::new(),
            scan_numbers: BTreeSet::new(),
            antenna_ids: BTreeSet::new(),
            unique_time_bits: BTreeSet::new(),
            solution_channels: 1,
            channel_graphs: Vec::new(),
            solution_time: 0.0,
            attempted: false,
            baseline_centroids: HashMap::new(),
            preaverage_starts: HashMap::new(),
            receptor_accumulations: Vec::new(),
        }
    }

    /// Add one MAIN row: its unflagged, positively weighted samples divided by
    /// the model, each weighted by its solve weight times |model|² as
    /// `VisEquation::divideCorrByModel` does, and its TIME_CENTROID.
    fn observe(
        &mut self,
        ms: &MeasurementSet,
        row: &SelectedSolveRow,
        preapplied_row: Option<&EvaluatedApplyRow>,
        options: &SolveGroupOptions,
        layouts_by_ddid: &mut HashMap<i32, SolveRowLayout>,
        inputs: (&SolveInputBatch, usize),
    ) -> Result<(), GainSolveError> {
        let inputs = read_row_inputs(ms, row, preapplied_row, options.model_source, inputs)?;
        let channel_count = inputs.data.shape()[1];
        let layout = match layouts_by_ddid.entry(row.data_desc_id) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => entry.insert(SolveRowLayout {
                correlations: correlation_types_for_ddid(ms, row.data_desc_id)?,
                channel_count: 0,
                selection: None,
                channels: vec![],
                all_channels: false,
            }),
        };
        if layout.channel_count != channel_count || layout.selection != row.channel_selection {
            layout.channels = selected_channels(row.channel_selection.as_ref(), channel_count)?;
            layout.all_channels = layout.channels.iter().copied().eq(0..channel_count);
            layout.channel_count = channel_count;
            layout.selection.clone_from(&row.channel_selection);
        }
        self.observe_selected_samples(
            &SolveSampleRow::of(row),
            &inputs.samples(),
            &layout.correlations,
            &layout.channels,
            options,
            layout.all_channels,
        )
    }

    /// Accumulate one sample's `channels` (every channel for a channelized
    /// solve): the arithmetic of [`Self::observe`] on arrays from any source.
    /// The MSv2/MSv4 trial (`solve::batch_trial`) feeds it
    /// from `casa_observation::VisibilityBatch` views without copying them.
    #[cfg(test)]
    pub(crate) fn observe_samples(
        &mut self,
        row: &SolveSampleRow,
        samples: &SolveSamples<'_>,
        correlation_types: &[i32],
        channels: &[usize],
        options: &SolveGroupOptions,
    ) -> Result<(), GainSolveError> {
        self.observe_selected_samples(
            row,
            samples,
            correlation_types,
            channels,
            options,
            channels.iter().copied().eq(0..samples.data.ncols()),
        )
    }

    fn observe_selected_samples(
        &mut self,
        row: &SolveSampleRow,
        samples: &SolveSamples<'_>,
        correlation_types: &[i32],
        channels: &[usize],
        options: &SolveGroupOptions,
        all_channels: bool,
    ) -> Result<(), GainSolveError> {
        if options.channelized {
            let channel_count = channels.len();
            if self.sample_rows > 0 && self.solution_channels != channel_count {
                return Err(GainSolveError::UnsupportedParameterShape {
                    path: "<measurement-set DATA>".to_string(),
                    shape: samples.data.shape().to_vec(),
                });
            }
            self.solution_channels = channel_count;
        }

        let graph_count = match options.gain_type {
            GainType::G => 2,
            GainType::T => 1,
        };
        if self.receptor_accumulations.len() < graph_count {
            self.receptor_accumulations
                .resize_with(graph_count, HashMap::new);
        }
        self.observe_metadata(row);
        let flags = samples::channel_flags(samples);
        let no_weighted_data =
            flags.iter().all(|flag| *flag) || samples.weights.iter().all(|weight| *weight <= 0.0);
        let antennas = (row.antenna1, row.antenna2);
        let start_bits = options.preaverage_max_span.map_or(0, |max_span| {
            let start = self
                .preaverage_starts
                .entry(antennas)
                .or_insert(row.time_seconds);
            if row.time_seconds - *start > max_span {
                *start = row.time_seconds;
            }
            start.to_bits()
        });
        let baseline = AveragedBaseline {
            antennas,
            start_bits,
        };
        let solution_channels = self.solution_channels;
        let mut row_weight = 0.0_f64;
        for (corr_index, &correlation_type) in correlation_types.iter().enumerate() {
            let Some(receptors) = correlation_receptors(correlation_type) else {
                return Err(GainSolveError::UnsupportedCorrelationLayout {
                    data_desc_id: row.data_desc_id,
                    correlation_types: correlation_types
                        .iter()
                        .map(|code| stokes_name(*code).to_string())
                        .collect(),
                });
            };
            if no_weighted_data || receptors.0 != receptors.1 {
                continue;
            }
            let graph_index = match options.gain_type {
                GainType::G => receptors.0,
                GainType::T => 0,
            };
            // Look the baseline/correlation up once per row; channels are
            // dense indices, not independent hash keys for every sample.
            let accumulations = self.receptor_accumulations[graph_index]
                .entry(SolveAccumulationKey {
                    baseline,
                    correlation_index: corr_index,
                })
                .or_insert_with(|| vec![SolveEdgeAccumulation::default(); solution_channels]);
            row_weight += samples::SampleAccumulation {
                values: accumulations,
                attempted: &mut self.attempted,
                channelized: options.channelized,
            }
            .observe(
                samples,
                corr_index,
                &flags,
                channels,
                options.stokes_i,
                all_channels,
            );
        }
        if row_weight > 0.0 {
            let centroid = self.baseline_centroids.entry(baseline).or_default();
            centroid.weighted_sum += row_weight * row.time_centroid_seconds;
            centroid.weight += row_weight;
        }
        Ok(())
    }

    fn observe_metadata(&mut self, row: &SolveSampleRow) {
        self.min_time = self.min_time.min(row.time_seconds);
        self.max_time = self.max_time.max(row.time_seconds);
        if self.unique_time_bits.insert(row.time_seconds.to_bits()) {
            self.unique_time_sum += row.time_seconds;
            self.unique_time_count += 1;
        }
        self.total_interval += row.interval_seconds;
        self.sample_rows += 1;
        self.field_ids.insert(row.field_id);
        self.scan_numbers.insert(row.scan_number);
        self.antenna_ids.insert(row.antenna1);
        self.antenna_ids.insert(row.antenna2);
    }

    /// Collapse each baseline's samples per correlation and solution channel
    /// into the solve graphs, and set the solution TIME.
    ///
    /// A phase-only solve sees each collapsed sample at unit amplitude with
    /// its weight times its squared amplitude. An amplitude-only solve
    /// keeps the collapsed magnitude and the original weight
    /// (`SolveDataBuffer::enforceAPonData`). TIME is
    /// `SDBList::aggregateTimeCentroid`: each baseline's TIME_CENTROID,
    /// averaged over its rows by row weight as time averaging does, weighted
    /// by the baseline's solve weight after that phase-only scaling.
    pub(crate) fn finalize_for_solve(&mut self, solve_mode: GainSolveMode) {
        self.channel_graphs = vec![
            vec![SolveGraph::default(); self.receptor_accumulations.len()];
            self.solution_channels
        ];
        let mut baseline_weights = HashMap::<AveragedBaseline, f64>::new();
        for (receptor, accumulations) in self.receptor_accumulations.iter().enumerate() {
            for (key, channels) in accumulations {
                for (channel_index, accumulation) in channels.iter().enumerate() {
                    if accumulation.weight <= f32::EPSILON {
                        continue;
                    }
                    let averaged =
                        accumulation.weighted_sum / Complex32::new(accumulation.weight, 0.0);
                    let (sample, weight) = match solve_mode {
                        GainSolveMode::Phase => {
                            let amplitude = averaged.norm();
                            if amplitude <= f32::EPSILON {
                                continue;
                            }
                            (
                                averaged / Complex32::new(amplitude, 0.0),
                                accumulation.weight * amplitude * amplitude,
                            )
                        }
                        GainSolveMode::Amplitude => {
                            (Complex32::new(averaged.norm(), 0.0), accumulation.weight)
                        }
                        GainSolveMode::AmplitudePhase => (averaged, accumulation.weight),
                    };
                    self.channel_graphs[channel_index][receptor].accumulate(
                        key.baseline.antennas,
                        weight,
                        sample * Complex32::new(weight, 0.0),
                        SolveEdgeStats {
                            weighted_sample_power: f64::from(weight) * f64::from(sample.norm_sqr()),
                            raw_weighted_sample_power: accumulation.weighted_sample_power,
                            sample_count: accumulation.sample_count,
                            collapsed_count: 1,
                        },
                    );
                    *baseline_weights.entry(key.baseline).or_default() += f64::from(weight);
                }
            }
        }

        let mut solution_time = WeightedTime::default();
        for (baseline, weight) in baseline_weights {
            let Some(centroid) = self.baseline_centroids.get(&baseline) else {
                continue;
            };
            if centroid.weight > 0.0 {
                solution_time.weighted_sum += weight * centroid.weighted_sum / centroid.weight;
                solution_time.weight += weight;
            }
        }
        self.solution_time = if solution_time.weight > 0.0 {
            solution_time.weighted_sum / solution_time.weight
        } else {
            self.aggregate_time()
        };
    }

    /// Add `other`'s accumulation, of the same group, to this one before
    /// [`Self::finalize_for_solve`], as if its samples had been observed
    /// here: the fragment work units of the MSv2/MSv4 trial accumulate apart
    /// and merge before the group closes. Sums change order, so results
    /// agree to rounding, not bitwise.
    #[cfg(test)]
    pub(crate) fn merge(&mut self, other: Self) {
        self.min_time = self.min_time.min(other.min_time);
        self.max_time = self.max_time.max(other.max_time);
        for bits in other.unique_time_bits {
            if self.unique_time_bits.insert(bits) {
                self.unique_time_sum += f64::from_bits(bits);
                self.unique_time_count += 1;
            }
        }
        self.total_interval += other.total_interval;
        self.sample_rows += other.sample_rows;
        self.field_ids.extend(other.field_ids);
        self.scan_numbers.extend(other.scan_numbers);
        self.antenna_ids.extend(other.antenna_ids);
        self.solution_channels = self.solution_channels.max(other.solution_channels);
        self.attempted |= other.attempted;
        for (baseline, centroid) in other.baseline_centroids {
            let mine = self.baseline_centroids.entry(baseline).or_default();
            mine.weighted_sum += centroid.weighted_sum;
            mine.weight += centroid.weight;
        }
        if self.receptor_accumulations.len() < other.receptor_accumulations.len() {
            self.receptor_accumulations
                .resize_with(other.receptor_accumulations.len(), HashMap::new);
        }
        for (mine, theirs) in self
            .receptor_accumulations
            .iter_mut()
            .zip(other.receptor_accumulations)
        {
            for (key, channels) in theirs {
                let entries = mine
                    .entry(key)
                    .or_insert_with(|| vec![SolveEdgeAccumulation::default(); channels.len()]);
                for (entry, accumulation) in entries.iter_mut().zip(channels) {
                    entry.weighted_sum += accumulation.weighted_sum;
                    entry.weight += accumulation.weight;
                    entry.weighted_sample_power += accumulation.weighted_sample_power;
                    entry.sample_count += accumulation.sample_count;
                }
            }
        }
    }

    /// Entries the accumulation's sets and maps hold: its state grows with
    /// the group's distinct times, antennas, baselines and baseline
    /// correlations, not with its samples.
    #[cfg(test)]
    pub(crate) fn state_entries(&self) -> usize {
        self.unique_time_bits.len()
            + self.scan_numbers.len()
            + self.antenna_ids.len()
            + self.baseline_centroids.len()
            + self
                .receptor_accumulations
                .iter()
                .flat_map(HashMap::values)
                .map(Vec::len)
                .sum::<usize>()
    }

    /// The mean of the interval's distinct TIMEs (`SDBList::aggregateTime`,
    /// CASA's TIME when no sample carries weight).
    pub(crate) fn aggregate_time(&self) -> f64 {
        if self.unique_time_count > 0 {
            self.unique_time_sum / self.unique_time_count as f64
        } else {
            (self.min_time + self.max_time) / 2.0
        }
    }
}

/// Read one MAIN row's data, flags, model and solve weights, from the row or
/// from its pre-applied calibration.
///
/// The weights are CASA's (`CalSolvingVi2::calibrateCurrentVB`, which calls
/// `VisBufferImpl2::resetWeightsUsingSigma`): 1/σ² from SIGMA_SPECTRUM when
/// the MeasurementSet has WEIGHT_SPECTRUM and SIGMA_SPECTRUM, otherwise from
/// SIGMA broadcast over channels; σ ≤ 0 gives no weight. WEIGHT and
/// WEIGHT_SPECTRUM are not read. Pre-applied `calwt` tables then scale them.
fn read_row_inputs<'a>(
    ms: &'a MeasurementSet,
    row: &SelectedSolveRow,
    preapplied_row: Option<&'a EvaluatedApplyRow>,
    model_source: GainSolveModelSource,
    inputs: (&'a SolveInputBatch, usize),
) -> Result<SolveRowInputs<'a>, GainSolveError> {
    let (data, flags) = match preapplied_row {
        Some(preapplied) => (&preapplied.corrected_data, &preapplied.flags),
        None => (
            input_cell(ms, row.row_index, inputs, "DATA")?,
            input_cell(ms, row.row_index, inputs, "FLAG")?,
        ),
    };
    let ArrayValue::Complex32(data) = data else {
        return Err(unsupported_shape("DATA", data));
    };
    let ArrayValue::Bool(flags) = flags else {
        return Err(unsupported_shape("FLAG", flags));
    };
    if data.ndim() != 2 || data.shape() != flags.shape() {
        return Err(GainSolveError::UnsupportedParameterShape {
            path: "<measurement-set row>".to_string(),
            shape: data.shape().to_vec(),
        });
    }
    let model = match model_source {
        GainSolveModelSource::ModelColumn => {
            let model = input_cell(ms, row.row_index, inputs, "MODEL_DATA")?;
            match model {
                ArrayValue::Complex32(model) if model.shape() == data.shape() => Some(model),
                other => return Err(unsupported_shape("MODEL_DATA", other)),
            }
        }
        GainSolveModelSource::PointSource => None,
    };
    let mut weights = solve_weights(ms, row.row_index, data.shape(), inputs)?;
    if let Some(factors) = preapplied_row.and_then(|row| row.weight_factors.as_ref()) {
        if factors.shape() != data.shape() {
            return Err(GainSolveError::UnsupportedParameterShape {
                path: "<pre-applied weight factors>".to_string(),
                shape: factors.shape().to_vec(),
            });
        }
        weights = &weights
            .broadcast(data.raw_dim())
            .expect("checked weight shape")
            * factors;
    }
    Ok(SolveRowInputs {
        data,
        flags: solve_flags(row, flags),
        weights,
        model,
    })
}

fn solve_weights(
    ms: &MeasurementSet,
    row_index: usize,
    shape: &[usize],
    inputs: (&SolveInputBatch, usize),
) -> Result<ArrayD<f32>, GainSolveError> {
    let has_column = |name| {
        ms.main_table()
            .schema()
            .is_some_and(|schema| schema.contains_column(name))
    };
    let inverse_square = |sigma: f32| {
        if sigma > 0.0 {
            1.0 / (sigma * sigma)
        } else {
            0.0
        }
    };
    if has_column("WEIGHT_SPECTRUM") && has_column("SIGMA_SPECTRUM") {
        let sigma = input_cell(ms, row_index, inputs, "SIGMA_SPECTRUM")?;
        return match sigma {
            ArrayValue::Float32(sigma) if sigma.shape() == shape => Ok(sigma.mapv(inverse_square)),
            other => Err(unsupported_shape("SIGMA_SPECTRUM", other)),
        };
    }
    let sigma = input_cell(ms, row_index, inputs, "SIGMA")?;
    let ArrayValue::Float32(sigma) = sigma else {
        return Err(unsupported_shape("SIGMA", sigma));
    };
    if sigma.ndim() != 1 || sigma.shape()[0] != shape[0] {
        return Err(GainSolveError::UnsupportedParameterShape {
            path: "<measurement-set SIGMA>".to_string(),
            shape: sigma.shape().to_vec(),
        });
    }
    Ok(sigma.mapv(inverse_square).insert_axis(Axis(1)))
}

fn main_cell<'a>(
    ms: &'a MeasurementSet,
    row_index: usize,
    column: &str,
) -> Result<&'a ArrayValue, GainSolveError> {
    ms.main_table()
        .cell_accessor(row_index, column)
        .and_then(|cell| cell.array())
        .map_err(|source| ms_read_error(ms, source))
}

fn unsupported_shape(column: &str, value: &ArrayValue) -> GainSolveError {
    GainSolveError::UnsupportedParameterShape {
        path: format!("<measurement-set {column}>"),
        shape: value.shape().to_vec(),
    }
}

/// The median, the mean of the middle two for an even count, as casacore's
/// `partialMedians(..., takeEvenMean=True)` that CASA uses for a row's
/// channel-independent weight.
pub(super) fn median_with_even_mean(values: &mut [f32]) -> f32 {
    if values.is_empty() {
        return 0.0;
    }
    let middle = values.len() / 2;
    let even = values.len().is_multiple_of(2);
    let first = values[0];
    if values
        .iter()
        .all(|value| value.to_bits() == first.to_bits())
    {
        return if even { (first + first) / 2.0 } else { first };
    }
    let (lower, upper, _) = values.select_nth_unstable_by(middle, f32::total_cmp);
    let upper = *upper;
    if even {
        let lower = lower
            .iter()
            .copied()
            .max_by(f32::total_cmp)
            .expect("nonempty even lower half");
        (lower + upper) / 2.0
    } else {
        upper
    }
}

/// The channels of a window of `channel_count` channels that a solve
/// uses: those `selection` names, else all. The gain table's
/// `SPECTRAL_WINDOW` row collapses the same channels.
pub(crate) fn selected_channels(
    selection: Option<&ChannelSelection>,
    channel_count: usize,
) -> Result<Vec<usize>, GainSolveError> {
    match selection {
        Some(selection) => {
            selection
                .indices(channel_count)
                .map_err(|source| GainSolveError::OpenMeasurementSet {
                    path: "<measurement-set channel selection>".to_string(),
                    source,
                })
        }
        None => Ok((0..channel_count).collect()),
    }
}

/// The sample flags a solve uses for `row`: its `FLAG` cell, or every sample
/// flagged when the row's `FLAG_ROW` is set, whatever `FLAG` holds. CASA's
/// solve does the same: `CalSolvingVi2::calibrateCurrentVB` calls
/// `VisEquation::collapse2`, whose `divideCorrByModel` flags the whole cube
/// of such a row and zeroes its weights before G, T or B accumulates it.
pub(crate) fn solve_flags<'a>(
    row: &SelectedSolveRow,
    flags: &'a ArrayD<bool>,
) -> Cow<'a, ArrayD<bool>> {
    if row.flag_row {
        Cow::Owned(ArrayD::from_elem(flags.raw_dim(), true))
    } else {
        Cow::Borrowed(flags)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accumulator(receptors: usize) -> SolveAccumulator {
        let mut group = SolveAccumulator::new(0, 0, 0);
        group
            .receptor_accumulations
            .resize_with(receptors, HashMap::new);
        group
    }

    fn add(group: &mut SolveAccumulator, correlation_index: usize, sample: Complex32) {
        let accumulation = group.receptor_accumulations[0]
            .entry(SolveAccumulationKey {
                baseline: unaveraged_baseline((0, 1)),
                correlation_index,
            })
            .or_insert_with(|| vec![SolveEdgeAccumulation::default()]);
        let accumulation = &mut accumulation[0];
        accumulation.weighted_sum += sample;
        accumulation.weight += 1.0;
        accumulation.weighted_sample_power += f64::from(sample.norm_sqr());
        accumulation.sample_count += 1;
    }

    fn unaveraged_baseline(antennas: (i32, i32)) -> AveragedBaseline {
        AveragedBaseline {
            antennas,
            start_bits: 0,
        }
    }

    #[test]
    fn finalize_for_phase_solve_collapses_before_unit_amplitude_normalization() {
        let mut group = accumulator(1);
        add(&mut group, 0, Complex32::new(2.0, 0.0));
        add(&mut group, 0, Complex32::new(0.0, 2.0));

        group.finalize_for_solve(GainSolveMode::Phase);

        let graph = &group.channel_graphs[0][0];
        assert!((graph.weights[&(0, 1)] - 4.0).abs() < 1.0e-6);
        let edge = graph.edges[&(0, 1)];
        let expected = 4.0_f32 / 2.0_f32.sqrt();
        assert!((edge.re - expected).abs() < 1.0e-6);
        assert!((edge.im - expected).abs() < 1.0e-6);
        assert_eq!(graph.stats[&(0, 1)].sample_count, 2);
    }

    #[test]
    fn finalize_for_phase_solve_keeps_correlations_separate_for_scalar_t() {
        let mut group = accumulator(1);
        add(&mut group, 0, Complex32::new(2.0, 0.0));
        add(&mut group, 3, Complex32::new(0.0, 2.0));

        group.finalize_for_solve(GainSolveMode::Phase);

        let graph = &group.channel_graphs[0][0];
        assert!((graph.weights[&(0, 1)] - 8.0).abs() < 1.0e-6);
        let edge = graph.edges[&(0, 1)];
        assert!((edge.re - 4.0).abs() < 1.0e-6);
        assert!((edge.im - 4.0).abs() < 1.0e-6);
        assert_eq!(graph.stats[&(0, 1)].sample_count, 2);
    }

    #[test]
    fn solution_time_weights_baseline_centroids_by_solve_weight() {
        // Baseline 0-1 (rows at 10 s and 12 s) carries three times the
        // weight of 0-2 (one row at 20 s).
        let mut group = accumulator(1);
        for (baseline, weight) in [((0, 1), 3.0_f32), ((0, 2), 1.0)] {
            group.receptor_accumulations[0].insert(
                SolveAccumulationKey {
                    baseline: unaveraged_baseline(baseline),
                    correlation_index: 0,
                },
                vec![SolveEdgeAccumulation {
                    weighted_sum: Complex32::new(weight, 0.0),
                    weight,
                    weighted_sample_power: f64::from(weight),
                    sample_count: 1,
                }],
            );
        }
        group.baseline_centroids.insert(
            unaveraged_baseline((0, 1)),
            WeightedTime {
                weighted_sum: 10.0 + 12.0,
                weight: 2.0,
            },
        );
        group.baseline_centroids.insert(
            unaveraged_baseline((0, 2)),
            WeightedTime {
                weighted_sum: 20.0,
                weight: 1.0,
            },
        );

        group.finalize_for_solve(GainSolveMode::AmplitudePhase);

        assert!((group.solution_time - (3.0 * 11.0 + 20.0) / 4.0).abs() < 1.0e-12);
    }

    /// A cross-correlation row of field `field_id`, scan `scan_number` and
    /// data description `data_desc_id` (on window 0) at `time_seconds`.
    fn row(
        row_index: usize,
        field_id: i32,
        scan_number: i32,
        data_desc_id: i32,
        time_seconds: f64,
    ) -> SelectedSolveRow {
        SelectedSolveRow {
            row_index,
            field_id,
            observation_id: 0,
            data_desc_id,
            data_spw_id: 0,
            channel_selection: None,
            antenna1: 0,
            antenna2: 1,
            time_seconds,
            time_centroid_seconds: time_seconds,
            interval_seconds: 1.0,
            scan_number,
            flag_row: false,
        }
    }

    #[test]
    fn combined_intervals_start_at_the_earliest_row_whatever_the_field_order() {
        // Field 1 is observed first, in scan 1; field 0 thirty seconds
        // later, in scan 2. Sorting by field first would start the 20 s
        // intervals at field 0's scan and fold field 1's into its first.
        let rows = [130.0, 131.0, 132.0]
            .map(|time| (0, 2, time))
            .into_iter()
            .chain([100.0, 101.0, 102.0].map(|time| (1, 1, time)))
            .enumerate()
            .map(|(index, (field, scan, time))| row(index, field, scan, 0, time))
            .collect::<Vec<_>>();
        let buckets = |combine| {
            bucketed_rows(&rows, GainSolveInterval::Seconds(20.0), combine)
                .into_iter()
                .map(|(row, base, bucket)| (row.time_seconds, base.field_id, bucket))
                .collect::<Vec<_>>()
        };
        let combined = buckets(GainSolveCombine {
            scans: true,
            fields: true,
        });
        let times = combined.iter().map(|(time, ..)| *time).collect::<Vec<_>>();
        assert_eq!(times, [100.0, 101.0, 102.0, 130.0, 131.0, 132.0]);
        let intervals = combined
            .iter()
            .map(|(.., bucket)| *bucket)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            intervals,
            BTreeSet::from([SolveBucketKey::Seconds(0), SolveBucketKey::Seconds(1)])
        );
        // Kept apart, each field's intervals start at its own first row.
        for (time, field, bucket) in buckets(GainSolveCombine::default()) {
            assert_eq!(bucket, SolveBucketKey::Seconds(0), "{time} {field}");
        }
    }

    #[test]
    fn data_descriptions_sharing_a_window_are_refused() {
        // Two descriptions on window 0, as a duplicated description or one
        // holding the cross hands would give.
        let rows = [row(0, 0, 1, 0, 100.0), row(1, 0, 1, 1, 100.0)];
        let error = refuse_shared_spectral_windows(&rows).unwrap_err();
        assert!(
            matches!(
                &error,
                GainSolveError::SharedSpectralWindow { spw_id: 0, data_desc_ids }
                    if data_desc_ids == &[0, 1]
            ),
            "{error}"
        );
        assert!(error.to_string().contains("#979"), "{error}");
        assert!(refuse_shared_spectral_windows(&rows[..1]).is_ok());
    }

    #[test]
    fn median_with_even_mean_matches_casacore_partial_medians() {
        assert_eq!(median_with_even_mean(&mut []), 0.0);
        assert_eq!(median_with_even_mean(&mut [3.0, 0.0, 1.0]), 1.0);
        assert_eq!(median_with_even_mean(&mut [4.0, 0.0, 4.0, 4.0]), 4.0);
        assert_eq!(median_with_even_mean(&mut [2.0, 0.0]), 1.0);
    }
}
