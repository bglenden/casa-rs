// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selection, bucketing, and accumulation for limited `gaincal`.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use casa_ms::MsError;
use casa_ms::ms::MeasurementSet;
use casa_ms::selection::MsSelection;
use casa_types::{ArrayValue, Complex32};

use super::{
    GainSolveCombine, GainSolveError, GainSolveInterval, GainSolveRequest, GainType,
    RefAntSelector, correlation_receptors, get_f64, get_i32, stokes_name,
};
use crate::execute::{EvaluatedApplyRow, evaluate_apply_rows};
use crate::plan::{ApplyPlanRequest, plan_apply};
use crate::solve::kernel::accumulate_edge;

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
    request: &GainSolveRequest,
) -> Result<Option<HashMap<usize, EvaluatedApplyRow>>, GainSolveError> {
    if request.prior_calibration_tables.is_empty() && !request.parang {
        return Ok(None);
    }
    let plan = plan_apply(
        ms,
        &ApplyPlanRequest {
            selection: request.selection.clone(),
            apply_mode: crate::ApplyMode::CalFlag,
            parang: request.parang,
            calibration_tables: request.prior_calibration_tables.clone(),
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
    gain_type: GainType,
    stokes_i: f32,
    solve_interval: GainSolveInterval,
    combine: GainSolveCombine,
) -> Result<BTreeMap<(SolveBaseKey, SolveBucketKey), SolveAccumulator>, GainSolveError> {
    let mut sorted_rows = rows.to_vec();
    sorted_rows.sort_by_key(|row| {
        (
            row.field_id,
            row.data_spw_id,
            row.observation_id,
            row.scan_number,
            row.time_seconds.to_bits(),
            row.interval_seconds.to_bits(),
            row.row_index,
        )
    });

    let mut groups = BTreeMap::<(SolveBaseKey, SolveBucketKey), SolveAccumulator>::new();
    let mut bucket_starts = BTreeMap::<SolveBaseKey, f64>::new();

    for row in sorted_rows {
        if row.antenna1 == row.antenna2 {
            continue;
        }
        let base_key = SolveBaseKey {
            field_id: if combine.fields { 0 } else { row.field_id },
            spw_id: row.data_spw_id,
            observation_id: row.observation_id,
            scan_number: if combine.scans { 0 } else { row.scan_number },
        };
        let bucket_key = match solve_interval {
            GainSolveInterval::Infinite => SolveBucketKey::Infinite,
            GainSolveInterval::Integration => SolveBucketKey::Integration {
                time_bits: row.time_seconds.to_bits(),
                interval_bits: row.interval_seconds.to_bits(),
            },
            GainSolveInterval::Seconds(seconds) => {
                let bucket_start = bucket_starts.entry(base_key).or_insert(row.time_seconds);
                let bucket_index = ((row.time_seconds - *bucket_start) / seconds)
                    .floor()
                    .max(0.0);
                SolveBucketKey::Seconds(bucket_index as u64)
            }
        };

        let group = groups.entry((base_key, bucket_key)).or_insert_with(|| {
            SolveAccumulator::new(row.field_id, row.data_spw_id, row.observation_id)
        });
        let preapplied = preapplied_rows.and_then(|rows| rows.get(&row.row_index));
        group.observe(ms, &row, preapplied, gain_type, stokes_i)?;
    }

    Ok(groups)
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
    let selected_rows =
        selection
            .apply(ms)
            .map_err(|source| GainSolveError::OpenMeasurementSet {
                path: ms
                    .path()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "<in-memory>".to_string()),
                source,
            })?;
    if selected_rows.is_empty() {
        return Err(GainSolveError::EmptySelection);
    }

    let dd = ms
        .data_description()
        .map_err(|source| GainSolveError::OpenMeasurementSet {
            path: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            source,
        })?;

    selected_rows
        .into_iter()
        .map(|row_index| {
            let data_desc_id = get_i32(ms.main_table(), row_index, "DATA_DESC_ID")?;
            let data_desc_row = usize::try_from(data_desc_id).map_err(|_| {
                GainSolveError::UnsupportedParameterShape {
                    path: "<measurement-set DATA_DESC_ID>".to_string(),
                    shape: vec![row_index],
                }
            })?;
            let data_spw_id = dd.spectral_window_id(data_desc_row).map_err(|source| {
                GainSolveError::OpenMeasurementSet {
                    path: ms
                        .path()
                        .map(|path| path.display().to_string())
                        .unwrap_or_else(|| "<in-memory>".to_string()),
                    source,
                }
            })?;
            Ok(SelectedSolveRow {
                row_index,
                field_id: get_i32(ms.main_table(), row_index, "FIELD_ID")?,
                observation_id: get_i32(ms.main_table(), row_index, "OBSERVATION_ID")?,
                data_desc_id,
                data_spw_id,
                antenna1: get_i32(ms.main_table(), row_index, "ANTENNA1")?,
                antenna2: get_i32(ms.main_table(), row_index, "ANTENNA2")?,
                time_seconds: get_f64(ms.main_table(), row_index, "TIME")?,
                interval_seconds: get_f64(ms.main_table(), row_index, "INTERVAL")?,
                scan_number: get_i32(ms.main_table(), row_index, "SCAN_NUMBER")?,
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

#[derive(Debug, Clone)]
pub(crate) struct SelectedSolveRow {
    pub(crate) row_index: usize,
    pub(crate) field_id: i32,
    pub(crate) observation_id: i32,
    pub(crate) data_desc_id: i32,
    pub(crate) data_spw_id: i32,
    pub(crate) antenna1: i32,
    pub(crate) antenna2: i32,
    pub(crate) time_seconds: f64,
    pub(crate) interval_seconds: f64,
    pub(crate) scan_number: i32,
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

#[derive(Debug, Clone)]
pub(crate) struct SolveAccumulator {
    pub(crate) field_id: i32,
    pub(crate) spw_id: i32,
    pub(crate) observation_id: i32,
    pub(crate) min_time: f64,
    pub(crate) max_time: f64,
    pub(crate) total_interval: f64,
    pub(crate) sample_rows: usize,
    pub(crate) scan_numbers: BTreeSet<i32>,
    pub(crate) antenna_ids: BTreeSet<i32>,
    pub(crate) receptor_graphs: Vec<HashMap<(i32, i32), Complex32>>,
    pub(crate) receptor_weights: Vec<HashMap<(i32, i32), f32>>,
}

impl SolveAccumulator {
    pub(crate) fn new(field_id: i32, spw_id: i32, observation_id: i32) -> Self {
        Self {
            field_id,
            spw_id,
            observation_id,
            min_time: f64::INFINITY,
            max_time: f64::NEG_INFINITY,
            total_interval: 0.0,
            sample_rows: 0,
            scan_numbers: BTreeSet::new(),
            antenna_ids: BTreeSet::new(),
            receptor_graphs: Vec::new(),
            receptor_weights: Vec::new(),
        }
    }

    pub(crate) fn observe(
        &mut self,
        ms: &MeasurementSet,
        row: &SelectedSolveRow,
        preapplied_row: Option<&EvaluatedApplyRow>,
        gain_type: GainType,
        stokes_i: f32,
    ) -> Result<(), GainSolveError> {
        let (data, flags) = match preapplied_row {
            Some(row) => (&row.corrected_data, &row.flags),
            None => {
                let data = ms
                    .main_table()
                    .get_array_cell(row.row_index, "DATA")
                    .map_err(|source| GainSolveError::OpenMeasurementSet {
                        path: ms
                            .path()
                            .map(|path| path.display().to_string())
                            .unwrap_or_else(|| "<in-memory>".to_string()),
                        source: MsError::from(source),
                    })?;
                let flags = ms
                    .main_table()
                    .get_array_cell(row.row_index, "FLAG")
                    .map_err(|source| GainSolveError::OpenMeasurementSet {
                        path: ms
                            .path()
                            .map(|path| path.display().to_string())
                            .unwrap_or_else(|| "<in-memory>".to_string()),
                        source: MsError::from(source),
                    })?;
                (data, flags)
            }
        };
        let weights = ms
            .main_table()
            .get_array_cell(row.row_index, "WEIGHT")
            .map_err(|source| GainSolveError::OpenMeasurementSet {
                path: ms
                    .path()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "<in-memory>".to_string()),
                source: MsError::from(source),
            })?;
        let correlation_types = correlation_types_for_ddid(ms, row.data_desc_id)?;

        let ArrayValue::Complex32(data) = data else {
            return Err(GainSolveError::UnsupportedParameterShape {
                path: "<measurement-set DATA>".to_string(),
                shape: data.shape().to_vec(),
            });
        };
        let ArrayValue::Bool(flags) = flags else {
            return Err(GainSolveError::UnsupportedParameterShape {
                path: "<measurement-set FLAG>".to_string(),
                shape: flags.shape().to_vec(),
            });
        };
        let ArrayValue::Float32(weights) = weights else {
            return Err(GainSolveError::UnsupportedParameterShape {
                path: "<measurement-set WEIGHT>".to_string(),
                shape: weights.shape().to_vec(),
            });
        };
        if data.ndim() != 2 || flags.ndim() != 2 || data.shape() != flags.shape() {
            return Err(GainSolveError::UnsupportedParameterShape {
                path: "<measurement-set row>".to_string(),
                shape: data.shape().to_vec(),
            });
        }
        if weights.ndim() != 1 || weights.shape()[0] != data.shape()[0] {
            return Err(GainSolveError::UnsupportedParameterShape {
                path: "<measurement-set WEIGHT>".to_string(),
                shape: weights.shape().to_vec(),
            });
        }

        let graph_count = match gain_type {
            GainType::G => 2,
            GainType::T => 1,
        };
        if self.receptor_graphs.len() < graph_count {
            self.receptor_graphs.resize_with(graph_count, HashMap::new);
            self.receptor_weights.resize_with(graph_count, HashMap::new);
        }

        self.min_time = self.min_time.min(row.time_seconds);
        self.max_time = self.max_time.max(row.time_seconds);
        self.total_interval += row.interval_seconds;
        self.sample_rows += 1;
        self.scan_numbers.insert(row.scan_number);
        self.antenna_ids.insert(row.antenna1);
        self.antenna_ids.insert(row.antenna2);

        for corr_index in 0..data.shape()[0] {
            let Some(receptors) = correlation_receptors(correlation_types[corr_index]) else {
                return Err(GainSolveError::UnsupportedCorrelationLayout {
                    data_desc_id: row.data_desc_id,
                    correlation_types: correlation_types
                        .iter()
                        .map(|code| stokes_name(*code).to_string())
                        .collect(),
                });
            };
            if receptors.0 != receptors.1 {
                continue;
            }
            let graph_index = match gain_type {
                GainType::G => receptors.0,
                GainType::T => 0,
            };
            let weight = weights[[corr_index]];
            if weight <= 0.0 {
                continue;
            }
            for chan_index in 0..data.shape()[1] {
                if flags[[corr_index, chan_index]] {
                    continue;
                }
                let sample = data[[corr_index, chan_index]] / Complex32::new(stokes_i, 0.0);
                if sample.norm() <= f32::EPSILON {
                    continue;
                }
                accumulate_edge(
                    &mut self.receptor_graphs[graph_index],
                    &mut self.receptor_weights[graph_index],
                    row.antenna1,
                    row.antenna2,
                    weight,
                    sample * Complex32::new(weight, 0.0),
                );
            }
        }

        Ok(())
    }
}
