// SPDX-License-Identifier: LGPL-3.0-or-later
//! Fit and persist legacy polynomial bandpass tables.

mod fit;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use casa_core::{ArrayValue, Complex32, RecordField, RecordValue, ScalarValue, Value};
use casa_ms::ms::MeasurementSet;
use casa_ms::schema::SubtableId;
use casa_numerics::solve_weighted_least_squares;
use casa_tables::{ColumnSchema, Table, TableError, TableOptions, TableSchema};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::{
    BPolySolutionRow, BandpassSolutionRow, BandpassSolveCuts, BandpassSolveError,
    BandpassSolveReport, BandpassSolveRequest, empty_report,
};
use crate::constants::{
    COL_ANTENNA1, COL_ARRAY_ID, COL_CAL_DESC_ID, COL_FIELD_ID, COL_FLAG, COL_INTERVAL,
    COL_N_POLY_AMP, COL_N_POLY_PHASE, COL_OBSERVATION_ID, COL_PARAMERR, COL_PHASE_UNITS,
    COL_POLY_COEFF_AMP, COL_POLY_COEFF_PHASE, COL_SCALE_FACTOR, COL_SCAN_NUMBER, COL_SNR, COL_TIME,
    COL_TIME_EXTRA_PREC, COL_VALID_DOMAIN, COL_WEIGHT, LEGACY_CAL_DESC_KEYWORD,
};
use crate::selected_data::SelectedData;
use crate::session::CalibrationSolveContext;
use crate::solve::grouping::SolveAccumulator;
use crate::solve_stats::{IntervalStages, SolveStats, SolveStatsBuilder, ToolSolveActivity};
use crate::writer::{
    CalibrationTableDescriptor, CalibrationTableWriter, set_fixed_unit_keyword,
    set_measinfo_keyword,
};

const LEGACY_CAL_HISTORY_KEYWORD: &str = "CAL_HISTORY";

pub(super) fn modern_gather(
    ms: &MeasurementSet,
    request: &BandpassSolveRequest,
    context: CalibrationSolveContext,
    groups: impl Iterator<Item = SolveAccumulator>,
    activity: &mut ToolSolveActivity,
) -> Result<BandpassSolveReport, BandpassSolveError> {
    let mut stats = SolveStatsBuilder::new(
        2,
        crate::solve::spectral_window_ids(ms)?,
        &context.available_antennas,
    );
    for group in groups {
        if group.gather_attempted() {
            return Err(BandpassSolveError::UnsupportedConfiguration {
                reason: "the modern visibility iterator cannot fit BPOLY".into(),
            });
        }
        stats.interval(&IntervalStages::of(&group, 0));
    }
    stats.track_activity(Some(activity));
    Ok(empty_report(
        request,
        context.refant_id,
        context.selected_data,
        stats.finish(),
    ))
}

pub(super) fn solve_polynomial(
    ms: &MeasurementSet,
    request: &BandpassSolveRequest,
    context: CalibrationSolveContext,
    groups: impl Iterator<Item = SolveAccumulator>,
    bin_widths: &BTreeMap<i32, usize>,
    mut activity: Option<&mut ToolSolveActivity>,
    mode: crate::CalibrationOutputMode,
) -> Result<BandpassSolveReport, BandpassSolveError> {
    let cuts = request
        .cuts
        .expect("polynomial solver with configured cuts");
    let window = ms
        .spectral_window()
        .map_err(|source| BandpassSolveError::OpenMeasurementSet {
            path: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_default(),
            source,
        })?;
    let descriptions = context
        .rows
        .iter()
        .map(|row| (row.data_desc_id, row))
        .collect::<BTreeMap<_, _>>();
    let mut frequencies_by_description = BTreeMap::new();
    for (id, row) in descriptions {
        let spw = row.data_spw_id;
        let frequencies = window.chan_freq(spw as usize).map_err(|source| {
            BandpassSolveError::OpenMeasurementSet {
                path: ms
                    .path()
                    .map(|path| path.display().to_string())
                    .unwrap_or_default(),
                source,
            }
        })?;
        let (channels, _) = crate::solve::grouping::polynomial::channel_selection(
            row.channel_selection.as_ref(),
            frequencies.len(),
        )?;
        let spacing = frequencies
            .windows(2)
            .next()
            .map_or(1.0, |pair| (pair[1] - pair[0]).abs());
        frequencies_by_description.insert(
            id,
            (
                super::metadata::average_frequencies(
                    &frequencies,
                    &channels,
                    bin_widths.get(&spw).copied().unwrap_or(1),
                ),
                spacing,
            ),
        );
    }
    let combined_window = request.combine.spectral_windows.then(|| {
        context
            .rows
            .iter()
            .map(|row| row.data_spw_id)
            .min()
            .expect("selected windows")
    });
    if combined_window.is_some()
        && frequencies_by_description
            .values()
            .map(|(channels, _)| channels.len())
            .min()
            != frequencies_by_description
                .values()
                .map(|(channels, _)| channels.len())
                .max()
    {
        if activity.is_some() {
            return Err(BandpassSolveError::UnsupportedConfiguration {
                reason:
                    "spectral windows with different selected channel counts cannot be combined"
                        .into(),
            });
        }
        tracing::warn!(
            "Spectral windows with different selected channel counts cannot be combined"
        );
        return Ok(empty_report(
            request,
            context.refant_id,
            context.selected_data,
            SolveStats::default(),
        ));
    }
    if let Some(activity) = activity.as_deref_mut() {
        activity.initialize(window.row_count());
    }
    let mut channel_count = 0;
    let mut fitted = Vec::new();
    for group in groups {
        let Some(polynomial) = &group.polynomial else {
            continue;
        };
        let frequencies = polynomial
            .descriptions()
            .iter()
            .map(|id| {
                let (frequencies, spacing) = &frequencies_by_description[id];
                (*id, (frequencies.as_slice(), *spacing))
            })
            .collect::<BTreeMap<_, _>>();
        let output_window = combined_window.unwrap_or(group.spw_id);
        if frequencies.is_empty() {
            if let Some(activity) = activity.as_deref_mut() {
                activity.count_mapped_interval(group.spw_id, output_window, false, false);
            }
            continue;
        }
        let mut rows = match fit::fit_interval(
            &group,
            &frequencies,
            &context.available_antennas,
            context.refant_id,
            request,
            cuts,
        ) {
            Ok(rows) => rows,
            Err(error @ BandpassSolveError::FitPolynomial { .. }) if activity.is_none() => {
                tracing::warn!("{error}");
                break;
            }
            Err(error) => return Err(error),
        };
        if let Some(activity) = activity.as_deref_mut() {
            activity.count_mapped_interval(group.spw_id, output_window, true, !rows.is_empty());
        }
        if !rows.is_empty() {
            if let Some(spw) = combined_window {
                for row in &mut rows {
                    row.spw_id = spw;
                }
            }
            channel_count = frequencies
                .values()
                .map(|(channels, _)| channels.len())
                .max()
                .unwrap_or(0);
            fitted.extend(rows);
        }
    }
    if fitted.is_empty() {
        return Ok(empty_report(
            request,
            context.refant_id,
            context.selected_data,
            SolveStats::default(),
        ));
    }
    let report =
        write_polynomial_rows(ms, request, context.refant_id, &fitted, channel_count, mode)?;
    Ok(BandpassSolveReport {
        selected_data: context.selected_data,
        ..report
    })
}

pub(super) fn write_bpoly_caltable(
    ms: &MeasurementSet,
    request: &BandpassSolveRequest,
    refant_id: i32,
    rows: &[BandpassSolutionRow],
    mode: crate::CalibrationOutputMode,
) -> Result<BandpassSolveReport, BandpassSolveError> {
    let fitted_rows = fit_bpoly_rows(
        ms,
        rows,
        request.amplitude_degree,
        request.phase_degree,
        request.cuts,
        &request.selection,
    )?;
    let channel_count = rows.first().map(|row| row.channel_count).unwrap_or(0);
    write_polynomial_rows(ms, request, refant_id, &fitted_rows, channel_count, mode)
}

#[expect(
    clippy::too_many_lines,
    reason = "predates the function-length limit; #705"
)]
fn write_polynomial_rows(
    ms: &MeasurementSet,
    request: &BandpassSolveRequest,
    refant_id: i32,
    fitted_rows: &[BPolySolutionRow],
    channel_count: usize,
    mode: crate::CalibrationOutputMode,
) -> Result<BandpassSolveReport, BandpassSolveError> {
    // Existing casa-rs BPOLY tables used rank-one placeholder arrays and
    // two extra diagnostic columns. Keep that layout when appending to one.
    let casa_layout = mode != crate::CalibrationOutputMode::Append
        || !request.output_table.exists()
        || !Table::open(TableOptions::new(&request.output_table))
            .map_err(|source| BandpassSolveError::SaveCalibrationTable {
                path: request.output_table.display().to_string(),
                source: Box::new(source),
            })?
            .schema()
            .is_some_and(|schema| schema.contains_column(COL_PARAMERR));
    let columns = legacy_polynomial_columns(casa_layout);
    let schema = TableSchema::new(columns).expect("valid BPOLY schema");
    let mut writer = CalibrationTableWriter::with_mode(
        ms,
        CalibrationTableDescriptor {
            output: &request.output_table,
            schema,
            subtype: "BPOLY",
            parameter_type: None,
            measurement_set_name: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            include_polarization_basis: false,
            time_extra_precision_column: Some(COL_TIME_EXTRA_PREC),
        },
        mode,
    )?;
    {
        let table = writer.table_mut();
        table
            .keywords_mut()
            .upsert(LEGACY_CAL_DESC_KEYWORD, Value::table_ref("././CAL_DESC"));
        table.keywords_mut().upsert(
            LEGACY_CAL_HISTORY_KEYWORD,
            Value::table_ref("././CAL_HISTORY"),
        );
        set_fixed_unit_keyword(table, COL_VALID_DOMAIN, &["Hz"]);
        set_fixed_unit_keyword(table, "REF_FREQUENCY", &["Hz"]);
        set_measinfo_keyword(
            table,
            "REF_FREQUENCY",
            "frequency",
            Some(if casa_layout { "LSRK" } else { "TOPO" }),
        );
        set_fixed_unit_keyword(table, "REF_DIRECTION", &["rad", "rad"]);
        set_measinfo_keyword(table, "REF_DIRECTION", "direction", Some("J2000"));
    }

    let cal_desc_ids = fitted_rows
        .iter()
        .map(|row| row.spw_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .enumerate()
        .map(|(cal_desc_id, spw_id)| {
            (
                spw_id,
                i32::try_from(cal_desc_id).expect("small BPOLY CAL_DESC id set"),
            )
        })
        .collect::<BTreeMap<_, _>>();

    let window_names = ms
        .spectral_window()
        .ok()
        .map(|window| {
            cal_desc_ids
                .keys()
                .map(|&id| (id, window.freq_group_name(id as usize).unwrap_or_default()))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let unit = Complex32::new(1.0, 0.0);
    let (gain_shape, reference_shape, direction_shape, diagnostic_shape): (
        &[usize],
        &[usize],
        &[usize],
        &[usize],
    ) = if casa_layout {
        (&[1, 1, 1, 1], &[1, 1, 1], &[2, 1, 1, 1], &[1, 1, 1, 1])
    } else {
        (&[1], &[1], &[2], &[2])
    };
    for row in fitted_rows {
        let from_casa_task = request.cuts.is_some();
        let status_weight = if casa_layout { 0.0 } else { 1.0 };
        let unset = if casa_layout { -1 } else { 0 };
        let reference_frequency_hz = (row.valid_domain_hz[0] + row.valid_domain_hz[1]) / 2.0;
        let flat_amp = row
            .amp_coefficients
            .iter()
            .flat_map(|coefficients| coefficients.iter().copied())
            .collect::<Vec<_>>();
        let flat_phase = row
            .phase_coefficients
            .iter()
            .flat_map(|coefficients| coefficients.iter().copied())
            .collect::<Vec<_>>();
        let amp_shape = IxDyn(&[1, 1, 1, flat_amp.len()]).f();
        let phase_shape = IxDyn(&[1, 1, 1, flat_phase.len()]).f();
        let mut fields = vec![
            RecordField::new(
                COL_TIME,
                Value::Scalar(ScalarValue::Float64(row.time_seconds)),
            ),
            RecordField::new(
                COL_TIME_EXTRA_PREC,
                Value::Scalar(ScalarValue::Float64(0.0)),
            ),
            RecordField::new(
                COL_FIELD_ID,
                Value::Scalar(ScalarValue::Int32(row.field_id)),
            ),
            RecordField::new(
                COL_ANTENNA1,
                Value::Scalar(ScalarValue::Int32(row.antenna_id)),
            ),
            RecordField::new("FEED1", Value::Scalar(ScalarValue::Int32(unset))),
            RecordField::new(
                COL_INTERVAL,
                Value::Scalar(ScalarValue::Float64(row.interval_seconds)),
            ),
            RecordField::new(
                COL_SCAN_NUMBER,
                Value::Scalar(ScalarValue::Int32(row.scan_number)),
            ),
            RecordField::new(
                COL_OBSERVATION_ID,
                Value::Scalar(ScalarValue::Int32(row.observation_id)),
            ),
            RecordField::new(COL_ARRAY_ID, Value::Scalar(ScalarValue::Int32(unset))),
            RecordField::new("PROCESSOR_ID", Value::Scalar(ScalarValue::Int32(-1))),
            RecordField::new("STATE_ID", Value::Scalar(ScalarValue::Int32(-1))),
            RecordField::new("PHASE_ID", Value::Scalar(ScalarValue::Int32(-1))),
            RecordField::new("PULSAR_BIN", Value::Scalar(ScalarValue::Int32(-1))),
            RecordField::new("PULSAR_GATE_ID", Value::Scalar(ScalarValue::Int32(-1))),
            RecordField::new("FREQ_GROUP", Value::Scalar(ScalarValue::Int32(unset))),
            RecordField::new(
                "FREQ_GROUP_NAME",
                Value::Scalar(ScalarValue::String(
                    window_names.get(&row.spw_id).cloned().unwrap_or_default(),
                )),
            ),
            RecordField::new(
                "FIELD_NAME",
                Value::Scalar(ScalarValue::String(String::new())),
            ),
            RecordField::new(
                "FIELD_CODE",
                Value::Scalar(ScalarValue::String(String::new())),
            ),
            RecordField::new(
                "SOURCE_NAME",
                Value::Scalar(ScalarValue::String(String::new())),
            ),
            RecordField::new(
                "SOURCE_CODE",
                Value::Scalar(ScalarValue::String(String::new())),
            ),
            RecordField::new(
                "CALIBRATION_GROUP",
                Value::Scalar(ScalarValue::Int32(unset)),
            ),
            RecordField::new(
                COL_CAL_DESC_ID,
                Value::Scalar(ScalarValue::Int32(
                    *cal_desc_ids
                        .get(&row.spw_id)
                        .expect("BPOLY CAL_DESC entry for solved SPW"),
                )),
            ),
            RecordField::new("CAL_HISTORY_ID", Value::Scalar(ScalarValue::Int32(unset))),
            RecordField::new(
                "GAIN",
                Value::Array(ArrayValue::Complex32(ArrayD::from_elem(
                    IxDyn(gain_shape).f(),
                    if casa_layout {
                        Complex32::default()
                    } else {
                        unit
                    },
                ))),
            ),
            RecordField::new(
                "SIDEBAND_REF",
                Value::Scalar(ScalarValue::Complex32(Complex32::new(1.0, 0.0))),
            ),
            RecordField::new(
                "REF_ANT",
                Value::Array(ArrayValue::Int32(ArrayD::from_elem(
                    IxDyn(reference_shape).f(),
                    if from_casa_task || casa_layout {
                        refant_id
                    } else {
                        row.antenna_id
                    },
                ))),
            ),
            RecordField::new(
                "REF_FEED",
                Value::Array(ArrayValue::Int32(ArrayD::zeros(IxDyn(reference_shape).f()))),
            ),
            RecordField::new(
                "REF_RECEPTOR",
                Value::Array(ArrayValue::Int32(ArrayD::zeros(IxDyn(reference_shape).f()))),
            ),
            RecordField::new(
                "REF_FREQUENCY",
                Value::Array(ArrayValue::Float64(ArrayD::from_elem(
                    IxDyn(reference_shape).f(),
                    reference_frequency_hz,
                ))),
            ),
            RecordField::new(
                "MEAS_FREQ_REF",
                Value::Scalar(ScalarValue::Int32(i32::from(casa_layout))),
            ),
            RecordField::new(
                "REF_DIRECTION",
                Value::Array(ArrayValue::Float64(ArrayD::zeros(
                    IxDyn(direction_shape).f(),
                ))),
            ),
            RecordField::new("MEAS_DIR_REF", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new(
                "TOTAL_SOLUTION_OK",
                Value::Scalar(ScalarValue::Bool(!casa_layout)),
            ),
            RecordField::new("TOTAL_FIT", Value::Scalar(ScalarValue::Float32(0.0))),
            RecordField::new(
                "TOTAL_FIT_WEIGHT",
                Value::Scalar(ScalarValue::Float32(status_weight)),
            ),
            RecordField::new(
                "SOLUTION_OK",
                Value::Array(ArrayValue::Bool(ArrayD::from_elem(
                    IxDyn(diagnostic_shape).f(),
                    !casa_layout,
                ))),
            ),
            RecordField::new(
                "FIT",
                Value::Array(ArrayValue::Float32(ArrayD::zeros(
                    IxDyn(diagnostic_shape).f(),
                ))),
            ),
            RecordField::new(
                "FIT_WEIGHT",
                Value::Array(ArrayValue::Float32(ArrayD::from_elem(
                    IxDyn(diagnostic_shape).f(),
                    status_weight,
                ))),
            ),
            RecordField::new(
                COL_FLAG,
                Value::Array(ArrayValue::Bool(ArrayD::from_elem(
                    IxDyn(diagnostic_shape).f(),
                    casa_layout,
                ))),
            ),
            RecordField::new(
                COL_PARAMERR,
                Value::Array(ArrayValue::from_f32_vec(vec![0.0, 0.0])),
            ),
            RecordField::new(
                COL_SNR,
                Value::Array(ArrayValue::Float32(ArrayD::from_elem(
                    IxDyn(diagnostic_shape).f(),
                    status_weight,
                ))),
            ),
            RecordField::new(
                COL_WEIGHT,
                Value::Array(ArrayValue::from_f32_vec(vec![1.0, 1.0])),
            ),
            RecordField::new(
                "POLY_TYPE",
                Value::Scalar(ScalarValue::String("CHEBYSHEV".to_string())),
            ),
            RecordField::new(
                "POLY_MODE",
                Value::Scalar(ScalarValue::String("A&P".to_string())),
            ),
            RecordField::new(
                COL_SCALE_FACTOR,
                Value::Scalar(ScalarValue::Complex32(row.scale_factor)),
            ),
            RecordField::new(
                COL_N_POLY_AMP,
                Value::Scalar(ScalarValue::Int32(
                    i32::try_from(row.amp_coefficients.first().map_or(0, Vec::len))
                        .expect("small BPOLY amp coefficient count"),
                )),
            ),
            RecordField::new(
                COL_N_POLY_PHASE,
                Value::Scalar(ScalarValue::Int32(
                    i32::try_from(row.phase_coefficients.first().map_or(0, Vec::len))
                        .expect("small BPOLY phase coefficient count"),
                )),
            ),
            RecordField::new(
                COL_PHASE_UNITS,
                Value::Scalar(ScalarValue::String(
                    if from_casa_task || casa_layout {
                        "RADIANS"
                    } else {
                        "RAD"
                    }
                    .to_string(),
                )),
            ),
            RecordField::new(
                COL_VALID_DOMAIN,
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(
                        IxDyn(&[2]).f(),
                        vec![row.valid_domain_hz[0], row.valid_domain_hz[1]],
                    )
                    .expect("BPOLY valid domain should reshape"),
                )),
            ),
            RecordField::new(
                COL_POLY_COEFF_AMP,
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(amp_shape, flat_amp)
                        .expect("BPOLY amp coefficients should reshape"),
                )),
            ),
            RecordField::new(
                COL_POLY_COEFF_PHASE,
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(phase_shape, flat_phase)
                        .expect("BPOLY phase coefficients should reshape"),
                )),
            ),
        ];
        if casa_layout {
            fields.retain(|field| !matches!(field.name.as_str(), COL_PARAMERR | COL_WEIGHT));
        }
        writer.append(RecordValue::new(fields))?;
    }

    writer.copy_subtables(&[
        (SubtableId::Observation, "OBSERVATION"),
        (SubtableId::Antenna, "ANTENNA"),
        (SubtableId::Field, "FIELD"),
        (SubtableId::History, "HISTORY"),
        (SubtableId::SpectralWindow, "SPECTRAL_WINDOW"),
    ])?;

    let descriptions = writer.output_path().join("CAL_DESC");
    write_legacy_cal_desc(
        descriptions.clone(),
        &cal_desc_ids,
        |spw| {
            fitted_rows
                .iter()
                .find(|row| row.spw_id == spw)
                .map_or(1, |row| row.amp_coefficients.len() as i32)
        },
        casa_layout,
        ms.path()
            .and_then(|path| path.file_name())
            .and_then(|name| name.to_str())
            .unwrap_or_default(),
    )
    .map_err(|source| BandpassSolveError::SaveCalibrationTable {
        path: descriptions.display().to_string(),
        source: Box::new(source),
    })?;
    let history = writer.output_path().join("CAL_HISTORY");
    write_legacy_history(&history).map_err(|source| BandpassSolveError::SaveCalibrationTable {
        path: history.display().to_string(),
        source: Box::new(source),
    })?;
    writer.save_main()?;

    let field_ids = fitted_rows
        .iter()
        .map(|row| row.field_id)
        .collect::<BTreeSet<_>>();
    let spw_ids = fitted_rows
        .iter()
        .map(|row| row.spw_id)
        .collect::<BTreeSet<_>>();
    Ok(BandpassSolveReport {
        output_table: request.output_table.clone(),
        table_subtype: "BPOLY".to_string(),
        refant_antenna_id: refant_id,
        field_ids: field_ids.iter().copied().collect(),
        spectral_window_ids: spw_ids.iter().copied().collect(),
        solution_row_count: fitted_rows.len(),
        channel_count,
        selected_data: SelectedData::default(),
        selection_failed: false,
        solve_stats: SolveStats::default(),
    })
}

pub(crate) fn legacy_polynomial_columns(casa_layout: bool) -> Vec<ColumnSchema> {
    let gain_rank = Some(if casa_layout { 4 } else { 1 });
    let reference_rank = Some(if casa_layout { 3 } else { 1 });
    let direction_rank = Some(if casa_layout { 4 } else { 1 });
    let mut columns = vec![
        ColumnSchema::scalar(COL_TIME, casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar(COL_TIME_EXTRA_PREC, casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar(COL_FIELD_ID, casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ANTENNA1, casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("FEED1", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_INTERVAL, casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar(COL_SCAN_NUMBER, casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_OBSERVATION_ID, casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ARRAY_ID, casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("PROCESSOR_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("STATE_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("PHASE_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("PULSAR_BIN", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("PULSAR_GATE_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("FREQ_GROUP", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("FREQ_GROUP_NAME", casa_core::PrimitiveType::String),
        ColumnSchema::scalar("FIELD_NAME", casa_core::PrimitiveType::String),
        ColumnSchema::scalar("FIELD_CODE", casa_core::PrimitiveType::String),
        ColumnSchema::scalar("SOURCE_NAME", casa_core::PrimitiveType::String),
        ColumnSchema::scalar("SOURCE_CODE", casa_core::PrimitiveType::String),
        ColumnSchema::scalar("CALIBRATION_GROUP", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_CAL_DESC_ID, casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("CAL_HISTORY_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::array_variable("GAIN", casa_core::PrimitiveType::Complex32, gain_rank),
        ColumnSchema::scalar("SIDEBAND_REF", casa_core::PrimitiveType::Complex32),
        ColumnSchema::array_variable("REF_ANT", casa_core::PrimitiveType::Int32, reference_rank),
        ColumnSchema::array_variable("REF_FEED", casa_core::PrimitiveType::Int32, reference_rank),
        ColumnSchema::array_variable(
            "REF_RECEPTOR",
            casa_core::PrimitiveType::Int32,
            reference_rank,
        ),
        ColumnSchema::array_variable(
            "REF_FREQUENCY",
            casa_core::PrimitiveType::Float64,
            reference_rank,
        ),
        ColumnSchema::scalar("MEAS_FREQ_REF", casa_core::PrimitiveType::Int32),
        ColumnSchema::array_variable(
            "REF_DIRECTION",
            casa_core::PrimitiveType::Float64,
            direction_rank,
        ),
        ColumnSchema::scalar("MEAS_DIR_REF", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("TOTAL_SOLUTION_OK", casa_core::PrimitiveType::Bool),
        ColumnSchema::scalar("TOTAL_FIT", casa_core::PrimitiveType::Float32),
        ColumnSchema::scalar("TOTAL_FIT_WEIGHT", casa_core::PrimitiveType::Float32),
        ColumnSchema::array_variable("SOLUTION_OK", casa_core::PrimitiveType::Bool, gain_rank),
        ColumnSchema::array_variable("FIT", casa_core::PrimitiveType::Float32, gain_rank),
        ColumnSchema::array_variable("FIT_WEIGHT", casa_core::PrimitiveType::Float32, gain_rank),
        ColumnSchema::array_variable(COL_FLAG, casa_core::PrimitiveType::Bool, gain_rank),
        ColumnSchema::array_variable(COL_PARAMERR, casa_core::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable(COL_SNR, casa_core::PrimitiveType::Float32, gain_rank),
        ColumnSchema::array_variable(COL_WEIGHT, casa_core::PrimitiveType::Float32, Some(1)),
        ColumnSchema::scalar("POLY_TYPE", casa_core::PrimitiveType::String),
        ColumnSchema::scalar("POLY_MODE", casa_core::PrimitiveType::String),
        ColumnSchema::scalar(COL_SCALE_FACTOR, casa_core::PrimitiveType::Complex32),
        ColumnSchema::scalar(COL_N_POLY_AMP, casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_N_POLY_PHASE, casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_PHASE_UNITS, casa_core::PrimitiveType::String),
        ColumnSchema::array_variable(COL_VALID_DOMAIN, casa_core::PrimitiveType::Float64, Some(1)),
        ColumnSchema::array_variable(
            COL_POLY_COEFF_AMP,
            casa_core::PrimitiveType::Float64,
            Some(4),
        ),
        ColumnSchema::array_variable(
            COL_POLY_COEFF_PHASE,
            casa_core::PrimitiveType::Float64,
            Some(4),
        ),
    ];
    if casa_layout {
        columns.retain(|column| !matches!(column.name(), COL_PARAMERR | COL_WEIGHT));
    }
    columns
}

fn fit_bpoly_rows(
    ms: &MeasurementSet,
    rows: &[BandpassSolutionRow],
    amplitude_degree: usize,
    phase_degree: usize,
    cuts: Option<BandpassSolveCuts>,
    selection: &casa_ms::MsSelection,
) -> Result<Vec<BPolySolutionRow>, BandpassSolveError> {
    let spectral_window =
        ms.spectral_window()
            .map_err(|source| BandpassSolveError::OpenMeasurementSet {
                path: ms
                    .path()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "<in-memory>".to_string()),
                source,
            })?;
    rows.iter()
        .map(|row| {
            let spw_row = usize::try_from(row.spw_id).map_err(|_| {
                BandpassSolveError::UnsupportedParameterShape {
                    path: "SPECTRAL_WINDOW row id".to_string(),
                    shape: vec![row.spw_id as usize],
                }
            })?;
            let channel_frequencies_hz = spectral_window.chan_freq(spw_row).map_err(|source| {
                BandpassSolveError::OpenMeasurementSet {
                    path: ms
                        .path()
                        .map(|path| path.display().to_string())
                        .unwrap_or_else(|| "<in-memory>".to_string()),
                    source,
                }
            })?;
            let selected = selection.channel_selection_for_spw(row.spw_id);
            let channel_frequencies_hz = crate::solve::grouping::selected_channels(
                selected.as_ref(),
                channel_frequencies_hz.len(),
            )?
            .into_iter()
            .map(|channel| channel_frequencies_hz[channel])
            .collect::<Vec<_>>();
            fit_bpoly_row(
                row,
                &channel_frequencies_hz,
                [amplitude_degree, phase_degree],
                cuts,
            )
        })
        .collect()
}

/// `row`'s channel solutions fitted with `BPOLY`'s polynomials over the
/// window, of `[amplitude, phase]` degrees: each receptor's log amplitudes
/// and unwrapped phases at its unflagged channels, leaving out the edges
/// `cuts` masks. A receptor with no such channel keeps a unit gain.
pub(super) fn fit_bpoly_row(
    row: &BandpassSolutionRow,
    channel_frequencies_hz: &[f64],
    degrees: [usize; 2],
    cuts: Option<BandpassSolveCuts>,
) -> Result<BPolySolutionRow, BandpassSolveError> {
    if channel_frequencies_hz.len() != row.channel_count || row.channel_count == 0 {
        return Err(BandpassSolveError::UnsupportedParameterShape {
            path: "BPOLY fit channel grid".to_string(),
            shape: vec![channel_frequencies_hz.len(), row.channel_count],
        });
    }
    let valid_domain_hz = [
        channel_frequencies_hz.first().copied().unwrap_or(0.0),
        channel_frequencies_hz.last().copied().unwrap_or(0.0),
    ];
    // Both receptors take as many coefficients, as the table holds one count.
    let [amplitude_count, phase_count] =
        degrees.map(|degree| degree.saturating_add(1).min(row.channel_count));
    let receptor_count = 2;
    let mut amp_coefficients = Vec::with_capacity(receptor_count);
    let mut phase_coefficients = Vec::with_capacity(receptor_count);
    for receptor in 0..receptor_count {
        let mut frequencies_hz = Vec::with_capacity(row.channel_count);
        let mut log_amplitudes = Vec::with_capacity(row.channel_count);
        let mut phases = Vec::with_capacity(row.channel_count);
        for (channel, frequency_hz) in channel_frequencies_hz.iter().copied().enumerate() {
            let index = receptor + receptor_count * channel;
            let masked =
                cuts.is_some_and(|cuts| cuts.masks_edge_channel(channel, row.channel_count));
            if row.flags[index] || masked {
                continue;
            }
            let gain = row.gains[index];
            frequencies_hz.push(frequency_hz);
            log_amplitudes.push(gain.norm().max(f32::EPSILON).ln() as f64);
            phases.push((gain.im as f64).atan2(gain.re as f64));
        }
        let unwrapped_phases = unwrap_phases(&phases);
        amp_coefficients.push(fit_legacy_bpoly_coefficients(
            &frequencies_hz,
            &log_amplitudes,
            valid_domain_hz,
            amplitude_count,
            row,
        )?);
        phase_coefficients.push(fit_legacy_bpoly_coefficients(
            &frequencies_hz,
            &unwrapped_phases,
            valid_domain_hz,
            phase_count,
            row,
        )?);
    }
    Ok(BPolySolutionRow {
        time_seconds: row.time_seconds,
        interval_seconds: row.interval_seconds,
        field_id: row.field_id,
        spw_id: row.spw_id,
        antenna_id: row.antenna_id,
        valid_domain_hz,
        scan_number: row.scan_number,
        observation_id: row.observation_id,
        scale_factor: Complex32::new(1.0, 0.0),
        amp_coefficients,
        phase_coefficients,
    })
}

/// `coefficient_count` coefficients of the polynomial through `values` at
/// `frequencies_hz`. Fewer points than coefficients fit as many
/// coefficients as there are points, and the rest are zero.
fn fit_legacy_bpoly_coefficients(
    frequencies_hz: &[f64],
    values: &[f64],
    valid_domain_hz: [f64; 2],
    coefficient_count: usize,
    row: &BandpassSolutionRow,
) -> Result<Vec<f64>, BandpassSolveError> {
    let mut coefficients = vec![0.0_f64; coefficient_count];
    let fitted = coefficient_count.min(frequencies_hz.len());
    if fitted == 0 {
        return Ok(coefficients);
    }

    let mut basis = vec![0.0_f64; fitted];
    let mut rows = Vec::with_capacity(frequencies_hz.len());
    for (frequency_hz, value) in frequencies_hz.iter().copied().zip(values.iter().copied()) {
        legacy_bpoly_basis(
            valid_domain_hz[0],
            valid_domain_hz[1],
            frequency_hz,
            &mut basis,
        );
        rows.push((basis.clone(), value, 1.0));
    }
    let solved = solve_weighted_least_squares(&rows, fitted).ok_or_else(|| {
        BandpassSolveError::FitPolynomial {
            antenna_id: row.antenna_id,
            field_id: row.field_id,
            spw_id: row.spw_id,
            reason: "least-squares system was singular".to_string(),
        }
    })?;
    coefficients[..fitted].copy_from_slice(&solved);
    Ok(coefficients)
}

fn legacy_bpoly_basis(x_start: f64, x_end: f64, x: f64, basis: &mut [f64]) {
    if basis.is_empty() {
        return;
    }
    basis.fill(0.0);
    basis[0] = 0.5_f64;
    if basis.len() == 1 || (x_end - x_start).abs() <= f64::EPSILON {
        return;
    }
    let xcap = ((x - x_start) - (x_end - x)) / (x_end - x_start);
    let mut t_prev = 1.0_f64;
    let mut t_curr = xcap;
    basis[1] = t_curr;
    for slot in basis.iter_mut().skip(2) {
        let t_next = 2.0_f64 * xcap * t_curr - t_prev;
        *slot = t_next;
        t_prev = t_curr;
        t_curr = t_next;
    }
}

fn unwrap_phases(phases: &[f64]) -> Vec<f64> {
    let mut unwrapped = Vec::with_capacity(phases.len());
    let mut offset = 0.0_f64;
    let mut previous = None;
    for phase in phases.iter().copied() {
        if let Some(previous_phase) = previous {
            let delta = phase - previous_phase;
            if delta > std::f64::consts::PI {
                offset -= 2.0_f64 * std::f64::consts::PI;
            } else if delta < -std::f64::consts::PI {
                offset += 2.0_f64 * std::f64::consts::PI;
            }
        }
        let adjusted = phase + offset;
        unwrapped.push(adjusted);
        previous = Some(phase);
    }
    unwrapped
}

pub(crate) fn write_legacy_history(path: &Path) -> Result<(), TableError> {
    let columns = ["CAL_PARMS", "CAL_TABLES", "CAL_SELECT", "CAL_NOTES"]
        .map(|column| ColumnSchema::scalar(column, casa_core::PrimitiveType::String));
    Table::with_schema(TableSchema::new(columns.to_vec()).expect("legacy CAL_HISTORY schema"))
        .save(TableOptions::new(path))
}

pub(crate) fn write_legacy_cal_desc(
    path: PathBuf,
    cal_desc_ids: &BTreeMap<i32, i32>,
    receptor_count: impl Fn(i32) -> i32,
    casa_layout: bool,
    ms_name: &str,
) -> Result<(), TableError> {
    use casa_core::PrimitiveType::{Float64, Int32, String as Text};
    let mut columns = vec![
        ColumnSchema::scalar("NUM_RECEPTORS", casa_core::PrimitiveType::Int32),
        ColumnSchema::array_variable(
            "SPECTRAL_WINDOW_ID",
            casa_core::PrimitiveType::Int32,
            Some(1),
        ),
    ];
    if casa_layout {
        columns.extend(
            ["NUM_SPW", "N_JONES", "MEAS_FREQ_REF"].map(|name| ColumnSchema::scalar(name, Int32)),
        );
        columns.extend(
            ["NUM_CHAN", "CHAN_RANGE"].map(|name| ColumnSchema::array_variable(name, Int32, None)),
        );
        columns.extend(
            ["CHAN_FREQ", "CHAN_WIDTH"]
                .map(|name| ColumnSchema::array_variable(name, Float64, None)),
        );
        columns.extend(["JONES_TYPE", "MS_NAME"].map(|name| ColumnSchema::scalar(name, Text)));
        columns.push(ColumnSchema::array_variable(
            "POLARIZATION_TYPE",
            Text,
            None,
        ));
    }
    let schema = TableSchema::new(columns).expect("BPOLY CAL_DESC schema");
    let mut table = Table::with_schema(schema);
    if casa_layout {
        set_fixed_unit_keyword(&mut table, "CHAN_FREQ", &["Hz"]);
        set_measinfo_keyword(&mut table, "CHAN_FREQ", "frequency", Some("REST"));
        set_fixed_unit_keyword(&mut table, "CHAN_WIDTH", &["Hz"]);
    }
    for spw_id in cal_desc_ids.keys() {
        let receptor_count = if casa_layout {
            0
        } else {
            receptor_count(*spw_id)
        };
        let mut fields = vec![
            RecordField::new(
                "NUM_RECEPTORS",
                Value::Scalar(ScalarValue::Int32(receptor_count)),
            ),
            RecordField::new(
                "SPECTRAL_WINDOW_ID",
                Value::Array(ArrayValue::from_i32_vec(vec![*spw_id])),
            ),
        ];
        if casa_layout {
            fields.extend(
                ["NUM_SPW", "N_JONES", "MEAS_FREQ_REF"]
                    .map(|name| RecordField::new(name, Value::Scalar(ScalarValue::Int32(0)))),
            );
            fields.push(RecordField::new(
                "JONES_TYPE",
                Value::Scalar(ScalarValue::String(String::new())),
            ));
            fields.push(RecordField::new(
                "MS_NAME",
                Value::Scalar(ScalarValue::String(ms_name.to_string())),
            ));
        }
        table
            .add_row(RecordValue::new(fields))
            .expect("insert BPOLY CAL_DESC row");
    }
    table.save(TableOptions::new(&path))
}
