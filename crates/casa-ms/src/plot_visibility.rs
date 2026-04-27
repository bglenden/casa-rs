// SPDX-License-Identifier: LGPL-3.0-or-later
//! Internal lowering for curated raw-visibility listobs plot payloads.

use std::collections::BTreeMap;

use casa_tables::Table;
use casa_types::{ArrayValue, Complex64};
use ndarray::Ix2;

use crate::columns::{main_ids, time_columns::TimeColumn, uvw_column::UvwColumn};
use crate::listobs::{self, ListObsOptions};
use crate::plot::{
    ListObsPlotKind, ListObsPlotPayload, ListObsPlotSpec, VisibilityScatterPlotPayload,
    VisibilityScatterSeries,
};
use crate::{MeasurementSet, subtables};

#[derive(Debug, Default)]
struct SeriesAccumulator {
    label: String,
    color_group: String,
    points: Vec<(f64, f64)>,
}

/// Build one curated raw-visibility plot payload directly from MAIN data.
pub(crate) fn build_listobs_visibility_plot_payload(
    ms: &MeasurementSet,
    options: &ListObsOptions,
    spec: &ListObsPlotSpec,
) -> Result<ListObsPlotPayload, String> {
    let y_axis = match spec.kind {
        ListObsPlotKind::AmplitudeVsTime | ListObsPlotKind::AmplitudeVsUvDistance => {
            VisibilityValue::Amplitude
        }
        ListObsPlotKind::PhaseVsTime => VisibilityValue::Phase,
        other => {
            return Err(format!(
                "{} is not a raw visibility listobs plot",
                other.display_name()
            ));
        }
    };
    let x_axis = match spec.kind {
        ListObsPlotKind::AmplitudeVsUvDistance => VisibilityDomain::UvDistance,
        _ => VisibilityDomain::Time,
    };
    let color_by = ColorBy::parse(spec.option("color_by").unwrap_or(match spec.kind {
        ListObsPlotKind::AmplitudeVsUvDistance => "spw",
        _ => "field",
    }))?;
    let data_column = DataColumn::parse(spec.option("data_column").unwrap_or("data"))?;

    let row_numbers = listobs::resolve_selected_rows(ms, options)
        .map_err(|error| error.to_string())?
        .unwrap_or_else(|| (0..ms.row_count()).collect());
    let selected_data =
        SelectedArrayColumn::load(ms.main_table(), data_column.column_name(), &row_numbers)?;
    let selected_flags = SelectedArrayColumn::load(ms.main_table(), "FLAG", &row_numbers)?;
    let field_id = main_ids::field_id(ms.main_table());
    let scan_number = main_ids::scan_number(ms.main_table());
    let data_desc_id = main_ids::data_desc_id(ms.main_table());
    let antenna1 = main_ids::antenna1(ms.main_table());
    let antenna2 = main_ids::antenna2(ms.main_table());
    let flag_row = ms.flag_row_column();
    let time = TimeColumn::new(ms.main_table());
    let uvw = UvwColumn::new(ms.main_table());

    let field = ms.field().map_err(|error| error.to_string())?;
    let spectral_window = ms.spectral_window().map_err(|error| error.to_string())?;
    let polarization = ms.polarization().map_err(|error| error.to_string())?;
    let data_description = ms.data_description().map_err(|error| error.to_string())?;
    let requested_corr_codes = options
        .correlation
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(listobs::parse_correlation_selector)
        .transpose()
        .map_err(|error| error.to_string())?;

    let mut series = BTreeMap::<String, SeriesAccumulator>::new();
    let mut contributing_rows = 0usize;
    let mut contributing_points = 0usize;
    for (row_slot, row) in row_numbers.iter().copied().enumerate() {
        if flag_row.get(row).map_err(|error| error.to_string())? {
            continue;
        }
        let ddid = data_desc_id.get(row).map_err(|error| error.to_string())?;
        if ddid < 0 || (ddid as usize) >= data_description.row_count() {
            continue;
        }
        let ddid = ddid as usize;
        let spw_id = data_description
            .spectral_window_id(ddid)
            .map_err(|error| error.to_string())?;
        let pol_id = data_description
            .polarization_id(ddid)
            .map_err(|error| error.to_string())?;
        let corr_types = if pol_id >= 0 && (pol_id as usize) < polarization.row_count() {
            polarization
                .corr_type(pol_id as usize)
                .map_err(|error| error.to_string())?
        } else {
            Vec::new()
        };

        let grid = complex_grid_from_array(selected_data.get(row_slot)?)?;
        let flags = match selected_flags.get(row_slot)? {
            ArrayValue::Bool(values) => {
                values.view().into_dimensionality::<Ix2>().map_err(|_| {
                    "msexplore expects FLAG cells with shape [num_corr, num_chan]".to_string()
                })?
            }
            other => {
                return Err(format!(
                    "msexplore requires BOOL flag cells, found {:?}",
                    other.primitive_type()
                ));
            }
        };
        if flags.shape() != [grid.corr_count, grid.chan_count] {
            return Err(format!(
                "visibility flag shape {:?} does not match data shape [{}, {}]",
                flags.shape(),
                grid.corr_count,
                grid.chan_count
            ));
        }

        let selected_correlations = select_correlation_slots(
            grid.corr_count,
            &corr_types,
            requested_corr_codes.as_deref(),
        );
        if selected_correlations.is_empty() {
            continue;
        }

        let field_id_value = field_id.get(row).map_err(|error| error.to_string())?;
        let scan_number_value = scan_number.get(row).map_err(|error| error.to_string())?;
        let antenna1_value = antenna1.get(row).map_err(|error| error.to_string())?;
        let antenna2_value = antenna2.get(row).map_err(|error| error.to_string())?;
        let x_value = match x_axis {
            VisibilityDomain::Time => time
                .get_mjd_seconds(row)
                .map_err(|error| error.to_string())?,
            VisibilityDomain::UvDistance => {
                let [u, v, _w] = uvw.get(row).map_err(|error| error.to_string())?;
                (u * u + v * v).sqrt()
            }
        };

        let mut row_contributed = false;
        for (corr_index, corr_label) in selected_correlations {
            let Some(y_value) = averaged_visibility_value(&grid, &flags, corr_index, y_axis) else {
                continue;
            };
            let (group_key, group_label) = visibility_group(
                color_by,
                field_id_value,
                &field,
                spw_id,
                &spectral_window,
                scan_number_value,
                antenna1_value,
                antenna2_value,
                Some(&corr_label),
            );
            let entry = series
                .entry(group_key.clone())
                .or_insert_with(|| SeriesAccumulator {
                    label: group_label,
                    color_group: group_key,
                    points: Vec::new(),
                });
            entry.points.push((x_value, y_value));
            contributing_points += 1;
            row_contributed = true;
        }
        if row_contributed {
            contributing_rows += 1;
        }
    }

    if contributing_points == 0 {
        return Err(format!(
            "{} produced no unflagged visibility points for the current selection",
            spec.kind.display_name()
        ));
    }

    let mut series = series
        .into_values()
        .map(|mut entry| {
            entry
                .points
                .sort_by(|left, right| left.0.total_cmp(&right.0));
            VisibilityScatterSeries {
                label: entry.label,
                color_group: entry.color_group,
                points: entry.points,
            }
        })
        .collect::<Vec<_>>();
    series.retain(|entry| !entry.points.is_empty());

    Ok(ListObsPlotPayload::VisibilityScatter(
        VisibilityScatterPlotPayload {
            kind: spec.kind,
            x_label: match x_axis {
                VisibilityDomain::Time => "Time (MJD seconds)".to_string(),
                VisibilityDomain::UvDistance => "UV Distance (m)".to_string(),
            },
            y_label: match y_axis {
                VisibilityValue::Amplitude => "Amplitude".to_string(),
                VisibilityValue::Phase => "Phase (deg)".to_string(),
            },
            fixed_y_bounds: matches!(y_axis, VisibilityValue::Phase).then_some((-180.0, 180.0)),
            summary: format!(
                "{}. Rows={} Points={} Data column={}",
                spec.kind.display_name(),
                contributing_rows,
                contributing_points,
                data_column.as_str()
            ),
            series,
        },
    ))
}

#[derive(Debug, Clone, Copy)]
enum VisibilityDomain {
    Time,
    UvDistance,
}

#[derive(Debug, Clone, Copy)]
enum VisibilityValue {
    Amplitude,
    Phase,
}

#[derive(Debug, Clone, Copy)]
enum DataColumn {
    Data,
    Corrected,
    Model,
}

impl DataColumn {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "data" => Ok(Self::Data),
            "corrected" | "corrected_data" => Ok(Self::Corrected),
            "model" | "model_data" => Ok(Self::Model),
            other => Err(format!(
                "unsupported listobs visibility data column {other:?}; expected data, corrected, or model"
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Corrected => "corrected",
            Self::Model => "model",
        }
    }

    fn column_name(self) -> &'static str {
        match self {
            Self::Data => "DATA",
            Self::Corrected => "CORRECTED_DATA",
            Self::Model => "MODEL_DATA",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ColorBy {
    None,
    Field,
    Scan,
    SpectralWindow,
    Baseline,
    Correlation,
}

impl ColorBy {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "none" => Ok(Self::None),
            "field" => Ok(Self::Field),
            "scan" => Ok(Self::Scan),
            "spw" | "spectral_window" | "spectral-window" => Ok(Self::SpectralWindow),
            "baseline" => Ok(Self::Baseline),
            "correlation" | "corr" => Ok(Self::Correlation),
            other => Err(format!("unsupported color axis {other:?}")),
        }
    }
}

#[derive(Debug, Clone)]
struct ComplexGrid {
    corr_count: usize,
    chan_count: usize,
    values: Vec<Complex64>,
}

struct SelectedArrayColumn {
    column: &'static str,
    values: Vec<Option<ArrayValue>>,
}

impl SelectedArrayColumn {
    fn load(table: &Table, column: &'static str, row_indices: &[usize]) -> Result<Self, String> {
        let values = table
            .column_accessor(column)
            .map_err(|error| error.to_string())?
            .array_cells_owned(row_indices)
            .map_err(|error| error.to_string())?;
        Ok(Self { column, values })
    }

    fn get(&self, row_slot: usize) -> Result<&ArrayValue, String> {
        self.values
            .get(row_slot)
            .and_then(|value| value.as_ref())
            .ok_or_else(|| {
                format!(
                    "msexplore requires {} data for selected row slot {}",
                    self.column, row_slot
                )
            })
    }
}

fn complex_grid_from_array(array: &ArrayValue) -> Result<ComplexGrid, String> {
    match array {
        ArrayValue::Complex32(values) => {
            let values = values.view().into_dimensionality::<Ix2>().map_err(|_| {
                "msexplore expects complex visibility cells with shape [num_corr, num_chan]"
                    .to_string()
            })?;
            Ok(ComplexGrid {
                corr_count: values.nrows(),
                chan_count: values.ncols(),
                values: values
                    .iter()
                    .map(|value| Complex64::new(value.re as f64, value.im as f64))
                    .collect(),
            })
        }
        ArrayValue::Complex64(values) => {
            let values = values.view().into_dimensionality::<Ix2>().map_err(|_| {
                "msexplore expects complex visibility cells with shape [num_corr, num_chan]"
                    .to_string()
            })?;
            Ok(ComplexGrid {
                corr_count: values.nrows(),
                chan_count: values.ncols(),
                values: values.iter().copied().collect(),
            })
        }
        other => Err(format!(
            "msexplore requires complex visibility data, found {:?}",
            other.primitive_type()
        )),
    }
}

fn averaged_visibility_value(
    grid: &ComplexGrid,
    flags: &ndarray::ArrayView2<'_, bool>,
    corr_index: usize,
    value: VisibilityValue,
) -> Option<f64> {
    if corr_index >= grid.corr_count {
        return None;
    }
    let mut samples = Vec::new();
    for chan_index in 0..grid.chan_count {
        if chan_index >= flags.ncols() || flags[(corr_index, chan_index)] {
            continue;
        }
        let sample = grid.values[corr_index * grid.chan_count + chan_index];
        if sample.re.is_finite() && sample.im.is_finite() {
            samples.push(sample);
        }
    }
    if samples.is_empty() {
        return None;
    }
    let average = samples.iter().copied().sum::<Complex64>() / samples.len() as f64;
    match value {
        VisibilityValue::Amplitude => Some(average.norm()),
        VisibilityValue::Phase => Some(average.arg().to_degrees()),
    }
}

fn select_correlation_slots(
    corr_count: usize,
    corr_types: &[i32],
    requested_corr_codes: Option<&[i32]>,
) -> Vec<(usize, String)> {
    let requested_corr_codes = requested_corr_codes.unwrap_or(&[]);
    let mut slots = Vec::new();
    for corr_index in 0..corr_count {
        let corr_label = corr_types
            .get(corr_index)
            .map(|code| listobs::stokes_name(*code).to_string())
            .unwrap_or_else(|| format!("corr-{corr_index}"));
        let corr_code = corr_types.get(corr_index).copied();
        if requested_corr_codes.is_empty()
            || corr_code.is_some_and(|code| requested_corr_codes.contains(&code))
        {
            slots.push((corr_index, corr_label));
        }
    }
    slots
}

#[allow(clippy::too_many_arguments)]
fn visibility_group(
    color_by: ColorBy,
    field_id: i32,
    field: &subtables::MsField<'_>,
    spw_id: i32,
    spectral_window: &subtables::MsSpectralWindow<'_>,
    scan_number: i32,
    antenna1: i32,
    antenna2: i32,
    correlation_label: Option<&str>,
) -> (String, String) {
    match color_by {
        ColorBy::None => ("all".to_string(), "All data".to_string()),
        ColorBy::Field => {
            let field_name = if field_id >= 0 && (field_id as usize) < field.row_count() {
                field
                    .name(field_id as usize)
                    .unwrap_or_else(|_| format!("FIELD {field_id}"))
            } else {
                format!("FIELD {field_id}")
            };
            (format!("field-{field_id}"), field_name)
        }
        ColorBy::Scan => (format!("scan-{scan_number}"), format!("Scan {scan_number}")),
        ColorBy::SpectralWindow => {
            let spw_name = if spw_id >= 0 && (spw_id as usize) < spectral_window.row_count() {
                spectral_window
                    .name(spw_id as usize)
                    .unwrap_or_else(|_| format!("SPW {spw_id}"))
            } else {
                format!("SPW {spw_id}")
            };
            (format!("spw-{spw_id}"), spw_name)
        }
        ColorBy::Baseline => {
            let label = format!("a{antenna1}-a{antenna2}");
            (format!("baseline-{label}"), label)
        }
        ColorBy::Correlation => {
            let label = correlation_label.unwrap_or("corr").to_string();
            (format!("corr-{label}"), label)
        }
    }
}
