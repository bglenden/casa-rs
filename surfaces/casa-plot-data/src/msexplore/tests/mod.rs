// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use crate::plot::UvCoveragePlotPayload;
use casa_core::{RecordField, RecordValue};
use casa_ms::{
    AntennaSummary, DataDescriptionSummary, FieldSummary, MeasurementSetInfo, ObservationSummary,
    PolarizationSummary, SourceSummary, SpectralWindowSummary,
};
use casa_ms::{ColumnDef, ColumnKind};
use casa_ms::{MeasurementSetBuilder, OptionalMainColumn, schema};
use ndarray::ArrayD;
use num_complex::Complex32;
use tempfile::tempdir;

fn default_value(col_name: &str) -> Value {
    let all_cols: Vec<&ColumnDef> = schema::main_table::REQUIRED_COLUMNS
        .iter()
        .chain(schema::main_table::OPTIONAL_COLUMNS.iter())
        .collect();
    if let Some(c) = all_cols.iter().find(|c| c.name == col_name) {
        default_value_for_def(c)
    } else {
        Value::Scalar(ScalarValue::Int32(0))
    }
}

fn default_value_for_def(c: &ColumnDef) -> Value {
    match c.column_kind {
        ColumnKind::Scalar => match c.data_type {
            casa_core::PrimitiveType::Int32 => Value::Scalar(ScalarValue::Int32(0)),
            casa_core::PrimitiveType::Float64 => Value::Scalar(ScalarValue::Float64(0.0)),
            casa_core::PrimitiveType::Bool => Value::Scalar(ScalarValue::Bool(false)),
            casa_core::PrimitiveType::String => Value::Scalar(ScalarValue::String(String::new())),
            _ => Value::Scalar(ScalarValue::Float64(0.0)),
        },
        ColumnKind::FixedArray { shape } => {
            let total: usize = shape.iter().product();
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(shape.to_vec(), vec![0.0; total]).unwrap(),
            ))
        }
        ColumnKind::VariableArray { ndim } => {
            let shape: Vec<usize> = vec![1; ndim];
            let total: usize = shape.iter().product();
            match c.data_type {
                casa_core::PrimitiveType::Bool => Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(shape, vec![false; total]).unwrap(),
                )),
                casa_core::PrimitiveType::Float32 => Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(shape, vec![1.0; total]).unwrap(),
                )),
                _ => Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap(),
                )),
            }
        }
    }
}

fn scatter_point_ref(row: usize) -> MsScatterPointRef {
    MsScatterPointRef {
        row,
        corr: 0,
        chan_start: 0,
        chan_end: 1,
    }
}

fn scatter_series(
    label: &str,
    color_group: &str,
    y_axis: MsAxis,
    points: &[(f64, f64)],
    row_base: usize,
) -> MsScatterSeries {
    MsScatterSeries {
        label: label.to_string(),
        color_group: color_group.to_string(),
        y_axis,
        y_index: 0,
        points: points.to_vec(),
        plotms_metadata: Vec::new(),
        plotms_reporting_frequencies_hz: Vec::new(),
        plotms_averaged_ids: None,
        provenance: (0..points.len())
            .map(|offset| scatter_point_ref(row_base + offset))
            .collect(),
    }
}

fn scatter_plot_payload(
    title: &str,
    secondary_y_axis: Option<MsAxis>,
    legend_position: MsLegendPosition,
    series: Vec<MsScatterSeries>,
) -> MsScatterPlotPayload {
    MsScatterPlotPayload {
        title: title.to_string(),
        x_axis: MsAxis::Time,
        y_axis: MsAxis::Amplitude,
        secondary_y_axis,
        x_label: "Time".to_string(),
        y_label: "Amplitude".to_string(),
        secondary_y_label: secondary_y_axis.map(|axis| axis.display_name().to_string()),
        fixed_x_bounds: None,
        fixed_y_bounds: None,
        secondary_fixed_y_bounds: None,
        showlegend: true,
        legend_position,
        showmajorgrid: true,
        showminorgrid: true,
        symbol_size_px: None,
        series,
        header_lines: vec!["Synthetic header".to_string()],
        summary: "Synthetic scatter payload".to_string(),
    }
}

#[test]
fn prepared_selected_data_source_preserves_selected_row_order() {
    let mut ms = MeasurementSet::create_memory(
        MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
    )
    .expect("create in-memory MS");
    let schema = ms.main_table().schema().expect("main schema").clone();

    for amplitude in [10.0_f32, 20.0_f32, 30.0_f32] {
        let data = ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                vec![1, 2],
                vec![
                    Complex32::new(amplitude, 0.0),
                    Complex32::new(amplitude + 1.0, 0.0),
                ],
            )
            .expect("data shape"),
        );
        let fields = schema
            .columns()
            .iter()
            .map(|column| {
                if column.name() == "DATA" {
                    RecordField::new("DATA", Value::Array(data.clone()))
                } else {
                    RecordField::new(column.name(), default_value(column.name()))
                }
            })
            .collect::<Vec<_>>();
        ms.main_table_mut()
            .add_row(RecordValue::new(fields))
            .expect("add main row");
    }

    let source =
        PreparedSelectedDataSource::new(&ms, MsDataColumn::Data, &[2, 0], None).expect("prefetch");
    let first = source.row(0, false, false).expect("selected row 0");
    let second = source.row(1, false, false).expect("selected row 1");

    assert_eq!(first.corr_count, 1);
    assert_eq!(first.chan_count, 2);
    assert_eq!(first.values[0], Complex64::new(30.0, 0.0));
    assert_eq!(first.values[1], Complex64::new(31.0, 0.0));
    assert_eq!(second.values[0], Complex64::new(10.0, 0.0));
    assert_eq!(second.values[1], Complex64::new(11.0, 0.0));
}

mod plot_spec;

#[test]
fn scalar_spans_do_not_enlarge_plotms_point_metadata() {
    assert_eq!(
        std::mem::size_of::<MsScatterPlotMsMetadata>(),
        std::mem::size_of::<(usize, Option<f64>, Option<usize>)>(),
    );
}

#[test]
fn compact_scatter_series_evenly_decimates_points() {
    let series = vec![MsScatterSeries {
        label: "Amplitude".to_string(),
        color_group: "all".to_string(),
        y_axis: MsAxis::Amplitude,
        y_index: 0,
        points: (0..5).map(|index| (index as f64, index as f64)).collect(),
        plotms_metadata: Vec::new(),
        plotms_reporting_frequencies_hz: vec![88e9, 89e9, 90e9, 91e9, 92e9],
        plotms_averaged_ids: None,
        provenance: (0..5)
            .map(|index| MsScatterPointRef {
                row: index,
                corr: 0,
                chan_start: index,
                chan_end: index + 1,
            })
            .collect(),
    }];

    let (compacted, original_points, kept_points) = compact_scatter_series(series, 3);
    assert_eq!(original_points, 5);
    assert_eq!(kept_points, 3);
    assert_eq!(compacted.len(), 1);
    assert_eq!(
        compacted[0].plotms_reporting_frequencies_hz,
        [88e9, 90e9, 92e9]
    );
    assert_eq!(
        compacted[0].points,
        vec![(0.0, 0.0), (2.0, 2.0), (4.0, 4.0)]
    );
    assert_eq!(
        compacted[0]
            .provenance
            .iter()
            .map(|point| point.row)
            .collect::<Vec<_>>(),
        vec![0, 2, 4]
    );
}

#[test]
fn compact_payload_to_budget_downsamples_scatter_payload_and_warns() {
    let payload = MsPlotPayload::Scatter(MsScatterPlotPayload {
        title: "Dense plot".to_string(),
        x_axis: MsAxis::Time,
        y_axis: MsAxis::Amplitude,
        secondary_y_axis: None,
        x_label: "Time".to_string(),
        y_label: "Amplitude".to_string(),
        secondary_y_label: None,
        fixed_x_bounds: None,
        fixed_y_bounds: None,
        secondary_fixed_y_bounds: None,
        showlegend: true,
        legend_position: MsLegendPosition::UpperRight,
        showmajorgrid: true,
        showminorgrid: false,
        symbol_size_px: None,
        series: vec![
            MsScatterSeries {
                label: "A".to_string(),
                color_group: "a".to_string(),
                y_axis: MsAxis::Amplitude,
                y_index: 0,
                points: (0..5).map(|index| (index as f64, index as f64)).collect(),
                plotms_metadata: Vec::new(),
                plotms_reporting_frequencies_hz: Vec::new(),
                plotms_averaged_ids: None,
                provenance: (0..5)
                    .map(|index| MsScatterPointRef {
                        row: index,
                        corr: 0,
                        chan_start: index,
                        chan_end: index + 1,
                    })
                    .collect(),
            },
            MsScatterSeries {
                label: "B".to_string(),
                color_group: "b".to_string(),
                y_axis: MsAxis::Amplitude,
                y_index: 0,
                points: (5..10).map(|index| (index as f64, index as f64)).collect(),
                plotms_metadata: Vec::new(),
                plotms_reporting_frequencies_hz: Vec::new(),
                plotms_averaged_ids: None,
                provenance: (5..10)
                    .map(|index| MsScatterPointRef {
                        row: index,
                        corr: 0,
                        chan_start: index,
                        chan_end: index + 1,
                    })
                    .collect(),
            },
        ],
        header_lines: Vec::new(),
        summary: "Dense plot summary.".to_string(),
    });
    let point_budget = PointBudget {
        max_plot_points: Some(6),
        rendered_points: 10,
        allow_predecimation: false,
    };

    let compacted = compact_payload_to_budget(payload, &point_budget).unwrap();
    let MsPlotPayload::Scatter(compacted) = compacted else {
        panic!("expected scatter payload");
    };
    assert_eq!(
        compacted
            .series
            .iter()
            .map(|series| series.points.len())
            .sum::<usize>(),
        6
    );
    assert_eq!(compacted.header_lines.len(), 1);
    assert!(compacted.header_lines[0].contains("Downsampled plot"));
    assert!(compacted.summary.contains("Decimated points from 10 to 6"));
}

fn synthetic_summary() -> ListObsSummary {
    ListObsSummary {
        schema_version: 1,
        options: ListObsOptions::default(),
        measurement_set: MeasurementSetInfo {
            path: Some("fixture.ms".to_string()),
            ms_version: Some(2.0),
            row_count: 4,
            observation_count: 1,
            field_count: 2,
            spectral_window_count: 2,
            polarization_count: 1,
            data_description_count: 2,
            source_count: 1,
            antenna_count: 2,
            antenna_table_count: 2,
            time_reference: Some("UTC".to_string()),
            start_mjd_seconds: None,
            end_mjd_seconds: Some(180.0),
            total_elapsed_seconds: Some(80.0),
        },
        observations: vec![ObservationSummary {
            observation_id: 0,
            telescope_name: "VLA".to_string(),
            observer: "  ".to_string(),
            project: "Project-42".to_string(),
            release_date_mjd_seconds: 0.0,
            start_mjd_seconds: Some(100.0),
            end_mjd_seconds: Some(180.0),
        }],
        scans: vec![],
        fields: vec![
            FieldSummary {
                field_id: 0,
                name: "FIELD_A".to_string(),
                code: "".to_string(),
                source_id: 0,
                row_count: 2,
                unflagged_row_count: None,
                time_mjd_seconds: 100.0,
                direction_reference: Some("J2000".to_string()),
                phase_direction_radians: [0.0, 0.0],
            },
            FieldSummary {
                field_id: 1,
                name: "FIELD_B".to_string(),
                code: "".to_string(),
                source_id: 0,
                row_count: 2,
                unflagged_row_count: None,
                time_mjd_seconds: 140.0,
                direction_reference: Some("J2000".to_string()),
                phase_direction_radians: [0.1, 0.2],
            },
        ],
        polarization_setups: vec![PolarizationSummary {
            polarization_id: 0,
            num_correlations: 2,
            correlation_types: vec!["XX".to_string(), "YY".to_string()],
        }],
        data_descriptions: vec![
            DataDescriptionSummary {
                data_description_id: 0,
                spectral_window_id: 0,
                polarization_id: 0,
                flagged: false,
            },
            DataDescriptionSummary {
                data_description_id: 1,
                spectral_window_id: 1,
                polarization_id: 0,
                flagged: false,
            },
        ],
        spectral_windows: vec![
            SpectralWindowSummary {
                spectral_window_id: 0,
                name: "LOW".to_string(),
                num_channels: 16,
                frame: Some("TOPO".to_string()),
                first_channel_frequency_hz: 1.0e9,
                channel_width_hz: 1.0e6,
                reference_frequency_hz: 1.0e9,
                center_frequency_hz: 1.008e9,
                min_frequency_hz: 0.9995e9,
                max_frequency_hz: 1.0165e9,
                total_bandwidth_hz: 16.0e6,
                data_description_ids: vec![0],
                polarization_ids: vec![0],
                correlation_types: vec!["XX".to_string(), "YY".to_string()],
            },
            SpectralWindowSummary {
                spectral_window_id: 1,
                name: "HIGH".to_string(),
                num_channels: 32,
                frame: Some("TOPO".to_string()),
                first_channel_frequency_hz: 1.2e9,
                channel_width_hz: 5.0e5,
                reference_frequency_hz: 1.2e9,
                center_frequency_hz: 1.208e9,
                min_frequency_hz: 1.19975e9,
                max_frequency_hz: 1.21625e9,
                total_bandwidth_hz: 16.0e6,
                data_description_ids: vec![1],
                polarization_ids: vec![0],
                correlation_types: vec!["XX".to_string(), "YY".to_string()],
            },
        ],
        sources: vec![SourceSummary {
            source_id: 0,
            name: "SOURCE_A".to_string(),
            code: "".to_string(),
            spectral_window_id: -1,
            calibration_group: 0,
            num_lines: 0,
            rest_frequency_hz: None,
            system_velocity_m_s: None,
            time_mjd_seconds: 100.0,
            direction_radians: [0.0, 0.0],
        }],
        antennas: vec![
            AntennaSummary {
                antenna_id: 0,
                name: "ea01".to_string(),
                station: "A01".to_string(),
                antenna_type: "GROUND-BASED".to_string(),
                mount: "ALT-AZ".to_string(),
                dish_diameter_m: 25.0,
                longitude_radians: 0.0,
                latitude_radians: 0.0,
                offset_from_observatory_m: [0.0, 0.0, 0.0],
                position_m: [1000.0, 2000.0, 0.0],
            },
            AntennaSummary {
                antenna_id: 1,
                name: "ea02".to_string(),
                station: "A02".to_string(),
                antenna_type: "GROUND-BASED".to_string(),
                mount: "ALT-AZ".to_string(),
                dish_diameter_m: 25.0,
                longitude_radians: 0.0,
                latitude_radians: 0.0,
                offset_from_observatory_m: [120.0, 40.0, 0.0],
                position_m: [1120.0, 2040.0, 0.0],
            },
        ],
    }
}

#[test]
fn compact_payload_to_budget_handles_grid_page_passthrough_and_empty_results() {
    let original = MsPlotPayload::ListObs(ListObsPlotPayload::UvCoverage(UvCoveragePlotPayload {
        draw_points: true,
        mirror: true,
        axis_extent_lambda: 0.0,
        tracks: vec![],
        summary: "uv".to_string(),
    }));
    assert_eq!(
        compact_payload_to_budget(original.clone(), &PointBudget::unlimited()).unwrap(),
        original
    );

    let grid_payload = MsPlotPayload::ScatterGrid(MsScatterGridPayload {
        title: "Dense grid".to_string(),
        x_axis: MsAxis::Time,
        y_axis: MsAxis::Amplitude,
        x_label: "Time".to_string(),
        y_label: "Amplitude".to_string(),
        fixed_x_bounds: None,
        fixed_y_bounds: None,
        showlegend: true,
        legend_position: MsLegendPosition::ExteriorBottom,
        showmajorgrid: true,
        showminorgrid: false,
        symbol_size_px: None,
        iteraxis: MsIterationAxis::Field,
        gridrows: 1,
        gridcols: 2,
        share_x_bounds: true,
        share_y_bounds: true,
        panels: vec![
            MsScatterPanelPayload {
                key: "field=0".to_string(),
                label: "Field 0".to_string(),
                summary: "Field 0 summary.".to_string(),
                series: vec![
                    scatter_series(
                        "A0",
                        "a0",
                        MsAxis::Amplitude,
                        &[(0.0, 1.0), (1.0, 2.0), (2.0, 3.0)],
                        0,
                    ),
                    scatter_series(
                        "A1",
                        "a1",
                        MsAxis::Amplitude,
                        &[(0.0, 1.5), (1.0, 2.5), (2.0, 3.5)],
                        10,
                    ),
                ],
            },
            MsScatterPanelPayload {
                key: "field=1".to_string(),
                label: "Field 1".to_string(),
                summary: "Field 1 summary.".to_string(),
                series: vec![scatter_series(
                    "B0",
                    "b0",
                    MsAxis::Amplitude,
                    &[(0.0, 4.0), (1.0, 5.0), (2.0, 6.0)],
                    20,
                )],
            },
        ],
        header_lines: vec![],
        summary: "Dense grid summary.".to_string(),
    });
    let compacted_grid = compact_payload_to_budget(
        grid_payload,
        &PointBudget {
            max_plot_points: Some(4),
            rendered_points: 9,
            allow_predecimation: false,
        },
    )
    .unwrap();
    let MsPlotPayload::ScatterGrid(compacted_grid) = compacted_grid else {
        panic!("expected scatter grid payload");
    };
    assert_eq!(compacted_grid.panels.len(), 2);
    assert!(compacted_grid.header_lines[0].contains("Downsampled plot"));
    assert!(
        compacted_grid
            .summary
            .contains("Decimated points from 9 to 4")
    );

    let page_payload = MsPlotPayload::ScatterPage(MsScatterPagePayload {
        title: "Dense page".to_string(),
        exprange: MsPageExportRange::All,
        gridrows: 1,
        gridcols: 2,
        header_lines: vec![],
        items: vec![
            MsScatterPageItemPayload {
                plotindex: 0,
                rowindex: 0,
                colindex: 0,
                plot: scatter_plot_payload(
                    "Left",
                    None,
                    MsLegendPosition::UpperRight,
                    vec![scatter_series(
                        "L",
                        "left",
                        MsAxis::Amplitude,
                        &[(0.0, 1.0), (1.0, 1.5), (2.0, 2.0)],
                        30,
                    )],
                ),
            },
            MsScatterPageItemPayload {
                plotindex: 1,
                rowindex: 0,
                colindex: 1,
                plot: scatter_plot_payload(
                    "Right",
                    None,
                    MsLegendPosition::UpperRight,
                    vec![scatter_series(
                        "R",
                        "right",
                        MsAxis::Amplitude,
                        &[(0.0, 3.0), (1.0, 3.5), (2.0, 4.0)],
                        40,
                    )],
                ),
            },
        ],
        summary: "Dense page summary.".to_string(),
    });
    let compacted_page = compact_payload_to_budget(
        page_payload,
        &PointBudget {
            max_plot_points: Some(3),
            rendered_points: 6,
            allow_predecimation: false,
        },
    )
    .unwrap();
    let MsPlotPayload::ScatterPage(compacted_page) = compacted_page else {
        panic!("expected scatter page payload");
    };
    assert_eq!(compacted_page.items.len(), 2);
    assert!(compacted_page.header_lines[0].contains("Downsampled plot"));
    assert!(
        compacted_page
            .summary
            .contains("Decimated points from 6 to 3")
    );

    let empty_grid = MsPlotPayload::ScatterGrid(MsScatterGridPayload {
        title: "Empty".to_string(),
        x_axis: MsAxis::Time,
        y_axis: MsAxis::Amplitude,
        x_label: "Time".to_string(),
        y_label: "Amplitude".to_string(),
        fixed_x_bounds: None,
        fixed_y_bounds: None,
        showlegend: false,
        legend_position: MsLegendPosition::UpperRight,
        showmajorgrid: true,
        showminorgrid: false,
        symbol_size_px: None,
        iteraxis: MsIterationAxis::Field,
        gridrows: 1,
        gridcols: 1,
        share_x_bounds: true,
        share_y_bounds: true,
        panels: vec![MsScatterPanelPayload {
            key: "field=0".to_string(),
            label: "Field 0".to_string(),
            summary: "Empty panel".to_string(),
            series: vec![scatter_series("Empty", "empty", MsAxis::Amplitude, &[], 50)],
        }],
        header_lines: vec![],
        summary: "Empty grid".to_string(),
    });
    assert!(
        compact_payload_to_budget(
            empty_grid,
            &PointBudget {
                max_plot_points: Some(1),
                rendered_points: 2,
                allow_predecimation: false,
            },
        )
        .unwrap_err()
        .contains("produced no drawable panels")
    );
}

#[test]
fn page_header_resolution_uses_summary_fallbacks_and_wrapping() {
    let summary = synthetic_summary();
    assert_eq!(measurement_set_start_time(&summary), Some(100.0));
    assert_eq!(first_non_empty([" ", "", "Observer"]), Some("Observer"));
    assert_eq!(first_non_empty([" ", "   "]), None);

    let spec = MsExploreSpec {
        ms_path: std::path::PathBuf::from("fixture.ms"),
        summary_format: MeasurementSetSummaryOutputFormat::Text,
        selection: MsSelection::default(),
        header_items: vec![
            MsPageHeaderItem::Filename,
            MsPageHeaderItem::ObsDate,
            MsPageHeaderItem::ObsTime,
            MsPageHeaderItem::Observer,
            MsPageHeaderItem::ProjId,
            MsPageHeaderItem::Telescope,
            MsPageHeaderItem::TargName,
            MsPageHeaderItem::TargDir,
            MsPageHeaderItem::YColumn,
        ],
        page_title: None,
        exprange: MsPageExportRange::Current,
        max_plot_points: 10,
        plots: vec![MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime)],
    };

    let lines = [
        resolve_page_header_segment(MsPageHeaderItem::Filename, &spec, &summary),
        resolve_page_header_segment(MsPageHeaderItem::ObsDate, &spec, &summary),
        resolve_page_header_segment(MsPageHeaderItem::ObsTime, &spec, &summary),
        resolve_page_header_segment(MsPageHeaderItem::Observer, &spec, &summary),
        resolve_page_header_segment(MsPageHeaderItem::ProjId, &spec, &summary),
        resolve_page_header_segment(MsPageHeaderItem::Telescope, &spec, &summary),
        resolve_page_header_segment(MsPageHeaderItem::TargName, &spec, &summary),
        resolve_page_header_segment(MsPageHeaderItem::TargDir, &spec, &summary),
        resolve_page_header_segment(MsPageHeaderItem::YColumn, &spec, &summary),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    let joined = lines.join(" | ");
    assert!(joined.contains("fixture.ms"));
    assert!(joined.contains("Obs Date:"));
    assert!(joined.contains("Obs Time:"));
    assert!(joined.contains("Project-42"));
    assert!(joined.contains("VLA"));
    assert!(joined.contains("FIELD_A"));
    assert!(joined.contains("J2000"));
    assert!(joined.contains("Amplitude"));
    assert_eq!(
        wrap_header_segments(&["x".repeat(40), "y".repeat(40), "z".repeat(40),]).len(),
        2
    );
}

#[test]
fn prepared_selected_data_source_and_grid_helpers_cover_ratio_and_errors() {
    let mut ms = MeasurementSet::create_memory(
        MeasurementSetBuilder::new()
            .with_main_column(OptionalMainColumn::Data)
            .with_main_column(OptionalMainColumn::CorrectedData)
            .with_main_column(OptionalMainColumn::ModelData),
    )
    .expect("create in-memory MS");
    let schema = ms.main_table().schema().expect("main schema").clone();
    let make_complex = |values: &[(f32, f32)]| {
        ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                vec![1, values.len()],
                values
                    .iter()
                    .map(|(re, im)| Complex32::new(*re, *im))
                    .collect(),
            )
            .expect("complex shape"),
        )
    };

    for (data, corrected, model) in [
        (
            make_complex(&[(10.0, 0.0), (20.0, 0.0)]),
            make_complex(&[(12.0, 0.0), (24.0, 0.0)]),
            make_complex(&[(2.0, 0.0), (4.0, 0.0)]),
        ),
        (
            make_complex(&[(5.0, 0.0), (15.0, 0.0)]),
            make_complex(&[(8.0, 0.0), (0.0, 0.0)]),
            make_complex(&[(4.0, 0.0), (0.0, 0.0)]),
        ),
    ] {
        let fields = schema
            .columns()
            .iter()
            .map(|column| {
                let value = match column.name() {
                    "DATA" => Value::Array(data.clone()),
                    "CORRECTED_DATA" => Value::Array(corrected.clone()),
                    "MODEL_DATA" => Value::Array(model.clone()),
                    _ => default_value(column.name()),
                };
                RecordField::new(column.name(), value)
            })
            .collect::<Vec<_>>();
        ms.main_table_mut()
            .add_row(RecordValue::new(fields))
            .expect("add main row");
    }

    let difference =
        PreparedSelectedDataSource::new(&ms, MsDataColumn::CorrectedMinusModel, &[0, 1], None)
            .unwrap();
    assert_eq!(
        difference.row(0, false, false).unwrap().values[0],
        Complex64::new(10.0, 0.0)
    );

    let scalar_ratio =
        PreparedSelectedDataSource::new(&ms, MsDataColumn::CorrectedDivModelScalar, &[0, 1], None)
            .unwrap();
    let scalar_ratio_row = scalar_ratio.row(0, false, false).unwrap();
    assert_eq!(scalar_ratio_row.values[0], Complex64::new(6.0, 0.0));
    assert!(
        scalar_ratio.row(1, false, false).unwrap().values[1]
            .re
            .is_nan()
    );

    let direct_ratio =
        PreparedSelectedDataSource::new(&ms, MsDataColumn::DataDivModel, &[0], None).unwrap();
    assert_eq!(
        direct_ratio.row(0, false, false).unwrap().values[1],
        Complex64::new(5.0, 0.0)
    );

    let wrong_shape = ArrayValue::Complex32(
        ArrayD::from_shape_vec(
            vec![2],
            vec![Complex32::new(1.0, 0.0), Complex32::new(2.0, 0.0)],
        )
        .unwrap(),
    );
    assert!(
        complex_grid_from_array(&wrong_shape)
            .unwrap_err()
            .contains("expects complex visibility cells")
    );

    let float_grid = float_grid_from_array(
        &ArrayValue::Float32(ArrayD::from_shape_vec(vec![1, 2], vec![1.0_f32, 2.0_f32]).unwrap()),
        "WEIGHT_SPECTRUM",
    )
    .unwrap();
    assert_eq!(float_grid.values, vec![1.0, 2.0]);
    assert_eq!(
        scalar_values_to_grid(&[3.0, 4.0], 2).values,
        vec![3.0, 3.0, 4.0, 4.0]
    );
    assert!(
        combine_grids(
            ComplexGrid {
                corr_count: 1,
                chan_count: 2,
                values: vec![Complex64::new(1.0, 0.0), Complex64::new(2.0, 0.0)],
                model: None,
                signed_amplitude: false,
            },
            ComplexGrid {
                corr_count: 2,
                chan_count: 1,
                values: vec![Complex64::new(1.0, 0.0), Complex64::new(2.0, 0.0)],
                model: None,
                signed_amplitude: false,
            },
            |left, right| left + right,
        )
        .unwrap_err()
        .contains("mismatched shapes")
    );
}

#[test]
fn listobs_visibility_seam_rejects_metadata_presets() {
    let ms =
        MeasurementSet::create_memory(MeasurementSetBuilder::new()).expect("create in-memory MS");
    assert!(
        crate::plot_visibility::build_listobs_visibility_plot_payload(
            &ms,
            &ListObsOptions::default(),
            &ListObsPlotSpec::new(ListObsPlotKind::UvCoverage),
        )
        .unwrap_err()
        .contains("not a raw visibility")
    );
}

#[test]
fn scatter_manifests_cover_dual_axis_and_grid_payloads() {
    let dual_axis = scatter_plot_payload(
        "Dual Axis Scatter",
        Some(MsAxis::Phase),
        MsLegendPosition::ExteriorRight,
        vec![
            scatter_series(
                "Amplitude A",
                "amp-a",
                MsAxis::Amplitude,
                &[(4_304_481_700.0, 1.0), (4_304_481_760.0, 2.5)],
                0,
            ),
            scatter_series(
                "Amplitude B",
                "amp-b",
                MsAxis::Amplitude,
                &[(4_304_481_700.0, 0.5), (4_304_481_760.0, 1.8)],
                10,
            ),
            scatter_series(
                "Phase",
                "phase",
                MsAxis::Phase,
                &[(4_304_481_700.0, -30.0), (4_304_481_760.0, 45.0)],
                20,
            ),
        ],
    );
    let manifest = render_scatter_manifest(&MsPlotPayload::Scatter(dual_axis))
        .expect("dual-axis scatter manifest");
    assert!(manifest.contains("# secondary_y_axis=phase"));
    assert!(manifest.contains("series_key\tseries_label\ty_axis\tx\ty\n"));
    assert!(manifest.contains("phase\tPhase\tphase\t"));
    assert!(manifest.contains("amp-b\tAmplitude B\tamplitude\t"));

    let grid = MsScatterGridPayload {
        title: "Grid Scatter".to_string(),
        x_axis: MsAxis::Channel,
        y_axis: MsAxis::Amplitude,
        x_label: "Channel".to_string(),
        y_label: "Amplitude".to_string(),
        fixed_x_bounds: None,
        fixed_y_bounds: None,
        showlegend: true,
        legend_position: MsLegendPosition::ExteriorBottom,
        showmajorgrid: true,
        showminorgrid: false,
        symbol_size_px: None,
        iteraxis: MsIterationAxis::Field,
        gridrows: 1,
        gridcols: 2,
        share_x_bounds: true,
        share_y_bounds: true,
        panels: vec![
            MsScatterPanelPayload {
                key: "field=0".to_string(),
                label: "Field 0".to_string(),
                series: vec![
                    scatter_series(
                        "Field 0 RR",
                        "rr",
                        MsAxis::Amplitude,
                        &[(0.0, 1.0), (1.0, 1.5), (2.0, 2.0)],
                        30,
                    ),
                    scatter_series(
                        "Field 0 LL",
                        "ll",
                        MsAxis::Amplitude,
                        &[(0.0, 0.8), (1.0, 1.1), (2.0, 1.4)],
                        40,
                    ),
                ],
                summary: "Field 0".to_string(),
            },
            MsScatterPanelPayload {
                key: "field=1".to_string(),
                label: "Field 1".to_string(),
                series: vec![
                    scatter_series(
                        "Field 1 RR",
                        "rr",
                        MsAxis::Amplitude,
                        &[(0.0, 2.5), (1.0, 2.0), (2.0, 1.7)],
                        50,
                    ),
                    scatter_series(
                        "Field 1 LL",
                        "ll",
                        MsAxis::Amplitude,
                        &[(0.0, 2.2), (1.0, 1.8), (2.0, 1.3)],
                        60,
                    ),
                ],
                summary: "Field 1".to_string(),
            },
        ],
        header_lines: vec!["Grid header".to_string()],
        summary: "Grid summary".to_string(),
    };
    let manifest =
        render_scatter_manifest(&MsPlotPayload::ScatterGrid(grid)).expect("grid scatter manifest");
    assert!(manifest.contains("# iteraxis=field"));
    assert!(manifest.contains("# gridcols=2"));
    assert!(manifest.contains("# header_line=Grid header"));
    assert!(manifest.contains("panel_key\tpanel_label\tseries_key\tseries_label\tx\ty\n"));
    assert!(manifest.contains("field=1\tField 1\tll\tField 1 LL\t"));
}

#[test]
fn scatter_page_exports_text_manifest_for_every_item() {
    let shared_series = vec![scatter_series(
        "Amplitude",
        "amp",
        MsAxis::Amplitude,
        &[(0.0, 1.0), (1.0, 1.5), (2.0, 1.75)],
        70,
    )];
    let phase_series = vec![scatter_series(
        "Phase",
        "phase",
        MsAxis::Phase,
        &[(0.0, -15.0), (1.0, 5.0), (2.0, 30.0)],
        80,
    )];
    let page = MsScatterPagePayload {
        title: "Composite Page".to_string(),
        exprange: MsPageExportRange::All,
        gridrows: 1,
        gridcols: 2,
        header_lines: vec!["Composite header".to_string()],
        items: vec![
            MsScatterPageItemPayload {
                plotindex: 0,
                rowindex: 0,
                colindex: 0,
                plot: scatter_plot_payload(
                    "Amplitude",
                    None,
                    MsLegendPosition::ExteriorTop,
                    shared_series.clone(),
                ),
            },
            MsScatterPageItemPayload {
                plotindex: 1,
                rowindex: 0,
                colindex: 0,
                plot: scatter_plot_payload(
                    "Phase",
                    None,
                    MsLegendPosition::ExteriorTop,
                    phase_series.clone(),
                ),
            },
            MsScatterPageItemPayload {
                plotindex: 2,
                rowindex: 0,
                colindex: 1,
                plot: scatter_plot_payload(
                    "Amplitude Detail",
                    None,
                    MsLegendPosition::UpperRight,
                    vec![scatter_series(
                        "Amplitude Detail",
                        "detail",
                        MsAxis::Amplitude,
                        &[(0.0, 0.4), (1.0, 0.6), (2.0, 1.2)],
                        90,
                    )],
                ),
            },
        ],
        summary: "Composite summary".to_string(),
    };

    let output_dir = tempdir().expect("tempdir");
    let txt_path = output_dir.path().join("page.txt");
    let payload = MsPlotPayload::ScatterPage(page.clone());
    export_msexplore_plot(&payload, &txt_path).expect("export scatter manifest");

    let manifest = std::fs::read_to_string(&txt_path).expect("read manifest");
    assert!(manifest.contains("# msexplore-manifest-v1"));
    assert!(manifest.contains("Composite Page"));
    assert!(manifest.contains("# header_line=Composite header"));
    assert!(manifest.contains("1\t0\t0\tPhase\t"));
    assert!(manifest.contains("2\t0\t1\tAmplitude Detail\t"));
}

fn overplot_item(
    plotindex: usize,
    colindex: usize,
    plot: MsScatterPlotPayload,
) -> MsScatterPageItemPayload {
    MsScatterPageItemPayload {
        plotindex,
        rowindex: 0,
        colindex,
        plot,
    }
}

#[test]
fn page_overplots_must_share_axes_bounds_and_legend() {
    let amplitude = || {
        scatter_plot_payload(
            "Amplitude",
            None,
            MsLegendPosition::ExteriorRight,
            vec![scatter_series(
                "Amplitude",
                "amp",
                MsAxis::Amplitude,
                &[(0.0, 1.0), (1.0, 1.5)],
                100,
            )],
        )
    };
    let by_channel = MsScatterPlotPayload {
        x_axis: MsAxis::Channel,
        x_label: "Channel".to_string(),
        ..amplitude()
    };

    let matching = [
        overplot_item(0, 0, amplitude()),
        overplot_item(1, 0, amplitude()),
    ];
    validate_page_overplots(&matching).expect("matching overplot");
    let side_by_side = [
        overplot_item(0, 0, amplitude()),
        overplot_item(1, 1, by_channel.clone()),
    ];
    validate_page_overplots(&side_by_side).expect("different cells may differ");

    let mixed_axes = [
        overplot_item(0, 0, amplitude()),
        overplot_item(1, 0, by_channel),
    ];
    let error = validate_page_overplots(&mixed_axes).expect_err("mixed axes");
    assert!(
        error.contains("cell (0, 0) mixes incompatible axes"),
        "{error}"
    );

    let bounded = MsScatterPlotPayload {
        fixed_y_bounds: Some((0.0, 2.0)),
        ..amplitude()
    };
    let mixed_bounds = [
        overplot_item(0, 0, amplitude()),
        overplot_item(1, 0, bounded),
    ];
    let error = validate_page_overplots(&mixed_bounds).expect_err("mixed bounds");
    assert!(error.contains("incompatible fixed axis bounds"), "{error}");

    let legend = MsScatterPlotPayload {
        legend_position: MsLegendPosition::UpperRight,
        ..amplitude()
    };
    let mixed_legends = [
        overplot_item(0, 0, amplitude()),
        overplot_item(1, 0, legend),
    ];
    let error = validate_page_overplots(&mixed_legends).expect_err("mixed legends");
    assert!(error.contains("incompatible legend positions"), "{error}");
}
