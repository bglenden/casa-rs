// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs;

use super::*;

#[test]
fn an_orphan_pending_figure_cannot_report_an_old_png_as_new_output() {
    let root = tempfile::tempdir().expect("directory");
    let png = root.path().join("previous.png");
    fs::write(&png, b"previous-run-png").expect("previous figure");
    let reply = serde_json::json!({
        "plots":[],"run":{"stem":"p"},"artifacts":[{
            "kind":"observe.png","label":"Observation","path":png,
            "exists":false,"render_pending":true,"resource_kind":"file"
        }]
    });
    let error = decode_plot_documents("simobserve", reply, 0).expect_err("orphan figure");
    assert!(
        error.contains("pending") && error.contains("figure"),
        "{error}"
    );
    assert_eq!(fs::read(png).expect("old figure"), b"previous-run-png");
}

#[test]
fn simulation_figures_validate_payload_bounds_and_declared_destinations() {
    use casa_plot_data::plot_document::{
        PlotArrayDtype, PlotBinaryArray, PlotDocument, PlotDocumentLayer, PlotFigure,
        PlotLayerData, PlotLayerKind, PlotPayloadStrategy,
    };
    let array = PlotBinaryArray {
        offset_bytes: 0,
        shape: vec![2],
        strides_bytes: vec![],
        dtype: PlotArrayDtype::F64Le,
    };
    let layer = PlotDocumentLayer {
        id: "points".into(),
        title: String::new(),
        kind: PlotLayerKind::Scatter,
        x_axis_id: "x".into(),
        y_axis_id: "y".into(),
        x_values: vec![],
        y_values: vec![],
        point_labels: vec![],
        point_symbol_sizes: vec![],
        interval_x_start: vec![],
        interval_x_end: vec![],
        interval_y: vec![],
        interval_height: vec![],
        interval_labels: vec![],
        provenance: vec![],
        color_group: String::new(),
        symbol_size: 1.0,
        line_width: 1.0,
        opacity: 1.0,
        source_sample_count: 2,
        payload_strategy: PlotPayloadStrategy::BinaryArrays,
        provenance_summary: String::new(),
        data: Some(PlotLayerData::Points {
            x: array.clone(),
            y: array,
        }),
        style: None,
    };
    let plot = PlotFigure {
        output_file: Some(PathBuf::from("plot.png")),
        show: false,
        document: PlotDocument {
            id: "test".into(),
            title: String::new(),
            subtitle: String::new(),
            header_lines: vec![],
            axes: vec![],
            layers: vec![layer],
            annotations: vec![],
            panels: vec![],
            show_legend: false,
            layout: None,
        },
    };
    let mut reply = serde_json::json!({
        "plots":[plot],"artifacts":[{"kind":"skymodel.png","label":"Model","path":"plot.png","exists":false,"render_pending":true,"resource_kind":"file"}]
    });
    assert_eq!(
        decode_plot_documents("simobserve", reply.clone(), 16)
            .expect("valid transport")
            .len(),
        1
    );
    assert!(
        decode_plot_documents("simobserve", reply.clone(), 15)
            .unwrap_err()
            .contains("beyond")
    );
    reply["artifacts"] = serde_json::json!([]);
    assert!(
        decode_plot_documents("simobserve", reply.clone(), 16)
            .unwrap_err()
            .contains("declared")
    );
}

#[test]
fn a_completion_reports_the_declared_product() {
    let dir = tempfile::tempdir().expect("tempdir");
    let table_path = dir.path().join("gain.cal");
    fs::create_dir(&table_path).expect("product directory");
    let completion = completion(
        "gaincal",
        r#"{"kind":"solve_gain","report":{}}"#,
        dir.path(),
        vec![("output".to_string(), "gain.cal".to_string())],
    )
    .expect("typed completion");

    assert_eq!(completion.products.len(), 1);
    let product = &completion.products[0];
    assert_eq!(product.path, table_path.display().to_string());
    assert_eq!(product.resource_kind, "CASA_TABLE");
    assert_eq!(product.role, "PRIMARY");
    assert!(product.exists);
    assert!(product.diagnostic.is_none());
}

#[test]
fn a_malformed_result_names_the_surface() {
    let dir = tempfile::tempdir().expect("tempdir");
    let error = completion("tclean", "runner stdout", dir.path(), Vec::new())
        .expect_err("malformed result");
    assert!(
        error.starts_with("unknown managed result for \"tclean\""),
        "{error}"
    );
}
