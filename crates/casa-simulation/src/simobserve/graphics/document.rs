// SPDX-License-Identifier: LGPL-3.0-or-later
//! The simulation's panels expressed through the shared plot document.

use casa_ms::plot_document::{
    PlotAspect, PlotAxisScale, PlotDocument, PlotDocumentAnnotation, PlotDocumentAxis,
    PlotDocumentLayer, PlotDocumentPanel, PlotLayerData, PlotLayerKind, PlotLayerStyle, PlotLayout,
    PlotPayloadStrategy, PlotTextStyle,
};

pub(super) fn panel(
    id: &str,
    title: &str,
    labels: [&str; 2],
    limits: [[f64; 2]; 2],
    equal: bool,
) -> PlotDocumentPanel {
    PlotDocumentPanel {
        id: id.into(),
        title: title.into(),
        axes: ["x", "y"]
            .into_iter()
            .zip(labels)
            .zip(limits)
            .map(|((id, label), [lower, upper])| PlotDocumentAxis {
                id: id.into(),
                label: label.into(),
                unit: String::new(),
                lower,
                upper,
                scale: PlotAxisScale::Linear,
                lane_labels: vec![],
                draws_on_trailing_edge: false,
            })
            .collect(),
        layers: vec![],
        annotations: vec![],
        aspect: Some(if equal {
            PlotAspect::EqualDataLimits
        } else {
            PlotAspect::Auto
        }),
        frame_visible: None,
    }
}

pub(super) fn document(id: &str, panels: Vec<PlotDocumentPanel>, columns: u32) -> PlotDocument {
    let rows = (panels.len() as u32).div_ceil(columns);
    PlotDocument {
        id: id.into(),
        title: String::new(),
        subtitle: String::new(),
        header_lines: vec![],
        axes: vec![],
        layers: vec![],
        annotations: vec![],
        panels,
        show_legend: false,
        layout: Some(PlotLayout {
            rows,
            columns,
            aspect: None,
            margins: (columns > 1).then_some([0.05, 0.98, 0.09, 0.95, 0.2, 0.2]),
            font_size: Some("x-small".into()),
        }),
    }
}

pub(super) fn layer(
    id: &str,
    kind: PlotLayerKind,
    data: PlotLayerData,
    color: Option<&str>,
    marker: Option<&str>,
) -> PlotDocumentLayer {
    let count = match &data {
        PlotLayerData::Points { x, .. } | PlotLayerData::Circles { x, .. } => x.shape[0],
        PlotLayerData::Image { pixels, .. } => pixels.shape.iter().product(),
    };
    PlotDocumentLayer {
        id: id.into(),
        title: String::new(),
        kind,
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
        symbol_size: 9.0,
        line_width: 1.5,
        opacity: 1.0,
        source_sample_count: count,
        payload_strategy: PlotPayloadStrategy::BinaryArrays,
        provenance_summary: "All native simulation samples; no thinning".into(),
        data: Some(data),
        style: Some(PlotLayerStyle {
            color: color.map(str::to_string),
            marker: marker.map(str::to_string),
            line_style: None,
        }),
    }
}

pub(super) fn annotation(
    id: &str,
    position: [f64; 2],
    text: String,
    relative: bool,
) -> PlotDocumentAnnotation {
    PlotDocumentAnnotation {
        id: id.into(),
        x: position[0],
        y: position[1],
        text,
        style: Some(PlotTextStyle {
            axes_relative: relative,
            horizontal_alignment: Some(if relative { "left" } else { "center" }.into()),
            vertical_alignment: Some(if relative { "top" } else { "center" }.into()),
            font_size: relative.then(|| "x-small".into()),
            background: relative.then(|| "white".into()),
        }),
    }
}
