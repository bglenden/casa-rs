// SPDX-License-Identifier: LGPL-3.0-or-later
//! Complete model pixels or component positions, plus the mosaic pointings.

use casa_components::ComponentList;
use casa_simulation::{SimobservePlotInput, simobserve::SimobserveError, simutil::direction};

use super::document::{layer, panel};
use super::failure;
use super::image::image_panel;
use crate::plot_document::{
    PlotArrayDtype, PlotDocumentPanel, PlotLayerData, PlotLayerKind, PlotPayloadWriter,
    plot_array_view as view,
};

pub(super) fn prepare(
    input: &SimobservePlotInput<'_>,
    payload: &mut PlotPayloadWriter,
) -> Result<PlotDocumentPanel, SimobserveError> {
    let mut output = if input.components_only {
        component_panel(input, payload)?
    } else {
        image_panel(input.sky_flat, payload, input.resources, false)?
    };
    let beam = input.primary_beam_arcsec;
    let [x, y] = [&output.axes[0], &output.axes[1]];
    let xhalf = x.lower.abs().max(x.upper.abs());
    let yhalf = y.lower.abs().max(y.upper.abs());
    let color = if x.lower.max(x.upper).max(y.lower.max(y.upper)) > beam && !input.components_only {
        "w"
    } else {
        "k"
    };
    let offsets = &input.pointing_offsets_deg;
    let x = payload
        .values(offsets.iter().map(|p| p[0] * 3600.0))
        .map_err(failure)?;
    let y = payload
        .values(offsets.iter().map(|p| p[1] * 3600.0))
        .map_err(failure)?;
    let many = offsets.len() > 19 || beam <= 0.0;
    if many {
        output.layers.push(layer(
            "pointings",
            PlotLayerKind::Scatter,
            PlotLayerData::Points { x, y },
            Some(color),
            Some("+"),
        ));
    } else {
        let offset = payload.position();
        payload.f64(beam / 2.0).map_err(failure)?;
        let radii = view(
            offset,
            vec![offsets.len() as u64],
            vec![0],
            PlotArrayDtype::F64Le,
        );
        output.layers.push(layer(
            "pointings",
            PlotLayerKind::Circles,
            PlotLayerData::Circles {
                x,
                y,
                radii,
                filled: false,
                anchored: false,
            },
            Some(color),
            None,
        ));
    }
    if many && beam > 0.0 {
        let x = payload.values([beam]).map_err(failure)?;
        let y = payload.values([beam]).map_err(failure)?;
        let radii = payload.values([beam / 2.0]).map_err(failure)?;
        let mut legend = layer(
            "primary-beam",
            PlotLayerKind::Circles,
            PlotLayerData::Circles {
                x,
                y,
                radii,
                filled: false,
                anchored: true,
            },
            Some(color),
            None,
        );
        legend.line_width = 2.0;
        output.layers.push(legend);
    }
    let xhalf = xhalf.max(beam / 2.0);
    let yhalf = yhalf.max(beam / 2.0);
    output.axes[0].lower = xhalf;
    output.axes[0].upper = -xhalf;
    output.axes[0].label = "resized model sky".into();
    output.axes[1].lower = -yhalf;
    output.axes[1].upper = yhalf;
    Ok(output)
}

fn component_panel(
    input: &SimobservePlotInput<'_>,
    payload: &mut PlotPayloadWriter,
) -> Result<PlotDocumentPanel, SimobserveError> {
    let list = ComponentList::read_table(std::path::Path::new(input.complist)).map_err(failure)?;
    let (_, centre) = direction::direction_measure(input.sky_refdir)?;
    let arcsec = 180.0 * 3600.0 / std::f64::consts::PI;
    let points: Vec<_> = list
        .components()
        .iter()
        .map(|component| {
            let d = component.shape().reference_direction();
            let lon = direction::wrap_angle(
                d.longitude_rad() - centre[0],
                0.0,
                2.0 * std::f64::consts::PI,
            );
            [
                lon * centre[1].cos() * arcsec,
                (d.latitude_rad() - centre[1]) * arcsec,
            ]
        })
        .collect();
    let half = |axis: usize| {
        let lo = points.iter().map(|p| p[axis]).fold(f64::INFINITY, f64::min);
        let hi = points
            .iter()
            .map(|p| p[axis])
            .fold(f64::NEG_INFINITY, f64::max);
        lo.abs().max(hi.abs()) + if hi > lo { (hi - lo) * 0.05 } else { 0.055 }
    };
    let (xhalf, yhalf) = (half(0), half(1));
    let mut output = panel(
        "components",
        "",
        ["", ""],
        [[-xhalf, xhalf], [-yhalf, yhalf]],
        true,
    );
    let x = payload
        .values(points.iter().map(|p| p[0]))
        .map_err(failure)?;
    let y = payload
        .values(points.iter().map(|p| p[1]))
        .map_err(failure)?;
    output.layers.push(layer(
        "components",
        PlotLayerKind::Scatter,
        PlotLayerData::Points { x, y },
        Some("#dddd66"),
        Some("o"),
    ));
    Ok(output)
}
