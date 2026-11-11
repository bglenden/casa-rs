// SPDX-License-Identifier: LGPL-3.0-or-later
//! Antenna layout and every positive/negative UV point in the observation.

use casa_ms::{
    MeasurementSet,
    columns::uvw_column::UvwColumn,
    plot_document::{PlotArrayDtype, PlotDocumentPanel, PlotLayerData, PlotLayerKind},
};

use super::document::{annotation, layer, panel};
use super::payload::{Payload, view};
use super::{Array, Run, SimobserveError, Sky, failure};
use crate::simutil::{baselines::itrf_to_local, qa};

pub(super) fn antennas(
    array: &Array,
    payload: &mut Payload,
) -> Result<PlotDocumentPanel, SimobserveError> {
    let config = &array.config;
    let positions: Vec<_> = config
        .x
        .iter()
        .zip(&config.y)
        .zip(&config.z)
        .map(|((&x, &y), &z)| [x, y, z])
        .collect();
    let n = positions.len();
    let centre = [0, 1, 2].map(|axis| positions.iter().map(|p| p[axis]).sum::<f64>() / n as f64);
    let local = itrf_to_local(&positions, centre)?;
    let bounds = |axis: usize| {
        [
            local.iter().map(|p| p[axis]).fold(f64::INFINITY, f64::min),
            local
                .iter()
                .map(|p| p[axis])
                .fold(f64::NEG_INFINITY, f64::max),
        ]
    };
    let [xb, yb] = [bounds(0), bounds(1)];
    let range = (xb[1] - xb[0]).max(yb[1] - yb[0]);
    let circles = config.dish_diameter.iter().any(|&d| d > 0.01 * range);
    let limits = [0, 1].map(|axis| {
        let lo = local
            .iter()
            .zip(&config.dish_diameter)
            .map(|(p, d)| p[axis] - if circles { d / 2.0 } else { 0.0 })
            .fold(f64::INFINITY, f64::min);
        let hi = local
            .iter()
            .zip(&config.dish_diameter)
            .map(|(p, d)| p[axis] + if circles { d / 2.0 } else { 0.0 })
            .fold(f64::NEG_INFINITY, f64::max);
        let margin = if hi > lo { (hi - lo) * 0.05 } else { 0.055 };
        [lo - margin, hi + margin]
    });
    let mut output = panel("antennas", "", ["", ""], limits, true);
    let x = payload.values(local.iter().map(|p| p[0]))?;
    let y = payload.values(local.iter().map(|p| p[1]))?;
    output.layers.push(layer(
        "pads",
        PlotLayerKind::Scatter,
        PlotLayerData::Points {
            x: x.clone(),
            y: y.clone(),
        },
        if circles { None } else { Some("#dddd66") },
        Some(if circles { "," } else { "o" }),
    ));
    if circles {
        let radii = payload.values(config.dish_diameter.iter().map(|d| d / 2.0))?;
        output.layers.push(layer(
            "dishes",
            PlotLayerKind::Circles,
            PlotLayerData::Circles {
                x,
                y,
                radii,
                filled: true,
                anchored: false,
            },
            Some("#dddd66"),
            None,
        ));
    }
    if n < 10 {
        for (i, p) in local.iter().enumerate() {
            output.annotations.push(annotation(
                &format!("pad-{i}"),
                [p[0], p[1]],
                config.pad_names[i].clone(),
                false,
            ));
        }
    }
    Ok(output)
}

pub(super) fn uv(
    run: &Run<'_>,
    sky: &Sky,
    payload: &mut Payload,
) -> Result<PlotDocumentPanel, SimobserveError> {
    let ms = MeasurementSet::open(run.measurement_set()).map_err(failure)?;
    let uvw = UvwColumn::new(ms.main_table());
    let rows = ms.main_table().row_count() as u64;
    let scale = 300.0 / qa::value_in(&sky.model.specrefval, "GHz")?;
    let start = payload.position();
    let (mut xmax, mut ymax) = (0.0_f64, 0.0_f64);
    for row in 0..ms.main_table().row_count() {
        let [u, v, _] = uvw.get(row).map_err(failure)?;
        let (u, v) = (u / scale, v / scale);
        if !u.is_finite() || !v.is_finite() {
            return Err(failure("nonfinite UV plot coordinate"));
        }
        for value in [u, v, -u, -v] {
            payload.f64(value)?;
        }
        xmax = xmax.max(u.abs());
        ymax = ymax.max(v.abs());
    }
    let mut output = panel(
        "uv",
        "",
        ["u[klambda]", "v[klambda]"],
        [[-1.1 * xmax, 1.1 * xmax], [-1.1 * ymax, 1.1 * ymax]],
        true,
    );
    // CASA's pyplot.box() toggles the initially visible UV frame off.
    output.frame_visible = Some(false);
    for (id, offset) in [("positive", 0), ("negative", 16)] {
        let x = view(start + offset, vec![rows], vec![32], PlotArrayDtype::F64Le);
        let y = view(
            start + offset + 8,
            vec![rows],
            vec![32],
            PlotArrayDtype::F64Le,
        );
        output.layers.push(layer(
            id,
            PlotLayerKind::Scatter,
            PlotLayerData::Points { x, y },
            Some("b"),
            Some(","),
        ));
    }
    Ok(output)
}
