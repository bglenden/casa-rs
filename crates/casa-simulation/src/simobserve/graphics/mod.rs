// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native plot preparation for the Python host's Matplotlib renderer (#1025).

pub(crate) mod document;
pub(crate) mod ephemeris;
pub(crate) mod image;
mod model;
pub(crate) mod observation;
pub(crate) mod payload;

use casa_imaging_runtime::charge_process_caches;
use casa_ms::plot_document::PlotFigure;

use super::antennas::Array;
use super::pointings::Pointings;
use super::sky::Sky;
use super::{Run, SimobserveError};
use payload::Payload;

fn failure(error: impl std::fmt::Display) -> SimobserveError {
    SimobserveError::raised(format!("simobserve graphics: {error}"))
}

pub(super) fn prepare(
    run: &mut Run<'_>,
    sky: &Sky,
    array: &Array,
    pointings: &Pointings,
    payload: Payload,
) -> Result<(), SimobserveError> {
    let resources = run.request.options.resources;
    charge_process_caches(resources.host(), resources.policy()).map_err(failure)?;
    let mut payload = payload;
    let mut plots = Vec::new();
    let model = model::prepare(run, sky, array, pointings, &mut payload)?;
    plots.push(figure(
        run,
        ".skymodel.png",
        document::document("skymodel", vec![model], 1),
    ));
    if run.predict {
        let mut panels = vec![ephemeris::prepare(run, array, pointings, &mut payload)?];
        if run.uvmode {
            panels.push(observation::antennas(array, &mut payload)?);
            panels.push(observation::uv(run, sky, &mut payload)?);
            panels.push(image::image_panel(
                &run.file(".quick.psf"),
                &mut payload,
                resources,
                true,
            )?);
        }
        let columns = if run.uvmode { 2 } else { 1 };
        plots.push(figure(
            run,
            ".observe.png",
            document::document("observe", panels, columns),
        ));
    }
    let bytes = payload.finish()?;
    for plot in &plots {
        plot.document.validate_payload(bytes).map_err(failure)?;
    }
    run.plots = plots;
    Ok(())
}

fn figure(
    run: &Run<'_>,
    suffix: &str,
    document: casa_ms::plot_document::PlotDocument,
) -> PlotFigure {
    let mode = run.parameters.graphics.as_str();
    PlotFigure {
        output_file: matches!(mode, "file" | "both").then(|| run.file(suffix)),
        show: matches!(mode, "screen" | "both"),
        document,
    }
}

pub(super) fn open(path: &std::path::Path) -> Result<Payload, SimobserveError> {
    Payload::open(path)
}
