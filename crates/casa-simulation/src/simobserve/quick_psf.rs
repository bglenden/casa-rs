// SPDX-License-Identifier: LGPL-3.0-or-later
//! The numerical quick PSF used by simobserve's observation figure (#1025).
//! CASA's legacy imager uses natural weighting, Stokes I, the first model
//! cell on both axes and at least 128 pixels, enlarged to eight resolution
//! elements. The existing imaging application owns gridding and publication.

use std::f64::consts::PI;
use std::path::Path;

use casa_images::{ImageBeamSet, PagedImage};
use casa_imaging_application::{
    BackendRequest as ImagingBackend, Cancel, ImagingRequest, RunContext, execute,
};
use casa_imaging_runtime::charge_process_caches;
use casa_ms::MeasurementSet;
use casa_ms::columns::uvw_column::UvwColumn;
use casa_provider_contracts::builtin_surface_bundle;
use casa_task_runtime::resolve_plain_values;
use serde_json::json;

use super::sky::Sky;
use super::{Run, SimobserveError, make_way};
use crate::simutil::qa;

fn failure(error: impl std::fmt::Display) -> SimobserveError {
    SimobserveError::raised(format!("simobserve quick PSF: {error}"))
}

/// CASA deliberately uses the largest positive u or v, rather than the
/// longest absolute baseline. Preserve that sizing rule. MAIN is read a
/// row at a time, so this survey does not retain every UV point.
fn image_size(
    ms: &Path,
    cell_arcsec: f64,
    frequency_ghz: f64,
    resources: crate::SessionResources,
) -> Result<usize, SimobserveError> {
    charge_process_caches(resources.host(), resources.policy()).map_err(failure)?;
    let ms = MeasurementSet::open(ms).map_err(failure)?;
    let uvw = UvwColumn::new(ms.main_table());
    let mut maximum = f64::NEG_INFINITY;
    for row in 0..ms.main_table().row_count() {
        let [u, v, _] = uvw.get(row).map_err(failure)?;
        if !u.is_finite() || !v.is_finite() {
            return Err(failure("nonfinite UVW"));
        }
        maximum = maximum.max(u).max(v);
    }
    if maximum <= 0.0 {
        return Err(failure("an interferometric PSF needs positive u or v"));
    }
    let resolution = 0.3 / frequency_ghz / maximum * 3600.0 * 180.0 / PI;
    let elements = (resolution / cell_arcsec).floor();
    if !elements.is_finite() || elements < 0.0 || elements > (usize::MAX / 8) as f64 {
        return Err(failure("the requested PSF size is not representable"));
    }
    (elements as usize)
        .checked_mul(8)
        .map(|size| size.max(128))
        .ok_or_else(|| failure("the requested PSF size is not representable"))
}

pub(super) fn prepare(run: &mut Run<'_>, sky: &Sky) -> Result<(), SimobserveError> {
    if !run.predict
        || !run.uvmode
        || !matches!(run.parameters.graphics.as_str(), "both" | "file" | "screen")
    {
        return Ok(());
    }
    let ms = run.measurement_set();
    let cell = qa::value_in(&sky.model.cell[0], "arcsec")?;
    let frequency = qa::value_in(&sky.model.specrefval, "GHz")?;
    let size = image_size(&ms, cell, frequency, run.request.options.resources)?;
    let scratch = tempfile::Builder::new()
        .prefix(".quick-psf-")
        .tempdir_in(&run.directory)
        .map_err(failure)?;
    let prefix = scratch.path().join("beam");
    // Resolve the canonical defaults once; do not copy tclean's defaults
    // into this caller or compose its execution stages here.
    let values = json!({
        "vis": ms, "imagename": prefix, "imsize": [size, size],
        "cell": format!("{cell}arcsec"), "datacolumn": "data",
        "gridder": "standard", "weighting": "natural", "stokes": "I",
        "niter": 0, "calcpsf": true, "calcres": false, "restoration": false,
    });
    let workspace = std::env::current_dir().map_err(failure)?;
    let resolved = resolve_plain_values(
        builtin_surface_bundle("tclean").map_err(failure)?,
        workspace,
        values.as_object().expect("an object"),
    )
    .map_err(failure)?;
    let mut request = ImagingRequest::from_tclean(&resolved).map_err(failure)?;
    let resources = run.request.options.resources;
    // This small diagnostic follows the legacy imager's double-precision
    // grid. At padding 1.0 the edge correction amplifies Metal FFT rounding
    // past the backend law (4.27e-4 versus 1e-4 on the CASA fixture).
    // The preceding simulation still predicts DATA on its requested device.
    request.backend = ImagingBackend::Cpu;
    request.gridprecision = Some(casa_imaging_application::GridPrecision::F64);
    // Imager::createFTMachine uses padding=1.0 for a single field,
    // unlike tclean's 1.2. Both still use composite-sized FFT grids.
    request.ft_padding = Some(casa_imaging_operator::PaddingFactor::new(1.0).map_err(failure)?);
    execute(
        &request,
        RunContext {
            host: *resources.host(),
            policy: *resources.policy(),
            cancel: Cancel::new(),
            summary: None,
        },
    )
    .map_err(failure)?;

    let source = prefix.with_extension("psf");
    let mut psf = PagedImage::<f32>::open(&source).map_err(failure)?;
    // imager.approximatepsf publishes Jy/pixel without a restoring beam;
    // simobserve fits a beam for its annotation without storing it there.
    psf.set_units("Jy/pixel").map_err(failure)?;
    let mut info = psf.image_info().map_err(failure)?;
    info.beam_set = ImageBeamSet::default();
    psf.set_image_info(&info).map_err(failure)?;
    psf.save().map_err(failure)?;
    drop(psf);
    let target = run.file(".quick.psf");
    make_way(&target)?;
    std::fs::rename(&source, &target).map_err(failure)?;
    run.wrote(&target);
    Ok(())
}
