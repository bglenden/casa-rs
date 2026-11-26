// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA simanalyze's SF grid, beam-area scaling and cubic final regrid.
use super::{
    Parameters, failure,
    imaging::context,
    plan::{Plan, suffixed},
};
use crate::{
    SessionResources,
    simobserve::SimobserveError,
    simutil::{analysis, qa},
};
use casa_contracts::builtin_surface_bundle;
use casa_coordinates::{Coordinate, CoordinateModel};
use casa_images::{ImageInterface, PagedImage, RegridMethod, RegridTemplate, regrid};
use casa_imaging_runtime::{Demand, admit};
use casa_ms::MeasurementSet;
use casa_task_runtime::resolve_plain_values;
use casa_types::quanta::Quantity;
use serde_json::json;
use std::path::{Path, PathBuf};

const ARCSEC: f64 = std::f64::consts::PI / (180. * 3600.);

pub(super) fn image(
    input: &Path,
    p: &Parameters<'_>,
    plan: &Plan,
    resources: SessionResources,
) -> Result<Vec<PathBuf>, SimobserveError> {
    let model = PagedImage::<f32>::open(&plan.model).map_err(failure)?;
    let CoordinateModel::Direction(direction) = model.coordinates().coordinate(0) else {
        return Err(failure("sky model has no direction coordinate"));
    };
    let world = direction
        .to_world(&[model.shape()[0] as f64 / 2., model.shape()[1] as f64 / 2.])
        .map_err(failure)?;
    let phasecenter = format!(
        "{} {} {}",
        direction.direction_ref().as_str(),
        qa::hms(&Quantity::new(world[0], "rad").map_err(failure)?).map_err(failure)?,
        qa::dms(&Quantity::new(world[1], "rad").map_err(failure)?).map_err(failure)?
    );
    let frequency = model.coordinates().coordinate(2).reference_value()[0];
    let nchan = model.shape()[3];
    let ms = MeasurementSet::open(input).map_err(failure)?;
    let antennas = ms.antenna().map_err(failure)?;
    let diameter = (0..antennas.row_count())
        .map(|row| antennas.dish_diameter(row))
        .collect::<Result<Vec<_>, _>>()
        .map_err(failure)?
        .iter()
        .sum::<f64>()
        / antennas.row_count() as f64;
    let primary_beam = casa_imaging_singledish::primary_beam_fwhm_arcsec(
        frequency,
        diameter,
        if diameter == 12. { 0.75 } else { 0. },
        10.,
    );
    let sfcell = primary_beam / 9.;
    let cell = plan.imaging["cell"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| qa::value_in(&qa::read(value.as_str().unwrap())?, "arcsec"))
        .collect::<Result<Vec<_>, _>>()
        .map_err(failure)?;
    if cell.iter().any(|&value| value > sfcell) {
        return Err(failure(format!(
            "SF total-power imaging requires cell <= {sfcell}arcsec"
        )));
    }
    let size = plan.imaging["imsize"].as_array().unwrap();
    let size = [
        size[0].as_u64().unwrap() as usize,
        size[1].as_u64().unwrap() as usize,
    ];
    let temporary_size =
        [0, 1].map(|axis| (cell[axis] / sfcell * size[axis] as f64).ceil() as usize);
    let project = plan.directory.file_name().unwrap().to_string_lossy();
    let output = plan.directory.join(format!("{project}.sd.image"));
    if output.exists() && !p.flag("overwrite") {
        return Err(failure("total-power image already exists"));
    }
    let temporary = suffixed(&output, "0");
    let source = std::fs::canonicalize(&plan.model).map_err(failure)?;
    for target in [
        output.clone(),
        temporary.clone(),
        suffixed(&temporary, ".scaled"),
        suffixed(&temporary, ".image"),
        suffixed(&temporary, ".weight"),
    ] {
        if target.exists() {
            if !p.flag("overwrite") {
                return Err(failure(format!("{} already exists", target.display())));
            }
            if std::fs::canonicalize(&target).map_err(failure)? == source {
                return Err(failure(
                    "sky model would be overwritten by total-power imaging",
                ));
            }
            PagedImage::<f32>::open(&target).map_err(failure)?;
        }
    }
    let values = resolve_plain_values(
        builtin_surface_bundle("tsdimaging").map_err(failure)?,
        std::env::current_dir().map_err(failure)?,
        json!({"infiles": [input], "outfile": temporary, "overwrite": p.flag("overwrite"),
            "phasecenter": phasecenter, "mode": "channel", "nchan": nchan, "start": 0, "width": 1,
            "gridfunction": "SF", "convsupport": 6, "imsize": temporary_size,
            "cell": [format!("{sfcell}arcsec"),format!("{sfcell}arcsec")]})
        .as_object()
        .unwrap(),
    )
    .map_err(failure)?;
    let gridded =
        casa_imaging_application::single_dish::image_from_parameters(&values, &context(resources))
            .map_err(failure)?;
    let beam = gridded
        .beam
        .ok_or_else(|| failure("total-power raster has no measurable beam"))?;
    let ratio = beam.major_in("arcsec").map_err(failure)?
        * beam.minor_in("arcsec").map_err(failure)?
        / primary_beam.powi(2);
    // CASA formats this immath factor with %f (six decimal places).
    let ratio: f32 = format!("{ratio:.6}").parse().map_err(failure)?;
    let _memory = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "simulation total-power scaling and regrid",
            memory: total_power_memory(temporary_size, size, nchan)?,
        },
    )
    .map_err(failure)?;
    let image = PagedImage::<f32>::open(&gridded.image).map_err(failure)?;
    let pixels = image.get().map_err(failure)?.mapv(|value| value * ratio);
    let scaled = suffixed(&temporary, ".scaled");
    let scaled_image = analysis::write_product(
        &scaled,
        &pixels,
        image.coordinates().clone(),
        image.units(),
        &image.image_info().map_err(failure)?,
        image.default_mask().map_err(failure)?.as_ref(),
    )
    .map_err(failure)?;
    let mut template = RegridTemplate::of(&scaled_image);
    let CoordinateModel::Direction(direction) = template.coordinates.coordinate(0) else {
        unreachable!()
    };
    let direction = direction
        .clone()
        .with_reference_pixel([size[0] as f64 / 2., size[1] as f64 / 2.])
        .with_increment([-cell[0] * ARCSEC, cell[1] * ARCSEC]);
    template.coordinates.replace_coordinate(0, direction);
    template.shape[..2].copy_from_slice(&size);
    let regridded = regrid(&scaled_image, &template, RegridMethod::Cubic).map_err(failure)?;
    let staging = tempfile::tempdir_in(&plan.directory).map_err(failure)?;
    let staged = staging.path().join("image");
    analysis::write_product(
        &staged,
        &regridded.data,
        regridded.coordinates,
        image.units(),
        &image.image_info().map_err(failure)?,
        Some(&regridded.mask),
    )
    .map_err(failure)?;
    casa_imaging_products::publish_staged_image(&staged, &output).map_err(failure)?;
    drop(image);
    super::replace(&temporary, true)?;
    std::fs::rename(&gridded.image, &temporary).map_err(failure)?;
    Ok(vec![temporary, gridded.weight, scaled, output])
}

fn total_power_memory(
    input: [usize; 2],
    output: [usize; 2],
    channels: usize,
) -> Result<u64, SimobserveError> {
    let grid = |shape: [usize; 2]| {
        (shape[0] as u64)
            .checked_mul(shape[1] as u64)
            .and_then(|cells| cells.checked_mul(channels as u64))
    };
    grid(input)
        .and_then(|cells| cells.checked_mul(32))
        .and_then(|bytes| {
            grid(output)
                .and_then(|cells| cells.checked_mul(48))
                .and_then(|output| bytes.checked_add(output))
        })
        .and_then(|bytes| bytes.checked_add(64 << 20))
        .ok_or_else(|| failure("total-power regrid memory overflows"))
}
