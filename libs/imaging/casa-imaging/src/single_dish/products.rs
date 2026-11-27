// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA image/weight publication and the single-dish raster beam.
use super::{TotalPowerError, TotalPowerOutcome, TotalPowerRequest, failure, grid_observation};
use crate::RunContext;
use casa_coordinates::{Coordinate, CoordinateModel};
use casa_images::{GaussianBeam, ImageBeamSet, ImageInfo, PagedImage};
use casa_imaging_products::publish_staged_image;
use casa_imaging_runtime::{Demand, Reservation, admit};
use casa_imaging_singledish::{
    BeamSetup, GridFunction, KernelTable, beam_sampling, raster_sampling, theoretical_beam_arcsec,
    weight_mask_threshold,
};
use casa_numerics::PrimaryBeam;
use casa_observation::Observation;
use std::path::{Path, PathBuf};

const ARCSEC: f64 = std::f64::consts::PI / (180. * 3600.);

/// Naming, overwrite, mask and brightness policy for a total-power image.
pub struct TotalPowerProducts {
    /// CASA's base `outfile`; products append `.image` and `.weight`.
    pub outfile: PathBuf,
    /// Replace the task's previous CASA images after preparation succeeds.
    pub overwrite: bool,
    /// Fraction of the median positive weight below which pixels are masked.
    pub minweight: f64,
    /// Image brightness label, normally `Jy/beam` or `K`.
    pub brightness_unit: String,
}

/// Published products and the beam needed for simulation flux scaling.
pub struct PublishedTotalPower {
    /// Gridded image.
    pub image: PathBuf,
    /// Sum-of-weights image.
    pub weight: PathBuf,
    /// Circular raster/kernel beam, when sampling is measurable.
    pub beam: Option<GaussianBeam>,
    _metadata: Reservation,
}

/// Grid and publish both CASA products, retaining previous results through
/// preparation, gridding and beam estimation.
///
/// # Errors
/// The gridding errors, invalid output/mask settings or image I/O failure.
pub fn image_observation(
    observation: &dyn Observation,
    request: &TotalPowerRequest,
    products: &TotalPowerProducts,
    context: &RunContext,
) -> Result<PublishedTotalPower, TotalPowerError> {
    let prepared = prepare(products, context)?;
    let result = grid_observation(observation, request, context)?;
    publish_prepared(observation, request, products, context, result, prepared)
}

/// Publish an admitted grid using the task's normal beam, mask and safety checks.
/// This also supports callers that retain a grid before choosing its output.
///
/// # Errors
/// Invalid publication settings, resource admission, beam estimation or image I/O.
pub fn publish_total_power(
    observation: &dyn Observation,
    request: &TotalPowerRequest,
    products: &TotalPowerProducts,
    context: &RunContext,
    result: TotalPowerOutcome,
) -> Result<PublishedTotalPower, TotalPowerError> {
    let prepared = prepare(products, context)?;
    publish_prepared(observation, request, products, context, result, prepared)
}

struct PreparedPublication {
    metadata: Reservation,
    image: PathBuf,
    weight: PathBuf,
}

fn prepare(
    products: &TotalPowerProducts,
    context: &RunContext,
) -> Result<PreparedPublication, TotalPowerError> {
    let metadata = admit(
        &context.host,
        &context.policy,
        &Demand {
            phase: "total-power product metadata",
            memory: 32 << 10,
        },
    )
    .map_err(failure)?;
    if !products.minweight.is_finite() || products.minweight < 0. {
        return Err(failure("minweight must be finite and nonnegative"));
    }
    let outfile = std::path::absolute(&products.outfile).map_err(failure)?;
    let target = |suffix: &str| {
        let mut p = outfile.as_os_str().to_os_string();
        p.push(suffix);
        PathBuf::from(p)
    };
    let image = target(".image");
    let weight = target(".weight");
    for path in [&image, &weight] {
        if path.try_exists().map_err(failure)? {
            if !products.overwrite {
                return Err(failure(format!("{} already exists", path.display())));
            }
            // An overwrite never replaces a different scientific dataset.
            PagedImage::<f32>::open(path).map_err(failure)?;
        }
    }
    Ok(PreparedPublication {
        metadata,
        image,
        weight,
    })
}

fn publish_prepared(
    observation: &dyn Observation,
    request: &TotalPowerRequest,
    products: &TotalPowerProducts,
    context: &RunContext,
    result: TotalPowerOutcome,
    prepared: PreparedPublication,
) -> Result<PublishedTotalPower, TotalPowerError> {
    let PreparedPublication {
        metadata,
        image,
        weight,
    } = prepared;
    let cells = result.arrays.value.image.len() as u64;
    let _scratch = admit(
        &context.host,
        &context.policy,
        &Demand {
            phase: "total-power masks, beam and publication",
            memory: cells
                .saturating_mul(5)
                .saturating_add(16 << 20)
                .saturating_add(casa_imaging_singledish::raster_sampling_working_bytes(
                    result.sampling.len(),
                )),
        },
    )
    .map_err(failure)?;
    let beam = beam(observation, request, &result)?;
    if context.cancel.is_cancelled() {
        return Err(failure("cancelled before publication"));
    }
    let parent = image
        .parent()
        .ok_or_else(|| failure("output has no parent"))?;
    let staging = tempfile::Builder::new()
        .prefix(".tsdimaging-")
        .tempdir_in(parent)
        .map_err(failure)?;
    let staged_image = staging.path().join("image");
    let staged_weight = staging.path().join("weight");
    write(&staged_image, &result, products, beam)?;
    let mut weights = PagedImage::<f32>::create(
        result.arrays.value.weight.shape().to_vec(),
        result.coordinates.clone(),
        &staged_weight,
    )
    .map_err(failure)?;
    weights
        .put_slice(&result.arrays.value.weight, &[0; 4])
        .map_err(failure)?;
    weights.save().map_err(failure)?;
    drop(weights);
    if context.cancel.is_cancelled() {
        return Err(failure("cancelled before publication"));
    }
    publish_staged_image(&staged_image, &image).map_err(failure)?;
    publish_staged_image(&staged_weight, &weight).map_err(failure)?;
    Ok(PublishedTotalPower {
        image,
        weight,
        beam,
        _metadata: metadata,
    })
}

fn write(
    path: &Path,
    result: &TotalPowerOutcome,
    products: &TotalPowerProducts,
    beam: Option<GaussianBeam>,
) -> Result<(), TotalPowerError> {
    let data = &result.arrays.value;
    let mut image = PagedImage::<f32>::create(
        data.image.shape().to_vec(),
        result.coordinates.clone(),
        path,
    )
    .map_err(failure)?;
    image.put_slice(&data.image, &[0; 4]).map_err(failure)?;
    image
        .set_units(&products.brightness_unit)
        .map_err(failure)?;
    if let Some(beam) = beam {
        image
            .set_image_info(&ImageInfo {
                beam_set: ImageBeamSet::new(beam),
                ..Default::default()
            })
            .map_err(failure)?;
    }
    if let Some(threshold) = weight_mask_threshold(
        data.weight
            .as_slice_memory_order()
            .expect("contiguous grid"),
        products.minweight,
    ) {
        let mask = data.weight.mapv(|value| f64::from(value) > threshold);
        image.put_mask("mask0", &mask).map_err(failure)?;
        image.set_default_mask("mask0").map_err(failure)?;
    }
    image.save().map_err(failure)
}

fn beam(
    observation: &dyn Observation,
    request: &TotalPowerRequest,
    result: &TotalPowerOutcome,
) -> Result<Option<GaussianBeam>, TotalPowerError> {
    let Some(raster) = raster_sampling(&result.sampling) else {
        return Ok(None);
    };
    let Some(sampling) = beam_sampling(raster.interval_rad.map(|v| v / ARCSEC)) else {
        return Ok(None);
    };
    let catalogue = observation.catalogue();
    let entry = catalogue.partition(result.source_partition);
    let antenna = &catalogue.antennas[entry
        .baselines
        .iter()
        .find(|b| {
            b.is_auto() && (request.antennas.is_empty() || request.antennas.contains(&b.antenna1))
        })
        .ok_or_else(|| failure("no selected autocorrelation"))?
        .antenna1
        .0 as usize];
    let CoordinateModel::Spectral(spectral) = result.coordinates.coordinate(2) else {
        return Err(failure("no spectral coordinate"));
    };
    let frequency = spectral.rest_frequency();
    let kernel = if request.kernel == GridFunction::PrimaryBeam {
        let primary = PrimaryBeam::for_telescope(
            &catalogue.observation(entry.observation).telescope,
            spectral.reference_value()[0],
        )
        .ok_or_else(|| failure("unsupported primary beam"))?;
        KernelTable::primary_beam(
            &primary.pattern(),
            request.direction.increment()[0],
            spectral.reference_value()[0],
        )
    } else {
        KernelTable::new(request.kernel).map_err(failure)?
    };
    let fwhm = theoretical_beam_arcsec(&BeamSetup {
        diameter_m: antenna
            .dish_diameter
            .get_value_in(&casa_core::quanta::Unit::new("m").map_err(failure)?)
            .map_err(failure)?,
        obscuration_m: if ["PM", "DV", "DA", "CM"]
            .iter()
            .any(|prefix| antenna.name.starts_with(prefix))
        {
            0.75
        } else {
            0.
        },
        frequency_hz: frequency,
        cell_arcsec: [
            request.direction.increment()[0].abs() / ARCSEC,
            request.direction.increment()[1].abs() / ARCSEC,
        ],
        kernel: (request.kernel, &kernel),
        sampling_arcsec: &sampling,
    });
    if !fwhm.is_finite() || fwhm <= 0. {
        return Err(failure("invalid theoretical beam"));
    }
    Ok(Some(GaussianBeam::new(fwhm * ARCSEC, fwhm * ARCSEC, 0.)))
}
