// SPDX-License-Identifier: LGPL-3.0-or-later
//! Legacy imager prediction from a populated CASA AW cache.

use std::path::Path;

use casa_coordinates::CoordinateModel;
use casa_images::PagedImage;
use casa_imaging_model::PolarizationCoordinate;
use casa_imaging_operator::{
    AwCatalog, AwIndexing, GridGeometry, GridPadding, ImageExtent, NativeCacheLock,
    PolarizationRouting,
};
use casa_imaging_runtime::{Demand, HostResources, Reservation, ResourcePolicy, admit};
use casa_ms::{MeasurementSet, MsResult, ResolvedMsSelection};
use casa_types::{Correlation, ScalarValue, Value};

use crate::calibrater_lifecycle::legacy_ep::{first_buffer, model_geometry, reserve_model};
use crate::paths::copy_dir_recursive;

use super::image::ImageModel;
use super::invalid;
use super::legacy_pbw::LegacyFourierImage;
use super::settings::{ImagerLegacyAwOptions, PredictionGeometry};

pub(crate) const CACHE: &str = "CASARS_LEGACY_AW_CACHE";
pub(crate) const MACHINE: &str = "AWProjectWBFT";

pub(super) fn prepare_model(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    path: &Path,
    geometry: &PredictionGeometry,
    options: &ImagerLegacyAwOptions,
    keep_zero: bool,
    scratch: bool,
) -> MsResult<Option<ImageModel>> {
    if options.w_planes > 1
        || options.apply_pointing_offsets
        || options.pa_step_deg != 360.
        || options.rotation_step_deg != 5.
    {
        return Err(invalid("this legacy AW configuration is pending #1520"));
    }
    if !options.cache_directory.is_dir() {
        return Err(invalid("legacy AW needs an existing CASA cache (#1520)"));
    }
    let _cache_lock = NativeCacheLock::shared(&options.cache_directory).map_err(invalid)?;
    let model = PagedImage::<f32>::open(path).map_err(invalid)?;
    let (shape, increment, coordinates) = model_geometry(&model)?;
    let [side, _, _, _] = shape;
    let host = HostResources::detect().map_err(invalid)?;
    let _memory = reserve_model(&host, side)?;
    let data = model.get().map_err(invalid)?;
    if data.iter().any(|sample| !sample.is_finite()) {
        return Err(invalid("legacy AW model contains a non-finite image cell"));
    }
    let nonzero = data.iter().any(|&sample| sample != 0.);
    let buffer = first_buffer(ms, selected)?;
    let correlations = buffer
        .correlations
        .iter()
        .map(|&pol| Correlation::from_casacore_code(pol as i32).expect("validated circular code"))
        .collect::<Vec<_>>();
    let routing = PolarizationRouting::from_saved_planes(
        &correlations,
        &[
            PolarizationCoordinate::CircularRr,
            PolarizationCoordinate::CircularRl,
            PolarizationCoordinate::CircularLr,
            PolarizationCoordinate::CircularLl,
        ],
    )
    .map_err(invalid)?;
    let grid = GridGeometry::new(
        ImageExtent {
            shape: [side; 2],
            increment_rad: increment,
            reference_pixel: [side / 2; 2],
        },
        GridPadding::None,
    )
    .map_err(invalid)?;
    let CoordinateModel::Spectral(spectral) = coordinates.coordinate(2) else {
        unreachable!("validated legacy AW frequency");
    };
    let reference_hz = spectral.world_frequency_hz(0.).map_err(invalid)?;
    let (catalog, _cache_memory) = open_catalog(
        &options.cache_directory,
        &grid,
        &routing,
        reference_hz,
        options.conjugate_beams,
    )?;
    let beam = options.cache_directory.join("avgPB");
    let average = beam
        .is_dir()
        .then(|| -> MsResult<_> {
            let beam = PagedImage::<f32>::open(beam).map_err(invalid)?;
            if beam.shape() != shape {
                return Err(invalid(
                    "legacy AW average beam has a different image shape",
                ));
            }
            beam.get().map_err(invalid)
        })
        .transpose()?;
    let cutoff = options
        .correct_primary_beam
        .then_some(options.primary_beam_limit as f32);
    let fourier = catalog
        .prepare_legacy_model(&data, average.as_ref(), cutoff)
        .map_err(invalid)?;
    LegacyFourierImage {
        shape,
        increment,
        coordinates,
        correlations: buffer.correlations,
        fourier,
        nonzero,
    }
    .publish(
        ms,
        selected,
        geometry,
        keep_zero,
        scratch,
        |path, record| {
            let retained = path.join(CACHE);
            for entry in std::fs::read_dir(&options.cache_directory).map_err(invalid)? {
                let entry = entry.map_err(invalid)?;
                let name = entry.file_name();
                let name_text = name.to_string_lossy();
                if name_text.starts_with("CFS_") || name_text.starts_with("WTCFS_") {
                    copy_dir_recursive(&entry.path(), &retained.join(name)).map_err(invalid)?;
                }
            }
            record.upsert("name", Value::Scalar(ScalarValue::String(MACHINE.into())));
            record.upsert(
                "casars_legacy_aw_conjbeams",
                Value::Scalar(ScalarValue::Bool(options.conjugate_beams)),
            );
            Ok(())
        },
    )
}

pub(crate) fn open_catalog(
    path: &Path,
    grid: &GridGeometry,
    routing: &PolarizationRouting,
    reference_hz: f64,
    conjugate_beams: bool,
) -> MsResult<(AwCatalog, Reservation)> {
    let index = AwIndexing {
        conjugate_beams,
        image_reference_hz: reference_hz,
        pa_tolerance_rad: std::f32::consts::TAU,
        feed_zero_angles_rad: None,
        rotation: None,
    };
    let terms =
        AwCatalog::open_legacy_casa(path, index, grid, routing, 16 << 20).map_err(invalid)?;
    if terms.parallactic_angles_deg().len() != 1 || terms.w_values().len() != 1 {
        return Err(invalid("this legacy AW cache geometry is pending #1520"));
    }
    let host = HostResources::detect().map_err(invalid)?;
    let memory = admit(
        &host,
        &ResourcePolicy::Balanced,
        &Demand {
            phase: "legacy AW convolution catalog",
            memory: terms.charge_bytes(1),
        },
    )
    .map_err(invalid)?;
    Ok((terms, memory))
}
