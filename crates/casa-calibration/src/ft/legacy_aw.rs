// SPDX-License-Identifier: LGPL-3.0-or-later
//! Legacy imager prediction from a populated CASA AW cache.

use std::collections::BTreeMap;
use std::path::Path;

use casa_coordinates::{CoordinateModel, StokesType};
use casa_images::PagedImage;
use casa_imaging_model::PolarizationCoordinate;
use casa_imaging_operator::{
    AwCatalog, AwIndexing, AwPaRotation, GridGeometry, GridPadding, ImageExtent, NativeCacheLock,
    PolarizationRouting,
};
use casa_imaging_runtime::{Demand, HostResources, Reservation, ResourcePolicy, admit};
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::{MeasurementSet, MsResult, ResolvedMsSelection};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::epoch::EpochRef;
use casa_types::{ArrayValue, Correlation, RecordValue, ScalarValue, Value};

use crate::calibrater_lifecycle::legacy_ep::{model_geometry, reserve_model};
use crate::paths::copy_editable_table_tree;

use super::image::ImageModel;
use super::invalid;
use super::legacy_pbw::LegacyFourierImage;
use super::moving_source::first_imager_row;
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
    if ms.observation()?.string(0, "TELESCOPE_NAME")? != "VLA" {
        return Err(invalid(
            "legacy AW currently needs the original VLA (#1520)",
        ));
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
    let row = first_imager_row(ms, selected)?;
    let samples = angle_samples(ms, selected, row.field_id, row.data_desc_id)?;
    let codes = ms.polarization()?.corr_type(row.polarization_id as usize)?;
    let stokes = codes
        .iter()
        .map(|&code| {
            StokesType::from_code(code)
                .filter(|pol| {
                    matches!(
                        pol,
                        StokesType::RR | StokesType::RL | StokesType::LR | StokesType::LL
                    )
                })
                .ok_or_else(|| invalid("legacy AW needs circular correlations"))
        })
        .collect::<MsResult<Vec<_>>>()?;
    let correlations = codes
        .iter()
        .map(|&code| Correlation::from_casacore_code(code).expect("validated circular code"))
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
    if !(1.0e9..=2.0e9).contains(&reference_hz) {
        return Err(invalid(
            "legacy AW currently needs a VLA L-band model (#1520)",
        ));
    }
    let (catalog, cache_memory) = open_catalog(
        &options.cache_directory,
        &grid,
        &routing,
        reference_hz,
        options.conjugate_beams,
        None,
        false,
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
    drop(catalog);
    drop(cache_memory);
    LegacyFourierImage {
        shape,
        increment,
        coordinates,
        correlations: stokes,
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
                    let target = retained.join(name);
                    copy_editable_table_tree(&entry.path(), &target).map_err(invalid)?;
                    check_owned(&target)?;
                }
            }
            let (_checked, _memory) = open_catalog(
                &retained,
                &grid,
                &routing,
                reference_hz,
                options.conjugate_beams,
                Some(rotation(samples.clone())),
                true,
            )?;
            record.upsert("name", Value::Scalar(ScalarValue::String(MACHINE.into())));
            record.upsert(
                "casars_legacy_aw_conjbeams",
                Value::Scalar(ScalarValue::Bool(options.conjugate_beams)),
            );
            record.upsert(
                "casars_legacy_aw_angles",
                Value::Array(ArrayValue::from_f64_vec(
                    samples
                        .into_iter()
                        .flat_map(|(time, angle)| [time, f64::from(angle)])
                        .collect(),
                )),
            );
            Ok(())
        },
    )
}

fn angle_samples(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    field: i32,
    data_description: i32,
) -> MsResult<Vec<(f64, f32)>> {
    let mut integrations = BTreeMap::new();
    for row in &selected.selected_rows {
        if row.field_id != field || row.data_desc_id != data_description {
            return Err(invalid(
                "legacy AW prediction with several fields or layouts is pending #1520",
            ));
        }
        let cross = !row.flag_row && row.antenna1_id != row.antenna2_id;
        *integrations
            .entry(row.time_mjd_seconds.to_bits())
            .or_insert(false) |= cross;
    }
    let engine = MsCalEngine::new(ms)?;
    integrations
        .into_iter()
        .map(|(time, cross)| {
            let time = f64::from_bits(time);
            // The legacy visibility buffer uses the first unflagged cross
            // row's timestamp for its feed angle, and zero if there is none.
            let angle = if cross {
                engine.feed_parallactic_angle(time, field as usize, 0)?
            } else {
                zero_timestamp_pa(&engine, field as usize)?
            } as f32;
            Ok((time, angle))
        })
        .collect()
}

fn zero_timestamp_pa(engine: &MsCalEngine, field: usize) -> MsResult<f64> {
    // The VisBuffer fallback is MJD 0, outside the annual-aberration
    // ephemeris range. Precession, nutation and Earth rotation are still
    // defined there, with UT1 equal to UTC as in casacore without EOP.
    let frame = engine.antenna_frame(0., 0)?.with_dut1(0.);
    let field = engine.field_direction_j2000(field)?;
    let (right_ascension, declination) = field.convert_to(DirectionRef::JTRUE, &frame)?.as_angles();
    let sidereal = frame
        .epoch()
        .expect("antenna frame epoch")
        .convert_to(EpochRef::LAST, &frame)?
        .value()
        .frac()
        * std::f64::consts::TAU;
    let latitude = frame
        .position()
        .expect("antenna frame position")
        .latitude_rad();
    let (sin_h, cos_h) = (sidereal - right_ascension).sin_cos();
    let (sin_lat, cos_lat) = latitude.sin_cos();
    let (sin_dec, cos_dec) = declination.sin_cos();
    // The reference also uses zero polar motion where no EOP are available.
    Ok((cos_lat * sin_h).atan2(sin_lat * cos_dec - cos_lat * sin_dec * cos_h))
}

pub(crate) fn record_rotation(record: &RecordValue) -> MsResult<AwPaRotation> {
    let Some(Value::Array(ArrayValue::Float64(values))) = record.get("casars_legacy_aw_angles")
    else {
        return Err(invalid("legacy AW recipe has no visibility-buffer angles"));
    };
    let Some(values) = values.as_slice() else {
        return Err(invalid(
            "legacy AW recipe has malformed visibility-buffer angles",
        ));
    };
    let (pairs, trailing) = values.as_chunks::<2>();
    if !trailing.is_empty()
        || pairs.is_empty()
        || pairs
            .iter()
            .any(|[time, angle]| !time.is_finite() || !(*angle as f32).is_finite())
        || pairs.windows(2).any(|pair| pair[0][0] >= pair[1][0])
    {
        return Err(invalid(
            "legacy AW recipe has malformed visibility-buffer angles",
        ));
    }
    Ok(rotation(
        pairs
            .iter()
            .map(|&[time, angle]| (time, angle as f32))
            .collect(),
    ))
}

fn rotation(samples: Vec<(f64, f32)>) -> AwPaRotation {
    AwPaRotation {
        step_rad: 5_f64.to_radians(),
        require_repeatable_schedule: false,
        samples: samples.into(),
    }
}

fn check_owned(directory: &Path) -> MsResult<()> {
    for entry in std::fs::read_dir(directory).map_err(invalid)? {
        let entry = entry.map_err(invalid)?;
        let kind = entry.file_type().map_err(invalid)?;
        if kind.is_symlink() {
            return Err(invalid(format!(
                "legacy AW cache contains an external link: {}",
                entry.path().display()
            )));
        }
        if kind.is_dir() {
            check_owned(&entry.path())?;
        }
    }
    Ok(())
}

pub(crate) fn open_catalog(
    path: &Path,
    grid: &GridGeometry,
    routing: &PolarizationRouting,
    reference_hz: f64,
    conjugate_beams: bool,
    rotation: Option<AwPaRotation>,
    preload: bool,
) -> MsResult<(AwCatalog, Reservation)> {
    let index = AwIndexing {
        conjugate_beams,
        image_reference_hz: reference_hz,
        pa_tolerance_rad: std::f32::consts::TAU,
        feed_zero_angles_rad: None,
        rotation,
    };
    let terms =
        AwCatalog::open_legacy_casa(path, index, grid, routing, 16 << 20).map_err(invalid)?;
    if terms.parallactic_angles_deg().len() != 1 || terms.w_values().len() != 1 {
        return Err(invalid("this legacy AW cache geometry is pending #1520"));
    }
    let host = HostResources::detect().map_err(invalid)?;
    let charge = terms.cache_charge();
    let bound = terms.cache_bound();
    if preload {
        bound.raise(usize::try_from(charge.every_bytes()).map_err(invalid)?);
    }
    let mut memory = admit(
        &host,
        &ResourcePolicy::Balanced,
        &Demand {
            phase: "legacy AW convolution catalog",
            memory: charge.bytes(usize::from(preload), bound.bytes()),
        },
    )
    .map_err(invalid)?;
    if preload {
        terms.preload_cells().map_err(invalid)?;
        memory.retain(charge.bytes(0, bound.bytes()));
    }
    Ok((terms, memory))
}
