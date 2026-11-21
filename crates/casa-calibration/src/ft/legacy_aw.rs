// SPDX-License-Identifier: LGPL-3.0-or-later
//! Legacy imager prediction from a populated CASA AW cache.

use std::collections::BTreeMap;
use std::path::Path;

use casa_coordinates::{CoordinateModel, StokesType};
use casa_images::PagedImage;
use casa_imaging_model::{
    NativeAwFrequencyGroup, NativeAwGrid, NativeAwRequestInput, NativeAwTerms,
    PolarizationCoordinate,
};
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

use crate::calibrater_lifecycle::legacy_ep::{legacy_vla_surface, model_geometry, reserve_model};
use crate::paths::copy_editable_table_tree;

use super::image::ImageModel;
use super::invalid;
use super::legacy_pbw::LegacyFourierImage;
use super::moving_source::first_imager_row;
use super::settings::{ImagerLegacyAwOptions, PredictionGeometry};

pub(crate) const CACHE: &str = "CASARS_LEGACY_AW_CACHE";
pub(crate) const MACHINE: &str = "AWProjectWBFT";
const COLD_STAGE: &str = ".casars-legacy-aw-";
const OWNED_STAGE: &str = "preparing";
const CELL_JOURNAL: &str = "promoting";

pub(super) fn prepare_model(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    model: &PagedImage<f32>,
    geometry: &PredictionGeometry,
    (options, switches): &(ImagerLegacyAwOptions, [bool; 2]),
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
    if options.cache_directory.as_os_str().is_empty() {
        return Err(invalid("legacy AW requires a convolution cache directory"));
    }
    if ms.observation()?.string(0, "TELESCOPE_NAME")? != "VLA" {
        return Err(invalid(
            "legacy AW currently needs the original VLA (#1520)",
        ));
    }
    let (shape, increment, coordinates) = model_geometry(model)?;
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
    let _cache_lock = ready_cache(
        &options.cache_directory,
        || {
            if options.correct_primary_beam || !options.wideband || !switches[1] {
                return Err(invalid(
                    "this cold legacy AW term configuration is pending #1520",
                ));
            }
            let spw = row.spectral_window_id;
            let frequencies = ms.spectral_window()?.chan_freq(spw as usize)?;
            let channels = match selected.channel_selections.get(&spw) {
                Some(selection) => selection.indices(frequencies.len())?,
                None => (0..frequencies.len()).collect(),
            };
            let channel_frequencies_hz = channels
                .into_iter()
                .map(|channel| frequencies[channel])
                .collect::<Vec<_>>();
            let cf_frequency_hz = channel_frequencies_hz.iter().copied().fold(0_f64, f64::max);
            let (_, pa) = samples
                .first()
                .ok_or_else(|| invalid("legacy AW has no first visibility buffer"))?;
            let fft_side = 2048;
            let oversampling = 20;
            let input = NativeAwRequestInput {
                surface: legacy_vla_surface()?,
                antenna_diameter_m: 25.,
                frequencies: vec![NativeAwFrequencyGroup {
                    spectral_window: spw.try_into().map_err(invalid)?,
                    channel_frequencies_hz,
                    cf_frequency_hz,
                }],
                w_values: vec![0.],
                w_increment: 0.,
                pa_values: vec![f64::from(*pa)],
                mueller_elements: vec![0, 15],
                reference_frequency_hz: reference_hz,
                grid: NativeAwGrid {
                    size: fft_side,
                    sky_increment_rad: increment
                        .map(|cell| cell * (oversampling * side) as f64 / fft_side as f64),
                    oversampling,
                },
                terms: NativeAwTerms {
                    aperture: switches[1],
                    w_term: false,
                    prolate_spheroidal: switches[0],
                    wideband: options.wideband,
                    conjugate_beams: options.conjugate_beams,
                },
                maximum_cells: 2,
            };
            Ok(input)
        },
        &host,
        geometry.native_threads,
    )?;
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
                if is_cell(&name_text) {
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

fn is_cell(name: &str) -> bool {
    name.starts_with("CFS_") || name.starts_with("WTCFS_")
}

fn has_cells(root: &Path) -> MsResult<bool> {
    for item in std::fs::read_dir(root).map_err(invalid)? {
        if is_cell(&item.map_err(invalid)?.file_name().to_string_lossy()) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn staged_caches(root: &Path) -> MsResult<Vec<std::path::PathBuf>> {
    let mut stages = Vec::new();
    for item in std::fs::read_dir(root).map_err(invalid)? {
        let item = item.map_err(invalid)?;
        if item.file_name().to_string_lossy().starts_with(COLD_STAGE)
            && item.file_type().map_err(invalid)?.is_dir()
            && item.path().join(OWNED_STAGE).is_file()
        {
            stages.push(item.path());
        }
    }
    Ok(stages)
}

fn recover_stage(root: &Path, stage: &Path) -> MsResult<()> {
    match std::fs::read_to_string(stage.join(CELL_JOURNAL)) {
        Ok(journal) => {
            for name in journal.lines() {
                if !is_cell(name) || Path::new(name).components().count() != 1 {
                    return Err(invalid("invalid legacy A/W cache journal"));
                }
                let target = root.join(name);
                if !stage.join(name).exists() && target.is_dir() {
                    std::fs::remove_dir_all(target).map_err(invalid)?;
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(invalid(error)),
    }
    std::fs::remove_dir_all(stage).map_err(invalid)
}

fn ready_cache(
    root: &Path,
    request: impl FnOnce() -> MsResult<NativeAwRequestInput>,
    host: &HostResources,
    desired_workers: i32,
) -> MsResult<NativeCacheLock> {
    let lock = NativeCacheLock::shared(root).map_err(invalid)?;
    if has_cells(root)? && staged_caches(root)?.is_empty() {
        return Ok(lock);
    }
    drop(lock);
    let lock = NativeCacheLock::exclusive(root).map_err(invalid)?;
    for stage in staged_caches(root)? {
        recover_stage(root, &stage)?;
    }
    if !has_cells(root)? {
        let input = request()?;
        let workers = if desired_workers > 1 { 2 } else { 1 };
        let _memory = admit(
            host,
            &ResourcePolicy::Balanced,
            &Demand {
                phase: "legacy A/W cold generation",
                memory: AwCatalog::generation_bytes(&input, workers),
            },
        )
        .map_err(invalid)?;
        let stage = tempfile::Builder::new()
            .prefix(COLD_STAGE)
            .tempdir_in(root)
            .map_err(invalid)?;
        std::fs::File::create(stage.path().join(OWNED_STAGE)).map_err(invalid)?;
        AwCatalog::generate_legacy_vla(stage.path(), &input, workers).map_err(invalid)?;
        promote_cells(root, stage)?;
    }
    drop(lock);
    NativeCacheLock::shared(root).map_err(invalid)
}

fn promote_cells(root: &Path, stage: tempfile::TempDir) -> MsResult<()> {
    let staged = stage.path();
    let mut names = Vec::new();
    for item in std::fs::read_dir(staged).map_err(invalid)? {
        let name = item.map_err(invalid)?.file_name();
        if is_cell(&name.to_string_lossy()) {
            if root.join(&name).exists() {
                return Err(invalid("legacy A/W cache gained a cell during generation"));
            }
            names.push(name.to_string_lossy().into_owned());
        }
    }
    let pending = staged.join("journal.new");
    std::fs::write(&pending, names.join("\n")).map_err(invalid)?;
    std::fs::rename(pending, staged.join(CELL_JOURNAL)).map_err(invalid)?;
    let staged = stage.keep();
    for name in names {
        std::fs::rename(staged.join(&name), root.join(&name)).map_err(invalid)?;
    }
    std::fs::remove_file(staged.join(CELL_JOURNAL)).map_err(invalid)?;
    std::fs::remove_dir_all(staged).map_err(invalid)
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
