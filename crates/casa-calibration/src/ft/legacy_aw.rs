// SPDX-License-Identifier: LGPL-3.0-or-later
//! Legacy imager prediction from a populated CASA AW cache.

use std::collections::BTreeMap;
use std::path::Path;

use casa_coordinates::{CoordinateModel, SpectralCoordinate, StokesType};
use casa_images::PagedImage;
use casa_imaging_model::{
    NativeAwFrequencyGroup, NativeAwGrid, NativeAwRequestInput, NativeAwTerms,
    PolarizationCoordinate,
};
use casa_imaging_operator::{
    AwCatalog, AwIndexing, AwLegacyFrequencyMap, AwPaRotation, GridGeometry, GridPadding,
    ImageExtent, NativeCacheLock, PolarizationRouting,
};
use casa_imaging_runtime::{Demand, HostResources, Reservation, ResourcePolicy, admit};
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::{ChannelSelectionSegment, MeasurementSet, MsResult, ResolvedMsSelection};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::epoch::EpochRef;
use casa_types::{ArrayValue, Correlation, RecordValue, ScalarValue, Value};

use crate::calibrater_lifecycle::legacy_ep::{legacy_vla_surface, model_geometry, reserve_model};
use crate::paths::copy_editable_table_tree;

use super::image::ImageModel;
use super::legacy_pbw::LegacyFourierImage;
use super::moving_source::first_imager_row;
use super::settings::{ImagerLegacyAwOptions, PredictionGeometry};
use super::{channel_span, invalid};

pub(crate) const CACHE: &str = "CASARS_LEGACY_AW_CACHE";
pub(crate) const MACHINE: &str = "AWProjectWBFT";
pub(super) const MISSING_SENSITIVITY: &str =
    "LatticeIterInterface::ok - Navigator Lattice and Data Lattice have different shapes";
const COLD_STAGE: &str = ".casars-legacy-aw-";
const OWNED_STAGE: &str = "preparing";
const CELL_JOURNAL: &str = "promoting";
const CELL_OWNER: &str = ".casars-legacy-aw-owner";
const FREQUENCY_MAP: &str = "casars_legacy_aw_frequency_map";

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
    let frequency_map = selected_frequency_map(ms, selected, row.spectral_window_id)?;
    let _cache_lock = ready_cache(
        &options.cache_directory,
        spectral,
        || {
            if !options.wideband || !switches[1] {
                return Err(invalid(
                    "this cold legacy AW term configuration is pending #1520",
                ));
            }
            if codes.len() != 2
                || !codes.contains(&(StokesType::RR as i32))
                || !codes.contains(&(StokesType::LL as i32))
            {
                return Err(invalid(
                    "cold VLA A/W currently needs the RR and LL correlation layout",
                ));
            }
            let (_, pa) = samples
                .first()
                .ok_or_else(|| invalid("legacy AW has no first visibility buffer"))?;
            cold_request(
                ms,
                selected,
                row.spectral_window_id,
                &grid,
                (reference_hz, f64::from(*pa)),
                NativeAwTerms {
                    aperture: switches[1],
                    w_term: false,
                    prolate_spheroidal: switches[0],
                    wideband: options.wideband,
                    conjugate_beams: options.conjugate_beams,
                },
                &host,
            )
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
    if options.correct_primary_beam && average.is_none() {
        return Err(invalid(MISSING_SENSITIVITY));
    }
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
            if let Some(map) = frequency_map {
                record.upsert(
                    FREQUENCY_MAP,
                    Value::Array(ArrayValue::from_f64_vec(vec![
                        map.start_channel as f64,
                        map.channel_stride as f64,
                        map.mapped_channels as f64,
                        map.start_hz,
                        map.increment_hz,
                    ])),
                );
            }
            Ok(())
        },
    )
}

fn selected_frequency_map(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    spw: i32,
) -> MsResult<Option<AwLegacyFrequencyMap>> {
    let Some(selection) = selected.channel_selections.get(&spw) else {
        return Ok(None);
    };
    if selection.segments.len() < 2 {
        return Ok(None);
    }
    check_range_count(selection.segments.len())?;
    let last = selection.segments.last().expect("multiple selected ranges");
    let spectral = ms.spectral_window()?;
    let window = usize::try_from(spw).map_err(invalid)?;
    let frequencies = spectral.chan_freq(window)?;
    let widths = spectral.chan_width(window)?;
    let frequency_at = |channel: i32| {
        usize::try_from(channel)
            .ok()
            .and_then(|channel| frequencies.get(channel).copied())
            .ok_or_else(|| invalid("the legacy A/W frequency map is outside its window"))
    };
    // CFBuffer::initMaps overwrites the SPW map for each supplied range.
    // MSSelection::getChanFreqList supplies the mean width times the stride.
    let start_hz = frequency_at(last.start)?;
    let increment_hz = last.stride as f64 * widths.iter().sum::<f64>() / widths.len() as f64;
    let mapped = ((frequency_at(last.end)? - start_hz) / increment_hz + 1.0).trunc();
    if !start_hz.is_finite()
        || start_hz <= 0.
        || !increment_hz.is_finite()
        || increment_hz == 0.
        || !(1.0..=f64::from(i32::MAX)).contains(&mapped)
    {
        return Err(invalid("legacy A/W frequency range has invalid sampling"));
    }
    let (span, channel_stride) = channel_span(selection);
    Ok(Some(AwLegacyFrequencyMap {
        start_channel: span.start,
        channel_stride,
        mapped_channels: mapped as usize,
        start_hz,
        increment_hz,
    }))
}

pub(crate) fn record_frequency_map(record: &RecordValue) -> MsResult<Option<AwLegacyFrequencyMap>> {
    let Some(value) = record.get(FREQUENCY_MAP) else {
        return Ok(None);
    };
    let malformed = || invalid("legacy AW recipe has an invalid frequency map");
    let Value::Array(ArrayValue::Float64(values)) = value else {
        return Err(malformed());
    };
    let Some(&[start, stride, count, start_hz, increment_hz]) = values.as_slice() else {
        return Err(malformed());
    };
    if values.shape() != [5]
        || [(start, 0.), (stride, 1.), (count, 1.)]
            .into_iter()
            .any(|(value, min)| {
                !(min..=f64::from(i32::MAX)).contains(&value) || value.fract() != 0.
            })
        || !start_hz.is_finite()
        || start_hz <= 0.
        || !increment_hz.is_finite()
        || increment_hz == 0.
    {
        return Err(malformed());
    }
    Ok(Some(AwLegacyFrequencyMap {
        start_channel: start as usize,
        channel_stride: stride as usize,
        mapped_channels: count as usize,
        start_hz,
        increment_hz,
    }))
}

fn cold_request(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    spw: i32,
    grid: &GridGeometry,
    (reference_frequency_hz, pa): (f64, f64),
    terms: NativeAwTerms,
    host: &HostResources,
) -> MsResult<(NativeAwRequestInput, String, Reservation)> {
    let window = ms.spectral_window()?;
    let band = window.name(spw as usize)?;
    let frequencies = window.chan_freq(spw as usize)?;
    let entire = ChannelSelectionSegment {
        start: 0,
        end: i32::try_from(
            frequencies
                .len()
                .checked_sub(1)
                .ok_or_else(|| invalid("the legacy A/W window has no channels"))?,
        )
        .map_err(invalid)?,
        stride: 1,
    };
    let segments = selected
        .channel_selections
        .get(&spw)
        .map_or(std::slice::from_ref(&entire), |selection| {
            selection.segments.as_slice()
        });
    check_range_count(segments.len())?;
    let memory = admit(
        host,
        &ResourcePolicy::Balanced,
        &Demand {
            phase: "legacy A/W channel range planning",
            // Each group retains one sample. Include the temporary endpoint index.
            memory: (segments.len() as u64).saturating_mul(
                (size_of::<NativeAwFrequencyGroup>() + size_of::<f64>() + 32) as u64,
            ),
        },
    )
    .map_err(invalid)?;
    let spectral_window = spw.try_into().map_err(invalid)?;
    let groups = legacy_frequency_groups(segments, &frequencies, spectral_window)?;
    let maximum_cells = groups.len() * 2;
    let (fft_side, oversampling) = (2048, 20);
    let image = grid.image();
    let input = NativeAwRequestInput {
        surface: legacy_vla_surface()?,
        antenna_diameter_m: 25.,
        frequencies: groups,
        w_values: vec![0.],
        w_increment: 0.,
        pa_values: vec![pa],
        mueller_elements: vec![0, 15],
        reference_frequency_hz,
        grid: NativeAwGrid {
            size: fft_side,
            sky_increment_rad: image
                .increment_rad
                .map(|cell| cell * (oversampling * image.shape[0]) as f64 / fft_side as f64),
            oversampling,
        },
        terms,
        maximum_cells,
    };
    Ok((input, band, memory))
}

fn legacy_frequency_groups(
    segments: &[ChannelSelectionSegment],
    frequencies: &[f64],
    spectral_window: u32,
) -> MsResult<Vec<NativeAwFrequencyGroup>> {
    check_range_count(segments.len())?;
    let in_band = |frequency: &f64| (1.0e9..=2.0e9).contains(frequency);
    let window_in_band = frequencies.iter().all(in_band);
    // CASA samples each range at its stop for the cell names and the
    // conjugate-frequency tie break. Keep that order and one sample per range.
    segments
        .iter()
        .map(|segment| {
            let start = usize::try_from(segment.start).map_err(invalid)?;
            let stop = usize::try_from(segment.end).map_err(invalid)?;
            let stride = segment.stride;
            if stride == 0 || start.max(stop) >= frequencies.len() {
                return Err(invalid(
                    "the cold A/W channel range is outside its window or has zero stride",
                ));
            }
            if !window_in_band {
                let sampled_span = start.abs_diff(stop) / stride * stride;
                let (first_sample, last_sample) = if start <= stop {
                    (start, start + sampled_span)
                } else {
                    (start - sampled_span, start)
                };
                if !in_band(&frequencies[stop])
                    || !frequencies[first_sample..=last_sample]
                        .iter()
                        .step_by(stride)
                        .all(in_band)
                {
                    return Err(invalid("cold VLA A/W needs L-band visibility channels"));
                }
            }
            let cf_frequency_hz = frequencies[stop];
            Ok(NativeAwFrequencyGroup {
                spectral_window,
                channel_frequencies_hz: vec![cf_frequency_hz],
                cf_frequency_hz,
            })
        })
        .collect()
}

fn check_range_count(count: usize) -> MsResult<()> {
    if (1..=usize::from(u16::MAX) + 1).contains(&count) {
        Ok(())
    } else {
        Err(invalid(
            "legacy A/W channel ranges exceed the cache index range",
        ))
    }
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
    let owner = stage_owner(stage)?;
    match std::fs::read_to_string(stage.join(CELL_JOURNAL)) {
        Ok(journal) => {
            for name in journal.lines() {
                if !is_cell(name) || Path::new(name).components().count() != 1 {
                    return Err(invalid("invalid legacy A/W cache journal"));
                }
                let target = root.join(name);
                if !stage.join(name).exists() && cell_owned_by(&target, owner)? {
                    std::fs::remove_dir_all(target).map_err(invalid)?;
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // A committed stage may still be cleaning its ownership markers.
            for item in std::fs::read_dir(root).map_err(invalid)? {
                let item = item.map_err(invalid)?;
                if is_cell(&item.file_name().to_string_lossy())
                    && cell_owned_by(&item.path(), owner)?
                {
                    std::fs::remove_file(item.path().join(CELL_OWNER)).map_err(invalid)?;
                }
            }
        }
        Err(error) => return Err(invalid(error)),
    }
    std::fs::remove_dir_all(stage).map_err(invalid)
}

fn stage_owner(stage: &Path) -> MsResult<&str> {
    stage
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .ok_or_else(|| invalid("invalid legacy A/W cache stage name"))
}

fn cell_owned_by(path: &Path, owner: &str) -> MsResult<bool> {
    match path.symlink_metadata() {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => return Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(invalid(error)),
    }
    match std::fs::read_to_string(path.join(CELL_OWNER)) {
        Ok(marker) => Ok(marker == owner),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(invalid(error)),
    }
}

fn ready_cache(
    root: &Path,
    spectral: &SpectralCoordinate,
    request: impl FnOnce() -> MsResult<(NativeAwRequestInput, String, Reservation)>,
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
        let (input, band, _planning_memory) = request()?;
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
        AwCatalog::generate_legacy_vla(stage.path(), &input, &band, spectral, workers)
            .map_err(invalid)?;
        promote_cells(root, stage)?;
    }
    drop(lock);
    NativeCacheLock::shared(root).map_err(invalid)
}

fn promote_cells(root: &Path, stage: tempfile::TempDir) -> MsResult<()> {
    let staged = stage.path();
    let owner = stage_owner(staged)?;
    let mut names = Vec::new();
    for item in std::fs::read_dir(staged).map_err(invalid)? {
        let name = item.map_err(invalid)?.file_name();
        if is_cell(&name.to_string_lossy()) {
            if root.join(&name).exists() {
                return Err(invalid("legacy A/W cache gained a cell during generation"));
            }
            std::fs::write(staged.join(&name).join(CELL_OWNER), owner).map_err(invalid)?;
            names.push(name.to_string_lossy().into_owned());
        }
    }
    let pending = staged.join("journal.new");
    std::fs::write(&pending, names.join("\n")).map_err(invalid)?;
    std::fs::rename(pending, staged.join(CELL_JOURNAL)).map_err(invalid)?;
    let staged = stage.keep();
    for name in &names {
        std::fs::rename(staged.join(name), root.join(name)).map_err(invalid)?;
    }
    std::fs::remove_file(staged.join(CELL_JOURNAL)).map_err(invalid)?;
    for name in names {
        std::fs::remove_file(root.join(name).join(CELL_OWNER)).map_err(invalid)?;
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cold_frequency_groups_keep_only_ordered_endpoints() {
        let channels = 8192;
        let frequencies: Vec<_> = (0..channels)
            .map(|index| 1.4e9 + f64::from(index))
            .collect();
        let ranges: Vec<_> = (0..channels)
            .rev()
            .map(|end| ChannelSelectionSegment {
                start: 0,
                end,
                stride: 1,
            })
            .collect();
        let groups = legacy_frequency_groups(&ranges, &frequencies, 7).unwrap();
        assert_eq!(groups.len(), ranges.len());
        for (group, &stop) in groups.iter().zip(frequencies.iter().rev()) {
            assert_eq!(group.spectral_window, 7);
            assert_eq!(group.cf_frequency_hz, stop);
            assert_eq!(group.channel_frequencies_hz, [stop]);
        }
        assert_eq!(
            legacy_frequency_groups(&ranges[..1], &frequencies, 7)
                .unwrap()
                .len(),
            1
        );
        assert!(check_range_count(usize::from(u16::MAX) + 1).is_ok());
        assert!(check_range_count(usize::from(u16::MAX) + 2).is_err());
    }

    #[test]
    fn cold_frequency_ranges_check_samples_and_the_raw_stop() {
        let frequencies = [1.41e9, 2.1e9, 1.42e9, 1.43e9, 1.44e9];
        let range = |start, end| {
            [ChannelSelectionSegment {
                start,
                end,
                stride: 2,
            }]
        };
        assert_eq!(
            legacy_frequency_groups(&range(4, 0), &frequencies, 0).unwrap()[0].cf_frequency_hz,
            1.41e9
        );
        assert!(legacy_frequency_groups(&range(0, 1), &frequencies, 0).is_err());
        assert!(legacy_frequency_groups(&range(3, 0), &frequencies, 0).is_err());
    }
}
