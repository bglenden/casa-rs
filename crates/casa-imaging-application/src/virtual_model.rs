// SPDX-License-Identifier: LGPL-3.0-or-later
//! `tclean`'s `savemodel='virtual'`: the final model saved in the
//! MeasurementSet as CASA's virtual model, which CASA's visibility
//! iterator predicts from in place of `MODEL_DATA` (`casa_ms::virtual_model`
//! holds the persistence).
//!
//! CASA clears the selected fields' and windows' models when it selects
//! the data (`SynthesisImagerVi2::selectData`, `VisModelData::clearModel`)
//! and, in the major cycle that degrids the final model, saves each image
//! domain's machine once per MeasurementSet
//! (`SIMapperCollection::saveVirtualModel`): `FTMachine::toRecord` writes
//! the machine's padded grid, the Fourier transform of the corrected model
//! it degrids from, as a complex image `FT_MODEL<pid>_<n>` beside SOURCE,
//! and the record naming it is added incrementally
//! (`VisibilityIteratorImpl2::writeModel`). Nothing is saved for a model
//! of no flux (`SIImageStore::isModelEmpty`).
//!
//! casa-rs does the same once the run has published its products: the
//! clearing, then the final model's grids through the run's measurement
//! operator ([`crate::imaging::prepare_model`], the grids its degridding
//! pass predicts from), a plane at a time, into the image, and the record.
//! The standard gridder's Stokes I main image is saved (`GridFT` and, for
//! MT-MFS, `MultiTermFTNew` over it); other gridders, polarizations,
//! facets and outlier fields are refused when the request is validated.

mod record;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType, ObsInfo};
use casa_images::PagedImage;
use casa_imaging_deconvolution::ModelGeneration;
use casa_imaging_model::{CompiledProblem, ModelSupport, ReconstructionBasis};
use casa_imaging_operator::{GridPrecision, PlaneRange};
use casa_imaging_runtime::pass::BackendChoice;
use casa_imaging_runtime::{Demand, admit};
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::{
    CubeInterpolation, MeasurementSet, MsSelection, VirtualModelEntry, VirtualModelKind,
    clear_virtual_model, put_virtual_model, virtual_model_clear_ids, virtual_model_image_path,
    virtual_model_index_combination,
};
use casa_types::measures::MeasuresProvider;
use casa_types::measures::frequency::FrequencyRef;
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use num_complex::Complex32;

use crate::imaging::{self, ImagingError, KernelAdmission};
use crate::native::NativeError;
use crate::{ApplicationRuntime, ImagingRequest, MeasurementSetSelection};
use record::{FirstBuffer, GridFt};

/// What saving a run's virtual model needs beyond the run: its
/// MeasurementSet and selection, the main image's coordinates, the cube's
/// interpolation and the run's Measures.
pub(crate) struct VirtualModelPlan {
    ms: PathBuf,
    field: Option<String>,
    spw: Option<String>,
    selection: MsSelection,
    coordinates: CoordinateSystem,
    interpolation: i32,
    measures: Arc<dyn MeasuresProvider>,
}

impl VirtualModelPlan {
    /// The plan of `vis`, one MeasurementSet of `request` and its
    /// selection, whose main image has `coordinates`. CASA saves the model
    /// in each MeasurementSet of its list (`saveVirtualModel` per
    /// MeasurementSet), so a run has one plan per MeasurementSet it reads.
    pub(crate) fn new(
        (request, vis): (&ImagingRequest, &MeasurementSetSelection),
        coordinates: CoordinateSystem,
        measures: Arc<dyn MeasuresProvider>,
    ) -> Self {
        let selection = MsSelection {
            field: vis.field.clone(),
            spw: vis.spw.clone(),
            timerange: vis.timerange.clone(),
            uvrange: vis.uvrange.clone(),
            antenna: vis.antenna.clone(),
            scan: vis.scan.clone(),
            observation: vis.observation.clone(),
            intent: vis.intent.clone(),
            data_description: request.ddid.map(|ddid| ddid.to_string()),
            ..MsSelection::default()
        };
        Self {
            ms: vis.vis.clone(),
            field: vis.field.clone(),
            spw: vis.spw.clone(),
            selection,
            coordinates,
            interpolation: match request.interpolation {
                CubeInterpolation::Nearest => 0,
                CubeInterpolation::Linear => 1,
                CubeInterpolation::Cubic => 2,
            },
            measures,
        }
    }
}

/// Clear the selected fields' and windows' virtual models and save the
/// final `model` of `problem` as theirs, as the module states. A record
/// CASA would not add to (an older model of the same fields) is logged
/// and nothing is filed, as CASA warns and goes on.
///
/// # Errors
///
/// The phase's memory does not fit; the MeasurementSet, its selection or
/// the model cannot be read; the operator cannot be built; a grid image
/// cannot be written; clearing cannot write the MeasurementSet.
pub(crate) fn save(
    problem: &CompiledProblem,
    model: &ModelGeneration,
    plan: &VirtualModelPlan,
    runtime: &ApplicationRuntime,
) -> Result<(), NativeError> {
    let other = |error: Box<dyn std::error::Error + Send + Sync>| NativeError::Other(error);
    let charge = |phase, memory| {
        admit(
            &runtime.host,
            &runtime.resource_policy,
            &Demand { phase, memory },
        )
    };
    let mut held = charge("virtual model", demand_bytes(problem))?;
    let mut ms = MeasurementSet::open(&plan.ms).map_err(|error| other(error.into()))?;
    // The selection's row list, one index per row at most.
    held.join(charge(
        "virtual model selection",
        ms.row_count() as u64 * 8,
    )?);
    let (fields, spws) = virtual_model_clear_ids(&ms, plan.field.as_deref(), plan.spw.as_deref())
        .map_err(|error| other(error.into()))?;
    clear_virtual_model(&mut ms, &fields, &spws).map_err(|error| other(error.into()))?;
    if model_is_empty(problem, model).map_err(|error| other(error.into()))? {
        tracing::info!("the model is empty: no virtual model is saved");
        return Ok(());
    }
    let selected = virtual_model_index_combination(&ms, &plan.selection)
        .map_err(|error| other(error.into()))?;
    let Some(first_row) = selected.first_row else {
        tracing::info!("no row is selected: no virtual model is saved");
        return Ok(());
    };
    let frequencies =
        image_frequencies(&plan.coordinates, image_channels(problem)).map_err(other)?;
    let buffer = first_buffer(&ms, plan, first_row, &frequencies).map_err(other)?;
    let base = virtual_model_image_path(&ms).map_err(|error| other(error.into()))?;
    let container = write_machine(
        (problem, model, plan, runtime),
        (&base, &frequencies, &buffer),
        &ms,
    )?;
    let entry = VirtualModelEntry {
        kind: VirtualModelKind::FtMachine,
        index_combination: selected.index_combination,
        channel_selection: channel_selection(&ms, plan.spw.as_deref()).map_err(other)?,
        container,
    };
    if let Err(error) = put_virtual_model(&mut ms, &entry, true) {
        tracing::warn!(
            %error,
            "could not save the virtual model; clear it, or save the model column instead"
        );
    }
    Ok(())
}

/// Whether the model has no flux: the first Taylor term's (every
/// channel's) sum over every domain and polarization below CASA's 1e-8
/// (`isModelEmpty`).
fn model_is_empty(
    problem: &CompiledProblem,
    model: &ModelGeneration,
) -> Result<bool, ImagingError> {
    let shape = model.shape();
    let coefficients = match problem.reconstruction().basis() {
        ReconstructionBasis::ChannelLocal { channels } => channels,
        ReconstructionBasis::Constant | ReconstructionBasis::Taylor { .. } => 1,
    };
    let mut flux = 0.0;
    for domain in 0..shape.domains().len() {
        for coefficient in 0..coefficients {
            for pol in 0..shape.polarizations() {
                flux += model
                    .read_plane(domain, coefficient, pol)?
                    .iter()
                    .filter(|sample| sample.support() == ModelSupport::Valid)
                    .map(|sample| sample.value().value())
                    .sum::<f64>();
            }
        }
    }
    Ok(flux.abs() < 1e-8)
}

/// The memory the phase holds at most besides the selection's row list:
/// the MeasurementSet's tables as opened (their metadata and subtables),
/// one plane's grids and their transform at f64, its model images, the
/// grid image's tile cache (one plane) and its f32 plane, and a chunk of
/// the selection's columns.
fn demand_bytes(problem: &CompiledProblem) -> u64 {
    const TABLES_BYTES: u64 = 32 << 20;
    const COLUMN_CHUNK_BYTES: u64 = 65_536 * 5 * 32;
    let domain = &problem.geometry().domains()[0];
    let [width, height] = domain.shape().pixels();
    let padded = |n: usize| (n as u64 * 6).div_ceil(5) + 16;
    let cells = padded(width) * padded(height);
    let terms = match problem.reconstruction().basis() {
        ReconstructionBasis::Taylor { terms } => terms as u64,
        ReconstructionBasis::Constant | ReconstructionBasis::ChannelLocal { .. } => 1,
    };
    let grids = cells * terms * 16 * 2;
    let images = (width * height) as u64 * terms * (4 + 16);
    let plane = cells * 8 * 2;
    TABLES_BYTES + grids + images + plane + COLUMN_CHUNK_BYTES
}

/// The image's channel frequencies, its linear spectral axis at each
/// channel (one for a continuum or Taylor image).
fn image_frequencies(
    coordinates: &CoordinateSystem,
    channels: usize,
) -> Result<Vec<f64>, Box<dyn std::error::Error + Send + Sync>> {
    let Some(index) = coordinates.find_coordinate(CoordinateType::Spectral) else {
        return Err("the image has no spectral axis".into());
    };
    let spectral = coordinates.coordinate(index);
    (0..channels)
        .map(|channel| {
            spectral
                .to_world(&[channel as f64])
                .map(|world| world[0])
                .map_err(|error| error.to_string().into())
        })
        .collect()
}

/// The image's channels: a cube's, else one.
fn image_channels(problem: &CompiledProblem) -> usize {
    match problem.reconstruction().basis() {
        ReconstructionBasis::ChannelLocal { channels } => channels,
        ReconstructionBasis::Constant | ReconstructionBasis::Taylor { .. } => 1,
    }
}

/// What CASA's machine recorded of the first buffer it degridded: the
/// selection's first row's field and window, the window's selected
/// channels' frequencies converted to LSRK at that row's time, the image
/// channel each falls in, and the correlations, the parallel hands of
/// which grid Stokes I's one polarization.
fn first_buffer(
    ms: &MeasurementSet,
    plan: &VirtualModelPlan,
    (field, description, time): (i32, i32, f64),
    image_hz: &[f64],
) -> Result<FirstBuffer, Box<dyn std::error::Error + Send + Sync>> {
    let descriptions = ms.data_description()?;
    let ddid = usize::try_from(description)?;
    let spw = usize::try_from(descriptions.spectral_window_id(ddid)?)?;
    let polarization = usize::try_from(descriptions.polarization_id(ddid)?)?;
    let windows = ms.spectral_window()?;
    let frequencies = windows.chan_freq(spw)?;
    let frame = FrequencyRef::from_casacore_code(windows.meas_freq_ref(spw)?)
        .ok_or("SPECTRAL_WINDOW names an unknown frequency frame")?;
    let selection = MsSelection {
        spw: plan.spw.clone(),
        ..MsSelection::default()
    };
    let channels = match selection.channel_selection_for_spw(spw as i32) {
        Some(selection) => selection.indices(frequencies.len())?,
        None => (0..frequencies.len()).collect(),
    };
    let engine = MsCalEngine::with_measures(ms, Arc::clone(&plan.measures))?;
    let lsrk = channels
        .iter()
        .map(|&channel| {
            casa_ms::convert_frequency_to_frame(
                frame,
                FrequencyRef::LSRK,
                frequencies[channel],
                time,
                usize::try_from(field).unwrap_or(0),
                &engine,
            )
        })
        .collect::<Result<Vec<f64>, _>>()?;
    let correlations = ms.polarization()?.corr_type(polarization)?;
    Ok(FirstBuffer {
        field,
        channel_map: lsrk
            .iter()
            .map(|&frequency| image_channel(image_hz, frequency))
            .collect(),
        frequencies_lsrk_hz: lsrk,
        polarization_map: correlations
            .iter()
            .map(|&code| if parallel_hand(code) { 0 } else { -1 })
            .collect(),
        correlations,
    })
}

/// The image channel a frequency falls in: the only one of a continuum
/// image, else the nearest within half a channel, `-1` beyond the cube.
fn image_channel(image_hz: &[f64], frequency_hz: f64) -> i32 {
    if image_hz.len() == 1 {
        return 0;
    }
    let width = (image_hz[1] - image_hz[0]).abs();
    image_hz
        .iter()
        .enumerate()
        .map(|(channel, hz)| (channel, (hz - frequency_hz).abs()))
        .min_by(|a, b| a.1.total_cmp(&b.1))
        .filter(|(_, distance)| *distance <= 0.5 * width)
        .map_or(-1, |(channel, _)| channel as i32)
}

/// RR, LL, XX and YY (`Stokes::StokesTypes` 5, 8, 9, 12).
const fn parallel_hand(code: i32) -> bool {
    matches!(code, 5 | 8 | 9 | 12)
}

/// The writer's channel selection, `(spw, first, channels, increment)` per
/// selected range (`VisibilityIteratorImpl2::getChannelInformation`):
/// each range the `spw` expression names, or every window whole.
fn channel_selection(
    ms: &MeasurementSet,
    spw: Option<&str>,
) -> Result<Vec<[i32; 4]>, Box<dyn std::error::Error + Send + Sync>> {
    let windows = ms.spectral_window()?;
    let whole = |id: usize| -> Result<[i32; 4], Box<dyn std::error::Error + Send + Sync>> {
        Ok([id as i32, 0, windows.num_chan(id)?, 1])
    };
    let Some(spw) = spw.filter(|text| !text.trim().is_empty()) else {
        return (0..windows.row_count()).map(whole).collect();
    };
    let mut rows = Vec::new();
    for selector in casa_ms::parse_spw_selector(spw)? {
        let id = usize::try_from(selector.spw_id)?;
        match selector.channels {
            None => rows.push(whole(id)?),
            Some(channels) => {
                for segment in channels.segments {
                    let stride = segment.stride.max(1) as i32;
                    let count = (segment.end - segment.start) / stride + 1;
                    rows.push([selector.spw_id, segment.start, count, stride]);
                }
            }
        }
    }
    Ok(rows)
}

/// Write the main image's grids, a plane at a time, into the machine's
/// images (`base`, or `base_term_<t>` for each Taylor term) and return the
/// machine's record.
fn write_machine(
    (problem, model, plan, runtime): (
        &CompiledProblem,
        &ModelGeneration,
        &VirtualModelPlan,
        &ApplicationRuntime,
    ),
    (base, image_hz, buffer): (&Path, &[f64], &FirstBuffer),
    ms: &MeasurementSet,
) -> Result<casa_types::RecordValue, NativeError> {
    let other = |error: Box<dyn std::error::Error + Send + Sync>| NativeError::Other(error);
    let domain = &problem.geometry().domains()[0];
    let correlations = imaging::selected_correlations(problem)?;
    let built = imaging::domain_operator(
        problem,
        imaging::image_extent(domain)?,
        &correlations,
        BackendChoice::Cpu,
        Some(GridPrecision::F64),
        None,
        &[],
        KernelAdmission {
            host: &runtime.host,
            policy: &runtime.resource_policy,
            workers: 1,
        },
    )?;
    let operator = &built.operator;
    let [nx, ny] = operator.geometry().grid_shape();
    let terms = operator.basis().data_terms();
    let planes = operator.basis().planes() as usize;
    let taylor = matches!(
        operator.basis(),
        casa_imaging_operator::Basis::Taylor { .. }
    );
    let paths: Vec<PathBuf> = if taylor {
        (0..terms)
            .map(|term| base.with_file_name(format!("{}_term_{term}", file_name(base))))
            .collect()
    } else {
        vec![base.to_path_buf()]
    };
    let coordinates = grid_coordinates(&plan.coordinates, [nx, ny]).map_err(other)?;
    let shape = vec![nx, ny, 1, planes];
    let mut images = paths
        .iter()
        .map(|path| {
            PagedImage::<Complex32>::create_with_tile_shape_and_cache(
                shape.clone(),
                vec![nx, ny, 1, 1],
                coordinates.clone(),
                path,
                nx * ny * 8,
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| other(error.into()))?;
    for plane in 0..planes {
        let grids =
            imaging::prepare_model(operator, model, (0, None), PlaneRange::single(plane as u32))?;
        for (term, image) in images.iter_mut().enumerate() {
            let cells = grids.block::<f64>(0, 0, term);
            let values = ArrayD::from_shape_fn(IxDyn(&[nx, ny, 1, 1]).f(), |index| {
                let cell = cells[index[1] * nx + index[0]];
                Complex32::new(cell.re as f32, cell.im as f32)
            });
            image
                .put_slice(&values, &[0, 0, 0, plane])
                .map_err(|error| other(error.into()))?;
        }
    }
    for image in &mut images {
        image.save().map_err(|error| other(error.into()))?;
    }
    let engine = MsCalEngine::with_measures(ms, Arc::clone(&plan.measures))
        .map_err(|error| other(error.into()))?;
    let (longitude, latitude, radius) = engine.observatory_position().as_spherical();
    let (centre, frame) = phase_centre(&plan.coordinates).map_err(other)?;
    let spectral = record::casa_spectral(&spectral_record(&plan.coordinates).map_err(other)?);
    let extent = imaging::image_extent(domain)?;
    let machine = |path: &Path| {
        GridFt {
            disk_image: path,
            grid: [nx, ny],
            polarizations: 1,
            image_frequencies_hz: image_hz,
            increment_rad: extent.increment_rad,
            phase_centre_rad: centre,
            direction_frame: &frame,
            observatory: [longitude, latitude, radius],
            spectral: spectral.clone(),
            buffer,
            // A Taylor term's machine, which `MultiTermFTNew` drives,
            // records nearest-neighbour interpolation.
            interpolation: if taylor { 0 } else { plan.interpolation },
            frequency_frame_valid: !taylor,
        }
        .record()
    };
    Ok(if taylor {
        record::multi_term(
            paths.iter().map(|path| machine(path)).collect(),
            imaging::reference_frequency_hz(problem)?,
        )
    } else {
        machine(base)
    })
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// The grid image's coordinates, `FTMachine::toRecord`'s: the image's,
/// its direction's reference pixel at the padded grid's centre cell, with
/// the empty observation information of CASA's complex image.
fn grid_coordinates(
    coordinates: &CoordinateSystem,
    [nx, ny]: [usize; 2],
) -> Result<CoordinateSystem, Box<dyn std::error::Error + Send + Sync>> {
    let mut grid = coordinates.clone();
    let index = grid
        .find_coordinate(CoordinateType::Direction)
        .ok_or("the image has no direction axes")?;
    let CoordinateModel::Direction(direction) = grid.coordinate(index).clone() else {
        return Err("the image's direction coordinate is not a direction".into());
    };
    grid.replace_coordinate(
        index,
        direction.with_reference_pixel([(nx / 2) as f64, (ny / 2) as f64]),
    );
    *grid.obs_info_mut() = ObsInfo::new("UNKNOWN").with_observer("UNKNOWN");
    Ok(grid)
}

/// The image's reference direction and its frame.
fn phase_centre(
    coordinates: &CoordinateSystem,
) -> Result<([f64; 2], String), Box<dyn std::error::Error + Send + Sync>> {
    let index = coordinates
        .find_coordinate(CoordinateType::Direction)
        .ok_or("the image has no direction axes")?;
    let CoordinateModel::Direction(direction) = coordinates.coordinate(index) else {
        return Err("the image's direction coordinate is not a direction".into());
    };
    let radians = coordinates.coordinate(index).reference_value();
    Ok((
        [radians[0], radians[1]],
        direction.direction_ref().as_str().to_string(),
    ))
}

/// The image's spectral coordinate's record (`SpectralCoordinate::save`).
fn spectral_record(
    coordinates: &CoordinateSystem,
) -> Result<casa_types::RecordValue, Box<dyn std::error::Error + Send + Sync>> {
    let index = coordinates
        .find_coordinate(CoordinateType::Spectral)
        .ok_or("the image has no spectral axis")?;
    Ok(coordinates.coordinate(index).to_record())
}
