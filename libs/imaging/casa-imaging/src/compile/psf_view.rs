// SPDX-License-Identifier: LGPL-3.0-or-later

//! The mosaic's separately centred PSF family (CASA task_tclean.py).

use casa_coordinates::{Coordinate, CoordinateModel, CoordinateSystem};
use casa_imageanalysis::RegridTemplate;
use casa_images::PagedImage;
use casa_imaging_model::{
    CentreLaws, CompiledProblem, DirectionCoordinateSpec, DirectionFrame, GeometryInput,
    ModelLifecycleRequirements, ObservationSelection, ObservationSnapshot,
    ObservationSnapshotInput, ObservationSourceInput, PhaseCentreLaw, ProblemInput,
    ProblemSpecification, SkyDirection, SpectralWindowSelection, UvwCoordinateLaw,
    compile_observation,
};
use casa_imaging_products::CasaImageWriter;
use casa_imaging_runtime::{Admitted, Demand, Reservation, admit};

use super::direction;
use super::domains::PreparedImageDomain;
use casa_core::measures::direction::{DirectionRef, MDirection};

use super::spectral::{self, FrameContext, PreparedSpectralAxis};
use super::{PrepareError, Surveyed, envelope, instrument, selection};
use crate::{Deconvolver, Gridder, ImagingRequest, RunContext, SpecMode};

pub(crate) struct PsfView {
    specification: ProblemSpecification,
    geometry: GeometryInput,
    windows: Option<Vec<Vec<SpectralWindowSelection>>>,
    reservation: Reservation,
}

impl PsfView {
    pub(crate) fn compile(
        self,
        main: &CompiledProblem,
        lifecycle: ModelLifecycleRequirements,
    ) -> Result<Admitted<CompiledProblem>, crate::ApplicationDispatchError> {
        let observation = match self.windows {
            Some(windows) => alternate_observation(main.observation(), windows)
                .map_err(|error| crate::ApplicationDispatchError::Preparation(error.into()))?,
            None => main.observation().clone(),
        };
        Ok(Admitted {
            value: casa_imaging_model::compile(ProblemInput::new(
                self.specification,
                self.geometry,
                observation,
                lifecycle,
            ))
            .map_err(crate::ApplicationDispatchError::Compile)?,
            reservation: self.reservation,
        })
    }
}

/// CASA forms the alternate PSF as a new mosaic view of the same selection.
/// The alternate phase law participates in row packing and convolution, not
/// just the image header. The other products retain the main view.
pub(super) fn prepare(
    request: &ImagingRequest,
    main: &PreparedImageDomain,
    tracks_source: bool,
    surveyed: &Surveyed<'_>,
    context: &RunContext,
    specification: &ProblemSpecification,
    writer: &mut CasaImageWriter,
) -> Result<Option<PsfView>, PrepareError> {
    let Some(text) = request.psfphasecenter.as_deref() else {
        return Ok(None);
    };
    if !request.calcpsf || !matches!(request.gridder, Gridder::Mosaic { .. }) {
        return Ok(None);
    }
    // A single selected channel leaves only the tiny frame-conversion
    // changes to determine the Taylor slope of the tracked continuum.
    if tracks_source
        && request.deconvolver == Deconvolver::Mtmfs
        && surveyed
            .spectral
            .selected_source_channels
            .iter()
            .flat_map(|windows| windows.values())
            .map(Vec::len)
            .sum::<usize>()
            == 1
    {
        return Err(PrepareError::MovingTaylorPsfSingleChannel);
    }
    let phase = direction::parse_phase_center_direction(text)?;
    let (longitude, latitude) = (phase.longitude_rad(), phase.latitude_rad());
    direction::refuse_ncp_at_equator(request.projection, latitude)?;
    let direction = direction::direction_spec(
        main.image_size,
        request.cell,
        request.projection,
        phase.frame(),
        [longitude, latitude],
    );
    // Cover both compiled and uncompiled single-domain metadata during
    // conversion. The alternate view shares the observation manifest
    // when frame conversion leaves the row and channel reads unchanged.
    let variable = (request.restfreq.len() as u64 + request.scales.len() as u64) * 32
        + (main.coordinates.obs_info().telescope.len() as u64
            + main.coordinates.obs_info().observer.len() as u64)
            * 2;
    let cube = request.specmode == SpecMode::Cube;
    let mut reservation = admit(
        &context.host,
        &context.policy,
        &Demand {
            phase: "separate mosaic PSF view",
            memory: 64 * 1024 + variable,
        },
    )?;
    let mut axis = None;
    let mut windows = None;
    if cube {
        let mut cube_charge = admit_cube_axis(surveyed, context)?;
        let alternate = alternate_axis(request, surveyed, phase, direction)?;
        let changed_selection = alternate.selected_source_channels
            != surveyed.spectral.selected_source_channels
            || alternate.window_reads != surveyed.spectral.window_reads;
        if changed_selection {
            windows = Some(alternate_windows(surveyed, &alternate)?);
            cube_charge.retain(alternate_selection_bytes(surveyed));
            reservation.join(cube_charge);
        }
        axis = Some(alternate);
    }
    let spectral = axis.as_ref().unwrap_or(surveyed.spectral);
    let coordinates = coordinates(request, main, direction, spectral);
    let geometry = GeometryInput::new(
        vec![main.spec().with_direction(direction)],
        CentreLaws::new(
            PhaseCentreLaw::Fixed(phase),
            instrument::pointing_centre_law(&request.gridder),
        ),
        UvwCoordinateLaw::MosaicPhaseTrackingCentre,
        spectral.coordinate(),
    );
    writer.set_main_psf_coordinates(coordinates);
    Ok(Some(PsfView {
        specification: specification.clone(),
        geometry,
        windows,
        reservation,
    }))
}

fn alternate_axis(
    request: &ImagingRequest,
    surveyed: &Surveyed<'_>,
    phase: SkyDirection,
    direction: DirectionCoordinateSpec,
) -> Result<PreparedSpectralAxis<'static>, PrepareError> {
    let frame = FrameContext {
        anchor_time_mjd_seconds: surveyed.survey.image_time_mjd_seconds,
        field_id: *surveyed.survey.fields.first().expect("selected field") as usize,
        phase: MDirection::from_angles(
            phase.longitude_rad(),
            phase.latitude_rad(),
            DirectionRef::J2000,
        ),
        direction,
        engine: surveyed.engine,
    };
    let spectral = spectral::prepare_axis(
        request,
        (surveyed.stores, surveyed.reference),
        None,
        &frame,
        None,
        request.reffreq,
    )?;
    if spectral.output_channels != surveyed.spectral.output_channels {
        return Err(PrepareError::AlternatePsfChannelCount {
            main: surveyed.spectral.output_channels,
            psf: spectral.output_channels,
        });
    }
    Ok(spectral)
}

fn coordinates(
    request: &ImagingRequest,
    main: &PreparedImageDomain,
    direction: DirectionCoordinateSpec,
    spectral: &PreparedSpectralAxis<'_>,
) -> CoordinateSystem {
    direction::image_coordinates(
        direction,
        main.image_size,
        &request.stokes,
        &spectral.image,
        main.coordinates.obs_info(),
    )
}

/// Validate the restart against the independently calculated alternate grid
/// at the saved direction. Other products still use the main grid.
pub(super) fn saved_cube_grid(
    path: &std::path::Path,
    request: &ImagingRequest,
    main: &PreparedImageDomain,
    surveyed: &Surveyed<'_>,
    context: &RunContext,
) -> Result<Option<RegridTemplate>, PrepareError> {
    let image = PagedImage::<f32>::open(path).map_err(|error| PrepareError::Restart {
        path: path.to_path_buf(),
        reason: error.to_string(),
    })?;
    let actual = image.coordinates();
    if actual.n_coordinates() == 0 {
        return Ok(None);
    }
    let CoordinateModel::Direction(saved) = actual.coordinate(0) else {
        return Ok(None);
    };
    if saved.direction_ref() != DirectionRef::J2000 {
        return Ok(None);
    }
    let values = saved.reference_value();
    let main_values = main.coordinates.coordinate(0).reference_value();
    if main.direction.reference_direction().frame() == DirectionFrame::J2000
        && values
            .iter()
            .zip(main_values)
            .all(|(a, b)| (a - b).abs() <= 1e-13)
    {
        return Ok(None);
    }
    let phase = SkyDirection::new(DirectionFrame::J2000, values[0], values[1]);
    direction::refuse_ncp_at_equator(request.projection, values[1])?;
    let direction = direction::direction_spec(
        main.image_size,
        request.cell,
        request.projection,
        phase.frame(),
        [values[0], values[1]],
    );
    let _axis = admit_cube_axis(surveyed, context)?;
    let spectral = alternate_axis(request, surveyed, phase, direction)?;
    let [nx, ny] = main.image_size;
    Ok(Some(RegridTemplate {
        coordinates: coordinates(request, main, direction, &spectral),
        shape: vec![nx, ny, request.stokes.len(), spectral.output_channels],
    }))
}

fn alternate_windows(
    surveyed: &Surveyed<'_>,
    spectral: &PreparedSpectralAxis<'_>,
) -> Result<Vec<Vec<SpectralWindowSelection>>, PrepareError> {
    surveyed
        .stores
        .iter()
        .zip(spectral.selected_source_channels.iter())
        .zip(&spectral.window_reads)
        .map(|((store, channels), reads)| {
            let factors = envelope::first_buffer_beam_factors(
                (store.catalogue(), &store.engine),
                &store.survey,
                None,
            )?;
            selection::window_selections(&store.survey, channels, &factors, reads)
        })
        .collect()
}

fn alternate_observation(
    main: &ObservationSnapshot,
    windows: Vec<Vec<SpectralWindowSelection>>,
) -> Result<ObservationSnapshot, casa_imaging_model::CompileObservationError> {
    let sources = main
        .sources()
        .iter()
        .zip(windows)
        .map(|(source, windows)| {
            let rows = source.selection();
            ObservationSourceInput::new(
                source.provenance().clone(),
                ObservationSelection::new(
                    rows.samples(),
                    rows.rows_filter().clone(),
                    rows.partitions().to_vec(),
                    windows,
                ),
                source.columns(),
                source.corrected_data_present(),
            )
        })
        .collect();
    compile_observation(ObservationSnapshotInput::new(sources))
}

fn alternate_selection_bytes(surveyed: &Surveyed<'_>) -> u64 {
    surveyed
        .stores
        .iter()
        .map(|store| {
            let windows = &store.survey.spectral_windows;
            let channels: usize = windows
                .iter()
                .map(|window| window.frequencies_hz.len())
                .sum();
            // Each window holds the catalog, three indexes and up to three
            // interval lists, each bounded by the number of physical channels.
            let channel_bytes = size_of::<f64>() + 3 * (size_of::<u32>() + size_of::<[f64; 2]>());
            let partitions =
                store.survey.partitions.len() * size_of::<casa_imaging_model::PartitionSelection>();
            (windows.len() * size_of::<SpectralWindowSelection>()
                + channels * channel_bytes
                + partitions) as u64
        })
        .sum()
}

fn admit_cube_axis(
    surveyed: &Surveyed<'_>,
    context: &RunContext,
) -> Result<Reservation, PrepareError> {
    // The alternate axis builder uses the same surveyed inputs and channel
    // scratch as the main cube, while the main specification is live.
    let workspace: u64 = surveyed
        .stores
        .iter()
        .map(|store| store.survey_charge.memory())
        .sum();
    Ok(admit(
        &context.host,
        &context.policy,
        &Demand {
            phase: "separate mosaic cube spectral axis",
            memory: workspace + alternate_selection_bytes(surveyed),
        },
    )?)
}
