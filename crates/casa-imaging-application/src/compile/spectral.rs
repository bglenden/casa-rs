// SPDX-License-Identifier: LGPL-3.0-or-later

//! The image's spectral axis: one constant-basis continuum plane, or a
//! cube's channel-local output axis in its output or the moving source's
//! rest frame (CASA `SpectralImagingMode`, `CubeSpectralSetup`).

use std::collections::BTreeMap;

use casa_imaging_model::{
    DirectionCoordinateSpec, DopplerConvention, Epoch, FrequencyBands, FrequencyFrame,
    ItrfPosition, ReconstructionBasis, RestFrequency, SpectralCoordinateSpec, SpectralFrameAnchor,
    SpectralSamplingLaw, SpectralWcs, TimeScale,
};
use casa_ms::{
    CubeAxisConfig, CubeAxisValue, CubeInterpolation, CubeSpectralSetup, MeasurementSet,
    SubtableId, spectral_selection::CubeSpecMode,
};
use casa_ms::derived::engine::MsCalEngine;
use casa_observation::{Catalogue, FieldId, WindowId};
use casa_types::ArrayValue;
use casa_types::measures::{
    direction::MDirection, doppler::DopplerRef, epoch::EpochRef, frame::MeasFrame,
    frequency::FrequencyRef,
};

use super::PrepareError;
use super::direction::ImageSpectralCoordinate;
use super::frequency_bands;
use super::selection::{SourceSpectralWindow, WindowChannels, one_window};
use super::stores::{ContinuumBand, SurveyedStore};
use super::survey::Survey;
use crate::{ImagingRequest, SaveModel, SpecMode};

/// The compiled spectral axis and the source channels it reads.
pub(super) struct PreparedSpectralAxis {
    /// Each store's source channels the axis reads, in store order.
    pub(super) selected_source_channels: Vec<WindowChannels>,
    /// Each store's windows whose times read only some of their selected
    /// channels ([`frequency_bands`]), by window number, in store order.
    pub(super) frequency_bands: Vec<WindowBands>,
    pub(super) source_frame: FrequencyFrame,
    pub(super) output_frequency_reference: FrequencyRef,
    pub(super) output_frame: FrequencyFrame,
    pub(super) anchor: SpectralFrameAnchor,
    pub(super) wcs: SpectralWcs,
    pub(super) rest_frequency: RestFrequency,
    /// The spectral axis the image header records: the gridded axis, but
    /// for an MFS image's frame and rest frequencies.
    pub(super) image: ImageSpectralCoordinate,
    pub(super) doppler: DopplerConvention,
    pub(super) sampling: SpectralSamplingLaw,
    pub(super) basis: ReconstructionBasis,
    pub(super) output_channels: usize,
    /// Centre of the first output channel.
    pub(super) reference_frequency_hz: f64,
    /// Channel increment; a continuum plane's full band.
    pub(super) increment_hz: f64,
}

impl PreparedSpectralAxis {
    /// The model's spectral coordinate of this axis.
    pub(super) fn coordinate(&self) -> SpectralCoordinateSpec {
        SpectralCoordinateSpec::new(
            self.source_frame,
            self.output_frame,
            self.anchor,
            self.wcs.clone(),
            self.rest_frequency,
            self.doppler,
        )
    }

    /// The source channels the axis reads from surveyed window `spw_id` of
    /// store `store`.
    pub(super) fn window_channels(&self, store: usize, spw_id: usize) -> &[usize] {
        self.selected_source_channels[store]
            .get(&spw_id)
            .expect("the spectral axis selects channels of every surveyed window")
    }
}

/// Each window's per-time frequency selection, by window number; a window
/// absent reads every selected channel at every time.
pub(super) type WindowBands = BTreeMap<usize, FrequencyBands>;

/// Where and when a frame conversion is evaluated.
pub(super) struct FrameContext<'a> {
    pub(super) anchor_time_mjd_seconds: f64,
    pub(super) field_id: usize,
    pub(super) phase: MDirection,
    pub(super) direction: DirectionCoordinateSpec,
    pub(super) engine: &'a MsCalEngine,
}

/// The output axis of one spectral mode, before its frame anchor.
struct AxisLaw {
    selected_source_channels: Vec<WindowChannels>,
    frequency_bands: Vec<WindowBands>,
    output_frequency_reference: FrequencyRef,
    reference_frequency_hz: f64,
    increment_hz: f64,
    output_channels: usize,
    rest_frequency: RestFrequency,
    image: ImageSpectralCoordinate,
    doppler: DopplerConvention,
    sampling: SpectralSamplingLaw,
    basis: ReconstructionBasis,
}

/// The request's spectral axis over the surveyed selection of `stores`,
/// laid on store `reference`'s metadata (CASA's `msobj`): a continuum
/// plane over `continuum`, the band of every store, or a cube on the
/// reference store's window that reads each store's window where it reaches
/// the cube.
pub(super) fn prepare_axis(
    request: &ImagingRequest,
    (stores, reference): (&[SurveyedStore], usize),
    continuum: Option<&ContinuumBand>,
    frame: &FrameContext<'_>,
    moving_rest_frame: Option<&MeasFrame>,
) -> Result<PreparedSpectralAxis, PrepareError> {
    let store = &stores[reference];
    let (msv2, catalogue) = (
        store
            .msv2()
            .map(|observation| observation.measurement_set()),
        store.catalogue(),
    );
    let survey = &store.survey;
    let source_reference = survey.source_frequency_reference;
    let law = match request.specmode {
        SpecMode::Mfs => continuum_law(
            request,
            (msv2, catalogue),
            survey,
            continuum.expect("prepare spans the band of every continuum run"),
        )?,
        SpecMode::Cube | SpecMode::Cubedata => cube_law(request, (stores, reference), frame)?,
        SpecMode::Cubesource => {
            super::stores::one_store(stores, "a source-frame cube (specmode cubesource)")?;
            let frame_with_velocity = moving_rest_frame
                .expect("prepare resolves the moving rest frame of every source-frame cube");
            source_frame_cube_law(request, store, frame, frame_with_velocity)?
        }
    };
    let source_frame = imaging_frequency_frame(source_reference)?;
    let output_frame = imaging_frequency_frame(law.output_frequency_reference)?;
    Ok(PreparedSpectralAxis {
        selected_source_channels: law.selected_source_channels,
        frequency_bands: law.frequency_bands,
        source_frame,
        output_frequency_reference: law.output_frequency_reference,
        output_frame,
        anchor: spectral_frame_anchor(source_frame, output_frame, request.specmode, frame)?,
        wcs: SpectralWcs::Linear {
            channels: law.output_channels,
            reference_pixel: 0.0,
            reference_frequency_hz: law.reference_frequency_hz,
            increment_hz: law.increment_hz,
        },
        rest_frequency: law.rest_frequency,
        // CASA lists tclean's further rest frequencies after the image's.
        image: ImageSpectralCoordinate {
            additional_rest_frequencies_hz: request.restfreq.iter().skip(1).copied().collect(),
            ..law.image
        },
        doppler: law.doppler,
        sampling: law.sampling,
        basis: law.basis,
        output_channels: law.output_channels,
        reference_frequency_hz: law.reference_frequency_hz,
        increment_hz: law.increment_hz,
    })
}

/// One plane over the selected band's envelope over every store (`band`),
/// gridded in LSRK as CASA's `FTMachine::matchChannel` grids it whatever
/// the image's `outframe`. Its header records the envelope in `outframe`
/// (`buildCoordinateSystem`'s `MSUtil::getFreqRangeInSpw`) and `restfreq`'s
/// first rest frequency, else the reference store's (`survey`'s) first
/// visibility buffer's source line ([`continuum_rest_frequency_hz`] on an
/// MSv2, whose `SOURCE` and `DOPPLER` tables CASA reads; the catalogue's
/// first line on an MSv4), else the recorded band's midpoint (`getImFreq`).
fn continuum_law(
    request: &ImagingRequest,
    (msv2, catalogue): (Option<&MeasurementSet>, &Catalogue),
    survey: &Survey,
    band: &ContinuumBand,
) -> Result<AxisLaw, PrepareError> {
    let (channels, envelope, recorded) = (&band.channels, &band.gridded, &band.recorded);
    let [lower_hz, upper_hz] = envelope.edges_hz();
    let [recorded_lower_hz, recorded_upper_hz] = recorded.edges_hz();
    // `getImFreq`: a given reffreq is the reference at pixel 0, with the
    // band widened by twice its offset from the band's centre (CAS-9518).
    let reference = |midpoint_hz: f64, width_hz: f64| match request.reffreq {
        Some(reference_hz) => (
            reference_hz,
            width_hz + 2.0 * (reference_hz - midpoint_hz).abs(),
        ),
        None => (midpoint_hz, width_hz),
    };
    if request.reffreq.is_some() && request.outframe != FrequencyRef::LSRK {
        return Err(PrepareError::ReferenceFrequencyOutsideLsrk {
            outframe: request.outframe,
        });
    }
    let (reference_frequency_hz, increment_hz) =
        reference(envelope.midpoint_hz(), upper_hz - lower_hz);
    let (image_reference_hz, image_increment_hz) = reference(
        recorded.midpoint_hz(),
        recorded_upper_hz - recorded_lower_hz,
    );
    let field_id = survey.first_visibility_field_id;
    let spw_id = survey.spectral_windows[0].spw_id;
    let image_rest_frequency_hz = match (request.restfreq.first(), msv2) {
        (Some(&hertz), _) => hertz,
        (None, Some(ms)) => {
            continuum_rest_frequency_hz(ms, field_id, spw_id, recorded.midpoint_hz())?
        }
        (None, None) => catalogue
            .source_records(
                FieldId(u32::try_from(field_id).expect("a FIELD row is an u32 id")),
                WindowId(u32::try_from(spw_id).expect("a window id fits u32")),
            )
            .first()
            .and_then(|record| record.rest_frequencies.as_ref()?.first())
            .map(casa_types::measures::frequency::MFrequency::hz)
            .filter(|value| *value != 0.0)
            .unwrap_or(recorded.midpoint_hz()),
    };
    Ok(AxisLaw {
        selected_source_channels: channels.clone(),
        frequency_bands: vec![WindowBands::new(); channels.len()],
        output_frequency_reference: FrequencyRef::LSRK,
        reference_frequency_hz,
        increment_hz,
        output_channels: 1,
        rest_frequency: RestFrequency::NotApplicable,
        image: ImageSpectralCoordinate {
            frequency_reference: request.outframe,
            reference_frequency_hz: image_reference_hz,
            increment_hz: image_increment_hz,
            rest_frequency_hz: image_rest_frequency_hz,
            additional_rest_frequencies_hz: Vec::new(),
            undefined_frame: false,
        },
        doppler: DopplerConvention::NotApplicable,
        sampling: SpectralSamplingLaw::IDENTITY,
        basis: ReconstructionBasis::Constant,
    })
}

/// CASA `SynthesisParamsImage::buildCoordinateSystemCore` asks
/// `MSDopplerUtil::dopplerInfo` for the first selected SPW and field. Its
/// first rest frequency wins; an absent or zero value uses the MFS band's
/// midpoint. This header label does not change the continuum/Taylor basis.
fn continuum_rest_frequency_hz(
    ms: &MeasurementSet,
    field_id: usize,
    spw_id: usize,
    band_midpoint_hz: f64,
) -> Result<f64, PrepareError> {
    let source_id = ms.field()?.source_id(field_id)?;
    let window = ms
        .subtable(SubtableId::SpectralWindow)
        .expect("required SPW table");
    let doppler_id = if window
        .schema()
        .is_some_and(|schema| schema.contains_column("DOPPLER_ID"))
    {
        let scalar = window
            .cell_accessor(spw_id, "DOPPLER_ID")
            .map_err(casa_ms::MsError::from)?
            .scalar()
            .map_err(casa_ms::MsError::from)?;
        match scalar {
            casa_types::ScalarValue::Int32(value) => *value,
            _ => {
                return Err(
                    casa_ms::MsError::InvalidInput("DOPPLER_ID must be Int32".into()).into(),
                );
            }
        }
    } else {
        -1
    };
    if doppler_id >= 0 && ms.subtable(SubtableId::Doppler).is_some() {
        return Err(casa_ms::MsError::InvalidInput(
            "MFS rest-frequency metadata with a DOPPLER table is not supported".into(),
        )
        .into());
    }
    if ms.subtable(SubtableId::Source).is_none() {
        return Ok(band_midpoint_hz);
    }
    let resolved = continuum_source_line(ms, source_id, spw_id)?;
    Ok(resolved
        .filter(|value| *value != 0.0)
        .unwrap_or(band_midpoint_hz))
}

/// The first SOURCE line within a matching key group; -1 is a wildcard.
/// CASA binary-searches sorted unique keys using its wildcard comparator.
/// With multiple key groups, wildcards can select or hide a matching group;
/// refuse that unresolved case instead of merging physical row order.
fn continuum_source_line(
    ms: &MeasurementSet,
    source_id: i32,
    spw_id: usize,
) -> Result<Option<f64>, PrepareError> {
    let source = ms.source()?;
    let spw_id = i32::try_from(spw_id).expect("SPW ids are stored i32 values");
    let mut first_key = None;
    let mut multiple_keys = false;
    let mut wildcard = false;
    let mut matching_key = None;
    let mut resolved = None;
    for row in 0..source.row_count() {
        let row_source = source.i32(row, "SOURCE_ID")?;
        let row_spw = source.i32(row, "SPECTRAL_WINDOW_ID")?;
        let key = (row_source, row_spw);
        multiple_keys |= first_key.is_some_and(|prior| prior != key);
        first_key.get_or_insert(key);
        wildcard |= row_source == -1 || row_spw == -1;
        if (row_source != source_id && row_source != -1) || (row_spw != spw_id && row_spw != -1) {
            continue;
        }
        if matching_key.is_some_and(|prior| prior != key) {
            return Err(casa_ms::MsError::InvalidInput(
                "overlapping SOURCE key groups for MFS rest-frequency metadata are not supported"
                    .into(),
            )
            .into());
        }
        matching_key = Some(key);
        if resolved.is_some() {
            continue;
        }
        let Some(ArrayValue::Float64(values)) = source.optional_array(row, "REST_FREQUENCY")?
        else {
            continue;
        };
        if values.is_empty() {
            continue;
        }
        resolved = values.first().copied();
    }
    if wildcard && multiple_keys {
        return Err(casa_ms::MsError::InvalidInput(
            "multiple SOURCE key groups with wildcard ids for MFS rest-frequency metadata are not supported".into(),
        ).into());
    }
    Ok(resolved)
}

/// Whether CASA tunes a `cube`'s frequency selection to the image's band
/// (`SynthesisImagerVi2::tuneSelectData`): when the window of its first
/// visibility buffer has 30 or more channels, or when the model is saved to
/// `MODEL_DATA`, `SynthesisImager::tuneSelectData` narrows each window's
/// bands to the LSRK frequencies from pixel −1.5 to `nchan + 0.5` of the
/// image axis ([`frequency_bands::cube_bands`]); a `cubedata` or
/// `cubesource` cube keeps the selection. The tuned band keeps every
/// channel a plane is interpolated from, since CASA refuses planes
/// narrower than a channel (`calcChanFreqs`), so tuning changes which
/// channels are read but not the image: a window the axis misses is still
/// read (its bands are kept where the image's overlaps none of them), and
/// the density `VisImagingWeight` forms is the untuned selection's.
/// casa-rs's visibility-domain continuum fit (`fitspw`, which CASA's
/// tclean lacks) keeps the named channels as its output channels.
fn tunes_selection(request: &ImagingRequest, first_window: &SourceSpectralWindow) -> bool {
    request.specmode == SpecMode::Cube
        && request.fitspw.is_none()
        && (first_window.frequencies_hz.len() >= 30 || request.savemodel == SaveModel::ModelColumn)
}

/// CASA's cube axis over the reference store's selected windows
/// (`buildCoordinateSystemCore` on `msobj`'s windows): one window's
/// channels, or several windows' merged into one axis
/// ([`casa_ms::combine_spectral_windows`]), with `axis`'s grid laid over
/// them; and the band their `spw` selection spans in the data's frame
/// ([`casa_ms::casa_selected_band_hz`]). Every window's samples are
/// gridded onto the one axis (`FTMachine::matchChannel`).
fn cube_setup(
    request: &ImagingRequest,
    survey: &Survey,
    frame: &FrameContext<'_>,
    mut axis: CubeAxisConfig,
) -> Result<(CubeSpectralSetup, [f64; 2]), PrepareError> {
    let windows = &survey.spectral_windows;
    let band_hz = casa_ms::casa_selected_band_hz(windows.iter().map(|window| {
        (
            window.frequencies_hz.as_slice(),
            window.channel_widths_hz.as_slice(),
            survey.spw_channels.get(&window.spw_id).map(Vec::as_slice),
        )
    }))
    .ok_or(PrepareError::SpectralWindowCoordinates {
        spw_id: windows[0].spw_id,
    })?;
    axis.selected_band_hz = Some(band_hz);
    let (frequencies_hz, widths_hz) = match windows.as_slice() {
        [window] => (
            window.frequencies_hz.clone(),
            window.channel_widths_hz.clone(),
        ),
        _ => casa_ms::combine_spectral_windows(
            &windows
                .iter()
                .map(|window| {
                    (
                        window.frequencies_hz.as_slice(),
                        window.channel_widths_hz.as_slice(),
                    )
                })
                .collect::<Vec<_>>(),
        )?,
    };
    let setup = CubeSpectralSetup::for_casa_cube_axis(
        survey.source_frequency_reference,
        &frequencies_hz,
        &widths_hz,
        request.channel_count,
        &axis,
        frame.anchor_time_mjd_seconds,
        frame.field_id,
        Some(frame.phase.clone()),
        frame.engine,
    )?;
    Ok((setup, band_hz))
}

/// What a cube on `setup` reads of each store, in store order: the
/// channels of each window, and the bands of the windows whose times read
/// only some of them ([`store_reads`]). Whether CASA tunes the selection
/// to the image's band is decided once, from the iterator's first buffer,
/// the first store's ([`tunes_selection`]), and applies to every store
/// (`SynthesisImager::tuneSelectData`).
///
/// The reference store's windows are those the axis is laid on; another
/// store's one window is read where it reaches the axis, as CASA's cube
/// gridding selects each MeasurementSet's channels in the image's band.
///
/// # Errors
///
/// An axis none of the reference store's windows reaches; another store
/// of several windows, or of another source frame than the reference
/// store's; a frame conversion that fails.
fn cube_store_reads(
    request: &ImagingRequest,
    (stores, reference): (&[SurveyedStore], usize),
    frame: &FrameContext<'_>,
    setup: &CubeSpectralSetup,
) -> Result<(Vec<WindowChannels>, Vec<WindowBands>), PrepareError> {
    let first = &stores[0].survey;
    let first_window = first
        .spectral_windows
        .iter()
        .find(|window| window.spw_id == first.first_visibility_spw_id)
        .expect("the first visibility is in a surveyed window");
    let tuned = tunes_selection(request, first_window);
    let image_band = image_band(frame, setup, tuned)?;
    let mut channels = Vec::with_capacity(stores.len());
    let mut bands = Vec::with_capacity(stores.len());
    for (index, store) in stores.iter().enumerate() {
        let survey = &store.survey;
        let field = if index == reference {
            frame.field_id
        } else {
            one_window(&survey.spectral_windows, "native cube imaging")?;
            if survey.source_frequency_reference != setup.source_freq_ref {
                return Err(PrepareError::MixedSourceFrames);
            }
            usize::try_from(*survey.fields.first().expect("a surveyed store's field"))
                .expect("a surveyed field is a FIELD row")
        };
        let reads = store_reads(
            request,
            ((store.catalogue(), &store.engine), survey, field),
            setup,
            (image_band, tuned),
        )?;
        if index == reference && !reads.reaches_axis {
            return Err(PrepareError::NoCommonCubeChannels);
        }
        channels.push(reads.channels);
        bands.push(reads.bands);
    }
    Ok((channels, bands))
}

/// What a cube reads of one store ([`store_reads`]).
struct StoreReads {
    channels: WindowChannels,
    bands: WindowBands,
    /// Whether some window's channels reach the axis.
    reaches_axis: bool,
}

/// The channels a cube on `setup` reads of each window of one store's
/// `survey`, its samples' frequencies converted towards `field`: those
/// whose samples reach the axis
/// ([`CubeSpectralSetup::source_channel_support`]) and that the window's
/// frequency bands, tuned to the image's band `image_band` where CASA
/// tunes the selection ([`tunes_selection`]), select at some epoch of its
/// samples ([`frequency_bands`]); where CASA selects by channel numbers
/// instead, those `spw` names unless it tunes. Where the bands do not
/// select every channel read at every epoch, the window carries them and
/// each time reads only the channels they select then, as CASA's iterator
/// does.
///
/// A window none of whose channels reaches the axis is read as `spw`
/// selects it, tuned or not, as CASA does: its samples fall outside every
/// plane, but `VisImagingWeight`'s density counts them, since CASA forms it
/// over the untuned selection of the imager that defines the cube, and a
/// final pass writes the cube model's prediction there, zero, into its
/// `MODEL_DATA` (test_modelvis_29).
fn store_reads(
    request: &ImagingRequest,
    ((catalogue, engine), survey, field): ((&Catalogue, &MsCalEngine), &Survey, usize),
    setup: &CubeSpectralSetup,
    (image_band, tuned): (Option<[f64; 2]>, bool),
) -> Result<StoreReads, PrepareError> {
    let bands = cube_bands(request, (catalogue, engine), survey, image_band)?;
    let mut reads = StoreReads {
        channels: WindowChannels::new(),
        bands: WindowBands::new(),
        reaches_axis: false,
    };
    for window in &survey.spectral_windows {
        let mut channels = setup
            .source_channel_support(
                &window.frequencies_hz,
                &window.channel_widths_hz,
                survey.time_bounds_mjd_seconds,
                field,
                engine,
            )?
            .indices;
        let selection = match &bands {
            Some(bands) => bands.epoch_selection((catalogue, engine), survey, window)?,
            None => None,
        };
        let explicit = survey.spw_channels.get(&window.spw_id);
        match (&selection, explicit) {
            (Some(selection), _) => channels.retain(|channel| selection.ever(*channel)),
            (None, Some(explicit)) if !tuned => {
                channels.retain(|channel| explicit.contains(channel));
            }
            _ => {}
        }
        if channels.is_empty() {
            // `spw`'s channels, or the whole window when it names none.
            channels = explicit
                .cloned()
                .unwrap_or_else(|| (0..window.frequencies_hz.len()).collect());
        } else {
            reads.reaches_axis = true;
            if let (Some(bands), Some(selection)) = (&bands, &selection)
                && channels.iter().any(|channel| !selection.always(*channel))
                && let Some(window_bands) = bands.frequency_bands(window.spw_id)?
            {
                reads.bands.insert(window.spw_id, window_bands);
            }
        }
        reads.channels.insert(window.spw_id, channels);
    }
    Ok(reads)
}

/// The image's LSRK band a `cube` tunes its selection to
/// ([`frequency_bands::image_band_lsrk`]), where CASA tunes it.
fn image_band(
    frame: &FrameContext<'_>,
    setup: &CubeSpectralSetup,
    tuned: bool,
) -> Result<Option<[f64; 2]>, PrepareError> {
    if !tuned {
        return Ok(None);
    }
    let channels = setup.output_channel_frequencies_hz.len();
    let (first_hz, increment_hz) = first_and_increment(setup, channels, 1.0)?;
    Ok(Some(frequency_bands::image_band_lsrk(
        frame,
        setup.output_freq_ref,
        (first_hz, increment_hz, channels),
    )?))
}

/// The bands a `cube`'s gridding selects one store's windows' channels by
/// at each time ([`frequency_bands::cube_bands`]), tuned to the image's
/// band `image_band` where CASA tunes the selection; none for any other
/// mode, which CASA selects by channel numbers (`cubedata`, `cubesource`),
/// or a `fitspw` fit, which reads its channels as named.
fn cube_bands(
    request: &ImagingRequest,
    (catalogue, engine): (&Catalogue, &MsCalEngine),
    survey: &Survey,
    image_band: Option<[f64; 2]>,
) -> Result<Option<frequency_bands::CubeBands>, PrepareError> {
    if request.specmode != SpecMode::Cube || request.fitspw.is_some() {
        return Ok(None);
    }
    frequency_bands::cube_bands((catalogue, engine), survey, request.outframe, image_band)
}

/// A cube in its output frame (`cube`) or the data's (`cubedata`), laid
/// on the windows of store `reference` (CASA's `msobj`), reading each
/// store's channels where they reach it ([`cube_store_reads`]).
fn cube_law(
    request: &ImagingRequest,
    (stores, reference): (&[SurveyedStore], usize),
    frame: &FrameContext<'_>,
) -> Result<AxisLaw, PrepareError> {
    let store = &stores[reference];
    let (catalogue, survey) = (store.catalogue(), &store.survey);
    let (setup, band_hz) = cube_setup(request, survey, frame, cube_axis(request)?)?;
    let (selected_source_channels, frequency_bands) =
        cube_store_reads(request, (stores, reference), frame, &setup)?;
    let output_channels = setup.output_channel_frequencies_hz.len();
    let (reference_frequency_hz, increment_hz) = first_and_increment(&setup, output_channels, 1.0)?;
    let source_rest_hz =
        source_rest_frequency(catalogue, frame.field_id, &survey.spectral_windows)?;
    let (resolved_rest_hz, image_rest_frequency_hz) =
        cube_rest_frequency_hz(request.restfreq.first().copied(), source_rest_hz, band_hz);
    let (rest_frequency, doppler) = match resolved_rest_hz {
        None => (
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
        Some(hertz) => (
            RestFrequency::Line { hertz },
            doppler_convention(request.veltype)?,
        ),
    };
    Ok(AxisLaw {
        selected_source_channels,
        frequency_bands,
        output_frequency_reference: setup.output_freq_ref,
        reference_frequency_hz,
        increment_hz,
        output_channels,
        rest_frequency,
        image: ImageSpectralCoordinate {
            frequency_reference: setup.output_freq_ref,
            reference_frequency_hz,
            increment_hz,
            rest_frequency_hz: image_rest_frequency_hz,
            additional_rest_frequencies_hz: Vec::new(),
            undefined_frame: request.specmode == SpecMode::Cubedata
                && setup.output_freq_ref != FrequencyRef::REST,
        },
        doppler,
        sampling: sampling_law(setup.interpolation),
        basis: ReconstructionBasis::ChannelLocal {
            channels: output_channels,
        },
    })
}

/// A cube in the rest frame of a moving source: the data-frame axis
/// scaled into the source's rest frame (`cubesource`), on `store`'s one
/// window, reading its channels as [`store_reads`] reads a `cubedata`
/// cube's.
///
/// # Errors
///
/// Several selected windows: CASA's source-frame cube over several
/// windows is not yet ported (#1059).
fn source_frame_cube_law(
    request: &ImagingRequest,
    store: &SurveyedStore,
    frame: &FrameContext<'_>,
    moving_rest_frame: &MeasFrame,
) -> Result<AxisLaw, PrepareError> {
    let (catalogue, survey) = (store.catalogue(), &store.survey);
    if survey.spectral_windows.len() > 1 {
        return Err(PrepareError::SourceFrameCubeWindows {
            selected: survey.spectral_windows.len(),
        });
    }
    let source_reference = survey.source_frequency_reference;
    let axis = CubeAxisConfig {
        specmode: CubeSpecMode::Cubedata,
        outframe: source_reference,
        ..cube_axis(request)?
    };
    let (setup, _) = cube_setup(request, survey, frame, axis)?;
    let reads = store_reads(
        request,
        ((catalogue, &store.engine), survey, frame.field_id),
        &setup,
        (None, false),
    )?;
    if !reads.reaches_axis {
        return Err(PrepareError::NoCommonCubeChannels);
    }
    let output_channels = setup.output_channel_frequencies_hz.len();
    let factor = casa_ms::convert_frequency_to_frame_with_frame(
        source_reference,
        FrequencyRef::REST,
        1.0,
        Some(moving_rest_frame),
    )?;
    let (reference_frequency_hz, increment_hz) =
        first_and_increment(&setup, output_channels, factor)?;
    let rest_frequency_hz = request
        .restfreq
        .first()
        .copied()
        .or(source_rest_frequency(
            catalogue,
            frame.field_id,
            &survey.spectral_windows,
        )?)
        .ok_or(PrepareError::RestFrequencyMissing)?;
    Ok(AxisLaw {
        selected_source_channels: vec![reads.channels],
        frequency_bands: vec![reads.bands],
        output_frequency_reference: FrequencyRef::REST,
        reference_frequency_hz,
        increment_hz,
        output_channels,
        rest_frequency: RestFrequency::Line {
            hertz: rest_frequency_hz,
        },
        image: ImageSpectralCoordinate {
            frequency_reference: FrequencyRef::REST,
            reference_frequency_hz,
            increment_hz,
            rest_frequency_hz,
            additional_rest_frequencies_hz: Vec::new(),
            undefined_frame: false,
        },
        doppler: doppler_convention(request.veltype)?,
        sampling: sampling_law(request.interpolation),
        basis: ReconstructionBasis::ChannelLocal {
            channels: output_channels,
        },
    })
}

/// CASA's cube axis of the request: `start` (else `channel_start`) and
/// `width` in the request's velocity convention; [`cube_setup`] sets the
/// band `spw` selects.
fn cube_axis(request: &ImagingRequest) -> Result<CubeAxisConfig, PrepareError> {
    let start = match (&request.start, request.channel_start) {
        (Some(start), _) => Some(CubeAxisValue::parse(start, request.veltype)?),
        (None, Some(channel)) => Some(CubeAxisValue::Channel(
            i32::try_from(channel).map_err(|_| PrepareError::CubeStartChannel { channel })?,
        )),
        (None, None) => None,
    };
    let width = request
        .width
        .as_deref()
        .map(|width| CubeAxisValue::parse(width, request.veltype))
        .transpose()?;
    Ok(CubeAxisConfig {
        specmode: if request.specmode == SpecMode::Cubedata {
            CubeSpecMode::Cubedata
        } else {
            CubeSpecMode::Cube
        },
        outframe: request.outframe,
        veltype: request.veltype,
        interpolation: request.interpolation,
        rest_frequency_hz: request.restfreq.first().copied(),
        start,
        width,
        selected_band_hz: None,
    })
}

/// The first output channel's centre and the channel increment, scaled by
/// `factor`; a one-channel cube's increment is its width.
fn first_and_increment(
    setup: &CubeSpectralSetup,
    output_channels: usize,
    factor: f64,
) -> Result<(f64, f64), PrepareError> {
    let first_hz = setup.output_channel_frequencies_hz[0];
    let increment_hz = if output_channels > 1 {
        setup.output_channel_frequencies_hz[1] - first_hz
    } else {
        setup.output_channel_widths_hz[0]
    } * factor;
    if !increment_hz.is_finite() || increment_hz == 0.0 {
        return Err(PrepareError::CubeIncrement { increment_hz });
    }
    Ok((first_hz * factor, increment_hz))
}

/// The model's Doppler convention of CASA `veltype`.
fn doppler_convention(veltype: DopplerRef) -> Result<DopplerConvention, PrepareError> {
    match veltype {
        DopplerRef::RADIO => Ok(DopplerConvention::Radio),
        DopplerRef::Z => Ok(DopplerConvention::Optical),
        DopplerRef::BETA => Ok(DopplerConvention::Relativistic),
        DopplerRef::RATIO | DopplerRef::GAMMA => Err(PrepareError::UnsupportedDoppler {
            convention: veltype,
        }),
    }
}

const fn sampling_law(interpolation: CubeInterpolation) -> SpectralSamplingLaw {
    match interpolation {
        CubeInterpolation::Nearest => SpectralSamplingLaw::NEAREST,
        CubeInterpolation::Linear => SpectralSamplingLaw::LINEAR,
        CubeInterpolation::Cubic => SpectralSamplingLaw::CUBIC,
    }
}

/// The image's epoch, reference direction and observatory, where the
/// rows' frame and the output frame differ, or where a `cube` is gridded
/// through LSRK ([`FrequencyFrame::gridded_through_lsrk`]): CASA grids a
/// `cube` (not `cubedata`, whose rows keep their frequencies) in LSRK
/// whatever its `outframe`, so a TOPO cube of TOPO rows is fixed in LSRK at
/// the first time and each row is Doppler tracked from it.
fn spectral_frame_anchor(
    source_frame: FrequencyFrame,
    output_frame: FrequencyFrame,
    specmode: SpecMode,
    frame: &FrameContext<'_>,
) -> Result<SpectralFrameAnchor, PrepareError> {
    let tracked = specmode == SpecMode::Cube && output_frame.gridded_through_lsrk();
    if source_frame == output_frame && !tracked {
        return Ok(SpectralFrameAnchor::NotApplicable);
    }
    let [x_metres, y_metres, z_metres] = frame.engine.observatory_position().as_itrf();
    Ok(SpectralFrameAnchor::Conversion {
        // As `MEpoch::from_mjd_seconds` reads a row's time, so a row at the
        // first time is at the anchor's epoch exactly.
        epoch: Epoch::new(
            frame.anchor_time_mjd_seconds * (1.0 / 86_400.0),
            imaging_time_scale(frame.engine.time_reference())?,
        ),
        direction: frame.direction.reference_direction(),
        observatory_position: ItrfPosition::new(x_metres, y_metres, z_metres),
    })
}

pub(super) fn imaging_frequency_frame(
    reference: FrequencyRef,
) -> Result<FrequencyFrame, PrepareError> {
    match reference {
        FrequencyRef::REST => Ok(FrequencyFrame::Rest),
        FrequencyRef::TOPO => Ok(FrequencyFrame::Topocentric),
        FrequencyRef::BARY => Ok(FrequencyFrame::Barycentric),
        FrequencyRef::LSRK => Ok(FrequencyFrame::Lsrk),
        _ => Err(PrepareError::UnsupportedFrequencyFrame { reference }),
    }
}

pub(super) fn imaging_time_scale(reference: EpochRef) -> Result<TimeScale, PrepareError> {
    match reference {
        EpochRef::UTC => Ok(TimeScale::Utc),
        EpochRef::TAI => Ok(TimeScale::Tai),
        EpochRef::TT => Ok(TimeScale::Tt),
        EpochRef::TDB => Ok(TimeScale::Tdb),
        _ => Err(PrepareError::UnsupportedEpochReference { reference }),
    }
}

/// The rest frequency the field's source states over the selected windows
/// (MSv2 `SOURCE::REST_FREQUENCY`, MSv4 `LINE_REST_FREQUENCY`), if any: the
/// first line of each record for a selected window or for every window
/// ([`Catalogue::source_records`]), which must agree.
fn source_rest_frequency(
    catalogue: &Catalogue,
    field_id: usize,
    spectral_windows: &[SourceSpectralWindow],
) -> Result<Option<f64>, PrepareError> {
    let field = FieldId(u32::try_from(field_id).expect("a FIELD row is an u32 id"));
    let mut resolved = None;
    for window in spectral_windows {
        let window = WindowId(u32::try_from(window.spw_id).expect("a window id fits u32"));
        for record in catalogue.source_records(field, window) {
            let Some(value) = record
                .rest_frequencies
                .as_ref()
                .and_then(|lines| lines.first())
                .map(casa_types::measures::frequency::MFrequency::hz)
                .filter(|value| *value > 0.0)
            else {
                continue;
            };
            if resolved.is_some_and(|prior: f64| prior.to_bits() != value.to_bits()) {
                return Err(PrepareError::RestFrequencyConflict);
            }
            resolved = Some(value);
        }
    }
    Ok(resolved)
}

/// CASA's precedence for a cube's rest frequency: the request's, else the
/// source's; the header records the centre of the selected band `band_hz`
/// when neither exists (`buildCoordinateSystemCore`'s provisional rest
/// frequency, `(datafend + datafstart)/2`).
fn cube_rest_frequency_hz(
    explicit_hz: Option<f64>,
    source_hz: Option<f64>,
    band_hz: [f64; 2],
) -> (Option<f64>, f64) {
    let resolved = explicit_hz.or(source_hz);
    (
        resolved,
        resolved.unwrap_or((band_hz[1] + band_hz[0]) / 2.0),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::tests::request;
    use serde_json::json;

    /// `SynthesisImagerVi2::tuneSelectData`: a `cube` over 30 or more
    /// channels, or saving its model to `MODEL_DATA`, reads the image's
    /// band; a `cubedata` cube or a narrower window keeps the selection.
    #[test]
    fn a_cube_reads_the_image_band_as_casa_tunes_its_selection() {
        let window = |channels: usize| SourceSpectralWindow {
            spw_id: 0,
            frequency_reference: FrequencyRef::TOPO,
            frequencies_hz: (0..channels)
                .map(|channel| 1.0e9 + channel as f64)
                .collect(),
            channel_widths_hz: vec![1.0; channels],
        };
        let cube = request(json!({ "specmode": "cube", "nchan": 4 }));
        assert!(tunes_selection(&cube, &window(30)));
        assert!(!tunes_selection(&cube, &window(29)));
        let saved = request(json!({ "specmode": "cube", "nchan": 4, "savemodel": "modelcolumn" }));
        assert!(tunes_selection(&saved, &window(20)));
        let cubedata = request(json!({ "specmode": "cubedata", "nchan": 4 }));
        assert!(!tunes_selection(&cubedata, &window(30)));
        let fitted = request(json!({ "specmode": "cube", "nchan": 4, "fitspw": "0:0~2" }));
        assert!(!tunes_selection(&fitted, &window(30)));
    }

    #[test]
    fn cube_rest_frequency_follows_casa_precedence() {
        let band_hz = [43.9e9, 109.5e9];
        assert_eq!(
            cube_rest_frequency_hz(Some(115.0e9), Some(110.0e9), band_hz),
            (Some(115.0e9), 115.0e9)
        );
        assert_eq!(
            cube_rest_frequency_hz(None, Some(110.0e9), band_hz),
            (Some(110.0e9), 110.0e9)
        );
        assert_eq!(cube_rest_frequency_hz(None, None, band_hz), (None, 76.7e9));
    }
}

#[cfg(test)]
#[path = "spectral_tests.rs"]
mod spectral_tests;
