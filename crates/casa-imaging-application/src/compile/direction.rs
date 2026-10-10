// SPDX-License-Identifier: LGPL-3.0-or-later

//! The image centre (a fixed direction, the selected fields' ephemerides, or
//! a named or external ephemeris), its direction coordinate, and the image
//! coordinate system the products are written in.

use std::path::Path;
use std::sync::Arc;

use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, ObsInfo, Projection as CoordinateProjection,
    ProjectionType, SpectralCoordinate, StokesCoordinate, StokesType,
};
use casa_imaging_model::{
    DirectionCoordinateSpec, DirectionFrame, PhaseCentreLaw, PolarizationCoordinate, Projection,
    SkyDirection,
};
use casa_ms::{MeasurementSet, SelectedObservationContentBudget, SelectedObservationEphemeris};
use casa_types::measures::{
    MeasuresProvider,
    direction::{DirectionRef, MDirection},
    epoch::MEpoch,
    frequency::FrequencyRef,
};

use super::boxed;
use super::selection::Survey;
use crate::{ApplicationError, ImagingRequest};

/// The resolved image centre.
pub(super) struct Centre {
    pub(super) law: PhaseCentreLaw,
    pub(super) ephemeris: Option<SelectedObservationEphemeris>,
    /// The J2000 phase centre frame conversions are evaluated at.
    pub(super) phase: MDirection,
    /// The image's reference direction, in the frame the image records.
    pub(super) image_centre: MDirection,
    pub(super) direction: DirectionCoordinateSpec,
    /// The `FIELD_ID` whose phase centre anchors the image.
    pub(super) field_id: usize,
    pub(super) measures: Arc<dyn MeasuresProvider>,
}

/// Resolve the request's image centre over the surveyed fields.
pub(super) fn resolve_centre(
    request: &ImagingRequest,
    ms: &MeasurementSet,
    survey: &Survey,
    engine: &casa_ms::derived::engine::MsCalEngine,
    budget: SelectedObservationContentBudget,
) -> Result<Centre, ApplicationError> {
    let selected_field = request
        .phasecenter_field
        .unwrap_or_else(|| *survey.fields.first().expect("nonempty selection"));
    if !survey.fields.contains(&selected_field) {
        return Err(boxed(format!(
            "phase-center FIELD_ID {selected_field} is not part of selected fields {:?}",
            survey.fields
        )));
    }
    let field_id =
        usize::try_from(selected_field).map_err(|_| boxed("selected FIELD_ID is negative"))?;
    let stored_phase = casa_ms::derived::engine::raw_field_phase_direction(ms, field_id)?;
    let field_phase = casa_ms::derived::engine::resolve_field_phase_direction_j2000(ms, field_id)?;
    let field_direction = SkyDirection::new(
        DirectionFrame::J2000,
        field_phase.as_angles().0,
        field_phase.as_angles().1,
    );
    let anchor_time = survey.first_time_mjd_seconds;
    let measures = casa_ms::open_measures_runtime()?;
    let ephemeris_direction = |name: &str, ephemeris: &SelectedObservationEphemeris| {
        let direction = engine.ephemeris_direction_j2000(anchor_time, field_id, name, ephemeris)?;
        let (longitude, latitude) = direction.as_angles();
        Ok::<_, ApplicationError>(SkyDirection::new(
            DirectionFrame::J2000,
            longitude,
            latitude,
        ))
    };
    let (law, ephemeris, main_direction) = match request.phasecenter.as_deref() {
        None => (
            PhaseCentreLaw::Fixed(field_direction),
            None,
            field_direction,
        ),
        Some(text) if text.split_ascii_whitespace().next() == Some("J2000") => {
            let direction = parse_phase_center_direction(text)?;
            (PhaseCentreLaw::Fixed(direction), None, direction)
        }
        Some("TRACKFIELD") => {
            let ephemeris = SelectedObservationEphemeris::tracked_fields(
                ms,
                survey
                    .fields
                    .iter()
                    .map(|field| usize::try_from(*field).expect("validated FIELD_ID")),
                budget.reference_data_budget(),
            )?;
            let direction = ephemeris_direction("TRACKFIELD", &ephemeris)?;
            (
                PhaseCentreLaw::Ephemeris("TRACKFIELD".to_string()),
                Some(ephemeris),
                direction,
            )
        }
        Some(text) => {
            let ephemeris = named_ephemeris(text, ms, survey, budget)?;
            let direction = ephemeris_direction(text, &ephemeris)?;
            (
                PhaseCentreLaw::Ephemeris(text.to_string()),
                Some(ephemeris),
                direction,
            )
        }
    };
    let phase = MDirection::from_angles(
        main_direction.longitude_rad(),
        main_direction.latitude_rad(),
        DirectionRef::J2000,
    );
    let (image_centre, frame) = image_centre(
        request,
        engine,
        anchor_time,
        &phase,
        stored_phase,
        ephemeris.is_some(),
    )?;
    let (longitude, latitude) = image_centre.as_angles();
    Ok(Centre {
        law,
        ephemeris,
        phase,
        direction: direction_spec(request.imsize, request.cell, frame, longitude, latitude),
        image_centre,
        field_id,
        measures,
    })
}

/// The image's reference direction and the frame it records: ICRS at the
/// anchor time for a moving source, the field's stored direction in its
/// own frame when the request names no centre, else the J2000 phase
/// centre.
fn image_centre(
    request: &ImagingRequest,
    engine: &casa_ms::derived::engine::MsCalEngine,
    anchor_time: f64,
    phase: &MDirection,
    stored_phase: MDirection,
    moving: bool,
) -> Result<(MDirection, DirectionFrame), ApplicationError> {
    if moving {
        let frame = engine.spectral_frame_observatory_direction(anchor_time, phase.clone())?;
        return Ok((
            phase.convert_to(DirectionRef::ICRS, &frame)?,
            DirectionFrame::Icrs,
        ));
    }
    let centre = if request.phasecenter.is_none()
        && matches!(
            stored_phase.refer(),
            DirectionRef::J2000 | DirectionRef::ICRS | DirectionRef::B1950 | DirectionRef::GALACTIC
        ) {
        stored_phase
    } else {
        phase.clone()
    };
    let frame = match centre.refer() {
        DirectionRef::ICRS => DirectionFrame::Icrs,
        DirectionRef::B1950 => DirectionFrame::B1950,
        DirectionRef::GALACTIC => DirectionFrame::Galactic,
        _ => DirectionFrame::J2000,
    };
    Ok((centre, frame))
}

/// A named ephemeris (or an external table), joined with the selected
/// fields' attached ephemerides when they have them.
fn named_ephemeris(
    text: &str,
    ms: &MeasurementSet,
    survey: &Survey,
    budget: SelectedObservationContentBudget,
) -> Result<SelectedObservationEphemeris, ApplicationError> {
    let budget = budget.reference_data_budget();
    let mut ephemeris = if Path::new(text).is_dir() {
        SelectedObservationEphemeris::external(text, budget)?
    } else {
        SelectedObservationEphemeris::named(text, budget)?
    };
    let field = ms.field()?;
    let mut attached = Vec::new();
    for field_id in &survey.fields {
        let field_id = usize::try_from(*field_id)?;
        if field
            .ephemeris_id(field_id)?
            .is_some_and(|value| value >= 0)
        {
            attached.push(field_id);
        }
    }
    if attached.is_empty() {
        return Ok(ephemeris);
    }
    if attached.len() != survey.fields.len() {
        return Err(boxed(
            "moving-source selection mixes FIELD rows with and without attached ephemerides",
        ));
    }
    ephemeris = ephemeris.with_attached_fields(
        SelectedObservationEphemeris::tracked_fields(ms, attached, budget)?,
        budget,
    )?;
    Ok(ephemeris)
}

/// The image's observation record: telescope and observer of the selected
/// observations, which must agree, the first selected time and the image
/// centre.
pub(super) fn observation_info(
    ms: &MeasurementSet,
    survey: &Survey,
    centre: &Centre,
    engine: &casa_ms::derived::engine::MsCalEngine,
) -> Result<ObsInfo, ApplicationError> {
    let observation = ms.observation()?;
    let mut labels = std::collections::BTreeSet::new();
    for id in &survey.observation_ids {
        let id = usize::try_from(*id).map_err(|_| boxed("selected OBSERVATION_ID is negative"))?;
        labels.insert(if id < observation.row_count() {
            (
                observation.string(id, "TELESCOPE_NAME")?,
                observation.string(id, "OBSERVER")?,
            )
        } else {
            (String::new(), String::new())
        });
    }
    if labels.len() != 1 {
        return Err(boxed(format!(
            "image observation metadata requires consistent telescope and observer labels for \
             selected OBSERVATION_IDs {:?}; found {labels:?}",
            survey.observation_ids
        )));
    }
    let (telescope_name, observer) = labels.pop_first().expect("one label pair");
    // ObsInfo::toRecord uses MVDirection::get, preserving the signed atan2
    // endpoint rather than mapping an exactly positive pi to negative pi.
    let [pointing_x, pointing_y, _] = centre.image_centre.cosines();
    let pointing_longitude = if pointing_x == 0.0 && pointing_y == 0.0 {
        0.0
    } else {
        pointing_y.atan2(pointing_x)
    };
    Ok(ObsInfo::new(telescope_name)
        .with_observer(observer)
        .with_date(MEpoch::from_mjd(
            survey.first_time_mjd_seconds / 86_400.0,
            engine.time_reference(),
        ))
        .with_telescope_position(engine.observatory_position().clone())
        .with_pointing_center(pointing_longitude, centre.image_centre.as_angles().1))
}

/// A SIN direction coordinate of `image_size` square cells of `cell_arcsec`
/// centred on `(longitude, latitude)` in `frame`.
pub(super) fn direction_spec(
    image_size: usize,
    cell_arcsec: f64,
    frame: DirectionFrame,
    longitude: f64,
    latitude: f64,
) -> DirectionCoordinateSpec {
    let cell = cell_arcsec * std::f64::consts::PI / (180.0 * 3600.0);
    let reference_pixel = image_reference_pixel(image_size);
    DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(frame, longitude, latitude),
        [reference_pixel, reference_pixel],
        [-cell, cell],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    )
}

/// The spectral axis an image header records.
#[derive(Clone, Copy)]
pub(super) struct ImageSpectralCoordinate {
    pub(super) frequency_reference: FrequencyRef,
    pub(super) reference_frequency_hz: f64,
    pub(super) increment_hz: f64,
    pub(super) rest_frequency_hz: f64,
}

/// The coordinate system of one image domain's products.
pub(super) fn image_coordinates(
    direction: DirectionCoordinateSpec,
    image_size: usize,
    polarizations: &[PolarizationCoordinate],
    spectral: ImageSpectralCoordinate,
    observation: &ObsInfo,
) -> CoordinateSystem {
    let reference = direction.reference_direction();
    let reference_pixel = image_reference_pixel(image_size);
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        direction_ref(reference.frame()),
        CoordinateProjection::new(ProjectionType::SIN),
        [reference.longitude_rad(), reference.latitude_rad()],
        direction.increment_rad(),
        [reference_pixel, reference_pixel],
    ));
    coordinates.add_coordinate(StokesCoordinate::new(
        polarizations.iter().copied().map(stokes_type).collect(),
    ));
    coordinates.add_coordinate(SpectralCoordinate::new(
        spectral.frequency_reference,
        spectral.reference_frequency_hz,
        spectral.increment_hz,
        0.0,
        spectral.rest_frequency_hz,
    ));
    *coordinates.obs_info_mut() = observation.clone();
    coordinates
}

/// CASA's direction reference pixel: half the image extent.
fn image_reference_pixel(image_size: usize) -> f64 {
    image_size as f64 / 2.0
}

pub(super) const fn direction_ref(frame: DirectionFrame) -> DirectionRef {
    match frame {
        DirectionFrame::J2000 => DirectionRef::J2000,
        DirectionFrame::Icrs => DirectionRef::ICRS,
        DirectionFrame::B1950 => DirectionRef::B1950,
        DirectionFrame::Galactic => DirectionRef::GALACTIC,
    }
}

const fn stokes_type(coordinate: PolarizationCoordinate) -> StokesType {
    match coordinate {
        PolarizationCoordinate::StokesI => StokesType::I,
        PolarizationCoordinate::StokesQ => StokesType::Q,
        PolarizationCoordinate::StokesU => StokesType::U,
        PolarizationCoordinate::StokesV => StokesType::V,
        PolarizationCoordinate::CircularRr => StokesType::RR,
        PolarizationCoordinate::CircularRl => StokesType::RL,
        PolarizationCoordinate::CircularLr => StokesType::LR,
        PolarizationCoordinate::CircularLl => StokesType::LL,
        PolarizationCoordinate::LinearXx => StokesType::XX,
        PolarizationCoordinate::LinearXy => StokesType::XY,
        PolarizationCoordinate::LinearYx => StokesType::YX,
        PolarizationCoordinate::LinearYy => StokesType::YY,
    }
}

/// A CASA phase-centre literal `J2000 <lon> <lat>`, with sexagesimal,
/// degree or radian angles.
pub(super) fn parse_phase_center_direction(text: &str) -> Result<SkyDirection, ApplicationError> {
    let parts = text.split_whitespace().collect::<Vec<_>>();
    if parts.len() != 3 || !parts[0].eq_ignore_ascii_case("J2000") {
        return Err(boxed(
            "phasecenter must be 'J2000 lon lat', for example 'J2000 19:59:28.500 +40.44.01.50'",
        ));
    }
    Ok(SkyDirection::new(
        DirectionFrame::J2000,
        parse_phase_center_angle(parts[1], true)?,
        parse_phase_center_angle(parts[2], false)?,
    ))
}

fn parse_phase_center_angle(text: &str, longitude: bool) -> Result<f64, ApplicationError> {
    let lower = text.to_ascii_lowercase();
    if let Some(radians) = lower.strip_suffix("rad") {
        return Ok(radians.trim().parse::<f64>()?);
    }
    if let Some(degrees) = lower.strip_suffix("deg") {
        return Ok(degrees.trim().parse::<f64>()?.to_radians());
    }
    if longitude {
        if let Some(hours) = parse_sexagesimal(text, true) {
            return Ok(hours * std::f64::consts::PI / 12.0);
        }
    } else if let Some(degrees) = parse_sexagesimal(text, false) {
        return Ok(degrees.to_radians());
    }
    Err(boxed(format!("unsupported phasecenter angle {text:?}")))
}

fn parse_sexagesimal(text: &str, hours: bool) -> Option<f64> {
    let trimmed = text.trim();
    let sign = if trimmed.starts_with('-') { -1.0 } else { 1.0 };
    let body = trimmed.trim_start_matches(['+', '-']);
    let fields = if body.contains(':') {
        body.split(':').map(str::to_owned).collect::<Vec<_>>()
    } else if body.contains('h') || body.contains('d') || body.contains('m') || body.contains('s') {
        body.replace(['h', 'd', 'm', 's'], " ")
            .split_whitespace()
            .map(str::to_owned)
            .collect()
    } else if !hours && body.matches('.').count() >= 2 {
        let mut split = body.split('.');
        let major = split.next()?.to_owned();
        let minutes = split.next()?.to_owned();
        let seconds = split.collect::<Vec<_>>().join(".");
        vec![major, minutes, seconds]
    } else {
        return None;
    };
    let [major, minutes, seconds] = fields.as_slice() else {
        return None;
    };
    let major = major.parse::<f64>().ok()?;
    let minutes = minutes.parse::<f64>().ok()?;
    let seconds = seconds.parse::<f64>().ok()?;
    if !(major.is_finite()
        && minutes.is_finite()
        && seconds.is_finite()
        && (0.0..60.0).contains(&minutes)
        && (0.0..60.0).contains(&seconds))
    {
        return None;
    }
    Some(sign * (major.abs() + minutes / 60.0 + seconds / 3600.0))
}

#[cfg(test)]
mod tests {
    use casa_coordinates::{CoordinateModel, CoordinateType};

    use super::*;

    #[test]
    fn casa_direction_reference_pixel_uses_half_the_image_extent() {
        assert_eq!(image_reference_pixel(16), 8.0);
        assert_eq!(image_reference_pixel(15), 7.5);
    }

    #[test]
    fn casa_phase_center_literal_preserves_recentered_chart_coordinates() {
        let direction = parse_phase_center_direction("J2000 19:58:40.895 +40.55.58.543")
            .expect("CASA outlier phase center");
        let expected_ra = (19.0 + 58.0 / 60.0 + 40.895 / 3600.0) * std::f64::consts::PI / 12.0;
        let expected_dec = (40.0 + 55.0 / 60.0 + 58.543 / 3600.0) * std::f64::consts::PI / 180.0;
        assert!((direction.longitude_rad() - expected_ra).abs() < 1.0e-14);
        assert!((direction.latitude_rad() - expected_dec).abs() < 1.0e-14);
    }

    #[test]
    fn the_stokes_axis_lists_the_imaged_coordinate() {
        let coordinates = image_coordinates(
            direction_spec(64, 8.0, DirectionFrame::J2000, 0.0, 0.0),
            64,
            &[PolarizationCoordinate::StokesQ],
            ImageSpectralCoordinate {
                frequency_reference: FrequencyRef::LSRK,
                reference_frequency_hz: 1.0e9,
                increment_hz: 1.0,
                rest_frequency_hz: 1.0e9,
            },
            &ObsInfo::default(),
        );
        let index = coordinates
            .find_coordinate(CoordinateType::Stokes)
            .expect("polarization coordinate");
        let CoordinateModel::Stokes(stokes) = coordinates.coordinate(index) else {
            panic!("polarization coordinate has the wrong type");
        };
        assert_eq!(stokes.stokes(), [StokesType::Q]);
    }
}
