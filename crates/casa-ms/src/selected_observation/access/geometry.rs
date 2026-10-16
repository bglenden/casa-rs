// SPDX-License-Identifier: LGPL-3.0-or-later

//! A selected row's geometry: its phase-centre projection onto every image
//! domain and facet, its antennas' pointing directions from POINTING, and
//! the AW pointing epochs whose grouped antenna pixels CASA's `AWProjectFT`
//! uses.

use std::{
    collections::{BTreeMap, BTreeSet},
    mem::size_of,
    sync::Arc,
};

use crate::{
    MsError, MsReadPlan, MsSelectionIoBudget, PointingDirectionBracket, PointingDirectionQuery,
    SelectedObservationBuffer, SelectedStoredRow,
};
use casa_coordinates::{
    Coordinate, DirectionCoordinate, Projection as CoordinateProjection, ProjectionType,
};
use casa_imaging_model::{
    CompiledProblem, DirectionFrame, MissingPointingPolicy, PhaseCentreLaw, PointingCentreLaw,
    PointingExtrapolation, PointingInterpolation, PointingTimeSampling,
    Projection as ModelProjection, SelectedImageDomainProjection, SelectedImageDomainProjections,
    SelectedPhaseCentreProjection, SelectedPointingDirections, SkyDirection,
};
use casa_types::measures::direction::{DirectionRef, MDirection};
use ndarray::arr2;

use super::{
    AwPointingChartPixels, AwPointingEpochInput, AwPointingEpochKey, AwPointingEpochPlan,
    BoundObservationSource, BoundObservationSourceError,
};

#[derive(Clone)]
pub(in crate::selected_observation) struct EvaluatedRowGeometry {
    pub(super) domain_projections: SelectedImageDomainProjections,
    pub(super) pointing_directions: SelectedPointingDirections,
}

pub(super) fn evaluate_row_geometry(
    source: &BoundObservationSource,
    problem: &CompiledProblem,
    stored: SelectedStoredRow,
    observation_pointing: Option<SelectedPointingDirections>,
) -> Result<EvaluatedRowGeometry, BoundObservationSourceError> {
    let field_id = usize::try_from(stored.field_id())
        .map_err(|_| BoundObservationSourceError::InvalidRowGeometry)?;
    let (longitude_rad, latitude_rad) = source
        .geometry_engine
        .observation_direction_j2000(stored.time_mjd_seconds(), field_id)?
        .as_angles();
    let observation_direction =
        SkyDirection::new(DirectionFrame::J2000, longitude_rad, latitude_rad);
    let centres = problem.geometry().centres();
    let phase_direction = match centres.phase_tracking() {
        PhaseCentreLaw::Observation => observation_direction,
        PhaseCentreLaw::Fixed(direction) => require_fixed_j2000(*direction)?,
        PhaseCentreLaw::Ephemeris(target) => {
            let direction = source.geometry_engine.moving_direction_j2000(
                stored.time_mjd_seconds(),
                field_id,
                target,
            )?;
            let (longitude_rad, latitude_rad) = direction.as_angles();
            SkyDirection::new(DirectionFrame::J2000, longitude_rad, latitude_rad)
        }
    };
    let pointing_directions = match centres.pointing() {
        PointingCentreLaw::PhaseTrackingCentre => SelectedPointingDirections {
            antenna1: phase_direction,
            antenna2: phase_direction,
        },
        PointingCentreLaw::FieldCentre => SelectedPointingDirections {
            antenna1: observation_direction,
            antenna2: observation_direction,
        },
        PointingCentreLaw::Fixed(direction) => {
            let direction = require_fixed_j2000(*direction)?;
            SelectedPointingDirections {
                antenna1: direction,
                antenna2: direction,
            }
        }
        PointingCentreLaw::Observation(_) => observation_pointing
            .ok_or(BoundObservationSourceError::MissingEvaluatedPointingDirections)?,
    };
    let domains = problem.geometry().domains();
    let moving_direction_shift = if matches!(centres.phase_tracking(), PhaseCentreLaw::Ephemeris(_))
    {
        let anchor = domains
            .first()
            .ok_or(BoundObservationSourceError::InvalidRowGeometry)?
            .model_phase_centre();
        let anchor_j2000 = source.geometry_engine.direction_angles_j2000(
            stored.time_mjd_seconds(),
            [anchor.longitude_rad(), anchor.latitude_rad()],
            direction_ref(anchor.frame()),
        )?;
        Some([
            phase_direction.longitude_rad() - anchor_j2000[0],
            phase_direction.latitude_rad() - anchor_j2000[1],
        ])
    } else {
        None
    };
    let chart_count = domains.iter().try_fold(0_usize, |count, domain| {
        count.checked_add(domain.facets().len())
    });
    let mut domain_projections = Vec::new();
    domain_projections
        .try_reserve_exact(chart_count.ok_or(BoundObservationSourceError::InvalidRowGeometry)?)
        .map_err(|_| BoundObservationSourceError::InvalidRowGeometry)?;
    for (domain_ordinal, domain) in domains.iter().enumerate() {
        let domain_ordinal = u32::try_from(domain_ordinal)
            .map_err(|_| BoundObservationSourceError::InvalidRowGeometry)?;
        let shared_psf = domain.psf_phase_centre() == domain.model_phase_centre();
        let distinct_psf = if shared_psf {
            None
        } else {
            Some(evaluate_phase_centre_projection(
                source,
                stored,
                observation_direction,
                PhaseCentreProjectionRequest {
                    field_id,
                    target_direction: domain.psf_phase_centre(),
                    project_to_observation_plane: domain.facets().len() > 1,
                    moving_direction_shift,
                    uvw_law: problem.geometry().uvw(),
                },
            )?)
        };
        for (facet_ordinal, facet) in domain.facets().iter().enumerate() {
            let facet_ordinal = u32::try_from(facet_ordinal)
                .map_err(|_| BoundObservationSourceError::InvalidRowGeometry)?;
            let model_phase_centre = if domain.facets().len() == 1 {
                domain.model_phase_centre()
            } else {
                facet.phase_centre()
            };
            let model = evaluate_phase_centre_projection(
                source,
                stored,
                observation_direction,
                PhaseCentreProjectionRequest {
                    field_id,
                    target_direction: model_phase_centre,
                    project_to_observation_plane: domain.facets().len() > 1,
                    moving_direction_shift,
                    uvw_law: problem.geometry().uvw(),
                },
            )?;
            let projection = match distinct_psf {
                Some(psf) => SelectedImageDomainProjection::new_facet(
                    domain_ordinal,
                    facet_ordinal,
                    model,
                    psf,
                ),
                None => SelectedImageDomainProjection::facet_with_shared_psf(
                    domain_ordinal,
                    facet_ordinal,
                    model,
                ),
            };
            let projection = if let Some(plan) = &source.aw_pointing_plan {
                let epoch = AwPointingEpochKey {
                    data_description_id: stored.data_description_id(),
                    field_id: stored.field_id(),
                    time_bits: stored.time_mjd_seconds().to_bits(),
                };
                let chart = domain_projections.len();
                let pixels = plan
                    .chart_pixels_by_epoch
                    .get(&epoch)
                    .and_then(|charts| charts.get(chart))
                    .ok_or(BoundObservationSourceError::InvalidRowGeometry)?;
                let antenna1 = pixels
                    .get(&stored.antenna1())
                    .ok_or(BoundObservationSourceError::InvalidRowGeometry)?;
                let antenna2 = pixels
                    .get(&stored.antenna2())
                    .ok_or(BoundObservationSourceError::InvalidRowGeometry)?;
                projection
                    .with_aw_pointing_pixel([
                        0.5 * (antenna1[0] + antenna2[0]),
                        0.5 * (antenna1[1] + antenna2[1]),
                    ])
                    .ok_or(BoundObservationSourceError::InvalidRowGeometry)?
            } else {
                projection
            };
            domain_projections.push(projection);
        }
    }
    let domain_projections = SelectedImageDomainProjections::new(domain_projections)
        .ok_or(BoundObservationSourceError::InvalidRowGeometry)?;
    Ok(EvaluatedRowGeometry {
        domain_projections,
        pointing_directions,
    })
}

struct PhaseCentreProjectionRequest {
    field_id: usize,
    target_direction: SkyDirection,
    project_to_observation_plane: bool,
    moving_direction_shift: Option<[f64; 2]>,
    uvw_law: casa_imaging_model::UvwCoordinateLaw,
}

fn evaluate_phase_centre_projection(
    source: &BoundObservationSource,
    stored: SelectedStoredRow,
    observation_direction: SkyDirection,
    request: PhaseCentreProjectionRequest,
) -> Result<SelectedPhaseCentreProjection, BoundObservationSourceError> {
    let mut target_angles = source.geometry_engine.direction_angles_j2000(
        stored.time_mjd_seconds(),
        [
            request.target_direction.longitude_rad(),
            request.target_direction.latitude_rad(),
        ],
        direction_ref(request.target_direction.frame()),
    )?;
    if let Some([longitude, latitude]) = request.moving_direction_shift {
        target_angles[0] += longitude;
        target_angles[1] += latitude;
    }
    let target_j2000 = SkyDirection::new(DirectionFrame::J2000, target_angles[0], target_angles[1]);
    let (transformed_uvw_m, phase_shift_m) = if matches!(
        request.uvw_law,
        casa_imaging_model::UvwCoordinateLaw::MosaicPhaseTrackingCentre
    ) {
        if request.project_to_observation_plane {
            return Err(BoundObservationSourceError::InvalidRowGeometry);
        }
        let target = MDirection::from_angles(
            target_j2000.longitude_rad(),
            target_j2000.latitude_rad(),
            DirectionRef::J2000,
        );
        let (uvw_m, casa_dphase_m) = source
            .geometry_engine
            .reproject_raw_uvw_for_mosaic_to_direction(stored.uvw_m(), request.field_id, &target)?;
        (uvw_m, -casa_dphase_m)
    } else if target_j2000 == observation_direction && !request.project_to_observation_plane {
        (stored.uvw_m(), 0.0)
    } else if request.project_to_observation_plane {
        source
            .geometry_engine
            .reproject_raw_uvw_for_faceted_gridft_between_j2000_directions(
                stored.uvw_m(),
                [
                    observation_direction.longitude_rad(),
                    observation_direction.latitude_rad(),
                ],
                target_angles,
            )?
    } else {
        source
            .geometry_engine
            .reproject_raw_uvw_for_gridft_between_j2000_directions(
                stored.uvw_m(),
                [
                    observation_direction.longitude_rad(),
                    observation_direction.latitude_rad(),
                ],
                target_angles,
            )?
    };
    SelectedPhaseCentreProjection::new(transformed_uvw_m, phase_shift_m)
        .ok_or(BoundObservationSourceError::InvalidRowGeometry)
}

const fn direction_ref(frame: DirectionFrame) -> DirectionRef {
    match frame {
        DirectionFrame::Icrs => DirectionRef::ICRS,
        DirectionFrame::J2000 => DirectionRef::J2000,
        DirectionFrame::B1950 => DirectionRef::B1950,
        DirectionFrame::Galactic => DirectionRef::GALACTIC,
    }
}

pub(super) fn evaluate_observation_pointings(
    source: &BoundObservationSource,
    problem: &CompiledProblem,
    buffer: &SelectedObservationBuffer,
) -> Result<Option<Vec<SelectedPointingDirections>>, BoundObservationSourceError> {
    let PointingCentreLaw::Observation(law) = problem.geometry().centres().pointing() else {
        return Ok(None);
    };
    let mut queries = Vec::with_capacity(buffer.row_count().saturating_mul(2));
    let mut phase_directions = Vec::with_capacity(buffer.row_count());
    for row in 0..buffer.row_count() {
        let stored = buffer
            .row(row)
            .ok_or(BoundObservationSourceError::StoredSampleShapeMismatch)?;
        let time_mjd_seconds = match law.time_sampling() {
            PointingTimeSampling::VisibilityTime => stored.time_mjd_seconds(),
            PointingTimeSampling::VisibilityTimeCentroid => stored.time_centroid_mjd_seconds(),
        };
        queries.push(PointingDirectionQuery::new(
            stored.antenna1(),
            time_mjd_seconds,
        )?);
        queries.push(PointingDirectionQuery::new(
            stored.antenna2(),
            time_mjd_seconds,
        )?);
        phase_directions.push(evaluate_phase_direction(source, problem, stored)?);
    }
    let brackets = source
        .pointing_catalog
        .as_ref()
        .ok_or(BoundObservationSourceError::MissingPointingQueryDomain)?
        .direction_brackets(&source.geometry_engine, &queries)?;
    let mut pointings = Vec::with_capacity(buffer.row_count());
    for ((row, antenna_brackets), fallback) in brackets
        .as_chunks::<2>()
        .0
        .iter()
        .enumerate()
        .zip(phase_directions)
    {
        pointings.push(SelectedPointingDirections {
            antenna1: resolve_pointing_direction(
                antenna_brackets[0],
                queries[2 * row],
                *law,
                fallback,
            )?,
            antenna2: resolve_pointing_direction(
                antenna_brackets[1],
                queries[2 * row + 1],
                *law,
                fallback,
            )?,
        });
    }
    Ok(Some(pointings))
}

pub(super) fn build_aw_pointing_epoch_plan(
    problem: &CompiledProblem,
    source: &BoundObservationSource,
) -> Result<AwPointingEpochPlan, BoundObservationSourceError> {
    let contract = problem
        .science()
        .measurement_equation()
        .aw_projection()
        .filter(|contract| contract.use_pointing())
        .ok_or(BoundObservationSourceError::UnsupportedCentreLaw)?;
    let law = match problem.geometry().centres().pointing() {
        PointingCentreLaw::Observation(law) => *law,
        _ => return Err(BoundObservationSourceError::UnsupportedCentreLaw),
    };
    let catalog = source
        .pointing_catalog
        .as_ref()
        .ok_or(BoundObservationSourceError::MissingPointingQueryDomain)?;
    let available_bytes = source
        .content_plan
        .rows_per_block()
        .checked_mul(crate::SelectedObservationRow::STORAGE_BYTES_PER_ROW)
        .ok_or(BoundObservationSourceError::MeasurementOverflow)?;
    let scan_plan = MsReadPlan::new(
        source.measurement_set.row_count(),
        MsSelectionIoBudget {
            available_bytes,
            maximum_live_blocks: 1,
            requested_bytes_per_row: crate::SelectedObservationRow::STORAGE_BYTES_PER_ROW,
            storage_alignment_rows: Some(source.content_plan.rows_per_block()),
        },
    )
    .map_err(|error| MsError::InvalidInput(error.to_string()))?;
    let mut cursor = source
        .measurement_set
        .main_row_selection_cursor(scan_plan)?;
    let mut epochs = Vec::<AwPointingEpochInput>::new();
    while let Some(row) = cursor.next(&source.measurement_set)? {
        if !source.row_predicate.matches(row) {
            continue;
        }
        let key = AwPointingEpochKey {
            data_description_id: row.data_description_id(),
            field_id: row.field_id(),
            time_bits: row.time_mjd_seconds().to_bits(),
        };
        if epochs.last().is_none_or(|epoch| epoch.key != key) {
            epochs.push(AwPointingEpochInput {
                key,
                row_count: 0,
                antennas: BTreeSet::new(),
            });
        }
        let epoch = epochs.last_mut().expect("AW pointing epoch was appended");
        epoch.row_count = epoch
            .row_count
            .checked_add(1)
            .ok_or(BoundObservationSourceError::MeasurementOverflow)?;
        epoch.antennas.insert(row.antenna1());
        epoch.antennas.insert(row.antenna2());
    }

    let charts = problem
        .geometry()
        .domains()
        .iter()
        .flat_map(|domain| domain.facets().iter().map(|facet| facet.direction()))
        .map(aw_direction_coordinate)
        .collect::<Result<Vec<_>, _>>()?;
    let [grouping_arcsec, refresh_arcsec] = contract.pointing_offset_sigdev_arcsec();
    let grouping_arcsec = if grouping_arcsec > 0.0 {
        grouping_arcsec
    } else {
        1.0e-3
    };
    let mut chart_pixels_by_epoch = BTreeMap::new();
    let mut cached_field = None;
    let mut cached_row_count = 0_usize;
    let mut cached_antenna_pixels = BTreeMap::<i32, [f64; 2]>::new();
    let mut cached_grouped = AwPointingChartPixels::from([]);
    let primary_increment = problem
        .geometry()
        .domains()
        .first()
        .ok_or(BoundObservationSourceError::InvalidRowGeometry)?
        .direction()
        .increment_rad();
    let primary_increment_norm = primary_increment[0].hypot(primary_increment[1]);
    let refresh_threshold_rad = refresh_arcsec.to_radians() / 3600.0;

    for epoch in epochs {
        let time = f64::from_bits(epoch.key.time_bits);
        let fallback = pointing_epoch_phase_direction(source, problem, epoch.key.field_id, time)?;
        let queries = epoch
            .antennas
            .iter()
            .copied()
            .map(|antenna| PointingDirectionQuery::new(antenna, time))
            .collect::<Result<Vec<_>, _>>()?;
        let brackets = catalog.direction_brackets(&source.geometry_engine, &queries)?;
        let directions = epoch
            .antennas
            .iter()
            .copied()
            .zip(brackets)
            .zip(queries.iter().copied())
            .map(|((antenna, bracket), query)| {
                resolve_pointing_direction(bracket, query, law, fallback)
                    .map(|direction| (antenna, direction))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let pixels_by_chart = charts
            .iter()
            .map(|coordinate| {
                directions
                    .iter()
                    .map(|(&antenna, direction)| {
                        coordinate
                            .to_pixel(&[direction.longitude_rad(), direction.latitude_rad()])
                            .map_err(|_| BoundObservationSourceError::InvalidRowGeometry)
                            .and_then(|pixel| {
                                pixel
                                    .get(0..2)
                                    .filter(|pixel| pixel.iter().all(|value| value.is_finite()))
                                    .map(|pixel| (antenna, [pixel[0], pixel[1]]))
                                    .ok_or(BoundObservationSourceError::InvalidRowGeometry)
                            })
                    })
                    .collect::<Result<BTreeMap<_, _>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let current_primary = pixels_by_chart
            .first()
            .ok_or(BoundObservationSourceError::InvalidRowGeometry)?;
        let same_antennas = cached_antenna_pixels.len() == current_primary.len()
            && cached_antenna_pixels.keys().eq(current_primary.keys());
        let refresh = if cached_field != Some(epoch.key.field_id)
            || cached_row_count != epoch.row_count
            || !same_antennas
        {
            true
        } else {
            let residual = cached_antenna_pixels.iter().zip(current_primary).fold(
                [0.0, 0.0],
                |[sum_x, sum_y], ((_, cached), (_, current))| {
                    [
                        sum_x + cached[0] - current[0],
                        sum_y + cached[1] - current[1],
                    ]
                },
            );
            residual[0].hypot(residual[1]) / current_primary.len().max(1) as f64
                * primary_increment_norm
                >= refresh_threshold_rad
        };
        if refresh {
            let grouped =
                pixels_by_chart
                    .iter()
                    .zip(
                        problem.geometry().domains().iter().flat_map(|domain| {
                            domain.facets().iter().map(|facet| facet.direction())
                        }),
                    )
                    .map(|(pixels, direction)| {
                        casa_aw_grouped_pixels(pixels, grouping_arcsec, direction.increment_rad())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
            cached_grouped = Arc::from(grouped);
        }
        cached_field = Some(epoch.key.field_id);
        cached_row_count = epoch.row_count;
        cached_antenna_pixels = current_primary.clone();
        if chart_pixels_by_epoch
            .insert(epoch.key, Arc::clone(&cached_grouped))
            .is_some()
        {
            return Err(BoundObservationSourceError::InvalidRowGeometry);
        }
    }
    let retained_byte_ceiling = size_of::<AwPointingEpochPlan>()
        .checked_add(
            chart_pixels_by_epoch
                .values()
                .try_fold(0_usize, |bytes, charts| {
                    charts.iter().try_fold(bytes, |bytes, chart| {
                        chart
                            .len()
                            .checked_mul(usize::BITS as usize)
                            .and_then(|entries| bytes.checked_add(entries))
                    })
                })
                .ok_or(BoundObservationSourceError::MeasurementOverflow)?,
        )
        .ok_or(BoundObservationSourceError::MeasurementOverflow)?;
    Ok(AwPointingEpochPlan {
        chart_pixels_by_epoch,
        retained_byte_ceiling,
    })
}

fn aw_direction_coordinate(
    spec: casa_imaging_model::DirectionCoordinateSpec,
) -> Result<DirectionCoordinate, BoundObservationSourceError> {
    if spec.projection() != ModelProjection::Sin {
        return Err(BoundObservationSourceError::InvalidRowGeometry);
    }
    let reference = spec.reference_direction();
    let pc = spec.pc();
    Ok(DirectionCoordinate::new(
        direction_ref(reference.frame()),
        CoordinateProjection::new(ProjectionType::SIN),
        [reference.longitude_rad(), reference.latitude_rad()],
        spec.increment_rad(),
        spec.reference_pixel(),
    )
    .with_pc_matrix(arr2(&pc))
    .with_longpole(spec.pole_deg()[0].to_radians())
    .with_latpole(spec.pole_deg()[1].to_radians()))
}

fn casa_aw_grouped_pixels(
    pixels: &BTreeMap<i32, [f64; 2]>,
    grouping_arcsec: f64,
    increment_rad: [f64; 2],
) -> Result<BTreeMap<i32, [f64; 2]>, BoundObservationSourceError> {
    let bin_width =
        grouping_arcsec.to_radians() / 3600.0 / increment_rad[0].hypot(increment_rad[1]);
    if !(bin_width.is_finite() && bin_width > 0.0) || pixels.is_empty() {
        return Err(BoundObservationSourceError::InvalidRowGeometry);
    }
    let min_x = pixels
        .values()
        .map(|pixel| pixel[0])
        .fold(f64::INFINITY, f64::min);
    let min_y = pixels
        .values()
        .map(|pixel| pixel[1])
        .fold(f64::INFINITY, f64::min);
    let mut bins = BTreeMap::<i32, (i64, i64)>::new();
    let mut sums = BTreeMap::<(i64, i64), (f32, f32, usize)>::new();
    for (&antenna, &pixel) in pixels {
        let bin = (
            ((pixel[0] - min_x) / bin_width).floor() as i64,
            ((pixel[1] - min_y) / bin_width).floor() as i64,
        );
        bins.insert(antenna, bin);
        let sum = sums.entry(bin).or_insert((0.0, 0.0, 0));
        sum.0 = (f64::from(sum.0) + pixel[0]) as f32;
        sum.1 = (f64::from(sum.1) + pixel[1]) as f32;
        sum.2 = sum
            .2
            .checked_add(1)
            .ok_or(BoundObservationSourceError::MeasurementOverflow)?;
    }
    let means = sums
        .into_iter()
        .map(|(bin, (x, y, count))| (bin, [(x / count as f32) as f64, (y / count as f32) as f64]))
        .collect::<BTreeMap<_, _>>();
    Ok(bins
        .into_iter()
        .map(|(antenna, bin)| (antenna, means[&bin]))
        .collect())
}

fn pointing_epoch_phase_direction(
    source: &BoundObservationSource,
    problem: &CompiledProblem,
    field_id: i32,
    time_mjd_seconds: f64,
) -> Result<SkyDirection, BoundObservationSourceError> {
    let field =
        usize::try_from(field_id).map_err(|_| BoundObservationSourceError::InvalidRowGeometry)?;
    let observation = source
        .geometry_engine
        .observation_direction_j2000(time_mjd_seconds, field)?;
    let (longitude, latitude) = observation.as_angles();
    let observation = SkyDirection::new(DirectionFrame::J2000, longitude, latitude);
    match problem.geometry().centres().phase_tracking() {
        PhaseCentreLaw::Observation => Ok(observation),
        PhaseCentreLaw::Fixed(direction) => require_fixed_j2000(*direction),
        PhaseCentreLaw::Ephemeris(target) => {
            let direction =
                source
                    .geometry_engine
                    .moving_direction_j2000(time_mjd_seconds, field, target)?;
            let (longitude, latitude) = direction.as_angles();
            Ok(SkyDirection::new(
                DirectionFrame::J2000,
                longitude,
                latitude,
            ))
        }
    }
}

fn evaluate_phase_direction(
    source: &BoundObservationSource,
    problem: &CompiledProblem,
    stored: SelectedStoredRow,
) -> Result<SkyDirection, BoundObservationSourceError> {
    let field_id = usize::try_from(stored.field_id())
        .map_err(|_| BoundObservationSourceError::InvalidRowGeometry)?;
    let (longitude_rad, latitude_rad) = source
        .geometry_engine
        .observation_direction_j2000(stored.time_mjd_seconds(), field_id)?
        .as_angles();
    let observation = SkyDirection::new(DirectionFrame::J2000, longitude_rad, latitude_rad);
    match problem.geometry().centres().phase_tracking() {
        PhaseCentreLaw::Observation => Ok(observation),
        PhaseCentreLaw::Fixed(direction) => require_fixed_j2000(*direction),
        PhaseCentreLaw::Ephemeris(target) => {
            let direction = source.geometry_engine.moving_direction_j2000(
                stored.time_mjd_seconds(),
                field_id,
                target,
            )?;
            let (longitude_rad, latitude_rad) = direction.as_angles();
            Ok(SkyDirection::new(
                DirectionFrame::J2000,
                longitude_rad,
                latitude_rad,
            ))
        }
    }
}

fn resolve_pointing_direction(
    bracket: PointingDirectionBracket,
    query: PointingDirectionQuery,
    law: casa_imaging_model::ObservationPointingLaw,
    fallback: SkyDirection,
) -> Result<SkyDirection, BoundObservationSourceError> {
    if let Some(covering) = bracket.covering() {
        return Ok(j2000_direction(covering.direction_j2000_rad()));
    }
    let direction = match (bracket.before(), bracket.after()) {
        (Some(before), Some(after)) if before.row_index() == after.row_index() => {
            before.direction_j2000_rad()
        }
        (Some(before), Some(after)) => match law.interpolation() {
            PointingInterpolation::Nearest => {
                let before_distance = query.time_mjd_seconds() - before.row_time_mjd_seconds();
                let after_distance = after.row_time_mjd_seconds() - query.time_mjd_seconds();
                if before_distance <= after_distance {
                    before.direction_j2000_rad()
                } else {
                    after.direction_j2000_rad()
                }
            }
            PointingInterpolation::GreatCircleShortestArc => interpolate_direction(
                before.direction_j2000_rad(),
                after.direction_j2000_rad(),
                (query.time_mjd_seconds() - before.row_time_mjd_seconds())
                    / (after.row_time_mjd_seconds() - before.row_time_mjd_seconds()),
            )?,
        },
        (Some(endpoint), None) | (None, Some(endpoint))
            if matches!(law.extrapolation(), PointingExtrapolation::HoldNearest) =>
        {
            endpoint.direction_j2000_rad()
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(BoundObservationSourceError::PointingOutsideCoverage {
                antenna_id: query.antenna_id(),
                time_mjd_seconds: query.time_mjd_seconds(),
            });
        }
        (None, None) if matches!(law.missing(), MissingPointingPolicy::UsePhaseTrackingCentre) => {
            return Ok(fallback);
        }
        (None, None) => {
            return Err(BoundObservationSourceError::MissingPointingDirection {
                antenna_id: query.antenna_id(),
                time_mjd_seconds: query.time_mjd_seconds(),
            });
        }
    };
    Ok(j2000_direction(direction))
}

fn j2000_direction(direction_rad: [f64; 2]) -> SkyDirection {
    SkyDirection::new(DirectionFrame::J2000, direction_rad[0], direction_rad[1])
}

fn interpolate_direction(
    before: [f64; 2],
    after: [f64; 2],
    fraction: f64,
) -> Result<[f64; 2], BoundObservationSourceError> {
    if !(fraction.is_finite() && (0.0..=1.0).contains(&fraction)) {
        return Err(BoundObservationSourceError::InvalidPointingInterpolation);
    }
    let left = unit_direction(before);
    let right = unit_direction(after);
    let cosine = left
        .iter()
        .zip(right)
        .map(|(left, right)| left * right)
        .sum::<f64>()
        .clamp(-1.0, 1.0);
    let angle = cosine.acos();
    let vector = if angle.abs() < 1.0e-15 {
        left
    } else {
        let denominator = angle.sin();
        let before_weight = ((1.0 - fraction) * angle).sin() / denominator;
        let after_weight = (fraction * angle).sin() / denominator;
        [
            before_weight * left[0] + after_weight * right[0],
            before_weight * left[1] + after_weight * right[1],
            before_weight * left[2] + after_weight * right[2],
        ]
    };
    let norm = vector.iter().map(|value| value * value).sum::<f64>().sqrt();
    if !(norm.is_finite() && norm > 0.0) {
        return Err(BoundObservationSourceError::InvalidPointingInterpolation);
    }
    let [x, y, z] = vector.map(|value| value / norm);
    Ok([y.atan2(x).rem_euclid(std::f64::consts::TAU), z.asin()])
}

fn unit_direction(direction: [f64; 2]) -> [f64; 3] {
    [
        direction[1].cos() * direction[0].cos(),
        direction[1].cos() * direction[0].sin(),
        direction[1].sin(),
    ]
}

fn require_fixed_j2000(
    direction: SkyDirection,
) -> Result<SkyDirection, BoundObservationSourceError> {
    if direction.frame() != DirectionFrame::J2000 {
        return Err(BoundObservationSourceError::UnsupportedFixedCentreFrame {
            frame: direction.frame(),
        });
    }
    Ok(direction)
}

#[cfg(test)]
mod aw_pointing_tests {
    use super::*;
    use crate::derived::engine::MsCalEngine;
    use crate::{
        MeasurementSet, PointingDirectionColumn as StoredPointingDirectionColumn, PointingReadPlan,
    };
    use casa_imaging_model::PointingDirectionColumn;

    #[test]
    #[ignore = "requires local MeasurementSet and native source-group pointing trace"]
    fn t51_native_source_group_pointing_matches_selected_catalog() {
        use casa_imaging_model::{ObservationPointingLaw, PointingDirectionSemantic};
        use casa_types::measures::MeasuresProvider;
        let ms =
            MeasurementSet::open(std::env::var_os("CASA_RS_T51_POINTING_MS").unwrap()).unwrap();
        let native =
            std::fs::read_to_string(std::env::var_os("CASA_RS_T51_NATIVE_POINTING").unwrap())
                .unwrap();
        let centre = std::env::var("CASA_RS_T51_POINTING_CENTRE")
            .unwrap()
            .split(',')
            .map(|v| v.parse::<f64>().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(centre.len(), 2);
        let records = native
            .lines()
            .map(|line| line.split('\t').collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let group = records.iter().find(|r| r[0] == "source_group").unwrap();
        let time = group[3].parse::<f64>().unwrap();
        let expected = records
            .iter()
            .filter(|r| r[0] == "antenna_pixel")
            .map(|r| {
                (
                    r[1].parse::<i32>().unwrap(),
                    [r[2].parse::<f64>().unwrap(), r[3].parse::<f64>().unwrap()],
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert!(!expected.is_empty());
        let mut domain = crate::selected_pointing::SelectedPointingQueryDomainBuilder::default();
        for &antenna in expected.keys() {
            domain.observe_row(antenna, antenna, time, time).unwrap();
        }
        let domain = domain.finish().unwrap().unwrap();
        let catalog = ms
            .prepare_selected_pointing_catalog(
                StoredPointingDirectionColumn::Direction,
                &domain,
                PointingTimeSampling::VisibilityTime,
                PointingReadPlan::new(1024, 16, 16 * 1024 * 1024).unwrap(),
            )
            .unwrap();
        let measures_root = records
            .iter()
            .find(|r| r[0] == "native_measures_root")
            .unwrap()[1];
        let runtime = crate::test_helpers::production_measures_runtime_at(measures_root).unwrap();
        eprintln!("rust_measures_root\t{}", runtime.root().display());
        let measures: std::sync::Arc<dyn MeasuresProvider> = runtime;
        let state = measures.prepare_bounded_state().unwrap().unwrap();
        eprintln!("rust_measures_identity\t{:?}", state.identity_sha256());
        eprintln!(
            "rust_eop\t{:?}\ttai_minus_utc={}",
            measures.eop_values(time / 86_400.0).unwrap(),
            measures.tai_minus_utc_seconds(time / 86_400.0).unwrap()
        );
        let engine = MsCalEngine::new_selected_observation(&ms, measures, None).unwrap();
        let queries = expected
            .keys()
            .map(|&antenna| PointingDirectionQuery::new(antenna, time).unwrap())
            .collect::<Vec<_>>();
        let brackets = catalog.direction_brackets(&engine, &queries).unwrap();
        let law = ObservationPointingLaw::new(
            PointingDirectionColumn::Direction,
            PointingDirectionSemantic::AntennaBoresight,
            PointingTimeSampling::VisibilityTime,
            PointingInterpolation::Nearest,
            PointingExtrapolation::HoldNearest,
            MissingPointingPolicy::UsePhaseTrackingCentre,
        );
        let field_direction = engine
            .field_direction_j2000(group[4].parse().unwrap())
            .unwrap();
        let fallback = SkyDirection::new(
            DirectionFrame::J2000,
            field_direction.longitude_rad(),
            field_direction.latitude_rad(),
        );
        let cell = (0.6_f64 / 3600.0).to_radians();
        let coordinate = DirectionCoordinate::new(
            DirectionRef::J2000,
            CoordinateProjection::new(ProjectionType::SIN),
            [centre[0], centre[1]],
            [-cell, cell],
            [256.0, 256.0],
        )
        .with_longpole(std::f64::consts::PI)
        .with_latpole(centre[1]);
        let mut pixels = BTreeMap::new();
        let mut maximum_error = 0.0_f64;
        for ((&antenna, query), bracket) in expected.keys().zip(queries).zip(brackets) {
            let direction = resolve_pointing_direction(bracket, query, law, fallback).unwrap();
            let pixel = coordinate
                .to_pixel(&[direction.longitude_rad(), direction.latitude_rad()])
                .unwrap();
            for axis in 0..2 {
                maximum_error = maximum_error.max((pixel[axis] - expected[&antenna][axis]).abs());
            }
            pixels.insert(antenna, [pixel[0], pixel[1]]);
        }
        eprintln!(
            "pointing_pixel_comparison antennas={} maximum_error_pixels={maximum_error:.17e}",
            pixels.len()
        );
        assert!(maximum_error < 1.0e-5, "native POINTING projection differs");
        let grouped = casa_aw_grouped_pixels(&pixels, 600.0, [-cell, cell]).unwrap();
        let baseline = records
            .iter()
            .find(|r| r[0] == "selected_baseline")
            .unwrap();
        let midpoint = records.iter().find(|r| r[0] == "grouped_midpoint").unwrap();
        let native_midpoint = [
            midpoint[1].parse::<f64>().unwrap(),
            midpoint[2].parse::<f64>().unwrap(),
        ];
        let left = grouped[&baseline[1].parse::<i32>().unwrap()];
        let right = grouped[&baseline[2].parse::<i32>().unwrap()];
        assert_eq!(
            [(left[0] + right[0]) / 2.0, (left[1] + right[1]) / 2.0],
            native_midpoint,
            "native grouped POINTING midpoint differs"
        );
    }

    #[test]
    fn casa_default_pointing_group_collapses_compact_array_to_f32_mean() {
        let pixels = BTreeMap::from([
            (0, [256.125_000_01, 255.875_000_01]),
            (1, [257.250_000_02, 256.500_000_02]),
            (2, [255.750_000_03, 257.125_000_03]),
        ]);

        let grouped = casa_aw_grouped_pixels(
            &pixels,
            600.0,
            [0.6_f64.to_radians() / 3600.0, 0.6_f64.to_radians() / 3600.0],
        )
        .expect("CASA default pointing grouping");
        let casa_mean = |values: [f64; 3]| {
            let sum = values
                .into_iter()
                .fold(0.0_f32, |sum, value| (f64::from(sum) + value) as f32);
            f64::from(sum / 3.0)
        };
        let expected_x = casa_mean([256.125_000_01, 257.250_000_02, 255.750_000_03]);
        let expected_y = casa_mean([255.875_000_01, 256.500_000_02, 257.125_000_03]);

        assert_eq!(grouped.len(), pixels.len());
        assert!(
            grouped
                .values()
                .all(|pixel| *pixel == [expected_x, expected_y])
        );
    }

    #[test]
    fn casa_pointing_group_keeps_separated_bins_independent() {
        let pixels = BTreeMap::from([(0, [10.0, 20.0]), (1, [10.2, 20.2]), (2, [30.0, 40.0])]);

        let grouped = casa_aw_grouped_pixels(&pixels, 1.0, [1.0_f64.to_radians() / 3600.0, 0.0])
            .expect("separated CASA pointing groups");

        assert_eq!(
            grouped[&0],
            [10.100_000_381_469_727, 20.100_000_381_469_727]
        );
        assert_eq!(grouped[&1], grouped[&0]);
        assert_eq!(grouped[&2], [30.0, 40.0]);
    }
}
