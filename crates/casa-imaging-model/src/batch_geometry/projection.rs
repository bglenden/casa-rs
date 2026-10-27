// SPDX-License-Identifier: LGPL-3.0-or-later
//! Each chart's reprojection of a time's rows: from the field's
//! observation direction to the chart's phase centre.

use casa_types::measures::direction::{DirectionRef, MDirection};

use super::BatchGeometryError;
use crate::compiled_problem::CompiledProblem;
use crate::geometry::{DirectionFrame, SkyDirection, UvwCoordinateLaw};
use crate::uvw_projection::GridFtProjection;

/// One chart (a domain, or one facet of a faceted domain) and the phase
/// centre its rows are projected to.
pub(super) struct Chart {
    pub(super) centre: SkyDirection,
    /// Whether the chart is a facet: CASA builds every facet's machine to
    /// reproject onto the observation's tangent plane.
    pub(super) faceted: bool,
}

/// The charts of `problem`, domain-major with each faceted domain's facets
/// in turn, and whether its rows are reprojected as `MosaicFT` does.
pub(super) fn charts(problem: &CompiledProblem) -> Result<(Vec<Chart>, bool), BatchGeometryError> {
    let mosaic = matches!(
        problem.geometry().uvw(),
        UvwCoordinateLaw::MosaicPhaseTrackingCentre
    );
    let mut charts = Vec::new();
    for domain in problem.geometry().domains() {
        let facets = domain.facets();
        if facets.len() == 1 {
            charts.push(Chart {
                centre: domain.model_phase_centre(),
                faceted: false,
            });
        } else {
            if mosaic {
                return Err(BatchGeometryError::FacetedMosaic);
            }
            charts.extend(facets.iter().map(|facet| Chart {
                centre: facet.phase_centre(),
                faceted: true,
            }));
        }
    }
    Ok((charts, mosaic))
}

/// The reprojection of rows of a field observed in direction `observation`
/// (J2000, as the field's J2000 phase centre `field`) to a chart centred
/// on J2000 `target_rad`; `None` keeps the rows as stored, as for a
/// non-faceted chart centred on the observation direction itself.
pub(super) fn reprojection(
    observation: SkyDirection,
    field: &MDirection,
    target_rad: [f64; 2],
    faceted: bool,
    mosaic: bool,
) -> Option<GridFtProjection> {
    let target = MDirection::from_angles(target_rad[0], target_rad[1], DirectionRef::J2000);
    if mosaic {
        return Some(GridFtProjection::mosaic_between_directions(field, &target));
    }
    if SkyDirection::new(DirectionFrame::J2000, target_rad[0], target_rad[1]) == observation
        && !faceted
    {
        return None;
    }
    let source = MDirection::from_angles(
        observation.longitude_rad(),
        observation.latitude_rad(),
        DirectionRef::J2000,
    );
    Some(if faceted {
        GridFtProjection::faceted_between_directions(&source, &target)
    } else {
        GridFtProjection::between_directions(&source, &target)
    })
}
