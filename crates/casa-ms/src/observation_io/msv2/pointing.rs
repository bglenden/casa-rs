// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MSv2 adapter's pointing: the `POINTING` rows of the requested
//! antennas over the requested times (and each antenna's nearest row on
//! either side), read once as the production selected-observation source
//! reads them (`selected_pointing.rs`), as the boundary's series.

use casa_imaging_model::PointingTimeSampling;
use casa_observation::{ObservationError, PointingDirection, PointingRequest, PointingSeries};
use casa_types::measures::direction::DirectionRef;

use super::{Msv2Observation, storage};
use crate::{PointingDirectionColumn, PointingReadPlan, SelectedPointingQueryDomain};

/// Rows of `POINTING` scanned per block while reading.
const SCAN_ROWS: usize = 1024;

/// The polynomial terms a `POINTING` row may have (`NUM_POLY` + 1): the
/// bound the production imaging source plans with.
const POLYNOMIAL_TERMS: usize = 4;

impl Msv2Observation {
    /// [`casa_observation::Observation::pointing`]: an MSv2's times are its
    /// `TIME` values (its partitions' time origin is MJD 0), so the
    /// request's times are `POINTING`'s.
    pub(super) fn read_pointing(
        &self,
        request: &PointingRequest,
    ) -> Result<PointingSeries, ObservationError> {
        self.partition(request.partition)?;
        let (start, end) = (*request.times.start(), *request.times.end());
        if !(start.is_finite() && end.is_finite() && start <= end) {
            return Err(ObservationError::InvalidRequest {
                reason: format!("pointing times {start}..={end} s are not a finite span"),
            });
        }
        let mut domain = SelectedPointingQueryDomain::builder();
        for antenna in &request.antennas {
            let id = i32::try_from(antenna.0)
                .ok()
                .filter(|_| (antenna.0 as usize) < self.catalogue.antennas.len())
                .ok_or_else(|| ObservationError::InvalidRequest {
                    reason: format!("no antenna {antenna:?}"),
                })?;
            for time in [start, end] {
                domain.observe_row(id, id, time, time).map_err(storage)?;
            }
        }
        let Some(domain) = domain.finish().map_err(storage)? else {
            return Ok(PointingSeries::empty(DirectionRef::J2000));
        };
        let column = match request.direction {
            PointingDirection::Beam => PointingDirectionColumn::Direction,
            PointingDirection::Target => PointingDirectionColumn::Target,
        };
        let plan = PointingReadPlan::new(SCAN_ROWS, POLYNOMIAL_TERMS, request.budget_bytes.max(1))
            .map_err(storage)?;
        self.ms
            .prepare_selected_pointing_catalog(
                column,
                &domain,
                PointingTimeSampling::VisibilityTime,
                plan,
            )
            .map_err(|error| ObservationError::InvalidRequest {
                reason: format!("POINTING of {:?}: {error}", request.partition),
            })?
            .series()
    }
}
