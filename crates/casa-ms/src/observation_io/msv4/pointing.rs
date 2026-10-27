// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MSv4 adapter's pointing: a dataset's `pointing_xds`
//! (`POINTING_BEAM[time_pointing | time, antenna_name, local_sky_dir_label
//! (, n_polynomial)]`) over the requested times, as the boundary's series.
//! MSv4 states no interval per sample, and a sample's polynomial terms,
//! when present, are in the time since the sample.

use std::mem::size_of;

use casa_observation::{
    AntennaPointing, ObservationError, PointingDirection, PointingRequest, PointingSeries,
};
use casa_types::measures::direction::DirectionRef;
use zarrs::array::ArraySubset;

use super::metadata::{
    invalid, numbers, open_array, require_units, storage, strings, text, time_reference,
};
use super::read::Msv4Observation;

/// XRADIO's `frame` for a pointing direction: the celestial frames, and
/// `altaz` for casacore's `AZELGEO`.
pub(super) const POINTING_FRAMES: [(&str, DirectionRef); 3] = [
    ("fk5", DirectionRef::J2000),
    ("icrs", DirectionRef::ICRS),
    ("altaz", DirectionRef::AZELGEO),
];

impl Msv4Observation {
    /// [`casa_observation::Observation::pointing`] on one dataset: the
    /// sample times within the span and the nearest one each side.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "array extents are in-memory element counts"
    )]
    pub(super) fn read_pointing(
        &self,
        request: &PointingRequest,
    ) -> Result<PointingSeries, ObservationError> {
        let (dataset, entry) = self.dataset(request.partition)?;
        if request.direction == PointingDirection::Target {
            return Err(ObservationError::Unsupported {
                what: "an MSv4 pointing target (pointing_xds holds the beam's direction only)",
            });
        }
        let (start, end) = (*request.times.start(), *request.times.end());
        if !(start.is_finite() && end.is_finite() && start <= end) {
            return Err(ObservationError::InvalidRequest {
                reason: format!("pointing times {start}..={end} s are not a finite span"),
            });
        }
        let path = format!("{}/pointing_xds", dataset.path);
        let names = strings(&self.store, &format!("{path}/antenna_name")).ok();
        let mut columns = Vec::new();
        for antenna in &request.antennas {
            let name = dataset
                .antennas
                .iter()
                .find(|(_, id)| id == antenna)
                .map(|(name, _)| name)
                .ok_or_else(|| ObservationError::InvalidRequest {
                    reason: format!("{antenna:?} is not an antenna of {:?}", request.partition),
                })?;
            if let Some(column) = names
                .as_ref()
                .and_then(|names| names.iter().position(|known| known == name))
            {
                columns.push((*antenna, column));
            }
        }
        let Ok(beam) = open_array(&self.store, &format!("{path}/POINTING_BEAM")) else {
            return Ok(PointingSeries::empty(DirectionRef::J2000));
        };
        let attributes = beam.attributes();
        require_units(attributes, "rad", "POINTING_BEAM")?;
        let frame_name = text(attributes, "frame", "POINTING_BEAM")?;
        let frame = POINTING_FRAMES
            .iter()
            .find(|(name, _)| *name == frame_name)
            .map(|(_, frame)| *frame)
            .ok_or(ObservationError::Unsupported {
                what: "an MSv4 pointing frame other than fk5, icrs or altaz",
            })?;
        let shape = beam.shape().to_vec();
        if !(shape.len() == 3 || shape.len() == 4) || shape[2] != 2 {
            return Err(invalid(format!(
                "{path}/POINTING_BEAM is not [time, antenna, 2(, polynomial)]"
            )));
        }
        let time_axis = if open_array(&self.store, &format!("{path}/time_pointing")).is_ok() {
            "time_pointing"
        } else {
            "time"
        };
        let (times, time_attributes) = numbers::<f64>(&self.store, &format!("{path}/{time_axis}"))?;
        if time_reference(&time_attributes, time_axis)? != (entry.time_scale, entry.time_origin) {
            return Err(invalid(format!(
                "{path}'s times are not in its partition's time system"
            )));
        }
        let antennas = names.map_or(0, |names| names.len());
        if times.len() as u64 != shape[0] || antennas as u64 != shape[1] {
            return Err(invalid(format!(
                "{path}/POINTING_BEAM does not span its coordinates"
            )));
        }
        let first = times
            .partition_point(|time| *time < start)
            .saturating_sub(1);
        let last = (times.partition_point(|time| *time <= end) + 1).min(times.len());
        let terms = shape.get(3).copied().unwrap_or(1) as usize;
        let read = (last - first) * antennas * 2 * terms * size_of::<f64>();
        if read > request.budget_bytes {
            return Err(ObservationError::InvalidRequest {
                reason: format!(
                    "{path} holds {read} bytes over the request, beyond its budget of {}",
                    request.budget_bytes
                ),
            });
        }
        let mut ranges = vec![first as u64..last as u64, 0..shape[1], 0..2];
        if shape.len() == 4 {
            ranges.push(0..shape[3]);
        }
        let values: Vec<f64> = beam
            .retrieve_array_subset(&ArraySubset::new_with_ranges(&ranges))
            .map_err(storage)?;
        let width = 2 * terms;
        let mut series = PointingSeries {
            terms,
            ..PointingSeries::empty(frame)
        };
        for (antenna, column) in columns {
            // A sample is absent (NaN) where the antenna has none at a time.
            let offset = |time: usize| ((time - first) * antennas + column) * width;
            let samples: Vec<usize> = (first..last)
                .filter(|time| !values[offset(*time)].is_nan())
                .collect();
            let coefficients = samples
                .iter()
                .flat_map(|time| values[offset(*time)..offset(*time) + width].iter().copied())
                .collect();
            let sample_times: Vec<f64> = samples.iter().map(|time| times[*time]).collect();
            series.antennas.insert(
                antenna,
                AntennaPointing::new(
                    sample_times.clone(),
                    None,
                    sample_times,
                    terms,
                    coefficients,
                )?,
            );
        }
        Ok(series)
    }
}
