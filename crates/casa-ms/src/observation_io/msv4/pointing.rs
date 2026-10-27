// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MSv4 adapter's pointing: a dataset's `pointing_xds`
//! (`POINTING_BEAM[time_pointing | time, antenna_name, local_sky_dir_label]`)
//! over the requested times, as the boundary's series. MSv4 states no
//! interval per sample. XRADIO keeps one polynomial term (an `n_polynomial`
//! of one) and defines no origin for more, so more are refused.

use std::mem::size_of;
use std::ops::Range;

use casa_observation::{
    AntennaPointing, ObservationError, PointingDirection, PointingRequest, PointingSeries,
};
use casa_types::measures::direction::DirectionRef;
use zarrs::array::{Array, ArraySubset};
use zarrs::filesystem::FilesystemStore;

use super::metadata::{
    has_node, invalid, numbers, open_array, require_layout, require_units, storage, strings, text,
    time_reference,
};
use super::read::Msv4Observation;

/// XRADIO's `frame` for a pointing direction: the celestial frames, and
/// `altaz` for casacore's `AZELGEO`.
pub(crate) const POINTING_FRAMES: [(&str, DirectionRef); 3] = [
    ("fk5", DirectionRef::J2000),
    ("icrs", DirectionRef::ICRS),
    ("altaz", DirectionRef::AZELGEO),
];

/// Pointing times read at once while searching outward for an antenna's
/// nearest sample beyond the requested times.
const SEARCH_TIMES: u64 = 256;

impl Msv4Observation {
    /// [`casa_observation::Observation::pointing`] on one dataset: each
    /// antenna's samples within the span and its nearest one each side.
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
        for antenna in &request.antennas {
            if !dataset.antennas.iter().any(|(_, id)| id == antenna) {
                return Err(ObservationError::InvalidRequest {
                    reason: format!("{antenna:?} is not an antenna of {:?}", request.partition),
                });
            }
        }
        if !has_node(&self.store, &format!("{path}/POINTING_BEAM"))? {
            return Ok(PointingSeries::empty(DirectionRef::J2000));
        }
        let beam = open_array(&self.store, &format!("{path}/POINTING_BEAM"))?;
        let time_axis = if has_node(&self.store, &format!("{path}/time_pointing"))? {
            "time_pointing"
        } else {
            "time"
        };
        let shape = beam.shape().to_vec();
        let mut dimensions = vec![time_axis, "antenna_name", "local_sky_dir_label"];
        if shape.len() == 4 {
            dimensions.push("n_polynomial");
        }
        require_layout(
            &beam,
            &format!("{path}/POINTING_BEAM"),
            "float64",
            &dimensions,
        )?;
        if shape.len() == 4 && shape[3] != 1 {
            return Err(ObservationError::Unsupported {
                what: "an MSv4 pointing polynomial of more than one term (XRADIO defines no \
                       origin for its terms)",
            });
        }
        if shape[2] != 2 {
            return Err(invalid(format!(
                "{path}/POINTING_BEAM does not hold two direction axes"
            )));
        }
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
        let (times, time_attributes) = numbers::<f64>(&self.store, &format!("{path}/{time_axis}"))?;
        if time_reference(&time_attributes, time_axis)? != (entry.time_scale, entry.time_origin) {
            return Err(invalid(format!(
                "{path}'s times are not in its partition's time system"
            )));
        }
        let names = strings(&self.store, &format!("{path}/antenna_name"))?;
        if times.len() as u64 != shape[0] || names.len() as u64 != shape[1] {
            return Err(invalid(format!(
                "{path}/POINTING_BEAM does not span its coordinates"
            )));
        }
        let first = times.partition_point(|time| *time < start);
        let last = times.partition_point(|time| *time <= end);
        // Read: the time coordinate and the span's values. Held, at most: a
        // time, an origin and a direction for each requested antenna at
        // each time of the span and one each side.
        let read = (times.len() + (last - first) * names.len() * 2) * size_of::<f64>();
        let held = request.antennas.len() * (last - first + 2) * 4 * size_of::<f64>();
        if read + held > request.budget_bytes {
            return Err(ObservationError::InvalidRequest {
                reason: format!(
                    "{path} needs {} bytes over the request, beyond its budget of {}",
                    read + held,
                    request.budget_bytes
                ),
            });
        }
        let span = self.beam_values(&beam, first as u64..last as u64, 0..shape[1])?;
        let mut series = PointingSeries::empty(frame);
        for antenna in &request.antennas {
            let name = dataset
                .antennas
                .iter()
                .find(|(_, id)| id == antenna)
                .map(|(name, _)| name)
                .expect("checked above");
            // An antenna the dataset's pointing does not name has no samples.
            let Some(column) = names.iter().position(|known| known == name) else {
                continue;
            };
            let mut samples: Vec<(usize, [f64; 2])> = (first..last)
                .filter_map(|time| {
                    let at = ((time - first) * names.len() + column) * 2;
                    let direction = [span[at], span[at + 1]];
                    (!direction[0].is_nan()).then_some((time, direction))
                })
                .collect();
            if let Some(before) = self.nearest(&beam, column, first, Outward::Before)? {
                samples.insert(0, before);
            }
            if let Some(after) = self.nearest(&beam, column, last, Outward::After)? {
                samples.push(after);
            }
            let sample_times: Vec<f64> = samples.iter().map(|(time, _)| times[*time]).collect();
            let coefficients = samples
                .iter()
                .flat_map(|(_, direction)| *direction)
                .collect();
            series.antennas.insert(
                *antenna,
                AntennaPointing::new(sample_times.clone(), None, sample_times, 1, coefficients)?,
            );
        }
        Ok(series)
    }

    /// `POINTING_BEAM`'s directions at `times` for `antennas`, `[time,
    /// antenna, axis]`.
    fn beam_values(
        &self,
        beam: &Array<FilesystemStore>,
        times: Range<u64>,
        antennas: Range<u64>,
    ) -> Result<Vec<f64>, ObservationError> {
        let mut ranges = vec![times, antennas, 0..2];
        if beam.shape().len() == 4 {
            ranges.push(0..1);
        }
        beam.retrieve_array_subset(&ArraySubset::new_with_ranges(&ranges))
            .map_err(storage)
    }

    /// Antenna `column`'s nearest sample before time index `from` (the
    /// samples before it) or at or after it, reading outward a block of
    /// times at a time until one holds a direction.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "array extents are in-memory element counts"
    )]
    fn nearest(
        &self,
        beam: &Array<FilesystemStore>,
        column: usize,
        from: usize,
        outward: Outward,
    ) -> Result<Option<(usize, [f64; 2])>, ObservationError> {
        let (column, times) = (column as u64, beam.shape()[0]);
        let mut edge = from as u64;
        loop {
            let block = match outward {
                Outward::Before if edge == 0 => return Ok(None),
                Outward::Before => edge.saturating_sub(SEARCH_TIMES)..edge,
                Outward::After if edge >= times => return Ok(None),
                Outward::After => edge..(edge + SEARCH_TIMES).min(times),
            };
            let values = self.beam_values(beam, block.clone(), column..column + 1)?;
            let found = match outward {
                Outward::Before => (0..values.len() / 2)
                    .rev()
                    .find(|at| !values[2 * at].is_nan()),
                Outward::After => (0..values.len() / 2).find(|at| !values[2 * at].is_nan()),
            };
            if let Some(at) = found {
                let time = (block.start as usize) + at;
                return Ok(Some((time, [values[2 * at], values[2 * at + 1]])));
            }
            edge = match outward {
                Outward::Before => block.start,
                Outward::After => block.end,
            };
        }
    }
}

/// Which way [`Msv4Observation::nearest`] searches.
#[derive(Clone, Copy)]
enum Outward {
    Before,
    After,
}
