// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MSv4 adapter's pointing: a dataset's `pointing_xds`
//! (`POINTING_BEAM[time_pointing | time, antenna_name, local_sky_dir_label]`)
//! over the requested times, as the boundary's series. MSv4 states no
//! interval per sample. XRADIO keeps one polynomial term (an `n_polynomial`
//! of one) and defines no origin for more, so more are refused.

use std::mem::size_of;
use std::ops::Range;

use casa_observation::{
    AntennaId, AntennaPointing, ObservationError, PointingDirection, PointingRequest,
    PointingSeries,
};
use casa_types::measures::direction::DirectionRef;
use zarrs::array::{Array, ArraySubset};
use zarrs::filesystem::FilesystemStore;

use super::metadata::{
    chunk_bytes, has_node, invalid, open_array, require_layout, require_units, storage, text,
    time_reference,
};
use super::opening::handle_bytes;
use super::read::Msv4Observation;

/// XRADIO's `frame` for a pointing direction: the celestial frames, and
/// `altaz` for casacore's `AZELGEO`.
pub(crate) const POINTING_FRAMES: [(&str, DirectionRef); 3] = [
    ("fk5", DirectionRef::J2000),
    ("icrs", DirectionRef::ICRS),
    ("altaz", DirectionRef::AZELGEO),
];

/// Pointing times read at first while searching outward for the antennas'
/// nearest samples beyond the requested times (the casa-rs writer's chunk
/// of times); each further read doubles, up to [`MOST_SEARCH_TIMES`]. Every
/// antenna is searched in one read, so a chunk is decoded about once a side
/// rather than once an antenna.
const SEARCH_TIMES: u64 = 4096;

/// The most pointing times one outward read covers.
const MOST_SEARCH_TIMES: u64 = 16_384;

/// Column indices and nearest-sample slots per requested antenna.
const ANTENNA_WORKSPACE_BYTES: usize = 128;

fn antenna_workspace(requested: usize) -> usize {
    requested
        .saturating_mul(ANTENNA_WORKSPACE_BYTES)
        .saturating_add(casa_types::memory::btree_node_bytes::<
            AntennaId,
            AntennaPointing,
        >(requested))
}

/// Small Zarr codec contexts and traversal state beside the charged chunk,
/// array handles and coordinates. The conversion API grants 1 MiB for a
/// pointing read.
const POINTING_WORKSPACE_BYTES: usize = 64 << 10;

/// The decoded coordinates, directions and final series, with the array
/// handles, string-label decode and per-antenna bookkeeping held alongside.
fn pointing_charge(
    times: usize,
    span: usize,
    search: usize,
    columns: usize,
    requested: usize,
    metadata: usize,
) -> usize {
    let read = times
        .saturating_add(
            span.saturating_add(search)
                .saturating_mul(columns)
                .saturating_mul(2),
        )
        .saturating_mul(size_of::<f64>());
    let held = requested
        .saturating_mul(span.saturating_add(2))
        .saturating_mul(4 * size_of::<f64>());
    read.saturating_add(held)
        .saturating_add(metadata)
        .saturating_add(antenna_workspace(requested))
}

impl Msv4Observation {
    /// A pointing read's array metadata and string-coordinate workspace.
    /// The opening projector uses the same conservative handle and UTF32
    /// costs; its codec workspace also covers transient Zarr decode buffers.
    fn pointing_metadata(
        &self,
        path: &str,
        beam: &Array<FilesystemStore>,
        time_axis: &str,
    ) -> Result<usize, ObservationError> {
        if beam.shape().len() < 3 {
            return Err(invalid(format!(
                "{path}/POINTING_BEAM has no pointing axes"
            )));
        }
        let time = open_array(&self.store, &format!("{path}/{time_axis}"))?;
        let names = open_array(&self.store, &format!("{path}/antenna_name"))?;
        if time.shape() != [beam.shape()[0]] || names.shape() != [beam.shape()[1]] {
            return Err(invalid(format!(
                "{path}/POINTING_BEAM does not span its coordinates"
            )));
        }
        let fixed = names.data_type().fixed_size().unwrap_or(0);
        let labels = usize::try_from(names.shape()[0])
            .unwrap_or(usize::MAX)
            .saturating_mul(
                fixed
                    .saturating_mul(4)
                    .saturating_add(size_of::<Vec<char>>() + size_of::<String>()),
            );
        Ok(POINTING_WORKSPACE_BYTES
            .saturating_add(handle_bytes(beam))
            .saturating_add(handle_bytes(&time))
            .saturating_add(handle_bytes(&names))
            .saturating_add(labels)
            .saturating_add(chunk_bytes(beam)?.saturating_mul(2))
            .saturating_add(chunk_bytes(&time)?.saturating_mul(2))
            .saturating_add(chunk_bytes(&names)?.saturating_mul(2)))
    }

    /// The most bytes a pointing query reads and holds
    /// ([`casa_observation::Observation::pointing_bytes`]): over the
    /// datasets, a query spanning every pointing time of every antenna, or
    /// one that also searches outward from a span, whichever reads more,
    /// and the series it gives every antenna of the catalogue.
    pub(super) fn pointing_bound(&self) -> Result<usize, ObservationError> {
        let mut most = 0;
        for dataset in &self.datasets {
            let path = format!("{}/pointing_xds", dataset.path);
            let beam_path = format!("{path}/POINTING_BEAM");
            if !has_node(&self.store, &beam_path)? {
                continue;
            }
            let beam = open_array(&self.store, &beam_path)?;
            let time_axis = if has_node(&self.store, &format!("{path}/time_pointing"))? {
                "time_pointing"
            } else {
                "time"
            };
            let metadata = self.pointing_metadata(&path, &beam, time_axis)?;
            let shape = beam.shape();
            let [times, antennas] =
                [shape[0], shape[1]].map(|extent| usize::try_from(extent).unwrap_or(usize::MAX));
            let search = times.min(MOST_SEARCH_TIMES as usize);
            most = most.max(pointing_charge(
                times,
                times,
                search,
                antennas,
                self.catalogue.antennas.len(),
                metadata,
            ));
        }
        Ok(most)
    }

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
        for (index, antenna) in request.antennas.iter().enumerate() {
            if request.antennas[..index].contains(antenna) {
                return Err(ObservationError::InvalidRequest {
                    reason: format!("{antenna:?} is repeated in a pointing query"),
                });
            }
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
        let metadata = self.pointing_metadata(&path, &beam, time_axis)?;
        let minimum = metadata
            .saturating_add(antenna_workspace(request.antennas.len()))
            .saturating_add(
                usize::try_from(shape[0])
                    .unwrap_or(usize::MAX)
                    .saturating_mul(size_of::<f64>()),
            );
        if minimum > request.budget_bytes {
            return Err(ObservationError::OverBudget {
                what: "an MSv4 pointing query",
                required: minimum,
                budget: request.budget_bytes,
            });
        }
        let time = open_array(&self.store, &format!("{path}/{time_axis}"))?;
        let times: Vec<f64> = time
            .retrieve_array_subset_opt(
                &ArraySubset::new_with_shape(time.shape().to_vec()),
                &self.options,
            )
            .map_err(storage)?;
        let time_attributes = time.attributes().clone();
        if time_reference(&time_attributes, time_axis)? != (entry.time_scale, entry.time_origin) {
            return Err(invalid(format!(
                "{path}'s times are not in its partition's time system"
            )));
        }
        let names_array = open_array(&self.store, &format!("{path}/antenna_name"))?;
        let names: Vec<String> = names_array
            .retrieve_array_subset_opt::<Vec<Vec<char>>>(
                &ArraySubset::new_with_shape(names_array.shape().to_vec()),
                &self.options,
            )
            .map_err(storage)?
            .into_iter()
            .map(|chars| chars.into_iter().collect())
            .collect();
        if times.len() as u64 != shape[0] || names.len() as u64 != shape[1] {
            return Err(invalid(format!(
                "{path}/POINTING_BEAM does not span its coordinates"
            )));
        }
        let first = times.partition_point(|time| *time < start);
        let last = times.partition_point(|time| *time <= end);
        // Read: the time coordinate, the span's values and the largest
        // outward block. Held, at most: a time, an origin and a direction for
        // each requested antenna at each time of the span and one each side.
        // No search runs when the span holds every time.
        let search = if first == 0 && last == times.len() {
            0
        } else {
            times.len().min(MOST_SEARCH_TIMES as usize)
        };
        let required = pointing_charge(
            times.len(),
            last - first,
            search,
            names.len(),
            request.antennas.len(),
            metadata,
        );
        if required > request.budget_bytes {
            return Err(ObservationError::OverBudget {
                what: "an MSv4 pointing query",
                required,
                budget: request.budget_bytes,
            });
        }
        let span = self.beam_values(&beam, first as u64..last as u64, 0..shape[1])?;
        // Each requested antenna's column; an antenna the dataset's pointing
        // does not name has no samples.
        let columns: Vec<(AntennaId, usize)> = request
            .antennas
            .iter()
            .filter_map(|antenna| {
                let name = dataset
                    .antennas
                    .iter()
                    .find(|(_, id)| id == antenna)
                    .map(|(name, _)| name)?;
                let column = names.iter().position(|known| known == name)?;
                Some((*antenna, column))
            })
            .collect();
        let wanted: Vec<usize> = columns.iter().map(|(_, column)| *column).collect();
        let before = self.nearest(&beam, &wanted, first, Outward::Before)?;
        let after = self.nearest(&beam, &wanted, last, Outward::After)?;
        let mut series = PointingSeries {
            store_has_pointing: shape[0] > 0,
            ..PointingSeries::empty(frame)
        };
        for (index, (antenna, column)) in columns.into_iter().enumerate() {
            let within = (first..last).filter_map(|time| {
                let at = ((time - first) * names.len() + column) * 2;
                let direction = [span[at], span[at + 1]];
                (!direction[0].is_nan()).then_some((time, direction))
            });
            let samples = before[index].into_iter().chain(within).chain(after[index]);
            let mut sample_times = Vec::with_capacity(last - first + 2);
            let mut coefficients = Vec::with_capacity((last - first + 2) * 2);
            for (time, direction) in samples {
                sample_times.push(times[time]);
                coefficients.extend_from_slice(&direction);
            }
            series.antennas.insert(
                antenna,
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
        beam.retrieve_array_subset_opt(&ArraySubset::new_with_ranges(&ranges), &self.options)
            .map_err(storage)
    }

    /// Each of `columns`' nearest sample before time index `from` (the
    /// samples before it) or at or after it: reading every antenna's
    /// directions outward, a block of times at a time, until each column
    /// holds one or the times end.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "array extents are in-memory element counts"
    )]
    fn nearest(
        &self,
        beam: &Array<FilesystemStore>,
        columns: &[usize],
        from: usize,
        outward: Outward,
    ) -> Result<Vec<Nearest>, ObservationError> {
        let (times, antennas) = (beam.shape()[0], beam.shape()[1]);
        let mut found = vec![None; columns.len()];
        let (mut edge, mut size) = (from as u64, SEARCH_TIMES);
        while found.iter().any(Option::is_none) {
            let block = match outward {
                Outward::Before if edge == 0 => break,
                Outward::Before => edge.saturating_sub(size)..edge,
                Outward::After if edge >= times => break,
                Outward::After => edge..(edge + size).min(times),
            };
            let values = self.beam_values(beam, block.clone(), 0..antennas)?;
            let rows = (block.end - block.start) as usize;
            for (slot, column) in columns.iter().enumerate() {
                if found[slot].is_some() {
                    continue;
                }
                let at = |row: usize| (row * antennas as usize + column) * 2;
                let held = |row: &usize| !values[at(*row)].is_nan();
                let row = match outward {
                    Outward::Before => (0..rows).rev().find(held),
                    Outward::After => (0..rows).find(held),
                };
                found[slot] = row.map(|row| {
                    let at = at(row);
                    (block.start as usize + row, [values[at], values[at + 1]])
                });
            }
            edge = match outward {
                Outward::Before => block.start,
                Outward::After => block.end,
            };
            size = (2 * size).min(MOST_SEARCH_TIMES);
        }
        Ok(found)
    }
}

/// An antenna's nearest sample one side of the requested times: its time
/// index and direction.
type Nearest = Option<(usize, [f64; 2])>;

/// Which way [`Msv4Observation::nearest`] searches.
#[derive(Clone, Copy)]
enum Outward {
    Before,
    After,
}
