// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_tables::{RequiredScalarColumnValues, Table};
use casa_types::measures::direction::MDirection;

use crate::derived::engine::resolve_direction_reference_selected;
use crate::{MsError, MsResult};

use super::super::metadata_field::signed_angles;
use super::MsPointing;

const BATCH_ROWS: usize = 4096;

#[derive(Clone, Copy)]
struct Sample {
    row: usize,
    time: f64,
    interval: f64,
}

struct PointingRows<'a>(&'a Table);

impl PointingRows<'_> {
    fn visit(
        &self,
        mut rows: impl Iterator<Item = usize>,
        antennas: &[i32],
        mut stop: impl FnMut(i32, Sample) -> MsResult<bool>,
    ) -> MsResult<()> {
        let mut indices = Vec::with_capacity(BATCH_ROWS);
        let mut selected = Vec::new();
        let mut selected_antennas = Vec::new();
        loop {
            indices.extend(rows.by_ref().take(BATCH_ROWS));
            if indices.is_empty() {
                return Ok(());
            }
            // Read candidate IDs first. Large POINTING tables can contain
            // many samples for antennas outside the requested baseline.
            let values = self
                .0
                .required_scalar_columns_owned_for_rows(&["ANTENNA_ID"], &indices)?;
            let Some(RequiredScalarColumnValues::Int32(ids)) = values.get("ANTENNA_ID") else {
                return Err(MsError::InvalidInput("Invalid POINTING antenna IDs".into()));
            };
            for (&row, &id) in indices.iter().zip(ids) {
                if antennas.contains(&id) {
                    selected.push(row);
                    selected_antennas.push(id);
                }
            }
            indices.clear();
            if selected.is_empty() {
                continue;
            }
            let values = self
                .0
                .required_scalar_columns_owned_for_rows(&["TIME", "INTERVAL"], &selected)?;
            let (
                Some(RequiredScalarColumnValues::Float64(times)),
                Some(RequiredScalarColumnValues::Float64(intervals)),
            ) = (values.get("TIME"), values.get("INTERVAL"))
            else {
                return Err(MsError::InvalidInput(
                    "Invalid POINTING time columns".into(),
                ));
            };
            for (index, &row) in selected.iter().enumerate() {
                let sample = Sample {
                    row,
                    time: times[index],
                    interval: intervals[index],
                };
                if stop(selected_antennas[index], sample)? {
                    return Ok(());
                }
            }
            selected.clear();
            selected_antennas.clear();
        }
    }

    fn distinct_time(
        &self,
        rows: impl Iterator<Item = usize>,
        antenna: i32,
        time: f64,
    ) -> MsResult<Option<f64>> {
        let mut found = None;
        self.visit(rows, &[antenna], |_, other| {
            if other.time != time {
                found = Some(other.time);
            }
            Ok(found.is_some())
        })?;
        Ok(found)
    }

    fn covers(&self, antenna: i32, sample: Sample, time: f64) -> MsResult<bool> {
        let half = if sample.interval == 0.0 {
            // A zero interval derives its coverage from the next sample at
            // a different timestamp for this antenna, or the previous one.
            let end = self.0.row_count();
            let neighbor = match self.distinct_time(sample.row + 1..end, antenna, sample.time)? {
                Some(next) => Some(next),
                None => self.distinct_time((0..sample.row).rev(), antenna, sample.time)?,
            };
            neighbor.map_or(0.0, |other| (other - sample.time).abs() / 2.0)
        } else {
            sample.interval / 2.0
        };
        // Preserve casacore's endpoint comparison. At MeasurementSet epochs
        // an absolute difference can round to the other side of the boundary.
        Ok(half <= 0.0 || (sample.time >= time - half && sample.time <= time + half))
    }

    fn lookup(&self, antennas: [i32; 2], time: f64, guess: usize) -> MsResult<[Option<Sample>; 2]> {
        let end = self.0.row_count();
        let guess = guess.min(end);
        let mut samples = [None; 2];
        self.visit((guess..end).chain(0..guess), &antennas, |id, sample| {
            let index = usize::from(id != antennas[0]);
            if samples[index].is_none() && self.covers(id, sample, time)? {
                samples[index] = Some(sample);
                if antennas[0] == antennas[1] {
                    samples[1] = Some(sample);
                }
            }
            Ok(samples.iter().all(Option::is_some))
        })?;
        Ok(samples)
    }
}

impl MsPointing<'_> {
    /// Read the pointing directions of the antennas associated with one MAIN
    /// row. Start at the requested POINTING row and wrap through the subtable.
    /// Interpolate only when requested and the integration interval is shorter
    /// than the matched POINTING interval.
    ///
    /// # Errors
    ///
    /// A required column or pointing sample is missing or malformed, or an
    /// adjacent sample is unavailable when interpolation is requested.
    pub fn metadata_baseline_directions(
        &self,
        antennas: [i32; 2],
        time: f64,
        main_interval: f64,
        initial_row: usize,
        interpolate: bool,
    ) -> MsResult<[MDirection; 2]> {
        let rows = PointingRows(self.table());
        let samples = rows.lookup(antennas, time, initial_row)?;
        let direction = |antenna, sample: Option<Sample>| {
            let sample = sample.ok_or_else(|| {
                MsError::InvalidInput(format!(
                    "No POINTING direction for antenna {antenna} at time {time}"
                ))
            })?;
            let left = self.read_metadata_direction(sample.row)?;
            if !interpolate || main_interval >= sample.interval {
                return Ok(left);
            }
            let after = (sample.row + 1, sample.time + sample.interval);
            let before = (0, sample.time - sample.interval);
            let [preferred, alternate] = if time >= sample.time {
                [after, before]
            } else {
                [before, after]
            };
            let distinct = |(guess, time)| -> MsResult<Option<Sample>> {
                Ok(rows.lookup([antenna; 2], time, guess)?[0]
                    .filter(|other| other.row != sample.row))
            };
            let neighbor = match distinct(preferred)? {
                Some(neighbor) => Some(neighbor),
                None => distinct(alternate)?,
            }
            .ok_or_else(|| {
                MsError::InvalidInput(
                    "Failed to find pointing index to interpolate direction".into(),
                )
            })?;
            if neighbor.time == sample.time {
                return Err(MsError::InvalidInput(
                    "Pointing interpolation needs distinct times".into(),
                ));
            }
            let a = signed_angles(&left);
            let b = signed_angles(&self.read_metadata_direction(neighbor.row)?);
            let delta = (time - sample.time) / (neighbor.time - sample.time);
            Ok(MDirection::from_angles(
                a[0] + (b[0] - a[0]) * delta,
                a[1] + (b[1] - a[1]) * delta,
                left.refer(),
            ))
        };
        let first = direction(antennas[0], samples[0])?;
        let last = if antennas[0] == antennas[1] {
            first.clone()
        } else {
            direction(antennas[1], samples[1])?
        };
        Ok([first, last])
    }

    fn read_metadata_direction(&self, row: usize) -> MsResult<MDirection> {
        let table = self.table();
        let values = table
            .column_accessor("DIRECTION")?
            .array_cells_owned_uncached(&[row])?
            .pop()
            .flatten()
            .ok_or_else(|| MsError::InvalidInput("Missing POINTING direction".into()))?;
        let angles = self.direction_angles(&values, row, "DIRECTION", None)?;
        let reference = resolve_direction_reference_selected(table, "POINTING", "DIRECTION", row)?;
        Ok(MDirection::from_angles(angles[0], angles[1], reference))
    }
}
