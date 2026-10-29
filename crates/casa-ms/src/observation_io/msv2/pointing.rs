// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MSv2 adapter's pointing: the `POINTING` rows of the requested
//! antennas over the requested times (and each antenna's nearest row on
//! either side), read once as the production selected-observation source
//! reads them (`selected_pointing.rs`), as the boundary's series.

use casa_observation::{ObservationError, PointingDirection, PointingRequest, PointingSeries};
use casa_types::measures::direction::DirectionRef;

use super::{Msv2Observation, storage};
use crate::selected_pointing::SCALAR_SCAN_BYTES_PER_ROW;
use crate::{MsError, PointingDirectionColumn, PointingReadPlan, SelectedPointingQueryDomain};

/// The most rows of `POINTING` scanned per block while reading. Each block
/// re-reads the storage manager's index (#949), so blocks are as large as a
/// sixteenth of the request's budget allows, up to this.
const SCAN_ROWS: usize = 1 << 16;

/// The polynomial terms a `POINTING` row may have (`NUM_POLY` + 1): the
/// bound the production imaging source plans with.
pub(crate) const POLYNOMIAL_TERMS: usize = 4;

/// The rows of `POINTING` scanned per block for a request of `budget`
/// bytes: a sixteenth of it, up to [`SCAN_ROWS`].
pub(crate) fn scan_rows(budget: usize) -> usize {
    (budget / 16 / SCALAR_SCAN_BYTES_PER_ROW).clamp(1, SCAN_ROWS)
}

impl Msv2Observation {
    /// The most bytes a pointing query of this observation holds while it
    /// is read and once it is answered, whatever its antennas and times
    /// ([`casa_observation::Observation::pointing_bytes`]): every `POINTING`
    /// row taken as a candidate at four polynomial terms (the production
    /// source's bound), with one scan block of at most every row, and the
    /// series it gives. A [`PointingRequest`] with this budget is never
    /// refused for its size, and reads `POINTING` in blocks as large as a
    /// query may scan. Zero without a `POINTING` table. Also the rows of the
    /// scan block it holds: `POINTING`'s, up to [`SCAN_ROWS`].
    pub(super) fn pointing_bound(&self) -> Result<(usize, usize), ObservationError> {
        let mut domain = SelectedPointingQueryDomain::builder();
        for antenna in 0..self.catalogue.antennas.len() {
            let id = i32::try_from(antenna).map_err(|_| ObservationError::InvalidStore {
                reason: format!("antenna {antenna} beyond the MSv2 identifier range"),
            })?;
            domain.observe_row(id, id, 0.0).map_err(storage)?;
        }
        let Some(domain) = domain.finish().map_err(storage)? else {
            return Ok((0, 1));
        };
        let (catalog, scan_bytes_per_row) = self
            .ms
            .selected_pointing_catalog_requirements(&domain, POLYNOMIAL_TERMS)
            .map_err(storage)?;
        // A scan block holds at most the table's rows.
        let rows = SCAN_ROWS.min(catalog.source_rows_scanned()).max(1);
        // The series holds a time, an interval, an origin and two
        // coefficients a term for each retained row, at most the catalog.
        let bytes = catalog
            .construction_peak_bytes()
            .saturating_add(rows.saturating_mul(scan_bytes_per_row))
            .saturating_add(catalog.retained_bytes());
        Ok((bytes, rows))
    }

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
                domain.observe_row(id, id, time).map_err(storage)?;
            }
        }
        let store_has_pointing = self
            .ms
            .subtable(crate::SubtableId::Pointing)
            .is_some_and(|table| table.row_count() > 0);
        let Some(domain) = domain.finish().map_err(storage)? else {
            return Ok(PointingSeries {
                store_has_pointing,
                ..PointingSeries::empty(DirectionRef::J2000)
            });
        };
        let column = match request.direction {
            PointingDirection::Beam => PointingDirectionColumn::Direction,
            PointingDirection::Target => PointingDirectionColumn::Target,
        };
        // A budget holding the whole bound holds its scan block, every row
        // up to `SCAN_ROWS`; a smaller one, a sixteenth of itself.
        let (bound, rows) = self.pointing_bound()?;
        let scan = if request.budget_bytes >= bound {
            rows
        } else {
            scan_rows(request.budget_bytes)
        };
        let plan = PointingReadPlan::new(scan, POLYNOMIAL_TERMS, request.budget_bytes.max(1))
            .map_err(storage)?;
        self.ms
            .prepare_selected_pointing_catalog(column, &domain, plan)
            .map_err(|error| match error {
                MsError::OverBudget {
                    required,
                    available,
                    ..
                } => ObservationError::OverBudget {
                    what: "an MSv2 pointing query",
                    required,
                    budget: available,
                },
                error => ObservationError::InvalidStore {
                    reason: format!("POINTING: {error}"),
                },
            })?
            .series()
            .map(|series| PointingSeries {
                store_has_pointing,
                ..series
            })
    }
}

#[cfg(test)]
mod tests {
    use casa_observation::{AntennaId, PartitionId, PointingDirection, PointingRequest};
    use casa_types::ArrayValue;

    use crate::MeasurementSet;
    use crate::observation_io::{OpenOptions, open};
    use crate::simulation::{
        SyntheticAntenna, SyntheticField, SyntheticObservationRequest,
        generate_synthetic_observation_ms,
    };

    /// A simulated MSv2 whose `POINTING` rows differ by antenna: antenna 0's
    /// have a rate term, antenna 1's no interval, antenna 2's intervals
    /// that overlap, so a time between two rows is covered by both.
    fn fixture(root: &std::path::Path) -> std::path::PathBuf {
        let mut request = SyntheticObservationRequest::vla_ppdisk(
            root.join("unused.fits"),
            root.join("pointing.ms"),
            vec![
                SyntheticAntenna::vla("VLA01", "N01", [-1_601_185.4, -5_041_977.5, 3_554_875.9]),
                SyntheticAntenna::vla("VLA02", "N02", [-1_601_085.4, -5_041_977.5, 3_554_875.9]),
                SyntheticAntenna::vla("VLA03", "N03", [-1_601_185.4, -5_041_877.5, 3_554_875.9]),
            ],
        );
        request.predict_model = false;
        request.duration_seconds = 60.0;
        request.integration_seconds = 10.0;
        // Fields of their own give a `POINTING` row per antenna and
        // integration.
        request.fields = vec![SyntheticField {
            name: "target".to_string(),
            phase_center_rad: [1.23, -0.45],
        }];
        generate_synthetic_observation_ms(&request).expect("simulate");
        let mut ms = MeasurementSet::open(&request.output_ms).expect("ms");
        {
            let mut pointing = ms.pointing_mut().expect("POINTING");
            for row in 0..pointing.row_count() {
                let antenna = pointing.as_ref().i32(row, "ANTENNA_ID").expect("antenna");
                match antenna {
                    0 => {
                        let [ra, dec] = match pointing.as_ref().array(row, "DIRECTION") {
                            Ok(ArrayValue::Float64(values)) => [values[[0, 0]], values[[1, 0]]],
                            other => panic!("DIRECTION is {other:?}"),
                        };
                        let terms = ndarray::arr2(&[[ra, 1e-6], [dec, -2e-6]]).into_dyn();
                        pointing.set_i32(row, "NUM_POLY", 1).expect("NUM_POLY");
                        pointing
                            .set_array(row, "DIRECTION", ArrayValue::Float64(terms))
                            .expect("DIRECTION");
                    }
                    1 => pointing.set_f64(row, "INTERVAL", 0.0).expect("INTERVAL"),
                    _ => pointing.set_f64(row, "INTERVAL", 25.0).expect("INTERVAL"),
                }
            }
        }
        ms.save().expect("save");
        request.output_ms
    }

    /// One `POINTING` row as the table states it.
    struct Row {
        antenna: i32,
        time_s: f64,
        interval_s: f64,
        origin_s: f64,
        /// `DIRECTION`'s terms, `[2, NUM_POLY + 1]`.
        terms: ndarray::Array2<f64>,
    }

    /// Every `POINTING` row of `path`, in table order.
    fn table_rows(path: &std::path::Path) -> Vec<Row> {
        let ms = MeasurementSet::open(path).expect("ms");
        let pointing = ms.pointing().expect("POINTING");
        (0..pointing.row_count())
            .map(|row| {
                let ArrayValue::Float64(terms) =
                    pointing.array(row, "DIRECTION").expect("DIRECTION").clone()
                else {
                    panic!("DIRECTION is Float64");
                };
                Row {
                    antenna: pointing.i32(row, "ANTENNA_ID").expect("ANTENNA_ID"),
                    time_s: pointing.f64(row, "TIME").expect("TIME"),
                    interval_s: pointing.f64(row, "INTERVAL").expect("INTERVAL"),
                    origin_s: pointing.f64(row, "TIME_ORIGIN").expect("TIME_ORIGIN"),
                    terms: terms.into_dimensionality().expect("[2, terms]"),
                }
            })
            .collect()
    }

    /// `row`'s direction at `time_s`: its polynomial about `TIME_ORIGIN`.
    fn direction_at(row: &Row, time_s: f64) -> [f64; 2] {
        let dt = time_s - row.origin_s;
        [0, 1].map(|axis| {
            row.terms
                .row(axis)
                .iter()
                .rev()
                .fold(0.0, |sum, term| sum * dt + term)
        })
    }

    /// `antenna`'s bracket at `time_s` read from `rows` one by one, as the
    /// boundary defines it ([`casa_observation::PointingBracket`]): the row
    /// whose interval holds the time, nearest it and then first in time
    /// order; the first row of the latest time at or before it; and the
    /// first row at or after it. Each is its time and its direction there.
    fn table_bracket(rows: &[Row], antenna: i32, time_s: f64) -> [Option<(f64, [f64; 2])>; 3] {
        let mut own: Vec<&Row> = rows.iter().filter(|row| row.antenna == antenna).collect();
        own.sort_by(|left, right| left.time_s.total_cmp(&right.time_s));
        let covering = own
            .iter()
            .filter(|row| {
                row.interval_s > 0.0 && (row.time_s - time_s).abs() <= 0.5 * row.interval_s
            })
            .min_by(|left, right| {
                (left.time_s - time_s)
                    .abs()
                    .total_cmp(&(right.time_s - time_s).abs())
            });
        let latest = own
            .iter()
            .rfind(|row| row.time_s <= time_s)
            .map(|row| row.time_s);
        let before = latest.and_then(|latest| own.iter().find(|row| row.time_s == latest));
        let after = own.iter().find(|row| row.time_s >= time_s);
        [covering, before, after].map(|row| row.map(|row| (row.time_s, direction_at(row, time_s))))
    }

    /// The boundary's MSv2 pointing brackets each (antenna, time) as the
    /// table states it, read row by row: the same covering, preceding and
    /// following rows, evaluated at the time to the same directions. The
    /// rows here are in time order for each antenna.
    #[test]
    fn brackets_are_the_tables_rows() {
        let root = tempfile::tempdir().expect("root");
        let path = fixture(root.path());
        let observation = open(&path, &OpenOptions::default()).expect("open");
        let grid = observation.grid(PartitionId(0), "base").expect("grid");
        let mut queries: Vec<f64> = grid
            .times_s
            .iter()
            .flat_map(|time| [time - 7.0, time - 5.0, *time, time + 3.0, time + 5.0])
            .collect();
        queries.extend([
            grid.times_s[0] - 30.0,
            grid.times_s[grid.times_s.len() - 1] + 30.0,
        ]);
        queries.sort_by(f64::total_cmp);
        let antennas: Vec<AntennaId> = (0..3).map(AntennaId).collect();
        let series = observation
            .pointing(&PointingRequest {
                partition: PartitionId(0),
                direction: PointingDirection::Beam,
                antennas: antennas.clone(),
                times: queries[0]..=queries[queries.len() - 1],
                budget_bytes: 1 << 20,
            })
            .expect("pointing");
        assert_eq!(series.terms, 2);
        let rows = table_rows(&path);
        let mut covered = [0; 3];
        for antenna in antennas {
            for time in &queries {
                let boundary = series.bracket(antenna, *time).expect("boundary");
                let boundary = [boundary.covering, boundary.before, boundary.after]
                    .map(|candidate| candidate.map(|c| (c.time_s, c.direction_rad)));
                let table = table_bracket(&rows, antenna.0 as i32, *time);
                for (got, want) in boundary.iter().zip(&table) {
                    match (got, want) {
                        (Some((got_time, got)), Some((want_time, want))) => {
                            assert_eq!(got_time, want_time, "{antenna:?} at {time}");
                            for (got, want) in got.iter().zip(want) {
                                assert!(
                                    (got - want).abs() <= 1e-15,
                                    "{antenna:?} at {time}: {got} against {want}"
                                );
                            }
                        }
                        (None, None) => {}
                        _ => panic!("{antenna:?} at {time}: {boundary:?} against {table:?}"),
                    }
                }
                covered[antenna.0 as usize] += usize::from(table[0].is_some());
            }
        }
        assert!(
            covered[0] > 0 && covered[2] > 0,
            "rows cover queries: {covered:?}"
        );
        assert_eq!(covered[1], 0, "rows of no interval cover nothing");
    }
}
