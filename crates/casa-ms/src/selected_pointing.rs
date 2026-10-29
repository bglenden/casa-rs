// SPDX-License-Identifier: LGPL-3.0-or-later
//! A bounded read of the `POINTING` rows a query domain needs, which the
//! MSv2 adapter gives as the observation boundary's pointing series.

use std::time::Instant;

use casa_types::{ArrayValue, ScalarValue, measures::direction::DirectionRef};

use crate::{
    MeasurementSet, MsError, MsResult, derived::engine::resolve_direction_reference_selected,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PointingDirectionColumn {
    Direction,
    Target,
}

impl PointingDirectionColumn {
    const fn name(self) -> &'static str {
        match self {
            Self::Direction => "DIRECTION",
            Self::Target => "TARGET",
        }
    }
}

/// Selected antennas and query-time envelope needed to prepare POINTING once.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SelectedPointingQueryDomain {
    antennas: Box<[AntennaPointingQueryDomain]>,
}

impl Eq for SelectedPointingQueryDomain {}

#[derive(Debug, Clone, Copy, PartialEq)]
struct AntennaPointingQueryDomain {
    antenna_id: i32,
    time_bounds: [f64; 2],
}

impl SelectedPointingQueryDomain {
    #[must_use]
    pub(crate) fn builder() -> SelectedPointingQueryDomainBuilder {
        SelectedPointingQueryDomainBuilder::default()
    }

    pub(crate) fn antenna_ids(&self) -> impl ExactSizeIterator<Item = i32> + '_ {
        self.antennas.iter().map(|antenna| antenna.antenna_id)
    }

    #[must_use]
    pub(crate) fn time_bounds_mjd_seconds(&self, antenna_id: i32) -> Option<[f64; 2]> {
        let antenna = self.antennas.get(
            self.antennas
                .binary_search_by_key(&antenna_id, |antenna| antenna.antenna_id)
                .ok()?,
        )?;
        Some(antenna.time_bounds)
    }
}

/// One-pass selected MAIN metadata accumulator for POINTING preparation.
#[derive(Debug, Default)]
pub(crate) struct SelectedPointingQueryDomainBuilder {
    antennas: Vec<AntennaPointingQueryDomain>,
}

impl SelectedPointingQueryDomainBuilder {
    pub(crate) fn observe_row(
        &mut self,
        antenna1: i32,
        antenna2: i32,
        time_mjd_seconds: f64,
    ) -> MsResult<()> {
        if antenna1 < 0 || antenna2 < 0 || !time_mjd_seconds.is_finite() {
            return Err(MsError::InvalidInput(
                "selected POINTING domain requires non-negative antennas and finite epochs"
                    .to_string(),
            ));
        }
        self.observe_antenna(antenna1, time_mjd_seconds);
        self.observe_antenna(antenna2, time_mjd_seconds);
        Ok(())
    }

    fn observe_antenna(&mut self, antenna_id: i32, time_mjd_seconds: f64) {
        if let Some(antenna) = self
            .antennas
            .iter_mut()
            .find(|antenna| antenna.antenna_id == antenna_id)
        {
            extend_bounds(&mut antenna.time_bounds, time_mjd_seconds);
        } else {
            self.antennas.push(AntennaPointingQueryDomain {
                antenna_id,
                time_bounds: [time_mjd_seconds; 2],
            });
        }
    }

    pub(crate) fn finish(mut self) -> MsResult<Option<SelectedPointingQueryDomain>> {
        if self.antennas.is_empty() {
            return Ok(None);
        }
        self.antennas.sort_by_key(|antenna| antenna.antenna_id);
        Ok(Some(SelectedPointingQueryDomain {
            antennas: self.antennas.into_boxed_slice(),
        }))
    }
}

fn extend_bounds([first, last]: &mut [f64; 2], value: f64) {
    *first = first.min(value);
    *last = last.max(value);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PointingReadPlan {
    scan_rows_per_block: usize,
    maximum_polynomial_terms: usize,
    maximum_catalog_bytes: usize,
}

impl PointingReadPlan {
    pub(crate) fn new(
        scan_rows_per_block: usize,
        maximum_polynomial_terms: usize,
        maximum_catalog_bytes: usize,
    ) -> MsResult<Self> {
        if scan_rows_per_block == 0 || maximum_polynomial_terms == 0 || maximum_catalog_bytes == 0 {
            return Err(MsError::InvalidInput(
                "POINTING read plan requires positive scan rows, polynomial terms, and catalog bytes"
                    .to_string(),
            ));
        }
        Ok(Self {
            scan_rows_per_block,
            maximum_polynomial_terms,
            maximum_catalog_bytes,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct CandidateMetadata {
    row_index: usize,
    antenna_id: i32,
    time_mjd_seconds: f64,
    interval_seconds: f64,
    time_origin_mjd_seconds: f64,
    polynomial_terms: usize,
}

#[derive(Debug)]
struct CatalogRow {
    metadata: CandidateMetadata,
    coefficient_offset: usize,
    source_ref: DirectionRef,
}

pub(crate) const SCALAR_SCAN_BYTES_PER_ROW: usize =
    size_of::<usize>() + 5 * size_of::<Option<ScalarValue>>();

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SelectedPointingCatalogMeasurements {
    retained_bytes: usize,
    construction_peak_bytes: usize,
    source_rows_scanned: usize,
    retained_rows: usize,
    build_nanos: u64,
}

impl SelectedPointingCatalogMeasurements {
    #[must_use]
    pub(crate) const fn retained_bytes(self) -> usize {
        self.retained_bytes
    }
    #[must_use]
    pub(crate) const fn construction_peak_bytes(self) -> usize {
        self.construction_peak_bytes
    }
    #[must_use]
    pub(crate) const fn source_rows_scanned(self) -> usize {
        self.source_rows_scanned
    }
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn retained_rows(self) -> usize {
        self.retained_rows
    }
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn build_nanos(self) -> u64 {
        self.build_nanos
    }
}

#[derive(Debug)]
pub(crate) struct PreparedSelectedPointingCatalog {
    rows: Vec<CatalogRow>,
    coefficients: Vec<f64>,
    /// What the read held, which the counting laws check.
    #[cfg_attr(not(test), allow(dead_code))]
    measurements: SelectedPointingCatalogMeasurements,
}

impl PreparedSelectedPointingCatalog {
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn measurements(&self) -> SelectedPointingCatalogMeasurements {
        self.measurements
    }

    /// The rows as the observation boundary's pointing series: per antenna
    /// (an antenna's catalogue id is its `ANTENNA` row), in the catalog's
    /// (time, row) order, with their intervals and polynomial origins, and
    /// coefficients padded with zeros to the most terms any row has.
    ///
    /// # Errors
    ///
    /// [`casa_observation::ObservationError::Unsupported`] when the rows'
    /// directions are in several frames.
    pub(crate) fn series(
        &self,
    ) -> Result<casa_observation::PointingSeries, casa_observation::ObservationError> {
        let frame = self
            .rows
            .first()
            .map_or(DirectionRef::J2000, |row| row.source_ref);
        if self.rows.iter().any(|row| row.source_ref != frame) {
            return Err(casa_observation::ObservationError::Unsupported {
                what: "MSv2 POINTING directions in several frames",
            });
        }
        let terms = self
            .rows
            .iter()
            .map(|row| row.metadata.polynomial_terms)
            .max()
            .unwrap_or(1);
        let mut series = casa_observation::PointingSeries {
            terms,
            ..casa_observation::PointingSeries::empty(frame)
        };
        for rows in self
            .rows
            .chunk_by(|left, right| left.metadata.antenna_id == right.metadata.antenna_id)
        {
            let mut coefficients = Vec::with_capacity(rows.len() * 2 * terms);
            for row in rows {
                let own = row.metadata.polynomial_terms;
                for axis in 0..2 {
                    let start = row.coefficient_offset + axis * own;
                    coefficients.extend_from_slice(&self.coefficients[start..start + own]);
                    coefficients.extend(std::iter::repeat_n(0.0, terms - own));
                }
            }
            let antenna = casa_observation::AntennaId(
                u32::try_from(rows[0].metadata.antenna_id).expect("ANTENNA_ID is non-negative"),
            );
            series.antennas.insert(
                antenna,
                casa_observation::AntennaPointing::new(
                    rows.iter()
                        .map(|row| row.metadata.time_mjd_seconds)
                        .collect(),
                    Some(
                        rows.iter()
                            .map(|row| row.metadata.interval_seconds)
                            .collect(),
                    ),
                    rows.iter()
                        .map(|row| row.metadata.time_origin_mjd_seconds)
                        .collect(),
                    terms,
                    coefficients,
                )?,
            );
        }
        Ok(series)
    }
}

impl MeasurementSet {
    /// Bound POINTING preparation from row-count metadata without reading its cells.
    ///
    /// All POINTING rows are conservatively treated as retained candidates at the
    /// planned polynomial bound. The measurements contain byte/count ceilings,
    /// with a zero build time; the construction ceiling excludes scalar scan row
    /// storage. The second result charges that storage per scan-block row. A
    /// missing POINTING table requires neither catalog nor scan storage.
    pub(crate) fn selected_pointing_catalog_requirements(
        &self,
        domain: &SelectedPointingQueryDomain,
        maximum_polynomial_terms: usize,
    ) -> MsResult<(SelectedPointingCatalogMeasurements, usize)> {
        if maximum_polynomial_terms == 0 {
            return Err(MsError::InvalidInput(
                "POINTING requirements need a positive polynomial bound".to_string(),
            ));
        }
        let rows = match self.pointing() {
            Ok(pointing) => pointing.row_count(),
            Err(MsError::MissingSubtable(_)) => {
                return Ok((SelectedPointingCatalogMeasurements::default(), 0));
            }
            Err(error) => return Err(error),
        };
        let endpoint_count = domain.antenna_ids().len();
        let endpoint_bytes = endpoint_count
            .checked_mul(size_of::<[Option<CandidateMetadata>; 2]>())
            .ok_or_else(byte_overflow)?;
        let prechecked_rows = endpoint_count
            .checked_mul(2)
            .and_then(|endpoints| rows.checked_add(endpoints))
            .ok_or_else(byte_overflow)?;
        let scan_fixed_bytes =
            catalog_upper_bound_bytes(prechecked_rows, maximum_polynomial_terms)?
                .checked_add(endpoint_bytes)
                .ok_or_else(byte_overflow)?;
        let retained_bytes = catalog_upper_bound_bytes(rows, maximum_polynomial_terms)?;
        // The catalog bound already includes one coefficient copy and array
        // dimensions. Direction cells coexist with the final coefficient copy.
        let copy_bytes_per_row = maximum_polynomial_terms
            .checked_mul(2)
            .and_then(|terms| terms.checked_mul(size_of::<f64>()))
            .and_then(|bytes| bytes.checked_add(size_of::<CandidateMetadata>()))
            .and_then(|bytes| bytes.checked_add(size_of::<usize>()))
            .and_then(|bytes| bytes.checked_add(size_of::<Option<ArrayValue>>()))
            .ok_or_else(byte_overflow)?;
        let copy_peak_bytes = rows
            .checked_mul(copy_bytes_per_row)
            .and_then(|bytes| bytes.checked_add(retained_bytes))
            .ok_or_else(byte_overflow)?;
        Ok((
            SelectedPointingCatalogMeasurements {
                retained_bytes,
                construction_peak_bytes: scan_fixed_bytes.max(copy_peak_bytes),
                source_rows_scanned: rows,
                retained_rows: rows,
                build_nanos: 0,
            },
            SCALAR_SCAN_BYTES_PER_ROW,
        ))
    }

    /// Scan POINTING scalar metadata once, then retain and read only selected candidates.
    #[expect(
        clippy::too_many_lines,
        reason = "predates the function-length limit; #705"
    )]
    pub(crate) fn prepare_selected_pointing_catalog(
        &self,
        column: PointingDirectionColumn,
        domain: &SelectedPointingQueryDomain,
        plan: PointingReadPlan,
    ) -> MsResult<PreparedSelectedPointingCatalog> {
        let started = Instant::now();
        let pointing = match self.pointing() {
            Ok(pointing) => pointing,
            Err(MsError::MissingSubtable(_)) => {
                return Ok(PreparedSelectedPointingCatalog {
                    rows: Vec::new(),
                    coefficients: Vec::new(),
                    measurements: SelectedPointingCatalogMeasurements {
                        build_nanos: elapsed_nanos(started),
                        ..Default::default()
                    },
                });
            }
            Err(error) => return Err(error),
        };
        let table = pointing.table();
        let mut retained = Vec::<CandidateMetadata>::new();
        let endpoint_count = domain.antenna_ids().len();
        let endpoint_bytes = endpoint_count
            .checked_mul(size_of::<[Option<CandidateMetadata>; 2]>())
            .ok_or_else(byte_overflow)?;
        let maximum_scan_bytes = plan
            .scan_rows_per_block
            .checked_mul(SCALAR_SCAN_BYTES_PER_ROW)
            .and_then(|bytes| bytes.checked_add(endpoint_bytes))
            .ok_or_else(byte_overflow)?;
        ensure_budget(maximum_scan_bytes, plan.maximum_catalog_bytes)?;
        let mut endpoints = Vec::new();
        endpoints.try_reserve_exact(endpoint_count).map_err(|_| {
            MsError::InvalidInput("POINTING endpoint allocation failed".to_string())
        })?;
        endpoints.resize(endpoint_count, [None; 2]);
        let mut construction_peak_bytes = 0usize;

        for block_start in (0..pointing.row_count()).step_by(plan.scan_rows_per_block) {
            let block_end = (block_start + plan.scan_rows_per_block).min(pointing.row_count());
            let rows = (block_start..block_end).collect::<Vec<_>>();
            let antennas = table
                .column_accessor("ANTENNA_ID")?
                .scalar_cells_owned_for_rows(&rows)?;
            let times = table
                .column_accessor("TIME")?
                .scalar_cells_owned_for_rows(&rows)?;
            let intervals = table
                .column_accessor("INTERVAL")?
                .scalar_cells_owned_for_rows(&rows)?;
            let time_origins = table
                .column_accessor("TIME_ORIGIN")?
                .scalar_cells_owned_for_rows(&rows)?;
            let orders = table
                .column_accessor("NUM_POLY")?
                .scalar_cells_owned_for_rows(&rows)?;
            let scan_bytes = rows.capacity() * size_of::<usize>()
                + (antennas.capacity()
                    + times.capacity()
                    + intervals.capacity()
                    + time_origins.capacity()
                    + orders.capacity())
                    * size_of::<Option<ScalarValue>>();
            construction_peak_bytes = construction_peak_bytes.max(
                retained.capacity() * size_of::<CandidateMetadata>()
                    + endpoints.capacity() * size_of::<[Option<CandidateMetadata>; 2]>()
                    + scan_bytes,
            );

            for (slot, row_index) in rows.into_iter().enumerate() {
                let antenna_id = required_i32(&antennas, slot, "ANTENNA_ID", row_index)?;
                let Some([first_query_time, last_query_time]) =
                    domain.time_bounds_mjd_seconds(antenna_id)
                else {
                    continue;
                };
                let time_mjd_seconds = required_f64(&times, slot, "TIME", row_index)?;
                let interval_seconds = required_f64(&intervals, slot, "INTERVAL", row_index)?;
                let time_origin_mjd_seconds =
                    required_f64(&time_origins, slot, "TIME_ORIGIN", row_index)?;
                let order = required_i32(&orders, slot, "NUM_POLY", row_index)?;
                let polynomial_terms = usize::try_from(order).ok()
                    .and_then(|order| order.checked_add(1))
                    .filter(|terms| *terms <= plan.maximum_polynomial_terms)
                    .ok_or_else(|| MsError::InvalidInput(format!(
                        "POINTING row {row_index} polynomial order {order} exceeds the planned bound"
                    )))?;
                if !time_mjd_seconds.is_finite()
                    || !interval_seconds.is_finite()
                    || !time_origin_mjd_seconds.is_finite()
                {
                    return Err(MsError::InvalidInput(format!(
                        "POINTING row {row_index} has non-finite time metadata"
                    )));
                }
                let candidate = CandidateMetadata {
                    row_index,
                    antenna_id,
                    time_mjd_seconds,
                    interval_seconds,
                    time_origin_mjd_seconds,
                    polynomial_terms,
                };
                let half_interval = 0.5 * interval_seconds;
                let intersects = interval_seconds > 0.0
                    && time_mjd_seconds + half_interval >= first_query_time
                    && time_mjd_seconds - half_interval <= last_query_time;
                let centre_inside =
                    time_mjd_seconds >= first_query_time && time_mjd_seconds <= last_query_time;
                if intersects || centre_inside {
                    let next_rows = retained
                        .len()
                        .checked_add(1 + 2 * endpoints.len())
                        .ok_or_else(byte_overflow)?;
                    let required =
                        catalog_upper_bound_bytes(next_rows, plan.maximum_polynomial_terms)?
                            .checked_add(scan_bytes)
                            .and_then(|bytes| {
                                bytes.checked_add(
                                    endpoints.capacity()
                                        * size_of::<[Option<CandidateMetadata>; 2]>(),
                                )
                            })
                            .ok_or_else(byte_overflow)?;
                    ensure_budget(required, plan.maximum_catalog_bytes)?;
                    retained.try_reserve_exact(1).map_err(|_| {
                        MsError::InvalidInput(
                            "POINTING candidate metadata allocation failed".to_string(),
                        )
                    })?;
                    retained.push(candidate);
                } else {
                    let endpoint_index = domain
                        .antenna_ids()
                        .position(|selected| selected == antenna_id)
                        .expect("selected antenna was already found");
                    let endpoint = &mut endpoints[endpoint_index];
                    if time_mjd_seconds < first_query_time {
                        retain_later(&mut endpoint[0], candidate);
                    }
                    if time_mjd_seconds > last_query_time {
                        retain_earlier(&mut endpoint[1], candidate);
                    }
                }
            }
            ensure_budget(
                catalog_upper_bound_bytes(
                    retained.capacity() + 2 * endpoints.len(),
                    plan.maximum_polynomial_terms,
                )?,
                plan.maximum_catalog_bytes,
            )?;
        }

        let endpoint_rows = endpoints.iter().flatten().flatten().count();
        let final_metadata_rows = retained
            .len()
            .checked_add(endpoint_rows)
            .ok_or_else(byte_overflow)?;
        ensure_budget(
            catalog_upper_bound_bytes(final_metadata_rows, plan.maximum_polynomial_terms)?,
            plan.maximum_catalog_bytes,
        )?;
        retained.try_reserve_exact(endpoint_rows).map_err(|_| {
            MsError::InvalidInput("POINTING endpoint retention allocation failed".to_string())
        })?;
        retained.extend(endpoints.into_iter().flatten().flatten());
        retained.sort_by(|left, right| {
            left.antenna_id
                .cmp(&right.antenna_id)
                .then_with(|| left.time_mjd_seconds.total_cmp(&right.time_mjd_seconds))
                .then_with(|| left.row_index.cmp(&right.row_index))
        });
        retained.dedup_by_key(|row| row.row_index);
        let index_bytes = retained
            .len()
            .checked_mul(size_of::<usize>())
            .ok_or_else(byte_overflow)?;
        let index_peak = catalog_upper_bound_bytes(retained.len(), plan.maximum_polynomial_terms)?
            .checked_add(retained.capacity() * size_of::<CandidateMetadata>())
            .and_then(|bytes| bytes.checked_add(index_bytes))
            .ok_or_else(byte_overflow)?;
        ensure_budget(index_peak, plan.maximum_catalog_bytes)?;
        let mut row_indices = Vec::new();
        row_indices.try_reserve_exact(retained.len()).map_err(|_| {
            MsError::InvalidInput("POINTING row-index allocation failed".to_string())
        })?;
        row_indices.extend(retained.iter().map(|row| row.row_index));
        ensure_budget(
            construction_upper_bound(&retained, &row_indices, plan.maximum_polynomial_terms)?,
            plan.maximum_catalog_bytes,
        )?;
        let direction_cells = table
            .column_accessor(column.name())?
            .array_cells_owned_uncached(&row_indices)?;
        let direction_heap_bytes = direction_cells
            .iter()
            .flatten()
            .try_fold(0usize, |bytes, value| {
                bytes.checked_add(array_heap_bytes(value)?)
            })
            .ok_or_else(byte_overflow)?;
        construction_peak_bytes = construction_peak_bytes.max(
            retained.capacity() * size_of::<CandidateMetadata>()
                + row_indices.capacity() * size_of::<usize>()
                + direction_cells.capacity() * size_of::<Option<ArrayValue>>()
                + direction_heap_bytes,
        );

        let total_coefficients = retained
            .iter()
            .try_fold(0usize, |total, metadata| {
                total.checked_add(2 * metadata.polynomial_terms)
            })
            .ok_or_else(byte_overflow)?;
        let copy_peak = retained
            .capacity()
            .checked_mul(size_of::<CandidateMetadata>())
            .and_then(|bytes| bytes.checked_add(row_indices.capacity() * size_of::<usize>()))
            .and_then(|bytes| {
                bytes.checked_add(direction_cells.capacity() * size_of::<Option<ArrayValue>>())
            })
            .and_then(|bytes| bytes.checked_add(direction_heap_bytes))
            .and_then(|bytes| bytes.checked_add(retained.len() * size_of::<CatalogRow>()))
            .and_then(|bytes| bytes.checked_add(total_coefficients * size_of::<f64>()))
            .ok_or_else(byte_overflow)?;
        ensure_budget(copy_peak, plan.maximum_catalog_bytes)?;
        construction_peak_bytes = construction_peak_bytes.max(copy_peak);
        let mut catalog_rows = Vec::new();
        catalog_rows
            .try_reserve_exact(retained.len())
            .map_err(|_| {
                MsError::InvalidInput("POINTING catalog row allocation failed".to_string())
            })?;
        let mut coefficients = Vec::new();
        coefficients
            .try_reserve_exact(total_coefficients)
            .map_err(|_| {
                MsError::InvalidInput("POINTING coefficient allocation failed".to_string())
            })?;
        for ((metadata, direction), row_index) in retained
            .into_iter()
            .zip(direction_cells)
            .zip(row_indices.iter().copied())
        {
            let direction = direction.ok_or_else(|| {
                MsError::InvalidInput(format!(
                    "POINTING.{} row {row_index} is undefined",
                    column.name()
                ))
            })?;
            let ArrayValue::Float64(coefficient_array) = direction else {
                return Err(MsError::ColumnTypeMismatch {
                    column: column.name().to_string(),
                    table: "POINTING".to_string(),
                    expected: "Float64 array shaped [2, nPoly+1]".to_string(),
                    found: format!("{:?}", direction.primitive_type()),
                });
            };
            if coefficient_array.shape() != [2, metadata.polynomial_terms] {
                return Err(MsError::ColumnTypeMismatch {
                    column: column.name().to_string(),
                    table: "POINTING".to_string(),
                    expected: format!("Float64 array shaped [2, {}]", metadata.polynomial_terms),
                    found: format!("Float64 array shaped {:?}", coefficient_array.shape()),
                });
            }
            let source_ref = resolve_direction_reference_selected(
                table,
                "POINTING",
                column.name(),
                metadata.row_index,
            )?;
            let coefficient_offset = coefficients.len();
            coefficients.extend(coefficient_array.iter().copied());
            catalog_rows.push(CatalogRow {
                metadata,
                coefficient_offset,
                source_ref,
            });
        }
        let retained_bytes = catalog_retained_bytes(&catalog_rows, &coefficients)?;
        ensure_budget(retained_bytes, plan.maximum_catalog_bytes)?;
        construction_peak_bytes = construction_peak_bytes.max(
            retained_bytes
                .checked_add(row_indices.capacity() * size_of::<usize>())
                .ok_or_else(byte_overflow)?,
        );
        let measurements = SelectedPointingCatalogMeasurements {
            retained_bytes,
            construction_peak_bytes,
            source_rows_scanned: pointing.row_count(),
            retained_rows: catalog_rows.len(),
            build_nanos: elapsed_nanos(started),
        };
        Ok(PreparedSelectedPointingCatalog {
            rows: catalog_rows,
            coefficients,
            measurements,
        })
    }
}

fn required_i32(
    values: &[Option<ScalarValue>],
    slot: usize,
    column: &str,
    row: usize,
) -> MsResult<i32> {
    match values.get(slot).and_then(Option::as_ref) {
        Some(ScalarValue::Int32(value)) => Ok(*value),
        Some(other) => Err(MsError::ColumnTypeMismatch {
            column: column.to_string(),
            table: "POINTING".to_string(),
            expected: "Int32".to_string(),
            found: format!("{:?}", other.primitive_type()),
        }),
        None => Err(MsError::InvalidInput(format!(
            "POINTING.{column} row {row} is undefined"
        ))),
    }
}

fn required_f64(
    values: &[Option<ScalarValue>],
    slot: usize,
    column: &str,
    row: usize,
) -> MsResult<f64> {
    match values.get(slot).and_then(Option::as_ref) {
        Some(ScalarValue::Float64(value)) => Ok(*value),
        Some(other) => Err(MsError::ColumnTypeMismatch {
            column: column.to_string(),
            table: "POINTING".to_string(),
            expected: "Float64".to_string(),
            found: format!("{:?}", other.primitive_type()),
        }),
        None => Err(MsError::InvalidInput(format!(
            "POINTING.{column} row {row} is undefined"
        ))),
    }
}

fn retain_later(current: &mut Option<CandidateMetadata>, candidate: CandidateMetadata) {
    if current.is_none_or(|existing| {
        candidate
            .time_mjd_seconds
            .total_cmp(&existing.time_mjd_seconds)
            .then_with(|| existing.row_index.cmp(&candidate.row_index))
            .is_gt()
    }) {
        *current = Some(candidate);
    }
}

fn retain_earlier(current: &mut Option<CandidateMetadata>, candidate: CandidateMetadata) {
    if current.is_none_or(|existing| {
        candidate
            .time_mjd_seconds
            .total_cmp(&existing.time_mjd_seconds)
            .then_with(|| candidate.row_index.cmp(&existing.row_index))
            .is_lt()
    }) {
        *current = Some(candidate);
    }
}

fn catalog_upper_bound_bytes(rows: usize, maximum_polynomial_terms: usize) -> MsResult<usize> {
    let bytes_per_row = maximum_polynomial_terms
        .checked_mul(2)
        .and_then(|terms| terms.checked_mul(size_of::<f64>()))
        .and_then(|bytes| bytes.checked_add(size_of::<CatalogRow>()))
        .and_then(|bytes| bytes.checked_add(4 * size_of::<usize>()))
        .ok_or_else(byte_overflow)?;
    rows.checked_mul(bytes_per_row).ok_or_else(byte_overflow)
}

fn construction_upper_bound(
    metadata: &Vec<CandidateMetadata>,
    row_indices: &Vec<usize>,
    maximum_polynomial_terms: usize,
) -> MsResult<usize> {
    catalog_upper_bound_bytes(metadata.len(), maximum_polynomial_terms)?
        .checked_add(metadata.capacity() * size_of::<CandidateMetadata>())
        .and_then(|bytes| bytes.checked_add(row_indices.capacity() * size_of::<usize>()))
        .and_then(|bytes| bytes.checked_add(metadata.len() * size_of::<Option<ArrayValue>>()))
        .ok_or_else(byte_overflow)
}

fn catalog_retained_bytes(rows: &Vec<CatalogRow>, coefficients: &Vec<f64>) -> MsResult<usize> {
    rows.capacity()
        .checked_mul(size_of::<CatalogRow>())
        .and_then(|bytes| bytes.checked_add(coefficients.capacity() * size_of::<f64>()))
        .ok_or_else(byte_overflow)
}

fn array_heap_bytes(value: &ArrayValue) -> Option<usize> {
    let dimensions = value.ndim().checked_mul(2 * size_of::<usize>())?;
    match value {
        ArrayValue::Float64(values) => values
            .len()
            .checked_mul(size_of::<f64>())?
            .checked_add(dimensions),
        _ => Some(dimensions),
    }
}

fn ensure_budget(required: usize, available: usize) -> MsResult<()> {
    if required > available {
        return Err(MsError::OverBudget {
            what: "POINTING catalog",
            required,
            available,
        });
    }
    Ok(())
}

fn byte_overflow() -> MsError {
    MsError::InvalidInput("POINTING catalog byte count overflow".to_string())
}

fn elapsed_nanos(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// Conservative catalog construction peak for content planning.
/// An upper bound on the bytes preparing a catalog of `rows` retained
/// `POINTING` rows for `antennas` antennas (two nearest rows each) holds at
/// any one time, scanning `scan_rows` rows at a time with polynomials of at
/// most `maximum_polynomial_terms` terms, and the series made from it: the
/// sum of every budget check [`MeasurementSet::prepare_selected_pointing_catalog`]
/// makes, so a budget of at least this is never refused. `None` on
/// overflow.
#[cfg(feature = "msv4")]
pub(crate) fn catalog_peak_bytes(
    rows: usize,
    antennas: usize,
    scan_rows: usize,
    maximum_polynomial_terms: usize,
) -> Option<usize> {
    let rows = rows.checked_add(antennas.checked_mul(2)?)?;
    let coefficients = 2 * maximum_polynomial_terms * size_of::<f64>();
    // Metadata, row index, direction cell and its heap, catalog row and its
    // coefficients, and the series' time, interval, origin and coefficients.
    let per_row = size_of::<CandidateMetadata>()
        + size_of::<usize>()
        + size_of::<Option<ArrayValue>>()
        + coefficients
        + 4 * size_of::<usize>()
        + size_of::<CatalogRow>()
        + coefficients
        + 3 * size_of::<f64>()
        + coefficients;
    catalog_upper_bound_bytes(rows, maximum_polynomial_terms)
        .ok()?
        .checked_add(rows.checked_mul(per_row)?)?
        .checked_add(scan_rows.checked_mul(SCALAR_SCAN_BYTES_PER_ROW)?)?
        .checked_add(antennas.checked_mul(size_of::<[Option<CandidateMetadata>; 2]>())?)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A catalog of `R` retained rows for `A` antennas (each with its nearest
    /// rows either side) prepared with exactly `catalog_peak_bytes(R, A, S,
    /// terms)` is never refused, whatever the scan block `S`.
    #[cfg(feature = "msv4")]
    #[test]
    fn catalog_peak_bytes_bounds_every_budget_check() {
        use casa_types::{RecordField, RecordValue, Value};

        let mut builder = SelectedPointingQueryDomain::builder();
        builder.observe_row(0, 1, 0.0).unwrap();
        builder.observe_row(0, 1, 100.0).unwrap();
        let domain = builder.finish().unwrap().unwrap();
        let terms = 4;
        let mut ms = MeasurementSet::create_memory(crate::MeasurementSetBuilder::new()).unwrap();
        // Each antenna: two rows before the times, twenty within, two after.
        let times: Vec<f64> = [-10.0, -5.0]
            .into_iter()
            .chain((0..20).map(f64::from))
            .chain([150.0, 200.0])
            .collect();
        for antenna in 0..2 {
            for time in &times {
                let mut record = RecordValue::new(
                    crate::schema::pointing::REQUIRED_COLUMNS
                        .iter()
                        .map(|column| {
                            RecordField::new(
                                column.name,
                                crate::test_helpers::default_value_for_def(column),
                            )
                        })
                        .collect(),
                );
                record.upsert("ANTENNA_ID", Value::Scalar(ScalarValue::Int32(antenna)));
                record.upsert("TIME", Value::Scalar(ScalarValue::Float64(*time)));
                record.upsert("TIME_ORIGIN", Value::Scalar(ScalarValue::Float64(*time)));
                record.upsert("INTERVAL", Value::Scalar(ScalarValue::Float64(1.0)));
                record.upsert(
                    "NUM_POLY",
                    Value::Scalar(ScalarValue::Int32(terms as i32 - 1)),
                );
                record.upsert(
                    "DIRECTION",
                    Value::Array(ArrayValue::Float64(ndarray::ArrayD::zeros(vec![2, terms]))),
                );
                ms.pointing_mut()
                    .unwrap()
                    .table_mut()
                    .add_row(record)
                    .unwrap();
            }
        }
        for scan_rows in [1, 7, 64] {
            let budget = catalog_peak_bytes(40, 2, scan_rows, terms).unwrap();
            let catalog = ms
                .prepare_selected_pointing_catalog(
                    PointingDirectionColumn::Direction,
                    &domain,
                    PointingReadPlan::new(scan_rows, terms, budget).unwrap(),
                )
                .unwrap_or_else(|error| panic!("scan {scan_rows}: {error}"));
            assert_eq!(
                catalog.measurements().retained_rows(),
                44,
                "within and nearest"
            );
            assert!(catalog.measurements().construction_peak_bytes() <= budget);
        }
    }

    #[test]
    fn catalog_requirements_bound_prechecks_and_actual_construction() {
        use casa_types::{RecordField, RecordValue, Value};

        let mut builder = SelectedPointingQueryDomain::builder();
        builder.observe_row(0, 1, 0.0).unwrap();
        builder.observe_row(0, 1, 100.0).unwrap();
        let domain = builder.finish().unwrap().unwrap();
        for row_count in [0, 1, 19] {
            for terms in [1, 4] {
                let mut ms =
                    MeasurementSet::create_memory(crate::MeasurementSetBuilder::new()).unwrap();
                for row in 0..row_count {
                    let mut record = RecordValue::new(
                        crate::schema::pointing::REQUIRED_COLUMNS
                            .iter()
                            .map(|column| {
                                RecordField::new(
                                    column.name,
                                    crate::test_helpers::default_value_for_def(column),
                                )
                            })
                            .collect(),
                    );
                    record.upsert("TIME", Value::Scalar(ScalarValue::Float64(row as f64)));
                    record.upsert(
                        "NUM_POLY",
                        Value::Scalar(ScalarValue::Int32(terms as i32 - 1)),
                    );
                    record.upsert(
                        "DIRECTION",
                        Value::Array(ArrayValue::Float64(ndarray::ArrayD::zeros(vec![2, terms]))),
                    );
                    ms.pointing_mut()
                        .unwrap()
                        .table_mut()
                        .add_row(record)
                        .unwrap();
                }
                let (requirements, scan_bytes_per_row) = ms
                    .selected_pointing_catalog_requirements(&domain, terms)
                    .unwrap();
                assert_eq!(requirements.source_rows_scanned(), row_count);
                assert_eq!(requirements.retained_rows(), row_count);
                assert_eq!(requirements.build_nanos(), 0);
                assert_eq!(scan_bytes_per_row, SCALAR_SCAN_BYTES_PER_ROW);
                for scan_rows in [1, 7] {
                    let catalog_budget =
                        requirements.construction_peak_bytes() + scan_rows * scan_bytes_per_row;
                    let catalog = ms
                        .prepare_selected_pointing_catalog(
                            PointingDirectionColumn::Direction,
                            &domain,
                            PointingReadPlan::new(scan_rows, terms, catalog_budget).unwrap(),
                        )
                        .unwrap();
                    assert_eq!(catalog.measurements().retained_rows(), row_count);
                    assert!(
                        catalog.measurements().retained_bytes() <= requirements.retained_bytes()
                    );
                    assert!(catalog.measurements().construction_peak_bytes() <= catalog_budget);
                }
            }
        }
    }

    #[test]
    fn catalog_requirements_reject_zero_and_overflowing_polynomial_bounds() {
        let ms = MeasurementSet::create_memory(crate::MeasurementSetBuilder::new()).unwrap();
        let mut builder = SelectedPointingQueryDomain::builder();
        builder.observe_row(0, 1, 0.0).unwrap();
        let domain = builder.finish().unwrap().unwrap();
        for terms in [0, usize::MAX] {
            assert!(matches!(
                ms.selected_pointing_catalog_requirements(&domain, terms),
                Err(MsError::InvalidInput(_))
            ));
        }
        assert!(catalog_upper_bound_bytes(usize::MAX, 1).is_err());
    }

    #[test]
    fn a_domain_bounds_each_antennas_times_and_sorts_the_antennas() {
        let mut builder = SelectedPointingQueryDomain::builder();
        builder.observe_row(7, 2, 30.0).unwrap();
        builder.observe_row(2, 5, 10.0).unwrap();
        let domain = builder.finish().unwrap().unwrap();
        assert_eq!(domain.antenna_ids().collect::<Vec<_>>(), [2, 5, 7]);
        assert_eq!(domain.time_bounds_mjd_seconds(2), Some([10.0, 30.0]));
        assert_eq!(domain.time_bounds_mjd_seconds(5), Some([10.0, 10.0]));
    }

    #[test]
    fn endpoint_ties_preserve_lowest_source_row() {
        let first = CandidateMetadata {
            row_index: 9,
            antenna_id: 0,
            time_mjd_seconds: 4.0,
            interval_seconds: 0.0,
            time_origin_mjd_seconds: 4.0,
            polynomial_terms: 1,
        };
        let second = CandidateMetadata {
            row_index: 3,
            ..first
        };
        let mut before = None;
        retain_later(&mut before, first);
        retain_later(&mut before, second);
        let mut after = None;
        retain_earlier(&mut after, first);
        retain_earlier(&mut after, second);
        assert_eq!(before.unwrap().row_index, 3);
        assert_eq!(after.unwrap().row_index, 3);
    }
}
