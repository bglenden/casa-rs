// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded prepared POINTING lookup for native selected-observation evaluation.

use std::time::Instant;

use casa_imaging_model::PointingTimeSampling;
use casa_types::{ArrayValue, ScalarValue, measures::direction::DirectionRef};

use crate::{
    MeasurementSet, MsError, MsResult,
    derived::engine::{MsCalEngine, resolve_direction_reference_selected},
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct PointingDirectionQuery {
    antenna_id: i32,
    time_mjd_seconds: f64,
}

impl PointingDirectionQuery {
    pub(crate) fn new(antenna_id: i32, time_mjd_seconds: f64) -> MsResult<Self> {
        if antenna_id < 0 || !time_mjd_seconds.is_finite() {
            return Err(MsError::InvalidInput(
                "POINTING query requires a non-negative antenna and finite epoch".to_string(),
            ));
        }
        Ok(Self {
            antenna_id,
            time_mjd_seconds,
        })
    }

    #[must_use]
    pub(crate) const fn antenna_id(self) -> i32 {
        self.antenna_id
    }

    #[must_use]
    pub(crate) const fn time_mjd_seconds(self) -> f64 {
        self.time_mjd_seconds
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
    visibility_time_bounds: [f64; 2],
    centroid_time_bounds: [f64; 2],
}

impl SelectedPointingQueryDomain {
    #[must_use]
    pub(crate) fn builder() -> SelectedPointingQueryDomainBuilder {
        SelectedPointingQueryDomainBuilder::default()
    }

    #[must_use]
    pub(crate) fn antenna_ids(&self) -> impl ExactSizeIterator<Item = i32> + '_ {
        self.antennas.iter().map(|antenna| antenna.antenna_id)
    }

    #[must_use]
    pub(crate) fn time_bounds_mjd_seconds(
        &self,
        antenna_id: i32,
        sampling: PointingTimeSampling,
    ) -> Option<[f64; 2]> {
        let antenna = self.antennas.get(
            self.antennas
                .binary_search_by_key(&antenna_id, |antenna| antenna.antenna_id)
                .ok()?,
        )?;
        match sampling {
            PointingTimeSampling::VisibilityTime => Some(antenna.visibility_time_bounds),
            PointingTimeSampling::VisibilityTimeCentroid => Some(antenna.centroid_time_bounds),
        }
    }

    #[must_use]
    pub(crate) fn retained_bytes(&self) -> usize {
        self.antennas.len() * size_of::<AntennaPointingQueryDomain>()
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
        time_centroid_mjd_seconds: f64,
    ) -> MsResult<()> {
        if antenna1 < 0
            || antenna2 < 0
            || !time_mjd_seconds.is_finite()
            || !time_centroid_mjd_seconds.is_finite()
        {
            return Err(MsError::InvalidInput(
                "selected POINTING domain requires non-negative antennas and finite epochs"
                    .to_string(),
            ));
        }
        self.observe_antenna(antenna1, time_mjd_seconds, time_centroid_mjd_seconds);
        self.observe_antenna(antenna2, time_mjd_seconds, time_centroid_mjd_seconds);
        Ok(())
    }

    fn observe_antenna(
        &mut self,
        antenna_id: i32,
        time_mjd_seconds: f64,
        time_centroid_mjd_seconds: f64,
    ) {
        if let Some(antenna) = self
            .antennas
            .iter_mut()
            .find(|antenna| antenna.antenna_id == antenna_id)
        {
            extend_bounds(&mut antenna.visibility_time_bounds, time_mjd_seconds);
            extend_bounds(&mut antenna.centroid_time_bounds, time_centroid_mjd_seconds);
        } else {
            self.antennas.push(AntennaPointingQueryDomain {
                antenna_id,
                visibility_time_bounds: [time_mjd_seconds; 2],
                centroid_time_bounds: [time_centroid_mjd_seconds; 2],
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct PointingDirectionCandidate {
    row_index: usize,
    row_time_mjd_seconds: f64,
    direction_j2000_rad: [f64; 2],
}

impl PointingDirectionCandidate {
    #[must_use]
    pub(crate) const fn row_index(self) -> usize {
        self.row_index
    }
    #[must_use]
    pub(crate) const fn row_time_mjd_seconds(self) -> f64 {
        self.row_time_mjd_seconds
    }
    #[must_use]
    pub(crate) const fn direction_j2000_rad(self) -> [f64; 2] {
        self.direction_j2000_rad
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct PointingDirectionBracket {
    covering: Option<PointingDirectionCandidate>,
    before: Option<PointingDirectionCandidate>,
    after: Option<PointingDirectionCandidate>,
}

impl PointingDirectionBracket {
    #[must_use]
    pub(crate) const fn covering(self) -> Option<PointingDirectionCandidate> {
        self.covering
    }
    #[must_use]
    pub(crate) const fn before(self) -> Option<PointingDirectionCandidate> {
        self.before
    }
    #[must_use]
    pub(crate) const fn after(self) -> Option<PointingDirectionCandidate> {
        self.after
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

const SCALAR_SCAN_BYTES_PER_ROW: usize = size_of::<usize>() + 5 * size_of::<Option<ScalarValue>>();

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
    #[must_use]
    pub(crate) const fn retained_rows(self) -> usize {
        self.retained_rows
    }
    #[must_use]
    pub(crate) const fn build_nanos(self) -> u64 {
        self.build_nanos
    }
}

#[derive(Debug)]
pub(crate) struct PreparedSelectedPointingCatalog {
    rows: Vec<CatalogRow>,
    coefficients: Vec<f64>,
    maximum_half_interval_seconds: f64,
    measurements: SelectedPointingCatalogMeasurements,
}

impl PreparedSelectedPointingCatalog {
    #[must_use]
    pub(crate) const fn measurements(&self) -> SelectedPointingCatalogMeasurements {
        self.measurements
    }

    pub(crate) fn direction_brackets(
        &self,
        engine: &MsCalEngine,
        queries: &[PointingDirectionQuery],
    ) -> MsResult<Vec<PointingDirectionBracket>> {
        queries
            .iter()
            .copied()
            .map(|query| self.direction_bracket(engine, query))
            .collect()
    }

    fn direction_bracket(
        &self,
        engine: &MsCalEngine,
        query: PointingDirectionQuery,
    ) -> MsResult<PointingDirectionBracket> {
        let start = self
            .rows
            .partition_point(|row| row.metadata.antenna_id < query.antenna_id);
        let end = self
            .rows
            .partition_point(|row| row.metadata.antenna_id <= query.antenna_id);
        let rows = &self.rows[start..end];
        let first_at_or_after =
            rows.partition_point(|row| row.metadata.time_mjd_seconds < query.time_mjd_seconds);
        let first_after =
            rows.partition_point(|row| row.metadata.time_mjd_seconds <= query.time_mjd_seconds);
        let before = if first_after == 0 {
            None
        } else {
            let time = rows[first_after - 1].metadata.time_mjd_seconds;
            rows[..first_after]
                .iter()
                .find(|row| row.metadata.time_mjd_seconds.to_bits() == time.to_bits())
        };
        let after = rows.get(first_at_or_after);

        let lower = query.time_mjd_seconds - self.maximum_half_interval_seconds;
        let upper = query.time_mjd_seconds + self.maximum_half_interval_seconds;
        let covering_start = rows.partition_point(|row| row.metadata.time_mjd_seconds < lower);
        let covering_end = rows.partition_point(|row| row.metadata.time_mjd_seconds <= upper);
        let covering = rows[covering_start..covering_end]
            .iter()
            .filter(|row| {
                row.metadata.interval_seconds > 0.0
                    && (row.metadata.time_mjd_seconds - query.time_mjd_seconds).abs()
                        <= 0.5 * row.metadata.interval_seconds
            })
            .min_by(|left, right| {
                (left.metadata.time_mjd_seconds - query.time_mjd_seconds)
                    .abs()
                    .total_cmp(&(right.metadata.time_mjd_seconds - query.time_mjd_seconds).abs())
                    .then_with(|| left.metadata.row_index.cmp(&right.metadata.row_index))
            });
        Ok(PointingDirectionBracket {
            covering: evaluate_candidate(engine, &self.coefficients, covering, query)?,
            before: evaluate_candidate(engine, &self.coefficients, before, query)?,
            after: evaluate_candidate(engine, &self.coefficients, after, query)?,
        })
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
    pub(crate) fn prepare_selected_pointing_catalog(
        &self,
        column: PointingDirectionColumn,
        domain: &SelectedPointingQueryDomain,
        sampling: PointingTimeSampling,
        plan: PointingReadPlan,
    ) -> MsResult<PreparedSelectedPointingCatalog> {
        let started = Instant::now();
        let pointing = match self.pointing() {
            Ok(pointing) => pointing,
            Err(MsError::MissingSubtable(_)) => {
                return Ok(PreparedSelectedPointingCatalog {
                    rows: Vec::new(),
                    coefficients: Vec::new(),
                    maximum_half_interval_seconds: 0.0,
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
                    domain.time_bounds_mjd_seconds(antenna_id, sampling)
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
        let maximum_half_interval_seconds = catalog_rows
            .iter()
            .filter(|row| row.metadata.interval_seconds > 0.0)
            .map(|row| 0.5 * row.metadata.interval_seconds)
            .fold(0.0, f64::max);
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
            maximum_half_interval_seconds,
            measurements,
        })
    }
}

fn evaluate_candidate(
    engine: &MsCalEngine,
    coefficients: &[f64],
    row: Option<&CatalogRow>,
    query: PointingDirectionQuery,
) -> MsResult<Option<PointingDirectionCandidate>> {
    let Some(row) = row else {
        return Ok(None);
    };
    let offset_seconds = query.time_mjd_seconds - row.metadata.time_origin_mjd_seconds;
    let coefficient_count = 2 * row.metadata.polynomial_terms;
    let coefficients =
        &coefficients[row.coefficient_offset..row.coefficient_offset + coefficient_count];
    let mut angles_rad = [0.0; 2];
    for axis in 0..2 {
        for term in (0..row.metadata.polynomial_terms).rev() {
            angles_rad[axis] = angles_rad[axis] * offset_seconds
                + coefficients[axis * row.metadata.polynomial_terms + term];
        }
    }
    if !angles_rad.into_iter().all(f64::is_finite) {
        return Err(MsError::InvalidInput(format!(
            "POINTING row {} evaluates to a non-finite direction",
            row.metadata.row_index
        )));
    }
    Ok(Some(PointingDirectionCandidate {
        row_index: row.metadata.row_index,
        row_time_mjd_seconds: row.metadata.time_mjd_seconds,
        direction_j2000_rad: engine.direction_angles_j2000(
            query.time_mjd_seconds,
            angles_rad,
            row.source_ref,
        )?,
    }))
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
        return Err(MsError::InvalidInput(format!(
            "POINTING catalog requires {required} bytes but the planner supplied {available} bytes"
        )));
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
pub(crate) fn selected_pointing_preparation_peak_bytes(
    selected_rows: usize,
    scan_rows: usize,
    maximum_polynomial_terms: usize,
    _direction_column: PointingDirectionColumn,
) -> Option<usize> {
    let candidate_rows = selected_rows.checked_mul(2)?.checked_add(2)?;
    catalog_upper_bound_bytes(candidate_rows, maximum_polynomial_terms)
        .ok()?
        .checked_add(scan_rows.checked_mul(SCALAR_SCAN_BYTES_PER_ROW)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_requirements_bound_prechecks_and_actual_construction() {
        use casa_types::{RecordField, RecordValue, Value};

        let mut builder = SelectedPointingQueryDomain::builder();
        builder.observe_row(0, 1, 0.0, 0.0).unwrap();
        builder.observe_row(0, 1, 100.0, 100.0).unwrap();
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
                            PointingTimeSampling::VisibilityTime,
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
        builder.observe_row(0, 1, 0.0, 0.0).unwrap();
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
    fn selected_domain_uses_requested_time_sampling_and_sorted_antennas() {
        let mut builder = SelectedPointingQueryDomain::builder();
        builder.observe_row(7, 2, 30.0, 31.0).unwrap();
        builder.observe_row(2, 5, 10.0, 12.0).unwrap();
        let domain = builder.finish().unwrap().unwrap();
        assert_eq!(domain.antenna_ids().collect::<Vec<_>>(), [2, 5, 7]);
        assert_eq!(
            domain.time_bounds_mjd_seconds(2, PointingTimeSampling::VisibilityTime),
            Some([10.0, 30.0])
        );
        assert_eq!(
            domain.time_bounds_mjd_seconds(5, PointingTimeSampling::VisibilityTimeCentroid),
            Some([12.0, 12.0])
        );
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
