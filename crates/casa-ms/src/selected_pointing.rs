// SPDX-License-Identifier: LGPL-3.0-or-later

//! Bounded POINTING candidate reads for native selected-observation evaluation.

use std::collections::BTreeMap;

use casa_types::{ArrayValue, ScalarValue};

use crate::{
    MeasurementSet, MsError, MsResult,
    derived::engine::{MsCalEngine, resolve_direction_reference},
};

/// POINTING direction column read from storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PointingDirectionColumn {
    /// The per-antenna `DIRECTION` polynomial.
    Direction,
    /// The intended `TARGET` polynomial.
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

/// One antenna/time lookup against the POINTING table.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct PointingDirectionQuery {
    antenna_id: i32,
    time_mjd_seconds: f64,
}

impl PointingDirectionQuery {
    /// Construct one finite antenna/time query.
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

    /// Return the queried antenna.
    #[must_use]
    pub(crate) const fn antenna_id(self) -> i32 {
        self.antenna_id
    }

    /// Return the queried MAIN epoch in MJD seconds.
    #[must_use]
    pub(crate) const fn time_mjd_seconds(self) -> f64 {
        self.time_mjd_seconds
    }
}

/// Explicit bounded storage plan for POINTING lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PointingReadPlan {
    scan_rows_per_block: usize,
    maximum_polynomial_terms: usize,
}

impl PointingReadPlan {
    /// Construct a non-empty scan and polynomial bound.
    pub(crate) fn new(
        scan_rows_per_block: usize,
        maximum_polynomial_terms: usize,
    ) -> MsResult<Self> {
        if scan_rows_per_block == 0 || maximum_polynomial_terms == 0 {
            return Err(MsError::InvalidInput(
                "POINTING read plan requires positive scan rows and polynomial terms".to_string(),
            ));
        }
        Ok(Self {
            scan_rows_per_block,
            maximum_polynomial_terms,
        })
    }
}

/// One evaluated J2000 POINTING candidate.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct PointingDirectionCandidate {
    row_index: usize,
    row_time_mjd_seconds: f64,
    direction_j2000_rad: [f64; 2],
}

impl PointingDirectionCandidate {
    /// Return the source POINTING row.
    #[must_use]
    pub(crate) const fn row_index(self) -> usize {
        self.row_index
    }

    /// Return the POINTING row midpoint in MJD seconds.
    #[must_use]
    pub(crate) const fn row_time_mjd_seconds(self) -> f64 {
        self.row_time_mjd_seconds
    }

    /// Return the evaluated J2000 direction in radians.
    #[must_use]
    pub(crate) const fn direction_j2000_rad(self) -> [f64; 2] {
        self.direction_j2000_rad
    }
}

/// Covering and temporal bracket candidates for one query.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct PointingDirectionBracket {
    covering: Option<PointingDirectionCandidate>,
    before: Option<PointingDirectionCandidate>,
    after: Option<PointingDirectionCandidate>,
}

impl PointingDirectionBracket {
    /// Return the closest row whose positive interval contains the query.
    #[must_use]
    pub(crate) const fn covering(self) -> Option<PointingDirectionCandidate> {
        self.covering
    }

    /// Return the closest row at or before the query epoch.
    #[must_use]
    pub(crate) const fn before(self) -> Option<PointingDirectionCandidate> {
        self.before
    }

    /// Return the closest row at or after the query epoch.
    #[must_use]
    pub(crate) const fn after(self) -> Option<PointingDirectionCandidate> {
        self.after
    }
}

#[derive(Debug, Clone, Copy)]
struct CandidateMetadata {
    row_index: usize,
    time_mjd_seconds: f64,
    time_origin_mjd_seconds: f64,
    polynomial_terms: usize,
}

#[derive(Default)]
struct CandidateIndices {
    covering: Option<CandidateMetadata>,
    before: Option<CandidateMetadata>,
    after: Option<CandidateMetadata>,
}

impl MeasurementSet {
    /// Scan POINTING scalar metadata in bounded blocks and evaluate only exact candidates.
    ///
    /// The returned vector preserves query order. At most three candidate rows are retained per
    /// query, so resident POINTING data is independent of the table row count.
    pub(crate) fn pointing_direction_brackets(
        &self,
        engine: &MsCalEngine,
        column: PointingDirectionColumn,
        queries: &[PointingDirectionQuery],
        plan: PointingReadPlan,
    ) -> MsResult<Vec<PointingDirectionBracket>> {
        if queries.is_empty() {
            return Ok(Vec::new());
        }
        let pointing = match self.pointing() {
            Ok(pointing) => pointing,
            Err(MsError::MissingSubtable(_)) => {
                return Ok((0..queries.len())
                    .map(|_| PointingDirectionBracket {
                        covering: None,
                        before: None,
                        after: None,
                    })
                    .collect());
            }
            Err(error) => return Err(error),
        };
        let table = pointing.table();
        let antenna_column = table.column_accessor("ANTENNA_ID")?;
        let time_column = table.column_accessor("TIME")?;
        let interval_column = table.column_accessor("INTERVAL")?;
        let time_origin_column = table.column_accessor("TIME_ORIGIN")?;
        let polynomial_column = table.column_accessor("NUM_POLY")?;
        let mut matches = (0..queries.len())
            .map(|_| CandidateIndices::default())
            .collect::<Vec<_>>();

        for block_start in (0..pointing.row_count()).step_by(plan.scan_rows_per_block) {
            let block_end = (block_start + plan.scan_rows_per_block).min(pointing.row_count());
            let rows = (block_start..block_end).collect::<Vec<_>>();
            let antennas = antenna_column.scalar_cells_owned_for_rows(&rows)?;
            let times = time_column.scalar_cells_owned_for_rows(&rows)?;
            let intervals = interval_column.scalar_cells_owned_for_rows(&rows)?;
            let time_origins = time_origin_column.scalar_cells_owned_for_rows(&rows)?;
            let polynomial_orders = polynomial_column.scalar_cells_owned_for_rows(&rows)?;
            for (slot, row_index) in rows.into_iter().enumerate() {
                let antenna_id = required_i32(&antennas, slot, "ANTENNA_ID", row_index)?;
                let time_mjd_seconds = required_f64(&times, slot, "TIME", row_index)?;
                let interval_seconds = required_f64(&intervals, slot, "INTERVAL", row_index)?;
                let time_origin_mjd_seconds =
                    required_f64(&time_origins, slot, "TIME_ORIGIN", row_index)?;
                let order = required_i32(&polynomial_orders, slot, "NUM_POLY", row_index)?;
                let polynomial_terms = usize::try_from(order)
                    .ok()
                    .and_then(|order| order.checked_add(1))
                    .filter(|terms| *terms <= plan.maximum_polynomial_terms)
                    .ok_or_else(|| {
                        MsError::InvalidInput(format!(
                            "POINTING row {row_index} polynomial order {order} exceeds the planned bound"
                        ))
                    })?;
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
                    time_mjd_seconds,
                    time_origin_mjd_seconds,
                    polynomial_terms,
                };
                for (query, matched) in queries.iter().copied().zip(&mut matches) {
                    if query.antenna_id != antenna_id {
                        continue;
                    }
                    let delta = time_mjd_seconds - query.time_mjd_seconds;
                    if interval_seconds > 0.0 && delta.abs() <= 0.5 * interval_seconds {
                        retain_closer(&mut matched.covering, candidate, query.time_mjd_seconds);
                    }
                    if delta <= 0.0 {
                        retain_later(&mut matched.before, candidate);
                    }
                    if delta >= 0.0 {
                        retain_earlier(&mut matched.after, candidate);
                    }
                }
            }
        }

        let metadata_by_row = matches
            .iter()
            .flat_map(|matched| [matched.covering, matched.before, matched.after])
            .flatten()
            .map(|metadata| (metadata.row_index, metadata))
            .collect::<BTreeMap<_, _>>();
        let row_indices = metadata_by_row.keys().copied().collect::<Vec<_>>();
        let direction_cells = table
            .column_accessor(column.name())?
            .array_cells_owned(&row_indices)?;
        let directions_by_row = row_indices
            .iter()
            .copied()
            .zip(direction_cells)
            .map(|(row_index, cell)| {
                cell.map(|cell| (row_index, cell)).ok_or_else(|| {
                    MsError::InvalidInput(format!(
                        "POINTING.{} row {row_index} is undefined",
                        column.name()
                    ))
                })
            })
            .collect::<MsResult<BTreeMap<_, _>>>()?;

        matches
            .into_iter()
            .zip(queries.iter().copied())
            .map(|(matched, query)| {
                Ok(PointingDirectionBracket {
                    covering: evaluate_candidate(
                        table,
                        engine,
                        column,
                        matched.covering,
                        query,
                        &directions_by_row,
                    )?,
                    before: evaluate_candidate(
                        table,
                        engine,
                        column,
                        matched.before,
                        query,
                        &directions_by_row,
                    )?,
                    after: evaluate_candidate(
                        table,
                        engine,
                        column,
                        matched.after,
                        query,
                        &directions_by_row,
                    )?,
                })
            })
            .collect()
    }
}

fn evaluate_candidate(
    table: &casa_tables::Table,
    engine: &MsCalEngine,
    column: PointingDirectionColumn,
    metadata: Option<CandidateMetadata>,
    query: PointingDirectionQuery,
    directions_by_row: &BTreeMap<usize, ArrayValue>,
) -> MsResult<Option<PointingDirectionCandidate>> {
    let Some(metadata) = metadata else {
        return Ok(None);
    };
    let direction = directions_by_row
        .get(&metadata.row_index)
        .expect("candidate direction was selected by row");
    let ArrayValue::Float64(coefficients) = direction else {
        return Err(MsError::ColumnTypeMismatch {
            column: column.name().to_string(),
            table: "POINTING".to_string(),
            expected: "Float64 array shaped [2, nPoly+1]".to_string(),
            found: format!("{:?}", direction.primitive_type()),
        });
    };
    if coefficients.shape() != [2, metadata.polynomial_terms] {
        return Err(MsError::ColumnTypeMismatch {
            column: column.name().to_string(),
            table: "POINTING".to_string(),
            expected: format!("Float64 array shaped [2, {}]", metadata.polynomial_terms),
            found: format!("Float64 array shaped {:?}", coefficients.shape()),
        });
    }
    let offset_seconds = query.time_mjd_seconds - metadata.time_origin_mjd_seconds;
    let mut angles_rad = [0.0; 2];
    for axis in 0..2 {
        for term in (0..metadata.polynomial_terms).rev() {
            angles_rad[axis] = angles_rad[axis] * offset_seconds + coefficients[[axis, term]];
        }
    }
    if !angles_rad.into_iter().all(f64::is_finite) {
        return Err(MsError::InvalidInput(format!(
            "POINTING.{} row {} evaluates to a non-finite direction",
            column.name(),
            metadata.row_index
        )));
    }
    let source_ref =
        resolve_direction_reference(table, "POINTING", column.name(), metadata.row_index)?;
    Ok(Some(PointingDirectionCandidate {
        row_index: metadata.row_index,
        row_time_mjd_seconds: metadata.time_mjd_seconds,
        direction_j2000_rad: engine.direction_angles_j2000(
            query.time_mjd_seconds,
            angles_rad,
            source_ref,
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

fn retain_closer(
    current: &mut Option<CandidateMetadata>,
    candidate: CandidateMetadata,
    query_time: f64,
) {
    if current.is_none_or(|existing| {
        (candidate.time_mjd_seconds - query_time)
            .abs()
            .total_cmp(&(existing.time_mjd_seconds - query_time).abs())
            .then_with(|| candidate.row_index.cmp(&existing.row_index))
            .is_lt()
    }) {
        *current = Some(candidate);
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
