// SPDX-License-Identifier: LGPL-3.0-or-later
//! The trial's `CalibrationBinding` (`docs/measurement-set/contracts.md`
//! section 2): a calibration table's antennas, fields and spectral windows
//! for each entity of an opened store's catalogue, and the writer that
//! persists a solve through the store's namespace.
//!
//! A caltable numbers entities by the rows of its own subtables, and
//! catalogue identities are scoped to one opened store, so neither side
//! holds the other's numbers. Binding compares the caltable's rows with
//! the rows the target store's namespace has
//! ([`PersistedNamespace`](casa_ms::observation_io::PersistedNamespace)):
//!
//! 1. **Stated associations first**, as CASA's `spwmap` states them: the
//!    caller's, validated, never second-guessed.
//! 2. **CASA's own association** where the caltable's namespace is the
//!    target's: the same rows in the same order, names included. Each row
//!    then binds to its own index, as CASA applies a table to the
//!    MeasurementSet it was solved on, even where two rows hold the same
//!    content.
//! 3. **Content** otherwise: an antenna by station and position, a field
//!    by phase centre, a spectral window by its frame and channels (or the
//!    one channel a gain table collapses them to). Names decide nothing,
//!    since converters rewrite them; an entity the other side lacks binds
//!    to nothing (a subset of antennas); and two candidates are refused,
//!    never resolved by name or position, since equal content alone does
//!    not prove two rows are one entity.
//!
//! The calibration port moves this into production; it sees casa-ms only
//! through the namespace and the gain table writer.

use std::collections::BTreeMap;

use casa_ms::observation_io::{
    NamespaceAntenna, NamespaceField, NamespaceRows, NamespaceWindow, PersistedNamespace,
};
use casa_observation::{AntennaId, BindingTolerance, Catalogue, ObservationError, PartitionId};

use crate::solve::kernel::SolutionRow;
use crate::solve::writer::write_gain_caltable;
use crate::solve::{GainSolveError, GainSolveReport, GainSolveRequest};

/// Associations a caller states, which binding takes before anything it
/// could infer: CASA's `spwmap` and its like for antennas.
#[derive(Clone, Debug, Default)]
pub(super) struct StatedAssociations {
    /// A target partition and the caltable `SPECTRAL_WINDOW` row whose
    /// solutions it takes; several partitions may take one row.
    pub(super) spectral_windows: Vec<(PartitionId, u32)>,
    /// A target antenna and its caltable `ANTENNA` row, one to one.
    pub(super) antennas: Vec<(AntennaId, u32)>,
}

/// A calibration table's rows for each entity of a target store's
/// catalogue: `None` where the table has no such entity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CalibrationBinding {
    /// The `ANTENNA` row of each catalogue antenna.
    pub(super) antennas: Vec<Option<u32>>,
    /// The `FIELD` row of each catalogue field.
    pub(super) fields: Vec<Option<u32>>,
    /// The `SPECTRAL_WINDOW` row of each partition.
    pub(super) spectral_windows: Vec<Option<u32>>,
}

impl CalibrationBinding {
    /// Bind the caltable whose namespace is `table` to the store whose
    /// namespace is `target`.
    ///
    /// # Errors
    ///
    /// When a stated association names a row or entity that does not exist,
    /// or two antennas for one row; or when an entity matches more than one
    /// row of the other side by content, or two entities one row.
    pub(super) fn bind(
        table: &NamespaceRows,
        target: &PersistedNamespace,
        tolerance: &BindingTolerance,
        stated: &StatedAssociations,
    ) -> Result<Self, ObservationError> {
        let stated_antennas = stated
            .antennas
            .iter()
            .map(|(antenna, row)| {
                let target_row = target.antennas.get(antenna.0 as usize).ok_or_else(|| {
                    refused(format!("a stated association names antenna {}", antenna.0))
                })?;
                Ok((*target_row as usize, *row))
            })
            .collect::<Result<Vec<_>, ObservationError>>()?;
        // A partition's statement is its window row's; partitions sharing
        // a row share its solutions.
        let mut stated_windows = BTreeMap::new();
        for (partition, row) in &stated.spectral_windows {
            let target_row = *target
                .spectral_windows
                .get(partition.0 as usize)
                .ok_or_else(|| {
                    refused(format!(
                        "a stated association names partition {}",
                        partition.0
                    ))
                })? as usize;
            if stated_windows
                .insert(target_row, *row)
                .is_some_and(|other| other != *row)
            {
                return Err(refused(format!(
                    "partitions of one spectral window are stated to take table rows {row} and \
                     another"
                )));
            }
        }
        let stated_windows: Vec<(usize, u32)> = stated_windows.into_iter().collect();
        let antenna_rows = associate(
            Kind::one_to_one("antenna"),
            &table.antennas,
            &target.rows.antennas,
            |a, b| same_antenna(a, b, tolerance),
            |a, b| a.name == b.name,
            &stated_antennas,
        )?;
        let field_rows = associate(
            Kind::one_to_one("field"),
            &table.fields,
            &target.rows.fields,
            |a, b| same_field(a, b, tolerance),
            |a, b| a.name == b.name,
            &[],
        )?;
        let window_rows = associate(
            Kind {
                what: "spectral window",
                statements_share: true,
            },
            &table.spectral_windows,
            &target.rows.spectral_windows,
            solves,
            |a, b| a.name == b.name,
            &stated_windows,
        )?;
        let through = |rows: &[Option<u32>], ids: &[u32]| -> Vec<Option<u32>> {
            ids.iter().map(|row| rows[*row as usize]).collect()
        };
        Ok(Self {
            antennas: through(&antenna_rows, &target.antennas),
            fields: through(&field_rows, &target.fields),
            spectral_windows: through(&window_rows, &target.spectral_windows),
        })
    }
}

/// What [`associate`] binds.
struct Kind {
    what: &'static str,
    /// Whether several entities may be stated to take one table row, as
    /// CASA's `spwmap` maps several windows to one; inferred bindings are
    /// one to one whatever this says.
    statements_share: bool,
}

impl Kind {
    fn one_to_one(what: &'static str) -> Self {
        Self {
            what,
            statements_share: false,
        }
    }
}

fn refused(reason: String) -> ObservationError {
    ObservationError::InvalidRequest { reason }
}

/// The table row of each target row of one kind: `stated` pairs (target
/// row, table row) first; then, where the namespaces have the same rows in
/// the same order (`same_content` and `same_name` row for row), each row's
/// own index; otherwise the one table row `same_content` matches, a row
/// stated one to one being no other's. No two target rows bind to one
/// table row unless both are stated and the kind lets statements share a
/// row.
fn associate<T>(
    kind: Kind,
    table: &[T],
    target: &[T],
    same_content: impl Fn(&T, &T) -> bool,
    same_name: impl Fn(&T, &T) -> bool,
    stated: &[(usize, u32)],
) -> Result<Vec<Option<u32>>, ObservationError> {
    let what = kind.what;
    let mut bound: Vec<Option<u32>> = vec![None; target.len()];
    let mut claimed = BTreeMap::new();
    for (target_row, row) in stated {
        if *row as usize >= table.len() || *target_row >= target.len() {
            return Err(refused(format!(
                "a stated association names {what} row {row}, which the table lacks"
            )));
        }
        if let Some(earlier) = claimed.insert(*row, *target_row)
            && !kind.statements_share
        {
            return Err(refused(format!(
                "{what}s {earlier} and {target_row} are both stated as table row {row}"
            )));
        }
        bound[*target_row] = Some(*row);
    }
    let own = table.len() == target.len()
        && table
            .iter()
            .zip(target)
            .all(|(a, b)| same_content(a, b) && same_name(a, b));
    let mut first = BTreeMap::new();
    for (target_row, entity) in target.iter().enumerate() {
        if bound[target_row].is_some() {
            continue;
        }
        let row = if own {
            Some(target_row)
        } else {
            // A row stated one to one is no other entity's.
            let mut candidates = (0..table.len())
                .filter(|row| kind.statements_share || !claimed.contains_key(&(*row as u32)))
                .filter(|row| same_content(&table[*row], entity));
            match (candidates.next(), candidates.next()) {
                (None, _) => None,
                (Some(row), None) => Some(row),
                (Some(a), Some(b)) => {
                    return Err(refused(format!(
                        "{what} row {target_row} of the store matches table rows {a} and {b} \
                         by content; state the association"
                    )));
                }
            }
        };
        let Some(row) = row.map(|row| row as u32) else {
            continue;
        };
        if let Some(other) = claimed.get(&row).or(first.get(&row)) {
            return Err(refused(format!(
                "{what} rows {other} and {target_row} of the store both bind to table row \
                 {row}, which does not keep them apart; state the association"
            )));
        }
        first.insert(row, target_row);
        bound[target_row] = Some(row);
    }
    Ok(bound)
}

fn same_antenna(a: &NamespaceAntenna, b: &NamespaceAntenna, tolerance: &BindingTolerance) -> bool {
    a.station == b.station
        && a.position_m
            .iter()
            .zip(&b.position_m)
            .all(|(x, y)| (x - y).abs() <= tolerance.position_m)
}

fn same_field(a: &NamespaceField, b: &NamespaceField, tolerance: &BindingTolerance) -> bool {
    let (al, ab) = a.phase_centre.as_angles();
    let (bl, bb) = b.phase_centre.as_angles();
    a.phase_centre.refer() == b.phase_centre.refer()
        && (al - bl).abs() <= tolerance.direction_rad
        && (ab - bb).abs() <= tolerance.direction_rad
}

/// Whether table window `solved` holds solutions for target window
/// `window`: the same frame and channels, bit for bit, or the one channel
/// a gain table collapses them to (the centre channel's frequency and the
/// summed width, as the writer collapses them).
fn solves(solved: &NamespaceWindow, window: &NamespaceWindow) -> bool {
    let bits = |values: &[f64]| values.iter().map(|v| v.to_bits()).collect::<Vec<_>>();
    if solved.frame != window.frame {
        return false;
    }
    if bits(&solved.frequencies_hz) == bits(&window.frequencies_hz)
        && bits(&solved.widths_hz) == bits(&window.widths_hz)
    {
        return true;
    }
    let channels = window.frequencies_hz.len();
    channels > 1
        && solved.frequencies_hz.len() == 1
        && solved.widths_hz.len() == 1
        && solved.frequencies_hz[0].to_bits() == window.frequencies_hz[channels / 2].to_bits()
        && solved.widths_hz[0].to_bits() == window.widths_hz.iter().copied().sum::<f64>().to_bits()
}

/// Write `rows`, a solve in `catalogue`'s numbering, as a CASA gain table
/// through `namespace`, the namespace of the store `catalogue` describes:
/// its subtables copied, and each row's antennas, field, spectral window
/// (the solve's partition) and session translated to their rows there.
/// Catalogue identities are never written.
pub(super) fn write_gain_table(
    namespace: &PersistedNamespace,
    catalogue: &Catalogue,
    request: &GainSolveRequest,
    refant: AntennaId,
    rows: &[SolutionRow],
) -> Result<GainSolveReport, GainSolveError> {
    let antenna = |id: i32| namespace.antennas[id as usize] as i32;
    let translated: Vec<SolutionRow> = rows
        .iter()
        .map(|row| {
            let partition = catalogue.partition(PartitionId(row.spw_id as u32));
            SolutionRow {
                antenna_id: antenna(row.antenna_id),
                refant_id: antenna(row.refant_id),
                field_id: namespace.fields[row.field_id as usize] as i32,
                spw_id: namespace.spectral_window(PartitionId(row.spw_id as u32)) as i32,
                observation_id: namespace.observations[partition.observation.0 as usize] as i32,
                ..row.clone()
            }
        })
        .collect();
    write_gain_caltable(
        &namespace.subtables,
        request,
        antenna(refant.0 as i32),
        &translated,
    )
}
