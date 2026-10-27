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
//! 2. **CASA's own association** where the caller states that the table was
//!    written through the target's namespace (as CASA assumes of a table
//!    applied to the MeasurementSet it was solved on): each row binds to its
//!    own index, even where two rows hold the same content, the rows checked
//!    consistent. Names, row counts and a gain table's collapsed channels do
//!    not establish it: two windows of other channels can collapse alike.
//! 3. **Content** otherwise: an antenna by station and position, a field
//!    by phase centre, a spectral window by its frame and channels (or the
//!    one channel a gain table collapses them to). Names never bind, since
//!    converters rewrite them, but an antenna whose name disagrees beyond a
//!    converter's `_STATION` suffix is refused: another antenna on the same
//!    pad does not take its gains. An entity the other side lacks binds to
//!    nothing (a subset of antennas), except a window some table window
//!    overlaps, which is refused; and two candidates are refused, never
//!    resolved by name or position, since equal content alone does not
//!    prove two rows are one entity.
//!
//! The calibration port moves this into production; it sees casa-ms only
//! through the namespace and the gain table writer.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use casa_ms::observation_io::{
    NamespaceAntenna, NamespaceField, NamespaceRows, NamespaceWindow, PersistedNamespace,
};
use casa_observation::{AntennaId, BindingTolerance, Catalogue, ObservationError, PartitionId};

use crate::solve::kernel::SolutionRow;
use crate::solve::writer::write_gain_caltable_named;
use crate::solve::{GainSolveReport, GainSolveRequest};

/// Associations a caller states, which binding takes before anything it
/// could infer: CASA's `spwmap` and its like for antennas, and CASA's own
/// association of a table with the store it was solved on.
#[derive(Clone, Debug, Default)]
pub(super) struct StatedAssociations {
    /// The caller states that the table was written through the target's
    /// own namespace, as CASA assumes of a table applied to the
    /// MeasurementSet it was solved on: each row then binds to its own
    /// index, even where rows hold equal content. Checked, not trusted
    /// blindly: the namespaces must have as many rows of each kind, each
    /// consistent with its counterpart. Nothing else, names and row counts
    /// included, establishes it.
    pub(super) same_namespace: bool,
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
    /// namespace is `target`, for an applier that opened the store as
    /// `catalogue` describes it.
    ///
    /// # Errors
    ///
    /// When `catalogue` is not the opening `target` describes; when the
    /// stated same namespace has other rows than the table's; when a stated
    /// association names a row or entity that does not exist,
    /// two antennas for one row or two rows for one antenna; when an entity
    /// matches more than one row of the other side by content, or two
    /// entities one row; when an antenna's station and position match but
    /// its name does not; or when a partition's window binds to no table
    /// window though one overlaps it.
    pub(super) fn bind(
        table: &NamespaceRows,
        target: &PersistedNamespace,
        catalogue: &Catalogue,
        tolerance: &BindingTolerance,
        stated: &StatedAssociations,
    ) -> Result<Self, ObservationError> {
        if !target.describes(catalogue) {
            return Err(refused(
                "the applier's catalogue is not the one the namespace describes: open the store \
                 with the namespace's options"
                    .to_string(),
            ));
        }
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
            Rules {
                content: |a: &NamespaceAntenna, b: &NamespaceAntenna| same_antenna(a, b, tolerance),
                consistent: |a: &NamespaceAntenna, b: &NamespaceAntenna| {
                    same_antenna(a, b, tolerance) && names_agree(a, b)
                },
                agree: names_agree,
            },
            &stated_antennas,
            stated.same_namespace,
        )?;
        let field_rows = associate(
            Kind::one_to_one("field"),
            &table.fields,
            &target.rows.fields,
            Rules {
                content: |a: &NamespaceField, b: &NamespaceField| same_field(a, b, tolerance),
                consistent: |a: &NamespaceField, b: &NamespaceField| same_field(a, b, tolerance),
                agree: |_: &NamespaceField, _: &NamespaceField| true,
            },
            &[],
            stated.same_namespace,
        )?;
        let window_rows = associate(
            Kind {
                what: "spectral window",
                statements_share: true,
            },
            &table.spectral_windows,
            &target.rows.spectral_windows,
            Rules {
                content: solves,
                // In its own namespace a window may hold a solve over part
                // of its channels (`spw='0:5~58'`), collapsed within its band.
                consistent: |solved: &NamespaceWindow, window: &NamespaceWindow| {
                    solves(solved, window) || within(solved, window)
                },
                agree: |_: &NamespaceWindow, _: &NamespaceWindow| true,
            },
            &stated_windows,
            stated.same_namespace,
        )?;
        // A window no table window solves, though one bound to no other
        // window overlaps it, has solutions the binding cannot place:
        // refused, not left unbound. Frames are not compared here: a table
        // solved in one frame and applied to the store regridded to another
        // (`cvel`) solves none of its windows, and must be stated, not
        // flagged whole.
        let used: BTreeSet<u32> = window_rows.iter().flatten().copied().collect();
        for &row in &target.spectral_windows {
            let window = &target.rows.spectral_windows[row as usize];
            if window_rows[row as usize].is_none()
                && let Some(other) = (0..table.spectral_windows.len()).find(|other| {
                    !used.contains(&(*other as u32))
                        && overlaps(&table.spectral_windows[*other], window)
                })
            {
                return Err(refused(format!(
                    "spectral window row {row} of the store binds to no table window, but table \
                     window {other} overlaps it in frequency; state the association"
                )));
            }
        }
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

/// How [`associate`] compares a table row with a target row.
struct Rules<C, S, A> {
    /// Whether they hold the same entity's content.
    content: C,
    /// Whether a row of a stated same namespace is consistent with its
    /// counterpart.
    consistent: S,
    /// Whether a content match may stand: false refuses it.
    agree: A,
}

fn refused(reason: String) -> ObservationError {
    ObservationError::InvalidRequest { reason }
}

/// The table row of each target row of one kind: `stated` pairs (target
/// row, table row) first; then, where the caller states the namespaces are
/// one (`same_namespace`), each row's own index, the rows checked
/// consistent; otherwise the one table row whose content matches, a row
/// stated one to one being no other's, provided the rules' `agree` lets it
/// stand. Two target rows never bind to one table row by inference; where
/// the kind lets statements share a row, a stated row may share with any
/// other, as CASA's `spwmap` maps an unlisted window to its own row.
fn associate<T>(
    kind: Kind,
    table: &[T],
    target: &[T],
    rules: Rules<impl Fn(&T, &T) -> bool, impl Fn(&T, &T) -> bool, impl Fn(&T, &T) -> bool>,
    stated: &[(usize, u32)],
    same_namespace: bool,
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
        if bound[*target_row].is_some_and(|earlier| earlier != *row) {
            return Err(refused(format!(
                "{what} {target_row} is stated as two table rows"
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
    if same_namespace && table.len() != target.len() {
        return Err(refused(format!(
            "the store is stated to be the table's namespace, but has {} {what} rows to the \
             table's {}",
            target.len(),
            table.len()
        )));
    }
    let mut first = BTreeMap::new();
    for (target_row, entity) in target.iter().enumerate() {
        if bound[target_row].is_some() {
            continue;
        }
        let row = if same_namespace {
            if !(rules.consistent)(&table[target_row], entity) {
                return Err(refused(format!(
                    "the store is stated to be the table's namespace, but its {what} row \
                     {target_row} is not the table's"
                )));
            }
            Some(target_row)
        } else {
            // A row stated one to one is no other entity's.
            let mut candidates = (0..table.len())
                .filter(|row| kind.statements_share || !claimed.contains_key(&(*row as u32)))
                .filter(|row| (rules.content)(&table[*row], entity));
            match (candidates.next(), candidates.next()) {
                (None, _) => None,
                (Some(row), None) if (rules.agree)(&table[row], entity) => Some(row),
                (Some(row), None) => {
                    return Err(refused(format!(
                        "{what} row {target_row} of the store matches table row {row} by \
                         content but not by name; state the association"
                    )));
                }
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
        let stated_by = (!kind.statements_share)
            .then(|| claimed.get(&row))
            .flatten();
        if let Some(other) = stated_by.or(first.get(&row)) {
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

/// Whether two antennas' names agree: the same once each loses the
/// `_STATION` suffixes a converter appends (XRADIO names an MSv2 antenna
/// `NAME_STATION`, and a round trip appends it again).
fn names_agree(a: &NamespaceAntenna, b: &NamespaceAntenna) -> bool {
    fn bare<'n>(mut name: &'n str, station: &str) -> &'n str {
        while let Some(stripped) = name
            .strip_suffix(station)
            .and_then(|rest| rest.strip_suffix('_'))
        {
            name = stripped;
        }
        name
    }
    bare(&a.name, &a.station) == bare(&b.name, &b.station)
}

fn same_field(a: &NamespaceField, b: &NamespaceField, tolerance: &BindingTolerance) -> bool {
    let (al, ab) = a.phase_centre.as_angles();
    let (bl, bb) = b.phase_centre.as_angles();
    a.phase_centre.refer() == b.phase_centre.refer()
        && (al - bl).abs() <= tolerance.direction_rad
        && (ab - bb).abs() <= tolerance.direction_rad
}

/// Whether two frequencies, or widths, are one: equal to a part in
/// 10^12, far below any channel's width, so that a mean CASA summed in
/// another order still matches.
fn same_hz(a: f64, b: f64) -> bool {
    (a - b).abs() <= 1e-12 * a.abs().max(b.abs())
}

/// Whether table window `solved` holds solutions for target window
/// `window`: the same frame, and the same channels, or the one channel a
/// gain table collapses them to, of their summed width at their arithmetic
/// mean frequency (CASA's `gaincal`: its `ChannelAverageTVI` averages the
/// frequencies plainly and sums the widths before a G or T solve) or at
/// the centre channel's (casa-rs's gain table writer, which differs from
/// CASA there, #917).
fn solves(solved: &NamespaceWindow, window: &NamespaceWindow) -> bool {
    let same =
        |a: &[f64], b: &[f64]| a.len() == b.len() && a.iter().zip(b).all(|(a, b)| same_hz(*a, *b));
    if solved.frame != window.frame {
        return false;
    }
    if same(&solved.frequencies_hz, &window.frequencies_hz)
        && same(&solved.widths_hz, &window.widths_hz)
    {
        return true;
    }
    let channels = window.frequencies_hz.len();
    if channels < 2 || solved.frequencies_hz.len() != 1 || solved.widths_hz.len() != 1 {
        return false;
    }
    let frequency = solved.frequencies_hz[0];
    // `PlainChannelAverageKernel`: unweighted, whatever the widths.
    let mean = window.frequencies_hz.iter().sum::<f64>() / channels as f64;
    same_hz(solved.widths_hz[0], window.widths_hz.iter().sum())
        && (same_hz(frequency, mean) || same_hz(frequency, window.frequencies_hz[channels / 2]))
}

/// Whether `solved`, one channel, lies within `window`'s band in its frame:
/// a solve over part of the window's channels. An edge the collapse
/// reaches is compared as [`same_hz`] compares, since its mean and summed
/// width put it an ulp either side.
fn within(solved: &NamespaceWindow, window: &NamespaceWindow) -> bool {
    let at_most = |a: f64, b: f64| a <= b || same_hz(a, b);
    let (low, high) = band(window);
    let (solved_low, solved_high) = band(solved);
    solved.frame == window.frame
        && solved.frequencies_hz.len() == 1
        && at_most(low, solved_low)
        && at_most(solved_high, high)
}

/// A window's band: each channel spans its frequency plus or minus half its
/// width.
fn band(window: &NamespaceWindow) -> (f64, f64) {
    window
        .frequencies_hz
        .iter()
        .zip(&window.widths_hz)
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(low, high), (f, w)| {
            (low.min(f - w.abs() / 2.0), high.max(f + w.abs() / 2.0))
        })
}

/// Whether two windows' bands overlap.
fn overlaps(a: &NamespaceWindow, b: &NamespaceWindow) -> bool {
    let ((a_low, a_high), (b_low, b_high)) = (band(a), band(b));
    a_low < b_high && b_low < a_high
}

/// Write `rows`, a solve in `catalogue`'s numbering, as a CASA gain table
/// through `namespace`, the namespace of the store `catalogue` describes:
/// its subtables copied, each row's antennas, field, spectral window (the
/// solve's partition) and session translated to their rows there, and the
/// store's path recorded as the table's `MSName`. Catalogue identities are
/// never written. A table written through an MSv4's namespace applies by
/// CASA's own association only to that store's MSv2 conversion; to any
/// other store it is bound.
///
/// # Errors
///
/// When `catalogue` does not number the store as the namespace's opening
/// does ([`PersistedNamespace::describes`]), or the table cannot be
/// written.
pub(super) fn write_gain_table(
    namespace: &PersistedNamespace,
    catalogue: &Catalogue,
    request: &GainSolveRequest,
    refant: AntennaId,
    rows: &[SolutionRow],
) -> Result<GainSolveReport, ObservationError> {
    if !namespace.describes(catalogue) {
        return Err(refused(
            "the solve's catalogue is not the one the namespace describes: open the store \
             with the namespace's options"
                .to_string(),
        ));
    }
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
    write_gain_caltable_named(
        &namespace.subtables,
        name(&namespace.source),
        request,
        antenna(refant.0 as i32),
        &translated,
    )
    .map_err(|error| ObservationError::Storage(Box::new(error)))
}

fn name(source: &Path) -> String {
    source.display().to_string()
}
