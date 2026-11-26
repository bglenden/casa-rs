// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reconcile persisted MS subtables for the Python concatenation methods.

mod antenna;
mod auxiliary;
mod calibration;
mod field;
mod identity;
mod layout;
pub(crate) mod locking;
mod main_rows;
mod named_output;
mod publication;
mod scan;
mod source;
mod spectral;
mod virtual_rows;

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use casa_core::RecordValue;
use casa_tables::{Table, TableWriteLock};

use crate::{MeasurementSet, MsError, MsResult, SubtableId};

/// Matching tolerances and text used by the tool's HISTORY record.
pub struct TestConcatenation<'a> {
    /// Frequency tolerance, in hertz.
    pub frequency_hz: f64,
    /// Direction tolerance, in radians.
    pub direction_rad: f64,
    /// Merge fields only when their names also agree.
    pub respect_name: bool,
    /// Frequency quantity as rendered for CASA's HISTORY command.
    pub frequency_text: &'a str,
    /// Direction quantity as rendered for CASA's HISTORY command.
    pub direction_text: &'a str,
    /// Donor pathname supplied by the caller.
    pub donor_name: &'a str,
    /// The attached path shown by the tool, including any symbolic alias.
    pub attached_name: &'a Path,
}

/// Reconcile the metadata of two MeasurementSets and write the donor history.
///
/// The attached table retains its MAIN and POINTING rows. Both inputs are
/// locked while the subtable changes are prepared.
///
/// # Errors
/// Returns an invalid-input error for unsupported schemas, invalid fields or
/// tolerances, and reports failures to lock, write, or publish the subtables.
pub fn test_concatenate(
    target: &Path,
    donor: &Path,
    request: &TestConcatenation<'_>,
    retained: &mut BTreeMap<PathBuf, TableWriteLock>,
) -> MsResult<()> {
    publication::metadata_only(target, donor, request, retained)
}

/// Settings for a physical MeasurementSet concatenation.
pub struct PhysicalConcatenation<'a> {
    /// Metadata matching tolerances and HISTORY arguments.
    pub metadata: TestConcatenation<'a>,
    /// Scale donor weights and apply the reciprocal square root to sigmas.
    pub weight_scale: f32,
    /// CASA mode 0 through 3 controlling which of MAIN and POINTING are added.
    pub handling: i32,
    /// A separate table for remapped donor MAIN rows, if requested.
    pub output: Option<&'a Path>,
}

/// Reconcile metadata and append the donor's physical MAIN and POINTING.
///
/// A named output receives donor MAIN rows only, with copies of the
/// reconciled subtables. The attached MAIN is extended for an in-place call.
///
/// # Errors
/// The inputs cannot be matched, locked, staged, or published.
pub fn concatenate(
    target: &Path,
    donor: &Path,
    request: &PhysicalConcatenation<'_>,
    retained: &mut BTreeMap<PathBuf, TableWriteLock>,
) -> MsResult<()> {
    if !matches!(request.handling, 0..=3) {
        return Err(MsError::InvalidInput(
            "concatenation handling must be in 0..=3".into(),
        ));
    }
    if request.weight_scale < 0.0 {
        return Err(MsError::InvalidInput(
            "concatenation weight scale must be nonnegative".into(),
        ));
    }
    publication::physical(target, donor, request, retained)
}

/// Settings for preparing a donor MeasurementSet for virtual concatenation.
pub struct VirtualConcatenation<'a> {
    /// Metadata matching tolerances and HISTORY arguments.
    pub metadata: TestConcatenation<'a>,
    /// Scale the incoming visibility weights and sigmas.
    pub weight_scale: f32,
    /// Optional scan bookkeeping shared across successive calls.
    pub scan_file: Option<&'a Path>,
    /// Physical row numbers in an attached working selection, if any.
    pub attached_rows: Option<&'a [usize]>,
}

/// Reconcile attached metadata and rewrite the donor IDs and visibilities
/// for use in a subsequent concatenation of the two MeasurementSets.
///
/// # Errors
/// An input or auxiliary file is invalid, or an edit cannot be published.
pub fn virtual_concatenate(
    target: &Path,
    donor: &Path,
    request: &VirtualConcatenation<'_>,
    retained: &mut BTreeMap<PathBuf, TableWriteLock>,
) -> MsResult<()> {
    if request.weight_scale < 0.0 {
        return Err(MsError::InvalidInput(
            "concatenation weight scale must be nonnegative".into(),
        ));
    }
    publication::virtual_concat(target, donor, request, retained)
}

struct MetadataPlan<'a> {
    changes: Vec<SubtableChange>,
    row_map: MainRowMap,
    calibration: Vec<(&'a Table, TableRowsChange)>,
    ephemerides: Vec<(PathBuf, String)>,
    add_ephemeris_column: bool,
}

struct MainRowMap {
    antennas: Vec<usize>,
    descriptions: Vec<spectral::DonorDescription>,
    fields: Vec<i32>,
    states: Vec<usize>,
    attached_has_state: bool,
    donor_has_state: bool,
    observations: Vec<usize>,
    processors: Vec<usize>,
    attached_observations: Vec<usize>,
    attached_processors: Vec<usize>,
}

struct SubtableChange {
    table: SubtableId,
    rows: TableRowsChange,
}

#[derive(Default)]
struct TableRowsChange {
    appended: Vec<RecordValue>,
    updated: Vec<(usize, RecordValue)>,
    removed: Vec<usize>,
}

impl<'a> MetadataPlan<'a> {
    fn reconcile(
        target: &'a MeasurementSet,
        donor: &MeasurementSet,
        calibration_tables: [&'a BTreeMap<&str, Table>; 2],
        frequency_hz: f64,
        direction_rad: f64,
        respect_name: bool,
        coalesce_attached: bool,
    ) -> MsResult<Self> {
        let states = identity::states(
            required_table(target, SubtableId::State)?,
            required_table(donor, SubtableId::State)?,
        )?;
        let observations = identity::observations(
            required_table(target, SubtableId::Observation)?,
            required_table(donor, SubtableId::Observation)?,
            coalesce_attached,
        )?;
        let processors = identity::processors(
            required_table(target, SubtableId::Processor)?,
            required_table(donor, SubtableId::Processor)?,
            coalesce_attached,
        )?;
        let spectral = spectral::reconcile(target, donor, frequency_hz)?;
        let antennas = antenna::reconcile(target, donor, &spectral.window_ids)?;
        let mut sources = source::reconcile(target, donor, &spectral.window_ids)?;
        let fields = field::reconcile(target, donor, &sources.ids, direction_rad, respect_name)?;
        if fields.matched {
            sources.consolidate(target, &fields.additions)?;
        }
        let calibration = calibration::reconcile(
            target,
            donor,
            calibration_tables,
            &antennas.antenna_ids,
            &spectral.window_ids,
            !spectral.windows.is_empty(),
            [&observations.attached_ids, &observations.donor_ids],
        )?;
        let mut plan = Self {
            changes: Vec::new(),
            row_map: MainRowMap {
                antennas: antennas.antenna_ids,
                descriptions: spectral.donor_descriptions,
                fields: fields.ids,
                states: states.donor_ids,
                attached_has_state: required_table(target, SubtableId::State)?.row_count() > 0,
                donor_has_state: required_table(donor, SubtableId::State)?.row_count() > 0,
                observations: observations.donor_ids,
                processors: processors.donor_ids,
                attached_observations: observations.attached_ids,
                attached_processors: processors.attached_ids,
            },
            calibration,
            ephemerides: fields.ephemerides,
            add_ephemeris_column: fields.add_ephemeris_column,
        };
        plan.add(SubtableId::State, states.additions, Vec::new());
        plan.add(SubtableId::SpectralWindow, spectral.windows, Vec::new());
        plan.add(SubtableId::Polarization, spectral.polarizations, Vec::new());
        plan.add(
            SubtableId::DataDescription,
            spectral.descriptions,
            Vec::new(),
        );
        plan.add(SubtableId::Antenna, antennas.antennas, Vec::new());
        plan.add(SubtableId::Feed, antennas.feeds, antennas.feed_updates);
        if !sources.additions.is_empty()
            || !sources.updates.is_empty()
            || !sources.removed.is_empty()
        {
            plan.changes.push(SubtableChange {
                table: SubtableId::Source,
                rows: TableRowsChange {
                    appended: sources.additions,
                    updated: sources.updates,
                    removed: sources.removed,
                },
            });
        }
        plan.add(SubtableId::Field, fields.additions, Vec::new());
        for (table, appended, removed) in [
            (
                SubtableId::Observation,
                observations.additions,
                observations.removed,
            ),
            (
                SubtableId::Processor,
                processors.additions,
                processors.removed,
            ),
        ] {
            if !appended.is_empty() || !removed.is_empty() {
                plan.changes.push(SubtableChange {
                    table,
                    rows: TableRowsChange {
                        appended,
                        removed,
                        ..TableRowsChange::default()
                    },
                });
            }
        }
        Ok(plan)
    }

    fn add(
        &mut self,
        table: SubtableId,
        appended: Vec<RecordValue>,
        updated: Vec<(usize, RecordValue)>,
    ) {
        if !appended.is_empty()
            || !updated.is_empty()
            || (table == SubtableId::Field && self.add_ephemeris_column)
        {
            self.changes.push(SubtableChange {
                table,
                rows: TableRowsChange {
                    appended,
                    updated,
                    ..TableRowsChange::default()
                },
            });
        }
    }
}

fn required_table(ms: &MeasurementSet, table: SubtableId) -> MsResult<&Table> {
    ms.subtable(table)
        .ok_or_else(|| MsError::MissingSubtable(table.name().into()))
}

pub(super) fn join_with_tracing<A, B, RA, RB>(left: A, right: B) -> (RA, RB)
where
    A: FnOnce() -> RA + Send,
    B: FnOnce() -> RB + Send,
    RA: Send,
    RB: Send,
{
    let dispatch = tracing::dispatcher::get_default(Clone::clone);
    rayon::join(
        || tracing::dispatcher::with_default(&dispatch, left),
        || tracing::dispatcher::with_default(&dispatch, right),
    )
}
