// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reconcile persisted MS subtables for the Python concatenation methods.

mod antenna;
mod calibration;
mod field;
mod identity;
mod layout;
pub(crate) mod locking;
mod publication;
mod source;
mod spectral;

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use casa_tables::{Table, TableWriteLock};
use casa_types::RecordValue;

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

struct MetadataPlan<'a> {
    changes: Vec<SubtableChange>,
    calibration: Vec<(&'a Table, Vec<RecordValue>)>,
    ephemerides: Vec<(PathBuf, String)>,
    add_ephemeris_column: bool,
}

struct SubtableChange {
    table: SubtableId,
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
    ) -> MsResult<Self> {
        let legacy_identity =
            |table, reconcile: fn(&Table, &Table) -> MsResult<identity::IdentityRows>| {
                reconcile(
                    required_table(target, table)?,
                    required_table(donor, table)?,
                )
            };
        let states = legacy_identity(SubtableId::State, identity::states)?;
        let observations = legacy_identity(SubtableId::Observation, identity::observations)?;
        let processors = legacy_identity(SubtableId::Processor, identity::processors)?;
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
            &observations.donor_ids,
        )?;
        let mut plan = Self {
            changes: Vec::new(),
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
                appended: sources.additions,
                updated: sources.updates,
                removed: sources.removed,
            });
        }
        plan.add(SubtableId::Field, fields.additions, Vec::new());
        plan.add(SubtableId::Observation, observations.additions, Vec::new());
        plan.add(SubtableId::Processor, processors.additions, Vec::new());
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
                appended,
                updated,
                removed: Vec::new(),
            });
        }
    }
}

fn required_table(ms: &MeasurementSet, table: SubtableId) -> MsResult<&Table> {
    ms.subtable(table)
        .ok_or_else(|| MsError::MissingSubtable(table.name().into()))
}
