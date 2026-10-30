// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounds on what an MSv2's tables hold in memory, taken before they hold
//! it. An opened table holds its description and keywords; once a row is
//! read it loads every row, and keeps them while it is open. A bound comes
//! from a table's files and, once it is open, its rows and columns.

use std::fs;
use std::path::Path;

use casa_tables::Table;
use casa_types::RecordField;

use crate::{MeasurementSet, SubtableId};

/// Bytes allowed each table held open beside its description and
/// keywords: its storage managers' headers, its lock and its handles.
const TABLE_BYTES: usize = 16 << 10;

/// Times their file's size (`table.dat`) a table's description and
/// keywords hold once parsed into records.
const DESCRIPTION_FACTOR: usize = 8;

/// Bytes a loaded cell holds beside its strings' and arrays' contents: its
/// record field, the column's name copied into it, and an array's shape.
const CELL_BYTES: usize = size_of::<RecordField>() + 96;

/// Times their files' size the tables nested in a subtable (a field's
/// ephemerides) hold loaded, their cells unknown until they are opened.
const NESTED_FACTOR: usize = 64;

/// The subtables the opening's readers load: those its catalogue is read
/// from.
const READ: [SubtableId; 10] = [
    SubtableId::Antenna,
    SubtableId::DataDescription,
    SubtableId::Feed,
    SubtableId::Field,
    SubtableId::Observation,
    SubtableId::Polarization,
    SubtableId::Processor,
    SubtableId::Source,
    SubtableId::SpectralWindow,
    SubtableId::State,
];

/// What opening the MeasurementSet at `path` holds before a row is read:
/// each table's description and keywords (MAIN's, every subtable's, and
/// those nested in them: a field's ephemerides).
pub(crate) fn opening_bytes(path: &Path) -> usize {
    let mut bytes = 0;
    visit(path, 2, &mut |table| {
        let description = fs::metadata(table.join("table.dat")).map_or(0, |meta| meta.len());
        bytes += TABLE_BYTES + DESCRIPTION_FACTOR * description as usize;
    });
    bytes
}

/// What `table` holds once its rows are loaded: a record of each row's
/// cells ([`CELL_BYTES`]) and, at most twice, its storage files' contents
/// (the rows' values, and a column's values cached apart from them).
pub(crate) fn loaded_bytes(table: &Table) -> usize {
    let columns = table.schema().map_or(0, |schema| schema.columns().len());
    let files = table.path().map_or(0, storage_bytes);
    table
        .row_count()
        .saturating_mul(columns)
        .saturating_mul(CELL_BYTES)
        .saturating_add(2 * files)
}

/// What the readers of `ms`'s catalogue load: the rows of the subtables
/// they read ([`loaded_bytes`]) and of the tables nested in them.
pub(crate) fn read_bytes(ms: &MeasurementSet) -> usize {
    let nested = ms
        .subtable(SubtableId::Field)
        .and_then(Table::path)
        .map_or(0, |field| {
            let mut bytes = 0;
            visit(field, 1, &mut |table| {
                if table != field {
                    bytes += TABLE_BYTES + NESTED_FACTOR * storage_bytes(table);
                }
            });
            bytes
        });
    READ.iter()
        .filter_map(|id| ms.subtable(*id))
        .map(loaded_bytes)
        .sum::<usize>()
        + nested
}

/// Call `each` with every table directory at most `depth` levels below
/// `directory`, itself included.
fn visit(directory: &Path, depth: usize, each: &mut impl FnMut(&Path)) {
    if directory.join("table.dat").is_file() {
        each(directory);
    }
    if depth == 0 {
        return;
    }
    let Ok(entries) = fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            visit(&entry.path(), depth - 1, each);
        }
    }
}

/// The bytes a table directory's storage managers' files (`table.f*`)
/// hold on disk.
fn storage_bytes(directory: &Path) -> usize {
    let Ok(entries) = fs::read_dir(directory) else {
        return 0;
    };
    entries
        .flatten()
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("table.f"))
        .filter_map(|entry| entry.metadata().ok())
        .filter(fs::Metadata::is_file)
        .map(|meta| meta.len() as usize)
        .sum()
}
