// SPDX-License-Identifier: LGPL-3.0-or-later
//! FIELD and spectral-window metadata on the current MS view.

use std::cell::{Ref, RefCell};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;

use casa_ms::subtables::{
    FieldDirectionColumn, MetadataDirection, MsDataDescription, MsField, MsPolarization,
    MsSpectralWindow, linked_subtable_row,
};
use casa_ms::{MeasurementSet, MsError, MsResult, SubtableId};
use casa_tables::{Table, TableOptions, TablePartNames};
use casa_types::ScalarValue;
use pyo3::prelude::*;

#[cfg(unix)]
use super::attachment_info::FileStamp;
use super::attachment_info::{TableFiles, TableStamp};
use super::{AttachedMs, PyMsTool, runtime_error};
use crate::convert::value::CasaValue;
use crate::tools::msmetadata::direction_record;
use crate::tools::{LogLine, emit};

#[derive(Default)]
pub(super) struct Subtables {
    antenna: Subtable,
    field: Subtable,
    data_description: Subtable,
    observation: Subtable,
    spectral_window: Subtable,
    polarization: Subtable,
}

pub(super) struct SpectralGridTables<'a> {
    pub(super) windows: Ref<'a, [Table]>,
    pub(super) fields: Ref<'a, [Table]>,
}

impl Subtables {
    pub(super) fn spectral_grid_tables(&self, main: &Table) -> MsResult<SpectralGridTables<'_>> {
        let windows = self.spectral_window.read_using(CellReads::Published, || {
            MeasurementSet::linked_subtable_paths(main, SubtableId::SpectralWindow)
        })?;
        let fields = self.field.read_using(CellReads::Published, || {
            MeasurementSet::linked_subtable_paths(main, SubtableId::Field)
        })?;
        Ok(SpectralGridTables { windows, fields })
    }

    pub(super) fn read_range(
        &self,
        main: &Table,
        ids: &[SubtableId],
    ) -> MsResult<HashMap<SubtableId, Ref<'_, [Table]>>> {
        self.read_tables(main, ids, CellReads::Fresh)
    }

    pub(super) fn spectral_grid_geometry(
        &self,
        main: &Table,
        ids: &[SubtableId],
    ) -> MsResult<HashMap<SubtableId, Ref<'_, [Table]>>> {
        self.read_tables(main, ids, CellReads::Published)
    }

    fn read_tables(
        &self,
        main: &Table,
        ids: &[SubtableId],
        cells: CellReads,
    ) -> MsResult<HashMap<SubtableId, Ref<'_, [Table]>>> {
        let mut links = None;
        ids.iter()
            .map(|&id| {
                let subtable = match id {
                    SubtableId::Antenna => &self.antenna,
                    SubtableId::DataDescription => &self.data_description,
                    SubtableId::Field => &self.field,
                    SubtableId::Observation => &self.observation,
                    SubtableId::Polarization => &self.polarization,
                    SubtableId::SpectralWindow => &self.spectral_window,
                    _ => return Err(MsError::MissingSubtable(id.name().into())),
                };
                let tables = subtable.read_using(cells, || {
                    if links.is_none() {
                        links = Some(MeasurementSet::linked_subtables(main, ids)?);
                    }
                    links
                        .as_mut()
                        .and_then(|links| links.remove(&id))
                        .ok_or_else(|| MsError::MissingSubtable(id.name().into()))
                })?;
                Ok((id, tables))
            })
            .collect()
    }
}

#[derive(Default)]
struct Subtable(RefCell<Option<SubtableSnapshot>>);

#[derive(Clone, Copy)]
enum CellReads {
    Descriptor,
    Fresh,
    Published,
}

struct SubtableSnapshot {
    tables: Vec<Table>,
    publication: Vec<(TableFiles, TableStamp)>,
    payloads: Option<Vec<SubtablePayload>>,
}

impl SubtableSnapshot {
    fn is_current(&mut self, reads: CellReads) -> bool {
        if self.publication.len() != self.tables.len()
            || self
                .publication
                .iter()
                .any(|(files, stamp)| files.stamp() != Some(*stamp))
        {
            return false;
        }
        match reads {
            CellReads::Descriptor => return true,
            CellReads::Fresh => {}
            CellReads::Published
                if self
                    .payloads
                    .as_ref()
                    .is_some_and(|payloads| payloads.iter().all(SubtablePayload::is_current)) =>
            {
                return true;
            }
            CellReads::Published => {}
        }
        if !self.tables.iter_mut().all(Table::try_clear_read_cells) {
            return false;
        }
        self.capture_payloads(reads);
        true
    }

    fn capture_payloads(&mut self, reads: CellReads) {
        self.payloads = if matches!(reads, CellReads::Published)
            && self.publication.len() == self.tables.len()
        {
            self.tables.iter().map(SubtablePayload::capture).collect()
        } else {
            None
        };
    }
}

#[cfg(unix)]
struct SubtablePayload(Vec<(PathBuf, FileStamp)>);
#[cfg(not(unix))]
struct SubtablePayload;

impl SubtablePayload {
    fn capture(table: &Table) -> Option<Self> {
        #[cfg(unix)]
        {
            // These managers keep the cell payloads in table.f* files in
            // the table directory, including indirect-array side files.
            if table.data_manager_info().iter().any(|manager| {
                !matches!(
                    manager.dm_type.as_str(),
                    "StandardStMan" | "IncrementalStMan"
                )
            }) {
                return None;
            }
            let root = table.path()?;
            let mut files = vec![(root.to_path_buf(), FileStamp::read(root).ok()?)];
            for entry in root.read_dir().ok()? {
                let entry = entry.ok()?;
                if entry.file_name().to_str()?.starts_with("table.f") {
                    let path = entry.path();
                    let stamp = FileStamp::read(&path).ok()?;
                    files.push((path, stamp));
                }
            }
            Some(Self(files))
        }
        #[cfg(not(unix))]
        {
            let _ = table;
            None
        }
    }

    fn is_current(&self) -> bool {
        #[cfg(unix)]
        {
            self.0
                .iter()
                .all(|(path, stamp)| FileStamp::read(path).ok() == Some(*stamp))
        }
        #[cfg(not(unix))]
        {
            false
        }
    }
}

impl Subtable {
    fn read(&self, main: &Table, id: SubtableId) -> MsResult<Ref<'_, [Table]>> {
        self.read_using(CellReads::Descriptor, || {
            MeasurementSet::linked_subtable_paths(main, id)
        })
    }

    fn read_using(
        &self,
        reads: CellReads,
        paths: impl FnOnce() -> MsResult<Vec<PathBuf>>,
    ) -> MsResult<Ref<'_, [Table]>> {
        let mut cached = self.0.borrow_mut();
        if !cached
            .as_mut()
            .is_some_and(|snapshot| snapshot.is_current(reads))
        {
            let mut snapshot = Self::open(paths()?)?;
            snapshot.capture_payloads(reads);
            *cached = Some(snapshot);
        }
        drop(cached);
        Ok(Ref::map(self.0.borrow(), |snapshot| {
            snapshot
                .as_ref()
                .expect("subtable was opened")
                .tables
                .as_slice()
        }))
    }

    fn open(paths: Vec<PathBuf>) -> MsResult<SubtableSnapshot> {
        let mut tables = Vec::with_capacity(paths.len());
        let mut publication = Vec::with_capacity(paths.len());
        for path in paths {
            let files = TableFiles::new(&path);
            let stamp = files.stamp();
            let table = Table::open(TableOptions::new(&path))?;
            // Reference, concatenated, or virtual columns may see changes
            // that do not touch this table's control and sync files.
            if let Some(stamp) = stamp
                && !table.is_memory()
                && table.schema().is_some_and(|schema| {
                    schema
                        .columns()
                        .iter()
                        .all(|col| !table.is_virtual_column(col.name()))
                })
                && matches!(
                    Table::immediate_part_names(&path, &path)?,
                    TablePartNames::Plain(_)
                )
            {
                publication.push((files, stamp));
            }
            tables.push(table);
        }
        Ok(SubtableSnapshot {
            tables,
            publication,
            payloads: None,
        })
    }
}

impl AttachedMs {
    fn field_direction(&self, column: &str, field: i64, time: f64) -> MsResult<MetadataDirection> {
        let column = match column.to_ascii_uppercase().as_str() {
            "DELAY_DIR" => FieldDirectionColumn::Delay,
            "PHASE_DIR" => FieldDirectionColumn::Phase,
            "REFERENCE_DIR" => FieldDirectionColumn::Reference,
            "EPHEMERIS_DIR" => FieldDirectionColumn::Ephemeris,
            _ => {
                return Err(MsError::InvalidInput(format!(
                    "Illegal FIELD direction column name: {column}"
                )));
            }
        };
        let field = usize::try_from(field)
            .map_err(|_| MsError::InvalidInput(format!("Invalid FIELD row {field}")))?;
        let tables = self.subtables.field.read(&self.main, SubtableId::Field)?;
        let (table, row) = linked_subtable_row(&tables, field, SubtableId::Field)?;
        MsField::new(table).metadata_direction_at_seconds(row, column, time)
    }

    fn spectral_window_info(&self) -> MsResult<CasaValue> {
        let dd = self
            .subtables
            .data_description
            .read(&self.main, SubtableId::DataDescription)?;
        let spw = self
            .subtables
            .spectral_window
            .read(&self.main, SubtableId::SpectralWindow)?;
        let pol = self
            .subtables
            .polarization
            .read(&self.main, SubtableId::Polarization)?;
        if dd.iter().all(|table| table.row_count() == 0)
            || spw.iter().all(|table| table.row_count() == 0)
        {
            return Ok(CasaValue::empty_record());
        }

        let mut records = Vec::new();
        for id in self.used_data_descriptions()? {
            let (table, dd_row) = linked_subtable_row(&dd, id, SubtableId::DataDescription)?;
            let description = MsDataDescription::new(table);
            let spw_id = nonnegative_id(
                description.spectral_window_id(dd_row)?,
                "SPECTRAL_WINDOW_ID",
            )?;
            let pol_id = nonnegative_id(description.polarization_id(dd_row)?, "POLARIZATION_ID")?;
            let (table, spw_row) = linked_subtable_row(&spw, spw_id, SubtableId::SpectralWindow)?;
            let window = MsSpectralWindow::new(table);
            let (table, pol_row) = linked_subtable_row(&pol, pol_id, SubtableId::Polarization)?;
            let polarization = MsPolarization::new(table);
            let chan_freq = window.chan_freq(spw_row)?;
            let chan_width = window.chan_width(spw_row)?;
            let (Some(&freq), Some(&width)) = (chan_freq.first(), chan_width.first()) else {
                return Err(MsError::InvalidInput(format!(
                    "Spectral window {spw_id} has no first channel frequency or width"
                )));
            };
            let record = CasaValue::record([
                ("SpectralWindowId", CasaValue::Int(spw_id as i64)),
                ("NumChan", CasaValue::Int(window.num_chan(spw_row)?.into())),
                (
                    "Frame",
                    CasaValue::Text(
                        window
                            .reference_frequency_reference(spw_row)?
                            .as_str()
                            .into(),
                    ),
                ),
                ("Chan1Freq", CasaValue::Float(freq)),
                ("ChanWidth", CasaValue::Float(width)),
                (
                    "TotalWidth",
                    CasaValue::Float(window.total_bandwidth(spw_row)?),
                ),
                ("RefFreq", CasaValue::Float(window.ref_frequency(spw_row)?)),
                ("PolId", CasaValue::Int(pol_id as i64)),
                (
                    "NumCorr",
                    CasaValue::Int(polarization.num_corr(pol_row)?.into()),
                ),
            ]);
            records.push((id.to_string(), record));
        }
        Ok(CasaValue::record(records))
    }

    fn used_data_descriptions(&self) -> MsResult<BTreeSet<usize>> {
        const ROWS_PER_BATCH: usize = 128 * 1024;
        let mut row_count = self.main.row_count();
        let refreshed;
        let main = if let Some(plain) = &self.plain_rows
            && plain.count(&self.path)? != row_count
        {
            // Selected column reads need a descriptor that admits new rows.
            refreshed = Table::open(TableOptions::new(&self.path))?;
            row_count = refreshed.row_count();
            &refreshed
        } else {
            &self.main
        };
        let column = main.column_accessor("DATA_DESC_ID")?;
        let selection = self.selected_rows.as_deref();
        let size = selection.map_or(row_count, <[usize]>::len);
        let mut row_numbers = Vec::with_capacity(size.min(ROWS_PER_BATCH));
        let mut ids = BTreeSet::new();
        for start in (0..size).step_by(ROWS_PER_BATCH) {
            let end = start.saturating_add(ROWS_PER_BATCH).min(size);
            let rows = match selection {
                Some(selection) => &selection[start..end],
                None => {
                    row_numbers.clear();
                    row_numbers.extend(start..end);
                    &row_numbers
                }
            };
            for value in column.scalar_cells_owned_for_rows(rows)? {
                let Some(ScalarValue::Int32(id)) = value else {
                    return Err(MsError::InvalidInput("Invalid MAIN.DATA_DESC_ID".into()));
                };
                ids.insert(nonnegative_id(id, "DATA_DESC_ID")?);
            }
        }
        Ok(ids)
    }
}

fn nonnegative_id(id: i32, name: &str) -> MsResult<usize> {
    usize::try_from(id).map_err(|_| MsError::InvalidInput(format!("Invalid {name} {id}")))
}

#[pymethods]
impl PyMsTool {
    #[pyo3(signature = (dircolname = "PHASE_DIR", fieldid = 0, time = 0.0, format = "measure"), text_signature = "($self, dircolname='PHASE_DIR', fieldid=0, time=0.0, format='measure')")]
    fn getfielddirmeas(
        &self,
        py: Python<'_>,
        dircolname: &str,
        fieldid: i64,
        time: f64,
        format: &str,
    ) -> PyResult<Py<PyAny>> {
        let direction = self.attachment_info(py, |attached| {
            Ok(attached.map(|ms| ms.field_direction(dircolname, fieldid, time)))
        })?;
        let value = match direction {
            Some(Ok(direction)) if format.starts_with(['s', 'S']) => {
                let [x, y, z] = direction.direction.cosines().map(casacore_direction_number);
                CasaValue::Text(format!("Direction: [{x}, {y}, {z}]"))
            }
            Some(Ok(direction)) => direction_record(direction),
            Some(Err(error)) => {
                emit(py, "casars.tools.ms", &[LogLine::error(error.to_string())])?;
                empty_direction()
            }
            None => empty_direction(),
        };
        value.into_python(py)
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn getspectralwindowinfo(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.attachment_info(py, |attached| match attached {
            Some(ms) => ms.spectral_window_info().map_err(runtime_error),
            None => Ok(CasaValue::empty_record()),
        })?
        .into_python(py)
    }
}

fn empty_direction() -> CasaValue {
    CasaValue::Bools {
        shape: vec![0],
        values: Vec::new(),
    }
}

// casacore prints direction cosines with default C++ stream formatting,
// which chooses fixed or scientific notation at six significant digits.
fn casacore_direction_number(value: f64) -> String {
    let scientific = format!("{value:.5e}");
    let Some((digits, exponent)) = scientific.split_once('e') else {
        return scientific.to_ascii_lowercase();
    };
    let exponent = exponent
        .parse::<i32>()
        .expect("Rust's finite float exponent");
    if (-4..6).contains(&exponent) {
        let precision = (5 - exponent) as usize;
        let fixed = format!("{value:.precision$}");
        if fixed.contains('.') {
            return fixed.trim_end_matches('0').trim_end_matches('.').to_owned();
        }
        return fixed;
    }
    let digits = digits.trim_end_matches('0').trim_end_matches('.');
    format!("{digits}e{exponent:+03}")
}
