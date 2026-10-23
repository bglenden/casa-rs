// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `cl` tool's state and the methods casa-rs implements, without
//! Python: CASA's `componentlist` over a [`ComponentList`].
//!
//! The list is attached to a table by `open` or `rename`, as CASA's is:
//! - `rename` of a list not attached creates the table and writes the list
//!   at once (CASA's `createTable`), refusing a table that exists; of an
//!   attached list it moves the table, whose contents are written later;
//! - an attached list opened for update (`open(nomodify=False)`) or renamed
//!   is rewritten when it is released: by `close`, `done`, an `open` that
//!   replaces it, or the tool's deletion (CASA's `ComponentList` destructor),
//!   even when nothing changed;
//! - a list opened read-only (`nomodify=True`) is never written, though it
//!   can be changed in memory.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_components::{ComponentList, ComponentListError, SkyComponent};
use casa_types::measures::MeasuresProvider;
use casa_types::{RecordValue, Value};

use super::add::AddComponent;
use crate::convert::value::CasaValue;
use crate::convert::{ConvertError, ConvertResult};
use crate::tools::LogLine;

/// The table a list is attached to.
#[derive(Debug, Clone)]
struct Attached {
    path: PathBuf,
    /// Whether releasing the list rewrites the table (`open` with
    /// `nomodify=False`, or `rename`).
    writable: bool,
}

/// The tool's state: the list, the table it is attached to, and the
/// measures data (for directions named by a source), loaded on first use.
#[derive(Default)]
pub(crate) struct State {
    list: ComponentList,
    table: Option<Attached>,
    measures: Option<Arc<dyn MeasuresProvider>>,
}

impl State {
    /// The measures data, loaded on first use.
    fn measures(&mut self) -> ConvertResult<Arc<dyn MeasuresProvider>> {
        if let Some(measures) = &self.measures {
            return Ok(Arc::clone(measures));
        }
        let measures = casa_ms::open_measures_runtime()
            .map_err(|error| ConvertError::runtime(format!("measures data: {error}")))?;
        self.measures = Some(Arc::clone(&measures));
        Ok(measures)
    }

    /// Release the list as CASA's `ComponentList` destructor does: rewrite
    /// its table when it is attached for update, then leave an empty list
    /// attached to nothing.
    ///
    /// # Errors
    ///
    /// The table could not be rewritten; the state is reset all the same.
    pub(crate) fn release(&mut self) -> Result<(), String> {
        let list = std::mem::take(&mut self.list);
        match self.table.take() {
            Some(Attached {
                path,
                writable: true,
            }) => list.rewrite_table(&path).map_err(|error| {
                format!("componentlist could not write {}: {error}", path.display())
            }),
            _ => Ok(()),
        }
    }

    /// `open(filename, nomodify)`.
    pub(crate) fn open(&mut self, filename: &str, nomodify: bool) -> ConvertResult<bool> {
        let not_a_list = || {
            ConvertError::runtime(format!(
                "{filename} is non existant or is not a componentlist table"
            ))
        };
        let path = Path::new(filename);
        if filename.is_empty() || !path.join("table.dat").is_file() {
            return Err(not_a_list());
        }
        let list = ComponentList::read_table(path).map_err(|error| match error {
            ComponentListError::Table { .. } | ComponentListError::MissingColumn { .. } => {
                not_a_list()
            }
            other => ConvertError::runtime(other.to_string()),
        })?;
        self.release().map_err(ConvertError::runtime)?;
        self.list = list;
        self.table = Some(Attached {
            path: path.to_path_buf(),
            writable: !nomodify,
        });
        Ok(true)
    }

    /// `rename(filename)`.
    pub(crate) fn rename(&mut self, filename: &str) -> ConvertResult<bool> {
        let path = PathBuf::from(filename);
        match &self.table {
            _ if filename.is_empty() && self.table.is_none() => Ok(true),
            Some(_) if filename.is_empty() => Err(ConvertError::runtime(
                "componentlist.rename('') of a list attached to a table (which CASA deletes) \
                 is not yet implemented in casa-rs",
            )),
            Some(Attached {
                writable: false, ..
            }) => Err(ConvertError::runtime(
                "Failed AlwaysAssert itsROFlag == false",
            )),
            _ if path.exists() => Err(ConvertError::runtime(format!(
                "Table {filename} already exists"
            ))),
            Some(Attached {
                path: current,
                writable: true,
            }) => {
                std::fs::rename(current, &path).map_err(|error| {
                    ConvertError::runtime(format!(
                        "cannot rename {} to {filename}: {error}",
                        current.display()
                    ))
                })?;
                self.table = Some(Attached {
                    path,
                    writable: true,
                });
                Ok(true)
            }
            None => {
                self.list
                    .write_table(&path)
                    .map_err(|error| ConvertError::runtime(error.to_string()))?;
                self.table = Some(Attached {
                    path,
                    writable: true,
                });
                Ok(true)
            }
        }
    }

    /// `done()`: release the list and start an empty one.
    pub(crate) fn done(&mut self) -> ConvertResult<bool> {
        self.release().map_err(ConvertError::runtime)?;
        Ok(true)
    }

    /// `length()`.
    pub(crate) fn length(&self) -> i64 {
        i64::try_from(self.list.len()).unwrap_or(i64::MAX)
    }

    /// `addcomponent(...)`: the component is added only when every setter
    /// that raises has succeeded.
    pub(crate) fn addcomponent(
        &mut self,
        request: &AddComponent,
        lines: &mut Vec<LogLine>,
    ) -> ConvertResult<bool> {
        let measures = self.measures()?;
        let component = request.component(&measures, lines)?;
        self.list.push(component);
        Ok(true)
    }

    /// `torecord()`: the list's record, or `{}` with CASA's warning when it
    /// is empty.
    pub(crate) fn torecord(&self, lines: &mut Vec<LogLine>) -> ConvertResult<CasaValue> {
        if self.list.is_empty() {
            lines.push(LogLine::warning("componentlist is empty"));
            return Ok(CasaValue::empty_record());
        }
        record(self.list.to_record())
    }

    /// The component `which`, checked as CASA's `_checkIndex` checks it.
    pub(crate) fn checked(&self, which: i64) -> ConvertResult<&SkyComponent> {
        usize::try_from(which)
            .ok()
            .and_then(|index| self.list.get(index))
            .ok_or_else(|| {
                ConvertError::runtime(format!(
                    "Index out of range. The component number must be between 0 and {}",
                    self.length() - 1
                ))
            })
    }

    /// The component `which`, for the methods CASA does not check: casacore's
    /// `ComponentList::component` asserts the index.
    pub(crate) fn unchecked(&self, which: i64) -> ConvertResult<&SkyComponent> {
        usize::try_from(which)
            .ok()
            .and_then(|index| self.list.get(index))
            .ok_or_else(|| ConvertError::runtime("Failed AlwaysAssert index < nelements()"))
    }

    /// `getrefdir(which)`: the reference direction's `MeasureHolder` record.
    pub(crate) fn getrefdir(&self, which: i64) -> ConvertResult<CasaValue> {
        let shape = self.checked(which)?.shape().to_record();
        match shape.get("direction") {
            Some(Value::Record(direction)) => Ok(CasaValue::from_record(&RecordValue::new(
                direction
                    .fields()
                    .iter()
                    .filter(|field| field.name != "error")
                    .cloned()
                    .collect(),
            ))),
            _ => unreachable!("a shape's record holds its direction"),
        }
    }
}

/// A record for Python, or the error CASA reports when it cannot form one.
pub(crate) fn record(
    record: Result<RecordValue, casa_components::ComponentError>,
) -> ConvertResult<CasaValue> {
    record
        .map(|record| CasaValue::from_record(&record))
        .map_err(|error| ConvertError::runtime(error.to_string()))
}
