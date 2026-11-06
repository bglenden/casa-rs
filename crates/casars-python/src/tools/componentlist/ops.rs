// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `cl` tool's state and the methods casa-rs implements, without
//! Python: CASA's `componentlist` over a [`ComponentList`].
//!
//! The list is attached to a table by `open` or `rename`, as CASA's is:
//! - the table is named by its absolute path, resolved against the working
//!   directory when the list is attached (casacore's `Path::absoluteName`),
//!   so a later change of directory does not move it;
//! - `rename` of a list not attached creates the table and writes the list
//!   at once (CASA's `createTable`), refusing a table that exists; of an
//!   attached list it moves the table, whose contents are written later;
//! - an attached list opened for update (`open(nomodify=False)`) or renamed
//!   is rewritten when it is released: by `close`, `done`, an `open` that
//!   replaces it (before the replacement is read, which may be the same
//!   table), or the tool's deletion (CASA's `ComponentList` destructor), even
//!   when nothing changed. The rewrite waits for a write lock another
//!   process holds on the table, as casacore's does
//!   ([`ComponentList::rewrite_table`]);
//! - a list opened read-only (`nomodify=True`) is never written, though it
//!   can be changed in memory.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_components::{ComponentList, ComponentListError, Flux, SkyComponent};
use casa_tables::{Table, TableOptions};
use casa_types::measures::MeasuresProvider;
use casa_types::quanta::Unit;
use casa_types::{RecordValue, Value};

use super::add::AddComponent;
use crate::convert::value::CasaValue;
use crate::convert::{ConvertError, ConvertResult, casacore_unit, quantity};
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
    selection: std::collections::BTreeSet<i64>,
    shape_units: BTreeMap<usize, [Unit; 3]>,
    /// Removed components in request order; recovery appends them in reverse.
    removed: Vec<(SkyComponent, Option<[Unit; 3]>)>,
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
        self.selection.clear();
        self.shape_units.clear();
        self.removed.clear();
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

    /// `open(filename, nomodify)`: as CASA's, the table is checked to be a
    /// component list, then the list it replaces is released (and written),
    /// and only then is the table read, so that reopening the list's own
    /// table reads what the release wrote.
    pub(crate) fn open(&mut self, filename: &str, nomodify: bool) -> ConvertResult<bool> {
        let not_a_list = || {
            ConvertError::runtime(format!(
                "{filename} is non existant or is not a componentlist table"
            ))
        };
        if filename.is_empty() {
            return Err(not_a_list());
        }
        let path = absolute(filename)?;
        if !is_component_list(&path) {
            return Err(not_a_list());
        }
        self.release().map_err(ConvertError::runtime)?;
        self.list = ComponentList::read_table(&path).map_err(|error| match error {
            ComponentListError::Table { .. } | ComponentListError::MissingColumn { .. } => {
                not_a_list()
            }
            other => ConvertError::runtime(other.to_string()),
        })?;
        self.table = Some(Attached {
            path,
            writable: !nomodify,
        });
        Ok(true)
    }

    /// `rename(filename)`.
    pub(crate) fn rename(&mut self, filename: &str) -> ConvertResult<bool> {
        match &self.table {
            None if filename.is_empty() => return Ok(true),
            Some(_) if filename.is_empty() => {
                return Err(ConvertError::runtime(
                    "componentlist.rename('') of a list attached to a table (which CASA \
                     deletes) is not yet implemented in casa-rs",
                ));
            }
            Some(Attached {
                writable: false, ..
            }) => {
                return Err(ConvertError::runtime(
                    "Failed AlwaysAssert itsROFlag == false",
                ));
            }
            _ => {}
        }
        let path = absolute(filename)?;
        if path.exists() {
            return Err(ConvertError::runtime(format!(
                "Table {filename} already exists"
            )));
        }
        match &self.table {
            Some(Attached { path: current, .. }) => {
                std::fs::rename(current, &path).map_err(|error| {
                    ConvertError::runtime(format!(
                        "cannot rename {} to {filename}: {error}",
                        current.display()
                    ))
                })?;
            }
            None => self
                .list
                .write_table(&path)
                .map_err(|error| ConvertError::runtime(error.to_string()))?,
        }
        self.table = Some(Attached {
            path,
            writable: true,
        });
        Ok(true)
    }

    /// `done()`: release the list and start an empty one.
    pub(crate) fn done(&mut self) -> ConvertResult<bool> {
        self.release().map_err(ConvertError::runtime)?;
        Ok(true)
    }

    /// Check the selected components using CASA's Stokes physicality test.
    pub(crate) fn is_physical(
        &self,
        indices: &[i64],
        lines: &mut Vec<LogLine>,
    ) -> ConvertResult<CasaValue> {
        if indices == [-1] && self.list.is_empty() {
            return Err(ConvertError::runtime(
                "Cannot check all components of an empty list (CASA exits: #1158)",
            ));
        }
        let indices = if indices == [-1] {
            (0..self.length()).collect::<Vec<_>>()
        } else {
            indices.to_vec()
        };
        if indices
            .iter()
            .any(|&index| index < 0 || index >= self.length())
        {
            let message = "Index out of range.\nA component number is less than one or greater than the list length\nNot checking any components";
            lines.push(LogLine::error(message));
            return Err(ConvertError::runtime(message));
        }
        Ok(CasaValue::Bool(indices.into_iter().all(|index| {
            self.list
                .get(index as usize)
                .is_some_and(|component| component.flux().is_physical())
        })))
    }

    /// Change selection only after validating the complete request.
    pub(crate) fn set_selection(
        &mut self,
        indices: &[i64],
        select: bool,
        lines: &mut Vec<LogLine>,
    ) -> ConvertResult<CasaValue> {
        if indices == [-1] {
            if select {
                self.selection = (0..self.length()).collect();
            } else {
                self.selection.clear();
            }
        } else {
            if indices.iter().any(|&i| i < 0 || i >= self.length()) {
                let action = if select { "selected" } else { "deselected" };
                let message = format!(
                    "Index out of range.\nA component number is less than one or greater than the list length\nNo components {action}"
                );
                lines.push(LogLine::error(message.clone()));
                return Err(ConvertError::runtime(message));
            }
            for &index in indices {
                if select {
                    self.selection.insert(index);
                } else {
                    self.selection.remove(&index);
                }
            }
        }
        Ok(CasaValue::Bool(true))
    }

    /// The selected indices in CASA's integer-array representation.
    pub(crate) fn selected(&self) -> CasaValue {
        CasaValue::Ints {
            shape: vec![self.selection.len()],
            values: self.selection.iter().copied().collect(),
        }
    }

    /// Remove components after validating all indices, keeping a recovery bin.
    pub(crate) fn remove(
        &mut self,
        indices: &[i64],
        lines: &mut Vec<LogLine>,
    ) -> ConvertResult<bool> {
        let requested: Vec<usize> = if indices == [-1] {
            if self.list.is_empty() {
                return Err(ConvertError::runtime(
                    "componentlist.remove on an empty list would crash CASA (#1053)",
                ));
            }
            (0..self.list.len()).collect()
        } else {
            if indices
                .iter()
                .any(|&index| index < 0 || index >= self.length())
            {
                let message = "Index out of range.\nA component number is less than one or greater than the list length\nNo components removed";
                lines.push(LogLine::error(message));
                return Err(ConvertError::runtime(message));
            }
            indices.iter().map(|&index| index as usize).collect()
        };
        let mut removed = std::collections::BTreeSet::new();
        for index in requested {
            // CASA loses a different component when the same index is repeated.
            if removed.insert(index) {
                self.removed.push((
                    self.list.get(index).expect("validated index").clone(),
                    self.shape_units.get(&index).cloned(),
                ));
            }
        }
        let sorted: Vec<usize> = removed.into_iter().collect();
        let mut components = std::mem::take(&mut self.list).into_components();
        let mut index = 0;
        components.retain(|_| {
            let keep = sorted.binary_search(&index).is_err();
            index += 1;
            keep
        });
        self.list = components.into();
        let remap = |index: usize| -> Option<usize> {
            match sorted.binary_search(&index) {
                Ok(_) => None,
                Err(before) => Some(index - before),
            }
        };
        self.selection = std::mem::take(&mut self.selection)
            .into_iter()
            .filter_map(|index| remap(index as usize).map(|new| new as i64))
            .collect();
        self.shape_units = std::mem::take(&mut self.shape_units)
            .into_iter()
            .filter_map(|(index, units)| remap(index).map(|new| (new, units)))
            .collect();
        Ok(true)
    }

    /// Restore removed components in reverse removal order, without selecting them.
    pub(crate) fn recover(&mut self) -> bool {
        while let Some((component, units)) = self.removed.pop() {
            if let Some(units) = units {
                self.shape_units.insert(self.list.len(), units);
            }
            self.list.push(component);
        }
        false // CASA returns false even on success.
    }

    /// Forget the recovery bin; CASA returns false even on success.
    pub(crate) fn purge(&mut self) -> bool {
        self.removed.clear();
        false
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
        if component.shape().kind().axes().is_some() {
            let units = [
                quantity(&request.majoraxis)?.unit().clone(),
                quantity(&request.minoraxis)?.unit().clone(),
                quantity(&request.positionangle)?.unit().clone(),
            ];
            self.shape_units.insert(self.list.len(), units);
        }
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
        let mut result = record(self.list.to_record())?;
        for &index in self.shape_units.keys() {
            let CasaValue::Record(components) = &mut result else {
                continue;
            };
            let Some((_, CasaValue::Record(component))) = components
                .iter_mut()
                .find(|(name, _)| name == &format!("component{index}"))
            else {
                continue;
            };
            let Some((_, CasaValue::Record(shape))) =
                component.iter_mut().find(|(name, _)| name == "shape")
            else {
                continue;
            };
            if let CasaValue::Record(quantities) = self.getshape(index as i64)? {
                for (name, quantity) in quantities {
                    if let Some((_, value)) = shape.iter_mut().find(|(key, _)| key == &name) {
                        *value = quantity;
                    }
                }
            }
        }
        Ok(result)
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

    /// Replace a label, using the index checks in casacore's `setLabel`.
    pub(crate) fn setlabel(&mut self, which: i64, value: &str) -> ConvertResult<bool> {
        if which < 0 {
            return Err(ConvertError::runtime("Failed AlwaysAssert which(i) >= 0"));
        }
        let component = usize::try_from(which)
            .ok()
            .and_then(|index| self.list.get_mut(index))
            .ok_or_else(|| ConvertError::runtime("Failed AlwaysAssert index < nelements()"))?;
        component.set_label(value);
        Ok(true)
    }

    /// Change the unit used in one component's spectrum record.
    pub(crate) fn convertfrequnit(&mut self, which: i64, name: &str) -> ConvertResult<bool> {
        let unit = casacore_unit(name).map_err(ConvertError::runtime)?;
        if which < 0 {
            return Err(ConvertError::runtime("Failed AlwaysAssert which(i) >= 0"));
        }
        let component = usize::try_from(which)
            .ok()
            .and_then(|index| self.list.get_mut(index))
            .ok_or_else(|| ConvertError::runtime("Failed AlwaysAssert index < nelements()"))?;
        component
            .spectrum_mut()
            .set_frequency_unit(unit)
            .map_err(|error| ConvertError::runtime(error.to_string()))?;
        Ok(true)
    }

    /// Convert selected component fluxes only after all source units conform.
    pub(crate) fn convertfluxunit(
        &mut self,
        which: i64,
        name: &str,
        lines: &mut Vec<LogLine>,
    ) -> ConvertResult<bool> {
        let target = casacore_unit(name).map_err(ConvertError::runtime)?;
        let jy = casacore_unit("Jy").map_err(ConvertError::runtime)?;
        if !target.conformant(&jy) {
            lines.push(LogLine::error(
                "The flux units must have the same dimensions as the Jansky\nFlux not changed on any components",
            ));
            return Ok(false);
        }
        let indices = if which == -1 {
            if self.list.is_empty() {
                return Err(ConvertError::runtime(
                    "componentlist.convertfluxunit on an empty list would crash CASA (#1053)",
                ));
            }
            (0..self.list.len()).collect::<Vec<_>>()
        } else if which < 0 || which >= self.length() {
            let message = "Index out of range.\nA component number is less than one or greater than the list length\nFlux not changed on any components";
            lines.push(LogLine::error(message));
            return Err(ConvertError::runtime(message));
        } else {
            vec![which as usize]
        };
        let mut converted = Vec::with_capacity(indices.len());
        for &index in &indices {
            let flux = self.list.get(index).expect("validated index").flux();
            // CASA does nothing when the unit names already match, even if
            // the unit's scale is zero; avoid a spurious zero divided by zero.
            if flux.unit().name() == target.name() {
                converted.push(flux.clone());
                continue;
            }
            if !flux.unit().conformant(&target) {
                return Err(ConvertError::runtime(format!(
                    "non-conforming flux unit '{}' cannot be converted to '{}'",
                    flux.unit().name(),
                    target.name()
                )));
            }
            let factor = flux.unit().val().factor / target.val().factor;
            let values = flux.values().map(|value| value * factor);
            let errors = flux.errors().map(|error| error * factor);
            let result = Flux::new(values, target.name(), flux.polarisation())
                .map_err(|error| ConvertError::runtime(error.to_string()))?
                .with_errors(errors);
            converted.push(result);
        }
        for (index, flux) in indices.into_iter().zip(converted) {
            *self
                .list
                .get_mut(index)
                .expect("validated index")
                .flux_mut() = flux;
        }
        Ok(true)
    }

    /// The component `which`, for the methods CASA does not check: casacore's
    /// `ComponentList::component` asserts the index.
    pub(crate) fn unchecked(&self, which: i64) -> ConvertResult<&SkyComponent> {
        usize::try_from(which)
            .ok()
            .and_then(|index| self.list.get(index))
            .ok_or_else(|| ConvertError::runtime("Failed AlwaysAssert index < nelements()"))
    }

    /// `getshape(which)`: axes retain their in-memory units until reopened.
    pub(crate) fn getshape(&self, which: i64) -> ConvertResult<CasaValue> {
        let component = self.unchecked(which)?;
        let Some((axes, errors)) = component.shape().kind().axes() else {
            return Ok(CasaValue::empty_record());
        };
        let defaults = [
            Unit::new("arcmin").expect("known unit"),
            Unit::new("arcmin").expect("known unit"),
            Unit::new("deg").expect("known unit"),
        ];
        let units = self.shape_units.get(&(which as usize)).unwrap_or(&defaults);
        let axis = |value: f64, unit: &Unit| {
            CasaValue::record([
                ("unit", CasaValue::Text(unit.name().to_string())),
                ("value", CasaValue::Float(value / unit.val().factor)),
            ])
        };
        let rad = Unit::new("rad").expect("known unit");
        Ok(CasaValue::record([
            ("majoraxis", axis(axes.major_axis(), &units[0])),
            ("minoraxis", axis(axes.minor_axis(), &units[1])),
            ("positionangle", axis(axes.position_angle(), &units[2])),
            ("majoraxiserror", axis(errors.major_axis, &rad)),
            ("minoraxiserror", axis(errors.minor_axis, &rad)),
            ("positionangleerror", axis(errors.position_angle, &rad)),
        ]))
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

/// `filename` made absolute against the working directory, as casacore's
/// `Path::absoluteName` makes the name a list is attached to. (casacore
/// also expands `~` and environment variables, which this does not.)
fn absolute(filename: &str) -> ConvertResult<PathBuf> {
    std::path::absolute(filename)
        .map_err(|error| ConvertError::runtime(format!("cannot resolve {filename}: {error}")))
}

/// Whether `path` is a table with a `Shape_Parameters` column, the test
/// CASA's `open` makes before it releases the current list.
fn is_component_list(path: &Path) -> bool {
    path.join("table.dat").is_file()
        && Table::open_metadata_only(TableOptions::new(path)).is_ok_and(|table| {
            table
                .schema()
                .is_some_and(|schema| schema.contains_column("Shape_Parameters"))
        })
}

/// A record for Python, or the error CASA reports when it cannot form one.
pub(crate) fn record(
    record: Result<RecordValue, casa_components::ComponentError>,
) -> ConvertResult<CasaValue> {
    record
        .map(|record| CasaValue::from_record(&record))
        .map_err(|error| ConvertError::runtime(error.to_string()))
}
