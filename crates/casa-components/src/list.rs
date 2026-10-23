// SPDX-License-Identifier: LGPL-3.0-or-later
//! An ordered list of sky components, CASA's `ComponentList`.

use std::path::Path;

use crate::component::SkyComponent;
use crate::error::ComponentListError;
use crate::table;

/// An ordered list of [`SkyComponent`]s, CASA's `ComponentList`.
///
/// The order is the table's row order: [`Self::read_table`] keeps it and
/// [`Self::write_table`] writes component `i` to row `i`, as CASA's
/// `readTable` and `writeTable` do.
///
/// CASA's `ComponentList` also carries tool state that its table does not
/// store: a selection flag per component and the table it is attached to,
/// which it rewrites when the list is destroyed. Those belong to the `cl`
/// tool and are not modelled here; a list is a value, read from and written
/// to tables explicitly.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ComponentList {
    components: Vec<SkyComponent>,
}

impl ComponentList {
    /// An empty list.
    pub fn new() -> Self {
        Self::default()
    }

    /// Read a CASA component-list table, as `ComponentList::readTable` does.
    ///
    /// Tables from before CASA added the error columns or `Spectral_Record`
    /// read with zero errors and default spectral details, as in CASA. See
    /// the [crate documentation](crate#the-table-format) for the format.
    ///
    /// # Errors
    ///
    /// [`ComponentListError`] when the table cannot be opened, lacks a column
    /// CASA requires, or holds a value CASA's model refuses.
    pub fn read_table(path: impl AsRef<Path>) -> Result<Self, ComponentListError> {
        table::read_component_list(path.as_ref())
    }

    /// Write this list as a new CASA component-list table, as CASA's
    /// `ComponentList::rename` (`createTable`) and `writeTable` do.
    ///
    /// The table is built in a staging directory beside `path` and moved
    /// into place when complete (ADR-0008).
    ///
    /// # Errors
    ///
    /// [`ComponentListError::Exists`] when `path` exists (CASA creates the
    /// table with `NewNoReplace`), [`ComponentListError::Unwritable`] for a
    /// component CASA could not write, and the table system's failures.
    pub fn write_table(&self, path: impl AsRef<Path>) -> Result<(), ComponentListError> {
        table::write_component_list(self, path.as_ref())
    }

    /// Rewrite the component-list table at `path` with this list, as CASA's
    /// `writeTable` rewrites a list it has open for update (the `cl` tool's
    /// `close`). The new table is staged beside `path` and swapped in; the
    /// old one is removed only once the new one is in place.
    ///
    /// # Errors
    ///
    /// [`ComponentListError::Unwritable`] for a component CASA could not
    /// write, [`ComponentListError::Publish`] when the tables cannot be
    /// swapped, and the table system's failures.
    pub fn rewrite_table(&self, path: impl AsRef<Path>) -> Result<(), ComponentListError> {
        table::rewrite_component_list(self, path.as_ref())
    }

    /// Append a component (`add`).
    pub fn push(&mut self, component: SkyComponent) {
        self.components.push(component);
    }

    /// Remove and return component `index` (`remove`).
    ///
    /// # Panics
    ///
    /// When `index` is out of range, as `Vec::remove`.
    pub fn remove(&mut self, index: usize) -> SkyComponent {
        self.components.remove(index)
    }

    /// The number of components (`nelements`).
    pub fn len(&self) -> usize {
        self.components.len()
    }

    /// Whether the list is empty.
    pub fn is_empty(&self) -> bool {
        self.components.is_empty()
    }

    /// Component `index`, if it exists (`component(index)`).
    pub fn get(&self, index: usize) -> Option<&SkyComponent> {
        self.components.get(index)
    }

    /// Mutable access to component `index`, if it exists.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut SkyComponent> {
        self.components.get_mut(index)
    }

    /// The components, in order.
    pub fn components(&self) -> &[SkyComponent] {
        &self.components
    }

    /// The components, in order, by value.
    pub fn into_components(self) -> Vec<SkyComponent> {
        self.components
    }

    /// Iterate over the components in order.
    pub fn iter(&self) -> std::slice::Iter<'_, SkyComponent> {
        self.components.iter()
    }
}

impl From<Vec<SkyComponent>> for ComponentList {
    fn from(components: Vec<SkyComponent>) -> Self {
        Self { components }
    }
}

impl FromIterator<SkyComponent> for ComponentList {
    fn from_iter<I: IntoIterator<Item = SkyComponent>>(iter: I) -> Self {
        Self {
            components: iter.into_iter().collect(),
        }
    }
}

impl IntoIterator for ComponentList {
    type Item = SkyComponent;
    type IntoIter = std::vec::IntoIter<SkyComponent>;

    fn into_iter(self) -> Self::IntoIter {
        self.components.into_iter()
    }
}

impl<'a> IntoIterator for &'a ComponentList {
    type Item = &'a SkyComponent;
    type IntoIter = std::slice::Iter<'a, SkyComponent>;

    fn into_iter(self) -> Self::IntoIter {
        self.components.iter()
    }
}
