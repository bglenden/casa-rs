// SPDX-License-Identifier: LGPL-3.0-or-later
//! One FT publication, with a write position for each independent model table.

use std::collections::BTreeSet;
use std::path::PathBuf;

use casa_ms::{MeasurementSet, MsResult, VirtualModelEntry};
use casa_ms::{
    VirtualModelPublication, verify_virtual_model_target_holding, virtual_model_target_for_field,
};
use casa_tables::TableWriteLock;

use super::{image::ImageModel, invalid};

/// Prepared virtual-model writes and owned image staging for a set of MeasurementSets.
pub struct VirtualFtPublication {
    components: Vec<VirtualModelEntry>,
    incremental: bool,
    field: Option<i32>,
    replace_windows: Option<Vec<i32>>,
    inputs: Vec<PlannedInput>,
    targets: Vec<Target>,
}

struct PlannedInput {
    main: PathBuf,
    storage: PathBuf,
}

struct Target {
    input: usize,
    images: Vec<ImageModel>,
    publication: VirtualModelPublication,
}

impl VirtualFtPublication {
    /// Reports whether any physical publication target can keep references to staged model images.
    pub fn may_reference_models(&self) -> bool {
        self.targets
            .iter()
            .any(|target| target.publication.may_reference_images())
    }

    /// Plans publication to the underlying model tables and owns staged image paths. Returns an
    /// error for invalid entries or incompatible target storage.
    pub fn new<'a>(
        tables: impl Iterator<Item = &'a MeasurementSet>,
        components: Vec<VirtualModelEntry>,
        images: Vec<ImageModel>,
        incremental: bool,
        concatenated_root: bool,
    ) -> MsResult<Self> {
        let first = components
            .first()
            .or_else(|| images.first().map(|image| &image.entry));
        let field = first
            .map(|entry| {
                entry
                    .fields()
                    .first()
                    .copied()
                    .ok_or_else(|| invalid("a virtual model entry names no field"))
            })
            .transpose()?;
        let publication = Self {
            components,
            incremental,
            field,
            replace_windows: None,
            inputs: Vec::new(),
            targets: Vec::new(),
        };
        publication.plan(tables, images, concatenated_root)
    }

    /// Prepare a field's replacement across a concatenation's physical
    /// SOURCE and MAIN tables. Distinct targets each own their images.
    pub fn replacing_field<'a>(
        tables: impl Iterator<Item = &'a MeasurementSet>,
        components: Vec<VirtualModelEntry>,
        images: Vec<ImageModel>,
        field: i32,
        windows: &[i32],
    ) -> MsResult<Self> {
        let publication = Self {
            components,
            incremental: true,
            field: Some(field),
            replace_windows: Some(windows.to_vec()),
            inputs: Vec::new(),
            targets: Vec::new(),
        };
        publication.plan(tables, images, true)
    }

    fn plan<'a>(
        mut self,
        tables: impl Iterator<Item = &'a MeasurementSet>,
        images: Vec<ImageModel>,
        concatenated_root: bool,
    ) -> MsResult<Self> {
        let Some(field) = self.field else {
            return Ok(self);
        };
        let mut storage_paths = BTreeSet::new();
        for (input, table) in tables.enumerate() {
            let storage = virtual_model_target_for_field(table, field)?
                .canonicalize()
                .map_err(invalid)?;
            let main = main_path(table)?;
            let skip_concat = input == 0 && concatenated_root && main == storage;
            self.inputs.push(PlannedInput {
                main,
                storage: storage.clone(),
            });
            if skip_concat {
                // Concatenated MAIN has no independently writable keywords.
                continue;
            }
            if !storage_paths.insert(storage) {
                continue;
            }
            // Each independent storage target owns its Fourier images.
            // Prepare all copies before publishing the first record.
            let target_images = if input == 0 {
                Vec::new()
            } else {
                images
                    .iter()
                    .map(|model| model.copy_to(table))
                    .collect::<MsResult<_>>()?
            };
            self.targets.push(Target {
                input,
                images: target_images,
                publication: VirtualModelPublication::default(),
            });
        }
        if let Some(target) = self.targets.first_mut().filter(|target| target.input == 0) {
            target.images = images;
        }
        Ok(self)
    }

    /// Resume this publication with the same set of inputs in the same
    /// order. A target with a finished record never receives it twice.
    pub fn publish<'a>(
        &mut self,
        tables: impl Iterator<Item = (&'a mut MeasurementSet, &'a mut TableWriteLock)>,
    ) -> MsResult<()> {
        let Some(field) = self.field else {
            return Ok(());
        };
        let mut tables = tables.collect::<Vec<_>>();
        if tables.len() != self.inputs.len() {
            return Err(invalid("the virtual model publication inputs changed"));
        }
        // Include aliases we would otherwise skip. A MAIN that changed its
        // SOURCE after preparation can no longer share the original write.
        for (input, (table, lock)) in self.inputs.iter().zip(&mut tables) {
            if main_path(table)? != input.main
                || lock.path().canonicalize().map_err(invalid)? != input.main
            {
                return Err(invalid("a virtual model publication input changed"));
            }
            verify_virtual_model_target_holding(table, lock, field, &input.storage)?;
        }
        for target in &mut self.targets {
            let (table, lock) = &mut tables[target.input];
            let entries = self.components.iter().map(|entry| (entry, true)).chain(
                target
                    .images
                    .iter()
                    .enumerate()
                    .map(|(index, model)| (&model.entry, self.incremental || index > 0)),
            );
            let result = if let Some(windows) = &self.replace_windows {
                target.publication.replace_windows_at_holding(
                    table,
                    entries.map(|(entry, _)| entry),
                    lock,
                    field,
                    windows,
                    &self.inputs[target.input].storage,
                )
            } else {
                target.publication.publish_holding(
                    table,
                    entries,
                    lock,
                    (field, &self.inputs[target.input].storage),
                )
            };
            if target.publication.may_reference_images() {
                for image in &mut target.images {
                    image.retain();
                }
            }
            result?;
        }
        Ok(())
    }
}

fn main_path(table: &MeasurementSet) -> MsResult<PathBuf> {
    table
        .main_table()
        .path()
        .ok_or_else(|| invalid("the model table is not on disk"))?
        .canonicalize()
        .map_err(invalid)
}

#[cfg(test)]
mod tests;
