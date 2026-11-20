// SPDX-License-Identifier: LGPL-3.0-or-later
//! One FT publication, with a write position for each independent model table.

use std::collections::BTreeSet;
use std::path::PathBuf;

use casa_ms::virtual_model::{VirtualModelPublication, virtual_model_target};
use casa_ms::{MeasurementSet, MsResult, VirtualModelEntry};
use casa_tables::TableWriteLock;

use super::{image::ImageModel, invalid};

pub(crate) struct VirtualFtPublication {
    components: Vec<VirtualModelEntry>,
    incremental: bool,
    targets: Vec<Target>,
}

struct Target {
    input: usize,
    main: PathBuf,
    images: Vec<ImageModel>,
    publication: VirtualModelPublication,
}

impl VirtualFtPublication {
    pub(crate) fn new<'a>(
        tables: impl Iterator<Item = &'a MeasurementSet>,
        components: Vec<VirtualModelEntry>,
        images: Vec<ImageModel>,
        incremental: bool,
        concatenated_root: bool,
    ) -> MsResult<Self> {
        let mut publication = Self {
            components,
            incremental,
            targets: Vec::new(),
        };
        let Some(first) = publication
            .components
            .first()
            .or_else(|| images.first().map(|image| &image.entry))
        else {
            return Ok(publication);
        };
        let mut storage_paths = BTreeSet::new();
        for (input, table) in tables.enumerate() {
            let storage = virtual_model_target(table, first)?;
            if input == 0 && concatenated_root && table.main_table().path() == Some(storage) {
                // Concatenated MAIN has no independently writable keywords.
                continue;
            }
            if !storage_paths.insert(storage.canonicalize().map_err(invalid)?) {
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
            publication.targets.push(Target {
                input,
                main: main_path(table)?,
                images: target_images,
                publication: VirtualModelPublication::default(),
            });
        }
        if let Some(target) = publication
            .targets
            .first_mut()
            .filter(|target| target.input == 0)
        {
            target.images = images;
        }
        Ok(publication)
    }

    /// Resume this publication with the same set of inputs in the same
    /// order. A target with a finished record never receives it twice.
    pub(crate) fn publish<'a>(
        &mut self,
        tables: impl Iterator<Item = (&'a mut MeasurementSet, &'a mut TableWriteLock)>,
    ) -> MsResult<()> {
        let mut tables = tables.collect::<Vec<_>>();
        for target in &self.targets {
            let Some((table, lock)) = tables.get(target.input) else {
                return Err(invalid("a virtual model publication input is missing"));
            };
            if main_path(table)? != target.main
                || lock.path().canonicalize().map_err(invalid)? != target.main
            {
                return Err(invalid("a virtual model publication input changed"));
            }
        }
        for target in &mut self.targets {
            let (table, lock) = &mut tables[target.input];
            let entries = self.components.iter().map(|entry| (entry, true)).chain(
                target
                    .images
                    .iter()
                    .enumerate()
                    .map(|(index, model)| (&model.entry, index > 0 || self.incremental)),
            );
            let result = target.publication.publish_holding(table, entries, lock);
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
