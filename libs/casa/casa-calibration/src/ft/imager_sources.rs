// SPDX-License-Identifier: LGPL-3.0-or-later
//! Inputs opened by one legacy sky equation.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::path::{Path, PathBuf};

use casa_components::ComponentList;
use casa_core::RecordValue;
use casa_images::PagedImage;
use casa_ms::MsResult;

use super::invalid;

#[derive(Default)]
pub(super) struct ModelSources {
    images: BTreeMap<PathBuf, PagedImage<f32>>,
    components: Option<(PathBuf, RecordValue)>,
}

impl ModelSources {
    pub(super) fn paths(&self) -> (Vec<PathBuf>, Option<PathBuf>) {
        (
            self.images.keys().cloned().collect(),
            self.components.as_ref().map(|(path, _)| path.clone()),
        )
    }

    pub(super) fn retain(&mut self, images: &[PathBuf], components: Option<&Path>) {
        self.images.retain(|path, _| images.contains(path));
        if self.components.as_ref().map(|(path, _)| path.as_path()) != components {
            self.components = None;
        }
    }

    pub(super) fn needs_image(&self, path: &Path) -> bool {
        !self.images.contains_key(path) && !path.exists()
    }

    pub(super) fn open_images(&mut self, paths: &[PathBuf]) -> MsResult<()> {
        for path in paths {
            match self.images.entry(path.clone()) {
                Entry::Occupied(slot) => slot.get().discard_clean_pixel_cache(),
                Entry::Vacant(slot) => {
                    // Keep the table/file handle with a small cache so a live
                    // imager can still use an opened image after it moves.
                    slot.insert(PagedImage::open_with_cache(path, 1).map_err(invalid)?);
                }
            }
        }
        Ok(())
    }

    pub(super) fn image(&self, path: &Path) -> MsResult<&PagedImage<f32>> {
        self.images
            .get(path)
            .ok_or_else(|| invalid("model image is not open"))
    }

    pub(super) fn component_record(&mut self, path: &Path) -> MsResult<&RecordValue> {
        if self.components.is_none() {
            let components = ComponentList::read_table(path).map_err(invalid)?;
            self.components = Some((path.to_owned(), components.to_record().map_err(invalid)?));
        }
        Ok(&self.components.as_ref().expect("opened component list").1)
    }
}
