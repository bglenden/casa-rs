// SPDX-License-Identifier: LGPL-3.0-or-later

//! Share each open native image's pixels and table metadata across Python tools.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use pyo3::prelude::*;

use super::handle::{ImageHandle, raised};

type OpenImages = BTreeMap<PathBuf, Weak<Mutex<ImageHandle>>>;

static ATTACHED: Mutex<OpenImages> = Mutex::new(BTreeMap::new());

pub(super) fn registry() -> MutexGuard<'static, OpenImages> {
    ATTACHED.lock().unwrap_or_else(PoisonError::into_inner)
}

pub(super) struct AttachedImage {
    image: Arc<Mutex<ImageHandle>>,
    key: Option<PathBuf>,
}

impl AttachedImage {
    pub(super) fn temporary(image: ImageHandle) -> Self {
        Self {
            image: Arc::new(Mutex::new(image)),
            key: None,
        }
    }

    pub(super) fn open(registry: &mut OpenImages, path: &Path) -> PyResult<Self> {
        let key = key(path)?;
        if let Some(image) = registry.get(&key).and_then(Weak::upgrade) {
            return Ok(Self {
                image,
                key: Some(key),
            });
        }
        let image = Arc::new(Mutex::new(ImageHandle::open(path).map_err(raised)?));
        registry.insert(key.clone(), Arc::downgrade(&image));
        Ok(Self {
            image,
            key: Some(key),
        })
    }

    pub(super) fn lock(&self) -> MutexGuard<'_, ImageHandle> {
        self.image.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn is_persistent(&self) -> bool {
        self.key.is_some()
    }

    pub(super) fn remove(&self) -> PyResult<()> {
        let Some(key) = &self.key else {
            return Ok(());
        };
        let mut registry = registry();
        if Arc::strong_count(&self.image) > 1 {
            return Err(raised(format!(
                "Cannot delete file {} because table is still open in this process",
                key.display()
            )));
        }
        std::fs::remove_dir_all(key).map_err(raised)?;
        registry.remove(key);
        Ok(())
    }
}

impl Drop for AttachedImage {
    fn drop(&mut self) {
        if let Some(key) = &self.key {
            let mut registry = registry();
            if Arc::strong_count(&self.image) == 1
                && registry
                    .get(key)
                    .is_some_and(|registered| registered.ptr_eq(&Arc::downgrade(&self.image)))
            {
                registry.remove(key);
            }
        }
    }
}

/// Registry keys resolve aliases to an existing native table directory.
pub(super) fn key(path: &Path) -> PyResult<PathBuf> {
    path.canonicalize().map_err(raised)
}
