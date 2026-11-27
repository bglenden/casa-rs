// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 adapter around `casa_imageanalysis::tool::attachment`.

use std::borrow::Cow;
use std::path::{Path, PathBuf};

use pyo3::prelude::*;

pub(super) use casa_imageanalysis::tool::attachment::{
    AttachedImage, key, registry, remove_file as pure_remove_file,
};

use crate::tools::coordsys::PyCoordsys;

use super::handle::{ImageHandle, external_open_check, raised};

pub(super) fn read_locked<T>(
    attachment: &AttachedImage,
    read: impl FnOnce(&ImageHandle) -> PyResult<T>,
) -> PyResult<T> {
    let mut handle = attachment.lock();
    attachment
        .require_pixel_read_lock(&mut handle)
        .map_err(raised)?;
    let staged = read(&handle);
    let release = attachment.release_requested_lock(&mut handle);
    let result = staged?;
    release.map_err(raised)?;
    Ok(result)
}

pub(super) fn remove_file(path: &Path) -> PyResult<bool> {
    pure_remove_file(path, external_open_check()).map_err(raised)
}

pub(super) fn coordsys_for(
    attachment: &AttachedImage,
    image: &ImageHandle,
) -> PyResult<PyCoordsys> {
    let (coordinates, record) = attachment.coordinate_view(image);
    PyCoordsys::from_image_coordinates(coordinates, record)
}

pub(super) fn rename_attached(
    attachment: AttachedImage,
    name: &str,
    expanded: Result<Cow<'_, str>, String>,
    overwrite: bool,
    history: bool,
) -> PyResult<AttachedImage> {
    attachment
        .rename_with(
            name,
            || {
                expanded
                    .map(Cow::into_owned)
                    .map_err(casa_imageanalysis::tool::ImageToolError::new)
            },
            overwrite,
            history,
            external_open_check(),
        )
        .map_err(raised)
}

pub(super) fn key_py(path: &Path) -> PyResult<PathBuf> {
    key(path).map_err(raised)
}
