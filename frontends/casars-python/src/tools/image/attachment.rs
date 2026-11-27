// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 adapter around `casa_imageanalysis::tool::attachment`.

use pyo3::prelude::*;

pub(super) use casa_imageanalysis::tool::attachment::AttachedImage;

use crate::tools::coordsys::PyCoordsys;

use super::handle::ImageHandle;

pub(super) fn coordsys_for(
    attachment: &AttachedImage,
    image: &ImageHandle,
) -> PyResult<PyCoordsys> {
    let (coordinates, record) = attachment.coordinate_view(image);
    PyCoordsys::from_image_coordinates(coordinates, record)
}
