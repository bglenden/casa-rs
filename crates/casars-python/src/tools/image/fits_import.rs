// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_images::fits_read::FitsToolImport;
use pyo3::prelude::*;

use super::PyImageTool;
use super::attachment::AttachedImage;
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised, with_storage};

struct ImportFits<'a> {
    outfile: &'a str,
    infile: &'a str,
    whichrep: i64,
    whichhdu: i64,
    zeroblanks: bool,
    overwrite: bool,
}

impl ImportFits<'_> {
    fn run(&self, add_history: bool) -> PyResult<AttachedImage> {
        let whichrep = usize::try_from(self.whichrep).map_err(|_| {
            raised("Exception: The Coordinate Representation index must be non-negative.")
        })?;
        let mut source = FitsToolImport::open(Path::new(self.infile), whichrep, self.whichhdu)
            .map_err(raised)?;
        if source.is_tile_compressed() {
            return Err(raised(format!(
                "Exception: No image at specified location in file {}.",
                self.infile
            )));
        }
        publish_image(self.outfile, self.overwrite, |path| {
            let mut storage = Storage::create(
                source.shape().to_vec(),
                source.coordinates().clone(),
                path,
                None,
            )
            .map_err(raised)?;
            with_storage!(&mut storage, image => source.copy_pixels(&mut **image, self.zeroblanks))
                .map_err(raised)?;
            with_storage!(&mut storage, image => image.set_units(source.units)).map_err(raised)?;
            with_storage!(&mut storage, image => image.set_image_info_record(source.image_info))
                .map_err(raised)?;
            with_storage!(&mut storage, image => image.set_misc_info(source.misc_info))
                .map_err(raised)?;
            with_storage!(&mut storage, image => image.append_history_entries(&source.history))
                .map_err(raised)?;
            let mut image = ImageHandle::Float(storage);
            image.set_coordinate_record(source.coordinate_record)?;
            if add_history {
                image.record_history("fromfits", format!(
                    "ia.fromfits(outfile={:?}, fitsfile={:?}, whichrep={}, whichhdu={}, zeroBlanks={}, overwrite={})",
                    self.outfile, self.infile, self.whichrep, self.whichhdu, self.zeroblanks, self.overwrite,
                ))?;
            }
            image.save().map_err(raised)?;
            Ok(image)
        })
    }
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (outfile="", infile="", whichrep=0, whichhdu=0, zeroblanks=false, overwrite=false), text_signature = "($self, outfile='', infile='', whichrep=0, whichhdu=0, zeroblanks=False, overwrite=False)")]
    #[allow(clippy::too_many_arguments)]
    fn fromfits(
        &self,
        py: Python<'_>,
        outfile: &str,
        infile: &str,
        whichrep: i64,
        whichhdu: i64,
        zeroblanks: bool,
        overwrite: bool,
    ) -> PyResult<bool> {
        py.detach(|| {
            let mut state = self.state();
            let request = ImportFits {
                outfile,
                infile,
                whichrep,
                whichhdu,
                zeroblanks,
                overwrite,
            };
            state.image = Some(request.run(state.history)?);
            Ok(true)
        })
    }

    #[pyo3(signature = (outfile="", infile="", whichrep=0, whichhdu=0, zeroblanks=false, overwrite=false), text_signature = "($self, outfile='', infile='', whichrep=0, whichhdu=0, zeroblanks=False, overwrite=False)")]
    #[allow(clippy::too_many_arguments)]
    fn newimagefromfits(
        &self,
        py: Python<'_>,
        outfile: &str,
        infile: &str,
        whichrep: i64,
        whichhdu: i64,
        zeroblanks: bool,
        overwrite: bool,
    ) -> PyResult<Py<Self>> {
        let image = py.detach(|| {
            ImportFits {
                outfile,
                infile,
                whichrep,
                whichhdu,
                zeroblanks,
                overwrite,
            }
            .run(false)
        })?;
        Py::new(py, Self::attached(image))
    }
}
