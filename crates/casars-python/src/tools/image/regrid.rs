// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_images::{
    ImageError, ImageInterface,
    analysis::{
        ImageRegridOptions, ImageRegridProduct, SubimageRegion, ensure_artifacts_do_not_overlap,
        ensure_outfile_does_not_alias_input, regrid_image,
    },
    image::image_history_rows,
    regrid::RegridPixel,
};
use casa_types::RecordValue;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::IntList;
use crate::tools::table::casa_record;

use super::PyImageTool;
use super::attachment::read_locked;
use super::beam_view::BeamView;
use super::beams::subset_beam_record;
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};
use super::subimage::{Region, mask_argument, require_bool};

#[pymethods]
impl PyImageTool {
    /// Resample the selected image onto the requested coordinate grid.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", shape=IntList(vec![-1]), csys=None, axes=IntList(vec![-1]), region=None, mask=None, method="linear", decimate=10, replicate=false, doref=true, dropdeg=false, overwrite=false, force=false, asvelocity=false, stretch=false), text_signature = "($self, outfile='', shape=[-1], csys={}, axes=[-1], region={}, mask='', method='linear', decimate=10, replicate=False, doref=True, dropdeg=False, overwrite=False, force=False, asvelocity=False, stretch=False)")]
    fn regrid(
        &self,
        py: Python<'_>,
        outfile: &str,
        shape: IntList,
        csys: Option<&Bound<'_, PyDict>>,
        axes: IntList,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        method: &str,
        decimate: i64,
        #[pyo3(from_py_with = require_bool)] replicate: bool,
        #[pyo3(from_py_with = require_bool)] doref: bool,
        #[pyo3(from_py_with = require_bool)] dropdeg: bool,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        #[pyo3(from_py_with = require_bool)] force: bool,
        #[pyo3(from_py_with = require_bool)] asvelocity: bool,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
    ) -> PyResult<Py<Self>> {
        let target = csys
            .filter(|record| !record.is_empty())
            .map(|record| casa_record(py, Some(record)))
            .transpose();
        let region = Region::capture(py, region);
        let mask = mask_argument(mask);
        let attachment = py.detach(|| {
            let state = self.state();
            let input = state.image.as_ref().ok_or_else(|| raised("Unable to create image"))?;
            let target = target?;
            let region = region?;
            let mask = mask?;
            let destination = (!outfile.is_empty()).then(|| Path::new(outfile));
            let options = ImageRegridOptions {
                shape: &shape.0, target: target.as_ref(), axes: &axes.0,
                region: region.as_ref().map(Region::as_region), mask: (!mask.is_empty()).then_some(mask.as_str()),
                method, decimate, replicate, convert_reference: doref, dropdeg, force,
                as_velocity: asvelocity, stretch, outfile: destination,
            };
            publish_image(outfile, overwrite, |stage| read_locked(input, |handle| {
                if let Some(destination) = destination {
                    for source in handle.path().into_iter().chain(input.operand_paths().iter().map(|path| path.as_path())) {
                        ensure_outfile_does_not_alias_input(destination, source).map_err(raised)?;
                    }
                    if let Some(region) = options.region.and_then(SubimageRegion::external_path) {
                        ensure_artifacts_do_not_overlap(destination, region).map_err(raised)?;
                    }
                }
                let native = handle.coordinate_record();
                let beams = input.beam_view();
                let mut output = match handle {
                    ImageHandle::Float(source) => ImageHandle::Float(source.with_float_view(|image| {
                        make_product(image, native, &beams, options, stage)
                    })?),
                    ImageHandle::Double(source) => ImageHandle::Double(with_storage!(source, image =>
                        make_product(image.as_ref(), native, &beams, options, stage))?),
                    ImageHandle::Complex(source) => ImageHandle::Complex(with_storage!(source, image =>
                        make_product(image.as_ref(), native, &beams, options, stage))?),
                    ImageHandle::DComplex(source) => ImageHandle::DComplex(with_storage!(source, image =>
                        make_product(image.as_ref(), native, &beams, options, stage))?),
                };
                let history = with_pixels!(handle, source => match source {
                    Storage::Paged(image) => image_history_rows(image.name().expect("native image path")).map_err(raised)?,
                    Storage::Temp { image, .. } => image.history_rows().to_vec(),
                });
                with_pixels!(&mut output, storage => with_storage!(storage, image => image.append_history_entries(&history))).map_err(raised)?;
                if state.history {
                    let region = region.as_ref().map_or_else(|| "{}".into(), Region::history);
                    let csys = if target.is_some() { "{...}" } else { "{}" };
                    output.record_history("regrid", format!(
                        "ia.regrid(outfile={outfile:?}, shape={:?}, csys={csys}, axes={:?}, region={region}, mask={mask:?}, method={method:?}, decimate={decimate}, replicate={replicate}, doref={doref}, dropdeg={dropdeg}, overwrite={overwrite}, force={force}, asvelocity={asvelocity}, stretch={stretch})",
                        shape.0, axes.0,
                    ))?;
                }
                output.save().map_err(raised)?;
                Ok(output)
            }))
        })?;
        Py::new(py, Self::attached(attachment))
    }
}

fn make_product<T: RegridPixel>(
    source: &dyn ImageInterface<T>,
    native: RecordValue,
    beam_view: &BeamView,
    options: ImageRegridOptions<'_>,
    stage: Option<&Path>,
) -> PyResult<Storage<T>> {
    let mut source_info = source.image_info().map_err(raised)?.to_record();
    beam_view.apply_to(&mut source_info);
    let ImageRegridProduct {
        regridded,
        coordinate_record,
        image_info,
        beam_planes,
    } = regrid_image(source, native, &source_info, options).map_err(raised)?;
    let mut info = image_info.to_record();
    let beams = beam_view.beam_set().map_err(raised)?;
    if beams.is_multi() {
        let [channels, polarizations] = &beam_planes;
        subset_beam_record(
            &mut info,
            Some(beam_view.record()),
            &beams,
            channels,
            polarizations,
        )
        .map_err(raised)?;
    } else {
        beam_view.apply_to(&mut info);
    }
    let mut storage = Storage::create(
        regridded.data.shape().to_vec(),
        regridded.coordinates,
        stage,
        Some(&regridded.data),
    )
    .map_err(raised)?;
    with_storage!(&mut storage, image => {
        image.set_units(source.units())?;
        image.set_misc_info(source.misc_info())?;
        image.set_image_info_record(info)?;
        if regridded.mask.iter().any(|&valid| !valid) {
            image.put_mask("mask0", &regridded.mask)?;
            image.set_default_mask("mask0")?;
        }
        Ok::<(), ImageError>(())
    })
    .map_err(raised)?;
    match &mut storage {
        Storage::Paged(image) => image
            .set_coordinate_record(coordinate_record)
            .map_err(raised)?,
        Storage::Temp { record, .. } => *record = coordinate_record,
    }
    Ok(storage)
}
