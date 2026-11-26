// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_coordinates::CoordinateType;
use casa_images::{
    ImageInterface,
    analysis::{
        ImageTwoPoint, StructurePixel, SubimageRegion, TwoPointOptions,
        ensure_artifacts_do_not_overlap, ensure_outfile_does_not_alias_input,
    },
    image::image_history_rows,
};
use casa_types::RecordValue;
use pyo3::prelude::*;

use crate::convert::IntList;

use super::PyImageTool;
use super::attachment::read_locked;
use super::beam_view::BeamView;
use super::beams::subset_beam_record;
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};
use super::records::numeric_axis_vector;
use super::subimage::{RegionArgument, mask_argument, require_bool};

fn make_product<T: StructurePixel>(
    source: &dyn ImageInterface<T>,
    native: RecordValue,
    beam_view: &BeamView,
    options: TwoPointOptions<'_>,
    stage: Option<&Path>,
) -> PyResult<Storage<T>> {
    let plan = ImageTwoPoint::new(source, native, beam_view.record(), options).map_err(raised)?;
    let mut result = Storage::create(plan.shape.clone(), plan.coordinates.clone(), stage, None)
        .map_err(raised)?;
    with_storage!(&mut result, image => plan.copy_to(source, image.as_mut())).map_err(raised)?;
    let mut info = plan.image_info.to_record();
    let beams = beam_view.beam_set().map_err(raised)?;
    if beams.is_multi() {
        let channels = plan.source.beam_planes(
            source.find_axis(CoordinateType::Spectral),
            beams.n_channels(),
        );
        let stokes = plan
            .source
            .beam_planes(source.find_axis(CoordinateType::Stokes), beams.n_stokes());
        subset_beam_record(
            &mut info,
            Some(beam_view.record()),
            &beams,
            &channels,
            &stokes,
        )
        .map_err(raised)?;
    } else if let Some(beam) = beam_view.record().get("restoringbeam") {
        info.upsert("restoringbeam", beam.clone());
    }
    with_storage!(&mut result, image => image.set_image_info_record(info)).map_err(raised)?;
    match &mut result {
        Storage::Paged(image) => image
            .set_coordinate_record(plan.coordinate_record)
            .map_err(raised)?,
        Storage::Temp { record, .. } => *record = plan.coordinate_record,
    }
    Ok(result)
}

#[pymethods]
impl PyImageTool {
    /// Write the Float or Complex structure function of a selected image.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", region=RegionArgument::default(), mask=None, axes=IntList::of(&[-1]), method="structurefunction", overwrite=false, stretch=false), text_signature = "($self, outfile='', region={}, mask='', axes=[-1], method='structurefunction', overwrite=False, stretch=False)")]
    fn twopointcorrelation(
        &self,
        py: Python<'_>,
        outfile: &str,
        region: RegionArgument,
        mask: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = numeric_axis_vector)] axes: IntList,
        method: &str,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
    ) -> PyResult<bool> {
        let mask = mask_argument(mask);
        py.detach(|| {
            let state = self.state();
            let Some(input) = state.image.as_ref() else {
                return Ok(false);
            };
            let mask = mask?;
            let destination = (!outfile.is_empty()).then(|| Path::new(outfile));
            let options = TwoPointOptions {
                axes: &axes.0, method, region: region.selection(),
                mask: (!mask.is_empty()).then_some(mask.as_str()), stretch, destination,
            };
            let product = publish_image(outfile, overwrite, |stage| read_locked(input, |handle| {
                handle.check_single_precision("twopointcorrelation")?;
                if let Some(destination) = destination {
                    for source in handle.path().into_iter().chain(input.operand_paths().iter().map(|path| path.as_path())) {
                        ensure_outfile_does_not_alias_input(destination, source).map_err(raised)?;
                    }
                    if let Some(region) = options.region.and_then(SubimageRegion::external_path) {
                        ensure_artifacts_do_not_overlap(destination, region).map_err(raised)?;
                    }
                }
                let beams = input.beam_view();
                let record = handle.coordinate_record();
                let mut output = match handle {
                    ImageHandle::Float(source) => ImageHandle::Float(source.with_float_view(|image| {
                        make_product(image, record, &beams, options, stage)
                    })?),
                    ImageHandle::Complex(source) => ImageHandle::Complex(with_storage!(source, image =>
                        make_product(image.as_ref(), record, &beams, options, stage))?),
                    _ => unreachable!("pixel type was checked"),
                };
                let history = with_pixels!(handle, source => match source {
                    Storage::Paged(image) => image_history_rows(image.name().expect("native image path")).map_err(raised)?,
                    Storage::Temp { image, .. } => image.history_rows().map_err(raised)?.into_owned(),
                });
                with_pixels!(&mut output, storage => with_storage!(storage, image => image.append_history_entries(&history)))
                    .map_err(raised)?;
                if state.history {
                    let region = region.history();
                    output.record_history("twopointcorrelation", format!(
                        "ia.twopointcorrelation(outfile={outfile:?}, region={region}, mask={mask:?}, axes={:?}, method={method:?}, overwrite={overwrite}, stretch={stretch})",
                        axes.0,
                    ))?;
                }
                output.save().map_err(raised)?;
                Ok(output)
            }))?;
            drop(product);
            Ok(true)
        })
    }
}
