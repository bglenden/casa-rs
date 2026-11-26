// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_core::RecordValue;
use casa_imageanalysis::analysis::{
    ContinuumSummary, ImageContinuum, ensure_outfile_does_not_alias_input,
};
use casa_images::{ImageInterface, image_history_rows};
use pyo3::prelude::*;

use crate::convert::IntList;

use super::PyImageTool;
use super::attachment::AttachedImage;
use super::create::publish_image_pair;
use super::handle::{ImageHandle, Storage, raised, with_storage};
use super::subimage::{RegionArgument, require_bool};

struct Request<'a> {
    outline: &'a str,
    outcont: &'a str,
    region: RegionArgument,
    channels: IntList,
    pol: &'a str,
    fitorder: i64,
    overwrite: bool,
}

impl Request<'_> {
    fn fit(
        &self,
        image: &dyn ImageInterface<f32>,
        record: RecordValue,
        beams: &RecordValue,
        outline: Option<&Path>,
        outcont: Option<&Path>,
    ) -> PyResult<(ImageHandle, Option<ImageHandle>, ContinuumSummary)> {
        let mut plan = ImageContinuum::new(
            image,
            record,
            self.region.selection(),
            &self.channels.0,
            self.fitorder,
            beams,
        )
        .map_err(raised)?;
        let allocate = |path| {
            Storage::create(
                plan.section.shape.clone(),
                plan.section.coordinates.clone(),
                path,
                None,
            )
        };
        let mut line = allocate(outline).map_err(raised)?;
        let mut model = outcont
            .map(|path| allocate(Some(path)))
            .transpose()
            .map_err(raised)?;
        let summary = match &mut model {
            Some(model) => with_storage!(&mut line, line => {
                with_storage!(model, model => plan.copy_to(image, line.as_mut(), Some(model.as_mut())))
            }),
            None => with_storage!(&mut line, line => plan.copy_to(image, line.as_mut(), None)),
        }.map_err(raised)?;
        let mut line = ImageHandle::Float(line);
        let mut model = model.map(ImageHandle::Float);
        if let Some(record) = plan.section.coordinate_record.take() {
            if let Some(model) = &mut model {
                model.set_coordinate_record(record.clone())?;
            }
            line.set_coordinate_record(record)?;
        }
        Ok((line, model, summary))
    }

    fn build(
        &self,
        image: &ImageHandle,
        beams: &RecordValue,
        outline: Option<&Path>,
        outcont: Option<&Path>,
        history: bool,
    ) -> PyResult<(ImageHandle, Option<ImageHandle>)> {
        image.check_single_precision("continuumsub")?;
        let ImageHandle::Float(storage) = image else {
            return Err(raised("This method only supports Float valued images"));
        };
        let record = image.coordinate_record();
        let (mut residual, mut model, summary) = match storage {
            Storage::Temp {
                image,
                fits: Some(fits),
                ..
            } => self.fit(&fits.image_view(image), record, beams, outline, outcont),
            _ => {
                with_storage!(storage, image => self.fit(image.as_ref(), record, beams, outline, outcont))
            }
        }?;
        let prior = match storage {
            Storage::Paged(image) => {
                image_history_rows(image.name().expect("native source")).map_err(raised)?
            }
            Storage::Temp { image, .. } => image.history_rows().map_err(raised)?.into_owned(),
        };
        let history_arguments = history.then(|| {
            let Self { outline, outcont, channels, region, pol, fitorder, overwrite } = self;
            let channels = channels.0.iter().map(i64::to_string).collect::<Vec<_>>().join(",");
            let region = region.history();
            format!("ia.continuumsub(outline={outline:?}, outcont={outcont:?}, region={region}, channels=[{channels}], pol={pol:?}, fitorder={fitorder}, overwrite={overwrite})")
        });
        let profiles = summary.profiles;
        let attempted = summary.attempted;
        let valid = summary.valid;
        let statistics = [
            format!("Number of profiles       = {profiles}"),
            format!("Number of fits attempted = {attempted}"),
            format!("Number succeeded         = {attempted}"),
            format!("Number converged         = {attempted}"),
            format!("Number valid             = {valid}"),
        ];
        for output in std::iter::once(&mut residual).chain(model.as_mut()) {
            let ImageHandle::Float(storage) = &mut *output else {
                unreachable!("continuum products store Float")
            };
            with_storage!(storage, product => product.append_history_entries(&prior))
                .map_err(raised)?;
            if let Some(command) = &history_arguments {
                output.record_history("continuumsub", command.clone())?;
            }
            output.append_history_at("::ImageProfileFitter::fit", &statistics)?;
            output.save().map_err(raised)?;
        }
        Ok((residual, model))
    }
}

#[pymethods]
impl PyImageTool {
    /// Fit a polynomial to the chosen channels and return the residual image.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outline="", outcont="continuumsub.im", region=RegionArgument::default(), channels=IntList::of(&[-1]), pol="", fitorder=0, overwrite=false), text_signature = "($self, outline='', outcont='continuumsub.im', region={}, channels=[-1], pol='', fitorder=0, overwrite=False)")]
    fn continuumsub(
        &self,
        py: Python<'_>,
        outline: &str,
        outcont: &str,
        region: RegionArgument,
        channels: IntList,
        pol: &str,
        fitorder: i64,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
    ) -> PyResult<Py<Self>> {
        let request = Request {
            outline,
            outcont,
            region,
            channels,
            pol,
            fitorder,
            overwrite,
        };
        let product = py.detach(|| {
            let state = self.state();
            let Some(attached) = state.image.as_ref() else {
                return Ok(None);
            };
            let mut coordinates = None;
            let outline = outline.trim();
            let outcont = outcont.trim();
            let mut product = publish_image_pair(outline, outcont, overwrite, |line, model| {
                let mut image = attached.lock();
                for target in [outline, outcont]
                    .into_iter()
                    .filter(|target| !target.is_empty())
                {
                    for source in image
                        .path()
                        .into_iter()
                        .chain(attached.operand_paths().iter().map(|path| path.as_path()))
                    {
                        ensure_outfile_does_not_alias_input(Path::new(target), source)
                            .map_err(raised)?;
                    }
                }
                attached.require_pixel_read_lock(&mut image)?;
                let beams = attached.beam_view().clone();
                let staged = request.build(&image, beams.record(), line, model, state.history);
                let released = attached.release_requested_lock(&mut image);
                let staged = staged?;
                released?;
                coordinates = AttachedImage::capture_live_coordinates(&staged.0)?;
                Ok(staged)
            })?;
            if let Some((record, system)) = coordinates {
                product.keep_live_coordinates(record, system);
            }
            Ok::<_, PyErr>(Some(product))
        })?;
        Py::new(py, product.map_or_else(Self::new, Self::attached))
    }
}
