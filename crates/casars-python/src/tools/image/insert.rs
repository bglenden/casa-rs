// SPDX-License-Identifier: LGPL-3.0-or-later

use std::cmp::Ordering;
use std::path::Path;

use casa_images::analysis::{ImageInsertion, SubimageRegion};
use casa_images::{ImageError, ImageInterface, ImagePixel};
use casa_tables::LockType;
use pyo3::prelude::*;

use crate::convert::DoubleList;
use crate::tools::{LogLine, emit};

use super::handle::{ImageHandle, Storage, raised, with_storage};
use super::subimage::Region;
use super::{PyImageTool, attachment::AttachedImage};

fn copy_into<T: ImagePixel>(
    target: &mut Storage<T>,
    source: &dyn ImageInterface<T>,
    region: Option<SubimageRegion<'_>>,
    locate: &[f64],
) -> Result<bool, ImageError> {
    let plan = with_storage!(target, image => ImageInsertion::new(source, image.as_ref(), region, locate))?;
    if let Some(plan) = plan {
        with_storage!(target, image => plan.copy(source, image.as_mut()))?;
        Ok(true)
    } else {
        Ok(false)
    }
}

fn copy_input(
    target: &mut ImageHandle,
    source: &ImageHandle,
    infile: &str,
    region: Option<SubimageRegion<'_>>,
    locate: &[f64],
) -> PyResult<bool> {
    match (target, source) {
        (ImageHandle::Float(target), ImageHandle::Float(source)) => source
            .with_float_view(|source| {
                if matches!(target, Storage::Temp { fits: Some(_), .. }) {
                    target.with_float_view(|target| {
                        if ImageInsertion::new(source, target, region, locate)?.is_some() {
                            return Err(ImageError::CasaSelection(
                                "This image is not writable".into(),
                            ));
                        }
                        Ok(false)
                    })
                } else {
                    copy_into(target, source, region, locate)
                }
            })
            .map_err(raised),
        (ImageHandle::Complex(target), ImageHandle::Complex(source)) => {
            with_storage!(source, image => copy_into(target, image.as_ref(), region, locate))
                .map_err(raised)
        }
        (_, ImageHandle::Double(_) | ImageHandle::DComplex(_)) => {
            Err(raised("Exception: Unsupported image data type."))
        }
        _ => Err(raised(format!(
            "Attached image pixel data type differs from that of {infile}"
        ))),
    }
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (infile="", region=None, locate=None, verbose=false), text_signature = "($self, infile='', region={}, locate=[-1.0], verbose=False)")]
    fn insert(
        &self,
        py: Python<'_>,
        infile: &str,
        region: Option<&Bound<'_, PyAny>>,
        locate: Option<DoubleList>,
        verbose: bool,
    ) -> PyResult<bool> {
        if self.state().image.is_none() {
            return Ok(false);
        }
        let region = Region::argument(py, region, "insert")?;
        let locate = locate.map_or_else(|| vec![-1.0], |values| values.0);
        let mut logs = Vec::new();
        let outcome = py.detach(|| {
            let state = self.state();
            let Some(attachment) = &state.image else {
                return Ok(false);
            };
            attachment.lock().check_single_precision("insert")?;
            let source_path = Path::new(infile);
            let lock_order = attachment
                .native_path()
                .zip(source_path.canonicalize().ok())
                .map(|(target, source)| target.cmp(&source));
            let selected = region.as_ref().map(Region::as_region);
            let (mut target, copied) = if lock_order == Some(Ordering::Equal) {
                let mut target = attachment.lock();
                attachment.require_lock(&mut target, LockType::Write)?;
                let source = ImageHandle::open(source_path).map_err(raised)?;
                let copied = copy_input(&mut target, &source, infile, selected, &locate)?;
                (target, copied)
            } else {
                let source = AttachedImage::open_for_ordered_read(source_path)?;
                // Both processes and local threads acquire the two images in path order.
                let (mut target, source_image) = if lock_order == Some(Ordering::Less) {
                    let mut target = attachment.lock();
                    let mut source_image = source.lock();
                    attachment.require_lock(&mut target, LockType::Write)?;
                    source.require_lock(&mut source_image, LockType::Read)?;
                    (target, source_image)
                } else {
                    let mut source_image = source.lock();
                    let mut target = attachment.lock();
                    source.require_lock(&mut source_image, LockType::Read)?;
                    attachment.require_lock(&mut target, LockType::Write)?;
                    (target, source_image)
                };
                let copied = copy_input(&mut target, &source_image, infile, selected, &locate)?;
                (target, copied)
            };
            if verbose && copied {
                logs.push(LogLine::info(format!("Inserted pixels from {infile}")));
            }
            if state.history {
                let region = region.as_ref().map_or_else(|| "{...}".into(), Region::history);
                let location = locate.iter().map(|value| format!("{value:.6}")).collect::<Vec<_>>().join(",");
                target.record_history("insert", format!(
                    "ia.insert(infile=\"{infile}\", region={region}, locate=[{location}], verbose={verbose})"
                ))?;
            }
            target.save().map_err(raised)?;
            Ok(true)
        });
        emit(py, "casars.tools.image", &logs)?;
        outcome
    }
}
