// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_images::analysis::{ImageInsertion, SubimageRegion};
use casa_images::{ImageError, ImageInterface, ImagePixel};
use pyo3::prelude::*;

use crate::convert::DoubleList;
use crate::tools::{LogLine, emit};

use super::PyImageTool;
use super::handle::{ImageHandle, Storage, raised, with_storage};
use super::subimage::Region;

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
            let mut target = attachment.lock();
            target.check_single_precision("insert")?;
            let source = ImageHandle::open(Path::new(infile)).map_err(raised)?;
            let selected = region.as_ref().map(Region::as_region);
            let copied = match (&mut *target, &source) {
                (ImageHandle::Float(target), ImageHandle::Float(source)) => {
                    source.with_float_view(|source| {
                        if matches!(target, Storage::Temp { fits: Some(_), .. }) {
                            target.with_float_view(|target| {
                                if ImageInsertion::new(source, target, selected, &locate)?.is_some() {
                                    return Err(ImageError::CasaSelection("This image is not writable".into()));
                                }
                                Ok(false)
                            })
                        } else {
                            copy_into(target, source, selected, &locate)
                        }
                    }).map_err(raised)?
                }
                (ImageHandle::Complex(target), ImageHandle::Complex(source)) => {
                    with_storage!(source, image => copy_into(target, image.as_ref(), selected, &locate))
                        .map_err(raised)?
                }
                (_, ImageHandle::Double(_) | ImageHandle::DComplex(_)) => {
                    return Err(raised("Exception: Unsupported image data type."));
                }
                _ => {
                    return Err(raised(format!("Attached image pixel data type differs from that of {infile}")));
                }
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
