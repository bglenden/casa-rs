// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_images::{
    ImageInterface,
    analysis::{
        DecimationMethod, ImageSmoothing, SmoothingKernel, SmoothingOptions, SmoothingPixel,
        ensure_outfile_does_not_alias_input,
    },
    image::{ImageHistoryRow, image_history_rows},
};
use casa_types::RecordValue;
use pyo3::prelude::*;
use pyo3::types::PyString;

use super::PyImageTool;
use super::attachment::AttachedImage;
use super::beam_view::BeamView;
use super::create::publish_masked_image;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};
use super::subimage::{RegionArgument, mask_argument, require_bool};

struct SmoothingMask {
    value: String,
    history: String,
}

impl SmoothingMask {
    fn boxcar_default() -> Self {
        Self {
            value: String::new(),
            history: "[]".into(),
        }
    }

    fn hanning_default() -> Self {
        Self {
            value: String::new(),
            history: r#""""#.into(),
        }
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for SmoothingMask {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let mask = mask_argument((!value.is_none()).then_some(&value))?;
        let history = if value.is_instance_of::<PyString>() {
            format!("{mask:?}")
        } else {
            "[]".into()
        };
        Ok(Self {
            value: mask,
            history,
        })
    }
}

struct Request<'a> {
    outfile: &'a str,
    region: RegionArgument,
    mask: SmoothingMask,
    kernel: SmoothingKernel,
    axis: i64,
    drop: bool,
    dmethod: &'a str,
    overwrite: bool,
    stretch: bool,
}

impl Request<'_> {
    fn name(&self) -> &'static str {
        match self.kernel {
            SmoothingKernel::Boxcar(_) => "boxcar",
            SmoothingKernel::Hanning => "hanning",
        }
    }

    fn options(&self) -> PyResult<SmoothingOptions<'_>> {
        let decimate = if self.drop {
            match self.dmethod.as_bytes().first().map(u8::to_ascii_lowercase) {
                Some(b'c') => Some(DecimationMethod::Copy),
                Some(b'm') => Some(DecimationMethod::Mean),
                _ => {
                    return Err(raised(
                        "Value of dmethod must be either 'm'(ean) or 'c'(opy)",
                    ));
                }
            }
        } else {
            None
        };
        Ok(SmoothingOptions {
            axis: self.axis,
            kernel: self.kernel,
            decimate,
            region: self.region.selection(),
            mask: (!self.mask.value.is_empty()).then_some(self.mask.value.as_str()),
            stretch: self.stretch,
            destination: (!self.outfile.is_empty()).then(|| Path::new(self.outfile)),
        })
    }

    fn call_history(&self, axis: usize) -> String {
        let Self {
            outfile,
            drop,
            dmethod,
            overwrite,
            stretch,
            ..
        } = self;
        let region = self.region.history();
        let mask = &self.mask.history;
        match self.kernel {
            SmoothingKernel::Boxcar(width) => format!(
                "ia.boxcar(outfile={outfile:?}, region={region}, mask={mask}, axis={axis}, width={width}, drop={drop}, dmethod={dmethod:?}, overwrite={overwrite}, stretch={stretch})"
            ),
            SmoothingKernel::Hanning => format!(
                "ia.hanning(outfile={outfile:?}, region={region}, mask={mask}, axis={axis}, drop={drop}, overwrite={overwrite}, stretch={stretch}, dmethod={dmethod:?})"
            ),
        }
    }

    fn copy<T: SmoothingPixel>(
        &self,
        image: &dyn ImageInterface<T>,
        coordinates: RecordValue,
        beams: &BeamView,
        path: Option<&Path>,
    ) -> PyResult<(Storage<T>, usize)> {
        let mut plan = ImageSmoothing::new(image, coordinates, self.options()?, beams.record())
            .map_err(raised)?;
        let mut output = Storage::create(
            plan.section.shape.clone(),
            plan.section.coordinates.clone(),
            path,
            None,
        )
        .map_err(raised)?;
        with_storage!(&mut output, product => plan.copy_to(image, product.as_mut()))
            .map_err(raised)?;
        let mut info = plan.section.image_info().to_record();
        beams.apply_to(&mut info);
        with_storage!(&mut output, product => product.set_image_info_record(info))
            .map_err(raised)?;
        if let Some(record) = plan.section.coordinate_record.take() {
            match &mut output {
                Storage::Paged(image) => image.set_coordinate_record(record).map_err(raised)?,
                Storage::Temp {
                    record: current, ..
                } => *current = record,
            }
        }
        Ok((output, plan.axis()))
    }

    fn build(
        &self,
        image: &ImageHandle,
        beams: &BeamView,
        path: Option<&Path>,
        history: bool,
    ) -> PyResult<ImageHandle> {
        let record = image.coordinate_record();
        let (mut output, axis) = match image {
            ImageHandle::Float(Storage::Temp {
                image,
                fits: Some(fits),
                ..
            }) => {
                let (result, axis) = self.copy(&fits.image_view(image), record, beams, path)?;
                (ImageHandle::Float(result), axis)
            }
            ImageHandle::Float(source) => {
                let (result, axis) =
                    with_storage!(source, image => self.copy(image.as_ref(), record, beams, path))?;
                (ImageHandle::Float(result), axis)
            }
            ImageHandle::Complex(source) => {
                let (result, axis) =
                    with_storage!(source, image => self.copy(image.as_ref(), record, beams, path))?;
                (ImageHandle::Complex(result), axis)
            }
            _ => unreachable!("checked single precision"),
        };
        let history_rows: Vec<ImageHistoryRow> = with_pixels!(image, storage => match storage {
            Storage::Paged(image) => image_history_rows(image.name().expect("native image path")).map_err(raised)?,
            Storage::Temp { image, .. } => image.history_rows().map_err(raised)?.into_owned(),
        });
        with_pixels!(&mut output, storage => with_storage!(storage, product => product.append_history_entries(&history_rows))).map_err(raised)?;
        if history {
            match self.kernel {
                SmoothingKernel::Boxcar(_) => {
                    output.record_history(self.name(), self.call_history(axis))?
                }
                SmoothingKernel::Hanning => output.append_history_at(
                    "::image::_hanning",
                    &["Ran ia.hanning".into(), self.call_history(axis)],
                )?,
            }
        }
        let source_name = image.path().map_or_else(
            || "Temporary_Image".into(),
            |name| name.display().to_string(),
        );
        let origin = match self.kernel {
            SmoothingKernel::Boxcar(_) => "::ImageBoxcarSmoother::_reportOldNewImageShapes",
            SmoothingKernel::Hanning => "::HanningSmoother::_reportOldNewImageShapes",
        };
        output.append_history_at(
            origin,
            &[
                format!("Original {source_name} size => {:?}", image.shape()),
                format!("New {} size => {:?}", self.outfile, output.shape()),
            ],
        )?;
        output.save().map_err(raised)?;
        Ok(output)
    }
}

impl PyImageTool {
    fn smooth_axis(&self, py: Python<'_>, request: Request<'_>) -> PyResult<Py<Self>> {
        let output = py.detach(|| {
            let state = self.state();
            let attached = state
                .image
                .as_ref()
                .ok_or_else(|| raised("Unable to create image"))?;
            let mut live_coordinates = None;
            let mut output = publish_masked_image(
                request.outfile,
                request.overwrite,
                Some(&request.mask.value),
                |path| {
                    let mut image = attached.lock();
                    image.check_single_precision(request.name())?;
                    if !request.outfile.is_empty() {
                        for input in image
                            .path()
                            .into_iter()
                            .chain(attached.operand_paths().iter().map(|path| path.as_path()))
                        {
                            ensure_outfile_does_not_alias_input(Path::new(request.outfile), input)
                                .map_err(raised)?;
                        }
                    }
                    attached.require_pixel_read_lock(&mut image)?;
                    let beams = attached.beam_view().clone();
                    let staged = request.build(&image, &beams, path, state.history);
                    let released = attached.release_requested_lock(&mut image);
                    let staged = staged?;
                    released?;
                    live_coordinates = AttachedImage::capture_live_coordinates(&staged)?;
                    Ok(staged)
                },
            )?;
            if let Some((record, system)) = live_coordinates {
                output.keep_live_coordinates(record, system);
            }
            Ok::<_, PyErr>(output)
        })?;
        Py::new(py, Self::attached(output))
    }
}

#[pymethods]
impl PyImageTool {
    /// Smooth an axis over consecutive windows, optionally decimating.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", region=RegionArgument::default(), mask=SmoothingMask::boxcar_default(), axis=-1, width=2, drop=true, dmethod="copy", overwrite=false, stretch=false), text_signature = "($self, outfile='', region={}, mask=[], axis=-1, width=2, drop=True, dmethod='copy', overwrite=False, stretch=False)")]
    fn boxcar(
        &self,
        py: Python<'_>,
        outfile: &str,
        region: RegionArgument,
        mask: SmoothingMask,
        axis: i64,
        width: i64,
        #[pyo3(from_py_with = require_bool)] drop: bool,
        dmethod: &str,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
    ) -> PyResult<Py<Self>> {
        let width = usize::try_from(width).map_err(|_| raised("Boxcar width must be positive"))?;
        self.smooth_axis(
            py,
            Request {
                outfile,
                region,
                mask,
                kernel: SmoothingKernel::Boxcar(width),
                axis,
                drop,
                dmethod,
                overwrite,
                stretch,
            },
        )
    }

    /// Apply the three-point Hanning kernel and optionally decimate.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", region=RegionArgument::default(), mask=SmoothingMask::hanning_default(), axis=-10, drop=true, overwrite=false, stretch=false, dmethod="copy"), text_signature = "($self, outfile='', region={}, mask='', axis=-10, drop=True, overwrite=False, stretch=False, dmethod='copy')")]
    fn hanning(
        &self,
        py: Python<'_>,
        outfile: &str,
        region: RegionArgument,
        mask: SmoothingMask,
        axis: i64,
        #[pyo3(from_py_with = require_bool)] drop: bool,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
        dmethod: &str,
    ) -> PyResult<Py<Self>> {
        self.smooth_axis(
            py,
            Request {
                outfile,
                region,
                mask,
                kernel: SmoothingKernel::Hanning,
                axis,
                drop,
                dmethod,
                overwrite,
                stretch,
            },
        )
    }
}
