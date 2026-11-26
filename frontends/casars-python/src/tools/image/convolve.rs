// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_core::{ArrayD, RecordValue};
use casa_imageanalysis::analysis::{
    ConvolutionKernel, ConvolutionOptions, ConvolutionPixel, ImageConvolution,
    ensure_outfile_does_not_alias_input,
};
use casa_images::ImageInterface;
use numpy::{PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use crate::convert::Variant;

use super::PyImageTool;
use super::attachment::{AttachedImage, read_locked};
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised, with_storage};
use super::pixels::snapshot_array;
use super::subimage::{RegionArgument, mask_argument, require_bool};

const INVALID_KERNEL: &str = "kernel is not understood, try using an array or an image";
const KERNEL_PRECISION: &str =
    "Invalid Table data type when accessing column in ArrayColumn ctor for column map";

#[derive(Default)]
enum KernelArgument {
    File {
        path: String,
        history: String,
    },
    Array {
        pixels: ArrayD<f64>,
        history: String,
    },
    #[default]
    Unsupported,
}

impl KernelArgument {
    fn capture(py: Python<'_>, value: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let Some(value) = value else {
            return Ok(Self::Unsupported);
        };
        let converted = if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
            if value.len()? == 0 {
                return Ok(Self::Unsupported);
            }
            match py.import("numpy")?.call_method1("asarray", (value,)) {
                Ok(array) => Some(array),
                Err(_) => return Ok(Self::Unsupported),
            }
        } else {
            None
        };
        let raw = converted.as_ref().unwrap_or(value);
        if let Ok(array) = raw.cast::<PyUntypedArray>()
            && array.ndim() != 0
        {
            let dtype = array.dtype();
            match (dtype.kind(), dtype.itemsize()) {
                (b'f', 4 | 8) => {
                    let pixels = snapshot_array(py, array)?;
                    let history = if pixels.len() > 100 {
                        "\"(...)\"".into()
                    } else {
                        let values = pixels
                            .view()
                            .reversed_axes()
                            .iter()
                            .map(|value| format!("{value:.6}"))
                            .collect::<Vec<_>>();
                        format!("[{}]", values.join(","))
                    };
                    return Ok(Self::Array { pixels, history });
                }
                (b'i' | b'u', _) => {
                    let integers = snapshot_array::<i64>(py, array)?;
                    let history = if integers.len() > 100 {
                        "\"(...)\"".into()
                    } else {
                        let values = integers
                            .view()
                            .reversed_axes()
                            .iter()
                            .map(i64::to_string)
                            .collect::<Vec<_>>();
                        format!("[{}]", values.join(","))
                    };
                    let pixels = integers.mapv(|value| f64::from(value as i32));
                    return Ok(Self::Array { pixels, history });
                }
                _ => {}
            }
        }
        Ok(match value.extract::<Variant>()? {
            Variant::Text(path) => {
                let history = format!("{path:?}");
                Self::File { path, history }
            }
            argument @ Variant::Texts(_) => {
                let path = argument.describe();
                Self::File {
                    history: path.clone(),
                    path,
                }
            }
            _ => Self::Unsupported,
        })
    }

    fn history(&self) -> &str {
        match self {
            Self::File { history, .. } | Self::Array { history, .. } => history,
            Self::Unsupported => unreachable!("validated convolution kernel"),
        }
    }
}

struct Request<'a> {
    outfile: &'a str,
    kernel: &'a KernelArgument,
    scale: f64,
    region: &'a RegionArgument,
    mask: &'a str,
    overwrite: bool,
    stretch: bool,
    history: bool,
}

impl Request<'_> {
    fn run(&self, source: &AttachedImage) -> PyResult<AttachedImage> {
        if !matches!(
            &*source.lock(),
            ImageHandle::Float(_) | ImageHandle::Double(_)
        ) {
            return Err(raised("This method only supports real-valued images"));
        }
        let kernel_path = match self.kernel {
            KernelArgument::File { path, .. } => {
                let path = Path::new(path);
                if !path.is_dir() {
                    return Err(raised(format!("Table {} does not exist", path.display())));
                }
                Some(path)
            }
            KernelArgument::Array { .. } => None,
            KernelArgument::Unsupported => return Err(raised(INVALID_KERNEL)),
        };
        let same = kernel_path
            .and_then(|path| path.canonicalize().ok())
            .is_some_and(|path| source.native_path() == Some(&path));
        let kernel = kernel_path
            .filter(|_| !same)
            .map(AttachedImage::open_for_ordered_read)
            .transpose()?;
        let mut coordinates = None;
        let mut output = publish_image(self.outfile, self.overwrite, |destination| {
            let build = |image: &ImageHandle, kernel: Option<&ImageHandle>| {
                self.build(source, image, kernel, destination)
            };
            let product = if let Some(kernel) = &kernel {
                let source_first = source
                    .native_path()
                    .zip(kernel.native_path())
                    .is_none_or(|(left, right)| left < right);
                if source_first {
                    read_locked(source, |image| {
                        read_locked(kernel, |kernel| build(image, Some(kernel)))
                    })?
                } else {
                    read_locked(kernel, |kernel| {
                        read_locked(source, |image| build(image, Some(kernel)))
                    })?
                }
            } else {
                read_locked(source, |image| build(image, same.then_some(image)))?
            };
            coordinates = AttachedImage::capture_live_coordinates(&product)?;
            Ok(product)
        })?;
        if let Some((record, system)) = coordinates {
            output.keep_live_coordinates(record, system);
        }
        Ok(output)
    }

    fn build(
        &self,
        attachment: &AttachedImage,
        image: &ImageHandle,
        kernel: Option<&ImageHandle>,
        destination: Option<&Path>,
    ) -> PyResult<ImageHandle> {
        if !self.outfile.is_empty() {
            let destination = Path::new(self.outfile);
            for input in image
                .path()
                .into_iter()
                .chain(kernel.and_then(ImageHandle::path))
                .chain(attachment.operand_paths().iter().map(|path| path.as_path()))
            {
                ensure_outfile_does_not_alias_input(destination, input).map_err(raised)?;
            }
        }
        let record = image.coordinate_record();
        let (mut product, changed, dimensions) = match image {
            ImageHandle::Float(storage) => {
                let native = match kernel {
                    Some(ImageHandle::Float(Storage::Paged(native))) => {
                        Some(native.as_ref() as &dyn ImageInterface<f32>)
                    }
                    Some(_) => return Err(raised(KERNEL_PRECISION)),
                    None => None,
                };
                let (out, changed, dimensions) = match storage {
                    Storage::Temp {
                        image,
                        fits: Some(fits),
                        ..
                    } => self.convolve(&fits.image_view(image), record, native, destination),
                    _ => {
                        with_storage!(storage, source => self.convolve(source.as_ref(), record, native, destination))
                    }
                }?;
                (ImageHandle::Float(out), changed, dimensions)
            }
            ImageHandle::Double(storage) => {
                let native = match kernel {
                    Some(ImageHandle::Double(Storage::Paged(native))) => {
                        Some(native.as_ref() as &dyn ImageInterface<f64>)
                    }
                    Some(_) => return Err(raised(KERNEL_PRECISION)),
                    None => None,
                };
                let (out, changed, dimensions) = with_storage!(storage, source => self.convolve(source.as_ref(), record, native, destination))?;
                (ImageHandle::Double(out), changed, dimensions)
            }
            _ => return Err(raised("This method only supports real-valued images")),
        };
        if let Some(record) = changed {
            product.set_coordinate_record(record)?;
        }
        product.append_history_entries(&image.history_entries()?)?;
        if self.history {
            let Self {
                outfile,
                kernel,
                scale,
                region,
                mask,
                overwrite,
                stretch,
                ..
            } = self;
            let kernel = kernel.history();
            let region = region.history();
            product.record_history("convolve", format!("ia.convolve(outfile={outfile:?}, kernel={kernel}, scale={scale:.6}, region={region}, vmask={mask:?}, overwrite={overwrite}, stretch={stretch})"))?;
        }
        if dimensions < image.shape().len() {
            product.append_history_at("ImageConvolverTask::convolve", &["NOTE: kernel with fewer dimensions than the input image applied. Degenerate axes added to the kernel".to_owned()])?;
        }
        product.save().map_err(raised)?;
        Ok(product)
    }

    fn convolve<T: ConvolutionPixel>(
        &self,
        source: &dyn ImageInterface<T>,
        coordinates: RecordValue,
        native: Option<&dyn ImageInterface<T>>,
        path: Option<&Path>,
    ) -> PyResult<(Storage<T>, Option<RecordValue>, usize)> {
        let data;
        let (kernel, rank) = if let Some(native) = native {
            (ConvolutionKernel::Image(native), native.ndim())
        } else {
            let KernelArgument::Array { pixels, .. } = self.kernel else {
                unreachable!("validated convolution kernel")
            };
            data = pixels.mapv(T::from_f64);
            (ConvolutionKernel::Array(&data), data.ndim())
        };
        let plan =
            ImageConvolution::new(source, coordinates, self.region.selection()).map_err(raised)?;
        let mut output = Storage::create(
            plan.section.shape.clone(),
            plan.section.coordinates.clone(),
            path,
            None,
        )
        .map_err(raised)?;
        let options = ConvolutionOptions {
            scale: self.scale,
            mask: Some(self.mask),
            stretch: self.stretch,
            destination: (!self.outfile.is_empty()).then(|| Path::new(self.outfile)),
        };
        with_storage!(&mut output, dest => plan.copy_to(source, kernel, options, dest.as_mut()))
            .map_err(raised)?;
        Ok((output, plan.section.coordinate_record, rank))
    }
}

#[pymethods]
impl PyImageTool {
    /// Linearly convolve a selected real image with an array or native image.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", kernel=None, scale=-1.0, region=RegionArgument::default(), mask=None, overwrite=false, stretch=false), text_signature = "($self, outfile='', kernel=[], scale=-1.0, region={}, mask='', overwrite=False, stretch=False)")]
    fn convolve(
        &self,
        py: Python<'_>,
        outfile: &str,
        kernel: Option<&Bound<'_, PyAny>>,
        scale: f64,
        region: RegionArgument,
        mask: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
    ) -> PyResult<Py<Self>> {
        let kernel = KernelArgument::capture(py, kernel)?;
        let mask = mask_argument(mask)?;
        let output = py.detach(|| {
            let state = self.state();
            let Some(source) = state.image.as_ref() else {
                return Ok(None);
            };
            Request {
                outfile,
                kernel: &kernel,
                scale,
                region: &region,
                mask: &mask,
                overwrite,
                stretch,
                history: state.history,
            }
            .run(source)
            .map(Some)
        })?;
        Py::new(py, output.map_or_else(Self::new, Self::attached))
    }
}
