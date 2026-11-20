// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_coordinates::CoordinateSystem;
use casa_images::{ImageInterface, ImagePixel, image::image_history_rows};
use casa_types::{RecordValue, Value};
use ndarray::{ArrayD, IxDyn, SliceInfoElem, Zip};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString};

use crate::convert::Variant;
use crate::tools::table::casa_record;

use super::beams::{axis_length, image_info, image_info_record, plane_axes, subset_beam_record};
use super::copy_region::Selection;
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};
use super::lifecycle::open_image;
use super::{PyImageTool, stub};

struct CopyImage<'a> {
    method: &'static str,
    infile: &'a str,
    outfile: &'a str,
    region: RecordValue,
    region_history: String,
    mask: String,
    mask_history: String,
    dropdeg: bool,
    overwrite: bool,
}

type PixelSection<T> = (ArrayD<T>, Option<ArrayD<bool>>);

fn slice(start: &[usize], shape: &[usize], stride: &[usize]) -> Vec<SliceInfoElem> {
    start
        .iter()
        .zip(shape)
        .zip(stride)
        .map(|((&start, &count), &stride)| SliceInfoElem::Slice {
            start: start as isize,
            end: Some((start + (count - 1) * stride + 1) as isize),
            step: stride as isize,
        })
        .collect()
}

fn selected_image_info(original: &ImageHandle, selection: &Selection) -> PyResult<RecordValue> {
    let mut info = image_info(original).map_err(raised)?;
    if info.image_type == casa_images::ImageType::Undefined {
        info.image_type = casa_images::ImageType::Intensity;
    }
    let stored = image_info_record(original);
    let mut record = stored.cloned().unwrap_or_else(|| info.to_record());
    let beams = &info.beam_set;
    if !beams.is_multi() {
        return Ok(record);
    }
    let (spectral, stokes) = plane_axes(&original.coordinate_record())?;
    for (axis, planes, diagnostic) in [
        (
            spectral,
            beams.n_channels(),
            "Number of channels is not consistent",
        ),
        (
            stokes,
            beams.n_stokes(),
            "Number of polarizations is not consistent",
        ),
    ] {
        if planes != 1 && planes != axis_length(original, axis)? {
            return Err(raised(diagnostic));
        }
    }
    let indices = |axis: Option<usize>, planes| match axis {
        Some(axis) if planes > 1 => (0..selection.shape[axis])
            .map(|index| selection.start[axis] + index * selection.stride[axis])
            .collect::<Vec<_>>(),
        _ => vec![0],
    };
    let channels = indices(spectral, beams.n_channels());
    let polarizations = indices(stokes, beams.n_stokes());
    if channels.len() == beams.n_channels() && polarizations.len() == beams.n_stokes() {
        return Ok(record);
    }
    subset_beam_record(&mut record, stored, beams, &channels, &polarizations).map_err(raised)?;
    Ok(record)
}

impl CopyImage<'_> {
    fn copy_pixels<T: ImagePixel>(
        selection: &Selection,
        coordinates: CoordinateSystem,
        path: Option<&Path>,
        expression_mask: Option<&ArrayD<bool>>,
        mut read: impl FnMut(&[usize], &[usize], &[usize]) -> PyResult<PixelSection<T>>,
    ) -> PyResult<Storage<T>> {
        let shape = &selection.shape;
        let output_shape = selection.projected(shape);
        let unit_stride = vec![1; output_shape.len()];
        let mut storage =
            Storage::create(output_shape.clone(), coordinates, path, None).map_err(raised)?;
        let total = shape
            .iter()
            .try_fold(1usize, |size, &axis| size.checked_mul(axis))
            .ok_or_else(|| raised("Image size is too large"))?;
        let mut axis = 0;
        let mut prefix = 1;
        let quota = 1_048_576;
        while axis + 1 < shape.len() && shape[axis] <= quota / prefix {
            prefix *= shape[axis];
            axis += 1;
        }
        let mut block_shape = vec![1; shape.len()];
        block_shape[..axis].copy_from_slice(&shape[..axis]);
        let mut offset = 0;
        let mut output_mask: Option<ArrayD<bool>> = None;
        while offset < total {
            let mut position = offset;
            let start = shape
                .iter()
                .map(|length| {
                    let index = position % length;
                    position /= length;
                    index
                })
                .collect::<Vec<_>>();
            block_shape[axis] = (shape[axis] - start[axis]).min(quota / prefix);
            let source = start
                .iter()
                .zip(&selection.start)
                .zip(&selection.stride)
                .map(|((&index, &base), &step)| base + index * step)
                .collect::<Vec<_>>();
            let (pixels, mask) = read(&source, &block_shape, &selection.stride)?;
            let output_start = selection.projected(&start);
            let pixels = selection.squeeze(pixels);
            with_storage!(&mut storage, image => image.put_slice(&pixels, &output_start))
                .map_err(raised)?;
            let expression = expression_mask
                .map(|mask| mask.slice(slice(&source, &block_shape, &selection.stride).as_slice()));
            let mask = match (mask, expression) {
                (Some(mut pixels_valid), Some(passes)) => {
                    Zip::from(&mut pixels_valid)
                        .and(passes)
                        .for_each(|pixel, passes| *pixel &= *passes);
                    Some(pixels_valid)
                }
                (None, Some(passes)) => Some(passes.to_owned()),
                (pixels_valid, None) => pixels_valid,
            };
            if let Some(mask) = mask
                && mask.iter().any(|value| !value)
            {
                let mask = selection.squeeze(mask);
                let output = output_mask
                    .get_or_insert_with(|| ArrayD::from_elem(IxDyn(&output_shape), true));
                output
                    .slice_mut(slice(&output_start, mask.shape(), &unit_stride).as_slice())
                    .assign(&mask);
            }
            offset += block_shape[axis] * prefix;
        }
        if let Some(mask) = output_mask {
            with_storage!(&mut storage, image => image.put_mask("mask0", &mask)).map_err(raised)?;
            with_storage!(&mut storage, image => image.set_default_mask("mask0"))
                .map_err(raised)?;
        }
        Ok(storage)
    }

    fn build(
        &self,
        original: &ImageHandle,
        path: Option<&Path>,
        add_history: bool,
    ) -> PyResult<ImageHandle> {
        let mut selection = Selection::new(original.shape(), &self.region, self.method)?;
        if self.dropdeg {
            selection.drop_degenerate()?;
        }
        let coordinates = original.coordinates();
        let info = selected_image_info(original, &selection)?;
        let record = selection.coordinates(
            original.coordinate_record(),
            coordinates,
            original.shape(),
            self.method,
        )?;
        let coordinates = CoordinateSystem::from_record(&record).map_err(raised)?;
        let mask = if self.mask.is_empty() {
            None
        } else {
            Some(
                casa_images::analysis::evaluate_image_tool_mask(&self.mask, original.shape())
                    .map_err(raised)?,
            )
        };
        macro_rules! copy_native {
            ($storage:expr, $variant:ident) => {
                ImageHandle::$variant(with_storage!($storage, image => {
                    Self::copy_pixels(&selection, coordinates, path, mask.as_ref(), |start, shape, stride| {
                        let pixels = image.get_slice_with_stride(start, shape, stride).map_err(raised)?;
                        let mask = ImageInterface::get_mask_slice(image.as_ref(), start, shape, stride).map_err(raised)?;
                        Ok((pixels, mask))
                    })
                })?)
            };
        }
        let mut copy = if let Some(fits) = original.fits() {
            ImageHandle::Float(Self::copy_pixels(
                &selection,
                coordinates,
                path,
                mask.as_ref(),
                |start, shape, stride| {
                    let pixels = fits.read(start, shape, stride)?;
                    let mask = pixels.mapv(|value| !value.is_nan());
                    Ok((pixels, Some(mask)))
                },
            )?)
        } else {
            match original {
                ImageHandle::Float(storage) => copy_native!(storage, Float),
                ImageHandle::Double(storage) => copy_native!(storage, Double),
                ImageHandle::Complex(storage) => copy_native!(storage, Complex),
                ImageHandle::DComplex(storage) => copy_native!(storage, DComplex),
            }
        };
        let units =
            with_pixels!(original, storage => with_storage!(storage, image => image.units()));
        let misc =
            with_pixels!(original, storage => with_storage!(storage, image => image.misc_info()));
        let history = with_pixels!(original, storage => match storage {
            Storage::Paged(image) => image_history_rows(image.name().expect("native image has a path")).map_err(raised)?,
            Storage::Temp { image, .. } => image.history_rows().to_vec(),
        });
        with_pixels!(&mut copy, storage => with_storage!(storage, image => (|| {
            image.set_units(units)?;
            image.set_image_info_record(info)?;
            image.set_misc_info(misc)?;
            image.append_history_entries(&history)
        })()))
        .map_err(raised)?;
        copy.set_coordinate_record(record)?;
        if add_history {
            let input = if self.method == "fromimage" {
                format!(
                    "outfile={:?}, infile={:?}, region={}, mask={}",
                    self.outfile, self.infile, self.region_history, self.mask_history
                )
            } else {
                format!(
                    "infile={:?}, outfile={:?}, region={{...}}, vmask={}",
                    self.infile, self.outfile, self.mask_history
                )
            };
            copy.record_history(
                self.method,
                format!(
                    "ia.{}({input}, dropdeg={}, overwrite={})",
                    self.method, self.dropdeg, self.overwrite
                ),
            )?;
        }
        copy.save().map_err(raised)?;
        Ok(copy)
    }

    fn run(&self, history: bool) -> PyResult<super::attachment::AttachedImage> {
        let original = open_image(self.infile)?;
        if self.overwrite
            && !self.outfile.is_empty()
            && let Ok(target) = Path::new(self.outfile).canonicalize()
        {
            let source = Path::new(self.infile).canonicalize().map_err(raised)?;
            if target != source && (target.starts_with(&source) || source.starts_with(&target)) {
                return Err(raised(
                    "Cannot overwrite an output that contains the source image or is part of it",
                ));
            }
        }
        publish_image(self.outfile, self.overwrite, |path| {
            let mut image = original.lock();
            original.require_lock(&mut image, casa_tables::LockType::Read)?;
            let copied = self.build(&image, path, history)?;
            image.release_requested_lock()?;
            Ok(copied)
        })
    }
}

fn record_argument(
    py: Python<'_>,
    raw: Option<&Bound<'_, PyAny>>,
    method: &str,
) -> PyResult<(RecordValue, String)> {
    let Some(raw) = raw else {
        return Ok((RecordValue::default(), "[]".into()));
    };
    if let Ok(dict) = raw.cast::<PyDict>() {
        return Ok((casa_record(py, Some(dict))?, "{...}".into()));
    }
    let value: Variant = raw.extract()?;
    if value.is_empty_sequence() {
        return Ok((RecordValue::default(), "[]".into()));
    }
    if let Variant::Text(text) = value {
        if text.is_empty() {
            return Ok((RecordValue::default(), format!("{text:?}")));
        }
        return Err(stub(method).unwrap_err());
    }
    Err(raised("Unsupported type for region"))
}

fn mask_argument(raw: Option<&Bound<'_, PyAny>>) -> PyResult<(String, String)> {
    let Some(raw) = raw else {
        return Ok((String::new(), "\"\"".into()));
    };
    if let Ok(text) = raw.cast::<PyString>() {
        let text = text.to_str()?.to_owned();
        let history = format!("{text:?}");
        return Ok((text, history));
    }
    let value: Variant = raw.extract()?;
    if value.is_empty_sequence() {
        return Ok((String::new(), "[]".into()));
    }
    Err(raised("Mask is not understood, try a valid LEL string"))
}

#[pymethods]
#[allow(clippy::too_many_arguments)]
impl PyImageTool {
    #[pyo3(signature = (outfile="", infile="", region=None, mask=None, dropdeg=false, overwrite=false), text_signature = "($self, outfile='', infile='', region=[], mask='', dropdeg=False, overwrite=False)")]
    fn fromimage(
        &self,
        py: Python<'_>,
        outfile: &str,
        infile: &str,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        dropdeg: bool,
        overwrite: bool,
    ) -> PyResult<bool> {
        let method = "fromimage";
        let (region, region_history) = record_argument(py, region, method)?;
        let (mask, mask_history) = mask_argument(mask)?;
        let request = CopyImage {
            method,
            infile,
            outfile,
            region,
            region_history,
            mask,
            mask_history,
            dropdeg,
            overwrite,
        };
        py.detach(|| {
            let mut state = self.state();
            state.image = Some(request.run(state.history)?);
            Ok(true)
        })
    }

    #[pyo3(signature = (infile="", outfile="", region=None, mask=None, dropdeg=false, overwrite=false), text_signature = "($self, infile='', outfile='', region={}, mask='', dropdeg=False, overwrite=False)")]
    fn newimagefromimage(
        &self,
        py: Python<'_>,
        infile: &str,
        outfile: &str,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        dropdeg: bool,
        overwrite: bool,
    ) -> PyResult<Py<Self>> {
        let method = "newimagefromimage";
        let (region, region_history) = record_argument(py, region, method)?;
        let (mask, mask_history) = mask_argument(mask)?;
        let request = CopyImage {
            method,
            infile,
            outfile,
            region,
            region_history,
            mask,
            mask_history,
            dropdeg,
            overwrite,
        };
        let output = py.detach(|| request.run(self.state().history))?;
        Py::new(py, Self::attached(output))
    }
}
