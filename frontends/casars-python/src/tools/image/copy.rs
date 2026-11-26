// SPDX-License-Identifier: LGPL-3.0-or-later

use std::borrow::Cow;
use std::path::{Path, PathBuf};

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_images::analysis::{ImageRegionSection, SubimageRegion};
use casa_images::{
    ImageInterface, ImagePixel, MutableImageInterface, PagedImage, image::image_history_rows,
};
use casa_lattices::TiledShape;
use casa_types::RecordValue;
use ndarray::{ArrayD, SliceInfoElem, Zip};
use numpy::{PyArrayDyn, PyArrayMethods};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString};

use crate::convert::Variant;
use crate::tools::table::casa_record;

use super::PyImageTool;
use super::attachment::AttachedImage;
use super::beams::{axis_length, image_info, image_info_record, plane_axes, subset_beam_record};
use super::copy_region::Selection;
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};
use super::lifecycle::{open_image, resolve_image_path};
use super::pixels::and_mask;
use super::region_read::{is_boolean_sequence, resolve_region_section};

struct CopyImage<'a> {
    method: &'static str,
    infile: &'a str,
    outfile: &'a str,
    region: CopyRegion,
    region_history: String,
    mask: String,
    mask_history: String,
    dropdeg: bool,
    overwrite: bool,
}

enum CopyRegion {
    Record(RecordValue),
    Text(String),
}

struct TextOrigin {
    coordinates: Option<(CoordinateSystem, Vec<usize>)>,
    table: Option<PathBuf>,
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
        region: &ImageRegionSection,
        mut read: impl FnMut(&[usize], &[usize], &[usize]) -> PyResult<PixelSection<T>>,
    ) -> PyResult<Storage<T>> {
        let shape = &selection.shape;
        let output_shape = selection.projected(shape);
        let mut storage = if let Some(path) = path {
            let tiled = TiledShape::new(output_shape.clone(), T::ELEM_SIZE).map_err(raised)?;
            let tile = tiled.tile_shape().to_vec();
            let bytes = 2 * tile.iter().product::<usize>() * T::ELEM_SIZE;
            Storage::Paged(Box::new(
                PagedImage::create_with_tile_shape_and_cache(
                    output_shape,
                    tile,
                    coordinates,
                    path,
                    bytes,
                )
                .map_err(raised)?,
            ))
        } else {
            Storage::create(output_shape, coordinates, None, None).map_err(raised)?
        };
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
        let mut wrote_mask = false;
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
            let mut mask = and_mask(
                mask,
                region.mask_window(&start, &block_shape).map_err(raised)?,
            );
            let output_start = selection.projected(&start);
            let pixels = selection.squeeze(pixels);
            with_storage!(&mut storage, image => image.put_slice(&pixels, &output_start))
                .map_err(raised)?;
            let expression = expression_mask
                .map(|mask| mask.slice(slice(&source, &block_shape, &selection.stride).as_slice()));
            if let Some(passes) = expression {
                if let Some(pixels_valid) = mask.as_mut() {
                    Zip::from(pixels_valid)
                        .and(passes)
                        .for_each(|pixel, passes| *pixel &= *passes);
                } else {
                    mask = Some(passes.to_owned());
                }
            }
            if let Some(mask) = mask
                && mask.iter().any(|value| !value)
            {
                let mask = selection.squeeze(mask);
                if !wrote_mask {
                    with_storage!(&mut storage, image => image.create_filled_mask("mask0", true))
                        .map_err(raised)?;
                    wrote_mask = true;
                }
                with_storage!(&mut storage, image => image.put_mask_slice("mask0", &mask, &output_start))
                    .map_err(raised)?;
            }
            offset += block_shape[axis] * prefix;
        }
        if wrote_mask {
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
        origin: Option<&TextOrigin>,
    ) -> PyResult<ImageHandle> {
        if original.image_type_name() == "MIRIADImage"
            && original.coordinates().n_pixel_axes() != original.shape().len()
        {
            return Err(raised(
                "Failed AlwaysAssert setCoordinateInfo (coordinateInfo)",
            ));
        }
        let section = match &self.region {
            CopyRegion::Record(region) => {
                let region = (!region.is_empty()).then_some(SubimageRegion::Record(region));
                resolve_region_section(original, region, false)?
            }
            CopyRegion::Text(text) if text.is_empty() => {
                resolve_region_section(original, None, false)?
            }
            CopyRegion::Text(text) => {
                let origin = origin.expect("text origin resolved before copying");
                let coordinates = origin
                    .coordinates
                    .as_ref()
                    .map(|(coords, shape)| (coords, shape.as_slice()));
                let table = origin.table.as_deref();
                if let ImageHandle::Float(storage) = original {
                    storage.with_float_view(|image| {
                        ImageRegionSection::resolve_copy_text(image, text, coordinates, table)
                    })
                } else {
                    with_pixels!(original, storage => with_storage!(storage, image =>
                        ImageRegionSection::resolve_copy_text(image.as_ref(), text, coordinates, table)))
                }
                .map_err(raised)?
            }
        };
        let mut selection = Selection::new(&section);
        if self.dropdeg {
            selection.drop_degenerate()?;
        }
        let coordinates = original.coordinates();
        let info = selected_image_info(original, &selection)?;
        let record =
            selection.coordinates(original.coordinate_record(), coordinates, original.shape())?;
        let coordinates = CoordinateSystem::from_record(&record).map_err(raised)?;
        let mask = if self.mask.is_empty() {
            None
        } else {
            let target =
                (self.overwrite && !self.outfile.is_empty()).then(|| Path::new(self.outfile));
            Some(super::saved_expression::evaluate_mask(
                &self.mask,
                original.shape(),
                target,
            )?)
        };
        macro_rules! copy_native {
            ($storage:expr, $variant:ident) => {
                ImageHandle::$variant(with_storage!($storage, image => {
                    Self::copy_pixels(&selection, coordinates, path, mask.as_ref(), &section, |start, shape, stride| {
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
                &section,
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
            Storage::Temp { image, .. } => image.history_rows().map_err(raised)?.into_owned(),
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
                    "infile={:?}, outfile={:?}, region={}, vmask={}",
                    self.infile, self.outfile, self.region_history, self.mask_history
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

    fn text_origin(
        &self,
        attached: Option<&super::attachment::AttachedImage>,
    ) -> PyResult<Option<TextOrigin>> {
        let CopyRegion::Text(text) = &self.region else {
            return Ok(None);
        };
        let use_attached_coordinates = self.method == "fromimage" || self.infile.is_empty();
        if !use_attached_coordinates && text.is_empty() {
            return Ok(None);
        }
        let attached = attached.ok_or_else(|| {
            if use_attached_coordinates {
                raised("No attached image. Cannot use a string value for region")
            } else {
                raised("Logic error at File: src/tools/image/image_cmpt.cc, line 4055")
            }
        })?;
        if text.is_empty() {
            return Ok(None);
        }
        let image = attached.lock();
        Ok(Some(TextOrigin {
            coordinates: use_attached_coordinates
                .then(|| (image.coordinates().clone(), image.shape().to_vec())),
            table: image.path().map(Path::to_path_buf),
        }))
    }

    fn run(
        &self,
        expanded: PyResult<Cow<'_, str>>,
        history: bool,
        attached: Option<&super::attachment::AttachedImage>,
    ) -> PyResult<super::attachment::AttachedImage> {
        let use_attached_coordinates = self.method == "fromimage" || self.infile.is_empty();
        let (original, origin) = if use_attached_coordinates {
            let origin = self.text_origin(attached)?;
            (open_image(self.infile, expanded?.as_ref())?, origin)
        } else {
            let original = open_image(self.infile, expanded?.as_ref())?;
            if matches!(&self.region, CopyRegion::Text(_))
                && !matches!(&*original.lock(), ImageHandle::Float(_))
            {
                return Err(raised(format!(
                    "Logic Error: {} has a different data type than the data type of the requested object",
                    self.infile
                )));
            }
            (original, self.text_origin(attached)?)
        };
        if self.overwrite
            && !self.outfile.is_empty()
            && let Ok(target) = Path::new(self.outfile).canonicalize()
        {
            let source = original
                .lock()
                .path()
                .ok_or_else(|| raised("The input image has no file name"))?
                .canonicalize()
                .map_err(raised)?;
            if target != source && (target.starts_with(&source) || source.starts_with(&target)) {
                return Err(raised(
                    "Cannot overwrite an output that contains the source image or is part of it",
                ));
            }
            if original
                .operand_paths()
                .iter()
                .any(|path| target.starts_with(path) || path.starts_with(&target))
            {
                return Err(raised(
                    "Cannot overwrite an output that overlaps a saved image expression operand",
                ));
            }
        }
        let mut live_coordinates = None;
        let mut output = publish_image(self.outfile, self.overwrite, |path| {
            let mut image = original.lock();
            original.require_lock(&mut image, casa_tables::LockType::Read)?;
            let copied = self.build(&image, path, history, origin.as_ref())?;
            image.release_requested_lock()?;
            if copied
                .coordinates()
                .find_coordinate(CoordinateType::Tabular)
                .is_some()
            {
                live_coordinates = AttachedImage::capture_live_coordinates(&copied)?;
            }
            Ok(copied)
        })?;
        if let Some((record, system)) = live_coordinates {
            output.keep_live_coordinates(record, system);
        }
        Ok(output)
    }
}

fn copy_region_argument(
    py: Python<'_>,
    raw: Option<&Bound<'_, PyAny>>,
    default_history: &str,
) -> PyResult<(CopyRegion, String)> {
    let empty_record = || CopyRegion::Record(RecordValue::default());
    let Some(raw) = raw else {
        return Ok((empty_record(), default_history.into()));
    };
    if let Ok(dict) = raw.cast::<PyDict>() {
        return Ok((
            CopyRegion::Record(casa_record(py, Some(dict))?),
            "{...}".into(),
        ));
    }
    if let Ok(text) = raw.cast::<PyString>() {
        let text = text.to_str()?.to_owned();
        let history = format!("{text:?}");
        return Ok((CopyRegion::Text(text), history));
    }
    if is_boolean_sequence(raw)? {
        let values = if let Ok(array) = raw.cast::<PyArrayDyn<bool>>() {
            array
                .readonly()
                .as_array()
                .reversed_axes()
                .iter()
                .copied()
                .collect()
        } else {
            raw.extract::<Vec<bool>>()?
        };
        let history = values
            .into_iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",");
        return Ok((empty_record(), format!("[{history}]")));
    }
    if raw.extract::<Variant>()?.is_empty_sequence() {
        return Ok((empty_record(), "[]".into()));
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
        let (region, region_history) = copy_region_argument(py, region, "[]")?;
        let (mask, mask_history) = mask_argument(mask)?;
        let expanded = resolve_image_path(py, infile);
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
            state.image = Some(request.run(expanded, state.history, state.image.as_ref())?);
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
        let (region, region_history) = copy_region_argument(py, region, "{...}")?;
        let (mask, mask_history) = mask_argument(mask)?;
        let expanded = resolve_image_path(py, infile);
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
        let output = py.detach(|| {
            let state = self.state();
            request.run(expanded, state.history, state.image.as_ref())
        })?;
        Py::new(py, Self::attached(output))
    }
}
