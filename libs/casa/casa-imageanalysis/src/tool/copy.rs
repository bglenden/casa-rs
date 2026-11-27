// SPDX-License-Identifier: LGPL-3.0-or-later

//! Copying an image or region into a new image (`ia.fromimage`, `ia.newimagefromimage`).

use std::path::{Path, PathBuf};

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_core::RecordValue;
use casa_images::{
    ImageInterface, ImagePixel, MutableImageInterface, PagedImage, image_history_rows,
};
use casa_lattices::TiledShape;
use ndarray::{ArrayD, SliceInfoElem, Zip};

use crate::analysis::imsubimage::{ImageRegionSection, SubimageRegion};

use super::attachment::{AttachedImage, ExternalPathCheck};
use super::beams::{axis_length, image_info, image_info_record, plane_axes, subset_beam_record};
use super::copy_region::Selection;
use super::error::{ImageToolError, ImageToolResult};
use super::handle::{ImageHandle, Storage, with_pixels, with_storage};
use super::pixels::and_mask;
use super::publish::publish_image;
use super::saved_expression;

/// Region specification accepted by [`copy_image`].
pub enum CopyRegion {
    /// Casacore region record (`{...}`, empty means full image).
    Record(RecordValue),
    /// Region string (CRTF text or region filename).
    Text(String),
}

/// Parameters controlling [`copy_image`] (`ia.fromimage` / `ia.newimagefromimage`).
pub struct CopyImageRequest<'a> {
    /// CASA method name (`"fromimage"` or `"newimagefromimage"`).
    pub method: &'static str,
    /// Raw `infile` display name (recorded in history/errors).
    pub infile: &'a str,
    /// Resolved on-disk input path.
    pub resolved_infile: &'a Path,
    /// Destination image path (`""` for a temporary in-memory image).
    pub outfile: &'a str,
    /// Region selector.
    pub region: CopyRegion,
    /// Formatted region string for CASA history.
    pub region_history: String,
    /// LEL mask expression (`""` if none).
    pub mask: String,
    /// Formatted mask string for CASA history.
    pub mask_history: String,
    /// Drop degenerate singleton axes.
    pub dropdeg: bool,
    /// Overwrite `outfile` if it already exists.
    pub overwrite: bool,
    /// Append CASA tool history entries to the output image.
    pub history: bool,
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

fn selected_image_info(
    original: &ImageHandle,
    selection: &Selection,
) -> ImageToolResult<RecordValue> {
    let mut info = image_info(original)?;
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
            return Err(ImageToolError::new(diagnostic));
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
    subset_beam_record(&mut record, stored, beams, &channels, &polarizations)?;
    Ok(record)
}

fn copy_pixels<T: ImagePixel>(
    selection: &Selection,
    coordinates: CoordinateSystem,
    path: Option<&Path>,
    expression_mask: Option<&ArrayD<bool>>,
    region: &ImageRegionSection,
    mut read: impl FnMut(&[usize], &[usize], &[usize]) -> ImageToolResult<PixelSection<T>>,
) -> ImageToolResult<Storage<T>> {
    let shape = &selection.shape;
    let output_shape = selection.projected(shape);
    let mut storage = if let Some(path) = path {
        let tiled = TiledShape::new(output_shape.clone(), T::ELEM_SIZE)?;
        let tile = tiled.tile_shape().to_vec();
        let bytes = 2 * tile.iter().product::<usize>() * T::ELEM_SIZE;
        Storage::Paged(Box::new(PagedImage::create_with_tile_shape_and_cache(
            output_shape,
            tile,
            coordinates,
            path,
            bytes,
        )?))
    } else {
        Storage::create(output_shape, coordinates, None, None)?
    };
    let total = shape
        .iter()
        .try_fold(1usize, |size, &axis| size.checked_mul(axis))
        .ok_or_else(|| ImageToolError::new("Image size is too large"))?;
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
        let mut mask = and_mask(mask, region.mask_window(&start, &block_shape)?);
        let output_start = selection.projected(&start);
        let pixels = selection.squeeze(pixels);
        with_storage!(&mut storage, image => image.put_slice(&pixels, &output_start))?;
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
                with_storage!(&mut storage, image => image.create_filled_mask("mask0", true))?;
                wrote_mask = true;
            }
            with_storage!(&mut storage, image => image.put_mask_slice("mask0", &mask, &output_start))?;
        }
        offset += block_shape[axis] * prefix;
    }
    if wrote_mask {
        with_storage!(&mut storage, image => image.set_default_mask("mask0"))?;
    }
    Ok(storage)
}

fn build_copy(
    request: &CopyImageRequest<'_>,
    original: &ImageHandle,
    path: Option<&Path>,
    origin: Option<&TextOrigin>,
) -> ImageToolResult<ImageHandle> {
    if original.image_type_name() == "MIRIADImage"
        && original.coordinates().n_pixel_axes() != original.shape().len()
    {
        return Err(ImageToolError::new(
            "Failed AlwaysAssert setCoordinateInfo (coordinateInfo)",
        ));
    }
    let section = match &request.region {
        CopyRegion::Record(region) => {
            let region = (!region.is_empty()).then_some(SubimageRegion::Record(region));
            resolve_section(original, region)?
        }
        CopyRegion::Text(text) if text.is_empty() => resolve_section(original, None)?,
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
            }?
        }
    };
    let mut selection = Selection::new(&section);
    if request.dropdeg {
        selection.drop_degenerate()?;
    }
    let coordinates = original.coordinates();
    let info = selected_image_info(original, &selection)?;
    let record =
        selection.coordinates(original.coordinate_record(), coordinates, original.shape())?;
    let coordinates = CoordinateSystem::from_record(&record)?;
    let mask = if request.mask.is_empty() {
        None
    } else {
        let target =
            (request.overwrite && !request.outfile.is_empty()).then(|| Path::new(request.outfile));
        Some(saved_expression::evaluate_mask(
            &request.mask,
            original.shape(),
            target,
        )?)
    };
    macro_rules! copy_native {
        ($storage:expr, $variant:ident) => {
            ImageHandle::$variant(with_storage!($storage, image => {
                copy_pixels(&selection, coordinates, path, mask.as_ref(), &section, |start, shape, stride| {
                    let pixels = image.get_slice_with_stride(start, shape, stride)?;
                    let mask = ImageInterface::get_mask_slice(image.as_ref(), start, shape, stride)?;
                    Ok((pixels, mask))
                })
            })?)
        };
    }
    let mut copy = if let Some(fits) = original.fits() {
        ImageHandle::Float(copy_pixels(
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
    let units = with_pixels!(original, storage => with_storage!(storage, image => image.units()));
    let misc =
        with_pixels!(original, storage => with_storage!(storage, image => image.misc_info()));
    let history = with_pixels!(original, storage => match storage {
        Storage::Paged(image) => image_history_rows(image.name().expect("native image has a path"))?,
        Storage::Temp { image, .. } => image.history_rows()?.into_owned(),
    });
    with_pixels!(&mut copy, storage => with_storage!(storage, image => (|| {
        image.set_units(units)?;
        image.set_image_info_record(info)?;
        image.set_misc_info(misc)?;
        image.append_history_entries(&history)
    })()))?;
    copy.set_coordinate_record(record)?;
    if request.history {
        let input = if request.method == "fromimage" {
            format!(
                "outfile={:?}, infile={:?}, region={}, mask={}",
                request.outfile, request.infile, request.region_history, request.mask_history
            )
        } else {
            format!(
                "infile={:?}, outfile={:?}, region={}, vmask={}",
                request.infile, request.outfile, request.region_history, request.mask_history
            )
        };
        copy.record_history(
            request.method,
            format!(
                "ia.{}({input}, dropdeg={}, overwrite={})",
                request.method, request.dropdeg, request.overwrite
            ),
        )?;
    }
    copy.save()?;
    Ok(copy)
}

fn resolve_section(
    image: &ImageHandle,
    region: Option<SubimageRegion<'_>>,
) -> ImageToolResult<ImageRegionSection> {
    if let ImageHandle::Float(storage) = image {
        Ok(storage.with_float_view(|image| ImageRegionSection::resolve(image, region, false))?)
    } else {
        Ok(
            with_pixels!(image, storage => with_storage!(storage, image =>
            ImageRegionSection::resolve(image.as_ref(), region, false)))?,
        )
    }
}

fn text_origin(
    request: &CopyImageRequest<'_>,
    attached: Option<&AttachedImage>,
) -> ImageToolResult<Option<TextOrigin>> {
    let CopyRegion::Text(text) = &request.region else {
        return Ok(None);
    };
    let use_attached_coordinates = request.method == "fromimage" || request.infile.is_empty();
    if !use_attached_coordinates && text.is_empty() {
        return Ok(None);
    }
    let attached = attached.ok_or_else(|| {
        if use_attached_coordinates {
            ImageToolError::new("No attached image. Cannot use a string value for region")
        } else {
            ImageToolError::new("Logic error at File: src/tools/image/image_cmpt.cc, line 4055")
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

fn open_input(raw: &str, resolved: &Path) -> ImageToolResult<AttachedImage> {
    if raw.is_empty() {
        return Err(ImageToolError::new("Exception: File name is empty."));
    }
    if !resolved.exists() {
        return Err(ImageToolError::new(format!(
            "Exception: File {raw} does not exist.."
        )));
    }
    let abs = std::path::absolute(resolved)?;
    let lexical = casa_tables::lexical_absolute(&abs);
    AttachedImage::open_resolved_with_cache(&lexical, &abs, Path::new(raw), true)
}

/// Copies an image or region according to `request` (`ia.fromimage` / `ia.newimagefromimage`).
pub fn copy_image(
    request: &CopyImageRequest<'_>,
    attached: Option<&AttachedImage>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<AttachedImage> {
    let use_attached_coordinates = request.method == "fromimage" || request.infile.is_empty();
    let (original, origin) = if use_attached_coordinates {
        let origin = text_origin(request, attached)?;
        (open_input(request.infile, request.resolved_infile)?, origin)
    } else {
        let original = open_input(request.infile, request.resolved_infile)?;
        if matches!(&request.region, CopyRegion::Text(_))
            && !matches!(&*original.lock(), ImageHandle::Float(_))
        {
            return Err(ImageToolError::new(format!(
                "Logic Error: {} has a different data type than the data type of the requested object",
                request.infile
            )));
        }
        (original, text_origin(request, attached)?)
    };
    if request.overwrite
        && !request.outfile.is_empty()
        && let Ok(target) = Path::new(request.outfile).canonicalize()
    {
        let source = original
            .lock()
            .path()
            .ok_or_else(|| ImageToolError::new("The input image has no file name"))?
            .canonicalize()?;
        if target != source && (target.starts_with(&source) || source.starts_with(&target)) {
            return Err(ImageToolError::new(
                "Cannot overwrite an output that contains the source image or is part of it",
            ));
        }
        if original
            .operand_paths()
            .iter()
            .any(|path| target.starts_with(path) || path.starts_with(&target))
        {
            return Err(ImageToolError::new(
                "Cannot overwrite an output that overlaps a saved image expression operand",
            ));
        }
    }
    let mut live_coordinates = None;
    let mut output = publish_image(
        request.outfile,
        request.overwrite,
        is_external_open,
        |path| {
            let mut image = original.lock();
            original.require_lock(&mut image, casa_tables::LockType::Read)?;
            let copied = build_copy(request, &image, path, origin.as_ref())?;
            image.release_requested_lock()?;
            if copied
                .coordinates()
                .find_coordinate(CoordinateType::Tabular)
                .is_some()
            {
                live_coordinates = AttachedImage::capture_live_coordinates(&copied)?;
            }
            Ok(copied)
        },
    )?;
    if let Some((record, system)) = live_coordinates {
        output.keep_live_coordinates(record, system);
    }
    Ok(output)
}
