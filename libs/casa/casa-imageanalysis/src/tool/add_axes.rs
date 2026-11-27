// SPDX-License-Identifier: LGPL-3.0-or-later

//! Adding missing degenerate direction, spectral, stokes, linear, or tabular axes (`ia.adddegaxes`).

use std::path::{Path, PathBuf};

use casa_coordinates::{
    CoordinateModel, CoordinateSystem, StokesCoordinate, StokesType, TabularCoordinate, defaults,
};
use casa_core::{RecordValue, ScalarValue, Value};
use casa_images::{ImageInterface, ImagePixel, image_history_rows};
use ndarray::{ArrayD, Axis, Dimension, IxDyn, indices};

use super::attachment::{AttachedImage, ExternalPathCheck, ensure_outfile_does_not_alias_input};
use super::beams::image_info_record;
use super::coords::{append_coordinate, validate_coordinate_indices};
use super::error::{ImageToolError, ImageToolResult, ToolLogLine};
use super::handle::{ImageHandle, Storage, with_storage};
use super::masks::validate_mask_name;
use super::publish::publish_image;

const COPY_BLOCK_PIXELS: usize = 1_048_576;

/// Parameters specifying which degenerate axes to append to an image.
#[derive(Debug, Clone)]
pub struct AddDegAxesRequest<'a> {
    /// Output image path (empty creates a temporary in-memory image).
    pub outfile: &'a str,
    /// Append a degenerate direction coordinate (RA/Dec).
    pub direction: bool,
    /// Append a degenerate spectral axis.
    pub spectral: bool,
    /// Stokes polarization string to append when non-empty (e.g. `"I"`).
    pub stokes: &'a str,
    /// Append a degenerate linear axis.
    pub linear: bool,
    /// Append a degenerate tabular axis.
    pub tabular: bool,
    /// Overwrite `outfile` if it exists.
    pub overwrite: bool,
    /// Suppress erroring when requested coordinate already exists or no axes are added.
    pub silent: bool,
}

impl<'a> AddDegAxesRequest<'a> {
    /// Extends `record` with any requested missing coordinates and parses the resulting [`CoordinateSystem`].
    pub fn build_coordinates(
        &self,
        record: &mut RecordValue,
        ndim: usize,
    ) -> ImageToolResult<CoordinateSystem> {
        let expected = validate_coordinate_indices(record)?;
        let mut coordinates = CoordinateSystem::from_record(record)?;
        let original_count = coordinates.n_coordinates();
        if original_count != expected || coordinates.n_pixel_axes() != ndim {
            return Err(ImageToolError::new(
                "Image coordinates do not describe all pixel axes",
            ));
        }
        // New image records use CASA's typed field names. Translate the
        // older generic spelling without rebuilding coordinate metadata.
        for placement in CoordinateSystem::record_coordinates(record)? {
            if let Some(index) = placement.field.strip_prefix("coordinate")
                && let Some(Value::Record(axis)) = record.get(&placement.field)
                && let Some(Value::Scalar(ScalarValue::String(kind))) = axis.get("coordinate_type")
            {
                let native_field = format!("{}{index}", kind.to_ascii_lowercase());
                let axis = axis.clone();
                record.remove(&placement.field);
                record.upsert(native_field, axis.into());
            }
        }
        let added: [Option<CoordinateModel>; 5] = [
            self.direction
                .then(|| defaults::direction_coordinate().into()),
            self.spectral
                .then(|| defaults::spectral_coordinate().into()),
            (!self.stokes.is_empty()).then(|| {
                let stokes = StokesType::from_name(self.stokes).unwrap_or(StokesType::Undefined);
                StokesCoordinate::new(vec![stokes]).into()
            }),
            self.linear
                .then(|| defaults::linear_coordinate(&["Axis1".into()], &[]).into()),
            self.tabular
                .then(|| TabularCoordinate::linear(0.0, 1.0, 0.0, "Tabular", "").into()),
        ];
        for coordinate in added.into_iter().flatten() {
            let kind = coordinate.coordinate_type();
            if coordinates.find_coordinate(kind).is_some() {
                if !self.silent {
                    return Err(ImageToolError::new(format!(
                        "Image already contains a {kind}Coordinate"
                    )));
                }
            } else {
                append_coordinate(&mut coordinates, record, coordinate)?;
            }
        }
        if coordinates.n_coordinates() == original_count && !self.silent {
            return Err(ImageToolError::new(
                "Exception: No degenerate axes specified.",
            ));
        }
        Ok(coordinates)
    }

    fn history_command(&self) -> String {
        format!(
            "ia.adddegaxes(outfile={:?}, direction={}, spectral={}, stokes={:?}, linear={}, tabular={}, overwrite={}, silent={})",
            self.outfile,
            self.direction,
            self.spectral,
            self.stokes,
            self.linear,
            self.tabular,
            self.overwrite,
            self.silent
        )
    }
}

/// Appends requested degenerate axes to `attached`, returning a new [`AttachedImage`] and logs.
pub fn add_degenerate_axes(
    attached: &AttachedImage,
    request: &AddDegAxesRequest<'_>,
    record_history: bool,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<(AttachedImage, Vec<ToolLogLine>)> {
    if !request.outfile.is_empty()
        && !request.overwrite
        && Path::new(request.outfile).symlink_metadata().is_ok()
    {
        return Err(ImageToolError::new(format!(
            "File {} exists, and the user does not want to remove it.",
            request.outfile
        )));
    }
    let mut logs = Vec::new();
    let handle = publish_image(
        request.outfile,
        request.overwrite,
        is_external_open,
        |path| {
            let mut original = attached.lock();
            if !request.outfile.is_empty() {
                for source in original
                    .path()
                    .into_iter()
                    .chain(attached.operand_paths().iter().map(PathBuf::as_path))
                {
                    ensure_outfile_does_not_alias_input(Path::new(request.outfile), source)?;
                }
            }
            attached.require_pixel_read_lock(&mut original)?;
            let mut record = original.coordinate_record();
            let input_shape = original.shape().to_vec();
            let coordinates = request.build_coordinates(&mut record, input_shape.len())?;
            let mut image_info = image_info_record(&original).cloned().unwrap_or_default();
            attached.beam_view().apply_to(&mut image_info);
            let mut shape = input_shape.clone();
            shape.resize(coordinates.n_pixel_axes(), 1);
            let target = if request.outfile.is_empty() {
                "(temp)image".to_owned()
            } else {
                format!("image '{}'", request.outfile)
            };
            logs.push(ToolLogLine::info(format!(
                "Creating {target} of shape {shape:?}"
            )));

            let mut copy = match &*original {
                ImageHandle::Float(source) => ImageHandle::Float(copy_storage(
                    source,
                    path,
                    &input_shape,
                    &shape,
                    &coordinates,
                    |src, start, lengths, stride| match original.fits() {
                        Some(fits) => Ok(fits.read(start, lengths, stride)?),
                        None => native_pixels(src, start, lengths, stride),
                    },
                )?),
                ImageHandle::Double(source) => ImageHandle::Double(copy_storage(
                    source,
                    path,
                    &input_shape,
                    &shape,
                    &coordinates,
                    native_pixels,
                )?),
                ImageHandle::Complex(source) => ImageHandle::Complex(copy_storage(
                    source,
                    path,
                    &input_shape,
                    &shape,
                    &coordinates,
                    native_pixels,
                )?),
                ImageHandle::DComplex(source) => ImageHandle::DComplex(copy_storage(
                    source,
                    path,
                    &input_shape,
                    &shape,
                    &coordinates,
                    native_pixels,
                )?),
            };
            copy.set_coordinate_record(record)?;
            if !image_info.is_empty() {
                copy.set_image_info_record(image_info)?;
            }
            if record_history {
                copy.record_history("adddegaxes", request.history_command())?;
            }
            copy.save()?;
            Ok(copy)
        },
    )?;
    Ok((handle, logs))
}

fn native_pixels<T: ImagePixel>(
    source: &dyn ImageInterface<T>,
    start: &[usize],
    lengths: &[usize],
    stride: &[usize],
) -> ImageToolResult<ArrayD<T>> {
    Ok(source.get_slice(start, lengths, stride)?)
}

fn expand<T>(mut block: ArrayD<T>, ndim: usize) -> ArrayD<T> {
    while block.ndim() < ndim {
        let axis = block.ndim();
        block = block.insert_axis(Axis(axis));
    }
    block
}

fn copy_storage<T: ImagePixel>(
    source: &Storage<T>,
    path: Option<&Path>,
    input_shape: &[usize],
    shape: &[usize],
    coordinates: &CoordinateSystem,
    read: impl Fn(&dyn ImageInterface<T>, &[usize], &[usize], &[usize]) -> ImageToolResult<ArrayD<T>>,
) -> ImageToolResult<Storage<T>> {
    if let Storage::Paged(image) = source
        && let Some(groups) = image.table().keywords().get("ATTRGROUPS")
        && !matches!(groups, Value::Record(groups) if groups.is_empty())
    {
        return Err(ImageToolError::not_implemented(
            "image",
            "adddegaxes with attribute groups",
            1625,
        ));
    }
    let mut output = Storage::create(shape.to_vec(), coordinates.clone(), path, None)?;
    with_storage!(source, original => {
        copy_pixels(original.as_ref(), input_shape, &mut output, shape.len(), read)?;
        with_storage!(&mut output, image => {
            image.set_units(original.units())?;
            image.set_misc_info(original.misc_info())?;
            Ok::<_, ImageToolError>(())
        })?;
        Ok::<_, ImageToolError>(())
    })?;
    let rows = match source {
        Storage::Paged(image) => image_history_rows(image.name().expect("persistent image"))?,
        Storage::Temp { image, .. } => image.history_rows()?.into_owned(),
    };
    with_storage!(&mut output, image => image.append_history_entries(&rows))?;
    Ok(output)
}

fn copy_pixels<T: ImagePixel>(
    source: &dyn ImageInterface<T>,
    input_shape: &[usize],
    output: &mut Storage<T>,
    output_ndim: usize,
    read: impl Fn(&dyn ImageInterface<T>, &[usize], &[usize], &[usize]) -> ImageToolResult<ArrayD<T>>,
) -> ImageToolResult<()> {
    let mut capacity = COPY_BLOCK_PIXELS;
    let chunk_shape: Vec<_> = input_shape
        .iter()
        .map(|length| {
            let extent = capacity.min(*length).max(1);
            capacity /= extent;
            extent
        })
        .collect();
    let names = source.mask_names();
    for name in &names {
        validate_mask_name(name)?;
    }
    let stride = vec![1; input_shape.len()];
    // Walk the first pixel axis fastest, matching the stored array order.
    let grid: Vec<_> = input_shape
        .iter()
        .zip(&chunk_shape)
        .rev()
        .map(|(&length, &extent)| length.div_ceil(extent))
        .collect();
    for index in indices(IxDyn(&grid)) {
        let start: Vec<_> = index
            .slice()
            .iter()
            .rev()
            .zip(&chunk_shape)
            .map(|(index, extent)| index * extent)
            .collect();
        let lengths: Vec<_> = input_shape
            .iter()
            .zip(&start)
            .zip(&chunk_shape)
            .map(|((&length, &start), &extent)| extent.min(length - start))
            .collect();
        let block = read(source, &start, &lengths, &stride)?;
        let mut position = start.clone();
        position.resize(output_ndim, 0);
        let pixels = expand(block, output_ndim);
        with_storage!(output, image => image.put_slice(&pixels, &position))?;
        for name in &names {
            let block = source.get_named_mask_slice(name, &start, &lengths, &stride)?;
            let block = expand(block, output_ndim);
            with_storage!(output, image => image.put_mask_slice(name, &block, &position))?;
        }
    }
    if let Some(default) = source.default_mask_name() {
        with_storage!(output, image => image.set_default_mask(&default))?;
    }
    Ok(())
}
