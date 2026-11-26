// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::{Path, PathBuf};

use casa_coordinates::{
    CoordinateModel, CoordinateSystem, StokesCoordinate, StokesType, TabularCoordinate, defaults,
};
use casa_core::{RecordValue, ScalarValue, Value};
use casa_imageanalysis::analysis::ensure_outfile_does_not_alias_input;
use casa_images::image_history_rows;
use casa_images::{ImageInterface, ImagePixel};
use ndarray::{ArrayD, Axis, Dimension, IxDyn, indices};
use pyo3::prelude::*;

use crate::tools::coordsys::{append_coordinate, validate_coordinate_indices};
use crate::tools::{LogLine, emit, not_yet_implemented_in};

use super::PyImageTool;
use super::beams::image_info_record;
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};
use super::masks::validate_name;

const COPY_BLOCK_PIXELS: usize = 1_048_576;

struct AddAxes<'a> {
    outfile: &'a str,
    direction: bool,
    spectral: bool,
    stokes: &'a str,
    linear: bool,
    tabular: bool,
    overwrite: bool,
    silent: bool,
}

impl AddAxes<'_> {
    fn coordinates(&self, record: &mut RecordValue, ndim: usize) -> PyResult<CoordinateSystem> {
        let expected = validate_coordinate_indices(record)?;
        let mut coordinates = CoordinateSystem::from_record(record).map_err(raised)?;
        let original_count = coordinates.n_coordinates();
        if original_count != expected || coordinates.n_pixel_axes() != ndim {
            return Err(raised("Image coordinates do not describe all pixel axes"));
        }
        // New image records use CASA's typed field names. Translate the
        // older generic spelling without rebuilding coordinate metadata.
        for placement in CoordinateSystem::record_coordinates(record).map_err(raised)? {
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
                    return Err(raised(format!("Image already contains a {kind}Coordinate")));
                }
            } else {
                append_coordinate(&mut coordinates, record, coordinate)?;
            }
        }
        if coordinates.n_coordinates() == original_count && !self.silent {
            return Err(raised("Exception: No degenerate axes specified."));
        }
        Ok(coordinates)
    }

    fn history(&self) -> String {
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

fn expand<T>(mut block: ArrayD<T>, ndim: usize) -> ArrayD<T> {
    while block.ndim() < ndim {
        let axis = block.ndim();
        block = block.insert_axis(Axis(axis));
    }
    block
}

fn copy_pixels<T: ImagePixel>(
    source: &dyn ImageInterface<T>,
    input_shape: &[usize],
    output: &mut Storage<T>,
    output_ndim: usize,
    read: impl Fn(&dyn ImageInterface<T>, &[usize], &[usize], &[usize]) -> PyResult<ArrayD<T>>,
) -> PyResult<()> {
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
        validate_name(name)?;
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
        with_storage!(output, image => image.put_slice(&pixels, &position)).map_err(raised)?;
        for name in &names {
            let block = source
                .get_named_mask_slice(name, &start, &lengths, &stride)
                .map_err(raised)?;
            let block = expand(block, output_ndim);
            with_storage!(output, image => image.put_mask_slice(name, &block, &position))
                .map_err(raised)?;
        }
    }
    if let Some(default) = source.default_mask_name() {
        with_storage!(output, image => image.set_default_mask(&default)).map_err(raised)?;
    }
    Ok(())
}

fn native_pixels<T: ImagePixel>(
    source: &dyn ImageInterface<T>,
    start: &[usize],
    lengths: &[usize],
    stride: &[usize],
) -> PyResult<ArrayD<T>> {
    source.get_slice(start, lengths, stride).map_err(raised)
}

fn copy_storage<T: ImagePixel>(
    source: &Storage<T>,
    path: Option<&Path>,
    input_shape: &[usize],
    shape: &[usize],
    coordinates: &CoordinateSystem,
    read: impl Fn(&dyn ImageInterface<T>, &[usize], &[usize], &[usize]) -> PyResult<ArrayD<T>>,
) -> PyResult<Storage<T>> {
    if let Storage::Paged(image) = source
        && let Some(groups) = image.table().keywords().get("ATTRGROUPS")
        && !matches!(groups, Value::Record(groups) if groups.is_empty())
    {
        return not_yet_implemented_in("image", "adddegaxes with attribute groups", 1625);
    }
    let mut output =
        Storage::create(shape.to_vec(), coordinates.clone(), path, None).map_err(raised)?;
    with_storage!(source, original => {
        copy_pixels(original.as_ref(), input_shape, &mut output, shape.len(), read)?;
        with_storage!(&mut output, image => {
            image.set_units(original.units()).map_err(raised)?;
            image.set_misc_info(original.misc_info()).map_err(raised)
        })
    })?;
    let rows = match source {
        Storage::Paged(image) => {
            image_history_rows(image.name().expect("persistent image")).map_err(raised)?
        }
        Storage::Temp { image, .. } => image.history_rows().map_err(raised)?.into_owned(),
    };
    with_storage!(&mut output, image => image.append_history_entries(&rows)).map_err(raised)?;
    Ok(output)
}

#[pymethods]
impl PyImageTool {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", direction=false, spectral=false, stokes="", linear=false, tabular=false, overwrite=false, silent=false), text_signature = "($self, outfile='', direction=False, spectral=False, stokes='', linear=False, tabular=False, overwrite=False, silent=False)")]
    fn adddegaxes(
        &self,
        py: Python<'_>,
        outfile: &str,
        direction: bool,
        spectral: bool,
        stokes: &str,
        linear: bool,
        tabular: bool,
        overwrite: bool,
        silent: bool,
    ) -> PyResult<Py<Self>> {
        let request = AddAxes {
            outfile,
            direction,
            spectral,
            stokes,
            linear,
            tabular,
            overwrite,
            silent,
        };
        let mut logs = Vec::new();
        let result: PyResult<Option<_>> = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(None);
            };
            if !outfile.is_empty() && !overwrite && Path::new(outfile).symlink_metadata().is_ok() {
                return Err(raised(format!(
                    "File {outfile} exists, and the user does not want to remove it."
                )));
            }
            publish_image(outfile, overwrite, |path| {
                let mut original = attachment.lock();
                if !outfile.is_empty() {
                    for source in original
                        .path()
                        .into_iter()
                        .chain(attachment.operand_paths().iter().map(PathBuf::as_path))
                    {
                        ensure_outfile_does_not_alias_input(Path::new(outfile), source)
                            .map_err(raised)?;
                    }
                }
                attachment.require_pixel_read_lock(&mut original)?;
                let mut record = original.coordinate_record();
                let input_shape = original.shape();
                let coordinates = request.coordinates(&mut record, input_shape.len())?;
                let mut image_info = image_info_record(&original).cloned().unwrap_or_default();
                attachment.beam_view().apply_to(&mut image_info);
                let mut shape = input_shape.to_vec();
                shape.resize(coordinates.n_pixel_axes(), 1);
                let target = if outfile.is_empty() {
                    "(temp)image".to_owned()
                } else {
                    format!("image '{outfile}'")
                };
                logs.push(LogLine::info(format!(
                    "Creating {target} of shape {shape:?}"
                )));
                let mut copy = match &*original {
                    ImageHandle::Float(source) => ImageHandle::Float(copy_storage(
                        source,
                        path,
                        input_shape,
                        &shape,
                        &coordinates,
                        |source, start, lengths, stride| match original.fits() {
                            // CASA copies the FITS values, including NaN, but the implicit FITS mask has no name to copy.
                            Some(fits) => fits.read(start, lengths, stride),
                            None => native_pixels(source, start, lengths, stride),
                        },
                    )?),
                    ImageHandle::Double(source) => ImageHandle::Double(copy_storage(
                        source,
                        path,
                        input_shape,
                        &shape,
                        &coordinates,
                        native_pixels,
                    )?),
                    ImageHandle::Complex(source) => ImageHandle::Complex(copy_storage(
                        source,
                        path,
                        input_shape,
                        &shape,
                        &coordinates,
                        native_pixels,
                    )?),
                    ImageHandle::DComplex(source) => ImageHandle::DComplex(copy_storage(
                        source,
                        path,
                        input_shape,
                        &shape,
                        &coordinates,
                        native_pixels,
                    )?),
                };
                copy.set_coordinate_record(record)?;
                if !image_info.is_empty() {
                    with_pixels!(&mut copy, storage => with_storage!(storage, image =>
                        image.set_image_info_record(image_info)
                    ))
                    .map_err(raised)?;
                }
                if state.history {
                    copy.record_history("adddegaxes", request.history())?;
                }
                copy.save().map_err(raised)?;
                Ok(copy)
            })
            .map(Some)
        });
        emit(py, "casars.tools.image", &logs)?;
        Py::new(py, result?.map_or_else(Self::new, Self::attached))
    }
}
