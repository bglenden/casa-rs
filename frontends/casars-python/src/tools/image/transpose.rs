// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::{Path, PathBuf};

use casa_coordinates::{Coordinate, CoordinateModel, CoordinateSystem, CoordinateType};
use casa_core::quanta::casacore::casacore_unit;
use casa_core::{ArrayValue, RecordValue, Value};
use casa_imageanalysis::analysis::{
    copy_transposed_image, ensure_outfile_does_not_alias_input, replace_stokes_axis_in_record,
    write_quality_labels,
};
use casa_images::image_history_rows;
use casa_images::{ImageBeamSet, ImageInterface, MutableImageInterface};
use numpy::PyUntypedArray;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyList, PyString, PyTuple};

use crate::convert::Variant;

use super::PyImageTool;
use super::beam_view::BeamView;
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised, with_storage};

enum TransposeOrder {
    Integer(i64),
    String(String),
    Names(Vec<String>),
    Unsupported(&'static str),
}

fn text(value: &Bound<'_, PyAny>) -> Option<String> {
    if let Ok(value) = value.cast::<PyBytes>() {
        return Some(String::from_utf8_lossy(value.as_bytes()).into_owned());
    }
    value.cast::<PyString>().ok().map(|value| value.to_string())
}

impl FromPyObject<'_, '_> for TransposeOrder {
    type Error = PyErr;

    fn extract(value: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        if value.is_instance_of::<PyBool>() {
            return Ok(Self::Unsupported("bool"));
        }
        if let Some(value) = text(&value) {
            return Ok(Self::String(value));
        }
        if let Ok(value) = value.extract::<i64>() {
            return Ok(Self::Integer(value));
        }
        if (value.is_instance_of::<PyList>()
            || value.is_instance_of::<PyTuple>()
            || value.is_instance_of::<PyUntypedArray>())
            && let Ok(items) = value.try_iter()
        {
            let items = items.collect::<PyResult<Vec<_>>>()?;
            if !items.is_empty()
                && let Some(names) = items.iter().map(text).collect::<Option<Vec<_>>>()
            {
                return Ok(Self::Names(names));
            }
            if !items.is_empty() && items.iter().all(|item| item.extract::<i64>().is_ok()) {
                return Ok(Self::Unsupported("intvec"));
            }
        }
        Ok(Self::Unsupported(match value.extract::<Variant>()? {
            Variant::Empty => "boolvec",
            Variant::Number(_) => "double",
            Variant::Complex(_) => "complex",
            Variant::Complexes(_) | Variant::ComplexArray { .. } => "complexvec",
            Variant::Numbers(_) | Variant::Array { .. } => "doublevec",
            Variant::Record(_) => "record",
            _ => "variant",
        }))
    }
}

struct AxisOrder {
    /// Old pixel index at each new pixel axis.
    axes: Vec<usize>,
    world_axes: Vec<usize>,
    /// Whether an old image axis is counted from its far edge.
    reversed: Vec<bool>,
}

impl TransposeOrder {
    fn axes(&self, coordinates: &CoordinateSystem, rank: usize) -> PyResult<AxisOrder> {
        let mut reversed = vec![false; rank];
        let axes = match self {
            Self::Integer(order) => numeric_order(&(*order as u32).to_string(), rank)?,
            Self::String(order) => {
                let mut digits = String::new();
                let mut preceding_dash = false;
                for byte in order.bytes() {
                    match byte {
                        b'-' if !preceding_dash => preceding_dash = true,
                        b'0'..=b'9' => {
                            if preceding_dash {
                                let index = usize::from(byte - b'0');
                                let axis = reversed.get_mut(index).ok_or_else(|| {
                                    raised(format!(
                                        "Image does not contain zero-based axis number {index}"
                                    ))
                                })?;
                                *axis = true;
                            }
                            preceding_dash = false;
                            digits.push(char::from(byte));
                        }
                        _ => return Err(invalid_order(order)),
                    }
                }
                if preceding_dash || digits.is_empty() {
                    return Err(invalid_order(order));
                }
                numeric_order(digits.trim_start_matches('0'), rank)?
            }
            Self::Names(requested) => {
                let names = (0..coordinates.n_world_axes())
                    .map(|axis| {
                        let (index, local) = coordinates
                            .world_axis_owner(axis)
                            .expect("public world axis");
                        coordinates.coordinate(index).axis_names()[local].to_ascii_lowercase()
                    })
                    .collect::<Vec<_>>();
                if requested.len() != names.len() || rank != names.len() {
                    return Err(raised(format!(
                        "Image has {} axes but {} were given. Number of given axes must match the number of image axes",
                        names.len(),
                        requested.len()
                    )));
                }
                let mut seen = vec![None; rank];
                let mut axes = Vec::with_capacity(rank);
                let mut world_axes = Vec::with_capacity(rank);
                for name in requested {
                    let flip = name.starts_with('-');
                    let name = name.strip_prefix('-').unwrap_or(name).to_ascii_lowercase();
                    let matches = names
                        .iter()
                        .enumerate()
                        .filter(|(_, candidate)| candidate.starts_with(&name))
                        .collect::<Vec<_>>();
                    let index = match matches.as_slice() {
                        [(axis, _)] => *axis,
                        [] => {
                            return Err(raised(format!(
                                "No axis matches requested axis {name}. Image axis names are [{}]",
                                names.join(", ")
                            )));
                        }
                        _ => {
                            let matches = matches
                                .into_iter()
                                .map(|(_, candidate)| candidate.as_str())
                                .collect::<Vec<_>>()
                                .join(", ");
                            return Err(raised(format!(
                                "Multiple axes [{matches}] match requested axis {name}"
                            )));
                        }
                    };
                    if let Some(first) = seen[index].replace(name.clone()) {
                        return Err(raised(format!(
                            "Ambiguous axis specification. Both {first} and {name} match image axis name {}",
                            names[index]
                        )));
                    }
                    let pixel = coordinates.world_axis_to_pixel_axis(index).ok_or_else(|| {
                        raised(format!("World axis {name} has no image pixel axis"))
                    })?;
                    reversed[pixel] = flip;
                    axes.push(pixel);
                    world_axes.push(index);
                }
                return Ok(AxisOrder {
                    axes,
                    world_axes,
                    reversed,
                });
            }
            Self::Unsupported(kind) => {
                return Err(raised(format!(
                    "Unsupported type for order parameter {kind}. Supported types are a non-negative integer,                  a single string containing all digits or a list of strings which unambiguously match the image axis names."
                )));
            }
        };
        Ok(AxisOrder {
            world_axes: axes.clone(),
            axes,
            reversed,
        })
    }

    fn history(&self) -> String {
        match self {
            Self::Integer(number) => number.to_string(),
            Self::String(value) => format!("{value:?}"),
            Self::Names(names) => format!("{names:?}"),
            Self::Unsupported(_) => unreachable!("only a valid order is recorded"),
        }
    }
}

fn invalid_order(order: &str) -> PyErr {
    raised(format!(
        "Incorrect order specification {order}. All characters must be digits."
    ))
}

fn numeric_order(digits: &str, rank: usize) -> PyResult<Vec<usize>> {
    let digits = if digits.is_empty() { "0" } else { digits };
    let padded = digits.len() != rank;
    let count = digits.len() + usize::from(padded);
    if count != rank {
        return Err(raised(format!(
            "Image has {rank} axes but {count} were given for reordering. Number of axes to reorder must match the number of image axes"
        )));
    }
    if count > 10 {
        return Err(raised(format!(
            "Only images with less than 10 axes can currently be reordered. This image has {rank} axes"
        )));
    }
    let prefix = padded.then_some(0);
    let mut axes = Vec::with_capacity(rank);
    for index in prefix
        .into_iter()
        .chain(digits.bytes().map(|value| usize::from(value - b'0')))
    {
        if index >= rank {
            return Err(raised(format!(
                "Image does not contain zero-based axis number {index} but this was incorrectly specified in order parameter. {digits}"
            )));
        }
        if axes.contains(&index) {
            return Err(raised(format!(
                "Axis number {index} specified multiple times in order parameter {digits}. It can only be specified once."
            )));
        }
        axes.push(index);
    }
    Ok(axes)
}

fn reversed_coordinates(
    active: &CoordinateSystem,
    mut record: RecordValue,
    shape: &[usize],
    order: &AxisOrder,
) -> PyResult<(CoordinateSystem, RecordValue)> {
    let source = CoordinateSystem::from_mapped_record(&record).map_err(raised)?;
    let placements = source.coordinates_in_record(&record).map_err(raised)?;
    let reference_pixels = source.reference_pixel();
    for (axis, &reverse) in order.reversed.iter().enumerate() {
        if !reverse {
            continue;
        }
        let (index, local) = source
            .pixel_axis_owner(axis)
            .ok_or_else(|| raised("Image pixel axis has no coordinate"))?;
        let field = &placements[index].field;
        let model = source.coordinate(index);
        if let CoordinateModel::Stokes(stokes) = model {
            let labels = stokes
                .stokes()
                .get(..shape[axis])
                .ok_or_else(|| raised("Image Stokes planes exceed their coordinate labels"))?;
            let labels = labels.iter().rev().copied().collect::<Vec<_>>();
            replace_stokes_axis_in_record(&mut record, field, &labels).map_err(raised)?;
            continue;
        }
        if let CoordinateModel::Quality(quality) = model {
            let labels = quality
                .values()
                .get(..shape[axis])
                .ok_or_else(|| raised("Image Quality planes exceed their coordinate labels"))?;
            let labels = labels.iter().rev().copied().collect::<Vec<_>>();
            let Some(Value::Record(coordinate)) = record.get_mut(field) else {
                return Err(raised("Missing Quality coordinate"));
            };
            write_quality_labels(coordinate, &labels).map_err(raised)?;
            continue;
        }
        if reflect_samples(&mut record, field, model, shape[axis])? {
            continue;
        }
        CoordinateSystem::set_reference_pixel_in_record(
            &mut record,
            axis,
            (shape[axis] - 1) as f64 - reference_pixels[axis],
        )
        .map_err(raised)?;
        let increment = model.increment()[local];
        let increment = if let CoordinateModel::Direction(direction) = model {
            let unit = casacore_unit(&direction.world_units()[local]).map_err(raised)?;
            increment / unit.val().factor
        } else {
            increment
        };
        CoordinateSystem::set_increment_in_record(&mut record, axis, -increment).map_err(raised)?;
        // CDELT owns the world-axis sign. Conjugating the PC by the axis
        // reflection turns this into a reversal of just the chosen pixel
        // column, including when the PC couples different axes.
        if matches!(
            model,
            CoordinateModel::Direction(_) | CoordinateModel::Linear(_)
        ) && let Some(Value::Record(coordinate)) = record.get_mut(field)
            && let Some(Value::Array(ArrayValue::Float64(values))) = coordinate.get_mut("pc")
        {
            if values.ndim() != 2 || local >= values.shape()[0] || local >= values.shape()[1] {
                return Err(raised("Invalid coordinate matrix for axis reversal"));
            }
            for (at, value) in values.indexed_iter_mut() {
                if (at[0] == local) != (at[1] == local) {
                    *value = -*value;
                }
            }
        }
    }
    let mut result = CoordinateSystem::from_mapped_record(&record).map_err(raised)?;
    let active_placements = active.coordinates_in_record(&record).map_err(raised)?;
    for (index, placement) in placements.iter().enumerate() {
        if placement
            .pixel_axes
            .iter()
            .flatten()
            .any(|&axis| order.reversed[axis])
        {
            continue;
        }
        let Some(live) = active_placements
            .iter()
            .position(|entry| entry.field == placement.field)
        else {
            continue;
        };
        let live = active.coordinate(live);
        let restored = result.coordinate(index);
        if live.n_pixel_axes() == restored.n_pixel_axes()
            && live.n_world_axes() == restored.n_world_axes()
        {
            result.replace_coordinate(index, live.clone());
        }
    }
    result
        .transpose_axes(&order.world_axes, &order.axes)
        .map_err(raised)?;
    let maps = result.to_record();
    for field in maps.fields() {
        if field.name.starts_with("pixelmap") || field.name.starts_with("worldmap") {
            record.upsert(&field.name, field.value.clone());
        }
    }
    Ok((result, record))
}

fn reflect_samples(
    record: &mut RecordValue,
    field: &str,
    model: &CoordinateModel,
    length: usize,
) -> PyResult<bool> {
    let edge = (length - 1) as f64;
    let put_vector = |table: &mut RecordValue, key, values| {
        table.upsert(key, Value::Array(ArrayValue::from_f64_vec(values)));
    };
    if let CoordinateModel::Tabular(tabular) = model
        && !tabular.pixel_values().is_empty()
    {
        let Some(Value::Record(table)) = record.get_mut(field) else {
            return Err(raised("Missing tabular coordinate"));
        };
        let pixels = tabular
            .pixel_values()
            .iter()
            .map(|&pixel| edge - pixel)
            .collect();
        put_vector(table, "pixelvalues", pixels);
        put_vector(table, "crpix", vec![edge - tabular.reference_pixel()[0]]);
        put_vector(table, "cdelt", vec![-tabular.increment()[0]]);
        return Ok(true);
    }
    if let CoordinateModel::Spectral(spectral) = model
        && !spectral.has_linear_mapping()
    {
        // The native spectral record reconstructs the lookup on consecutive
        // channels. Sample the reflected native frequencies on that grid, so
        // changes to the output reference metadata cannot rescale the knots.
        let native = spectral.as_ref().clone().without_conversion();
        let frequencies = (0..length)
            .map(|index| {
                let value = native.to_world(&[edge - index as f64])?[0];
                native.value_in_hz(value)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(raised)?;
        let Some(Value::Record(coordinate)) = record.get_mut(field) else {
            return Err(raised("Missing spectral coordinate"));
        };
        let Some(Value::Record(table)) = coordinate.get_mut("tabular") else {
            return Err(raised("Missing spectral lookup table"));
        };
        let increment = if length > 1 {
            (frequencies[length - 1] - frequencies[0]) / edge
        } else {
            0.0
        };
        put_vector(table, "crpix", vec![0.0]);
        put_vector(table, "crval", vec![frequencies[0]]);
        put_vector(table, "cdelt", vec![increment]);
        put_vector(
            table,
            "pixelvalues",
            (0..length).map(|index| index as f64).collect(),
        );
        put_vector(table, "worldvalues", frequencies);
        table.upsert(
            "units",
            Value::Array(ArrayValue::from_string_vec(vec!["Hz".into()])),
        );
        return Ok(true);
    }
    Ok(false)
}

fn flip_beam_planes(
    record: &mut RecordValue,
    coordinates: &CoordinateSystem,
    reverse: &[bool],
) -> PyResult<()> {
    let flips = |kind| {
        coordinates
            .find_coordinate(kind)
            .and_then(|index| coordinates.coordinate_pixel_axes(index))
            .and_then(|axes| axes.into_iter().flatten().next())
            .is_some_and(|axis| reverse[axis])
    };
    let channels = flips(CoordinateType::Spectral);
    let polarizations = flips(CoordinateType::Stokes);
    if !channels && !polarizations {
        return Ok(());
    }
    let Some(Value::Record(grid)) = record.get_mut("perplanebeams") else {
        return Ok(());
    };
    let original = grid.clone();
    let (nchan, npol) = ImageBeamSet::from_record(&original)
        .map_err(raised)?
        .shape();
    let key = |channel, polarization| {
        let current = format!("*{}", nchan * polarization + channel);
        if original.get(&current).is_some() {
            current
        } else {
            format!("*{channel}_{polarization}")
        }
    };
    for polarization in 0..npol {
        for channel in 0..nchan {
            let old = key(
                if channels {
                    nchan - 1 - channel
                } else {
                    channel
                },
                if polarizations {
                    npol - 1 - polarization
                } else {
                    polarization
                },
            );
            let value = original
                .get(&old)
                .ok_or_else(|| raised("Missing restoring beam plane"))?;
            grid.upsert(key(channel, polarization), value.clone());
        }
    }
    Ok(())
}

fn make_output(
    input: &dyn ImageInterface<f32>,
    active: &CoordinateSystem,
    record: RecordValue,
    beams: &BeamView,
    order: &AxisOrder,
    path: Option<&Path>,
) -> PyResult<(Storage<f32>, RecordValue, CoordinateSystem)> {
    let (coordinates, record) = reversed_coordinates(active, record, input.shape(), order)?;
    let shape = order.axes.iter().map(|&axis| input.shape()[axis]).collect();
    let mut product = Storage::create(shape, coordinates.clone(), path, None).map_err(raised)?;
    with_storage!(&mut product, output => {
        copy_transposed_image(input, output.as_mut(), &order.axes, &order.reversed).map_err(raised)?;
        output.set_units_string(input.units().to_owned()).map_err(raised)?;
        output.set_misc_info(input.misc_info()).map_err(raised)?;
        let mut info = input.image_info().map_err(raised)?.to_record();
        beams.apply_to(&mut info);
        flip_beam_planes(&mut info, input.coordinates(), &order.reversed)?;
        output.set_image_info_record(info).map_err(raised)
    })?;
    match &mut product {
        Storage::Paged(image) => image
            .set_coordinate_record(record.clone())
            .map_err(raised)?,
        Storage::Temp { record: stored, .. } => stored.clone_from(&record),
    }
    Ok((product, record, coordinates))
}

struct OutputName(String);

impl FromPyObject<'_, '_> for OutputName {
    type Error = PyErr;

    fn extract(value: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        text(&value).map(Self).ok_or_else(|| {
            pyo3::exceptions::PyTypeError::new_err("argument _outfile must be a string")
        })
    }
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (outfile=OutputName(String::new()), order=TransposeOrder::Unsupported("boolvec")), text_signature = "($self, outfile='', order=[])")]
    fn transpose(
        &self,
        py: Python<'_>,
        outfile: OutputName,
        order: TransposeOrder,
    ) -> PyResult<Py<Self>> {
        let output = py.detach(|| {
            let state = self.state();
            let attachment = state.image.as_ref().ok_or_else(|| raised("No image specified to transpose"))?;
            let outfile = &outfile.0;
            if !outfile.is_empty() && Path::new(outfile).symlink_metadata().is_ok() {
                return Err(raised(format!("File {outfile} exists but overwrite is false so it cannot be overwritten")));
            }
            let mut live_coordinates = None;
            let mut product = publish_image(outfile, false, |path| {
                let mut handle = attachment.lock();
                if !outfile.is_empty() {
                    for input in handle.path().into_iter().chain(attachment.operand_paths().iter().map(PathBuf::as_path)) {
                        ensure_outfile_does_not_alias_input(Path::new(outfile), input).map_err(raised)?;
                    }
                }
                if !matches!(&*handle, ImageHandle::Float(_)) {
                    return Err(raised("This method only supports Float valued images"));
                }
                attachment.require_pixel_read_lock(&mut handle)?;
                let mut make = || {
                    let (active, record) = attachment.coordinate_view(&handle);
                    let beams = attachment.beam_view().clone();
                    let ImageHandle::Float(storage) = &*handle else {
                        unreachable!("pixel type was checked");
                    };
                    let axes = order.axes(active, handle.shape().len())?;
                    let (result, record, coordinates) = storage.with_float_view(|image|
                        make_output(image, active, record, &beams, &axes, path))?;
                    let original_history = match storage {
                        Storage::Paged(image) => image_history_rows(image.name().expect("native path")).map_err(raised)?,
                        Storage::Temp { image, .. } => image.history_rows().map_err(raised)?.into_owned(),
                    };
                    let mut output = ImageHandle::Float(result);
                    let ImageHandle::Float(storage) = &mut output else { unreachable!() };
                    with_storage!(storage, image => image.append_history_entries(&original_history)).map_err(raised)?;
                    if state.history {
                        let order = order.history();
                        output.record_history("transpose", format!("ia.transpose(outfile={outfile:?}, order={order})"))?;
                    }
                    output.save().map_err(raised)?;
                    live_coordinates = Some((record, coordinates));
                    Ok(output)
                };
                let result = make();
                let release = attachment.release_requested_lock(&mut handle);
                result.and_then(|output| release.map(|()| output))
            })?;
            if let Some((record, coordinates)) = live_coordinates {
                product.keep_live_coordinates(record, coordinates);
            }
            Ok::<_, PyErr>(product)
        })?;
        Py::new(py, Self::attached(output))
    }
}
