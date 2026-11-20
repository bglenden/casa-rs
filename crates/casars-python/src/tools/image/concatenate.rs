// SPDX-License-Identifier: LGPL-3.0-or-later
//! Paged image concatenation, including temporary returned images.

use std::path::{Path, PathBuf};

use casa_coordinates::CoordinateSystem;
use casa_images::concatenate::{
    ConcatInput, ConcatenationPlan, copy_concatenated_input, group_fits_linear_axes,
};
use casa_images::image::{ImageHistoryRow, image_history_rows};
use casa_images::{ImageError, ImageInfo, ImagePixel, ImagePixelType};
use casa_types::{RecordField, RecordValue, Value};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use crate::convert::Variant;
use crate::tools::{LogLine, emit, not_yet_implemented_in};

use super::PyImageTool;
use super::attachment;
use super::beams::{image_info, image_info_record, plane_axes, stored_beam_record};
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};

fn filenames(py: Python<'_>, files: &Variant) -> PyResult<Vec<PathBuf>> {
    let names = match files {
        Variant::Empty => Vec::new(),
        Variant::Text(text) => {
            let delimiter = if text.contains(',') { ',' } else { ' ' };
            text.split(delimiter)
                .filter(|word| !word.is_empty())
                .map(str::to_owned)
                .collect()
        }
        Variant::Texts(names) => names.clone(),
        _ => return Err(raised("Exception: Unrecognized infiles datatype.")),
    };
    let glob = py.import("glob")?;
    let path_module = py.import("os.path")?;
    let mut result = Vec::new();
    for name in names {
        let pattern: String = path_module.call_method1("expanduser", (name,))?.extract()?;
        if let Some(parent) = Path::new(&pattern)
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            && !parent.to_string_lossy().contains(['*', '?', '['])
            && !parent.is_dir()
        {
            return Err(raised(format!(
                "DirectoryIterator: error on directory {}: No such file or directory",
                parent.display()
            )));
        }
        let matches: Vec<String> = glob.call_method1("glob", (pattern,))?.extract()?;
        for entry in matches {
            result.push(std::path::absolute(entry).map_err(raised)?);
        }
    }
    if result.len() < 2 {
        return Err(raised(
            "Exception: You must provide at least two images to concatentate.",
        ));
    }
    Ok(result)
}

struct Input<'a> {
    path: &'a Path,
    shape: Vec<usize>,
    kind: ImagePixelType,
    coordinates: CoordinateSystem,
    coordinate_record: RecordValue,
    info: ImageInfo,
    info_record: Option<RecordValue>,
    units: String,
    misc: RecordValue,
    history: Vec<ImageHistoryRow>,
    has_mask: bool,
    opened: Option<ImageHandle>,
}

impl<'a> Input<'a> {
    fn open(path: &'a Path, tempclose: bool) -> PyResult<Self> {
        let image = ImageHandle::open(path).map_err(raised)?;
        let mut coordinate_record = image.coordinate_record();
        let mut coordinates = CoordinateSystem::from_record(&coordinate_record).map_err(raised)?;
        if image.fits().is_some()
            && let Some(grouped) = group_fits_linear_axes(&coordinates)
        {
            coordinate_record = grouped.to_record();
            coordinates = grouped;
        }
        let (units, misc, has_mask) = with_pixels!(&image, storage => with_storage!(storage, image => (
            image.units().to_string(),
            image.misc_info(),
            image.default_mask_name().is_some(),
        )));
        let history = with_pixels!(&image, storage => match storage {
            Storage::Paged(_) => image_history_rows(path).map_err(raised)?,
            Storage::Temp { image, .. } => image.history_rows().to_vec(),
        });
        Ok(Self {
            path,
            shape: image.shape().to_vec(),
            kind: image.kind(),
            coordinates,
            coordinate_record,
            info: image_info(&image).map_err(raised)?,
            info_record: image_info_record(&image).cloned(),
            units,
            misc,
            history,
            has_mask: has_mask || image.fits().is_some_and(|fits| fits.has_mask),
            opened: (!tempclose).then_some(image),
        })
    }

    fn geometry(&self) -> ConcatInput<'_> {
        ConcatInput {
            path: self.path,
            shape: &self.shape,
            coordinates: &self.coordinates,
            image_info: &self.info,
            brightness_unit: &self.units,
        }
    }
}

fn merge_misc(output: &mut RecordValue, source: &RecordValue) {
    for field in source.fields() {
        let mut name = field.name.clone();
        let mut suffix = 1;
        while output.get(&name).is_some() {
            name = format!("{}_{suffix}", field.name);
            suffix += 1;
        }
        output.push(RecordField::new(name, field.value.clone()));
    }
}

fn beam_records(plan: &ConcatenationPlan, sources: &[Input<'_>]) -> PyResult<RecordValue> {
    let first = &sources[plan.order[0]];
    let mut record = first
        .info_record
        .clone()
        .unwrap_or_else(|| first.info.to_record());
    let beams = &plan.image_info.beam_set;
    if beams == &first.info.beam_set {
        return Ok(record);
    }
    record.remove("restoringbeam");
    record.remove("perplanebeams");
    if beams.is_empty() {
        return Ok(record);
    }
    let (spectral, stokes) = plane_axes(&first.coordinate_record)?;
    let channel_concat = spectral == Some(plan.axis) && beams.n_channels() == plan.shape[plan.axis];
    let stokes_concat = stokes == Some(plan.axis) && beams.n_stokes() == plan.shape[plan.axis];
    let origins = if channel_concat || stokes_concat {
        plan.order
            .iter()
            .flat_map(|&index| {
                (0..sources[index].shape[plan.axis]).map(move |local| (index, local))
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let equivalent = plan
        .order
        .iter()
        .map(|&index| &sources[index])
        .find(|input| &input.info.beam_set == beams);
    let beam = |channel, plane| {
        let (input, source_channel, source_plane) = if channel_concat {
            let (index, local) = origins[channel];
            (Some(&sources[index]), local, plane)
        } else if stokes_concat {
            let (index, local) = origins[plane];
            (Some(&sources[index]), channel, local)
        } else {
            (equivalent, channel, plane)
        };
        Value::Record(input.map_or_else(
            || beams.beam(channel, plane).to_record(),
            |input| {
                let source = &input.info.beam_set;
                let channel = if source.n_channels() == 1 {
                    0
                } else {
                    source_channel
                };
                let plane = if source.n_stokes() == 1 {
                    0
                } else {
                    source_plane
                };
                stored_beam_record(input.info_record.as_ref(), source, channel, plane)
            },
        ))
    };
    if beams.is_single() {
        record.upsert("restoringbeam", beam(0, 0));
    } else if beams.is_multi() {
        let mut grid = beams.to_record();
        for plane in 0..beams.n_stokes() {
            for channel in 0..beams.n_channels() {
                grid.upsert(
                    format!("*{}", plane * beams.n_channels() + channel),
                    beam(channel, plane),
                );
            }
        }
        record.upsert("perplanebeams", Value::Record(grid));
    }
    Ok(record)
}

fn copy_native<T: ImagePixel>(
    input: &Storage<T>,
    output: &mut Storage<T>,
    axis: usize,
    offset: usize,
    mask: Option<&str>,
) -> Result<(), ImageError> {
    with_storage!(input, input_image => with_storage!(output, output_image =>
        copy_concatenated_input(input_image.as_ref(), output_image.as_mut(), axis, offset, mask)
    ))
}

struct Request<'a> {
    outfile: &'a str,
    files: &'a Variant,
    axis: i64,
    relax: bool,
    tempclose: bool,
    overwrite: bool,
    reorder: bool,
    mode: &'a str,
    history: bool,
}

impl Request<'_> {
    fn build(
        &self,
        source_paths: &[PathBuf],
        path: Option<&Path>,
        logs: &mut Vec<LogLine>,
    ) -> PyResult<ImageHandle> {
        let inputs = source_paths
            .iter()
            .map(|path| Input::open(path, self.tempclose))
            .collect::<PyResult<Vec<_>>>()?;
        let first = inputs[0].kind;
        if inputs.iter().any(|input| input.kind != first) {
            return Err(raised(
                "Concatenation of images of different data types is not supported",
            ));
        }
        let geometry: Vec<_> = inputs.iter().map(Input::geometry).collect();
        let plan = ConcatenationPlan::new(&geometry, self.axis, self.relax, self.reorder)
            .map_err(raised)?;
        logs.extend(plan.warnings.iter().cloned().map(LogLine::warning));
        let first = &inputs[plan.order[0]];
        let mask = inputs.iter().any(|input| input.has_mask).then_some("mask0");
        let mut output = match first.kind {
            ImagePixelType::Float32 => {
                Storage::create(plan.shape.clone(), plan.coordinates.clone(), path, None)
                    .map(ImageHandle::Float)
            }
            ImagePixelType::Float64 => {
                Storage::create(plan.shape.clone(), plan.coordinates.clone(), path, None)
                    .map(ImageHandle::Double)
            }
            ImagePixelType::Complex32 => {
                Storage::create(plan.shape.clone(), plan.coordinates.clone(), path, None)
                    .map(ImageHandle::Complex)
            }
            ImagePixelType::Complex64 => {
                Storage::create(plan.shape.clone(), plan.coordinates.clone(), path, None)
                    .map(ImageHandle::DComplex)
            }
        }
        .map_err(raised)?;
        let coords = if plan.coordinates_changed {
            plan.coordinates.to_record()
        } else {
            first.coordinate_record.clone()
        };
        output.set_coordinate_record(coords)?;
        let mut misc = RecordValue::default();
        let mut offset = 0;
        for &index in &plan.order {
            let entry = &inputs[index];
            let reopened;
            let image = if let Some(opened) = &entry.opened {
                opened
            } else {
                reopened = ImageHandle::open(entry.path).map_err(raised)?;
                &reopened
            };
            if image.shape() != entry.shape || image.kind() != entry.kind {
                return Err(raised(format!(
                    "Image {} changed while it was being concatenated",
                    entry.path.display()
                )));
            }
            match (image, &mut output) {
                (ImageHandle::Float(input), ImageHandle::Float(output)) => {
                    input.with_float_view(|input| {
                        with_storage!(output, output =>
                        copy_concatenated_input(input, output.as_mut(), plan.axis, offset, mask))
                    })
                }
                (ImageHandle::Double(input), ImageHandle::Double(output)) => {
                    copy_native(input, output, plan.axis, offset, mask)
                }
                (ImageHandle::Complex(input), ImageHandle::Complex(output)) => {
                    copy_native(input, output, plan.axis, offset, mask)
                }
                (ImageHandle::DComplex(input), ImageHandle::DComplex(output)) => {
                    copy_native(input, output, plan.axis, offset, mask)
                }
                _ => unreachable!("all pixel types checked"),
            }
            .map_err(raised)?;
            offset += entry.shape[plan.axis];
            merge_misc(&mut misc, &entry.misc);
            with_pixels!(&mut output, storage => with_storage!(storage, image => image.append_history_entries(&entry.history))).map_err(raised)?;
        }
        let beams = beam_records(&plan, &inputs)?;
        with_pixels!(&mut output, storage => with_storage!(storage, image => {
            image.set_units(&first.units).map_err(raised)?;
            image.set_misc_info(misc).map_err(raised)?;
            image.set_image_info_record(beams).map_err(raised)?;
            if let Some(name) = mask {
                image.set_default_mask(name).map_err(raised)?;
            }
        }));
        if self.history {
            self.history_for(&mut output)?;
        }
        output.save().map_err(raised)?;
        Ok(output)
    }

    fn history_for(&self, output: &mut ImageHandle) -> PyResult<()> {
        let name = if self.outfile.is_empty() {
            "Temporary_Image".into()
        } else {
            std::path::absolute(self.outfile)
                .map_err(raised)?
                .display()
                .to_string()
        };
        let infiles = match self.files {
            Variant::Text(text) => format!("'{text}'"),
            files => files.describe(),
        };
        output.append_history_at("::image::imageconcat", &[
            format!("Ran ia.imageconcat on {name}"),
            format!(
                "ia.imageconcat(outfile='{}', infiles={infiles}, axis={}, relax={}, tempclose={}, overwrite={}, reorder={}, mode='{}')",
                self.outfile, self.axis, self.relax, self.tempclose, self.overwrite, self.reorder, self.mode
            ),
        ])
    }
}

#[pymethods]
impl PyImageTool {
    /// Combine images along one axis and return the resulting image tool.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", infiles=None, axis=-1, relax=false, tempclose=true, overwrite=false, reorder=false, mode="paged"), text_signature = "($self, outfile='', infiles=[], axis=-1, relax=False, tempclose=True, overwrite=False, reorder=False, mode='paged')")]
    fn imageconcat(
        &self,
        py: Python<'_>,
        outfile: &str,
        infiles: Option<&Bound<'_, PyAny>>,
        axis: i64,
        relax: bool,
        tempclose: bool,
        overwrite: bool,
        reorder: bool,
        mode: &str,
    ) -> PyResult<Py<PyImageTool>> {
        let files = match infiles {
            Some(value)
                if (value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>())
                    && value.try_iter()?.all(|item| {
                        item.is_ok_and(|item| item.is_instance_of::<pyo3::types::PyBool>())
                    }) =>
            {
                Variant::Empty
            }
            Some(value) => value.extract::<Variant>()?,
            None => Variant::Empty,
        };
        let paths = filenames(py, &files)?;
        if !outfile.is_empty() && Path::new(outfile).exists() && !overwrite {
            return Err(raised(format!(
                "Exception: {outfile} exists and overwrite is false."
            )));
        }
        match mode.as_bytes().first().map(u8::to_ascii_lowercase) {
            Some(b'p') => {}
            Some(b'n' | b'c' | b'm') if outfile.is_empty() => {
                return Err(raised(
                    "Exception: An empty outname can be used only if mode == PAGED.",
                ));
            }
            Some(b'n' | b'c' | b'm') => {
                return not_yet_implemented_in("image", "imageconcat virtual mode", 1625);
            }
            _ => return Err(raised(format!("Unsupported mode {mode}"))),
        }
        if !outfile.is_empty() && Path::new(outfile).exists() {
            let destination = attachment::key(Path::new(outfile))?;
            for path in &paths {
                if attachment::key(path)? == destination {
                    return Err(raised(format!(
                        "Invalid Table operation: SetupNewTable {} is already opened (is in the table cache)",
                        destination.display()
                    )));
                }
            }
        }
        let mut logs = Vec::new();
        let result = py.detach(|| {
            let request = Request {
                outfile,
                files: &files,
                axis,
                relax,
                tempclose,
                overwrite,
                reorder,
                mode,
                history: self.state().history,
            };
            publish_image(outfile, overwrite, |path| {
                request.build(&paths, path, &mut logs)
            })
        });
        emit(py, "casars.tools.image", &logs)?;
        Py::new(py, Self::attached(result?))
    }
}
