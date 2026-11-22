// SPDX-License-Identifier: LGPL-3.0-or-later
//! Paged image concatenation, including temporary returned images.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use casa_coordinates::CoordinateSystem;
use casa_images::analysis::ensure_outfile_does_not_alias_input;
use casa_images::concatenate::{ConcatInput, ConcatenationPlan, copy_concatenated_input};
use casa_images::image::{ImageHistoryRow, image_history_rows};
use casa_images::{ImageError, ImageInfo, ImagePixel, ImagePixelType, MutableImageInterface};
use casa_types::{RecordField, RecordValue, Value};
use numpy::{PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use crate::convert::Variant;
use crate::tools::{LogLine, emit};

use super::PyImageTool;
use super::attachment;
use super::beams::{image_info, image_info_record, plane_axes, stored_beam_record};
use super::create::{publish_image, publish_owned_virtual_concat, publish_virtual_concat};
use super::handle::{ImageHandle, Storage, raised, same_file_snapshot, with_pixels, with_storage};
use super::lifecycle::resolve_image_path;
use super::saved_expression::{self, SavedExpressionInputs};

pub(super) mod owned_members;
mod relocation;
pub(super) mod virtual_image;

fn filename_argument(value: Option<&Bound<'_, PyAny>>) -> PyResult<Variant> {
    let Some(value) = value else {
        return Ok(Variant::Empty);
    };
    if let Ok(array) = value.cast::<PyUntypedArray>() {
        return match array.dtype().kind() {
            b'b' | b'O' => Ok(Variant::Empty),
            // The Unicode typemap supplies a filename vector only if at most one axis varies.
            b'U' if array.shape().iter().filter(|&&length| length > 1).count() > 1 => {
                Ok(Variant::Empty)
            }
            b'S' | b'U' => flatten_filenames(value),
            _ => value.extract(),
        };
    }
    let captured: Variant = value.extract()?;
    if matches!(captured, Variant::Other(_))
        && (value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>())
    {
        let array = value
            .py()
            .import("numpy")?
            .call_method1("asarray", (value, "object"))?;
        return flatten_filenames(&array);
    }
    Ok(captured)
}

fn flatten_filenames(array: &Bound<'_, PyAny>) -> PyResult<Variant> {
    let flattened = array
        .call_method1("ravel", ("F",))?
        .call_method0("tolist")?;
    let mut names = Vec::new();
    for item in flattened.try_iter()? {
        match item?.extract()? {
            Variant::Text(name) => names.push(name),
            _ => return Ok(Variant::Empty),
        }
    }
    Ok(Variant::Texts(names))
}

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
        let name = path_module.call_method1("expandvars", (name,))?;
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

struct InputRevision {
    file: fs::Metadata,
    generation: Option<u32>,
}

impl InputRevision {
    fn read(path: &Path, image: &ImageHandle) -> PyResult<Self> {
        let file = fs::metadata(path).map_err(raised)?;
        #[cfg(unix)]
        let generation = with_pixels!(image, storage => match storage {
            Storage::Paged(image) if !image.uses_advisory_locks() => Some(image.table().locked_modify_counter().map_err(raised)?),
            Storage::Paged(_) => None,
            Storage::Temp { .. } => None,
        });
        #[cfg(not(unix))]
        let generation = {
            let _ = image;
            None
        };
        Ok(Self { file, generation })
    }

    fn matches(&self, other: &Self) -> bool {
        self.generation == other.generation && same_file_snapshot(&self.file, &other.file)
    }
}

struct Input<'a> {
    path: &'a Path,
    resolved_path: PathBuf,
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
    writable: bool,
    writable_mask: bool,
    advisory_lock: bool,
    opened: Option<ImageHandle>,
    expression: Option<SavedExpressionInputs>,
    expression_operands: Vec<PathBuf>,
    nested_operands: Vec<PathBuf>,
    revision: Option<InputRevision>,
}

impl<'a> Input<'a> {
    fn open(path: &'a Path, tempclose: bool) -> PyResult<Self> {
        Self::open_resolved(path, path, tempclose, &mut HashSet::new())
    }

    fn open_resolved(
        path: &'a Path,
        resolved: &Path,
        tempclose: bool,
        active: &mut HashSet<PathBuf>,
    ) -> PyResult<Self> {
        if !casa_images::concat_file::is_image_concat(resolved)
            && casa_images::expr_file::is_image_expr(resolved)
        {
            let (image, inputs) = saved_expression::open_for_concatenation(resolved, active)?;
            return Self::from_image(path, image, tempclose, Some(inputs));
        }
        let image = ImageHandle::open_for_concatenation(resolved).map_err(raised)?;
        Self::from_image(path, image, tempclose, None)
    }

    fn from_image(
        path: &'a Path,
        image: ImageHandle,
        tempclose: bool,
        mut expression: Option<SavedExpressionInputs>,
    ) -> PyResult<Self> {
        let resolved_path = image.path().unwrap_or(path).to_path_buf();
        let coordinate_record = image.coordinate_record();
        let coordinates = CoordinateSystem::from_record(&coordinate_record).map_err(raised)?;
        let (units, misc, has_mask) = with_pixels!(&image, storage => with_storage!(storage, image => (
            image.units().to_string(),
            image.misc_info(),
            image.has_pixel_mask(),
        )));
        let history = with_pixels!(&image, storage => match storage {
            Storage::Paged(_) => image_history_rows(path).map_err(raised)?,
            Storage::Temp { image, .. } => image.history_rows().map_err(raised)?.into_owned(),
        });
        let revision = tempclose
            .then(|| InputRevision::read(&resolved_path, &image))
            .transpose()?;
        let writable = with_pixels!(&image, storage => match storage {
            Storage::Paged(image) => image.is_writable()
                && casa_tables::publication::check_write_access(&resolved_path.join("table.dat")).is_ok(),
            Storage::Temp { image, fits, .. } => fits.is_none() && image.is_writable(),
        });
        let writable_mask = with_pixels!(&image, storage => match storage {
            Storage::Paged(image) => {
                image.active_pixel_mask_name().is_some_and(|name| {
                    image.named_mask_is_writable(&name).unwrap_or(false)
                })
            }
            Storage::Temp { image, .. } => image.concat_source().is_some_and(|source| source.mask_writable()),
        }) && writable;
        // MIRIAD inherits casacore's lattice locks, which report success
        // without a table lease, even under TempClose and after unlock.
        let advisory_lock = with_pixels!(&image, storage => match storage {
            Storage::Paged(_) => false,
            Storage::Temp { image, .. } => {
                image.image_type_name() == "MIRIADImage"
                    || image.concat_source().is_some_and(|source| source.has_advisory_lock())
            }
        });
        let expression_operands = match &mut expression {
            Some(expression) => std::mem::take(&mut expression.paths),
            None => image.concat_expression_operands(),
        };
        let mut nested_operands = image.concat_operands();
        if path != resolved_path {
            nested_operands.push(casa_tables::lexical_absolute(path));
        }
        Ok(Self {
            path,
            resolved_path,
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
            writable,
            writable_mask,
            advisory_lock,
            opened: (!tempclose || !expression_operands.is_empty()).then_some(image),
            expression,
            expression_operands,
            nested_operands,
            revision,
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

fn protect_expression_sources(output: &Path, inputs: &[Input<'_>]) -> PyResult<()> {
    let route = casa_tables::lexical_absolute(output);
    let entry = route
        .parent()
        .and_then(|parent| parent.canonicalize().ok())
        .zip(route.file_name())
        .map(|(parent, name)| parent.join(name));
    let referent = output.canonicalize().ok();
    for input in inputs {
        for path in &input.expression_operands {
            if std::iter::once(&route)
                .chain(entry.iter())
                .chain(referent.iter())
                .any(|output| path.starts_with(output) || output.starts_with(path))
            {
                return Err(raised(format!(
                    "Cannot overwrite output {} because it overlaps a saved image expression input {}",
                    output.display(),
                    path.display(),
                )));
            }
        }
    }
    Ok(())
}

fn coordinate_record(first: &Input<'_>, plan: &ConcatenationPlan) -> PyResult<RecordValue> {
    let mut record = first.coordinate_record.clone();
    if let Some(index) = plan.changed_coordinate {
        let coordinates = CoordinateSystem::record_coordinates(&record).map_err(raised)?;
        let old = &coordinates[index].field;
        let old_kind = old.trim_end_matches(|character: char| character.is_ascii_digit());
        let replacement = plan.coordinates.to_record();
        let coordinates = CoordinateSystem::record_coordinates(&replacement).map_err(raised)?;
        let new = &coordinates[index].field;
        let new_kind = new.trim_end_matches(|character: char| character.is_ascii_digit());
        let value = replacement
            .get(new)
            .expect("replacement coordinate")
            .clone();
        record.remove(old);
        record.upsert(format!("{new_kind}{}", &old[old_kind.len()..]), value);
    }
    Ok(record)
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
            (Some(&sources[plan.beam_sources[index]]), local, plane)
        } else if stokes_concat {
            let (index, local) = origins[plane];
            (Some(&sources[plan.beam_sources[index]]), channel, local)
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
        if !self.outfile.is_empty() {
            protect_expression_sources(Path::new(self.outfile), &inputs)?;
        }
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
        output.set_coordinate_record(coordinate_record(first, &plan)?)?;
        let mut misc = RecordValue::default();
        let mut offset = 0;
        for &index in &plan.order {
            let entry = &inputs[index];
            let reopened;
            let image = if let Some(opened) = &entry.opened {
                opened
            } else {
                reopened = ImageHandle::open_for_concatenation(entry.path).map_err(raised)?;
                &reopened
            };
            let same_revision = match &entry.revision {
                Some(revision) => revision.matches(&InputRevision::read(entry.path, image)?),
                None => true,
            };
            if !same_revision || image.shape() != entry.shape || image.kind() != entry.kind {
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
        let files = filename_argument(infiles)?;
        let paths = filenames(py, &files)?;
        if !outfile.is_empty() && Path::new(outfile).exists() && !overwrite {
            return Err(raised(format!(
                "Exception: {outfile} exists and overwrite is false."
            )));
        }
        let mode_letter = mode.as_bytes().first().map(u8::to_ascii_lowercase);
        match mode_letter {
            Some(b'p') => {}
            Some(b'n' | b'c' | b'm') if outfile.is_empty() => {
                return Err(raised(
                    "Exception: An empty outname can be used only if mode == PAGED.",
                ));
            }
            Some(b'n' | b'c' | b'm') => {}
            _ => return Err(raised(format!("Unsupported mode {mode}"))),
        }
        let expanded = if mode_letter != Some(b'p') {
            resolve_image_path(py, outfile)?
        } else {
            std::borrow::Cow::Borrowed(outfile)
        };
        if !expanded.is_empty() {
            let destination = Path::new(expanded.as_ref());
            let existing = destination
                .exists()
                .then(|| attachment::key(destination))
                .transpose()?;
            for path in &paths {
                if let Some(existing) = &existing
                    && &attachment::key(path)? == existing
                {
                    return Err(raised(format!(
                        "Invalid Table operation: SetupNewTable {} is already opened (is in the table cache)",
                        existing.display()
                    )));
                }
                ensure_outfile_does_not_alias_input(destination, path).map_err(raised)?;
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
            if mode_letter != Some(b'p') {
                let attached = if mode_letter == Some(b'n') {
                    publish_virtual_concat(
                        outfile,
                        expanded.as_ref(),
                        overwrite,
                        |final_path, stage| {
                            virtual_image::stage_nomove(
                                &request, &paths, final_path, stage, &mut logs,
                            )
                        },
                    )
                } else {
                    let (attached, cleanup_warnings) = publish_owned_virtual_concat(
                        outfile,
                        expanded.as_ref(),
                        overwrite,
                        |final_path, stage| {
                            virtual_image::stage_owned(
                                &request,
                                &paths,
                                final_path,
                                stage,
                                mode_letter == Some(b'm'),
                                &mut logs,
                            )
                        },
                    )?;
                    logs.extend(cleanup_warnings.into_iter().map(LogLine::warning));
                    Ok(attached)
                }?;
                if request.history {
                    request.history_for(&mut attached.lock())?;
                }
                Ok(attached)
            } else {
                publish_image(outfile, overwrite, |path| {
                    request.build(&paths, path, &mut logs)
                })
            }
        });
        emit(py, "casars.tools.image", &logs)?;
        Py::new(py, Self::attached(result?))
    }
}
