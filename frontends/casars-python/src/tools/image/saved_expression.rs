// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use casa_core::{ArrayD, Complex32, Complex64, RecordValue};
use casa_imageanalysis::analysis::{
    BoundImageToolOperands, ImageToolExpressionInputs, ImageToolPixelExpression,
};
use casa_images::MiriadImage;
use casa_images::expr_file::{ToolExpressionManifest, is_image_expr};
use casa_images::is_image_concat;
use casa_images::{
    ImageError, ImageExprBuilder, ImageExprValue, ImageExpression, ImageInterface, TempImage,
};
use casa_lattices::Lattice;
use casa_tables::lexical_absolute;
use pyo3::prelude::*;

use super::attachment::AttachedImage;
use super::concatenate::virtual_image;
use super::handle::{ImageHandle, Storage, raised, with_storage};

mod manifest;
mod rename;
pub(super) use manifest::SavedExpressionDirectory;

pub(super) fn open(path: &Path) -> PyResult<AttachedImage> {
    let (image, inputs) = compile_at(path, AttachedImage::open, false, &mut HashSet::new(), None)?;
    let mut attachment = AttachedImage::with_saved_expression(image, inputs.directory);
    attachment.retain_expression_inputs(inputs.retained, inputs.paths);
    Ok(attachment)
}

/// Resolve the final output name to its staged image when reading `imagemd`.
pub(super) struct StagedOutput<'a> {
    pub(super) target: &'a Path,
    pub(super) stage: &'a Path,
    entry: PathBuf,
}

impl<'a> StagedOutput<'a> {
    pub(super) fn new(target: &'a Path, stage: &'a Path) -> PyResult<Self> {
        let entry = if let Ok(path) = target.canonicalize() {
            path
        } else {
            let path = std::path::absolute(target).map_err(raised)?;
            let parent = path
                .parent()
                .ok_or_else(|| raised("Missing output parent"))?;
            let name = path
                .file_name()
                .ok_or_else(|| raised("Missing output name"))?;
            parent.canonicalize().map_err(raised)?.join(name)
        };
        Ok(Self {
            target,
            stage,
            entry,
        })
    }

    pub(super) fn substitute(&self, path: &Path) -> Option<PathBuf> {
        let path = std::path::absolute(path).ok()?;
        let parent = path.parent()?.canonicalize().ok()?;
        let path_entry = parent.join(path.file_name()?);
        let same_entry = path_entry == self.entry;
        if !same_entry && path_entry.starts_with(&self.entry) {
            return None;
        }
        let same_directory = path.canonicalize().is_ok_and(|path| path == self.entry);
        (same_entry || same_directory).then(|| self.stage.to_path_buf())
    }
}

pub(super) fn open_metadata(path: &Path, output: &StagedOutput<'_>) -> PyResult<ImageHandle> {
    let (image, inputs) = compile_at(
        path,
        AttachedImage::open_unregistered_expression_operand,
        true,
        &mut HashSet::new(),
        Some(output),
    )?;
    check_output_dependencies(output.target, &inputs.paths, false)?;
    Ok(image)
}

pub(super) struct SavedExpressionInputs {
    directory: SavedExpressionDirectory,
    pub(super) retained: Vec<AttachedImage>,
    pub(super) paths: Vec<PathBuf>,
}

impl SavedExpressionInputs {
    pub(super) fn check_name(&self, path: &Path) -> PyResult<()> {
        self.directory.check_name(path)
    }
}

pub(super) fn open_for_concatenation(
    path: &Path,
    active: &mut HashSet<PathBuf>,
    output: Option<&StagedOutput<'_>>,
) -> PyResult<(ImageHandle, SavedExpressionInputs)> {
    // Final publication already holds the image registry. Source leases can
    // still be opened directly without consulting or registering in it.
    compile_at(
        path,
        AttachedImage::open_unregistered_expression_operand,
        true,
        active,
        output,
    )
}

pub(super) fn check_output_dependencies<'a>(
    output: &Path,
    paths: impl IntoIterator<Item = &'a PathBuf>,
    check_new_children: bool,
) -> PyResult<()> {
    let route = casa_tables::lexical_absolute(output);
    let entry = route
        .parent()
        .and_then(|parent| parent.canonicalize().ok())
        .zip(route.file_name())
        .map(|(parent, name)| parent.join(name));
    let referent = output.canonicalize().ok();
    let replaces_entry = check_new_children || output.symlink_metadata().is_ok();
    for path in paths {
        if std::iter::once(&route)
            .chain(entry.iter())
            .chain(referent.iter())
            .any(|output| path.starts_with(output) || (replaces_entry && output.starts_with(path)))
        {
            return Err(raised(format!(
                "Cannot overwrite output {} because it overlaps a saved image expression input {}",
                output.display(),
                path.display(),
            )));
        }
    }
    Ok(())
}

pub(super) fn compile_inline(
    inputs: &ImageToolExpressionInputs<'_>,
    temporary_source_locks: bool,
) -> PyResult<(ImageToolPixelExpression, Vec<AttachedImage>, Vec<PathBuf>)> {
    let (graph, operands) = capture_inline(inputs, AttachedImage::open)?;
    let (pixels, _) = inputs
        .compile_with_bound_operands(&operands, temporary_source_locks)
        .map_err(raised)?;
    Ok((pixels, graph.retained, graph.paths))
}

pub(super) fn has_manifest_operands(inputs: &ImageToolExpressionInputs<'_>) -> bool {
    inputs
        .paths()
        .any(|path| is_image_expr(path) || is_image_concat(path))
}

pub(super) fn compile_edit(expression: &str) -> PyResult<ImageToolPixelExpression> {
    let inputs = ImageToolExpressionInputs::resolve(expression).map_err(raised)?;
    if !has_manifest_operands(&inputs) {
        return inputs.compile().map_err(raised);
    }
    // The caller already holds the target image mutex. Open independent
    // source handles so a transitive reference to that target can be read.
    let (_graph, operands) =
        capture_inline(&inputs, AttachedImage::open_unregistered_expression_operand)?;
    inputs
        .compile_with_bound_operands(&operands, false)
        .map(|(pixels, _)| pixels)
        .map_err(raised)
}

pub(super) fn evaluate_mask(
    expression: &str,
    shape: &[usize],
    overwrite_target: Option<&Path>,
) -> PyResult<ArrayD<bool>> {
    let inputs = ImageToolExpressionInputs::resolve(expression).map_err(raised)?;
    if !has_manifest_operands(&inputs) {
        return inputs
            .evaluate_mask_with_bound_operands(&BoundImageToolOperands::new(), shape)
            .map_err(raised);
    }
    let (graph, operands) =
        capture_inline(&inputs, AttachedImage::open_unregistered_expression_operand)?;
    if let Some(target) = overwrite_target {
        check_output_dependencies(target, &graph.paths, false)?;
    }
    inputs
        .evaluate_mask_with_bound_operands(&operands, shape)
        .map_err(raised)
}

fn capture_inline(
    inputs: &ImageToolExpressionInputs<'_>,
    open_source: fn(&Path) -> PyResult<AttachedImage>,
) -> PyResult<(CapturedGraph<'static>, BoundImageToolOperands)> {
    let mut graph = CapturedGraph {
        include_manifest_paths: true,
        ..CapturedGraph::default()
    };
    let directory = std::env::current_dir().map_err(raised)?;
    let operands = graph.capture(inputs, None, &directory, open_source, &mut HashSet::new())?;
    Ok((graph, operands))
}

fn compile_at(
    path: &Path,
    open_source: fn(&Path) -> PyResult<AttachedImage>,
    include_manifest_paths: bool,
    active: &mut HashSet<PathBuf>,
    output: Option<&StagedOutput<'_>>,
) -> PyResult<(ImageHandle, SavedExpressionInputs)> {
    let directory = std::env::current_dir().map_err(raised)?;
    let root = path.canonicalize().map_err(raised)?;
    let (captured, manifest) = SavedExpressionDirectory::open(path, root.clone())?;
    let inputs = ImageToolExpressionInputs::resolve_with_image_paths(
        manifest.expression.as_str(),
        &directory,
        |path| output.and_then(|output| output.substitute(path)),
    )
    .map_err(raised)?;
    let mut graph = CapturedGraph {
        include_manifest_paths,
        output,
        ..CapturedGraph::default()
    };
    graph.retain_manifest(&captured)?;
    let operands = graph.capture(&inputs, Some(root), &directory, open_source, active)?;
    let (pixels, single_precision) = compile_manifest(&inputs, &operands)?;
    let misc = manifest.misc_info().map_err(raised)?;
    captured.check_name(path)?;
    let image = match pixels {
        ImageToolPixelExpression::Real(expression) if single_precision => {
            ImageHandle::Float(expression_storage(
                expression
                    .convert_pixels(|value| value as f32)
                    .map_err(raised)?,
                path,
                misc,
            )?)
        }
        ImageToolPixelExpression::Real(expression) => {
            ImageHandle::Double(expression_storage(expression, path, misc)?)
        }
        ImageToolPixelExpression::Complex(expression) if single_precision => {
            ImageHandle::Complex(expression_storage(
                expression
                    .convert_pixels(|value| Complex32::new(value.re as f32, value.im as f32))
                    .map_err(raised)?,
                path,
                misc,
            )?)
        }
        ImageToolPixelExpression::Complex(expression) => {
            ImageHandle::DComplex(expression_storage(expression, path, misc)?)
        }
        ImageToolPixelExpression::Boolean(_) => unreachable!("saved image geometry is numeric"),
    };
    Ok((
        image,
        SavedExpressionInputs {
            directory: captured,
            retained: graph.retained,
            paths: graph.paths,
        },
    ))
}

#[derive(Default)]
struct CapturedGraph<'a> {
    paths: Vec<PathBuf>,
    seen_paths: HashSet<PathBuf>,
    links: HashSet<PathBuf>,
    retained: Vec<AttachedImage>,
    include_manifest_paths: bool,
    output: Option<&'a StagedOutput<'a>>,
}

impl CapturedGraph<'_> {
    fn capture(
        &mut self,
        inputs: &ImageToolExpressionInputs<'_>,
        root: Option<PathBuf>,
        directory: &Path,
        open_source: fn(&Path) -> PyResult<AttachedImage>,
        active: &mut HashSet<PathBuf>,
    ) -> PyResult<BoundImageToolOperands> {
        if let Some(path) = &root
            && !active.insert(path.clone())
        {
            return Err(expression_cycle(path));
        }
        let mut pending = Vec::new();
        let mut top = inputs.paths_and_entries();
        let mut compiled = BoundImageToolOperands::new();
        loop {
            let next = match pending.last_mut() {
                Some(frame) => PendingExpression::next_path(frame),
                None => top.next(),
            };
            if let Some((path, entry)) = next {
                if active.contains(path) {
                    return Err(expression_cycle(path));
                }
                if entry != path {
                    self.retain_operand_aliases(entry)?;
                }
                if !self.seen_paths.insert(path.to_path_buf()) {
                    continue;
                }
                self.paths.push(path.to_path_buf());
                if is_image_concat(path) {
                    let image = virtual_image::open_expression_leaf(path, active, self.output)?;
                    self.paths.extend(image.concat_expression_operands());
                    for operand in image.concat_operands() {
                        self.retain_operand_aliases(&operand)?;
                        self.retain_miriad_files(&operand)?;
                    }
                    let source = AttachedImage::temporary(image);
                    compiled.insert(path.to_path_buf(), bound_pixels(&source).map_err(raised)?);
                    self.retained.push(source);
                } else if is_image_expr(path) {
                    let path = path.to_path_buf();
                    active.insert(path.clone());
                    let (captured, manifest) = SavedExpressionDirectory::open(&path, path.clone())?;
                    self.retain_manifest(&captured)?;
                    let nested =
                        PendingExpression::from_manifest(path, manifest, directory, self.output)?;
                    pending.push(nested);
                } else if !path.is_file() {
                    self.retain_miriad_files(path)?;
                    self.retained.push(open_source(path)?);
                }
            } else if let Some(frame) = pending.pop() {
                let (pixels, single) = compile_manifest(&frame.inputs, &compiled)?;
                let misc = frame.manifest.misc_info().map_err(raised)?;
                active.remove(&frame.path);
                let pixels = match misc {
                    Some(misc) => replace_misc_info(pixels, misc),
                    None => pixels,
                };
                compiled.insert(frame.path, (pixels, single));
            } else {
                if let Some(path) = &root {
                    active.remove(path);
                }
                return Ok(compiled);
            }
        }
    }

    fn retain_manifest(&mut self, directory: &SavedExpressionDirectory) -> PyResult<()> {
        if self.include_manifest_paths
            && let Some((entry, referent)) = directory.linked_manifest_paths()
        {
            self.retain_operand_aliases(&entry)?;
            self.paths.push(referent.to_owned());
        }
        Ok(())
    }

    fn retain_miriad_files(&mut self, path: &Path) -> PyResult<()> {
        if ImageHandle::recognizes_miriad(path, path) {
            for entry in MiriadImage::linked_input_files(path) {
                self.retain_operand_aliases(&entry)?;
                self.paths.push(entry.canonicalize().map_err(raised)?);
            }
        }
        Ok(())
    }

    fn retain_operand_aliases(&mut self, entry: &Path) -> PyResult<()> {
        self.paths.push(lexical_absolute(entry));
        let mut routes = vec![entry.to_path_buf()];
        while let Some(route) = routes.pop() {
            for prefix in route.ancestors() {
                if !prefix
                    .symlink_metadata()
                    .is_ok_and(|entry| entry.is_symlink())
                {
                    continue;
                }
                let (Some(parent), Some(name)) = (prefix.parent(), prefix.file_name()) else {
                    continue;
                };
                let parent = parent.canonicalize().map_err(raised)?;
                let link = parent.join(name);
                if self.links.insert(link.clone()) {
                    routes.push(parent.join(fs::read_link(&link).map_err(raised)?));
                    self.paths.push(link);
                }
            }
        }
        Ok(())
    }
}

fn expression_cycle(path: &Path) -> PyErr {
    raised(format!(
        "Cycle in saved image expression at {}",
        path.display()
    ))
}

fn bound_pixels(source: &AttachedImage) -> Result<(ImageToolPixelExpression, bool), ImageError> {
    let handle = source.lock();
    macro_rules! bind {
        ($variant:ident, $storage:ident) => {
            bound_image(source, $storage, |handle| {
                let ImageHandle::$variant(storage) = handle else {
                    unreachable!("a retained source keeps its pixel type");
                };
                storage
            })?
        };
    }
    Ok(match &*handle {
        ImageHandle::Float(storage) => (
            ImageToolPixelExpression::Real(bind!(Float, storage).convert_pixels(f64::from)?),
            true,
        ),
        ImageHandle::Double(storage) => (
            ImageToolPixelExpression::Real(bind!(Double, storage)),
            false,
        ),
        ImageHandle::Complex(storage) => (
            ImageToolPixelExpression::Complex(bind!(Complex, storage).convert_pixels(|pixel| {
                Complex64::new(f64::from(pixel.re), f64::from(pixel.im))
            })?),
            true,
        ),
        ImageHandle::DComplex(storage) => (
            ImageToolPixelExpression::Complex(bind!(DComplex, storage)),
            false,
        ),
    })
}

fn bound_image<T: ImageExprValue>(
    source: &AttachedImage,
    storage: &Storage<T>,
    select: fn(&ImageHandle) -> &Storage<T>,
) -> Result<ImageExpression<T>, ImageError> {
    let pixels = source.share_image();
    let mask = source.share_image();
    let coordinate_record = match storage {
        Storage::Temp { record, .. } => Some(record),
        Storage::Paged(_) => None,
    };
    with_storage!(storage, image => ImageExprBuilder::from_image_sections(
        image.as_ref(),
        coordinate_record,
        move |start, count, stride| {
            let handle = pixels.lock();
            let storage = select(&handle);
            if let Storage::Temp { image, .. } = storage
                && let Some(concat) = image.concat_source()
            {
                concat.check_directory()?;
            }
            Ok(with_storage!(storage, image => Lattice::get_slice(image.as_ref(), start, count, stride))?)
        },
        move |start, count, stride| {
            let handle = mask.lock();
            let storage = select(&handle);
            if let Storage::Temp { image, .. } = storage
                && let Some(concat) = image.concat_source()
            {
                concat.check_directory()?;
            }
            with_storage!(storage, image => image.get_mask_slice(start, count, stride))
        },
    ))?
    .compile()
}

struct PendingExpression {
    path: PathBuf,
    manifest: ToolExpressionManifest,
    inputs: ImageToolExpressionInputs<'static>,
    next: usize,
}

impl PendingExpression {
    fn from_manifest(
        path: PathBuf,
        manifest: ToolExpressionManifest,
        directory: &Path,
        output: Option<&StagedOutput<'_>>,
    ) -> PyResult<Self> {
        let inputs = ImageToolExpressionInputs::resolve_with_image_paths(
            manifest.expression.clone(),
            directory,
            |path| output.and_then(|output| output.substitute(path)),
        )
        .map_err(raised)?;
        Ok(Self {
            path,
            manifest,
            inputs,
            next: 0,
        })
    }

    fn next_path(&mut self) -> Option<(&Path, &Path)> {
        let paths = self.inputs.paths_and_entries().nth(self.next)?;
        self.next += 1;
        Some(paths)
    }
}

fn compile_manifest(
    inputs: &ImageToolExpressionInputs<'_>,
    operands: &BoundImageToolOperands,
) -> PyResult<(ImageToolPixelExpression, bool)> {
    let (pixels, single) = inputs
        .compile_with_bound_operands(operands, true)
        .map_err(raised)?;
    super::imagecalc::check_geometry(&pixels, inputs.paths().len() != 0)?;
    Ok((pixels, single))
}

fn replace_misc_info(
    pixels: ImageToolPixelExpression,
    misc: RecordValue,
) -> ImageToolPixelExpression {
    match pixels {
        ImageToolPixelExpression::Real(expression) => {
            ImageToolPixelExpression::Real(expression.with_misc_info(misc))
        }
        ImageToolPixelExpression::Complex(expression) => {
            ImageToolPixelExpression::Complex(expression.with_misc_info(misc))
        }
        ImageToolPixelExpression::Boolean(_) => unreachable!("saved image geometry is numeric"),
    }
}

fn expression_storage<T: ImageExprValue>(
    expression: ImageExpression<T>,
    path: &Path,
    misc: Option<RecordValue>,
) -> PyResult<Storage<T>> {
    let record = expression.coordinate_record();
    let mut image =
        TempImage::from_saved_expression(expression, path.to_path_buf()).map_err(raised)?;
    if let Some(misc) = misc {
        image.set_misc_info(misc).map_err(raised)?;
    }
    Ok(Storage::Temp {
        image: Box::new(image),
        record,
        fits: None,
    })
}
