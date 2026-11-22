// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use casa_images::analysis::{ImageToolExpressionInputs, ImageToolPixelExpression};
use casa_images::concat_file::is_image_concat;
use casa_images::expr_file::{ToolExpressionManifest, is_image_expr};
use casa_images::{
    ImageError, ImageExprBuilder, ImageExpression, ImageInterface, TempImage,
    image_expr::ImageExprValue,
};
use casa_lattices::Lattice;
use casa_tables::lexical_absolute;
use casa_types::{Complex32, Complex64, RecordValue};
use pyo3::prelude::*;

use super::attachment::AttachedImage;
use super::concatenate::virtual_image;
use super::handle::{ImageHandle, Storage, raised, with_storage};

mod manifest;
mod rename;
pub(super) use manifest::SavedExpressionDirectory;

pub(super) fn open(path: &Path) -> PyResult<AttachedImage> {
    let (image, inputs) = compile_at(path, AttachedImage::open, false, &mut HashSet::new())?;
    let mut attachment = AttachedImage::with_saved_expression(image, inputs.directory);
    attachment.retain_expression_inputs(inputs.retained, inputs.paths);
    Ok(attachment)
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
) -> PyResult<(ImageHandle, SavedExpressionInputs)> {
    // Final publication already holds the image registry. Source leases can
    // still be opened directly without consulting or registering in it.
    compile_at(
        path,
        AttachedImage::open_concat_expression_operand,
        true,
        active,
    )
}

fn compile_at(
    path: &Path,
    open_source: fn(&Path) -> PyResult<AttachedImage>,
    include_manifest_paths: bool,
    active: &mut HashSet<PathBuf>,
) -> PyResult<(ImageHandle, SavedExpressionInputs)> {
    let directory = std::env::current_dir().map_err(raised)?;
    let root = path.canonicalize().map_err(raised)?;
    let (captured, manifest) = SavedExpressionDirectory::open(path, root.clone())?;
    let initial = PendingExpression::from_manifest(root, manifest, &directory)?;
    let mut graph = CapturedGraph {
        include_manifest_paths,
        ..CapturedGraph::default()
    };
    graph.retain_manifest(&captured)?;
    let (pixels, single_precision, misc) =
        graph.compile(initial, &directory, open_source, active)?;
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
struct CapturedGraph {
    paths: Vec<PathBuf>,
    seen_paths: HashSet<PathBuf>,
    links: HashSet<PathBuf>,
    retained: Vec<AttachedImage>,
    include_manifest_paths: bool,
}

impl CapturedGraph {
    fn compile(
        &mut self,
        root: PendingExpression,
        directory: &Path,
        open_source: fn(&Path) -> PyResult<AttachedImage>,
        active: &mut HashSet<PathBuf>,
    ) -> PyResult<(ImageToolPixelExpression, bool, Option<RecordValue>)> {
        if !active.insert(root.path.clone()) {
            return Err(expression_cycle(&root.path));
        }
        let mut pending = vec![root];
        let mut compiled = HashMap::new();
        while let Some(frame) = pending.last_mut() {
            if let Some((path, entry)) = frame.next_path() {
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
                    let image = virtual_image::open_expression_leaf(path, active)?;
                    self.paths.extend(image.concat_expression_operands());
                    for operand in image.concat_operands() {
                        self.retain_operand_aliases(&operand)?;
                    }
                    let source = AttachedImage::temporary(image);
                    compiled.insert(path.to_path_buf(), bound_pixels(&source).map_err(raised)?);
                    self.retained.push(source);
                } else if is_image_expr(path) {
                    let path = path.to_path_buf();
                    active.insert(path.clone());
                    let (captured, manifest) = SavedExpressionDirectory::open(&path, path.clone())?;
                    self.retain_manifest(&captured)?;
                    let nested = PendingExpression::from_manifest(path, manifest, directory)?;
                    pending.push(nested);
                } else if !path.is_file() {
                    self.retained.push(open_source(path)?);
                }
            } else {
                let frame = pending.pop().expect("checked for an expression frame");
                let (pixels, single) = frame.compile(&compiled)?;
                let misc = frame.manifest.misc_info().map_err(raised)?;
                active.remove(&frame.path);
                if pending.is_empty() {
                    return Ok((pixels, single, misc));
                }
                let pixels = match misc {
                    Some(misc) => replace_misc_info(pixels, misc),
                    None => pixels,
                };
                compiled.insert(frame.path, (pixels, single));
            }
        }
        unreachable!("completed graph returns its root");
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
    with_storage!(storage, image => ImageExprBuilder::from_image_sections(
        image.as_ref(),
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
    ) -> PyResult<Self> {
        let inputs =
            ImageToolExpressionInputs::resolve_in_directory(manifest.expression.clone(), directory)
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

    fn compile(
        &self,
        compiled: &HashMap<PathBuf, (ImageToolPixelExpression, bool)>,
    ) -> PyResult<(ImageToolPixelExpression, bool)> {
        let (pixels, single) = self
            .inputs
            .compile_with_saved_operands(compiled)
            .map_err(raised)?;
        super::imagecalc::check_geometry(&pixels, self.inputs.paths().len() != 0)?;
        Ok((pixels, single))
    }
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
