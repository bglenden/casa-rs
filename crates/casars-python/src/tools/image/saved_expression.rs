// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use casa_images::analysis::{ImageToolExpressionInputs, ImageToolPixelExpression};
use casa_images::expr_file::{ToolExpressionManifest, is_image_expr, read_tool_manifest};
use casa_images::{ImageExpression, TempImage, image_expr::ImageExprValue};
use casa_tables::lexical_absolute;
use casa_types::{Complex32, RecordValue};
use pyo3::prelude::*;

use super::attachment::AttachedImage;
use super::handle::{ImageHandle, Storage, raised};

mod manifest;
mod rename;
pub(super) use manifest::SavedExpressionDirectory;

pub(super) fn open(path: &Path) -> PyResult<AttachedImage> {
    let (image, inputs) = compile_at(path, AttachedImage::open)?;
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
) -> PyResult<(ImageHandle, SavedExpressionInputs)> {
    // Final publication already holds the image registry. Source leases can
    // still be opened directly without consulting or registering in it.
    compile_at(path, AttachedImage::open_concat_expression_operand)
}

fn compile_at(
    path: &Path,
    open_source: fn(&Path) -> PyResult<AttachedImage>,
) -> PyResult<(ImageHandle, SavedExpressionInputs)> {
    let directory = std::env::current_dir().map_err(raised)?;
    let root = path.canonicalize().map_err(raised)?;
    let (captured, manifest) = SavedExpressionDirectory::open(path, root.clone())?;
    let initial = PendingExpression::from_manifest(root, manifest, &directory)?;
    let mut graph = CapturedGraph::default();
    let (pixels, single_precision, misc) = graph.compile(initial, &directory, open_source)?;
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
}

impl CapturedGraph {
    fn compile(
        &mut self,
        root: PendingExpression,
        directory: &Path,
        open_source: fn(&Path) -> PyResult<AttachedImage>,
    ) -> PyResult<(ImageToolPixelExpression, bool, Option<RecordValue>)> {
        let mut active = HashSet::from([root.path.clone()]);
        let mut pending = vec![root];
        let mut compiled = HashMap::new();
        while let Some(frame) = pending.last_mut() {
            if let Some((path, entry)) = frame.next_path() {
                if active.contains(path) {
                    return Err(raised(format!(
                        "Cycle in saved image expression at {}",
                        path.display()
                    )));
                }
                if entry != path {
                    self.retain_operand_aliases(entry)?;
                }
                if !self.seen_paths.insert(path.to_path_buf()) {
                    continue;
                }
                self.paths.push(path.to_path_buf());
                if is_image_expr(path) {
                    active.insert(path.to_path_buf());
                    let nested = PendingExpression::read(path.to_path_buf(), directory)?;
                    pending.push(nested);
                } else if !path.is_file() {
                    self.retained.push(open_source(path)?);
                }
            } else {
                let frame = pending.pop().expect("checked for an expression frame");
                let (pixels, single) = frame.compile(&compiled)?;
                let misc = frame.manifest.misc_info().map_err(raised)?;
                if pending.is_empty() {
                    return Ok((pixels, single, misc));
                }
                let pixels = match misc {
                    Some(misc) => replace_misc_info(pixels, misc),
                    None => pixels,
                };
                active.remove(&frame.path);
                compiled.insert(frame.path, (pixels, single));
            }
        }
        unreachable!("completed graph returns its root");
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

struct PendingExpression {
    path: PathBuf,
    manifest: ToolExpressionManifest,
    inputs: ImageToolExpressionInputs<'static>,
    next: usize,
}

impl PendingExpression {
    fn read(path: PathBuf, directory: &Path) -> PyResult<Self> {
        if path.join("imageconcat.json").is_file() {
            return Err(raised(
                "Opening a virtual concatenated image is not implemented yet (#1749)",
            ));
        }
        let manifest = read_tool_manifest(&path).map_err(raised)?;
        Self::from_manifest(path, manifest, directory)
    }

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
