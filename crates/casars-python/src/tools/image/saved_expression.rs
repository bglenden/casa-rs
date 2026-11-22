// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{HashMap, HashSet};
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
    let directory = std::env::current_dir().map_err(raised)?;
    let root = path.canonicalize().map_err(raised)?;
    let (captured, manifest) = SavedExpressionDirectory::open(path, root.clone())?;
    let initial = PendingExpression::from_manifest(root, manifest, &directory)?;
    let mut graph = CapturedGraph::default();
    let (pixels, single_precision) = graph.compile(initial, &directory)?;
    captured.check_name(path)?;
    let image = match pixels {
        ImageToolPixelExpression::Real(expression) if single_precision => {
            ImageHandle::Float(expression_storage(
                expression
                    .convert_pixels(|value| value as f32)
                    .map_err(raised)?,
                path,
            )?)
        }
        ImageToolPixelExpression::Real(expression) => {
            ImageHandle::Double(expression_storage(expression, path)?)
        }
        ImageToolPixelExpression::Complex(expression) if single_precision => {
            ImageHandle::Complex(expression_storage(
                expression
                    .convert_pixels(|value| Complex32::new(value.re as f32, value.im as f32))
                    .map_err(raised)?,
                path,
            )?)
        }
        ImageToolPixelExpression::Complex(expression) => {
            ImageHandle::DComplex(expression_storage(expression, path)?)
        }
        ImageToolPixelExpression::Boolean(_) => unreachable!("saved image geometry is numeric"),
    };
    let mut attachment = AttachedImage::with_saved_expression(image, captured);
    attachment.retain_expression_inputs(graph.retained, graph.paths);
    Ok(attachment)
}

#[derive(Default)]
struct CapturedGraph {
    paths: Vec<PathBuf>,
    seen_paths: HashSet<PathBuf>,
    retained: Vec<AttachedImage>,
}

impl CapturedGraph {
    fn compile(
        &mut self,
        root: PendingExpression,
        directory: &Path,
    ) -> PyResult<(ImageToolPixelExpression, bool)> {
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
                    self.paths.push(lexical_absolute(entry));
                }
                if !self.seen_paths.insert(path.to_path_buf()) {
                    continue;
                }
                self.paths.push(path.to_path_buf());
                if is_image_expr(path) {
                    active.insert(path.to_path_buf());
                    let nested = PendingExpression::read(path.to_path_buf(), directory)?;
                    pending.push(nested);
                } else {
                    self.retained
                        .extend(AttachedImage::open_expression_inputs(std::iter::once(
                            path,
                        ))?);
                }
            } else {
                let frame = pending.pop().expect("checked for an expression frame");
                let result = frame.compile(&compiled)?;
                if pending.is_empty() {
                    return Ok(result);
                }
                active.remove(&frame.path);
                compiled.insert(frame.path, result);
            }
        }
        unreachable!("completed graph returns its root");
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
        let (mut pixels, single) = self
            .inputs
            .compile_with_saved_operands(compiled)
            .map_err(raised)?;
        super::imagecalc::check_geometry(&pixels, self.inputs.paths().len() != 0)?;
        if let Some(misc) = self.manifest.misc_info().map_err(raised)? {
            pixels = replace_misc_info(pixels, misc);
        }
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
) -> PyResult<Storage<T>> {
    let record = expression.coordinate_record();
    let image = TempImage::from_saved_expression(expression, path.to_path_buf()).map_err(raised)?;
    Ok(Storage::Temp {
        image: Box::new(image),
        record,
        fits: None,
    })
}
