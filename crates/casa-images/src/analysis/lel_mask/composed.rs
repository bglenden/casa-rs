// SPDX-License-Identifier: LGPL-3.0-or-later

//! Read-only section access to saved expressions and virtual images used in masks.

use std::collections::{BTreeSet, HashSet};
use std::fs::File;
use std::sync::Arc;

use casa_core::{Complex64, RecordValue};
use casa_tables::{LockType, lexical_absolute};

use crate::concat_file::{ConcatManifest, is_image_concat};
use crate::concatenate::{ConcatInput, ConcatenationPlan};
use crate::expr_file::{ToolExpressionManifest, is_image_expr, read_tool_manifest_from_reader};
use crate::virtual_concat::{ConcatImage, ConcatLockState, ConcatMember};
use crate::{ImageInfo, ImagePixelType};

use super::{
    ArrayD, BoundImageToolOperands, ImageError, ImageExprBuilder, ImageExprValue, ImageExpression,
    ImageInterface, ImageToolExpressionInputs, ImageToolPixelExpression, Lattice, MaskSource,
    Operand, Path, PathBuf, convert_paged_mask_source, open_path_with_mask_name, saved_operand,
};

pub(super) fn recognizes(path: &Path) -> bool {
    is_image_concat(path) || is_image_expr(path)
}

pub(super) fn open(
    path: &Path,
    entry: &Path,
    temporary_locks: bool,
) -> Result<(Operand, Dependencies), ImageError> {
    let (frames, dependencies) = capture(path, entry, temporary_locks)?;
    let mut frames = frames.into_iter().peekable();
    let mut compiled = BoundImageToolOperands::new();
    while let Some(frame) = frames.next() {
        let (pixels, single) = frame.compile(&compiled)?;
        if frames.peek().is_none() {
            return Ok((saved_operand(&pixels, single)?, dependencies));
        }
        compiled.insert(frame.path, (pixels, single));
    }
    unreachable!("the graph has a root")
}

#[derive(Default)]
pub(super) struct Dependencies {
    entries: BTreeSet<PathBuf>,
    images: BTreeSet<PathBuf>,
}

impl Dependencies {
    pub(super) fn check_output(&self, output: &Path) -> Result<(), ImageError> {
        let route = lexical_absolute(output);
        let path = std::path::absolute(output).map_err(io)?;
        let entry = path
            .parent()
            .and_then(|parent| parent.canonicalize().ok())
            .zip(path.file_name())
            .map(|(parent, name)| parent.join(name));
        let referent = output.canonicalize().ok();
        for destination in std::iter::once(&route)
            .chain(entry.iter())
            .chain(referent.iter())
        {
            // Aliases are protected entries. Using the same directory alias
            // to address a disjoint sibling does not remove the alias itself.
            let overlap = self
                .entries
                .iter()
                .find(|entry| entry.starts_with(destination))
                .or_else(|| {
                    self.images
                        .iter()
                        .find(|image| destination.starts_with(image))
                });
            if let Some(dependency) = overlap {
                return Err(ImageError::CasaSelection(format!(
                    "Output path '{}' overlaps image mask operand '{}'",
                    output.display(),
                    dependency.display()
                )));
            }
        }
        Ok(())
    }
}

fn capture(
    path: &Path,
    entry: &Path,
    temporary_locks: bool,
) -> Result<(Vec<Frame>, Dependencies), ImageError> {
    let directory = crate::analysis::expression_workspace::directory()?;
    let first = Frame::open(
        path.canonicalize().map_err(io)?,
        &directory,
        temporary_locks,
    )?;
    let mut active = HashSet::from([first.path.clone()]);
    let mut completed = HashSet::new();
    let mut frames = vec![first];
    let mut ordered = Vec::new();
    let mut dependencies = Dependencies::default();
    capture_image(path, entry, &mut dependencies)?;

    while let Some(frame) = frames.last_mut() {
        if let Some((path, entry)) = frame.next_member() {
            capture_image(path, entry, &mut dependencies)?;
            if active.contains(path) {
                return Err(ImageError::CasaSelection(format!(
                    "Cycle in image expression or concatenation at {}",
                    path.display()
                )));
            }
            if recognizes(path) && !completed.contains(path) {
                let path = path.to_path_buf();
                let temporary = frame.temporary_locks;
                active.insert(path.clone());
                frames.push(Frame::open(path, &directory, temporary)?);
            }
        } else {
            let frame = frames.pop().expect("an active graph frame");
            capture_entry(&frame.descriptor, &mut dependencies.entries)?;
            dependencies
                .entries
                .insert(frame.resolved_descriptor.clone());
            active.remove(&frame.path);
            completed.insert(frame.path.clone());
            ordered.push(frame);
        }
    }
    Ok((ordered, dependencies))
}

fn io(error: std::io::Error) -> ImageError {
    ImageError::Io(error.to_string())
}

fn capture_image(
    path: &Path,
    entry: &Path,
    dependencies: &mut Dependencies,
) -> Result<(), ImageError> {
    capture_entry(path, &mut dependencies.entries)?;
    if entry != path {
        capture_entry(entry, &mut dependencies.entries)?;
    }
    if path.is_dir() {
        dependencies.images.insert(path.to_path_buf());
    }
    if crate::miriad_read::MiriadImage::recognizes_input(path, path) {
        for entry in crate::miriad_read::MiriadImage::linked_input_files(path) {
            capture_entry(&entry, &mut dependencies.entries)?;
        }
    }
    Ok(())
}

fn capture_entry(path: &Path, dependencies: &mut BTreeSet<PathBuf>) -> Result<(), ImageError> {
    dependencies.insert(lexical_absolute(path));
    dependencies.insert(path.canonicalize().map_err(io)?);
    let mut routes = vec![path.to_path_buf()];
    let mut seen_links = HashSet::new();
    while let Some(route) = routes.pop() {
        for candidate in route.ancestors() {
            if !candidate
                .symlink_metadata()
                .is_ok_and(|entry| entry.is_symlink())
            {
                continue;
            }
            let parent = candidate.parent().expect("symlink parent");
            let parent = if parent.as_os_str().is_empty() {
                Path::new(".")
            } else {
                parent
            };
            let parent = parent.canonicalize().map_err(io)?;
            let link = parent.join(candidate.file_name().expect("symlink name"));
            if seen_links.insert(link.clone()) {
                routes.push(parent.join(link.read_link().map_err(io)?));
                dependencies.insert(link);
            }
        }
    }
    Ok(())
}

enum Source {
    Saved {
        manifest: ToolExpressionManifest,
        inputs: ImageToolExpressionInputs<'static>,
    },
    Concat {
        manifest: ConcatManifest,
        paths: Vec<(PathBuf, PathBuf)>,
    },
}

struct Frame {
    path: PathBuf,
    descriptor: PathBuf,
    resolved_descriptor: PathBuf,
    source: Source,
    temporary_locks: bool,
    next: usize,
}

impl Frame {
    fn open(
        path: PathBuf,
        directory: &Path,
        mut temporary_locks: bool,
    ) -> Result<Self, ImageError> {
        let is_concat = is_image_concat(&path);
        let descriptor = path.join(if is_concat {
            "imageconcat.json"
        } else {
            "imageexpr.json"
        });
        let resolved_descriptor = descriptor.canonicalize().map_err(io)?;
        let source = if is_concat {
            let manifest = ConcatManifest::open_descriptor(&resolved_descriptor)?;
            temporary_locks |= manifest.temp_close();
            let paths = manifest
                .paths(&path)
                .into_iter()
                .map(|entry| Ok((entry.canonicalize().map_err(io)?, entry)))
                .collect::<Result<_, ImageError>>()?;
            Source::Concat { manifest, paths }
        } else {
            let manifest =
                read_tool_manifest_from_reader(File::open(&resolved_descriptor).map_err(io)?)?;
            let inputs = ImageToolExpressionInputs::resolve_in_directory(
                manifest.expression.clone(),
                directory,
            )?;
            Source::Saved { manifest, inputs }
        };
        Ok(Self {
            path,
            descriptor,
            resolved_descriptor,
            source,
            temporary_locks,
            next: 0,
        })
    }

    fn next_member(&mut self) -> Option<(&Path, &Path)> {
        let paths = match &self.source {
            Source::Saved { inputs, .. } => inputs.paths_and_entries().nth(self.next),
            Source::Concat { paths, .. } => paths
                .get(self.next)
                .map(|(path, entry)| (path.as_path(), entry.as_path())),
        }?;
        self.next += 1;
        Some(paths)
    }

    fn compile(
        &self,
        compiled: &BoundImageToolOperands,
    ) -> Result<(ImageToolPixelExpression, bool), ImageError> {
        match &self.source {
            Source::Saved { manifest, inputs } => {
                let (pixels, single) =
                    inputs.compile_with_bound_operands(compiled, self.temporary_locks)?;
                let misc = manifest.misc_info()?;
                let pixels = match pixels {
                    ImageToolPixelExpression::Real(real) if !real.shape().is_empty() => {
                        ImageToolPixelExpression::Real(match misc {
                            Some(record) => real.with_misc_info(record),
                            None => real,
                        })
                    }
                    ImageToolPixelExpression::Complex(complex) if !complex.shape().is_empty() => {
                        ImageToolPixelExpression::Complex(match misc {
                            Some(record) => complex.with_misc_info(record),
                            None => complex,
                        })
                    }
                    _ => {
                        return Err(ImageError::CasaSelection(
                            "A saved image expression must have numeric image pixels".into(),
                        ));
                    }
                };
                Ok((pixels, single))
            }
            Source::Concat { manifest, paths } => {
                let operands = paths
                    .iter()
                    .map(|(path, _)| match compiled.get(path) {
                        Some((pixels, single)) => saved_operand(pixels, *single),
                        None => open_path_with_mask_name(path, None, self.temporary_locks),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                self.join(manifest, paths, operands)
            }
        }
    }

    fn join(
        &self,
        manifest: &ConcatManifest,
        paths: &[(PathBuf, PathBuf)],
        operands: Vec<Operand>,
    ) -> Result<(ImageToolPixelExpression, bool), ImageError> {
        let kind = manifest.pixel_type()?;
        let axis = i64::try_from(manifest.axis())
            .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?;
        match kind {
            ImagePixelType::Float32 | ImagePixelType::Float64 => {
                let single = kind == ImagePixelType::Float32;
                let members = operands
                    .into_iter()
                    .map(|operand| match operand {
                        Operand::Real(source, source_single) if source_single == single => {
                            sections(source)
                        }
                        _ => Err(incompatible_pixels()),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let pixels = join_sections(&self.path, manifest, paths, axis, members)?;
                Ok((ImageToolPixelExpression::Real(pixels), single))
            }
            ImagePixelType::Complex32 | ImagePixelType::Complex64 => {
                let single = kind == ImagePixelType::Complex32;
                let members = operands
                    .into_iter()
                    .map(|operand| match operand {
                        Operand::Complex(source, source_single) if source_single == single => {
                            sections(source)
                        }
                        _ => Err(incompatible_pixels()),
                    })
                    .collect::<Result<Vec<ImageExpression<Complex64>>, _>>()?;
                let pixels = join_sections(&self.path, manifest, paths, axis, members)?;
                Ok((ImageToolPixelExpression::Complex(pixels), single))
            }
        }
    }
}

fn incompatible_pixels() -> ImageError {
    ImageError::CasaSelection(
        "Concatenation of images of different data types is not supported".into(),
    )
}

fn sections<T: ImageExprValue>(source: MaskSource<T>) -> Result<ImageExpression<T>, ImageError> {
    let source = match source {
        MaskSource::Sections(sections) => return Ok(*sections),
        MaskSource::Paged(image) => convert_paged_mask_source(*image, None, false, |value| value)?,
    };
    let MaskSource::Sections(expression) = source else {
        unreachable!("owned section readers")
    };
    Ok(*expression)
}

fn join_sections<T: ImageExprValue>(
    path: &Path,
    manifest: &ConcatManifest,
    paths: &[(PathBuf, PathBuf)],
    axis: i64,
    members: Vec<ImageExpression<T>>,
) -> Result<ImageExpression<T>, ImageError> {
    let geometry = members
        .iter()
        .zip(paths)
        .map(|(image, (_, path))| ConcatInput {
            path,
            shape: image.shape(),
            coordinates: image.coordinates(),
            image_info: image.image_info_ref(),
            brightness_unit: image.units(),
        })
        .collect::<Vec<_>>();
    let plan = ConcatenationPlan::from_manifest(&geometry, axis)?;
    let units = members[0].units().to_owned();
    let members = members
        .into_iter()
        .zip(paths)
        .map(|(expression, (path, _))| {
            Arc::new(ReadOnlyMember {
                path: path.clone(),
                expression,
            }) as Arc<dyn ConcatMember<T>>
        })
        .collect();
    let image = Arc::new(ConcatImage::new(
        path.to_path_buf(),
        plan,
        members,
        Vec::new(),
        manifest.misc_info()?.unwrap_or_default(),
        units,
        manifest.temp_close(),
    )?);
    let pixels = Arc::clone(&image);
    let mask = Arc::clone(&image);
    ImageExprBuilder::from_image_sections(
        image.as_ref(),
        None,
        move |start, count, stride| Ok(pixels.get_slice(start, count, stride)?),
        move |start, count, stride| mask.get_mask_slice(start, count, stride),
    )?
    .compile()
}

struct ReadOnlyMember<T: ImageExprValue> {
    path: PathBuf,
    expression: ImageExpression<T>,
}

impl<T: ImageExprValue> ConcatMember<T> for ReadOnlyMember<T> {
    fn path(&self) -> &Path {
        &self.path
    }
    fn shape(&self) -> &[usize] {
        self.expression.shape()
    }
    fn is_writable(&self) -> bool {
        false
    }
    fn has_mask(&self) -> bool {
        self.expression.has_mask()
    }
    fn mask_writable(&self) -> bool {
        false
    }
    fn read(
        &self,
        start: &[usize],
        count: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, ImageError> {
        Ok(Lattice::get_slice(&self.expression, start, count, stride)?)
    }

    fn read_mask(
        &self,
        start: &[usize],
        count: &[usize],
        stride: &[usize],
    ) -> Result<Option<ArrayD<bool>>, ImageError> {
        ImageInterface::get_mask_slice(&self.expression, start, count, stride)
    }

    fn write(&self, _: &ArrayD<T>, _: &[usize]) -> Result<(), ImageError> {
        Err(read_only())
    }
    fn write_mask(&self, _: &ArrayD<bool>, _: &[usize]) -> Result<(), ImageError> {
        Err(read_only())
    }
    fn check_image_info_write(&self) -> Result<(), ImageError> {
        Err(read_only())
    }
    fn write_image_info(
        &self,
        _: &ImageInfo,
        _: &RecordValue,
        _: usize,
        _: usize,
    ) -> Result<(), ImageError> {
        Err(read_only())
    }

    // The expression's section readers already hold or acquire their own read leases.
    fn lock(&self, kind: LockType, _: u32) -> Result<bool, ImageError> {
        Ok(kind == LockType::Read)
    }
    fn has_lock(&self, _: LockType) -> bool {
        false
    }
    fn lock_state(&self) -> ConcatLockState {
        ConcatLockState::Leaf(None)
    }
    fn restore_locks(&self, _: ConcatLockState) -> Result<(), ImageError> {
        Ok(())
    }
    fn unlock(&self) -> Result<(), ImageError> {
        Ok(())
    }
    fn release_requested_lock(&self) -> Result<(), ImageError> {
        Ok(())
    }
}

fn read_only() -> ImageError {
    ImageError::CasaSelection("An analysis image operand is read-only".into())
}
