// SPDX-License-Identifier: LGPL-3.0-or-later
//! LEL mask evaluation, `image:maskname` resolution, and `stretch` handling
//! for image tasks.

mod composed;
mod geometry;
pub(super) use geometry::validate_mask;

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use casa_coordinates::CoordinateSystem;
use casa_lattices::Lattice;
use casa_tables::TableUseLock;
use casa_types::{ArrayD, Complex32, Complex64};
use ndarray::{IxDyn, Slice};

use crate::analysis::axes::StridedSection;
use crate::analysis::mask_error_wording;
use crate::expr_parser::{
    HashMapResolver, ImageToolPixelExpression, ReplacementPixels, compile_image_tool_pixels,
    evaluate_real_scalar, evaluate_replacement, image_tool_mask_builder, parse_image_tool_mask,
    referenced_images, subimage_mask_builder,
};
use crate::fits_read::{FitsToolImport, SharedFitsSectionReader};
use crate::image_expr::{
    ImageExprMeta, ImageExprValue, MaskExpression, bounded_read_cursor, validate_slice_request,
};
use crate::image_expr_lel::broadcast_array;
use crate::miriad_read::MiriadImage;
use crate::{
    AnyPagedImage, ComponentListImage, ImageError, ImageExprBuilder, ImageExpression,
    ImageInterface, ImagePixel, PagedImage,
};

enum MaskSource<T: ImageExprValue> {
    Paged(Box<PagedImage<T>>),
    Sections(Box<ImageExpression<T>>),
}

impl<T: ImageExprValue> MaskSource<T> {
    fn as_interface(&self) -> &dyn ImageInterface<T> {
        match self {
            Self::Paged(img) => img.as_ref(),
            Self::Sections(img) => img.as_ref(),
        }
    }
}

enum Operand {
    Real(MaskSource<f64>, bool),
    Complex(MaskSource<Complex64>, bool),
}
impl Operand {
    fn geometry(&self) -> (&[usize], &CoordinateSystem) {
        match self {
            Self::Real(source, _) => (
                source.as_interface().shape(),
                source.as_interface().coordinates(),
            ),
            Self::Complex(source, _) => (
                source.as_interface().shape(),
                source.as_interface().coordinates(),
            ),
        }
    }
}

fn is_single_precision(opened: &[(String, Operand)], name: &str) -> bool {
    opened.iter().any(|(source, value)| {
        source == name && matches!(value, Operand::Real(_, true) | Operand::Complex(_, true))
    })
}

/// Compile a real or complex LEL pixel expression for the public image tool.
///
/// # Errors
/// Returns an error for invalid expressions or undefined operands.
pub fn compile_image_tool_expression(
    expression: &str,
) -> Result<ImageToolPixelExpression, ImageError> {
    ImageToolExpressionInputs::resolve(expression)?.compile()
}

struct ResolvedOperand {
    name: String,
    path: PathBuf,
    entry: PathBuf,
    mask: Option<String>,
}

/// Resolved image operand paths for a public calculator expression.
pub struct ImageToolExpressionInputs<'a> {
    expression: Cow<'a, str>,
    operands: Vec<ResolvedOperand>,
}

/// Pre-opened numeric operands keyed by resolved image path.
pub type BoundImageToolOperands = HashMap<PathBuf, (ImageToolPixelExpression, bool)>;

impl<'a> ImageToolExpressionInputs<'a> {
    /// Resolve the operand paths relative to the current working directory.
    /// # Errors
    /// Returns an error for a missing operand or invalid expression.
    pub fn resolve(expression: &'a str) -> Result<Self, ImageError> {
        let directory = super::expression_workspace::directory()?;
        Self::resolve_in_directory(expression, &directory)
    }

    /// Resolve operand names against an already captured working directory.
    ///
    /// # Errors
    /// Returns an error for a missing operand or invalid expression.
    pub fn resolve_in_directory(
        expression: impl Into<Cow<'a, str>>,
        directory: &Path,
    ) -> Result<Self, ImageError> {
        Self::resolve_with_image_paths(expression, directory, |_| None)
    }

    /// Resolve image names in a directory, allowing a caller to substitute a
    /// staged image for an operand before the new image is published.
    ///
    /// # Errors
    /// Returns an error for a missing operand or invalid expression.
    pub fn resolve_with_image_paths(
        expression: impl Into<Cow<'a, str>>,
        directory: &Path,
        substitute: impl Fn(&Path) -> Option<PathBuf>,
    ) -> Result<Self, ImageError> {
        let expression = expression.into();
        if expression.is_empty() {
            return Err(ImageError::CasaSelection(
                "Exception: You must specify an expression.".into(),
            ));
        }
        let operands = referenced_images(&expression)?
            .into_iter()
            .map(|name| resolve_named_operand_with(directory, name, &substitute))
            .collect::<Result<_, _>>()?;
        Ok(Self {
            expression,
            operands,
        })
    }

    /// Paths of the images supplying expression values or masks.
    pub fn paths(&self) -> impl ExactSizeIterator<Item = &Path> {
        self.operands.iter().map(|operand| operand.path.as_path())
    }

    /// Resolved paths and the directory entries used to reach them.
    pub fn paths_and_entries(&self) -> impl ExactSizeIterator<Item = (&Path, &Path)> {
        self.operands
            .iter()
            .map(|operand| (operand.path.as_path(), operand.entry.as_path()))
    }

    /// Compile the expression against these resolved operands.
    /// # Errors
    /// Returns an error if an operand is unreadable or the expression is invalid.
    pub fn compile(&self) -> Result<ImageToolPixelExpression, ImageError> {
        self.compile_with_source_locks(false)
            .map(|(pixels, _)| pixels)
    }

    /// Compile an expression whose owning tool can release its source locks.
    ///
    /// # Errors
    /// Returns an error if an operand is unreadable or the expression is invalid.
    pub fn compile_lazy(&self) -> Result<ImageToolPixelExpression, ImageError> {
        self.compile_lazy_inferred().map(|(pixels, _)| pixels)
    }

    /// Compile a lazy expression and return whether LEL inferred single precision.
    ///
    /// # Errors
    /// Returns an error if an operand is unreadable or the expression is invalid.
    pub fn compile_lazy_inferred(&self) -> Result<(ImageToolPixelExpression, bool), ImageError> {
        self.compile_with_source_locks(true)
    }

    /// Compile a numeric expression with supplied operands and a source lock policy.
    ///
    /// Callers supply already validated expressions keyed by resolved path. Each
    /// saved operand is captured as a section reader with its original precision
    /// and intrinsic validity. CASA ignores mask selectors on saved expressions.
    ///
    /// # Errors
    /// Returns an error for an unreadable operand or an invalid expression.
    pub fn compile_with_bound_operands(
        &self,
        bound: &BoundImageToolOperands,
        temporary_source_locks: bool,
    ) -> Result<(ImageToolPixelExpression, bool), ImageError> {
        let opened = self.open_bound_operands(bound, temporary_source_locks)?;
        self.compile_opened(&opened)
    }

    /// Evaluate a Boolean mask using supplied saved or virtual image operands.
    ///
    /// # Errors
    /// Returns an error for invalid expressions, operands, or target shapes.
    pub fn evaluate_mask_with_bound_operands(
        &self,
        bound: &BoundImageToolOperands,
        target_shape: &[usize],
    ) -> Result<ArrayD<bool>, ImageError> {
        let opened = self.open_bound_operands(bound, false)?;
        evaluate_image_tool_mask_opened(&self.expression, target_shape, &opened)
    }

    fn open_bound_operands(
        &self,
        bound: &BoundImageToolOperands,
        temporary_source_locks: bool,
    ) -> Result<Vec<(String, Operand)>, ImageError> {
        self.operands
            .iter()
            .map(|operand| {
                let source = match bound.get(&operand.path) {
                    Some((pixels, single_precision)) => saved_operand(pixels, *single_precision)?,
                    None => open_path_with_mask_name(
                        &operand.path,
                        operand.mask.clone(),
                        temporary_source_locks,
                    )?,
                };
                Ok((operand.name.clone(), source))
            })
            .collect()
    }

    fn compile_with_source_locks(
        &self,
        temporary_source_locks: bool,
    ) -> Result<(ImageToolPixelExpression, bool), ImageError> {
        let opened = open_operands(&self.operands, temporary_source_locks)?;
        self.compile_opened(&opened)
    }

    fn compile_opened(
        &self,
        opened: &[(String, Operand)],
    ) -> Result<(ImageToolPixelExpression, bool), ImageError> {
        let (real, complex) = operand_resolvers(opened);
        compile_image_tool_pixels(&self.expression, &real, &complex, &|name| {
            is_single_precision(opened, name)
        })
    }
}

fn saved_operand(pixels: &ImageToolPixelExpression, single: bool) -> Result<Operand, ImageError> {
    match pixels {
        ImageToolPixelExpression::Real(image) => {
            let sections = image.clone().convert_pixels(move |value| {
                if single {
                    f64::from(value as f32)
                } else {
                    value
                }
            })?;
            Ok(Operand::Real(
                MaskSource::Sections(Box::new(sections)),
                single,
            ))
        }
        ImageToolPixelExpression::Complex(image) => {
            let sections = image.clone().convert_pixels(move |value| {
                if single {
                    Complex64::new(f64::from(value.re as f32), f64::from(value.im as f32))
                } else {
                    value
                }
            })?;
            Ok(Operand::Complex(
                MaskSource::Sections(Box::new(sections)),
                single,
            ))
        }
        ImageToolPixelExpression::Boolean(_) => Err(ImageError::CasaSelection(
            "A saved expression operand must have numeric pixels".into(),
        )),
    }
}

/// Evaluate the Boolean mask used by the public image tool's `calcmask`.
/// The expression must match the target shape or evaluate to a Boolean scalar.
///
/// # Errors
/// Returns an error for invalid expressions, operands, or target shapes.
pub fn evaluate_image_tool_mask(
    expression: &str,
    target_shape: &[usize],
) -> Result<ArrayD<bool>, ImageError> {
    let opened = image_operands(expression)?;
    evaluate_image_tool_mask_opened(expression, target_shape, &opened)
}

fn evaluate_image_tool_mask_opened(
    expression: &str,
    target_shape: &[usize],
    opened: &[(String, Operand)],
) -> Result<ArrayD<bool>, ImageError> {
    let (real, complex) = operand_resolvers(opened);
    let parsed = parse_image_tool_mask(expression, &real, &complex, &|name| {
        is_single_precision(opened, name)
    })?;
    if parsed.shape().is_empty() && !parsed.is_shape_polymorphic() {
        return Ok(ArrayD::from_elem(
            ndarray::IxDyn(target_shape),
            parsed.get_at(&[])?,
        ));
    }
    if parsed.shape() != target_shape {
        return Err(ImageError::CasaSelection(format!(
            "The shape of the expression does not conform with the shape of the output imageExpression shape = {:?}Image shape      = {target_shape:?}",
            parsed.shape(),
        )));
    }
    if parsed.is_shape_polymorphic() {
        return Err(ImageError::CasaSelection(
            "LatticeExprNode - shapes of operands mismatch".into(),
        ));
    }
    parsed.get_in_sections()
}

/// Evaluate a Boolean image-tool mask over a strided section of a target image.
///
/// The section holds the start, sample count, and stride of each image axis.
/// Real and complex operands may be combined in one expression. FITS operands
/// are read by section, and `stretch` allows compatible singleton axes.
///
/// # Errors
/// Returns an error for invalid expressions or incompatible source axes.
pub fn evaluate_image_tool_mask_section(
    expression: &str,
    shape: &[usize],
    coordinates: &CoordinateSystem,
    section: (&[usize], &[usize], &[usize]),
    stretch: bool,
) -> Result<ArrayD<bool>, ImageError> {
    let (start, count, stride) = section;
    validate_slice_request(shape, start, count, stride)?;
    let opened = image_operands(expression)?;
    let (real, complex) = operand_resolvers(&opened);
    let mask = image_tool_mask_builder(
        expression,
        &real,
        &complex,
        &|name| is_single_precision(&opened, name),
        true,
    )?;
    let source_shape = mask.shape();
    if source_shape.is_empty() {
        return mask
            .compile()?
            .eval_section_for_shape(start, count, stride, shape);
    }
    if source_shape != shape && !stretch {
        return Err(ImageError::CasaSelection(format!(
            "The input image shape ({shape:?}) and mask shape ({source_shape:?}) are different, and it was specified that the mask should not be extended"
        )));
    }
    let axes = geometry::image_tool_mask_axes(
        mask.expression_coordinates(),
        source_shape,
        coordinates,
        shape,
    )?;
    let select = |values: &[usize]| axes.iter().map(|&axis| values[axis]).collect::<Vec<_>>();
    let mut mask = mask.compile()?.eval_section_for_shape(
        &select(start),
        &select(count),
        &select(stride),
        &select(shape),
    )?;
    for axis in 0..shape.len() {
        if axes.binary_search(&axis).is_err() {
            mask = mask.insert_axis(ndarray::Axis(axis));
        }
    }
    broadcast_array(mask, count)
}

pub(super) fn evaluate_replacement_pixels(
    expression: &str,
    selected_shape: &[usize],
    want_complex: bool,
) -> Result<ReplacementPixels, ImageError> {
    if expression.trim() == "[]" {
        return Err(ImageError::CasaSelection(
            "Image Expression: Parse error at or near ']'".into(),
        ));
    }
    let opened = image_operands(expression)?;
    let (real, complex) = operand_resolvers(&opened);
    evaluate_replacement(
        expression,
        &real,
        &complex,
        &|name| is_single_precision(&opened, name),
        selected_shape,
        want_complex,
    )
}

/// Evaluate a scalar LEL expression used to fill the selected image pixels.
pub fn evaluate_pixel_expression(expression: &str) -> Result<f32, ImageError> {
    let opened = image_operands(expression)?;
    let (real, complex) = operand_resolvers(&opened);
    evaluate_real_scalar(expression, &real, &complex, &|name| {
        is_single_precision(&opened, name)
    })
    .map(|number| number as f32)
}

fn image_operands(expression: &str) -> Result<Vec<(String, Operand)>, ImageError> {
    open_operands(&resolve_operands(expression)?, false)
}

fn resolve_operands(expression: &str) -> Result<Vec<ResolvedOperand>, ImageError> {
    resolve_named_operands(referenced_images(expression)?)
}

fn resolve_named_operands(names: Vec<String>) -> Result<Vec<ResolvedOperand>, ImageError> {
    let base = super::expression_workspace::directory()?;
    names
        .into_iter()
        .map(|name| resolve_named_operand(&base, name))
        .collect()
}

fn resolve_named_operand(base: &Path, name: String) -> Result<ResolvedOperand, ImageError> {
    resolve_named_operand_with(base, name, &|_| None)
}

fn resolve_named_operand_with(
    base: &Path,
    name: String,
    substitute: &impl Fn(&Path) -> Option<PathBuf>,
) -> Result<ResolvedOperand, ImageError> {
    let (entry, mask) = operand_path(base, &name, substitute).ok_or_else(|| ImageError::CasaSelection(format!(
        "Input mask specification is incorrect: ImageExprParse: '{name}' is an unknown lattice or image or it is an unqualified region"
    )))?;
    let path = entry
        .canonicalize()
        .map_err(|error| ImageError::Io(error.to_string()))?;
    Ok(ResolvedOperand {
        name,
        path,
        entry,
        mask,
    })
}

fn open_operands(
    operands: &[ResolvedOperand],
    temporary_source_locks: bool,
) -> Result<Vec<(String, Operand)>, ImageError> {
    open_operands_for_outputs(operands, temporary_source_locks, &[])
}

fn open_operands_for_outputs(
    operands: &[ResolvedOperand],
    temporary_source_locks: bool,
    destinations: &[&Path],
) -> Result<Vec<(String, Operand)>, ImageError> {
    operands
        .iter()
        .map(|operand| {
            let image = if composed::recognizes(&operand.path) {
                let (image, dependencies) =
                    composed::open(&operand.path, &operand.entry, temporary_source_locks)?;
                check_mask_outputs(destinations, &dependencies)?;
                image
            } else {
                open_path_with_mask_name(
                    &operand.path,
                    operand.mask.clone(),
                    temporary_source_locks,
                )?
            };
            Ok((operand.name.clone(), image))
        })
        .collect()
}

fn operand_resolvers(
    opened: &[(String, Operand)],
) -> (HashMapResolver<'_, f64>, HashMapResolver<'_, Complex64>) {
    let mut real = HashMap::new();
    let mut complex = HashMap::new();
    for (name, operand) in opened {
        match operand {
            Operand::Real(source, _) => {
                real.insert(name.clone(), source.as_interface());
            }
            Operand::Complex(source, _) => {
                complex.insert(name.clone(), source.as_interface());
            }
        }
    }
    (HashMapResolver(real), HashMapResolver(complex))
}

/// Evaluates an optional `mask` expression over `window` `(start, shape)` of a
/// parent image of shape `target_shape`.
pub(super) fn evaluate_mask(
    mask_expr: Option<&str>,
    target_shape: &[usize],
    target_coordinates: &CoordinateSystem,
    stretch: bool,
    window: (&[usize], &[usize]),
) -> Result<Option<ArrayD<bool>>, ImageError> {
    evaluate_mask_section(
        mask_expr,
        target_shape,
        target_coordinates,
        stretch,
        window,
        None,
    )
}

pub(super) fn evaluate_strided_mask(
    mask_expr: Option<&str>,
    target_shape: &[usize],
    target_coordinates: &CoordinateSystem,
    stretch: bool,
    section: &StridedSection<'_>,
) -> Result<Option<ArrayD<bool>>, ImageError> {
    evaluate_mask_section(
        mask_expr,
        target_shape,
        target_coordinates,
        stretch,
        (section.start, section.shape),
        Some(section.stride),
    )
}

fn evaluate_mask_section(
    mask_expr: Option<&str>,
    target_shape: &[usize],
    target_coordinates: &CoordinateSystem,
    stretch: bool,
    window: (&[usize], &[usize]),
    stride: Option<&[usize]>,
) -> Result<Option<ArrayD<bool>>, ImageError> {
    let Some(mask) = PreparedMask::new(mask_expr, target_shape, target_coordinates, stretch)?
    else {
        return Ok(None);
    };
    match stride {
        Some(stride) => mask.strided(&StridedSection {
            start: window.0,
            shape: window.1,
            stride,
        }),
        None => mask.window(window.0, window.1),
    }
    .map(Some)
}

fn mask_expression(raw: &str) -> String {
    if super::expression_workspace::operand_path(raw).exists() {
        // Quote bare file names that include spaces or punctuation.
        format!("\"{raw}\"")
    } else if let Some((left, _)) = raw.split_once(':')
        && !raw.contains([' ', '>', '<', '=', '!', '&', '|', '(', ')'])
        && super::expression_workspace::operand_path(left).exists()
    {
        // Unquoted path:maskname expression in top-level `mask=` parameter.
        format!("mask(\"{raw}\")")
    } else if let Some(operator) = raw.find(['<', '>', '=', '!'])
        && let (name, suffix) = raw.split_at(operator)
        && super::expression_workspace::operand_path(name.trim()).exists()
    {
        // These tasks accept an unquoted image path before a comparison,
        // including an absolute path that would otherwise lex as division.
        format!("\"{}\"{suffix}", name.trim())
    } else {
        raw.to_string()
    }
}

thread_local! {
    static MASK_OUTPUTS: RefCell<Vec<PathBuf>> = const { RefCell::new(Vec::new()) };
}

struct MaskOutputScope(usize);

impl Drop for MaskOutputScope {
    fn drop(&mut self) {
        MASK_OUTPUTS.with_borrow_mut(|paths| paths.truncate(self.0));
    }
}

/// Check composed mask operands opened by a synchronous image build against
/// its pending output paths. The scope is local to the current thread.
pub fn with_mask_output_paths<'a, R>(
    outputs: impl IntoIterator<Item = &'a Path>,
    build: impl FnOnce() -> R,
) -> R {
    let outputs: Vec<_> = outputs.into_iter().map(Path::to_path_buf).collect();
    if outputs.is_empty() {
        return build();
    }
    let previous = MASK_OUTPUTS.with_borrow_mut(|paths| {
        let previous = paths.len();
        paths.extend(outputs);
        previous
    });
    let _scope = MaskOutputScope(previous);
    build()
}

fn check_mask_outputs(
    destinations: &[&Path],
    dependencies: &composed::Dependencies,
) -> Result<(), ImageError> {
    MASK_OUTPUTS.with_borrow(|pending| {
        let pending_overwrites = pending.iter().map(PathBuf::as_path).filter(|path| {
            // A fresh child cannot remove an existing graph dependency.
            path.symlink_metadata().is_ok()
        });
        for output in destinations.iter().copied().chain(pending_overwrites) {
            dependencies.check_output(output)?;
        }
        Ok(())
    })
}

pub(super) struct PreparedMask<'a> {
    expression: MaskExpression<f64>,
    target_shape: &'a [usize],
    mapped_axes: Option<Vec<usize>>,
}

impl<'a> PreparedMask<'a> {
    pub(super) fn new(
        mask_expr: Option<&str>,
        target_shape: &'a [usize],
        target_coordinates: &CoordinateSystem,
        stretch: bool,
    ) -> Result<Option<Self>, ImageError> {
        Self::new_for_output(mask_expr, target_shape, target_coordinates, stretch, None)
    }

    pub(super) fn new_for_output(
        mask_expr: Option<&str>,
        target_shape: &'a [usize],
        target_coordinates: &CoordinateSystem,
        stretch: bool,
        destination: Option<&Path>,
    ) -> Result<Option<Self>, ImageError> {
        Self::new_for_outputs(
            mask_expr,
            target_shape,
            target_coordinates,
            stretch,
            destination,
        )
    }

    pub(super) fn new_for_outputs<'b>(
        mask_expr: Option<&str>,
        target_shape: &'a [usize],
        target_coordinates: &CoordinateSystem,
        stretch: bool,
        destinations: impl IntoIterator<Item = &'b Path>,
    ) -> Result<Option<Self>, ImageError> {
        let Some(raw) = mask_expr.map(str::trim).filter(|s| !s.is_empty()) else {
            return Ok(None);
        };
        let expr = mask_expression(raw);
        let names = referenced_images(&expr).map_err(mask_parse_error)?;
        let operands = resolve_named_operands(names)?;
        let destinations: Vec<_> = destinations.into_iter().collect();
        for destination in &destinations {
            for operand in &operands {
                super::ensure_outfile_does_not_alias_input(destination, &operand.path)?;
                super::ensure_artifacts_do_not_overlap(destination, &operand.path)?;
            }
        }
        let opened = open_operands_for_outputs(&operands, false, &destinations)?;
        let (real, complex) = operand_resolvers(&opened);
        let has_composed = operands
            .iter()
            .any(|operand| composed::recognizes(&operand.path));
        let precision = |name: &str| is_single_precision(&opened, name);
        let builder = subimage_mask_builder(
            &expr,
            &real,
            &complex,
            has_composed.then_some(&precision as &dyn Fn(&str) -> bool),
        )
        .map_err(mask_parse_error)?;
        let mask_shape = builder.shape();
        let extends = mask_shape != target_shape && !mask_shape.is_empty();
        if extends && !stretch {
            return Err(ImageError::CasaSelection(format!(
                "The input image shape ({target_shape:?}) and mask shape ({mask_shape:?}) are different, and it was specified that the mask should not be extended, so the mask cannot be applied to the (sub)image"
            )));
        }
        let mapped_axes = if has_composed && !mask_shape.is_empty() {
            Some(geometry::image_tool_mask_axes(
                builder.expression_coordinates(),
                mask_shape,
                target_coordinates,
                target_shape,
            )?)
        } else if extends {
            opened
                .iter()
                .map(|(_, image)| image.geometry())
                .find(|(shape, _)| *shape == mask_shape)
                .map(|(_, coordinates)| {
                    stretch_axes(coordinates, target_coordinates, mask_shape, target_shape)
                })
                .transpose()?
        } else {
            None
        }
        .filter(|axes| axes.iter().copied().ne(0..axes.len()));
        let expression = builder.compile().map_err(|error| {
            mask_parse_error(crate::expr_parser::ExprParseError {
                message: error.to_string(),
                position: 0,
            })
        })?;
        Ok(Some(Self {
            expression,
            target_shape,
            mapped_axes,
        }))
    }

    fn window(&self, origin: &[usize], extent: &[usize]) -> Result<ArrayD<bool>, ImageError> {
        let Some(axes) = &self.mapped_axes else {
            return self
                .expression
                .eval_window_for_shape(origin, extent, self.target_shape);
        };
        let mut start = Vec::with_capacity(axes.len());
        let mut shape = Vec::with_capacity(axes.len());
        for (i, &axis) in axes.iter().enumerate() {
            let single = self.expression.shape()[i] == 1;
            start.push(if single { 0 } else { origin[axis] });
            shape.push(if single { 1 } else { extent[axis] });
        }
        let mut data = self.expression.get_slice(&start, &shape)?;
        for axis in 0..self.target_shape.len() {
            if !axes.contains(&axis) {
                data = data.insert_axis(ndarray::Axis(axis));
            }
        }
        broadcast_array(data, extent)
    }

    pub(super) fn strided(&self, section: &StridedSection<'_>) -> Result<ArrayD<bool>, ImageError> {
        let dense_cursor = bounded_read_cursor(section.dense_shape(section.shape))?;
        let mut mask = ArrayD::from_elem(IxDyn(section.shape), true);
        for cursor in section.cursors(dense_cursor) {
            let cursor = cursor?;
            let extent = section.dense_shape(&cursor.shape);
            let dense = self.window(&section.source_start(&cursor.position), &extent)?;
            let selected = dense.slice_each_axis(|axis| {
                let index = axis.axis.index();
                Slice::new(0, None, section.stride[index].min(extent[index]) as isize)
            });
            mask.slice_each_axis_mut(|axis| {
                let index = axis.axis.index();
                let start = cursor.position[index];
                Slice::from(start..start + cursor.shape[index])
            })
            .assign(&selected);
        }
        Ok(mask)
    }
}

fn stretch_axes(
    source: &CoordinateSystem,
    target: &CoordinateSystem,
    source_shape: &[usize],
    target_shape: &[usize],
) -> Result<Vec<usize>, ImageError> {
    let axes = |coords: &CoordinateSystem| {
        (0..coords.n_pixel_axes())
            .map(|axis| {
                let (index, local) = coords.pixel_axis_owner(axis).expect("dense pixel axes");
                let coord = coords.coordinate(index);
                (coord.coordinate_type(), coord.axis_names().remove(local))
            })
            .collect::<Vec<_>>()
    };
    let source_axes = axes(source);
    let target_axes = axes(target);
    let incompatible = || {
        ImageError::CasaSelection(
            "Unable to extend mask: coordinate axes or axis lengths do not conform".into(),
        )
    };
    let mapped = source_axes
        .iter()
        .map(|axis| {
            target_axes
                .iter()
                .position(|candidate| candidate == axis)
                .ok_or_else(incompatible)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if mapped.len() != source_shape.len()
        || target_axes.len() != target_shape.len()
        || !mapped.is_sorted_by(|a, b| a < b)
        || mapped
            .iter()
            .zip(source_shape)
            .any(|(&i, &len)| len != 1 && len != target_shape[i])
    {
        return Err(incompatible());
    }
    Ok(mapped)
}

fn mask_parse_error(error: crate::expr_parser::ExprParseError) -> ImageError {
    ImageError::CasaSelection(format!(
        "Input mask specification is incorrect: {}",
        mask_error_wording(error)
    ))
}

fn operand_path(
    base: &Path,
    spec: &str,
    substitute: &impl Fn(&Path) -> Option<PathBuf>,
) -> Option<(PathBuf, Option<String>)> {
    let existing = |path: PathBuf| substitute(&path).or_else(|| path.exists().then_some(path));
    if let Some(direct_path) = existing(base.join(spec)) {
        return Some((direct_path, None));
    }
    if let Some((left, right)) = spec.rsplit_once(':')
        && !left.is_empty()
        && !right.trim().is_empty()
        && let Some(source) = existing(base.join(left))
    {
        return Some((source, Some(right.trim().to_owned())));
    }
    None
}

/// Open a lazy FITS expression operand with its optional validity mask.
///
/// The image and mask share one section reader for the lifetime of the
/// expression.
///
/// # Errors
/// Returns an error if the file has no supported FITS image.
pub fn open_fits_expression<T: ImageExprValue + From<f32>>(
    path: &Path,
    use_mask: bool,
) -> Result<ImageExpression<T>, ImageError> {
    open_fits_expression_with_history(path, use_mask).map(|(image, _)| image)
}

pub(super) fn open_fits_expression_with_history<T: ImageExprValue + From<f32>>(
    path: &Path,
    use_mask: bool,
) -> Result<(ImageExpression<T>, Vec<crate::image::ImageHistoryEntry>), ImageError> {
    let fits = FitsToolImport::open(path, 0, -1)?;
    if fits.hdu_index() > 1 || fits.is_tile_compressed() {
        return Err(ImageError::CasaSelection(format!(
            "No data in the zeroth or first extension of {}",
            path.display(),
        )));
    }
    let reader = Arc::new(SharedFitsSectionReader::new(
        path.to_path_buf(),
        fits.hdu_index(),
        fits.shape().to_vec(),
    )?);
    let mask_reader = Arc::clone(&reader);
    let mask_fn = (fits.has_implicit_mask() && use_mask).then(|| {
        Arc::new(move |start: &[usize], shape: &[usize], stride: &[usize]| {
            let pixels = mask_reader.read(start, shape, stride)?;
            Ok(pixels
                .iter()
                .any(|value| value.is_nan())
                .then(|| pixels.mapv(|value| !value.is_nan())))
        }) as crate::image_expr::OwnedMaskReader
    });
    let expression = ImageExprBuilder::from_section_readers(
        ImageExprMeta::from_fits(&fits, path)?,
        Arc::new(move |start, shape, stride| {
            reader
                .read(start, shape, stride)
                .map(|pixels| pixels.mapv(<T as From<f32>>::from))
        }),
        mask_fn,
    );
    let history = fits.history.into_iter().map(|row| row.entry).collect();
    Ok((expression.compile()?, history))
}

fn open_path_with_mask_name(
    path: &Path,
    mask_name: Option<String>,
    temporary_source_locks: bool,
) -> Result<Operand, ImageError> {
    let ignore_default = mask_name
        .as_deref()
        .is_some_and(|name| name.eq_ignore_ascii_case("nomask"));
    if path.is_file() {
        if mask_name.is_some() && !ignore_default {
            return Err(ImageError::CasaSelection(format!(
                "FITSImage {} has no named masks",
                path.display(),
            )));
        }
        let expression = open_fits_expression(path, !ignore_default)?;
        return Ok(Operand::Real(
            MaskSource::Sections(Box::new(expression)),
            true,
        ));
    }
    if MiriadImage::recognizes_input(path, path) {
        if mask_name.is_some() && !ignore_default {
            return Err(ImageError::CasaSelection(format!(
                "MIRIADImage {} has no named masks",
                path.display(),
            )));
        }
        let source = Arc::new(MiriadImage::open(path)?);
        let meta = ImageExprMeta::from_image(source.as_ref())?;
        let expression = ImageExprBuilder::from_section_readers(
            meta,
            Arc::new(move |start, count, stride| {
                Ok(source.get_slice(start, count, stride)?.mapv(f64::from))
            }),
            None,
        )
        .compile()?;
        return Ok(Operand::Real(
            MaskSource::Sections(Box::new(expression)),
            true,
        ));
    }
    if ComponentListImage::recognizes_native(path) {
        if mask_name.is_some() {
            return Err(ImageError::CasaSelection(format!(
                "ImageExprParse: {} is not a PagedImage",
                path.display(),
            )));
        }
        return open_component_mask_source(path).map(|source| Operand::Real(source, true));
    }

    match open_mask_image(path)? {
        AnyPagedImage::Float64(_) | AnyPagedImage::Complex64(_) if mask_name.is_some() => {
            Err(ImageError::CasaSelection(format!(
                "ImageExprParse: {} is a PagedImage with an unsupported data type",
                path.display(),
            )))
        }
        AnyPagedImage::Float64(img) => Ok(Operand::Real(MaskSource::Paged(Box::new(img)), false)),
        AnyPagedImage::Float32(img) => {
            convert_paged_mask_source(img, mask_name, temporary_source_locks, f64::from)
                .map(|image| Operand::Real(image, true))
        }
        AnyPagedImage::Complex64(img) => {
            Ok(Operand::Complex(MaskSource::Paged(Box::new(img)), false))
        }
        AnyPagedImage::Complex32(img) => {
            convert_paged_mask_source(img, mask_name, temporary_source_locks, |c: Complex32| {
                Complex64::new(f64::from(c.re), f64::from(c.im))
            })
            .map(|image| Operand::Complex(image, true))
        }
    }
}

fn open_component_mask_source(path: &Path) -> Result<MaskSource<f64>, ImageError> {
    let in_use = TableUseLock::acquire_readonly(path)?;
    let (source, _) = ComponentListImage::open_native(path, true)?;
    let meta = ImageExprMeta::from_image(&source)?;
    let has_mask = source.has_pixel_mask();
    let source = Arc::new((source, in_use));
    let mask = has_mask.then(|| {
        let source = Arc::clone(&source);
        Arc::new(move |start: &[usize], count: &[usize], stride: &[usize]| {
            let path = source.0.name().expect("native component image");
            let changed = || {
                ImageError::Io(format!(
                    "Image {} changed while reading its mask",
                    path.display()
                ))
            };
            if !source.1.matches_directory(path) {
                return Err(changed());
            }
            let mask = source.0.get_mask_slice(start, count, stride)?;
            if !source.1.matches_directory(path) {
                return Err(changed());
            }
            Ok(mask)
        }) as crate::image_expr::OwnedMaskReader
    });
    let expression = ImageExprBuilder::from_section_readers(
        meta,
        Arc::new(move |start, count, stride| {
            Ok(source.0.get_slice(start, count, stride)?.mapv(f64::from))
        }),
        mask,
    )
    .compile()?;
    Ok(MaskSource::Sections(Box::new(expression)))
}

fn open_mask_image(path: &Path) -> Result<AnyPagedImage, ImageError> {
    #[cfg(unix)]
    return AnyPagedImage::open_with_lock(path, 0);
    #[cfg(not(unix))]
    AnyPagedImage::open(path)
}

fn convert_paged_mask_source<S: ImageExprValue, T: ImageExprValue>(
    img: PagedImage<S>,
    mask_name: Option<String>,
    temporary_source_locks: bool,
    convert: impl Fn(S) -> T + Copy + Send + Sync + 'static,
) -> Result<MaskSource<T>, ImageError> {
    let ignore_default = mask_name
        .as_deref()
        .is_some_and(|name| name.eq_ignore_ascii_case("nomask"));
    if !ignore_default {
        validate_mask_name(&img, mask_name.as_deref())?;
    }
    let meta = ImageExprMeta::from_image(&img)?;
    let has_mask = !ignore_default && (mask_name.is_some() || img.has_pixel_mask());
    let image = Arc::new(Mutex::new(img));
    #[cfg(unix)]
    if temporary_source_locks {
        image.lock().map_err(mask_source_lock_error)?.unlock()?;
    }
    let mask = has_mask.then(|| {
        let image = Arc::clone(&image);
        Arc::new(move |start: &[usize], shape: &[usize], stride: &[usize]| {
            read_paged_mask_source(&image, temporary_source_locks, |image| match &mask_name {
                Some(name) => image
                    .get_named_mask_slice(name, start, shape, stride)
                    .map(Some),
                None => image.get_mask_slice(start, shape, stride),
            })
        }) as crate::image_expr::OwnedMaskReader
    });
    let expression = ImageExprBuilder::from_section_readers(
        meta,
        Arc::new(move |start, shape, stride| {
            read_paged_mask_source(&image, temporary_source_locks, |image| {
                image.get_slice_with_stride(start, shape, stride)
            })
            .map(|pixels| pixels.mapv(convert))
        }),
        mask,
    )
    .compile()?;
    Ok(MaskSource::Sections(Box::new(expression)))
}

fn read_paged_mask_source<S: ImageExprValue, R>(
    source: &Mutex<PagedImage<S>>,
    temporary_lock: bool,
    read: impl FnOnce(&PagedImage<S>) -> Result<R, ImageError>,
) -> Result<R, ImageError> {
    let mut image = source.lock().map_err(mask_source_lock_error)?;
    #[cfg(unix)]
    if temporary_lock {
        if !image.lock(casa_tables::LockType::Read, 0)? {
            return Err(ImageError::CasaSelection(
                "Could not acquire the mask image read lock".into(),
            ));
        }
        let result = read(&image);
        let release = image.unlock();
        return result.and_then(|value| release.map(|()| value));
    }
    #[cfg(not(unix))]
    let _ = (&mut image, temporary_lock);
    read(&image)
}

fn mask_source_lock_error<T>(_: std::sync::PoisonError<T>) -> ImageError {
    ImageError::CasaSelection("Could not read the mask image".into())
}

fn validate_mask_name<T: ImagePixel>(
    img: &PagedImage<T>,
    mask_name: Option<&str>,
) -> Result<(), ImageError> {
    if let Some(name) = mask_name
        && !img.mask_names().iter().any(|existing| existing == name)
    {
        return Err(ImageError::CasaSelection(format!(
            "Mask '{name}' not found in image"
        )));
    }
    Ok(())
}

#[cfg(all(test, unix))]
#[path = "../../tests/support/mask_operand_lock.rs"]
mod tests;
