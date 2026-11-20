// SPDX-License-Identifier: LGPL-3.0-or-later
//! LEL mask evaluation, `image:maskname` resolution, and `stretch` handling
//! for image tasks.

mod geometry;
pub(super) use geometry::validate_mask;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use casa_coordinates::CoordinateSystem;
use casa_types::{ArrayD, Complex32, Complex64};
use ndarray::{IxDyn, Slice};

use crate::analysis::axes::StridedSection;
use crate::analysis::mask_error_wording;
use crate::expr_parser::{
    HashMapResolver, ImageToolPixelExpression, ReplacementPixels, compile_image_tool_pixels,
    evaluate_real_scalar, evaluate_replacement, parse_image_tool_mask, parse_subimage_mask_expr,
    referenced_images,
};
use crate::fits_read::{FitsToolImport, SharedFitsSectionReader};
use crate::image_expr::{ImageExprMeta, ImageExprValue, bounded_read_cursor};
use crate::image_expr_lel::broadcast_array;
use crate::{
    AnyPagedImage, ImageError, ImageExprBuilder, ImageExpression, ImageInterface, ImagePixel,
    PagedImage,
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
    mask: Option<String>,
}

/// Resolved image operand paths for a public calculator expression.
pub struct ImageToolExpressionInputs<'a> {
    expression: &'a str,
    operands: Vec<ResolvedOperand>,
}

impl<'a> ImageToolExpressionInputs<'a> {
    /// Resolve the operand paths relative to the current working directory.
    /// # Errors
    /// Returns an error for a missing operand or invalid expression.
    pub fn resolve(expression: &'a str) -> Result<Self, ImageError> {
        if expression.is_empty() {
            return Err(ImageError::CasaSelection(
                "Exception: You must specify an expression.".into(),
            ));
        }
        Ok(Self {
            expression,
            operands: resolve_operands(expression)?,
        })
    }

    /// Paths of the images supplying expression values or masks.
    pub fn paths(&self) -> impl ExactSizeIterator<Item = &Path> {
        self.operands.iter().map(|operand| operand.path.as_path())
    }

    /// Compile the expression against these resolved operands.
    /// # Errors
    /// Returns an error if an operand is unreadable or the expression is invalid.
    pub fn compile(&self) -> Result<ImageToolPixelExpression, ImageError> {
        self.compile_with_source_locks(false)
    }

    /// Compile an expression whose owning tool can release its source locks.
    ///
    /// # Errors
    /// Returns an error if an operand is unreadable or the expression is invalid.
    pub fn compile_lazy(&self) -> Result<ImageToolPixelExpression, ImageError> {
        self.compile_with_source_locks(true)
    }

    fn compile_with_source_locks(
        &self,
        temporary_source_locks: bool,
    ) -> Result<ImageToolPixelExpression, ImageError> {
        let opened = open_operands(&self.operands, temporary_source_locks)?;
        let (real, complex) = operand_resolvers(&opened);
        compile_image_tool_pixels(self.expression, &real, &complex, &|name| {
            is_single_precision(&opened, name)
        })
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
    let (real, complex) = operand_resolvers(&opened);
    let parsed = parse_image_tool_mask(expression, &real, &complex, &|name| {
        is_single_precision(&opened, name)
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
    let base = std::env::current_dir().map_err(|error| ImageError::Io(error.to_string()))?;
    names
        .into_iter()
        .map(|name| resolve_named_operand(&base, name))
        .collect()
}

fn resolve_named_operand(base: &Path, name: String) -> Result<ResolvedOperand, ImageError> {
    let (path, mask) = operand_path(base, &name).ok_or_else(|| ImageError::CasaSelection(format!(
        "Input mask specification is incorrect: ImageExprParse: '{name}' is an unknown lattice or image or it is an unqualified region"
    )))?;
    let path = path
        .canonicalize()
        .map_err(|error| ImageError::Io(error.to_string()))?;
    Ok(ResolvedOperand { name, path, mask })
}

fn open_operands(
    operands: &[ResolvedOperand],
    temporary_source_locks: bool,
) -> Result<Vec<(String, Operand)>, ImageError> {
    operands
        .iter()
        .map(|operand| {
            let image = open_path_with_mask_name(
                &operand.path,
                operand.mask.clone(),
                temporary_source_locks,
            )?;
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
    let Some(raw) = mask_expr.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };

    let expr = mask_expression(raw);
    let names = referenced_images(&expr).map_err(mask_parse_error)?;
    let opened = open_operands(&resolve_named_operands(names)?, false)?;
    evaluate(
        &expr,
        target_shape,
        target_coordinates,
        stretch,
        window,
        stride,
        &opened,
    )
}

fn mask_expression(raw: &str) -> String {
    if Path::new(raw).exists() {
        // Quote bare file names that include spaces or punctuation.
        format!("\"{raw}\"")
    } else if let Some((left, _)) = raw.split_once(':')
        && !raw.contains([' ', '>', '<', '=', '!', '&', '|', '(', ')'])
        && Path::new(left).exists()
    {
        // Unquoted path:maskname expression in top-level `mask=` parameter.
        format!("mask(\"{raw}\")")
    } else if let Some(operator) = raw.find(['<', '>', '=', '!'])
        && let (name, suffix) = raw.split_at(operator)
        && Path::new(name.trim()).exists()
    {
        // These tasks accept an unquoted image path before a comparison,
        // including an absolute path that would otherwise lex as division.
        format!("\"{}\"{suffix}", name.trim())
    } else {
        raw.to_string()
    }
}

fn evaluate(
    expr: &str,
    target_shape: &[usize],
    target_coordinates: &CoordinateSystem,
    stretch: bool,
    window: (&[usize], &[usize]),
    stride: Option<&[usize]>,
    opened: &[(String, Operand)],
) -> Result<Option<ArrayD<bool>>, ImageError> {
    let (real, complex) = operand_resolvers(opened);
    let parsed = parse_subimage_mask_expr(expr, &real, &complex).map_err(mask_parse_error)?;
    let mask_shape = parsed.shape();
    let mapped_axes = if mask_shape != target_shape && !mask_shape.is_empty() {
        if !stretch {
            return Err(ImageError::CasaSelection(format!(
                "The input image shape ({target_shape:?}) and mask shape ({mask_shape:?}) \
                 are different, and it was specified that the mask should not be extended, \
                 so the mask cannot be applied to the (sub)image"
            )));
        }
        opened
            .iter()
            .map(|(_, image)| image.geometry())
            .find(|(shape, _)| *shape == mask_shape)
            .map(|(_, coordinates)| {
                stretch_axes(coordinates, target_coordinates, mask_shape, target_shape)
            })
            .transpose()?
            .filter(|axes| axes.iter().copied().ne(0..axes.len()))
    } else {
        None
    };

    let evaluate_window =
        |origin: &[usize], extent: &[usize]| -> Result<ArrayD<bool>, ImageError> {
            let Some(axes) = &mapped_axes else {
                return parsed.eval_window_for_shape(origin, extent, target_shape);
            };
            let mut start = Vec::with_capacity(axes.len());
            let mut shape = Vec::with_capacity(axes.len());
            for (i, &axis) in axes.iter().enumerate() {
                let single = mask_shape[i] == 1;
                start.push(if single { 0 } else { origin[axis] });
                shape.push(if single { 1 } else { extent[axis] });
            }
            let mut data = parsed.get_slice(&start, &shape)?;
            for axis in 0..target_shape.len() {
                if !axes.contains(&axis) {
                    data = data.insert_axis(ndarray::Axis(axis));
                }
            }
            broadcast_array(data, extent)
        };

    let Some(stride) = stride else {
        return evaluate_window(window.0, window.1).map(Some);
    };
    let section = StridedSection {
        start: window.0,
        shape: window.1,
        stride,
    };
    let dense_cursor = bounded_read_cursor(section.dense_shape(section.shape))?;
    let mut mask = ArrayD::from_elem(IxDyn(section.shape), true);
    for cursor in section.cursors(dense_cursor) {
        let cursor = cursor?;
        let extent = section.dense_shape(&cursor.shape);
        let dense = evaluate_window(&section.source_start(&cursor.position), &extent)?;
        let selected = dense.slice_each_axis(|axis| {
            let index = axis.axis.index();
            Slice::new(0, None, stride[index].min(extent[index]) as isize)
        });
        mask.slice_each_axis_mut(|axis| {
            let index = axis.axis.index();
            let start = cursor.position[index];
            Slice::from(start..start + cursor.shape[index])
        })
        .assign(&selected);
    }
    Ok(Some(mask))
}

fn stretch_axes(
    source: &CoordinateSystem,
    target: &CoordinateSystem,
    source_shape: &[usize],
    target_shape: &[usize],
) -> Result<Vec<usize>, ImageError> {
    let axes = |coords: &CoordinateSystem| {
        (0..coords.n_coordinates())
            .flat_map(|i| {
                let coord = coords.coordinate(i);
                let kind = coord.coordinate_type();
                coord.axis_names().into_iter().map(move |name| (kind, name))
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

fn operand_path(base: &Path, spec: &str) -> Option<(PathBuf, Option<String>)> {
    let direct_path = base.join(spec);
    if direct_path.exists() {
        return Some((direct_path, None));
    }
    if let Some((left, right)) = spec.rsplit_once(':')
        && !left.is_empty()
        && !right.trim().is_empty()
    {
        let source = base.join(left);
        if source.exists() {
            return Some((source, Some(right.trim().to_owned())));
        }
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
    let fits = FitsToolImport::open(path, 0, -1)?;
    if fits.is_tile_compressed() {
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
    expression.compile()
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
