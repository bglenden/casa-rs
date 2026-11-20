// SPDX-License-Identifier: LGPL-3.0-or-later
//! LEL mask evaluation, `image:maskname` resolution, and `stretch` handling
//! for image tasks.

mod geometry;
pub(super) use geometry::validate_mask;

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use casa_coordinates::CoordinateSystem;
use casa_types::{ArrayD, Complex32, Complex64};

use crate::analysis::mask_error_wording;
use crate::expr_parser::{
    HashMapResolver, ImageToolPixelExpression, ReplacementPixels, compile_image_tool_pixels,
    evaluate_replacement, parse_image_tool_mask, parse_subimage_mask_expr, referenced_images,
};
use crate::fits_read::{FitsSectionReader, FitsToolImport};
use crate::image_expr::{ImageExprMeta, ImageExprValue};
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
/// Returns an error for invalid expressions, undefined operands, or Boolean results.
pub fn compile_image_tool_expression(
    expression: &str,
) -> Result<ImageToolPixelExpression, ImageError> {
    if expression.is_empty() {
        return Err(ImageError::CasaSelection(
            "Exception: You must specify an expression.".into(),
        ));
    }
    let opened = image_operands(expression)?;
    let (real, complex) = operand_resolvers(&opened);
    compile_image_tool_pixels(expression, &real, &complex, &|name| {
        is_single_precision(&opened, name)
    })
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

fn image_operands(expression: &str) -> Result<Vec<(String, Operand)>, ImageError> {
    referenced_images(expression)?
        .into_iter()
        .map(|name| {
            let image = open_operand(&name)?;
            Ok::<_, ImageError>((name, image))
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
    let Some(raw) = mask_expr.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };

    let expr = mask_expression(raw);
    let opened = referenced_images(&expr)
        .map_err(mask_parse_error)?
        .into_iter()
        .map(|name| {
            let image = open_operand(&name)?;
            Ok((name, image))
        })
        .collect::<Result<Vec<_>, ImageError>>()?;
    evaluate(
        &expr,
        target_shape,
        target_coordinates,
        stretch,
        window,
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
    opened: &[(String, Operand)],
) -> Result<Option<ArrayD<bool>>, ImageError> {
    let (real, complex) = operand_resolvers(opened);
    let parsed = parse_subimage_mask_expr(expr, &real, &complex).map_err(mask_parse_error)?;
    let mask_shape = parsed.shape();
    if mask_shape != target_shape && !mask_shape.is_empty() {
        if !stretch {
            return Err(ImageError::CasaSelection(format!(
                "The input image shape ({target_shape:?}) and mask shape ({mask_shape:?}) \
                 are different, and it was specified that the mask should not be extended, \
                 so the mask cannot be applied to the (sub)image"
            )));
        }
        let source = opened
            .iter()
            .map(|(_, image)| image.geometry())
            .find(|(shape, _)| *shape == mask_shape);
        if let Some((_, coordinates)) = source {
            let axes = stretch_axes(coordinates, target_coordinates, mask_shape, target_shape)?;
            if axes.iter().copied().ne(0..axes.len()) {
                let mut start = Vec::with_capacity(axes.len());
                let mut shape = Vec::with_capacity(axes.len());
                for (i, &axis) in axes.iter().enumerate() {
                    let single = mask_shape[i] == 1;
                    start.push(if single { 0 } else { window.0[axis] });
                    shape.push(if single { 1 } else { window.1[axis] });
                }
                let mut data = parsed.get_slice(&start, &shape)?;
                for axis in 0..target_shape.len() {
                    if !axes.contains(&axis) {
                        data = data.insert_axis(ndarray::Axis(axis));
                    }
                }
                return broadcast_array(data, window.1).map(Some);
            }
        }
    }
    parsed
        .eval_window_for_shape(window.0, window.1, target_shape)
        .map(Some)
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

fn open_operand(spec: &str) -> Result<Operand, ImageError> {
    let (path, mask) = operand_path(spec)?;
    open_path_with_mask_name(path, mask.map(str::to_owned))
}

fn operand_path(spec: &str) -> Result<(&Path, Option<&str>), ImageError> {
    let direct_path = Path::new(spec);
    if direct_path.exists() {
        return Ok((direct_path, None));
    }
    if let Some((left, right)) = spec.rsplit_once(':')
        && !left.is_empty()
        && !right.trim().is_empty()
        && Path::new(left).exists()
    {
        return Ok((Path::new(left), Some(right.trim())));
    }
    Err(ImageError::CasaSelection(format!(
        "Input mask specification is incorrect: ImageExprParse: '{spec}' is an unknown \
         lattice or image or it is an unqualified region"
    )))
}

fn open_path_with_mask_name(path: &Path, mask_name: Option<String>) -> Result<Operand, ImageError> {
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
        let fits = FitsToolImport::open(path, 0, -1)?;
        if fits.is_tile_compressed() {
            return Err(ImageError::CasaSelection(format!(
                "No data in the zeroth or first extension of {}",
                path.display(),
            )));
        }
        let path_owned = path.to_path_buf();
        let dimensions = fits.shape().to_vec();
        let hdu = fits.hdu_index();
        let read = move |start: &[usize], shape: &[usize], stride: &[usize]| {
            FitsSectionReader::open(&path_owned, hdu, &dimensions)?.read(start, shape, stride)
        };
        let mask_reader = read.clone();
        let mask_fn = (fits.has_implicit_mask() && !ignore_default).then(|| {
            Arc::new(move |start: &[usize], shape: &[usize], stride: &[usize]| {
                let pixels = mask_reader(start, shape, stride)?;
                Ok(pixels
                    .iter()
                    .any(|value| value.is_nan())
                    .then(|| pixels.mapv(|value| !value.is_nan())))
            }) as crate::image_expr::OwnedMaskReader
        });
        let expression = ImageExprBuilder::from_section_readers(
            ImageExprMeta::from_fits(&fits, path)?,
            Arc::new(move |start, shape, stride| {
                read(start, shape, stride).map(|pixels| pixels.mapv(f64::from))
            }),
            mask_fn,
        );
        return Ok(Operand::Real(
            MaskSource::Sections(Box::new(expression.compile()?)),
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
        AnyPagedImage::Float32(img) => convert_paged_mask_source(img, mask_name, f64::from)
            .map(|image| Operand::Real(image, true)),
        AnyPagedImage::Complex64(img) => {
            Ok(Operand::Complex(MaskSource::Paged(Box::new(img)), false))
        }
        AnyPagedImage::Complex32(img) => {
            convert_paged_mask_source(img, mask_name, |c: Complex32| {
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
    // Pixel reads, mask reads and repeated passes observe one native generation.
    let image = Arc::new(Mutex::new(img));
    let mask = has_mask.then(|| {
        let image = Arc::clone(&image);
        Arc::new(move |start: &[usize], shape: &[usize], stride: &[usize]| {
            let image = image.lock().map_err(mask_source_lock_error)?;
            match &mask_name {
                Some(name) => image
                    .get_named_mask_slice(name, start, shape, stride)
                    .map(Some),
                None => image.get_mask_slice(start, shape, stride),
            }
        }) as crate::image_expr::OwnedMaskReader
    });
    let expression = ImageExprBuilder::from_section_readers(
        meta,
        Arc::new(move |start, shape, stride| {
            image
                .lock()
                .map_err(mask_source_lock_error)?
                .get_slice_with_stride(start, shape, stride)
                .map(|pixels| pixels.mapv(convert))
        }),
        mask,
    )
    .compile()?;
    Ok(MaskSource::Sections(Box::new(expression)))
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
mod tests {
    use super::*;
    use casa_tables::{LockMode, LockOptions, LockType, Table, TableOptions};

    #[test]
    fn converted_operand_keeps_writer_out_between_sections() {
        const CHILD_CASE: &str =
            "analysis::lel_mask::tests::converted_operand_keeps_writer_out_between_sections";
        const PATH_KEY: &str = "CASA_RS_MASK_OPERAND_LOCK_TEST_PATH";
        const WRITE_KEY: &str = "CASA_RS_MASK_OPERAND_LOCK_TEST_WRITE";
        if let Some(path) = std::env::var_os(PATH_KEY) {
            let mut table = Table::open_with_lock(
                TableOptions::new(path),
                LockOptions::new(LockMode::UserLocking),
            )
            .unwrap();
            assert_eq!(
                table.lock(LockType::Write, 1).unwrap(),
                std::env::var_os(WRITE_KEY).is_some()
            );
            return;
        }

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("mask.image");
        let mut image =
            PagedImage::<f32>::create(vec![4, 4], CoordinateSystem::new(), &path).unwrap();
        let mask = ArrayD::from_elem(ndarray::IxDyn(&[4, 4]), true);
        image.put_mask("selected", &mask).unwrap();
        image.set_default_mask("selected").unwrap();
        image.save().unwrap();
        drop(image);

        let check_peer = |can_write: bool| {
            let mut child = std::process::Command::new(std::env::current_exe().unwrap());
            child.args(["--exact", CHILD_CASE]).env(PATH_KEY, &path);
            if can_write {
                child.env(WRITE_KEY, "yes");
            } else {
                child.env_remove(WRITE_KEY);
            }
            let output = child.output().unwrap();
            let report = String::from_utf8_lossy(&output.stdout);
            assert!(
                output.status.success() && report.contains("1 passed"),
                "{report}"
            );
        };
        for name in [None, Some("nomask".to_owned())] {
            let expect_mask = name.is_none();
            let AnyPagedImage::Float32(image) = open_mask_image(&path).unwrap() else {
                panic!("float image");
            };
            let source = convert_paged_mask_source(image, name, f64::from).unwrap();
            {
                let reader = source.as_interface();
                reader.get_slice(&[0, 0], &[2, 2], &[1, 1]).unwrap();
                check_peer(false);
                assert_eq!(
                    reader
                        .get_mask_slice(&[2, 2], &[2, 2], &[1, 1])
                        .unwrap()
                        .is_some(),
                    expect_mask
                );
                reader.get_slice(&[2, 2], &[2, 2], &[1, 1]).unwrap();
            }
            drop(source);
            check_peer(true);
        }
    }
}
