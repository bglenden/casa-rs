// SPDX-License-Identifier: LGPL-3.0-or-later
//! LEL mask evaluation, `image:maskname` resolution, and `stretch` handling
//! for image tasks.

use std::collections::HashMap;
use std::path::Path;

use casa_coordinates::CoordinateSystem;
use casa_types::{ArrayD, Complex32, Complex64};

use crate::analysis::mask_error_wording;
use crate::expr_parser::{
    HashMapResolver, ReplacementPixels, evaluate_replacement, parse_subimage_mask_expr,
    referenced_images,
};
use crate::image_expr_lel::broadcast_array;
use crate::{
    AnyPagedImage, ImageError, ImageInfo, ImageInterface, ImagePixel, PagedImage, TempImage,
    TempStoragePolicy, read_fits_image,
};

enum MaskSource<T: ImagePixel> {
    Paged(Box<PagedImage<T>>),
    Memory(Box<TempImage<T>>),
}

impl<T: ImagePixel> MaskSource<T> {
    fn as_interface(&self) -> &dyn ImageInterface<T> {
        match self {
            Self::Paged(img) => img.as_ref(),
            Self::Memory(img) => img.as_ref(),
        }
    }
}

enum Operand {
    Real(MaskSource<f64>, bool),
    Complex(MaskSource<Complex64>),
}
impl Operand {
    fn geometry(&self) -> (&[usize], &CoordinateSystem) {
        match self {
            Self::Real(source, _) => (
                source.as_interface().shape(),
                source.as_interface().coordinates(),
            ),
            Self::Complex(source) => (
                source.as_interface().shape(),
                source.as_interface().coordinates(),
            ),
        }
    }
}

fn is_single_precision(opened: &[(String, Operand)], name: &str) -> bool {
    opened
        .iter()
        .any(|(source, value)| source == name && matches!(value, Operand::Real(_, true)))
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
            Operand::Complex(source) => {
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

    let mut expr = raw.to_string();
    if Path::new(raw).exists() {
        // Quote bare file names that include spaces or punctuation.
        expr = format!("\"{raw}\"");
    } else if let Some((left, _)) = raw.split_once(':')
        && !raw.contains([' ', '>', '<', '=', '!', '&', '|', '(', ')'])
        && Path::new(left).exists()
    {
        // Unquoted path:maskname expression in top-level `mask=` parameter.
        expr = format!("mask(\"{raw}\")");
    } else if let Some(operator) = raw.find(['<', '>', '=', '!'])
        && let (name, suffix) = raw.split_at(operator)
        && Path::new(name.trim()).exists()
    {
        // These tasks accept an unquoted image path before a comparison,
        // including an absolute path that would otherwise lex as division.
        expr = format!("\"{}\"{suffix}", name.trim());
    }
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
    let direct_path = Path::new(spec);
    if direct_path.exists() {
        return open_path_with_mask_name(direct_path, None, spec);
    }
    if let Some((left, right)) = spec.rsplit_once(':')
        && !left.is_empty()
        && !right.trim().is_empty()
        && Path::new(left).exists()
    {
        return open_path_with_mask_name(Path::new(left), Some(right.trim().to_string()), spec);
    }
    Err(ImageError::CasaSelection(format!(
        "Input mask specification is incorrect: ImageExprParse: '{spec}' is an unknown \
         lattice or image or it is an unqualified region"
    )))
}

fn open_path_with_mask_name(
    path: &Path,
    mask_name: Option<String>,
    spec: &str,
) -> Result<Operand, ImageError> {
    if path.is_file() {
        if mask_name.is_some() {
            return Err(ImageError::CasaSelection(format!(
                "FITS image '{spec}' does not hold named masks"
            )));
        }
        let fits = read_fits_image(path)?;
        let pixels = fits.pixels.mapv(f64::from);
        let has_nan = fits.pixels.iter().any(|v| v.is_nan());
        let mask = has_nan.then(|| fits.pixels.mapv(|v| !v.is_nan()));
        let temp = make_temp_image(
            fits.shape,
            fits.coordinates,
            &fits.units,
            &fits.image_info,
            pixels,
            mask,
        )?;
        return Ok(Operand::Real(MaskSource::Memory(Box::new(temp)), true));
    }

    match AnyPagedImage::open(path)? {
        AnyPagedImage::Float64(img) => {
            if mask_name.is_some() {
                return Err(ImageError::CasaSelection(format!(
                    "Input mask specification is incorrect: ImageExprParse: {} is a PagedImage with an unsupported data type",
                    path.display()
                )));
            }
            Ok(Operand::Real(MaskSource::Paged(Box::new(img)), false))
        }
        AnyPagedImage::Float32(img) => {
            convert_paged_mask_source(&img, mask_name.as_deref(), f64::from)
                .map(|image| Operand::Real(image, true))
        }
        AnyPagedImage::Complex64(img) if mask_name.is_none() => {
            Ok(Operand::Complex(MaskSource::Paged(Box::new(img))))
        }
        AnyPagedImage::Complex64(img) => {
            convert_paged_mask_source(&img, mask_name.as_deref(), |c| c).map(Operand::Complex)
        }
        AnyPagedImage::Complex32(img) => {
            convert_paged_mask_source(&img, mask_name.as_deref(), |c: Complex32| {
                Complex64::new(f64::from(c.re), f64::from(c.im))
            })
            .map(Operand::Complex)
        }
    }
}

fn convert_paged_mask_source<S: ImagePixel, T: ImagePixel>(
    img: &PagedImage<S>,
    mask_name: Option<&str>,
    convert: impl FnMut(S) -> T,
) -> Result<MaskSource<T>, ImageError> {
    validate_mask_name(img, mask_name)?;
    let data = img.get()?.mapv(convert);
    let mask = match mask_name {
        Some(name) => Some(img.get_named_mask(name)?),
        None => img.default_mask()?,
    };
    let temp = make_temp_image(
        img.shape().to_vec(),
        img.coordinates().clone(),
        img.units(),
        &img.image_info()?,
        data,
        mask,
    )?;
    Ok(MaskSource::Memory(Box::new(temp)))
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

fn make_temp_image<T: ImagePixel>(
    shape: Vec<usize>,
    coords: CoordinateSystem,
    units: &str,
    info: &ImageInfo,
    data: ArrayD<T>,
    mask: Option<ArrayD<bool>>,
) -> Result<TempImage<T>, ImageError> {
    let mut temp = TempImage::new(shape, coords, TempStoragePolicy::Memory)?;
    temp.set_units(units)?;
    temp.set_image_info(info)?;
    let ndim = temp.ndim();
    temp.put_slice(&data, &vec![0; ndim])?;
    if let Some(m) = mask {
        temp.put_mask("mask0", &m)?;
        temp.set_default_mask("mask0")?;
    }
    Ok(temp)
}
