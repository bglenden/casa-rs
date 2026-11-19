// SPDX-License-Identifier: LGPL-3.0-or-later
//! LEL mask evaluation, `image:maskname` resolution, and `stretch` handling
//! for `imsubimage`.

use std::collections::HashMap;
use std::path::Path;

use casa_coordinates::CoordinateSystem;
use casa_types::{ArrayD, Complex32, Complex64};

use crate::analysis::mask_error_wording;
use crate::expr_parser::{ExprValueConvert, HashMapResolver, parse_mask_expr, referenced_images};
use crate::image_expr::ImageExprValue;
use crate::image_expr_lel::can_broadcast_to;
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

/// Evaluates an optional `mask` expression over `window` `(start, shape)` of a
/// parent image of shape `target_shape`.
pub(super) fn evaluate_subimage_mask(
    mask_expr: Option<&str>,
    target_shape: &[usize],
    stretch: bool,
    window: (&[usize], &[usize]),
) -> Result<Option<ArrayD<bool>>, ImageError> {
    let Some(raw) = mask_expr.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };

    let mut expr = raw.to_string();
    if Path::new(raw).exists() {
        // A clean-mask image name alone means its pixels at least 0.5.
        expr = format!("\"{raw}\" >= 0.5");
    } else if let Some((left, _)) = raw.split_once(':')
        && !raw.contains([' ', '>', '<', '=', '!', '&', '|', '(', ')'])
        && Path::new(left).exists()
    {
        // Unquoted path:maskname expression in top-level `mask=` parameter.
        expr = format!("mask(\"{raw}\")");
    }
    let ref_names = referenced_images(&expr).map_err(mask_parse_error)?;

    let mut opened: Vec<(String, MaskSource<f64>)> = Vec::with_capacity(ref_names.len());
    for name in &ref_names {
        opened.push((name.clone(), open_mask_target_f64(name)?));
    }

    evaluate_typed::<f64>(&expr, target_shape, stretch, window, opened)
}

fn evaluate_typed<T>(
    expr: &str,
    target_shape: &[usize],
    stretch: bool,
    window: (&[usize], &[usize]),
    opened: Vec<(String, MaskSource<T>)>,
) -> Result<Option<ArrayD<bool>>, ImageError>
where
    T: ImagePixel + ImageExprValue + PartialOrd + ExprValueConvert + Copy + 'static,
{
    let mut map: HashMap<String, &dyn ImageInterface<T>> = HashMap::new();
    for (name, img) in &opened {
        map.insert(name.clone(), img.as_interface());
    }
    let resolver = HashMapResolver(map);
    let parsed = parse_mask_expr(expr, &resolver).map_err(mask_parse_error)?;
    let mask_shape = parsed.shape();
    if mask_shape != target_shape && !mask_shape.is_empty() {
        if !can_broadcast_to(mask_shape, target_shape) {
            return Err(ImageError::CasaSelection(
                "Unable to extend mask: ExtendSpecifier - lengths of old axis mismatch".into(),
            ));
        }
        if !stretch {
            return Err(ImageError::CasaSelection(format!(
                "The input image shape ({target_shape:?}) and mask shape ({mask_shape:?}) \
                 are different, and it was specified that the mask should not be extended, \
                 so the mask cannot be applied to the (sub)image"
            )));
        }
    }
    parsed
        .eval_window_for_shape(window.0, window.1, target_shape)
        .map(Some)
}

fn mask_parse_error(error: crate::expr_parser::ExprParseError) -> ImageError {
    ImageError::CasaSelection(format!(
        "Input mask specification is incorrect: {}",
        mask_error_wording(error)
    ))
}

fn open_mask_target_f64(spec: &str) -> Result<MaskSource<f64>, ImageError> {
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
) -> Result<MaskSource<f64>, ImageError> {
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
        return Ok(MaskSource::Memory(Box::new(temp)));
    }

    match AnyPagedImage::open(path)? {
        AnyPagedImage::Float64(img) => {
            if mask_name.is_some() {
                return Err(ImageError::CasaSelection(format!(
                    "Input mask specification is incorrect: ImageExprParse: {} is a PagedImage with an unsupported data type",
                    path.display()
                )));
            }
            Ok(MaskSource::Paged(Box::new(img)))
        }
        AnyPagedImage::Float32(img) => {
            convert_paged_to_f64_mask_source(&img, mask_name.as_deref(), f64::from)
        }
        AnyPagedImage::Complex64(img) => {
            convert_paged_to_f64_mask_source(&img, mask_name.as_deref(), |c: Complex64| c.norm())
        }
        AnyPagedImage::Complex32(img) => {
            convert_paged_to_f64_mask_source(&img, mask_name.as_deref(), |c: Complex32| {
                f64::from(c.re).hypot(f64::from(c.im))
            })
        }
    }
}

fn convert_paged_to_f64_mask_source<S: ImagePixel + Copy>(
    img: &PagedImage<S>,
    mask_name: Option<&str>,
    to_f64: impl Fn(S) -> f64,
) -> Result<MaskSource<f64>, ImageError> {
    validate_mask_name(img, mask_name)?;
    let data = img.get()?.mapv(to_f64);
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
