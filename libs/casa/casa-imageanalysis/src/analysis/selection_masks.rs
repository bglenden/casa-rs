// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// The image mask of a selection, combined with its region's and a LEL
/// mask expression's. The expression is CASA's full lattice expression
/// language, evaluated over the image and extended to its shape when
/// `stretch` allows.
pub(super) fn selection_pixel_mask<T>(
    image: &PagedImage<T>,
    effective_region: Option<&PixelRegion>,
    selection: &Selection,
    mask_expr: Option<&str>,
    stretch: bool,
) -> Result<Option<ndarray::ArrayD<bool>>, ImageError>
where
    T: ImagePixel + ImageExprValue + PartialOrd + ExprValueConvert,
{
    let mut pixel_mask =
        image.get_mask_slice(&selection.start, &selection.shape, &vec![1; image.ndim()])?;
    if let Some(region) = effective_region {
        let region_mask = region_pixel_mask(region, selection)?;
        pixel_mask = Some(match pixel_mask {
            Some(mut mask) => {
                Zip::from(&mut mask)
                    .and(&region_mask)
                    .for_each(|existing, region_valid| *existing = *existing && *region_valid);
                mask
            }
            None => region_mask,
        });
    }
    if let Some(expr) = mask_expr.filter(|text| !text.trim().is_empty()) {
        let expr_mask = image_mask_expression(expr, image, selection, stretch)?;
        pixel_mask = Some(match pixel_mask {
            Some(mut mask) => {
                Zip::from(&mut mask)
                    .and(&expr_mask)
                    .for_each(|existing, expr_valid| *existing = *existing && *expr_valid);
                mask
            }
            None => expr_mask,
        });
    }
    Ok(pixel_mask)
}

/// A CASA LEL mask expression over the image: parsed by the expression
/// engine, evaluated over the whole image, extended to the image's shape
/// when `stretch` allows it, and cut to the selection.
pub(super) fn image_mask_expression<T>(
    expr: &str,
    image: &PagedImage<T>,
    selection: &Selection,
    stretch: bool,
) -> Result<ndarray::ArrayD<bool>, ImageError>
where
    T: ImagePixel + ImageExprValue + PartialOrd + ExprValueConvert,
{
    // Open each referenced image as the
    // target's pixel type and hand them to the parser's resolver.
    let names = casa_images::expr_parser::referenced_images(expr).map_err(|error| {
        ImageError::CasaSelection(format!(
            "Input mask specification is incorrect: {}",
            mask_error_wording(error)
        ))
    })?;
    let mut opened: Vec<PagedImage<T>> = Vec::with_capacity(names.len());
    for name in &names {
        let opened_image = PagedImage::<T>::open(name).map_err(|_| {
            ImageError::CasaSelection(format!(
                "Input mask specification is incorrect: ImageExprParse: '{name}' \
                 is an unknown lattice or image or it is an unqualified region"
            ))
        })?;
        opened.push(opened_image);
    }
    let sources: std::collections::HashMap<String, &dyn ImageInterface<T>> = names
        .iter()
        .zip(opened.iter())
        .map(|(name, opened)| (name.clone(), opened as &dyn ImageInterface<T>))
        .collect();
    let resolver = HashMapResolver(sources);
    let expression = parse_mask_expr(expr, &resolver).map_err(|error| {
        ImageError::CasaSelection(format!(
            "Input mask specification is incorrect: {}",
            mask_error_wording(error)
        ))
    })?;
    let mask = expression.get()?;
    let mask_shape = mask.shape().to_vec();
    let target_shape = image.shape().to_vec();
    let extended = if mask_shape == target_shape {
        mask
    } else {
        let extendable = mask_shape.len() <= target_shape.len()
            && mask_shape
                .iter()
                .zip(&target_shape)
                .all(|(mask_axis, image_axis)| mask_axis == image_axis || *mask_axis == 1);
        if !extendable {
            return Err(ImageError::CasaSelection(
                "Unable to extend mask: ExtendSpecifier - lengths of old axis mismatch".to_string(),
            ));
        }
        if !stretch {
            return Err(ImageError::CasaSelection(format!(
                "The input image shape ({target_shape:?}) and mask shape ({mask_shape:?}) are \
                 different, and it was specified that the mask should not be extended, so the \
                 mask cannot be applied to the (sub)image. Specifying that the mask should be \
                 extended may resolve the issue"
            )));
        }
        extend_mask_to_shape(mask, &target_shape)?
    };
    let start = &selection.start;
    let shape = &selection.shape;
    let cut = extended.slice_each_axis(|axis| {
        ndarray::Slice::from(start[axis.axis.0]..start[axis.axis.0] + shape[axis.axis.0])
    });
    Ok(cut.to_owned())
}

/// Tile a mask whose axes prefix the image's, each equal or degenerate,
/// to the image's full shape: casacore extends along the image's later
/// axes and broadcasts degenerate ones.
pub(super) fn extend_mask_to_shape(
    mask: ndarray::ArrayD<bool>,
    target_shape: &[usize],
) -> Result<ndarray::ArrayD<bool>, ImageError> {
    let mask_shape = mask.shape().to_vec();
    let tiled = ndarray::ArrayD::<bool>::from_shape_fn(ndarray::IxDyn(target_shape), |index| {
        let target: Vec<usize> = index.slice().to_vec();
        let source: Vec<usize> = mask_shape
            .iter()
            .enumerate()
            .map(|(axis, &mask_axis)| target.get(axis).copied().unwrap_or(0).min(mask_axis - 1))
            .collect();
        mask[ndarray::IxDyn(&source)]
    });
    Ok(tiled)
}

/// The quoted image names a LEL expression carries.
pub fn quoted_expression_names(expr: &str) -> Vec<String> {
    let mut names = Vec::new();
    let characters: Vec<char> = expr.chars().collect();
    let mut index = 0;
    while index < characters.len() {
        let quote = characters[index];
        if (quote == '"' || quote == '\'')
            && let Some(end) = characters[index + 1..].iter().position(|&c| c == quote)
        {
            let name: String = characters[index + 1..index + 1 + end].iter().collect();
            if !name.is_empty() && !names.contains(&name) {
                names.push(name);
            }
            index += end + 2;
            continue;
        }
        index += 1;
    }
    names
}

/// The selection's pixel values as a robust-algorithm sample, with each
/// value's Fortran-linear position inside the selection, filtered by
/// the pixel mask, an inclusive pixel-value range, and the named
/// channel and Stokes sets.
#[allow(clippy::too_many_arguments)]
pub(super) fn sample_points<T>(
    pixels: &ndarray::ArrayD<T>,
    pixel_mask: Option<&ndarray::ArrayD<bool>>,
    includepix: Option<[f64; 2]>,
    channel_axis: Option<usize>,
    channel_indices: Option<&[usize]>,
    stokes_axis: Option<usize>,
    stokes_indices: Option<&[usize]>,
    start: &[usize],
) -> Vec<SamplePoint>
where
    T: ImagePixel + StatsElement,
{
    let shape = pixels.shape();
    let mut strides = Vec::with_capacity(shape.len());
    let mut stride = 1u64;
    for &axis_shape in shape {
        strides.push(stride);
        stride *= axis_shape as u64;
    }
    let mut sample = Vec::new();
    for (index, value) in pixels.indexed_iter() {
        if let Some(mask) = pixel_mask {
            let position: &[usize] = index.slice();
            if !mask[position] {
                continue;
            }
        }
        let number = value.to_f64_stats();
        if let Some([min, max]) = includepix
            && !(min <= number && number <= max)
        {
            continue;
        }
        let in_named_set = |axis: Option<usize>, indices: Option<&[usize]>| match (axis, indices) {
            (Some(axis), Some(indices)) => {
                let absolute = index[axis] + start[axis];
                indices.contains(&absolute)
            }
            _ => true,
        };
        if !in_named_set(channel_axis, channel_indices)
            || !in_named_set(stokes_axis, stokes_indices)
        {
            continue;
        }
        let mut linear = 0u64;
        for (axis, &axis_stride) in strides.iter().enumerate() {
            linear += index[axis] as u64 * axis_stride;
        }
        sample.push(SamplePoint {
            value: number,
            position: linear,
        });
    }
    // casacore walks its arrays in Fortran order, so a tie's reported
    // position is the first in that order.
    sample.sort_by_key(|point| point.position);
    sample
}

/// The position of a Fortran-linear index inside a selection shape.
pub(super) fn selection_position(linear: u64, shape: &[usize]) -> Vec<usize> {
    let mut position = Vec::with_capacity(shape.len());
    let mut remainder = linear;
    for &axis_shape in shape {
        position.push((remainder % axis_shape as u64) as usize);
        remainder /= axis_shape.max(1) as u64;
    }
    position
}

/// The inclusive top-right corner of a selection, as pixels.
pub(super) fn trc_pixels(start: &[usize], shape: &[usize]) -> Vec<usize> {
    start
        .iter()
        .zip(shape)
        .map(|(start, shape)| start + shape - 1)
        .collect()
}

/// One statistic of the selection. With no valid pixel, a count or a sum
/// is 0 and every other statistic NaN; CASA returns each as an empty array.
pub(super) fn scalar_stat<T: StatsElement>(
    stats: &LatticeStatistics<'_, T>,
    stat: Statistic,
) -> Result<f64, ImageError> {
    let values = stats.get_statistic(stat)?;
    Ok(values.iter().next().copied().unwrap_or(match stat {
        Statistic::Npts | Statistic::Sum | Statistic::SumSq => 0.0,
        _ => f64::NAN,
    }))
}

pub(super) fn offset_pos(start: &[usize], pos: Vec<usize>) -> Vec<usize> {
    start
        .iter()
        .zip(pos)
        .map(|(base, offset)| base + offset)
        .collect()
}

pub(super) fn combined_selection_mask<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
    selection: &imsubimage::region::SubimageSelection,
    mask_expr: Option<&str>,
    stretch: bool,
) -> Result<Option<ArrayD<bool>>, ImageError> {
    let mut base =
        image.get_mask_slice(&selection.start, &selection.shape, &vec![1; image.ndim()])?;
    let expression = lel_mask::evaluate_mask(
        mask_expr,
        image.shape(),
        image.coordinates(),
        stretch,
        (&selection.start, &selection.shape),
    )?;
    if let Some(expression) = expression {
        if let Some(base) = &mut base {
            Zip::from(base)
                .and(&expression)
                .for_each(|valid, expression| *valid &= *expression);
        } else {
            base = Some(expression);
        }
    }
    Ok(base)
}
