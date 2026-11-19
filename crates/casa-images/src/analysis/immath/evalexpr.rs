// SPDX-License-Identifier: LGPL-3.0-or-later
//! Expression evaluation (`mode="evalexpr"` and `"spix"`) for `immath`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use casa_coordinates::CoordinateSystem;
use casa_types::RecordValue;

use crate::expr_parser::{HashMapResolver, parse_image_expr};
use crate::{
    ImageError, ImageInfo, ImageInterface, ImageMathSummary, ImagePixelType, ImmathRequest,
};

use super::ResolvedInputs;
use super::selection::{
    ImmathSelection, PreparedInput, RawImage, RealMathPixel, apply_selection_to_raw,
    evaluate_immath_mask, open_raw_image, quote_var_aliases,
};

pub(super) fn run_evalexpr(
    request: &ImmathRequest,
    outfile: &Path,
    inputs: &ResolvedInputs,
    varnames: &[String],
    expr: &str,
    clear_units: bool,
    prec: ImagePixelType,
) -> Result<ImageMathSummary, ImageError> {
    match prec {
        ImagePixelType::Float32 => {
            run_evalexpr_typed::<f32>(request, outfile, inputs, varnames, expr, clear_units)
        }
        ImagePixelType::Float64 => {
            run_evalexpr_typed::<f64>(request, outfile, inputs, varnames, expr, clear_units)
        }
        _ => Err(ImageError::InvalidMetadata(
            "Unsupported value for type, it must be 'float' or 'double'.".into(),
        )),
    }
}

fn run_evalexpr_typed<T: RealMathPixel>(
    request: &ImmathRequest,
    outfile: &Path,
    inputs: &ResolvedInputs,
    varnames: &[String],
    expr: &str,
    clear_units: bool,
) -> Result<ImageMathSummary, ImageError> {
    let imagename_paths = &inputs.imagename_paths;
    let extra_quoted = &inputs.extra_quoted;
    let all_paths: Vec<PathBuf> = imagename_paths
        .iter()
        .chain(extra_quoted.iter())
        .cloned()
        .collect();
    let mut raws: Vec<RawImage<T>> = Vec::with_capacity(all_paths.len());
    for path in &all_paths {
        raws.push(open_raw_image::<T>(path)?);
    }

    let has_selection = has_any_selection(request);
    let selections: Vec<Option<ImmathSelection>> = if has_selection {
        raws.iter()
            .map(|raw| ImmathSelection::from_raw(raw, request, false).map(Some))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![None; raws.len()]
    };

    let has_mask = request
        .mask
        .as_deref()
        .is_some_and(|m| !m.trim().is_empty());

    let mask_overrides: Vec<Option<casa_types::ArrayD<bool>>> = if has_mask {
        let mut raw_aliases: Vec<(String, &dyn ImageInterface<T>)> = Vec::new();
        for (idx, raw) in raws.iter().enumerate() {
            if idx < imagename_paths.len()
                && let Some(vn) = varnames.get(idx)
            {
                raw_aliases.push((vn.clone(), raw.as_interface()));
            }
            if let Some(path) = all_paths.get(idx) {
                raw_aliases.push((path.display().to_string(), raw.as_interface()));
            }
        }
        let mut masks = Vec::with_capacity(raws.len());
        for (raw, sel) in raws.iter().zip(selections.iter()) {
            let window = sel
                .as_ref()
                .map(|s| (s.start.as_slice(), s.shape.as_slice()));
            masks.push(evaluate_immath_mask(
                request.mask.as_deref(),
                raw.as_interface().shape(),
                &raw_aliases,
                request.stretch,
                window,
            )?);
        }
        masks
    } else {
        vec![None; raws.len()]
    };

    let mut prepared: Vec<PreparedInput<T>> = Vec::with_capacity(raws.len());
    for ((raw, mask_override), sel) in raws.into_iter().zip(mask_overrides).zip(selections.iter()) {
        prepared.push(apply_selection_to_raw(raw, mask_override, sel.as_ref())?);
    }

    let rewritten_expr = quote_var_aliases(expr, varnames);
    let mut resolver_map: HashMap<String, &dyn ImageInterface<T>> = HashMap::new();
    for (idx, prep) in prepared.iter().enumerate() {
        if idx < imagename_paths.len()
            && let Some(vn) = varnames.get(idx)
        {
            resolver_map.insert(vn.clone(), prep.as_interface());
        }
        if let Some(path) = all_paths.get(idx) {
            resolver_map.insert(path.display().to_string(), prep.as_interface());
        }
    }
    let resolver = HashMapResolver(resolver_map);
    let compiled = parse_image_expr::<T, _>(&rewritten_expr, &resolver)
        .map_err(|err| ImageError::InvalidMetadata(format!("Invalid image expression: {err}")))?;

    let out_shape = compiled.shape().to_vec();
    let (coords, units, image_info, misc_info, legacy_coords) =
        resolve_output_metadata(request, imagename_paths, &compiled, clear_units)?;

    super::remove_existing_outfile_if_overwrite(outfile, request.overwrite)?;

    let (_saved, valid_pixels) = compiled.save_as_with_full_metadata(
        outfile,
        &coords,
        &units,
        &image_info,
        &misc_info,
        legacy_coords,
    )?;

    Ok(ImageMathSummary {
        imagename: all_paths.iter().map(|p| p.display().to_string()).collect(),
        outfile: outfile.display().to_string(),
        expr: request.expr.clone(),
        shape: out_shape,
        units,
        valid_pixels,
    })
}

fn has_any_selection(request: &ImmathRequest) -> bool {
    [
        request.box_pixels.as_deref(),
        request.region.as_deref(),
        request.chans.as_deref(),
        request.stokes.as_deref(),
    ]
    .into_iter()
    .any(|opt| opt.is_some_and(|s| !s.trim().is_empty()))
}

fn resolve_output_metadata<T: RealMathPixel>(
    request: &ImmathRequest,
    filenames: &[PathBuf],
    compiled: &crate::ImageExpression<T>,
    clear_units: bool,
) -> Result<
    (
        CoordinateSystem,
        String,
        ImageInfo,
        RecordValue,
        Option<RecordValue>,
    ),
    ImageError,
> {
    let out_shape = compiled.shape();
    if let Some(raw_md) = request.imagemd.as_deref().filter(|s| !s.trim().is_empty()) {
        let md_path = translate_imagemd(filenames, raw_md);
        let md_raw = open_raw_image::<T>(&md_path).map_err(|_| {
            ImageError::InvalidMetadata(format!(
                "Cannot access {raw_md} so cannot copy its metadata to output image."
            ))
        })?;
        let md = md_raw.as_interface();
        let coords = if md.coordinates().n_pixel_axes() == out_shape.len() {
            md.coordinates().clone()
        } else {
            compiled.coordinates().clone()
        };
        let legacy_coords = if md.coordinates().n_pixel_axes() == out_shape.len() {
            md_raw.legacy_coords().cloned()
        } else {
            None
        };
        let units = if clear_units {
            String::new()
        } else {
            md.units().to_string()
        };
        return Ok((
            coords,
            units,
            md.image_info()?,
            md.misc_info(),
            legacy_coords,
        ));
    }

    let coords = compiled.coordinates().clone();
    let units = if clear_units {
        String::new()
    } else {
        compiled.units().to_string()
    };
    let image_info = compiled.image_info_ref().clone();
    let misc_info = compiled.misc_info_ref().clone();
    let legacy_coords = compiled.legacy_coords_ref().cloned();
    Ok((coords, units, image_info, misc_info, legacy_coords))
}

fn translate_imagemd(filenames: &[PathBuf], raw_md: &str) -> PathBuf {
    let p = Path::new(raw_md);
    if p.exists() {
        return p.to_path_buf();
    }
    if let Some(rest) = raw_md.strip_prefix("IM")
        && !rest.is_empty()
        && rest.bytes().all(|b| b.is_ascii_digit())
        && let Ok(idx) = rest.parse::<usize>()
        && let Some(target) = filenames.get(idx)
    {
        return target.clone();
    }
    p.to_path_buf()
}
