// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs;
use std::path::{Path, PathBuf};

use casa_core::ArrayD;
use ndarray::{IxDyn, ShapeBuilder, Zip};

use crate::RegridTemplate;
use crate::regrid::regrid_mosaic_pointing;
use casa_images::{Image, ImageError, ImageInfo, TempImage, TempStoragePolicy};

/// Stitch separately restored pointings into a flux-correct linear mosaic.
///
/// Each pair contains an uncorrected restored image (the `.image` from
/// `tclean`) and its primary-beam response (`.pb`). Use the same units and
/// restoring beam for all pointings. Their direction grids may differ, but
/// the direction frame and non-direction axes must match the template. Only
/// the template's shape and coordinates are used.
///
/// The output image is the sum of bilinearly regridded image-times-beam
/// products divided by the sum of bilinearly regridded squared beams. Both
/// input masks restrict a pair's contribution. The coverage image records
/// the denominator. As in CASA's optimal `linearmosaic`, the image is zero
/// where the weight is at most 1e-6 of peak coverage.
///
/// # Errors
///
/// Empty input, incompatible grids, units or beams, non-finite input on
/// valid pixels, or a failure to read or write a CASA image. Both output
/// paths must be new and distinct.
pub fn linearmosaic(
    pointings: &[(PathBuf, PathBuf)],
    template: &Path,
    output: &Path,
    output_weight: &Path,
) -> Result<(), ImageError> {
    let Some((first, _)) = pointings.first() else {
        return Err(invalid("at least one image and primary beam are required"));
    };
    let output = fresh_path(output)?;
    let output_weight = fresh_path(output_weight)?;
    if output == output_weight {
        return Err(invalid("image and weight outputs must be distinct"));
    }

    let grid = RegridTemplate::of(&Image::open(template)?);
    let first = Image::open(first)?;
    let units = first.units().to_owned();
    let info = first.image_info()?;
    drop(first);
    let shape = IxDyn(&grid.shape).f();
    let mut numerator = ArrayD::<f32>::zeros(shape.clone());
    let mut weight = ArrayD::<f32>::zeros(shape);
    for (image, primary_beam) in pointings {
        let image = Image::open(image)?;
        if image.units() != units || image.image_info()?.beam_set != info.beam_set {
            return Err(invalid(
                "all pointings must use the same units and restoring beam",
            ));
        }
        accumulate(
            &image,
            &Image::open(primary_beam)?,
            &grid,
            &mut numerator,
            &mut weight,
        )?;
    }
    let cutoff = 1e-6 * weight.iter().copied().fold(0.0_f32, f32::max);
    Zip::from(&mut numerator)
        .and(&weight)
        .for_each(|image, &w| {
            *image = if w > cutoff { *image / w } else { 0.0 };
        });
    write_image(&output, &grid, &numerator, &units, &info)?;
    write_image(&output_weight, &grid, &weight, "", &ImageInfo::default())
}

fn accumulate(
    image: &Image,
    beam: &Image,
    grid: &RegridTemplate,
    numerator: &mut ArrayD<f32>,
    weight: &mut ArrayD<f32>,
) -> Result<(), ImageError> {
    let coords = image.coordinates();
    if image.shape() != beam.shape()
        || coords.n_coordinates() != beam.coordinates().n_coordinates()
        || (0..coords.n_coordinates()).any(|k| {
            coords.coordinate(k).to_record() != beam.coordinates().coordinate(k).to_record()
        })
    {
        return Err(invalid(
            "each restored image and primary beam must share a grid",
        ));
    }
    let mut weighted_pixels = image.get()?;
    let mut squared_beam = beam.get()?;
    let mask = match (image.get_mask()?, beam.get_mask()?) {
        (Some(mut a), Some(b)) => {
            Zip::from(&mut a).and(&b).for_each(|p, &q| *p &= q);
            Some(a)
        }
        (mask, None) | (None, mask) => mask,
    };
    for (index, pixel) in weighted_pixels.indexed_iter_mut() {
        let pb = &mut squared_beam[&index];
        if mask.as_ref().is_some_and(|mask| !mask[&index]) {
            *pixel = 0.0;
            *pb = 0.0;
            continue;
        }
        *pixel *= *pb;
        *pb *= *pb;
        if !pixel.is_finite() || !pb.is_finite() {
            return Err(invalid("non-finite weighted pixel or primary beam"));
        }
    }

    let mut scratch = TempImage::new(
        image.shape().to_vec(),
        coords.clone(),
        TempStoragePolicy::Memory,
    )?;
    let start = vec![0; image.shape().len()];
    if let Some(mask) = mask {
        scratch.make_mask("coverage", true, true)?;
        scratch.put_mask("coverage", &mask)?;
    }
    for (pixels, sum, units) in [
        (weighted_pixels, numerator, image.units()),
        (squared_beam, weight, ""),
    ] {
        scratch.set_units(units)?;
        scratch.put_slice(&pixels, &start)?;
        let projected = regrid_mosaic_pointing(&scratch, grid)?;
        if !projected.template_coordinates {
            return Err(invalid(
                "all spectral and Stokes coordinates must match the template",
            ));
        }
        Zip::from(sum)
            .and(&projected.data)
            .and(&projected.mask)
            .for_each(|total, &value, &valid| {
                if valid {
                    *total += value;
                }
            });
    }
    Ok(())
}

fn fresh_path(path: &Path) -> Result<PathBuf, ImageError> {
    let name = path
        .file_name()
        .ok_or_else(|| invalid("missing output image name"))?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let path = fs::canonicalize(parent)
        .map_err(|error| ImageError::Io(format!("{}: {error}", parent.display())))?
        .join(name);
    match path.symlink_metadata() {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(path),
        Err(error) => Err(ImageError::Io(format!("{}: {error}", path.display()))),
        Ok(_) => Err(invalid(&format!(
            "output already exists: {}",
            path.display()
        ))),
    }
}

fn write_image(
    path: &Path,
    grid: &RegridTemplate,
    pixels: &ArrayD<f32>,
    units: &str,
    info: &ImageInfo,
) -> Result<(), ImageError> {
    fs::create_dir(path).map_err(|error| ImageError::Io(format!("{}: {error}", path.display())))?;
    let mut output = Image::create(grid.shape.clone(), grid.coordinates.clone(), path)?;
    output.put_slice(pixels, &vec![0; grid.shape.len()])?;
    output.set_units(units)?;
    output.set_image_info(info)?;
    output.save()
}

fn invalid(message: &str) -> ImageError {
    ImageError::InvalidMetadata(format!("linearmosaic: {message}"))
}
