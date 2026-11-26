// SPDX-License-Identifier: LGPL-3.0-or-later
//! Combine interferometric and low-resolution image planes in Fourier space.

use std::{io, path::Path};

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType};
use casa_core::{ArrayD, Value};
use casa_fft::Fft2;
use casa_tables::publication::{
    exchange_directories, lock_table_tree_for_replacement, rename_without_replacing,
};
use casa_tables::{Table, TableOptions};
use ndarray::{Array2, IxDyn, ShapeBuilder, Zip};
use num_complex::Complex32;

use super::axes::direction_plane_axes;
use super::{SPEED_OF_LIGHT_M_S, ensure_outfile_does_not_alias_input, patch_saved_coords_keyword};
use crate::plane::PlaneIndex;
use crate::regrid::{RegridTemplate, Regridded, regrid};
use crate::{FeatherRequest, FeatherSummary};
use casa_images::resample::RegridMethod;
use casa_images::{
    AnyPagedImage, GaussianBeam, ImageBeamSet, ImageError, PagedImage, SubImage, TempImage,
};

/// Feather high- and low-resolution images on the high-resolution grid.
pub fn feather(request: &FeatherRequest) -> Result<FeatherSummary, ImageError> {
    let high = AnyPagedImage::open(&request.highres)?;
    let low = AnyPagedImage::open(&request.lowres)?;
    match (&high, &low) {
        (AnyPagedImage::Float32(high), AnyPagedImage::Float32(low)) => {
            feather_typed(high, low, request)
        }
        _ => Err(ImageError::InvalidMetadata(
            "feather tutorial support currently requires f32 high/low images".to_string(),
        )),
    }
}

fn feather_typed(
    high: &PagedImage<f32>,
    low: &PagedImage<f32>,
    request: &FeatherRequest,
) -> Result<FeatherSummary, ImageError> {
    if request.imagename.as_os_str().is_empty() {
        return Err(invalid("feather requires an output image name"));
    }
    let destination = std::path::absolute(&request.imagename).map_err(output_io)?;
    check_output(request, &destination)?;
    let high_shape = high.shape().to_vec();
    let high_info = high.image_info()?;
    let low_info = low.image_info()?;
    let high_beams = &high_info.beam_set;
    let low_beams = &low_info.beam_set;
    if high_beams.is_empty() || low_beams.is_empty() {
        return Err(invalid("feather requires restoring beams on both images"));
    }
    let mut template = RegridTemplate::of(high);
    let low_regridded = if high_beams.is_multi() || low_beams.is_multi() {
        // CASA pairs channel and polarization planes by pixel index before it
        // regrids the low-resolution sky plane. Retain those low-image axes.
        let paired_shape = pair_nonspatial_axes(&mut template, low)?;
        let paired = SubImage::new(low, vec![0; low.ndim()], paired_shape)?;
        regrid(&paired, &template, RegridMethod::Linear)?
    } else {
        regrid(low, &template, RegridMethod::Linear)?
    };
    let Regridded {
        data: mut low_data,
        mask: low_mask,
        ..
    } = low_regridded;
    let origin = vec![0; high.ndim()];
    let mut high_data = high.get_slice(&origin, &high_shape)?;
    let high_mask = high.get_mask_slice(&origin, &high_shape, &vec![1; high.ndim()])?;
    let low_mask_name = low.default_mask_name();
    let low_mask = low_mask_name.as_ref().map(|_| low_mask);
    // CASA copies each default mask and replaces its invalid samples with zero
    // before they can contribute to the Fourier transform.
    clear_masked_pixels(&mut high_data, high_mask.as_ref());
    clear_masked_pixels(&mut low_data, low_mask.as_ref());
    if high_shape.len() < 2 {
        return Err(ImageError::InvalidMetadata(
            "feather requires at least two direction axes".to_string(),
        ));
    }
    // Feathered in the direction plane, wherever its axes are.
    let direction = direction_plane_axes(high)?;
    let planes = PlaneIndex::new(&high_shape, direction);
    let nx = high_shape[direction[0]];
    let ny = high_shape[direction[1]];
    let common = high_beams
        .single_beam()
        .zip(low_beams.single_beam())
        .map(|beams| plane_transfer([nx, ny], beams, high.coordinates(), 0, request))
        .transpose()?;
    let beam_axes =
        [CoordinateType::Spectral, CoordinateType::Stokes].map(|kind| high.find_axis(kind));
    let mut output_data = ArrayD::<f32>::zeros(IxDyn(&high_shape).f());
    let mut output_mask = ArrayD::<bool>::from_elem(IxDyn(&high_shape).f(), true);
    for plane in 0..planes.count() {
        let plane_filter;
        let transfer = if let Some(common) = &common {
            common
        } else {
            let [channel, stokes] =
                beam_axes.map(|axis| axis.map_or(0, |axis| planes.pixel(plane, axis)));
            let hi_beam = plane_beam(high_beams, channel, stokes)?;
            let lo_beam = plane_beam(low_beams, channel, stokes)?;
            plane_filter = plane_transfer(
                [nx, ny],
                (hi_beam, lo_beam),
                high.coordinates(),
                channel,
                request,
            )?;
            &plane_filter
        };
        let high_fft = centered_fft2(
            &planes
                .view(plane, &high_data)
                .mapv(|value| Complex32::new(value, 0.0)),
        );
        let low_fft = centered_fft2(
            &planes
                .view(plane, &low_data)
                .mapv(|value| Complex32::new(value, 0.0)),
        );
        let mut combined = Array2::<Complex32>::zeros((nx, ny));
        for x in 0..nx {
            for y in 0..ny {
                let high_weight = transfer.crossover[(x, y)];
                let low_value = transfer
                    .low_filter
                    .as_ref()
                    .map_or(low_fft[(x, y)], |filter| low_fft[(x, y)] * filter[(x, y)]);
                combined[(x, y)] = high_fft[(x, y)] * high_weight + low_value * transfer.low_scale;
            }
        }
        let image_plane = centered_ifft2(&combined);
        Zip::from(planes.view_mut(plane, &mut output_data))
            .and(&image_plane)
            .for_each(|out, value| *out = value.re);
    }
    clear_masked_pixels(&mut output_data, high_mask.as_ref().or(low_mask.as_ref()));
    for mask in [high_mask.as_ref(), low_mask.as_ref()]
        .into_iter()
        .flatten()
    {
        Zip::from(&mut output_mask)
            .and(mask)
            .for_each(|out, valid| {
                *out = *out && *valid;
            });
    }
    let mut output = TempImage::<f32>::new(
        high_shape.clone(),
        high.coordinates().clone(),
        casa_lattices::TempStoragePolicy::Auto {
            memory_budget_bytes: 64 * 1024 * 1024,
            scratch: casa_lattices::ScratchSpace::SystemTemp,
        },
    )?;
    let legacy_coordinates = high.table().keywords().get("coords").cloned();
    output.set_units(high.units())?;
    output.set_image_info(&high_info)?;
    output.set_misc_info(high.misc_info())?;
    output.put_slice(&output_data, &vec![0; output.ndim()])?;
    if let Some(name) = high.default_mask_name().or(low_mask_name) {
        output.put_mask(&name, &output_mask)?;
        output.set_default_mask(&name)?;
    }
    let stage = tempfile::tempdir_in(destination.parent().ok_or(ImageError::NotPersistent)?)
        .map_err(output_io)?;
    let product = stage.path().join("image");
    output.save_as(&product)?;
    if let Some(Value::Record(coords)) = legacy_coordinates {
        patch_saved_coords_keyword(&product, coords)?;
    }
    match rename_without_replacing(&product, &destination) {
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists && request.overwrite => {
            check_output(request, &destination)?;
            let _locks = lock_table_tree_for_replacement(&destination)?;
            exchange_directories(&product, &destination).map_err(output_io)?;
        }
        result => result.map_err(output_io)?,
    }
    Ok(FeatherSummary {
        highres: request.highres.display().to_string(),
        lowres: request.lowres.display().to_string(),
        imagename: request.imagename.display().to_string(),
        shape: high_shape,
        sdfactor: request.sdfactor,
        valid_pixels: output_mask.iter().filter(|value| **value).count(),
    })
}

fn clear_masked_pixels(data: &mut ArrayD<f32>, mask: Option<&ArrayD<bool>>) {
    if let Some(mask) = mask {
        Zip::from(data).and(mask).for_each(|value, valid| {
            if !valid {
                *value = 0.0;
            }
        });
    }
}

fn check_output(request: &FeatherRequest, destination: &Path) -> Result<(), ImageError> {
    for input in [&request.highres, &request.lowres] {
        ensure_outfile_does_not_alias_input(destination, input)?;
        if input
            .canonicalize()
            .ok()
            .zip(
                destination
                    .parent()
                    .and_then(|parent| parent.canonicalize().ok()),
            )
            .is_some_and(|(input, parent)| parent.starts_with(input))
        {
            return Err(invalid("feather output must be outside both input images"));
        }
    }
    match destination.symlink_metadata() {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(output_io(error)),
        Ok(_) if !request.overwrite => Err(invalid(format!(
            "feather output already exists: {}",
            destination.display()
        ))),
        Ok(metadata)
            if metadata.is_dir()
                && Table::open_metadata_only(TableOptions::new(destination))
                    .is_ok_and(|table| table.info().table_type == "Image") =>
        {
            Ok(())
        }
        Ok(_) => Err(invalid(format!(
            "{} is not a CASA image directory",
            destination.display()
        ))),
    }
}

fn output_io(error: io::Error) -> ImageError {
    ImageError::Io(error.to_string())
}

fn pair_nonspatial_axes(
    template: &mut RegridTemplate,
    low: &PagedImage<f32>,
) -> Result<Vec<usize>, ImageError> {
    let mut paired_shape = low.shape().to_vec();
    for kind in [CoordinateType::Spectral, CoordinateType::Stokes] {
        let (Some(target), Some(source)) = (
            template.coordinates.find_coordinate(kind),
            low.coordinates().find_coordinate(kind),
        ) else {
            continue;
        };
        let [Some(output_axis), Some(input_axis)] = [
            template
                .coordinates
                .coordinate_pixel_axes(target)
                .and_then(|axes| axes.first().copied().flatten()),
            low.coordinates()
                .coordinate_pixel_axes(source)
                .and_then(|axes| axes.first().copied().flatten()),
        ] else {
            continue;
        };
        if template.shape[output_axis] > low.shape()[input_axis] {
            return Err(invalid(
                "per-plane feather requires a low-resolution plane for each high-resolution plane",
            ));
        }
        paired_shape[input_axis] = template.shape[output_axis];
        template
            .coordinates
            .replace_coordinate(target, low.coordinates().coordinate(source).clone());
    }
    Ok(paired_shape)
}

fn plane_beam(
    beams: &ImageBeamSet,
    channel: usize,
    stokes: usize,
) -> Result<GaussianBeam, ImageError> {
    if (beams.n_channels() != 1 && channel >= beams.n_channels())
        || (beams.n_stokes() != 1 && stokes >= beams.n_stokes())
    {
        return Err(invalid(
            "restoring beam set does not conform to the image planes",
        ));
    }
    let beam = *beams.beam(channel, stokes);
    if beam.is_null() {
        return Err(invalid("a feather plane is missing its restoring beam"));
    }
    Ok(beam)
}

struct PlaneTransfer {
    crossover: Array2<Complex32>,
    low_filter: Option<Array2<Complex32>>,
    low_scale: f32,
}

fn plane_transfer(
    shape: [usize; 2],
    (high, low): (GaussianBeam, GaussianBeam),
    coordinates: &CoordinateSystem,
    channel: usize,
    request: &FeatherRequest,
) -> Result<PlaneTransfer, ImageError> {
    let (low, low_filter) = single_dish_filter(shape, low, coordinates, channel, request)?;
    let crossover = feather_crossover_weight(shape[0], shape[1], low, coordinates)?;
    let low_scale = request.sdfactor * (high.area() / low.area()) as f32;
    Ok(PlaneTransfer {
        crossover,
        low_filter,
        low_scale,
    })
}

fn single_dish_filter(
    [nx, ny]: [usize; 2],
    low_beam: GaussianBeam,
    coordinates: &CoordinateSystem,
    channel: usize,
    request: &FeatherRequest,
) -> Result<(GaussianBeam, Option<Array2<Complex32>>), ImageError> {
    if !request.effdishdiam.is_finite() {
        return Err(invalid("effective dish diameter must be finite"));
    }
    if request.effdishdiam <= 0.0 && !request.lowpassfiltersd {
        return Ok((low_beam, None));
    }
    let frequency = coordinates
        .find_coordinate(CoordinateType::Spectral)
        .map(|index| coordinates.coordinate(index))
        .and_then(|model| match model {
            CoordinateModel::Spectral(spectral) => Some(spectral),
            _ => None,
        })
        .ok_or_else(|| invalid("single-dish filtering requires a spectral coordinate"))?
        .world_frequency_hz(channel as f64)?;
    if !frequency.is_finite() || frequency <= 0.0 || low_beam.is_null() {
        return Err(invalid(
            "single-dish filtering requires a positive frequency and a restoring beam",
        ));
    }
    let diffraction = 1.22 * SPEED_OF_LIGHT_M_S / frequency;
    let diameter = if request.effdishdiam > 0.0 {
        request.effdishdiam
    } else {
        (diffraction / low_beam.major) as f32
    };
    let width = diffraction / f64::from(diameter);
    let target = GaussianBeam::new(width, width, 0.0);
    let kernel = dish_kernel(target, low_beam)?;
    let mut psf = gaussian_psf(nx, ny, kernel, coordinates)?;
    let volume: f32 = psf.t().iter().map(|value| value.re).sum();
    if !volume.is_finite() || volume == 0.0 {
        return Err(invalid(
            "single-dish smoothing kernel has zero or nonfinite volume",
        ));
    }
    psf.mapv_inplace(|value| value / volume);
    // The CASA real FFT convolution flips by ceil(n/2) on its way back
    // to the image, placing the kernel one pixel earlier on odd axes.
    let psf = shifted_plane(&psf, [nx % 2, ny % 2]);
    Ok((target, Some(centered_fft2(&psf))))
}

fn dish_kernel(target: GaussianBeam, low: GaussianBeam) -> Result<GaussianBeam, ImageError> {
    let error = || {
        invalid("beam from the effective diameter is smaller than the original single-dish beam")
    };
    if target.major < low.major {
        // GaussianDeconvolver returns the original beam as the kernel when it
        // classifies a nearly unresolved difference as a point source.
        let (sin, cos) = low.position_angle.sin_cos();
        let minor_limit = 0.1 * target.major.min(low.minor).powi(2);
        let alpha = target.major.powi(2) - (low.major * cos).powi(2) - (low.minor * sin).powi(2);
        let beta = target.major.powi(2) - (low.major * sin).powi(2) - (low.minor * cos).powi(2);
        return if alpha > -minor_limit && beta > -minor_limit {
            Ok(low)
        } else {
            Err(error())
        };
    }
    Ok(target
        .deconvolving_beam(low)
        .map_err(|_| error())?
        .unwrap_or(low))
}

fn invalid(message: impl Into<String>) -> ImageError {
    ImageError::InvalidMetadata(message.into())
}

fn feather_crossover_weight(
    nx: usize,
    ny: usize,
    low_beam: GaussianBeam,
    coordinates: &CoordinateSystem,
) -> Result<Array2<Complex32>, ImageError> {
    let psf = gaussian_psf(nx, ny, low_beam, coordinates)?;
    // The crossover PSF is made by convolving an impulse at 2*(n/4),
    // with integer division, instead of the usual FFT origin at n/2.
    let crossover_shift = |size| size / 2 - 2 * (size / 4) + size % 2;
    let psf = shifted_plane(&psf, [crossover_shift(nx), crossover_shift(ny)]);
    let transformed = centered_fft2(&psf);
    let peak = transformed
        .iter()
        .map(|value| value.norm())
        .fold(0.0_f32, f32::max)
        .max(1.0e-20);
    Ok(transformed.mapv(|value| Complex32::new(1.0, 0.0) - value / peak))
}

fn gaussian_psf(
    nx: usize,
    ny: usize,
    beam: GaussianBeam,
    coordinates: &CoordinateSystem,
) -> Result<Array2<Complex32>, ImageError> {
    let mut psf = Array2::<Complex32>::zeros((nx, ny));
    let (x_inc, y_inc, x_ref, y_ref) = direction_axis_geometry(coordinates)?;
    let major_width = beam.major.abs().max(1.0e-12);
    let minor_width = beam.minor.abs().max(1.0e-12);
    let major_scale = 4.0 * std::f64::consts::LN_2 / major_width.powi(2);
    let minor_scale = 4.0 * std::f64::consts::LN_2 / minor_width.powi(2);
    let pa = beam.position_angle + std::f64::consts::FRAC_PI_2;
    let cos_pa = pa.cos();
    let sin_pa = pa.sin();
    for x in 0..nx {
        for y in 0..ny {
            let dx = (x as f64 - x_ref) * x_inc.abs();
            let dy = (y as f64 - y_ref) * y_inc.abs();
            let major = dx * cos_pa + dy * sin_pa;
            let minor = -dx * sin_pa + dy * cos_pa;
            let radius = major_scale * major.powi(2) + minor_scale * minor.powi(2);
            let value = if radius < 20.0 {
                (-radius).exp() as f32
            } else {
                0.0
            };
            psf[(x, y)] = Complex32::new(value, 0.0);
        }
    }
    Ok(psf)
}

fn direction_axis_geometry(
    coordinates: &CoordinateSystem,
) -> Result<(f64, f64, f64, f64), ImageError> {
    let direction_index = coordinates
        .find_coordinate(CoordinateType::Direction)
        .ok_or_else(|| ImageError::InvalidMetadata("missing direction coordinate".to_string()))?;
    let coordinate = coordinates.coordinate(direction_index);
    let increments = coordinate.increment();
    let reference_pixel = coordinate.reference_pixel();
    if increments.len() < 2 {
        return Err(ImageError::InvalidMetadata(
            "direction coordinate has fewer than two axes".to_string(),
        ));
    }
    if reference_pixel.len() < 2 {
        return Err(ImageError::InvalidMetadata(
            "direction coordinate has fewer than two reference pixels".to_string(),
        ));
    }
    Ok((
        increments[0],
        increments[1],
        reference_pixel[0],
        reference_pixel[1],
    ))
}

fn centered_fft2(input: &Array2<Complex32>) -> Array2<Complex32> {
    let mut shifted = ifftshift2(input);
    let mut fft = Fft2::<f32>::with_threads([shifted.shape()[0], shifted.shape()[1]], 1)
        .expect("valid feather FFT shape");
    fft.transform(
        shifted.as_slice_mut().expect("contiguous feather plane"),
        false,
    )
    .expect("valid feather FFT plan");
    fftshift2(&shifted)
}

fn centered_ifft2(input: &Array2<Complex32>) -> Array2<Complex32> {
    let mut shifted = ifftshift2(input);
    let mut fft = Fft2::<f32>::with_threads([shifted.shape()[0], shifted.shape()[1]], 1)
        .expect("valid feather FFT shape");
    fft.transform(
        shifted.as_slice_mut().expect("contiguous feather plane"),
        true,
    )
    .expect("valid feather FFT plan");
    let scale = 1.0 / (input.shape()[0] * input.shape()[1]) as f32;
    shifted.mapv_inplace(|value| value * scale);
    fftshift2(&shifted)
}

fn fftshift2(input: &Array2<Complex32>) -> Array2<Complex32> {
    shift2(input, false)
}

fn ifftshift2(input: &Array2<Complex32>) -> Array2<Complex32> {
    shift2(input, true)
}

fn shift2(input: &Array2<Complex32>, inverse: bool) -> Array2<Complex32> {
    let nx = input.shape()[0];
    let ny = input.shape()[1];
    let x_shift = if inverse { nx / 2 } else { nx.div_ceil(2) };
    let y_shift = if inverse { ny / 2 } else { ny.div_ceil(2) };
    shifted_plane(input, [x_shift, y_shift])
}

fn shifted_plane(input: &Array2<Complex32>, [x_shift, y_shift]: [usize; 2]) -> Array2<Complex32> {
    let (nx, ny) = input.dim();
    Array2::from_shape_fn((nx, ny), |(x, y)| {
        input[((x + x_shift) % nx, (y + y_shift) % ny)]
    })
}
