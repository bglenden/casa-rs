// SPDX-License-Identifier: LGPL-3.0-or-later

//! Convolution, FFT, and 1D smoothing ops (`convolve`, `convolve2d`, `sepconvolve`, `boxcar`, `hanning`, `fft`).

use std::path::{Path, PathBuf};

use casa_coordinates::CoordinateType;
use casa_core::RecordValue;
use casa_core::quanta::casacore::stream_double;
use casa_images::{
    BeamComponent, GaussianBeam, ImageInfo, ImageInterface, ImagePixel, PagedImage,
    StoredGaussianBeam,
};
use casa_lattices::TiledShape;
use ndarray::ArrayD;
use num_complex::{Complex, Complex32, Complex64};

use crate::analysis::imsubimage::{
    ConvolutionKernel, ConvolutionOptions, ConvolutionPixel, FourierOptions, FourierPixel,
    GaussianConvolutionOptions, ImageConvolution, ImageFourier, ImageGaussianConvolution,
    ImageSepConvolution, ImageSmoothing, SepConvolutionOptions, SmoothingKernel, SmoothingOptions,
    SmoothingPixel, SubimageRegion,
};

use super::super::attachment::{AttachedImage, ExternalPathCheck, read_locked};
use super::super::beam_view::BeamView;
use super::super::beams::{image_info_record, stored_beam_record};
use super::super::error::{ImageToolError, ImageToolResult};
use super::super::handle::{ImageHandle, Storage, with_pixels, with_storage};
use super::super::product_helpers::{
    apply_beam_subset, check_handle_aliasing, check_region_overlap, finalize_output_history,
    set_storage_coordinate_record,
};
use super::super::publish::{publish_image, publish_image_set};

const KERNEL_PRECISION: &str =
    "Invalid Table data type when accessing column in ArrayColumn ctor for column map";

/// Kernel source for `ia.convolve`.
pub enum ConvolveKernelInput {
    /// Explicit numeric array kernel.
    Array(ArrayD<f64>),
    /// Path to a kernel image.
    ImagePath(PathBuf),
}

/// Linear n-dimensional convolution (`ia.convolve`).
#[allow(clippy::too_many_arguments)]
pub fn convolve_image(
    attached: &AttachedImage,
    outfile: &str,
    kernel_in: &ConvolveKernelInput,
    scale: f64,
    region: Option<SubimageRegion<'_>>,
    mask: &str,
    overwrite: bool,
    stretch: bool,
    history_command: Option<String>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<AttachedImage> {
    if !matches!(
        &*attached.lock(),
        ImageHandle::Float(_) | ImageHandle::Double(_)
    ) {
        return Err(ImageToolError::new(
            "This method only supports real-valued images",
        ));
    }
    let kernel_path = match kernel_in {
        ConvolveKernelInput::ImagePath(path) => {
            if !path.is_dir() {
                return Err(ImageToolError::new(format!(
                    "Table {} does not exist",
                    path.display()
                )));
            }
            Some(path.as_path())
        }
        ConvolveKernelInput::Array(_) => None,
    };
    let same = kernel_path
        .and_then(|path| path.canonicalize().ok())
        .is_some_and(|path| attached.native_path() == Some(&path));
    let kernel = kernel_path
        .filter(|_| !same)
        .map(AttachedImage::open_for_ordered_read)
        .transpose()?;
    let mut coordinates = None;
    let mut out = publish_image(outfile, overwrite, is_external_open, |dest| {
        let run_build = |image: &ImageHandle, k: Option<&ImageHandle>| {
            convolve_build(
                attached,
                image,
                k,
                outfile,
                kernel_in,
                scale,
                region,
                mask,
                stretch,
                dest,
                history_command.clone(),
            )
        };
        let product = if let Some(k) = &kernel {
            let source_first = attached
                .native_path()
                .zip(k.native_path())
                .is_none_or(|(left, right)| left < right);
            if source_first {
                read_locked(attached, |image| {
                    read_locked(k, |k_img| run_build(image, Some(k_img)))
                })?
            } else {
                read_locked(k, |k_img| {
                    read_locked(attached, |image| run_build(image, Some(k_img)))
                })?
            }
        } else {
            read_locked(attached, |image| run_build(image, same.then_some(image)))?
        };
        coordinates = AttachedImage::capture_live_coordinates(&product)?;
        Ok(product)
    })?;
    if let Some((record, system)) = coordinates {
        out.keep_live_coordinates(record, system);
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn convolve_build(
    attachment: &AttachedImage,
    image: &ImageHandle,
    kernel: Option<&ImageHandle>,
    outfile: &str,
    kernel_in: &ConvolveKernelInput,
    scale: f64,
    region: Option<SubimageRegion<'_>>,
    mask: &str,
    stretch: bool,
    destination: Option<&Path>,
    history_command: Option<String>,
) -> ImageToolResult<ImageHandle> {
    if !outfile.is_empty() {
        check_handle_aliasing(image, attachment, Path::new(outfile), kernel)?;
    }
    let record = image.coordinate_record();
    let (mut product, changed, dimensions) = match image {
        ImageHandle::Float(storage) => {
            let native = match kernel {
                Some(ImageHandle::Float(Storage::Paged(native))) => {
                    Some(native.as_ref() as &dyn ImageInterface<f32>)
                }
                Some(_) => return Err(ImageToolError::new(KERNEL_PRECISION)),
                None => None,
            };
            let (out, changed, dims) = storage.with_float_view(|src| {
                convolve_typed(
                    src,
                    record,
                    native,
                    kernel_in,
                    scale,
                    region,
                    mask,
                    stretch,
                    outfile,
                    destination,
                )
            })?;
            (ImageHandle::Float(out), changed, dims)
        }
        ImageHandle::Double(storage) => {
            let native = match kernel {
                Some(ImageHandle::Double(Storage::Paged(native))) => {
                    Some(native.as_ref() as &dyn ImageInterface<f64>)
                }
                Some(_) => return Err(ImageToolError::new(KERNEL_PRECISION)),
                None => None,
            };
            let (out, changed, dims) = with_storage!(storage, src => {
                convolve_typed(src.as_ref(), record, native, kernel_in, scale, region, mask, stretch, outfile, destination)
            })?;
            (ImageHandle::Double(out), changed, dims)
        }
        _ => {
            return Err(ImageToolError::new(
                "This method only supports real-valued images",
            ));
        }
    };
    if let Some(record) = changed {
        product.set_coordinate_record(record)?;
    }
    let extra_lines = [
        "NOTE: kernel with fewer dimensions than the input image applied. Degenerate axes added to the kernel".to_owned(),
    ];
    let extra = (dimensions < image.shape().len())
        .then_some(("ImageConvolverTask::convolve", extra_lines.as_slice()));
    let entries = image.history_entries()?;
    finalize_output_history(&mut product, &entries, "convolve", history_command, extra)?;
    Ok(product)
}

#[allow(clippy::too_many_arguments)]
fn convolve_typed<T: ConvolutionPixel>(
    source: &dyn ImageInterface<T>,
    coordinates: RecordValue,
    native: Option<&dyn ImageInterface<T>>,
    kernel_in: &ConvolveKernelInput,
    scale: f64,
    region: Option<SubimageRegion<'_>>,
    mask: &str,
    stretch: bool,
    outfile: &str,
    path: Option<&Path>,
) -> ImageToolResult<(Storage<T>, Option<RecordValue>, usize)> {
    let data;
    let (kernel, rank) = if let Some(native) = native {
        (ConvolutionKernel::Image(native), native.ndim())
    } else {
        let ConvolveKernelInput::Array(pixels) = kernel_in else {
            unreachable!("validated convolution kernel");
        };
        data = pixels.mapv(T::from_f64);
        (ConvolutionKernel::Array(&data), data.ndim())
    };
    let plan = ImageConvolution::new(source, coordinates, region)?;
    let mut output = Storage::create(
        plan.section.shape.clone(),
        plan.section.coordinates.clone(),
        path,
        None,
    )?;
    let options = ConvolutionOptions {
        scale,
        mask: Some(mask),
        stretch,
        destination: (!outfile.is_empty()).then(|| Path::new(outfile)),
    };
    with_storage!(&mut output, dest => plan.copy_to(source, kernel, options, dest.as_mut()))?;
    Ok((output, plan.section.coordinate_record, rank))
}

/// 2D Gaussian/elliptical beam convolution (`ia.convolve2d`).
#[allow(clippy::too_many_arguments)]
pub fn convolve_2d_gaussian(
    attached: &AttachedImage,
    outfile: &str,
    region: Option<SubimageRegion<'_>>,
    mask: &str,
    geometry: GaussianConvolutionOptions,
    scale: f64,
    overwrite: bool,
    stretch: bool,
    history_command: Option<String>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<AttachedImage> {
    let mut live_coordinates = None;
    let mut product = publish_image(outfile, overwrite, is_external_open, |path| {
        read_locked(attached, |source| {
            if !matches!(source, ImageHandle::Float(_) | ImageHandle::Double(_)) {
                return Err(ImageToolError::new(
                    "convolve2d only supports real-valued images",
                ));
            }
            if !outfile.is_empty() {
                check_handle_aliasing(source, attached, Path::new(outfile), None)?;
            }
            let coordinates = source.coordinate_record();
            let beams = attached.beam_view().clone();
            let (mut output, updated) = match source {
                ImageHandle::Float(storage) => {
                    let (product, updated) = storage.with_float_view(|img| {
                        convolve2d_typed(
                            img,
                            coordinates,
                            beams.record(),
                            region,
                            mask,
                            geometry,
                            scale,
                            stretch,
                            outfile,
                            path,
                        )
                    })?;
                    (ImageHandle::Float(product), updated)
                }
                ImageHandle::Double(storage) => {
                    let (product, updated) = with_storage!(storage, img => {
                        convolve2d_typed(
                            img.as_ref(),
                            coordinates,
                            beams.record(),
                            region,
                            mask,
                            geometry,
                            scale,
                            stretch,
                            outfile,
                            path,
                        )
                    })?;
                    (ImageHandle::Double(product), updated)
                }
                _ => unreachable!("checked real image precision"),
            };
            if let Some(record) = updated {
                output.set_coordinate_record(record)?;
            }
            let source_name = source
                .path()
                .map_or_else(|| "Temporary_Image".to_owned(), |p| p.display().to_string());
            let lines = [
                beam_history(&format!("Original {source_name}"), Some(beams.record()))?,
                beam_history(&format!("Output {outfile}"), image_info_record(&output))?,
            ];
            let entries = source.history_entries()?;
            finalize_output_history(
                &mut output,
                &entries,
                "convolve2d",
                history_command,
                Some(("Image2DConvolver::_dealWithRestoringBeam", &lines)),
            )?;
            live_coordinates = AttachedImage::capture_live_coordinates(&output)?;
            Ok(output)
        })
    })?;
    if let Some((record, system)) = live_coordinates {
        product.keep_live_coordinates(record, system);
    }
    Ok(product)
}

#[allow(clippy::too_many_arguments)]
fn convolve2d_typed<T: ConvolutionPixel>(
    input: &dyn ImageInterface<T>,
    coordinates: RecordValue,
    beams: &RecordValue,
    region: Option<SubimageRegion<'_>>,
    mask: &str,
    geometry: GaussianConvolutionOptions,
    scale: f64,
    stretch: bool,
    outfile: &str,
    destination: Option<&Path>,
) -> ImageToolResult<(Storage<T>, Option<RecordValue>)> {
    let plan = ImageGaussianConvolution::new(input, coordinates, beams, region, geometry)?;
    let options = ConvolutionOptions {
        scale,
        mask: Some(mask),
        stretch,
        destination: (!outfile.is_empty()).then(|| Path::new(outfile)),
    };
    let section = plan.section();
    let mut result = Storage::create(
        section.shape.clone(),
        section.coordinates.clone(),
        destination,
        None,
    )?;
    with_storage!(&mut result, image => plan.copy_to(input, options, image.as_mut()))?;
    with_storage!(&mut result, image => image.set_image_info_record(plan.image_info_record().clone()))?;
    Ok((result, section.coordinate_record.clone()))
}

fn beam_history(description: &str, record: Option<&RecordValue>) -> ImageToolResult<String> {
    let Some(info) = record else {
        return Ok(format!("{description} has no beam"));
    };
    let beams = ImageInfo::from_record(info)?.beam_set;
    let format_beam = |beam: GaussianBeam| -> ImageToolResult<String> {
        let mut stored = None;
        for pol in 0..beams.n_stokes() {
            for channel in 0..beams.n_channels() {
                if *beams.beam(channel, pol) == beam {
                    stored = Some(stored_beam_record(Some(info), &beams, channel, pol));
                    break;
                }
            }
        }
        let rec = stored.unwrap_or_else(|| beam.to_record());
        let s = StoredGaussianBeam::from_record(&rec)?;
        Ok([
            BeamComponent::Major,
            BeamComponent::Minor,
            BeamComponent::PositionAngle,
        ]
        .into_iter()
        .zip(["major", "minor", "pa"])
        .map(|(component, name)| {
            let q = s.component(component);
            format!("{name}: {} {}", stream_double(q.value()), q.unit().name())
        })
        .collect::<Vec<_>>()
        .join(", "))
    };
    if beams.is_multi() {
        let min = format_beam(beams.min_area_beam().copied().unwrap_or_default())?;
        let max = format_beam(beams.max_area_beam().copied().unwrap_or_default())?;
        let median = format_beam(beams.median_area_beam().unwrap_or_default())?;
        Ok(format!(
            "{description} has multiple beams. Min area beam: {min}. Max area beam: {max}. Median area beam {median}"
        ))
    } else if let Some(beam) = beams.single_beam().filter(|beam| !beam.is_null()) {
        Ok(format!("{description} resolution {}", format_beam(beam)?))
    } else {
        Ok(format!("{description} has no beam"))
    }
}

/// Computes FFT of `attached` and writes requested `[real, imag, amp, phase, complex]` outputs (`ia.fft`).
#[allow(clippy::too_many_arguments)]
pub fn fft_image(
    attached: &AttachedImage,
    outfiles: [&str; 5],
    axes: &[i64],
    region: Option<SubimageRegion<'_>>,
    mask: &str,
    stretch: bool,
    history_command: Option<String>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<bool> {
    let destinations: Vec<_> = outfiles
        .iter()
        .filter(|name| !name.is_empty())
        .map(Path::new)
        .collect();
    if destinations.is_empty() {
        return Err(ImageToolError::new(
            "No output image names have been specified",
        ));
    }
    publish_image_set(outfiles, false, is_external_open, |staged| {
        read_locked(attached, |source| {
            for target in &destinations {
                check_handle_aliasing(source, attached, target, None)?;
            }
            if let ImageHandle::Float(storage) = source {
                storage.with_float_view(|img| {
                    fft_compute(
                        attached,
                        source,
                        img,
                        axes,
                        region,
                        mask,
                        stretch,
                        &destinations,
                        staged,
                        history_command,
                    )
                })
            } else {
                with_pixels!(source, pixels => with_storage!(pixels, img => {
                    fft_compute(
                        attached,
                        source,
                        img.as_ref(),
                        axes,
                        region,
                        mask,
                        stretch,
                        &destinations,
                        staged,
                        history_command,
                    )
                }))
            }
        })
    })?;
    Ok(true)
}

trait FftProductPixel: ImagePixel {
    fn wrap_fft(image: Storage<Self>) -> ImageHandle;
}

impl FftProductPixel for f32 {
    fn wrap_fft(image: Storage<Self>) -> ImageHandle {
        ImageHandle::Float(image)
    }
}
impl FftProductPixel for f64 {
    fn wrap_fft(image: Storage<Self>) -> ImageHandle {
        ImageHandle::Double(image)
    }
}
impl FftProductPixel for Complex32 {
    fn wrap_fft(image: Storage<Self>) -> ImageHandle {
        ImageHandle::Complex(image)
    }
}
impl FftProductPixel for Complex64 {
    fn wrap_fft(image: Storage<Self>) -> ImageHandle {
        ImageHandle::DComplex(image)
    }
}

fn create_fft_product<T: ImagePixel>(
    plan: &ImageFourier<'_>,
    path: &Path,
) -> ImageToolResult<Storage<T>> {
    let shape = plan.shape().to_vec();
    let tiled = TiledShape::new(shape.clone(), std::mem::size_of::<T>())?;
    PagedImage::create_with_tile_shape_and_cache(
        shape,
        tiled.tile_shape().to_vec(),
        plan.coordinates().clone(),
        path,
        16 * 1024 * 1024,
    )
    .map(|image| Storage::Paged(Box::new(image)))
    .map_err(ImageToolError::from)
}

#[allow(clippy::too_many_arguments)]
fn fft_compute<T: FourierPixel>(
    attachment: &AttachedImage,
    source: &ImageHandle,
    input: &dyn ImageInterface<T>,
    axes: &[i64],
    region: Option<SubimageRegion<'_>>,
    mask: &str,
    stretch: bool,
    destinations: &[&Path],
    staged: [Option<&Path>; 5],
    history_command: Option<String>,
) -> ImageToolResult<()>
where
    T::Scalar: FftProductPixel,
    Complex<T::Scalar>: FftProductPixel,
{
    let beams = attachment.beam_view();
    let plan = ImageFourier::new(
        input,
        source.coordinate_record(),
        beams.record(),
        FourierOptions {
            region,
            axes,
            mask: Some(mask),
            stretch,
            destinations,
        },
    )?;
    let section = plan.section();
    let original = beams.beam_set()?;
    let channels = section.beam_planes(
        input.find_axis(CoordinateType::Spectral),
        original.n_channels(),
    );
    let polarizations =
        section.beam_planes(input.find_axis(CoordinateType::Stokes), original.n_stokes());
    let base_info = section.image_info().to_record();
    let scratch = plan.transform(input)?;
    type ExtractScalar<S> = fn(Complex<S>) -> S;
    let components: [ExtractScalar<T::Scalar>; 4] = [|v| v.re, |v| v.im, T::amplitude, T::phase];
    let history = source.history_entries()?;
    for (index, component) in components.into_iter().enumerate() {
        if let Some(path) = staged[index] {
            let mut output = create_fft_product(&plan, path)?;
            with_storage!(&mut output, img => {
                plan.write_product(input, &scratch, img.as_mut(), index == 3, component)
            })?;
            apply_beam_subset(
                &mut output,
                base_info.clone(),
                &beams,
                &channels,
                &polarizations,
            )?;
            let mut handle = T::Scalar::wrap_fft(output);
            handle.set_coordinate_record(plan.coordinate_record().clone())?;
            finalize_output_history(&mut handle, &history, "fft", history_command.clone(), None)?;
        }
    }
    if let Some(path) = staged[4] {
        let mut output = create_fft_product(&plan, path)?;
        with_storage!(&mut output, img => {
            plan.write_product(input, &scratch, img.as_mut(), false, |v| v)
        })?;
        apply_beam_subset(&mut output, base_info, &beams, &channels, &polarizations)?;
        let mut handle = Complex::<T::Scalar>::wrap_fft(output);
        handle.set_coordinate_record(plan.coordinate_record().clone())?;
        finalize_output_history(&mut handle, &history, "fft", history_command, None)?;
    }
    Ok(())
}

/// Separable axis-by-axis convolution (`ia.sepconvolve`).
pub fn sepconvolve_image(
    attached: &AttachedImage,
    outfile: &str,
    overwrite: bool,
    options: SepConvolutionOptions<'_>,
    history_command: Option<String>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<AttachedImage> {
    let destination = options.destination;
    let region = options.region;
    publish_image(outfile, overwrite, is_external_open, |stage| {
        read_locked(attached, |handle| {
            handle.check_single_precision("sepconvolve")?;
            let ImageHandle::Float(source) = handle else {
                return Err(ImageToolError::new("Exception: image cannot be NULL."));
            };
            if let Some(dest) = destination {
                check_handle_aliasing(handle, attached, dest, None)?;
                check_region_overlap(outfile, region)?;
            }
            let beams = attached.beam_view().clone();
            let coord_rec = handle.coordinate_record();
            let mut output = source.with_float_view(|image| {
                sepconvolve_float(image, coord_rec, &beams, options, stage)
            })?;
            let entries = handle.history_entries()?;
            finalize_output_history(&mut output, &entries, "sepconvolve", history_command, None)?;
            Ok(output)
        })
    })
}

fn sepconvolve_float(
    image: &dyn ImageInterface<f32>,
    record: RecordValue,
    beams: &BeamView,
    options: SepConvolutionOptions<'_>,
    stage: Option<&Path>,
) -> ImageToolResult<ImageHandle> {
    let mut plan = ImageSepConvolution::new(image, record, beams.record(), options)?;
    let beam_set = beams.beam_set()?;
    let channels = plan.section.beam_planes(
        image.find_axis(CoordinateType::Spectral),
        beam_set.n_channels(),
    );
    let polarizations = plan
        .section
        .beam_planes(image.find_axis(CoordinateType::Stokes), beam_set.n_stokes());
    let mut output = Storage::create(
        plan.section.shape.clone(),
        plan.section.coordinates.clone(),
        stage,
        None,
    )?;
    with_storage!(&mut output, product => plan.copy_to(image, product.as_mut()))?;
    apply_beam_subset(
        &mut output,
        plan.section.image_info().to_record(),
        beams,
        &channels,
        &polarizations,
    )?;
    set_storage_coordinate_record(&mut output, plan.section.coordinate_record.take())?;
    Ok(ImageHandle::Float(output))
}

/// 1D Hanning or Boxcar smoothing along an axis (`ia.hanning`, `ia.boxcar`).
#[allow(clippy::too_many_arguments)]
pub fn smooth_axis(
    attached: &AttachedImage,
    outfile: &str,
    overwrite: bool,
    options: SmoothingOptions<'_>,
    history_fn: Option<&dyn Fn(usize) -> String>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<AttachedImage> {
    let kernel = options.kernel;
    let method_name = match kernel {
        SmoothingKernel::Boxcar(_) => "boxcar",
        SmoothingKernel::Hanning => "hanning",
    };
    let mut live_coordinates = None;
    let mut product = publish_image(outfile, overwrite, is_external_open, |path| {
        read_locked(attached, |image| {
            image.check_single_precision(method_name)?;
            if !outfile.is_empty() {
                check_handle_aliasing(image, attached, Path::new(outfile), None)?;
            }
            let beams = attached.beam_view().clone();
            let record = image.coordinate_record();
            let (mut output, axis) = match image {
                ImageHandle::Float(storage) => storage.with_float_view(|img| {
                    let (st, ax) = smooth_typed(img, record, &beams, options, path)?;
                    Ok::<_, ImageToolError>((ImageHandle::Float(st), ax))
                })?,
                ImageHandle::Complex(storage) => with_storage!(storage, img => {
                    let (st, ax) = smooth_typed(img.as_ref(), record, &beams, options, path)?;
                    Ok::<_, ImageToolError>((ImageHandle::Complex(st), ax))
                })?,
                _ => unreachable!("checked single precision"),
            };
            let rows = image.history_entries()?;
            output.append_history_entries(&rows)?;
            if let Some(build_cmd) = history_fn {
                let cmd = build_cmd(axis);
                match kernel {
                    SmoothingKernel::Boxcar(_) => output.record_history(method_name, cmd)?,
                    SmoothingKernel::Hanning => {
                        output.append_history_at(
                            "::image::_hanning",
                            &["Ran ia.hanning".into(), cmd],
                        )?;
                    }
                }
            }
            let source_name = image.path().map_or_else(
                || "Temporary_Image".into(),
                |name| name.display().to_string(),
            );
            let origin = match kernel {
                SmoothingKernel::Boxcar(_) => "::ImageBoxcarSmoother::_reportOldNewImageShapes",
                SmoothingKernel::Hanning => "::HanningSmoother::_reportOldNewImageShapes",
            };
            output.append_history_at(
                origin,
                &[
                    format!("Original {source_name} size => {:?}", image.shape()),
                    format!("New {outfile} size => {:?}", output.shape()),
                ],
            )?;
            output.save()?;
            live_coordinates = AttachedImage::capture_live_coordinates(&output)?;
            Ok(output)
        })
    })?;
    if let Some((record, system)) = live_coordinates {
        product.keep_live_coordinates(record, system);
    }
    Ok(product)
}

fn smooth_typed<T: SmoothingPixel>(
    image: &dyn ImageInterface<T>,
    coordinates: RecordValue,
    beams: &BeamView,
    options: SmoothingOptions<'_>,
    path: Option<&Path>,
) -> ImageToolResult<(Storage<T>, usize)> {
    let mut plan = ImageSmoothing::new(image, coordinates, options, beams.record())?;
    let mut output = Storage::create(
        plan.section.shape.clone(),
        plan.section.coordinates.clone(),
        path,
        None,
    )?;
    with_storage!(&mut output, product => plan.copy_to(image, product.as_mut()))?;
    let mut info = plan.section.image_info().to_record();
    beams.apply_to(&mut info);
    with_storage!(&mut output, product => product.set_image_info_record(info))?;
    set_storage_coordinate_record(&mut output, plan.section.coordinate_record.take())?;
    Ok((output, plan.axis()))
}
