// SPDX-License-Identifier: LGPL-3.0-or-later

//! Spatial/dimension manipulation ops: `crop`, `decimate`, `pad`, `pbcor`, `rebin`, `makecomplex`, `pv`.

use std::path::{Path, PathBuf};

use casa_coordinates::CoordinateType;
use casa_core::{RecordValue, Value};
use casa_images::{ImageExprValue, ImageInterface, ImagePixel};
use ndarray::ArrayD as NdArrayD;
use num_complex::Complex;

use crate::analysis::imsubimage::{
    DecimationMethod, DecimationPixel, ImageCrop, ImageCropOptions, ImageDecimation,
    ImageDecimationOptions, ImagePad, ImagePadOptions, ImagePbcor, ImagePbcorBeam,
    ImagePbcorOptions, ImageRebin, ImageRebinOptions, SubimageOptions, SubimagePlan,
    SubimageRegion, ensure_artifacts_do_not_overlap,
};
use crate::analysis::{ImpvRequest, generate_image_pv};

use super::attachment::{AttachedImage, ExternalPathCheck, read_locked};
use super::beam_view::BeamView;
use super::beams::{
    image_info, image_info_record, plane_axes, stored_beam_record, subset_beam_record,
};
use super::conformance::coordinates_conform;
use super::error::{ImageToolError, ImageToolResult, ToolLogLine};
use super::handle::{ImageHandle, Storage, with_storage};
use super::product_helpers::{
    apply_beam_subset, check_handle_aliasing, check_output_overwrite, check_region_overlap,
    finalize_output_history, set_product_mask, set_storage_coordinate_record,
};
use super::publish::publish_image;

fn display_source_name(handle: &ImageHandle) -> String {
    handle.path().map_or_else(
        || "Temporary_Image".into(),
        |path| path.display().to_string(),
    )
}

/// Crops masked border planes (`ia.crop`).
#[allow(clippy::too_many_arguments)]
pub fn crop_image(
    attached: &AttachedImage,
    outfile: &str,
    overwrite: bool,
    options: ImageCropOptions<'_>,
    wantreturn: bool,
    history_command: Option<String>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<(Option<AttachedImage>, Vec<ToolLogLine>)> {
    if !matches!(&*attached.lock(), ImageHandle::Float(_)) {
        return Err(ImageToolError::new(
            "This method only supports Float valued images",
        ));
    }
    check_output_overwrite(outfile, overwrite)?;
    let region = options.region;
    let output = publish_image(outfile, overwrite, is_external_open, |dest| {
        read_locked(attached, |input| {
            let ImageHandle::Float(source) = input else {
                unreachable!("checked float pixels");
            };
            if !outfile.is_empty() {
                check_handle_aliasing(input, attached, Path::new(outfile), None)?;
                check_region_overlap(outfile, region)?;
            }
            let beams = attached.beam_view().clone();
            let coordinates = input.coordinate_record();
            let history = input.history_entries()?;
            let mut out_storage = source
                .with_float_view(|image| crop_float(image, coordinates, options, &beams, dest))?;
            with_storage!(&mut out_storage, img => img.append_history_entries(&history))?;
            let mut out = ImageHandle::Float(out_storage);
            let source_name = display_source_name(input);
            let lines = [
                format!("Original {source_name} size => {:?}", input.shape()),
                format!("New {outfile} size => {:?}", out.shape()),
            ];
            finalize_output_history(
                &mut out,
                &[],
                "crop",
                history_command,
                Some(("::ImageCropper::_reportOldNewImageShapes", &lines)),
            )?;
            Ok(out)
        })
    })?;
    let mut logs = Vec::new();
    if !wantreturn && outfile.trim().is_empty() {
        logs.push(ToolLogLine::warning(
            "outfile was not specified and wantreturn is false. The resulting image will be inaccessible",
        ));
    }
    Ok((wantreturn.then(|| AttachedImage::from_handle(output)), logs))
}

fn crop_float(
    image: &dyn ImageInterface<f32>,
    coordinates: RecordValue,
    options: ImageCropOptions<'_>,
    beams: &BeamView,
    dest: Option<&Path>,
) -> ImageToolResult<Storage<f32>> {
    let mut crop = ImageCrop::new(image, Some(coordinates), options, Some(beams.record()))?;
    let record = crop.section.coordinate_record.take();
    let info = crop.section.image_info().to_record();
    let beam_set = beams.beam_set()?;
    let channels = crop.beam_planes(
        image.find_axis(CoordinateType::Spectral),
        beam_set.n_channels(),
    );
    let stokes = crop.beam_planes(image.find_axis(CoordinateType::Stokes), beam_set.n_stokes());
    let mut product = Storage::create(
        crop.section.shape.clone(),
        crop.section.coordinates.clone(),
        dest,
        None,
    )?;
    with_storage!(&mut product, image_out => crop.copy_to(image, image_out.as_mut()))?;
    apply_beam_subset(&mut product, info, beams, &channels, &stokes)?;
    set_storage_coordinate_record(&mut product, record)?;
    Ok(product)
}

/// Decimates an image along one axis (`ia.decimate`).
#[allow(clippy::too_many_arguments)]
pub fn decimate_image(
    attached: &AttachedImage,
    outfile: &str,
    overwrite: bool,
    axis: usize,
    factor: usize,
    method: DecimationMethod,
    region: Option<SubimageRegion<'_>>,
    mask: Option<&str>,
    stretch: bool,
    history_command: Option<String>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<AttachedImage> {
    let output = publish_image(outfile, overwrite, is_external_open, |dest| {
        read_locked(attached, |handle| {
            if matches!(handle, ImageHandle::Double(_) | ImageHandle::DComplex(_)) {
                return Err(ImageToolError::new(
                    "decimate does not support images with double precision pixel values",
                ));
            }
            if !outfile.is_empty() {
                check_handle_aliasing(handle, attached, Path::new(outfile), None)?;
            }
            let beams = attached.beam_view().clone();
            let coordinates = handle.coordinate_record();
            let options = ImageDecimationOptions {
                axis,
                factor,
                method,
                region,
                mask,
                stretch,
            };
            let mut out = match handle {
                ImageHandle::Float(source) => {
                    ImageHandle::Float(source.with_float_view(|img| {
                        decimate_typed(img, coordinates, options, &beams, dest)
                    })?)
                }
                ImageHandle::Complex(source) => {
                    ImageHandle::Complex(with_storage!(source, img => {
                        decimate_typed(img.as_ref(), coordinates, options, &beams, dest)
                    })?)
                }
                _ => unreachable!("double precision is checked before decimation"),
            };
            let history = handle.history_entries()?;
            let extra_lines;
            let extra = if factor > 1 {
                let operation = match method {
                    DecimationMethod::Copy => {
                        "Directly copying every i*nth plane in input to plane i in output."
                    }
                    DecimationMethod::Mean => {
                        "Averaging every i to i*(n-1) planes in the input image to form plane i in the output image."
                    }
                };
                let source = display_source_name(handle);
                extra_lines = [
                    format!(
                        "Decimated axis {axis} by keeping only every nth plane, where n={factor}. {operation}"
                    ),
                    format!("Original {source} size => {:?}", handle.shape()),
                    format!("New {outfile} size => {:?}", out.shape()),
                ];
                Some(("::ImageDecimator::decimate", extra_lines.as_slice()))
            } else {
                None
            };
            finalize_output_history(&mut out, &history, "decimate", history_command, extra)?;
            Ok(out)
        })
    })?;
    Ok(AttachedImage::from_handle(output))
}

fn decimate_typed<T: DecimationPixel>(
    view: &dyn ImageInterface<T>,
    coordinates: RecordValue,
    options: ImageDecimationOptions<'_>,
    beams: &BeamView,
    dest: Option<&Path>,
) -> ImageToolResult<Storage<T>> {
    let plan = ImageDecimation::new(view, coordinates, options, beams.record())?;
    let sec = plan.section();
    let mut product = Storage::create(sec.shape.clone(), sec.coordinates.clone(), dest, None)?;
    let mask = with_storage!(&mut product, output => plan.copy_to(view, output.as_mut()))?;
    if let Some(mask) = mask {
        set_product_mask(&mut product, mask)?;
    }
    let mut info = sec.image_info().to_record();
    beams.apply_to(&mut info);
    with_storage!(&mut product, output => output.set_image_info_record(info))?;
    set_storage_coordinate_record(&mut product, sec.coordinate_record.clone())?;
    Ok(product)
}

/// Pads direction borders of an image (`ia.pad`).
#[allow(clippy::too_many_arguments)]
pub fn pad_image(
    attached: &AttachedImage,
    outfile: &str,
    overwrite: bool,
    options: ImagePadOptions<'_>,
    wantreturn: bool,
    history_command: Option<String>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<Option<AttachedImage>> {
    if !matches!(&*attached.lock(), ImageHandle::Float(_)) {
        return Err(ImageToolError::new(
            "This method only supports Float valued images",
        ));
    }
    check_output_overwrite(outfile, overwrite)?;
    let output = publish_image(outfile, overwrite, is_external_open, |dest| {
        read_locked(attached, |input| {
            let ImageHandle::Float(source) = input else {
                unreachable!("checked float pixels");
            };
            if !outfile.is_empty() {
                check_handle_aliasing(input, attached, Path::new(outfile), None)?;
            }
            let beams = attached.beam_view().clone();
            let coordinates = input.coordinate_record();
            let history = input.history_entries()?;
            let mut out_storage = source
                .with_float_view(|image| pad_float(image, coordinates, options, &beams, dest))?;
            with_storage!(&mut out_storage, img => img.append_history_entries(&history))?;
            let mut out = ImageHandle::Float(out_storage);
            let source_name = display_source_name(input);
            let lines = [
                format!("Original {source_name} size => {:?}", input.shape()),
                format!("New {outfile} size => {:?}", out.shape()),
            ];
            finalize_output_history(
                &mut out,
                &[],
                "pad",
                history_command,
                Some(("::ImagePadder::_reportOldNewImageShapes", &lines)),
            )?;
            Ok(out)
        })
    })?;
    Ok(wantreturn.then(|| AttachedImage::from_handle(output)))
}

fn pad_float(
    image: &dyn ImageInterface<f32>,
    coordinates: RecordValue,
    options: ImagePadOptions<'_>,
    beams: &BeamView,
    dest: Option<&Path>,
) -> ImageToolResult<Storage<f32>> {
    let mut pad = ImagePad::new(image, coordinates, options, beams.record())?;
    let beam_set = beams.beam_set()?;
    let channels = pad.beam_planes(
        image.find_axis(CoordinateType::Spectral),
        beam_set.n_channels(),
    );
    let stokes = pad.beam_planes(image.find_axis(CoordinateType::Stokes), beam_set.n_stokes());
    let record = pad.section.coordinate_record.take();
    let mut output = Storage::create(
        pad.section.shape.clone(),
        pad.section.coordinates.clone(),
        dest,
        None,
    )?;
    with_storage!(&mut output, dest_img => pad.copy_to(image, dest_img.as_mut()))?;
    apply_beam_subset(
        &mut output,
        pad.section.image_info().to_record(),
        beams,
        &channels,
        &stokes,
    )?;
    set_storage_coordinate_record(&mut output, record)?;
    Ok(output)
}

/// Primary-beam correction input (`ia.pbcor`).
pub enum PbcorInput {
    /// Path to a primary beam image file.
    ImagePath(PathBuf),
    /// In-memory primary beam array.
    Array {
        /// Primary beam pixel values.
        data: NdArrayD<f32>,
        /// Primary beam shape.
        shape: Vec<usize>,
    },
}

/// Applies primary-beam division or multiplication (`ia.pbcor`).
#[allow(clippy::too_many_arguments)]
pub fn pbcor_image(
    attached: &AttachedImage,
    pb: &PbcorInput,
    outfile: &str,
    overwrite: bool,
    options: ImagePbcorOptions<'_>,
    history_command: Option<String>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<AttachedImage> {
    if !matches!(&*attached.lock(), ImageHandle::Float(_)) {
        return Err(ImageToolError::new(
            "This method only supports Float valued images",
        ));
    }
    let beam_path = match pb {
        PbcorInput::ImagePath(path) if path.as_os_str().is_empty() => {
            return Err(ImageToolError::new("The image filename is empty"));
        }
        PbcorInput::ImagePath(path) => Some(path.as_path()),
        PbcorInput::Array { .. } => None,
    };
    let same_native = beam_path
        .and_then(|path| path.canonicalize().ok())
        .is_some_and(|path| attached.native_path() == Some(&path));
    let beam_attachment = beam_path
        .filter(|_| !same_native)
        .map(|path| {
            AttachedImage::open_for_ordered_read(path).map_err(|error| {
                ImageToolError::new(format!(
                    "Unable to open primary beam image {}: {error}",
                    path.display()
                ))
            })
        })
        .transpose()?;
    let handle = publish_image(outfile, overwrite, is_external_open, |dest| {
        if let Some(beam) = &beam_attachment {
            let source_first = attached
                .native_path()
                .zip(beam.native_path())
                .is_none_or(|(s, b)| s < b);
            if source_first {
                read_locked(attached, |img| {
                    read_locked(beam, |beam_img| {
                        pbcor_stage(
                            attached,
                            img,
                            Some(beam_img),
                            pb,
                            outfile,
                            options,
                            dest,
                            history_command,
                        )
                    })
                })
            } else {
                read_locked(beam, |beam_img| {
                    read_locked(attached, |img| {
                        pbcor_stage(
                            attached,
                            img,
                            Some(beam_img),
                            pb,
                            outfile,
                            options,
                            dest,
                            history_command,
                        )
                    })
                })
            }
        } else {
            read_locked(attached, |img| {
                pbcor_stage(
                    attached,
                    img,
                    same_native.then_some(img),
                    pb,
                    outfile,
                    options,
                    dest,
                    history_command,
                )
            })
        }
    })?;
    Ok(AttachedImage::from_handle(handle))
}

#[allow(clippy::too_many_arguments)]
fn pbcor_stage(
    source: &AttachedImage,
    image: &ImageHandle,
    beam: Option<&ImageHandle>,
    pb: &PbcorInput,
    outfile: &str,
    options: ImagePbcorOptions<'_>,
    dest: Option<&Path>,
    history_command: Option<String>,
) -> ImageToolResult<ImageHandle> {
    if !outfile.is_empty() {
        check_handle_aliasing(image, source, Path::new(outfile), beam)?;
    }
    let beams = source.beam_view().clone();
    let coordinates = image.coordinate_record();
    let ImageHandle::Float(source_storage) = image else {
        return Err(ImageToolError::new(
            "This method only supports Float valued images",
        ));
    };
    let mut output = source_storage.with_float_view(|pixels| {
        if let Some(beam_handle) = beam {
            let ImageHandle::Float(beam_storage) = beam_handle else {
                return Err(ImageToolError::new(
                    "This method only supports Float valued images",
                ));
            };
            beam_storage.with_float_view(|beam_pixels| {
                pbcor_float(
                    pixels,
                    coordinates,
                    ImagePbcorBeam::Image(beam_pixels),
                    options,
                    &beams,
                    dest,
                )
            })
        } else {
            let PbcorInput::Array { data, shape } = pb else {
                unreachable!("resolved pb array");
            };
            pbcor_float(
                pixels,
                coordinates,
                ImagePbcorBeam::Array {
                    pixels: data,
                    shape,
                },
                options,
                &beams,
                dest,
            )
        }
    })?;
    if let Some(beam_handle) = beam {
        for operand in [image, beam_handle] {
            let name = display_source_name(operand);
            output.append_history_at("::image::pbcor", &[format!("History of image {name}")])?;
            let entries = operand.history_entries()?;
            output.append_history_entries(&entries)?;
            output
                .append_history_at("::image::pbcor", &[format!("End history of image {name}")])?;
        }
    } else {
        let entries = image.history_entries()?;
        output.append_history_entries(&entries)?;
        if let Some(cmd) = history_command {
            output.record_history("pbcor", cmd)?;
        }
    }
    output.save()?;
    Ok(output)
}

fn pbcor_float(
    image: &dyn ImageInterface<f32>,
    coordinates: RecordValue,
    beam: ImagePbcorBeam<'_>,
    options: ImagePbcorOptions<'_>,
    beams: &BeamView,
    dest: Option<&Path>,
) -> ImageToolResult<ImageHandle> {
    let mut correction = ImagePbcor::new(image, coordinates, beam, options, beams.record())?;
    let source_beams = beams.beam_set()?;
    let channels = correction.beam_planes(
        image.find_axis(CoordinateType::Spectral),
        source_beams.n_channels(),
    );
    let polarizations = correction.beam_planes(
        image.find_axis(CoordinateType::Stokes),
        source_beams.n_stokes(),
    );
    let mut product = Storage::create(
        correction.section.shape.clone(),
        correction.section.coordinates.clone(),
        dest,
        None,
    )?;
    with_storage!(&mut product, dest_img => correction.copy_to(image, dest_img.as_mut()))?;
    apply_beam_subset(
        &mut product,
        correction.section.image_info().to_record(),
        beams,
        &channels,
        &polarizations,
    )?;
    set_storage_coordinate_record(&mut product, correction.section.coordinate_record.take())?;
    Ok(ImageHandle::Float(product))
}

/// Rebins an image by integer factors along axes (`ia.rebin`).
pub fn rebin_image(
    attached: &AttachedImage,
    outfile: &str,
    overwrite: bool,
    options: ImageRebinOptions<'_>,
    history_command: Option<String>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<AttachedImage> {
    let mut live_coordinates = None;
    let handle = publish_image(outfile, overwrite, is_external_open, |dest| {
        read_locked(attached, |handle| {
            match handle {
                ImageHandle::Double(_) => {
                    return Err(ImageToolError::new(
                        "rebin does not support images with double precision pixel values.",
                    ));
                }
                ImageHandle::DComplex(_) => {
                    return Err(ImageToolError::new(
                        "rebin does not support images with complex<double> precision pixel values.",
                    ));
                }
                _ => {}
            }
            if !outfile.is_empty() {
                check_handle_aliasing(handle, attached, Path::new(outfile), None)?;
            }
            let beams = attached.beam_view().clone();
            let coordinates = handle.coordinate_record();
            let mut out = match handle {
                ImageHandle::Float(source) => {
                    ImageHandle::Float(source.with_float_view(|img| {
                        rebin_typed(img, coordinates, options, &beams, dest)
                    })?)
                }
                ImageHandle::Complex(source) => {
                    ImageHandle::Complex(with_storage!(source, img => {
                        rebin_typed(img.as_ref(), coordinates, options, &beams, dest)
                    })?)
                }
                _ => unreachable!("double precision checked before rebin"),
            };
            let history = handle.history_entries()?;
            finalize_output_history(&mut out, &history, "rebin", history_command, None)?;
            if dest.is_some() {
                live_coordinates = AttachedImage::capture_live_coordinates(&out)?;
            }
            Ok(out)
        })
    })?;
    let mut result = AttachedImage::from_handle(handle);
    if let Some((record, system)) = live_coordinates {
        result.keep_live_coordinates(record, system);
    }
    Ok(result)
}

fn rebin_typed<T: ImageExprValue>(
    view: &dyn ImageInterface<T>,
    coordinates: RecordValue,
    options: ImageRebinOptions<'_>,
    beams: &BeamView,
    dest: Option<&Path>,
) -> ImageToolResult<Storage<T>> {
    let rebin = ImageRebin::new(view, coordinates, options, beams.record())?;
    let section = rebin.section();
    let source_beams = beams.beam_set()?;
    let channels = rebin.beam_planes(
        view.find_axis(CoordinateType::Spectral),
        source_beams.n_channels(),
    );
    let stokes = rebin.beam_planes(
        view.find_axis(CoordinateType::Stokes),
        source_beams.n_stokes(),
    );
    let mut product = Storage::create(
        section.shape.clone(),
        section.coordinates.clone(),
        dest,
        None,
    )?;
    with_storage!(&mut product, output => rebin.copy_to(view, output.as_mut()))?;
    apply_beam_subset(
        &mut product,
        section.image_info().to_record(),
        beams,
        &channels,
        &stokes,
    )?;
    set_storage_coordinate_record(&mut product, section.coordinate_record.clone())?;
    Ok(product)
}

/// Combines real (`attached`) and imaginary (`imag_path`) images into a complex image (`ia.makecomplex`).
pub fn make_complex_image(
    attached: &AttachedImage,
    outfile: &str,
    imag_path: &Path,
    region: Option<SubimageRegion<'_>>,
    overwrite: bool,
    history_command: Option<String>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<()> {
    let product = publish_image(outfile, overwrite, is_external_open, |dest| {
        let mut real = attached.lock();
        if !matches!(*real, ImageHandle::Float(_) | ImageHandle::Double(_)) {
            return Err(ImageToolError::new(
                "The attached image must be float valued",
            ));
        }
        attached.require_pixel_read_lock(&mut real)?;
        let imaginary = ImageHandle::open_read(imag_path)?;
        if !matches!(imaginary, ImageHandle::Float(_) | ImageHandle::Double(_)) {
            return Err(ImageToolError::new(format!(
                "{} does not have supported real valued pixels",
                imag_path.display()
            )));
        }
        if real.kind() != imaginary.kind() {
            return Err(ImageToolError::new(
                "Real and imaginary images do not have the same precision",
            ));
        }
        if !outfile.is_empty() {
            check_handle_aliasing(&real, attached, Path::new(outfile), Some(&imaginary))?;
        }
        if real.shape() != imaginary.shape() {
            return Err(ImageToolError::new("Image shapes are not identical"));
        }
        let real_record = real.coordinate_record();
        if !coordinates_conform(&real_record, &imaginary.coordinate_record(), false)? {
            return Err(ImageToolError::new(
                "Image Coordinate systems are not conformant",
            ));
        }
        let (mut output, origin) = match (&*real, &imaginary) {
            (ImageHandle::Float(left), ImageHandle::Float(right)) => left.with_float_view(|l| {
                right.with_float_view(|r| {
                    combine_complex(l, r, &real_record, region, dest, ImageHandle::Complex)
                })
            })?,
            (ImageHandle::Double(left), ImageHandle::Double(right)) => {
                with_storage!(left, l => with_storage!(right, r => {
                    combine_complex(l.as_ref(), r.as_ref(), &real_record, region, dest, ImageHandle::DComplex)
                }))?
            }
            _ => unreachable!("real images with matching precision"),
        };
        preserve_complex_beams(&real, &mut output, &real_record, &origin)?;
        let rows = real.history_entries()?;
        finalize_output_history(&mut output, &rows, "makecomplex", history_command, None)?;
        Ok(output)
    })?;
    drop(product);
    Ok(())
}

fn combine_complex<T>(
    real: &dyn ImageInterface<T>,
    imaginary: &dyn ImageInterface<T>,
    real_record: &RecordValue,
    region: Option<SubimageRegion<'_>>,
    dest: Option<&Path>,
    wrap: fn(Storage<Complex<T>>) -> ImageHandle,
) -> ImageToolResult<(ImageHandle, Vec<usize>)>
where
    T: ImagePixel + Copy,
    Complex<T>: ImagePixel + Copy,
{
    let plan = SubimagePlan::new(
        real,
        Some(real_record.clone()),
        SubimageOptions {
            region,
            dropdeg: false,
            keepaxes: &[],
        },
    )?;
    let mut storage = Storage::create(plan.shape.clone(), plan.coordinates.clone(), dest, None)?;
    with_storage!(&mut storage, output => plan.make_complex_to(real, imaginary, output.as_mut(), region))?;
    let origin = plan.bounds().0.to_vec();
    let mut result = wrap(storage);
    if let Some(record) = plan.coordinate_record {
        result.set_coordinate_record(record)?;
    }
    Ok((result, origin))
}

fn preserve_complex_beams(
    real: &ImageHandle,
    output: &mut ImageHandle,
    coordinates: &RecordValue,
    origin: &[usize],
) -> ImageToolResult<()> {
    let Some(stored) = image_info_record(real) else {
        return Ok(());
    };
    let source = image_info(real)?.beam_set;
    let result = image_info(output)?.beam_set;
    let mut record = stored.clone();
    record.remove("restoringbeam");
    record.remove("perplanebeams");
    let (spectral, stokes) = plane_axes(coordinates)?;
    let offset = |axis: Option<usize>, length: usize| {
        axis.and_then(|axis| origin.get(axis).copied())
            .filter(|_| length > 1)
            .unwrap_or(0)
    };
    let first_channel = offset(spectral, source.n_channels());
    let first_stokes = offset(stokes, source.n_stokes());
    let beam = |channel: usize, stokes: usize| {
        let channel = (first_channel + channel).min(source.n_channels().saturating_sub(1));
        let stokes = (first_stokes + stokes).min(source.n_stokes().saturating_sub(1));
        Value::Record(stored_beam_record(Some(stored), &source, channel, stokes))
    };
    if result.is_single() {
        record.upsert("restoringbeam", beam(0, 0));
    } else if result.is_multi() {
        let mut grid = result.to_record();
        for polarization in 0..result.n_stokes() {
            for channel in 0..result.n_channels() {
                let index = polarization * result.n_channels() + channel;
                grid.upsert(format!("*{index}"), beam(channel, polarization));
            }
        }
        record.upsert("perplanebeams", Value::Record(grid));
    }
    output.set_image_info_record(record)?;
    Ok(())
}

/// Generates a position-velocity slice image (`ia.pv`).
#[allow(clippy::too_many_arguments)]
pub fn pv_image(
    attached: &AttachedImage,
    request: &ImpvRequest,
    region: Option<SubimageRegion<'_>>,
    outfile: &str,
    overwrite: bool,
    wantreturn: bool,
    history_command: Option<String>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<(Option<AttachedImage>, Vec<ToolLogLine>)> {
    let trimmed = outfile.trim();
    let handle = publish_image(trimmed, overwrite, is_external_open, |stage| {
        read_locked(attached, |handle| {
            let ImageHandle::Float(storage) = handle else {
                return Err(ImageToolError::new(
                    "This method only supports Float valued images",
                ));
            };
            if stage.is_some() {
                check_handle_aliasing(handle, attached, &request.outfile, None)?;
                if let Some(ext) = region.and_then(SubimageRegion::external_path) {
                    ensure_artifacts_do_not_overlap(&request.outfile, ext)?;
                }
            }
            let view = attached.beam_view();
            let mut result = storage.with_float_view(|image| {
                let mut beams = image.image_info()?.to_record();
                view.apply_to(&mut beams);
                generate_image_pv(image, handle.coordinate_record(), &beams, request, region)
            })?;
            let source_beams = view.beam_set()?;
            let mut info = result.image.image_info()?.to_record();
            if source_beams.is_multi() {
                let [channels, polarizations] = &result.beam_planes;
                subset_beam_record(
                    &mut info,
                    Some(view.record()),
                    &source_beams,
                    channels,
                    polarizations,
                )?;
            } else {
                view.apply_to(&mut info);
            }
            result.image.set_image_info_record(info)?;
            let entries = handle.history_entries()?;
            result.image.append_history_entries(&entries)?;
            if let Some(cmd) = history_command {
                result
                    .image
                    .append_history_at("image::pv", &["Ran ia.pv".into(), cmd])?;
            }
            let record = result.coordinate_record;
            let output = if let Some(path) = stage {
                let mut image = result.image.save_as(path)?;
                image.set_coordinate_record(record)?;
                image.save()?;
                Storage::Paged(Box::new(image))
            } else {
                Storage::Temp {
                    image: Box::new(result.image),
                    fits: None,
                    record,
                }
            };
            Ok(ImageHandle::Float(output))
        })
    })?;
    let mut logs = Vec::new();
    if !wantreturn && trimmed.is_empty() {
        logs.push(ToolLogLine::warning(
            "outfile was not specified and wantreturn is false. The resulting image will be inaccessible",
        ));
    }
    Ok((wantreturn.then(|| AttachedImage::from_handle(handle)), logs))
}
