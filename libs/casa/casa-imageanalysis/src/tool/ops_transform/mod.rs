// SPDX-License-Identifier: LGPL-3.0-or-later

//! Transformation, collapse, continuum subtraction, moments, regridding, and correlation ops.

pub mod convolve;

use std::path::Path;

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_core::RecordValue;
use casa_core::measures::doppler::DopplerRef;
use casa_images::ImageInterface;

use crate::analysis::imsubimage::{
    CollapseFunction, CollapsePixel, ContinuumSummary, DeviationOptions, ImageCollapse,
    ImageCollapseOptions, ImageContinuum, ImageDeviation, ImageTwoPoint, StructurePixel,
    SubimageRegion, TwoPointOptions,
};
use crate::analysis::{
    ImageMomentOptions, ImageRegridOptions, ImageRegridProduct, ensure_artifacts_do_not_overlap,
    make_image_moments, moment_output_names, regrid_image,
};

use super::attachment::{AttachedImage, ExternalPathCheck, read_locked};
use super::beam_view::BeamView;
use super::error::{ImageToolError, ImageToolResult};
use super::handle::{ImageHandle, Storage, with_storage};
use super::product_helpers::{
    apply_beam_subset, check_handle_aliasing, check_region_overlap, finalize_output_history,
    set_storage_coordinate_record,
};
use super::publish::{NamedImageStages, publish_image, publish_image_pair};

pub use convolve::{
    ConvolveKernelInput, convolve_2d_gaussian, convolve_image, fft_image, sepconvolve_image,
    smooth_axis,
};

/// Axis specification for `ia.collapse`.
pub enum CollapseAxes {
    /// No axes provided (`axes` omitted/empty).
    Missing,
    /// Numeric pixel axis indices.
    Numbers(Vec<i64>),
    /// Axis name prefixes (`ra`, `dec`, `freq`, `stokes`).
    Names(Vec<String>),
    /// Invalid type provided by caller.
    Unsupported,
}

impl CollapseAxes {
    /// Resolves numeric indices or world axis name prefixes to pixel axis indices.
    pub fn resolve(&self, coordinates: &CoordinateSystem) -> ImageToolResult<Vec<usize>> {
        match self {
            Self::Missing => Err(ImageToolError::new("axes must be specified")),
            Self::Unsupported => Err(ImageToolError::new("Unsupported type for parameter axes")),
            Self::Numbers(indices) => indices
                .iter()
                .map(|&axis| {
                    usize::try_from(axis)
                        .map_err(|_| ImageToolError::new("Collapse axes must be nonnegative"))
                })
                .collect(),
            Self::Names(names) => {
                let mut available = vec![String::new(); coordinates.n_world_axes()];
                for index in 0..coordinates.n_coordinates() {
                    let model = coordinates.coordinate(index);
                    for (axis, name) in coordinates
                        .coordinate_world_axes(index)
                        .expect("existing coordinate")
                        .into_iter()
                        .zip(model.axis_names())
                    {
                        if let Some(axis) = axis {
                            available[axis] = name.to_ascii_lowercase();
                        }
                    }
                }
                names
                    .iter()
                    .map(|name| {
                        let name = name.to_ascii_lowercase();
                        let matches: Vec<_> = available
                            .iter()
                            .enumerate()
                            .filter(|(_, axis)| axis.starts_with(&name))
                            .collect();
                        match matches.as_slice() {
                            [(axis, _)] => {
                                coordinates.world_axis_to_pixel_axis(*axis).ok_or_else(|| {
                                    ImageToolError::new(format!(
                                        "World axis {name} has no image pixel axis"
                                    ))
                                })
                            }
                            [] => Err(ImageToolError::new(format!(
                                "No axis matches requested axis {name}. Image axis names are [{}]",
                                available.join(", ")
                            ))),
                            _ => Err(ImageToolError::new(format!(
                                "Multiple axes [{}] match requested axis {name}",
                                matches
                                    .iter()
                                    .map(|(_, axis)| axis.as_str())
                                    .collect::<Vec<_>>()
                                    .join(",")
                            ))),
                        }
                    })
                    .collect()
            }
        }
    }
}

/// Request parameters for `ia.collapse`.
pub struct CollapseRequest<'a> {
    /// Reduction function (`mean`, `sum`, `max`, ...).
    pub function: &'a str,
    /// Axes specification.
    pub axes: CollapseAxes,
    /// Output image path (`""` creates temporary image).
    pub outfile: &'a str,
    /// Optional region.
    pub region: Option<SubimageRegion<'a>>,
    /// Pixel box string.
    pub spatial: &'a str,
    /// Channel range string.
    pub chans: &'a str,
    /// Stokes selection string.
    pub stokes: &'a str,
    /// LEL mask expression.
    pub mask: &'a str,
    /// Overwrite existing `outfile`.
    pub overwrite: bool,
    /// Stretch compatible mask axes.
    pub stretch: bool,
    /// Preformatted CASA call string `ia.collapse(...)` (`None` skips history).
    pub history_command: Option<String>,
}

/// Collapses selected axes using `function` (`ia.collapse`).
pub fn collapse_image(
    attached: &AttachedImage,
    request: &CollapseRequest<'_>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<AttachedImage> {
    let mut live_coordinates = None;
    let outfile = request.outfile.trim();
    let product = publish_image(outfile, request.overwrite, is_external_open, |dest| {
        read_locked(attached, |image| {
            image.check_single_precision("collapse")?;
            if !outfile.is_empty() {
                check_handle_aliasing(image, attached, Path::new(outfile), None)?;
            }
            let beams = attached.beam_view().clone();
            let record = image.coordinate_record();
            let (mut output, coords) = match image {
                ImageHandle::Float(storage) => storage.with_float_view(|src| {
                    let (st, cr) = collapse_typed(src, record, beams.record(), request, dest)?;
                    Ok::<_, ImageToolError>((ImageHandle::Float(st), cr))
                })?,
                ImageHandle::Complex(storage) => with_storage!(storage, src => {
                    let (st, cr) = collapse_typed(src.as_ref(), record, beams.record(), request, dest)?;
                    Ok::<_, ImageToolError>((ImageHandle::Complex(st), cr))
                })?,
                _ => unreachable!("checked single-precision image"),
            };
            if let Some(coords) = coords {
                output.set_coordinate_record(coords)?;
            }
            let entries = image.history_entries()?;
            output.append_history_entries(&entries)?;
            if let Some(cmd) = &request.history_command {
                let name = image.path().map_or_else(
                    || "Temporary_Image".into(),
                    |path| path.display().to_string(),
                );
                output.append_history_at(
                    "::image::collapse",
                    &[format!("Ran ia.collapse on {name}"), cmd.clone()],
                )?;
            }
            output.save()?;
            live_coordinates = AttachedImage::capture_live_coordinates(&output)?;
            Ok(output)
        })
    })?;
    let mut out = AttachedImage::from_handle(product);
    if let Some((record, system)) = live_coordinates.take() {
        out.keep_live_coordinates(record, system);
    }
    Ok(out)
}

fn collapse_typed<T: CollapsePixel>(
    source: &dyn ImageInterface<T>,
    record: RecordValue,
    beams: &RecordValue,
    request: &CollapseRequest<'_>,
    dest: Option<&Path>,
) -> ImageToolResult<(Storage<T>, Option<RecordValue>)> {
    let axes = request.axes.resolve(source.coordinates())?;
    let trimmed = request.outfile.trim();
    let options = ImageCollapseOptions {
        function: CollapseFunction::parse(request.function)?,
        axes: &axes,
        region: request.region,
        spatial: (!request.spatial.is_empty()).then_some(request.spatial),
        chans: (!request.chans.is_empty()).then_some(request.chans),
        stokes: (!request.stokes.is_empty()).then_some(request.stokes),
        mask: (!request.mask.is_empty()).then_some(request.mask),
        stretch: request.stretch,
        destination: (!trimmed.is_empty()).then(|| Path::new(trimmed)),
    };
    let mut collapse = ImageCollapse::new(source, record, options, beams)?;
    let mut storage = Storage::create(
        collapse.section.shape.clone(),
        collapse.section.coordinates.clone(),
        dest,
        None,
    )?;
    with_storage!(&mut storage, image => collapse.copy_to(source, image.as_mut()))?;
    Ok((storage, collapse.section.coordinate_record.take()))
}

/// Polynomial continuum subtraction along the spectral axis (`ia.continuumsub`).
#[allow(clippy::too_many_arguments)]
pub fn continuum_sub(
    attached: &AttachedImage,
    outline: &str,
    outcont: &str,
    region: Option<SubimageRegion<'_>>,
    channels: &[i64],
    fitorder: i64,
    overwrite: bool,
    history_command: Option<String>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<AttachedImage> {
    let mut coordinates = None;
    let outline = outline.trim();
    let outcont = outcont.trim();
    let mut product = publish_image_pair(
        outline,
        outcont,
        overwrite,
        is_external_open,
        |line, model| {
            read_locked(attached, |image| {
                for target in [outline, outcont].into_iter().filter(|t| !t.is_empty()) {
                    check_handle_aliasing(image, attached, Path::new(target), None)?;
                }
                image.check_single_precision("continuumsub")?;
                let ImageHandle::Float(storage) = image else {
                    return Err(ImageToolError::new(
                        "This method only supports Float valued images",
                    ));
                };
                let beams = attached.beam_view().clone();
                let record = image.coordinate_record();
                let (mut residual, mut model_h, summary) = storage.with_float_view(|src| {
                    continuum_fit(
                        src,
                        record,
                        beams.record(),
                        region,
                        channels,
                        fitorder,
                        line,
                        model,
                    )
                })?;
                let prior = image.history_entries()?;
                let stats = continuum_history_lines(&summary);
                for output in std::iter::once(&mut residual).chain(model_h.as_mut()) {
                    output.append_history_entries(&prior)?;
                    if let Some(cmd) = &history_command {
                        output.record_history("continuumsub", cmd.clone())?;
                    }
                    output.append_history_at("::ImageProfileFitter::fit", &stats)?;
                    output.save()?;
                }
                coordinates = AttachedImage::capture_live_coordinates(&residual)?;
                Ok((residual, model_h))
            })
        },
    )?;
    if let Some((record, system)) = coordinates {
        product.keep_live_coordinates(record, system);
    }
    Ok(product)
}

#[allow(clippy::too_many_arguments)]
fn continuum_fit(
    image: &dyn ImageInterface<f32>,
    record: RecordValue,
    beams: &RecordValue,
    region: Option<SubimageRegion<'_>>,
    channels: &[i64],
    fitorder: i64,
    outline: Option<&Path>,
    outcont: Option<&Path>,
) -> ImageToolResult<(ImageHandle, Option<ImageHandle>, ContinuumSummary)> {
    let mut plan = ImageContinuum::new(image, record, region, channels, fitorder, beams)?;
    let allocate = |path| {
        Storage::<f32>::create(
            plan.section.shape.clone(),
            plan.section.coordinates.clone(),
            path,
            None,
        )
    };
    let mut line = allocate(outline)?;
    let mut model = outcont.map(|p| allocate(Some(p))).transpose()?;
    let summary = match &mut model {
        Some(m) => with_storage!(&mut line, l => with_storage!(m, mdl => {
            plan.copy_to(image, l.as_mut(), Some(mdl.as_mut()))
        }))?,
        None => with_storage!(&mut line, l => plan.copy_to(image, l.as_mut(), None))?,
    };
    let mut line = ImageHandle::Float(line);
    let mut model = model.map(ImageHandle::Float);
    if let Some(rec) = plan.section.coordinate_record.take() {
        if let Some(mdl) = &mut model {
            mdl.set_coordinate_record(rec.clone())?;
        }
        line.set_coordinate_record(rec)?;
    }
    Ok((line, model, summary))
}

fn continuum_history_lines(summary: &ContinuumSummary) -> [String; 5] {
    let p = summary.profiles;
    let a = summary.attempted;
    let v = summary.valid;
    [
        format!("Number of profiles       = {p}"),
        format!("Number of fits attempted = {a}"),
        format!("Number succeeded         = {a}"),
        format!("Number converged         = {a}"),
        format!("Number valid             = {v}"),
    ]
}

/// Local deviation image along direction plane (`ia.deviation`).
pub fn deviation_image(
    attached: &AttachedImage,
    outfile: &str,
    overwrite: bool,
    options: DeviationOptions<'_>,
    history_command: Option<String>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<AttachedImage> {
    let mut live_coordinates = None;
    let handle = publish_image(outfile, overwrite, is_external_open, |path| {
        read_locked(attached, |source| {
            let ImageHandle::Float(storage) = source else {
                return Err(ImageToolError::new(
                    "This method only supports Float valued images",
                ));
            };
            if !outfile.is_empty() {
                check_handle_aliasing(source, attached, Path::new(outfile), None)?;
            }
            let beams = attached.beam_view().clone();
            let mut output = storage.with_float_view(|image| {
                let plan = ImageDeviation::new(
                    image,
                    source.coordinate_record(),
                    beams.record(),
                    options,
                )?;
                let section = plan.section();
                let mut result = Storage::<f32>::create(
                    section.shape.clone(),
                    section.coordinates.clone(),
                    path,
                    None,
                )?;
                with_storage!(&mut result, out => plan.copy_to(image, out.as_mut()))?;
                let mut handle = ImageHandle::Float(result);
                if let Some(record) = &section.coordinate_record {
                    handle.set_coordinate_record(record.clone())?;
                }
                Ok::<_, ImageToolError>(handle)
            })?;
            let entries = source.history_entries()?;
            finalize_output_history(&mut output, &entries, "deviation", history_command, None)?;
            live_coordinates = AttachedImage::capture_live_coordinates(&output)?;
            Ok(output)
        })
    })?;
    let mut product = AttachedImage::from_handle(handle);
    if let Some((record, system)) = live_coordinates {
        product.keep_live_coordinates(record, system);
    }
    Ok(product)
}

/// Computes image moments along an axis (`ia.moments`).
#[allow(clippy::too_many_arguments)]
pub fn moments_image(
    attached: &AttachedImage,
    moments: &[i32],
    axis: i64,
    region: Option<SubimageRegion<'_>>,
    mask: Option<&str>,
    stretch: bool,
    drop: bool,
    methods: &[String],
    smoothing: (&[i64], &[String], &[f64]),
    inclusion: &[f64],
    exclusion: &[f64],
    peak_snr: f32,
    noise: f32,
    doppler: DopplerRef,
    outfile: &str,
    smoother: &str,
    overwrite: bool,
    history_command: Option<String>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<AttachedImage> {
    let options = ImageMomentOptions {
        moments,
        axis,
        region,
        mask,
        stretch,
        drop,
        methods,
        smoothing,
        inclusion,
        exclusion,
        peak_snr,
        noise,
        doppler,
        destinations: &[],
    };
    let (stages, output) = read_locked(attached, |handle| {
        let ImageHandle::Float(storage) = handle else {
            return Err(ImageToolError::new(
                "This method only supports float-valued images",
            ));
        };
        let mut names = moment_output_names(
            options.moments,
            outfile.trim(),
            handle.path().and_then(Path::to_str),
        )?;
        let temporary = names[0].is_empty();
        names.push(smoother.trim().to_owned());
        let destinations = names
            .iter()
            .filter(|name| !name.is_empty())
            .map(Path::new)
            .collect::<Vec<_>>();
        for destination in &destinations {
            check_handle_aliasing(handle, attached, destination, None)?;
            if let Some(ext) = options.region.and_then(SubimageRegion::external_path) {
                ensure_artifacts_do_not_overlap(destination, ext)?;
            }
        }
        let mut stages = NamedImageStages::new(names.iter().map(String::as_str), overwrite)?;
        let mut products = storage.with_float_view(|image| {
            let mut beams = image.image_info()?.to_record();
            attached.beam_view().apply_to(&mut beams);
            let opts = ImageMomentOptions {
                destinations: &destinations,
                ..options
            };
            make_image_moments(image, handle.coordinate_record(), Some(&beams), opts)
        })?;
        let entries = handle.history_entries()?;
        let command = history_command.map(|details| {
            [
                concat!("version: casa-rs ", env!("CARGO_PKG_VERSION")).to_owned(),
                details,
            ]
        });
        let paths = stages.paths();
        for (path, image) in paths.iter().zip(&mut products.moments) {
            image.append_history_entries(&entries)?;
            if let Some(cmd) = &command {
                image.append_history_at("image::moments", cmd)?;
            }
            if let Some(path) = path {
                image.save_as(path)?;
            }
        }
        if let (Some(path), Some(image)) = (paths[products.moments.len()], &products.smoothed) {
            image.save_as(path)?;
        }
        if products.smoothed.is_none() {
            stages.omit(products.moments.len());
        }
        let output = temporary.then(|| products.moments.remove(0));
        Ok((stages, output))
    })?;
    let primary = stages.publish_and_open_primary(is_external_open)?;
    match (primary, output) {
        (None, Some(image)) => {
            let record = image.coordinates().to_record();
            Ok(AttachedImage::temporary(ImageHandle::Float(
                Storage::Temp {
                    image: Box::new(image),
                    fits: None,
                    record,
                },
            )))
        }
        (Some(image), None) => Ok(image),
        _ => Err(ImageToolError::new(
            "Expected exactly one primary moment image",
        )),
    }
}

/// Regrids `attached` onto a coordinate record or rotation (`ia.regrid` / `ia.rotate`).
pub fn regrid_attached(
    attached: &AttachedImage,
    outfile: &str,
    overwrite: bool,
    options: ImageRegridOptions<'_>,
    history: Option<(&str, String)>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<AttachedImage> {
    let destination = options.outfile;
    let region = options.region;
    let handle = publish_image(outfile, overwrite, is_external_open, |stage| {
        read_locked(attached, |handle| {
            if let Some(destination) = destination {
                check_handle_aliasing(handle, attached, destination, None)?;
                check_region_overlap(outfile, region)?;
            }
            let native = handle.coordinate_record();
            let beams = attached.beam_view().clone();
            let mut output = match handle {
                ImageHandle::Float(source) => {
                    ImageHandle::Float(source.with_float_view(|image| {
                        regrid_typed(image, native, &beams, options, stage)
                    })?)
                }
                ImageHandle::Double(source) => {
                    ImageHandle::Double(with_storage!(source, image => {
                        regrid_typed(image.as_ref(), native, &beams, options, stage)
                    })?)
                }
                ImageHandle::Complex(source) => {
                    ImageHandle::Complex(with_storage!(source, image => {
                        regrid_typed(image.as_ref(), native, &beams, options, stage)
                    })?)
                }
                ImageHandle::DComplex(source) => {
                    ImageHandle::DComplex(with_storage!(source, image => {
                        regrid_typed(image.as_ref(), native, &beams, options, stage)
                    })?)
                }
            };
            let inherited = handle.history_entries()?;
            if let Some((method, cmd)) = history {
                finalize_output_history(&mut output, &inherited, method, Some(cmd), None)?;
            } else {
                finalize_output_history(&mut output, &inherited, "regrid", None, None)?;
            }
            Ok(output)
        })
    })?;
    Ok(AttachedImage::from_handle(handle))
}

fn regrid_typed<T: casa_images::resample::RegridPixel>(
    source: &dyn ImageInterface<T>,
    native: RecordValue,
    beam_view: &BeamView,
    options: ImageRegridOptions<'_>,
    stage: Option<&Path>,
) -> ImageToolResult<Storage<T>> {
    let mut source_info = source.image_info()?.to_record();
    beam_view.apply_to(&mut source_info);
    let ImageRegridProduct {
        regridded,
        coordinate_record,
        image_info,
        beam_planes,
    } = regrid_image(source, native, &source_info, options)?;
    let [channels, polarizations] = &beam_planes;
    let mut storage = Storage::<T>::create(
        regridded.data.shape().to_vec(),
        regridded.coordinates,
        stage,
        Some(&regridded.data),
    )?;
    with_storage!(&mut storage, image => {
        image.set_units(source.units())?;
        image.set_misc_info(source.misc_info())?;
        if regridded.mask.iter().any(|&valid| !valid) {
            image.put_mask("mask0", &regridded.mask)?;
            image.set_default_mask("mask0")?;
        }
        Ok::<(), ImageToolError>(())
    })?;
    apply_beam_subset(
        &mut storage,
        image_info.to_record(),
        beam_view,
        channels,
        polarizations,
    )?;
    set_storage_coordinate_record(&mut storage, Some(coordinate_record))?;
    Ok(storage)
}

/// Two-point correlation / structure function (`ia.twopointcorrelation`).
pub fn two_point_correlation(
    attached: &AttachedImage,
    outfile: &str,
    overwrite: bool,
    options: TwoPointOptions<'_>,
    history_command: Option<String>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<ImageHandle> {
    let destination = options.destination;
    let region = options.region;
    publish_image(outfile, overwrite, is_external_open, |stage| {
        read_locked(attached, |handle| {
            handle.check_single_precision("twopointcorrelation")?;
            if let Some(destination) = destination {
                check_handle_aliasing(handle, attached, destination, None)?;
                check_region_overlap(outfile, region)?;
            }
            let beams = attached.beam_view().clone();
            let record = handle.coordinate_record();
            let mut output = match handle {
                ImageHandle::Float(source) => {
                    ImageHandle::Float(source.with_float_view(|image| {
                        twopoint_typed(image, record, &beams, options, stage)
                    })?)
                }
                ImageHandle::Complex(source) => {
                    ImageHandle::Complex(with_storage!(source, image => {
                        twopoint_typed(image.as_ref(), record, &beams, options, stage)
                    })?)
                }
                _ => unreachable!("pixel type was checked"),
            };
            let entries = handle.history_entries()?;
            finalize_output_history(
                &mut output,
                &entries,
                "twopointcorrelation",
                history_command,
                None,
            )?;
            Ok(output)
        })
    })
}

fn twopoint_typed<T: StructurePixel>(
    source: &dyn ImageInterface<T>,
    native: RecordValue,
    beam_view: &BeamView,
    options: TwoPointOptions<'_>,
    stage: Option<&Path>,
) -> ImageToolResult<Storage<T>> {
    let plan = ImageTwoPoint::new(source, native, beam_view.record(), options)?;
    let mut result = Storage::create(plan.shape.clone(), plan.coordinates.clone(), stage, None)?;
    with_storage!(&mut result, image => plan.copy_to(source, image.as_mut()))?;
    let mut info = plan.image_info.to_record();
    let beams = beam_view.beam_set()?;
    if beams.is_multi() {
        let channels = plan.source.beam_planes(
            source.find_axis(CoordinateType::Spectral),
            beams.n_channels(),
        );
        let stokes = plan
            .source
            .beam_planes(source.find_axis(CoordinateType::Stokes), beams.n_stokes());
        super::beams::subset_beam_record(
            &mut info,
            Some(beam_view.record()),
            &beams,
            &channels,
            &stokes,
        )?;
    } else if let Some(beam) = beam_view.record().get("restoringbeam") {
        info.upsert("restoringbeam", beam.clone());
    }
    with_storage!(&mut result, image => image.set_image_info_record(info))?;
    set_storage_coordinate_record(&mut result, Some(plan.coordinate_record))?;
    Ok(result)
}
