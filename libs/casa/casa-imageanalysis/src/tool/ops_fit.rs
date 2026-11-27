// SPDX-License-Identifier: LGPL-3.0-or-later

//! Fitting, source finding, decomposition, histogram, profile, and bounding-box ops.

use std::fmt::Write as _;
use std::path::Path;

use casa_core::{ArrayD, ArrayValue, Complex32, RecordValue, ScalarValue, Value};
use casa_images::{Image, ImageInterface, MutableImageInterface, PagedImage};
use casa_lattices::TiledShape;
use ndarray::{Dimension, IxDyn};

use crate::analysis::imsubimage::{
    CollapsePixel, ComponentFitPixel, ComponentFitQuantity, DecompositionOptions,
    FindSourcesOptions, FitComponentsOptions, HistogramOptions, ImageComponentFit,
    ImageDecomposition, ImageMaxFit, ImageProfileFit, ImageProfileFitResult, ImageReadProfile,
    ImageRegionBounds, MaxFitOptions, ProfileDivisor, ProfileEstimates, ProfileFitOptions,
    ProfileLineReport, ProfileOutputNames, ProfileRegion, ProfileSigma, ReadProfileOptions,
    SubimageRegion, decompose_image, ensure_artifacts_do_not_overlap, find_image_sources,
    fit_image_components, fit_image_maximum, image_histogram, read_image_profile,
};
use crate::analysis::with_mask_output_paths;

use super::attachment::{AttachedImage, ExternalPathCheck, read_locked};
use super::error::{ImageToolError, ImageToolResult, ToolLogLine};
use super::handle::{ImageHandle, Storage, with_storage};
use super::product_helpers::with_float_image_view;
use super::publish::{NamedImageStages, publish_image_set};
use super::text_output::StagedTextOutput;

/// Profile result across real (`f32`) or complex (`Complex32`) images (`ia.getprofile`).
pub enum ProfileData {
    /// Single-precision real profile.
    Float(ImageReadProfile<f32>),
    /// Single-precision complex profile.
    Complex(ImageReadProfile<Complex32>),
}

/// Sigma input source for `ia.fitprofile`.
pub enum ProfileSigmaInput<'a> {
    /// No per-channel sigma weighting.
    None,
    /// Explicit sigma values array.
    Values(&'a ArrayD<f32>),
    /// Path to an image supplying sigma weights.
    Path(&'a str),
}

/// Resolves the pixel and world bounding box for a region (`ia.boundingbox`).
pub fn bounding_box(
    attached: &AttachedImage,
    region: Option<SubimageRegion<'_>>,
) -> ImageToolResult<ImageRegionBounds> {
    let mut image = attached.lock();
    attached.require_pixel_read_lock(&mut image)?;
    let unsupported_precision = match &*image {
        ImageHandle::Double(_) => Some("double"),
        ImageHandle::DComplex(_) => Some("complex<double>"),
        _ => None,
    };
    if let Some(precision) = unsupported_precision {
        return Err(ImageToolError::new(format!(
            "boundingbox does not support images with {precision} precision pixel values"
        )));
    }
    match &*image {
        ImageHandle::Float(storage) => {
            Ok(storage.with_float_view(|img| ImageRegionBounds::resolve(img, region))?)
        }
        ImageHandle::Complex(storage) => {
            with_storage!(storage, img => Ok(ImageRegionBounds::resolve(img.as_ref(), region)?))
        }
        _ => unreachable!("double precision checked above"),
    }
}

/// Computes image pixel histograms (`ia.histograms`).
pub fn histograms(
    attached: &AttachedImage,
    options: HistogramOptions<'_>,
) -> ImageToolResult<RecordValue> {
    if !matches!(
        *attached.lock(),
        ImageHandle::Float(_) | ImageHandle::Double(_)
    ) {
        return Err(ImageToolError::new(
            "This method only supports real-valued images",
        ));
    }
    read_locked(attached, |handle| match handle {
        ImageHandle::Float(storage) => {
            Ok(storage.with_float_view(|img| image_histogram(img, options))?)
        }
        ImageHandle::Double(storage) => {
            with_storage!(storage, img => Ok(image_histogram(img.as_ref(), options)?))
        }
        _ => Err(ImageToolError::new(
            "This method only supports real-valued images",
        )),
    })
}

/// Decomposes emission islands / Gaussian components (`ia.decompose`).
pub fn decompose(
    attached: &AttachedImage,
    options: DecompositionOptions<'_>,
) -> ImageToolResult<ImageDecomposition> {
    read_locked(attached, |image| {
        let ImageHandle::Float(storage) = image else {
            return Err(ImageToolError::new(
                "This application supports only real-valued images",
            ));
        };
        let record = image.coordinate_record();
        Ok(storage.with_float_view(|src| decompose_image(src, record, options))?)
    })
}

/// Finds point/Gaussian sources on a sky plane (`ia.findsources`).
pub fn find_sources(
    attached: &AttachedImage,
    options: FindSourcesOptions<'_>,
) -> ImageToolResult<RecordValue> {
    read_locked(attached, |image| {
        let ImageHandle::Float(storage) = image else {
            return Err(ImageToolError::new(
                "This application supports only float-valued images",
            ));
        };
        let record = image.coordinate_record();
        Ok(storage.with_float_view(|src| find_image_sources(src, record, options))?)
    })
}

/// Fits the brightest source around a local maximum (`ia.maxfit`).
pub fn max_fit(
    attached: &AttachedImage,
    options: MaxFitOptions<'_>,
) -> ImageToolResult<ImageMaxFit> {
    with_float_image_view(attached, |view, handle| {
        let native = handle.coordinate_record();
        Ok(fit_image_maximum(view, native, options)?)
    })
    .map_err(|err| {
        if err.to_string().contains("single-precision") {
            ImageToolError::new("This method only supports float-valued images")
        } else {
            err
        }
    })
}

/// Extracts a 1D collapsed profile along `options.axis` and optionally writes a text report (`ia.getprofile`).
pub fn get_profile(
    attached: &AttachedImage,
    options: ReadProfileOptions<'_>,
) -> ImageToolResult<ProfileData> {
    match *attached.lock() {
        ImageHandle::Float(_) | ImageHandle::Complex(_) => {}
        _ => {
            return Err(ImageToolError::new(
                "getprofile does not support images with double precision pixel values",
            ));
        }
    }
    read_locked(attached, |source| match source {
        ImageHandle::Float(storage) => storage.with_float_view(|image| {
            read_profile_typed(attached, source, image, options).map(ProfileData::Float)
        }),
        ImageHandle::Complex(storage) => with_storage!(storage, image => {
            read_profile_typed(attached, source, image.as_ref(), options).map(ProfileData::Complex)
        }),
        _ => Err(ImageToolError::new(
            "getprofile does not support images with double precision pixel values",
        )),
    })
}

trait ProfileLogPixel: CollapsePixel {
    fn format_report_value(self) -> String;
}

impl ProfileLogPixel for f32 {
    fn format_report_value(self) -> String {
        format!("{self:10.7}")
    }
}

impl ProfileLogPixel for Complex32 {
    fn format_report_value(self) -> String {
        format!("({:.7},{:.7})", self.re, self.im)
    }
}

fn read_profile_typed<T: ProfileLogPixel>(
    attached: &AttachedImage,
    source: &ImageHandle,
    image: &dyn ImageInterface<T>,
    options: ReadProfileOptions<'_>,
) -> ImageToolResult<ImageReadProfile<T>> {
    if let Some(path) = options.logfile {
        let region_file = options.region.and_then(SubimageRegion::external_path);
        for input in source
            .path()
            .into_iter()
            .chain(
                attached
                    .operand_paths()
                    .iter()
                    .map(std::path::PathBuf::as_path),
            )
            .chain(region_file)
        {
            ensure_artifacts_do_not_overlap(path, input)?;
        }
    }
    let logfile = options.logfile;
    let region_path = match &options.region {
        Some(SubimageRegion::Text(text)) => Some(*text),
        _ => None,
    };
    let mut beams = image.image_info()?.to_record();
    attached.beam_view().apply_to(&mut beams);
    let profile = read_image_profile(image, source.coordinate_record(), &beams, options)?;
    if let Some(path) = logfile {
        let name = source
            .path()
            .map(|p| p.to_string_lossy())
            .unwrap_or_else(|| "Temporary_Image".into());
        let text = format_profile_report(&profile, &name, region_path);
        StagedTextOutput::new(path, &text, false)?.install()?;
    }
    Ok(profile)
}

fn format_profile_report<T: ProfileLogPixel>(
    profile: &ImageReadProfile<T>,
    image: &str,
    region: Option<&str>,
) -> String {
    let mut text = format!("#title: {} profile - {image}\n", profile.axis_name);
    if let Some(region) = region {
        let _ = writeln!(text, "#region : {region}");
    }
    let _ = writeln!(
        text,
        "#xUnit {}\n#yUnit {}\n# {image}\n",
        profile.x_unit, profile.y_unit
    );
    for ((&coordinate, &value), &valid) in profile
        .coordinates
        .iter()
        .zip(&profile.values)
        .zip(&profile.mask)
    {
        if valid {
            let _ = writeln!(text, "{coordinate:.7} {}", value.format_report_value());
        }
    }
    text
}

/// Parameters for `ia.fitcomponents`.
pub struct FitComponentsRequest<'a> {
    /// Pixel box string.
    pub box_selection: Option<&'a str>,
    /// Region specification.
    pub region: Option<SubimageRegion<'a>>,
    /// Channel range string.
    pub chans: Option<&'a str>,
    /// Stokes selection string.
    pub stokes: Option<&'a str>,
    /// LEL mask expression.
    pub mask: Option<&'a str>,
    /// Stretch compatible mask axes.
    pub stretch: bool,
    /// Include pixel value range (`[lo, hi]` or empty).
    pub includepix: &'a [f64],
    /// Exclude pixel value range (`[lo, hi]` or empty).
    pub excludepix: &'a [f64],
    /// Residual output image path (`""` omits).
    pub residual: &'a str,
    /// Model output image path (`""` omits).
    pub model: &'a str,
    /// Path to initial estimates file.
    pub estimates: Option<&'a Path>,
    /// Path to write text report logfile (`""` omits).
    pub logfile: &'a str,
    /// Append to logfile instead of replacing.
    pub append: bool,
    /// Path to write updated estimates text file (`""` omits).
    pub newestimates: &'a str,
    /// Path to write fitted component list table (`""` omits).
    pub complist: &'a str,
    /// Path to write summary text file (`""` omits).
    pub summary: &'a str,
    /// Overwrite existing output files/tables.
    pub overwrite: bool,
    /// Constant zero level initial guess and fixed flag (`None` disables).
    pub zero_level: Option<(f64, bool)>,
    /// Explicit RMS noise value (`0.0` uses residual RMS).
    pub rms: Option<ComponentFitQuantity<'a>>,
    /// Noise correlation FWHM.
    pub noise_fwhm: Option<ComponentFitQuantity<'a>>,
    /// History string for `ia.fitcomponents(...)` and logfile header.
    pub history_call: String,
    /// Record CASA command history in created outputs.
    pub record_history: bool,
}

/// Fits 2D Gaussian components on selected sky planes (`ia.fitcomponents`).
pub fn fit_components(
    attached: &AttachedImage,
    request: FitComponentsRequest<'_>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<RecordValue> {
    if !matches!(
        *attached.lock(),
        ImageHandle::Float(_) | ImageHandle::Double(_)
    ) {
        return Err(ImageToolError::new(
            "This method only supports real valued images",
        ));
    }
    let paths = [
        request.model,
        request.residual,
        request.complist,
        request.logfile,
        request.newestimates,
        request.summary,
    ]
    .into_iter()
    .filter(|path| !path.is_empty())
    .map(Path::new)
    .collect::<Vec<_>>();
    for (index, path) in paths.iter().enumerate() {
        for peer in &paths[..index] {
            ensure_artifacts_do_not_overlap(path, peer)?;
        }
    }
    let fit = with_mask_output_paths(paths.iter().copied(), || {
        publish_image_set(
            [request.model, request.residual],
            true,
            is_external_open,
            |staged| {
                read_locked(attached, |source| {
                    for target in &paths {
                        for input in source.path().into_iter().chain(
                            attached
                                .operand_paths()
                                .iter()
                                .map(std::path::PathBuf::as_path),
                        ) {
                            ensure_artifacts_do_not_overlap(target, input)?;
                        }
                    }
                    match source {
                        ImageHandle::Float(storage) => storage.with_float_view(|img| {
                            fit_components_typed(attached, source, img, &request, &paths, staged)
                        }),
                        ImageHandle::Double(storage) => with_storage!(storage, img => {
                            fit_components_typed(attached, source, img.as_ref(), &request, &paths, staged)
                        }),
                        _ => Err(ImageToolError::new(
                            "This method only supports real valued images",
                        )),
                    }
                })
            },
        )
    })?;
    if !fit.components.is_empty() {
        if !request.complist.is_empty()
            && (request.overwrite || !Path::new(request.complist).exists())
        {
            publish_image_set(
                [request.complist],
                request.overwrite,
                is_external_open,
                |[path]| {
                    fit.components
                        .write_table(path.expect("component list destination"))
                        .map_err(|err| ImageToolError::new(err.to_string()))
                },
            )?;
        }
        if !request.newestimates.is_empty() {
            StagedTextOutput::new(
                Path::new(request.newestimates),
                &fit.estimates_text(),
                false,
            )?
            .install()?;
        }
        if !request.summary.is_empty() {
            StagedTextOutput::new(Path::new(request.summary), &fit.summary_text(), false)?
                .install()?;
        }
    }
    if !request.logfile.is_empty() {
        let text = format!(
            "{}\n{} fitted components\n{}",
            request.history_call,
            fit.components.len(),
            fit.log_text()
        );
        StagedTextOutput::new(Path::new(request.logfile), &text, request.append)?.install()?;
    }
    Ok(fit.record)
}

fn fit_components_typed<T: ComponentFitPixel>(
    attached: &AttachedImage,
    source: &ImageHandle,
    image: &dyn ImageInterface<T>,
    request: &FitComponentsRequest<'_>,
    paths: &[&Path],
    staged: [Option<&Path>; 2],
) -> ImageToolResult<ImageComponentFit> {
    let beam_view = attached.beam_view();
    let beams = beam_view.beam_set()?;
    let fit = fit_image_components(
        image,
        FitComponentsOptions {
            pixel_box: request.box_selection,
            region: request.region,
            channels: request.chans,
            stokes: request.stokes,
            mask: request.mask,
            destinations: paths,
            stretch: request.stretch,
            include_pixels: request.includepix,
            exclude_pixels: request.excludepix,
            estimates: request.estimates,
            zero_level: request.zero_level,
            beams: Some(&beams),
            rms: request.rms.as_ref().map(|q| match q {
                ComponentFitQuantity::Numeric(v) => ComponentFitQuantity::Numeric(*v),
                ComponentFitQuantity::Quantity(r) => ComponentFitQuantity::Quantity(r),
            }),
            report_log: !request.logfile.is_empty(),
            noise_fwhm: request.noise_fwhm.as_ref().map(|q| match q {
                ComponentFitQuantity::Numeric(v) => ComponentFitQuantity::Numeric(*v),
                ComponentFitQuantity::Quantity(r) => ComponentFitQuantity::Quantity(r),
            }),
        },
    )?;
    let [model, residual] = staged.map(|path| {
        path.map(|path| {
            let shape = image.shape().to_vec();
            let tiled = TiledShape::new(shape.clone(), std::mem::size_of::<f32>())?;
            PagedImage::<f32>::create_with_tile_shape_and_cache(
                shape,
                tiled.tile_shape().to_vec(),
                image.coordinates().clone(),
                path,
                16 * 1024 * 1024,
            )
            .map_err(ImageToolError::from)
        })
        .transpose()
    });
    let mut model = model?;
    let mut residual = residual?;
    fit.write_images(
        image,
        model
            .as_mut()
            .map(|img| img as &mut dyn MutableImageInterface<f32>),
        residual
            .as_mut()
            .map(|img| img as &mut dyn MutableImageInterface<f32>),
    )?;
    if model.is_some() || residual.is_some() {
        let coordinate_record = source.coordinate_record();
        let mut image_info = image.image_info()?.to_record();
        beam_view.apply_to(&mut image_info);
        let source_history = source.history_entries()?;
        for mut output in [model, residual].into_iter().flatten() {
            output.set_coordinate_record(coordinate_record.clone())?;
            output.set_image_info_record(image_info.clone())?;
            let mut handle = ImageHandle::Float(Storage::Paged(Box::new(output)));
            handle.append_history_entries(&source_history)?;
            if request.record_history {
                handle.record_history("fitcomponents", request.history_call.clone())?;
            }
            handle.save()?;
        }
    }
    Ok(fit)
}

/// Request parameters for `ia.fitprofile`.
pub struct FitProfileRequest<'a> {
    /// Pixel box, channel, and Stokes strings.
    pub selection: [&'a str; 3],
    /// Optional region specification.
    pub region: Option<SubimageRegion<'a>>,
    /// Mask expression (`""` omits).
    pub mask: &'a str,
    /// Profile axis index (`< 0` selects spectral).
    pub axis: i64,
    /// Number of singlets (`< 0` emits warning).
    pub ngauss: i64,
    /// Continuum polynomial degree (`< 0` disables).
    pub poly: i64,
    /// Minimum unmasked points required per profile.
    pub min_points: i64,
    /// Path to initial estimates text file.
    pub estimates_file: &'a str,
    /// Fit every spatial pixel vs spatial average.
    pub multifit: bool,
    /// Stretch compatible mask axes.
    pub stretch: bool,
    /// Analytic component estimates.
    pub estimates: ProfileEstimates<'a>,
    /// Good `[amp, center, fwhm]` bounds.
    pub good_ranges: [Option<[f64; 2]>; 3],
    /// Restrict fit to specific plane indices.
    pub planes: &'a [i64],
    /// Spectral index divisor specification.
    pub divisor: ProfileDivisor,
    /// Optional per-channel sigma weights.
    pub sigma: ProfileSigmaInput<'a>,
    /// Output image names (`model`, `residual`, lines, spectral coeffs, `outsigma`).
    pub outputs: ProfileOutputNames<'a>,
    /// Logfile output path (`""` omits).
    pub logfile: &'a str,
    /// Append to `logfile` rather than truncating.
    pub append: bool,
    /// Include full fitted parameter report lines in returned `ToolLogLine`s.
    pub log_details: bool,
    /// Formatted `ia.fitprofile(...)` command string for logs/history.
    pub history_call: String,
    /// Write history into output spectra/sigma images.
    pub record_history: bool,
}

/// Fits 1D spectral/axis profiles (`ia.fitprofile`).
pub fn fit_profile(
    attached: &AttachedImage,
    request: FitProfileRequest<'_>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<(RecordValue, Vec<ToolLogLine>)> {
    if !matches!(*attached.lock(), ImageHandle::Float(_)) {
        return Err(ImageToolError::new(
            "This method only supports Float valued images",
        ));
    }
    let sigma_img = match request.sigma {
        ProfileSigmaInput::Path(path) => Some(Image::open(path)?),
        _ => None,
    };
    validate_profile_log(request.logfile)?;
    let (stages, fit) = read_locked(attached, |source| match source {
        ImageHandle::Float(storage) => storage.with_float_view(|image| {
            compute_profile_fit(
                attached,
                source,
                image,
                &request,
                sigma_img.as_ref(),
                is_external_open,
            )
        }),
        _ => Err(ImageToolError::new(
            "This method only supports Float valued images",
        )),
    })?;
    let mut logs: Vec<_> = fit.summary.iter().cloned().map(ToolLogLine::info).collect();
    if request.ngauss < 0 {
        logs.push(ToolLogLine::warning(
            "ngauss < 0 is meaningless. Setting ngauss = 0",
        ));
    }
    if !request.outputs.sigma.is_empty() && matches!(request.sigma, ProfileSigmaInput::None) {
        logs.push(ToolLogLine::warning(
            "Ignoring outsigma because no sigma image or array was specified",
        ));
    }
    let details = (request.log_details || !request.logfile.is_empty())
        .then(|| profile_log_text(&request.history_call, &fit));
    let staged_log = if let Some(details) = &details
        && !request.logfile.is_empty()
    {
        let text = format!("{}\n\n{details}", fit.summary.join("\n"));
        Some(StagedTextOutput::new(
            Path::new(request.logfile),
            &text,
            request.append,
        )?)
    } else {
        None
    };
    stages.publish_with(true, is_external_open, || {
        staged_log.map_or(Ok(()), StagedTextOutput::install)
    })?;
    if let Some(details) = details
        && request.log_details
    {
        logs.push(ToolLogLine::info(details));
    }
    Ok((fit.record, logs))
}

fn compute_profile_fit(
    attachment: &AttachedImage,
    source: &ImageHandle,
    image: &dyn ImageInterface<f32>,
    request: &FitProfileRequest<'_>,
    sigma_img: Option<&Image>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<(NamedImageStages, ImageProfileFitResult)> {
    let mut beam_record = image.image_info()?.to_record();
    attachment.beam_view().apply_to(&mut beam_record);
    let [pixel_box, channels, stokes] = request.selection;
    let region_file = request.region.and_then(SubimageRegion::external_path);
    let fit = ImageProfileFit::new(
        image,
        source.coordinate_record(),
        ProfileFitOptions {
            selection: ProfileRegion {
                pixel_box: nonempty(pixel_box),
                region: request.region,
                chans: nonempty(channels),
                stokes: nonempty(stokes),
            },
            axis: request.axis,
            mask: nonempty(request.mask),
            stretch: request.stretch,
            multifit: request.multifit,
            min_points: request.min_points.max(0) as usize,
            gaussians: request.ngauss.max(0) as usize,
            polynomial: usize::try_from(request.poly).ok(),
            estimates_file: nonempty(request.estimates_file).map(Path::new),
            estimates: request.estimates,
            good_ranges: request.good_ranges,
            planes: request.planes,
            divisor: request.divisor.clone(),
            sigma: match request.sigma {
                ProfileSigmaInput::Values(values) => Some(ProfileSigma::Values(values)),
                ProfileSigmaInput::Path(_) => {
                    sigma_img.map(|img| ProfileSigma::Image(img as &dyn ImageInterface<f32>))
                }
                ProfileSigmaInput::None => None,
            },
            other_artifact: nonempty(request.logfile).map(Path::new),
            beam_record: Some(&beam_record),
            log_details: request.log_details || !request.logfile.is_empty(),
        },
        request.outputs,
    )?;
    let _ = is_external_open;
    validate_fit_paths(
        fit.destinations().chain(nonempty(request.logfile)),
        source
            .path()
            .into_iter()
            .chain(
                attachment
                    .operand_paths()
                    .iter()
                    .map(std::path::PathBuf::as_path),
            )
            .chain(sigma_img.and_then(ImageInterface::name))
            .chain(nonempty(request.estimates_file).map(Path::new))
            .chain(region_file),
    )?;
    let output_is_spectrum: Vec<_> = fit
        .destinations()
        .map(|name| request.outputs.spectra.contains(&name) || request.outputs.sigma == name)
        .collect();
    let stages = NamedImageStages::with_replacements(
        fit.destinations()
            .map(|name| (name, name == request.outputs.sigma)),
    )?;
    let paths = stages.paths();
    let fit = fit.fit(&paths)?;
    {
        let mut paths = paths
            .into_iter()
            .zip(output_is_spectrum)
            .filter_map(|(path, with_history)| path.filter(|_| with_history))
            .peekable();
        let prior = if paths.peek().is_some() {
            source.history_entries()?
        } else {
            Vec::new()
        };
        for path in paths {
            let mut out = Image::open(path)?;
            if out.history()?.is_empty() {
                out.append_history_entries(&prior)?;
            }
            if request.record_history {
                out.append_history_at(
                    "::image::fitprofile",
                    &["Ran ia.fitprofile".into(), request.history_call.clone()],
                )?;
            }
            out.append_history_at("::ImageProfileFitter::fit", &fit.summary)?;
            out.save()?;
        }
    }
    Ok((stages, fit))
}

fn profile_log_text(history_call: &str, fit: &ImageProfileFitResult) -> String {
    let mut text = format!("Input parameters ---\n{history_call}\n");
    let record = &fit.record;
    let Some(Value::Array(ArrayValue::Bool(valid))) = record.get("valid") else {
        return text;
    };
    let unit = |name| match record.get(name) {
        Some(Value::Scalar(ScalarValue::String(unit))) => unit.as_str(),
        _ => "",
    };
    let (x_unit, y_unit) = (unit("xUnit"), unit("yUnit"));
    let integral_unit = if x_unit.is_empty() || y_unit.is_empty() {
        format!("{x_unit}{y_unit}")
    } else {
        format!("{x_unit}.{y_unit}")
    };
    for (pixel, &ok) in valid.indexed_iter() {
        let position = pixel.slice();
        let Some(report) = fit.profile_reports.get(position) else {
            continue;
        };
        let coordinates = report
            .position
            .iter()
            .map(|coordinate| coordinate.map_or_else(|| "*".into(), |c| c.to_string()))
            .collect::<Vec<_>>();
        let _ = writeln!(text, "Fit\n    Pixel        : [{}]", coordinates.join(", "));
        if let Some(Value::Array(ArrayValue::String(direction))) = record.get("direction") {
            let _ = writeln!(text, "    Direction    : {}", direction[IxDyn(position)]);
        }
        for (key, label) in [("attempted", "Attempted"), ("converged", "Converged")] {
            if let Some(Value::Array(ArrayValue::Bool(flags))) = record.get(key) {
                let status = if flags[IxDyn(position)] { "YES" } else { "NO" };
                let _ = writeln!(text, "    {label:12} : {status}");
            }
        }
        if let Some(iterations) = report.iterations {
            let _ = writeln!(text, "    Iterations   : {iterations}");
        }
        let _ = writeln!(text, "    Valid        : {}", if ok { "YES" } else { "NO" });
        if !ok {
            continue;
        }
        for line in &report.lines {
            write_profile_line(&mut text, line, (x_unit, y_unit, &integral_unit));
        }
        if let Some(polynomial) = fit.polynomials.get(position) {
            let _ = writeln!(text, "    Type: POLYNOMIAL");
            for (kind, terms) in [("pixel", &polynomial.pixel), ("world", &polynomial.world)] {
                let _ = writeln!(text, "        Coefficients in {kind} coordinates");
                for (index, &[value, error]) in terms.iter().enumerate() {
                    let _ = writeln!(text, "        c{index} : {value:.12e} +/- {error:.8e}");
                }
            }
        }
        for field in record.fields() {
            if !matches!(field.name.as_str(), "plp" | "ltp") {
                continue;
            }
            let Value::Record(model) = &field.value else {
                continue;
            };
            let _ = writeln!(text, "    {}", field.name);
            for field in model.fields() {
                let Value::Array(ArrayValue::Float64(values)) = &field.value else {
                    continue;
                };
                let mut index = position.to_vec();
                index.push(0);
                let count = values.shape()[values.ndim() - 1];
                let _ = write!(text, "        {}", field.name);
                for component in 0..count {
                    index[position.len()] = component;
                    let _ = write!(text, "  {:.8e}", values[IxDyn(&index)]);
                }
                text.push('\n');
            }
        }
    }
    text
}

fn write_profile_line(
    text: &mut String,
    line: &ProfileLineReport,
    (x_unit, y_unit, integral_unit): (&str, &str, &str),
) {
    if line.subcomponent.is_none_or(|index| index == 0) {
        let _ = writeln!(text, "    Results for component {}:", line.component);
        let _ = writeln!(text, "        Type     : {}", line.kind);
    }
    if let Some(index) = line.subcomponent {
        let _ = writeln!(text, "        Results for subcomponent {index}:");
    }
    let [peak_fixed, center_fixed, width_fixed] = line.fixed;
    for (label, estimate, unit, fixed) in [
        ("Peak", Some(line.peak), y_unit, peak_fixed),
        ("Center", Some(line.center), x_unit, center_fixed),
        ("Center", line.pixel_center, "pixel", center_fixed),
        ("FWHM", Some(line.fwhm), x_unit, width_fixed),
        ("FWHM", line.pixel_fwhm, "pixel", width_fixed),
        (
            "Integral",
            Some(line.integral),
            integral_unit,
            peak_fixed && width_fixed,
        ),
    ] {
        let fixed = if fixed { " (fixed)" } else { "" };
        let Some([value, error]) = estimate else {
            let _ = writeln!(
                text,
                "                   Could not convert {label} to pixel"
            );
            continue;
        };
        if unit == "pixel" {
            let _ = writeln!(
                text,
                "                   {value:.12e} +/- {error:.12e} {unit}{fixed}"
            );
        } else {
            let _ = writeln!(
                text,
                "        {label:8} : {value:.12e} +/- {error:.12e} {unit}{fixed}"
            );
        }
    }
    if line.channel_fwhm <= 3. {
        let _ = writeln!(
            text,
            "        WARNING: The FWHM is at most three channel widths."
        );
    }
}

fn validate_fit_paths<'a, 'b>(
    outputs: impl IntoIterator<Item = &'a str>,
    inputs: impl IntoIterator<Item = &'b Path>,
) -> ImageToolResult<()> {
    let outputs: Vec<_> = outputs.into_iter().map(Path::new).collect();
    let inputs: Vec<_> = inputs.into_iter().collect();
    for (index, path) in outputs.iter().enumerate() {
        for other in inputs
            .iter()
            .copied()
            .chain(outputs[..index].iter().copied())
        {
            ensure_artifacts_do_not_overlap(path, other)?;
        }
    }
    Ok(())
}

fn validate_profile_log(path: &str) -> ImageToolResult<()> {
    if let Some(path) = nonempty(path).map(Path::new) {
        if path.is_dir() {
            return Err(ImageToolError::new(format!(
                "Log destination {} is a directory",
                path.display()
            )));
        }
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
            && !parent.is_dir()
        {
            return Err(ImageToolError::new(format!(
                "Log directory {} does not exist",
                parent.display()
            )));
        }
    }
    Ok(())
}

fn nonempty(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}
