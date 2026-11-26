// SPDX-License-Identifier: LGPL-3.0-or-later
//! Tutorial-scoped image-analysis operations matching CASA image task semantics.

use std::fs;
use std::path::Path;

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, FitsHeader, StokesType,
};
use casa_lattices::{
    LatticeStatistics, RobustStats, SamplePoint, Statistic, StatisticsAlgorithm,
    StatisticsAlgorithmKind, StatsElement, TiledShape, compute_robust_stats, parse_algorithm,
    parse_center, parse_clmethod,
};
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayD, RecordValue, ScalarValue, Value};
use fitsio::images::ImageType as FitsImageType;
use ndarray::{Dimension, IxDyn, ShapeBuilder, Zip};

mod axes;
mod channel_selection;
pub use axes::{replace_stokes_axis_in_record, section_quality_labels, write_quality_labels};
pub use channel_selection::parse_image_channel_selection;
pub(in crate::analysis) use channel_selection::{ChannelSpan, parse_image_channel_spans};
mod custom_values;
mod exportfits;
mod expression_workspace;
mod extrema;
pub use expression_workspace::with_image_expression_workspace;
mod feather;
pub use feather::feather;
mod imhead_fits;
mod imhead_masks;
mod immath;
mod importfits;
mod imregrid;
mod imsubimage;
pub use imsubimage::ImageRegionSection;
pub use imsubimage::{
    ChannelLineFit, ChannelLineShape, ImageProfileFit, ImageProfileFitResult, ProfileDivisor,
    ProfileEstimates, ProfileFitOptions, ProfileLineReport, ProfileOutputNames, ProfilePeaks,
    ProfileRegion, ProfileReport, ProfileSigma, fit_channel_lines,
};
pub use imsubimage::{CollapseFunction, CollapsePixel, ImageCollapse, ImageCollapseOptions};
pub use imsubimage::{
    ComponentFitPixel, ComponentFitQuantity, FitComponentsOptions, ImageComponentFit,
    fit_image_components,
};
pub use imsubimage::{ContinuumSummary, ImageContinuum};
pub use imsubimage::{ConvolutionKernel, ConvolutionOptions, ConvolutionPixel, ImageConvolution};
pub use imsubimage::{
    DecimationMethod, DecimationPixel, ImageCrop, ImageCropOptions, ImageDecimation,
    ImageDecimationOptions, ImageInsertion, ImagePad, ImagePadOptions, ImagePbcor, ImagePbcorBeam,
    ImagePbcorOptions, ImagePixelSelection, ImageRebin, ImageRebinOptions, ImageRegionBounds,
    ImageSmoothing, MaskReplacementOptions, MaskReplacementOutcome, SmoothingKernel,
    SmoothingOptions, SmoothingPixel, SubimageOptions, SubimagePlan, SubimageRegion,
    ensure_artifacts_do_not_overlap, ensure_outfile_does_not_alias_input, put_image_region,
    reduce_subimage_coordinate_record, replace_masked_complex_pixels, replace_masked_real_pixels,
    validate_mask_replacement_region, validate_put_region_shapes,
};
pub use imsubimage::{DecompositionOptions, ImageDecomposition, decompose_image};
pub use imsubimage::{DeviationOptions, ImageDeviation};
pub use imsubimage::{FindSourcesOptions, find_image_sources};
pub use imsubimage::{FourierOptions, FourierPixel, ImageFourier};
pub use imsubimage::{GaussianConvolutionOptions, ImageGaussianConvolution};
pub use imsubimage::{HistogramOptions, HistogramPixel, image_histogram};
pub use imsubimage::{ImageMaxFit, MaxFitOptions, fit_image_maximum};
pub use imsubimage::{
    ImageReadProfile, ProfileFrequencyFrame, ReadProfileOptions, read_image_profile,
};
pub use imsubimage::{ImageSepConvolution, SepConvolutionOptions};
pub use imsubimage::{ImageTwoPoint, StructurePixel, TwoPointOptions};
mod imhead_value;
mod list_log;
mod projection_header;
mod quantity_input;
mod schema;
mod summary_log;
mod transpose;
pub use transpose::copy_transposed_image;
mod world_header;

mod immoments;
pub use immoments::{
    ImageMomentOptions, ImageMomentProducts, make_image_moments, moment_output_names,
};
mod impbcor;
mod impv;
pub use impv::{ImagePvProduct, generate_image_pv};
mod imstat_log;
mod lel_mask;

use self::axes::{direction_plane_axes, world_axes_for_plane};
pub use self::exportfits::{
    ImageFitsExportOptions, ImageFitsHeader, export_attached_fits, image_fits_header,
    image_fits_header_with_beams, validate_attached_fits_target,
};
pub use self::extrema::imhead_extrema;
pub use self::importfits::importfits;
pub use self::imsubimage::ImageNoise;
pub use self::lel_mask::{
    BoundImageToolOperands, ImageToolExpressionInputs, compile_image_tool_expression,
    evaluate_image_tool_mask, evaluate_image_tool_mask_section, evaluate_pixel_expression,
    open_fits_expression, with_mask_output_paths,
};
pub use crate::expr_parser::ImageToolPixelExpression;
mod lel_pixels;
pub use self::imhead_value::ImheadValue;
pub use self::lel_pixels::{
    calculate_complex_pixels, calculate_real_pixels, materialize_image_tool_expression,
};
pub use self::list_log::imhead_list_log;
pub use self::schema::*;
pub use self::summary_log::imhead_summary_log;
use crate::expr_parser::{ExprParseError, ExprValueConvert, HashMapResolver, parse_mask_expr};
use crate::fits_read::{PrimaryHdu, image_info_from_fits_header};
use crate::image_expr::ImageExprValue;
use crate::{
    AnyPagedImage, GaussianBeam, ImageBeamSet, ImageError, ImageInfo, ImageInterface, ImagePixel,
    ImagePixelType, ImageType, PagedImage,
};

const SPEED_OF_LIGHT_KM_S: f64 = 299_792.458;
const SPEED_OF_LIGHT_M_S: f64 = 299_792_458.0;

/// Return a CASA `imhead(..., mode="summary")`-style metadata summary.
pub fn imhead(path: impl AsRef<Path>) -> Result<ImageHeaderSummary, ImageError> {
    let path = path.as_ref();
    if path.is_file() {
        return imhead_fits::summary(path);
    }
    let image = AnyPagedImage::open(path)?;
    match &image {
        AnyPagedImage::Float32(image) => imhead_typed(path, image, image.pixel_type_label()),
        AnyPagedImage::Float64(image) => imhead_typed(path, image, image.pixel_type_label()),
        AnyPagedImage::Complex32(image) => imhead_typed(path, image, "Complex32".into()),
        AnyPagedImage::Complex64(image) => imhead_typed(path, image, "Complex64".into()),
    }
}

/// Return CASA `imstat`-style statistics for tutorial selections.
///
/// The `algorithm` argument selects CASA's statistics algorithms; the
/// classical one streams through the lattice statistics, and the robust
/// ones materialize the selection.
pub fn parse_imstat_algorithm(request: &ImstatRequest) -> Result<StatisticsAlgorithm, String> {
    let kind = parse_algorithm(&request.algorithm)?;
    parse_clmethod(&request.clmethod)?;
    Ok(match kind {
        StatisticsAlgorithmKind::Classic => StatisticsAlgorithm::Classic,
        StatisticsAlgorithmKind::HingesFences => StatisticsAlgorithm::HingesFences {
            fence: request.fence,
        },
        StatisticsAlgorithmKind::Chauvenet => StatisticsAlgorithm::Chauvenet {
            zscore: request.zscore,
            maxiter: request.maxiter,
        },
        StatisticsAlgorithmKind::FitToHalf => StatisticsAlgorithm::FitToHalf {
            center: parse_center(&request.center)?,
            upper: !request.lside,
        },
        StatisticsAlgorithmKind::Biweight => StatisticsAlgorithm::Biweight {
            niter: request.niter,
        },
    })
}

/// Return CASA `imstat`-style statistics for one request: the global
/// statistics over the whole selection, or one per display plane when
/// the request collapses only some axes.
pub fn imstat(request: &ImstatRequest) -> Result<ImstatOutcome, ImageError> {
    // CASA's image tool opens FITS images directly, so imstat accepts a
    // FITS file as its image: converted here, then run as any image.
    let imported = import_fits_for_reading(&request.imagename)?;
    let open_path = imported.as_ref().map_or_else(
        || request.imagename.as_path(),
        |(_, converted)| converted.as_path(),
    );
    let image = AnyPagedImage::open(open_path)?;
    let path = request.imagename.as_path();
    match &image {
        AnyPagedImage::Float32(image) => imstat_typed(path, image, request),
        AnyPagedImage::Float64(image) => imstat_typed(path, image, request),
        AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => Err(
            ImageError::InvalidMetadata("imstat currently supports real-valued images".to_string()),
        ),
    }
}

/// A FITS image converted to a CASA image for reading, in a directory
/// that lives until the caller drops the returned pair. CASA's image
/// tool converts FITS images the same way when it opens them.
fn import_fits_for_reading(
    path: &Path,
) -> Result<Option<(tempfile::TempDir, std::path::PathBuf)>, ImageError> {
    if !path.exists() || !is_fits_file(path)? {
        return Ok(None);
    }
    let temp = tempfile::tempdir().map_err(|error| ImageError::Io(error.to_string()))?;
    let converted = temp.path().join("converted.image");
    import_fits(path, &converted, false)?;
    Ok(Some((temp, converted)))
}

/// Whether a file begins FITS's required first card, which is how
/// casacore's image opener tells a FITS image from a CASA one.
fn is_fits_file(path: &Path) -> Result<bool, ImageError> {
    use std::io::Read;
    let mut file = fs::File::open(path).map_err(|error| ImageError::Io(error.to_string()))?;
    let mut magic = [0u8; 9];
    let read = file.read(&mut magic).unwrap_or(0);
    Ok(read == magic.len() && &magic == b"SIMPLE  =")
}

/// Generate a CASA `immoments`-style moment map.
pub fn immoments(request: &ImmomentsRequest) -> Result<MomentMapSummary, ImageError> {
    if request.imagename.is_file() && is_fits_file(&request.imagename)? {
        let (image, history) =
            lel_mask::open_fits_expression_with_history::<f32>(&request.imagename, true)?;
        return immoments::run(&image, image.legacy_coords_ref(), &history, request);
    }
    let image = AnyPagedImage::open(&request.imagename)?;
    match &image {
        AnyPagedImage::Float32(image) => immoments::run_paged(image, request),
        AnyPagedImage::Float64(image) => immoments::run_paged(image, request),
        AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => {
            Err(ImageError::InvalidMetadata(
                "immoments currently supports real-valued images".to_string(),
            ))
        }
    }
}

/// Extract a CASA-style position-velocity image from a slice of the sky plane.
///
/// # Errors
/// Returns an error for an invalid strip, selection, coordinate system, or output.
pub fn impv(request: &ImpvRequest) -> Result<PvImageSummary, ImageError> {
    let image = AnyPagedImage::open(&request.imagename)?;
    match &image {
        AnyPagedImage::Float32(image) => impv::run(image, request),
        AnyPagedImage::Float64(image) => impv::run(image, request),
        AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => Err(
            ImageError::InvalidMetadata("impv currently supports real-valued images".to_string()),
        ),
    }
}

/// Extract a CASA `imsubimage` section across pixel/world boxes, CRTF and
/// saved regions, channel/Stokes selections, LEL masks, and `dropdeg`/`keepaxes`.
pub fn imsubimage(request: &ImsubimageRequest) -> Result<ImageSubimageSummary, ImageError> {
    imsubimage::run(request)
}

/// Evaluate CASA `immath` across `evalexpr`, `spix`, `pola`, `poli`, `lpoli`, and `tpoli` modes.
pub fn immath(request: &ImmathRequest) -> Result<ImageMathSummary, ImageError> {
    immath::run(request)
}

/// Apply primary-beam correction to a CASA or FITS image selected by region,
/// channel, Stokes and LEL mask. The beam may be an image or a pixel array.
///
/// # Errors
/// Returns an error for incompatible beam geometry, invalid selections,
/// unsupported pixel types, or failed output publication.
pub fn impbcor(request: &ImpbcorRequest) -> Result<PbcorSummary, ImageError> {
    impbcor::run(request)
}

pub use self::imregrid::{
    ImageRegridOptions, ImageRegridProduct, ImageRegridTarget, imregrid, regrid_image,
};

/// Export a CASA image (or FITS image) to a primary-HDU FITS file using full
/// CASA `exportfits` semantics.
pub fn exportfits(request: &ExportFitsRequest) -> Result<FitsExportSummary, ImageError> {
    exportfits::run(request)
}

/// Export a CASA image as a primary-HDU FITS image.
pub fn export_fits(
    imagename: impl AsRef<Path>,
    fitsimage: impl AsRef<Path>,
    velocity: bool,
    overwrite: bool,
) -> Result<FitsExportSummary, ImageError> {
    exportfits(&ExportFitsRequest {
        imagename: imagename.as_ref().to_path_buf(),
        fitsimage: fitsimage.as_ref().to_path_buf(),
        velocity,
        overwrite,
        ..ExportFitsRequest::default()
    })
}

/// Import a primary-HDU FITS image, preserving double-precision pixels.
///
/// For the CASA task defaults and selection options, use [`importfits()`].
pub fn import_fits(
    fitsimage: impl AsRef<Path>,
    imagename: impl AsRef<Path>,
    overwrite: bool,
) -> Result<FitsImportSummary, ImageError> {
    let fitsimage = fitsimage.as_ref();
    let imagename = imagename.as_ref();
    if imagename.exists() {
        if overwrite {
            fs::remove_dir_all(imagename).map_err(|error| ImageError::Io(error.to_string()))?;
        } else {
            return Err(ImageError::Io(format!(
                "CASA image output already exists: {}",
                imagename.display()
            )));
        }
    }

    let mut primary = PrimaryHdu::open(fitsimage)?;
    let mut image_info = image_info_from_fits_header(&primary.header);
    let beam_data = primary.beam_set()?;
    if let Some((beams, _)) = &beam_data {
        image_info.beam_set = beams.clone();
    }
    let image_info = (image_info, beam_data.map(|(_, records)| records));
    let PrimaryHdu {
        mut fits,
        hdu,
        image_type,
        shape,
        header,
        coordinates,
    } = primary;
    match image_type {
        FitsImageType::Double => {
            let data = hdu
                .read_image::<Vec<f64>>(&mut fits)
                .map_err(|error| ImageError::Io(error.to_string()))?;
            import_fits_typed::<f64>(
                fitsimage,
                imagename,
                shape,
                coordinates,
                header,
                image_info,
                data,
            )
        }
        FitsImageType::Float
        | FitsImageType::UnsignedByte
        | FitsImageType::Byte
        | FitsImageType::Short
        | FitsImageType::UnsignedShort
        | FitsImageType::Long
        | FitsImageType::UnsignedLong
        | FitsImageType::LongLong => {
            let data = hdu
                .read_image::<Vec<f32>>(&mut fits)
                .map_err(|error| ImageError::Io(error.to_string()))?;
            import_fits_typed::<f32>(
                fitsimage,
                imagename,
                shape,
                coordinates,
                header,
                image_info,
                data,
            )
        }
    }
}

fn import_fits_typed<T>(
    fitsimage: &Path,
    imagename: &Path,
    shape: Vec<usize>,
    coordinates: CoordinateSystem,
    header: FitsHeader,
    image_info: (ImageInfo, Option<RecordValue>),
    data: Vec<T>,
) -> Result<FitsImportSummary, ImageError>
where
    T: ImagePixel,
{
    let array = ArrayD::from_shape_vec(IxDyn(&shape).f(), data).map_err(|error| {
        ImageError::InvalidMetadata(format!("invalid FITS image shape: {error}"))
    })?;
    let tile_shape = TiledShape::new(shape.clone(), T::ELEM_SIZE)?
        .tile_shape()
        .to_vec();
    let mut image =
        PagedImage::<T>::create_with_tile_shape(shape.clone(), tile_shape, coordinates, imagename)?;
    image.put_slice(&array, &vec![0; shape.len()])?;
    if header.get_string("BUNIT").is_some() {
        image.set_units(crate::fits_read::brightness_unit(&header))?;
    }
    if let Some(beams) = image_info.1.as_ref() {
        image.set_image_info_with_beam_records(&image_info.0, beams)?;
    } else {
        image.set_image_info(&image_info.0)?;
    }
    image.add_history(format!("Imported from FITS {}", fitsimage.display()))?;
    image.save()?;

    Ok(FitsImportSummary {
        fitsimage: fitsimage.display().to_string(),
        imagename: imagename.display().to_string(),
        shape,
        pixel_type: image_pixel_type::<T>().pixel_type_label(),
        units: crate::fits_read::brightness_unit(&header),
        object_name: image_info.0.object_name,
        has_restoring_beam: image_info
            .0
            .beam_set
            .single_beam()
            .is_some_and(|beam| !beam.is_null()),
    })
}

trait PixelTypeLabel {
    fn pixel_type_label(&self) -> String;
}

impl PixelTypeLabel for ImagePixelType {
    fn pixel_type_label(&self) -> String {
        match self {
            Self::Float32 => "Float32",
            Self::Float64 => "Float64",
            Self::Complex32 => "Complex32",
            Self::Complex64 => "Complex64",
        }
        .to_string()
    }
}

impl PixelTypeLabel for PagedImage<f32> {
    fn pixel_type_label(&self) -> String {
        "Float32".to_string()
    }
}

impl PixelTypeLabel for PagedImage<f64> {
    fn pixel_type_label(&self) -> String {
        "Float64".to_string()
    }
}

/// Apply a supported string-valued CASA `imhead(..., mode="put")` update.
pub fn imhead_put(
    path: impl AsRef<Path>,
    hdkey: &str,
    hdvalue: impl Into<String>,
) -> Result<ImageHeaderSummary, ImageError> {
    imhead_change_string(path, "put", hdkey, &hdvalue.into())
}

/// Set a CASA `crpixN` image-header value, preserving coordinate axis maps.
///
/// `hdvalue` may be a finite number or a numeric string. The change is saved
/// with an image history entry and the updated header is returned.
///
/// # Errors
///
/// Returns an error for an invalid or polarization axis, non-numeric value,
/// malformed metadata, or image I/O failure. As in CASA, a history I/O error
/// can be reported after the metadata has been saved.
pub fn imhead_put_reference_pixel(
    path: impl AsRef<Path>,
    hdkey: &str,
    hdvalue: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    let path = path.as_ref();
    validate_imhead_image(path)?;
    let key = hdkey.to_ascii_lowercase();
    let axis = key
        .strip_prefix("crpix")
        .and_then(imhead_axis_index)
        .ok_or_else(|| ImageError::InvalidMetadata(format!("invalid axis keyword {hdkey:?}")))?;
    let mut image = AnyPagedImage::open(path)?;
    let coordinates = image.coordinates();
    if coordinates.pixel_axis_owner(axis).is_none() {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid axis keyword {hdkey:?}"
        )));
    }
    if coordinates
        .world_axis_owner(axis)
        .is_some_and(|(index, _)| {
            coordinates.coordinate(index).coordinate_type() == CoordinateType::Stokes
        })
    {
        return Err(ImageError::InvalidMetadata(
            "A polarization axis cannot have a reference pixel".into(),
        ));
    }
    let (value, rendered) = match hdvalue {
        ImheadValue::Int(value) => (f64::from(*value), value.to_string()),
        ImheadValue::Float(value) if value.is_finite() => {
            (*value, projection_header::cpp_number_precision(*value, 12))
        }
        ImheadValue::String(value) => (
            value.parse::<f64>().map_err(|_| {
                ImageError::InvalidMetadata(format!("fromString failure for string {value:?}"))
            })?,
            format!("\"{value}\""),
        ),
        _ => {
            return Err(ImageError::InvalidMetadata(
                "For crpix, value must be numeric".into(),
            ));
        }
    };
    let old = match &mut image {
        AnyPagedImage::Float32(image) => image.set_reference_pixel(axis, value)?,
        AnyPagedImage::Float64(image) => image.set_reference_pixel(axis, value)?,
        AnyPagedImage::Complex32(image) => image.set_reference_pixel(axis, value)?,
        AnyPagedImage::Complex64(image) => image.set_reference_pixel(axis, value)?,
    };
    image.save()?;
    drop(image);
    let old = projection_header::cpp_number_precision(old, 12);
    crate::image::add_image_history(
        path,
        "ImageMetaDataRW::_setCoordinateValue",
        &[format!("Modified {key} from {old} to {rendered}")],
    )?;
    imhead(path)
}

/// Parse the one-based digit prefix used by CASA's image-header axis keys.
pub fn imhead_axis_index(suffix: &str) -> Option<usize> {
    let digits = suffix.bytes().take_while(u8::is_ascii_digit).count();
    suffix[..digits].parse::<usize>().ok()?.checked_sub(1)
}

/// Set `cdeltN` or `crvalN` from a CASA quantity or Stokes names.
///
/// # Errors
///
/// Returns an error for invalid or unsupported axes, quantities, metadata,
/// or image I/O. History I/O may fail after metadata has been saved.
pub fn imhead_put_axis_quantity(
    path: impl AsRef<Path>,
    hdkey: &str,
    hdvalue: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    let path = path.as_ref();
    validate_imhead_image(path)?;
    let key = hdkey.to_ascii_lowercase();
    let (prefix, suffix) = key
        .get(..5)
        .zip(key.get(5..))
        .filter(|(prefix, _)| ["cdelt", "crval"].contains(prefix))
        .ok_or_else(|| ImageError::InvalidMetadata(format!("invalid axis keyword {hdkey:?}")))?;
    let axis = imhead_axis_index(suffix)
        .ok_or_else(|| ImageError::InvalidMetadata(format!("invalid axis keyword {hdkey:?}")))?;
    let summary = imhead(path)?;
    let header = summary
        .world_axes
        .get(axis)
        .filter(|_| axis < summary.shape.len())
        .ok_or_else(|| ImageError::InvalidMetadata(format!("invalid axis keyword {hdkey:?}")))?;
    if header.coordinate_type == "Stokes" {
        if prefix == "cdelt" {
            return Err(ImageError::InvalidMetadata(
                "A polarization axis cannot have an increment".into(),
            ));
        }
        return imhead_put_stokes_values(path, &key, axis, hdvalue);
    }
    let (quantity, rendered) = quantity_input::parse(hdvalue)?;
    let value = quantity_input::in_axis_unit(&quantity, &header.unit)?;
    let old = if prefix == "cdelt" {
        header.increment
    } else {
        header.reference_value
    };
    let old = format!(
        "{}{}",
        projection_header::cpp_number_precision(old, 6),
        header.unit
    );
    let mut image = AnyPagedImage::open(path)?;
    let set_value = if prefix == "cdelt" {
        CoordinateSystem::set_world_increment_in_record
    } else {
        CoordinateSystem::set_world_reference_value_in_record
    };
    let edit = |record: &mut RecordValue| set_value(record, axis, value);
    match &mut image {
        AnyPagedImage::Float32(image) => image.edit_coordinates(edit)?,
        AnyPagedImage::Float64(image) => image.edit_coordinates(edit)?,
        AnyPagedImage::Complex32(image) => image.edit_coordinates(edit)?,
        AnyPagedImage::Complex64(image) => image.edit_coordinates(edit)?,
    };
    image.save()?;
    drop(image);
    crate::image::add_image_history(
        path,
        "ImageMetaDataRW::_setCoordinateValue",
        &[format!("Modified {key} from \"{old}\" to {rendered}")],
    )?;
    imhead(path)
}

fn imhead_put_stokes_values(
    path: &Path,
    key: &str,
    axis: usize,
    value: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    let (names, rendered) = match value {
        ImheadValue::String(value) if !value.is_empty() => {
            (vec![value.clone()], format!("\"{value}\""))
        }
        ImheadValue::List(values) if !values.is_empty() => {
            let mixed = values
                .iter()
                .any(|value| matches!(value, ImheadValue::String(_)))
                && values
                    .iter()
                    .any(|value| !matches!(value, ImheadValue::String(_)));
            if mixed {
                return Err(ImageError::InvalidMetadata(
                    "Mixed-type polarization arrays are unsupported (#977)".into(),
                ));
            }
            let names = values
                .iter()
                .map(|value| match value {
                    ImheadValue::String(value) => Ok(value.clone()),
                    _ => Err(ImageError::InvalidMetadata(
                        "Polarization values must be strings".into(),
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?;
            let rendered = format!("[{}]", names.join(", "));
            (names, rendered)
        }
        ImheadValue::Array(casa_types::ArrayValue::String(values)) if !values.is_empty() => {
            let names: Vec<String> = values.iter().cloned().collect();
            let rendered = format!("[{}]", names.join(", "));
            (names, rendered)
        }
        ImheadValue::Array(array)
            if !matches!(
                array,
                casa_types::ArrayValue::Bool(values) if values.is_empty()
            ) && !matches!(
                array,
                casa_types::ArrayValue::Int32(values) if values.is_empty()
            ) && !matches!(
                array,
                casa_types::ArrayValue::Float64(values) if values.is_empty()
            ) && !matches!(
                array,
                casa_types::ArrayValue::Complex64(values) if values.is_empty()
            ) && !matches!(
                array,
                casa_types::ArrayValue::String(values) if values.is_empty()
            ) =>
        {
            return Err(ImageError::InvalidMetadata(
                "Polarization values must be strings".into(),
            ));
        }
        _ => {
            return Err(ImageError::InvalidMetadata(
                "Polarization values must be a string or string array".into(),
            ));
        }
    };
    if names.iter().any(|name| name.contains('\0')) {
        return Err(ImageError::InvalidMetadata(
            "Polarization values containing NUL are unsupported (#977)".into(),
        ));
    }
    let mut image = AnyPagedImage::open(path)?;
    let edit = |record: &mut RecordValue| {
        CoordinateSystem::set_world_stokes_types_in_record(record, axis, &names)
    };
    let old = match &mut image {
        AnyPagedImage::Float32(image) => image.edit_coordinates(edit)?,
        AnyPagedImage::Float64(image) => image.edit_coordinates(edit)?,
        AnyPagedImage::Complex32(image) => image.edit_coordinates(edit)?,
        AnyPagedImage::Complex64(image) => image.edit_coordinates(edit)?,
    };
    image.save()?;
    drop(image);
    crate::image::add_image_history(
        path,
        "ImageMetaDataRW::_setCoordinateValue",
        &[format!(
            "Modified {key} from [{}] to {rendered}",
            old.join(", ")
        )],
    )?;
    imhead(path)
}

/// Set a CASA `restfreq` value from a quantity string or value-and-unit record.
///
/// # Errors
///
/// Returns an error for an invalid quantity, absent spectral metadata, or
/// image I/O failure. History I/O may fail after the metadata is saved.
pub fn imhead_put_rest_frequency(
    path: impl AsRef<Path>,
    hdvalue: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    let path = path.as_ref();
    validate_imhead_image(path)?;
    let summary = imhead(path)?;
    let spectral = summary
        .spectral
        .as_ref()
        .ok_or_else(|| ImageError::InvalidMetadata("Image has no spectral coordinate".into()))?;
    let (quantity, rendered) = quantity_input::parse(hdvalue)?;
    if quantity.value() <= 0.0 {
        return Err(ImageError::InvalidMetadata(
            "Rest frequency must be positive".into(),
        ));
    }
    // CASA interprets an unqualified rest frequency as Hz, unlike axis
    // increments and reference values, which default to their axis units.
    let quantity = if quantity.unit().name().is_empty() {
        casa_types::quanta::Quantity::new(quantity.value(), "Hz")
            .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?
    } else {
        quantity
    };
    let frequency = quantity_input::in_unit(&quantity, &spectral.unit)?;
    let mut image = AnyPagedImage::open(path)?;
    let old = match &mut image {
        AnyPagedImage::Float32(image) => image.set_rest_frequency(frequency)?,
        AnyPagedImage::Float64(image) => image.set_rest_frequency(frequency)?,
        AnyPagedImage::Complex32(image) => image.set_rest_frequency(frequency)?,
        AnyPagedImage::Complex64(image) => image.set_rest_frequency(frequency)?,
    };
    image.save()?;
    drop(image);
    let old = projection_header::cpp_number_precision(old, 12);
    crate::image::add_image_history(
        path,
        "ImageMetaDataRW::set",
        &[format!("Modified restfreq from {old} to {rendered}")],
    )?;
    imhead(path)
}

/// Add a rest frequency if the image does not already have a positive one.
///
/// # Errors
///
/// Returns an error if a rest frequency exists, or if the quantity or image
/// cannot be updated. History I/O may fail after the metadata is saved.
pub fn imhead_add_rest_frequency(
    path: impl AsRef<Path>,
    hdvalue: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    let path = path.as_ref();
    let summary = imhead(path)?;
    if summary
        .spectral
        .as_ref()
        .is_some_and(|spectral| spectral.rest_frequency > 0.0)
    {
        return Err(ImageError::InvalidMetadata(
            "Rest frequency already exists; it may be modified but not added".into(),
        ));
    }
    imhead_put_rest_frequency(path, hdvalue)
}

/// Set CASA's observation date from a time quantity or date string.
///
/// # Errors
///
/// Returns an error for an invalid time quantity or image I/O failure.
/// History I/O may fail after the metadata is saved.
pub fn imhead_put_observation_date(
    path: impl AsRef<Path>,
    hdkey: &str,
    hdvalue: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    use casa_types::measures::{EpochRef, MEpoch};
    use casa_types::quanta::{Unit, casacore::get_conformant_value, format};

    let path = path.as_ref();
    validate_imhead_image(path)?;
    if matches!(hdvalue, ImheadValue::String(value) if value.is_empty()) {
        return Err(ImageError::InvalidMetadata(format!(
            "{hdkey} value not specified"
        )));
    }
    let (quantity, rendered) = quantity_input::parse(hdvalue)?;
    let days = get_conformant_value(&quantity, &Unit::new("d").expect("valid day unit")).map_err(
        |_| ImageError::InvalidMetadata(format!("{hdkey} value must have units of time")),
    )?;
    // MVTime formatting uses integer calendar arithmetic. Refuse extreme
    // values before writing rather than overflowing while building the reply.
    if !valid_imhead_calendar_date(days) {
        return Err(ImageError::InvalidMetadata(
            "Non-finite or extreme observation dates are unsupported (#977)".into(),
        ));
    }
    let epoch = MEpoch::from_mjd(days, EpochRef::UTC);
    let mut image = AnyPagedImage::open(path)?;
    macro_rules! set_date {
        ($image:expr) => {{
            let old = $image
                .coordinates()
                .obs_info()
                .date
                .as_ref()
                .map_or(0.0, |epoch| epoch.value().as_mjd());
            if !valid_imhead_calendar_date(old) {
                return Err(ImageError::InvalidMetadata(
                    "Existing observation date is non-finite or extreme (#977)".into(),
                ));
            }
            $image.set_observation_date(epoch)?;
            old
        }};
    }
    let old = match &mut image {
        AnyPagedImage::Float32(image) => set_date!(image),
        AnyPagedImage::Float64(image) => set_date!(image),
        AnyPagedImage::Complex32(image) => set_date!(image),
        AnyPagedImage::Complex64(image) => set_date!(image),
    };
    image.save()?;
    drop(image);
    let old = format::format_time(old, format::MJD, 10).replacen('/', "::", 1);
    crate::image::add_image_history(
        path,
        "ImageMetaDataRW::set",
        &[format!(
            "Modified {} from \"Epoch: {old}\n\" to {rendered}",
            hdkey
        )],
    )?;
    imhead(path)
}

fn valid_imhead_calendar_date(days: f64) -> bool {
    // Casacore's ifloor decrements negative whole numbers too. MVTime uses
    // Int for the MJD and Julian day and Long for the Gregorian correction.
    let casa_floor = |value: f64| {
        if value < 0.0 {
            (value - 1.0).trunc()
        } else {
            value.trunc()
        }
    };
    let day = casa_floor(days);
    let julian_day = casa_floor(day + 2_400_001.0);
    let calendar_range = f64::from(i32::MIN)..=f64::from(i32::MAX);
    if !calendar_range.contains(&day) || !calendar_range.contains(&julian_day) {
        return false;
    }
    let gregorian = if julian_day >= 2_299_161.0 {
        let centuries = ((julian_day - 1_867_216.25) / 36_524.25).floor();
        1.0 + centuries - (centuries / 4.0).floor()
    } else {
        0.0
    };
    calendar_range.contains(&(julian_day + gregorian + 1524.0))
}

/// Set CASA's native direction reference while retaining its conversion frame.
///
/// # Errors
///
/// Returns an error for an invalid or unsupported frame or image I/O.
/// History I/O may fail after the metadata is saved.
pub fn imhead_put_direction_reference(
    path: impl AsRef<Path>,
    hdkey: &str,
    hdvalue: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    let path = path.as_ref();
    validate_imhead_image(path)?;
    let ImheadValue::String(reference) = hdvalue else {
        return Err(ImageError::InvalidMetadata(
            "equinox value must be a string".into(),
        ));
    };
    if reference.contains('\0') {
        return Err(ImageError::InvalidMetadata(
            "direction frames containing NUL are unsupported (#977)".into(),
        ));
    }
    let mut image = AnyPagedImage::open(path)?;
    let old = match &mut image {
        AnyPagedImage::Float32(image) => image.set_direction_reference(reference)?,
        AnyPagedImage::Float64(image) => image.set_direction_reference(reference)?,
        AnyPagedImage::Complex32(image) => image.set_direction_reference(reference)?,
        AnyPagedImage::Complex64(image) => image.set_direction_reference(reference)?,
    };
    if old.is_some() {
        image.save()?;
    }
    drop(image);
    if let Some(old) = old {
        crate::image::add_image_history(
            path,
            "ImageMetaDataRW::set",
            &[format!(
                "Modified {hdkey} from \"{old}\" to \"{reference}\""
            )],
        )?;
    }
    imhead(path)
}

/// Set CASA's direction projection where its coordinate model is supported.
///
/// # Errors
///
/// Returns an error for an invalid or unsupported projection or image I/O.
/// History I/O may fail after the metadata is saved.
pub fn imhead_put_projection(
    path: impl AsRef<Path>,
    hdkey: &str,
    hdvalue: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    let path = path.as_ref();
    validate_imhead_image(path)?;
    let ImheadValue::String(projection) = hdvalue else {
        return Err(ImageError::InvalidMetadata(
            "projection value must be a string".into(),
        ));
    };
    if projection.contains('\0') {
        return Err(ImageError::InvalidMetadata(
            "projection names containing NUL are unsupported (#977)".into(),
        ));
    }
    let mut image = AnyPagedImage::open(path)?;
    let old = match &mut image {
        AnyPagedImage::Float32(image) => image.set_direction_projection(projection)?,
        AnyPagedImage::Float64(image) => image.set_direction_projection(projection)?,
        AnyPagedImage::Complex32(image) => image.set_direction_projection(projection)?,
        AnyPagedImage::Complex64(image) => image.set_direction_projection(projection)?,
    };
    if old.is_some() {
        image.save()?;
    }
    drop(image);
    if let Some(old) = old {
        crate::image::add_image_history(
            path,
            "ImageMetaDataRW::set",
            &[format!(
                "Modified {hdkey} from \"{old}\" to \"{projection}\""
            )],
        )?;
    }
    imhead(path)
}

/// Set CASA's `reffreqtype`, resetting the spectral conversion frame.
///
/// # Errors
///
/// Returns an error for an invalid frame, absent or malformed spectral
/// metadata, or image I/O failure. History I/O may fail after metadata is saved.
pub fn imhead_put_spectral_reference(
    path: impl AsRef<Path>,
    hdkey: &str,
    hdvalue: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    let path = path.as_ref();
    validate_imhead_image(path)?;
    let ImheadValue::String(reference) = hdvalue else {
        return Err(ImageError::InvalidMetadata(
            "reffreqtype value must be a string".into(),
        ));
    };
    if reference.contains('\0') {
        return Err(ImageError::InvalidMetadata(
            "spectral frames containing NUL are unsupported (#977)".into(),
        ));
    }
    let mut image = AnyPagedImage::open(path)?;
    let old = match &mut image {
        AnyPagedImage::Float32(image) => image.set_spectral_reference(reference)?,
        AnyPagedImage::Float64(image) => image.set_spectral_reference(reference)?,
        AnyPagedImage::Complex32(image) => image.set_spectral_reference(reference)?,
        AnyPagedImage::Complex64(image) => image.set_spectral_reference(reference)?,
    };
    if old.is_some() {
        image.save()?;
    }
    drop(image);
    if let Some(old) = old {
        crate::image::add_image_history(
            path,
            "ImageMetaDataRW::set",
            &[format!(
                "Modified {hdkey} from \"{old}\" to \"{reference}\""
            )],
        )?;
    }
    imhead(path)
}

/// Add, modify, or remove restoring-beam header metadata.
///
/// The boolean indicates whether CASA would append task history. In
/// particular, deleting a beam from an image without one is a no-op.
///
/// # Errors
///
/// Returns an error for invalid quantities, unsupported operations, malformed
/// beam metadata, or image I/O failure. History I/O may fail after saving.
pub fn imhead_change_beam(
    path: impl AsRef<Path>,
    mode: &str,
    hdkey: &str,
    hdvalue: &ImheadValue,
) -> Result<(ImageHeaderSummary, bool), ImageError> {
    let path = path.as_ref();
    validate_imhead_image(path)?;
    let key = hdkey.to_ascii_lowercase();
    let component = match key.as_str() {
        "beammajor" | "bmaj" => crate::BeamComponent::Major,
        "beamminor" | "bmin" => crate::BeamComponent::Minor,
        "beampa" | "bpa" => crate::BeamComponent::PositionAngle,
        _ => {
            return Err(ImageError::InvalidMetadata(format!(
                "invalid beam keyword {hdkey:?}"
            )));
        }
    };
    let mut image = AnyPagedImage::open(path)?;
    let history = match &mut image {
        AnyPagedImage::Float32(image) => edit_beam(image, mode, hdkey, component, hdvalue)?,
        AnyPagedImage::Float64(image) => edit_beam(image, mode, hdkey, component, hdvalue)?,
        AnyPagedImage::Complex32(image) => edit_beam(image, mode, hdkey, component, hdvalue)?,
        AnyPagedImage::Complex64(image) => edit_beam(image, mode, hdkey, component, hdvalue)?,
    };
    if history.is_some() {
        image.save()?;
    }
    drop(image);
    if let Some((origin, message)) = history {
        crate::image::add_image_history(path, origin, &[message])?;
        Ok((imhead(path)?, true))
    } else {
        Ok((imhead(path)?, false))
    }
}

fn edit_beam<T: ImagePixel>(
    image: &mut PagedImage<T>,
    mode: &str,
    hdkey: &str,
    component: crate::BeamComponent,
    hdvalue: &ImheadValue,
) -> Result<Option<(&'static str, String)>, ImageError> {
    use crate::BeamComponent;
    let info = image.image_info()?;
    if mode == "del" {
        let multiple = info.beam_set.is_multi();
        if !image.remove_restoring_beams()? {
            return Ok(None);
        }
        let message = if multiple {
            "Removed image's multiple beams"
        } else {
            "Removed image's single beam"
        };
        return Ok(Some(("ImageMetaDataRW::remove", message.into())));
    }
    if mode != "add" && mode != "put" {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid beam operation {mode:?}"
        )));
    }
    if mode == "add" && component == BeamComponent::PositionAngle {
        return Err(ImageError::InvalidMetadata(
            "Cannot add a beam position angle".into(),
        ));
    }
    if mode == "add" && !info.beam_set.is_empty() {
        return Err(ImageError::InvalidMetadata(
            "This image already has a beam(s)".into(),
        ));
    }
    if info.beam_set.is_multi() {
        return Err(ImageError::InvalidMetadata(
            "This image has multiple beams".into(),
        ));
    }
    let (quantity, rendered) = quantity_input::parse(hdvalue)?;
    let old = image.stored_restoring_beam()?;
    let quantity_text = |quantity: &casa_types::quanta::Quantity| {
        format!(
            "{} {}",
            projection_header::cpp_number_precision(quantity.value(), 6),
            quantity.unit().name()
        )
    };
    let (beam, message) = if let Some(old) = old {
        let suffix = if component == BeamComponent::PositionAngle {
            ""
        } else {
            "\n"
        };
        let previous = format!("{}{suffix}", quantity_text(old.component(component)));
        let beam = old.with_component(component, quantity)?;
        (
            beam,
            format!("Modified {hdkey} from \"{previous}\" to {rendered}"),
        )
    } else {
        if component == BeamComponent::PositionAngle {
            return Err(ImageError::InvalidMetadata(
                "This image has no beam; add a beam axis before its position angle".into(),
            ));
        }
        let beam = crate::StoredGaussianBeam::new(
            quantity.clone(),
            quantity,
            casa_types::quanta::Quantity::new(0.0, "deg").expect("built-in degrees"),
        )?;
        let message = format!(
            "Added beam major: {}, minor: {}, pa: {}",
            quantity_text(beam.component(BeamComponent::Major)),
            quantity_text(beam.component(BeamComponent::Minor)),
            quantity_text(beam.component(BeamComponent::PositionAngle))
        );
        (beam, message)
    };
    image.set_stored_restoring_beam(&beam)?;
    Ok(Some(("ImageMetaDataRW::set", message)))
}

/// Set a CASA `ctypeN` axis name and record the image history.
///
/// # Errors
///
/// Returns an error for an invalid axis or value, malformed metadata, or an
/// image I/O failure. History I/O may fail after the metadata is saved.
pub fn imhead_put_axis_name(
    path: impl AsRef<Path>,
    hdkey: &str,
    hdvalue: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    imhead_put_axis_text(path.as_ref(), hdkey, hdvalue, "ctype")
}

/// Set a CASA `cunitN` axis unit and rescale its coordinate values.
///
/// # Errors
///
/// Returns an error for invalid units, unsupported axes, malformed metadata,
/// or image I/O failure. History I/O may fail after the metadata is saved.
pub fn imhead_put_axis_unit(
    path: impl AsRef<Path>,
    hdkey: &str,
    hdvalue: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    imhead_put_axis_text(path.as_ref(), hdkey, hdvalue, "cunit")
}

fn imhead_put_axis_text(
    path: &Path,
    hdkey: &str,
    hdvalue: &ImheadValue,
    prefix: &str,
) -> Result<ImageHeaderSummary, ImageError> {
    validate_imhead_image(path)?;
    let key = hdkey.to_ascii_lowercase();
    let axis = key
        .strip_prefix(prefix)
        .and_then(imhead_axis_index)
        .ok_or_else(|| ImageError::InvalidMetadata(format!("invalid axis keyword {hdkey:?}")))?;
    let mut image = AnyPagedImage::open(path)?;
    let coordinates = image.coordinates();
    let (index, _) = coordinates
        .world_axis_owner(axis)
        .filter(|_| axis < image.shape().len())
        .ok_or_else(|| ImageError::InvalidMetadata(format!("invalid axis keyword {hdkey:?}")))?;
    if prefix == "cunit"
        && coordinates.coordinate(index).coordinate_type() == CoordinateType::Stokes
    {
        return Err(ImageError::InvalidMetadata(
            "A polarization axis cannot have a unit".into(),
        ));
    }
    let ImheadValue::String(name) = hdvalue else {
        return Err(ImageError::InvalidMetadata(format!(
            "{key} value must be a string"
        )));
    };
    if name.contains('\0') {
        return Err(ImageError::InvalidMetadata(
            "Axis names and units containing NUL are not supported (#977)".into(),
        ));
    }
    let set_label = if prefix == "cunit" {
        CoordinateSystem::set_world_axis_unit_in_record
    } else {
        CoordinateSystem::set_world_axis_name_in_record
    };
    let edit = |record: &mut RecordValue| set_label(record, axis, name);
    let old = match &mut image {
        AnyPagedImage::Float32(image) => image.edit_coordinates(edit)?,
        AnyPagedImage::Float64(image) => image.edit_coordinates(edit)?,
        AnyPagedImage::Complex32(image) => image.edit_coordinates(edit)?,
        AnyPagedImage::Complex64(image) => image.edit_coordinates(edit)?,
    };
    image.save()?;
    drop(image);
    crate::image::add_image_history(
        path,
        "ImageMetaDataRW::_setCoordinateValue",
        &[format!("Modified {key} from \"{old}\" to \"{name}\"")],
    )?;
    imhead(path)
}

/// Add or remove a supported string-valued CASA image header field.
/// For `del` of `masks`, an empty value removes all masks.
pub fn imhead_add_delete(
    path: impl AsRef<Path>,
    mode: &str,
    hdkey: &str,
    hdvalue: &str,
) -> Result<ImageHeaderSummary, ImageError> {
    if !["add", "del"].contains(&mode) {
        return Err(ImageError::InvalidMetadata(format!(
            "unknown imhead operation {mode:?}"
        )));
    }
    if mode == "del" && hdkey == "masks" {
        let path = path.as_ref();
        validate_imhead_writable(path)?;
        imhead_masks::remove(path, hdvalue)?;
        return imhead(path);
    }
    imhead_change_string(path, mode, hdkey, if mode == "del" { "" } else { hdvalue })
}

/// Whether a name is available for CASA's user-defined metadata fields.
pub fn imhead_is_custom_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    !["cdelt", "crpix", "crval", "ctype", "cunit"]
        .iter()
        .any(|prefix| lower.starts_with(prefix))
        && ![
            "bunit",
            "object",
            "imtype",
            "observer",
            "telescope",
            "date-obs",
            "epoch",
            "equinox",
            "projection",
            "reffreqtype",
            "restfreq",
            "masks",
            "shape",
            "datamin",
            "datamax",
            "minpixpos",
            "maxpixpos",
            "minpos",
            "maxpos",
            "beammajor",
            "beamminor",
            "beampa",
            "bmaj",
            "bmin",
            "bpa",
        ]
        .contains(&lower.as_str())
}

/// Read a user-defined scalar field, preserving its stored casacore type.
/// CASA checks the exact field name first and then its lowercase spelling.
///
/// # Errors
///
/// Returns an error for a missing field, a type CASA cannot read, an
/// unsupported field type, or an unreadable image.
pub fn imhead_custom_value(path: impl AsRef<Path>, key: &str) -> Result<Value, ImageError> {
    let path = path.as_ref();
    if path.is_file() {
        return imhead_fits::custom_value(path, key);
    }
    fn get<T: ImagePixel>(image: &PagedImage<T>, key: &str) -> Result<Value, ImageError> {
        let info = image.misc_info();
        let value = info
            .get(key)
            .or_else(|| info.get(&key.to_ascii_lowercase()))
            .ok_or_else(|| imhead_unknown_keyword(key))?;
        match value {
            Value::Scalar(ScalarValue::String(_))
            | Value::Scalar(ScalarValue::Int32(_))
            | Value::Scalar(ScalarValue::Float64(_))
            | Value::Record(_) => Ok(value.clone()),
            Value::Scalar(ScalarValue::Bool(_)) => Err(ImageError::InvalidMetadata(
                "Unhandled data type Bool for user defined type. Send us a bug report".into(),
            )),
            Value::Array(array)
                if matches!(
                    array,
                    casa_types::ArrayValue::Bool(_)
                        | casa_types::ArrayValue::Int32(_)
                        | casa_types::ArrayValue::Float64(_)
                        | casa_types::ArrayValue::String(_)
                ) =>
            {
                let kind = match array {
                    casa_types::ArrayValue::Bool(_) => "Bool",
                    casa_types::ArrayValue::Int32(_) => "Int",
                    casa_types::ArrayValue::Float64(_) => "double",
                    casa_types::ArrayValue::String(_) => "String",
                    _ => unreachable!(),
                };
                Err(ImageError::InvalidMetadata(format!(
                    "Unhandled data type Array<{kind}> for user defined type. Send us a bug report"
                )))
            }
            _ => Err(ImageError::InvalidMetadata(format!(
                "Reading this user-defined value is not implemented (#977): {key}"
            ))),
        }
    }
    let image = AnyPagedImage::open(path)?;
    match &image {
        AnyPagedImage::Float32(image) => get(image, key),
        AnyPagedImage::Float64(image) => get(image, key),
        AnyPagedImage::Complex32(image) => get(image, key),
        AnyPagedImage::Complex64(image) => get(image, key),
    }
}

fn imhead_unknown_keyword(key: &str) -> ImageError {
    ImageError::InvalidMetadata(format!(
        "Unknown keyword {key}. If you are trying to use a key name from the imhead summary \
         dictionary, note that some keys in mode='put'/'get' are different from mode='summary'. \
         Please see imhead description in the CASA online documentation for complete details."
    ))
}

/// Write or remove user-defined scalar, record, and array metadata.
///
/// `mode` is `"add"`, `"put"`, or `"del"`. Adds refuse an existing field;
/// puts preserve its existing type. Values may be strings, booleans, 32-bit
/// integers, finite doubles, or rectangular arrays of those. Deleting ignores `value`.
///
/// # Errors
///
/// Returns an error for a reserved key, unsupported value, duplicate add,
/// missing delete, incompatible existing field, or image I/O failure. The
/// metadata may already be updated if appending image history fails, as in
/// CASA's metadata writer.
pub fn imhead_custom_value_change(
    path: impl AsRef<Path>,
    mode: &str,
    key: &str,
    value: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    let path = path.as_ref();
    validate_imhead_image(path)?;
    if !imhead_is_custom_key(key) || !["add", "put", "del"].contains(&mode) {
        return Err(ImageError::InvalidMetadata(format!(
            "Unsupported custom metadata operation {mode:?} for {key:?} (#977)"
        )));
    }
    let (stored, rendered) = if mode == "del" {
        (
            Value::Scalar(ScalarValue::String(String::new())),
            String::new(),
        )
    } else {
        match value {
            ImheadValue::String(value) => (
                Value::Scalar(ScalarValue::String(value.clone())),
                format!("\"{value}\""),
            ),
            ImheadValue::Int(value) => {
                (Value::Scalar(ScalarValue::Int32(*value)), value.to_string())
            }
            ImheadValue::Float(value) => (
                Value::Scalar(ScalarValue::Float64(*value)),
                self::projection_header::cpp_number_precision(*value, 12),
            ),
            ImheadValue::Bool(value) => (
                Value::Scalar(ScalarValue::Bool(*value)),
                if *value { "1" } else { "0" }.to_string(),
            ),
            ImheadValue::Array(array) => {
                if matches!(array, casa_types::ArrayValue::Complex64(_)) {
                    return Err(ImageError::InvalidMetadata(format!(
                        "Unhandled value type Array<DComplex> for {key}"
                    )));
                }
                let rendered = custom_values::format_array(array);
                (Value::Array(array.clone()), rendered)
            }
            ImheadValue::Record(record) => {
                let rendered = custom_values::format_record(record);
                (Value::Record(record.clone()), rendered)
            }
            _ => {
                return Err(ImageError::InvalidMetadata(
                    "This user-defined value type is not implemented (#977)".into(),
                ));
            }
        }
    };
    fn change<T: ImagePixel>(
        image: &mut PagedImage<T>,
        mode: &str,
        key: &str,
        value: &Value,
        rendered: &str,
    ) -> Result<String, ImageError> {
        let mut info = image.misc_info();
        let lower = key.to_ascii_lowercase();
        let existing = if info.get(key).is_some() {
            Some(key)
        } else if info.get(&lower).is_some() {
            Some(lower.as_str())
        } else {
            None
        };
        let message = match mode {
            "add" if existing.is_some() => {
                return Err(ImageError::InvalidMetadata(format!(
                    "Keyword {key} already exists so cannot be added."
                )));
            }
            "del" => {
                let name = existing.ok_or_else(|| {
                    ImageError::InvalidMetadata(format!("Unknown keyword {lower}"))
                })?;
                info.remove(name);
                format!("Removed user-defined keyword {name}")
            }
            _ => {
                if info.get(key).is_some_and(|old| {
                    old.kind() != value.kind() || old.type_tag() != value.type_tag()
                }) {
                    return Err(ImageError::InvalidMetadata(format!(
                        "incorrect data type used for field {key}"
                    )));
                }
                info.upsert(key, value.clone());
                format!("Added {key} = {rendered}")
            }
        };
        image.set_misc_info(info)?;
        Ok(message)
    }
    let mut image = AnyPagedImage::open(path)?;
    let message = match &mut image {
        AnyPagedImage::Float32(image) => change(image, mode, key, &stored, &rendered)?,
        AnyPagedImage::Float64(image) => change(image, mode, key, &stored, &rendered)?,
        AnyPagedImage::Complex32(image) => change(image, mode, key, &stored, &rendered)?,
        AnyPagedImage::Complex64(image) => change(image, mode, key, &stored, &rendered)?,
    };
    image.save()?;
    drop(image);
    let origin = if mode == "del" {
        "ImageMetaDataRW::remove"
    } else {
        "ImageMetaDataRW::_setUserDefined"
    };
    crate::image::add_image_history(path, origin, &[message])?;
    imhead(path)
}

fn imhead_change_string(
    path: impl AsRef<Path>,
    mode: &str,
    hdkey: &str,
    hdvalue: &str,
) -> Result<ImageHeaderSummary, ImageError> {
    let path = path.as_ref();
    if imhead_is_custom_key(hdkey) {
        return imhead_custom_value_change(
            path,
            mode,
            hdkey,
            &ImheadValue::String(hdvalue.to_string()),
        );
    }
    validate_imhead_image(path)?;
    let key = hdkey.to_ascii_lowercase();
    if !["bunit", "object", "observer", "telescope"].contains(&key.as_str())
        && !(mode == "put" && key == "imtype")
    {
        return Err(ImageError::InvalidMetadata(format!(
            "imhead mode={mode:?} does not yet support hdkey={hdkey:?} (#977)"
        )));
    }
    let mut image = AnyPagedImage::open(path)?;
    let old = match &image {
        AnyPagedImage::Float32(image) => header_string(image, &key)?,
        AnyPagedImage::Float64(image) => header_string(image, &key)?,
        AnyPagedImage::Complex32(image) => header_string(image, &key)?,
        AnyPagedImage::Complex64(image) => header_string(image, &key)?,
    };
    if mode == "add" && !old.is_empty() {
        return Err(ImageError::InvalidMetadata(format!(
            "{hdkey} is already present and has value {old}. It may be modified but not added."
        )));
    }
    let value = if key == "bunit" {
        let unit = casa_types::quanta::unit::normalize_casa_unit_name(hdvalue);
        casa_types::quanta::Unit::new(&unit)
            .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?;
        unit
    } else if key == "imtype" {
        hdvalue
            .parse::<ImageType>()
            .ok()
            .filter(|kind| kind.to_string().eq_ignore_ascii_case(hdvalue))
            .unwrap_or(ImageType::Intensity)
            .to_string()
    } else {
        hdvalue.to_string()
    };
    match &mut image {
        AnyPagedImage::Float32(image) => put_header_string(image, &key, &value)?,
        AnyPagedImage::Float64(image) => put_header_string(image, &key, &value)?,
        AnyPagedImage::Complex32(image) => put_header_string(image, &key, &value)?,
        AnyPagedImage::Complex64(image) => put_header_string(image, &key, &value)?,
    }
    image.save()?;
    drop(image);
    let origin = match (mode, key.as_str()) {
        ("del", _) => "ImageMetaDataRW::remove",
        ("add", "bunit") => "ImageMetaDataRW::add",
        _ => "ImageMetaDataRW::set",
    };
    let history_key = if mode == "put" { hdkey } else { &key };
    crate::image::add_image_history(
        path,
        origin,
        &[format!(
            "Modified {history_key} from \"{old}\" to \"{hdvalue}\""
        )],
    )?;
    imhead(path)
}

/// Validate image metadata required when CASA opens an image for `imhead`.
///
/// # Errors
///
/// Returns an error for an unreadable image or an invalid restoring-beam set.
pub fn validate_imhead_image(path: impl AsRef<Path>) -> Result<(), ImageError> {
    let path = path.as_ref();
    if path.is_file() {
        PrimaryHdu::open(path)?;
        return Ok(());
    }
    fn validate<T: ImagePixel>(image: &PagedImage<T>) -> Result<(), ImageError> {
        let info = image.image_info()?;
        per_plane_beam_summary(image, &info.beam_set)?;
        Ok(())
    }
    match &AnyPagedImage::open(path)? {
        AnyPagedImage::Float32(image) => validate(image),
        AnyPagedImage::Float64(image) => validate(image),
        AnyPagedImage::Complex32(image) => validate(image),
        AnyPagedImage::Complex64(image) => validate(image),
    }
}

/// Validate metadata and require a writable CASA image for an imhead edit.
///
/// # Errors
///
/// Returns an error for unreadable metadata or a read-only FITS image.
pub fn validate_imhead_writable(path: impl AsRef<Path>) -> Result<(), ImageError> {
    let path = path.as_ref();
    validate_imhead_image(path)?;
    if path.is_file() {
        return Err(ImageError::ReadOnly("FITS image"));
    }
    Ok(())
}

fn header_string<T: ImagePixel>(image: &PagedImage<T>, key: &str) -> Result<String, ImageError> {
    Ok(match key {
        "bunit" => image.units().to_string(),
        "object" => image.image_info()?.object_name,
        "imtype" => header_image_type(image)?.to_string(),
        "observer" => image.coordinates().obs_info().observer.clone(),
        "telescope" => image.coordinates().obs_info().telescope.clone(),
        _ => unreachable!("the caller checked the key"),
    })
}

// casacore defaults ImageInfo to Intensity when its record or type is absent.
fn header_image_type<T: ImagePixel>(image: &PagedImage<T>) -> Result<ImageType, ImageError> {
    if image.has_image_type_record() {
        Ok(image.image_info()?.image_type)
    } else {
        Ok(ImageType::Intensity)
    }
}

fn put_header_string<T: ImagePixel>(
    image: &mut PagedImage<T>,
    key: &str,
    value: &str,
) -> Result<(), ImageError> {
    match key {
        "bunit" => image.set_units(value),
        "object" => {
            image.set_image_info_string("objectname", value);
            Ok(())
        }
        "imtype" => {
            image.set_image_info_string("imagetype", value);
            Ok(())
        }
        "observer" | "telescope" => image.set_observation_string(key, value),
        _ => unreachable!("the caller checked the key"),
    }
}

fn imhead_typed<T>(
    path: &Path,
    image: &PagedImage<T>,
    pixel_type: String,
) -> Result<ImageHeaderSummary, ImageError>
where
    T: ImagePixel,
{
    let info = image.image_info()?;
    let default_mask = image.default_mask_name();
    let has_mask = default_mask.is_some();
    Ok(ImageHeaderSummary {
        imagename: path.display().to_string(),
        pixel_type,
        shape: image.shape().to_vec(),
        tile_shape: image.tile_shape().to_vec(),
        units: image.units().to_string(),
        object_name: info.object_name.clone(),
        observer: image.coordinates().obs_info().observer.clone(),
        telescope: image.coordinates().obs_info().telescope.clone(),
        observation_date: casa_types::quanta::format::format_time(
            image
                .coordinates()
                .obs_info()
                .date
                .as_ref()
                .map_or(0.0, |epoch| epoch.value().as_mjd()),
            casa_types::quanta::format::YMD,
            12,
        ),
        direction_reference: (0..image.coordinates().n_coordinates()).find_map(|index| match image
            .coordinates()
            .coordinate(index)
        {
            casa_coordinates::CoordinateModel::Direction(direction) => {
                Some(direction.direction_ref().as_str().to_string())
            }
            _ => None,
        }),
        direction_projection: (0..image.coordinates().n_coordinates()).find_map(
            |index| match image.coordinates().coordinate(index) {
                casa_coordinates::CoordinateModel::Direction(direction) => {
                    Some(projection_header::projection_header(direction))
                }
                _ => None,
            },
        ),
        spectral: spectral_header(image)?.map(Box::new),
        image_type: header_image_type(image)?.to_string(),
        default_mask,
        has_mask,
        masks: image.mask_names(),
        regions: image.region_names(),
        restoring_beam: info
            .beam_set
            .single_beam()
            .and_then(|beam| format_beam(beam, image.restoring_beam_record()))
            .map(Box::new),
        has_multiple_beams: info.beam_set.is_multi(),
        per_plane_beams: per_plane_beam_summary(image, &info.beam_set)?.map(Box::new),
        extrema: None,
        axes: axis_summaries(image.coordinates(), image.shape(), false),
        world_axes: axis_summaries(image.coordinates(), image.shape(), true),
    })
}

fn per_plane_beam_summary<T: ImagePixel>(
    image: &PagedImage<T>,
    beam_set: &ImageBeamSet,
) -> Result<Option<PerPlaneBeamSummary>, ImageError> {
    summarize_beam_set(
        image.coordinates(),
        image.shape(),
        beam_set,
        image.per_plane_beam_record(),
    )
}

fn summarize_beam_set(
    coordinates: &CoordinateSystem,
    shape: &[usize],
    beam_set: &ImageBeamSet,
    beam_record: Option<&RecordValue>,
) -> Result<Option<PerPlaneBeamSummary>, ImageError> {
    if beam_set.single_beam().is_some_and(|beam| beam.is_null()) {
        return Err(ImageError::InvalidMetadata(
            "Invalid per plane beam found".into(),
        ));
    }
    if !beam_set.is_multi() {
        return Ok(None);
    }
    let channels = beam_set.n_channels();
    let stokes = beam_set.n_stokes();
    let mut image_channels = 1;
    let mut image_stokes = 1;
    for index in 0..coordinates.n_coordinates() {
        let coordinate = coordinates.coordinate(index);
        let extent = || {
            coordinates
                .coordinate_pixel_axes(index)
                .and_then(|axes| axes.into_iter().flatten().next())
                .and_then(|axis| shape.get(axis).copied())
                .unwrap_or(1)
        };
        match coordinate.coordinate_type() {
            CoordinateType::Spectral => image_channels = extent(),
            CoordinateType::Stokes => image_stokes = extent(),
            _ => {}
        }
    }
    if channels != 1 && channels != image_channels {
        return Err(ImageError::InvalidMetadata(
            "Number of channels is not consistent".into(),
        ));
    }
    if stokes != 1 && stokes != image_stokes {
        return Err(ImageError::InvalidMetadata(
            "Number of polarizations is not consistent".into(),
        ));
    }
    let mut beams = Vec::with_capacity(channels * stokes);
    let mut areas = Vec::with_capacity(channels * stokes);
    let area_unit = casa_types::quanta::Unit::new("arcsec2")
        .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?;
    for polarization in 0..stokes {
        for channel in 0..channels {
            let index = polarization * channels + channel;
            let beam = *beam_set.beam(channel, polarization);
            let raw = beam_record
                .and_then(|record| record.get(&format!("*{index}")))
                .and_then(|value| match value {
                    Value::Record(record) => Some(record),
                    _ => None,
                });
            let formatted = format_beam(beam, raw).ok_or_else(|| {
                ImageError::InvalidMetadata("Invalid per plane beam found".into())
            })?;
            // CASA multiplies the stored quantities before converting their
            // product to arcsec². That order affects ties in the median.
            let major_unit = casa_types::quanta::Unit::new(&formatted.major.unit)
                .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?;
            let minor_unit = casa_types::quanta::Unit::new(&formatted.minor.unit)
                .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?;
            let ratio =
                area_unit.val().factor / (major_unit.val().factor * minor_unit.val().factor);
            let area = std::f64::consts::PI / (4.0 * 2.0_f64.ln())
                * (formatted.major.value * formatted.minor.value / ratio);
            beams.push(formatted);
            areas.push((index, area));
        }
    }
    areas.sort_by(|left, right| left.1.total_cmp(&right.1));
    let median_index = areas[areas.len() / 2].0;
    Ok(Some(PerPlaneBeamSummary {
        channels,
        stokes,
        beams,
        median_index,
    }))
}

fn spectral_header<T: ImagePixel>(
    image: &PagedImage<T>,
) -> Result<Option<SpectralHeaderSummary>, ImageError> {
    let Some(spectral) = image.first_spectral_coordinate()? else {
        return Ok(None);
    };
    Ok(Some(spectral_summary(&spectral)))
}

fn spectral_summary(spectral: &casa_coordinates::SpectralCoordinate) -> SpectralHeaderSummary {
    let unit = spectral
        .axis_units()
        .into_iter()
        .next()
        .unwrap_or_else(|| "Hz".into());
    // The stored record's rest frequencies are already in its axis unit.
    SpectralHeaderSummary {
        reference_frame: if spectral.has_undefined_frame() {
            "Undefined".into()
        } else {
            spectral.frequency_ref().as_str().into()
        },
        unit,
        rest_frequency: spectral.rest_frequency(),
        rest_frequencies: spectral.rest_frequencies().to_vec(),
    }
}

fn format_beam(beam: GaussianBeam, record: Option<&RecordValue>) -> Option<RestoringBeamSummary> {
    if beam.is_null() {
        return None;
    }
    let major_arcsec = beam.major_in("arcsec").ok()?;
    let minor_arcsec = beam.minor_in("arcsec").ok()?;
    let position_angle_deg = beam.position_angle_in("deg").ok()?;
    let fallback = |value, unit: &str| BeamQuantity {
        value,
        unit: unit.into(),
    };
    let major = record
        .and_then(|r| beam_quantity(r, "major"))
        .unwrap_or_else(|| fallback(major_arcsec, "arcsec"));
    let minor = record
        .and_then(|r| beam_quantity(r, "minor"))
        .unwrap_or_else(|| fallback(minor_arcsec, "arcsec"));
    let position_angle = record
        .and_then(|r| beam_quantity(r, "positionangle"))
        .unwrap_or_else(|| fallback(position_angle_deg, "deg"));
    let quantity =
        casa_types::quanta::Quantity::new(position_angle.value, &position_angle.unit).ok()?;
    let wrapped = crate::beam::wrap_position_angle(&quantity).ok()?;
    let wrapped_position_angle = BeamQuantity {
        value: wrapped.value(),
        unit: wrapped.unit().name().into(),
    };
    Some(RestoringBeamSummary {
        major_arcsec,
        minor_arcsec,
        position_angle_deg,
        major,
        minor,
        position_angle,
        wrapped_position_angle,
    })
}

fn beam_quantity(record: &RecordValue, key: &str) -> Option<BeamQuantity> {
    let Value::Record(quantity) = record.get(key)? else {
        return None;
    };
    let quantity = crate::beam::quantity_from_record(quantity).ok()?;
    Some(BeamQuantity {
        value: quantity.value(),
        unit: casa_types::quanta::unit::normalize_casa_unit_name(quantity.unit().name()),
    })
}

fn axis_summaries(
    coordinates: &CoordinateSystem,
    shape: &[usize],
    world: bool,
) -> Vec<ImageAxisSummary> {
    let mut summaries = Vec::new();
    for coord_idx in 0..coordinates.n_coordinates() {
        let coord = coordinates.coordinate(coord_idx);
        let mut refs = coord.reference_value();
        let refpix = coord.reference_pixel();
        let mut increments = coord.increment();
        let names = coord.axis_names();
        let mut units = coord.axis_units();
        // A direction coordinate presents its values in the units it is
        // stored in, as casacore's does, though it computes in radians.
        if let casa_coordinates::CoordinateModel::Direction(direction) = coord {
            (refs, increments) = direction.reference_and_increment_in_world_units();
            units = direction.visible_world_units();
        }
        let pixel_axes = coordinates
            .coordinate_pixel_axes(coord_idx)
            .unwrap_or_default();
        let world_axes;
        let axes = if world {
            world_axes = coordinates
                .coordinate_world_axes(coord_idx)
                .unwrap_or_default();
            &world_axes
        } else {
            &pixel_axes
        };
        for (axis_in_coord, &axis) in axes.iter().enumerate() {
            let Some(axis) = axis else {
                continue;
            };
            summaries.push(ImageAxisSummary {
                axis,
                coordinate_type: format!("{:?}", coord.coordinate_type()),
                name: names
                    .get(axis_in_coord)
                    .cloned()
                    .unwrap_or_else(|| format!("Axis{axis}")),
                shape: pixel_axes
                    .get(axis_in_coord)
                    .copied()
                    .flatten()
                    .and_then(|pixel| shape.get(pixel))
                    .copied()
                    .unwrap_or(0),
                reference_value: *refs.get(axis_in_coord).unwrap_or(&0.0),
                reference_pixel: *refpix.get(axis_in_coord).unwrap_or(&0.0),
                increment: *increments.get(axis_in_coord).unwrap_or(&0.0),
                unit: units.get(axis_in_coord).cloned().unwrap_or_default(),
                stokes: match coord {
                    casa_coordinates::CoordinateModel::Stokes(stokes) => {
                        stokes.stokes().iter().map(ToString::to_string).collect()
                    }
                    _ => Vec::new(),
                },
            });
        }
    }
    summaries.sort_unstable_by_key(|axis| axis.axis);
    if world {
        return summaries;
    }
    for (axis, &extent) in shape.iter().enumerate().skip(summaries.len()) {
        summaries.push(ImageAxisSummary {
            axis,
            coordinate_type: "Linear".to_string(),
            name: format!("Axis{axis}"),
            shape: extent,
            reference_value: 0.0,
            reference_pixel: 0.0,
            increment: 1.0,
            unit: String::new(),
            stokes: Vec::new(),
        });
    }
    summaries
}

fn imstat_typed<T>(
    path: &Path,
    image: &PagedImage<T>,
    request: &ImstatRequest,
) -> Result<ImstatOutcome, ImageError>
where
    T: ImagePixel + ImageExprValue + PartialOrd + ExprValueConvert + StatsElement,
{
    let algorithm = parse_imstat_algorithm(request).map_err(ImageError::InvalidMetadata)?;
    let axes =
        parse_axes(request.axes.as_deref(), image.ndim()).map_err(ImageError::CasaSelection)?;
    let region_selection = region_pixels(image, request.region.as_deref())?;
    if request
        .box_pixels
        .as_deref()
        .is_some_and(|text| !text.trim().is_empty())
        && region_selection.is_some()
    {
        return Err(ImageError::InvalidMetadata(
            "imstat accepts either box or region, not both".to_string(),
        ));
    }
    let explicit_box = match request
        .box_pixels
        .as_deref()
        .filter(|text| !text.trim().is_empty())
    {
        Some(box_text) => Some(parse_box_region(
            box_text,
            image.shape(),
            direction_plane_axes(image)?,
        )?),
        None => None,
    };
    let effective_region = explicit_box.as_ref().or(region_selection.as_ref());
    let selection = Selection::new(
        image,
        effective_region,
        request.chans.as_deref(),
        request.stokes.as_deref(),
    )?;
    let pixel_mask = selection_pixel_mask(
        image,
        effective_region,
        &selection,
        request.mask.as_deref(),
        request.stretch,
    )?;
    let coordinates = image.coordinates();
    let world_text = |position: &[usize]| -> Result<String, ImageError> {
        let pixel: Vec<f64> = position.iter().map(|&value| value as f64).collect();
        Ok(casa_coordinates::format_world_display(coordinates, &pixel)?)
    };
    let blc = selection.start.clone();
    let trc = trc_pixels(&selection.start, &selection.shape);
    let blcf = world_text(&blc)?;
    let trcf = world_text(&trc)?;

    let display_axes: Vec<usize> = match &axes {
        AxisSelection::All => Vec::new(),
        AxisSelection::Collapsed(collapse) => (0..image.ndim())
            .filter(|axis| !collapse.contains(axis))
            .collect(),
    };
    if display_axes.is_empty() {
        let summary = imstat_global_summary(
            path,
            image,
            request,
            &algorithm,
            &selection,
            pixel_mask.as_ref(),
            blc,
            trc,
            blcf,
            trcf,
            &world_text,
        )?;
        Ok(ImstatOutcome::Global(summary))
    } else {
        let summary = imstat_planes_summary(
            path,
            image,
            request,
            &algorithm,
            &selection,
            pixel_mask.as_ref(),
            &display_axes,
            blc,
            trc,
            blcf,
            trcf,
        )?;
        Ok(ImstatOutcome::Planes(summary))
    }
}

/// The whole selection's statistics, as `imstat` reports them with every
/// axis collapsed.
#[allow(clippy::too_many_arguments)]
fn imstat_global_summary<T>(
    path: &Path,
    image: &PagedImage<T>,
    request: &ImstatRequest,
    algorithm: &StatisticsAlgorithm,
    selection: &Selection,
    pixel_mask: Option<&ndarray::ArrayD<bool>>,
    blc: Vec<usize>,
    trc: Vec<usize>,
    blcf: String,
    trcf: String,
    world_text: &dyn Fn(&[usize]) -> Result<String, ImageError>,
) -> Result<ImageStatisticsSummary, ImageError>
where
    T: ImagePixel + StatsElement,
{
    // The statistics the algorithm reports, with the extremum positions
    // relative to the original image.
    let computed = match algorithm {
        StatisticsAlgorithm::Classic
            if !selection.stokes_has_gaps() && !selection.channels_have_gaps() =>
        {
            // The classical moments stream through the lattice
            // statistics; a gapped Stokes set needs the sample route.
            let sub = image.sub_image(selection.start.clone(), selection.shape.clone())?;
            let mut stats = LatticeStatistics::new(&sub);
            if let Some([min, max]) = request.includepix {
                stats.set_include_range(min, max);
            }
            if let Some(mask) = pixel_mask.cloned() {
                stats.set_pixel_mask(mask);
            }
            let (minpos, maxpos) = stats.get_min_max_pos()?;
            ComputedStats {
                npts: scalar_stat(&stats, Statistic::Npts)?,
                sum: scalar_stat(&stats, Statistic::Sum)?,
                min: scalar_stat(&stats, Statistic::Min)?,
                max: scalar_stat(&stats, Statistic::Max)?,
                sumsq: scalar_stat(&stats, Statistic::SumSq)?,
                mean: scalar_stat(&stats, Statistic::Mean)?,
                rms: scalar_stat(&stats, Statistic::Rms)?,
                sigma: scalar_stat(&stats, Statistic::Sigma)?,
                median: scalar_stat(&stats, Statistic::Median)?,
                medabsdevmed: scalar_stat(&stats, Statistic::MedAbsDevMed)?,
                q1: scalar_stat(&stats, Statistic::Q1)?,
                q3: scalar_stat(&stats, Statistic::Q3)?,
                quartile: scalar_stat(&stats, Statistic::Quartile)?,
                minpos: minpos.map(|pos| offset_pos(&selection.start, pos)),
                maxpos: maxpos.map(|pos| offset_pos(&selection.start, pos)),
            }
        }
        robust => {
            let pixels = image.get_slice(&selection.start, &selection.shape)?;
            let sample = sample_points(
                &pixels,
                pixel_mask,
                request.includepix,
                selection.channel_axis,
                selection.channel_indices.as_deref(),
                selection.stokes_axis,
                selection.stokes_indices.as_deref(),
                &selection.start,
            );
            match compute_robust_stats(&sample, robust)? {
                RobustStats::Sample(stats) => ComputedStats {
                    npts: stats.npts,
                    sum: stats.sum,
                    min: stats.min,
                    max: stats.max,
                    sumsq: stats.sumsq,
                    mean: stats.mean,
                    rms: stats.rms,
                    sigma: stats.sigma,
                    median: stats.median,
                    medabsdevmed: stats.medabsdevmed,
                    q1: stats.q1,
                    q3: stats.q3,
                    quartile: stats.quartile,
                    minpos: stats.minpos.map(|linear| {
                        offset_pos(
                            &selection.start,
                            selection_position(linear, &selection.shape),
                        )
                    }),
                    maxpos: stats.maxpos.map(|linear| {
                        offset_pos(
                            &selection.start,
                            selection_position(linear, &selection.shape),
                        )
                    }),
                },
                RobustStats::Biweight { location, scale } => {
                    // The biweight reports only its location and scale,
                    // with the sample's count and extrema.
                    let npts = sample.len() as f64;
                    let min = sample.iter().map(|p| p.value).fold(f64::NAN, f64::min);
                    let max = sample.iter().map(|p| p.value).fold(f64::NAN, f64::max);
                    ComputedStats {
                        npts,
                        sum: f64::NAN,
                        min,
                        max,
                        sumsq: f64::NAN,
                        mean: location,
                        rms: f64::NAN,
                        sigma: scale,
                        median: f64::NAN,
                        medabsdevmed: f64::NAN,
                        q1: f64::NAN,
                        q3: f64::NAN,
                        quartile: f64::NAN,
                        minpos: None,
                        maxpos: None,
                    }
                }
            }
        }
    };

    let minposf = match &computed.minpos {
        Some(position) => Some(world_text(position)?),
        None => None,
    };
    let maxposf = match &computed.maxpos {
        Some(position) => Some(world_text(position)?),
        None => None,
    };
    let flux = if algorithm.kind() == StatisticsAlgorithmKind::Biweight {
        None
    } else {
        image_flux(image, computed.sum, selection)
    };
    Ok(ImageStatisticsSummary {
        blcf,
        trcf,
        npts: computed.npts,
        min: computed.min,
        max: computed.max,
        sum: computed.sum,
        sumsq: computed.sumsq,
        mean: computed.mean,
        rms: computed.rms,
        sigma: computed.sigma,
        median: computed.median,
        medabsdevmed: computed.medabsdevmed,
        q1: computed.q1,
        q3: computed.q3,
        quartile: computed.quartile,
        minposf,
        maxposf,
        minpos: computed.minpos,
        maxpos: computed.maxpos,
        imagename: path.display().to_string(),
        blc,
        trc,
        flux,
        units: image.units().to_string(),
    })
}

/// Each display plane's statistics, as `imstat` reports them with only
/// some axes collapsed: one value per display position, in display
/// order, with CASA's degenerate display axes dropped from the record's
/// shape.
#[allow(clippy::too_many_arguments)]
fn imstat_planes_summary<T>(
    path: &Path,
    image: &PagedImage<T>,
    request: &ImstatRequest,
    algorithm: &StatisticsAlgorithm,
    selection: &Selection,
    pixel_mask: Option<&ndarray::ArrayD<bool>>,
    display_axes: &[usize],
    blc: Vec<usize>,
    trc: Vec<usize>,
    blcf: String,
    trcf: String,
) -> Result<ImstatPlanesSummary, ImageError>
where
    T: ImagePixel + StatsElement,
{
    let display_positions: Vec<Vec<usize>> = display_axes
        .iter()
        .map(|&axis| selection.axis_positions(axis))
        .collect();
    let display_counts: Vec<usize> = display_positions.iter().map(|p| p.len()).collect();
    let display_shape: Vec<usize> = display_counts
        .iter()
        .copied()
        .filter(|&count| count > 1)
        .collect();
    let plane_count: usize = display_counts.iter().product();

    // One pass over the selection's pixels, bucketed by display
    // position: each pixel's plane is its display coordinates' index.
    let pixels = image.get_slice(&selection.start, &selection.shape)?;
    let mut buckets: Vec<Vec<SamplePoint>> = vec![Vec::new(); plane_count];
    let stokes_collapsed = selection
        .stokes_axis
        .is_some_and(|axis| display_axes.iter().all(|&display| display != axis));
    let mut strides = Vec::with_capacity(pixels.shape().len());
    let mut stride = 1u64;
    for &axis_length in pixels.shape() {
        strides.push(stride);
        stride *= axis_length as u64;
    }
    for (index, value) in pixels.indexed_iter() {
        if let Some(mask) = pixel_mask {
            let mask_index: &[usize] = index.slice();
            if !mask[mask_index] {
                continue;
            }
        }
        let number = value.to_f64_stats();
        if let Some([min, max]) = request.includepix
            && !(min <= number && number <= max)
        {
            continue;
        }
        let mut linear = 0u64;
        for (axis, &axis_stride) in strides.iter().enumerate() {
            linear += index[axis] as u64 * axis_stride;
        }
        let mut bucket = 0usize;
        // The display positions run in Fortran order, the order casacore
        // keeps array data in and casa-rs's records carry.
        let mut slot_stride = 1usize;
        let mut in_selection = true;
        for (slot, &axis) in display_axes.iter().enumerate() {
            let absolute = index[axis] + selection.start[axis];
            let Some(slot_index) = display_positions[slot]
                .iter()
                .position(|&position| position == absolute)
            else {
                in_selection = false;
                break;
            };
            bucket += slot_index * slot_stride;
            slot_stride *= display_counts[slot];
        }
        if !in_selection {
            continue;
        }
        if stokes_collapsed {
            let axis = selection.stokes_axis.expect("the axis was found above");
            let kept = selection.stokes_indices.as_deref().unwrap_or(&[]);
            let absolute = index[axis] + selection.start[axis];
            if !kept.contains(&absolute) {
                continue;
            }
        }
        buckets[bucket].push(SamplePoint {
            value: number,
            position: linear,
        });
    }

    let biweight = algorithm.kind() == StatisticsAlgorithmKind::Biweight;
    let spectral_collapsed = image
        .find_axis(CoordinateType::Spectral)
        .is_some_and(|axis| display_axes.iter().all(|&display| display != axis));
    // CASA's flux density exists per plane only when the direction plane
    // is at least partly collapsed, as casacore beam-integrates the sky
    // it sums: a plane that still varies over the whole sky has none.
    let [plane_x, plane_y] = selection.plane;
    let flux_over_sky = !display_axes.contains(&plane_x) || !display_axes.contains(&plane_y);
    let mut planes = ImstatPlanesSummary {
        imagename: path.display().to_string(),
        blc,
        trc,
        blcf,
        trcf,
        display_shape,
        plane_log: String::new(),
        npts: Vec::with_capacity(plane_count),
        min: Vec::with_capacity(plane_count),
        max: Vec::with_capacity(plane_count),
        sum: Vec::with_capacity(plane_count),
        sumsq: Vec::with_capacity(plane_count),
        mean: Vec::with_capacity(plane_count),
        rms: Vec::with_capacity(plane_count),
        sigma: Vec::with_capacity(plane_count),
        median: Vec::with_capacity(plane_count),
        medabsdevmed: Vec::with_capacity(plane_count),
        q1: Vec::with_capacity(plane_count),
        q3: Vec::with_capacity(plane_count),
        quartile: Vec::with_capacity(plane_count),
        flux: None,
        biweight,
    };
    let mut iterations = Vec::new();
    for bucket in &buckets {
        let mut sample = bucket.clone();
        sample.sort_by_key(|point| point.position);
        match compute_robust_stats(&sample, algorithm)? {
            RobustStats::Sample(stats) => {
                iterations.push(stats.chauvenet_iterations);
                planes.npts.push(stats.npts);
                planes.min.push(stats.min);
                planes.max.push(stats.max);
                planes.sum.push(stats.sum);
                planes.sumsq.push(stats.sumsq);
                planes.mean.push(stats.mean);
                planes.rms.push(stats.rms);
                planes.sigma.push(stats.sigma);
                planes.median.push(stats.median);
                planes.medabsdevmed.push(stats.medabsdevmed);
                planes.q1.push(stats.q1);
                planes.q3.push(stats.q3);
                planes.quartile.push(stats.quartile);
                let flux = if flux_over_sky {
                    image_flux_with_integration(image, stats.sum, selection, spectral_collapsed)
                } else {
                    None
                };
                if let Some(flux) = flux {
                    planes
                        .flux
                        .get_or_insert_with(|| Vec::with_capacity(plane_count))
                        .push(flux);
                }
            }
            RobustStats::Biweight { location, scale } => {
                let count = sample.len() as f64;
                let min = sample.iter().map(|p| p.value).fold(f64::NAN, f64::min);
                let max = sample.iter().map(|p| p.value).fold(f64::NAN, f64::max);
                planes.npts.push(count);
                planes.min.push(min);
                planes.max.push(max);
                planes.sum.push(f64::NAN);
                planes.sumsq.push(f64::NAN);
                planes.mean.push(location);
                planes.rms.push(f64::NAN);
                planes.sigma.push(scale);
                planes.median.push(f64::NAN);
                planes.medabsdevmed.push(f64::NAN);
                planes.q1.push(f64::NAN);
                planes.q3.push(f64::NAN);
                planes.quartile.push(f64::NAN);
            }
        }
    }
    if biweight {
        planes.flux = None;
    }
    if request.listit
        || request
            .logfile
            .as_deref()
            .is_some_and(|name| !name.is_empty())
    {
        planes.plane_log = imstat_log::plane_table(
            image,
            &planes,
            display_axes,
            &display_positions,
            &iterations,
        )?;
    }
    Ok(planes)
}

/// The statistics one `imstat` computation reports, before the record and
/// the world-coordinate strings are built.
struct ComputedStats {
    npts: f64,
    min: f64,
    max: f64,
    sum: f64,
    sumsq: f64,
    mean: f64,
    rms: f64,
    sigma: f64,
    median: f64,
    medabsdevmed: f64,
    q1: f64,
    q3: f64,
    quartile: f64,
    minpos: Option<Vec<usize>>,
    maxpos: Option<Vec<usize>>,
}

/// The image mask of a selection, combined with its region's and a LEL
/// mask expression's. The expression is CASA's full lattice expression
/// language, evaluated over the image and extended to its shape when
/// `stretch` allows.
fn selection_pixel_mask<T>(
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
fn image_mask_expression<T>(
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
    let names = crate::expr_parser::referenced_images(expr).map_err(|error| {
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
fn extend_mask_to_shape(
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

/// The parser's own wording, as CASA reports a refused mask expression.
pub(crate) fn mask_error_wording(error: ExprParseError) -> String {
    if error.message.starts_with("unknown image") {
        // The resolver's miss: CASA's ImageExprParse wording.
        error
            .message
            .replace("unknown image: ", "ImageExprParse: '")
            + "' is an unknown lattice or image or it is an unqualified region"
    } else {
        format!("Image Expression: {error}")
    }
}

/// The selection's pixel values as a robust-algorithm sample, with each
/// value's Fortran-linear position inside the selection, filtered by
/// the pixel mask, an inclusive pixel-value range, and the named
/// channel and Stokes sets.
#[allow(clippy::too_many_arguments)]
fn sample_points<T>(
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
fn selection_position(linear: u64, shape: &[usize]) -> Vec<usize> {
    let mut position = Vec::with_capacity(shape.len());
    let mut remainder = linear;
    for &axis_shape in shape {
        position.push((remainder % axis_shape as u64) as usize);
        remainder /= axis_shape.max(1) as u64;
    }
    position
}

/// The inclusive top-right corner of a selection, as pixels.
fn trc_pixels(start: &[usize], shape: &[usize]) -> Vec<usize> {
    start
        .iter()
        .zip(shape)
        .map(|(start, shape)| start + shape - 1)
        .collect()
}

/// One statistic of the selection. With no valid pixel, a count or a sum
/// is 0 and every other statistic NaN; CASA returns each as an empty array.
fn scalar_stat<T: StatsElement>(
    stats: &LatticeStatistics<'_, T>,
    stat: Statistic,
) -> Result<f64, ImageError> {
    let values = stats.get_statistic(stat)?;
    Ok(values.iter().next().copied().unwrap_or(match stat {
        Statistic::Npts | Statistic::Sum | Statistic::SumSq => 0.0,
        _ => f64::NAN,
    }))
}

fn offset_pos(start: &[usize], pos: Vec<usize>) -> Vec<usize> {
    start
        .iter()
        .zip(pos)
        .map(|(base, offset)| base + offset)
        .collect()
}

#[derive(Debug, Clone)]
pub(super) struct Selection {
    start: Vec<usize>,
    shape: Vec<usize>,
    /// The spectral axis its named channels belong to.
    channel_axis: Option<usize>,
    channel_indices: Option<Vec<usize>>,
    /// The Stokes axis its named planes belong to.
    stokes_axis: Option<usize>,
    /// The image's Stokes-axis indices the selection holds, when a Stokes
    /// selection named them.
    stokes_indices: Option<Vec<usize>>,
    /// The image axes a box or polygon's x and y run along: the direction
    /// plane's.
    plane: [usize; 2],
}

impl Selection {
    fn new<T: ImagePixel>(
        image: &impl ImageInterface<T>,
        region: Option<&PixelRegion>,
        chans: Option<&str>,
        stokes: Option<&str>,
    ) -> Result<Self, ImageError> {
        let mut start = vec![0; image.ndim()];
        let mut shape = image.shape().to_vec();
        let plane = region.map_or(Ok([0, 1]), |_| direction_plane_axes(image))?;
        if let Some(region) = region {
            let [x0, y0, x1, y1] = region.bounding_box();
            let [x, y] = plane;
            if image.ndim() < 2 || x1 >= shape[x] || y1 >= shape[y] {
                return Err(ImageError::ShapeMismatch {
                    expected: image.shape().to_vec(),
                    got: vec![x0, y0, x1, y1],
                });
            }
            start[x] = x0;
            start[y] = y0;
            shape[x] = x1 - x0 + 1;
            shape[y] = y1 - y0 + 1;
        }
        let (channel_axis, channel_indices) =
            if let Some(chans) = chans.filter(|text| !text.trim().is_empty()) {
                let spectral_axis = image.find_axis(CoordinateType::Spectral).ok_or_else(|| {
                    ImageError::InvalidMetadata(
                        "channel selection requires a spectral axis".to_string(),
                    )
                })?;
                let indices = parse_image_channel_selection(chans, image.shape()[spectral_axis])?;
                start[spectral_axis] = indices[0];
                shape[spectral_axis] = indices[indices.len() - 1] - indices[0] + 1;
                (Some(spectral_axis), Some(indices))
            } else {
                (None, None)
            };
        let (stokes_axis, stokes_indices) =
            if let Some(text) = stokes.filter(|text| !text.trim().is_empty()) {
                let indices = parse_stokes_selection(image, text)?;
                let axis = image
                    .find_axis(CoordinateType::Stokes)
                    .expect("parse_stokes_selection found the axis");
                start[axis] = indices[0];
                shape[axis] = indices[indices.len() - 1] - indices[0] + 1;
                (Some(axis), Some(indices))
            } else {
                (None, None)
            };
        Ok(Self {
            start,
            shape,
            channel_axis,
            channel_indices,
            stokes_axis,
            stokes_indices,
            plane,
        })
    }

    /// The image-axis indices this selection holds along `axis`, in
    /// order: a named set's, or the bounding rectangle's span.
    fn axis_positions(&self, axis: usize) -> Vec<usize> {
        if Some(axis) == self.channel_axis
            && let Some(indices) = &self.channel_indices
        {
            return indices.clone();
        }
        if Some(axis) == self.stokes_axis
            && let Some(indices) = &self.stokes_indices
        {
            return indices.clone();
        }
        (self.start[axis]..self.start[axis] + self.shape[axis]).collect()
    }

    /// Whether the named Stokes set is not a contiguous span, which the
    /// streaming classical path cannot express.
    fn stokes_has_gaps(&self) -> bool {
        Self::named_set_has_gaps(self.stokes_axis, &self.stokes_indices)
    }

    /// Whether the named channel set is not a contiguous span, which the
    /// streaming classical path cannot express.
    fn channels_have_gaps(&self) -> bool {
        Self::named_set_has_gaps(self.channel_axis, &self.channel_indices)
    }

    /// Whether a named selection set skips indices between its first and
    /// last.
    fn named_set_has_gaps(axis: Option<usize>, indices: &Option<Vec<usize>>) -> bool {
        match (axis, indices) {
            (Some(_), Some(indices)) => {
                indices.len() > 1 && indices[indices.len() - 1] - indices[0] + 1 != indices.len()
            }
            _ => false,
        }
    }
}

/// Parse CASA's `stokes` selection: comma-separated Stokes plane names,
/// concatenated as in `QU` or `IQUV`. Numeric forms are not names.
pub(super) fn parse_stokes_selection<T: ImagePixel>(
    image: &(impl ImageInterface<T> + ?Sized),
    text: &str,
) -> Result<Vec<usize>, ImageError> {
    let axis = image.find_axis(CoordinateType::Stokes).ok_or_else(|| {
        ImageError::InvalidMetadata("Stokes selection requires a Stokes axis".to_string())
    })?;
    parse_stokes_names(image.coordinates(), axis, text)
}

fn parse_stokes_names(
    coordinates: &CoordinateSystem,
    axis: usize,
    text: &str,
) -> Result<Vec<usize>, ImageError> {
    let Some(CoordinateModel::Stokes(stokes)) = coordinate_for_axis(coordinates, axis) else {
        return Err(ImageError::InvalidMetadata(
            "Stokes selection requires a Stokes coordinate".to_string(),
        ));
    };
    let mut indices = Vec::new();
    for part in text.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let mut rest = part;
        while !rest.is_empty() {
            let name = if rest.len() >= 2 && StokesType::from_name(&rest[..2]).is_some() {
                rest[..2].to_string()
            } else if let Some(kind) = rest
                .chars()
                .next()
                .and_then(|first| StokesType::from_name(&first.to_string()))
            {
                let _ = kind;
                rest[..1].to_string()
            } else {
                return Err(ImageError::CasaSelection(format!(
                    "(Sub)String {part} in stokes specification part {part} does not match a known polarization"
                )));
            };
            let kind =
                StokesType::from_name(&name).expect("the name was matched against the table");
            let Some(index) = stokes.stokes().iter().position(|known| *known == kind) else {
                return Err(ImageError::CasaSelection(format!(
                    "Stokes {name} is not one of this image's Stokes planes"
                )));
            };
            indices.push(index);
            rest = &rest[name.len()..];
        }
    }
    if indices.is_empty() {
        return Err(ImageError::InvalidMetadata(
            "the Stokes selection names no plane".to_string(),
        ));
    }
    indices.sort_unstable();
    indices.dedup();
    Ok(indices)
}

/// The `axes` selection: all axes, or the named axes to collapse.
#[derive(Debug, Clone, PartialEq, Eq)]
enum AxisSelection {
    /// Every axis: the statistics of the whole selection.
    All,
    /// Collapse these pixel axes; the statistics vary along the rest.
    Collapsed(Vec<usize>),
}

/// Parse CASA's `axes` argument: a pixel-axis list, `-1` for all of them,
/// or an empty list for the default. A negative axis other than a lone
/// `-1` is invalid, with CASA's wording.
fn parse_axes(axes: Option<&[i64]>, n_dim: usize) -> Result<AxisSelection, String> {
    let Some(axes) = axes else {
        return Ok(AxisSelection::All);
    };
    if axes.is_empty() {
        return Ok(AxisSelection::All);
    }
    if axes == [-1] {
        return Ok(AxisSelection::All);
    }
    let mut named = Vec::with_capacity(axes.len());
    for &axis in axes {
        if !(0..n_dim as i64).contains(&axis) {
            return Err(format!("Invalid cursor axes: {axes:?}."));
        }
        if named.contains(&(axis as usize)) {
            return Err(format!(
                "Invalid cursor axes: {axes:?}: an axis is named twice; CASA 6.7.6.14 aborts on it"
            ));
        }
        named.push(axis as usize);
    }
    named.sort_unstable();
    Ok(AxisSelection::Collapsed(named))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PixelRegion {
    /// One or more rectangular boxes: CASA's task `box` parameter takes
    /// comma-separated quadruplets, and CRTF strings name one box.
    Boxes(Vec<[usize; 4]>),
    Polygon(Vec<[usize; 2]>),
}

impl PixelRegion {
    fn bounding_box(&self) -> [usize; 4] {
        match self {
            Self::Boxes(boxes) => {
                let min_x = boxes.iter().map(|b| b[0]).min().unwrap_or(0);
                let min_y = boxes.iter().map(|b| b[1]).min().unwrap_or(0);
                let max_x = boxes.iter().map(|b| b[2]).max().unwrap_or(min_x);
                let max_y = boxes.iter().map(|b| b[3]).max().unwrap_or(min_y);
                [min_x, min_y, max_x, max_y]
            }
            Self::Polygon(vertices) => {
                let min_x = vertices.iter().map(|vertex| vertex[0]).min().unwrap_or(0);
                let min_y = vertices.iter().map(|vertex| vertex[1]).min().unwrap_or(0);
                let max_x = vertices
                    .iter()
                    .map(|vertex| vertex[0])
                    .max()
                    .unwrap_or(min_x);
                let max_y = vertices
                    .iter()
                    .map(|vertex| vertex[1])
                    .max()
                    .unwrap_or(min_y);
                [min_x, min_y, max_x, max_y]
            }
        }
    }
}

pub(super) fn region_pixels<T: ImagePixel>(
    image: &impl ImageInterface<T>,
    region: Option<&str>,
) -> Result<Option<PixelRegion>, ImageError> {
    let Some(region) = region.map(str::trim).filter(|text| !text.is_empty()) else {
        return Ok(None);
    };
    if let Some(parsed) = crtf_box_pixels(image, region)? {
        return Ok(Some(PixelRegion::Boxes(vec![parse_box(&parsed)?])));
    }
    if let Some(parsed) = crtf_polygon_pixels(image, region)? {
        return Ok(Some(PixelRegion::Polygon(parsed)));
    }
    if let Some(box_text) = region.strip_prefix("box:") {
        let parsed = parse_box(box_text.trim())?;
        return Ok(Some(PixelRegion::Boxes(vec![parsed])));
    }
    if let Some(contents) = region
        .strip_prefix("pixelbox(")
        .and_then(|text| text.strip_suffix(')'))
    {
        let parsed = parse_box(contents.trim())?;
        return Ok(Some(PixelRegion::Boxes(vec![parsed])));
    }
    if looks_like_box(region) {
        let parsed = parse_box(region)?;
        return Ok(Some(PixelRegion::Boxes(vec![parsed])));
    }

    let region_path = Path::new(region);
    let contents = fs::read_to_string(region_path).map_err(|error| {
        ImageError::Io(format!(
            "read region file {}: {error}",
            region_path.display()
        ))
    })?;
    if let Some(box_pixels) = crtf_box_pixels(image, &contents)? {
        return Ok(Some(PixelRegion::Boxes(vec![parse_box(&box_pixels)?])));
    }
    if let Some(polygon) = crtf_polygon_pixels(image, &contents)? {
        return Ok(Some(PixelRegion::Polygon(polygon)));
    }
    let format = if contents.trim_start().starts_with('{') {
        "casa-rs JSON region files are not a persistent interchange format; use CASA CRTF"
    } else {
        "expected CASA CRTF box or polygon, for example box[[100pix,100pix],[150pix,150pix]] or poly [[100pix,100pix],...]"
    };
    Err(ImageError::InvalidMetadata(format!(
        "unsupported region file {}: {format}",
        region_path.display()
    )))
}

pub(super) fn region_pixel_mask(
    region: &PixelRegion,
    selection: &Selection,
) -> Result<ArrayD<bool>, ImageError> {
    let mask = match region {
        PixelRegion::Boxes(boxes) => {
            if boxes.len() < 2 {
                // One box is the selection itself; every pixel is in it.
                ArrayD::from_elem(IxDyn(&selection.shape).f(), true)
            } else {
                // Several boxes: the selection is their bounding box, and
                // a pixel is in the region when some box holds it.
                let [x_axis, y_axis] = selection.plane;
                ArrayD::from_shape_fn(IxDyn(&selection.shape).f(), |idx| {
                    let x = selection.start[x_axis] + idx[x_axis];
                    let y = selection.start[y_axis] + idx[y_axis];
                    boxes
                        .iter()
                        .any(|b| b[0] <= x && x <= b[2] && b[1] <= y && y <= b[3])
                })
            }
        }
        PixelRegion::Polygon(vertices) => {
            if vertices.len() < 3 {
                return Err(ImageError::InvalidMetadata(
                    "CRTF polygon regions require at least three vertices".to_string(),
                ));
            }
            let [x_axis, y_axis] = selection.plane;
            ArrayD::from_shape_fn(IxDyn(&selection.shape).f(), |idx| {
                let x = selection.start[x_axis] + idx[x_axis];
                let y = selection.start[y_axis] + idx[y_axis];
                point_in_polygon_or_boundary(x as f64 + 0.5, y as f64 + 0.5, vertices)
            })
        }
    };
    Ok(mask)
}

fn crtf_box_pixels<T: ImagePixel>(
    image: &impl ImageInterface<T>,
    text: &str,
) -> Result<Option<String>, ImageError> {
    let Some(parts) = crtf_box_parts(text)? else {
        return Ok(None);
    };
    let pixels = parts
        .iter()
        .map(|part| parse_crtf_pixel_quantity(part))
        .collect::<Result<Vec<_>, ImageError>>()?;
    if pixels.iter().all(Option::is_some) {
        let values = pixels.into_iter().flatten().collect::<Vec<_>>();
        return Ok(Some(format!(
            "{},{},{},{}",
            values[0], values[1], values[2], values[3]
        )));
    }
    if pixels.iter().any(Option::is_some) {
        return Err(ImageError::InvalidMetadata(
            "cannot mix pix and non-pix coordinates in a CRTF box region".to_string(),
        ));
    }

    let world = parts
        .iter()
        .map(|part| parse_crtf_world_quantity(part))
        .collect::<Result<Vec<_>, ImageError>>()?;
    Ok(Some(crtf_world_box_pixels(
        image,
        [world[0], world[1], world[2], world[3]],
    )?))
}

fn crtf_polygon_pixels<T: ImagePixel>(
    image: &impl ImageInterface<T>,
    text: &str,
) -> Result<Option<Vec<[usize; 2]>>, ImageError> {
    let Some(pairs) = crtf_polygon_parts(text)? else {
        return Ok(None);
    };
    if pairs.len() < 3 {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid CASA CRTF polygon region {text:?}: expected at least three vertices"
        )));
    }
    crtf_coordinate_pairs_to_pixels(image, &pairs).map(Some)
}

fn crtf_polygon_parts(text: &str) -> Result<Option<Vec<[String; 2]>>, ImageError> {
    let normalized = text.trim();
    let Some(after) = find_crtf_body(normalized, "poly", "[") else {
        return Ok(None);
    };
    let end = after.rfind(']').ok_or_else(|| {
        ImageError::InvalidMetadata(format!(
            "invalid CASA CRTF polygon region {text:?}: missing closing ]"
        ))
    })?;
    let body = &after[..=end];
    let body = body
        .strip_prefix('[')
        .and_then(|body| body.strip_suffix(']'))
        .unwrap_or(body);
    let mut pairs = Vec::new();
    let mut cursor = 0usize;
    while let Some(relative_start) = body[cursor..].find('[') {
        let pair_start = cursor + relative_start + 1;
        let pair_end = body[pair_start..].find(']').ok_or_else(|| {
            ImageError::InvalidMetadata(format!(
                "invalid CASA CRTF polygon region {text:?}: missing vertex closing ]"
            ))
        })? + pair_start;
        let pair = body[pair_start..pair_end]
            .split(',')
            .map(|part| part.trim().to_string())
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>();
        if pair.len() == 2 {
            pairs.push([pair[0].clone(), pair[1].clone()]);
        }
        cursor = pair_end + 1;
    }
    if pairs.is_empty() {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid CASA CRTF polygon region {text:?}: expected poly [[x0,y0],...]"
        )));
    }
    Ok(Some(pairs))
}

fn crtf_coordinate_pairs_to_pixels<T: ImagePixel>(
    image: &impl ImageInterface<T>,
    pairs: &[[String; 2]],
) -> Result<Vec<[usize; 2]>, ImageError> {
    let pixel_pairs = pairs
        .iter()
        .map(|pair| {
            Ok([
                parse_crtf_pixel_quantity(&pair[0])?,
                parse_crtf_pixel_quantity(&pair[1])?,
            ])
        })
        .collect::<Result<Vec<_>, ImageError>>()?;
    if pixel_pairs
        .iter()
        .all(|pair| pair[0].is_some() && pair[1].is_some())
    {
        return Ok(pixel_pairs
            .into_iter()
            .map(|pair| [pair[0].unwrap(), pair[1].unwrap()])
            .collect());
    }
    if pixel_pairs
        .iter()
        .any(|pair| pair[0].is_some() || pair[1].is_some())
    {
        return Err(ImageError::InvalidMetadata(
            "cannot mix pix and non-pix coordinates in a CRTF polygon region".to_string(),
        ));
    }

    let coordinates = image.coordinates();
    if coordinates.n_pixel_axes() < 2 || coordinates.n_world_axes() < 2 {
        return Err(ImageError::InvalidMetadata(
            "world-coordinate CRTF polygon regions require an image coordinate system with at least two axes"
                .to_string(),
        ));
    }
    let [x, y] = direction_plane_axes(image)?;
    let [longitude, latitude] = world_axes_for_plane(coordinates, [x, y])?;
    let base_pixel = vec![0.0; coordinates.n_pixel_axes()];
    let base_world = coordinates.to_world(&base_pixel)?;
    pairs
        .iter()
        .map(|pair| {
            let mut world = base_world.clone();
            world[longitude] = parse_crtf_world_quantity(&pair[0])?;
            world[latitude] = parse_crtf_world_quantity(&pair[1])?;
            let pixel = coordinates.to_pixel(&world)?;
            Ok([
                rounded_nonnegative_pixel_value(pixel[x], "CRTF polygon world x")?,
                rounded_nonnegative_pixel_value(pixel[y], "CRTF polygon world y")?,
            ])
        })
        .collect()
}

fn crtf_box_parts(text: &str) -> Result<Option<[String; 4]>, ImageError> {
    let normalized = text.trim();
    let Some(after) = find_crtf_body(normalized, "box", "[[").and_then(|s| s.strip_prefix("[["))
    else {
        return Ok(None);
    };
    let end = after.find("]]").ok_or_else(|| {
        ImageError::InvalidMetadata(format!(
            "invalid CASA CRTF box region {text:?}: missing closing ]]"
        ))
    })?;
    let body = &after[..end];
    let body = body
        .replace("], [", ",")
        .replace("],[", ",")
        .replace(['[', ']'], "");
    let parts = body
        .split(',')
        .map(|part| part.trim().to_string())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.len() != 4 {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid CASA CRTF box region {text:?}: expected four coordinates"
        )));
    }
    Ok(Some([
        parts[0].clone(),
        parts[1].clone(),
        parts[2].clone(),
        parts[3].clone(),
    ]))
}

fn find_crtf_body<'a>(text: &'a str, keyword: &str, open: &str) -> Option<&'a str> {
    let mut offset = 0usize;
    while let Some(rel) = text[offset..].find(keyword) {
        let pos = offset + rel;
        let boundary = pos == 0
            || text[..pos]
                .chars()
                .next_back()
                .is_none_or(|ch| !ch.is_ascii_alphanumeric() && ch != '_');
        let rest = &text[pos + keyword.len()..];
        if boundary {
            let trimmed = rest.trim_start();
            if trimmed.starts_with(open) {
                return Some(trimmed);
            }
        }
        offset = pos + keyword.len();
    }
    None
}

fn parse_crtf_pixel_quantity(text: &str) -> Result<Option<usize>, ImageError> {
    let text = text.trim();
    let lowercase = text.to_ascii_lowercase();
    let Some(value) = lowercase.strip_suffix("pix") else {
        return Ok(None);
    };
    rounded_nonnegative_pixel(value.trim(), text).map(Some)
}

fn parse_crtf_world_quantity(text: &str) -> Result<f64, ImageError> {
    let text = text.trim();
    let lowercase = text.to_ascii_lowercase();
    let (value, scale) = if let Some(value) = lowercase.strip_suffix("arcsec") {
        (value, std::f64::consts::PI / 180.0 / 3600.0)
    } else if let Some(value) = lowercase.strip_suffix("arcmin") {
        (value, std::f64::consts::PI / 180.0 / 60.0)
    } else if let Some(value) = lowercase.strip_suffix("deg") {
        (value, std::f64::consts::PI / 180.0)
    } else if let Some(value) = lowercase.strip_suffix("rad") {
        (value, 1.0)
    } else {
        (lowercase.as_str(), 1.0)
    };
    let parsed = value.trim().parse::<f64>().map_err(|error| {
        ImageError::InvalidMetadata(format!("invalid CRTF world coordinate {text:?}: {error}"))
    })?;
    if !parsed.is_finite() {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid CRTF world coordinate {text:?}: value is not finite"
        )));
    }
    Ok(parsed * scale)
}

fn crtf_world_box_pixels<T: ImagePixel>(
    image: &impl ImageInterface<T>,
    world_box: [f64; 4],
) -> Result<String, ImageError> {
    let coordinates = image.coordinates();
    if coordinates.n_pixel_axes() < 2 || coordinates.n_world_axes() < 2 {
        return Err(ImageError::InvalidMetadata(
            "world-coordinate CRTF box regions require an image coordinate system with at least two axes"
                .to_string(),
        ));
    }
    let [x, y] = direction_plane_axes(image)?;
    let base_pixel = vec![0.0; coordinates.n_pixel_axes()];
    let mut first_world = coordinates.to_world(&base_pixel)?;
    let [longitude, latitude] = world_axes_for_plane(coordinates, [x, y])?;
    let mut second_world = first_world.clone();
    first_world[longitude] = world_box[0];
    first_world[latitude] = world_box[1];
    second_world[longitude] = world_box[2];
    second_world[latitude] = world_box[3];
    let first_pixel = coordinates.to_pixel(&first_world)?;
    let second_pixel = coordinates.to_pixel(&second_world)?;
    let x0 = rounded_nonnegative_pixel_value(first_pixel[x], "first CRTF world x")?;
    let y0 = rounded_nonnegative_pixel_value(first_pixel[y], "first CRTF world y")?;
    let x1 = rounded_nonnegative_pixel_value(second_pixel[x], "second CRTF world x")?;
    let y1 = rounded_nonnegative_pixel_value(second_pixel[y], "second CRTF world y")?;
    Ok(format!(
        "{},{},{},{}",
        x0.min(x1),
        y0.min(y1),
        x0.max(x1),
        y0.max(y1)
    ))
}

fn rounded_nonnegative_pixel(text: &str, original: &str) -> Result<usize, ImageError> {
    let value = text.parse::<f64>().map_err(|error| {
        ImageError::InvalidMetadata(format!(
            "invalid CRTF pixel coordinate {original:?}: {error}"
        ))
    })?;
    rounded_nonnegative_pixel_value(value, original)
}

fn rounded_nonnegative_pixel_value(value: f64, label: &str) -> Result<usize, ImageError> {
    if !value.is_finite() || value < -1.0e-6 {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid CRTF pixel coordinate {label:?}: value is not a non-negative finite number"
        )));
    }
    Ok(value.max(0.0).round() as usize)
}

fn point_in_polygon_or_boundary(x: f64, y: f64, vertices: &[[usize; 2]]) -> bool {
    let mut inside = false;
    let mut previous = vertices[vertices.len() - 1];
    for &current in vertices {
        let x0 = previous[0] as f64;
        let y0 = previous[1] as f64;
        let x1 = current[0] as f64;
        let y1 = current[1] as f64;
        if point_on_segment(x, y, x0, y0, x1, y1) {
            return true;
        }
        let crosses = (y0 > y) != (y1 > y);
        if crosses {
            let x_intersection = (x1 - x0) * (y - y0) / (y1 - y0) + x0;
            if x <= x_intersection {
                inside = !inside;
            }
        }
        previous = current;
    }
    inside
}

fn point_on_segment(x: f64, y: f64, x0: f64, y0: f64, x1: f64, y1: f64) -> bool {
    let cross = (x - x0) * (y1 - y0) - (y - y0) * (x1 - x0);
    if cross.abs() > 1.0e-9 {
        return false;
    }
    x >= x0.min(x1) - 1.0e-9
        && x <= x0.max(x1) + 1.0e-9
        && y >= y0.min(y1) - 1.0e-9
        && y <= y0.max(y1) + 1.0e-9
}

fn looks_like_box(text: &str) -> bool {
    let mut count = 0usize;
    for part in text.split(',') {
        let part = part.trim();
        if part.is_empty() || part.parse::<usize>().is_err() {
            return false;
        }
        count += 1;
    }
    count == 4
}

fn parse_box(text: &str) -> Result<[usize; 4], ImageError> {
    let values = text
        .split(',')
        .map(|part| {
            part.trim().parse::<usize>().map_err(|error| {
                ImageError::InvalidMetadata(format!("invalid box {text:?}: {error}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.len() != 4 || values[2] < values[0] || values[3] < values[1] {
        return Err(ImageError::InvalidMetadata(format!(
            "box must be x0,y0,x1,y1 with inclusive upper bounds: {text:?}"
        )));
    }
    Ok([values[0], values[1], values[2], values[3]])
}

/// Parse CASA's task `box` parameter: comma-separated quadruplets of
/// inclusive pixel corners, one box per quadruplet. CASA's
/// `CasacRegionManager` refuses any other count with the same wording.
fn parse_boxes(text: &str) -> Result<Vec<[usize; 4]>, ImageError> {
    let values = text
        .split(',')
        .map(|part| {
            part.trim().parse::<usize>().map_err(|error| {
                ImageError::InvalidMetadata(format!("invalid box {text:?}: {error}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty() || values.len() % 4 != 0 {
        return Err(ImageError::CasaSelection(
            "box not specified correctly.".to_string(),
        ));
    }
    Ok(values
        .as_chunks::<4>()
        .0
        .iter()
        .map(|b| [b[0], b[1], b[2], b[3]])
        .collect())
}

/// A task `box` parameter as a pixel region, validated as CASA's
/// `CasacRegionManager::fromBCS` validates it: each quadruplet's trc
/// inside the direction plane, and each box's blc no greater than its
/// trc, with CASA's own refusals. A box's x and y run along the
/// direction plane's axes wherever they are.
pub(super) fn parse_box_region(
    text: &str,
    image_shape: &[usize],
    plane: [usize; 2],
) -> Result<PixelRegion, ImageError> {
    let boxes = parse_boxes(text)?;
    let [x, y] = plane;
    for [x0, y0, x1, y1] in &boxes {
        if image_shape.len() < 2 || *x1 >= image_shape[x] || *y1 >= image_shape[y] {
            return Err(ImageError::CasaSelection(
                "trc in box spec is greater than or equal to number of direction \
                 coordinate pixels in the image"
                    .to_string(),
            ));
        }
        if x1 < x0 || y1 < y0 {
            let mut blc = vec![0; image_shape.len()];
            blc[x] = *x0;
            blc[y] = *y0;
            let mut trc: Vec<usize> = image_shape.iter().map(|axis| axis - 1).collect();
            trc[x] = *x1;
            trc[y] = *y1;
            return Err(ImageError::CasaSelection(format!(
                "LCBox::LCBox - blc {blc:?} must be <= trc {trc:?}"
            )));
        }
    }
    Ok(PixelRegion::Boxes(boxes))
}

/// CASA imstat's `flux` over the selection, as casacore's
/// `ImageStatistics::_flux` computes it: a brightness in kelvin times the
/// pixel area in square arcseconds; a brightness per beam divided by the
/// beam's area in pixels (one beam; per-plane beams are not computed). Over
/// a selection of more than one channel the flux is integrated along the
/// spectral axis: times the channel width in km/s (radio, casacore's default
/// velocity) when the axis has a rest frequency, else its frequency
/// increment in Hz.
fn image_flux<T: ImagePixel>(
    image: &PagedImage<T>,
    sum: f64,
    selection: &Selection,
) -> Option<f64> {
    image_flux_with_integration(image, sum, selection, true)
}

fn image_flux_with_integration<T: ImagePixel>(
    image: &PagedImage<T>,
    sum: f64,
    selection: &Selection,
    spectral_axis_collapsed: bool,
) -> Option<f64> {
    let spectral_axis = spectral_axis_collapsed
        .then(|| image.find_axis(CoordinateType::Spectral))
        .flatten()
        .filter(|&axis| selection.shape[axis] > 1);
    let channels = spectral_axis.map(|axis| [selection.start[axis], selection.start[axis] + 1]);
    let beam = image
        .units()
        .contains("/beam")
        .then(|| image.image_info().ok()?.beam_set.single_beam())
        .flatten();
    statistics_flux_scale(image.coordinates(), image.units(), beam, channels)
        .map(|factor| sum * factor)
}

/// Converts a sum of image pixels to the flux reported by CASA statistics.
///
/// The factor uses square arcseconds for temperature and pixels per beam for
/// per-beam brightness. If the spectral axis is collapsed over multiple
/// channels, `channels` gives the first two measured spectral pixels.
pub fn statistics_flux_scale(
    coordinates: &CoordinateSystem,
    units: &str,
    beam: Option<GaussianBeam>,
    channels: Option<[usize; 2]>,
) -> Option<f64> {
    let pixel_area = direction_pixel_area(coordinates)?;
    let flux_factor = if units.contains('K') {
        let arcsec_per_rad = 180.0 * 3600.0 / std::f64::consts::PI;
        pixel_area * arcsec_per_rad * arcsec_per_rad
    } else if units.contains("/beam") {
        let beam_area = beam?.area();
        if beam_area <= 0.0 {
            return None;
        }
        pixel_area / beam_area
    } else {
        return None;
    };
    let channel_width = channels
        .and_then(|pixels| spectral_integration(coordinates, pixels))
        .unwrap_or(1.0);
    Some(flux_factor * channel_width)
}

/// The width casacore integrates `flux` over: one channel of the selected
/// spectral axis, when the selection holds more than one.
fn spectral_integration(
    coordinates: &CoordinateSystem,
    [first, second]: [usize; 2],
) -> Option<f64> {
    let coordinate = coordinates.coordinate(coordinates.find_coordinate(CoordinateType::Spectral)?);
    let rest = spectral_rest_frequency(coordinates).unwrap_or(0.0);
    if rest > 0.0 {
        // The selection's own first two channels, as casacore's sub-image.
        let f0 = coordinate.to_world(&[first as f64]).ok()?[0];
        let f1 = coordinate.to_world(&[second as f64]).ok()?[0];
        Some(SPEED_OF_LIGHT_KM_S * (f1 - f0).abs() / rest)
    } else {
        Some(coordinate.increment()[0] * second.abs_diff(first) as f64)
    }
}

fn direction_pixel_area(coords: &CoordinateSystem) -> Option<f64> {
    for idx in 0..coords.n_coordinates() {
        let coord = coords.coordinate(idx);
        if coord.coordinate_type() == CoordinateType::Direction && coord.n_pixel_axes() >= 2 {
            let increment = coord.increment();
            return Some(increment[0].abs() * increment[1].abs());
        }
    }
    None
}

fn combined_selection_mask<T: ImagePixel>(
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

pub(super) fn patch_saved_coords_keyword(
    path: &Path,
    coords: casa_types::RecordValue,
) -> Result<(), ImageError> {
    let mut table = Table::open(TableOptions::new(path))?;
    table.keywords_mut().upsert("coords", Value::Record(coords));
    table.save_metadata_only(TableOptions::new(path))?;
    Ok(())
}

#[cfg(test)]
fn all_indices(shape: &[usize]) -> Vec<Vec<usize>> {
    fn push_indices(
        shape: &[usize],
        axis: usize,
        current: &mut Vec<usize>,
        out: &mut Vec<Vec<usize>>,
    ) {
        if axis == shape.len() {
            out.push(current.clone());
            return;
        }
        for value in 0..shape[axis] {
            current.push(value);
            push_indices(shape, axis + 1, current, out);
            current.pop();
        }
    }
    let mut out = Vec::new();
    push_indices(shape, 0, &mut Vec::new(), &mut out);
    out
}

#[cfg(test)]
fn collapse_moment_reference<T>(
    input: &ArrayD<T>,
    mask: Option<&ArrayD<bool>>,
    axis: usize,
    coords: &[f64],
    integrated_scale: f64,
    includepix: Option<[f64; 2]>,
    moment: i32,
) -> Result<(ArrayD<f32>, ArrayD<bool>), ImageError>
where
    T: Into<f64> + Copy,
{
    let mut out_shape = input.shape().to_vec();
    out_shape.remove(axis);
    let mut out = ArrayD::<f32>::zeros(IxDyn(&out_shape));
    let mut out_mask = ArrayD::<bool>::from_elem(IxDyn(&out_shape), false);
    for out_index in all_indices(&out_shape) {
        let mut samples = Vec::new();
        for (chan, coord) in coords.iter().enumerate().take(input.shape()[axis]) {
            let mut full_index = out_index.clone();
            full_index.insert(axis, chan);
            if let Some(mask) = mask
                && !mask[IxDyn(&full_index)]
            {
                continue;
            }
            let value = (*input.get(IxDyn(&full_index)).ok_or_else(|| {
                ImageError::InvalidMetadata("moment index out of bounds".to_string())
            })?)
            .into();
            if let Some([min, max]) = includepix
                && (value < min || value > max)
            {
                continue;
            }
            samples.push((value, *coord));
        }
        let npts = samples.len();
        let (result, valid) = if npts == 0 {
            (0.0, false)
        } else {
            match moment {
                -1 => (
                    samples.iter().map(|(value, _)| *value).sum::<f64>() / npts as f64,
                    true,
                ),
                0 => (
                    samples.iter().map(|(value, _)| *value).sum::<f64>() * integrated_scale,
                    true,
                ),
                1 | 2 => {
                    let s0 = samples.iter().map(|(value, _)| *value).sum::<f64>();
                    if s0 == 0.0 {
                        (0.0, false)
                    } else {
                        let mean = samples
                            .iter()
                            .map(|(value, coord)| value * coord)
                            .sum::<f64>()
                            / s0;
                        if moment == 1 {
                            (mean, true)
                        } else {
                            let variance = samples
                                .iter()
                                .map(|(value, coord)| value * (coord - mean).powi(2))
                                .sum::<f64>()
                                / s0;
                            if variance < 0.0 {
                                (0.0, false)
                            } else {
                                (variance.sqrt(), true)
                            }
                        }
                    }
                }
                3 => {
                    let mut values = samples.iter().map(|(value, _)| *value).collect::<Vec<_>>();
                    values.sort_by(|left, right| left.total_cmp(right));
                    if values.len() % 2 == 0 {
                        let upper = values.len() / 2;
                        ((values[upper - 1] + values[upper]) / 2.0, true)
                    } else {
                        (values[values.len() / 2], true)
                    }
                }
                _ => (0.0, false),
            }
        };
        out[IxDyn(&out_index)] = result as f32;
        out_mask[IxDyn(&out_index)] = valid;
    }
    Ok((
        out.insert_axis(ndarray::Axis(axis)).to_owned(),
        out_mask.insert_axis(ndarray::Axis(axis)).to_owned(),
    ))
}

fn coordinate_for_axis(
    coords: &CoordinateSystem,
    axis: usize,
) -> Option<&casa_coordinates::CoordinateModel> {
    coords
        .pixel_axis_owner(axis)
        .map(|(index, _)| coords.coordinate(index))
}

fn spectral_rest_frequency(coords: &CoordinateSystem) -> Option<f64> {
    for idx in 0..coords.n_coordinates() {
        let coord = coords.coordinate(idx);
        if let casa_coordinates::CoordinateModel::Spectral(spectral) = coord {
            return Some(spectral.rest_frequency());
        }
    }
    None
}

fn image_pixel_type<T: ImagePixel>() -> ImagePixelType {
    if std::any::TypeId::of::<T>() == std::any::TypeId::of::<f32>() {
        ImagePixelType::Float32
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<f64>() {
        ImagePixelType::Float64
    } else {
        ImagePixelType::Complex32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ImageType;
    use casa_coordinates::{
        DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate, StokesCoordinate,
        StokesType,
    };
    use casa_types::Complex32;
    use casa_types::measures::direction::DirectionRef;
    use casa_types::measures::frequency::FrequencyRef;
    use fitsio::FitsFile;

    /// An imstat request for a classic run with one selection.
    fn imstat_request(
        imagename: &Path,
        box_pixels: Option<&str>,
        region: Option<&str>,
    ) -> ImstatRequest {
        ImstatRequest {
            imagename: imagename.to_path_buf(),
            box_pixels: box_pixels.map(str::to_string),
            region: region.map(str::to_string),
            ..ImstatRequest::default()
        }
    }

    /// The global statistics of a classic run.
    fn imstat_global(request: &ImstatRequest) -> ImageStatisticsSummary {
        match imstat(request).unwrap() {
            ImstatOutcome::Global(summary) => summary,
            other => panic!("expected global statistics: {other:?}"),
        }
    }

    #[test]
    fn parse_box_is_inclusive() {
        assert_eq!(parse_box("100,100,150,150").unwrap(), [100, 100, 150, 150]);
    }

    #[test]
    fn parse_indices_expands_casa_range() {
        assert_eq!(
            parse_image_channel_selection("4~6,8", 10).unwrap(),
            vec![4, 5, 6, 8]
        );
        assert_eq!(
            parse_image_channel_selection("0~8^2", 10).unwrap(),
            vec![0, 2, 4, 6, 8]
        );
    }

    #[test]
    fn image_analysis_task_dispatch_roundtrips_real_image_products() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("source.image");
        let moment_path = temp.path().join("moment.image");
        let fits_path = temp.path().join("source.fits");
        let imported_path = temp.path().join("imported.image");
        create_spectral_test_image(&source_path);

        let header = imhead(&source_path).unwrap();
        assert_eq!(header.shape, vec![2, 2, 3]);
        assert_eq!(header.units, "Jy/beam");
        assert_eq!(header.object_name, "NGC Test");
        assert_eq!(header.axes.len(), 3);
        assert!(header.restoring_beam.is_some());

        let imstat_result = imstat(&ImstatRequest {
            imagename: source_path.clone(),
            box_pixels: Some("0,0,1,1".to_string()),
            chans: Some("1~2".to_string()),
            includepix: Some([5.0, 12.0]),
            ..ImstatRequest::default()
        })
        .unwrap();
        let ImstatOutcome::Global(stats) = imstat_result else {
            panic!("expected a global imstat result");
        };
        assert_eq!(stats.blc, vec![0, 0, 1]);
        assert_eq!(stats.trc, vec![1, 1, 2]);
        assert_eq!(stats.npts, 8.0);
        assert_eq!(stats.units, "Jy/beam");
        let source = PagedImage::<f32>::open(&source_path).unwrap();
        // casacore integrates the two selected channels' flux density over
        // one channel's radio velocity width: 2 MHz at a 372 GHz rest
        // frequency.
        let channel_km_s = SPEED_OF_LIGHT_KM_S * 2.0e6 / 372.0e9;
        let expected_flux = stats.sum * direction_pixel_area(source.coordinates()).unwrap()
            / source
                .image_info()
                .unwrap()
                .beam_set
                .single_beam()
                .unwrap()
                .area()
            * channel_km_s;
        assert!((stats.flux.unwrap() - expected_flux).abs() < 1e-12 * expected_flux);

        let moment = immoments(&ImmomentsRequest {
            imagename: source_path.clone(),
            outfile: moment_path.clone(),
            moments: vec![0],
            chans: Some("0~2".to_string()),
            includepix: Some([2.0, 12.0]),
            mask: None,
            overwrite: false,
            ..ImmomentsRequest::default()
        })
        .unwrap();
        assert_eq!(moment.shape, vec![2, 2, 1]);
        assert_eq!(moment.products[0].units, "Jy/beam.km/s");
        assert_eq!(moment.products[0].valid_profiles, 4);
        let moment_image = PagedImage::<f32>::open(&moment_path).unwrap();
        // CASA creates mask0 even when every profile is valid.
        assert_eq!(moment_image.default_mask_name().as_deref(), Some("mask0"));
        for (moment_number, expected_units) in
            [(-1, "Jy/beam"), (1, "km/s"), (2, "km/s"), (3, "Jy/beam")]
        {
            let output_path = temp.path().join(format!("moment{moment_number}.image"));
            let summary = immoments(&ImmomentsRequest {
                imagename: source_path.clone(),
                outfile: output_path,
                moments: vec![moment_number],
                chans: Some("0~2".to_string()),
                includepix: Some([2.0, 12.0]),
                mask: None,
                overwrite: false,
                ..ImmomentsRequest::default()
            })
            .unwrap();
            assert_eq!(summary.products[0].moment, moment_number);
            assert_eq!(summary.products[0].units, expected_units);
            assert_eq!(summary.products[0].valid_profiles, 4);
        }

        let mask_path = temp.path().join("source.pb");
        create_spectral_mask_image(&mask_path);
        let masked_moment_path = temp.path().join("masked-moment.image");
        let masked_moment = immoments(&ImmomentsRequest {
            imagename: source_path.clone(),
            outfile: masked_moment_path,
            moments: vec![0],
            chans: Some("0~2".to_string()),
            includepix: None,
            mask: Some(format!("{}>0.3", mask_path.display())),
            overwrite: false,
            ..ImmomentsRequest::default()
        })
        .unwrap();
        assert_eq!(masked_moment.products[0].valid_profiles, 3);

        let pv_path = temp.path().join("pv.image");
        let pv_error = impv(&ImpvRequest {
            imagename: source_path.clone(),
            outfile: pv_path.clone(),
            start: Some(PvPosition::Pixel([0.0, 0.0])),
            end: Some(PvPosition::Pixel([1.0, 1.0])),
            chans: Some("1~2".into()),
            ..ImpvRequest::default()
        })
        .unwrap_err();
        assert!(pv_error.to_string().contains("square direction pixels"));
        assert!(!pv_path.exists());

        let subimage_path = temp.path().join("sub.image");
        let subimage = imsubimage(&ImsubimageRequest {
            imagename: source_path.clone(),
            outfile: subimage_path.clone(),
            box_pixels: Some("0,0,0,1".to_string()),
            chans: Some("1~2".to_string()),
            ..ImsubimageRequest::default()
        })
        .unwrap();
        assert_eq!(subimage.shape, vec![1, 2, 2]);
        let subimage_image = PagedImage::<f32>::open(&subimage_path).unwrap();
        assert_eq!(subimage_image.shape(), &[1, 2, 2]);

        let product_path = temp.path().join("product.image");
        let math = immath(&ImmathRequest {
            imagename: vec![subimage_path.clone(), subimage_path.clone()],
            outfile: product_path.clone(),
            expr: "IM0 * IM1".to_string(),
            overwrite: false,
            ..ImmathRequest::default()
        })
        .unwrap();
        assert_eq!(math.shape, vec![1, 2, 2]);
        assert_eq!(math.valid_pixels, 4);
        let product_image = PagedImage::<f32>::open(&product_path).unwrap();
        assert_eq!(product_image.shape(), &[1, 2, 2]);

        let scaled_path = temp.path().join("scaled.image");
        let scaled = immath(&ImmathRequest {
            imagename: vec![subimage_path.clone()],
            outfile: scaled_path.clone(),
            expr: "1.222e6*IM0/1.579^2/(29.30*29.03)".to_string(),
            overwrite: false,
            ..ImmathRequest::default()
        })
        .unwrap();
        assert_eq!(scaled.shape, vec![1, 2, 2]);
        assert_eq!(scaled.valid_pixels, 4);
        let scaled_image = PagedImage::<f32>::open(&scaled_path).unwrap();
        let subimage_input = subimage_image.get_slice(&[0, 0, 0], &[1, 1, 1]).unwrap();
        let scaled_data = scaled_image.get_slice(&[0, 0, 0], &[1, 1, 1]).unwrap();
        let factor = 1.222e6 / 1.579_f64.powi(2) / (29.30 * 29.03);
        let expected = f64::from(subimage_input[[0, 0, 0]]) * factor;
        assert!((f64::from(scaled_data[[0, 0, 0]]) - expected).abs() < 1.0e-3);
        let put_header = imhead_put(&scaled_path, "bunit", "K").unwrap();
        assert_eq!(put_header.units, "K");
        assert_eq!(PagedImage::<f32>::open(&scaled_path).unwrap().units(), "K");

        let pbcor_path = temp.path().join("pbcor.image");
        let pbcor = impbcor(&ImpbcorRequest {
            imagename: source_path.clone(),
            pbimage: mask_path.clone().into(),
            outfile: pbcor_path.clone(),
            mode: "divide".to_string(),
            cutoff: 0.3,
            box_selection: None,
            region: None,
            chans: None,
            stokes: None,
            mask: None,
            stretch: false,
            overwrite: false,
        })
        .unwrap();
        assert_eq!(pbcor.shape, vec![2, 2, 3]);
        assert_eq!(pbcor.units, "Jy/beam");
        assert_eq!(pbcor.valid_pixels, 9);
        let pbcor_image = PagedImage::<f32>::open(&pbcor_path).unwrap();
        assert_eq!(pbcor_image.default_mask_name(), Some("mask0".to_string()));
        let pbcor_data = pbcor_image.get_slice(&[1, 0, 0], &[1, 1, 1]).unwrap();
        let source_data = source.get_slice(&[1, 0, 0], &[1, 1, 1]).unwrap();
        assert!((pbcor_data[[0, 0, 0]] - source_data[[0, 0, 0]]).abs() < 1.0e-6);

        exportfits(&ExportFitsRequest {
            imagename: source_path.clone(),
            fitsimage: fits_path.clone(),
            velocity: true,
            overwrite: false,
            ..ExportFitsRequest::default()
        })
        .unwrap();
        assert!(fits_path.exists());

        importfits(&ImportFitsRequest {
            fitsimage: fits_path.clone(),
            imagename: imported_path.clone(),
            ..ImportFitsRequest::default()
        })
        .unwrap();
        let imported = PagedImage::<f32>::open(&imported_path).unwrap();
        assert_eq!(imported.shape(), &[2, 2, 3]);
        assert_eq!(imported.units(), "Jy/beam");
    }

    #[test]
    fn image_analysis_rejects_unsupported_pixels_and_invalid_selections() {
        let temp = tempfile::tempdir().unwrap();
        let real_path = temp.path().join("real.image");
        let complex_path = temp.path().join("complex.image");
        let output_path = temp.path().join("moment.image");
        create_direction_only_test_image(&real_path);
        create_complex_test_image(&complex_path);

        let complex_header = imhead(&complex_path).unwrap();
        assert_eq!(complex_header.pixel_type, "Complex32");
        assert_eq!(complex_header.units, "Jy");
        assert_eq!(complex_header.axes.len(), 2);

        assert!(matches!(
            imstat(&imstat_request(&complex_path, None, None)),
            Err(ImageError::InvalidMetadata(message))
                if message.contains("real-valued images")
        ));
        assert!(matches!(
            immoments(&ImmomentsRequest {
                imagename: complex_path.clone(),
                outfile: output_path.clone(),
                moments: vec![0],
                chans: None,
                includepix: None,
                mask: None,
                overwrite: false,
                ..ImmomentsRequest::default()
            }),
            Err(ImageError::InvalidMetadata(message))
                if message.contains("real-valued images")
        ));
        assert!(matches!(
            export_fits(&complex_path, temp.path().join("complex.fits"), false, false),
            Err(ImageError::InvalidMetadata(message))
                if message.contains("real-valued images")
        ));

        assert!(matches!(
            imstat(&imstat_request(&real_path, Some("0,0,4,4"), None)),
            Err(ImageError::CasaSelection(message))
                if message.contains("trc in box spec")
        ));
        assert!(matches!(
            imstat(&ImstatRequest {
                imagename: real_path.clone(),
                chans: Some("0".to_string()),
                ..ImstatRequest::default()
            }),
            Err(ImageError::InvalidMetadata(message))
                if message.contains("spectral axis")
        ));
        assert!(matches!(
        imstat(&imstat_request(
                        &real_path,
                        Some("0,0,1,1"),
                        Some("box:0,0,1,1"),
                    )),
                    Err(ImageError::InvalidMetadata(message))
                        if message.contains("either box or region")
                ));
        assert!(matches!(
            parse_box("2,0,1,1"),
            Err(ImageError::InvalidMetadata(message))
                if message.contains("inclusive upper bounds")
        ));
        assert!(matches!(
            parse_image_channel_selection("3~1", 4),
            Err(ImageError::InvalidMetadata(message))
                if message.contains("end precedes start")
        ));
        assert!(matches!(
            parse_image_channel_selection("0,9", 4),
            Err(ImageError::ShapeMismatch { .. })
        ));
        assert_eq!(
            parse_image_channel_selection("2,1,2", 4).unwrap(),
            vec![1, 2]
        );

        assert!(matches!(
            immoments(&ImmomentsRequest {
                imagename: real_path.clone(),
                outfile: output_path.clone(),
                moments: vec![12],
                chans: None,
                includepix: None,
                mask: None,
                overwrite: false,
                ..ImmomentsRequest::default()
            }),
            Err(ImageError::InvalidMetadata(message))
                if message.contains("moment numbers -1 through 11")
        ));
        assert!(matches!(
            immoments(&ImmomentsRequest {
                imagename: real_path,
                outfile: output_path,
                moments: vec![0],
                chans: None,
                includepix: None,
                mask: None,
                overwrite: false,
                ..ImmomentsRequest::default()
            }),
            Err(ImageError::InvalidMetadata(message))
                if message.contains("spectral axis")
        ));
    }

    #[test]
    fn imstat_accepts_inline_and_crtf_pixel_box_regions() {
        let temp = tempfile::tempdir().unwrap();
        let image_path = temp.path().join("real.image");
        let region_path = temp.path().join("source.crtf");
        create_direction_only_test_image(&image_path);

        let inline = imstat_global(&imstat_request(
            &image_path,
            None,
            Some("box[[0pix,0pix],[0pix,0pix]]"),
        ));
        assert_eq!(inline.npts, 1.0);
        assert_eq!(inline.sum, 1.0);

        fs::write(
            &region_path,
            "#CRTFv0 CASA Region Text Format version 0\nbox[[0pix,0pix],[1pix,0pix]]\n",
        )
        .unwrap();

        let from_file = imstat_global(&imstat_request(
            &image_path,
            None,
            Some(region_path.to_str().unwrap()),
        ));
        assert_eq!(from_file.npts, 2.0);
        assert_eq!(from_file.sum, 3.0);
        assert_eq!(from_file.blc, vec![0, 0]);
        assert_eq!(from_file.trc, vec![1, 0]);
    }

    #[test]
    fn imstat_accepts_crtf_world_box_regions() {
        let temp = tempfile::tempdir().unwrap();
        let image_path = temp.path().join("real.image");
        let region_path = temp.path().join("source.crtf");
        create_direction_only_test_image(&image_path);

        let image = PagedImage::<f32>::open(&image_path).unwrap();
        let first = image.coordinates().to_world(&[0.0, 0.0]).unwrap();
        let second = image.coordinates().to_world(&[1.0, 0.0]).unwrap();
        fs::write(
            &region_path,
            format!(
                "#CRTFv0 CASA Region Text Format version 0\nbox[[{:.15}rad,{:.15}rad],[{:.15}rad,{:.15}rad]]\n",
                first[0], first[1], second[0], second[1]
            ),
        )
        .unwrap();

        let from_file = imstat_global(&imstat_request(
            &image_path,
            None,
            Some(region_path.to_str().unwrap()),
        ));
        assert_eq!(from_file.npts, 2.0);
        assert_eq!(from_file.sum, 3.0);
        assert_eq!(from_file.blc, vec![0, 0]);
        assert_eq!(from_file.trc, vec![1, 0]);
    }

    #[test]
    fn imstat_accepts_crtf_pixel_polygon_regions() {
        let temp = tempfile::tempdir().unwrap();
        let image_path = temp.path().join("real.image");
        let region_path = temp.path().join("source.crtf");
        create_direction_grid_test_image(&image_path, &[3, 3]);
        fs::write(
            &region_path,
            "#CRTFv0 CASA Region Text Format version 0\npoly [[0pix,0pix], [2pix,0pix], [0pix,2pix]]\n",
        )
        .unwrap();

        let from_file = imstat_global(&imstat_request(
            &image_path,
            None,
            Some(region_path.to_str().unwrap()),
        ));

        assert_eq!(from_file.blc, vec![0, 0]);
        assert_eq!(from_file.trc, vec![2, 2]);
        assert_eq!(from_file.npts, 3.0);
        assert_eq!(from_file.sum, 14.0);
    }

    #[test]
    fn imstat_accepts_crtf_world_polygon_regions() {
        let temp = tempfile::tempdir().unwrap();
        let image_path = temp.path().join("real.image");
        let region_path = temp.path().join("source.crtf");
        create_direction_grid_test_image(&image_path, &[3, 3]);
        let image = PagedImage::<f32>::open(&image_path).unwrap();
        let a = image.coordinates().to_world(&[0.0, 0.0]).unwrap();
        let b = image.coordinates().to_world(&[2.0, 0.0]).unwrap();
        let c = image.coordinates().to_world(&[0.0, 2.0]).unwrap();
        fs::write(
            &region_path,
            format!(
                "#CRTFv0 CASA Region Text Format version 0\npoly [[{:.15}rad,{:.15}rad], [{:.15}rad,{:.15}rad], [{:.15}rad,{:.15}rad]]\n",
                a[0], a[1], b[0], b[1], c[0], c[1]
            ),
        )
        .unwrap();

        let from_file = imstat_global(&imstat_request(
            &image_path,
            None,
            Some(region_path.to_str().unwrap()),
        ));

        assert_eq!(from_file.blc, vec![0, 0]);
        assert_eq!(from_file.trc, vec![2, 2]);
        assert_eq!(from_file.npts, 3.0);
        assert_eq!(from_file.sum, 14.0);
    }

    #[test]
    fn crtf_parsers_allow_optional_keyword_whitespace_with_word_boundary() {
        assert_eq!(
            crtf_polygon_parts("poly[[0pix,0pix],[2pix,0pix],[2pix,2pix],[0pix,2pix]]").unwrap(),
            Some(vec![
                ["0pix".to_string(), "0pix".to_string()],
                ["2pix".to_string(), "0pix".to_string()],
                ["2pix".to_string(), "2pix".to_string()],
                ["0pix".to_string(), "2pix".to_string()],
            ])
        );
        assert_eq!(
            crtf_box_parts("box [[1pix, 2pix], [3pix, 4pix]]").unwrap(),
            Some([
                "1pix".to_string(),
                "2pix".to_string(),
                "3pix".to_string(),
                "4pix".to_string(),
            ])
        );
        assert_eq!(crtf_polygon_parts("my_poly[0].crtf").unwrap(), None);
        assert_eq!(crtf_box_parts("my_box[[0]].crtf").unwrap(), None);
    }

    #[test]
    fn fits_import_export_roundtrip_preserves_pixels_wcs_units_beam_and_object() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("source.image");
        let first_fits = temp.path().join("first.fits");
        let imported_path = temp.path().join("imported.image");
        let second_fits = temp.path().join("second.fits");

        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.2, -0.4],
            [-2.0e-6, 3.0e-6],
            [1.0, 0.0],
        ));
        coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I, StokesType::Q]));
        coordinates.add_coordinate(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            372.0e9,
            2.0e6,
            2.0,
            372.0e9,
        ));

        // Observed at 2020-05-31 12:00 TAI: the date keeps its time scale
        // (#1017).
        coordinates.obs_info_mut().date = Some(casa_types::measures::epoch::MEpoch::from_mjd(
            59_000.5,
            casa_types::measures::EpochRef::TAI,
        ));

        let shape = vec![3, 2, 2, 5];
        let mut source =
            PagedImage::<f32>::create(shape.clone(), coordinates, &source_path).unwrap();
        let data = ArrayD::from_shape_vec(
            IxDyn(&shape).f(),
            (0..shape.iter().product::<usize>())
                .map(|value| value as f32 + 0.25)
                .collect(),
        )
        .unwrap();
        source.put_slice(&data, &[0, 0, 0, 0]).unwrap();
        source.set_units("Jy/beam").unwrap();
        source
            .set_image_info(&ImageInfo {
                beam_set: ImageBeamSet::new(GaussianBeam::new(
                    1.7e-4_f64.to_radians(),
                    1.2e-4_f64.to_radians(),
                    (-34.5_f64).to_radians(),
                )),
                image_type: ImageType::Intensity,
                object_name: "TW Hya".to_string(),
            })
            .unwrap();
        source.save().unwrap();

        export_fits(&source_path, &first_fits, false, false).unwrap();
        import_fits(&first_fits, &imported_path, false).unwrap();
        export_fits(&imported_path, &second_fits, false, false).unwrap();

        let imported = PagedImage::<f32>::open(&imported_path).unwrap();
        assert_eq!(imported.shape(), &[3, 2, 5, 2]);
        let imported_pixels = imported.get().unwrap();
        for x in 0..3 {
            for y in 0..2 {
                for st in 0..2 {
                    for ch in 0..5 {
                        assert_eq!(imported_pixels[[x, y, ch, st]], data[[x, y, st, ch]]);
                    }
                }
            }
        }
        assert_eq!(imported.units(), "Jy/beam");
        let info = imported.image_info().unwrap();
        assert_eq!(info.object_name, "TW Hya");
        let beam = info.beam_set.single_beam().unwrap();
        assert!((beam.major_in("deg").unwrap() - 1.7e-4).abs() < 1.0e-12);
        assert!((beam.minor_in("deg").unwrap() - 1.2e-4).abs() < 1.0e-12);
        assert!((beam.position_angle_in("deg").unwrap() + 34.5).abs() < 1.0e-12);
        let date = imported.coordinates().obs_info().date.as_ref().unwrap();
        assert_eq!(date.refer(), casa_types::measures::EpochRef::TAI);
        assert_eq!(date.value().as_mjd(), 59_000.5);
        // As CASA 6.7.6's exportfits writes the date: ISO, in its own scale.
        assert_eq!(
            read_fits_key_as_string(&first_fits, "DATE-OBS"),
            "2020-05-31T12:00:00.000000"
        );
        assert_eq!(read_fits_key_as_string(&first_fits, "TIMESYS"), "TAI");

        assert_eq!(
            read_fits_pixels(&first_fits),
            read_fits_pixels(&second_fits)
        );
        for key in [
            "NAXIS", "NAXIS1", "NAXIS2", "NAXIS3", "NAXIS4", "BUNIT", "BMAJ", "BMIN", "BPA",
            "OBJECT", "RADESYS", "CTYPE1", "CTYPE2", "CTYPE3", "CTYPE4", "CRVAL1", "CRVAL2",
            "CRVAL3", "CRVAL4", "CRPIX1", "CRPIX2", "CRPIX3", "CRPIX4", "CDELT1", "CDELT2",
            "CDELT3", "CDELT4", "CUNIT1", "CUNIT2", "CUNIT3", "CUNIT4", "RESTFRQ", "SPECSYS",
            "DATE-OBS", "TIMESYS",
        ] {
            assert_fits_key_matches(&first_fits, &second_fits, key);
        }
    }

    /// A 2 by 2 FITS sky image of float pixels whose header holds `cards`,
    /// each written as given, after the required ones and a SIN direction.
    fn write_fits_with_cards(path: &Path, cards: &[&str]) {
        let mut header = vec![
            "SIMPLE  =                    T",
            "BITPIX  =                  -32",
            "NAXIS   =                    2",
            "NAXIS1  =                    2",
            "NAXIS2  =                    2",
            "CTYPE1  = 'RA---SIN'",
            "CTYPE2  = 'DEC--SIN'",
            "CRVAL1  =                 68.8",
            "CRVAL2  =                -22.9",
            "CDELT1  =           -0.0002778",
            "CDELT2  =            0.0002778",
            "CRPIX1  =                  1.0",
            "CRPIX2  =                  1.0",
            "CUNIT1  = 'deg'",
            "CUNIT2  = 'deg'",
        ];
        header.extend_from_slice(cards);
        header.push("END");
        let mut bytes = header
            .iter()
            .flat_map(|card| format!("{card:<80}").into_bytes())
            .collect::<Vec<u8>>();
        bytes.resize(bytes.len().div_ceil(2880) * 2880, b' ');
        for value in [1.0_f32, 2.0, 3.0, 4.0] {
            bytes.extend(value.to_be_bytes());
        }
        bytes.resize(bytes.len().div_ceil(2880) * 2880, 0);
        fs::write(path, bytes).unwrap();
    }

    #[test]
    fn importfits_takes_the_date_past_unusable_date_cards() {
        // An MJD-OBS with no value (FITS's way to say it is unknown) or one
        // CFITSIO cannot read as a number leaves DATE-OBS to give the date,
        // as CASA 6.7.6 imports such headers; it once stopped the import
        // (#1020 review). A TIMESYS with no value is UTC, and a DATE-OBS with
        // none gives no date.
        use casa_types::measures::EpochRef;
        let temp = tempfile::tempdir().unwrap();
        let date = "DATE-OBS= '2000-01-01T02:57:46.666569'";
        let no_mjd = "MJD-OBS =                      / date unavailable";
        let cases: [(&[&str], Option<EpochRef>); 5] = [
            (&[date, "TIMESYS = 'UTC'", no_mjd], Some(EpochRef::UTC)),
            (
                &[date, "TIMESYS = 'TAI'", "MJD-OBS = 'garbage'"],
                Some(EpochRef::TAI),
            ),
            (
                &[date, "TIMESYS =                      / unknown"],
                Some(EpochRef::UTC),
            ),
            (&["DATE-OBS=                      / unknown"], None),
            (&[], None),
        ];
        for (index, (cards, expected)) in cases.into_iter().enumerate() {
            let fits = temp.path().join(format!("{index}.fits"));
            let image_path = temp.path().join(format!("{index}.image"));
            write_fits_with_cards(&fits, cards);
            import_fits(&fits, &image_path, false)
                .unwrap_or_else(|error| panic!("case {index}: {error}"));
            let image = PagedImage::<f32>::open(&image_path).unwrap();
            let date = image.coordinates().obs_info().date.as_ref();
            match expected {
                Some(reference) => {
                    let date = date.unwrap_or_else(|| panic!("case {index}: no date"));
                    assert_eq!(date.refer(), reference, "case {index}");
                    let seconds = (date.value().as_mjd() - 51_544.123_456_789) * 86_400.0;
                    assert!(seconds.abs() < 1e-5, "case {index}: {seconds} s");
                }
                None => assert!(date.is_none(), "case {index}"),
            }
        }
    }

    #[test]
    fn fits_import_export_overwrite_policy_preserves_double_images() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("source.image");
        let fits_path = temp.path().join("source.fits");
        let imported_path = temp.path().join("imported.image");
        create_double_spectral_test_image(&source_path);

        export_fits(&source_path, &fits_path, false, false).unwrap();
        assert!(matches!(
            export_fits(&source_path, &fits_path, false, false),
            Err(ImageError::Io(message)) if message.contains("already exists")
        ));
        let export_summary = export_fits(&source_path, &fits_path, false, true).unwrap();
        assert_eq!(export_summary.shape, vec![2, 2, 2]);

        import_fits(&fits_path, &imported_path, false).unwrap();
        assert!(matches!(
            import_fits(&fits_path, &imported_path, false),
            Err(ImageError::Io(message)) if message.contains("already exists")
        ));
        let import_summary = import_fits(&fits_path, &imported_path, true).unwrap();
        assert_eq!(import_summary.pixel_type, "Float64");
        assert_eq!(import_summary.units, "K");
        assert_eq!(import_summary.object_name, "Double Test");

        let imported = PagedImage::<f64>::open(&imported_path).unwrap();
        assert_eq!(imported.shape(), &[2, 2, 2]);
        assert_eq!(imported.units(), "K");

        let header = imhead(&imported_path).unwrap();
        assert_eq!(header.pixel_type, "Float64");
        assert_eq!(header.object_name, "Double Test");
        let stats = imstat_global(&ImstatRequest {
            imagename: imported_path.clone(),
            box_pixels: Some("0,0,1,1".to_string()),
            chans: Some("0~1".to_string()),
            includepix: Some([1.0, 8.0]),
            ..ImstatRequest::default()
        });
        assert_eq!(stats.npts, 7.0);
        assert_eq!(stats.units, "K");
    }

    #[test]
    fn exportfits_writes_default_mask_as_nan_and_velocity_wcs() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("masked.image");
        let fits_path = temp.path().join("masked.fits");

        let rest_frequency = 372.0e9;
        let frequency_increment = -620_000.0;
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.2, -0.4],
            [-2.0e-6, 3.0e-6],
            [0.0, 0.0],
        ));
        coordinates.add_coordinate(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            rest_frequency,
            frequency_increment,
            0.0,
            rest_frequency,
        ));
        let shape = vec![2, 2, 3];
        let mut image =
            PagedImage::<f32>::create(shape.clone(), coordinates, &source_path).unwrap();
        let data = ArrayD::from_shape_vec(
            IxDyn(&shape).f(),
            (0..shape.iter().product::<usize>())
                .map(|value| value as f32)
                .collect(),
        )
        .unwrap();
        image.put_slice(&data, &[0, 0, 0]).unwrap();
        image.make_mask("mask0", true, true).unwrap();
        let mut mask = ArrayD::from_elem(IxDyn(&shape).f(), true);
        mask[[0, 0, 0]] = false;
        mask[[1, 1, 2]] = false;
        image.put_mask("mask0", &mask).unwrap();
        image.save().unwrap();

        export_fits(&source_path, &fits_path, true, false).unwrap();

        let pixels = read_fits_pixels(&fits_path);
        assert_eq!(pixels.iter().filter(|value| value.is_nan()).count(), 2);
        assert_eq!(read_fits_key_as_string(&fits_path, "CTYPE3"), "VRAD");
        assert_eq!(read_fits_key_as_string(&fits_path, "CUNIT3"), "m/s");
        let crval3 = read_fits_key_as_f64(&fits_path, "CRVAL3");
        let cdelt3 = read_fits_key_as_f64(&fits_path, "CDELT3");
        assert!(crval3.abs() < 1.0e-9);
        let expected_increment = -SPEED_OF_LIGHT_M_S * frequency_increment / rest_frequency;
        assert!((cdelt3 - expected_increment).abs() < 1.0e-6);
    }

    #[test]
    fn optimized_moment_collapse_matches_reference_for_masked_axis() {
        let shape = vec![3, 2, 4];
        let input = ArrayD::from_shape_vec(
            IxDyn(&shape).f(),
            (0..shape.iter().product::<usize>())
                .map(|value| value as f32 / 10.0)
                .collect(),
        )
        .unwrap();
        let mask = ArrayD::from_shape_vec(
            IxDyn(&shape).f(),
            (0..shape.iter().product::<usize>())
                .map(|value| value % 3 != 0)
                .collect(),
        )
        .unwrap();
        let coords = vec![-2.0, -1.0, 0.0, 1.0];
        for moment in [-1, 0, 1, 2, 3] {
            let actual = immoments::collapse_moment(
                &input,
                Some(&mask),
                2,
                &immoments::ProfileCoordinates::fixed(&coords),
                0.25,
                immoments::PixelClip::from_include(Some([0.4, 1.8])),
                moment,
            );
            let expected = collapse_moment_reference(
                &input,
                Some(&mask),
                2,
                &coords,
                0.25,
                Some([0.4, 1.8]),
                moment,
            )
            .unwrap();
            assert_eq!(actual, expected);
        }
    }

    fn create_spectral_test_image(path: &Path) {
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.2, -0.4],
            [-2.0e-6, 3.0e-6],
            [0.0, 0.0],
        ));
        coordinates.add_coordinate(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            372.0e9,
            2.0e6,
            0.0,
            372.0e9,
        ));
        let shape = vec![2, 2, 3];
        let mut image = PagedImage::<f32>::create(shape.clone(), coordinates, path).unwrap();
        let data = ArrayD::from_shape_vec(
            IxDyn(&shape).f(),
            (1..=shape.iter().product::<usize>())
                .map(|value| value as f32)
                .collect(),
        )
        .unwrap();
        image.put_slice(&data, &[0, 0, 0]).unwrap();
        image.set_units("Jy/beam").unwrap();
        image
            .set_image_info(&ImageInfo {
                beam_set: ImageBeamSet::new(GaussianBeam::new(
                    1.7e-4_f64.to_radians(),
                    1.2e-4_f64.to_radians(),
                    (-34.5_f64).to_radians(),
                )),
                image_type: ImageType::Intensity,
                object_name: "NGC Test".to_string(),
            })
            .unwrap();
        image.save().unwrap();
    }

    fn create_spectral_mask_image(path: &Path) {
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.2, -0.4],
            [-2.0e-6, 3.0e-6],
            [0.0, 0.0],
        ));
        coordinates.add_coordinate(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            372.0e9,
            2.0e6,
            0.0,
            372.0e9,
        ));
        let shape = vec![2, 2, 3];
        let mut image = PagedImage::<f32>::create(shape.clone(), coordinates, path).unwrap();
        let mut data = ArrayD::<f32>::ones(IxDyn(&shape).f());
        for chan in 0..shape[2] {
            data[[0, 0, chan]] = 0.0;
        }
        image.put_slice(&data, &[0, 0, 0]).unwrap();
        image.save().unwrap();
    }

    fn create_double_spectral_test_image(path: &Path) {
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [0.0, 0.0],
            [-1.0e-6, 1.0e-6],
            [0.0, 0.0],
        ));
        coordinates.add_coordinate(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            115.0e9,
            1.0e6,
            0.0,
            115.0e9,
        ));
        let shape = vec![2, 2, 2];
        let mut image = PagedImage::<f64>::create(shape.clone(), coordinates, path).unwrap();
        let data = ArrayD::from_shape_vec(
            IxDyn(&shape).f(),
            (0..shape.iter().product::<usize>())
                .map(|value| value as f64 + 0.125)
                .collect(),
        )
        .unwrap();
        image.put_slice(&data, &[0, 0, 0]).unwrap();
        image.set_units("K").unwrap();
        image
            .set_image_info(&ImageInfo {
                beam_set: ImageBeamSet::default(),
                image_type: ImageType::Intensity,
                object_name: "Double Test".to_string(),
            })
            .unwrap();
        image.save().unwrap();
    }

    fn create_direction_only_test_image(path: &Path) {
        let shape = vec![2, 2];
        let mut image =
            PagedImage::<f32>::create(shape.clone(), direction_coordinates(), path).unwrap();
        let data = ArrayD::from_shape_vec(IxDyn(&shape).f(), vec![1.0, 2.0, 3.0, 4.0]).unwrap();
        image.put_slice(&data, &[0, 0]).unwrap();
        image.save().unwrap();
    }

    fn create_direction_grid_test_image(path: &Path, shape: &[usize]) {
        let mut image =
            PagedImage::<f32>::create(shape.to_vec(), direction_coordinates(), path).unwrap();
        let data = ArrayD::from_shape_fn(IxDyn(shape).f(), |idx| (idx[0] + idx[1] * 10 + 1) as f32);
        image.put_slice(&data, &[0, 0]).unwrap();
        image.save().unwrap();
    }

    fn create_complex_test_image(path: &Path) {
        let shape = vec![2, 2];
        let mut image =
            PagedImage::<Complex32>::create(shape.clone(), direction_coordinates(), path).unwrap();
        let data = ArrayD::from_shape_vec(
            IxDyn(&shape).f(),
            vec![
                Complex32::new(1.0, 0.5),
                Complex32::new(2.0, -0.5),
                Complex32::new(3.0, 1.5),
                Complex32::new(4.0, -1.5),
            ],
        )
        .unwrap();
        image.put_slice(&data, &[0, 0]).unwrap();
        image.set_units("Jy").unwrap();
        image.save().unwrap();
    }

    fn direction_coordinates() -> CoordinateSystem {
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.2, -0.4],
            [-2.0e-6, 3.0e-6],
            [0.0, 0.0],
        ));
        coordinates
    }

    fn read_fits_pixels(path: &Path) -> Vec<f32> {
        let mut fits = FitsFile::open(path).unwrap();
        let hdu = fits.primary_hdu().unwrap();
        hdu.read_image::<Vec<f32>>(&mut fits).unwrap()
    }

    fn assert_fits_key_matches(lhs: &Path, rhs: &Path, key: &str) {
        let lhs_value = read_fits_key_as_string(lhs, key);
        let rhs_value = read_fits_key_as_string(rhs, key);
        assert_eq!(lhs_value, rhs_value, "FITS key {key}");
    }

    fn read_fits_key_as_string(path: &Path, key: &str) -> String {
        let mut fits = FitsFile::open(path).unwrap();
        let hdu = fits.primary_hdu().unwrap();
        if let Ok(value) = hdu.read_key::<String>(&mut fits, key) {
            return value.trim_end().to_string();
        }
        if let Ok(value) = hdu.read_key::<i64>(&mut fits, key) {
            return value.to_string();
        }
        hdu.read_key::<f64>(&mut fits, key).unwrap().to_string()
    }

    fn read_fits_key_as_f64(path: &Path, key: &str) -> f64 {
        let mut fits = FitsFile::open(path).unwrap();
        let hdu = fits.primary_hdu().unwrap();
        hdu.read_key::<f64>(&mut fits, key).unwrap()
    }
}
