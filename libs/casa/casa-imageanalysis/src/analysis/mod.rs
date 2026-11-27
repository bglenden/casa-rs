// SPDX-License-Identifier: LGPL-3.0-or-later
//! Tutorial-scoped image-analysis operations matching CASA image task semantics.

use std::fs;
use std::path::Path;

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, FitsHeader, StokesType,
};
use casa_core::{ArrayD, RecordValue, ScalarValue, Value};
use casa_lattices::{
    LatticeStatistics, RobustStats, SamplePoint, Statistic, StatisticsAlgorithm,
    StatisticsAlgorithmKind, StatsElement, TiledShape, compute_robust_stats, parse_algorithm,
    parse_center, parse_clmethod,
};
use casa_tables::{Table, TableOptions};
use fitsio::images::ImageType as FitsImageType;
use ndarray::{Dimension, IxDyn, ShapeBuilder, Zip};

mod axes;
mod channel_selection;
pub use axes::{replace_stokes_axis_in_record, section_quality_labels, write_quality_labels};
pub use channel_selection::parse_image_channel_selection;
pub(in crate::analysis) use channel_selection::{ChannelSpan, parse_image_channel_spans};
mod custom_values;
pub(crate) mod exportfits;
mod expression_workspace;
mod extrema;
pub use expression_workspace::with_image_expression_workspace;
mod feather;
pub use feather::feather;
mod imhead_fits;
mod imhead_masks;
mod immath;
mod importfits;
pub(crate) mod imregrid;
pub(crate) mod imsubimage;
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

pub(crate) mod immoments;
pub use immoments::{
    ImageMomentOptions, ImageMomentProducts, make_image_moments, moment_output_names,
};
mod impbcor;
pub(crate) mod impv;
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
pub use casa_images::expr_parser::ImageToolPixelExpression;
mod lel_pixels;
pub use self::imhead_value::ImheadValue;
pub use self::lel_pixels::{
    calculate_complex_pixels, calculate_real_pixels, materialize_image_tool_expression,
};
pub use self::list_log::imhead_list_log;
pub use self::schema::*;
pub use self::summary_log::imhead_summary_log;
use casa_images::ImageExprValue;
use casa_images::expr_parser::{
    ExprValueConvert, HashMapResolver, mask_error_wording, parse_mask_expr,
};
use casa_images::fits_read::{PrimaryHdu, image_info_from_fits_header};
use casa_images::{
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

mod edit_coordinates;
mod edit_metadata;
mod header_summary;
mod moment_coordinates;
mod regions;
mod selection_masks;
mod statistics;

pub use edit_coordinates::imhead_add_rest_frequency;
pub use edit_coordinates::imhead_axis_index;
pub use edit_coordinates::imhead_change_beam;
pub use edit_coordinates::imhead_put;
pub use edit_coordinates::imhead_put_axis_quantity;
pub use edit_coordinates::imhead_put_direction_reference;
pub use edit_coordinates::imhead_put_observation_date;
pub use edit_coordinates::imhead_put_projection;
pub use edit_coordinates::imhead_put_reference_pixel;
pub use edit_coordinates::imhead_put_rest_frequency;
pub use edit_coordinates::imhead_put_spectral_reference;
use edit_coordinates::*;
pub use edit_metadata::imhead_add_delete;
pub use edit_metadata::imhead_custom_value;
pub use edit_metadata::imhead_custom_value_change;
pub use edit_metadata::imhead_is_custom_key;
pub use edit_metadata::imhead_put_axis_name;
pub use edit_metadata::imhead_put_axis_unit;
pub use edit_metadata::validate_imhead_image;
pub use edit_metadata::validate_imhead_writable;
use edit_metadata::*;
use header_summary::*;
pub(super) use moment_coordinates::patch_saved_coords_keyword;
use moment_coordinates::*;
pub(super) use regions::PixelRegion;
pub(super) use regions::Selection;
pub(super) use regions::parse_box_region;
pub(super) use regions::parse_stokes_selection;
pub(super) use regions::region_pixel_mask;
pub(super) use regions::region_pixels;
use regions::*;
pub use selection_masks::quoted_expression_names;
use selection_masks::*;
pub use statistics::statistics_flux_scale;
use statistics::*;

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
        image.set_units(casa_images::fits_read::brightness_unit(&header))?;
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
        units: casa_images::fits_read::brightness_unit(&header),
        object_name: image_info.0.object_name,
        has_restoring_beam: image_info
            .0
            .beam_set
            .single_beam()
            .is_some_and(|beam| !beam.is_null()),
    })
}

#[cfg(test)]
mod tests;
