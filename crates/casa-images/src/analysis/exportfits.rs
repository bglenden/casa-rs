// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA `exportfits` implementation: primary-HDU FITS export with velocity/optical
//! spectral coordinates, 16-bit/32-bit pixel scaling, axis filtering/reordering,
//! CASA `LOGTABLE` history cards, and per-plane `BEAMS` binary table extensions.

use std::collections::{BTreeMap, HashMap};
use std::ffi::CString;
use std::fs;
use std::path::Path;

use casa_coordinates::{
    CoordinateModel, CoordinateSystem, CoordinateType, FitsHeader, FitsValue,
    fits::try_to_fits_header,
};
use casa_types::{ArrayD, RecordValue, quanta::time::MvTime};
use fitsio::{
    FitsFile,
    hdu::FitsHdu,
    images::{ImageDescription, ImageType as FitsImageType, WriteImage},
    tables::{ColumnDataType, ColumnDescription},
};
use ndarray::IxDyn;

use super::{FitsExportSummary, SPEED_OF_LIGHT_M_S, import_fits_for_reading};
use crate::image::{ImageHistoryEntry, image_history_entries};
use crate::{
    AnyPagedImage, ImageError, ImageInfo, ImageInterface, ImagePixel, PagedImage,
    StoredGaussianBeam,
};

mod publication;
mod tool_export;
mod tool_header;

use publication::StagedFitsFile;
use tool_export::Wavelength;

pub use tool_export::{
    ImageFitsExportOptions, export_attached_fits, validate_attached_fits_target,
};

pub use tool_header::{ImageFitsHeader, image_fits_header, image_fits_header_with_beams};

/// Minimum blank sentinel value in scaled 16-bit FITS images.
const SHORT_BLANK: i16 = i16::MIN;

/// Pixel types supported by `exportfits`.
pub(super) trait ExportFitsPixel: ImagePixel + WriteImage + Copy {
    const FITS_FLOAT_TYPE: FitsImageType;
    fn to_f64(self) -> f64;
    fn is_finite_pixel(self) -> bool;
    fn fits_blank() -> Self;
}

impl ExportFitsPixel for f32 {
    const FITS_FLOAT_TYPE: FitsImageType = FitsImageType::Float;

    fn to_f64(self) -> f64 {
        f64::from(self)
    }

    fn is_finite_pixel(self) -> bool {
        self.is_finite()
    }

    fn fits_blank() -> Self {
        Self::NAN
    }
}

impl ExportFitsPixel for f64 {
    const FITS_FLOAT_TYPE: FitsImageType = FitsImageType::Double;

    fn to_f64(self) -> f64 {
        self
    }

    fn is_finite_pixel(self) -> bool {
        self.is_finite()
    }

    fn fits_blank() -> Self {
        Self::NAN
    }
}

fn validate_output_target(request: &super::ExportFitsRequest) -> Result<(), ImageError> {
    if request.fitsimage.as_os_str().is_empty()
        || request
            .fitsimage
            .to_str()
            .is_some_and(|s| s.trim().is_empty())
    {
        return Err(ImageError::InvalidMetadata(
            "fitsfile must be specified".to_string(),
        ));
    }
    if request.bitpix != -32 && request.bitpix != 16 {
        return Err(ImageError::InvalidMetadata(format!(
            "unsupported bitpix {}: must be -32 or 16",
            request.bitpix
        )));
    }
    if !request.minpix.is_finite() || !request.maxpix.is_finite() {
        return Err(ImageError::InvalidMetadata(
            "minpix and maxpix must be finite numbers".to_string(),
        ));
    }
    if let Ok(meta) = fs::symlink_metadata(&request.fitsimage) {
        if meta.is_dir() || (meta.file_type().is_symlink() && request.fitsimage.is_dir()) {
            return Err(ImageError::Io(format!(
                "FITS output path is an existing directory: {}",
                request.fitsimage.display()
            )));
        }
        if let (Ok(src), Ok(dst)) = (
            fs::canonicalize(&request.imagename),
            fs::canonicalize(&request.fitsimage),
        ) && src == dst
        {
            return Err(ImageError::Io(format!(
                "refusing to overwrite input image in place: {}",
                request.fitsimage.display()
            )));
        }
        if !request.overwrite {
            return Err(ImageError::Io(format!(
                "FITS output already exists: {}",
                request.fitsimage.display()
            )));
        }
    }
    Ok(())
}

/// Entry point for CASA `exportfits`.
pub(super) fn run(request: &super::ExportFitsRequest) -> Result<FitsExportSummary, ImageError> {
    validate_output_target(request)?;

    let imported = import_fits_for_reading(&request.imagename)?;
    let open_path = imported.as_ref().map_or_else(
        || request.imagename.as_path(),
        |(_, converted)| converted.as_path(),
    );
    let image = AnyPagedImage::open(open_path)?;
    match &image {
        AnyPagedImage::Float32(image) => export_paged(request, image),
        AnyPagedImage::Float64(image) => export_paged(request, image),
        AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => Err(
            ImageError::InvalidMetadata("exportfits supports only real-valued images".to_string()),
        ),
    }
}

fn export_paged<T: ExportFitsPixel>(
    request: &super::ExportFitsRequest,
    image: &PagedImage<T>,
) -> Result<FitsExportSummary, ImageError> {
    let history_entries = if request.history {
        image_history_entries(&request.imagename)?
    } else {
        Vec::new()
    };
    let stored_bpa = if image
        .image_info()?
        .beam_set
        .single_beam()
        .is_some_and(|beam| !beam.is_null())
    {
        image
            .stored_restoring_beam()?
            .as_ref()
            .map(stored_position_angle_degrees)
            .transpose()?
    } else {
        None
    };
    export_fits_typed(
        request,
        image,
        &history_entries,
        stored_bpa,
        FitsWriteOptions::default(),
    )
}

fn stored_position_angle_degrees(stored: &StoredGaussianBeam) -> Result<f64, ImageError> {
    let wrapped = stored.wrapped_position_angle()?;
    let degrees = casa_types::quanta::Unit::new("deg").expect("built-in degrees");
    casa_types::quanta::casacore::get_conformant_value(&wrapped, &degrees)
        .map_err(|error| ImageError::InvalidMetadata(error.to_string()))
}

#[derive(Default)]
struct FitsWriteOptions<'a> {
    deglast: bool,
    wavelength: Option<Wavelength>,
    tool_metadata: bool,
    beam_record: Option<&'a RecordValue>,
}

fn export_fits_typed<T: ExportFitsPixel>(
    request: &super::ExportFitsRequest,
    image: &dyn ImageInterface<T>,
    history_entries: &[ImageHistoryEntry],
    stored_bpa: Option<f64>,
    options: FitsWriteOptions<'_>,
) -> Result<FitsExportSummary, ImageError> {
    validate_output_target(request)?;
    let plan = ExportAxisPlan::build(
        image.coordinates(),
        image.shape(),
        request.dropdeg,
        request.dropstokes,
        request.stokeslast,
        options.deglast,
        options.tool_metadata,
    )?;
    let base_header = try_to_fits_header(image.coordinates(), image.shape())?;
    let mut header_items = build_export_keywords(
        image.coordinates(),
        &base_header,
        &plan,
        request.velocity,
        request.optical,
        options.wavelength,
    )?;
    let mut info = image.image_info()?;
    if let Some(record) = options.beam_record {
        info.beam_set = ImageInfo::from_record(record)?.beam_set;
    }
    let extra_cards = if options.tool_metadata {
        let coordinates = image.coordinates();
        let output_axis = |axis| plan.old_to_new.get(axis - 1).copied().flatten();
        header_items.extend(tool_header::projection_parameters(coordinates, output_axis));
        for index in 0..coordinates.n_coordinates() {
            let coordinate = coordinates.coordinate(index);
            if let CoordinateModel::Stokes(stokes) = coordinate
                && let [Some(world)] = coordinates
                    .coordinate_world_axes(index)
                    .expect("existing coordinate")
                    .as_slice()
                && let [Some(pixel)] = coordinates
                    .coordinate_pixel_axes(index)
                    .expect("existing coordinate")
                    .as_slice()
                && let Some(axis) = output_axis(world + 1)
                && let Some(pixel) = output_axis(pixel + 1)
            {
                for (key, value) in tool_header::stokes_axis_keywords(stokes, axis, pixel)? {
                    if let Some((_, existing)) =
                        header_items.iter_mut().find(|(name, _)| *name == key)
                    {
                        *existing = value;
                    } else {
                        header_items.push((key, value));
                    }
                }
            }
        }
        tool_header::image_export_cards(image, &info, &header_items)?
    } else {
        Vec::new()
    };
    let bpa_deg = if let Some(beam) = info.beam_set.single_beam().filter(|beam| !beam.is_null()) {
        Some(match stored_bpa {
            Some(angle) => angle,
            None if options.tool_metadata => {
                tool_header::wrapped_beam_position_angle(beam.position_angle_in("deg")?)
            }
            None => beam.position_angle_in("deg")?,
        })
    } else {
        None
    };

    let data = image.get()?;
    let mask = image.default_mask()?;
    let encoded = prepare_export_pixels(
        data,
        mask,
        &plan.kept_orig_axes,
        image.shape(),
        request.bitpix,
        request.minpix,
        request.maxpix,
    )?;

    let fits_dimensions: Vec<usize> = plan.output_shape.iter().rev().copied().collect();
    let staged = StagedFitsFile::new_sibling(&request.fitsimage)?;
    write_staged_fits(
        staged.path(),
        &fits_dimensions,
        &encoded,
        &header_items,
        &extra_cards,
        image.units(),
        &info,
        bpa_deg,
        options.tool_metadata,
        request.history.then_some(history_entries),
    )?;

    validate_output_target(request)?;
    staged.persist_to(&request.fitsimage, request.overwrite)?;

    Ok(FitsExportSummary {
        imagename: request.imagename.display().to_string(),
        fitsimage: request.fitsimage.display().to_string(),
        shape: plan.output_shape,
        velocity: request.velocity,
    })
}

#[allow(clippy::too_many_arguments)]
fn write_staged_fits<T: ExportFitsPixel>(
    staging_path: &Path,
    fits_dimensions: &[usize],
    encoded: &PreparedPixels<T>,
    header_items: &[(String, FitsValue)],
    extra_cards: &[String],
    units: &str,
    info: &crate::ImageInfo,
    bpa_deg: Option<f64>,
    wrap_per_plane_pa: bool,
    history_entries: Option<&[crate::image::ImageHistoryEntry]>,
) -> Result<(), ImageError> {
    let description = ImageDescription {
        data_type: match encoded {
            PreparedPixels::Short(_) => FitsImageType::Short,
            PreparedPixels::FloatArray(_) | PreparedPixels::FloatVec(_) => T::FITS_FLOAT_TYPE,
        },
        dimensions: fits_dimensions,
    };
    let mut fits = FitsFile::create(staging_path)
        .with_custom_primary(&description)
        .open()
        .map_err(|error| ImageError::Io(error.to_string()))?;
    let hdu = fits
        .primary_hdu()
        .map_err(|error| ImageError::Io(error.to_string()))?;

    // Write all primary-HDU header keywords and HISTORY cards before image data so CFITSIO does
    // not shift the pixel payload on disk when header blocks grow beyond 2880 bytes.
    match encoded {
        PreparedPixels::Short(short) => {
            hdu.write_key(&mut fits, "BSCALE", short.bscale)
                .map_err(|error| ImageError::Io(error.to_string()))?;
            hdu.write_key(&mut fits, "BZERO", short.bzero)
                .map_err(|error| ImageError::Io(error.to_string()))?;
            if let Some(blank) = short.blank {
                hdu.write_key(&mut fits, "BLANK", blank)
                    .map_err(|error| ImageError::Io(error.to_string()))?;
            }
            if let (Some(dmin), Some(dmax)) = (short.datamin, short.datamax) {
                hdu.write_key(&mut fits, "DATAMIN", dmin)
                    .map_err(|error| ImageError::Io(error.to_string()))?;
                hdu.write_key(&mut fits, "DATAMAX", dmax)
                    .map_err(|error| ImageError::Io(error.to_string()))?;
            }
        }
        PreparedPixels::FloatArray(_) | PreparedPixels::FloatVec(_) => {
            hdu.write_key(&mut fits, "BSCALE", 1.0_f64)
                .map_err(|error| ImageError::Io(error.to_string()))?;
            hdu.write_key(&mut fits, "BZERO", 0.0_f64)
                .map_err(|error| ImageError::Io(error.to_string()))?;
        }
    }

    for (name, value) in header_items {
        write_fits_key(&hdu, &mut fits, name, value)?;
    }
    if !units.is_empty() {
        hdu.write_key(&mut fits, "BUNIT", units)
            .map_err(|error| ImageError::Io(error.to_string()))?;
    }
    if !info.object_name.is_empty() {
        hdu.write_key(&mut fits, "OBJECT", info.object_name.as_str())
            .map_err(|error| ImageError::Io(error.to_string()))?;
    }
    if let (Some(beam), Some(bpa)) = (info.beam_set.single_beam(), bpa_deg) {
        hdu.write_key(&mut fits, "BMAJ", beam.major_in("deg")?)
            .map_err(|error| ImageError::Io(error.to_string()))?;
        hdu.write_key(&mut fits, "BMIN", beam.minor_in("deg")?)
            .map_err(|error| ImageError::Io(error.to_string()))?;
        hdu.write_key(&mut fits, "BPA", bpa)
            .map_err(|error| ImageError::Io(error.to_string()))?;
    } else if info.beam_set.is_multi() {
        write_fits_logical(
            &mut fits,
            "CASAMBM",
            true,
            "CASA multiple BEAMS table present",
        )?;
    }

    for card in extra_cards {
        let card = CString::new(card.as_str())
            .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?;
        let mut status = 0;
        // SAFETY: the primary HDU is writable and the supplied card is NUL-terminated.
        unsafe { fitsio::sys::ffprec(fits.as_raw(), card.as_ptr(), &mut status) };
        fitsio::errors::check_status(status).map_err(|error| ImageError::Io(error.to_string()))?;
    }

    if let Some(entries) = history_entries
        && !entries.is_empty()
    {
        write_history_cards(&mut fits, entries)?;
    }

    match encoded {
        PreparedPixels::Short(short) => {
            // Keep CFITSIO's write scaling at 1.0/0.0 so raw i16 codes are written verbatim
            // while preserving the header BSCALE/BZERO keywords above.
            disable_cfitsio_write_scaling(&mut fits)?;
            hdu.write_image(&mut fits, &short.pixels)
                .map_err(|error| ImageError::Io(error.to_string()))?;
        }
        PreparedPixels::FloatArray(arr) => {
            let slice = arr
                .as_slice_memory_order()
                .expect("verified contiguous Fortran slice");
            hdu.write_image(&mut fits, slice)
                .map_err(|error| ImageError::Io(error.to_string()))?;
        }
        PreparedPixels::FloatVec(flat) => {
            hdu.write_image(&mut fits, flat)
                .map_err(|error| ImageError::Io(error.to_string()))?;
        }
    }

    if info.beam_set.is_multi() {
        write_beams_extension(&mut fits, &info.beam_set, wrap_per_plane_pa)?;
    }
    let mut status = 0;
    // SAFETY: Flush the writable handle and report any buffered I/O error before publication.
    unsafe { fitsio::sys::ffflus(fits.as_raw(), &mut status) };
    fitsio::errors::check_status(status)
        .map_err(|error| ImageError::Io(format!("finalizing staged FITS output: {error}")))
}

struct ExportAxisPlan {
    /// Original 0-based axes in output order (fastest-varying FITS axis first).
    kept_orig_axes: Vec<usize>,
    /// Output shape corresponding to `kept_orig_axes`.
    output_shape: Vec<usize>,
    /// Map from 1-based original FITS axis to 1-based exported FITS axis, or `None` if dropped.
    old_to_new: Vec<Option<usize>>,
    /// Coordinate type of each kept output axis.
    kept_types: Vec<CoordinateType>,
    /// 1-based output spectral axis index, if any spectral axis survived.
    output_spectral_axis: Option<usize>,
}

impl ExportAxisPlan {
    fn build(
        coordinates: &CoordinateSystem,
        image_shape: &[usize],
        dropdeg: bool,
        dropstokes: bool,
        stokeslast: bool,
        deglast: bool,
        tool_order: bool,
    ) -> Result<Self, ImageError> {
        let ndim = image_shape.len();
        let orig_types: Vec<_> = (0..coordinates.n_pixel_axes())
            .map(|axis| coordinates.pixel_axis_type(axis).expect("dense pixel axes"))
            .collect();
        if orig_types.len() != ndim {
            return Err(ImageError::InvalidMetadata(format!(
                "coordinate system has {} axes for image shape {image_shape:?}",
                orig_types.len()
            )));
        }

        let mut kept: Vec<usize> = (0..ndim)
            .filter(|&axis| {
                if dropdeg && image_shape[axis] == 1 {
                    return false;
                }
                if dropstokes
                    && orig_types[axis] == CoordinateType::Stokes
                    && image_shape[axis] == 1
                    && (!tool_order || coordinates.n_coordinates() > 1)
                {
                    return false;
                }
                true
            })
            .collect();

        if kept.is_empty() {
            return Err(ImageError::InvalidMetadata(
                "cannot drop all axes; all axes are degenerate".to_string(),
            ));
        }

        let non_stokes_end = if stokeslast && tool_order {
            if let Some(index) = kept
                .iter()
                .position(|&axis| orig_types[axis] == CoordinateType::Stokes)
            {
                let last = kept.len() - 1;
                kept.swap(index, last);
                last
            } else {
                kept.len()
            }
        } else {
            if stokeslast {
                kept.sort_by_key(|&axis| orig_types[axis] == CoordinateType::Stokes);
            }
            kept.len()
        };
        if deglast {
            kept[..non_stokes_end].sort_by_key(|&axis| image_shape[axis] == 1);
        }

        let mut old_to_new = vec![None; ndim];
        let mut output_shape = Vec::with_capacity(kept.len());
        let mut kept_types = Vec::with_capacity(kept.len());

        for (new_idx, &orig_axis) in kept.iter().enumerate() {
            let new_1 = new_idx + 1;
            old_to_new[orig_axis] = Some(new_1);
            output_shape.push(image_shape[orig_axis]);
            let ct = orig_types[orig_axis];
            kept_types.push(ct);
        }

        if kept.iter().any(|&pixel| {
            coordinates
                .pixel_axis_to_world_axis(pixel)
                .is_some_and(|world| old_to_new.get(world).is_none_or(Option::is_none))
        }) {
            return Err(ImageError::InvalidMetadata(
                "FITS axes cannot be dropped across distinct pixel and world placements".into(),
            ));
        }
        let output_spectral_axis = coordinates
            .find_coordinate(CoordinateType::Spectral)
            .and_then(|coordinate| {
                coordinates
                    .coordinate_world_axes(coordinate)
                    .expect("existing coordinate")
                    .into_iter()
                    .flatten()
                    .next()
            })
            .and_then(|axis| old_to_new.get(axis).copied().flatten());
        Ok(Self {
            kept_orig_axes: kept,
            output_shape,
            old_to_new,
            kept_types,
            output_spectral_axis,
        })
    }
}

enum PreparedPixels<T> {
    FloatArray(ArrayD<T>),
    FloatVec(Vec<T>),
    Short(Int16Pixels),
}

struct Int16Pixels {
    pixels: Vec<i16>,
    bscale: f64,
    bzero: f64,
    blank: Option<i64>,
    datamin: Option<f64>,
    datamax: Option<f64>,
}

fn is_fortran_contiguous(strides: &[isize], shape: &[usize]) -> bool {
    let mut expected = 1isize;
    for (&stride, &dim) in strides.iter().zip(shape.iter()) {
        if dim > 1 && stride != expected {
            return false;
        }
        expected = expected.saturating_mul(dim as isize);
    }
    true
}

fn mask_pixels_in_place<T: ExportFitsPixel>(pixels: &mut [T], valid_mask: &[bool]) {
    for (pixel, &valid) in pixels.iter_mut().zip(valid_mask.iter()) {
        if !valid {
            *pixel = T::fits_blank();
        }
    }
}

fn prepare_export_pixels<T: ExportFitsPixel>(
    mut data: ArrayD<T>,
    mask: Option<ArrayD<bool>>,
    kept_orig_axes: &[usize],
    orig_shape: &[usize],
    bitpix: i32,
    minpix: f64,
    maxpix: f64,
) -> Result<PreparedPixels<T>, ImageError> {
    if data.shape() != orig_shape {
        return Err(ImageError::InvalidMetadata(format!(
            "pixel array shape {:?} does not match image shape {orig_shape:?}",
            data.shape()
        )));
    }
    let monotonic = kept_orig_axes.windows(2).all(|w| w[0] < w[1]);
    let data_fortran = is_fortran_contiguous(data.strides(), orig_shape);

    if bitpix == 16 {
        if monotonic
            && data_fortran
            && let Some(data_slice) = data.as_slice_memory_order()
        {
            return Ok(PreparedPixels::Short(match mask {
                Some(ref m) => {
                    if is_fortran_contiguous(m.strides(), orig_shape)
                        && let Some(mslice) = m.as_slice_memory_order()
                    {
                        encode_int16(data_slice, Some(mslice), minpix, maxpix)
                    } else {
                        let mvec = collect_reordered_fortran(m, kept_orig_axes, orig_shape)?;
                        encode_int16(data_slice, Some(&mvec), minpix, maxpix)
                    }
                }
                None => encode_int16(data_slice, None, minpix, maxpix),
            }));
        }
        let reordered_data = collect_reordered_fortran(&data, kept_orig_axes, orig_shape)?;
        let reordered_mask = match mask {
            Some(ref m) => Some(collect_reordered_fortran(m, kept_orig_axes, orig_shape)?),
            None => None,
        };
        return Ok(PreparedPixels::Short(encode_int16(
            &reordered_data,
            reordered_mask.as_deref(),
            minpix,
            maxpix,
        )));
    }

    if monotonic && data_fortran && data.as_slice_memory_order().is_some() {
        if let Some(ref m) = mask {
            if is_fortran_contiguous(m.strides(), orig_shape)
                && let Some(mslice) = m.as_slice_memory_order()
            {
                let dslice = data
                    .as_slice_memory_order_mut()
                    .expect("verified contiguous Fortran slice");
                mask_pixels_in_place(dslice, mslice);
            } else {
                let mvec = collect_reordered_fortran(m, kept_orig_axes, orig_shape)?;
                let dslice = data
                    .as_slice_memory_order_mut()
                    .expect("verified contiguous Fortran slice");
                mask_pixels_in_place(dslice, &mvec);
            }
        }
        return Ok(PreparedPixels::FloatArray(data));
    }

    let mut flat = collect_reordered_fortran(&data, kept_orig_axes, orig_shape)?;
    if let Some(ref m) = mask {
        let mvec = collect_reordered_fortran(m, kept_orig_axes, orig_shape)?;
        mask_pixels_in_place(&mut flat, &mvec);
    }
    Ok(PreparedPixels::FloatVec(flat))
}

fn collect_reordered_fortran<A: Copy>(
    arr: &ArrayD<A>,
    kept_orig_axes: &[usize],
    orig_shape: &[usize],
) -> Result<Vec<A>, ImageError> {
    let out_shape: Vec<usize> = kept_orig_axes.iter().map(|&ax| orig_shape[ax]).collect();
    let total: usize = out_shape.iter().product();
    let mut out = Vec::with_capacity(total);
    if total == 0 {
        return Ok(out);
    }

    if is_fortran_contiguous(arr.strides(), orig_shape)
        && let Some(src) = arr.as_slice_memory_order()
    {
        let mut orig_strides = vec![1usize; orig_shape.len()];
        for ax in 1..orig_shape.len() {
            orig_strides[ax] = orig_strides[ax - 1] * orig_shape[ax - 1];
        }

        // Coalesce leading axes that remain contiguous in source Fortran memory (for instance
        // RA+Dec planes when only Spectral/Stokes axes are reordered).
        let mut contig_len = 1usize;
        let mut contig_axes = 0usize;
        for &ax in kept_orig_axes {
            let dim = orig_shape[ax];
            if dim == 1 || orig_strides[ax] == contig_len {
                contig_len *= dim;
                contig_axes += 1;
            } else {
                break;
            }
        }

        if contig_axes > 0 {
            let outer_dims = &out_shape[contig_axes..];
            let outer_axes = &kept_orig_axes[contig_axes..];
            let outer_total: usize = outer_dims.iter().product();
            let mut outer_idx = vec![0usize; outer_dims.len()];
            for _ in 0..outer_total {
                let mut base = 0usize;
                for (pos, &ax) in outer_axes.iter().enumerate() {
                    base += outer_idx[pos] * orig_strides[ax];
                }
                out.extend_from_slice(&src[base..base + contig_len]);
                for pos in 0..outer_dims.len() {
                    outer_idx[pos] += 1;
                    if outer_idx[pos] < outer_dims[pos] {
                        break;
                    }
                    outer_idx[pos] = 0;
                }
            }
            return Ok(out);
        }

        let step0 = orig_strides[kept_orig_axes[0]];
        let len0 = out_shape[0];
        let outer_dims = &out_shape[1..];
        let outer_axes = &kept_orig_axes[1..];
        let outer_total: usize = outer_dims.iter().product();
        let mut outer_idx = vec![0usize; outer_dims.len()];
        for _ in 0..outer_total {
            let mut base = 0usize;
            for (pos, &ax) in outer_axes.iter().enumerate() {
                base += outer_idx[pos] * orig_strides[ax];
            }
            for i in 0..len0 {
                out.push(src[base + i * step0]);
            }
            for pos in 0..outer_dims.len() {
                outer_idx[pos] += 1;
                if outer_idx[pos] < outer_dims[pos] {
                    break;
                }
                outer_idx[pos] = 0;
            }
        }
        return Ok(out);
    }

    // ndarray iterates a view in row-major order. Reverse the requested
    // axes to walk the FITS order without reconstructing an index per pixel.
    // Omitted axes are singletons and do not change the iteration order.
    let mut axes: Vec<_> = kept_orig_axes.iter().copied().rev().collect();
    axes.extend((0..orig_shape.len()).filter(|axis| !kept_orig_axes.contains(axis)));
    out.extend(arr.view().permuted_axes(IxDyn(&axes)).iter().copied());
    Ok(out)
}

fn scan_minmax<T: ExportFitsPixel>(
    pixels: &[T],
    valid_mask: Option<&[bool]>,
) -> (f64, f64, bool, bool) {
    let mut min_valid = f64::INFINITY;
    let mut max_valid = f64::NEG_INFINITY;
    let mut has_valid = false;
    let mut has_invalid = false;

    if let Some(mask) = valid_mask {
        for (&pix, &mask_ok) in pixels.iter().zip(mask.iter()) {
            if mask_ok && pix.is_finite_pixel() {
                let v = pix.to_f64();
                has_valid = true;
                min_valid = min_valid.min(v);
                max_valid = max_valid.max(v);
            } else {
                has_invalid = true;
            }
        }
    } else {
        for &pix in pixels {
            if pix.is_finite_pixel() {
                let v = pix.to_f64();
                has_valid = true;
                min_valid = min_valid.min(v);
                max_valid = max_valid.max(v);
            } else {
                has_invalid = true;
            }
        }
    }
    (min_valid, max_valid, has_valid, has_invalid)
}

fn encode_int16<T: ExportFitsPixel>(
    pixels: &[T],
    valid_mask: Option<&[bool]>,
    minpix: f64,
    maxpix: f64,
) -> Int16Pixels {
    let explicit = minpix <= maxpix;
    let (min_valid, max_valid, has_valid, has_invalid) = scan_minmax(pixels, valid_mask);

    let has_blanks = has_invalid || explicit;
    if !has_valid && !explicit {
        // CASA uses the supplied minpix/maxpix defaults (0 / -1) when all pixels are masked.
        let bscale = (maxpix - minpix) / 65_534.0;
        let bzero = 0.5 * (maxpix + minpix);
        return Int16Pixels {
            pixels: vec![SHORT_BLANK; pixels.len()],
            bscale,
            bzero,
            blank: Some(i64::from(SHORT_BLANK)),
            datamin: Some(minpix),
            datamax: Some(maxpix),
        };
    }

    let (dmin, mut dmax) = if explicit {
        (minpix, maxpix)
    } else {
        (min_valid, max_valid)
    };

    if (dmax - dmin).abs() == 0.0 {
        if dmin == 0.0 {
            dmax = 1.0;
        } else {
            let expanded = dmin * 1.01;
            if expanded.is_finite() && (expanded - dmin).abs() > 0.0 {
                dmax = expanded;
            } else {
                dmax = dmin * 0.99;
                if !dmax.is_finite() || (dmax - dmin).abs() == 0.0 {
                    dmax = dmin + 1.0;
                }
            }
        }
    }

    let half_diff = 0.5 * dmax - 0.5 * dmin;
    let half_sum = 0.5 * dmax + 0.5 * dmin;
    let (bscale, bzero, min_code, max_code) = if has_blanks {
        (half_diff / 32_767.0, half_sum, -32_767.0_f64, 32_767.0_f64)
    } else {
        let scale = half_diff / 32_767.5;
        let zero = if (half_sum + 0.5 * scale).is_finite() {
            half_sum + 0.5 * scale
        } else {
            dmin + 32_768.0 * scale
        };
        (scale, zero, -32_768.0_f64, 32_767.0_f64)
    };

    let lo_phys = dmin.min(dmax);
    let hi_phys = dmin.max(dmax);
    let mut encoded = Vec::with_capacity(pixels.len());
    if bscale == 0.0 || !bscale.is_finite() {
        encoded.resize(pixels.len(), 0);
        if let Some(mask) = valid_mask {
            for (slot, (&pix, &mask_ok)) in encoded.iter_mut().zip(pixels.iter().zip(mask.iter())) {
                if !mask_ok || !pix.is_finite_pixel() {
                    *slot = SHORT_BLANK;
                }
            }
        } else {
            for (slot, &pix) in encoded.iter_mut().zip(pixels.iter()) {
                if !pix.is_finite_pixel() {
                    *slot = SHORT_BLANK;
                }
            }
        }
    } else if let Some(mask) = valid_mask {
        for (&pix, &mask_ok) in pixels.iter().zip(mask.iter()) {
            if !mask_ok || !pix.is_finite_pixel() {
                encoded.push(SHORT_BLANK);
                continue;
            }
            encoded.push(scale_int16_value(
                pix.to_f64(),
                lo_phys,
                hi_phys,
                bzero,
                bscale,
                min_code,
                max_code,
            ));
        }
    } else {
        for &pix in pixels {
            if !pix.is_finite_pixel() {
                encoded.push(SHORT_BLANK);
                continue;
            }
            encoded.push(scale_int16_value(
                pix.to_f64(),
                lo_phys,
                hi_phys,
                bzero,
                bscale,
                min_code,
                max_code,
            ));
        }
    }

    Int16Pixels {
        pixels: encoded,
        bscale,
        bzero,
        blank: has_blanks.then_some(i64::from(SHORT_BLANK)),
        datamin: Some(dmin),
        datamax: Some(dmax),
    }
}

#[inline]
fn scale_int16_value(
    value: f64,
    lo_phys: f64,
    hi_phys: f64,
    bzero: f64,
    bscale: f64,
    min_code: f64,
    max_code: f64,
) -> i16 {
    let clamped = value.clamp(lo_phys, hi_phys);
    let diff = clamped - bzero;
    let scaled = if diff.is_finite() {
        diff / bscale
    } else {
        (clamped / bscale) - (bzero / bscale)
    };
    if scaled.is_nan() {
        0
    } else {
        scaled.clamp(min_code, max_code) as i16
    }
}

fn build_export_keywords(
    coordinates: &CoordinateSystem,
    base_header: &FitsHeader,
    plan: &ExportAxisPlan,
    velocity: bool,
    optical: bool,
    wavelength: Option<Wavelength>,
) -> Result<Vec<(String, FitsValue)>, ImageError> {
    let has_spectral = plan.output_spectral_axis.is_some();
    let has_direction = plan.kept_types.contains(&CoordinateType::Direction);
    let spec_info = plan
        .output_spectral_axis
        .map(|spec_axis| {
            spectral_export_overrides(coordinates, spec_axis, velocity, optical, wavelength)
        })
        .transpose()?;
    let (has_restfreq, spectral_overrides) = match spec_info {
        Some((rest_ok, map)) => (rest_ok, map),
        None => (false, BTreeMap::new()),
    };

    let mut out = Vec::new();
    let mut seen_overrides = HashMap::<String, bool>::new();

    for keyword in base_header.iter() {
        let name = &keyword.name;
        if name == "NAXIS"
            || name.strip_prefix("NAXIS").is_some_and(|suffix| {
                !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit())
            })
        {
            continue;
        }
        if matches!(name.as_str(), "SPECSYS" | "RESTFRQ")
            && (!has_spectral || (name == "RESTFRQ" && !has_restfreq))
        {
            continue;
        }
        if matches!(name.as_str(), "RADESYS" | "EQUINOX" | "LONPOLE" | "LATPOLE") && !has_direction
        {
            continue;
        }

        if let Some((prefix, old_axis)) = split_axis_keyword(name) {
            if let Some(new_axis) = old_axis
                .checked_sub(1)
                .and_then(|idx| plan.old_to_new.get(idx).copied().flatten())
            {
                let remapped = format!("{prefix}{new_axis}");
                if let Some(val) = spectral_overrides.get(&remapped) {
                    seen_overrides.insert(remapped.clone(), true);
                    out.push((remapped, val.clone()));
                } else {
                    out.push((remapped, keyword.value.clone()));
                }
            }
            continue;
        }

        if let Some((axis, parameter)) = split_indexed_keyword(name, "PV") {
            if let Some(new_axis) = axis
                .checked_sub(1)
                .and_then(|idx| plan.old_to_new.get(idx).copied().flatten())
            {
                out.push((format!("PV{new_axis}_{parameter}"), keyword.value.clone()));
            }
            continue;
        }

        if let Some((old_i, old_j)) = split_indexed_keyword(name, "PC") {
            if let (Some(new_i), Some(new_j)) = (
                old_i
                    .checked_sub(1)
                    .and_then(|idx| plan.old_to_new.get(idx).copied().flatten()),
                old_j
                    .checked_sub(1)
                    .and_then(|idx| plan.old_to_new.get(idx).copied().flatten()),
            ) {
                out.push((format!("PC{new_i}_{new_j}"), keyword.value.clone()));
            }
            continue;
        }

        out.push((name.clone(), keyword.value.clone()));
    }

    for (key, value) in &spectral_overrides {
        if !seen_overrides.contains_key(key) {
            out.push((key.clone(), value.clone()));
        }
    }

    Ok(out)
}

fn split_axis_keyword(name: &str) -> Option<(&'static str, usize)> {
    for prefix in ["CTYPE", "CRVAL", "CRPIX", "CDELT", "CUNIT"] {
        if let Some(rest) = name.strip_prefix(prefix)
            && !rest.is_empty()
            && rest.bytes().all(|b| b.is_ascii_digit())
            && let Ok(axis) = rest.parse::<usize>()
        {
            return Some((prefix, axis));
        }
    }
    None
}

fn split_indexed_keyword(name: &str, prefix: &str) -> Option<(usize, usize)> {
    let rest = name.strip_prefix(prefix)?;
    let (lhs, rhs) = rest.split_once('_')?;
    if lhs.is_empty()
        || rhs.is_empty()
        || !lhs.bytes().all(|b| b.is_ascii_digit())
        || !rhs.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    Some((lhs.parse().ok()?, rhs.parse().ok()?))
}

fn spectral_export_overrides(
    coordinates: &CoordinateSystem,
    output_axis: usize,
    velocity: bool,
    optical: bool,
    wavelength: Option<Wavelength>,
) -> Result<(bool, BTreeMap<String, FitsValue>), ImageError> {
    for ci in 0..coordinates.n_coordinates() {
        if let CoordinateModel::Spectral(spectral) = coordinates.coordinate(ci) {
            let rest_frequency = spectral.rest_frequency_hz()?;
            if wavelength.is_none() && rest_frequency <= 0.0 {
                return Ok((false, BTreeMap::new()));
            }
            let (crpix_0, crval_hz, cdelt_hz) = spectral.fits_linearized_axis_hz()?;
            if let Some(wavelength) = wavelength {
                if velocity {
                    return Err(ImageError::InvalidMetadata(
                        "Cannot export spectral axis for velocity AND wavelength. You have to choose one.".into(),
                    ));
                }
                return Ok((
                    rest_frequency > 0.0,
                    wavelength.cards(output_axis, crval_hz, cdelt_hz)?,
                ));
            }
            let crpix_1 = crpix_0 + 1.0;
            let v_rad = |freq_hz: f64| SPEED_OF_LIGHT_M_S * (1.0 - freq_hz / rest_frequency);
            let v_opt = |freq_hz: f64| SPEED_OF_LIGHT_M_S * (rest_frequency / freq_hz - 1.0);

            let velref_base = match spectral.world_frequency_ref().as_str() {
                "LSRK" => 1_i64,
                "BARY" => 2,
                "TOPO" => 3,
                "LSRD" => 4,
                "GEO" => 5,
                "REST" => 6,
                "GALACTO" => 7,
                _ => 3,
            };
            let velref = velref_base + if optical { 0 } else { 256 };

            let mut map = BTreeMap::new();
            if velocity {
                if optical {
                    let crval_v = v_opt(crval_hz);
                    let cdelt_v = v_opt(crval_hz + cdelt_hz) - crval_v;
                    map.insert(
                        format!("CTYPE{output_axis}"),
                        FitsValue::String("VOPT".to_string()),
                    );
                    map.insert(format!("CRVAL{output_axis}"), FitsValue::Float(crval_v));
                    map.insert(format!("CDELT{output_axis}"), FitsValue::Float(cdelt_v));
                } else {
                    let crval_v = v_rad(crval_hz);
                    let cdelt_v = -SPEED_OF_LIGHT_M_S * cdelt_hz / rest_frequency;
                    map.insert(
                        format!("CTYPE{output_axis}"),
                        FitsValue::String("VRAD".to_string()),
                    );
                    map.insert(format!("CRVAL{output_axis}"), FitsValue::Float(crval_v));
                    map.insert(format!("CDELT{output_axis}"), FitsValue::Float(cdelt_v));
                }
                map.insert(
                    format!("CUNIT{output_axis}"),
                    FitsValue::String("m/s".to_string()),
                );
                map.insert("ALTRVAL".to_string(), FitsValue::Float(crval_hz));
            } else {
                let altrval = if optical {
                    v_opt(crval_hz)
                } else {
                    v_rad(crval_hz)
                };
                map.insert("ALTRVAL".to_string(), FitsValue::Float(altrval));
            }
            map.insert("ALTRPIX".to_string(), FitsValue::Float(crpix_1));
            map.insert("VELREF".to_string(), FitsValue::Integer(velref));
            return Ok((true, map));
        }
    }
    Ok((false, BTreeMap::new()))
}

fn write_fits_key(
    hdu: &FitsHdu,
    fits: &mut FitsFile,
    name: &str,
    value: &FitsValue,
) -> Result<(), ImageError> {
    match value {
        FitsValue::String(value) => hdu
            .write_key(fits, name, value.as_str())
            .map_err(|error| ImageError::Io(error.to_string())),
        FitsValue::Integer(value) => hdu
            .write_key(fits, name, *value)
            .map_err(|error| ImageError::Io(error.to_string())),
        FitsValue::Float(value) => hdu
            .write_key(fits, name, *value)
            .map_err(|error| ImageError::Io(error.to_string())),
        FitsValue::Logical(value) => write_fits_logical(fits, name, *value, ""),
    }
}

fn write_fits_logical(
    fits: &mut FitsFile,
    name: &str,
    value: bool,
    comment: &str,
) -> Result<(), ImageError> {
    let _ = fits
        .primary_hdu()
        .map_err(|error| ImageError::Io(error.to_string()))?;
    let key = CString::new(name).map_err(|e| ImageError::InvalidMetadata(e.to_string()))?;
    let comm = CString::new(comment).map_err(|e| ImageError::InvalidMetadata(e.to_string()))?;
    let mut status = 0;
    // SAFETY: `fits` is an open read-write `FitsFile` positioned on the primary HDU above.
    unsafe {
        fitsio::sys::ffpkyl(
            fits.as_raw(),
            key.as_ptr(),
            i32::from(value),
            comm.as_ptr(),
            &mut status,
        );
    }
    fitsio::errors::check_status(status).map_err(|error| ImageError::Io(error.to_string()))
}

fn disable_cfitsio_write_scaling(fits: &mut FitsFile) -> Result<(), ImageError> {
    let _ = fits
        .primary_hdu()
        .map_err(|error| ImageError::Io(error.to_string()))?;
    let mut status = 0;
    // SAFETY: `fits` is an open read-write `FitsFile` positioned on the primary HDU above.
    // Finalize the header structure first (`ffrdef`) so CFITSIO does not later re-read `BSCALE`
    // and `BZERO` from the header when `write_image` runs after `ffpscl`.
    unsafe {
        fitsio::sys::ffrdef(fits.as_raw(), &mut status);
        if status == 0 {
            fitsio::sys::ffpscl(fits.as_raw(), 1.0, 0.0, &mut status);
        }
    }
    fitsio::errors::check_status(status).map_err(|error| ImageError::Io(error.to_string()))
}

fn write_history_cards(
    fits: &mut FitsFile,
    entries: &[crate::image::ImageHistoryEntry],
) -> Result<(), ImageError> {
    let _ = fits
        .primary_hdu()
        .map_err(|error| ImageError::Io(error.to_string()))?;
    visit_history_cards(entries, true, |text| write_single_history_card(fits, text))
}

fn visit_history_cards(
    entries: &[crate::image::ImageHistoryEntry],
    provenance_per_line: bool,
    mut emit: impl FnMut(&str) -> Result<(), ImageError>,
) -> Result<(), ImageError> {
    emit("CASA START LOGTABLE")?;
    for entry in entries {
        let timestamp = format_utc_history_timestamp(entry.time_mjd_seconds);
        let priority = if entry.priority.trim().is_empty() {
            "INFO"
        } else {
            entry.priority.trim()
        };
        let srccode = if entry.location.is_empty() {
            "ImageHistory::get".to_string()
        } else if entry.location.starts_with("::") {
            entry.location.clone()
        } else {
            format!("::{}", entry.location)
        };
        let meta = format!("{timestamp} {priority} SRCCODE='{srccode}'");
        let lines: Vec<&str> = if entry.message.is_empty() {
            vec![""]
        } else {
            let split: Vec<&str> = entry.message.lines().collect();
            if split.is_empty() { vec![""] } else { split }
        };
        // The export reader expects a provenance card for each exported
        // line. The tool's metadata view uses one per native log row.
        for (index, line) in lines.into_iter().enumerate() {
            if index == 0 || provenance_per_line {
                for chunk in wrap_fits_history_text(&meta) {
                    emit(&chunk)?;
                }
            }
            for chunk in wrap_fits_history_text(line) {
                emit(&chunk)?;
            }
        }
    }
    emit("CASA END LOGTABLE")
}

fn write_single_history_card(fits: &mut FitsFile, text: &str) -> Result<(), ImageError> {
    let clean: String = text
        .chars()
        .map(|ch| if ch == '\0' { ' ' } else { ch })
        .collect();
    let c_text = CString::new(clean).map_err(|e| ImageError::InvalidMetadata(e.to_string()))?;
    let mut status = 0;
    // SAFETY: `fits` is an open read-write `FitsFile` positioned on the primary HDU.
    unsafe {
        fitsio::sys::ffphis(fits.as_raw(), c_text.as_ptr(), &mut status);
    }
    fitsio::errors::check_status(status).map_err(|error| ImageError::Io(error.to_string()))
}

fn wrap_fits_history_text(line: &str) -> Vec<String> {
    let clean: String = line
        .chars()
        .map(|ch| if ch < ' ' || ch == '\x7f' { ' ' } else { ch })
        .collect();
    if clean.len() <= 72 {
        return vec![clean];
    }
    let mut out = Vec::new();
    let mut rest = clean.as_str();
    let mut first = true;
    while !rest.is_empty() {
        let max_bytes = if first { 72 } else { 71 };
        let split_at = if rest.len() <= max_bytes {
            rest.len()
        } else {
            (1..=max_bytes)
                .rfind(|&idx| rest.is_char_boundary(idx))
                .unwrap_or_else(|| rest.chars().next().map_or(0, char::len_utf8))
        };
        let (chunk, tail) = rest.split_at(split_at);
        if first {
            out.push(chunk.to_string());
            first = false;
        } else {
            out.push(format!(">{chunk}"));
        }
        rest = tail;
    }
    out
}

fn format_utc_history_timestamp(mjd_seconds: f64) -> String {
    if !mjd_seconds.is_finite() || mjd_seconds < 0.0 {
        return "0000-00-00T00:00:00".to_string();
    }
    let total_secs = mjd_seconds.round() as i64;
    let mjd_days = total_secs.div_euclid(86_400);
    let sec_of_day = total_secs.rem_euclid(86_400);
    let mv = MvTime::from_mjd_seconds(mjd_days as f64 * 86_400.0 + 43_200.0);
    let hour = sec_of_day / 3_600;
    let minute = (sec_of_day % 3_600) / 60;
    let second = sec_of_day % 60;
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        mv.year(),
        mv.month(),
        mv.month_day(),
        hour,
        minute,
        second
    )
}

fn write_beams_extension(
    fits: &mut FitsFile,
    beam_set: &crate::ImageBeamSet,
    wrap_pa: bool,
) -> Result<(), ImageError> {
    let nchan = beam_set.nchan();
    let npol = beam_set.nstokes();
    let descs = [
        ColumnDescription::new("BMAJ")
            .with_type(ColumnDataType::Float)
            .create()
            .map_err(|e| ImageError::Io(e.to_string()))?,
        ColumnDescription::new("BMIN")
            .with_type(ColumnDataType::Float)
            .create()
            .map_err(|e| ImageError::Io(e.to_string()))?,
        ColumnDescription::new("BPA")
            .with_type(ColumnDataType::Float)
            .create()
            .map_err(|e| ImageError::Io(e.to_string()))?,
        ColumnDescription::new("CHAN")
            .with_type(ColumnDataType::Int)
            .create()
            .map_err(|e| ImageError::Io(e.to_string()))?,
        ColumnDescription::new("POL")
            .with_type(ColumnDataType::Int)
            .create()
            .map_err(|e| ImageError::Io(e.to_string()))?,
    ];
    let hdu = fits
        .create_table("BEAMS", &descs)
        .map_err(|error| ImageError::Io(error.to_string()))?;
    hdu.write_key(fits, "EXTVER", 1_i64)
        .map_err(|error| ImageError::Io(error.to_string()))?;
    hdu.write_key(fits, "NCHAN", nchan as i64)
        .map_err(|error| ImageError::Io(error.to_string()))?;
    hdu.write_key(fits, "NPOL", npol as i64)
        .map_err(|error| ImageError::Io(error.to_string()))?;
    hdu.write_key(fits, "TUNIT1", "arcsec")
        .map_err(|error| ImageError::Io(error.to_string()))?;
    hdu.write_key(fits, "TUNIT2", "arcsec")
        .map_err(|error| ImageError::Io(error.to_string()))?;
    hdu.write_key(fits, "TUNIT3", "deg")
        .map_err(|error| ImageError::Io(error.to_string()))?;

    let rows = nchan * npol;
    let mut bmaj = Vec::with_capacity(rows);
    let mut bmin = Vec::with_capacity(rows);
    let mut bpa = Vec::with_capacity(rows);
    let mut chan = Vec::with_capacity(rows);
    let mut pol = Vec::with_capacity(rows);
    for p in 0..npol {
        for c in 0..nchan {
            let beam = beam_set.beam(c, p);
            bmaj.push(beam.major_in("arcsec")? as f32);
            bmin.push(beam.minor_in("arcsec")? as f32);
            let angle = beam.position_angle_in("deg")?;
            let angle = if wrap_pa {
                tool_header::wrapped_beam_position_angle(angle)
            } else {
                angle
            };
            bpa.push(angle as f32);
            chan.push(c as i32);
            pol.push(p as i32);
        }
    }

    hdu.write_col(fits, "BMAJ", &bmaj)
        .map_err(|error| ImageError::Io(error.to_string()))?;
    hdu.write_col(fits, "BMIN", &bmin)
        .map_err(|error| ImageError::Io(error.to_string()))?;
    hdu.write_col(fits, "BPA", &bpa)
        .map_err(|error| ImageError::Io(error.to_string()))?;
    hdu.write_col(fits, "CHAN", &chan)
        .map_err(|error| ImageError::Io(error.to_string()))?;
    hdu.write_col(fits, "POL", &pol)
        .map_err(|error| ImageError::Io(error.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests;
