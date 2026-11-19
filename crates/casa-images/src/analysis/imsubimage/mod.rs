// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA-compatible `imsubimage` task implementation across all four pixel
//! types, FITS/CASA inputs, full spatial/spectral/Stokes/mask selection, and
//! degenerate axis dropping (`dropdeg`/`keepaxes`).

mod coords;
mod mask;
mod region;

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use casa_tables::publication::{exchange_directories, rename_without_replacing};
use casa_types::ArrayD;
use fitsio::images::ImageType as FitsImageType;
use ndarray::{Axis, IxDyn, ShapeBuilder, Zip};

use crate::fits_read::{
    PrimaryHdu, brightness_unit, history_entries as fits_history_entries,
    image_info_from_fits_header,
};
use crate::image::{ImageHistoryEntry, append_image_history_entries, image_history_entries};
use crate::{
    AnyPagedImage, ImageError, ImageInfo, ImageInterface, ImagePixel, ImageSubimageSummary,
    ImsubimageRequest, PagedImage, TempImage, TempStoragePolicy,
};

use self::coords::{CoordReductionSpec, reduce_coordinates_and_beams};
use self::mask::evaluate_subimage_mask;
use self::region::SubimageSelection;

enum SourceData<T: ImagePixel> {
    Paged {
        image: Box<PagedImage<T>>,
        history: Vec<ImageHistoryEntry>,
    },
    Memory {
        image: Box<TempImage<T>>,
        history: Vec<ImageHistoryEntry>,
    },
}

impl<T: ImagePixel> SourceData<T> {
    fn as_interface(&self) -> &dyn ImageInterface<T> {
        match self {
            Self::Paged { image, .. } => image.as_ref(),
            Self::Memory { image, .. } => image.as_ref(),
        }
    }

    fn paged(&self) -> Option<&PagedImage<T>> {
        match self {
            Self::Paged { image, .. } => Some(image.as_ref()),
            Self::Memory { .. } => None,
        }
    }

    fn history(&self) -> &[ImageHistoryEntry] {
        match self {
            Self::Paged { history, .. } | Self::Memory { history, .. } => history,
        }
    }
}

pub(in crate::analysis) fn run(
    request: &ImsubimageRequest,
) -> Result<ImageSubimageSummary, ImageError> {
    let out_str = request.outfile.to_string_lossy();
    if out_str.trim().is_empty() {
        return Err(ImageError::InvalidMetadata(
            "outfile must be specified.".into(),
        ));
    }
    ensure_outfile_does_not_alias_input(&request.outfile, &request.imagename)?;
    if request.outfile.exists() && !request.overwrite {
        return Err(ImageError::Io(format!(
            "subimage output already exists: {}",
            request.outfile.display()
        )));
    }

    if request.imagename.is_file() {
        return run_fits_input(request);
    }

    let history = image_history_entries(&request.imagename)?;
    match AnyPagedImage::open(&request.imagename)? {
        AnyPagedImage::Float32(img) => {
            let src = SourceData::Paged {
                image: Box::new(img),
                history,
            };
            run_typed(&src, request)
        }
        AnyPagedImage::Float64(img) => {
            let src = SourceData::Paged {
                image: Box::new(img),
                history,
            };
            run_typed(&src, request)
        }
        AnyPagedImage::Complex32(img) => {
            let src = SourceData::Paged {
                image: Box::new(img),
                history,
            };
            run_typed(&src, request)
        }
        AnyPagedImage::Complex64(img) => {
            let src = SourceData::Paged {
                image: Box::new(img),
                history,
            };
            run_typed(&src, request)
        }
    }
}

fn ensure_outfile_does_not_alias_input(outfile: &Path, imagename: &Path) -> Result<(), ImageError> {
    let same_lexical = outfile == imagename;
    let same_canon = fs::canonicalize(outfile)
        .ok()
        .zip(fs::canonicalize(imagename).ok())
        .is_some_and(|(a, b)| a == b);
    if same_lexical || same_canon {
        return Err(ImageError::InvalidMetadata(format!(
            "outfile '{}' matches input image '{}'",
            outfile.display(),
            imagename.display()
        )));
    }
    Ok(())
}

fn run_fits_input(request: &ImsubimageRequest) -> Result<ImageSubimageSummary, ImageError> {
    let history = fits_history_entries(&request.imagename).unwrap_or_default();
    let mut primary = PrimaryHdu::open(&request.imagename)?;
    let mut image_info = image_info_from_fits_header(&primary.header);
    if let Some((beams, _)) = primary.beam_set()? {
        image_info.beam_set = beams;
    }
    let units = brightness_unit(&primary.header);
    let PrimaryHdu {
        mut fits,
        hdu,
        image_type,
        shape,
        coordinates,
        ..
    } = primary;

    match image_type {
        FitsImageType::Double => {
            let raw = hdu
                .read_image::<Vec<f64>>(&mut fits)
                .map_err(|e| ImageError::Io(e.to_string()))?;
            let pixels = ArrayD::from_shape_vec(IxDyn(&shape).f(), raw).map_err(|e| {
                ImageError::InvalidMetadata(format!("invalid FITS image shape: {e}"))
            })?;
            let mask = pixels
                .iter()
                .any(|v| v.is_nan())
                .then(|| pixels.mapv(|v| !v.is_nan()));
            let temp = build_fits_temp(shape, coordinates, &units, &image_info, pixels, mask)?;
            let src = SourceData::Memory {
                image: Box::new(temp),
                history,
            };
            run_typed(&src, request)
        }
        _ => {
            let raw = hdu
                .read_image::<Vec<f32>>(&mut fits)
                .map_err(|e| ImageError::Io(e.to_string()))?;
            let pixels = ArrayD::from_shape_vec(IxDyn(&shape).f(), raw).map_err(|e| {
                ImageError::InvalidMetadata(format!("invalid FITS image shape: {e}"))
            })?;
            let mask = pixels
                .iter()
                .any(|v| v.is_nan())
                .then(|| pixels.mapv(|v| !v.is_nan()));
            let temp = build_fits_temp(shape, coordinates, &units, &image_info, pixels, mask)?;
            let src = SourceData::Memory {
                image: Box::new(temp),
                history,
            };
            run_typed(&src, request)
        }
    }
}

fn build_fits_temp<T: ImagePixel>(
    shape: Vec<usize>,
    coordinates: casa_coordinates::CoordinateSystem,
    units: &str,
    info: &ImageInfo,
    pixels: ArrayD<T>,
    mask: Option<ArrayD<bool>>,
) -> Result<TempImage<T>, ImageError> {
    let ndim = shape.len();
    let mut temp = TempImage::new(shape, coordinates, TempStoragePolicy::Memory)?;
    temp.set_units(units)?;
    temp.set_image_info(info)?;
    temp.put_slice(&pixels, &vec![0; ndim])?;
    if let Some(m) = mask {
        temp.put_mask("mask0", &m)?;
        temp.set_default_mask("mask0")?;
    }
    Ok(temp)
}

fn run_typed<T: ImagePixel + Copy>(
    source: &SourceData<T>,
    request: &ImsubimageRequest,
) -> Result<ImageSubimageSummary, ImageError> {
    let img = source.as_interface();
    let full_shape = img.shape().to_vec();
    let ndim = full_shape.len();

    let selection = SubimageSelection::resolve(
        img,
        request.box_pixels.as_deref(),
        request.region.as_deref(),
        request.chans.as_deref(),
        request.stokes.as_deref(),
    )?;

    let kept_axes = resolve_kept_axes(&selection.shape, request.dropdeg, &request.keepaxes)?;
    let (out_coords, out_legacy_coords, out_info) = reduce_coordinates_and_beams(
        img.coordinates(),
        source.paged(),
        CoordReductionSpec {
            full_shape: &full_shape,
            start: &selection.start,
            shape: &selection.shape,
            kept_axes: &kept_axes,
            selected_stokes: selection.selected_stokes.as_deref(),
        },
        img.image_info()?,
    )?;

    let mut mask = img.get_mask_slice(&selection.start, &selection.shape, &vec![1; ndim])?;
    selection.apply_selection_masks(&mut mask);
    if let Some(expr_mask) = evaluate_subimage_mask(
        request.mask.as_deref(),
        &full_shape,
        request.stretch,
        (&selection.start, &selection.shape),
    )? {
        and_opt_mask(&mut mask, expr_mask);
    }

    let data = img.get_slice(&selection.start, &selection.shape, &vec![1; ndim])?;
    let (data, mask, out_shape) = drop_selected_axes(data, mask, &selection.shape, &kept_axes);

    let destination = std::path::absolute(&request.outfile).map_err(output_io)?;
    let parent = destination.parent().ok_or_else(|| {
        ImageError::InvalidMetadata("outfile must name an image directory".into())
    })?;
    let stage = tempfile::tempdir_in(parent).map_err(output_io)?;
    let product = stage.path().join("image");
    let mut output = PagedImage::<T>::create(out_shape.clone(), out_coords, &product)?;
    output.set_units(img.units())?;
    output.set_image_info(&out_info)?;
    output.set_misc_info(img.misc_info())?;
    if let Some(legacy) = out_legacy_coords {
        output.set_coords_record_keyword(legacy);
    }
    output.put_slice(&data, &vec![0; out_shape.len()])?;
    if let Some(m) = mask
        && m.iter().any(|&v| !v)
    {
        output.put_mask("mask0", &m)?;
        output.set_default_mask("mask0")?;
    }
    output.save()?;
    drop(output);
    write_output_history(&product, source.history(), request)?;

    match rename_without_replacing(&product, &destination) {
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && request.overwrite => {
            exchange_directories(&product, &destination).map_err(output_io)?;
        }
        result => result.map_err(output_io)?,
    }

    Ok(ImageSubimageSummary {
        imagename: request.imagename.display().to_string(),
        outfile: request.outfile.display().to_string(),
        blc: selection.start.clone(),
        trc: selection
            .start
            .iter()
            .zip(&selection.shape)
            .map(|(s, len)| s + len - 1)
            .collect(),
        shape: out_shape,
        units: img.units().to_string(),
    })
}

fn output_io(error: std::io::Error) -> ImageError {
    ImageError::Io(error.to_string())
}

fn and_opt_mask(dst: &mut Option<ArrayD<bool>>, rhs: ArrayD<bool>) {
    *dst = Some(match dst.take() {
        Some(mut lhs) => {
            Zip::from(&mut lhs).and(&rhs).for_each(|a, &b| *a = *a && b);
            lhs
        }
        None => rhs,
    });
}

fn resolve_kept_axes(
    selected_shape: &[usize],
    dropdeg: bool,
    keepaxes: &[i64],
) -> Result<Vec<usize>, ImageError> {
    let ndim = selected_shape.len();
    if !dropdeg {
        return Ok((0..ndim).collect());
    }
    let mut keep_set = BTreeSet::new();
    for &ax in keepaxes {
        if ax < 0 {
            // Matches CASA 6.7.6.14: negative axis entries are ignored.
            continue;
        }
        let u = ax as usize;
        if u >= ndim {
            return Err(ImageError::InvalidMetadata(format!(
                "keepaxes axis {ax} is out of range for {ndim}-D image"
            )));
        }
        keep_set.insert(u);
    }
    let kept: Vec<usize> = (0..ndim)
        .filter(|&ax| selected_shape[ax] > 1 || keep_set.contains(&ax))
        .collect();
    if kept.is_empty() {
        return Err(ImageError::InvalidMetadata(
            "cannot drop all axes of image (result would have 0 dimensions)".into(),
        ));
    }
    Ok(kept)
}

fn drop_selected_axes<T: Copy>(
    mut data: ArrayD<T>,
    mut mask: Option<ArrayD<bool>>,
    selected_shape: &[usize],
    kept_axes: &[usize],
) -> (ArrayD<T>, Option<ArrayD<bool>>, Vec<usize>) {
    if kept_axes.len() == selected_shape.len() {
        return (data, mask, selected_shape.to_vec());
    }
    for ax in (0..selected_shape.len()).rev() {
        if !kept_axes.contains(&ax) {
            debug_assert_eq!(data.shape()[ax], 1);
            data = data.index_axis_move(Axis(ax), 0);
            if let Some(m) = mask.take() {
                mask = Some(m.index_axis_move(Axis(ax), 0));
            }
        }
    }
    let out_shape = kept_axes.iter().map(|&ax| selected_shape[ax]).collect();
    (data, mask, out_shape)
}

fn write_output_history(
    outfile: &Path,
    input_history: &[ImageHistoryEntry],
    request: &ImsubimageRequest,
) -> Result<(), ImageError> {
    const UNIX_EPOCH_MJD_SECONDS: f64 = 40_587.0 * 86_400.0;
    let time = UNIX_EPOCH_MJD_SECONDS
        + std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| ImageError::Io(e.to_string()))?
            .as_secs_f64();

    let mut entries = input_history.to_vec();
    let msg = format!(
        "imsubimage(imagename={:?}, outfile={:?}, box={:?}, region={:?}, chans={:?}, \
         stokes={:?}, mask={:?}, dropdeg={}, overwrite={}, verbose={}, stretch={}, keepaxes={:?})",
        request.imagename.display().to_string(),
        request.outfile.display().to_string(),
        request.box_pixels.as_deref().unwrap_or(""),
        request.region.as_deref().unwrap_or(""),
        request.chans.as_deref().unwrap_or(""),
        request.stokes.as_deref().unwrap_or(""),
        request.mask.as_deref().unwrap_or(""),
        request.dropdeg,
        request.overwrite,
        request.verbose,
        request.stretch,
        request.keepaxes
    );
    entries.push(ImageHistoryEntry {
        time_mjd_seconds: time,
        message: msg,
        location: "::imsubimage".to_string(),
    });
    append_image_history_entries(outfile, &entries)
}
