// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA-compatible `imsubimage` task implementation across all four pixel
//! types, FITS/CASA inputs, full spatial/spectral/Stokes/mask selection, and
//! degenerate axis dropping (`dropdeg`/`keepaxes`).

pub(super) mod coords;
mod pixel_edit;
pub(super) mod region;

pub use pixel_edit::ImagePixelSelection;

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use casa_lattices::{TraversalIter, TraversalSpec};
use casa_tables::publication::{exchange_directories, rename_without_replacing};
use casa_types::{ArrayD, RecordValue, Value};
use ndarray::{Axis, Zip};

use crate::image::{ImageHistoryEntry, append_image_history_entries, image_history_entries};
use crate::{
    AnyPagedImage, ImageError, ImageInfo, ImageInterface, ImagePixel, ImageSubimageSummary,
    ImsubimageRequest, MutableImageInterface, PagedImage, TempImage, TempStoragePolicy,
    read_fits_image,
};

use self::coords::{CoordReductionSpec, reduce_coordinates_and_beams};
use self::region::SubimageSelection;
use super::axes::stokes_for_axis;
use super::lel_mask::evaluate_mask;

/// A region as CASA text, a region filename, or a casacore region record.
pub enum SubimageRegion<'a> {
    Text(&'a str),
    Record(&'a RecordValue),
}

/// Region and degenerate-axis choices for image section extraction.
pub struct SubimageOptions<'a> {
    /// The full image is selected when no region is given.
    pub region: Option<SubimageRegion<'a>>,
    /// Drop singleton axes from the selection.
    pub dropdeg: bool,
    /// Singleton input axes to keep when dropping axes.
    pub keepaxes: &'a [i64],
}

/// An image selection and the matching coordinates and beam metadata. Pixels
/// remain in the source and are copied in chunks when writing the result.
pub struct SubimagePlan {
    selection: SubimageSelection,
    kept_axes: Vec<usize>,
    /// Pixel shape in output axis order.
    pub shape: Vec<usize>,
    /// Coordinates shifted to the lower bound of the output.
    pub coordinates: casa_coordinates::CoordinateSystem,
    /// Native coordinate representation when it differs from the typed model.
    pub coordinate_record: Option<RecordValue>,
    pub(in crate::analysis) image_info: ImageInfo,
}

impl SubimagePlan {
    /// Resolves the requested image region and the output pixel coordinates.
    ///
    /// Supply the source's saved coordinate record to retain its native layout.
    /// Invalid or unsupported regions and axis lists return an error.
    pub fn new<T: ImagePixel>(
        image: &dyn ImageInterface<T>,
        native_coordinates: Option<RecordValue>,
        options: SubimageOptions<'_>,
    ) -> Result<Self, ImageError> {
        if matches!(&options.region, Some(SubimageRegion::Text(_)))
            && image
                .coordinates()
                .find_coordinate(casa_coordinates::CoordinateType::Direction)
                .is_none()
        {
            return Err(ImageError::CasaSelection(
                "Coordinate system has no direction coordinate".into(),
            ));
        }
        let selection = SubimageSelection::resolve(image, None, options.region, None, None)?;
        Self::from_selection(
            image,
            native_coordinates,
            selection,
            options.dropdeg,
            options.keepaxes,
        )
    }

    pub(in crate::analysis) fn from_selection<T: ImagePixel>(
        image: &dyn ImageInterface<T>,
        native_coordinates: Option<RecordValue>,
        selection: SubimageSelection,
        dropdeg: bool,
        keepaxes: &[i64],
    ) -> Result<Self, ImageError> {
        let kept_axes = resolve_kept_axes(&selection.shape, dropdeg, keepaxes)?;
        let cropped_stokes = if selection.selected_stokes.is_none()
            && let Some(axis) = image.find_axis(casa_coordinates::CoordinateType::Stokes)
            && selection.shape[axis] != image.shape()[axis]
        {
            let start = selection.start[axis];
            let stokes = stokes_for_axis(image.coordinates(), axis)?;
            Some(
                stokes
                    .get(start..start + selection.shape[axis])
                    .ok_or_else(|| {
                        ImageError::CasaSelection("Stokes region exceeds coordinate bounds".into())
                    })?
                    .to_vec(),
            )
        } else {
            None
        };
        let (coordinates, coordinate_record, image_info) = reduce_coordinates_and_beams(
            image.coordinates(),
            native_coordinates,
            CoordReductionSpec {
                full_shape: image.shape(),
                start: &selection.start,
                shape: &selection.shape,
                kept_axes: &kept_axes,
                selected_stokes: selection
                    .selected_stokes
                    .as_deref()
                    .or(cropped_stokes.as_deref()),
            },
            image.image_info()?,
        )?;
        let shape = kept_axes
            .iter()
            .map(|&axis| selection.shape[axis])
            .collect();
        Ok(Self {
            selection,
            kept_axes,
            shape,
            coordinates,
            coordinate_record,
            image_info,
        })
    }

    /// The lower and upper pixel bounds in the source image, inclusive.
    pub fn bounds(&self) -> (&[usize], Vec<usize>) {
        let end = self
            .selection
            .start
            .iter()
            .zip(&self.selection.shape)
            .map(|(start, count)| start + count - 1)
            .collect();
        (&self.selection.start, end)
    }

    /// Copies selected pixels, image metadata and the combined mask into a
    /// new image allocated with the plan's shape and coordinates. The input
    /// must be the source used to create the plan. Output is populated in
    /// place, so file publication belongs to the caller after success.
    pub fn copy_to<T: ImagePixel + Copy>(
        &self,
        image: &dyn ImageInterface<T>,
        output: &mut dyn MutableImageInterface<T>,
        mask_expression: Option<&str>,
        stretch: bool,
    ) -> Result<(), ImageError> {
        if output.shape() != self.shape {
            return Err(ImageError::ShapeMismatch {
                expected: self.shape.clone(),
                got: output.shape().to_vec(),
            });
        }
        output.set_units_string(image.units().to_owned())?;
        output.set_image_info(&self.image_info)?;
        output.set_misc_info(image.misc_info())?;
        let selection = &self.selection;
        let mut mask =
            image.get_mask_slice(&selection.start, &selection.shape, &vec![1; image.ndim()])?;
        selection.apply_selection_masks(&mut mask);
        if let Some(expression) = evaluate_mask(
            mask_expression,
            image.shape(),
            image.coordinates(),
            stretch,
            (&selection.start, &selection.shape),
        )? {
            and_opt_mask(&mut mask, expression);
        }
        if let Some(mask) = mask
            && mask.iter().any(|valid| !valid)
        {
            output.put_mask("mask0", &self.squeeze(mask))?;
            output.set_default_mask("mask0")?;
        }
        let spec = TraversalSpec::tiles().section(selection.start.clone(), selection.shape.clone());
        for chunk in TraversalIter::new(image, spec) {
            let chunk = chunk?;
            let position = self
                .kept_axes
                .iter()
                .map(|&axis| chunk.cursor.position[axis] - selection.start[axis])
                .collect::<Vec<_>>();
            output.put_slice(&self.squeeze(chunk.data), &position)?;
        }
        Ok(())
    }

    fn squeeze<T>(&self, mut values: ArrayD<T>) -> ArrayD<T> {
        for axis in (0..values.ndim()).rev() {
            if !self.kept_axes.contains(&axis) {
                values = values.index_axis_move(Axis(axis), 0);
            }
        }
        values
    }
}

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

/// Refuses to replace the input image, its ancestors, or existing entries inside it.
pub fn ensure_outfile_does_not_alias_input(
    outfile: &Path,
    imagename: &Path,
) -> Result<(), ImageError> {
    let input = fs::canonicalize(imagename).ok();
    let output = fs::canonicalize(outfile).ok();
    if outfile == imagename
        || input
            .as_ref()
            .zip(output.as_ref())
            .is_some_and(|(input, output)| input.starts_with(output))
    {
        return Err(ImageError::InvalidMetadata(format!(
            "outfile '{}' matches or contains input image '{}'",
            outfile.display(),
            imagename.display()
        )));
    }
    if outfile.symlink_metadata().is_ok()
        && input.as_ref().is_some_and(|input| {
            let output_parent = outfile.parent().and_then(|parent| {
                // A bare relative name has an empty parent that refers to this directory.
                fs::canonicalize(if parent.as_os_str().is_empty() {
                    Path::new(".")
                } else {
                    parent
                })
                .ok()
            });
            output
                .iter()
                .chain(output_parent.iter())
                .any(|path| path.starts_with(input))
        })
    {
        return Err(ImageError::InvalidMetadata(format!(
            "outfile '{}' is inside input image '{}'",
            outfile.display(),
            imagename.display()
        )));
    }
    Ok(())
}

fn run_fits_input(request: &ImsubimageRequest) -> Result<ImageSubimageSummary, ImageError> {
    let history = image_history_entries(&request.imagename)?;
    let fits = read_fits_image(&request.imagename)?;
    let mask = fits
        .pixels
        .iter()
        .any(|v| v.is_nan())
        .then(|| fits.pixels.mapv(|v| !v.is_nan()));
    let image = build_fits_temp(
        fits.shape,
        fits.coordinates,
        &fits.units,
        &fits.image_info,
        fits.pixels,
        mask,
    )?;
    run_typed(
        &SourceData::Memory {
            image: Box::new(image),
            history,
        },
        request,
    )
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
    let region = request
        .region
        .as_deref()
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(SubimageRegion::Text);
    let selection = SubimageSelection::resolve(
        img,
        request.box_pixels.as_deref(),
        region,
        request.chans.as_deref(),
        request.stokes.as_deref(),
    )?;
    let native_coordinates =
        source
            .paged()
            .and_then(|image| match image.table().keywords().get("coords") {
                Some(Value::Record(record)) => Some(record.clone()),
                _ => None,
            });
    let plan = SubimagePlan::from_selection(
        img,
        native_coordinates,
        selection,
        request.dropdeg,
        &request.keepaxes,
    )?;

    let destination = std::path::absolute(&request.outfile).map_err(output_io)?;
    let parent = destination.parent().ok_or_else(|| {
        ImageError::InvalidMetadata("outfile must name an image directory".into())
    })?;
    let stage = tempfile::tempdir_in(parent).map_err(output_io)?;
    let product = stage.path().join("image");
    let mut output =
        PagedImage::<T>::create(plan.shape.clone(), plan.coordinates.clone(), &product)?;
    if let Some(record) = &plan.coordinate_record {
        output.set_coords_record_keyword(record.clone());
    }
    plan.copy_to(img, &mut output, request.mask.as_deref(), request.stretch)?;
    output.save()?;
    drop(output);
    write_output_history(&product, source.history(), request)?;

    match rename_without_replacing(&product, &destination) {
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && request.overwrite => {
            exchange_directories(&product, &destination).map_err(output_io)?;
        }
        result => result.map_err(output_io)?,
    }

    let (blc, trc) = plan.bounds();
    Ok(ImageSubimageSummary {
        imagename: request.imagename.display().to_string(),
        outfile: request.outfile.display().to_string(),
        blc: blc.to_vec(),
        trc,
        shape: plan.shape,
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
        priority: "INFO".into(),
        location: "::imsubimage".to_string(),
    });
    append_image_history_entries(outfile, &entries)
}
