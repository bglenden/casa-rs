// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA-compatible `imsubimage` task implementation across all four pixel
//! types, FITS/CASA inputs, full spatial/spectral/Stokes/mask selection, and
//! degenerate axis dropping (`dropdeg`/`keepaxes`).

mod bounding_box;
mod collapse;
mod continuum;
mod convolve;
mod convolve2d;
pub use collapse::{CollapseFunction, CollapsePixel, ImageCollapse, ImageCollapseOptions};
pub use continuum::{ContinuumSummary, ImageContinuum};
pub use convolve::{ConvolutionKernel, ConvolutionOptions, ConvolutionPixel, ImageConvolution};
pub use convolve2d::{GaussianConvolutionOptions, ImageGaussianConvolution};
pub(super) mod coords;
mod crop;
mod decimate;
mod decompose;
mod deviation;
mod findsources;
mod getprofile;
pub use getprofile::{
    ImageReadProfile, ProfileFrequencyFrame, ReadProfileOptions, read_image_profile,
};
mod fitcomponents;
pub(in crate::analysis) mod fitprofile;
mod histogram;
pub use fitcomponents::{
    ComponentFitPixel, ComponentFitQuantity, FitComponentsOptions, ImageComponentFit,
    fit_image_components,
};
pub use fitprofile::{
    ChannelLineFit, ChannelLineShape, ImageProfileFit, ImageProfileFitResult, ProfileDivisor,
    ProfileEstimates, ProfileFitOptions, ProfileLineReport, ProfileOutputNames, ProfilePeaks,
    ProfileRegion, ProfileReport, ProfileSigma, fit_channel_lines,
};
pub use histogram::{HistogramOptions, HistogramPixel, image_histogram};
mod fourier;
pub use decompose::{DecompositionOptions, ImageDecomposition, decompose_image};
pub use deviation::{DeviationOptions, ImageDeviation};
pub use findsources::{
    FindSourcesOptions, ImageMaxFit, MaxFitOptions, find_image_sources, fit_image_maximum,
};
mod insert;
mod noise;
mod pad;
mod pbcor;
mod pixel_edit;
mod read_section;
pub(in crate::analysis) mod read_window;
mod rebin;
pub(super) mod region;
mod region_write;
pub use region_write::{put_image_region, validate_put_region_shapes};
mod replace_masked;
mod sepconvolve;
mod smoothing;
mod two_point;

pub use replace_masked::{
    MaskReplacementOptions, MaskReplacementOutcome, replace_masked_complex_pixels,
    replace_masked_real_pixels, validate_mask_replacement_region,
};

pub use bounding_box::ImageRegionBounds;
pub use crop::{ImageCrop, ImageCropOptions};
pub use decimate::{DecimationMethod, DecimationPixel, ImageDecimation, ImageDecimationOptions};
pub use fourier::{FourierOptions, FourierPixel, ImageFourier};
pub use insert::ImageInsertion;
pub use noise::ImageNoise;
pub use pad::{ImagePad, ImagePadOptions};
pub use pbcor::{ImagePbcor, ImagePbcorBeam, ImagePbcorOptions};
pub use pixel_edit::ImagePixelSelection;
pub use read_section::ImageRegionSection;
pub use rebin::{ImageRebin, ImageRebinOptions};
pub use sepconvolve::{ImageSepConvolution, SepConvolutionOptions};
pub use smoothing::{ImageSmoothing, SmoothingKernel, SmoothingOptions, SmoothingPixel};
pub use two_point::{ImageTwoPoint, StructurePixel, TwoPointOptions};

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use casa_core::{ArrayD, RecordValue, Value};
use casa_lattices::{TraversalIter, TraversalSpec};
use casa_tables::publication::{exchange_directories, rename_without_replacing};
use ndarray::{Axis, IxDyn, Slice, SliceInfoElem, Zip};
use num_complex::Complex;

use crate::{ImageSubimageSummary, ImsubimageRequest};
use casa_images::{
    AnyPagedImage, ImageError, ImageInfo, ImageInterface, ImagePixel, MutableImageInterface,
    PagedImage, TempImage, TempStoragePolicy, read_fits_image,
};
use casa_images::{ImageHistoryEntry, append_image_history_entries, image_history_entries};

pub use self::coords::reduce_subimage_coordinate_record;
use self::coords::{CoordReductionSpec, reduce_coordinates_and_beams};
use self::region::SubimageSelection;
use super::axes::StridedSection;
use super::axes::stokes_for_axis;
use super::lel_mask::{evaluate_mask, evaluate_strided_mask};

/// A region as CASA text, a region filename, or a casacore region record.
#[derive(Clone, Copy)]
pub enum SubimageRegion<'a> {
    Text(&'a str),
    Record(&'a RecordValue),
}

impl<'a> SubimageRegion<'a> {
    /// File or image holding this region when it is stored outside the fitted image.
    pub fn external_path(self) -> Option<&'a Path> {
        match self {
            Self::Text(text) => self::region::external_region_path(text),
            Self::Record(_) => None,
        }
    }
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
    pub(in crate::analysis) selection: SubimageSelection,
    kept_axes: Vec<usize>,
    /// Pixel shape in output axis order.
    pub shape: Vec<usize>,
    /// Coordinates shifted to the lower bound of the output.
    pub coordinates: casa_coordinates::CoordinateSystem,
    /// Native coordinate representation when it differs from the typed model.
    pub coordinate_record: Option<RecordValue>,
    pub(in crate::analysis) image_info: ImageInfo,
    stored_image_info: Option<RecordValue>,
}

mod beam_records;

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
        Self::new_with_beams(image, native_coordinates, options, None)
    }

    /// Plans an image section and retains the saved values and units of its beams.
    ///
    /// # Errors
    /// The region, axes, or saved beam metadata are invalid.
    pub fn new_preserving_beams<T: ImagePixel>(
        image: &dyn ImageInterface<T>,
        native_coordinates: Option<RecordValue>,
        options: SubimageOptions<'_>,
        source_beams: &RecordValue,
    ) -> Result<Self, ImageError> {
        let mut plan =
            Self::new_with_beams(image, native_coordinates, options, Some(source_beams))?;
        plan.preserve_beams(image.coordinates(), source_beams)?;
        Ok(plan)
    }

    pub(in crate::analysis) fn new_with_beams<T: ImagePixel>(
        image: &dyn ImageInterface<T>,
        native_coordinates: Option<RecordValue>,
        options: SubimageOptions<'_>,
        beam_record: Option<&RecordValue>,
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
        let selection =
            SubimageSelection::resolve_section(image, None, options.region, None, None)?;
        Self::from_selection(
            image,
            native_coordinates,
            selection,
            options.dropdeg,
            options.keepaxes,
            beam_record,
        )
    }

    /// Resolve a writable selection, including a positive-stride pixel slice.
    /// Returns the packed pixel geometry and its stride in the parent image.
    ///
    /// # Errors
    ///
    /// A region, coordinate system or stride the image cannot represent.
    pub fn for_edit<T: ImagePixel>(
        image: &dyn ImageInterface<T>,
        region: Option<SubimageRegion<'_>>,
    ) -> Result<(Self, Vec<usize>), ImageError> {
        let plan = Self::new(
            image,
            None,
            SubimageOptions {
                region,
                dropdeg: false,
                keepaxes: &[],
            },
        )?;
        let stride = plan
            .selection
            .stride
            .clone()
            .unwrap_or_else(|| vec![1; image.ndim()]);
        Ok((plan, stride))
    }

    pub(in crate::analysis) fn from_selection<T: ImagePixel>(
        image: &dyn ImageInterface<T>,
        native_coordinates: Option<RecordValue>,
        selection: SubimageSelection,
        dropdeg: bool,
        keepaxes: &[i64],
        beam_record: Option<&RecordValue>,
    ) -> Result<Self, ImageError> {
        let packed_shape = selection.packed_shape();
        let kept_axes = resolve_kept_axes(&packed_shape, dropdeg, keepaxes)?;
        let step = |axis| selection.stride.as_ref().map_or(1, |stride| stride[axis]);
        let cropped_stokes = if selection.selected_stokes.is_none()
            && let Some(axis) = image.find_axis(casa_coordinates::CoordinateType::Stokes)
            && let stokes = stokes_for_axis(image.coordinates(), axis)?
            && (packed_shape[axis] != image.shape()[axis] || stokes.len() > image.shape()[axis])
        {
            let start = selection.start[axis];
            Some(
                stokes
                    .get(start..start + selection.shape[axis])
                    .ok_or_else(|| {
                        ImageError::CasaSelection("Stokes region exceeds coordinate bounds".into())
                    })?
                    .iter()
                    .step_by(step(axis))
                    .copied()
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        };
        let mut source_info = image.image_info()?;
        if let Some(record) = beam_record {
            source_info.beam_set = ImageInfo::from_record(record)?.beam_set;
        }
        let (coordinates, coordinate_record, image_info) = reduce_coordinates_and_beams(
            image.coordinates(),
            native_coordinates,
            CoordReductionSpec {
                full_shape: image.shape(),
                start: &selection.start,
                shape: &packed_shape,
                stride: selection.stride.as_deref(),
                kept_axes: &kept_axes,
                selected_stokes: selection
                    .selected_stokes
                    .as_deref()
                    .or(cropped_stokes.as_deref()),
            },
            source_info,
        )?;
        let shape = kept_axes.iter().map(|&axis| packed_shape[axis]).collect();
        Ok(Self {
            selection,
            kept_axes,
            shape,
            coordinates,
            coordinate_record,
            image_info,
            stored_image_info: None,
        })
    }

    fn preserve_beams(
        &mut self,
        coordinates: &casa_coordinates::CoordinateSystem,
        source: &RecordValue,
    ) -> Result<(), ImageError> {
        self.stored_image_info = beam_records::select_image_info(
            coordinates,
            &self.selection,
            &self.image_info,
            source,
        )?;
        Ok(())
    }

    fn keep_first_beam(&mut self) {
        let first = *self.image_info.beam_set.beam(0, 0);
        self.image_info.beam_set.set_all(first);
        if let Some(info) = &mut self.stored_image_info
            && let Some(Value::Record(grid)) = info.remove("perplanebeams")
            && !first.is_null()
            && let Some(beam) = grid.get("*0")
        {
            info.upsert("restoringbeam", beam.clone());
        }
    }

    /// The lower and upper pixel bounds in the source image, inclusive.
    pub fn bounds(&self) -> (&[usize], Vec<usize>) {
        let end = self
            .selection
            .start
            .iter()
            .zip(self.selection.packed_shape())
            .enumerate()
            .map(|(axis, (start, count))| {
                let step = self
                    .selection
                    .stride
                    .as_ref()
                    .map_or(1, |stride| stride[axis]);
                start + (count - 1) * step
            })
            .collect();
        (&self.selection.start, end)
    }

    /// Image metadata with the restoring beams cropped to this section.
    pub fn image_info(&self) -> &ImageInfo {
        &self.image_info
    }

    fn transformed_coordinate_record(
        &self,
        coordinates: &casa_coordinates::CoordinateSystem,
        axes: &[usize],
    ) -> Result<RecordValue, ImageError> {
        let mut output = coordinates.to_record();
        if let Some(native) = &self.coordinate_record {
            let originals = self.coordinates.coordinates_in_record(native)?;
            let transformed = coordinates.coordinates_in_record(&output)?;
            for (source, destination) in originals.iter().zip(transformed) {
                if !source
                    .pixel_axes
                    .iter()
                    .flatten()
                    .any(|axis| axes.contains(axis))
                    && let Some(value) = native.get(&source.field)
                {
                    output.upsert(destination.field, value.clone());
                }
            }
        }
        Ok(output)
    }

    /// Whether the resolved selection consists of multiple regions or
    /// nonconsecutive spectral or polarization intervals.
    pub fn has_multiple_regions(&self) -> bool {
        self.selection.has_multiple_regions
    }

    pub(in crate::analysis) fn require_single_region(&self) -> Result<(), ImageError> {
        if self.has_multiple_regions() {
            return Err(ImageError::CasaSelection(
                "Only a single n-dimensional rectangular region is supported".into(),
            ));
        }
        Ok(())
    }

    /// Check an optional mask expression against the parent image. This also
    /// checks stretching along the selected axes. Operations that edit the
    /// underlying pixel values can validate a mask without materializing it.
    ///
    /// # Errors
    ///
    /// An invalid expression or a mask that does not conform to the image.
    pub fn validate_mask<T: ImagePixel>(
        &self,
        image: &dyn ImageInterface<T>,
        mask: Option<&str>,
        stretch: bool,
    ) -> Result<(), ImageError> {
        super::lel_mask::validate_mask(mask, image.shape(), image.coordinates(), stretch)
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
        self.copy_metadata(image, output)?;
        let selection = &self.selection;
        if let Some(stride) = selection.stride.as_deref() {
            return self.copy_strided_to(image, output, mask_expression, stretch, stride);
        }
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
        self.write_selection_mask(output, mask)?;
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

    fn copy_strided_to<T: ImagePixel + Copy>(
        &self,
        image: &dyn ImageInterface<T>,
        output: &mut dyn MutableImageInterface<T>,
        mask_expression: Option<&str>,
        stretch: bool,
        stride: &[usize],
    ) -> Result<(), ImageError> {
        let packed_shape = self.selection.packed_shape();
        let section = StridedSection {
            start: &self.selection.start,
            shape: &packed_shape,
            stride,
        };
        let mut mask = evaluate_strided_mask(
            mask_expression,
            image.shape(),
            image.coordinates(),
            stretch,
            &section,
        )?;
        for cursor in section.cursors(image.nice_cursor_shape()) {
            let cursor = cursor?;
            let source = section.source_start(&cursor.position);
            if let Some(inherited) = image.get_mask_slice(&source, &cursor.shape, stride)? {
                let target =
                    mask.get_or_insert_with(|| ArrayD::from_elem(IxDyn(&packed_shape), true));
                let mut tile = target.slice_each_axis_mut(|axis| {
                    let index = axis.axis.index();
                    let start = cursor.position[index];
                    Slice::from(start..start + cursor.shape[index])
                });
                Zip::from(&mut tile)
                    .and(&inherited)
                    .for_each(|out, &valid| *out &= valid);
            }
            let values = image.get_slice(&source, &cursor.shape, stride)?;
            let destination = self
                .kept_axes
                .iter()
                .map(|&axis| cursor.position[axis])
                .collect::<Vec<_>>();
            output.put_slice(&self.squeeze(values), &destination)?;
        }
        self.write_selection_mask(output, mask)
    }

    /// Copy the selected real and imaginary sections into a complex image.
    /// Coordinates and metadata come from the real image used to plan the section.
    pub fn make_complex_to<T>(
        &self,
        real: &dyn ImageInterface<T>,
        imaginary: &dyn ImageInterface<T>,
        output: &mut dyn MutableImageInterface<Complex<T>>,
        region: Option<SubimageRegion<'_>>,
    ) -> Result<(), ImageError>
    where
        T: ImagePixel + Copy,
        Complex<T>: ImagePixel + Copy,
    {
        let selection = &self.selection;
        if selection.stride.is_some() {
            return Err(ImageError::InvalidMetadata(
                "makecomplex requires a unit-stride image section".into(),
            ));
        }
        let imaginary_selection = SubimageSelection::resolve(imaginary, None, region, None, None)?;
        if selection.shape != imaginary_selection.shape {
            return Err(ImageError::CasaSelection(
                "Real and imaginary parts have different shapes".into(),
            ));
        }
        self.copy_metadata(real, output)?;
        let stride = vec![1; real.ndim()];
        let mut real_membership = None;
        let mut imaginary_membership = None;
        selection.apply_selection_masks(&mut real_membership);
        imaginary_selection.apply_selection_masks(&mut imaginary_membership);
        let check_mask = real.has_pixel_mask()
            || imaginary.has_pixel_mask()
            || real_membership.is_some()
            || imaginary_membership.is_some();
        let mut has_invalid = false;

        let spec = TraversalSpec::tiles().section(selection.start.clone(), selection.shape.clone());
        for chunk in TraversalIter::new(real, spec) {
            let chunk = chunk?;
            let relative: Vec<_> = chunk
                .cursor
                .position
                .iter()
                .zip(&selection.start)
                .map(|(&position, &start)| position - start)
                .collect();
            let imaginary_origin: Vec<_> = relative
                .iter()
                .zip(&imaginary_selection.start)
                .map(|(&offset, &start)| offset + start)
                .collect();
            let imag = imaginary.get_slice(&imaginary_origin, &chunk.cursor.shape, &stride)?;
            let data = Zip::from(&chunk.data)
                .and(&imag)
                .map_collect(|&real, &imag| Complex::new(real, imag));
            let position: Vec<_> = self.kept_axes.iter().map(|&axis| relative[axis]).collect();
            if check_mask {
                let mut mask =
                    real.get_mask_slice(&chunk.cursor.position, &chunk.cursor.shape, &stride)?;
                if let Some(imaginary_mask) =
                    imaginary.get_mask_slice(&imaginary_origin, &chunk.cursor.shape, &stride)?
                {
                    and_opt_mask(&mut mask, imaginary_mask);
                }
                for membership in [&real_membership, &imaginary_membership]
                    .into_iter()
                    .flatten()
                {
                    let slices: Vec<_> = relative
                        .iter()
                        .zip(&chunk.cursor.shape)
                        .map(|(&start, &size)| SliceInfoElem::Slice {
                            start: start as isize,
                            end: Some((start + size) as isize),
                            step: 1,
                        })
                        .collect();
                    and_opt_mask(&mut mask, membership.slice(slices.as_slice()).to_owned());
                }
                let mask =
                    mask.unwrap_or_else(|| ArrayD::from_elem(IxDyn(&chunk.cursor.shape), true));
                has_invalid |= mask.iter().any(|&valid| !valid);
                output.put_mask_slice("mask0", &self.squeeze(mask), &position)?;
            }
            output.put_slice(&self.squeeze(data), &position)?;
        }
        if has_invalid {
            output.set_default_mask("mask0")?;
        } else if check_mask {
            output.remove_mask("mask0")?;
        }
        Ok(())
    }

    /// Indices of the original beam planes retained on a selected source axis.
    pub fn beam_planes(&self, axis: Option<usize>, planes: usize) -> Vec<usize> {
        let Some(axis) = axis.filter(|_| planes > 1) else {
            return vec![0];
        };
        let selected = &self.selection;
        let start = selected.start[axis];
        let step = selected.stride.as_ref().map_or(1, |stride| stride[axis]);
        (0..selected.shape[axis].div_ceil(step))
            .map(|index| start + index * step)
            .collect()
    }

    fn copy_metadata<T: ImagePixel, U: ImagePixel>(
        &self,
        source: &dyn ImageInterface<T>,
        output: &mut dyn MutableImageInterface<U>,
    ) -> Result<(), ImageError> {
        if output.shape() != self.shape {
            return Err(ImageError::ShapeMismatch {
                expected: self.shape.clone(),
                got: output.shape().to_vec(),
            });
        }
        output.set_units_string(source.units().to_owned())?;
        if let Some(info) = &self.stored_image_info {
            output.set_image_info_record(info.clone())?;
        } else {
            output.set_image_info(&self.image_info)?;
        }
        output.set_misc_info(source.misc_info())
    }

    fn write_selection_mask<T: ImagePixel>(
        &self,
        output: &mut dyn MutableImageInterface<T>,
        mask: Option<ArrayD<bool>>,
    ) -> Result<(), ImageError> {
        if let Some(mask) = mask
            && mask.iter().any(|valid| !valid)
        {
            output.put_mask("mask0", &self.squeeze(mask))?;
            output.set_default_mask("mask0")?;
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

/// Reject overlapping table, image or text paths before an operation creates
/// any of them. Existing symlinks are resolved along each path.
///
/// # Errors
/// Paths overlap, or their existing components cannot be inspected.
pub fn ensure_artifacts_do_not_overlap(first: &Path, second: &Path) -> Result<(), ImageError> {
    let a = resolve_artifact_path(first)?;
    let b = resolve_artifact_path(second)?;
    if a.starts_with(&b) || b.starts_with(&a) {
        return Err(ImageError::CasaSelection(format!(
            "Output path '{}' overlaps '{}'",
            first.display(),
            second.display()
        )));
    }
    Ok(())
}

fn resolve_artifact_path(path: &Path) -> Result<std::path::PathBuf, ImageError> {
    use std::path::Component;

    let inspect_error = |error| {
        ImageError::CasaSelection(format!(
            "Could not resolve path '{}': {error}",
            path.display()
        ))
    };
    let absolute = std::path::absolute(path).map_err(inspect_error)?;
    let mut resolved = std::path::PathBuf::new();
    for part in absolute.components() {
        if part == Component::ParentDir {
            resolved.pop();
        } else {
            resolved.push(part);
        }
        if matches!(part, Component::Normal(_)) {
            match resolved.canonicalize() {
                Ok(canonical) => resolved = canonical,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(inspect_error(error)),
            }
        }
    }
    Ok(resolved)
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
    let selection = SubimageSelection::resolve_section(
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
        None,
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
    let time = ImageHistoryEntry::current_time()?;
    let mut entries = input_history.to_vec();
    entries.push(ImageHistoryEntry {
        time_mjd_seconds: time,
        message: format!("version: casa-rs {}", env!("CARGO_PKG_VERSION")),
        priority: "INFO".into(),
        location: "::imsubimage".to_string(),
    });
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
