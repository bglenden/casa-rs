// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ffi::{CStr, c_long};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;

use crate::StoredGaussianBeam;
use casa_coordinates::{
    Coordinate, CoordinateError, CoordinateModel, CoordinateSystem, FitsHeader, FitsValue,
    LinearCoordinate,
    fits::coordinate_util::{
        align_fits_pc_rows, fallback_fits_linear_coordinates, from_fits_header_with_axis_order,
    },
};
use casa_core::measures::{
    EpochRef, MEpoch, MPosition,
    direction::{DirectionRef, MDirection},
    record::{direction_to_record, epoch_to_record, position_to_record},
};
use casa_core::quanta::Quantity;
use casa_core::{ArrayD, ArrayValue, RecordValue, ScalarValue, Value};
use casa_lattices::{Lattice, LatticeError, TraversalCursorIter, TraversalSpec};
use fitsio::{
    FitsFile,
    errors::check_status,
    hdu::{FitsHdu, HduInfo},
    threadsafe_fitsfile::ThreadsafeFitsFile,
};
use ndarray::{Array2, Axis, IxDyn, ShapeBuilder, Slice, Zip};

use super::{
    PrimaryHdu, brightness_unit,
    disk_file::{
        FITS_RECORD_BYTES, attachment_file_length, open_disk_file, unable_to_open_image,
        unreadable_fits,
    },
    history::{history_from_cards, restoring_beam_from_history},
    image_info_from_fits_header, read_fits_axis_shape,
};
use crate::{
    ImageError, ImageInfo, ImageInterface, MutableImageInterface, TempImage, image::ImageHistoryRow,
};

/// A selected FITS image ready to copy into a CASA image.
///
/// The image tools and the `importfits` task share this single-precision
/// reader.
pub struct FitsToolImport {
    source: PrimaryHdu,
    /// Coordinate record as the CASA FITS image tool saves it.
    pub coordinate_record: RecordValue,
    /// Brightness unit as interpreted by casacore.
    pub units: String,
    /// Original image-info record, including units for each beam.
    pub image_info: RecordValue,
    /// FITS keywords that Casa did not consume to decode the image.
    pub misc_info: RecordValue,
    /// Original history entries, in FITS card order.
    pub history: Vec<ImageHistoryRow>,
}

impl FitsToolImport {
    /// Read the metadata from the requested image HDU and coordinate representation.
    /// Negative HDU numbers select the first nonempty image.
    ///
    /// # Errors
    /// Returns an error for an unreadable FITS file, a missing image HDU,
    /// or invalid coordinate or beam metadata.
    pub fn open(path: &Path, which_rep: usize, which_hdu: i64) -> Result<Self, ImageError> {
        Self::open_impl(FitsFile::open(path), path, which_rep, which_hdu, false)
    }

    /// Import from a resolved path, using the original name in FITS diagnostics.
    ///
    /// # Errors
    /// As [`Self::open`].
    pub fn open_named(
        path: &Path,
        name: &Path,
        which_rep: usize,
        which_hdu: i64,
    ) -> Result<Self, ImageError> {
        if !path.is_file() {
            return Err(unreadable_fits(name));
        }
        Self::open_impl(open_disk_file(path), name, which_rep, which_hdu, false)
    }

    /// Open the FITS image used by the direct image attachment and copy tools.
    ///
    /// CASA's direct reader accepts an image in the primary or first extension
    /// and requires the first physical data record. The explicit importer may
    /// select any image HDU. CFITSIO also opens externally compressed images.
    ///
    /// # Errors
    /// Returns an error for an unrecognized input, missing initial record,
    /// absent image HDU, or invalid metadata.
    pub fn open_attachment(path: &Path) -> Result<Self, ImageError> {
        Self::open_attachment_named(path, path, path)
    }

    /// Recognize FITS using the expanded spelling and read it at its lexical
    /// absolute path. Report an unrecognized input with the caller's spelling.
    ///
    /// # Errors
    /// As [`Self::open_attachment`].
    pub fn open_attachment_named(
        path: &Path,
        probe: &Path,
        name: &Path,
    ) -> Result<Self, ImageError> {
        let (mut length, mut tight_value) = attachment_file_length(probe, name)?;
        if probe != path {
            if !path.is_file() {
                return Err(ImageError::CasaSelection(format!(
                    "{} does not exist or is not readable",
                    path.display()
                )));
            }
            (length, tight_value) = attachment_file_length(path, name).map_err(|_| {
                ImageError::CasaSelection(format!(
                    "Cannot open file {} (or other I/O error)",
                    path.display()
                ))
            })?;
        }
        if tight_value {
            return Err(initial_record_error(path));
        }
        let mut fits = open_disk_file(path).map_err(|_| {
            if length.is_some() {
                ImageError::CasaSelection(format!(
                    "FITSImgParser::setup - {} Error opening FITS input.",
                    path.display()
                ))
            } else {
                unable_to_open_image(name)
            }
        })?;
        let count = if length.is_some() {
            2
        } else {
            fits.num_hdus().map_err(fits_error)?
        };
        let hdu = (0..count)
            .find_map(|index| fits.hdu(index).ok().filter(has_image_data))
            .ok_or_else(|| missing_attachment_data(path))?;
        if let Some(length) = length {
            validate_initial_image_record(path, &mut fits, hdu.number, length)?;
        }
        Self::read_metadata(fits, hdu, 0, false)
    }

    /// Read the first FITS image using the linear WCS layout of CASA's image concatenator.
    ///
    /// # Errors
    /// As [`Self::open`].
    pub fn open_for_concatenation(path: &Path) -> Result<Self, ImageError> {
        Self::open_impl(FitsFile::open(path), path, 0, -1, true)
    }

    fn open_impl(
        fits: fitsio::errors::Result<FitsFile>,
        name: &Path,
        which_rep: usize,
        which_hdu: i64,
        group_linear_axes: bool,
    ) -> Result<Self, ImageError> {
        let mut fits = fits.map_err(|_| unreadable_fits(name))?;
        let missing_hdu = || {
            ImageError::CasaSelection(format!(
                "Exception: No image at specified location in file {}.",
                name.display()
            ))
        };
        let count = fits.num_hdus().map_err(fits_error)?;
        let hdu = if which_hdu < 0 {
            (0..count).find_map(|index| fits.hdu(index).ok().filter(has_image_data))
        } else {
            usize::try_from(which_hdu)
                .ok()
                .filter(|&index| index < count)
                .and_then(|index| fits.hdu(index).ok())
                .filter(has_image_data)
        }
        .ok_or_else(missing_hdu)?;
        Self::read_metadata(fits, hdu, which_rep, group_linear_axes)
    }

    fn read_metadata(
        mut fits: FitsFile,
        hdu: FitsHdu,
        which_rep: usize,
        group_linear_axes: bool,
    ) -> Result<Self, ImageError> {
        let HduInfo::ImageInfo {
            ref shape,
            image_type,
        } = hdu.info
        else {
            unreachable!("image HDU selected above");
        };
        let cards = read_hdu_cards(&mut fits)?;
        let physical = FitsHeader::from_cards(
            &cards
                .iter()
                .filter(|card| card.as_bytes().get(8) == Some(&b'='))
                .map(String::as_str)
                .collect::<Vec<_>>(),
        );
        let mut header = select_wcs(&physical, which_rep);
        let shape = if is_tile_compressed(&physical) {
            // The physical NAXIS cards describe the compression table rows.
            let shape: Vec<_> = shape.iter().rev().copied().collect();
            header.set("NAXIS", FitsValue::Integer(shape.len() as i64));
            for (axis, &length) in shape.iter().enumerate() {
                header.set(
                    format!("NAXIS{}", axis + 1),
                    FitsValue::Integer(length as i64),
                );
            }
            shape
        } else {
            read_fits_axis_shape(&mut fits, &hdu)?
        };
        if header.get("RADESYS").is_none() {
            let system = match header.get_string("RADECSYS") {
                Some(system) => system,
                None => match header
                    .get_float("EQUINOX")
                    .or_else(|| header.get_float("EPOCH"))
                {
                    Some(epoch) if epoch < 1984.0 => "FK4",
                    Some(_) => "FK5",
                    None => "ICRS",
                },
            };
            header.set("RADESYS", FitsValue::String(system.into()));
        }
        let mut has_wcs = false;
        for axis in 1..=shape.len() {
            for (prefix, default) in [("CRVAL", 0.0), ("CRPIX", 0.0), ("CDELT", 1.0)] {
                let name = format!("{prefix}{axis}");
                if header.get(&name).is_none() {
                    header.set(name, FitsValue::Float(default));
                } else {
                    has_wcs = true;
                }
            }
            has_wcs |= header.get(&format!("CTYPE{axis}")).is_some();
        }
        let aligned = align_fits_pc_rows(&header, shape.len());
        let (coordinate_header, mut world_order) =
            aligned.as_ref().map_or((&header, None), |(header, order)| {
                (header, Some(order.as_slice()))
            });
        let mut use_fallback = false;
        let (mut coordinates, axis_order) =
            match from_fits_header_with_axis_order(coordinate_header, &shape) {
                Err(CoordinateError::InvalidFitsAxisUnit { .. }) => {
                    use_fallback = true;
                    world_order = None;
                    (
                        fallback_fits_linear_coordinates(&shape),
                        (0..shape.len()).collect(),
                    )
                }
                result => result?,
            };
        if !has_wcs && !use_fallback {
            let ndim = shape.len();
            coordinates = CoordinateSystem::new().with_obs_info(coordinates.obs_info().clone());
            coordinates.add_coordinate(
                LinearCoordinate::new(ndim, vec![String::new(); ndim], vec![String::new(); ndim])
                    .with_reference_pixel(vec![-1.0; ndim]),
            );
        } else if group_linear_axes && !use_fallback {
            coordinates = group_linear_wcs(&coordinates, &axis_order, coordinate_header);
        }
        let obs = coordinates.obs_info_mut();
        if obs.observer.is_empty() {
            obs.observer = "UNKNOWN".into();
        }
        if obs.telescope.is_empty() {
            obs.telescope = "UNKNOWN".into();
        }
        if obs.date.is_none() {
            obs.date = Some(MEpoch::from_mjd(0.0, EpochRef::LAST));
        }
        if !use_fallback
            && let [Some(x), Some(y), Some(z)] =
                ["OBSGEO-X", "OBSGEO-Y", "OBSGEO-Z"].map(|key| header.get_float(key))
        {
            obs.telescope_position = Some(MPosition::new_itrf(x, y, z));
        }
        let coordinate_record =
            fits_coordinate_record(&coordinates, &axis_order, world_order, coordinate_header)?;
        let coordinates = CoordinateSystem::from_record(&coordinate_record).map_err(|error| {
            if matches!(
                error,
                casa_coordinates::CoordinateError::UnsupportedAxisLayout(_)
            ) {
                ImageError::CasaSelection(format!("{error} (casa-rs issue #1002)"))
            } else {
                error.into()
            }
        })?;
        let mut source = PrimaryHdu {
            fits,
            hdu,
            image_type,
            shape,
            header,
            coordinates,
        };
        let history = history_from_cards(&cards, true);
        let mut info = image_info_from_fits_header(&source.header).to_record();
        if let Some((beams, records)) = source.beam_set()? {
            info.remove("restoringbeam");
            if beams.is_single() {
                if let Some(beam) = records.get("*0") {
                    info.upsert("restoringbeam", beam.clone());
                }
            } else {
                let mut grid = beams.to_record();
                for field in records.fields() {
                    grid.upsert(&field.name, field.value.clone());
                }
                info.upsert("perplanebeams", Value::Record(grid));
            }
        } else if let Some(beam) = primary_restoring_beam(&source.header) {
            info.upsert("restoringbeam", Value::Record(beam.to_record()));
        } else if info.get("restoringbeam").is_none()
            && let Some(beam) = restoring_beam_from_history(&history)
        {
            info.upsert("restoringbeam", Value::Record(beam.to_record()));
        }
        Ok(Self {
            coordinate_record,
            units: brightness_unit(&source.header),
            image_info: info,
            misc_info: miscellaneous(&physical),
            history,
            source,
        })
    }

    /// Shape in CASA's axis order, including any singleton axes.
    pub fn shape(&self) -> &[usize] {
        &self.source.shape
    }

    /// The cursor shape used by a directly attached FITS image.
    pub fn tile_shape(&self) -> Vec<usize> {
        super::fits_tile_shape(self.shape())
    }

    /// Whether this HDU defines invalid pixels through NaN or integer BLANK.
    pub fn has_implicit_mask(&self) -> bool {
        self.source
            .header
            .get_int("BITPIX")
            .is_some_and(|bitpix| bitpix < 0)
            || self.source.header.get("BLANK").is_some()
    }

    /// Coordinate system selected from the FITS header.
    pub fn coordinates(&self) -> &CoordinateSystem {
        &self.source.coordinates
    }

    /// Zero-based index of the selected image HDU.
    pub fn hdu_index(&self) -> usize {
        self.source.hdu.number
    }

    /// Whether the selected image is stored as a tile-compressed FITS table.
    pub fn is_tile_compressed(&self) -> bool {
        is_tile_compressed(&self.source.header)
    }

    /// Copy single-precision pixels and the FITS validity mask into an image.
    /// The copy uses bounded contiguous blocks along the FITS axis order.
    ///
    /// # Errors
    /// Returns an error when the image's shape disagrees or reading or
    /// writing fails.
    pub fn copy_pixels(
        &mut self,
        image: &mut impl MutableImageInterface<f32>,
        zero_blanks: bool,
    ) -> Result<(), ImageError> {
        let axes: Vec<_> = (0..self.source.shape.len()).collect();
        self.copy_pixels_with_axes(image, zero_blanks, &axes)
    }

    // Destination axes in source order, with singleton axes appended beyond
    // the FITS rank before applying the same permutation to pixels and masks.
    /// Streams FITS pixels and validity into a destination whose axes are indexed by the given source
    /// permutation. Appended axes have extent one. Invalid permutations, mismatched extents, and read
    /// or write failures return an image error.
    pub fn copy_pixels_with_axes(
        &mut self,
        image: &mut impl MutableImageInterface<f32>,
        zero_blanks: bool,
        axes: &[usize],
    ) -> Result<(), ImageError> {
        let mut shape = self.source.shape.clone();
        let ndim = shape.len();
        if axes.len() < ndim {
            return Err(ImageError::InvalidMetadata(
                "FITS axes were dropped during import".into(),
            ));
        }
        let mut remaining = vec![true; axes.len()];
        for &axis in axes {
            if !remaining.get_mut(axis).is_some_and(std::mem::take) {
                return Err(ImageError::InvalidMetadata(
                    "invalid FITS axis order".into(),
                ));
            }
        }
        shape.resize(axes.len(), 1);
        let expected: Vec<_> = axes.iter().map(|&axis| shape[axis]).collect();
        if image.shape() != expected {
            return Err(ImageError::ShapeMismatch {
                expected,
                got: image.shape().to_vec(),
            });
        }
        let total = shape
            .iter()
            .try_fold(1usize, |product, &length| product.checked_mul(length))
            .ok_or_else(|| ImageError::InvalidMetadata("FITS image shape is too large".into()))?;
        let quota = 1_048_576;
        let mut stride = 1;
        let mut axis = 0;
        while axis + 1 < ndim && stride * shape[axis] <= quota {
            stride *= shape[axis];
            axis += 1;
        }
        let mut block_shape = vec![1; axes.len()];
        block_shape[..axis].copy_from_slice(&shape[..axis]);
        let mut origin = vec![0; axes.len()];
        let mut output_origin = origin.clone();
        let mut start = 0;
        let mut mask = None;
        while start < total {
            let mut offset = start;
            for (position, length) in origin.iter_mut().zip(&shape) {
                *position = offset % length;
                offset /= length;
            }
            for (position, &source_axis) in output_origin.iter_mut().zip(axes) {
                *position = origin[source_axis];
            }
            let run = (quota / stride).min(shape[axis] - origin[axis]);
            block_shape[axis] = run;
            let count = run * stride;
            let (mut values, valid) = self.source.read_pixels(start, count)?;
            if valid.iter().any(|bit| !bit) {
                let mask = mask.get_or_insert_with(|| vec![true; total]);
                mask[start..start + count].copy_from_slice(&valid);
                for (value, valid) in values.iter_mut().zip(valid) {
                    if !valid {
                        *value = if zero_blanks { 0.0 } else { f32::NAN };
                    }
                }
            }
            let block = ArrayD::from_shape_vec(IxDyn(&block_shape).f(), values)
                .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?;
            image.put_slice(&block.permuted_axes(IxDyn(axes)), &output_origin)?;
            start += count;
        }
        if let Some(mask) = mask {
            let mask = ArrayD::from_shape_vec(IxDyn(&shape).f(), mask)
                .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?;
            image.put_mask("mask0", &mask.permuted_axes(IxDyn(axes)))?;
            image.set_default_mask("mask0")?;
        }
        Ok(())
    }
}

fn primary_restoring_beam(header: &FitsHeader) -> Option<StoredGaussianBeam> {
    let major = header.get_float("BMAJ")?;
    let minor = header.get_float("BMIN")?;
    let angle = header.get_float("BPA")?;
    if !major.is_finite() || !minor.is_finite() || major <= 0. || minor <= 0. {
        return None;
    }
    // The primary cards use degrees, while CASA's native beam record uses
    // arcseconds for widths and keeps the angle in its original degrees.
    StoredGaussianBeam::new(
        Quantity::new(3600. * major.max(minor), "arcsec").ok()?,
        Quantity::new(3600. * major.min(minor), "arcsec").ok()?,
        Quantity::new(angle, "deg").ok()?,
    )
    .ok()
}

fn initial_record_error(path: &Path) -> ImageError {
    ImageError::CasaSelection(format!(
        "FITSImgParser::setup - {} Error reading initial record -- exiting.",
        path.display()
    ))
}

fn missing_attachment_data(path: &Path) -> ImageError {
    ImageError::CasaSelection(format!(
        "No data in the zeroth or first extension of {}",
        path.display()
    ))
}

fn validate_initial_image_record(
    path: &Path,
    fits: &mut FitsFile,
    hdu: usize,
    length: u64,
) -> Result<(), ImageError> {
    let mut start = 0;
    let mut status = 0;
    // SAFETY: the file is open on the selected image HDU. CFITSIO writes
    // the data offset and status to the two live output pointers.
    unsafe {
        fitsio::sys::ffghadll(
            fits.as_raw(),
            std::ptr::null_mut(),
            &mut start,
            std::ptr::null_mut(),
            &mut status,
        );
    }
    check_status(status).map_err(fits_error)?;
    let start = u64::try_from(start).map_err(|_| missing_attachment_data(path))?;
    let available = length.saturating_sub(start);
    if available >= FITS_RECORD_BYTES {
        return Ok(());
    }
    if hdu == 0 && available != 0 {
        return Err(initial_record_error(path));
    }
    Err(missing_attachment_data(path))
}

fn is_tile_compressed(header: &FitsHeader) -> bool {
    matches!(header.get("ZIMAGE"), Some(FitsValue::Logical(true)))
}

/// A read-only pixel stream for a specific FITS image HDU.
pub struct FitsSectionReader {
    file: ThreadsafeFitsFile,
    shape: Vec<usize>,
    bitpix: i32,
}

impl FitsSectionReader {
    /// Open one image HDU and retain it for successive reads. `shape` is
    /// the logical image shape, in FITS pixel axis order.
    ///
    /// # Errors
    /// Returns an error for missing or incompatible image data, including
    /// a source whose shape changed since its metadata was read.
    pub fn open(path: &Path, hdu_index: usize, shape: &[usize]) -> Result<Self, ImageError> {
        let mut file = open_disk_file(path).map_err(fits_error)?;
        let hdu = file.hdu(hdu_index).map_err(fits_error)?;
        let HduInfo::ImageInfo { shape: actual, .. } = hdu.info else {
            return Err(ImageError::InvalidMetadata(
                "selected FITS HDU is not an image".into(),
            ));
        };
        if shape.is_empty() || shape.len() > 9 || !actual.iter().rev().eq(shape) {
            return Err(ImageError::InvalidMetadata(
                "FITS pixel source has incompatible image dimensions".into(),
            ));
        }
        let (mut bitpix, mut status) = (0, 0);
        // SAFETY: the handle is on the selected image and both outputs are
        // valid. CFITSIO reports the logical BITPIX for compressed images.
        unsafe { fitsio::sys::ffgidt(file.as_raw(), &mut bitpix, &mut status) };
        check_status(status).map_err(fits_error)?;
        Ok(Self {
            file: file.threadsafe(),
            shape: shape.to_vec(),
            bitpix,
        })
    }

    /// Read a strided section as `f32`, converting integer blanks to NaN.
    /// Only the requested region is allocated.
    ///
    /// # Errors
    /// Returns an error if reading fails or the slice is outside the image.
    pub fn read(
        &self,
        start: &[usize],
        count: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<f32>, ImageError> {
        let mut file = self.file.lock().map_err(fits_error)?;
        read_section(&mut file, &self.shape, self.bitpix, start, count, stride)
    }
}

struct SectionRequest {
    start: Vec<usize>,
    shape: Vec<usize>,
    stride: Vec<usize>,
    result: mpsc::SyncSender<Result<ArrayD<f32>, ImageError>>,
}

/// Retains the non-Send CFITSIO handle on one thread while expression workers
/// request sections. The first pixel request opens the file.
pub struct SharedFitsSectionReader {
    requests: mpsc::Sender<Option<SectionRequest>>,
    worker: Option<thread::JoinHandle<()>>,
}

impl SharedFitsSectionReader {
    /// Starts a worker that lazily opens the requested zero-based HDU on its first read. The caller
    /// supplies its axis extents. An error starting the worker is returned immediately.
    pub fn new(path: PathBuf, hdu: usize, shape: Vec<usize>) -> Result<Self, ImageError> {
        let (requests, receive) = mpsc::channel::<Option<SectionRequest>>();
        let worker = thread::Builder::new()
            .name("FITS expression reader".into())
            .spawn(move || {
                let mut reader = None;
                while let Ok(Some(request)) = receive.recv() {
                    let result = (|| {
                        if reader.is_none() {
                            reader = Some(FitsSectionReader::open(&path, hdu, &shape)?);
                        }
                        reader.as_ref().expect("open FITS section reader").read(
                            &request.start,
                            &request.shape,
                            &request.stride,
                        )
                    })();
                    let _ = request.result.send(result);
                }
            })
            .map_err(|error| ImageError::Io(error.to_string()))?;
        Ok(Self {
            requests,
            worker: Some(worker),
        })
    }

    /// Reads owned float pixels from a zero-based section with per-axis extents and strides. Failed
    /// reads and a disconnected worker return an image error.
    pub fn read(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<f32>, ImageError> {
        let (result, receive) = mpsc::sync_channel(1);
        self.requests
            .send(Some(SectionRequest {
                start: start.to_vec(),
                shape: shape.to_vec(),
                stride: stride.to_vec(),
                result,
            }))
            .map_err(|_| ImageError::Io("FITS expression reader has closed".into()))?;
        receive.recv().map_err(|_| {
            ImageError::Io("FITS expression reader ended before returning data".into())
        })?
    }
}

impl Drop for SharedFitsSectionReader {
    fn drop(&mut self) {
        let _ = self.requests.send(None);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

/// Borrow FITS metadata and a lazy section reader as a read-only image.
pub fn fits_section_image<'a>(
    shape: &'a [usize],
    metadata: &'a TempImage<f32>,
    path: &'a Path,
    read: impl Fn(&[usize], &[usize], &[usize]) -> Result<ArrayD<f32>, ImageError> + 'a,
) -> impl ImageInterface<f32> + 'a {
    FitsSectionImage {
        shape,
        metadata,
        path,
        read,
    }
}

struct FitsSectionImage<'a, F> {
    shape: &'a [usize],
    metadata: &'a TempImage<f32>,
    path: &'a Path,
    read: F,
}

impl<F> Lattice<f32> for FitsSectionImage<'_, F>
where
    F: Fn(&[usize], &[usize], &[usize]) -> Result<ArrayD<f32>, ImageError>,
{
    fn shape(&self) -> &[usize] {
        self.shape
    }

    fn is_persistent(&self) -> bool {
        true
    }

    fn is_paged(&self) -> bool {
        true
    }

    fn is_writable(&self) -> bool {
        false
    }

    fn get_at(&self, position: &[usize]) -> Result<f32, LatticeError> {
        let ones = vec![1; self.ndim()];
        let data = self.get_slice(position, &ones, &ones)?;
        Ok(*data.first().expect("read one FITS pixel"))
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<f32>, LatticeError> {
        (self.read)(start, shape, stride).map_err(|error| LatticeError::Table(error.to_string()))
    }
}

impl<F> ImageInterface<f32> for FitsSectionImage<'_, F>
where
    F: Fn(&[usize], &[usize], &[usize]) -> Result<ArrayD<f32>, ImageError>,
{
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        None
    }

    fn coordinates(&self) -> &CoordinateSystem {
        self.metadata.coordinates()
    }

    fn units(&self) -> &str {
        self.metadata.units()
    }

    fn misc_info(&self) -> RecordValue {
        self.metadata.misc_info()
    }

    fn image_info(&self) -> Result<ImageInfo, ImageError> {
        self.metadata.image_info()
    }

    fn name(&self) -> Option<&Path> {
        Some(self.path)
    }

    fn has_pixel_mask(&self) -> bool {
        // FITS blanks are discovered while reading the selected pixel sections.
        true
    }

    fn get_mask_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<Option<ArrayD<bool>>, ImageError> {
        let invalid =
            || ImageError::InvalidMetadata("FITS pixel section is outside the image".into());
        if start.len() != self.ndim() || shape.len() != self.ndim() || stride.len() != self.ndim() {
            return Err(invalid());
        }
        for (axis, &count) in shape.iter().enumerate() {
            if count == 0
                || stride[axis] == 0
                || (count - 1)
                    .checked_mul(stride[axis])
                    .and_then(|span| start[axis].checked_add(span))
                    .is_none_or(|last| last >= self.shape[axis])
            {
                return Err(invalid());
            }
        }
        let mut full_mask = None;
        let cursors = TraversalCursorIter::new(
            shape.to_vec(),
            self.nice_cursor_shape(),
            TraversalSpec::tiles(),
        );
        for cursor in cursors {
            let cursor = cursor?;
            let source = cursor
                .position
                .iter()
                .zip(start)
                .zip(stride)
                .map(|((&offset, &first), &step)| first + offset * step)
                .collect::<Vec<_>>();
            let pixels = (self.read)(&source, &cursor.shape, stride)?;
            if pixels.iter().any(|value| value.is_nan()) {
                let mask =
                    full_mask.get_or_insert_with(|| ArrayD::from_elem(IxDyn(shape).f(), true));
                let mut window = mask.slice_each_axis_mut(|axis| {
                    let dim = axis.axis.index();
                    let offset = cursor.position[dim];
                    Slice::from(offset..offset + cursor.shape[dim])
                });
                Zip::from(&mut window)
                    .and(&pixels)
                    .for_each(|valid, &pixel| *valid = !pixel.is_nan());
            }
        }
        Ok(full_mask)
    }
}

fn read_section(
    file: &mut FitsFile,
    shape: &[usize],
    bitpix: i32,
    start: &[usize],
    count: &[usize],
    stride: &[usize],
) -> Result<ArrayD<f32>, ImageError> {
    let invalid = || ImageError::InvalidMetadata("FITS pixel section is outside the image".into());
    let dims = shape.len();
    if start.len() != dims || count.len() != dims || stride.len() != dims {
        return Err(invalid());
    }
    let mut begin = Vec::with_capacity(dims);
    let mut end = Vec::with_capacity(dims);
    let mut increment = Vec::with_capacity(dims);
    let mut size = 1usize;
    for axis in 0..dims {
        if stride[axis] == 0 || count[axis] == 0 {
            return Err(invalid());
        }
        let last = (count[axis] - 1)
            .checked_mul(stride[axis])
            .and_then(|width| start[axis].checked_add(width))
            .filter(|last| *last < shape[axis])
            .ok_or_else(invalid)?;
        begin.push(c_long::try_from(start[axis] + 1).map_err(|_| invalid())?);
        end.push(c_long::try_from(last + 1).map_err(|_| invalid())?);
        increment.push(c_long::try_from(stride[axis]).map_err(|_| invalid())?);
        size = size.checked_mul(count[axis]).ok_or_else(invalid)?;
    }
    let mut values = vec![0.0_f32; size];
    let mut null = f32::NAN;
    // FITS float pixels already encode blanks as NaN. Asking CFITSIO to
    // substitute them would also turn valid infinities into blanks.
    let null_value = if bitpix < 0 {
        std::ptr::null_mut()
    } else {
        (&mut null as *mut f32).cast()
    };
    let (mut any_null, mut status) = (0, 0);
    // SAFETY: the FITS handle is on the selected image and the coordinate
    // vectors and output buffer match the checked section above.
    unsafe {
        fitsio::sys::ffgsv(
            file.as_raw(),
            fitsio::sys::TFLOAT as i32,
            begin.as_mut_ptr(),
            end.as_mut_ptr(),
            increment.as_mut_ptr(),
            null_value,
            values.as_mut_ptr().cast(),
            &mut any_null,
            &mut status,
        );
    }
    check_status(status).map_err(fits_error)?;
    ArrayD::from_shape_vec(IxDyn(count).f(), values)
        .map_err(|error| ImageError::InvalidMetadata(error.to_string()))
}

fn group_linear_wcs(
    input: &CoordinateSystem,
    axis_order: &[usize],
    header: &FitsHeader,
) -> CoordinateSystem {
    let mut output = CoordinateSystem::new().with_obs_info(input.obs_info().clone());
    let mut index = 0;
    let mut offset = 0;
    while index < input.n_coordinates() {
        let mut axes = Vec::new();
        while index < input.n_coordinates() {
            let CoordinateModel::Linear(axis) = input.coordinate(index) else {
                break;
            };
            axes.push(axis);
            index += 1;
        }
        if axes.is_empty() {
            let coordinate = input.coordinate(index);
            offset += coordinate.n_pixel_axes();
            output.add_coordinate(coordinate.clone());
            index += 1;
            continue;
        }
        let count = axes.iter().map(|axis| axis.n_pixel_axes()).sum();
        let (transform, increments) = read_linear_wcs(header, &axis_order[offset..offset + count]);
        offset += count;
        let names = axes.iter().flat_map(|axis| axis.axis_names()).collect();
        let units = axes.iter().flat_map(|axis| axis.axis_units()).collect();
        output.add_coordinate(
            LinearCoordinate::new(count, names, units)
                .with_reference_value(
                    axes.iter()
                        .flat_map(|axis| axis.reference_value())
                        .collect(),
                )
                .with_reference_pixel(
                    axes.iter()
                        .flat_map(|axis| axis.reference_pixel())
                        .collect(),
                )
                .with_increment(increments)
                .with_pc_matrix(transform),
        );
    }
    output
}

fn read_linear_wcs(header: &FitsHeader, axes: &[usize]) -> (Array2<f64>, Vec<f64>) {
    let matrix = |key: &str, row: usize, column: usize| {
        header.get_float(&format!("{key}{}_{}", axes[row] + 1, axes[column] + 1))
    };
    let dimensions = (axes.len(), axes.len());
    let cd = Array2::from_shape_fn(dimensions, |(row, column)| matrix("CD", row, column));
    // casacore represents linear CD matrices directly with unit increments.
    if cd.iter().any(Option::is_some) {
        return (cd.mapv(|value| value.unwrap_or(0.0)), vec![1.0; axes.len()]);
    }
    let pc = Array2::from_shape_fn(dimensions, |(row, column)| {
        matrix("PC", row, column).unwrap_or(if row == column { 1.0 } else { 0.0 })
    });
    let increments = axes
        .iter()
        .map(|axis| {
            header
                .get_float(&format!("CDELT{}", axis + 1))
                .unwrap_or(1.0)
        })
        .collect();
    (pc, increments)
}

fn fits_coordinate_record(
    coordinates: &CoordinateSystem,
    axis_order: &[usize],
    world_order: Option<&[usize]>,
    header: &FitsHeader,
) -> Result<RecordValue, ImageError> {
    let original = coordinates.to_record();
    let mut record = original.clone();
    let mut descriptors = CoordinateSystem::record_coordinates(&original)?;
    let coordinate_kind = |field: &str| match field.trim_end_matches(|ch: char| ch.is_ascii_digit())
    {
        "direction" => 0,
        "stokes" => 1,
        "spectral" => 2,
        _ => 3,
    };
    descriptors.sort_by_key(|coordinate| coordinate_kind(&coordinate.field));
    let mapping_fields = ["pixelmap", "pixelreplace", "worldmap", "worldreplace"];
    for descriptor in &descriptors {
        let kind = descriptor
            .field
            .trim_end_matches(|ch: char| ch.is_ascii_digit());
        let index = &descriptor.field[kind.len()..];
        record.remove(&descriptor.field);
        for field in mapping_fields {
            record.remove(&format!("{field}{index}"));
        }
    }
    for (index, coordinate) in descriptors.iter().enumerate() {
        let kind = coordinate
            .field
            .trim_end_matches(|ch: char| ch.is_ascii_digit());
        let old_index = &coordinate.field[kind.len()..];
        for field in mapping_fields {
            if let Some(value) = original.get(&format!("{field}{old_index}")) {
                let value = if matches!(field, "pixelmap" | "worldmap")
                    && let Value::Array(ArrayValue::Int32(map)) = value
                {
                    Value::Array(ArrayValue::Int32(map.mapv(|axis| {
                        let pixel = axis_order[axis as usize];
                        if field == "worldmap" {
                            world_order.map_or(pixel, |order| order[pixel]) as i32
                        } else {
                            pixel as i32
                        }
                    })))
                } else {
                    value.clone()
                };
                record.upsert(format!("{field}{index}"), value);
            }
        }
        if let Some(Value::Record(mut fields)) = original.get(&coordinate.field).cloned() {
            let pixel_axes = coordinate
                .pixel_axes
                .iter()
                .map(|axis| axis.map(|axis| axis_order[axis]))
                .collect::<Vec<_>>();
            if kind == "direction" {
                set_fits_pole_and_projection(&mut fields, &pixel_axes, header);
                normalize_direction_matrix(&mut fields);
            } else if kind == "linear" {
                fields.remove("naxes");
                fields.remove("coordinate_type");
            } else if kind == "spectral"
                && let Some(Some(axis)) = pixel_axes.first()
            {
                let ctype = header
                    .get_string(&format!("CTYPE{}", axis + 1))
                    .unwrap_or("");
                if ctype.eq_ignore_ascii_case("VOPT") {
                    fields.upsert("nativeType", Value::Scalar(ScalarValue::Int32(2)));
                }
                if let Some(Value::Record(tabular)) = fields.get_mut("tabular") {
                    tabular.upsert(
                        "pc",
                        Value::Array(ArrayValue::Float64(ndarray::arr2(&[[1.0]]).into_dyn())),
                    );
                }
                if header.get("SPECSYS").is_none()
                    && header.get("VELREF").is_none()
                    && !ctype.to_ascii_uppercase().ends_with("-LSR")
                {
                    fields.upsert(
                        "system",
                        Value::Scalar(ScalarValue::String("Undefined".into())),
                    );
                }
                let system = fields.get("system").expect("spectral coordinate system");
                let mut conversion = RecordValue::default();
                conversion.upsert("system", system.clone());
                conversion.upsert(
                    "epoch",
                    Value::Record(epoch_to_record(&MEpoch::from_mjd(0.0, EpochRef::LAST))),
                );
                conversion.upsert(
                    "position",
                    Value::Record(position_to_record(&MPosition::new_itrf(0.0, 0.0, 0.0))),
                );
                conversion.upsert(
                    "direction",
                    Value::Record(direction_to_record(&MDirection::from_angles(
                        0.0,
                        std::f64::consts::FRAC_PI_2,
                        DirectionRef::J2000,
                    ))),
                );
                fields.upsert("conversion", Value::Record(conversion));
            }
            record.upsert(format!("{kind}{index}"), Value::Record(fields));
        }
    }
    Ok(record)
}

fn normalize_direction_matrix(record: &mut RecordValue) {
    let Some(Value::Array(ArrayValue::Float64(mut matrix))) = record.get("pc").cloned() else {
        return;
    };
    let Some(Value::Array(ArrayValue::Float64(mut increments))) = record.get("cdelt").cloned()
    else {
        return;
    };
    // The saved PC matrix is transposed with respect to the WCS transform.
    // casacore moves each world-axis row's norm into its CDELT.
    for (axis, increment) in matrix.axis_iter_mut(Axis(1)).zip(&mut increments) {
        let norm = axis
            .iter()
            .map(|coefficient| coefficient * coefficient)
            .sum::<f64>()
            .sqrt();
        if norm != 0.0 && norm != 1.0 {
            *increment *= norm;
            axis.into_iter()
                .for_each(|coefficient| *coefficient /= norm);
        }
    }
    record.upsert("pc", Value::Array(ArrayValue::Float64(matrix)));
    record.upsert("cdelt", Value::Array(ArrayValue::Float64(increments)));
}

fn set_fits_pole_and_projection(
    direction: &mut RecordValue,
    pixel_axes: &[Option<usize>],
    header: &FitsHeader,
) {
    let Some(Value::Scalar(ScalarValue::String(projection))) = direction.get("projection") else {
        return;
    };
    let sin = projection == "SIN";
    if sin
        || matches!(
            projection.as_str(),
            "TAN" | "STG" | "ARC" | "ZEA" | "AZP" | "ZPN"
        )
    {
        // The pole for a zenithal WCS is the center of the projection.
        if let Some(Value::Array(ArrayValue::Float64(reference))) = direction.get("crval")
            && let Some(latitude) = reference.get(IxDyn(&[1]))
        {
            direction.upsert(
                "latpole",
                Value::Scalar(ScalarValue::Float64(latitude.to_degrees())),
            );
        }
    }
    if sin && let Some(Some(axis)) = pixel_axes.get(1) {
        let values = [1, 2].map(|index| {
            header
                .get_float(&format!("PV{}_{index}", axis + 1))
                .unwrap_or(0.0)
        });
        direction.upsert(
            "projection_parameters",
            Value::Array(ArrayValue::Float64(ndarray::arr1(&values).into_dyn())),
        );
    }
}

fn fits_error(error: fitsio::errors::Error) -> ImageError {
    ImageError::Io(error.to_string())
}

fn has_image_data(hdu: &FitsHdu) -> bool {
    matches!(&hdu.info, HduInfo::ImageInfo { shape, .. } if !shape.is_empty() && !shape.contains(&0))
}

fn read_hdu_cards(fits: &mut FitsFile) -> Result<Vec<String>, ImageError> {
    let (mut count, mut more, mut status) = (0, 0, 0);
    // SAFETY: the fitsio file is open on the image HDU and all outputs are live.
    unsafe { fitsio::sys::ffghsp(fits.as_raw(), &mut count, &mut more, &mut status) };
    check_status(status).map_err(fits_error)?;
    let mut cards = Vec::with_capacity(count as usize);
    for index in 1..=count {
        let mut buffer = [0; 81];
        // SAFETY: CFITSIO writes one card, at most 80 bytes and a terminator.
        unsafe { fitsio::sys::ffgrec(fits.as_raw(), index, buffer.as_mut_ptr(), &mut status) };
        check_status(status).map_err(fits_error)?;
        // SAFETY: a successful ffgrec NUL-terminates its output.
        cards.push(
            unsafe { CStr::from_ptr(buffer.as_ptr()) }
                .to_string_lossy()
                .into_owned(),
        );
    }
    Ok(cards)
}

fn wcs_key(name: &str) -> bool {
    let prefix = name.trim_end_matches(|ch: char| ch.is_ascii_digit() || ch == '_');
    matches!(
        prefix,
        "CTYPE"
            | "CRPIX"
            | "CRVAL"
            | "CDELT"
            | "CUNIT"
            | "CROTA"
            | "PV"
            | "PS"
            | "PC"
            | "CD"
            | "WCSAXES"
            | "WCSNAME"
            | "RADESYS"
            | "RADECSYS"
            | "EQUINOX"
            | "EPOCH"
            | "SPECSYS"
            | "SSYSOBS"
            | "LONPOLE"
            | "LATPOLE"
            | "RESTFRQ"
            | "RESTFREQ"
            | "RESTWAV"
            | "VELREF"
    )
}

fn alternate_wcs_key(name: &str) -> Option<(&str, u8)> {
    let last = *name.as_bytes().last()?;
    if !last.is_ascii_uppercase() {
        return None;
    }
    let base = &name[..name.len() - 1];
    wcs_key(base).then_some((base, last))
}

fn select_wcs(header: &FitsHeader, which_rep: usize) -> FitsHeader {
    if which_rep == 0 {
        return header.clone();
    }
    let mut alternatives: Vec<u8> = header
        .iter()
        .filter_map(|field| alternate_wcs_key(&field.name))
        .filter_map(|(base, suffix)| base.starts_with("CTYPE").then_some(suffix))
        .collect();
    alternatives.sort_unstable();
    alternatives.dedup();
    let Some(suffix) = alternatives.get(which_rep.min(alternatives.len()).saturating_sub(1)) else {
        return header.clone();
    };
    let mut selected = FitsHeader::new();
    for field in header.iter() {
        if let Some((base, candidate)) = alternate_wcs_key(&field.name) {
            if candidate == *suffix {
                selected.set(base, field.value.clone());
            }
        } else if !wcs_key(&field.name) {
            selected.set(&field.name, field.value.clone());
        }
    }
    selected
}

fn miscellaneous(header: &FitsHeader) -> RecordValue {
    let mut record = RecordValue::default();
    for field in header.iter() {
        let name = &field.name;
        if wcs_key(name)
            || alternate_wcs_key(name).is_some()
            || name.starts_with("NAXIS")
            || matches!(
                name.as_str(),
                "BUNIT"
                    | "BITPIX"
                    | "DATAMAX"
                    | "DATAMIN"
                    | "ORIGIN"
                    | "EXTEND"
                    | "BLOCKED"
                    | "BLANK"
                    | "SIMPLE"
                    | "BSCALE"
                    | "BZERO"
                    | "XTENSION"
                    | "PCOUNT"
                    | "GCOUNT"
                    | "TELESCOP"
                    | "OBSERVER"
                    | "DATE"
                    | "DATE-OBS"
                    | "OBSGEO-X"
                    | "OBSGEO-Y"
                    | "OBSGEO-Z"
                    | "MJD-OBS"
                    | "TIMESYS"
                    | "BMAJ"
                    | "BMIN"
                    | "BPA"
                    | "IMTYPE"
            )
        {
            continue;
        }
        let value = match &field.value {
            FitsValue::Integer(value) => {
                i32::try_from(*value).map_or(ScalarValue::Int64(*value), ScalarValue::Int32)
            }
            FitsValue::Float(value) => ScalarValue::Float64(*value),
            FitsValue::Logical(value) => ScalarValue::Bool(*value),
            FitsValue::String(value) if name == "OBJECT" && value.is_empty() => {
                ScalarValue::String(" ".into())
            }
            FitsValue::String(value) => ScalarValue::String(value.clone()),
        };
        record.upsert(name.to_ascii_lowercase(), Value::Scalar(value));
    }
    record
}

#[cfg(test)]
mod section_tests {
    use std::cell::Cell;

    use crate::TempStoragePolicy;

    use fitsio::images::{ImageDescription, ImageType};

    use super::*;

    #[test]
    fn invalid_linear_axis_unit_uses_casacore_fallback_coordinates() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("invalid-unit.fits");
        let mut file = FitsFile::create(&path)
            .with_custom_primary(&ImageDescription {
                data_type: ImageType::Float,
                dimensions: &[4, 3],
            })
            .open()
            .unwrap();
        let hdu = file.primary_hdu().unwrap();
        hdu.write_key(&mut file, "CTYPE1", "index axis").unwrap();
        hdu.write_key(&mut file, "CUNIT1", "index").unwrap();
        hdu.write_key(&mut file, "CRVAL1", 0.0).unwrap();
        hdu.write_key(&mut file, "OBSGEO-X", 6378137.0).unwrap();
        hdu.write_key(&mut file, "OBSGEO-Y", 0.0).unwrap();
        hdu.write_key(&mut file, "OBSGEO-Z", 0.0).unwrap();
        drop(file);

        let imported = FitsToolImport::open(&path, 0, 0).unwrap();
        let direct = PrimaryHdu::open(&path).unwrap();
        for coordinates in [imported.coordinates(), &direct.coordinates] {
            let CoordinateModel::Linear(coordinate) = coordinates.coordinate(0) else {
                panic!("expected fallback linear coordinate");
            };
            assert_eq!(coordinates.n_coordinates(), 1);
            assert_eq!(coordinate.axis_names(), ["linear0", "linear1"]);
            assert_eq!(coordinate.axis_units(), ["km", "km"]);
            assert_eq!(coordinate.reference_pixel(), [2., 2.]);
            assert_eq!(coordinate.to_world(&[3., 4.]).unwrap(), [1., 2.]);
            assert!(coordinates.obs_info().telescope_position.is_none());
        }
    }

    #[test]
    fn fits_unit_alias_preserves_linear_and_celestial_coordinates() {
        use crate::fits_read::read_fits_image;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("unit-alias.fits");
        let mut file = FitsFile::create(&path)
            .with_custom_primary(&ImageDescription {
                data_type: ImageType::Float,
                dimensions: &[3, 4, 4],
            })
            .open()
            .unwrap();
        let hdu = file.primary_hdu().unwrap();
        for (key, value) in [
            ("CTYPE1", "RA---SIN"),
            ("CTYPE2", "DEC--SIN"),
            ("CTYPE3", "ANGLE"),
            ("CUNIT3", "DEGREE"),
        ] {
            hdu.write_key(&mut file, key, value).unwrap();
        }
        for (key, value) in [
            ("CRVAL1", 15.0),
            ("CRVAL2", 27.0),
            ("CRVAL3", 13.0),
            ("CDELT1", -0.01),
            ("CDELT2", 0.01),
            ("CDELT3", 2.5),
            ("CRPIX3", 2.0),
        ] {
            hdu.write_key(&mut file, key, value).unwrap();
        }
        drop(file);
        let imported = FitsToolImport::open(&path, 0, 0).unwrap();
        let direct = read_fits_image(&path).unwrap();
        for coordinates in [imported.coordinates(), &direct.coordinates] {
            assert!(matches!(
                coordinates.coordinate(0),
                CoordinateModel::Direction(_)
            ));
            let CoordinateModel::Linear(coordinate) = coordinates.coordinate(1) else {
                panic!("expected FITS linear coordinate");
            };
            assert_eq!(coordinate.axis_names(), ["ANGLE"]);
            assert_eq!(coordinate.axis_units(), ["deg"]);
            assert_eq!(coordinate.to_world(&[2.]).unwrap(), [15.5]);
        }
    }

    #[test]
    fn reads_logical_axes_of_a_tile_compressed_cube() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cube.fits");
        let specification = format!("{}[compress]", path.display());
        let mut file = FitsFile::create(specification).open().unwrap();
        let hdu = file
            .create_image(
                "CUBE",
                &ImageDescription {
                    data_type: ImageType::Short,
                    dimensions: &[4, 3, 2],
                },
            )
            .unwrap();
        hdu.write_image(&mut file, &(0_i16..24).collect::<Vec<_>>())
            .unwrap();
        assert!(hdu.read_key::<bool>(&mut file, "ZIMAGE").unwrap());
        drop(file);

        let source = FitsToolImport::open(&path, 0, -1).unwrap();
        assert_eq!(source.shape(), [2, 3, 4]);
        assert!(source.is_tile_compressed());
        let reader = FitsSectionReader::open(&path, source.hdu_index(), source.shape()).unwrap();
        let region = reader.read(&[1, 0, 1], &[1, 2, 2], &[1, 2, 2]).unwrap();
        assert_eq!(region.shape(), [1, 2, 2]);
        assert_eq!(region.as_slice_memory_order().unwrap(), [7., 11., 19., 23.]);
    }

    #[test]
    fn fits_mask_batches_large_strided_requests() {
        let shape = [2050, 2052];
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(LinearCoordinate::new(
            2,
            vec!["X".into(), "Y".into()],
            vec!["".into(); 2],
        ));
        let metadata = TempImage::new(vec![1, 1], coordinates, TempStoragePolicy::Memory).unwrap();
        let largest = Cell::new(0);
        let image = fits_section_image(
            &shape,
            &metadata,
            Path::new("virtual.fits"),
            |origin, extent, stride| {
                largest.set(largest.get().max(extent.iter().product()));
                Ok(ArrayD::from_shape_fn(IxDyn(extent), |index| {
                    let pixel = [
                        origin[0] + index[0] * stride[0],
                        origin[1] + index[1] * stride[1],
                    ];
                    if matches!(pixel, [1, 2] | [2049, 2050]) {
                        f32::NAN
                    } else {
                        1.0
                    }
                }))
            },
        );
        let count = [1025, 1025];
        let mask = image
            .get_mask_slice(&[1, 2], &count, &[2, 2])
            .unwrap()
            .unwrap();
        assert!(largest.get() < count.iter().product());
        assert!(!mask[[0, 0]] && !mask[[1024, 1024]]);
        assert_eq!(mask.iter().filter(|&&valid| !valid).count(), 2);
    }
}
