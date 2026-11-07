// SPDX-License-Identifier: LGPL-3.0-or-later
//! A primary-HDU FITS image read as `importfits` reads it: its header, the
//! coordinate system casa-coordinates makes of it, its image info and its
//! pixels.

use std::path::Path;

use casa_coordinates::{CoordinateSystem, FitsHeader, FitsValue, fits::from_fits_header};
use casa_types::ArrayD;
use fitsio::{FitsFile, hdu::HduInfo, images::ImageType as FitsImageType};
use ndarray::{IxDyn, ShapeBuilder};

use crate::{GaussianBeam, ImageBeamSet, ImageError, ImageInfo, ImageType};

mod beams;
mod history;
pub(crate) use history::{history_entries, read_cards};

/// A FITS image read into memory ([`read_fits_image`]).
#[derive(Debug, Clone)]
pub struct FitsImage {
    /// The axis lengths, `NAXIS1` first.
    pub shape: Vec<usize>,
    /// The coordinate system, as `importfits` makes it.
    pub coordinates: CoordinateSystem,
    /// The header keywords casa-rs reads.
    pub header: FitsHeader,
    /// The restoring beam, image type and object name.
    pub image_info: ImageInfo,
    /// `BUNIT`, or empty.
    pub units: String,
    /// The pixels in Fortran order, as single precision: casacore's
    /// `FITSImage` reads every `BITPIX` as `Float`.
    pub pixels: ArrayD<f32>,
}

/// Read the primary-HDU image of `fitsimage` into memory, as CASA's
/// `ia.newimagefromfile` opens a FITS file.
///
/// # Errors
///
/// [`ImageError`] when the file cannot be read, its primary HDU is not an
/// image, or its header does not make a coordinate system.
pub fn read_fits_image(fitsimage: impl AsRef<Path>) -> Result<FitsImage, ImageError> {
    let mut primary = PrimaryHdu::open(fitsimage.as_ref())?;
    let data = primary
        .hdu
        .read_image::<Vec<f32>>(&mut primary.fits)
        .map_err(|error| ImageError::Io(error.to_string()))?;
    let pixels = ArrayD::from_shape_vec(IxDyn(&primary.shape).f(), data).map_err(|error| {
        ImageError::InvalidMetadata(format!("invalid FITS image shape: {error}"))
    })?;
    let mut image_info = image_info_from_fits_header(&primary.header);
    if let Some((beams, _)) = beams::read_beams(&mut primary)? {
        image_info.beam_set = beams;
    }
    Ok(FitsImage {
        image_info,
        units: primary.header.get_string("BUNIT").unwrap_or("").to_string(),
        shape: primary.shape,
        coordinates: primary.coordinates,
        header: primary.header,
        pixels,
    })
}

/// An open FITS file's primary HDU, with its shape, header and coordinates
/// read.
pub(crate) struct PrimaryHdu {
    pub(crate) fits: FitsFile,
    pub(crate) hdu: fitsio::hdu::FitsHdu,
    pub(crate) image_type: FitsImageType,
    pub(crate) shape: Vec<usize>,
    pub(crate) header: FitsHeader,
    pub(crate) coordinates: CoordinateSystem,
}

impl PrimaryHdu {
    pub(crate) fn open(fitsimage: &Path) -> Result<Self, ImageError> {
        let mut fits =
            FitsFile::open(fitsimage).map_err(|error| ImageError::Io(error.to_string()))?;
        let hdu = fits
            .primary_hdu()
            .map_err(|error| ImageError::Io(error.to_string()))?;
        let image_type = match &hdu.info {
            HduInfo::ImageInfo { image_type, .. } => *image_type,
            HduInfo::TableInfo { .. } | HduInfo::AnyInfo => {
                return Err(ImageError::InvalidMetadata(
                    "primary HDU is not a FITS image".to_string(),
                ));
            }
        };
        let shape = read_fits_axis_shape(&mut fits, &hdu)?;
        let header = read_primary_fits_header(&mut fits, &hdu, &shape)?;
        let coordinates = from_fits_header(&header, &shape)?;
        Ok(Self {
            fits,
            hdu,
            image_type,
            shape,
            header,
            coordinates,
        })
    }

    /// Read a bounded, linear block and its FITS null flags.
    pub(crate) fn read_pixels(
        &mut self,
        start: usize,
        count: usize,
    ) -> Result<(Vec<f32>, Vec<bool>), ImageError> {
        self.fits
            .primary_hdu()
            .map_err(|error| ImageError::Io(error.to_string()))?;
        let start = i64::try_from(start)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| ImageError::InvalidMetadata("FITS pixel offset is too large".into()))?;
        let length = i64::try_from(count)
            .map_err(|_| ImageError::InvalidMetadata("FITS pixel chunk is too large".into()))?;
        let mut pixels = vec![0.0_f32; count];
        let mut nulls = vec![0 as std::ffi::c_char; count];
        let mut any_null = 0;
        let mut status = 0;
        // SAFETY: the file is open on the primary HDU and the buffers are
        // writable for `count` elements of the types requested from CFITSIO.
        unsafe {
            fitsio::sys::ffgpf(
                self.fits.as_raw(),
                fitsio::sys::TFLOAT as i32,
                start,
                length,
                pixels.as_mut_ptr().cast(),
                nulls.as_mut_ptr(),
                &mut any_null,
                &mut status,
            );
        }
        fitsio::errors::check_status(status).map_err(|error| ImageError::Io(error.to_string()))?;
        Ok((pixels, nulls.into_iter().map(|null| null == 0).collect()))
    }

    pub(crate) fn beam_set(
        &mut self,
    ) -> Result<Option<(ImageBeamSet, casa_types::RecordValue)>, ImageError> {
        beams::read_beams(self)
    }
}

pub(crate) fn image_info_from_fits_header(header: &FitsHeader) -> ImageInfo {
    let object_name = header.get_string("OBJECT").unwrap_or("").to_string();
    let beam_set = match (
        header.get_float("BMAJ"),
        header.get_float("BMIN"),
        header.get_float("BPA"),
    ) {
        (Some(major_deg), Some(minor_deg), Some(position_angle_deg)) => {
            ImageBeamSet::new(GaussianBeam::new(
                major_deg.to_radians(),
                minor_deg.to_radians(),
                position_angle_deg.to_radians(),
            ))
        }
        _ => ImageBeamSet::default(),
    };
    ImageInfo {
        beam_set,
        image_type: header
            .get_int("IMTYPE")
            .map(|value| ImageType::from_fits_value(value as i32))
            .unwrap_or(ImageType::Undefined),
        object_name,
    }
}

fn read_primary_fits_header(
    fits: &mut FitsFile,
    hdu: &fitsio::hdu::FitsHdu,
    shape: &[usize],
) -> Result<FitsHeader, ImageError> {
    let mut header = FitsHeader::new();
    header.set("NAXIS", FitsValue::Integer(shape.len() as i64));
    for (axis, axis_len) in shape.iter().enumerate() {
        header.set(
            format!("NAXIS{}", axis + 1),
            FitsValue::Integer(*axis_len as i64),
        );
    }
    for key in [
        "RADESYS", "SPECSYS", "BUNIT", "OBJECT", "TELESCOP", "OBSERVER",
    ] {
        if let Some(value) = read_optional_key::<String>(fits, hdu, key)? {
            header.set(key, FitsValue::String(value.trim_end().to_string()));
        }
    }
    for key in [
        "EQUINOX", "RESTFRQ", "BMAJ", "BMIN", "BPA", "LONPOLE", "LATPOLE", "CROTA2",
    ] {
        if let Some(value) = read_optional_key::<f64>(fits, hdu, key)? {
            header.set(key, FitsValue::Float(value));
        }
    }
    for axis in 1..=shape.len() {
        for prefix in ["CUNIT", "CTYPE"] {
            let key = format!("{prefix}{axis}");
            if let Some(value) = read_optional_key::<String>(fits, hdu, &key)? {
                header.set(key, FitsValue::String(value.trim_end().to_string()));
            }
        }
        for prefix in ["CRVAL", "CRPIX", "CDELT"] {
            let key = format!("{prefix}{axis}");
            if let Some(value) = read_optional_key::<f64>(fits, hdu, &key)? {
                header.set(key, FitsValue::Float(value));
            }
        }
    }
    for key in ["IMTYPE", "BLANK"] {
        if let Some(value) = read_optional_key::<i64>(fits, hdu, key)? {
            header.set(key, FitsValue::Integer(value));
        }
    }
    if let Some(value) = read_optional_key::<bool>(fits, hdu, "CASAMBM")? {
        header.set("CASAMBM", FitsValue::Logical(value));
    }
    // The observation date (#1017). A date card with no value (`MJD-OBS =`,
    // FITS's way to say it is unknown) or one of another type is left out,
    // as casacore warns and leaves it out, so `DATE-OBS` can still give the
    // date and the image still imports.
    for key in ["DATE-OBS", "TIMESYS"] {
        if let Some(value) = read_optional_date_key::<String>(fits, hdu, key)? {
            header.set(key, FitsValue::String(value.trim_end().to_string()));
        }
    }
    if let Some(value) = read_optional_date_key::<f64>(fits, hdu, "MJD-OBS")? {
        header.set("MJD-OBS", FitsValue::Float(value));
    }
    for i in 1..=shape.len() {
        for j in 1..=shape.len() {
            let pc_key = format!("PC{i}_{j}");
            if let Some(value) = read_optional_key::<f64>(fits, hdu, &pc_key)? {
                header.set(pc_key, FitsValue::Float(value));
            }
            let cd_key = format!("CD{i}_{j}");
            if let Some(value) = read_optional_key::<f64>(fits, hdu, &cd_key)? {
                header.set(cd_key, FitsValue::Float(value));
            }
        }
    }
    Ok(header)
}

fn read_fits_axis_shape(
    fits: &mut FitsFile,
    hdu: &fitsio::hdu::FitsHdu,
) -> Result<Vec<usize>, ImageError> {
    let naxis = hdu
        .read_key::<i64>(fits, "NAXIS")
        .map_err(|error| ImageError::Io(error.to_string()))? as usize;
    let mut shape = Vec::with_capacity(naxis);
    for axis in 1..=naxis {
        let axis_len = hdu
            .read_key::<i64>(fits, &format!("NAXIS{axis}"))
            .map_err(|error| ImageError::Io(error.to_string()))?;
        shape.push(axis_len as usize);
    }
    Ok(shape)
}

/// CFITSIO's statuses for a missing card, a card with no value, and a
/// value it cannot convert to an integer, a float or a double.
const KEY_NO_EXIST: i32 = 202;
const VALUE_UNDEFINED: i32 = 204;
const BAD_C2I: i32 = 407;
const BAD_C2D: i32 = 409;

/// Card `key` of an observation date, `None` when the header lacks it,
/// gives it no value, or gives a value CFITSIO cannot read as `T`; any
/// other error stands.
fn read_optional_date_key<T>(
    fits: &mut FitsFile,
    hdu: &fitsio::hdu::FitsHdu,
    key: &str,
) -> Result<Option<T>, ImageError>
where
    T: fitsio::headers::ReadsKey,
{
    match hdu.read_key::<T>(fits, key) {
        Ok(value) => Ok(Some(value)),
        Err(fitsio::errors::Error::Fits(error))
            if matches!(
                error.status,
                KEY_NO_EXIST | VALUE_UNDEFINED | BAD_C2I..=BAD_C2D
            ) =>
        {
            Ok(None)
        }
        Err(error) => Err(ImageError::Io(error.to_string())),
    }
}

fn read_optional_key<T>(
    fits: &mut FitsFile,
    hdu: &fitsio::hdu::FitsHdu,
    key: &str,
) -> Result<Option<T>, ImageError>
where
    T: fitsio::headers::ReadsKey,
{
    match hdu.read_key::<T>(fits, key) {
        Ok(value) => Ok(Some(value)),
        Err(error) => {
            let text = error.to_string();
            if text.contains("keyword not found") || text.contains("not found in header") {
                Ok(None)
            } else {
                Err(ImageError::Io(text))
            }
        }
    }
}
