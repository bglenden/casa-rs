// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ffi::CStr;
use std::path::Path;

use casa_coordinates::{
    CoordinateSystem, FitsHeader, FitsValue, LinearCoordinate,
    fits::coordinate_util::from_fits_header_with_axis_order,
};
use casa_types::measures::{
    EpochRef, MEpoch, MPosition,
    direction::{DirectionRef, MDirection},
    record::{direction_to_record, epoch_to_record, position_to_record},
};
use casa_types::{ArrayD, ArrayValue, RecordValue, ScalarValue, Value};
use fitsio::{
    FitsFile,
    errors::check_status,
    hdu::{FitsHdu, HduInfo},
};
use ndarray::{Axis, IxDyn, ShapeBuilder};

use super::{
    PrimaryHdu, brightness_unit, history::history_from_cards, image_info_from_fits_header,
    read_fits_axis_shape,
};
use crate::{ImageError, MutableImageInterface, image::ImageHistoryRow};

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
        let mut fits = FitsFile::open(path).map_err(|_| {
            ImageError::CasaSelection(format!(
                "Exception: {} does not exist or is not readable.",
                path.display()
            ))
        })?;
        let missing_hdu = || {
            ImageError::CasaSelection(format!(
                "Exception: No image at specified location in file {}.",
                path.display()
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
        let HduInfo::ImageInfo { image_type, .. } = hdu.info else {
            unreachable!("image HDU selected above");
        };
        let shape = read_fits_axis_shape(&mut fits, &hdu)?;
        let cards = read_hdu_cards(&mut fits)?;
        let physical = FitsHeader::from_cards(
            &cards
                .iter()
                .filter(|card| card.as_bytes().get(8) == Some(&b'='))
                .map(String::as_str)
                .collect::<Vec<_>>(),
        );
        let mut header = select_wcs(&physical, which_rep);
        if header.get("RADESYS").is_none() {
            let system = match header.get_string("RADECSYS") {
                Some(system) => system,
                None => match header.get_float("EQUINOX") {
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
        let (mut coordinates, axis_order) = from_fits_header_with_axis_order(&header, &shape)?;
        if !has_wcs {
            let ndim = shape.len();
            coordinates = CoordinateSystem::new().with_obs_info(coordinates.obs_info().clone());
            coordinates.add_coordinate(
                LinearCoordinate::new(ndim, vec![String::new(); ndim], vec![String::new(); ndim])
                    .with_reference_pixel(vec![-1.0; ndim]),
            );
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
        if let [Some(x), Some(y), Some(z)] =
            ["OBSGEO-X", "OBSGEO-Y", "OBSGEO-Z"].map(|key| header.get_float(key))
        {
            obs.telescope_position = Some(MPosition::new_itrf(x, y, z));
        }
        let coordinate_record = fits_coordinate_record(&coordinates, &axis_order, &header)?;
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
        }
        Ok(Self {
            coordinate_record,
            units: brightness_unit(&source.header),
            image_info: info,
            misc_info: miscellaneous(&physical),
            history: history_from_cards(&cards, true),
            source,
        })
    }

    /// Shape in CASA's axis order, including any singleton axes.
    pub fn shape(&self) -> &[usize] {
        &self.source.shape
    }

    /// Coordinate system selected from the FITS header.
    pub fn coordinates(&self) -> &CoordinateSystem {
        &self.source.coordinates
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
    pub(crate) fn copy_pixels_with_axes(
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

fn fits_coordinate_record(
    coordinates: &CoordinateSystem,
    axis_order: &[usize],
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
                    Value::Array(ArrayValue::Int32(
                        map.mapv(|axis| axis_order[axis as usize] as i32),
                    ))
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
    if sin || matches!(projection.as_str(), "TAN" | "STG" | "ARC" | "ZEA" | "AZP") {
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
