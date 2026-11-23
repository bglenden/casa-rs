// SPDX-License-Identifier: LGPL-3.0-or-later
//! The Python image tool's metadata-only FITS header view.

use std::collections::BTreeMap;

use casa_coordinates::{
    CoordinateModel, CoordinateSystem, FitsKeyword, FitsValue, ProjectionType, StokesCoordinate,
    fits::{coordinate_util::fits_stokes_axis, try_to_fits_header},
};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::position::PositionRef;
use casa_types::quanta::format::{FITS, format_time};
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};

use crate::image::ImageHistoryEntry;
use crate::{ImageError, ImageInfo, ImageInterface, ImagePixel, ImageType, StoredGaussianBeam};

use super::{
    ExportAxisPlan, build_export_keywords, split_axis_keyword, stored_position_angle_degrees,
    visit_history_cards,
};

/// Ordered FITS cards and CASA's grouped dictionary representation of them.
pub struct ImageFitsHeader(Vec<HeaderCard>);

struct HeaderCard {
    keyword: FitsKeyword,
    group: Option<&'static str>,
}

/// Build a FITS header for an attached image without reading its pixels.
///
/// The shape is supplied separately so that virtual and native attachments
/// use the same header path.
///
/// # Errors
/// Returns an error for invalid WCS, metadata or an unreadable clock.
pub fn image_fits_header(
    image: &dyn ImageInterface<f32>,
    shape: &[usize],
    name: &str,
    history: &[ImageHistoryEntry],
) -> Result<ImageFitsHeader, ImageError> {
    image_fits_header_with_beams(image, shape, name, history, None)
}

/// Build an attached image's FITS header using its saved restoring beam view.
///
/// An open tool can retain beam metadata independently of shared native pixels.
pub fn image_fits_header_with_beams(
    image: &dyn ImageInterface<f32>,
    shape: &[usize],
    name: &str,
    history: &[ImageHistoryEntry],
    beam_info: Option<&RecordValue>,
) -> Result<ImageFitsHeader, ImageError> {
    let coords = image.coordinates();
    let plan = ExportAxisPlan::build(coords, shape, false, false, false, false, false)?;
    let standard_wcs = || {
        let base = try_to_fits_header(coords, shape)?;
        let mut wcs = build_export_keywords(coords, &base, &plan, true, true, None)?
            .into_iter()
            .collect();
        normalize_wcs(coords, &mut wcs)?;
        Ok::<_, ImageError>(wcs)
    };
    let (mut wcs, projection) = match standard_wcs() {
        Ok(wcs) => (wcs, Some(projection_parameters(coords, Some))),
        Err(_) => (linear_fallback(coords, shape.len())?, None),
    };
    let mut header = ImageFitsHeader(Vec::new());
    header.add("SIMPLE", FitsValue::Logical(true), Some("Standard FITS"));
    header.add(
        "BITPIX",
        FitsValue::Integer(-32),
        Some("Floating point (32 bit)"),
    );
    header.integer("NAXIS", shape.len() as i64);
    for (index, length) in shape.iter().enumerate() {
        header.integer(format!("NAXIS{}", index + 1), *length as i64);
    }
    header.add("EXTEND", FitsValue::Logical(true), None);
    header.add(
        "BSCALE",
        FitsValue::Float(1.0),
        Some("PHYSICAL = PIXEL*BSCALE + BZERO"),
    );
    header.add("BZERO", FitsValue::Float(0.0), None);
    let mut info = image.image_info()?;
    if let Some(record) = beam_info {
        info.beam_set = ImageInfo::from_record(record)?.beam_set;
    }
    header.image_metadata(image.units(), &info, beam_info)?;
    let standard = projection.is_some();
    header.axes(shape.len(), &mut wcs, projection);
    for (name, comment) in [
        ("RESTFRQ", "Rest Frequency (Hz)"),
        ("SPECSYS", "Spectral reference frame"),
        ("ALTRVAL", "Alternate frequency reference value"),
        ("ALTRPIX", "Alternate frequency reference pixel"),
        ("VELREF", "1 LSR, 2 HEL, 3 OBS, +256 Radio"),
    ] {
        if let Some(value) = wcs.remove(name) {
            header.add(name, value, Some(comment));
            if name == "VELREF" {
                header.text(
                    "COMMENT",
                    "casacore non-standard usage: 4 LSD, 5 GEO, 6 SOU, 7 GAL",
                );
            }
        }
    }
    if standard {
        header.observation(image, &mut wcs)?;
    } else {
        header.text("TIMESYS", "UTC");
    }
    header.extend_misc(image.misc_info())?;
    header.add(
        "DATE",
        FitsValue::String(format_time(
            ImageHistoryEntry::current_time()? / 86_400.0,
            FITS,
            12,
        )),
        Some("Date FITS file was written"),
    );
    header.text("ORIGIN", "casacore-3.8.0");
    if info.beam_set.is_multi() {
        header.add(
            "CASAMBM",
            FitsValue::Logical(true),
            Some("CASA multiple BEAMS table present"),
        );
    }
    if !history.is_empty() {
        visit_history_cards(history, false, |text| {
            header.text("HISTORY", text);
            Ok(())
        })?;
    }
    header.text("END", "");
    let start = name.len().saturating_sub(68);
    let suffix = name.get(start..).unwrap_or_else(|| {
        let boundary = name.char_indices().find(|(i, _)| *i >= start);
        &name[boundary.map_or(name.len(), |(i, _)| i)..]
    });
    header.text("IMAGENME", suffix);
    Ok(header)
}

pub(super) fn image_export_cards<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
    info: &ImageInfo,
    wcs: &[(String, FitsValue)],
) -> Result<Vec<String>, ImageError> {
    let mut header = ImageFitsHeader(Vec::new());
    header.image_metadata(image.units(), info)?;
    header.0.retain(|card| match card.keyword.name.as_str() {
        "BMAJ" | "BMIN" | "BPA" => false,
        "BUNIT" => image.units().is_empty(),
        "OBJECT" => info.object_name.is_empty(),
        _ => true,
    });
    header.observation(image, &mut BTreeMap::new())?;
    let mut misc = ImageFitsHeader(Vec::new());
    misc.extend_misc(image.misc_info())?;
    let keep_misc = |name: &str| {
        !matches!(
            name,
            "SIMPLE"
                | "BITPIX"
                | "NAXIS"
                | "EXTEND"
                | "EXTNAME"
                | "END"
                | "BSCALE"
                | "BZERO"
                | "BLANK"
                | "DATAMIN"
                | "DATAMAX"
                | "BMAJ"
                | "BMIN"
                | "BPA"
                | "CASAMBM"
                | "BTYPE"
                | "BUNIT"
                | "OBJECT"
                | "DATE"
                | "ORIGIN"
        ) && !(name.starts_with("NAXIS") && name[5..].bytes().all(|byte| byte.is_ascii_digit()))
            && !wcs.iter().any(|(key, _)| key == name)
    };
    header.0.extend(
        misc.0
            .into_iter()
            .filter(|card| keep_misc(&card.keyword.name)),
    );
    header.text(
        "DATE",
        format_time(ImageHistoryEntry::current_time()? / 86_400.0, FITS, 12),
    );
    header.text("ORIGIN", "casa-rs");
    Ok(header
        .0
        .iter()
        .map(|card| render_card(&card.keyword))
        .collect())
}

impl ImageFitsHeader {
    fn add(&mut self, name: impl Into<String>, value: FitsValue, comment: Option<&str>) {
        let name = name.into();
        let group = group_name(&name);
        self.0.push(HeaderCard {
            keyword: FitsKeyword {
                name,
                value,
                comment: comment.map(str::to_owned),
            },
            group,
        });
    }

    fn text(&mut self, name: impl Into<String>, text: impl Into<String>) {
        self.add(name, FitsValue::String(text.into()), None);
    }

    fn integer(&mut self, name: impl Into<String>, number: i64) {
        self.add(name, FitsValue::Integer(number), None);
    }

    fn image_metadata(
        &mut self,
        units: &str,
        info: &ImageInfo,
        beam_info: Option<&RecordValue>,
    ) -> Result<(), ImageError> {
        if let Some(beam) = info.beam_set.single_beam().filter(|beam| !beam.is_null()) {
            self.add("BMAJ", FitsValue::Float(beam.major_in("deg")?), None);
            self.add("BMIN", FitsValue::Float(beam.minor_in("deg")?), None);
            let angle = match beam_info.and_then(|record| record.get("restoringbeam")) {
                Some(Value::Record(stored)) => {
                    stored_position_angle_degrees(&StoredGaussianBeam::from_record(stored)?)?
                }
                _ => wrapped_beam_position_angle(beam.position_angle_in("deg")?),
            };
            self.add("BPA", FitsValue::Float(angle), None);
        }
        let image_type = if info.image_type == ImageType::Undefined {
            ImageType::Intensity
        } else {
            info.image_type
        };
        self.text("BTYPE", image_type.to_string());
        self.text("OBJECT", &info.object_name);
        self.add(
            "BUNIT",
            FitsValue::String(units.to_owned()),
            Some("Brightness (pixel) unit"),
        );
        Ok(())
    }

    fn axes(
        &mut self,
        rank: usize,
        wcs: &mut BTreeMap<String, FitsValue>,
        projection: Option<Vec<(String, FitsValue)>>,
    ) {
        for keyword in ["EQUINOX", "RADESYS", "LONPOLE", "LATPOLE"] {
            if let Some(value) = wcs.remove(keyword) {
                self.add(keyword, value, None);
            }
        }
        if projection.is_some() {
            for col in 1..=rank {
                for row in 1..=rank {
                    let name = format!("PC{row}_{col}");
                    let value = wcs
                        .remove(&name)
                        .unwrap_or(FitsValue::Float(f64::from(row == col)));
                    self.add(name, value, None);
                }
            }
        }
        let projection = projection.unwrap_or_default();
        let interleave_projection = projection.len() == rank;
        let mut projection = projection.into_iter();
        for axis in 1..=rank {
            for key in ["CTYPE", "CRVAL", "CDELT", "CRPIX", "CUNIT"] {
                let name = format!("{key}{axis}");
                if let Some(value) = wcs.remove(&name) {
                    self.add(name, value, None);
                } else if key == "CUNIT" {
                    self.text(name, "");
                }
            }
            if interleave_projection && let Some((name, value)) = projection.next() {
                self.add(name, value, None);
            }
        }
        for (name, value) in projection {
            self.add(name, value, None);
        }
    }

    fn observation<T: ImagePixel>(
        &mut self,
        image: &dyn ImageInterface<T>,
        wcs: &mut BTreeMap<String, FitsValue>,
    ) -> Result<(), ImageError> {
        let obs = image.coordinates().obs_info();
        if obs
            .date
            .as_ref()
            .is_none_or(|epoch| epoch.value().as_mjd() == 0.0)
        {
            wcs.remove("DATE-OBS");
            wcs.insert("TIMESYS".into(), FitsValue::String("UTC".into()));
        }
        for keyword in ["TELESCOP", "OBSERVER", "DATE-OBS", "TIMESYS"] {
            if let Some(value) = wcs.remove(keyword) {
                self.add(keyword, value, None);
            }
        }
        if let Some(position) = &obs.telescope_position {
            let xyz = position
                .convert_to(PositionRef::ITRF)
                .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?
                .values();
            for (key, value) in ["OBSGEO-X", "OBSGEO-Y", "OBSGEO-Z"].into_iter().zip(xyz) {
                self.add(key, FitsValue::Float(value), None);
            }
        }
        if !obs.pointing_center_initial {
            for (key, angle) in ["OBSRA", "OBSDEC"].into_iter().zip(obs.pointing_center_rad) {
                self.add(key, FitsValue::Float(angle.to_degrees()), None);
            }
        }
        Ok(())
    }

    fn extend_misc(&mut self, info: RecordValue) -> Result<(), ImageError> {
        let mut fields = info.fields().iter().collect::<Vec<_>>();
        fields.sort_by_key(|field| field.name.to_ascii_uppercase());
        let mut cards_by_field = Vec::new();
        for field in fields {
            let name = field.name.to_ascii_uppercase();
            if name.is_empty() || !name.is_ascii() {
                continue;
            }
            let values = misc_values(&name, &field.value)?;
            if values.is_empty() {
                continue;
            }
            let shape = match &field.value {
                Value::Array(array) => array.shape().to_vec(),
                _ => Vec::new(),
            };
            cards_by_field.push((name, shape, values));
        }
        // casacore interleaves arrays of the same rank and element count,
        // naming matrix cards using the last array's row count in the run.
        for run in cards_by_field.chunk_by(|(_, first, values), (_, next, other)| {
            first.len() == next.len() && values.len() == other.len()
        }) {
            let shape = &run.last().expect("nonempty run").1;
            let mut fields = Vec::with_capacity(run.len());
            for (name, _, values) in run {
                let names = misc_keyword_names(name, shape, values.len())?;
                fields.push(names.into_iter().zip(values));
            }
            for _ in 0..run[0].2.len() {
                for cards in &mut fields {
                    let (mut key, value) = cards.next().expect("one card per value");
                    if key == "COMMENT"
                        && matches!(value, FitsValue::String(text) if text.is_empty())
                    {
                        key.clear();
                    }
                    self.0.push(HeaderCard {
                        keyword: FitsKeyword {
                            name: key,
                            value: value.clone(),
                            comment: None,
                        },
                        group: None,
                    });
                }
            }
        }
        Ok(())
    }

    /// The FITS header as consecutive 80-byte cards, with no block padding.
    pub fn card_string(&self) -> String {
        self.0
            .iter()
            .map(|card| render_card(&card.keyword))
            .collect()
    }

    /// The FITS header as a CASA record with the given entry names omitted.
    pub fn record(&self, exclude: &[String]) -> RecordValue {
        let mut grouped = BTreeMap::<String, Vec<FitsValue>>::new();
        for card in &self.0 {
            let keyword = &card.keyword;
            let name = card.group.unwrap_or(&keyword.name);
            let name = if name.is_empty() { "        " } else { name };
            grouped
                .entry(name.to_owned())
                .or_default()
                .push(keyword.value.clone());
        }
        let mut record = RecordValue::default();
        for (name, values) in grouped {
            if !exclude.contains(&name) {
                let axis_vector = matches!(
                    name.as_str(),
                    "NAXIS" | "CTYPE" | "CRVAL" | "CRPIX" | "CDELT"
                );
                let value = if !axis_vector && values.len() == 1 {
                    scalar(&name, &values[0]).into()
                } else {
                    Value::Array(array(&name, &values))
                };
                record.upsert(name, value);
            }
        }
        record
    }
}

fn linear_fallback(
    coords: &CoordinateSystem,
    rank: usize,
) -> Result<BTreeMap<String, FitsValue>, ImageError> {
    if coords.n_world_axes() != rank
        || (0..rank).any(|axis| coords.pixel_axis_to_world_axis(axis) != Some(axis))
    {
        return Err(header_creation_error());
    }
    let mut wcs = BTreeMap::new();
    for index in 0..coords.n_coordinates() {
        let coordinate = coords.coordinate(index);
        let (reference, increment, units) = match coordinate {
            CoordinateModel::Direction(direction) => {
                let (reference, increment) = direction.reference_and_increment_in_world_units();
                (reference, increment, direction.visible_world_units())
            }
            _ => (
                coordinate.reference_value(),
                coordinate.increment(),
                coordinate.axis_units(),
            ),
        };
        let names = coordinate.axis_names();
        let pixels = coordinate.reference_pixel();
        for (local, axis) in coords
            .coordinate_pixel_axes(index)
            .expect("existing coordinate")
            .into_iter()
            .enumerate()
        {
            let Some(axis) = axis else {
                continue;
            };
            let axis = axis + 1;
            let mut name = names[local].to_ascii_uppercase();
            let mut unit = units[local].clone();
            truncate_bytes(&mut name, 8);
            truncate_bytes(&mut unit, 8);
            for (key, value) in [
                ("CTYPE", FitsValue::String(name)),
                ("CRVAL", FitsValue::Float(reference[local])),
                ("CDELT", FitsValue::Float(increment[local])),
                ("CRPIX", FitsValue::Float(pixels[local] + 1.0)),
                ("CUNIT", FitsValue::String(unit)),
            ] {
                wcs.insert(format!("{key}{axis}"), value);
            }
        }
    }
    Ok(wcs)
}

fn normalize_wcs(
    coords: &CoordinateSystem,
    wcs: &mut BTreeMap<String, FitsValue>,
) -> Result<(), ImageError> {
    if !wcs.contains_key("RESTFRQ") {
        wcs.remove("SPECSYS");
    }
    for index in 0..coords.n_coordinates() {
        let coordinate = coords.coordinate(index);
        let axes = coords
            .coordinate_world_axes(index)
            .expect("existing coordinate");
        match coordinate {
            CoordinateModel::Direction(direction) => {
                let frame = direction.direction_ref();
                let (longitude, latitude) = match frame {
                    DirectionRef::GALACTIC => ("GLON", "GLAT"),
                    DirectionRef::SUPERGAL => ("SLON", "SLAT"),
                    DirectionRef::ECLIPTIC | DirectionRef::MECLIPTIC | DirectionRef::TECLIPTIC => {
                        ("ELON", "ELAT")
                    }
                    DirectionRef::J2000
                    | DirectionRef::JMEAN
                    | DirectionRef::APP
                    | DirectionRef::B1950
                    | DirectionRef::B1950Vla
                    | DirectionRef::BMEAN
                    | DirectionRef::BTRUE
                    | DirectionRef::ICRS
                    | DirectionRef::HADEC => ("RA--", "DEC-"),
                    _ => ("??LN", "??LT"),
                };
                let projection = direction.projection().name();
                for (axis, name) in axes.iter().zip([longitude, latitude]) {
                    let Some(axis) = axis else {
                        continue;
                    };
                    wcs.insert(
                        format!("CTYPE{}", axis + 1),
                        FitsValue::String(format!("{name}-{projection}")),
                    );
                }
                wcs.remove("EQUINOX");
                wcs.remove("RADESYS");
                let reference = match frame {
                    DirectionRef::J2000 => Some(("FK5", Some(2000.0))),
                    DirectionRef::B1950 => Some(("FK4", Some(1950.0))),
                    DirectionRef::B1950Vla => Some(("FK4", Some(1979.9))),
                    DirectionRef::ICRS => Some(("ICRS", None)),
                    _ => None,
                };
                if let Some((system, equinox)) = reference {
                    wcs.insert("RADESYS".into(), FitsValue::String(system.into()));
                    if let Some(equinox) = equinox {
                        wcs.insert("EQUINOX".into(), FitsValue::Float(equinox));
                    }
                }
            }
            CoordinateModel::Stokes(stokes) => {
                if let [Some(axis)] = axes.as_slice()
                    && let [Some(pixel)] = coords
                        .coordinate_pixel_axes(index)
                        .expect("existing coordinate")
                        .as_slice()
                {
                    wcs.extend(stokes_axis_keywords(stokes, axis + 1, pixel + 1)?);
                }
            }
            _ => {}
        }
    }
    for (key, value) in wcs.iter_mut() {
        if matches!(split_axis_keyword(key), Some(("CTYPE" | "CUNIT", _)))
            && let FitsValue::String(text) = value
        {
            truncate_bytes(text, 8);
        }
    }
    Ok(())
}

pub(super) fn wrapped_beam_position_angle(angle: f64) -> f64 {
    let angle = angle % 180.0;
    if angle > 90.0 {
        angle - 180.0
    } else if angle <= -90.0 {
        angle + 180.0
    } else {
        angle
    }
}

pub(super) fn stokes_axis_keywords(
    stokes: &StokesCoordinate,
    axis: usize,
    pixel: usize,
) -> Result<[(String, FitsValue); 3], ImageError> {
    let (reference, increment) =
        fits_stokes_axis(stokes.stokes()).ok_or_else(header_creation_error)?;
    Ok([
        (format!("CRVAL{axis}"), FitsValue::Float(reference)),
        (format!("CDELT{axis}"), FitsValue::Float(increment)),
        (format!("CRPIX{pixel}"), FitsValue::Float(1.0)),
    ])
}

pub(super) fn projection_parameters(
    coords: &CoordinateSystem,
    output_axis: impl Fn(usize) -> Option<usize>,
) -> Vec<(String, FitsValue)> {
    let mut result = Vec::new();
    for index in 0..coords.n_coordinates() {
        let axis = coords.coordinate(index);
        if let CoordinateModel::Direction(direction) = axis
            && let [_, Some(latitude)] = coords
                .coordinate_world_axes(index)
                .expect("existing coordinate")
                .as_slice()
            && let Some(latitude_axis) = output_axis(latitude + 1)
        {
            let projection = direction.projection();
            let params = projection.parameters();
            let start = projection.projection_type().fits_parameter_start();
            let count = match projection.projection_type() {
                ProjectionType::SIN => 2,
                ProjectionType::ZPN => params
                    .len()
                    .min(projection.projection_type().fits_parameter_defaults().len()),
                _ => params.len(),
            };
            for index in 0..count {
                result.push((
                    format!("PV{}_{}", latitude_axis, index + start),
                    FitsValue::Float(params.get(index).copied().unwrap_or(0.0)),
                ));
            }
        }
    }
    result
}

fn header_creation_error() -> ImageError {
    ImageError::InvalidMetadata("header creation failed: Error creating initial FITS header".into())
}

fn misc_keyword_names(
    name: &str,
    shape: &[usize],
    count: usize,
) -> Result<Vec<String>, ImageError> {
    match shape {
        [] => Ok(vec![if name.starts_with("COMMENT") {
            "COMMENT".into()
        } else if name.starts_with("HISTORY") {
            "HISTORY".into()
        } else {
            name[..name.len().min(8)].to_owned()
        }]),
        [length] if *length <= 999 && name.len() + length.to_string().len() <= 8 => {
            Ok((1..=count).map(|index| format!("{name}{index}")).collect())
        }
        [rows, cols] if name.len() <= 2 && *rows <= 99 && *cols <= 99 => {
            let width = if *rows > 9 { 2 } else { 1 };
            Ok((0..count)
                .map(|index| {
                    let row = index % rows + 1;
                    let col = index / rows + 1;
                    format!("{name}{row:0width$}_{col:0width$}")
                })
                .collect())
        }
        _ => Err(header_creation_error()),
    }
}

fn misc_values(name: &str, value: &Value) -> Result<Vec<FitsValue>, ImageError> {
    let card_integer = |value| FitsValue::Integer(i64::from(value as i32));
    macro_rules! array {
        ($values:expr, $variant:ident, $map:expr) => {
            $values
                .t()
                .iter()
                .map(|value| FitsValue::$variant($map(value)))
                .collect()
        };
    }
    let values = match value {
        Value::Scalar(value) => {
            let scalar = match value {
                ScalarValue::Bool(value) => FitsValue::Logical(*value),
                ScalarValue::UInt8(value) => FitsValue::Integer(i64::from(*value)),
                ScalarValue::UInt16(value) => FitsValue::Integer(i64::from(*value)),
                ScalarValue::UInt32(value) => card_integer(i64::from(*value)),
                ScalarValue::Int16(value) => FitsValue::Integer(i64::from(*value)),
                ScalarValue::Int32(value) => FitsValue::Integer(i64::from(*value)),
                ScalarValue::Int64(value) => card_integer(*value),
                ScalarValue::Float32(value) => FitsValue::Float(f64::from(*value)),
                ScalarValue::Float64(value) => FitsValue::Float(*value),
                ScalarValue::String(value) => {
                    let mut text = value.clone();
                    if name.starts_with("COMMENT") || name.starts_with("HISTORY") {
                        truncate_bytes(&mut text, 72);
                        return Ok(vec![FitsValue::String(text)]);
                    }
                    FitsValue::String(text)
                }
                ScalarValue::Complex32(_) | ScalarValue::Complex64(_) => {
                    return Err(header_creation_error());
                }
            };
            vec![scalar]
        }
        Value::Array(ArrayValue::Complex32(_) | ArrayValue::Complex64(_)) => Vec::new(),
        Value::Array(value) if value.shape().len() > 2 => return Err(header_creation_error()),
        Value::Array(value) => match value {
            ArrayValue::Bool(values) => array!(values, Logical, |value: &bool| *value),
            ArrayValue::UInt8(values) => array!(values, Integer, |value: &u8| i64::from(*value)),
            ArrayValue::UInt16(values) => array!(values, Integer, |value: &u16| i64::from(*value)),
            ArrayValue::UInt32(values) => {
                array!(values, Integer, |value: &u32| i64::from(*value as i32))
            }
            ArrayValue::Int16(values) => array!(values, Integer, |value: &i16| i64::from(*value)),
            ArrayValue::Int32(values) => array!(values, Integer, |value: &i32| i64::from(*value)),
            ArrayValue::Int64(values) => {
                array!(values, Integer, |value: &i64| i64::from(*value as i32))
            }
            ArrayValue::Float32(values) => array!(values, Float, |value: &f32| f64::from(*value)),
            ArrayValue::Float64(values) => array!(values, Float, |value: &f64| *value),
            ArrayValue::String(values) => array!(values, String, Clone::clone),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    };
    if values
        .iter()
        .any(|value| matches!(value, FitsValue::String(text) if text.len() > 68))
    {
        return Err(header_creation_error());
    }
    Ok(values)
}

fn group_name(name: &str) -> Option<&'static str> {
    if name == "NAXIS" {
        return Some("NAXIS");
    }
    if let Some((base, _)) = split_axis_keyword(name) {
        return (base != "CUNIT").then_some(base);
    }
    name.strip_prefix("NAXIS")
        .filter(|axis| !axis.is_empty() && axis.bytes().all(|ch| ch.is_ascii_digit()))
        .map(|_| "NAXIS")
}

fn padded_text(name: &str, value: &str) -> String {
    if matches!(name, "        " | "END" | "HISTORY" | "COMMENT") {
        value.to_owned()
    } else {
        let mut text = value.to_owned();
        text.extend(std::iter::repeat_n(' ', 8_usize.saturating_sub(text.len())));
        text
    }
}

fn scalar(name: &str, value: &FitsValue) -> ScalarValue {
    match value {
        FitsValue::String(value) => ScalarValue::String(padded_text(name, value)),
        FitsValue::Integer(value) => ScalarValue::Int64(*value),
        FitsValue::Float(value) => ScalarValue::Float64(*value),
        FitsValue::Logical(value) => ScalarValue::Bool(*value),
    }
}

fn array(name: &str, values: &[FitsValue]) -> ArrayValue {
    match values.first().expect("at least one FITS card in a group") {
        FitsValue::String(_) => ArrayValue::from_string_vec(
            values
                .iter()
                .filter_map(|value| match value {
                    FitsValue::String(text) => Some(padded_text(name, text)),
                    _ => None,
                })
                .collect(),
        ),
        FitsValue::Integer(_) => ArrayValue::Int64(
            ndarray::Array1::from_iter(values.iter().filter_map(|value| match value {
                FitsValue::Integer(integer) => Some(*integer),
                _ => None,
            }))
            .into_dyn(),
        ),
        FitsValue::Float(_) => ArrayValue::from_f64_vec(
            values
                .iter()
                .filter_map(|value| match value {
                    FitsValue::Float(float) => Some(*float),
                    _ => None,
                })
                .collect(),
        ),
        FitsValue::Logical(_) => ArrayValue::Bool(
            ndarray::Array1::from_iter(values.iter().filter_map(|value| match value {
                FitsValue::Logical(flag) => Some(*flag),
                _ => None,
            }))
            .into_dyn(),
        ),
    }
}

fn render_card(keyword: &FitsKeyword) -> String {
    let name = &keyword.name;
    let mut card = match &keyword.value {
        FitsValue::String(text) if matches!(name.as_str(), "" | "END" | "HISTORY" | "COMMENT") => {
            format!("{name:<8}{text}")
        }
        value => {
            let field = match value {
                FitsValue::String(text) => {
                    format!("'{}'", padded_text(name, text).replace('\'', "''"))
                }
                FitsValue::Integer(integer) => format!("{integer:>20}"),
                FitsValue::Logical(flag) => format!("{:>20}", if *flag { 'T' } else { 'F' }),
                FitsValue::Float(number) => {
                    let token = format!("{number:.12E}");
                    let field = token.split_once('E').map_or_else(
                        || token.clone(),
                        |(mantissa, exponent)| {
                            format!(
                                "{mantissa}E{:+03}",
                                exponent.parse::<i32>().expect("decimal exponent")
                            )
                        },
                    );
                    format!("{field:>20}")
                }
            };
            format!("{name:<8}= {field}")
        }
    };
    if let Some(comment) = &keyword.comment {
        card.extend(std::iter::repeat_n(
            ' ',
            30_usize.saturating_sub(card.len()),
        ));
        card.push_str(" /");
        card.push_str(comment);
    }
    truncate_bytes(&mut card, 80);
    card.extend(std::iter::repeat_n(' ', 80 - card.len()));
    card
}

fn truncate_bytes(text: &mut String, limit: usize) {
    if text.len() > limit {
        let boundary = (0..=limit)
            .rev()
            .find(|index| text.is_char_boundary(*index))
            .expect("zero is a boundary");
        text.truncate(boundary);
    }
}
