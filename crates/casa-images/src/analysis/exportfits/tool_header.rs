// SPDX-License-Identifier: LGPL-3.0-or-later
//! The Python image tool's metadata-only FITS header view.

use std::collections::BTreeMap;

use casa_coordinates::{
    CoordinateModel, FitsKeyword, FitsValue, ProjectionType, fits::try_to_fits_header,
};
use casa_types::measures::position::PositionRef;
use casa_types::quanta::format::{FITS, format_time};
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};

use crate::image::ImageHistoryEntry;
use crate::{ImageError, ImageInterface, ImageType};

use super::{ExportAxisPlan, build_export_keywords, split_axis_keyword, visit_history_cards};

/// Ordered FITS cards and CASA's grouped dictionary representation of them.
pub struct ImageFitsHeader(Vec<FitsKeyword>);

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
    let coords = image.coordinates();
    let plan = ExportAxisPlan::build(coords, shape, false, false, false)?;
    let base = try_to_fits_header(coords, shape)?;
    let mut wcs: BTreeMap<String, FitsValue> =
        build_export_keywords(coords, &base, &plan, true, true)?
            .into_iter()
            .collect();
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
    header.image_metadata(image)?;
    header.axes(shape.len(), &mut wcs, projection_parameters(coords));
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
    header.observation(image, &mut wcs)?;
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
    if !history.is_empty() {
        visit_history_cards(history, |text| {
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

impl ImageFitsHeader {
    fn add(&mut self, name: impl Into<String>, value: FitsValue, comment: Option<&str>) {
        self.0.push(FitsKeyword {
            name: name.into(),
            value,
            comment: comment.map(str::to_owned),
        });
    }

    fn text(&mut self, name: impl Into<String>, text: impl Into<String>) {
        self.add(name, FitsValue::String(text.into()), None);
    }

    fn integer(&mut self, name: impl Into<String>, number: i64) {
        self.add(name, FitsValue::Integer(number), None);
    }

    fn image_metadata(&mut self, image: &dyn ImageInterface<f32>) -> Result<(), ImageError> {
        let info = image.image_info()?;
        if let Some(beam) = info.beam_set.single_beam().filter(|beam| !beam.is_null()) {
            self.add("BMAJ", FitsValue::Float(beam.major_in("deg")?), None);
            self.add("BMIN", FitsValue::Float(beam.minor_in("deg")?), None);
            let mut angle = beam.position_angle_in("deg")? % 180.0;
            if angle > 90.0 {
                angle -= 180.0;
            } else if angle <= -90.0 {
                angle += 180.0;
            }
            self.add("BPA", FitsValue::Float(angle), None);
        } else if info.beam_set.is_multi() {
            self.add(
                "CASAMBM",
                FitsValue::Logical(true),
                Some("CASA multiple BEAMS table present"),
            );
        }
        let image_type = if info.image_type == ImageType::Undefined {
            ImageType::Intensity
        } else {
            info.image_type
        };
        self.text("BTYPE", image_type.to_string());
        self.text("OBJECT", info.object_name);
        self.add(
            "BUNIT",
            FitsValue::String(image.units().to_owned()),
            Some("Brightness (pixel) unit"),
        );
        Ok(())
    }

    fn axes(
        &mut self,
        rank: usize,
        wcs: &mut BTreeMap<String, FitsValue>,
        projection: Vec<(String, FitsValue)>,
    ) {
        for keyword in ["EQUINOX", "RADESYS", "LONPOLE", "LATPOLE"] {
            if let Some(value) = wcs.remove(keyword) {
                self.add(keyword, value, None);
            }
        }
        for col in 1..=rank {
            for row in 1..=rank {
                let name = format!("PC{row}_{col}");
                let value = wcs
                    .remove(&name)
                    .unwrap_or(FitsValue::Float(f64::from(row == col)));
                self.add(name, value, None);
            }
        }
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

    fn observation(
        &mut self,
        image: &dyn ImageInterface<f32>,
        wcs: &mut BTreeMap<String, FitsValue>,
    ) -> Result<(), ImageError> {
        for keyword in ["TELESCOP", "OBSERVER", "DATE-OBS", "TIMESYS"] {
            if let Some(value) = wcs.remove(keyword) {
                self.add(keyword, value, None);
            }
        }
        let obs = image.coordinates().obs_info();
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
            let values = misc_values(&field.value)?;
            if values.is_empty() {
                continue;
            }
            let shape = match &field.value {
                Value::Array(array) => array.shape().to_vec(),
                _ => Vec::new(),
            };
            let names = misc_keyword_names(&name, &shape, values.len())?;
            cards_by_field.push((shape, names.into_iter().zip(values).collect::<Vec<_>>()));
        }
        // casacore interleaves adjacent arrays with the same shape.
        for run in cards_by_field.chunk_by(|(first, _), (next, _)| first == next) {
            for index in 0..run[0].1.len() {
                for (_, cards) in run {
                    let (name, value) = &cards[index];
                    self.add(name, value.clone(), None);
                }
            }
        }
        Ok(())
    }

    /// The FITS header as consecutive 80-byte cards, with no block padding.
    pub fn card_string(&self) -> String {
        self.0.iter().map(render_card).collect()
    }

    /// The FITS header as a CASA record with the given entry names omitted.
    pub fn record(&self, exclude: &[String]) -> RecordValue {
        let mut grouped = BTreeMap::<String, Vec<FitsValue>>::new();
        for keyword in &self.0 {
            let name = group_name(&keyword.name).unwrap_or(&keyword.name);
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

fn projection_parameters(coords: &casa_coordinates::CoordinateSystem) -> Vec<(String, FitsValue)> {
    let mut result = Vec::new();
    let mut offset = 0;
    for index in 0..coords.n_coordinates() {
        let axis = coords.coordinate(index);
        if let CoordinateModel::Direction(direction) = axis {
            let projection = direction.projection();
            let params = projection.parameters();
            let count = if projection.projection_type() == ProjectionType::SIN {
                2
            } else {
                params.len()
            };
            for index in 0..count {
                result.push((
                    format!("PV{}_{}", offset + 2, index + 1),
                    FitsValue::Float(params.get(index).copied().unwrap_or(0.0)),
                ));
            }
        }
        offset += axis.n_pixel_axes();
    }
    result
}

fn unsupported_misc() -> ImageError {
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
        _ => Err(unsupported_misc()),
    }
}

fn misc_values(value: &Value) -> Result<Vec<FitsValue>, ImageError> {
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
                ScalarValue::String(value) => FitsValue::String(value.clone()),
                ScalarValue::Complex32(_) | ScalarValue::Complex64(_) => {
                    return Err(unsupported_misc());
                }
            };
            vec![scalar]
        }
        Value::Array(ArrayValue::Complex32(_) | ArrayValue::Complex64(_)) => Vec::new(),
        Value::Array(value) if value.shape().len() > 2 => return Err(unsupported_misc()),
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
        return Err(unsupported_misc());
    }
    Ok(values)
}

fn group_name(name: &str) -> Option<&str> {
    if matches!(name, "COMMENT" | "HISTORY" | "NAXIS") {
        return Some(name);
    }
    if let Some((base, _)) = split_axis_keyword(name) {
        return (base != "CUNIT").then_some(base);
    }
    name.strip_prefix("NAXIS")
        .filter(|axis| !axis.is_empty() && axis.bytes().all(|ch| ch.is_ascii_digit()))
        .map(|_| "NAXIS")
}

fn padded_text(name: &str, value: &str) -> String {
    if matches!(name, "END" | "HISTORY" | "COMMENT") {
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
        FitsValue::String(text) if matches!(name.as_str(), "END" | "HISTORY" | "COMMENT") => {
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
    if card.len() > 80 {
        let length = (0..=80)
            .rev()
            .find(|index| card.is_char_boundary(*index))
            .expect("zero is a boundary");
        card.truncate(length);
    }
    card.extend(std::iter::repeat_n(' ', 80 - card.len()));
    card
}
