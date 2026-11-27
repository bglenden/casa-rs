// SPDX-License-Identifier: LGPL-3.0-or-later
//! The serialized `FTMachine` a virtual model entry holds, field for field
//! as CASA 6.7.6 writes it: `refim::GridFT::toRecord` (over
//! `FTMachine::toRecord` and `storeMovingSourceState`) and
//! `MultiTermFTNew::toRecord`, measured in the records `tclean` left in
//! `refim_twochan.ms` and `refim_point.ms`.
//!
//! CASA reads the record back with `GridFT(const RecordInterface&)`, which
//! takes the grid from the image `diskimage` names and recomputes its
//! sizes, scales and channel and polarization maps from that image and
//! each visibility buffer (`GridFT::init`, `FTMachine::initMaps`). What it
//! keeps is the phase centre (`mimage_rec`), the observatory
//! (`mlocation_rec`) and the image's coordinates; the rest is the state the
//! machine had after degridding the first buffer, which casa-rs writes as
//! CASA would for its selection's first buffer.

use std::path::Path;

use casa_core::measures::direction::MDirection;
use casa_core::{ArrayValue, Complex32, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn};

/// What CASA's machine knew of the first visibility buffer it degridded:
/// its field, the selected channels' LSRK frequencies, their image
/// channels, its correlations and their grid polarizations.
#[derive(Clone, Debug, PartialEq)]
pub struct FirstBuffer {
    pub field: i32,
    pub frequencies_lsrk_hz: Vec<f64>,
    pub channel_map: Vec<i32>,
    /// The correlations' `CORR_TYPE` codes.
    pub correlations: Vec<i32>,
    pub polarization_map: Vec<i32>,
}

/// One `GridFT` of a virtual model: where its grid image is and what CASA
/// recorded beside it.
pub struct GridFt<'a> {
    pub disk_image: &'a Path,
    /// The padded grid `[nx, ny]`, the image's shape.
    pub grid: [usize; 2],
    /// Grid polarizations and image channels.
    pub polarizations: usize,
    pub image_frequencies_hz: &'a [f64],
    /// Signed radians per pixel.
    pub increment_rad: [f64; 2],
    /// The image's phase centre and its frame (`J2000`, ...).
    pub phase_centre_rad: [f64; 2],
    pub direction_frame: &'a str,
    /// The observatory, ITRF longitude, latitude (geocentric) and radius.
    pub observatory: [f64; 3],
    /// The image's spectral coordinate record.
    pub spectral: RecordValue,
    pub buffer: &'a FirstBuffer,
    /// `InterpolateArray1D`'s method: nearest 0, linear 1, cubic 2.
    pub interpolation: i32,
    /// Whether the machine converted its buffers' frequencies itself: a
    /// Taylor term's machine, which `MultiTermFTNew` drives, records not.
    pub frequency_frame_valid: bool,
}

impl GridFt<'_> {
    /// The record, `GridFT::toRecord` with its image on disk: CASA writes
    /// padding 1 since the image is the padded grid.
    pub fn record(&self) -> RecordValue {
        let [nx, ny] = self.grid;
        let channels = self.image_frequencies_hz.len();
        let buffer = self.buffer;
        let used = buffer
            .polarization_map
            .iter()
            .filter(|&&pol| pol >= 0)
            .count();
        let mut record = RecordValue::default();
        let mut define = |name: &str, value: Value| record.push(RecordField::new(name, value));
        define("name", text("GridFT"));
        define("diskimage", text(&self.disk_image.display().to_string()));
        define("nantenna", Value::Scalar(ScalarValue::UInt32(0)));
        define("distance", double(0.0));
        define("lastfieldid", int(buffer.field));
        define("lastmsid", int(0));
        define("tangentspecified", boolean(false));
        define(
            "mtangent_rec",
            direction([0.0, std::f64::consts::FRAC_PI_2], "J2000"),
        );
        define(
            "mimage_rec",
            direction(self.phase_centre_rad, self.direction_frame),
        );
        define("nx", int(nx as i32));
        define("ny", int(ny as i32));
        define("npol", int(self.polarizations as i32));
        define("nchan", int(channels as i32));
        define("nvischan", int(buffer.frequencies_lsrk_hz.len() as i32));
        define("nvispol", int(buffer.correlations.len() as i32));
        define("douvwrotation", boolean(true));
        define("freqinterpmethod", int(self.interpolation));
        define("spwchanselflag", ints(&[0, 0, 0], &[]));
        define("freqframevalid", boolean(self.frequency_frame_valid));
        define("imagefreq", doubles(self.image_frequencies_hz));
        define("lsrfreq", doubles(&buffer.frequencies_lsrk_hz));
        // A continuum machine interpolates onto the buffer's own
        // frequencies; a cube's onto its image channels.
        define(
            "interpvisfreq",
            doubles(if channels == 1 {
                &buffer.frequencies_lsrk_hz
            } else {
                self.image_frequencies_hz
            }),
        );
        define(
            "chanmap",
            ints(&[buffer.channel_map.len()], &buffer.channel_map),
        );
        define(
            "polmap",
            ints(&[buffer.polarization_map.len()], &buffer.polarization_map),
        );
        define("nvischanmulti", ints(&[0], &[]));
        define("mlocation_rec", position(self.observatory));
        define("spectralcoord", Value::Record(self.spectral.clone()));
        define(
            "movingdir_rec",
            direction([0.0, std::f64::consts::FRAC_PI_2], "J2000"),
        );
        define("fixmovingsource", boolean(false));
        define(
            "firstmovingdir_rec",
            direction([0.0, std::f64::consts::FRAC_PI_2], "J2000"),
        );
        define("pointingdircol", text("DIRECTION"));
        define("usedoublegrid", boolean(true));
        define(
            "cfstokes",
            ints(&[buffer.correlations.len()], &buffer.correlations),
        );
        define("polinuse", int(used as i32));
        define("tovis", boolean(true));
        define(
            "sumweight",
            Value::Array(ArrayValue::Float64(ArrayD::zeros(IxDyn(&[
                self.polarizations,
                channels,
            ])))),
        );
        define("numthreads", int(-1));
        define("phasecentertime", double(-1.0));
        define(
            "cachesize",
            Value::Scalar(ScalarValue::Int64(1_000_000_000)),
        );
        define("tilesize", int(16));
        define("convtype", text("SF"));
        define(
            "uvscale",
            doubles(&[
                nx as f64 * self.increment_rad[0],
                ny as f64 * self.increment_rad[1],
            ]),
        );
        define("uvoffset", doubles(&[(nx / 2) as f64, (ny / 2) as f64]));
        define("istiled", boolean(false));
        define("maxabsdata", Value::Scalar(ScalarValue::Float32(0.0)));
        define("centerloc", ints(&[4], &[0, 0, 0, 0]));
        define("offsetloc", ints(&[4], &[0, 0, 0, 0]));
        define("padding", Value::Scalar(ScalarValue::Float32(1.0)));
        define("usezero", boolean(false));
        define("nopadding", boolean(false));
        record
    }
}

/// Set the common tangent point used to project every facet of a model.
pub fn set_grid_tangent(record: &mut RecordValue, tangent: &MDirection) {
    let (lon, lat) = tangent.as_angles();
    record.upsert("tangentspecified", boolean(true));
    record.upsert(
        "mtangent_rec",
        direction([lon, lat], tangent.refer().as_str()),
    );
}

/// Save the legacy moving source and its initially observed direction.
pub fn set_grid_motion(
    record: &mut RecordValue,
    source: &MDirection,
    anchor: &MDirection,
    location: &casa_core::measures::position::MPosition,
) {
    let measure = |value: &MDirection| {
        let (longitude, latitude) = value.as_angles();
        direction([longitude, latitude], value.refer().as_str())
    };
    record.upsert("movingdir_rec", measure(source));
    record.upsert("firstmovingdir_rec", measure(anchor));
    record.upsert("fixmovingsource", boolean(true));
    let (lon, lat, radius) = location.as_spherical();
    record.upsert("mlocation_rec", position([lon, lat, radius]));
}

/// Turn the common Fourier record into the legacy `MosaicFT` state.
/// CASA's reader constructs the convolution from the next buffer when
/// the `HetArrayConvFunc` cache is empty.
pub fn legacy_mosaic(record: &mut RecordValue, stokes: &str, beam_class: i32, sampling: u16) {
    legacy_array_mosaic(record, stokes, beam_class, "", sampling);
}

/// Record an externally supplied table of per-antenna image voltage patterns.
pub fn legacy_image_mosaic(record: &mut RecordValue, stokes: &str, table: &str, sampling: u16) {
    legacy_array_mosaic(record, stokes, 8, table, sampling);
}

fn legacy_array_mosaic(
    record: &mut RecordValue,
    stokes: &str,
    beam_class: i32,
    table: &str,
    sampling: u16,
) {
    let beam = RecordValue::new(vec![
        RecordField::new("name", text("HetArrayConvFunc")),
        RecordField::new("ndefined", int(0)),
        RecordField::new("actualconvindex", int(0)),
        RecordField::new(
            "donemainconv",
            Value::Array(ArrayValue::Bool(ArrayD::from_elem(IxDyn(&[0]), false))),
        ),
        RecordField::new("vptable", text(table)),
        RecordField::new("pbclass", int(beam_class)),
    ]);
    legacy_mosaic_record(record, stokes, sampling, beam);
}

/// Record a legacy MosaicFT common beam. The C++ reader rebuilds the
/// convolution when it initializes the next visibility buffer.
pub fn legacy_common_mosaic(
    record: &mut RecordValue,
    stokes: &str,
    telescope: &str,
    sampling: u16,
) {
    let beam = RecordValue::new(vec![
        RecordField::new("name", text("SimplePBConvFunc")),
        RecordField::new("numconv", int(0)),
        RecordField::new("actualconvindex", int(-1)),
        RecordField::new(
            "donemainconv",
            Value::Array(ArrayValue::Bool(ArrayD::from_elem(IxDyn(&[0]), false))),
        ),
        RecordField::new("pbclass", int(1)),
    ]);
    record.upsert("telescope", text(telescope));
    legacy_mosaic_record(record, stokes, sampling, beam);
}

fn legacy_mosaic_record(record: &mut RecordValue, stokes: &str, sampling: u16, beam: RecordValue) {
    record.upsert("name", text("MosaicFT"));
    record.upsert("stokes", text(stokes));
    record.upsert("usezero", boolean(true));
    record.upsert("convsampling", int(i32::from(sampling)));
    record.upsert("convsize", int(0));
    record.upsert("convsupport", int(0));
    for name in ["convfunc", "weightconvfunc"] {
        record.upsert(
            name,
            Value::Array(ArrayValue::Complex32(ArrayD::<Complex32>::zeros(IxDyn(
                &[0; 5],
            )))),
        );
    }
    for name in ["convsupportplanes", "convsizeplanes", "convRowMap"] {
        record.upsert(name, ints(&[0], &[]));
    }
    record.upsert("pbconvfunc", Value::Record(beam));
}

/// `MultiTermFTNew::toRecord` of a prediction machine: its Taylor terms'
/// `GridFT` records, each naming its own grid image.
pub fn multi_term(terms: Vec<RecordValue>, reference_hz: f64) -> RecordValue {
    let count = terms.len();
    let mut record = RecordValue::new(vec![
        RecordField::new("name", text("MultiTermFTNew")),
        RecordField::new("nterms", Value::Scalar(ScalarValue::UInt32(count as u32))),
        RecordField::new("reffreq", double(reference_hz)),
        RecordField::new("machinename", text("MultiTermFTNew")),
        RecordField::new(
            "psfnterms",
            Value::Scalar(ScalarValue::UInt32(count as u32)),
        ),
        RecordField::new("numfts", int(count as i32)),
    ]);
    for (term, machine) in terms.into_iter().enumerate() {
        record.push(RecordField::new(
            format!("subftm_{term}"),
            Value::Record(machine),
        ));
    }
    record
}

/// The fields of CASA's spectral coordinate record, in the order
/// `SpectralCoordinate::save` writes them; `conversion` follows.
const SPECTRAL_FIELDS: [&str; 12] = [
    "version",
    "system",
    "restfreq",
    "restfreqs",
    "velType",
    "nativeType",
    "velUnit",
    "waveUnit",
    "formatUnit",
    "wcs",
    "unit",
    "name",
];

/// The legacy imager emits a separate machine record for each Taylor term.
pub fn legacy_multi_term(
    grid: RecordValue,
    terms: i32,
    term: i32,
    reference_hz: f64,
) -> RecordValue {
    let mut record = grid.clone();
    record.remove("diskimage");
    record.upsert("name", text("MultiTermFT"));
    let name = grid.get("name").cloned().unwrap_or_else(|| text("GridFT"));
    record.upsert("subftm", Value::Record(grid));
    record.upsert("subftname", name);
    record.upsert("nterms", int(terms));
    record.upsert("thisterm", int(term));
    record.upsert("reffreq", double(reference_hz));
    record.upsert("dbg", boolean(false));
    record.upsert("dotime", boolean(false));
    for field in ["time_get", "time_put", "time_res"] {
        record.upsert(field, double(0.0));
    }
    record
}

/// A spectral coordinate's record as CASA's `SpectralCoordinate::save`
/// leaves it in a machine record: its fields in CASA's order, a linear
/// axis's WCS in `crval, crpix, cdelt, pc, ctype` with the type as wcslib's
/// nine-character `FREQ`, and, for a coordinate with no conversion layer,
/// CASA's default one (the default J2000 direction, ITRF origin and LAST
/// epoch, to the coordinate's own system).
pub fn casa_spectral(record: &RecordValue) -> RecordValue {
    let mut out = RecordValue::default();
    for name in SPECTRAL_FIELDS {
        let Some(value) = record.get(name) else {
            continue;
        };
        let value = match (name, value) {
            ("wcs", Value::Record(wcs)) => Value::Record(casa_wcs(wcs)),
            _ => value.clone(),
        };
        out.push(RecordField::new(name, value));
    }
    for field in record.fields() {
        if !SPECTRAL_FIELDS.contains(&field.name.as_str()) && field.name != "conversion" {
            out.push(field.clone());
        }
    }
    let conversion = match record.get("conversion") {
        Some(Value::Record(conversion)) => {
            let mut ordered = RecordValue::default();
            for name in ["direction", "position", "epoch", "system"] {
                if let Some(value) = conversion.get(name) {
                    ordered.push(RecordField::new(name, value.clone()));
                }
            }
            ordered
        }
        _ => {
            let system = record
                .get("system")
                .cloned()
                .unwrap_or_else(|| text("LSRK"));
            RecordValue::new(vec![
                RecordField::new(
                    "direction",
                    direction([0.0, std::f64::consts::FRAC_PI_2], "J2000"),
                ),
                RecordField::new("position", position([0.0, 0.0, 0.0])),
                RecordField::new(
                    "epoch",
                    Value::Record(RecordValue::new(vec![
                        RecordField::new("type", text("epoch")),
                        RecordField::new("refer", text("LAST")),
                        RecordField::new("m0", quantity(0.0, "d", false)),
                    ])),
                ),
                RecordField::new("system", system),
            ])
        }
    };
    out.push(RecordField::new("conversion", Value::Record(conversion)));
    out
}

/// A linear spectral axis's WCS in CASA's order, its `FREQ` type padded
/// with NULs to wcslib's nine characters as CASA records it.
fn casa_wcs(wcs: &RecordValue) -> RecordValue {
    let mut out = RecordValue::default();
    for name in ["crval", "crpix", "cdelt", "pc", "ctype"] {
        if let Some(value) = wcs.get(name) {
            let value = match value {
                Value::Scalar(ScalarValue::String(kind)) if kind == "FREQ" => {
                    text("FREQ\0\0\0\0\0")
                }
                value => value.clone(),
            };
            out.push(RecordField::new(name, value));
        }
    }
    for field in wcs.fields() {
        if out.get(&field.name).is_none() {
            out.push(field.clone());
        }
    }
    out
}

fn text(value: &str) -> Value {
    Value::Scalar(ScalarValue::String(value.to_string()))
}

fn int(value: i32) -> Value {
    Value::Scalar(ScalarValue::Int32(value))
}

fn double(value: f64) -> Value {
    Value::Scalar(ScalarValue::Float64(value))
}

fn boolean(value: bool) -> Value {
    Value::Scalar(ScalarValue::Bool(value))
}

fn doubles(values: &[f64]) -> Value {
    Value::Array(ArrayValue::Float64(
        ArrayD::from_shape_vec(IxDyn(&[values.len()]), values.to_vec()).expect("one axis"),
    ))
}

fn ints(shape: &[usize], values: &[i32]) -> Value {
    Value::Array(ArrayValue::Int32(
        ArrayD::from_shape_vec(IxDyn(shape), values.to_vec()).expect("the shape holds the values"),
    ))
}

/// A quantity `{value, unit}` or, as CASA writes the first axis of a
/// measure, `{unit, value}`.
fn quantity(value: f64, unit: &str, unit_first: bool) -> Value {
    let value = RecordField::new("value", double(value));
    let unit = RecordField::new("unit", text(unit));
    Value::Record(RecordValue::new(if unit_first {
        vec![unit, value]
    } else {
        vec![value, unit]
    }))
}

/// A direction measure as `MeasureHolder::toRecord` leaves it in CASA's
/// machine records: `type`, `refer`, then `m1` and `m0`, the latitude's
/// value before its unit and the longitude's unit before its value. The
/// longitude is CASA's `MVDirection` angle, the `atan2` of its direction
/// cosines, in (−π, π].
fn direction([longitude, latitude]: [f64; 2], frame: &str) -> Value {
    let cosine = latitude.cos();
    let longitude = (cosine * longitude.sin()).atan2(cosine * longitude.cos());
    Value::Record(RecordValue::new(vec![
        RecordField::new("type", text("direction")),
        RecordField::new("refer", text(frame)),
        RecordField::new("m1", quantity(latitude, "rad", false)),
        RecordField::new("m0", quantity(longitude, "rad", true)),
    ]))
}

/// An ITRF position measure, as CASA leaves it: `m2` (the radius), `m1`
/// and `m0`.
fn position([longitude, latitude, radius]: [f64; 3]) -> Value {
    Value::Record(RecordValue::new(vec![
        RecordField::new("type", text("position")),
        RecordField::new("refer", text("ITRF")),
        RecordField::new("m2", quantity(radius, "m", false)),
        RecordField::new("m1", quantity(latitude, "rad", true)),
        RecordField::new("m0", quantity(longitude, "rad", true)),
    ]))
}

#[cfg(test)]
#[path = "ft_record_tests.rs"]
mod tests;
