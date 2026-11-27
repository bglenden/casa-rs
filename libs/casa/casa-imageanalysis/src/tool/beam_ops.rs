// SPDX-License-Identifier: LGPL-3.0-or-later

//! Restoring-beam mutation, rotation, and Gaussian deconvolution operations.

use std::path::Path;

use casa_coordinates::CoordinateType;
use casa_core::quanta::{Quantity, Unit, casacore};
use casa_core::{RecordField, RecordValue, ScalarValue, Value};
use casa_images::{
    BeamComponent, GaussianBeam, ImageBeamSet, ImageInfo, ImagePixelType, StoredGaussianBeam,
    wrap_position_angle,
};
use casa_tables::LockType;

use super::attachment::AttachedImage;
use super::beam_view::BeamView;
use super::beams::{
    axis_length, beam_for_units, image_info, image_info_record, image_info_with_beams,
    parse_beam_record, plane_axes, stored_beam_record, subrecord, validate_beam_indices,
};
use super::error::{ImageToolError, ImageToolResult, ToolLogLine};
use super::handle::{ImageHandle, Storage, with_pixels};

/// Parameters for `set_restoring_beam`.
#[derive(Debug, Clone)]
pub struct SetBeamRequest {
    /// Explicit major axis as `(value, unit)`, if overriding.
    pub major: Option<(f64, String)>,
    /// Explicit minor axis as `(value, unit)`, if overriding.
    pub minor: Option<(f64, String)>,
    /// Explicit position angle as `(value, unit)`, if overriding.
    pub pa: Option<(f64, String)>,
    /// Pre-parsed beam record, if supplied via `beam={...}`.
    pub beam_record: Option<RecordValue>,
    /// Remove all beams if `true`.
    pub remove: bool,
    /// Optional log toggle.
    pub log: bool,
    /// Channel index (`< 0` means unset).
    pub channel: isize,
    /// Polarization index (`< 0` means unset).
    pub polarization: isize,
}

/// Result of deconvolving a component Gaussian from a restoring beam.
#[derive(Debug, Clone)]
pub struct DeconvolveBeamResult {
    /// Intrinsic (deconvolved) source Gaussian beam.
    pub fit: GaussianBeam,
    /// `true` when the component is unresolved (point-source solution).
    pub point: bool,
    /// Log lines produced during deconvolution.
    pub logs: Vec<ToolLogLine>,
}

/// Sets, replaces, or removes the restoring beam(s) on `attached`.
pub fn set_restoring_beam(
    attached: &AttachedImage,
    request: &SetBeamRequest,
    record_history: bool,
    history_command: Option<String>,
) -> ImageToolResult<()> {
    let mut image = attached.lock();
    attached.require_write_lock(&mut image, "setrestoringbeam")?;

    let mut beam_view = attached.beam_view();
    let current = beam_view
        .beam_set()
        .unwrap_or_else(|_| ImageBeamSet::empty());

    let next_beams = if request.remove {
        ImageBeamSet::empty()
    } else {
        let base_beam = if let Some(record) = &request.beam_record {
            parse_beam_record(record)?
        } else if current.is_single() {
            current.single_beam().unwrap_or_default()
        } else if !current.is_empty() && request.channel >= 0 {
            let (ch, pol) = validate_beam_indices(
                image.shape(),
                image.coordinates(),
                request.channel,
                request.polarization,
                false,
            )?;
            let c = ch.unwrap_or(0).min(current.n_channels().saturating_sub(1));
            let p = pol.unwrap_or(0).min(current.n_stokes().saturating_sub(1));
            *current.beam(c, p)
        } else {
            GaussianBeam::default()
        };

        let updated = build_beam_from_parts(base_beam, request)?;
        if request.channel < 0 && request.polarization < 0 {
            ImageBeamSet::new(updated)
        } else {
            let (ch, pol) = validate_beam_indices(
                image.shape(),
                image.coordinates(),
                request.channel,
                request.polarization,
                false,
            )?;
            let shape = image.shape();
            let coords = image.coordinates();
            let n_chan = coords
                .find_coordinate(CoordinateType::Spectral)
                .map_or(1, |ax| shape[ax]);
            let n_stokes = coords
                .find_coordinate(CoordinateType::Stokes)
                .map_or(1, |ax| shape[ax]);
            let mut next = if current.is_multi()
                && current.n_channels() == n_chan
                && current.n_stokes() == n_stokes
            {
                current
            } else {
                let fill = current.single_beam().unwrap_or(updated);
                ImageBeamSet::with_shape(n_chan, n_stokes, fill)
            };
            let chan_range: Vec<usize> = match ch {
                Some(c) => vec![c],
                None => (0..n_chan).collect(),
            };
            let stokes_range: Vec<usize> = match pol {
                Some(p) => vec![p],
                None => (0..n_stokes).collect(),
            };
            for c in chan_range {
                for &p in &stokes_range {
                    next.set_beam(Some(c), Some(p), updated)?;
                }
            }
            next
        }
    };

    let info = image_info_with_beams(&image, next_beams)?;
    let info_record = info.to_record();
    image.set_image_info_record(info_record.clone())?;
    beam_view.capture(&info_record);
    if record_history && let Some(command) = history_command {
        image.record_history("setrestoringbeam", command)?;
    }
    image.save()?;
    Ok(())
}

/// Replaces all beams in `attached` using a parsed [`ImageBeamSet`].
pub fn replace_beam_set(
    attached: &AttachedImage,
    beam_set: ImageBeamSet,
    record_history: bool,
    history_method: &str,
    history_command: Option<String>,
) -> ImageToolResult<()> {
    let mut image = attached.lock();
    attached.require_write_lock(&mut image, history_method)?;
    let info = image_info_with_beams(&image, beam_set)?;
    let record = info.to_record();
    image.set_image_info_record(record.clone())?;
    attached.beam_view().capture(&record);
    if record_history && let Some(command) = history_command {
        image.record_history(history_method, command)?;
    }
    image.save()?;
    Ok(())
}

/// Rotates all restoring beams on `attached` counter-clockwise by `angle_deg` degrees.
///
/// Matches CASA's handling on beamless images by accumulating `empty_angle_degrees`.
pub fn rotate_restoring_beam(
    attached: &AttachedImage,
    angle_deg: f64,
    record_history: bool,
    history_command: Option<String>,
) -> ImageToolResult<()> {
    let mut image = attached.lock();
    attached.require_write_lock(&mut image, "rotatebeam")?;
    let mut beam_view = attached.beam_view();
    let beams = beam_view.beam_set().map_err(|err| err.to_string())?;
    if beams.is_empty() {
        beam_view.empty_angle_degrees += angle_deg;
        return Ok(());
    }

    let mut grid = Vec::with_capacity(beams.n_channels());
    for chan in 0..beams.n_channels() {
        let mut row = Vec::with_capacity(beams.n_stokes());
        for stokes in 0..beams.n_stokes() {
            let beam = *beams.beam(chan, stokes);
            let pa_deg = beam.position_angle_in("deg")? + angle_deg;
            let rotated =
                GaussianBeam::new(beam.major, beam.minor, wrap_pa_rad(pa_deg.to_radians()));
            row.push(rotated);
        }
        grid.push(row);
    }
    let next = if let Some(single) = beams.single_beam() {
        let pa_deg = single.position_angle_in("deg")? + angle_deg;
        ImageBeamSet::new(GaussianBeam::new(
            single.major,
            single.minor,
            wrap_pa_rad(pa_deg.to_radians()),
        ))
    } else {
        ImageBeamSet::from_grid(grid)
    };
    let info = image_info_with_beams(&image, next)?;
    let record = info.to_record();
    image.set_image_info_record(record.clone())?;
    beam_view.capture(&record);
    if record_history && let Some(command) = history_command {
        image.record_history("rotatebeam", command)?;
    }
    image.save()?;
    Ok(())
}

/// Deconvolves a source Gaussian from a beam Gaussian (both supplied as `GaussianBeam`).
pub fn deconvolve_component_beam(
    source: GaussianBeam,
    beam: GaussianBeam,
) -> ImageToolResult<DeconvolveBeamResult> {
    if source.is_null() {
        return Err(ImageToolError::new("Source Gaussian cannot be null"));
    }
    if beam.is_null() {
        return Err(ImageToolError::new("Beam Gaussian cannot be null"));
    }
    let Some((fit, point)) = source.fitted_deconvolution(beam) else {
        return Err(ImageToolError::new(
            "Unable to deconvolve source from beam.",
        ));
    };
    let logs = if point {
        vec![ToolLogLine::warning(
            "Component is unresolved (point source) along at least one axis.",
        )]
    } else {
        Vec::new()
    };
    Ok(DeconvolveBeamResult { fit, point, logs })
}

/// Reads the `ImageInfo` of an attached image applying the attachment's `BeamView`.
pub fn effective_image_info(attached: &AttachedImage) -> ImageToolResult<ImageInfo> {
    let image = attached.lock();
    let mut record = super::beams::image_info(&image)?.to_record();
    attached.beam_view().apply_to(&mut record);
    Ok(ImageInfo::from_record(&record)?)
}

fn build_beam_from_parts(
    base: GaussianBeam,
    request: &SetBeamRequest,
) -> ImageToolResult<GaussianBeam> {
    let stored = StoredGaussianBeam::new(
        casa_core::quanta::Quantity::new(base.major_in("arcsec")?, "arcsec")?,
        casa_core::quanta::Quantity::new(base.minor_in("arcsec")?, "arcsec")?,
        casa_core::quanta::Quantity::new(base.position_angle_in("deg")?, "deg")?,
    )?;
    let stored = if let Some((val, unit)) = &request.major {
        let q = casa_core::quanta::Quantity::new(*val, unit.as_str())?;
        stored.with_component(casa_images::BeamComponent::Major, q)?
    } else {
        stored
    };
    let stored = if let Some((val, unit)) = &request.minor {
        let q = casa_core::quanta::Quantity::new(*val, unit.as_str())?;
        stored.with_component(casa_images::BeamComponent::Minor, q)?
    } else {
        stored
    };
    let stored = if let Some((val, unit)) = &request.pa {
        let q = casa_core::quanta::Quantity::new(*val, unit.as_str())?;
        stored.with_component(casa_images::BeamComponent::PositionAngle, q)?
    } else {
        stored
    };
    let beam = GaussianBeam::from_record(&stored.to_record())?;
    if beam.is_null() {
        return Err(ImageToolError::new(
            "Restoring beam cannot have zero major or minor axis unless remove=True.",
        ));
    }
    Ok(beam)
}

fn wrap_pa_rad(mut rad: f64) -> f64 {
    let half_pi = std::f64::consts::FRAC_PI_2;
    let pi = std::f64::consts::PI;
    while rad > half_pi {
        rad -= pi;
    }
    while rad <= -half_pi {
        rad += pi;
    }
    rad
}

/// Parameters for full CASA `ia.setrestoringbeam` behavior.
pub struct FullSetBeamRequest<'a> {
    /// Parsed `major`, `minor`, `positionangle` quantities when specified without `beam` or `imagename`.
    pub quantities: Option<[Quantity; 3]>,
    /// Pre-parsed casacore `beam` record (`{}`).
    pub beam: &'a RecordValue,
    /// Formatted `beam` history string.
    pub beam_history: &'a str,
    /// History representations of `major`, `minor`, `pa` inputs.
    pub input_histories: [&'a str; 3],
    /// Remove restoring beam(s) when `true`.
    pub remove: bool,
    /// Log beam changes when `true`.
    pub log: bool,
    /// 0-based channel index (`< 0` selects all/global).
    pub channel: i64,
    /// 0-based polarization index (`< 0` selects all/global).
    pub polarization: i64,
    /// Source image name to copy beams from (empty when unused).
    pub imagename: &'a str,
    /// Resolved path for `imagename`.
    pub imagename_path: Option<&'a Path>,
    /// Whether `major`/`minor`/`pa` had non-default values (for conflict checks with `imagename`).
    pub has_axis_args: bool,
}

fn beam_record_from_quantities(
    current: &BeamView,
    quantities: &[Quantity; 3],
    channel: i64,
    polarization: i64,
) -> ImageToolResult<RecordValue> {
    let rad = Unit::new("rad").expect("built-in angular unit");
    for (label, value) in [
        ("Major axis", &quantities[0]),
        ("Minor axis", &quantities[1]),
        ("Position angle", &quantities[2]),
    ] {
        if !value.unit().conformant(&Unit::dimensionless()) && !value.unit().conformant(&rad) {
            return Err(ImageToolError::new(format!(
                "Exception: {label} must have angular units ({} is not)..",
                value.unit().name()
            )));
        }
    }
    let dimensionless = Unit::dimensionless();
    let existing = if quantities
        .iter()
        .any(|value| value.unit().conformant(&dimensionless))
    {
        beam_for_units(current, channel, polarization)?
    } else {
        None
    };
    let with_units = |value: &Quantity, component, default| {
        if !value.unit().conformant(&dimensionless) {
            return value.clone();
        }
        let unit = existing.as_ref().map_or_else(
            || Unit::new(default).expect("built-in beam unit"),
            |beam| beam.component(component).unit().clone(),
        );
        Quantity::with_unit(value.value(), unit)
    };
    let major = with_units(&quantities[0], BeamComponent::Major, "arcsec");
    let minor = with_units(&quantities[1], BeamComponent::Minor, "arcsec");
    let pa = with_units(&quantities[2], BeamComponent::PositionAngle, "deg");
    if major.value() <= 0.0 || minor.value() <= 0.0 {
        return Err(ImageToolError::new(
            "Exception: Both the major and minor axes must be non-negative.",
        ));
    }
    let beam = StoredGaussianBeam::new(major, minor, pa).map_err(|error| {
        if error.to_string().contains("greater or equal") {
            ImageToolError::new("Exception: Major axis must be greater or equal to minor axis.")
        } else {
            ImageToolError::from(error)
        }
    })?;
    Ok(beam.to_record())
}

fn validate_beam_record_for_set(record: &RecordValue) -> ImageToolResult<RecordValue> {
    if ["beams", "nChannels", "nStokes"]
        .iter()
        .all(|key| record.get(key).is_some())
    {
        return Err(ImageToolError::new(
            "Exception: Both the major and minor axes must be non-negative.",
        ));
    }
    if record.len() != 3 {
        return Err(ImageToolError::new("Beam record does not contain 3 fields"));
    }
    for name in ["major", "minor", "positionangle"] {
        if record.get(name).is_none() {
            return Err(ImageToolError::new(format!(
                "Field {name} missing from restoring beam record"
            )));
        }
    }
    let beam = StoredGaussianBeam::from_record(record).map_err(|error| {
        if error.to_string().contains("greater or equal") {
            ImageToolError::new("Exception: Major axis must be greater or equal to minor axis.")
        } else {
            ImageToolError::from(error)
        }
    })?;
    if beam.is_null() {
        return Err(ImageToolError::new(
            "Exception: Both the major and minor axes must be non-negative.",
        ));
    }
    let angle = beam.wrapped_position_angle()?;
    Ok(beam
        .with_component(BeamComponent::PositionAngle, angle)?
        .to_record())
}

fn grid_record(channels: usize, stokes: usize) -> RecordValue {
    let mut grid = RecordValue::default();
    grid.upsert(
        "nChannels",
        Value::Scalar(ScalarValue::Int32(channels as i32)),
    );
    grid.upsert("nStokes", Value::Scalar(ScalarValue::Int32(stokes as i32)));
    grid
}

fn replace_all(info: &mut RecordValue, beam: RecordValue, channels: usize, stokes: usize) {
    if channels == 1 && stokes == 1 {
        info.upsert("restoringbeam", Value::Record(beam));
    } else {
        let mut grid = grid_record(channels, stokes);
        for index in 0..channels * stokes {
            grid.upsert(format!("*{index}"), Value::Record(beam.clone()));
        }
        info.upsert("perplanebeams", Value::Record(grid));
    }
}

fn copy_beams_from_image(
    destination: &mut RecordValue,
    source_name: &str,
    path: &Path,
    channels: usize,
    stokes: usize,
) -> ImageToolResult<()> {
    if !path.exists() {
        return Err(ImageToolError::new(format!(
            "Exception: File '{source_name}' does not exist."
        )));
    }
    let image = ImageHandle::open(path)?;
    if image.kind() != ImagePixelType::Float32 {
        return Err(ImageToolError::new(format!(
            "Logic Error: {source_name} has a different data type than the data type of the requested object"
        )));
    }
    let beams = image_info(&image)?.beam_set;
    if beams.is_empty() {
        return Err(ImageToolError::new(format!(
            "Image {source_name} has no beam"
        )));
    }
    if beams.n_channels() != 1 && beams.n_channels() != channels {
        return Err(ImageToolError::new(
            "Exception: Number of channels is not consistent.",
        ));
    }
    if beams.n_stokes() != 1 && beams.n_stokes() != stokes {
        return Err(ImageToolError::new(
            "Exception: Number of polarizations is not consistent.",
        ));
    }
    let source = image_info_record(&image);
    if beams.is_single() {
        destination.upsert(
            "restoringbeam",
            Value::Record(stored_beam_record(source, &beams, 0, 0)),
        );
    } else {
        let grid = source
            .and_then(|record| subrecord(record, "perplanebeams"))
            .cloned()
            .unwrap_or_else(|| beams.to_record());
        destination.upsert("perplanebeams", Value::Record(grid));
    }
    Ok(())
}

fn edit_beam_record(
    image: &ImageHandle,
    view: &BeamView,
    beam: RecordValue,
    channel: i64,
    polarization: i64,
) -> ImageToolResult<RecordValue> {
    let beams = view
        .beam_set()
        .map_err(|err| ImageToolError::new(err.to_string()))?;
    let mut result = image_info_record(image)
        .cloned()
        .unwrap_or(image_info(image)?.to_record());
    result.remove("restoringbeam");
    result.remove("perplanebeams");
    let indexed = channel >= 0 || polarization >= 0;
    if beams.is_single() && indexed {
        return Err(ImageToolError::new(
            "Channel and/or polarization has been specified, but this image has a single (global restoring beam. This beam will not be altered. If you really want to modify the global beam, rerun setting both channel and polarization less than zero",
        ));
    }
    if !indexed || beams.is_empty() {
        let (channels, stokes) = if indexed || beams.is_multi() {
            let (spectral, stokes_axis) = plane_axes(&image.coordinate_record())?;
            (
                axis_length(image, spectral)?,
                axis_length(image, stokes_axis)?,
            )
        } else {
            (1, 1)
        };
        replace_all(&mut result, beam, channels, stokes);
        return Ok(result);
    }
    let channels = beams.n_channels();
    let stokes = beams.n_stokes();
    if channel >= channels as i64 || polarization >= stokes as i64 {
        return Err(ImageToolError::new(
            "Failed AlwaysAssert Int(chan) < _beams.shape()[0] && Int(stokes) < _beams.shape()[1]",
        ));
    }
    let mut grid = subrecord(view.record(), "perplanebeams")
        .cloned()
        .unwrap_or_else(|| beams.to_record());
    for pol in 0..stokes {
        for chan in 0..channels {
            if (polarization < 0 || polarization as usize == pol)
                && (channel < 0 || channel as usize == chan)
            {
                grid.upsert(
                    format!("*{}", chan + pol * channels),
                    Value::Record(beam.clone()),
                );
            }
        }
    }
    result.upsert("perplanebeams", Value::Record(grid));
    Ok(result)
}

/// Formats a casacore beam record (`major`, `minor`, `positionangle`/`pa`) as a CASA history dict string.
pub fn format_beam_record_history(record: &RecordValue) -> ImageToolResult<String> {
    if record.is_empty() {
        return Ok("{...}".into());
    }
    let field = |name: &str| -> ImageToolResult<String> {
        let field = subrecord(record, name).ok_or_else(|| {
            ImageToolError::new(format!("RecordInterface: field {name} is unknown"))
        })?;
        let Some(Value::Scalar(ScalarValue::String(unit))) = field.get("unit") else {
            return Err(ImageToolError::new("RecordInterface: invalid unit field"));
        };
        let value = match field.get("value") {
            Some(Value::Scalar(ScalarValue::Int16(v))) => casacore::stream_double(f64::from(*v)),
            Some(Value::Scalar(ScalarValue::Int32(v))) => casacore::stream_double(f64::from(*v)),
            Some(Value::Scalar(ScalarValue::Int64(v))) => casacore::stream_double(*v as f64),
            Some(Value::Scalar(ScalarValue::Float32(v))) => casacore::stream_double(f64::from(*v)),
            Some(Value::Scalar(ScalarValue::Float64(v))) => casacore::stream_double(*v),
            _ => return Err(ImageToolError::new("RecordInterface: invalid value field")),
        };
        Ok(format!("{{'value': {value}, 'unit': '{unit}'}}"))
    };
    let angle = if record.get("pa").is_some() {
        "pa"
    } else {
        "positionangle"
    };
    Ok(format!(
        "{{'major': {}, 'minor': {}, 'positionangle': {}}}",
        field("major")?,
        field("minor")?,
        field(angle)?,
    ))
}

/// Executes full CASA `ia.setrestoringbeam`, returning an optional log line.
pub fn set_restoring_beam_full(
    attached: &AttachedImage,
    request: &FullSetBeamRequest<'_>,
    record_history: bool,
) -> ImageToolResult<Option<String>> {
    let mut image = attached.lock();
    attached.require_lock(&mut image, LockType::Write)?;
    let mut view = attached.beam_view();
    let (info, message) = if !request.imagename.is_empty() {
        if request.has_axis_args {
            return Err(ImageToolError::new(
                "Cannot specify both imagename and major, minor, and/or pa",
            ));
        }
        if request.remove {
            return Err(ImageToolError::new(
                "remove cannot be true if imagename is specified",
            ));
        }
        if !request.beam.is_empty() {
            return Err(ImageToolError::new(
                "beam must be empty if imagename specified",
            ));
        }
        if request.channel >= 0 || request.polarization >= 0 {
            return Err(ImageToolError::new(
                "Neither channel nor polarization can be non-negative if imagename is specified",
            ));
        }
        let mut info = image_info_record(&image)
            .cloned()
            .unwrap_or(image_info(&image)?.to_record());
        info.remove("restoringbeam");
        info.remove("perplanebeams");
        let (spectral, stokes) = plane_axes(&image.coordinate_record())?;
        let source_path = request
            .imagename_path
            .unwrap_or_else(|| Path::new(request.imagename));
        copy_beams_from_image(
            &mut info,
            request.imagename,
            source_path,
            axis_length(&image, spectral)?,
            axis_length(&image, stokes)?,
        )?;
        (info, "Set image beam set".to_owned())
    } else if request.remove {
        let mut info = image_info_record(&image)
            .cloned()
            .unwrap_or(image_info(&image)?.to_record());
        info.remove("restoringbeam");
        info.remove("perplanebeams");
        (info, "Removed restoring beam(s)".to_owned())
    } else {
        let updated = if request.beam.is_empty() {
            let quantities = request.quantities.as_ref().ok_or_else(|| {
                ImageToolError::new(
                    "If beam record not specified, all of major, minor, and positionangle must be specified and be either valid quantity records or strings",
                )
            })?;
            beam_record_from_quantities(&view, quantities, request.channel, request.polarization)?
        } else {
            validate_beam_record_for_set(request.beam)?
        };
        let message = if request.log {
            format!(
                "Set restoring beam {}",
                format_beam_record_history(&updated)?
            )
        } else {
            String::new()
        };
        (
            edit_beam_record(
                &image,
                &view,
                updated,
                request.channel,
                request.polarization,
            )?,
            message,
        )
    };
    let command = record_history.then(|| {
        format!(
            "ia.setrestoringbeam(major={}, minor={}, pa={}, beam={}, remove={}, log={}, channel={}, polarization={}, imagename={:?})",
            request.input_histories[0],
            request.input_histories[1],
            request.input_histories[2],
            request.beam_history,
            request.remove,
            request.log,
            request.channel,
            request.polarization,
            request.imagename,
        )
    });
    image.set_image_info_record(info.clone())?;
    view.capture(&info);
    if let Some(command) = command {
        image.record_history("setrestoringbeam", command)?;
    }
    image.save()?;
    Ok(Some(message))
}

struct BeamChange {
    original: StoredGaussianBeam,
    rotated: StoredGaussianBeam,
}

impl BeamChange {
    fn new(original: StoredGaussianBeam, angle: &Quantity) -> ImageToolResult<Self> {
        let position = original.wrapped_position_angle()?;
        let increment = casacore::get_conformant_value(angle, position.unit())?;
        let new_position =
            Quantity::with_unit(position.value() + increment, position.unit().clone());
        let new_position = wrap_position_angle(&new_position)?;
        let rotated = original.with_component(BeamComponent::PositionAngle, new_position)?;
        Ok(Self { original, rotated })
    }

    fn messages(&self, name: &str, kind: &str) -> ImageToolResult<[String; 2]> {
        let describe = |beam: &StoredGaussianBeam| -> ImageToolResult<String> {
            let format = |value: &Quantity| {
                format!(
                    "{} {}",
                    casacore::stream_double(value.value()),
                    value.unit().name()
                )
            };
            Ok(format!(
                "major: {}, minor: {}, pa: {}",
                format(beam.component(BeamComponent::Major)),
                format(beam.component(BeamComponent::Minor)),
                format(&beam.wrapped_position_angle()?)
            ))
        };
        Ok([
            format!(
                "Original {name} {kind}restoring beam: {}",
                describe(&self.original)?
            ),
            format!(
                "New {name} {kind}restoring beam: {}",
                describe(&self.rotated)?
            ),
        ])
    }
}

fn multiple_beam_messages(
    name: &str,
    min: &BeamChange,
    max: &BeamChange,
) -> ImageToolResult<Vec<String>> {
    let mut messages = vec![
        "This image has multiple beams. Reporting the rotation of the minimum and maximum area beams:"
            .into(),
    ];
    messages.extend(min.messages(name, "minimum area ")?);
    messages.extend(max.messages(name, "maximum area ")?);
    Ok(messages)
}

fn rotate_beam_record(
    beams: &ImageBeamSet,
    view: &BeamView,
    updated: &mut RecordValue,
    angle: &Quantity,
    name: &str,
) -> ImageToolResult<Vec<String>> {
    let change = |channel, stokes| {
        let beam = stored_beam_record(Some(view.record()), beams, channel, stokes);
        BeamChange::new(StoredGaussianBeam::from_record(&beam)?, angle)
    };
    if beams.is_single() {
        let single = change(0, 0)?;
        updated.remove("perplanebeams");
        updated.upsert("restoringbeam", Value::Record(single.rotated.to_record()));
        return Ok(Vec::from(single.messages(name, "")?));
    }
    let mut grid = subrecord(view.record(), "perplanebeams")
        .cloned()
        .unwrap_or_else(|| beams.to_record());
    let [minimum, maximum] = view.extrema();
    let indices = |index| (index % beams.n_channels(), index / beams.n_channels());
    for stokes in 0..beams.n_stokes() {
        for channel in 0..beams.n_channels() {
            let plane = change(channel, stokes)?;
            let index = stokes * beams.n_channels() + channel;
            grid.upsert(
                format!("*{index}"),
                Value::Record(plane.rotated.to_record()),
            );
        }
    }
    updated.remove("restoringbeam");
    updated.upsert("perplanebeams", Value::Record(grid));
    let (min_chan, min_stokes) = indices(minimum);
    let (max_chan, max_stokes) = indices(maximum);
    multiple_beam_messages(
        name,
        &change(min_chan, min_stokes)?,
        &change(max_chan, max_stokes)?,
    )
}

/// Rotates restoring beam(s) by an angular [`Quantity`], matching CASA `ia.rotatebeam`.
pub fn rotate_beam_quantity(
    attached: &AttachedImage,
    rotation: &Quantity,
    record_history: bool,
    angle_history: &str,
) -> ImageToolResult<Vec<String>> {
    let mut image = attached.lock();
    image.check_single_precision("rotatebeam")?;
    let degrees = Unit::new("deg").expect("built-in angular unit");
    if !rotation.unit().conformant(&degrees) {
        return Err(ImageToolError::new("Exception: Quantity is not an angle."));
    }
    attached.require_lock(&mut image, LockType::Write)?;
    let mut view = attached.beam_view();
    let beams = view
        .beam_set()
        .map_err(|err| ImageToolError::new(err.to_string()))?;
    let name = image.path().map_or_else(
        || "Temporary_Image".into(),
        |path| path.display().to_string(),
    );
    let (messages, updated_info, empty_angle) = if beams.is_empty() {
        let zero = Quantity::new(0.0, "arcsec").expect("built-in angular unit");
        let previous = Quantity::with_unit(view.empty_angle_degrees, degrees);
        let beam = StoredGaussianBeam::new(zero.clone(), zero, previous)?;
        let change = BeamChange::new(beam, rotation)?;
        let messages = multiple_beam_messages(&name, &change, &change)?;
        let angle = change
            .rotated
            .component(BeamComponent::PositionAngle)
            .value();
        (messages, None, angle)
    } else {
        let mut updated = match image_info_record(&image) {
            Some(info) => info.clone(),
            None => image_info(&image)?.to_record(),
        };
        let messages = rotate_beam_record(&beams, &view, &mut updated, rotation, &name)?;
        ImageInfo::from_record(&updated)?;
        (messages, Some(updated), 0.0)
    };
    let mut history = Vec::with_capacity(messages.len() + 2);
    if record_history {
        history.push("Ran ia.rotatebeam".into());
        history.push(format!("ia.rotatebeam(angle={angle_history})"));
    }
    history.extend(messages.iter().cloned());
    let location = "::BeamManipulator::rotate";
    with_pixels!(&mut *image, storage => match storage {
        Storage::Paged(image) => image.save_image_info_with_history(updated_info.as_ref(), location, &history),
        Storage::Temp { image, .. } => {
            if let Some(updated) = &updated_info {
                image.set_image_info_record(updated.clone())?;
            }
            image.append_history_at(location, &history)
        }
    })?;
    if let Some(updated) = updated_info {
        view.capture(&updated);
    } else {
        view.empty_angle_degrees = empty_angle;
    }
    Ok(messages)
}

fn validate_stored_gaussian(values: &[Quantity]) -> ImageToolResult<StoredGaussianBeam> {
    let major = &values[0];
    let minor = values.get(1).unwrap_or(major);
    let zero = Quantity::new(0.0, "deg").expect("built-in unit");
    let pa = values.get(2).unwrap_or(&zero);
    let rad = Unit::new("rad").expect("built-in unit");
    for (label, quantity) in [
        ("Major axis", major),
        ("Minor axis", minor),
        ("Position angle", pa),
    ] {
        if !quantity.unit().conformant(&rad) {
            return Err(ImageToolError::new(format!(
                "Exception: {label} must have angular units ({} is not)..",
                quantity.unit().name()
            )));
        }
    }
    for (label, quantity) in [("Major axis", major), ("Minor axis", minor)] {
        if quantity.value() < 0.0 {
            return Err(ImageToolError::new(format!(
                "Exception: {label} cannot be less than zero.."
            )));
        }
    }
    if major.value() < casacore::get_conformant_value(minor, major.unit())? {
        return Err(ImageToolError::new(
            "Exception: Major axis must be greater or equal to minor axis.",
        ));
    }
    Ok(StoredGaussianBeam::new(
        major.clone(),
        minor.clone(),
        pa.clone(),
    )?)
}

fn beam_to_radians(beam: &StoredGaussianBeam) -> ImageToolResult<GaussianBeam> {
    let unit = Unit::new("rad").expect("built-in unit");
    let angular = |quantity: &Quantity| casacore::get_conformant_value(quantity, &unit);
    Ok(GaussianBeam::new(
        angular(beam.component(BeamComponent::Major))?,
        angular(beam.component(BeamComponent::Minor))?,
        angular(&beam.wrapped_position_angle()?)?,
    ))
}

fn fit_gaussian_size(total: GaussianBeam, applied: GaussianBeam) -> Option<(GaussianBeam, bool)> {
    let (total_sin, total_cos) = (total.position_angle.sin(), total.position_angle.cos());
    let (beam_sin, beam_cos) = (applied.position_angle.sin(), applied.position_angle.cos());
    let alpha = (total.major * total_cos).powi(2) + (total.minor * total_sin).powi(2)
        - (applied.major * beam_cos).powi(2)
        - (applied.minor * beam_sin).powi(2);
    let beta = (total.major * total_sin).powi(2) + (total.minor * total_cos).powi(2)
        - (applied.major * beam_sin).powi(2)
        - (applied.minor * beam_cos).powi(2);
    let gamma = 2.0
        * ((total.minor.powi(2) - total.major.powi(2)) * total_sin * total_cos
            - (applied.minor.powi(2) - applied.major.powi(2)) * beam_sin * beam_cos);
    let sum = alpha + beta;
    let separation = ((alpha - beta).powi(2) + gamma.powi(2)).sqrt();
    let smallest = total.minor.min(applied.minor);
    let limit = 0.1 * smallest * smallest;
    if alpha < 0.0 || beta < 0.0 || sum < separation {
        return (0.5 * (sum - separation) < limit && alpha > -limit && beta > -limit)
            .then_some((applied, true));
    }
    let pa = if gamma.abs() + (alpha - beta).abs() == 0.0 {
        0.0
    } else {
        0.5 * (-gamma).atan2(alpha - beta)
    };
    Some((
        GaussianBeam::new(
            (0.5 * (sum + separation)).sqrt(),
            (0.5 * (sum - separation)).sqrt(),
            pa,
        ),
        false,
    ))
}

fn quantity_value_record(value: f64, unit: &str) -> RecordValue {
    RecordValue::new(vec![
        RecordField::new("unit", Value::from(unit.to_owned())),
        RecordField::new("value", Value::Scalar(ScalarValue::Float64(value))),
    ])
}

fn fit_gaussian_fields(beam: GaussianBeam) -> ImageToolResult<Vec<RecordField>> {
    let angle = Quantity::new(beam.position_angle, "rad")?;
    let angle = wrap_position_angle(&angle)?;
    let unit = Unit::new("deg").expect("built-in unit");
    Ok(vec![
        RecordField::new(
            "major",
            Value::Record(quantity_value_record(beam.major_in("arcsec")?, "arcsec")),
        ),
        RecordField::new(
            "minor",
            Value::Record(quantity_value_record(beam.minor_in("arcsec")?, "arcsec")),
        ),
        RecordField::new(
            "pa",
            Value::Record(quantity_value_record(
                casacore::get_conformant_value(&angle, &unit)?,
                "deg",
            )),
        ),
    ])
}

fn describe_stored_beam(beam: &StoredGaussianBeam) -> String {
    let quantity = |component| {
        let quantity = beam.component(component);
        format!(
            "{} {}",
            casacore::stream_double(quantity.value()),
            quantity.unit().name()
        )
    };
    format!(
        "major: {}, minor: {}, pa: {}",
        quantity(BeamComponent::Major),
        quantity(BeamComponent::Minor),
        quantity(BeamComponent::PositionAngle)
    )
}

/// Computes the beam required to reach `convolved` size from `source` (`ia.beamforconvolvedsize`).
pub fn beam_for_convolved_size(
    source: &[Quantity],
    convolved: &[Quantity],
) -> ImageToolResult<RecordValue> {
    if source.len() != 3 {
        return Err(ImageToolError::new("Cannot understand source values"));
    }
    if convolved.len() < 3 {
        return Err(ImageToolError::new("Cannot understand target values"));
    }
    let source = validate_stored_gaussian(source)?;
    let convolved = validate_stored_gaussian(convolved)?;
    let Some((fit, false)) =
        fit_gaussian_size(beam_to_radians(&convolved)?, beam_to_radians(&source)?)
    else {
        return Err(ImageToolError::new(format!(
            "Unable to reach target resolution of {} Input source {} is probably too large.",
            describe_stored_beam(&convolved),
            describe_stored_beam(&source),
        )));
    };
    Ok(RecordValue::new(fit_gaussian_fields(fit)?))
}

/// Deconvolves a source Gaussian from a beam Gaussian (`ia.deconvolvefrombeam`).
pub fn deconvolve_from_beam(
    source: &[Quantity],
    beam: &[Quantity],
) -> ImageToolResult<RecordValue> {
    if !(1..=3).contains(&source.len()) {
        return Err(ImageToolError::new("Cannot understand source values"));
    }
    let source = validate_stored_gaussian(source)?;
    if beam.is_empty() {
        return Err(ImageToolError::new("Cannot understand beam values"));
    }
    let beam = validate_stored_gaussian(beam)?;
    let result = fit_gaussian_size(beam_to_radians(&source)?, beam_to_radians(&beam)?);
    let (fit, unresolved) = result.unwrap_or((GaussianBeam::new(0.0, 0.0, 0.0), false));
    let mut fit_fields = fit_gaussian_fields(fit)?;
    fit_fields.push(RecordField::new(
        "success",
        Value::Scalar(ScalarValue::Bool(result.is_some())),
    ));
    Ok(RecordValue::new(vec![
        RecordField::new("fit", Value::Record(RecordValue::new(fit_fields))),
        RecordField::new("return", Value::Scalar(ScalarValue::Bool(unresolved))),
    ]))
}

/// Validates that `image` supports component deconvolution and returns the restoring [`GaussianBeam`]
/// for `(channel, polarization)` (`ia.deconvolvecomponentlist`).
pub fn deconvolve_beam_context(
    image: &ImageHandle,
    view: &BeamView,
    channel: i64,
    polarization: i64,
) -> ImageToolResult<GaussianBeam> {
    image.check_single_precision("deconvolvecomponentlist")?;
    let missing_beam =
        || ImageToolError::new("Exception: This image does not have a restoring beam.");
    if view.beam_set()?.is_empty() {
        return Err(missing_beam());
    }
    if casa_coordinates::CoordinateSystem::first_direction_in_record(&image.coordinate_record())?
        .is_none()
    {
        return Err(ImageToolError::new(
            "Exception: This image does not contain a direction coordinate.",
        ));
    }
    let stored_beam = beam_for_units(view, channel, polarization)?.ok_or_else(missing_beam)?;
    let rad = Unit::new("rad").expect("built-in radians unit");
    let radians = |quantity: &Quantity| -> ImageToolResult<f64> {
        Ok(casa_core::quanta::casacore::get_conformant_value(
            quantity, &rad,
        )?)
    };
    Ok(GaussianBeam::new(
        radians(stored_beam.component(BeamComponent::Major))?,
        radians(stored_beam.component(BeamComponent::Minor))?,
        radians(&stored_beam.wrapped_position_angle()?)?,
    ))
}

/// Deconvolves one Gaussian source `[major, minor, pa]` from `beam` (`ia.deconvolvecomponentlist`).
pub fn deconvolve_gaussian_source(
    axes: &[Quantity; 3],
    beam: GaussianBeam,
) -> ImageToolResult<GaussianBeam> {
    let rad = Unit::new("rad").expect("built-in radians unit");
    let radians = |quantity: &Quantity| -> ImageToolResult<f64> {
        Ok(casa_core::quanta::casacore::get_conformant_value(
            quantity, &rad,
        )?)
    };
    let [major, minor, pa] = axes;
    let major_rad = radians(major)?;
    let minor_rad = radians(minor)?;
    let angle_rad = radians(pa)?;
    if angle_rad.abs() > std::f64::consts::TAU {
        return Err(ImageToolError::new(
            "Gaussian2DParam<T>::setPA(const T &pa) - PA must be in radians and between -2pi and 2pi",
        ));
    }
    let default_width = Quantity::new(1.0, "arcmin").expect("built-in angular unit");
    let default_width = radians(&default_width)?;
    let minor_rad = if major_rad <= default_width {
        default_width * (minor_rad / default_width)
    } else {
        minor_rad
    };
    let minor_rad = major_rad * (minor_rad / major_rad);
    let in_record_units = |value, unit: &Unit| -> ImageToolResult<Quantity> {
        Ok(casa_core::quanta::casacore::convert(
            &Quantity::with_unit(value, rad.clone()),
            Some(unit),
        )?)
    };
    let angle_rad = angle_rad % std::f64::consts::PI;
    let angle_rad = if angle_rad < 0.0 {
        angle_rad + std::f64::consts::PI
    } else {
        angle_rad
    };
    let angle = in_record_units(angle_rad, pa.unit())?;
    let angle = casa_images::wrap_position_angle(&angle)?;
    let source = GaussianBeam::new(
        radians(&in_record_units(major_rad, major.unit())?)?,
        radians(&in_record_units(minor_rad, minor.unit())?)?,
        radians(&angle)?,
    );
    let (fit, _) = source.fitted_deconvolution(beam).ok_or_else(|| {
        ImageToolError::new("Source may be only (slightly) resolved in one direction")
    })?;
    if fit.minor == 0.0 {
        return Err(ImageToolError::new(
            "Gaussian2DParam<T>::setAxialRatio(const T &axialRatio) - axialRatio must be between (0,1]",
        ));
    }
    Ok(fit)
}
