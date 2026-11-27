// SPDX-License-Identifier: LGPL-3.0-or-later

//! Restoring-beam inspection, formatting, and validation helpers.

use std::collections::HashMap;

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_core::quanta::{Unit, casacore};
use casa_core::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use casa_images::{
    BeamComponent, GaussianBeam, ImageBeamSet, ImageError, ImageInfo, ImageType, StoredGaussianBeam,
};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::attachment::AttachedImage;
use super::beam_view::BeamView;
use super::coords::format_number_with_precision;
use super::error::{ImageToolError, ImageToolResult, ToolLogLine};
use super::handle::{ImageHandle, Storage, with_pixels};

/// Result of querying an image's restoring beam(s).
#[derive(Debug, Clone)]
pub enum RestoringBeamQuery {
    /// No restoring beam is stored on the image.
    Empty,
    /// Single restoring beam (either global or a selected channel/stokes plane).
    Single(GaussianBeam),
    /// Per-plane restoring beam matrix (`n_channels` x `n_stokes`).
    PerPlane(ImageBeamSet),
}

/// Which beam-area extremum to query from a per-plane beam set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BeamExtremumKind {
    /// Minimum-area beam.
    Min,
    /// Maximum-area beam.
    Max,
    /// Median-area beam (first match for CASA compatibility).
    Median,
}

/// Summary of a beam extremum query result.
#[derive(Debug, Clone)]
pub struct BeamExtremum {
    /// The selected beam (`major`/`minor`/`position_angle` in radians).
    pub beam: GaussianBeam,
    /// Zero-based channel index, if the image has a spectral axis.
    pub channel: Option<usize>,
    /// Zero-based polarization index, if the image has a Stokes axis.
    pub polarization: Option<usize>,
}

/// Returns the raw casacore `imageinfo` record from `image`, if present on disk or temp storage.
pub fn image_info_record(image: &ImageHandle) -> Option<&RecordValue> {
    with_pixels!(image, storage => match storage {
        Storage::Paged(paged) => match paged.table().keywords().get("imageinfo") {
            Some(Value::Record(record)) => Some(record),
            _ => None,
        },
        Storage::Temp { image, .. } => image.image_info_record(),
    })
}

/// Reads the parsed [`ImageInfo`] from `image`.
pub fn image_info(image: &ImageHandle) -> Result<ImageInfo, ImageError> {
    with_pixels!(image, storage => match storage {
        Storage::Paged(paged) => paged.image_info(),
        Storage::Temp { image, .. } => image.image_info(),
    })
}

/// Builds an `ImageInfo` value inheriting non-beam fields (such as `objectname`) from `image`.
pub fn image_info_with_beams(
    image: &ImageHandle,
    beam_set: ImageBeamSet,
) -> Result<ImageInfo, ImageError> {
    let mut info = image_info(image)?;
    info.beam_set = beam_set;
    if info.image_type == ImageType::Undefined {
        info.image_type = ImageInfo::default_image_type();
    }
    Ok(info)
}

/// Encodes a [`GaussianBeam`] as a casacore beam record (`major`/`minor`/`positionangle` in arcsec/deg).
pub fn gaussian_beam_record(beam: &GaussianBeam) -> ImageToolResult<RecordValue> {
    Ok(beam.to_record())
}

/// Formats a [`GaussianBeam`] using CASA's summary precision.
pub fn format_gaussian_beam(beam: &GaussianBeam) -> ImageToolResult<String> {
    let major = beam.major_in("arcsec")?;
    let minor = beam.minor_in("arcsec")?;
    let pa = beam.position_angle_in("deg")?;
    Ok(format!(
        "major: {} arcsec, minor: {} arcsec, pa: {} deg",
        format_number_with_precision(major, 0.0, false, 6),
        format_number_with_precision(minor, 0.0, false, 6),
        format_number_with_precision(pa, 0.0, false, 6)
    ))
}

/// Validates and resolves optional `(channel, polarization)` selectors against `shape` and `coordinates`.
///
/// Negative values (`-1`) mean "no specific slice selected" (unless checking per-plane rules).
pub fn validate_beam_indices(
    shape: &[usize],
    coordinates: &CoordinateSystem,
    channel: isize,
    polarization: isize,
    has_multi_beams: bool,
) -> ImageToolResult<(Option<usize>, Option<usize>)> {
    let spectral_axis = coordinates.find_coordinate(CoordinateType::Spectral);
    let stokes_axis = coordinates.find_coordinate(CoordinateType::Stokes);
    let n_chan = spectral_axis.map_or(1, |axis| shape[axis]);
    let n_stokes = stokes_axis.map_or(1, |axis| shape[axis]);

    let resolved_chan = if channel < 0 {
        None
    } else if spectral_axis.is_none() {
        return Err(ImageToolError::new(
            "Channel specified, but image has no spectral coordinate.",
        ));
    } else {
        let ch = usize::try_from(channel).map_err(|_| "Invalid negative channel index")?;
        if ch >= n_chan {
            return Err(ImageToolError::new(format!(
                "Channel {channel} is out of range for spectral axis of length {n_chan}"
            )));
        }
        Some(ch)
    };

    let resolved_stokes = if polarization < 0 {
        None
    } else if stokes_axis.is_none() {
        return Err(ImageToolError::new(
            "Polarization specified, but image has no polarization coordinate.",
        ));
    } else {
        let pol =
            usize::try_from(polarization).map_err(|_| "Invalid negative polarization index")?;
        if pol >= n_stokes {
            return Err(ImageToolError::new(format!(
                "Polarization {polarization} is out of range for polarization axis of length {n_stokes}"
            )));
        }
        Some(pol)
    };

    if has_multi_beams
        && ((spectral_axis.is_some() && n_chan > 1 && resolved_chan.is_none())
            || (stokes_axis.is_some() && n_stokes > 1 && resolved_stokes.is_none()))
        && (resolved_chan.is_some() || resolved_stokes.is_some())
    {
        return Err(ImageToolError::new(
            "Both channel and polarization must be specified when querying a per-plane beam set.",
        ));
    }

    Ok((resolved_chan, resolved_stokes))
}

/// Queries the restoring beam(s) of `attached`.
pub fn restoring_beam(
    attached: &AttachedImage,
    channel: isize,
    polarization: isize,
) -> ImageToolResult<RestoringBeamQuery> {
    let image = attached.lock();
    let beams = attached
        .beam_view()
        .beam_set()
        .map_err(|err| err.to_string())?;
    if beams.is_empty() {
        return Ok(RestoringBeamQuery::Empty);
    }
    if let Some(single) = beams.single_beam() {
        if channel >= 0 || polarization >= 0 {
            let _ = validate_beam_indices(
                image.shape(),
                image.coordinates(),
                channel,
                polarization,
                false,
            )?;
        }
        return Ok(RestoringBeamQuery::Single(single));
    }
    if channel < 0 && polarization < 0 {
        return Ok(RestoringBeamQuery::PerPlane(beams));
    }
    let (ch, pol) = validate_beam_indices(
        image.shape(),
        image.coordinates(),
        channel,
        polarization,
        true,
    )?;
    let ch = ch.unwrap_or(0);
    let pol = pol.unwrap_or(0);
    if ch >= beams.n_channels() || pol >= beams.n_stokes() {
        return Err(ImageToolError::new(format!(
            "Beam index (channel={ch}, stokes={pol}) out of range for {}x{} beam set",
            beams.n_channels(),
            beams.n_stokes()
        )));
    }
    Ok(RestoringBeamQuery::Single(*beams.beam(ch, pol)))
}

/// Computes the smallest enclosing common beam for `attached`.
pub fn common_beam(attached: &AttachedImage) -> ImageToolResult<(GaussianBeam, Vec<ToolLogLine>)> {
    let beams = attached
        .beam_view()
        .beam_set()
        .map_err(|err| err.to_string())?;
    if beams.is_empty() {
        return Err(ImageToolError::new("This image has no beams."));
    }
    if let Some(single) = beams.single_beam() {
        let line = ToolLogLine::info(format!(
            "Image only has a single beam ({}), so it is the common beam",
            format_gaussian_beam(&single)?
        ));
        return Ok((single, vec![line]));
    }
    let beam = beams.common_beam()?;
    let line = ToolLogLine::info(format!(
        "Computed common beam: {}",
        format_gaussian_beam(&beam)?
    ));
    Ok((beam, vec![line]))
}

/// Computes the Gaussian beam area in steradians (or pixel area? CASA `ia.beamarea` returns `arcsec2` or pixel count).
///
/// Wait: `GaussianBeam::area()` returns `pi / (4 ln 2) * major_rad * minor_rad`, which converted from rad^2 to arcsec^2 is `(180*3600/pi)^2`.
/// Returns the beam area in square arcseconds for consistency with CASA `ia.beamarea()`.
pub fn beam_area_arcsec2(
    attached: &AttachedImage,
    channel: isize,
    polarization: isize,
) -> ImageToolResult<f64> {
    let beam = match restoring_beam(attached, channel, polarization)? {
        RestoringBeamQuery::Single(beam) => beam,
        RestoringBeamQuery::Empty => {
            return Err(ImageToolError::new("This image has no restoring beam."));
        }
        RestoringBeamQuery::PerPlane(_) => {
            return Err(ImageToolError::new(
                "This image has per-plane beams; specify channel and/or polarization.",
            ));
        }
    };
    let major = beam.major_in("arcsec")?;
    let minor = beam.minor_in("arcsec")?;
    Ok(std::f64::consts::PI / (4.0 * 2.0_f64.ln()) * major * minor)
}

/// Finds the beam with minimum, maximum, or median area in `attached`.
pub fn beam_extremum(
    attached: &AttachedImage,
    which: BeamExtremumKind,
) -> ImageToolResult<BeamExtremum> {
    let image = attached.lock();
    let coordinates = image.coordinates();
    let has_spectral = coordinates
        .find_coordinate(CoordinateType::Spectral)
        .is_some();
    let has_stokes = coordinates
        .find_coordinate(CoordinateType::Stokes)
        .is_some();
    drop(image);

    let beams = attached
        .beam_view()
        .beam_set()
        .map_err(|err| err.to_string())?;
    if beams.is_empty() {
        return Err(ImageToolError::new("This image has no beams."));
    }
    if let Some(single) = beams.single_beam() {
        return Ok(BeamExtremum {
            beam: single,
            channel: has_spectral.then_some(0),
            polarization: has_stokes.then_some(0),
        });
    }
    let n_chan = beams.n_channels();
    let n_stokes = beams.n_stokes();
    let mut entries = Vec::with_capacity(n_chan * n_stokes);
    for stokes in 0..n_stokes {
        for chan in 0..n_chan {
            let beam = *beams.beam(chan, stokes);
            entries.push((beam.area(), chan, stokes, beam));
        }
    }
    let selected = match which {
        BeamExtremumKind::Min => {
            entries.sort_by(|a, b| a.0.total_cmp(&b.0));
            entries[0]
        }
        BeamExtremumKind::Max => {
            entries.sort_by(|a, b| a.0.total_cmp(&b.0));
            *entries.last().expect("non-empty beam set")
        }
        BeamExtremumKind::Median => {
            entries.sort_by(|a, b| a.0.total_cmp(&b.0));
            entries[entries.len() / 2]
        }
    };
    let best_chan = selected.1;
    let best_stokes = selected.2;
    Ok(BeamExtremum {
        beam: selected.3,
        channel: has_spectral.then_some(best_chan),
        polarization: has_stokes.then_some(best_stokes),
    })
}

/// Parses a `RecordValue` (direct `major`/`minor`/`positionangle` or `restoringbeam` wrapper) into [`GaussianBeam`].
pub fn parse_beam_record(record: &RecordValue) -> ImageToolResult<GaussianBeam> {
    if let Some(Value::Record(inner)) = record.get("restoringbeam") {
        return Ok(GaussianBeam::from_record(inner)?);
    }
    let _ = StoredGaussianBeam::from_record;
    Ok(GaussianBeam::from_record(record)?)
}

/// Subsets a multi-plane `beam_set` for dropped/sliced spectral and Stokes axes.
pub fn subset_beam_set(
    beams: &ImageBeamSet,
    channels: Option<&[usize]>,
    stokes: Option<&[usize]>,
) -> ImageToolResult<ImageBeamSet> {
    if beams.is_empty() || beams.is_single() {
        return Ok(beams.clone());
    }
    let chan_indices: Vec<usize> = match channels {
        Some(slice) if beams.n_channels() > 1 => slice.to_vec(),
        _ => (0..beams.n_channels()).collect(),
    };
    let stokes_indices: Vec<usize> = match stokes {
        Some(slice) if beams.n_stokes() > 1 => slice.to_vec(),
        _ => (0..beams.n_stokes()).collect(),
    };
    Ok(beams.subset(&chan_indices, &stokes_indices)?)
}

/// Returns the sub-record at `field` inside `record`, if present and a record.
pub fn subrecord<'a>(record: &'a RecordValue, field: &str) -> Option<&'a RecordValue> {
    match record.get(field) {
        Some(Value::Record(value)) => Some(value),
        _ => None,
    }
}

/// Looks up the stored casacore beam record for `(channel, polarization)` from `info` or `beams`.
pub fn stored_beam_record(
    info: Option<&RecordValue>,
    beams: &ImageBeamSet,
    channel: usize,
    polarization: usize,
) -> RecordValue {
    if let Some(info) = info {
        if let Some(grid) = subrecord(info, "perplanebeams") {
            let index = polarization * beams.n_channels() + channel;
            if let Some(beam) = subrecord(grid, &format!("*{index}"))
                .or_else(|| subrecord(grid, &format!("*{channel}_{polarization}")))
            {
                return beam.clone();
            }
        } else if let Some(beam) = subrecord(info, "restoringbeam") {
            return beam.clone();
        }
    }
    beams.beam(channel, polarization).to_record()
}

/// Updates `output`'s `restoringbeam` / `perplanebeams` for the selected `(channels, polarizations)` subset.
pub fn subset_beam_record(
    output: &mut RecordValue,
    stored: Option<&RecordValue>,
    beams: &ImageBeamSet,
    channels: &[usize],
    polarizations: &[usize],
) -> Result<(), ImageError> {
    let subset = beams.subset(channels, polarizations)?;
    let beam = |channel, polarization| {
        Value::Record(stored_beam_record(stored, beams, channel, polarization))
    };
    output.remove("perplanebeams");
    output.remove("restoringbeam");
    if subset.is_single() {
        output.upsert("restoringbeam", beam(channels[0], polarizations[0]));
    } else {
        let mut grid = subset.to_record();
        for (plane, &polarization) in polarizations.iter().enumerate() {
            for (slot, &channel) in channels.iter().enumerate() {
                grid.upsert(
                    format!("*{}", plane * channels.len() + slot),
                    beam(channel, polarization),
                );
            }
        }
        output.upsert("perplanebeams", Value::Record(grid));
    }
    Ok(())
}

/// Resolves the pixel axis indices of `(spectral, stokes)` from a raw coordinate record.
pub fn plane_axes(record: &RecordValue) -> ImageToolResult<(Option<usize>, Option<usize>)> {
    let mut axes = CoordinateSystem::record_coordinates(record)?;
    // CASA assigns beams by coordinate creation order, including after transposition.
    axes.sort_by_key(|axis| {
        axis.field
            .trim_start_matches(|character: char| character.is_ascii_alphabetic())
            .parse::<usize>()
            .ok()
    });
    let mut spectral = None;
    let mut stokes = None;
    for axis in axes {
        let kind = if axis.field.starts_with("spectral") {
            CoordinateType::Spectral
        } else if axis.field.starts_with("stokes") {
            CoordinateType::Stokes
        } else if axis.field.starts_with("coordinate") {
            axis.model(record)?.coordinate_type()
        } else {
            continue;
        };
        let selected = match kind {
            CoordinateType::Spectral => &mut spectral,
            CoordinateType::Stokes => &mut stokes,
            _ => continue,
        };
        selected.get_or_insert_with(|| axis.pixel_axes.first().copied().flatten());
        if spectral.is_some() && stokes.is_some() {
            break;
        }
    }
    Ok((spectral.flatten(), stokes.flatten()))
}

/// Returns the length of `axis` in `image`, or `1` when `axis` is `None`.
pub fn axis_length(image: &ImageHandle, axis: Option<usize>) -> ImageToolResult<usize> {
    match axis {
        Some(axis) => {
            image.shape().get(axis).copied().ok_or_else(|| {
                ImageToolError::new("Image coordinate axis is outside the image shape")
            })
        }
        None => Ok(1),
    }
}

fn beam_matrix_planes(
    beams: &ImageBeamSet,
    stokes_first: bool,
) -> ([usize; 2], impl Iterator<Item = (usize, usize)>) {
    let channels = beams.n_channels();
    let stokes = beams.n_stokes();
    let (rows, columns) = if stokes_first {
        (stokes, channels)
    } else {
        (channels, stokes)
    };
    let planes = (0..columns).flat_map(move |column| {
        (0..rows).map(move |row| {
            if stokes_first {
                (column, row)
            } else {
                (row, column)
            }
        })
    });
    ([rows, columns], planes)
}

fn beam_matrix_array(
    beams: &ImageBeamSet,
    stokes_first: bool,
    value: impl Fn(&GaussianBeam) -> Result<f64, ImageError>,
) -> ImageToolResult<Value> {
    let (shape, planes) = beam_matrix_planes(beams, stokes_first);
    let mut values = Vec::with_capacity(beams.nelements());
    for (channel, polarization) in planes {
        values.push(value(beams.beam(channel, polarization))?);
    }
    let array = ArrayD::from_shape_vec(IxDyn(&shape).f(), values)
        .map_err(|err| ImageToolError::new(err.to_string()))?;
    Ok(Value::Array(ArrayValue::Float64(array)))
}

fn quantity_record(value: f64, unit: &str) -> RecordValue {
    RecordValue::new(vec![
        RecordField::new("unit", Value::from(unit.to_owned())),
        RecordField::new("value", Value::Scalar(ScalarValue::Float64(value))),
    ])
}

fn beam_parameter_matrices(
    info: &RecordValue,
    beams: &ImageBeamSet,
    stokes_first: bool,
) -> ImageToolResult<Vec<(&'static str, RecordValue)>> {
    let (shape, planes) = beam_matrix_planes(beams, stokes_first);
    let grid = subrecord(info, "perplanebeams")
        .ok_or_else(|| ImageToolError::new("Missing per-plane beams"))?;
    let mut stored_grid = HashMap::with_capacity(beams.nelements());
    for field in grid.fields() {
        stored_grid
            .entry(field.name.as_str())
            .or_insert(&field.value);
    }
    let parameters = [
        ("major", BeamComponent::Major, "arcsec"),
        ("minor", BeamComponent::Minor, "arcsec"),
        ("pa", BeamComponent::PositionAngle, "deg"),
    ];
    let units = parameters.map(|(_, _, unit)| Unit::new(unit).expect("built-in angular unit"));
    let mut arrays: [Vec<f64>; 3] = std::array::from_fn(|_| Vec::with_capacity(beams.nelements()));
    for (channel, polarization) in planes {
        if beams.beam(channel, polarization).is_null() {
            return Err(ImageToolError::new("Invalid per plane beam found"));
        }
        let linear_index = polarization * beams.n_channels() + channel;
        let lookup = |name: &str| match stored_grid.get(name) {
            Some(Value::Record(record)) => Some(record),
            _ => None,
        };
        let stored = lookup(&format!("*{linear_index}"))
            .or_else(|| lookup(&format!("*{channel}_{polarization}")))
            .ok_or_else(|| ImageToolError::new("Missing per-plane beam"))?;
        let beam = StoredGaussianBeam::from_record(stored)?;
        for (index, (_, component, _)) in parameters.iter().enumerate() {
            arrays[index].push(casacore::get_conformant_value(
                beam.component(*component),
                &units[index],
            )?);
        }
    }
    parameters
        .into_iter()
        .zip(arrays)
        .map(|((name, _, unit), values)| {
            let array = ArrayD::from_shape_vec(IxDyn(&shape).f(), values)
                .map_err(|err| ImageToolError::new(err.to_string()))?;
            let rec = RecordValue::new(vec![
                RecordField::new("unit", Value::from(unit.to_owned())),
                RecordField::new("value", Value::Array(ArrayValue::Float64(array))),
            ]);
            Ok((name, rec))
        })
        .collect()
}

/// Returns the restoring beam record matching CASA's `ia.restoringbeam(channel, polarization, mbret)`.
pub fn restoring_beam_record(
    view: &BeamView,
    coordinate_record: impl FnOnce() -> RecordValue,
    channel: i64,
    polarization: i64,
    mode: &str,
) -> ImageToolResult<RecordValue> {
    let beams = view
        .beam_set()
        .map_err(|err| ImageToolError::new(err.to_string()))?;
    let first = mode.as_bytes().first().map(u8::to_ascii_lowercase);
    let matrix = first == Some(b'm');
    if !matrix && first != Some(b'l') {
        return Err(ImageToolError::new(format!(
            "Unsupported mbret value {mode}"
        )));
    }
    let indexed = channel >= 0 || polarization >= 0;
    if matrix {
        if !beams.is_multi() {
            return Err(ImageToolError::new(
                "Exception: mbret='matrix' only makes sense if the image has per-plane beams. This image does not.",
            ));
        }
        if indexed {
            return Err(ImageToolError::new(
                "Exception: mbret='matrix' only makes sense if both channel and polarization are negative.",
            ));
        }
    }
    if beams.is_empty() {
        return Ok(RecordValue::default());
    }
    let info = Some(view.record());
    if beams.is_single() {
        return Ok(stored_beam_record(info, &beams, 0, 0));
    }
    let channels = beams.n_channels() as i64;
    let stokes = beams.n_stokes() as i64;
    if indexed {
        if channels > 1 && stokes > 1 && (channel < 0 || polarization < 0) {
            return Err(ImageToolError::new(
                "Exception: In the case of an image with both a nondegenerate spectral axis and a nondegenerate polarization axis either both channel and polarization must be positive, or both must be negative.",
            ));
        }
        if channels > 1 && channel >= channels {
            return Err(ImageToolError::new(format!(
                "Exception: channel value specified as {channel} must be less than the number of channels in the image, which is {channels}."
            )));
        }
        if stokes > 1 && polarization >= stokes {
            return Err(ImageToolError::new(format!(
                "Exception: polarization value must be less than the number of polarization planes in the image, which is {stokes}."
            )));
        }
        if (channels > 1 && channel < 0) || (stokes > 1 && polarization < 0) {
            return Err(ImageToolError::new(
                "Failed AlwaysAssert chan >=0 && chan < Int(nchan()) && stokes >= 0 && stokes < Int(nstokes())",
            ));
        }
        let channel = if channels == 1 { 0 } else { channel as usize };
        let polarization = if stokes == 1 {
            0
        } else {
            polarization as usize
        };
        return Ok(stored_beam_record(info, &beams, channel, polarization));
    }
    let mut fields = vec![
        RecordField::new("nChannels", Value::Scalar(ScalarValue::Int64(channels))),
        RecordField::new("nStokes", Value::Scalar(ScalarValue::Int64(stokes))),
    ];
    if matrix {
        let (spectral, stokes) = plane_axes(&coordinate_record())?;
        let stokes_first = stokes.is_some_and(|stokes| {
            spectral.is_none_or(|spectral| spectral == 0 || spectral >= stokes)
        });
        for (name, rec) in beam_parameter_matrices(view.record(), &beams, stokes_first)? {
            fields.push(RecordField::new(name, Value::Record(rec)));
        }
    } else {
        let mut by_channel = RecordValue::default();
        for channel in 0..beams.n_channels() {
            let mut by_stokes = RecordValue::default();
            for polarization in 0..beams.n_stokes() {
                by_stokes.upsert(
                    format!("*{polarization}"),
                    Value::Record(stored_beam_record(info, &beams, channel, polarization)),
                );
            }
            by_channel.upsert(format!("*{channel}"), Value::Record(by_stokes));
        }
        fields.push(RecordField::new("beams", Value::Record(by_channel)));
    }
    Ok(RecordValue::new(fields))
}

/// Computes the CASA `ia.beamarea(channel, polarization)` record (`arcsec2` and `pixels`).
pub fn beam_area_record(
    image: &ImageHandle,
    view: &BeamView,
    channel: i64,
    polarization: i64,
) -> ImageToolResult<RecordValue> {
    let beams = view
        .beam_set()
        .map_err(|err| ImageToolError::new(err.to_string()))?;
    if beams.is_empty() {
        return Err(ImageToolError::new(
            "Exception: This image has no beam(s). The setrestoringbeam() method may be used to set its beam(s).",
        ));
    }
    let coord_record = image.coordinate_record();
    let coords = CoordinateSystem::from_record(&coord_record)?;
    let direction = coords
        .find_coordinate(CoordinateType::Direction)
        .ok_or_else(|| {
            ImageToolError::new(
                "directionCoordinate: Coordinate system has no direction coordinate",
            )
        })?;
    let increment = coords.coordinate(direction).increment();
    let pixel_area = (increment[0] * increment[1]).abs();
    let to_arcsec = 3600.0 * 180.0 / std::f64::consts::PI;
    let beam_in_arcsec = |area: f64| area * to_arcsec * to_arcsec;
    let single = |area| {
        RecordValue::new(vec![
            RecordField::new(
                "arcsec2",
                Value::Scalar(ScalarValue::Float64(beam_in_arcsec(area))),
            ),
            RecordField::new(
                "pixels",
                Value::Scalar(ScalarValue::Float64(area / pixel_area)),
            ),
        ])
    };
    if let Some(beam) = beams.single_beam() {
        return Ok(single(beam.area()));
    }
    let (spectral, stokes) = plane_axes(&coord_record)?;
    let channels = axis_length(image, spectral)? as i64;
    let polarizations = axis_length(image, stokes)? as i64;
    if channel >= channels {
        return Err(ImageToolError::new(format!(
            "Exception: This image only has {channels} channels, so a channel value less than {channels} must be specified."
        )));
    }
    if polarization >= polarizations {
        return Err(ImageToolError::new(format!(
            "Exception: This image only has {polarizations} polarizations, so a polarization value less than {polarizations} must be specified."
        )));
    }
    if channel >= 0 && polarization >= 0 {
        if (beams.n_channels() > 1 && channel as usize >= beams.n_channels())
            || (beams.n_stokes() > 1 && polarization as usize >= beams.n_stokes())
        {
            return Err(ImageToolError::new(
                "Selected plane is outside the restoring beam grid",
            ));
        }
        return Ok(single(
            beams.beam(channel as usize, polarization as usize).area(),
        ));
    }
    if channel >= 0 || polarization >= 0 {
        return Err(ImageToolError::new(
            "Exception: In the case of a multibeam image, either both channel and polarization must be non-negative, or both must be negative.",
        ));
    }
    let stokes_first = matches!((stokes, spectral), (Some(s), Some(c)) if s < c);
    Ok(RecordValue::new(vec![
        RecordField::new(
            "arcsec2",
            beam_matrix_array(&beams, stokes_first, |beam| Ok(beam_in_arcsec(beam.area())))?,
        ),
        RecordField::new(
            "pixels",
            beam_matrix_array(&beams, stokes_first, |beam| Ok(beam.area() / pixel_area))?,
        ),
    ]))
}

/// Computes CASA's `ia.commonbeam()` record (`pa`, `major`, `minor` quantities).
pub fn common_beam_record(image: &ImageHandle, view: &BeamView) -> ImageToolResult<RecordValue> {
    image.check_single_precision("commonbeam")?;
    let beams = view
        .beam_set()
        .map_err(|err| ImageToolError::new(err.to_string()))?;
    if beams.is_empty() {
        return Err(ImageToolError::new("Exception: This image has no beam(s)."));
    }
    let common = beams.common_beam()?;
    let info = Some(view.record());
    let matching_plane = (0..beams.n_stokes()).find_map(|polarization| {
        (0..beams.n_channels())
            .find(|&channel| *beams.beam(channel, polarization) == common)
            .map(|channel| (channel, polarization))
    });
    let record = if let Some((channel, polarization)) = matching_plane {
        stored_beam_record(info, &beams, channel, polarization)
    } else {
        common.to_record()
    };
    let stored = StoredGaussianBeam::from_record(&record)?;
    let degrees = Unit::new("deg").expect("built-in degrees");
    let angle = casacore::convert(&stored.wrapped_position_angle()?, Some(&degrees))?;
    let mut fields = vec![RecordField::new(
        "pa",
        Value::Record(quantity_record(angle.value(), "deg")),
    )];
    for component in ["major", "minor"] {
        fields.push(RecordField::new(
            component,
            Value::Record(
                subrecord(&record, component)
                    .expect("validated beam component")
                    .clone(),
            ),
        ));
    }
    Ok(RecordValue::new(fields))
}

/// Returns the stored beam for unit conversions (`convertflux`) at `(channel, polarization)`.
pub fn beam_for_units(
    current: &BeamView,
    channel: i64,
    polarization: i64,
) -> ImageToolResult<Option<StoredGaussianBeam>> {
    let beams = current
        .beam_set()
        .map_err(|err| ImageToolError::new(err.to_string()))?;
    if beams.is_empty() {
        return Ok(None);
    }
    let index = |index: i64, length: usize| {
        if length == 1 {
            Ok(0)
        } else if index >= 0 && (index as usize) < length {
            Ok(index as usize)
        } else {
            Err(ImageToolError::new(
                "Failed AlwaysAssert chan >=0 && chan < Int(nchan()) && stokes >= 0 && stokes < Int(nstokes())",
            ))
        }
    };
    let record = stored_beam_record(
        Some(current.record()),
        &beams,
        index(channel, beams.n_channels())?,
        index(polarization, beams.n_stokes())?,
    );
    Ok(Some(StoredGaussianBeam::from_record(&record)?))
}
