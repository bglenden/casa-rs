// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::HashMap;

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_images::{
    BeamComponent, GaussianBeam, ImageBeamSet, ImageError, ImageInfo, StoredGaussianBeam,
};
use casa_types::quanta::{Unit, casacore};
use casa_types::{RecordValue, Value};
use pyo3::prelude::*;

use crate::convert::value::CasaValue;

use super::PyImageTool;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};

pub(super) fn subrecord<'a>(record: &'a RecordValue, field: &str) -> Option<&'a RecordValue> {
    match record.get(field) {
        Some(Value::Record(value)) => Some(value),
        _ => None,
    }
}

pub(super) fn image_info(image: &ImageHandle) -> Result<ImageInfo, ImageError> {
    with_pixels!(image, storage => with_storage!(storage, image => image.image_info()))
}

pub(super) fn image_info_record(image: &ImageHandle) -> Option<&RecordValue> {
    with_pixels!(image, storage => match storage {
        Storage::Paged(image) => subrecord(image.table().keywords(), "imageinfo"),
        Storage::Temp { image, .. } => image.image_info_record(),
    })
}

pub(super) fn stored_beam_record(
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

pub(super) fn subset_beam_record(
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

fn beam_matrix(
    beams: &ImageBeamSet,
    stokes_first: bool,
    value: impl Fn(&GaussianBeam) -> Result<f64, ImageError>,
) -> PyResult<CasaValue> {
    let (shape, planes) = beam_matrix_planes(beams, stokes_first);
    let mut values = Vec::with_capacity(beams.nelements());
    for (channel, polarization) in planes {
        values.push(value(beams.beam(channel, polarization)).map_err(raised)?);
    }
    Ok(CasaValue::Floats {
        shape: shape.to_vec(),
        values,
    })
}

fn beam_parameter_matrices(
    coordinates: &CoordinateSystem,
    info: &RecordValue,
    beams: &ImageBeamSet,
) -> PyResult<Vec<(&'static str, CasaValue)>> {
    let pixel_axis = |kind| {
        coordinates
            .find_coordinate(kind)
            .and_then(|index| coordinates.coordinate_pixel_axes(index))
            .and_then(|axes| axes.into_iter().flatten().next())
    };
    let stokes_first = pixel_axis(CoordinateType::Stokes).is_some_and(|stokes| {
        pixel_axis(CoordinateType::Spectral)
            .is_none_or(|spectral| spectral == 0 || spectral >= stokes)
    });
    let (shape, planes) = beam_matrix_planes(beams, stokes_first);
    let grid = subrecord(info, "perplanebeams").ok_or_else(|| raised("Missing per-plane beams"))?;
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
            return Err(raised("Invalid per plane beam found"));
        }
        let linear_index = polarization * beams.n_channels() + channel;
        let lookup = |name: &str| match stored_grid.get(name) {
            Some(Value::Record(record)) => Some(record),
            _ => None,
        };
        let stored = lookup(&format!("*{linear_index}"))
            .or_else(|| lookup(&format!("*{channel}_{polarization}")))
            .ok_or_else(|| raised("Missing per-plane beam"))?;
        let beam = StoredGaussianBeam::from_record(stored).map_err(raised)?;
        for (index, (_, component, _)) in parameters.iter().enumerate() {
            arrays[index].push(
                casacore::get_conformant_value(beam.component(*component), &units[index])
                    .map_err(raised)?,
            );
        }
    }
    Ok(parameters
        .into_iter()
        .zip(arrays)
        .map(|((name, _, unit), values)| {
            let array = CasaValue::Floats {
                shape: shape.to_vec(),
                values,
            };
            (
                name,
                CasaValue::record([("unit", CasaValue::Text(unit.into())), ("value", array)]),
            )
        })
        .collect())
}

pub(super) fn restoring_beam(
    view: &super::beam_view::BeamView,
    coordinates: &CoordinateSystem,
    channel: i64,
    polarization: i64,
    mode: &str,
) -> PyResult<CasaValue> {
    let beams = view.beam_set().map_err(raised)?;
    let first = mode.as_bytes().first().map(u8::to_ascii_lowercase);
    let matrix = first == Some(b'm');
    if !matrix && first != Some(b'l') {
        return Err(raised(format!("Unsupported mbret value {mode}")));
    }
    let indexed = channel >= 0 || polarization >= 0;
    if matrix {
        if !beams.is_multi() {
            return Err(raised(
                "Exception: mbret='matrix' only makes sense if the image has per-plane beams. This image does not.",
            ));
        }
        if indexed {
            return Err(raised(
                "Exception: mbret='matrix' only makes sense if both channel and polarization are negative.",
            ));
        }
    }
    if beams.is_empty() {
        return Ok(CasaValue::empty_record());
    }
    let info = Some(view.record());
    let beam = |channel, polarization| {
        CasaValue::from_record(&stored_beam_record(info, &beams, channel, polarization))
    };
    if beams.is_single() {
        return Ok(beam(0, 0));
    }
    let channels = beams.n_channels() as i64;
    let stokes = beams.n_stokes() as i64;
    if indexed {
        if channels > 1 && stokes > 1 && (channel < 0 || polarization < 0) {
            return Err(raised(
                "Exception: In the case of an image with both a nondegenerate spectral axis and a nondegenerate polarization axis either both channel and polarization must be positive, or both must be negative.",
            ));
        }
        if channels > 1 && channel >= channels {
            return Err(raised(format!(
                "Exception: channel value specified as {channel} must be less than the number of channels in the image, which is {channels}."
            )));
        }
        if stokes > 1 && polarization >= stokes {
            return Err(raised(format!(
                "Exception: polarization value must be less than the number of polarization planes in the image, which is {stokes}."
            )));
        }
        if (channels > 1 && channel < 0) || (stokes > 1 && polarization < 0) {
            return Err(raised(
                "Failed AlwaysAssert chan >=0 && chan < Int(nchan()) && stokes >= 0 && stokes < Int(nstokes())",
            ));
        }
        let channel = if channels == 1 { 0 } else { channel as usize };
        let polarization = if stokes == 1 {
            0
        } else {
            polarization as usize
        };
        return Ok(beam(channel, polarization));
    }
    let mut result = vec![
        ("nChannels", CasaValue::Int(channels)),
        ("nStokes", CasaValue::Int(stokes)),
    ];
    if matrix {
        result.extend(beam_parameter_matrices(coordinates, view.record(), &beams)?);
    } else {
        let by_channel = (0..beams.n_channels()).map(|channel| {
            let by_stokes = (0..beams.n_stokes())
                .map(|polarization| (format!("*{polarization}"), beam(channel, polarization)));
            (format!("*{channel}"), CasaValue::record(by_stokes))
        });
        result.push(("beams", CasaValue::record(by_channel)));
    }
    Ok(CasaValue::record(result))
}

pub(super) fn plane_axes(record: &RecordValue) -> PyResult<(Option<usize>, Option<usize>)> {
    let axes = CoordinateSystem::record_coordinates(record).map_err(raised)?;
    let axis = |kind: &str| {
        // CASA assigns beams by coordinate creation order, including after transposition.
        axes.iter()
            .filter_map(|axis| Some((axis.field.strip_prefix(kind)?.parse::<usize>().ok()?, axis)))
            .min_by_key(|(index, _)| *index)
            .and_then(|(_, axis)| axis.pixel_axes.first().copied().flatten())
    };
    Ok((axis("spectral"), axis("stokes")))
}

pub(super) fn axis_length(image: &ImageHandle, axis: Option<usize>) -> PyResult<usize> {
    match axis {
        Some(axis) => image
            .shape()
            .get(axis)
            .copied()
            .ok_or_else(|| raised("Image coordinate axis is outside the image shape")),
        None => Ok(1),
    }
}

fn beam_area(
    image: &ImageHandle,
    view: &super::beam_view::BeamView,
    channel: i64,
    polarization: i64,
) -> PyResult<CasaValue> {
    let beams = view.beam_set().map_err(raised)?;
    if beams.is_empty() {
        return Err(raised(
            "Exception: This image has no beam(s). The setrestoringbeam() method may be used to set its beam(s).",
        ));
    }
    let coord_record = image.coordinate_record();
    let coords = CoordinateSystem::from_record(&coord_record).map_err(raised)?;
    let direction = coords
        .find_coordinate(CoordinateType::Direction)
        .ok_or_else(|| {
            raised("directionCoordinate: Coordinate system has no direction coordinate")
        })?;
    let increment = coords.coordinate(direction).increment();
    let pixel_area = (increment[0] * increment[1]).abs();
    let to_arcsec = 3600.0 * 180.0 / std::f64::consts::PI;
    let beam_in_arcsec = |area: f64| area * to_arcsec * to_arcsec;
    let single = |area| {
        CasaValue::record([
            ("arcsec2", CasaValue::Float(beam_in_arcsec(area))),
            ("pixels", CasaValue::Float(area / pixel_area)),
        ])
    };
    if let Some(beam) = beams.single_beam() {
        return Ok(single(beam.area()));
    }
    let (spectral, stokes) = plane_axes(&coord_record)?;
    let channels = axis_length(image, spectral)? as i64;
    let polarizations = axis_length(image, stokes)? as i64;
    if channel >= channels {
        return Err(raised(format!(
            "Exception: This image only has {channels} channels, so a channel value less than {channels} must be specified."
        )));
    }
    if polarization >= polarizations {
        return Err(raised(format!(
            "Exception: This image only has {polarizations} polarizations, so a polarization value less than {polarizations} must be specified."
        )));
    }
    if channel >= 0 && polarization >= 0 {
        if (beams.n_channels() > 1 && channel as usize >= beams.n_channels())
            || (beams.n_stokes() > 1 && polarization as usize >= beams.n_stokes())
        {
            return Err(raised("Selected plane is outside the restoring beam grid"));
        }
        return Ok(single(
            beams.beam(channel as usize, polarization as usize).area(),
        ));
    }
    if channel >= 0 || polarization >= 0 {
        return Err(raised(
            "Exception: In the case of a multibeam image, either both channel and polarization must be non-negative, or both must be negative.",
        ));
    }
    let stokes_first = matches!((stokes, spectral), (Some(s), Some(c)) if s < c);
    Ok(CasaValue::record([
        (
            "arcsec2",
            beam_matrix(&beams, stokes_first, |beam| Ok(beam_in_arcsec(beam.area())))?,
        ),
        (
            "pixels",
            beam_matrix(&beams, stokes_first, |beam| Ok(beam.area() / pixel_area))?,
        ),
    ]))
}

fn common_beam(image: &ImageHandle, view: &super::beam_view::BeamView) -> PyResult<CasaValue> {
    image.check_single_precision("commonbeam")?;
    let beams = view.beam_set().map_err(raised)?;
    if beams.is_empty() {
        return Err(raised("Exception: This image has no beam(s)."));
    }
    let common = beams.common_beam().map_err(raised)?;
    let info = Some(view.record());
    let matching_plane = (0..beams.n_stokes()).find_map(|polarization| {
        (0..beams.n_channels())
            .find(|&channel| *beams.beam(channel, polarization) == common)
            .map(|channel| (channel, polarization))
    });
    let mut record = if let Some((channel, polarization)) = matching_plane {
        stored_beam_record(info, &beams, channel, polarization)
    } else {
        common.to_record()
    };
    let stored = StoredGaussianBeam::from_record(&record).map_err(raised)?;
    let degrees = Unit::new("deg").expect("built-in degrees");
    let angle = casacore::convert(
        &stored.wrapped_position_angle().map_err(raised)?,
        Some(&degrees),
    )
    .map_err(raised)?;
    record.remove("positionangle");
    let mut result = vec![("pa", CasaValue::quantity(angle.value(), "deg"))];
    for component in ["major", "minor"] {
        result.push((
            component,
            CasaValue::from_record(
                subrecord(&record, component).expect("validated beam component"),
            ),
        ));
    }
    Ok(CasaValue::record(result))
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (channel=-1, polarization=-1, mbret="list"), text_signature = "($self, channel=-1, polarization=-1, mbret='list')")]
    fn restoringbeam(
        &self,
        py: Python<'_>,
        channel: i64,
        polarization: i64,
        mbret: &str,
    ) -> PyResult<Py<PyAny>> {
        py.detach(|| {
            self.state().image.as_ref().map_or_else(
                || Ok(CasaValue::empty_record()),
                |image| {
                    let handle = image.lock();
                    restoring_beam(
                        &image.beam_view(),
                        handle.coordinates(),
                        channel,
                        polarization,
                        mbret,
                    )
                },
            )
        })?
        .into_python(py)
    }

    #[pyo3(signature = (channel=-1, polarization=-1), text_signature = "($self, channel=-1, polarization=-1)")]
    fn beamarea(&self, py: Python<'_>, channel: i64, polarization: i64) -> PyResult<Py<PyAny>> {
        py.detach(|| {
            self.state().image.as_ref().map_or_else(
                || Ok(CasaValue::empty_record()),
                |image| beam_area(&image.lock(), &image.beam_view(), channel, polarization),
            )
        })?
        .into_python(py)
    }

    fn commonbeam(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        py.detach(|| {
            self.state().image.as_ref().map_or_else(
                || Ok(CasaValue::empty_record()),
                |image| common_beam(&image.lock(), &image.beam_view()),
            )
        })?
        .into_python(py)
    }
}
