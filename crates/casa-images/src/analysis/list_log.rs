// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA image-metadata list presentation, independent of the task front end.

use casa_coordinates::StokesType;
use casa_types::quanta::{
    Quantity, Unit,
    format::{CLEAN, format_angle},
};

use super::{ImageAxisSummary, ImageHeaderSummary, projection_header::cpp_number_precision};

/// Format the messages emitted by CASA's image-metadata `list` operation.
///
/// The summary should include extrema when they are available. Messages do
/// not include timestamps, priorities, or log origins; the caller supplies
/// those when it sends them to its configured logging sinks.
pub fn imhead_list_log(header: &ImageHeaderSummary) -> Vec<String> {
    let mut lines = vec!["General --".to_string()];
    let mut field = |name: &str, value: String| lines.push(format!("        -- {name}: {value}"));
    field("imtype", header.image_type.clone());
    field("object", header.object_name.clone());
    field(
        "equinox",
        header.direction_reference.clone().unwrap_or_else(not_found),
    );
    field("date-obs", header.observation_date.clone());
    field("observer", header.observer.clone());
    field(
        "projection",
        header
            .direction_projection
            .clone()
            .unwrap_or_else(not_found),
    );
    if let Some(spectral) = &header.spectral {
        field(
            "restfreq",
            format!(
                "[{}]",
                spectral
                    .rest_frequencies
                    .iter()
                    .map(|v| format!("{v:.1}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        );
    }
    field(
        "reffreqtype",
        header
            .spectral
            .as_ref()
            .map_or_else(not_found, |s| s.reference_frame.clone()),
    );
    field("telescope", header.telescope.clone());
    for (name, quantity) in [
        (
            "beammajor",
            header.restoring_beam.as_ref().map(|beam| &beam.major),
        ),
        (
            "beamminor",
            header.restoring_beam.as_ref().map(|beam| &beam.minor),
        ),
        (
            "beampa",
            header
                .restoring_beam
                .as_ref()
                .map(|beam| &beam.wrapped_position_angle),
        ),
    ] {
        field(
            name,
            quantity.map_or_else(not_found, |q| {
                format!("{}{}", cpp_number_precision(q.value, 12), q.unit)
            }),
        );
    }
    field("bunit", header.units.clone());
    field("masks", format!("[{}]", header.masks.join(", ")));
    field("shape", format!("{:?}", header.shape));
    let e = header.extrema.as_ref();
    field(
        "datamin",
        e.map_or_else(not_found, |e| cpp_number_precision(e.min, 6)),
    );
    field(
        "datamax",
        e.map_or_else(not_found, |e| cpp_number_precision(e.max, 6)),
    );
    field("minpos", e.map_or_else(not_found, |e| e.min_world.clone()));
    field(
        "minpixpos",
        e.map_or_else(not_found, |e| format!("{:?}", e.min_position)),
    );
    field("maxpos", e.map_or_else(not_found, |e| e.max_world.clone()));
    field(
        "maxpixpos",
        e.map_or_else(not_found, |e| format!("{:?}", e.max_position)),
    );
    for (title, prefix) in [
        ("axes", "ctype"),
        ("crpix", "crpix"),
        ("crval", "crval"),
        ("cdelt", "cdelt"),
        ("units", "cunit"),
    ] {
        lines.push(format!("{title} --"));
        for (index, axis) in header.axes.iter().enumerate() {
            let value = match prefix {
                "ctype" => axis.name.clone(),
                "crpix" => format!("{:.1}", axis.reference_pixel),
                "crval" => axis_value(axis, axis.reference_value, true),
                "cdelt" => axis_value(axis, axis.increment, false),
                _ => axis.unit.clone(),
            };
            lines.push(format!("        -- {prefix}{}: {value}", index + 1));
        }
    }
    lines
}

fn not_found() -> String {
    "Not found".into()
}

fn axis_value(axis: &ImageAxisSummary, value: f64, reference: bool) -> String {
    if axis.unit.is_empty() && reference && axis.name == "Stokes" {
        let code = value as i32;
        return format!(
            "['{}']",
            StokesType::from_code(code).map_or("Undefined", StokesType::name)
        );
    }
    if !axis.unit.is_empty() {
        if let (Ok(quantity), Ok(radians)) = (Quantity::new(value, &axis.unit), Unit::new("rad"))
            && let Ok(value) = quantity.get_value_in(&radians)
        {
            return format!("{}deg.min.sec", format_angle(value, CLEAN, 9));
        }
        if axis.unit == "Hz" {
            return format!("{value:.1}Hz");
        }
    }
    format!("{}{}", cpp_number_precision(value, 6), axis.unit)
}
