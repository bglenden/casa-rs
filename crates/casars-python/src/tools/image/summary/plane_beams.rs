// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fmt::Write;

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, SpectralCoordinate,
};
use casa_images::beam::wrap_position_angle;
use casa_images::{GaussianBeam, ImageBeamSet};
use casa_types::measures::doppler::DopplerRef;
use casa_types::quanta::{Quantity, Unit, casacore};
use casa_types::{RecordValue, ScalarValue, Value};
use pyo3::PyResult;

use crate::tools::coordsys::world_reference::spectral_text;

use super::super::beams::subrecord;
use super::super::handle::raised;
use super::coordinates::{scientific, velocity};

pub(super) fn normalized_angle(beam: &GaussianBeam) -> PyResult<f64> {
    let angle = Quantity::new(beam.position_angle, "rad").expect("built-in angle unit");
    let angle = wrap_position_angle(&angle).map_err(raised)?;
    let degrees = Unit::new("deg").expect("built-in angle unit");
    casacore::get_conformant_value(&angle, &degrees).map_err(raised)
}

struct ChannelColumns<'a> {
    spectral: &'a SpectralCoordinate,
    channel_width: usize,
    frequency_width: usize,
    frequency_precision: usize,
    velocity_precision: usize,
    doppler: DopplerRef,
    velocity_scale: f64,
}

impl<'a> ChannelColumns<'a> {
    fn new(
        spectral: &'a SpectralCoordinate,
        record: Option<&RecordValue>,
        count: usize,
    ) -> PyResult<Self> {
        let doppler =
            DopplerRef::from_casacore_code(spectral_text::integer_field(record, "velType"))
                .unwrap_or(DopplerRef::RADIO);
        let name = match record.and_then(|record| record.get("velUnit")) {
            Some(Value::Scalar(ScalarValue::String(unit))) if !unit.is_empty() => unit,
            _ => "km/s",
        };
        let km_per_second = Quantity::new(1.0, "km/s").expect("built-in velocity unit");
        let unit = Unit::new(name).map_err(raised)?;
        let velocity_scale = km_per_second.get_value_in(&unit).map_err(raised)?;
        let frequency = spectral.to_world(&[0.0]).map_err(raised)?[0];
        let (frequency_precision, velocity_precision) = if spectral.has_linear_mapping() {
            let rest = spectral.rest_frequency();
            let v0 = velocity(frequency, rest, doppler);
            let v1 = velocity(spectral.to_world(&[1.0]).map_err(raised)?[0], rest, doppler);
            let step = (v1 - v0).abs() * velocity_scale;
            let exponent = |x: f64| {
                let value = x.abs().log10().abs();
                if value.is_finite() { value as usize } else { 6 }
            };
            (
                exponent(spectral.increment()[0] / frequency) + 1,
                if step > 10.0 { 0 } else { exponent(step) + 2 },
            )
        } else {
            (6, 3)
        };
        Ok(Self {
            spectral,
            channel_width: count.to_string().len().max(4),
            frequency_width: scientific(frequency, frequency_precision).len(),
            frequency_precision,
            velocity_precision,
            doppler,
            velocity_scale,
        })
    }

    fn header(&self, report: &mut String) {
        write!(
            report,
            "{:>cw$} {:>fw$} {:>vw$}",
            "Chan",
            "Freq",
            "Vel",
            cw = self.channel_width,
            fw = self.frequency_width,
            vw = self.velocity_precision + 5
        )
        .unwrap();
    }

    fn row(&self, report: &mut String, channel: usize) -> PyResult<()> {
        let frequency = self.spectral.to_world(&[channel as f64]).map_err(raised)?[0];
        let speed =
            velocity(frequency, self.spectral.rest_frequency(), self.doppler) * self.velocity_scale;
        let frequency = scientific(frequency, self.frequency_precision);
        write!(
            report,
            "{channel:>cw$} {frequency} {speed:>vw$.prec$} ",
            cw = self.channel_width,
            vw = self.velocity_precision + 5,
            prec = self.velocity_precision
        )
        .unwrap();
        Ok(())
    }
}

fn beam_row(report: &mut String, beam: &GaussianBeam, unit: &str) -> PyResult<()> {
    let major = beam.major_in(unit).map_err(raised)?;
    let minor = beam.minor_in(unit).map_err(raised)?;
    let angle = normalized_angle(beam)?;
    write!(
        report,
        "{major:>9.4} {unit} x {minor:>9.4} {unit} pa={angle:>8.4} deg"
    )
    .unwrap();
    Ok(())
}

pub(super) fn append(
    report: &mut String,
    record: &RecordValue,
    coords: &CoordinateSystem,
    beams: &ImageBeamSet,
    verbose: bool,
) -> PyResult<()> {
    let count = beams.n_channels();
    let spectral = CoordinateSystem::first_spectral_in_record(record).map_err(raised)?;
    let stokes = coords
        .find_coordinate(CoordinateType::Stokes)
        .and_then(|index| {
            if let CoordinateModel::Stokes(stokes) = coords.coordinate(index) {
                Some(stokes.stokes())
            } else {
                None
            }
        });
    let polarization = |index: usize| {
        stokes
            .and_then(|kinds| kinds.get(index))
            .map_or("?", |kind| kind.name())
    };
    let columns = spectral
        .as_ref()
        .map(|(field, spectral)| ChannelColumns::new(spectral, subrecord(record, field), count))
        .transpose()?;
    let unit = if (0..beams.n_stokes()).any(|pol| {
        (0..count).any(|chan| {
            beams
                .beam(chan, pol)
                .minor_in("deg")
                .is_ok_and(|minor| minor < 1.0)
        })
    }) {
        "arcsec"
    } else {
        "deg"
    };
    report.push_str("\nRestoring Beams \n");
    if let Some(columns) = &columns
        && !verbose
        && count > 3
    {
        if stokes.is_some() {
            report.push_str("Pol ");
        }
        report.push_str("  Type ");
        columns.header(report);
        for pol in 0..beams.n_stokes() {
            let mut channels: Vec<_> = (0..count).collect();
            channels.sort_by(|&left, &right| {
                beams
                    .beam(left, pol)
                    .area()
                    .total_cmp(&beams.beam(right, pol).area())
            });
            let max_area = beams.beam(channels[count - 1], pol).area();
            let first_max =
                channels.partition_point(|&channel| beams.beam(channel, pol).area() < max_area);
            for (kind, index) in [("Max", first_max), ("Min", 0), ("Median", count / 2)] {
                let channel = channels[index];
                report.push('\n');
                if stokes.is_some() {
                    write!(report, "{:>3} ", polarization(pol)).unwrap();
                }
                write!(report, "{kind:>6} ").unwrap();
                columns.row(report, channel)?;
                beam_row(report, beams.beam(channel, pol), unit)?;
            }
        }
        return Ok(());
    }
    if let Some(columns) = &columns {
        columns.header(report);
    }
    if stokes.is_some() {
        report.push_str(" Pol");
    }
    for channel in 0..count {
        for pol in 0..beams.n_stokes() {
            report.push('\n');
            if let Some(columns) = &columns {
                columns.row(report, channel)?;
            }
            if stokes.is_some() {
                write!(report, "{:>3} ", polarization(pol)).unwrap();
            }
            beam_row(report, beams.beam(channel, pol), unit)?;
        }
    }
    Ok(())
}
