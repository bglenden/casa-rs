// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fmt::Write as _;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write as _};

use crate::convert::value::CasaValue;
use crate::tools::LogLine;
use casa_coordinates::display::{
    format_latitude_dms_with_precision, format_longitude_hours_with_precision, format_stream_value,
    format_stream_value_with_precision,
};
use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType, StokesType};
use casa_core::quanta::Unit;
use casa_lattices::{StatisticsAlgorithm, StatisticsAlgorithmKind};

use super::{Chunk, SpectralVelocity};

const COLUMNS: [(&str, &str); 7] = [
    ("Npts", "npts"),
    ("Sum", "sum"),
    ("Mean", "mean"),
    ("Rms", "rms"),
    ("Std_dev", "sigma"),
    ("Minimum", "min"),
    ("Maximum", "max"),
];

pub(super) struct StatisticsReport<'a> {
    pub(super) coordinates: &'a CoordinateSystem,
    pub(super) spectral_velocity: Option<&'a SpectralVelocity>,
    pub(super) chunk: &'a Chunk,
    pub(super) collapsed_axes: &'a [usize],
    pub(super) values: &'a CasaValue,
    pub(super) iterations: &'a [usize],
    pub(super) units: &'a str,
    pub(super) algorithm: StatisticsAlgorithm,
}

impl StatisticsReport<'_> {
    fn field(&self, key: &str) -> Option<&CasaValue> {
        let CasaValue::Record(fields) = self.values else {
            return None;
        };
        fields
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value)
    }

    fn numbers(&self, key: &str) -> &[f64] {
        match self.field(key) {
            Some(CasaValue::Floats { values, .. }) => values,
            _ => &[],
        }
    }

    fn position(&self, key: &str) -> Option<String> {
        match self.field(key)? {
            CasaValue::Text(text) => Some(text.clone()),
            CasaValue::Ints { values, .. } => Some(format!("{values:?}")),
            _ => None,
        }
    }

    fn summary(&self, global: bool) -> Vec<String> {
        let algorithm = match self.algorithm.kind() {
            StatisticsAlgorithmKind::Classic => "Classic",
            StatisticsAlgorithmKind::HingesFences => "Hinges-Fences",
            StatisticsAlgorithmKind::Chauvenet => "Chauvenet",
            StatisticsAlgorithmKind::FitToHalf => "Fit-to-Half",
            StatisticsAlgorithmKind::Biweight => "Biweight",
        };
        let mut lines = vec![
            format!("Statistics calculated using {algorithm} algorithm"),
            "Regions --- ".into(),
        ];
        for (key, prefix) in [
            ("blc", "         -- bottom-left corner (pixel) [blc]:  "),
            ("trc", "         -- top-right corner (pixel) [trc]:    "),
            ("blcf", "         -- bottom-left corner (world) [blcf]: "),
            ("trcf", "         -- top-right corner (world) [trcf]:   "),
        ] {
            if let Some(value) = self.position(key) {
                lines.push(format!("{prefix}{value}"));
            }
        }
        if !global || self.numbers("npts").is_empty() {
            return lines;
        }
        lines.push("Values --- ".into());
        if let Some(&flux) = self.numbers("flux").first() {
            lines.push(self.flux_summary(flux));
        }
        let sumsq_unit = if self.units.is_empty() {
            String::new()
        } else {
            format!("{}.{}", self.units, self.units)
        };
        let groups = [
            (
                "npts",
                "         -- number of points [npts]:                ",
                None,
            ),
            (
                "max",
                "         -- maximum value [max]:                    ",
                Some(self.units),
            ),
            (
                "min",
                "         -- minimum value [min]:                    ",
                Some(self.units),
            ),
            (
                "maxpos",
                "         -- position of max value (pixel) [maxpos]: ",
                None,
            ),
            (
                "minpos",
                "         -- position of min value (pixel) [minpos]: ",
                None,
            ),
            (
                "maxposf",
                "         -- position of max value (world) [maxposf]: ",
                None,
            ),
            (
                "minposf",
                "         -- position of min value (world) [minposf]: ",
                None,
            ),
            (
                "sum",
                "         -- Sum of pixel values [sum]:               ",
                Some(self.units),
            ),
            (
                "sumsq",
                "         -- Sum of squared pixel values [sumsq]:     ",
                Some(sumsq_unit.as_str()),
            ),
        ];
        for (name, prefix, unit) in groups {
            if let Some(&number) = self.numbers(name).first() {
                lines.push(summary_number(prefix, number, unit));
            } else if let Some(position) = self.position(name) {
                lines.push(format!("{prefix}{position}"));
            }
        }
        lines.push("Statistics --- ".into());
        for (name, prefix) in [
            (
                "mean",
                "        -- Mean of the pixel values [mean]:         ",
            ),
            (
                "variance",
                "        -- Variance of the pixel values :           ",
            ),
            (
                "sigma",
                "        -- Standard deviation of the Mean [sigma]:  ",
            ),
            (
                "rms",
                "        -- Root mean square [rms]:                  ",
            ),
            (
                "median",
                "        -- Median of the pixel values [median]:     ",
            ),
            (
                "medabsdevmed",
                "        -- Median of the deviations [medabsdevmed]: ",
            ),
            (
                "quartile",
                "        -- IQR [quartile]:                          ",
            ),
            ("q1", "        -- First quartile [q1]:                     "),
            ("q3", "        -- Third quartile [q3]:                     "),
        ] {
            let value = if name == "variance"
                && !matches!(self.algorithm, StatisticsAlgorithm::Biweight { .. })
            {
                self.numbers("sigma").first().map(|sigma| sigma * sigma)
            } else {
                self.numbers(name).first().copied()
            };
            if let Some(value) = value {
                lines.push(summary_number(prefix, value, Some(self.units)));
            }
        }
        lines
    }

    fn flux_summary(&self, flux: f64) -> String {
        let mut unit = self.units.split_once("/beam").map_or_else(
            || self.units.to_owned(),
            |(prefix, suffix)| format!("{prefix}{suffix}"),
        );
        if self.units.contains('K') {
            unit.push_str(".arcsec2");
        }
        if let Some(index) = self.coordinates.find_coordinate(CoordinateType::Spectral)
            && let CoordinateModel::Spectral(spectral) = self.coordinates.coordinate(index)
            && let [Some(axis)] = self
                .coordinates
                .coordinate_pixel_axes(index)
                .expect("existing coordinate")
                .as_slice()
            && self.chunk.size[*axis] > 1
        {
            unit.push('.');
            if spectral.rest_frequency() > 0.0 {
                unit.push_str(
                    self.spectral_velocity
                        .map_or("km/s", |velocity| velocity.unit.name()),
                );
            } else {
                unit.push_str(&self.coordinates.coordinate(index).axis_units()[0]);
            }
            return summary_number(
                "         -- flux [flux]:                            ",
                flux,
                Some(&unit),
            );
        }
        let integrated = Unit::new(&unit).is_ok_and(|actual| {
            ["Jy.km/s", "Jy.Hz", "K.arcsec2.km/s", "K.arcsec2.Hz"]
                .into_iter()
                .any(|name| Unit::new(name).is_ok_and(|target| actual.conformant(&target)))
        });
        let label = if integrated {
            "         -- flux [flux]:                            "
        } else {
            "         -- flux density [flux]:                    "
        };
        summary_number(label, flux, Some(&unit))
    }

    pub(super) fn publish(
        &self,
        path: &str,
        append: bool,
        list: bool,
        verbose: bool,
        log: &mut Vec<LogLine>,
    ) {
        let columns = self.axis_columns();
        let mut file = (!path.is_empty()).then(|| {
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(append)
                .truncate(!append)
                .open(path)
                .map(BufWriter::new)
        });
        let mut write_line = |prefix: &str, line: &str, suffix: &str| {
            if let Some(Ok(writer)) = &mut file
                && let Err(error) = write!(writer, "{prefix}{line}{suffix}")
            {
                file = Some(Err(error));
            }
        };
        if list {
            let global = self.collapsed_axes.is_empty()
                || self.collapsed_axes.len() == self.chunk.size.len();
            for line in self.summary(global) {
                write_line("# ", &line, "\n");
                log.push(LogLine::info(line));
            }
        }
        if self.numbers("npts").is_empty() {
            let message = "NO UNMASKED POINTS FOUND, NO STATISTICS WERE COMPUTED";
            write_line("", message, "");
            log.push(LogLine::warning(message));
        } else if !path.is_empty() || verbose {
            self.details(&columns, |line, header| {
                write_line(if header { "#" } else { " " }, &line, "\n");
                if verbose {
                    log.push(LogLine::info(line));
                }
            });
        }
        if let Some(Err(error)) = file.map(|writer| writer.and_then(|mut writer| writer.flush())) {
            log.push(LogLine::warning(format!(
                "Requested log file {path} could not be written: {error}"
            )));
        }
    }

    fn axis_columns(&self) -> Vec<AxisColumn<'_>> {
        let mut columns = Vec::new();
        for axis in 0..self.coordinates.n_pixel_axes() {
            if self.collapsed_axes.is_empty()
                || self.collapsed_axes.contains(&axis)
                || self.chunk.size[axis] <= 1
            {
                continue;
            }
            let (index, local) = self
                .coordinates
                .pixel_axis_owner(axis)
                .expect("dense pixel axes");
            let model = self.coordinates.coordinate(index);
            let label = model.axis_names().remove(local).to_ascii_uppercase();
            let velocity = self
                .spectral_velocity
                .filter(|_| label == "FREQUENCY" && matches!(model, CoordinateModel::Spectral(_)));
            columns.push(AxisColumn {
                axis,
                model,
                label,
                velocity,
            });
        }
        columns.sort_by_key(|column| match column.label.as_str() {
            "RIGHT ASCENSION" => (0, 0),
            "DECLINATION" => (1, 0),
            "FREQUENCY" => (2, 0),
            "STOKES" => (3, 0),
            _ => (4, usize::MAX - column.axis),
        });
        columns
    }

    fn details(&self, columns: &[AxisColumn<'_>], mut line: impl FnMut(String, bool)) {
        let npts = self.numbers("npts");
        let stats: Vec<_> = COLUMNS
            .iter()
            .filter_map(|&(label, key)| self.field(key).map(|_| (label, self.numbers(key))))
            .collect();
        let mut header = String::new();
        for column in columns {
            if column.label == "FREQUENCY" {
                let unit = if let Some(velocity) = column.velocity {
                    velocity.unit.name().to_owned()
                } else {
                    column.model.axis_units().remove(0)
                };
                line(format!("{} column unit = {unit}", column.name()), true);
            }
            let label = column.name();
            let width = column.width();
            let _ = write!(header, "{label:>width$}  {label}(Plane) ");
        }
        for (index, &(label, _)) in stats.iter().enumerate() {
            if !matches!(label, "Npts" | "Rms") {
                line(format!("{label} column unit = {}", self.units), true);
            }
            let width = if index + 1 == stats.len() { 12 } else { 14 };
            let _ = write!(header, "{label:<width$}");
        }
        if !self.iterations.is_empty() {
            header.push_str("  N Iter");
        }
        line(header, true);

        let mut reference = self.coordinates.reference_pixel();
        for &axis in self.collapsed_axes {
            reference[axis] = self.chunk.start[axis] as f64;
        }
        let labels: Vec<Vec<_>> = columns
            .iter()
            .map(|column| {
                (0..self.chunk.size[column.axis])
                    .map(|plane| {
                        let absolute =
                            self.chunk.start[column.axis] + plane * self.chunk.stride[column.axis];
                        let mut pixel = reference.clone();
                        pixel[column.axis] = absolute as f64;
                        (column.label_at(self.coordinates, &pixel), absolute)
                    })
                    .collect()
            })
            .collect();
        let mut strides = vec![0; self.chunk.size.len()];
        let mut stride = 1;
        for (axis, step) in strides.iter_mut().enumerate() {
            if columns.iter().any(|column| column.axis == axis) {
                *step = stride;
                stride *= self.chunk.size[axis];
            }
        }
        let mut position = vec![0; columns.len()];
        let mut iteration_order: Vec<_> = (0..columns.len()).collect();
        iteration_order.sort_by_key(|&index| match columns[index].label.as_str() {
            "STOKES" => 0,
            "FREQUENCY" => 1,
            "DECLINATION" => 2,
            "RIGHT ASCENSION" => 3,
            _ => 4,
        });
        for _ in 0..npts.len() {
            let index = columns
                .iter()
                .zip(&position)
                .map(|(column, &plane)| plane * strides[column.axis])
                .sum::<usize>();
            if npts[index] > 0.0 {
                let mut row = String::new();
                for ((column, labels), &plane) in columns.iter().zip(&labels).zip(&position) {
                    let (text, pixel) = &labels[plane];
                    let width = column.width();
                    let plane_width = column.name().len() + 8;
                    let _ = write!(row, "{text:>width$} {pixel:>plane_width$} ");
                }
                for (cell, &(_, values)) in stats.iter().enumerate() {
                    if cell != 0 {
                        row.push(' ');
                    }
                    let text = scientific(values[index]);
                    let _ = write!(row, "{text:>13}");
                }
                if let Some(count) = self.iterations.get(index) {
                    let _ = write!(row, "      {count}");
                }
                line(row, false);
            }
            for &axis in &iteration_order {
                let plane = &mut position[axis];
                *plane += 1;
                if *plane < labels[axis].len() {
                    break;
                }
                *plane = 0;
            }
        }
    }
}

fn summary_number(prefix: &str, value: f64, unit: Option<&str>) -> String {
    let value = format_stream_value(value);
    match unit {
        Some(unit) => format!("{prefix}{value} {unit}"),
        None => format!("{prefix}{value}"),
    }
}

fn scientific(value: f64) -> String {
    let text = format!("{value:.6e}");
    let Some((mantissa, exponent)) = text.split_once('e') else {
        return text.to_ascii_lowercase();
    };
    let exponent = exponent.parse::<i32>().expect("scientific exponent");
    format!("{mantissa}e{exponent:+03}")
}

struct AxisColumn<'a> {
    axis: usize,
    model: &'a CoordinateModel,
    label: String,
    velocity: Option<&'a SpectralVelocity>,
}

impl AxisColumn<'_> {
    fn name(&self) -> &str {
        if self.velocity.is_some() {
            "VELOCITY"
        } else {
            &self.label
        }
    }

    fn width(&self) -> usize {
        match self.label.as_str() {
            "STOKES" => 6,
            "FREQUENCY" => 16,
            _ => 15,
        }
    }

    fn label_at(&self, coordinates: &CoordinateSystem, pixel: &[f64]) -> String {
        let Ok(world) = coordinates.to_world(pixel) else {
            return "?".into();
        };
        let Some(axis) = coordinates.pixel_axis_to_world_axis(self.axis) else {
            return "?".into();
        };
        let value = world[axis];
        match self.model {
            CoordinateModel::Direction(_) => match self.label.as_str() {
                "RIGHT ASCENSION" => format_longitude_hours_with_precision(value, 5),
                "DECLINATION" => format_latitude_dms_with_precision(value, 5),
                _ => format!("{:.5}", value.to_degrees()),
            },
            CoordinateModel::Stokes(_) => StokesType::from_code(value as i32)
                .map_or_else(|| value.to_string(), |stokes| stokes.name().into()),
            CoordinateModel::Spectral(spectral) => {
                if let Some(velocity) = self.velocity {
                    let ratio = value.abs() / spectral.rest_frequency();
                    velocity.velocity_for_ratio(ratio).map_or_else(
                        || "?".into(),
                        |shift| format_stream_value_with_precision(shift, 9),
                    )
                } else {
                    format_stream_value_with_precision(value, 9)
                }
            }
            _ => format_stream_value_with_precision(value, 5),
        }
    }
}
