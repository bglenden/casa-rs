// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selection and coordinate projection shared by calibration plot exports.

use num_complex::Complex64;

use casa_plot_data::MsAverageSpec;

use super::averaging;
use super::axis::{Axis, SampleCoordinates};
use super::iteration::IterationAxis;
use super::selection::Selection;
use super::table_data::{CalibrationTable, Solution};

#[derive(Clone, Copy)]
pub(super) struct Point {
    pub(super) coordinates: SampleCoordinates,
    pub(super) x: f64,
    pub(super) y: f64,
    pub(super) flagged: bool,
}

pub(super) struct SeriesRequest<'a> {
    pub(super) x: Axis,
    pub(super) y: Axis,
    pub(super) polarization: &'a str,
    pub(super) averaging: &'a MsAverageSpec,
    pub(super) spline: Option<super::legacy::SplineDisplay>,
    pub(super) iteration: Option<IterationAxis>,
}

pub(super) fn points(
    table: &CalibrationTable,
    selection: &Selection,
    request: &SeriesRequest<'_>,
) -> Result<Vec<Point>, String> {
    let x_component = Component::new(request.x, table)?;
    let y_component = Component::new(request.y, table)?;
    let mut rows = (0..table.rows())
        .map(|row| table.coordinates(row))
        .collect::<Result<Vec<_>, _>>()?;
    rows.retain(|row| selection.includes(row));
    if rows.is_empty() {
        return Err("calibration selection contains no rows".into());
    }
    averaging::sort(&mut rows, request.averaging);
    let mut result = Vec::new();
    let axes = [x_component, y_component];
    if averaging::active(request.averaging) {
        for interval in averaging::intervals(&rows, request.averaging) {
            let interval = &rows[interval];
            let metadata = averaging::coordinates(interval);
            let frequencies =
                averaging::channel_frequencies(table, interval, selection, request.averaging)?;
            for (antennas, group) in averaging::row_groups(
                interval,
                request.averaging,
                table.subtype.contains("Mueller"),
            ) {
                let input = group
                    .iter()
                    .map(|row| table.solution(*row))
                    .collect::<Result<Vec<_>, _>>()?;
                let coordinates = SampleCoordinates {
                    antennas,
                    ..metadata
                };
                append_points(
                    table,
                    (&input, &frequencies),
                    coordinates,
                    selection,
                    request,
                    axes,
                    &mut result,
                )?;
            }
        }
    } else if request.spline.is_some()
        && !request.x.uses_spline_antenna()
        && !request.y.uses_spline_antenna()
        && !matches!(
            request.iteration,
            Some(IterationAxis::Antenna | IterationAxis::Baseline)
        )
    {
        for chunk in rows.chunk_by(|first, second| first.time == second.time) {
            let input = chunk
                .iter()
                .map(|row| table.solution(*row))
                .collect::<Result<Vec<_>, _>>()?;
            append_points(
                table,
                (&input, &[]),
                chunk[0],
                selection,
                request,
                axes,
                &mut result,
            )?;
        }
    } else {
        for coordinates in rows {
            let input = [table.solution(coordinates)?];
            append_points(
                table,
                (&input, &[]),
                coordinates,
                selection,
                request,
                axes,
                &mut result,
            )?;
        }
    }
    Ok(result)
}

fn append_points(
    table: &CalibrationTable,
    (input, averaged_frequencies): (&[Solution<'_>], &[f64]),
    row: SampleCoordinates,
    selection: &Selection,
    request: &SeriesRequest<'_>,
    [x, y]: [Component; 2],
    points: &mut Vec<Point>,
) -> Result<(), String> {
    let first = &input[0];
    let window = usize::try_from(first.coordinates.window)
        .ok()
        .and_then(|id| table.windows.get(id))
        .ok_or_else(|| {
            format!(
                "calibration row {} has unknown window {}",
                row.row, row.window
            )
        })?;
    let shape = first.values.shape();
    if input
        .iter()
        .any(|solution| solution.values.shape() != shape)
    {
        return Err("calibration solutions being averaged must have the same shape".into());
    }
    let receptors = shape[0] / x.stride;
    let mut polarizations = polarizations(table, request.polarization, receptors)?;
    if request.spline.is_some()
        && !x.axis.uses_receptor()
        && !y.axis.uses_receptor()
        && !matches!(
            request.iteration,
            Some(IterationAxis::Polarization | IterationAxis::AntennaPosition)
        )
    {
        polarizations.truncate(1);
    }
    let channels = selection.channels(first.coordinates.window, shape[1]);
    let size = request
        .averaging
        .avgchannel
        .unwrap_or(1)
        .min(channels.len().max(1));

    for (bin, members) in channels.chunks_exact(size).enumerate() {
        let mut coordinates = row;
        coordinates.channel = if request.averaging.avgchannel.is_some_and(|count| count > 1) {
            bin
        } else {
            members[0]
        };
        coordinates.frequency_ghz = averaged_frequencies.get(bin).copied().unwrap_or_else(|| {
            members
                .iter()
                .map(|&channel| {
                    window
                        .frequencies
                        .get(channel)
                        .copied()
                        .unwrap_or(window.reference)
                })
                .sum::<f64>()
                / (members.len() as f64 * 1e9)
        });
        if let Some(spline) = request.spline {
            spline.coordinates(&mut coordinates);
        }
        for &polarization in &polarizations {
            coordinates.receptor = polarization.output_index();
            let x_value = x.project(
                input,
                polarization,
                members,
                &coordinates,
                request.averaging,
            )?;
            let y_value = if x.axis == y.axis {
                x_value
            } else {
                y.project(
                    input,
                    polarization,
                    members,
                    &coordinates,
                    request.averaging,
                )?
            };
            points.push(Point {
                coordinates,
                x: x_value.0,
                y: y_value.0,
                flagged: x_value.1 || y_value.1,
            });
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Polarization {
    Receptor(usize),
    Ratio,
}

impl Polarization {
    fn output_index(self) -> i32 {
        match self {
            Self::Receptor(index) => index as i32,
            Self::Ratio => -1,
        }
    }
}

fn polarizations(
    table: &CalibrationTable,
    selection: &str,
    count: usize,
) -> Result<Vec<Polarization>, String> {
    use Polarization::{Ratio, Receptor};
    let indices: &[usize] = match selection.to_ascii_uppercase().as_str() {
        "" | "RL" | "XYZ" => {
            return Ok((0..count).map(Receptor).collect());
        }
        "R" | "X" => &[0],
        "L" | "Y" => &[1],
        "Z" if table.subtype.starts_with("KAntPos") => &[2],
        "XY" => &[0, 1],
        "XZ" if table.subtype.starts_with("KAntPos") => &[0, 2],
        "YZ" if table.subtype.starts_with("KAntPos") => &[1, 2],
        "/" if count == 2
            && !table.subtype.starts_with('T')
            && (!table.subtype.starts_with('F') || table.subtype == "Fringe Jones") =>
        {
            return Ok(vec![Ratio]);
        }
        _ => {
            return Err(format!(
                "invalid polarization selection {selection:?} for {}",
                table.subtype
            ));
        }
    };
    if indices.iter().any(|&index| index >= count) {
        return Err(format!(
            "invalid polarization selection {selection:?} for {}",
            table.subtype
        ));
    }
    Ok(indices.iter().copied().map(Receptor).collect())
}

#[derive(Clone, Copy)]
struct Component {
    axis: Axis,
    stride: usize,
    offset: usize,
    real: bool,
    fringe: bool,
    scale: f64,
}

impl Component {
    fn new(axis: Axis, table: &CalibrationTable) -> Result<Self, String> {
        let kind = table.subtype.as_str();
        let real = table.parameter_column == "FPARAM";
        let fringe = kind == "Fringe Jones";
        let (stride, offset) = if fringe {
            (
                4,
                match axis {
                    Axis::Phase | Axis::GainPhase => 0,
                    Axis::Rate => 2,
                    Axis::DispersiveDelay => 3,
                    _ => 1,
                },
            )
        } else if kind.contains("EVLASWPOW") {
            (
                3,
                if axis == Axis::SystemTemperature {
                    1
                } else {
                    0
                },
            )
        } else {
            (1, 0)
        };
        let valid = match axis {
            Axis::Phase | Axis::GainPhase => !real || fringe,
            Axis::Imaginary | Axis::GainImaginary => !real,
            Axis::Delay => real && (fringe || kind.starts_with("K Jones")),
            Axis::Rate | Axis::DispersiveDelay => fringe,
            Axis::SystemTemperature => {
                real && (kind.contains("TSYS") || kind.contains("EVLASWPOW"))
            }
            Axis::Opacity => real && kind.contains("Opac"),
            Axis::SwitchedPower => real && kind.contains("EVLASWPOW"),
            Axis::Tec => real && kind == "F Jones",
            Axis::AntennaPosition => real && kind.starts_with("KAntPos"),
            _ => true,
        };
        if !valid {
            return Err(format!(
                "{} has no meaning for {}",
                axis.label(),
                table.subtype
            ));
        }
        let scale = match axis {
            Axis::Rate => 1e12,
            Axis::DispersiveDelay => 1.0 / 1.334_537,
            Axis::Tec => 1e-16,
            Axis::Amplitude | Axis::GainAmplitude if kind.starts_with('F') => 1e-16,
            _ => 1.0,
        };
        Ok(Self {
            axis,
            stride,
            offset,
            real,
            fringe,
            scale,
        })
    }

    fn project(
        self,
        input: &[Solution<'_>],
        polarization: Polarization,
        channels: &[usize],
        coordinates: &SampleCoordinates,
        average: &MsAverageSpec,
    ) -> Result<(f64, bool), String> {
        let receptor = |index| self.stride * index + self.offset;
        let average = |index| average_cell(input, receptor(index), channels, average, self.real);
        let (value, snr, flagged) = match polarization {
            Polarization::Receptor(index) => average(index)?,
            Polarization::Ratio => {
                let (first, first_snr, first_flag) = average(0)?;
                let (second, second_snr, second_flag) = average(1)?;
                let subtract = self.axis == Axis::Delay || self.fringe;
                let div_zero = !subtract && second.norm_sqr() == 0.0;
                let value = if subtract {
                    first - second
                } else if div_zero {
                    Complex64::new(1.0, 0.0)
                } else {
                    first / second
                };
                (
                    value,
                    first_snr / second_snr,
                    div_zero || first_flag || second_flag,
                )
            }
        };
        let projected = match self.axis {
            Axis::Flag => f64::from(flagged),
            Axis::Amplitude | Axis::GainAmplitude if self.real => value.re,
            Axis::Phase | Axis::GainPhase if self.fringe => value.re.to_degrees(),
            _ => self.axis.value(coordinates, value, snr),
        };
        Ok((projected * self.scale, flagged))
    }
}

fn average_cell(
    input: &[Solution<'_>],
    receptor: usize,
    channels: &[usize],
    average: &MsAverageSpec,
    real: bool,
) -> Result<(Complex64, f64, bool), String> {
    let scalar = average.scalar && !real;
    let mut accumulator = CellAverage::default();
    for solution in input {
        for &channel in channels {
            let mut value = solution.value(receptor, channel).ok_or_else(|| {
                format!(
                    "invalid calibration parameters for row {}",
                    solution.coordinates.row
                )
            })?;
            if scalar {
                value = Complex64::new(value.norm(), value.arg());
            }
            accumulator.add(
                value,
                solution.snr(receptor, channel).unwrap_or(f64::NAN),
                solution.weight(receptor, channel),
                solution.flagged(receptor, channel),
            );
        }
    }
    Ok(accumulator.result())
}

struct CellAverage {
    value: Complex64,
    snr: f64,
    weight: f64,
    flagged: bool,
}

impl Default for CellAverage {
    fn default() -> Self {
        Self {
            value: Complex64::default(),
            snr: 0.0,
            weight: 0.0,
            flagged: true,
        }
    }
}

impl CellAverage {
    fn add(&mut self, value: Complex64, snr: f64, weight: f64, flagged: bool) {
        if flagged && !self.flagged {
            return;
        }
        if !flagged && self.flagged {
            *self = Self {
                flagged: false,
                ..Self::default()
            };
        }
        self.value += value * weight;
        self.snr += snr * weight;
        self.weight += weight;
    }

    fn result(self) -> (Complex64, f64, bool) {
        (
            self.value / self.weight,
            self.snr / self.weight,
            self.flagged,
        )
    }
}
