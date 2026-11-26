// SPDX-License-Identifier: LGPL-3.0-or-later
//! Render the calibration listing and preserve its integration boundaries.

use std::fmt::Write;
use std::path::Path;

use casa_core::ArrayValue;
use casa_core::quanta::format::{YMD, format_time};

use super::{CalibrationListing, ListingPart, Solutions};

const SOLUTION_WIDTH: usize = 15;

pub(super) struct Window<'a> {
    subtype: &'a str,
    complex: bool,
    receptors: usize,
    field_width: usize,
    channel_width: usize,
}

impl<'a> Window<'a> {
    pub(super) fn new(
        subtype: &'a str,
        complex: bool,
        receptors: usize,
        channels: usize,
        field_width: usize,
    ) -> Self {
        let channel_width = ((channels as f64).log10() + 0.5).round_ties_even().max(3.0) as usize;
        Self {
            subtype,
            complex,
            receptors,
            field_width,
            channel_width,
        }
    }

    pub(super) fn antennas_per_line(&self) -> usize {
        (8 / self.receptors).max(1)
    }

    fn prefix_width(&self) -> usize {
        12 + self.field_width + self.channel_width
    }

    fn total_width(&self) -> usize {
        self.prefix_width() + 1 + SOLUTION_WIDTH * self.receptors * self.antennas_per_line()
    }

    fn labels(&self, param: usize) -> (&str, &str) {
        let name = self.subtype;
        let amp = if name.contains("EVLAGAIN") {
            ["Gain", "Tsys"][param % 2]
        } else if name == "K Jones" {
            "Delay "
        } else if name.contains("Opac") {
            "Opac "
        } else if name.contains("KAntPos") {
            ["dX m", "dY m", "dZ m"][param % 3]
        } else if name.contains("Fringe Jones") {
            ["Ph deg", "Del ns", "Rat ps/s", "DDel uHz"][param % 4]
        } else if name.contains("EGainCurve") || name.contains("EPowerCurve") {
            const COEFFICIENTS: [&str; 8] = [
                "c[0]", "c[1]", "c[2]", "c[3]", "c[4]", "c[5]", "c[6]", "c[7]",
            ];
            COEFFICIENTS[param % if name.contains("EGainCurve") { 4 } else { 8 }]
        } else if name.contains("TSYS") {
            "Tsys"
        } else {
            return ("Amp ", "Phs ");
        };
        (amp, " ")
    }

    fn antenna_heading(&self, text: &mut String, names: &[String], antennas: &[usize]) {
        write!(text, "{:width$}|", "", width = self.prefix_width()).unwrap();
        let antenna_width = SOLUTION_WIDTH * self.receptors - 1;
        for &antenna in antennas {
            write!(
                text,
                "{:<antenna_width$}|",
                format!(" Ant = {}", names[antenna])
            )
            .unwrap();
        }
        text.push('\n');
        self.prefix(text, "Time", "Field", "Chn");
        for _ in antennas {
            for param in 0..self.receptors {
                if param > 0 {
                    text.push(' ');
                }
                let (value, phase) = self.labels(param);
                if self.complex {
                    write!(text, "{value:>5} {phase:>6} F").unwrap();
                } else {
                    write!(text, "{value:>10} {phase:>1} F").unwrap();
                }
            }
            text.push('|');
        }
        text.push('\n');
        let mut separator = vec![
            "-".repeat(10),
            "-".repeat(self.field_width),
            "-".repeat(self.channel_width),
        ];
        separator.extend(std::iter::repeat_n(
            "-".repeat(SOLUTION_WIDTH - 1),
            self.receptors * self.antennas_per_line(),
        ));
        writeln!(text, "{}|", separator.join("|")).unwrap();
    }

    fn prefix(&self, text: &mut String, time: &str, field: &str, channel: &str) {
        write!(
            text,
            "{time:<10} {field:<width$} {channel:<channels$}|",
            width = self.field_width,
            channels = self.channel_width
        )
        .unwrap();
    }

    fn data(&self, text: &mut String, line: Line<'_>) -> Result<(), String> {
        let time = date_time(line.time);
        let time = time
            .get(11..)
            .ok_or("invalid time in calibration listing")?;
        write!(
            text,
            "{time:<10} {:>width$} {:>channels$}|",
            line.field,
            line.channel,
            width = self.field_width,
            channels = self.channel_width
        )
        .unwrap();
        for solution in line.solutions {
            let Some((value, flag)) = solution else {
                text.push_str(&" ".repeat(SOLUTION_WIDTH * self.receptors));
                continue;
            };
            let ArrayValue::Bool(flags) = flag else {
                return Err("calibration FLAG must be a boolean array".into());
            };
            for param in 0..self.receptors {
                let index = [param, line.channel];
                let flagged = flags
                    .get(index)
                    .copied()
                    .ok_or("calibration FLAG dimensions do not match the listing")?;
                let mark = if flagged { 'F' } else { ' ' };
                match value {
                    ArrayValue::Complex32(values) => {
                        let gain = values
                            .get(index)
                            .ok_or("CPARAM dimensions do not match the listing")?;
                        write_parameter(text, gain.norm(), 5, 3);
                        text.push(' ');
                        write_parameter(text, gain.arg().to_degrees(), 6, 1);
                        write!(text, " {mark} ").unwrap();
                    }
                    ArrayValue::Float32(values) => {
                        let value = *values
                            .get(index)
                            .ok_or("FPARAM dimensions do not match the listing")?;
                        let printed = if self.subtype.contains("Fringe Jones") {
                            fringe_value(value, param)
                        } else {
                            value
                        };
                        write_parameter(text, printed, 10, 5);
                        write!(text, "   {mark} ").unwrap();
                    }
                    _ => return Err("unsupported calibration parameter array".into()),
                }
            }
        }
        text.push('\n');
        Ok(())
    }
}

fn write_parameter(text: &mut String, value: f32, width: usize, precision: usize) {
    if value.is_nan() {
        let nan = if value.is_sign_negative() {
            "-nan"
        } else {
            "nan"
        };
        write!(text, "{nan:>width$}").unwrap();
    } else {
        write!(text, "{value:>width$.precision$}").unwrap();
    }
}

fn fringe_value(value: f32, param: usize) -> f32 {
    match param % 4 {
        0 => {
            let pi = std::f32::consts::PI;
            ((value + pi).rem_euclid(2.0 * pi) - pi).to_degrees()
        }
        2 => value / 1e-12,
        _ => value,
    }
}

fn date_time(seconds: f64) -> String {
    format_time(seconds / 86_400.0, YMD, 7)
}

pub(super) struct Line<'a> {
    pub time: f64,
    pub field: &'a str,
    pub channel: usize,
    pub solutions: &'a Solutions<'a>,
}

pub(super) struct Listing {
    parts: Vec<ListingPart>,
    text: String,
    solutions: usize,
    screen_rows: usize,
    page_rows: usize,
}

impl Listing {
    pub(super) fn new(page_rows: i32) -> Self {
        Self {
            parts: Vec::new(),
            text: String::new(),
            solutions: 0,
            screen_rows: 0,
            page_rows: usize::try_from(page_rows).unwrap_or(0),
        }
    }

    pub(super) fn start_window(
        &mut self,
        window: &Window<'_>,
        spw: usize,
        time: f64,
        caltable: &Path,
        ms: Option<&Path>,
    ) {
        let date_time = date_time(time);
        let date = date_time.get(..10).unwrap_or(&date_time);
        writeln!(
            self.text,
            "\nSpwID = {spw}, Date = {date},  CalTable = {} ({}), MS name = {}\n{}",
            caltable.display(),
            window.subtype,
            ms.unwrap_or(Path::new("none")).display(),
            "-".repeat(window.total_width())
        )
        .unwrap();
        self.screen_rows += 2;
    }

    pub(super) fn row(
        &mut self,
        window: &Window<'_>,
        names: &[String],
        antennas: &[usize],
        first_heading: &mut bool,
        line: Line<'_>,
    ) -> Result<(), String> {
        if self.screen_rows == 0 || *first_heading {
            *first_heading = false;
            window.antenna_heading(&mut self.text, names, antennas);
            self.screen_rows += 3;
        }
        window.data(&mut self.text, line)?;
        self.solutions += window.antennas_per_line();
        self.screen_rows += 1;
        if self.page_rows > 0 && self.screen_rows >= self.page_rows - 1 {
            self.screen_rows = 0;
            self.flush(true, false);
        }
        Ok(())
    }

    pub(super) fn end_integration(&mut self) {
        if self.text.is_empty() {
            if let Some(part) = self.parts.last_mut() {
                part.last_in_integration = true;
            }
        } else {
            self.flush(false, true);
        }
    }

    fn flush(&mut self, prompt: bool, last_in_integration: bool) {
        self.parts.push(ListingPart {
            text: std::mem::take(&mut self.text),
            prompt,
            last_in_integration,
            solutions: self.solutions,
        });
    }

    pub(super) fn finish(mut self, error: Option<String>) -> CalibrationListing {
        if !self.text.is_empty() {
            self.flush(false, error.is_none());
        }
        CalibrationListing {
            parts: self.parts,
            solutions: self.solutions,
            error,
        }
    }
}
