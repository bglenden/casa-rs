// SPDX-License-Identifier: LGPL-3.0-or-later

use std::io::{BufRead, Write};

use casa_core::quanta::format::{NO_TIME, TIME, YMD, format_time};

use super::{ListingChunk, ListingSpectra, ListingWindow, listing_error, valid_index};
use crate::MsResult;

const DAY_SECONDS: f64 = 86_400.0;
const PROMPT: &str =
    "Type Q to quit, A to toggle long/short list, or RETURN to continue [continue]: ";

pub(super) struct ListingState<'a> {
    output: &'a mut dyn Write,
    input: Option<&'a mut dyn BufRead>,
    show_page_prompt: bool,
    page_rows: usize,
    date: String,
    fixed: bool,
}

impl<'a> ListingState<'a> {
    pub(super) fn new(
        output: &'a mut dyn Write,
        input: Option<&'a mut dyn BufRead>,
        page_rows: i32,
    ) -> Self {
        Self {
            output,
            input,
            show_page_prompt: true,
            page_rows: page_rows.unsigned_abs() as usize,
            date: String::new(),
            fixed: false,
        }
    }

    fn line(&mut self, text: &str) -> MsResult<()> {
        writeln!(self.output, "{text}").map_err(io_error)
    }

    fn prompt(&mut self) -> MsResult<bool> {
        let Some(input) = self.input.as_deref_mut() else {
            return Ok(true);
        };
        write!(self.output, "{PROMPT}").map_err(io_error)?;
        self.output.flush().map_err(io_error)?;
        let mut command = String::new();
        input.read_line(&mut command).map_err(io_error)?;
        if matches!(command.as_bytes().first(), Some(b'a' | b'A')) {
            self.show_page_prompt = !self.show_page_prompt;
        }
        Ok(!matches!(command.as_bytes().first(), Some(b'q' | b'Q')))
    }

    fn display_value(&self, value: f64) -> String {
        if self.fixed {
            format!("{value:.2}")
        } else {
            format!("{value}")
        }
    }
}

pub(super) fn report_chunk(
    state: &mut ListingState<'_>,
    chunk: &ListingChunk<'_>,
    spectra: &ListingSpectra,
    antenna_names: &[String],
) -> MsResult<bool> {
    let description = chunk.descriptions[0];
    let window = spectra
        .windows
        .get(&description)
        .ok_or_else(|| listing_error(format!("unknown data description {description}")))?;
    let geometry = Geometry::build(chunk, window, antenna_names)?;
    let widths = Widths::new(chunk, &geometry, spectra.multiple_channels);
    state.line("Units of columns are: Date/Time(YYMMDD/HH:MM:SS UT), UVDist(wavelength), Phase(deg), UVW(m)")?;
    if let Some(value) = constant(chunk.amplitude.iter().copied().map(f64::from)) {
        state.line(&format!(
            "All selected data has AMPLITUDE = {}",
            state.display_value(value)
        ))?;
    }
    if let Some(value) = chunk
        .phase
        .and_then(|phase| constant(phase.iter().map(|value| phase_degrees(*value).abs())))
    {
        state.line(&format!(
            "All selected data has PHASE = {}",
            state.display_value(value)
        ))?;
    }
    if let Some(value) = constant(chunk.weight.iter().map(|value| f64::from(value.abs()))) {
        state.line(&format!("WEIGHT: {}", state.display_value(value)))?;
    }
    if widths.field == 0 {
        state.line(&format!("FIELD: {}", chunk.fields[0]))?;
    }
    if widths.spw == 0 {
        state.line(&format!("SPW: {description}"))?;
    }
    if widths.channel == 0
        && let Some(channel) = window.channels.first()
    {
        state.line(&format!("CHANNEL: {channel}"))?;
    }

    let rule = widths.separator(window.correlations.len());
    if state.page_rows == 0 {
        header(state, window, &widths, &rule)?;
    }
    let mut count = 0usize;
    for (row, &seconds) in chunk.times.iter().enumerate() {
        let time = seconds / DAY_SECONDS;
        state.date = format_time(time, YMD | NO_TIME, 0);
        let timestamp = format_time(time, TIME, 7);
        let window = spectra
            .windows
            .get(&chunk.descriptions[row])
            .ok_or_else(|| listing_error("unknown data description in listing"))?;
        for &channel in &window.channels {
            if state.page_rows != 0 && count.is_multiple_of(state.page_rows) {
                if count != 0 && state.show_page_prompt && !state.prompt()? {
                    return Ok(false);
                }
                header(state, window, &widths, &rule)?;
            }
            print_row(
                state, chunk, &geometry, window, &widths, &timestamp, row, channel,
            )?;
            state.fixed = true;
            count += 1;
        }
    }
    state.line(&rule)?;
    state.prompt()
}

fn phase_degrees(value: f32) -> f64 {
    f64::from(f64::from(value).to_degrees() as f32)
}

struct Geometry<'a> {
    ant1: Vec<&'a str>,
    ant2: Vec<&'a str>,
    distance: Vec<f64>,
}

impl<'a> Geometry<'a> {
    fn build(
        chunk: &ListingChunk<'_>,
        window: &ListingWindow,
        names: &'a [String],
    ) -> MsResult<Self> {
        let name = |index: &i32| -> MsResult<&'a str> {
            names
                .get(valid_index(*index)?)
                .map(String::as_str)
                .ok_or_else(|| listing_error(format!("unknown antenna {index}")))
        };
        let ant1 = chunk.antenna1.iter().map(name).collect::<MsResult<_>>()?;
        let ant2 = chunk.antenna2.iter().map(name).collect::<MsResult<_>>()?;
        let distance = (0..chunk.times.len())
            .map(|row| {
                let u = chunk.uvw[[0, row]];
                let v = chunk.uvw[[1, row]];
                (u * u + v * v).sqrt() * window.uvdist_scale
            })
            .collect();
        Ok(Self {
            ant1,
            ant2,
            distance,
        })
    }
}

struct Widths {
    ant1: usize,
    ant2: usize,
    distance: usize,
    field: usize,
    spw: usize,
    channel: usize,
    amplitude: usize,
    phase: usize,
    weight: usize,
    uvw: usize,
}

impl Widths {
    fn new(chunk: &ListingChunk<'_>, geometry: &Geometry<'_>, multi_channel: bool) -> Self {
        Self {
            ant1: geometry
                .ant1
                .iter()
                .map(|name| name.len())
                .max()
                .unwrap_or(0)
                + 1,
            ant2: geometry
                .ant2
                .iter()
                .map(|name| name.len())
                .max()
                .unwrap_or(0),
            distance: digits(geometry.distance.iter().copied()).max(6) + 1,
            field: index_width(chunk.fields.iter().copied()),
            spw: index_width(chunk.descriptions.iter().copied()),
            channel: usize::from(multi_channel) * 5,
            amplitude: {
                let sign = usize::from(chunk.amplitude.iter().any(|value| *value < 0.0));
                let magnitude = chunk.amplitude.iter().map(|value| f64::from(value.abs()));
                digits(magnitude) + 5 + sign
            },
            phase: chunk.phase.map_or(0, |phase| {
                let sign_and_border = if phase.iter().any(|value| *value < 0.0) {
                    3
                } else {
                    1
                };
                (digits(phase.iter().copied().map(phase_degrees)) + sign_and_border + 3).max(4)
            }),
            weight: digits(chunk.weight.iter().copied().map(f64::from)).max(3) + 1,
            uvw: (digits(chunk.uvw.iter().copied()) + 4).max(9) + 1,
        }
    }

    fn baseline(&self) -> usize {
        self.ant1 + 1 + self.ant2
    }

    fn visibility(&self) -> usize {
        self.amplitude + self.phase + self.weight + 2
    }

    fn separator(&self, correlations: usize) -> String {
        let mut fields = vec![
            12,
            self.baseline(),
            self.distance,
            self.field,
            self.spw,
            self.channel,
        ];
        fields.retain(|width| *width > 0);
        let mut rule = String::new();
        for width in fields {
            rule.push_str(&"-".repeat(width - usize::from(!rule.is_empty())));
            rule.push('|');
        }
        rule.push('-');
        for width in std::iter::repeat_n(self.visibility(), correlations)
            .chain(std::iter::repeat_n(self.uvw, 3))
        {
            rule.push_str(&"-".repeat(width - 1));
            rule.push('|');
        }
        rule
    }
}

fn digits(values: impl Iterator<Item = f64>) -> usize {
    let maximum = values.fold(f64::NEG_INFINITY, f64::max).abs();
    if maximum == 0.0 || !maximum.is_finite() {
        1
    } else {
        ((maximum.log10().abs() + 0.5).round_ties_even() as usize).max(1)
    }
}

fn index_width(data: impl Iterator<Item = i32> + Clone) -> usize {
    if constant(data.clone().map(f64::from)).is_some() {
        0
    } else {
        digits(data.map(f64::from)).max(3) + 1
    }
}

fn constant(mut values: impl Iterator<Item = f64>) -> Option<f64> {
    let first = values.next()?;
    values.all(|value| value == first).then_some(first)
}

fn header(
    state: &mut ListingState<'_>,
    window: &ListingWindow,
    w: &Widths,
    rule: &str,
) -> MsResult<()> {
    let left = format!(
        "{:<12}{:>width$}",
        "Date/Time:",
        "",
        width = w.baseline() + w.distance + w.field + w.spw + w.channel
    );
    let mut first = format!("{left} ");
    let date = format!("{}/", state.date);
    let mut second = format!(
        "{date:<12}{:>baseline$}{:>distance$}",
        "Intrf",
        "UVDist",
        baseline = w.baseline(),
        distance = w.distance
    );
    for (name, width) in [("Fld", w.field), ("SpW", w.spw), ("Chn", w.channel)] {
        if width > 0 {
            second.push_str(&format!("{name:>width$}"));
        }
    }
    second.push(' ');
    for (_, name) in &window.correlations {
        let title = format!("  {name}:");
        first.push_str(&format!("{title:<width$}", width = w.visibility()));
        second.push_str(&format!("{:>width$}", "Amp", width = w.amplitude));
        if w.phase > 0 {
            second.push_str(&format!("{:>width$}", "Phs", width = w.phase));
        }
        second.push_str(&format!("{:>width$} F", "Wt", width = w.weight));
    }
    first.push_str(&" ".repeat(3 * w.uvw));
    for name in ["U", "V", "W"] {
        second.push_str(&format!("{name:>width$}", width = w.uvw));
    }
    state.line(&first)?;
    state.line(&second)?;
    state.line(rule)
}

#[expect(
    clippy::too_many_arguments,
    reason = "one physical visibility row and its report formatting context"
)]
fn print_row(
    state: &mut ListingState<'_>,
    chunk: &ListingChunk<'_>,
    geometry: &Geometry<'_>,
    window: &ListingWindow,
    w: &Widths,
    timestamp: &str,
    row: usize,
    channel: usize,
) -> MsResult<()> {
    let mut line = format!(
        "{timestamp:>12}{:>ant1$}-{:>ant2$}{:>distance$.0}",
        geometry.ant1[row],
        geometry.ant2[row],
        geometry.distance[row],
        ant1 = w.ant1,
        ant2 = w.ant2,
        distance = w.distance
    );
    for (value, width) in [
        (i64::from(chunk.fields[row]), w.field),
        (i64::from(window.id), w.spw),
        (channel as i64, w.channel),
    ] {
        if width > 0 {
            line.push_str(&format!("{value:>width$}"));
        }
    }
    line.push(':');
    for &(corr, _) in &window.correlations {
        let index = [corr, channel, row];
        let amplitude = chunk
            .amplitude
            .get(index)
            .ok_or_else(|| listing_error("visibility channel exceeds stored shape"))?;
        line.push_str(&format!("{amplitude:>width$.3}", width = w.amplitude));
        if let Some(phase) = chunk.phase {
            let degrees = phase
                .get(index)
                .copied()
                .map(phase_degrees)
                .ok_or_else(|| listing_error("phase channel exceeds stored shape"))?;
            line.push_str(&format!("{degrees:>width$.1}", width = w.phase));
        }
        let weight = chunk
            .weight
            .get([corr, row])
            .ok_or_else(|| listing_error("weight correlation exceeds stored shape"))?;
        let flagged = chunk
            .flags
            .get(index)
            .ok_or_else(|| listing_error("flag channel exceeds stored shape"))?;
        line.push_str(&format!(
            "{weight:>width$.0} {}",
            if *flagged { 'F' } else { ' ' },
            width = w.weight
        ));
    }
    for i in 0..3 {
        line.push_str(&format!("{:>width$.2}", chunk.uvw[[i, row]], width = w.uvw));
    }
    state.line(&line)
}

fn io_error(error: std::io::Error) -> crate::MsError {
    listing_error(error.to_string())
}
