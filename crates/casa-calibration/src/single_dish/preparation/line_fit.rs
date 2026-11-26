// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selected single-dish Gaussian and Lorentzian line profiles.

mod product;

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufWriter, Write};
use std::ops::RangeInclusive;
use std::path::Path;

use casa_core::{
    ArrayValue,
    quanta::{Quantity, Unit},
};
use casa_images::analysis::{ChannelLineShape, fit_channel_lines};
use casa_ms::MeasurementSet;
use casa_ms::transform::{
    MsTransformRequest, MsTransformTaskOptions, SpectrumBaseline, SpectrumRow, SpectrumWeights,
    TimeAverageOptions, transform_baseline_spectra,
};
use ndarray::{ArrayD, Axis};

use super::{SingleDishSession, baseline::SingleDishLineFinder, data_column};
use crate::output::CalibrationOutput;

/// Parameters for an attached single-dish line fit.
pub struct SingleDishLineFit<'a> {
    /// Spectral data column.
    pub datacolumn: &'a str,
    /// SPW and channel ranges for fitting or automatic line detection.
    pub spw: &'a str,
    /// Optional new time averaging interval. An empty value retains the current interval.
    pub timebin: &'a str,
    /// Scan, state and field boundaries allowed inside a time bin.
    pub timespan: &'a str,
    /// Optional new polarization averaging mode. An empty value retains the current mode.
    pub polaverage: &'a str,
    /// Profile shape, either gaussian or lorentzian.
    pub fitfunc: &'a str,
    /// Comma-separated component counts for each fit range.
    pub nfit: &'a str,
    /// Detect ranges automatically when supplied.
    pub finder: Option<SingleDishLineFinder>,
    /// Output file with original coordinates and fitted components.
    pub tempfile: &'a Path,
    /// Empty selected MeasurementSet created by the public tool.
    pub tempoutfile: &'a Path,
}

#[derive(Default)]
pub(super) struct LineAveraging {
    time: Option<(String, String)>,
    polarization: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Polarization {
    Selected,
    Stokes,
    Geometric,
}

impl SingleDishSession {
    /// Fit line profiles and write the CASA tool's two temporary products.
    pub fn fit_line(&mut self, request: SingleDishLineFit<'_>) -> Result<bool, String> {
        self.name()?;
        if !request.timebin.is_empty() {
            self.line_averaging.time = Some((request.timebin.into(), request.timespan.into()));
        }
        if !request.polaverage.is_empty() {
            self.line_averaging.polarization = request.polaverage.into();
        }
        self.write_line_fits(request)?;
        Ok(true)
    }

    fn write_line_fits(&self, request: SingleDishLineFit<'_>) -> Result<(), String> {
        if request.tempoutfile.exists() {
            return Err("temporary ms file unexpectedly exists.".into());
        }
        if request.tempfile.exists() {
            return Err("temporary file unexpectedly exists.".into());
        }
        let name = self.name()?;
        let column = data_column(request.datacolumn)?;
        let input = MeasurementSet::open(name).map_err(error_text)?;
        let windows = fit_windows(&input, request.spw)?;
        let mode = if let Some(finder) = request.finder {
            FitMode::Auto(finder)
        } else {
            FitMode::List(parse_components(request.nfit)?)
        };
        let averaging = self.line_averaging.time_options()?;
        // SDMSManager uses its time-averaging iterator in place of the
        // polarization-averaging iterator when both are requested.
        let polarization = if averaging.is_some() {
            Polarization::Selected
        } else {
            self.line_averaging.polarization()?
        };
        if !request.tempoutfile.as_os_str().is_empty()
            && crate::paths::overlap(name, request.tempoutfile).map_err(error_text)?
        {
            return Err("The temporary output overlaps the input MeasurementSet.".into());
        }
        if !request.tempfile.as_os_str().is_empty()
            && (crate::paths::overlap(name, request.tempfile).map_err(error_text)?
                || (!request.tempoutfile.as_os_str().is_empty()
                    && crate::paths::overlap(request.tempfile, request.tempoutfile)
                        .map_err(error_text)?))
        {
            return Err("The temporary fit report overlaps a MeasurementSet.".into());
        }
        let scratch = request
            .tempoutfile
            .as_os_str()
            .is_empty()
            .then(tempfile::tempdir)
            .transpose()
            .map_err(error_text)?;
        let hidden_output = scratch
            .as_ref()
            .map(|scratch| scratch.path().join("fit.ms"));
        let output = hidden_output.as_deref().unwrap_or(request.tempoutfile);
        let ms = CalibrationOutput::prepare_measurement_set(output, false).map_err(error_text)?;
        // An empty filename discards the fit text. A dangling symlink
        // names its future target, just as for an ordinary file write.
        let report_path = (!request.tempfile.as_os_str().is_empty())
            .then(|| crate::paths::normalized(request.tempfile))
            .transpose()
            .map_err(error_text)?;
        let mut results = report_path
            .as_deref()
            .map(|path| {
                tempfile::NamedTempFile::new_in(path.parent().unwrap_or_else(|| Path::new(".")))
            })
            .transpose()
            .map_err(error_text)?;
        let mut fit = FitStream {
            windows,
            mode,
            fitfunc: request.fitfunc,
            polarization,
            results: results.as_mut().map(BufWriter::new),
        };
        let selection_spw = self.selection.spw.clone().unwrap_or_default();
        transform_baseline_spectra(
            &MsTransformRequest {
                input_ms: name.to_path_buf(),
                output_ms: ms.staging.clone(),
                spw: selection_spw,
                width: 1,
                data_column: column,
                selection: self.selection.clone(),
                keep_flags: true,
            },
            &MsTransformTaskOptions {
                time_averaging: averaging,
                ..MsTransformTaskOptions::default()
            },
            self.reindex.unwrap_or(true),
            &mut fit,
        )
        .map_err(error_text)?;
        if let Some(writer) = &mut fit.results {
            writer.flush().map_err(error_text)?;
        }
        drop(fit);
        if !self.line_averaging.polarization.is_empty() {
            product::write_polarization(&ms.staging)?;
        }
        if let (Some(results), Some(report)) = (results, report_path.as_ref()) {
            results.persist_noclobber(report).map_err(error_text)?;
        }
        if let Err(error) = ms.commit() {
            if let Some(report) = report_path {
                fs::remove_file(&report).map_err(|rollback| {
                    format!("{error}. Could not remove {}: {rollback}", report.display())
                })?;
            }
            return Err(error_text(error));
        }
        Ok(())
    }
}

impl LineAveraging {
    pub(super) fn operation_options(&self) -> Result<MsTransformTaskOptions, String> {
        use casa_ms::transform::SingleDishPolarizationAverage;

        let time_averaging = self.time_options()?;
        let single_dish_polarization = if time_averaging.is_some() {
            None
        } else {
            match self.polarization()? {
                Polarization::Selected => None,
                Polarization::Stokes => Some(SingleDishPolarizationAverage::Stokes),
                Polarization::Geometric => Some(SingleDishPolarizationAverage::Geometric),
            }
        };
        Ok(MsTransformTaskOptions {
            time_averaging,
            single_dish_polarization,
            ..MsTransformTaskOptions::default()
        })
    }

    fn time_options(&self) -> Result<Option<TimeAverageOptions>, String> {
        self.time
            .as_ref()
            .map(|(timebin, span)| {
                let seconds = Unit::new("s").expect("seconds are a defined unit");
                let quantity = Quantity::read(timebin).map_err(error_text)?;
                let interval_seconds = quantity
                    .get_value_in(&seconds)
                    .unwrap_or_else(|_| quantity.get_si_value());
                if !interval_seconds.is_finite() || interval_seconds <= 0. {
                    return Err("time bin should be positive".into());
                }
                Ok(TimeAverageOptions {
                    interval_seconds,
                    span_scans: span.contains("scan"),
                    span_states: span.contains("state"),
                    span_fields: span.contains("field"),
                    max_uvw_distance: 0.,
                })
            })
            .transpose()
    }

    fn polarization(&self) -> Result<Polarization, String> {
        match self.polarization.as_str() {
            "" => Ok(Polarization::Selected),
            "stokes" => Ok(Polarization::Stokes),
            "geometric" => Ok(Polarization::Geometric),
            _ => Err("Invalid Averaging Mode for PolAverageTVI.".into()),
        }
    }
}

struct FitWindow {
    mask: Vec<bool>,
    ranges: Vec<RangeInclusive<usize>>,
}

fn fit_windows(input: &MeasurementSet, selector: &str) -> Result<BTreeMap<i32, FitWindow>, String> {
    if selector.is_empty() {
        return Ok(BTreeMap::new());
    }
    let selection = input.resolve_spw_selector(selector).map_err(error_text)?;
    let metadata = input.spectral_window().map_err(error_text)?;
    selection
        .spw_ids()
        .iter()
        .map(|&window| {
            let count = metadata.num_chan(window).map_err(error_text)? as usize;
            let mut mask = vec![false; count];
            let mut ranges = Vec::new();
            for range in selection
                .channel_list()
                .iter()
                .filter(|range| range.spw_id == window)
            {
                if let Some(channels) = range.channels() {
                    for channel in channels {
                        mask[channel] = true;
                    }
                    ranges.push(range.start as usize..=range.stop as usize);
                }
            }
            Ok((window as i32, FitWindow { mask, ranges }))
        })
        .collect()
}

fn parse_components(input: &str) -> Result<Vec<usize>, String> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    input
        .split(',')
        .filter(|count| !count.is_empty())
        .map(|count| {
            let count = count.trim_start();
            let unsigned = count.strip_prefix(['-', '+']).unwrap_or(count);
            let end = count.len() - unsigned.len()
                + unsigned.bytes().take_while(u8::is_ascii_digit).count();
            count[..end]
                .parse::<i32>()
                .map_err(|_| "stoi".to_owned())
                .and_then(|value| {
                    usize::try_from(value).map_err(|_| "Invalid number of line components.".into())
                })
        })
        .collect()
}

enum FitMode {
    Auto(SingleDishLineFinder),
    List(Vec<usize>),
}

struct FitStream<'a> {
    windows: BTreeMap<i32, FitWindow>,
    mode: FitMode,
    fitfunc: &'a str,
    polarization: Polarization,
    results: Option<BufWriter<&'a mut tempfile::NamedTempFile>>,
}

impl SpectrumBaseline for FitStream<'_> {
    fn writes_rows(&self) -> bool {
        false
    }

    fn updates_weight(&self) -> bool {
        false
    }

    fn reads_parallel_hands(&self) -> bool {
        self.polarization != Polarization::Selected
    }

    fn reads_spectral_weights(&self) -> bool {
        self.polarization == Polarization::Geometric
    }

    fn omit_spectral_weights(&self) -> bool {
        false
    }

    fn process(
        &mut self,
        row: &SpectrumRow,
        data: &mut ArrayValue,
        flags: &mut ArrayD<bool>,
        weights: SpectrumWeights<'_>,
    ) -> Result<Option<ArrayValue>, String> {
        if let FitMode::List(ref counts) = self.mode
            && counts.len()
                != self
                    .windows
                    .get(&row.window)
                    .map_or(0, |window| window.ranges.len())
        {
            return Err(
                "the number of elements of nfit and fitranges specified in spw must be identical."
                    .into(),
            );
        }
        let profiles = row_profiles(data, flags, weights, row.parallel_hands, self.polarization)?;
        for (polarization, (spectrum, unflagged)) in profiles.into_iter().enumerate() {
            if !unflagged.iter().any(|&channel| channel) {
                continue;
            }
            let mut included = unflagged;
            if let Some(window) = self
                .windows
                .get(&row.window)
                .or_else(|| self.windows.values().next())
            {
                for (channel, keep) in included.iter_mut().enumerate() {
                    *keep &= window.mask.get(channel).copied().unwrap_or(false);
                }
            }
            self.write_spectrum(row, polarization, &spectrum, &mut included)?;
        }
        Ok(None)
    }
}

type Spectrum = (Vec<f32>, Vec<bool>);

fn row_profiles(
    data: &ArrayValue,
    flags: &ArrayD<bool>,
    weights: SpectrumWeights<'_>,
    hands: Option<[usize; 2]>,
    mode: Polarization,
) -> Result<Vec<Spectrum>, String> {
    let mut profiles = match data {
        ArrayValue::Float32(data) => collect_profiles(data, flags, |value| value),
        ArrayValue::Complex32(data) => collect_profiles(data, flags, |value| value.re),
        _ => return Err("Invalid spectral array type.".into()),
    };
    let Some([first, second]) = hands else {
        return Ok(profiles);
    };
    let weight = |polarization, channel| {
        if mode == Polarization::Geometric {
            if let Some(ArrayValue::Float32(spectrum)) = weights.spectrum
                && let Some(&value) = spectrum.get([polarization, channel])
            {
                return value;
            }
            if let Some(ArrayValue::Float32(scalar)) = weights.scalar
                && let Some(&value) = scalar.get([polarization])
            {
                return value;
            }
        }
        1.
    };
    if first >= profiles.len() || second >= profiles.len() {
        return Err("Parallel-hand indices do not match the spectrum.".into());
    }
    let (mut left, mut left_mask) = std::mem::take(&mut profiles[first]);
    let (right, right_mask) = std::mem::take(&mut profiles[second]);
    for (channel, ((left, valid), (right, right_valid))) in left
        .iter_mut()
        .zip(&mut left_mask)
        .zip(right.into_iter().zip(right_mask))
        .enumerate()
    {
        let (left_term, wl) = if *valid {
            let weight = weight(first, channel);
            (*left * weight, weight)
        } else {
            (0., 0.)
        };
        let (right_term, wr) = if right_valid {
            let weight = weight(second, channel);
            (right * weight, weight)
        } else {
            (0., 0.)
        };
        let sum = wl + wr;
        *left = left_term + right_term;
        if sum > 0. {
            *left /= sum;
        }
        *valid |= right_valid;
    }
    Ok(vec![(left, left_mask)])
}

fn collect_profiles<T: Copy>(
    data: &ArrayD<T>,
    flags: &ArrayD<bool>,
    real: impl Fn(T) -> f32,
) -> Vec<Spectrum> {
    data.axis_iter(Axis(0))
        .zip(flags.axis_iter(Axis(0)))
        .map(|(data, flags)| {
            (
                data.iter().copied().map(&real).collect(),
                flags.iter().map(|&flag| !flag).collect(),
            )
        })
        .collect()
}

impl FitStream<'_> {
    fn write_spectrum(
        &mut self,
        row: &SpectrumRow,
        polarization: usize,
        spectrum: &[f32],
        included: &mut [bool],
    ) -> Result<(), String> {
        let (ranges, counts): (&[RangeInclusive<usize>], &[usize]);
        let detected;
        let automatic_counts;
        match self.mode {
            FitMode::Auto(finder) => {
                detected = finder.find_lines(spectrum, included)?;
                if detected.is_empty() {
                    return Ok(());
                }
                automatic_counts = vec![1; detected.len()];
                (ranges, counts) = (&detected, &automatic_counts);
            }
            FitMode::List(ref components) => {
                ranges = self
                    .windows
                    .get(&row.window)
                    .map_or(&[], |window| &window.ranges);
                counts = components;
            }
        }
        for (index, (range, &count)) in ranges.iter().zip(counts).enumerate() {
            if count == 0 {
                continue;
            }
            let shape = match self.fitfunc {
                "gaussian" => ChannelLineShape::Gaussian,
                "lorentzian" => ChannelLineShape::Lorentzian,
                _ => return Err(format!("Unsupported line profile: {}", self.fitfunc)),
            };
            let components = fit_range(spectrum, included, range, count, shape)?;
            if let Some(results) = &mut self.results {
                for (component, (values, errors)) in components.into_iter().enumerate() {
                    if component > 0 || index > 0 {
                        write!(results, ":").map_err(error_text)?;
                    }
                    write_component(results, row, polarization, values, errors)
                        .map_err(error_text)?;
                }
            }
        }
        if let Some(results) = &mut self.results {
            writeln!(results).map_err(error_text)?;
        }
        Ok(())
    }
}

type LineComponent = ([f32; 3], [f32; 3]);

fn fit_range(
    spectrum: &[f32],
    included: &[bool],
    range: &RangeInclusive<usize>,
    count: usize,
    shape: ChannelLineShape,
) -> Result<Vec<LineComponent>, String> {
    let start = *range.start();
    let end = *range.end();
    if end >= spectrum.len() || count > spectrum.len() {
        return Err("Not enough channels for the requested line components.".into());
    }
    let width = (end - start) / count;
    if count > 1 && width == 0 {
        return Ok(vec![([f32::NAN; 3], [f32::NAN; 3]); count]);
    }
    let estimates = (0..count)
        .map(|index| {
            let from = start + width * index;
            let to = if index == count - 1 {
                end + 1
            } else {
                from + width
            };
            initial_estimate(spectrum, from, to)
        })
        .collect::<Vec<_>>();
    let mask = included
        .iter()
        .enumerate()
        .map(|(channel, &included)| included && range.contains(&channel))
        .collect::<Vec<_>>();
    let fit = fit_channel_lines(spectrum, &mask, shape, &estimates)
        .ok_or("Failed in fitting defined line range.")?;
    Ok(fit
        .parameters
        .into_iter()
        .zip(fit.errors)
        .map(|(value, error)| {
            (
                value.map(|value| value as f32),
                error.map(|value| value as f32),
            )
        })
        .collect())
}

fn initial_estimate(spectrum: &[f32], start: usize, end: usize) -> [f64; 3] {
    let mut peak = spectrum[start].abs();
    let mut center = start;
    let values = &spectrum[start..end];
    for (index, &value) in values.iter().enumerate() {
        if value.abs() > peak.abs() {
            peak = value;
            center = start + index;
        }
    }
    let sum = values.iter().sum::<f32>();
    let width = (f64::from(sum / peak) * 0.7).abs() as f32;
    [f64::from(peak), center as f64, f64::from(width)]
}

fn write_component(
    writer: &mut impl Write,
    row: &SpectrumRow,
    polarization: usize,
    [peak, center, width]: [f32; 3],
    [peak_error, center_error, width_error]: [f32; 3],
) -> std::io::Result<()> {
    write!(
        writer,
        "{},{:.4},{},{},{},{polarization},{center:.8},{center_error:.8},{peak:.8},{peak_error:.8},{width:.8},{width_error:.8}",
        row.scan, row.time, row.antenna, row.feed, row.window,
    )
}

fn error_text(error: impl std::fmt::Display) -> String {
    error.to_string()
}
