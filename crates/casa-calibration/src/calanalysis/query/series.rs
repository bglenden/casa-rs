// SPDX-License-Identifier: LGPL-3.0-or-later
//! Project calibration cells into CASA's frequency and time series.

use std::f64::consts::{PI, TAU};

use casa_provider_contracts::CasaValue;
use num_complex::Complex64;

use super::fit::FitOptions;
use super::*;
use crate::constants::{COL_CPARAM, COL_FLAG, COL_FPARAM, COL_PARAMERR};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Component {
    Real,
    Amplitude,
    Phase,
}

impl Component {
    fn name(self) -> &'static str {
        match self {
            Self::Real => "REAL",
            Self::Amplitude => "AMPLITUDE",
            Self::Phase => "PHASE",
        }
    }
}

#[derive(Clone, Copy)]
enum Iteration {
    Time,
    Frequency,
}

#[derive(Clone, Copy)]
struct Sample {
    value: Complex64,
    error: f64,
    flagged: bool,
}

impl Sample {
    const MISSING: Self = Self {
        value: Complex64::new(0., 0.),
        error: 0.,
        flagged: true,
    };
}

struct GroupSamples {
    times: Vec<f64>,
    frequencies: Vec<f64>,
    samples: Vec<Sample>,
}

impl CalAnalysis {
    /// Return selected calibration solutions as CASA-numbered records.
    ///
    /// # Errors
    /// Invalid selector or unreadable calibration metadata and data.
    pub fn get(&self, request: CalAnalysisQuery<'_>) -> Result<CasaValue, String> {
        self.records(request, None)
    }

    /// Fit each selected series and return its data, polynomial and statistics.
    /// Invalid individual fits retain their input series and an empty fit record.
    /// # Errors
    /// Invalid selector, fitting options, or unreadable calibration data.
    pub fn fit(
        &self,
        request: CalAnalysisQuery<'_>,
        options: CalAnalysisFit<'_>,
    ) -> Result<CasaValue, String> {
        self.records(request, Some(FitOptions::parse(options)?))
    }

    fn records(
        &self,
        request: CalAnalysisQuery<'_>,
        fit: Option<FitOptions>,
    ) -> Result<CasaValue, String> {
        let component = self.component(request.amplitude_or_phase)?;
        let iteration = match initial(request.axis, 'T') {
            'T' => Iteration::Time,
            'F' => Iteration::Frequency,
            _ => return Err("Invalid analysis axis".into()),
        };
        if request.jump_max < 0. {
            return Err("Invalid maximum jump parameter".into());
        }
        let selected = self.select(request)?;
        let mut records = Vec::new();
        for group in &selected.groups {
            let samples = selected.group(group, component)?;
            for (index, &feed) in selected.feeds.iter().enumerate() {
                samples.series(
                    index,
                    |along, fixed, samples| {
                        let series = build_record(
                            group.baseline,
                            &self.feeds[feed],
                            component,
                            (iteration, along, fixed),
                            samples,
                            request,
                            fit,
                        )?;
                        records.push((records.len().to_string(), series));
                        Ok(())
                    },
                    iteration,
                )?;
            }
        }
        records.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        Ok(CasaValue::dict(records))
    }

    fn component(&self, mode: &str) -> Result<Component, String> {
        match (self.parameter_type.as_str(), initial(mode, 'A')) {
            ("Float", _) => Ok(Component::Real),
            ("Complex", 'A') => Ok(Component::Amplitude),
            ("Complex", 'P') => Ok(Component::Phase),
            _ => Err("Invalid parameter type or amplitude/phase selector".into()),
        }
    }
}

pub(super) fn initial(value: &str, default: char) -> char {
    value
        .trim()
        .chars()
        .next()
        .unwrap_or(default)
        .to_ascii_uppercase()
}

impl SelectedQuery {
    fn group(&self, group: &SelectedGroup, component: Component) -> Result<GroupSamples, String> {
        let mut times = group.rows.iter().map(|row| row.time).collect::<Vec<_>>();
        times.sort_by(f64::total_cmp);
        times.dedup();
        let windows = group
            .rows
            .iter()
            .map(|row| row.window)
            .collect::<BTreeSet<_>>();
        let mut frequencies = Vec::new();
        let offsets = windows
            .into_iter()
            .map(|window| {
                let offset = frequencies.len();
                frequencies.extend(
                    self.channels[&window]
                        .iter()
                        .map(|&channel| self.windows[window].frequencies[channel]),
                );
                (window, offset)
            })
            .collect::<BTreeMap<_, _>>();
        let total = self
            .feeds
            .len()
            .checked_mul(times.len())
            .and_then(|size| size.checked_mul(frequencies.len()))
            .ok_or_else(|| String::from("calibration analysis dimensions overflow"))?;
        let mut samples = Vec::new();
        samples
            .try_reserve_exact(total)
            .map_err(|error| error.to_string())?;
        samples.resize(total, Sample::MISSING);

        let rows = group.rows.iter().map(|row| row.index).collect::<Vec<_>>();
        let parameter = if component == Component::Real {
            COL_FPARAM
        } else {
            COL_CPARAM
        };
        let values = cells(&self.table, parameter, &rows)?;
        let errors = cells(&self.table, COL_PARAMERR, &rows)?;
        let flags = cells(&self.table, COL_FLAG, &rows)?;
        for (((row, values), errors), flags) in group.rows.iter().zip(values).zip(errors).zip(flags)
        {
            let (Some(values), Some(ArrayValue::Float32(errors)), Some(ArrayValue::Bool(flags))) =
                (values, errors, flags)
            else {
                return Err(format!(
                    "invalid calibration parameters in row {}",
                    row.index
                ));
            };
            let frequency_start = offsets[&row.window];
            let time = times
                .binary_search_by(|value| value.total_cmp(&row.time))
                .map_err(|_| String::from("missing calibration time"))?;
            for (slot, &feed) in self.feeds.iter().enumerate() {
                let start = (slot * times.len() + time) * frequencies.len() + frequency_start;
                for (index, &channel) in self.channels[&row.window].iter().enumerate() {
                    let key = [feed, channel];
                    let value = match &values {
                        ArrayValue::Float32(table) if component == Component::Real => table
                            .get(key)
                            .map(|&value| Complex64::new(f64::from(value), 0.)),
                        ArrayValue::Complex32(table) if component != Component::Real => table
                            .get(key)
                            .map(|&value| Complex64::new(f64::from(value.re), f64::from(value.im))),
                        _ => None,
                    };
                    let (Some(value), Some(&error), Some(&flagged)) =
                        (value, errors.get(key), flags.get(key))
                    else {
                        return Err(format!(
                            "invalid feed or channel in calibration row {}",
                            row.index
                        ));
                    };
                    samples[start + index] = Sample {
                        value,
                        error: f64::from(error),
                        flagged,
                    };
                }
            }
        }
        Ok(GroupSamples {
            times,
            frequencies,
            samples,
        })
    }
}

fn cells(table: &Table, column: &str, rows: &[usize]) -> Result<Vec<Option<ArrayValue>>, String> {
    table
        .column_accessor(column)
        .and_then(|column| column.array_cells_owned_uncached(rows))
        .map_err(|error| error.to_string())
}

impl GroupSamples {
    fn series(
        &self,
        feed: usize,
        mut output: impl FnMut(&[f64], f64, &[Sample]) -> Result<(), String>,
        iteration: Iteration,
    ) -> Result<(), String> {
        let frequency_count = self.frequencies.len();
        let total_cells = self.times.len() * frequency_count;
        let samples = &self.samples[feed * total_cells..(feed + 1) * total_cells];
        match iteration {
            Iteration::Time => {
                for (&time, samples) in self.times.iter().zip(samples.chunks_exact(frequency_count))
                {
                    output(&self.frequencies, time, samples)?;
                }
            }
            Iteration::Frequency => {
                let mut column = Vec::with_capacity(self.times.len());
                for (index, &frequency) in self.frequencies.iter().enumerate() {
                    column.clear();
                    column.extend(samples[index..].iter().step_by(frequency_count).copied());
                    output(&self.times, frequency, &column)?;
                }
            }
        }
        Ok(())
    }
}

fn build_record(
    (field, first, second): (i32, i32, i32),
    feed: &str,
    component: Component,
    (iteration, abscissa, coordinate): (Iteration, &[f64], f64),
    samples: &[Sample],
    query: CalAnalysisQuery<'_>,
    fit: Option<FitOptions>,
) -> Result<CasaValue, String> {
    let (x, fixed) = match iteration {
        Iteration::Time => ("frequency", "time"),
        Iteration::Frequency => ("time", "frequency"),
    };
    let mut values = Vec::with_capacity(samples.len());
    let mut errors = Vec::with_capacity(samples.len());
    let mut flags = samples
        .iter()
        .map(|sample| sample.flagged)
        .collect::<Vec<_>>();
    for sample in samples {
        values.push(match component {
            Component::Real => sample.value.re,
            Component::Amplitude => sample.value.norm(),
            Component::Phase => sample.value.arg(),
        });
        errors.push(sample.error);
    }
    let mut entries = vec![
        ("abscissa", CasaValue::string(x)),
        ("field", CasaValue::string(field.to_string())),
        ("antenna1", CasaValue::string(first.to_string())),
        ("antenna2", CasaValue::string(second.to_string())),
        ("feed", CasaValue::string(feed)),
        ("rap", CasaValue::string(component.name())),
        (x, CasaValue::float64_vector(abscissa.iter().copied())),
        (fixed, CasaValue::float(coordinate)),
    ];
    match component {
        Component::Amplitude => {
            if query.normalize {
                normalize(&mut values, &mut errors, &mut flags);
            }
            entries.push((
                "norm",
                CasaValue::Bool {
                    value: query.normalize,
                },
            ));
        }
        Component::Phase => {
            if query.unwrap {
                unwrap(&mut values, &flags, abscissa, iteration, query.jump_max)?;
            }
            for ((sample, error), flag) in samples.iter().zip(&mut errors).zip(&mut flags) {
                let amplitude = sample.value.norm();
                *flag |= amplitude <= *error / PI;
                *error = if *flag { PI } else { *error / amplitude };
            }
            entries.push((
                "unwrap",
                CasaValue::Bool {
                    value: query.unwrap,
                },
            ));
            entries.push(("jumpMax", CasaValue::float(query.jump_max)));
        }
        Component::Real => (),
    }
    if let Some(fit) = fit {
        entries.extend(fit.record(abscissa, &values, &errors, &mut flags));
    }
    entries.push(("value", CasaValue::float64_vector(values)));
    entries.push(("valueErr", CasaValue::float64_vector(errors)));
    entries.push((
        "flag",
        CasaValue::BoolArray {
            shape: vec![flags.len()],
            data: flags,
        },
    ));
    entries.sort_unstable_by_key(|(key, _)| *key);
    Ok(CasaValue::dict(entries))
}

fn normalize(values: &mut [f64], errors: &mut [f64], flags: &mut [bool]) {
    if flags.iter().filter(|&&flag| !flag).count() <= 1 {
        return;
    }
    let max = values
        .iter()
        .zip(flags.iter())
        .filter_map(|(&v, &flag)| (!flag).then_some(v))
        .fold(f64::NEG_INFINITY, f64::max);
    for ((value, error), flag) in values.iter_mut().zip(errors).zip(flags) {
        *flag |= *value <= 1e-8 * max;
        if !*flag {
            *value /= max;
            *error /= max;
        }
    }
}

fn unwrap(
    values: &mut [f64],
    flags: &[bool],
    x: &[f64],
    iteration: Iteration,
    jump: f64,
) -> Result<(), String> {
    let delay = if matches!(iteration, Iteration::Time) && jump == 0. {
        group_delay(values, flags, x)?
    } else {
        None
    };
    let mut previous = None::<(usize, f64)>;
    let mut shift = 0.;
    for (index, (value, &flag)) in values.iter_mut().zip(flags).enumerate() {
        let original = *value;
        if !flag {
            if let Some((prior_index, prior)) = previous {
                if let Some(delay) = delay {
                    let residual = original - prior - TAU * (x[index] - x[prior_index]) * delay;
                    shift -= TAU * ((residual - PI) / TAU).ceil();
                } else {
                    if PI - prior <= jump && PI + original <= jump {
                        shift += TAU;
                    }
                    if PI + prior <= jump && PI - original <= jump {
                        shift -= TAU;
                    }
                }
            }
            previous = Some((index, original));
        }
        *value += shift;
    }
    Ok(())
}

fn group_delay(values: &[f64], flags: &[bool], freq: &[f64]) -> Result<Option<f64>, String> {
    let samples = freq
        .iter()
        .zip(values)
        .zip(flags)
        .filter_map(|((&freq, &phase), &flag)| (!flag).then_some((freq, phase)))
        .collect::<Vec<_>>();
    if samples.len() <= 1 {
        return Ok(None);
    }
    let spacing = samples
        .windows(2)
        .map(|pair| pair[1].0 - pair[0].0)
        .filter(|spacing| *spacing != 0.)
        .min_by(f64::total_cmp)
        .ok_or_else(|| String::from("phase unwrapping needs distinct frequencies"))?;
    let max = samples
        .iter()
        .map(|sample| sample.0)
        .fold(f64::NEG_INFINITY, f64::max);
    let mut step = 0.5 / max;
    let span = 1. / spacing;
    let count = (span / step + 1.).abs();
    if !count.is_finite() || count < 1. || !step.is_finite() || step <= 0. {
        return Err("phase unwrapping has an invalid frequency grid".into());
    }
    let count = count as usize;
    let mut delay = packet_peak(&samples, -0.5 * span, step, count);
    for _ in 0..50 {
        let finer = 5. * step / (count as f64 + 1.);
        let refined = packet_peak(&samples, delay - 0.5 * step, finer, count);
        if (refined - delay).abs() / step < 1e-8 {
            break;
        }
        step = finer;
        delay = refined;
    }
    Ok(Some(delay))
}

fn packet_peak(samples: &[(f64, f64)], start: f64, step: f64, count: usize) -> f64 {
    let mut best = (f64::NEG_INFINITY, start);
    for i in 0..count {
        let delay = start + step * (i as f64);
        let mut real = 0.;
        let mut imag = 0.;
        for &(frequency, phase) in samples {
            let (sin, cos) = (TAU * frequency * delay - phase).sin_cos();
            real += cos;
            imag += sin;
        }
        let score = real * real + imag * imag;
        if score > best.0 {
            best = (score, delay);
        }
    }
    best.1
}
