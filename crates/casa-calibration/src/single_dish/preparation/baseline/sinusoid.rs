// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sinusoidal bases and Fourier-assisted wavenumber selection for baseline fitting.

use std::collections::BTreeSet;
use std::f64::consts::TAU;

use casa_fft::RealFft2;
use casa_types::Complex32;

use super::linear_fit::{Fit, LinearFit};

/// Invalid wave selector error message.
pub const INVALID_WAVE: &str = "wrong value given for addwn/rejwn";
const NO_WAVES: &str = "No effective wave number given for sinusoidal fitting.";
pub(super) const INVALID_FIT: &str = "sakura_LSQFitSinusoidFloat() failed -- InvalidArgument";

/// Wave-number selection for sinusoidal baseline fitting.
#[derive(Clone)]
pub enum WaveSelection {
    /// Explicit 1-based/0-based integer list.
    Explicit(Vec<usize>),
    /// Exact wave indices.
    Exact(Vec<usize>),
    /// Closed `[first, last]` wave interval.
    Interval {
        /// Lower bound.
        first: usize,
        /// Upper bound.
        last: usize,
    },
    /// All waves from this threshold through Nyquist.
    ThroughNyquist(usize),
}

impl WaveSelection {
    /// Construct an explicit wave list from integer arguments.
    pub fn explicit(numbers: impl IntoIterator<Item = i64>) -> Result<Self, String> {
        numbers
            .into_iter()
            .map(|number| usize::try_from(number).map_err(|_| INVALID_WAVE.into()))
            .collect::<Result<Vec<_>, _>>()
            .map(Self::Explicit)
    }

    /// Parse wave-number specification strings into a selection.
    pub fn from_task_text(text: &str) -> Result<Self, String> {
        let index = |value: &str| value.trim().parse::<usize>().map_err(|_| INVALID_WAVE);
        if text.contains(',') {
            let indexes = text
                .split(',')
                .map(index)
                .collect::<Result<BTreeSet<_>, _>>()?;
            return Ok(Self::Explicit(indexes.into_iter().collect()));
        }
        if let Some(delimiter) = ['-', '~']
            .into_iter()
            .find(|&delimiter| text.contains(delimiter))
        {
            let bounds = text
                .split(delimiter)
                .map(index)
                .collect::<Result<Vec<_>, _>>()?;
            let [first, last, ..] = bounds.as_slice() else {
                return Err(INVALID_WAVE.into());
            };
            let (first, last) = (*first, *last);
            return Ok(Self::Interval {
                first: first.min(last),
                last: first.max(last),
            });
        }
        if let Some(last) = text
            .strip_prefix("<=")
            .or_else(|| text.strip_prefix("=<"))
            .or_else(|| text.strip_suffix(">="))
            .or_else(|| text.strip_suffix("=>"))
        {
            return Ok(Self::Interval {
                first: 0,
                last: index(last)?,
            });
        }
        if let Some(last) = text.strip_prefix('<').or_else(|| text.strip_suffix('>')) {
            return Ok(Self::Interval {
                first: 0,
                last: index(last)?.checked_sub(1).ok_or(INVALID_WAVE)?,
            });
        }
        if let Some(first) = text
            .strip_prefix(">=")
            .or_else(|| text.strip_prefix("=>"))
            .or_else(|| text.strip_suffix("<="))
            .or_else(|| text.strip_suffix("=<"))
        {
            return Ok(Self::ThroughNyquist(index(first)?));
        }
        if let Some(first) = text.strip_prefix('>').or_else(|| text.strip_suffix('<')) {
            return Ok(Self::ThroughNyquist(
                index(first)?.checked_add(1).ok_or(INVALID_WAVE)?,
            ));
        }
        Ok(Self::Explicit(vec![index(text)?]))
    }

    fn from_tool_text(text: &str) -> Self {
        let values = text
            .split(',')
            .filter(|term| !term.is_empty())
            .map(c_integer)
            .collect::<Vec<_>>();
        if let [first, -999] = values.as_slice() {
            return Self::ThroughNyquist(usize::try_from(*first).unwrap_or_default());
        }
        Self::Explicit(
            values
                .into_iter()
                .filter_map(|value| usize::try_from(value).ok())
                .collect(),
        )
    }

    fn includes(&self, wave: usize) -> bool {
        match self {
            Self::Explicit(values) | Self::Exact(values) => values.contains(&wave),
            Self::Interval { first, last } => (*first..=*last).contains(&wave),
            Self::ThroughNyquist(first) => wave >= *first,
        }
    }

    fn rejects_fft(&self, wave: usize) -> bool {
        match self {
            // CASA's merge sees the original two-element sentinel representation.
            Self::ThroughNyquist(first) => wave == *first,
            _ => self.includes(wave),
        }
    }
}

// The direct binding passes each comma-delimited token through atoi.
fn c_integer(text: &str) -> i32 {
    parse_integer_prefix(text).unwrap_or_default() as i32
}

fn parse_integer_prefix(text: &str) -> Option<i64> {
    let text = text.trim_start();
    let digits = text.strip_prefix(['-', '+']).unwrap_or(text);
    let count = digits.bytes().take_while(u8::is_ascii_digit).count();
    let sign = text.len() - digits.len();
    text[..sign + count].parse().ok()
}

#[derive(Clone, Copy)]
/// FFT peak detection threshold for automatic sinusoidal wave selection.
pub enum FftThreshold {
    /// Signal-to-noise ratio threshold.
    Sigma(f32),
    /// Fixed number of top Fourier components.
    Top(usize),
}

impl FftThreshold {
    /// Validate an `fftthresh` parameter string.
    pub fn validate(text: &str) -> Result<(), String> {
        const FORMAT: &str = "fftthresh has a wrong format.";
        const POSITIVE: &str = "threshold given to fftthresh must be positive.";
        let not_positive = if let Some(top) = text.strip_prefix("top") {
            top.trim().parse::<i64>().map_err(|_| FORMAT)? <= 0
        } else {
            text.strip_suffix("sigma")
                .unwrap_or(text)
                .trim()
                .parse::<f64>()
                .map_err(|_| FORMAT)?
                <= 0.
        };
        if not_positive {
            Err(POSITIVE.into())
        } else {
            Ok(())
        }
    }

    fn from_tool(text: &str) -> Result<Self, &'static str> {
        const INVALID: &str = "fftthresh has a wrong value";
        if let Some(count) = text.strip_prefix("top") {
            return parse_integer_prefix(count)
                .and_then(|count| i32::try_from(count).ok())
                .map(|count| Self::Top(count.max(0) as usize))
                .ok_or(INVALID);
        }
        let sigma = if let Some(sigma) = text.strip_suffix("sigma") {
            sigma
        } else if text.is_empty()
            || !text
                .bytes()
                .take(text.len() - 1)
                .all(|byte| byte.is_ascii_digit() || byte == b'.')
        {
            return Err(INVALID);
        } else {
            text
        };
        let sigma = sigma.trim_start();
        let prefix = sigma
            .bytes()
            .take_while(|&byte| {
                byte.is_ascii_digit() || matches!(byte, b'+' | b'-' | b'.' | b'e' | b'E')
            })
            .count();
        sigma[..prefix]
            .parse::<f32>()
            .ok()
            .filter(|value| value.is_finite())
            .map(Self::Sigma)
            .ok_or(INVALID)
    }
}

/// Wave selection for the sinusoidal single-dish baseline.
pub struct SingleDishSinusoid {
    include: WaveSelection,
    exclude: WaveSelection,
    fft: Option<Result<FftThreshold, &'static str>>,
}

impl SingleDishSinusoid {
    /// Interpret the comma-delimited wavenumbers from the direct CASA tool.
    /// A final sentinel of `-999` denotes an interval through Nyquist.
    pub fn from_tool(
        addwn: &str,
        rejwn: &str,
        applyfft: bool,
        fftmethod: &str,
        fftthresh: &str,
    ) -> Result<Self, String> {
        if addwn.split(',').all(str::is_empty) {
            return Err("addwn must contain at least one element.".into());
        }
        Ok(Self::new(
            WaveSelection::from_tool_text(addwn),
            WaveSelection::from_tool_text(rejwn),
            applyfft,
            fftmethod,
            fftthresh,
        ))
    }

    pub(super) fn from_variable_waves(waves: Vec<usize>) -> Self {
        Self {
            include: WaveSelection::Exact(waves),
            exclude: WaveSelection::Explicit(Vec::new()),
            fft: None,
        }
    }

    pub(super) fn variable_waves(&self) -> &[usize] {
        match &self.include {
            WaveSelection::Exact(waves) => waves,
            _ => &[],
        }
    }

    /// Construct a sinusoidal baseline model from parsed wave selectors and FFT options.
    pub fn new(
        include: WaveSelection,
        exclude: WaveSelection,
        applyfft: bool,
        fftmethod: &str,
        fftthresh: &str,
    ) -> Self {
        let fft = applyfft.then(|| {
            if fftmethod != "fft" {
                return Err("fftmethod must be 'fft' for now.");
            }
            FftThreshold::from_tool(fftthresh)
        });
        Self {
            include,
            exclude,
            fft,
        }
    }

    pub(super) fn fit(
        &self,
        spectrum: &[f32],
        included: &[bool],
        clip_sigma: f64,
        max_fits: usize,
        has_unflagged_data: bool,
    ) -> Result<Option<(Fit, Vec<usize>)>, String> {
        let maximum = (spectrum.len() / 2).checked_sub(1).ok_or(NO_WAVES)?;
        let extended = matches!(self.exclude, WaveSelection::ThroughNyquist(_));
        let first_rejected = match self.exclude {
            WaveSelection::ThroughNyquist(first) => Some(first),
            _ => None,
        };
        let mut waves = match (&self.include, first_rejected) {
            (WaveSelection::Exact(waves), _) => waves.clone(),
            (WaveSelection::Explicit(values), _) => values
                .iter()
                .copied()
                .filter(|&wave| (extended || wave <= maximum) && !self.exclude.includes(wave))
                .collect::<Vec<_>>(),
            (WaveSelection::Interval { first, last }, Some(rejected)) => {
                let last = (*last).min(rejected.checked_sub(1).ok_or(NO_WAVES)?);
                if last < *first {
                    return Err(NO_WAVES.into());
                }
                validate_context(last, spectrum.len())?;
                (*first..=last).collect()
            }
            _ => (0..=maximum)
                .filter(|&wave| self.include.includes(wave) && !self.exclude.includes(wave))
                .collect(),
        };
        let base_maximum = *waves.last().ok_or(NO_WAVES)?;
        if self.fft.is_none() || extended {
            validate_context(base_maximum, spectrum.len())?;
        }
        if !has_unflagged_data {
            return Ok(None);
        }
        let base_len = waves.len();
        if let Some(choice) = self.fft {
            let choice = choice?;
            let selected = peaks(spectrum, included, maximum, choice)?;
            for wave in selected {
                if !self.exclude.rejects_fft(wave) && !waves[..base_len].contains(&wave) {
                    waves.push(wave);
                }
            }
            waves.sort_unstable();
        }
        let terms = terms(&waves);
        if included.iter().filter(|&&value| value).count() < terms {
            return Ok(None);
        }
        let largest = *waves.last().expect("nonempty waves");
        if self.fft.is_some() && !extended {
            validate_context(largest, spectrum.len())?;
        }
        if (extended && largest > base_maximum) || !waves.windows(2).all(|pair| pair[0] < pair[1]) {
            return Err(INVALID_FIT.into());
        }
        let size = spectrum.len();
        let basis = (0..size)
            .map(|channel| basis(channel, size, &waves))
            .collect();
        let linear = LinearFit {
            basis,
            terms,
            segment_ends: vec![size],
            failure: "sakura_LSQFitSinusoidFloat() failed -- NG",
        };
        Ok(linear
            .solve(spectrum, included, clip_sigma, max_fits, None)?
            .map(|fit| (fit, waves)))
    }
}

pub(super) fn validate_context(wave: usize, channels: usize) -> Result<(), String> {
    if wave.saturating_mul(2).saturating_add(2) > channels {
        Err("sakura_CreateLSQFitContextSinusoidFloat() failed -- InvalidArgument".into())
    } else {
        Ok(())
    }
}

fn peaks(
    spectrum: &[f32],
    included: &[bool],
    maximum: usize,
    threshold: FftThreshold,
) -> Result<Vec<usize>, String> {
    let samples = interpolate(spectrum, included)?;
    let mut fft = RealFft2::<f32>::with_threads([1, spectrum.len()], 1)
        .map_err(|error| error.to_string())?
        .with_estimated_plan();
    let mut storage = vec![Complex32::new(0., 0.); fft.storage_len()];
    for (i, sample) in samples.into_iter().enumerate() {
        if i.is_multiple_of(2) {
            storage[i / 2].re = sample;
        } else {
            storage[i / 2].im = sample;
        }
    }
    fft.forward(&mut storage)
        .map_err(|error| error.to_string())?;
    let scale = (2. / spectrum.len() as f64) as f32;
    let amplitudes = storage
        .into_iter()
        .map(|amplitude| amplitude.norm() * scale)
        .collect::<Vec<_>>();
    match threshold {
        FftThreshold::Sigma(sigma) => {
            let count = amplitudes.len() as f32;
            let sum = amplitudes.iter().sum::<f32>();
            let squared = amplitudes
                .iter()
                .map(|&amplitude| amplitude * amplitude)
                .sum::<f32>();
            let mean = sum / count;
            let cutoff = mean + sigma * (squared / count - mean * mean).sqrt();
            Ok(amplitudes
                .iter()
                .take(maximum + 1)
                .enumerate()
                .filter_map(|(wave, &amplitude)| (cutoff <= amplitude).then_some(wave))
                .collect())
        }
        FftThreshold::Top(count) => {
            let mut indexes = (0..=maximum)
                .filter(|&index| amplitudes[index] > 0.)
                .collect::<Vec<_>>();
            indexes.sort_by(|&left, &right| amplitudes[right].total_cmp(&amplitudes[left]));
            indexes.truncate(count);
            indexes.extend(std::iter::repeat_n(
                0,
                count
                    .saturating_sub(indexes.len())
                    .min(spectrum.len().saturating_add(1)),
            ));
            Ok(indexes)
        }
    }
}

fn interpolate(spectrum: &[f32], included: &[bool]) -> Result<Vec<f32>, String> {
    let mut values = spectrum.to_vec();
    let mut previous = None;
    let mut from = 0;
    for (channel, &valid) in included.iter().enumerate() {
        if !valid {
            continue;
        }
        if from < channel {
            let value = previous.map_or(spectrum[channel], |before| {
                (before + spectrum[channel]) / 2.
            });
            values[from..channel].fill(value);
        }
        previous = Some(spectrum[channel]);
        from = channel + 1;
    }
    let last = previous.ok_or("Bad data. all channels are masked.")?;
    values[from..].fill(last);
    Ok(values)
}

pub(super) fn terms(waves: &[usize]) -> usize {
    2 * waves.len() - usize::from(waves.first() == Some(&0))
}

fn basis(channel: usize, channels: usize, waves: &[usize]) -> Vec<f64> {
    let norm = TAU / channels.saturating_sub(1).max(1) as f64;
    let mut result = Vec::with_capacity(terms(waves));
    for &wave in waves {
        if wave == 0 {
            result.push(1.);
        } else {
            let (sine, cosine) = (channel as f64 * wave as f64 * norm).sin_cos();
            result.extend([sine, cosine]);
        }
    }
    result
}

pub(super) fn subtract(
    spectrum: &mut [f32],
    coefficients: &[f64],
    waves: &[usize],
) -> Result<(), String> {
    if waves.is_empty() || coefficients.len() != terms(waves) {
        return Err("Invalid sinusoidal baseline coefficients.".into());
    }
    let channels = spectrum.len();
    for (channel, sample) in spectrum.iter_mut().enumerate() {
        let terms = basis(channel, channels, waves);
        let fitted = terms
            .iter()
            .zip(coefficients)
            .map(|(basis, coefficient)| basis * coefficient)
            .sum::<f64>();
        *sample -= fitted as f32;
    }
    Ok(())
}
