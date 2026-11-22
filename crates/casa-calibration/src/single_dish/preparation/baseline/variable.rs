// SPDX-License-Identifier: LGPL-3.0-or-later
//! Per-row and per-polarization fitting parameters for variable baselines.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use super::{FitSettings, SingleDishBaselineBasis, SingleDishSinusoid, sinusoid};

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct StoredLineFinder {
    pub(super) enabled: bool,
    pub(super) threshold: f32,
    pub(super) average: u32,
    pub(super) edges: [u32; 2],
}

pub(super) struct SpectrumParameters {
    pub(super) mask: String,
    pub(super) settings: FitSettings,
    pub(super) line_finder: StoredLineFinder,
}

impl SpectrumParameters {
    pub(super) fn describe_basis(&self) -> String {
        match &self.settings.basis {
            SingleDishBaselineBasis::Polynomial { order, chebyshev } => {
                let family = if *chebyshev { "chebyshev" } else { "poly" };
                format!("{family} order={order}")
            }
            SingleDishBaselineBasis::CubicSpline { pieces } => format!("cspline npiece={pieces}"),
            SingleDishBaselineBasis::Sinusoid(sinusoid) => {
                format!("sinusoid nwave={:?}", sinusoid.variable_waves())
            }
        }
    }
}

type SpectrumKey = (usize, usize);
type ContextLimit = (usize, usize);

pub(super) struct VariableParameters {
    entries: BTreeMap<SpectrumKey, SpectrumParameters>,
    context_limits: [Option<usize>; 4],
}

impl VariableParameters {
    pub(super) fn open(path: &Path) -> Result<Self, String> {
        let reader = BufReader::new(File::open(path).map_err(|error| {
            format!(
                "Cannot open baseline parameter file '{}': {error}",
                path.display()
            )
        })?);
        let mut entries = BTreeMap::new();
        let mut context_limits = [None::<usize>; 4];
        for (index, line) in reader.lines().enumerate() {
            let line = line.map_err(|error| error.to_string())?;
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (key, parameters, (family, order)) =
                parse_line(&line).map_err(|error| format!("line {}: {error}", index + 1))?;
            context_limits[family] =
                Some(context_limits[family].map_or(order, |old| old.max(order)));
            entries.insert(key, parameters);
        }
        Ok(Self {
            entries,
            context_limits,
        })
    }

    pub(super) fn get(&self, row: usize, polarization: usize) -> Option<&SpectrumParameters> {
        self.entries.get(&(row, polarization))
    }

    pub(super) fn validate_contexts(&self, channels: usize) -> Result<(), String> {
        let [polynomial, chebyshev, spline, wave] = self.context_limits;
        if polynomial.is_some_and(|order| order >= channels)
            || chebyshev.is_some_and(|order| order >= channels)
            || spline.is_some_and(|pieces| pieces == 0 || pieces + 3 > channels)
        {
            return Err("sakura_CreateLSQFitContextFloat() failed -- InvalidArgument".into());
        }
        if let Some(wave) = wave {
            sinusoid::validate_context(wave, channels)?;
        }
        Ok(())
    }
}

fn parse_line(line: &str) -> Result<(SpectrumKey, SpectrumParameters, ContextLimit), String> {
    let mut values = line.split(',');
    let mut field = || values.next().unwrap_or_default();
    let row = number(field(), "row")?;
    let polarization = number(field(), "polarization")?;
    let mask = field().to_owned();
    let iterations = field();
    if iterations.is_empty() {
        return Err("Number of maximum clip iteration is mandatory".into());
    }
    let iterations: u16 = number(iterations, "clip iteration")?;
    let clip_sigma = f64::from(optional(field(), "clip threshold", 3.0_f32)?);
    let enabled = field() == "true";
    let threshold = field();
    let left = field();
    let right = field();
    let average = field();
    let line_finder = if enabled {
        StoredLineFinder {
            enabled,
            threshold: optional(threshold, "line threshold", 5.)?,
            average: optional(average, "channel average limit", 0)?,
            edges: [
                optional(left, "left edge", 0)?,
                optional(right, "right edge", 0)?,
            ],
        }
    } else {
        StoredLineFinder {
            threshold: 5.,
            ..Default::default()
        }
    };
    let family = field();
    let order = field();
    let pieces = field();
    let (basis, context) = match family {
        "cspline" => {
            if pieces.is_empty() {
                return Err("Baseline type 'cspline' requires num_piece value.".into());
            }
            let pieces: usize = number(pieces, "num_piece")?;
            if pieces > usize::from(u16::MAX) {
                return Err("num_piece is larger than max of uint16_t".into());
            }
            (SingleDishBaselineBasis::CubicSpline { pieces }, (2, pieces))
        }
        "sinusoid" => {
            let waves = bracketed_waves(line)?;
            // Sakura's context takes a uint16_t maximum wavenumber.
            let maximum = *waves.last().expect("nonempty wavenumbers") as u16;
            (
                SingleDishBaselineBasis::Sinusoid(SingleDishSinusoid::from_variable_waves(waves)),
                (3, usize::from(maximum)),
            )
        }
        _ => {
            if order.is_empty() {
                return Err("Baseline type 'poly' and 'chebyshev' require order value.".into());
            }
            let order: u16 = number(order, "polynomial order")?;
            let order = usize::from(order);
            let chebyshev = family == "chebyshev";
            (
                SingleDishBaselineBasis::Polynomial { order, chebyshev },
                (usize::from(chebyshev), order),
            )
        }
    };
    Ok((
        (row, polarization),
        SpectrumParameters {
            mask,
            settings: FitSettings {
                basis,
                clip_sigma,
                max_fits: usize::from(iterations.wrapping_add(1)),
            },
            line_finder,
        },
        context,
    ))
}

fn number<T: std::str::FromStr>(text: &str, name: &str) -> Result<T, String> {
    text.trim()
        .parse()
        .map_err(|_| format!("Invalid {name}: {text:?}"))
}

fn optional<T: std::str::FromStr>(text: &str, name: &str, default: T) -> Result<T, String> {
    if text.is_empty() {
        Ok(default)
    } else {
        number(text, name)
    }
}

fn bracketed_waves(line: &str) -> Result<Vec<usize>, String> {
    for tail in line.split('[').skip(1) {
        let Some((contents, _)) = tail.split_once(']') else {
            continue;
        };
        let mut waves = Vec::new();
        if contents.split(',').all(|word| {
            if !word.bytes().all(|byte| byte.is_ascii_digit()) {
                return false;
            }
            match word.parse::<usize>() {
                Ok(wave) => {
                    waves.push(wave);
                    true
                }
                Err(_) => false,
            }
        }) {
            waves.sort_unstable();
            waves.dedup();
            return Ok(waves);
        }
    }
    Err("Incorrect format or empty nwave list given. Use a list such as [1,2].".into())
}
