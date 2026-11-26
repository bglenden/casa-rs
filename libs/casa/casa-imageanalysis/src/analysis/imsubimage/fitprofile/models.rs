// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_images::ImageError;

#[derive(Clone, Copy)]
pub(super) enum LineKind {
    Gaussian,
    Lorentzian,
}

#[derive(Clone, Copy)]
pub(super) struct LineEstimate {
    pub(super) kind: LineKind,
    /// Peak, center and full width at half maximum, in pixel coordinates.
    pub(super) values: [f64; 3],
    pub(super) fixed: [bool; 3],
}

pub(super) struct Multiplet {
    pub(super) components: Vec<LineEstimate>,
    /// Ties to the first line in this multiplet. A zero specifies no tie.
    /// Amplitude and width use ratios, center uses an additive separation.
    pub(super) constraints: Vec<[f64; 3]>,
}

#[derive(Clone, Copy)]
pub(super) enum SpectralPolynomial {
    PowerLog,
    LogTransformed,
}

pub(super) struct PolynomialEstimate {
    pub(super) kind: SpectralPolynomial,
    pub(super) values: Vec<f64>,
    pub(super) fixed: Vec<bool>,
}

#[derive(Default)]
pub(super) struct ProfileModels {
    pub(super) singlets: Vec<LineEstimate>,
    pub(super) multiplets: Vec<Multiplet>,
    pub(super) spectral_polynomial: Option<PolynomialEstimate>,
}

pub(super) struct PeakLists<'a> {
    pub(super) amplitudes: &'a [f64],
    pub(super) centers: &'a [f64],
    pub(super) widths: &'a [f64],
    pub(super) fixed: &'a [String],
}

impl PeakLists<'_> {
    pub(super) fn singlets(&self, functions: &[String]) -> Result<Vec<LineEstimate>, ImageError> {
        let count = self.amplitudes.len();
        if self.centers.len() != count
            || self.widths.len() != count
            || (!functions.is_empty() && functions.len() != count)
        {
            return Err(invalid(
                "pampest, pcenterest, pfwhmest, and pfunc arrays must all be the same length",
            ));
        }
        self.validate_fixed(count, "pfix")?;
        (0..count)
            .map(|index| {
                let kind =
                    functions
                        .get(index)
                        .map_or(Ok(LineKind::Gaussian), |name| {
                            match name.as_bytes().first().map(u8::to_ascii_lowercase) {
                                Some(b'g') => Ok(LineKind::Gaussian),
                                Some(b'l') => Ok(LineKind::Lorentzian),
                                _ => Err(invalid(format!(
                                    "{name} does not minimally match 'gaussian' or 'lorentzian'"
                                ))),
                            }
                        })?;
                self.line(index, kind)
            })
            .collect()
    }

    pub(super) fn multiplets(
        &self,
        sizes: &[i64],
        constraints: [&[f64]; 3],
    ) -> Result<Vec<Multiplet>, ImageError> {
        let Some(count) = sizes.iter().try_fold(0_usize, |sum, &size| {
            usize::try_from(size)
                .ok()
                .filter(|&size| size > 0)
                .and_then(|size| sum.checked_add(size))
        }) else {
            return Err(invalid("All elements of gmncomps must be greater than 0"));
        };
        if sizes.is_empty() {
            return Ok(Vec::new());
        }
        for (name, values) in [
            ("gmampest", self.amplitudes),
            ("gmcenterest", self.centers),
            ("gmfwhmest", self.widths),
        ] {
            if values.len() != count {
                return Err(invalid(format!(
                    "{name} must have exactly {count} elements"
                )));
            }
        }
        self.validate_fixed(count, "gmfix")?;
        let ties = count - sizes.len();
        for (name, values) in ["gmampcon", "gmcentercon", "gmfwhmcon"]
            .into_iter()
            .zip(constraints)
        {
            if !values.is_empty() && values.len() != ties {
                return Err(invalid(format!(
                    "If specified, {name} must have exactly {ties} elements, even if some are zero"
                )));
            }
        }
        let mut first = 0;
        let mut tie = 0;
        sizes
            .iter()
            .map(|&size| {
                let size = size as usize;
                let components = (first..first + size)
                    .map(|index| self.line(index, LineKind::Gaussian))
                    .collect::<Result<Vec<_>, _>>()?;
                let constraints = (tie..tie + size - 1)
                    .map(|index| constraints.map(|column| column.get(index).copied().unwrap_or(0.)))
                    .collect::<Vec<_>>();
                validate_ties(&components, &constraints)?;
                first += size;
                tie += size - 1;
                Ok(Multiplet {
                    components,
                    constraints,
                })
            })
            .collect()
    }

    fn validate_fixed(&self, count: usize, name: &str) -> Result<(), ImageError> {
        if !self.fixed.is_empty() && self.fixed.len() != count {
            return Err(invalid(format!(
                "{name} must have either zero or {count} elements, even if some are empty strings"
            )));
        }
        Ok(())
    }

    fn line(&self, index: usize, kind: LineKind) -> Result<LineEstimate, ImageError> {
        let fixed = self.fixed.get(index).map_or([false; 3], |flags| {
            let flags = flags.to_ascii_lowercase();
            ["p", "c", "f"].map(|parameter| flags.contains(parameter))
        });
        let values = [
            self.amplitudes[index],
            self.centers[index],
            self.widths[index],
        ];
        LineEstimate::new(kind, values, fixed)
    }
}

impl LineEstimate {
    pub(super) fn new(
        kind: LineKind,
        mut values: [f64; 3],
        fixed: [bool; 3],
    ) -> Result<Self, ImageError> {
        if values[0] == 0. {
            return Err(invalid("PCF amplitude cannot equal 0"));
        }
        values[2] = values[2].abs();
        Ok(Self {
            kind,
            values,
            fixed,
        })
    }
}

fn validate_ties(components: &[LineEstimate], ties: &[[f64; 3]]) -> Result<(), ImageError> {
    for (secondary, tie) in components[1..].iter().zip(ties) {
        for (index, &constraint) in tie.iter().enumerate() {
            if constraint != 0. && secondary.fixed[index] && !components[0].fixed[index] {
                let parameter = ["amplitude", "center", "width"][index];
                return Err(invalid(format!(
                    "You cannot fix the {parameter} of a non-reference Gaussian if its tied reference parameter is free"
                )));
            }
        }
    }
    Ok(())
}

impl PolynomialEstimate {
    pub(super) fn new(
        kind: &str,
        values: &[f64],
        fixed: &[bool],
    ) -> Result<Option<Self>, ImageError> {
        let kind = match kind.to_ascii_lowercase().as_str() {
            "" => return Ok(None),
            "plp" => SpectralPolynomial::PowerLog,
            "ltp" => SpectralPolynomial::LogTransformed,
            _ => return Err(invalid("Unsupported value for spxtype")),
        };
        if values.is_empty() {
            return Ok(None);
        }
        if values.len() < 2 {
            return Err(invalid(
                "Number of elements in the spectral polynomial estimates list must be at least 2",
            ));
        }
        let fixed = if fixed.is_empty() {
            vec![false; values.len()]
        } else if fixed.len() == values.len() {
            fixed.to_vec()
        } else {
            return Err(invalid(
                "The spectral polynomial fixed parameter list must have the same length as its estimates",
            ));
        };
        Ok(Some(Self {
            kind,
            values: values.to_vec(),
            fixed,
        }))
    }
}

fn invalid(message: impl Into<String>) -> ImageError {
    ImageError::CasaSelection(message.into())
}
