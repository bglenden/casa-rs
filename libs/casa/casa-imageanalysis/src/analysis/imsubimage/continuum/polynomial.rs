// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_numerics::solve_symmetric_positive_definite;

use casa_images::ImageError;

use super::invalid;

// Bound one dense system, including a high-order fit on a short set of
// selected channels. Sparse fits use the number of observed samples instead.
const MAX_MATRIX_ELEMENTS: usize = 32 * 1024 * 1024 / std::mem::size_of::<f64>();

#[derive(Default)]
pub(super) struct Coverage {
    has_data: bool,
    nonfinite: bool,
    fitted: usize,
    first: usize,
    last: usize,
    sum: f64,
}

impl Coverage {
    pub(super) fn observe(&mut self, channel: usize, pixel: f32, selected: bool) {
        self.has_data = true;
        if !selected {
            return;
        }
        if !pixel.is_finite() {
            self.nonfinite = true;
            return;
        }
        if self.fitted == 0 {
            self.first = channel;
        }
        self.last = channel;
        self.fitted += 1;
        self.sum += f64::from(pixel);
    }

    pub(super) fn prepare(self, terms: usize) -> Result<Fit, ImageError> {
        let fit = if !self.has_data {
            Fit::Complete(Prediction::Input)
        } else if self.nonfinite {
            Fit::Complete(Prediction::Invalid)
        } else if self.fitted == 0 {
            Fit::Complete(Prediction::Constant(0.0))
        } else if terms == 1 {
            Fit::Complete(Prediction::Constant(self.sum / self.fitted as f64))
        } else if self.fitted == 1 {
            let prediction = if self.first == 0 {
                Prediction::Constant(self.sum)
            } else {
                Prediction::Single {
                    channel: self.first,
                    terms,
                    observed: self.sum,
                }
            };
            Fit::Complete(prediction)
        } else if self.fitted < terms {
            checked_matrix(self.fitted)?;
            if terms > MAX_MATRIX_ELEMENTS {
                return Err(invalid(
                    "Polynomial coefficient storage exceeds the fit workspace",
                ));
            }
            Fit::Underdetermined {
                terms,
                samples: Vec::with_capacity(self.fitted),
            }
        } else {
            Fit::Full(Qr::new(terms, self.first, self.last)?)
        };
        Ok(fit)
    }
}

fn checked_matrix(side: usize) -> Result<usize, ImageError> {
    side.checked_mul(side)
        .filter(|&elements| elements <= MAX_MATRIX_ELEMENTS)
        .ok_or_else(|| invalid("Polynomial matrix exceeds the fit workspace"))
}

pub(super) enum Fit {
    Complete(Prediction),
    Full(Qr),
    Underdetermined {
        terms: usize,
        samples: Vec<(usize, f64)>,
    },
}

impl Fit {
    pub(super) fn needs_samples(&self) -> bool {
        !matches!(self, Self::Complete(_))
    }

    pub(super) fn add(&mut self, channel: usize, value: f32) {
        match self {
            Self::Complete(_) => {}
            Self::Full(qr) => qr.add(channel, f64::from(value)),
            Self::Underdetermined { samples, .. } => samples.push((channel, f64::from(value))),
        }
    }

    pub(super) fn solve(self) -> Prediction {
        match self {
            Self::Complete(prediction) => prediction,
            Self::Full(qr) => qr.solve(),
            Self::Underdetermined { terms, samples } => {
                fit_underdetermined(terms, &samples).unwrap_or(Prediction::Invalid)
            }
        }
    }
}

pub(super) enum Prediction {
    Input,
    Invalid,
    Constant(f64),
    Single {
        channel: usize,
        terms: usize,
        observed: f64,
    },
    Chebyshev {
        coefficients: Vec<f64>,
        lower: usize,
        upper: usize,
    },
    ScaledPowers {
        coefficients: Vec<f64>,
        divisor: f64,
    },
}

impl Prediction {
    pub(super) fn evaluate(&self, channel: usize) -> f64 {
        match self {
            Self::Input | Self::Invalid => f64::NAN,
            Self::Constant(value) => *value,
            Self::Single {
                channel: fit_channel,
                terms,
                observed,
            } => {
                // At one nonzero abscissa all column-normalized design
                // entries are one. Every term contributes equally there.
                let ratio = channel as f64 / *fit_channel as f64;
                if channel == *fit_channel || *observed == 0.0 {
                    *observed
                } else if channel == 0 {
                    observed / *terms as f64
                } else {
                    let log = ratio.ln();
                    (observed / *terms as f64) * ((log * *terms as f64).exp_m1() / log.exp_m1())
                }
            }
            Self::Chebyshev {
                coefficients,
                lower,
                upper,
            } => {
                let x = conditioned(channel, *lower, *upper);
                let mut previous = 1.0;
                let mut current = x;
                let mut value = 0.0;
                for &coefficient in coefficients {
                    value += coefficient * previous;
                    (previous, current) = (current, 2.0 * x * current - previous);
                }
                value
            }
            Self::ScaledPowers {
                coefficients,
                divisor,
            } => {
                let x = channel as f64 / divisor;
                coefficients
                    .iter()
                    .rev()
                    .fold(0.0, |value, coefficient| value * x + coefficient)
            }
        }
    }
}

fn conditioned(channel: usize, lower: usize, upper: usize) -> f64 {
    2.0 * (channel as f64 - lower as f64) / (upper - lower) as f64 - 1.0
}

// Update the QR factorization one row at a time in a basis conditioned on
// the actual fit interval. The unique polynomial is unchanged by this basis.
pub(super) struct Qr {
    size: usize,
    lower: usize,
    upper: usize,
    matrix: Vec<f64>,
    rhs: Vec<f64>,
    row: Vec<f64>,
}

impl Qr {
    fn new(size: usize, lower: usize, upper: usize) -> Result<Self, ImageError> {
        let count = checked_matrix(size)?;
        Ok(Self {
            size,
            lower,
            upper,
            matrix: vec![0.0; count],
            rhs: vec![0.0; size],
            row: vec![0.0; size],
        })
    }

    fn add(&mut self, channel: usize, mut value: f64) {
        let x = conditioned(channel, self.lower, self.upper);
        let mut previous = 1.0;
        let mut current = x;
        for element in &mut self.row {
            *element = previous;
            (previous, current) = (current, 2.0 * x * current - previous);
        }
        for index in 0..self.size {
            let pivot = index * self.size + index;
            let norm = self.matrix[pivot].hypot(self.row[index]);
            if norm == 0.0 {
                continue;
            }
            let cosine = self.matrix[pivot] / norm;
            let sine = self.row[index] / norm;
            self.matrix[pivot] = norm;
            for (a, b) in self.matrix[pivot + 1..(index + 1) * self.size]
                .iter_mut()
                .zip(&mut self.row[index + 1..])
            {
                (*a, *b) = (cosine * *a + sine * *b, -sine * *a + cosine * *b);
            }
            let rhs = &mut self.rhs[index];
            (*rhs, value) = (cosine * *rhs + sine * value, -sine * *rhs + cosine * value);
        }
    }

    fn solve(mut self) -> Prediction {
        for row in (0..self.size).rev() {
            let pivot = row * self.size + row;
            let diagonal = self.matrix[pivot];
            if diagonal == 0.0 || !diagonal.is_finite() {
                return Prediction::Invalid;
            }
            let tail: f64 = self.rhs[row + 1..]
                .iter()
                .zip(&self.matrix[pivot + 1..])
                .map(|(&coefficient, &matrix)| coefficient * matrix)
                .sum();
            self.rhs[row] = (self.rhs[row] - tail) / diagonal;
            if !self.rhs[row].is_finite() {
                return Prediction::Invalid;
            }
        }
        Prediction::Chebyshev {
            coefficients: self.rhs,
            lower: self.lower,
            upper: self.upper,
        }
    }
}

// CASA minimizes the norm in its column-scaled power basis when the number
// of samples is too small for a unique polynomial. Solve in sample space so
// storage grows with the number of samples instead of the requested degree squared.
fn fit_underdetermined(terms: usize, samples: &[(usize, f64)]) -> Option<Prediction> {
    let size = samples.len();
    let divisor = samples.last()?.0 as f64;
    let x: Vec<_> = samples
        .iter()
        .map(|&(index, _)| index as f64 / divisor)
        .collect();
    let values: Vec<_> = samples.iter().map(|&(_, value)| value).collect();
    let mut powers = vec![1.0; size];
    let mut gram = vec![0.0; size * size];
    for _ in 0..terms {
        let norm_sq: f64 = powers.iter().map(|value| value * value).sum();
        for (row, &power) in gram.chunks_exact_mut(size).zip(&powers) {
            for (element, &other) in row.iter_mut().zip(&powers) {
                *element += power * other / norm_sq;
            }
        }
        for (power, &x) in powers.iter_mut().zip(&x) {
            *power *= x;
        }
    }
    let dual = solve_symmetric_positive_definite(&gram, &values)?;
    powers.fill(1.0);
    let mut coefficients = Vec::with_capacity(terms);
    for _ in 0..terms {
        let norm_sq: f64 = powers.iter().map(|value| value * value).sum();
        let product: f64 = powers
            .iter()
            .zip(&dual)
            .map(|(power, weight)| power * weight)
            .sum();
        coefficients.push(product / norm_sq);
        for (power, &x) in powers.iter_mut().zip(&x) {
            *power *= x;
        }
    }
    Some(Prediction::ScaledPowers {
        coefficients,
        divisor,
    })
}
