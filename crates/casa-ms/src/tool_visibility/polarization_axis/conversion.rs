// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA-normalized complex polarization transforms for the visibility tool.

use casa_coordinates::StokesType;
use num_complex::{Complex32, Complex64};

use super::{MsResult, input};

pub(super) struct Conversion {
    input_count: usize,
    outputs: Result<Vec<Product>, &'static str>,
    stokes: Option<[LinearCombination; 4]>,
}

struct Product {
    value: ProductValue,
    flags: Vec<usize>,
    noise: Vec<(usize, f32)>,
}

enum ProductValue {
    Linear(LinearCombination),
    Derived(StokesType),
}

type LinearCombination = Vec<(usize, Complex32)>;

impl Conversion {
    pub(super) fn new(codes: &[i32], outputs: &[StokesType]) -> Self {
        let mut conversion = Self {
            input_count: codes.len(),
            outputs: Ok(Vec::with_capacity(outputs.len())),
            stokes: None,
        };
        let first = codes.first().copied().unwrap_or(0);
        if !(1..=20).contains(&first) {
            conversion.outputs = Err("Stokes conversion input frame is unsupported");
            return conversion;
        }
        if codes
            .iter()
            .any(|code| !(1..=20).contains(code) || (code - 1) / 4 != (first - 1) / 4)
        {
            conversion.outputs = Err("Stokes conversion cannot mix input frames");
            return conversion;
        }
        let basis = codes
            .iter()
            .copied()
            .map(stokes_contribution)
            .collect::<Vec<_>>();
        let plans = conversion.outputs.as_mut().expect("valid basis");
        for &output in outputs {
            if (1..=20).contains(&output.code()) {
                plans.push(Product::linear(coefficients(&basis, |stokes| {
                    project_product(output.code(), stokes)
                })));
            } else if (21..=27).contains(&output.code()) {
                // CASA recognizes these generic and single-dish products but
                // leaves their Complex output at zero when converting.
                plans.push(Product::linear(Vec::new()));
            } else if matches!(
                output,
                StokesType::Ptotal
                    | StokesType::Plinear
                    | StokesType::PFtotal
                    | StokesType::PFlinear
                    | StokesType::Pangle
            ) {
                let iquv = conversion.stokes.get_or_insert_with(|| {
                    std::array::from_fn(|index| coefficients(&basis, |stokes| stokes[index]))
                });
                plans.push(Product::derived(output, iquv, codes.len()));
            } else {
                conversion.outputs = Err("Stokes conversion output type is unsupported");
                break;
            }
        }
        conversion
    }

    pub(super) fn append(
        &self,
        samples: &[Complex32],
        output: &mut Vec<Complex32>,
    ) -> MsResult<()> {
        let products = self.products(samples.len())?;
        let iquv = self
            .stokes
            .as_ref()
            .map(|terms| terms.each_ref().map(|term| sum(term, samples)));
        for product in products {
            let value = match &product.value {
                ProductValue::Linear(terms) => sum(terms, samples),
                ProductValue::Derived(stokes) => {
                    derive(*stokes, iquv.expect("derived basis exists"))
                }
            };
            output.push(value);
        }
        Ok(())
    }

    pub(super) fn append_flags(&self, samples: &[bool], output: &mut Vec<bool>) -> MsResult<()> {
        for product in self.products(samples.len())? {
            output.push(product.flags.iter().any(|&index| samples[index]));
        }
        Ok(())
    }

    pub(super) fn append_weights(&self, samples: &[f32], output: &mut Vec<f32>) -> MsResult<()> {
        for product in self.products(samples.len())? {
            let variance = product
                .noise
                .iter()
                .try_fold(0.0, |variance, &(index, square)| {
                    let weight = samples[index];
                    (weight.is_finite() && weight > 0.0).then_some(variance + square / weight)
                });
            output.push(
                variance
                    .filter(|&value| value > 0.0)
                    .map_or(0.0, f32::recip),
            );
        }
        Ok(())
    }

    fn products(&self, count: usize) -> MsResult<&[Product]> {
        if count != self.input_count {
            return Err(input(
                "visibility correlation count does not match its polarization table",
            ));
        }
        self.outputs.as_deref().map_err(|&error| input(error))
    }
}

impl Product {
    fn linear(terms: LinearCombination) -> Self {
        Self {
            flags: terms.iter().map(|&(index, _)| index).collect(),
            noise: terms
                .iter()
                .map(|&(index, term)| (index, term.norm_sqr()))
                .collect(),
            value: ProductValue::Linear(terms),
        }
    }

    fn derived(kind: StokesType, iquv: &[LinearCombination; 4], input_count: usize) -> Self {
        let terms = match kind {
            StokesType::Ptotal | StokesType::PFtotal => &iquv[1..4],
            _ => &iquv[1..3],
        };
        let magnitude = |index| {
            terms
                .iter()
                .flat_map(|group| group.iter())
                .filter_map(|&(sample, factor)| (sample == index).then_some(factor.norm()))
                .sum::<f32>()
                / terms.len() as f32
        };
        let mut flags = Vec::new();
        let mut noise = Vec::new();
        for index in 0..input_count {
            let weight = magnitude(index);
            if weight > 0.0 {
                noise.push((index, weight * weight));
            }
            if weight > 0.0
                || kind == StokesType::PFtotal
                || (kind == StokesType::PFlinear
                    && iquv[0].iter().any(|&(sample, _)| sample == index))
            {
                flags.push(index);
            }
        }
        Self {
            value: ProductValue::Derived(kind),
            flags,
            noise,
        }
    }
}

fn coefficients(
    basis: &[[Complex64; 4]],
    mut coefficient: impl FnMut([Complex64; 4]) -> Complex64,
) -> LinearCombination {
    basis
        .iter()
        .copied()
        .enumerate()
        .filter_map(|(index, stokes)| {
            let coefficient = coefficient(stokes);
            (coefficient.norm_sqr() > 1e-24).then_some((
                index,
                Complex32::new(coefficient.re as f32, coefficient.im as f32),
            ))
        })
        .collect()
}

fn sum(terms: &LinearCombination, samples: &[Complex32]) -> Complex32 {
    terms
        .iter()
        .map(|&(index, scalar)| scalar * samples[index])
        .sum()
}

fn derive(kind: StokesType, [i, q, u, v]: [Complex32; 4]) -> Complex32 {
    let linear = q.norm_sqr() + u.norm_sqr();
    let value = match kind {
        StokesType::Ptotal => (linear + v.norm_sqr()).sqrt(),
        StokesType::Plinear => linear.sqrt(),
        StokesType::PFtotal => (linear + v.norm_sqr()).sqrt() / i.norm(),
        StokesType::PFlinear => linear.sqrt() / i.norm(),
        StokesType::Pangle => 0.5 * u.re.atan2(q.re),
        _ => unreachable!("only derived outputs are compiled"),
    };
    Complex32::new(value, 0.0)
}

/// Treat one stored product as nonzero and reverse its receptor basis rotation.
fn stokes_contribution(code: i32) -> [Complex64; 4] {
    if code <= 4 {
        let mut result = [Complex64::ZERO; 4];
        result[code as usize - 1] = Complex64::ONE;
        return result;
    }
    let (left, right, scale) = receptors(code);
    let xx = left[0].conj() * right[0] * scale;
    let xy = left[0].conj() * right[1] * scale;
    let yx = left[1].conj() * right[0] * scale;
    let yy = left[1].conj() * right[1] * scale;
    [xx + yy, xx - yy, xy + yx, -Complex64::I * (xy - yx)]
}

fn project_product(code: i32, [i, q, u, v]: [Complex64; 4]) -> Complex64 {
    if code <= 4 {
        return [i, q, u, v][code as usize - 1];
    }
    let (left, right, scale) = receptors(code);
    let x = (i + q) * right[0].conj() + (u + Complex64::I * v) * right[1].conj();
    let y = (u - Complex64::I * v) * right[0].conj() + (i - q) * right[1].conj();
    (left[0] * x + left[1] * y) * (0.5 / scale)
}

/// Jones vectors for the left and right receptors in the linear basis. CASA
/// normalizes same-basis products by 1/2 and mixed products by sqrt(2)/4.
fn receptors(code: i32) -> ([Complex64; 2], [Complex64; 2], f64) {
    use std::f64::consts::FRAC_1_SQRT_2;
    let linear = [
        [Complex64::ONE, Complex64::ZERO],
        [Complex64::ZERO, Complex64::ONE],
    ];
    let circular = [
        [Complex64::ONE * FRAC_1_SQRT_2, Complex64::I * FRAC_1_SQRT_2],
        [
            Complex64::ONE * FRAC_1_SQRT_2,
            -Complex64::I * FRAC_1_SQRT_2,
        ],
    ];
    let index = (code - 5) as usize;
    let (left, right, scale) = match index / 4 {
        0 => (circular, circular, 0.5),
        1 => (linear, linear, 0.5),
        2 => (circular, linear, 0.5 * FRAC_1_SQRT_2),
        3 => (linear, circular, 0.5 * FRAC_1_SQRT_2),
        _ => unreachable!("polarization type validated before projection"),
    };
    (left[index % 4 / 2], right[index % 2], scale)
}
