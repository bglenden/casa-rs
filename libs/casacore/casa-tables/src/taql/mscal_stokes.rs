// SPDX-License-Identifier: LGPL-3.0-or-later
//! Correlation conversion for MeasurementSet TaQL expressions.

use std::f32::consts::FRAC_1_SQRT_2;

use casa_core::{ArrayValue as CellArray, Correlation};
use num_complex::{Complex32, Complex64};

use super::ast::{Expr, IndexStyle};
use super::eval::{
    ArrayValue, EvalContext, ExprValue, eval_constant, eval_constant_string, eval_expr,
};
use super::mscal_lookup::{open_subtable, table_error};
use super::{BoundRowFunction, TaqlError};
use crate::Table;

pub(super) fn bind(
    name: &str,
    expression: String,
    args: &[Expr],
    table: &Table,
    style: IndexStyle,
) -> Result<BoundRowFunction, TaqlError> {
    if !(1..=3).contains(&args.len()) {
        return Err(TaqlError::ArgumentCount {
            name: name.to_owned(),
            expected: "1 to 3".into(),
            got: args.len(),
        });
    }
    let products = args
        .get(1)
        .map(|value| eval_constant_string(value, name, style))
        .transpose()?
        .unwrap_or_else(|| "IQUV".into());
    let rescale = match args
        .get(2)
        .map(|value| eval_constant(value, name, style))
        .transpose()?
    {
        Some(ExprValue::Bool(value)) => value,
        None => false,
        _ => {
            return Err(type_error(
                "MSCAL.STOKES requires a constant Boolean rescale argument",
            ));
        }
    };
    let pol = open_subtable(table, "POLARIZATION")?;
    let codes = pol.column_accessor("CORR_TYPE").map_err(table_error)?;
    // casacore sets up the conversion with the first polarization row.
    let CellArray::Int32(codes) = codes.array_cell(0).map_err(table_error)? else {
        return Err(type_error("MSCAL.STOKES requires integer CORR_TYPE codes"));
    };
    let input = codes
        .iter()
        .copied()
        .map(|code| {
            (1..=20)
                .contains(&code)
                .then(|| code as usize - 1)
                .ok_or_else(|| type_error("MSCAL.STOKES input polarization frame is unsupported"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let plan = Conversion::new(&input, &products, rescale)?;
    let data = args[0].clone();
    Ok(BoundRowFunction::new(
        expression,
        move |context: &EvalContext<'_>| match eval_expr(&data, context)? {
            ExprValue::Array(input) => plan.convert(input, None),
            ExprValue::MaskedArray(input, mask) => plan.convert(*input, Some(mask)),
            _ => Err(type_error(
                "MSCAL.STOKES requires a Complex, Double or Bool array",
            )),
        },
    ))
}

struct Conversion {
    input_width: usize,
    outputs: Vec<Product>,
    stokes: Option<[Vec<Complex32>; 4]>,
}

struct Product {
    kind: Correlation,
    linear: Vec<Complex32>,
    weights: Vec<f32>,
    flags: Vec<bool>,
}

impl Conversion {
    fn new(input: &[usize], text: &str, rescale: bool) -> Result<Self, TaqlError> {
        let Some(first) = input.first() else {
            return Err(type_error(
                "MSCAL.STOKES requires at least one input correlation",
            ));
        };
        if !input.iter().all(|code| code / 4 == first / 4) {
            return Err(type_error("MSCAL.STOKES input polarizations mix frames"));
        }
        let text = match text.to_ascii_uppercase().as_str() {
            "IQUV" | "STOKES" => "I,Q,U,V",
            "CIRC" | "CIRCULAR" => "RR,RL,LR,LL",
            "LIN" | "LINEAR" => "XX,XY,YX,YY",
            _ => text,
        };
        let outputs = text
            .split(',')
            .map(|label| {
                let label = label.trim();
                Correlation::ALL
                    .into_iter()
                    .find(|value| value.label().eq_ignore_ascii_case(label))
                    .filter(|value| value.casacore_code() <= 20 || is_derived(*value))
                    .ok_or_else(|| {
                        type_error(&format!("unsupported MSCAL.STOKES product {label:?}"))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let stokes = outputs
            .iter()
            .any(|&kind| is_derived(kind))
            .then(|| std::array::from_fn(|code| coefficients(code, input, rescale)));
        let outputs = outputs
            .into_iter()
            .map(|kind| match &stokes {
                Some(rows) if is_derived(kind) => Product::derived(kind, rows, input.len()),
                _ => Product::linear(
                    kind,
                    coefficients(kind.casacore_code() as usize - 1, input, rescale),
                ),
            })
            .collect();
        Ok(Self {
            input_width: input.len(),
            outputs,
            stokes,
        })
    }

    fn convert(
        &self,
        mut input: ArrayValue,
        mask: Option<Vec<bool>>,
    ) -> Result<ExprValue, TaqlError> {
        if input.shape.first().copied() != Some(self.input_width) {
            return Err(type_error(
                "MSCAL.STOKES array correlation axis does not match CORR_TYPE",
            ));
        }
        input.shape[0] = self.outputs.len();
        let mut data = Vec::with_capacity(input.data.len() / self.input_width * self.outputs.len());
        for sample in input.data.chunks_exact(self.input_width) {
            match sample[0] {
                ExprValue::Complex(_) => self.convert_complex(sample, &mut data)?,
                ExprValue::Float(_) => self.convert_weights(sample, &mut data)?,
                ExprValue::Bool(_) => self.convert_flags(sample, &mut data)?,
                _ => {
                    return Err(type_error(
                        "MSCAL.STOKES requires a Complex, Double or Bool array",
                    ));
                }
            }
        }
        let mask = mask.map(|mask| {
            mask.chunks_exact(self.input_width)
                .flat_map(|sample| {
                    self.outputs
                        .iter()
                        .map(move |output| output.flagged(sample))
                })
                .collect()
        });
        Ok(ExprValue::from_array_with_mask(
            ArrayValue {
                shape: input.shape,
                data,
            },
            mask,
        ))
    }

    fn convert_complex(
        &self,
        sample: &[ExprValue],
        data: &mut Vec<ExprValue>,
    ) -> Result<(), TaqlError> {
        let sample = sample
            .iter()
            .map(|value| match value {
                ExprValue::Complex(value) => Ok(Complex32::new(value.re as f32, value.im as f32)),
                _ => Err(type_error("MSCAL.STOKES input array must be homogeneous")),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let product = |row: &[Complex32]| row.iter().zip(&sample).map(|(a, b)| a * b).sum();
        let stokes = self
            .stokes
            .as_ref()
            .map(|rows| rows.each_ref().map(|row| product(row)));
        data.extend(self.outputs.iter().map(|output| {
            let value = match stokes {
                Some(stokes) if is_derived(output.kind) => derived_value(output.kind, stokes),
                _ => product(&output.linear),
            };
            ExprValue::Complex(Complex64::new(f64::from(value.re), f64::from(value.im)))
        }));
        Ok(())
    }

    fn convert_flags(
        &self,
        sample: &[ExprValue],
        data: &mut Vec<ExprValue>,
    ) -> Result<(), TaqlError> {
        let sample = sample
            .iter()
            .map(|value| match value {
                ExprValue::Bool(value) => Ok(*value),
                _ => Err(type_error("MSCAL.STOKES input array must be homogeneous")),
            })
            .collect::<Result<Vec<_>, _>>()?;
        data.extend(
            self.outputs
                .iter()
                .map(|output| ExprValue::Bool(output.flagged(&sample))),
        );
        Ok(())
    }

    fn convert_weights(
        &self,
        sample: &[ExprValue],
        data: &mut Vec<ExprValue>,
    ) -> Result<(), TaqlError> {
        let sample = sample
            .iter()
            .map(|value| match value {
                ExprValue::Float(value) => Ok(*value as f32),
                _ => Err(type_error("MSCAL.STOKES input array must be homogeneous")),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let has_zero = sample.contains(&0.0);
        data.extend(self.outputs.iter().map(|output| {
            let variance = if has_zero {
                0.0
            } else {
                output
                    .weights
                    .iter()
                    .zip(&sample)
                    .map(|(factor, weight)| factor * factor / weight)
                    .sum()
            };
            ExprValue::Float(if variance == 0.0 {
                0.0
            } else {
                f64::from(1.0_f32 / variance)
            })
        }));
        Ok(())
    }
}

impl Product {
    fn flagged(&self, sample: &[bool]) -> bool {
        self.flags
            .iter()
            .zip(sample)
            .any(|(&uses, &flag)| uses && flag)
    }

    fn linear(kind: Correlation, linear: Vec<Complex32>) -> Self {
        Self {
            kind,
            weights: linear.iter().map(|value| value.norm()).collect(),
            flags: linear
                .iter()
                .map(|value| *value != Complex32::ZERO)
                .collect(),
            linear,
        }
    }

    fn derived(kind: Correlation, rows: &[Vec<Complex32>; 4], width: usize) -> Self {
        use Correlation::{PFlinear, PFtotal, Ptotal};
        let terms = if matches!(kind, Ptotal | PFtotal) {
            3
        } else {
            2
        };
        let first_flag = usize::from(kind != PFlinear);
        let flags = (0..width)
            .map(|index| {
                kind == PFtotal
                    || rows[first_flag..=terms]
                        .iter()
                        .any(|row| row[index] != Complex32::ZERO)
            })
            .collect();
        let weights = (0..width)
            .map(|index| {
                rows[1..=terms]
                    .iter()
                    .map(|row| row[index].norm())
                    .sum::<f32>()
                    / terms as f32
            })
            .collect();
        Self {
            kind,
            linear: Vec::new(),
            weights,
            flags,
        }
    }
}

fn is_derived(kind: Correlation) -> bool {
    kind.casacore_code() >= Correlation::Ptotal.casacore_code()
}

fn derived_value(kind: Correlation, [i, q, u, v]: [Complex32; 4]) -> Complex32 {
    use Correlation::{PFlinear, PFtotal, Pangle, Plinear, Ptotal};
    let linear_squared = q.norm_sqr() + u.norm_sqr();
    Complex32::new(
        match kind {
            Ptotal => (linear_squared + v.norm_sqr()).sqrt(),
            Plinear => linear_squared.sqrt(),
            PFtotal => (linear_squared + v.norm_sqr()).sqrt() / i.norm(),
            PFlinear => linear_squared.sqrt() / i.norm(),
            Pangle => u.re.atan2(q.re) * 0.5,
            _ => unreachable!("linear product handled by the coefficient matrix"),
        },
        0.0,
    )
}

fn coefficients(output: usize, input: &[usize], rescale: bool) -> Vec<Complex32> {
    let output_row = basis_row(output);
    input
        .iter()
        .map(|&code| {
            let input_row = basis_row(code);
            let norm: f32 = input_row.iter().map(|value| value.norm_sqr()).sum();
            let value = output_row
                .iter()
                .zip(input_row)
                .map(|(out, input)| out * input.conj())
                .sum::<Complex32>()
                / norm;
            let clean = |part: f32| {
                [-1.0, -0.5, 0.0, 0.5, 1.0]
                    .into_iter()
                    .find(|value| (part - value).abs() < 1e-4)
                    .unwrap_or(part)
            };
            Complex32::new(clean(value.re), clean(value.im))
                * (factor(code, rescale) / factor(output, rescale))
        })
        .collect()
}

fn factor(code: usize, rescale: bool) -> f32 {
    match (rescale, code) {
        (true, 4..=11) => 0.5,
        (true, 12..=19) => FRAC_1_SQRT_2 * 0.5,
        _ => 1.0,
    }
}

/// Each row expands a product in I, Q, U and V. Receptor bases use
/// the same electric-field transforms on each end of the baseline.
fn basis_row(code: usize) -> [Complex32; 4] {
    let c = Complex32::new;
    let z = Complex32::ZERO;
    let h = c(0.5, 0.0);
    let ih = c(0.0, 0.5);
    let linear = [[h, h, z, z], [z, z, h, ih], [z, z, h, -ih], [h, -h, z, z]];
    if code < 4 {
        return std::array::from_fn(|index| c(if index == code { 1.0 } else { 0.0 }, 0.0));
    }
    let circular = [
        [c(FRAC_1_SQRT_2, 0.0), c(0.0, FRAC_1_SQRT_2)],
        [c(FRAC_1_SQRT_2, 0.0), c(0.0, -FRAC_1_SQRT_2)],
    ];
    let identity = [[c(1.0, 0.0), z], [z, c(1.0, 0.0)]];
    let (left, right) = match code / 4 {
        1 => (circular, circular),
        2 => (identity, identity),
        3 => (circular, identity),
        4 => (identity, circular),
        _ => unreachable!("only valid linear product codes are used"),
    };
    let left = left[(code % 4) / 2];
    let right = right[code % 2];
    let receptor = [
        left[0] * right[0].conj(),
        left[0] * right[1].conj(),
        left[1] * right[0].conj(),
        left[1] * right[1].conj(),
    ];
    std::array::from_fn(|index| {
        receptor
            .iter()
            .zip(linear)
            .map(|(weight, row)| weight * row[index])
            .sum()
    })
}

fn type_error(message: &str) -> TaqlError {
    TaqlError::TypeError {
        message: message.to_owned(),
    }
}
