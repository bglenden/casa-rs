// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's incremental type promotion while traversing nested Python lists.

use crate::{CasaFloat, ParameterValue};

use super::{array_parameter_element, casa_array_string};

struct Accumulator {
    rank: u8,
    values: Vec<ParameterValue>,
}

impl Accumulator {
    fn new() -> Self {
        Self {
            rank: 0,
            values: Vec::new(),
        }
    }

    fn resize(&mut self, size: usize) {
        let default = match self.rank {
            0 => ParameterValue::Bool(false),
            1 => ParameterValue::Integer(0),
            2 => ParameterValue::Float(0.0),
            3 => ParameterValue::Complex([CasaFloat(0.0), CasaFloat(0.0)]),
            _ => ParameterValue::String(String::new()),
        };
        if size > self.values.len() {
            self.values.resize(size, default);
        }
    }

    fn place(&mut self, index: usize, value: &ParameterValue) -> Result<(), &'static str> {
        let Some(value) = array_parameter_element(value) else {
            return Ok(());
        };
        let rank = scalar_rank(&value)?;
        if rank > self.rank {
            for old in &mut self.values {
                *old = promote(old, rank)?;
            }
            self.rank = rank;
        }
        self.values[index] = promote(&value, self.rank)?;
        Ok(())
    }

    fn visit(
        &mut self,
        values: &[ParameterValue],
        stride: usize,
        offset: usize,
    ) -> Result<(), &'static str> {
        if matches!(values.first(), Some(ParameterValue::Array(_))) {
            let stride_next = stride
                .checked_mul(values.len())
                .ok_or("array shape is too large (#977)")?;
            for (index, value) in values.iter().enumerate() {
                let ParameterValue::Array(nested) = value else {
                    return Err("ragged arrays are unsupported (#977)");
                };
                let position = index
                    .checked_mul(stride)
                    .and_then(|n| n.checked_add(offset))
                    .ok_or("array shape is too large (#977)")?;
                self.visit(nested, stride_next, position)?;
            }
        } else {
            let size = values
                .len()
                .checked_sub(1)
                .and_then(|n| n.checked_mul(stride))
                .and_then(|n| n.checked_add(offset))
                .and_then(|n| n.checked_add(1))
                .ok_or("array shape is too large (#977)")?;
            self.resize(size);
            for (index, value) in values.iter().enumerate() {
                let position = index
                    .checked_mul(stride)
                    .and_then(|n| n.checked_add(offset))
                    .ok_or("array shape is too large (#977)")?;
                self.place(position, value)?;
            }
        }
        Ok(())
    }
}

fn scalar_rank(value: &ParameterValue) -> Result<u8, &'static str> {
    match value {
        ParameterValue::Bool(_) => Ok(0),
        ParameterValue::Integer(_) => Ok(1),
        ParameterValue::Float(_) => Ok(2),
        ParameterValue::Complex(_) => Ok(3),
        ParameterValue::String(value) if !value.contains('\0') => Ok(4),
        _ => Err("array element is not supported (#977)"),
    }
}

fn promote(value: &ParameterValue, rank: u8) -> Result<ParameterValue, &'static str> {
    use ParameterValue::{Bool, Complex, Float, Integer, String as Text};
    if rank == 4 {
        return Ok(Text(casa_array_string(value)));
    }
    Ok(match (rank, value) {
        (0, Bool(value)) => Bool(*value),
        (1, Bool(value)) => Integer(i64::from(*value)),
        (1, Integer(value)) => Integer(*value),
        (2, Bool(value)) => Float(f64::from(u8::from(*value))),
        (2, Integer(value)) => Float(*value as f64),
        (2, Float(value)) => Float(*value),
        (3, Bool(value)) => Complex([CasaFloat(f64::from(u8::from(*value))), CasaFloat(0.0)]),
        (3, Integer(value)) => Complex([CasaFloat(*value as f64), CasaFloat(0.0)]),
        (3, Float(value)) => Complex([CasaFloat(*value), CasaFloat(0.0)]),
        (3, Complex(value)) => Complex(*value),
        _ => return Err("array element cannot be promoted (#977)"),
    })
}

/// Traverse in Python order and return the promoted values in row-major order.
pub(super) fn convert(
    values: &[ParameterValue],
    shape: &[usize],
) -> Result<Vec<ParameterValue>, &'static str> {
    let mut accumulator = Accumulator::new();
    accumulator.visit(values, 1, 0)?;
    let mut strides = Vec::with_capacity(shape.len());
    let mut total = 1usize;
    for &size in shape {
        strides.push(total);
        total = total
            .checked_mul(size)
            .ok_or("array shape is too large (#977)")?;
    }
    if accumulator.values.len() != total {
        return Err("invalid array shape (#977)");
    }
    let mut result = Vec::with_capacity(total);
    for linear in 0..total {
        let mut remaining = linear;
        let mut fortran_index = 0usize;
        for axis in (0..shape.len()).rev() {
            let position = remaining % shape[axis];
            remaining /= shape[axis];
            fortran_index += position * strides[axis];
        }
        result.push(accumulator.values[fortran_index].clone());
    }
    Ok(result)
}

/// Flat Python lists append supported elements while promoting the prior values.
pub(super) fn flat(values: &[ParameterValue]) -> Result<Vec<ParameterValue>, &'static str> {
    let mut accumulator = Accumulator::new();
    for value in values {
        let Some(value) = array_parameter_element(value) else {
            continue;
        };
        let index = accumulator.values.len();
        accumulator.resize(index + 1);
        accumulator.place(index, &value)?;
    }
    Ok(accumulator.values)
}
