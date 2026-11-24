// SPDX-License-Identifier: LGPL-3.0-or-later
//! Fused extrema over bounded blocks of recorded visibility columns.

use std::collections::{BTreeMap, BTreeSet};

use casa_tables::{PlainReferenceRead, Table};
use casa_types::{ArrayValue, Value};
use ndarray::ArrayD;
use num_complex::Complex32;

use super::Extrema;
use crate::tool_visibility::{
    COMPLEX_PRODUCTS, VisibilityRead, VisibilityReadOptions, divide_visibility,
};
use crate::{MsError, MsResult};

const PRODUCTS: [&str; 4] = ["amplitude", "phase", "real", "imaginary"];

pub(super) struct RangeSamples {
    groups: Vec<ComplexRanges>,
    matrix_columns: BTreeSet<&'static str>,
    float: Option<Extrema>,
    weight: Option<Extrema>,
    sigma: Option<Extrema>,
    masked: bool,
}

struct ComplexRanges {
    prefix: &'static str,
    source: &'static str,
    uses_model: bool,
    extrema: [Option<Extrema>; 4],
}

impl RangeSamples {
    pub(super) fn new(main: &Table, names: &BTreeSet<String>, use_flags: bool) -> Self {
        let has_column = |name| {
            main.schema()
                .is_some_and(|schema| schema.contains_column(name))
        };
        let mut groups = Vec::new();
        let mut matrix_columns = BTreeSet::new();
        for (prefix, source, model) in COMPLEX_PRODUCTS {
            if !has_column(source) || (model && !has_column("MODEL_DATA")) {
                continue;
            }
            let extrema = PRODUCTS.map(|name| {
                names
                    .contains(&format!("{prefix}{name}"))
                    .then(Extrema::default)
            });
            if extrema.iter().all(Option::is_none) {
                continue;
            }
            matrix_columns.insert(source);
            if model {
                matrix_columns.insert("MODEL_DATA");
            }
            groups.push(ComplexRanges {
                prefix,
                source,
                uses_model: model,
                extrema,
            });
        }
        let float =
            (names.contains("float_data") && has_column("FLOAT_DATA")).then(Extrema::default);
        if float.is_some() {
            matrix_columns.insert("FLOAT_DATA");
        }
        let masked = use_flags && !matrix_columns.is_empty();
        Self {
            groups,
            matrix_columns,
            float,
            weight: names.contains("weight").then(Extrema::default),
            sigma: names.contains("sigma").then(Extrema::default),
            masked,
        }
    }

    fn bytes_per_row(&self, correlations: usize, channels: usize) -> usize {
        let size = self
            .matrix_columns
            .iter()
            .map(|&name| if name == "FLOAT_DATA" { 4 } else { 8 })
            .sum::<usize>();
        let vectors = 4 * (usize::from(self.weight.is_some()) + usize::from(self.sigma.is_some()));
        correlations
            .saturating_mul(
                channels
                    .saturating_mul(size + usize::from(self.masked))
                    .saturating_add(vectors),
            )
            .max(1)
    }

    pub(super) fn read(
        mut self,
        main: &Table,
        members: &[Table],
        reference: Option<&PlainReferenceRead>,
        rows: &[usize],
        (correlations, channels): (usize, usize),
        block_bytes: usize,
    ) -> MsResult<BTreeMap<String, Value>> {
        if self.matrix_columns.is_empty() && self.weight.is_none() && self.sigma.is_none() {
            return Ok(BTreeMap::new());
        }
        let block_rows = (block_bytes / self.bytes_per_row(correlations, channels)).max(1);
        let no_scalars = BTreeSet::new();
        for rows in rows.chunks(block_rows) {
            let read = VisibilityRead::new(
                main,
                members,
                reference,
                rows,
                rows.first().copied(),
                &no_scalars,
                VisibilityReadOptions::default(),
            )?;
            self.read_block(&read)?;
        }
        Ok(self.results())
    }

    fn read_block(&mut self, read: &VisibilityRead<'_>) -> MsResult<()> {
        let mask = self
            .masked
            .then(|| read.raw_matrix_cells("FLAG", false))
            .transpose()?;
        let flags = match &mask {
            Some(ArrayValue::Bool(values)) => Some(values),
            None => None,
            _ => return Err(invalid("FLAG range requires Bool cells")),
        };
        let mut matrices = BTreeMap::new();
        for &column in &self.matrix_columns {
            matrices.insert(column, read.raw_matrix_cells(column, false)?);
        }
        for group in &mut self.groups {
            let Some(ArrayValue::Complex32(values)) = matrices.get(group.source) else {
                return Err(invalid("visibility range requires Complex cells"));
            };
            let expected = if group.uses_model {
                let Some(ArrayValue::Complex32(expected)) = matrices.get("MODEL_DATA") else {
                    return Err(invalid("MODEL_DATA range requires Complex cells"));
                };
                if values.shape() != expected.shape() {
                    return Err(invalid("visibility and model range axes differ"));
                }
                Some(memory(expected)?)
            } else {
                None
            };
            let flags = mask_for(values, flags)?;
            let data = memory(values)?;
            match (expected, group.prefix == "ratio_") {
                (Some(expected), true) => group.reduce(
                    data.iter().zip(expected).map(|(&data, &model)| {
                        if model == Complex32::ZERO {
                            Complex32::ONE
                        } else {
                            divide_visibility(data, model)
                        }
                    }),
                    flags,
                ),
                (Some(expected), false) => group.reduce(
                    data.iter()
                        .zip(expected)
                        .map(|(&data, &model)| data - model),
                    flags,
                ),
                (None, _) => group.reduce(data.iter().copied(), flags),
            }
        }
        if let Some(range) = &mut self.float {
            let Some(ArrayValue::Float32(values)) = matrices.get("FLOAT_DATA") else {
                return Err(invalid("FLOAT_DATA range requires Float cells"));
            };
            let flags = mask_for(values, flags)?;
            for (index, &value) in memory(values)?.iter().enumerate() {
                if !flags.is_some_and(|mask| mask[index]) {
                    range.add(value.into());
                }
            }
        }
        for (name, range) in [("SIGMA", &mut self.sigma), ("WEIGHT", &mut self.weight)] {
            if let Some(range) = range {
                let ArrayValue::Float32(values) = read.vector_cells(name)? else {
                    return Err(invalid(
                        "visibility weight or sigma range requires Float cells",
                    ));
                };
                for &value in memory(&values)? {
                    range.add(value.into());
                }
            }
        }
        Ok(())
    }

    fn results(self) -> BTreeMap<String, Value> {
        let mut results = BTreeMap::new();
        for group in self.groups {
            for (quantity, range) in PRODUCTS.into_iter().zip(group.extrema) {
                if let Some(mut range) = range {
                    if quantity == "amplitude" {
                        range.0 = range
                            .0
                            .map(|values| values.map(|value| f64::from(value.sqrt() as f32)));
                    }
                    results.insert(
                        format!("{}{quantity}", group.prefix),
                        Value::Array(range.array()),
                    );
                }
            }
        }
        for (name, range) in [
            ("float_data", self.float),
            ("sigma", self.sigma),
            ("weight", self.weight),
        ] {
            if let Some(range) = range {
                results.insert(name.into(), Value::Array(range.array()));
            }
        }
        results
    }
}

impl ComplexRanges {
    fn reduce(&mut self, values: impl Iterator<Item = Complex32>, flags: Option<&[bool]>) {
        let [amplitude, phase, real, imaginary] = &mut self.extrema;
        for (index, value) in values.enumerate() {
            if flags.is_some_and(|mask| mask[index]) {
                continue;
            }
            if let Some(amplitude) = amplitude {
                // Square in f64 to retain the full finite f32 input range.
                amplitude.add(f64::from(value.re).powi(2) + f64::from(value.im).powi(2));
            }
            if let Some(phase) = phase {
                phase.add(f64::from(value.arg()));
            }
            if let Some(real) = real {
                real.add(f64::from(value.re));
            }
            if let Some(imaginary) = imaginary {
                imaginary.add(f64::from(value.im));
            }
        }
    }
}

fn memory<T>(array: &ArrayD<T>) -> MsResult<&[T]> {
    array
        .as_slice_memory_order()
        .ok_or_else(|| invalid("visibility range block is not contiguous"))
}

fn mask_for<'a, T>(
    values: &ArrayD<T>,
    flags: Option<&'a ArrayD<bool>>,
) -> MsResult<Option<&'a [bool]>> {
    flags
        .map(|flags| {
            if flags.shape() != values.shape() {
                return Err(invalid("FLAG and visibility range axes differ"));
            }
            memory(flags)
        })
        .transpose()
}

fn invalid(message: &str) -> MsError {
    MsError::InvalidInput(message.into())
}
