// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::{ArrayValue, PrimitiveType, ScalarValue, Value};
use casa_tables::ColumnSchema;
use ndarray::{ArrayD, IxDyn};
use num_complex::Complex32;

use super::{Result, invalid};

const SCHEMAS: &str = include_str!("tables.txt");

pub(super) struct Field<'a> {
    pub name: &'a str,
    kind: PrimitiveType,
    array: bool,
}

pub(super) fn read_schema(name: &str) -> Result<Vec<Field<'static>>> {
    let row = SCHEMAS
        .lines()
        .filter_map(|line| line.split_once(' '))
        .find_map(|(root, columns)| (root == name).then_some(columns))
        .ok_or_else(|| invalid(format!("unsupported table {name}")))?;
    row.split_ascii_whitespace()
        .map(|spec| {
            let (name, ty) = spec
                .split_once(':')
                .ok_or_else(|| invalid("incorrect built-in field schema"))?;
            let kind = match ty.to_ascii_uppercase().as_str() {
                "B" => PrimitiveType::Bool,
                "I" => PrimitiveType::Int32,
                "R" => PrimitiveType::Float32,
                "D" => PrimitiveType::Float64,
                "X" => PrimitiveType::Complex32,
                "A" => PrimitiveType::String,
                _ => return Err(invalid(format!("incorrect built-in type {ty}"))),
            };
            Ok(Field {
                name,
                kind,
                array: ty.as_bytes()[0].is_ascii_lowercase(),
            })
        })
        .collect()
}

impl Field<'_> {
    pub(super) fn column(&self) -> ColumnSchema {
        if self.array {
            ColumnSchema::array_variable(self.name, self.kind, None)
        } else {
            ColumnSchema::scalar(self.name, self.kind)
        }
    }

    pub(super) fn scalar_default(&self) -> Option<Value> {
        if self.array {
            return None;
        }
        use ScalarValue as S;
        Some(Value::Scalar(match self.kind {
            PrimitiveType::Bool => S::Bool(false),
            PrimitiveType::Int32 => S::Int32(0),
            PrimitiveType::Float32 => S::Float32(0.0),
            PrimitiveType::Float64 => S::Float64(0.0),
            PrimitiveType::String => S::String(String::new()),
            _ => unreachable!("ASDM schema declares no other scalars"),
        }))
    }

    pub(super) fn value(&self, text: &str) -> Result<Value> {
        let text = text.trim();
        if self.array {
            return self.array_value(text).map(Value::Array);
        }
        use ScalarValue as S;
        Ok(Value::Scalar(match self.kind {
            PrimitiveType::Bool => S::Bool(self.parse_boolean(text)?),
            PrimitiveType::Int32 => S::Int32(self.parse(text)?),
            PrimitiveType::Float32 => S::Float32(self.parse(text)?),
            PrimitiveType::Float64 => S::Float64(self.parse(text)?),
            PrimitiveType::String => S::String(text.into()),
            _ => unreachable!("ASDM schema declares no other scalars"),
        }))
    }

    fn array_value(&self, text: &str) -> Result<ArrayValue> {
        let mut tokens = Tokens(text);
        let rank: usize = self.parse(tokens.next()?)?;
        if !(1..=4).contains(&rank) {
            return Err(invalid(format!("{}: invalid array rank {rank}", self.name)));
        }
        let shape = (0..rank)
            .map(|_| self.parse(tokens.next()?))
            .collect::<Result<Vec<usize>>>()?;
        let count = shape
            .iter()
            .try_fold(1_usize, |size, len| size.checked_mul(*len))
            .ok_or_else(|| invalid(format!("{}: array shape overflows", self.name)))?;
        let malformed = || {
            invalid(format!(
                "{}: array dimensions do not match value count",
                self.name
            ))
        };
        if count > text.len() {
            return Err(malformed());
        }
        macro_rules! build {
            ($variant:ident, $read:expr) => {{
                let values = (0..count).map(|_| $read).collect::<Result<Vec<_>>>()?;
                ArrayValue::$variant(
                    ArrayD::from_shape_vec(IxDyn(&shape), values).map_err(|_| malformed())?,
                )
            }};
        }
        let data = match self.kind {
            PrimitiveType::Bool => build!(Bool, self.parse_boolean(tokens.next()?)),
            PrimitiveType::Int32 => build!(Int32, self.parse(tokens.next()?)),
            PrimitiveType::Float64 => build!(Float64, self.parse(tokens.next()?)),
            PrimitiveType::String => build!(String, Ok(tokens.next()?.into())),
            PrimitiveType::Complex32 => build!(
                Complex32,
                Ok(Complex32::new(
                    self.parse(tokens.next()?)?,
                    self.parse(tokens.next()?)?
                ))
            ),
            _ => unreachable!("ASDM schema declares no other arrays"),
        };
        if !tokens.0.trim().is_empty() {
            return Err(malformed());
        }
        Ok(data)
    }

    fn parse<T: std::str::FromStr>(&self, value: &str) -> Result<T> {
        value
            .parse()
            .map_err(|_| invalid(format!("{}: invalid value '{value}'", self.name)))
    }

    fn parse_boolean(&self, value: &str) -> Result<bool> {
        match value {
            "true" | "1" => Ok(true),
            "false" | "0" => Ok(false),
            _ => Err(invalid(format!("{}: invalid boolean '{value}'", self.name))),
        }
    }
}

struct Tokens<'a>(&'a str);

impl<'a> Tokens<'a> {
    fn next(&mut self) -> Result<&'a str> {
        let rest = self.0.trim_start();
        if let Some(quoted) = rest.strip_prefix('"') {
            let (token, remainder) = quoted
                .split_once('"')
                .ok_or_else(|| invalid("unclosed quoted array element"))?;
            self.0 = remainder;
            return Ok(token);
        }
        if rest.is_empty() {
            return Err(invalid("missing array element"));
        }
        let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        let (token, remainder) = rest.split_at(end);
        self.0 = remainder;
        Ok(token)
    }
}
