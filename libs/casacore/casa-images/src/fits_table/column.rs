// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ffi::{CStr, c_char};
use std::ptr;

use casa_core::{ArrayValue, PrimitiveType, RecordValue, ScalarValue, Value};
use casa_tables::{ColumnOptions, ColumnSchema};
use fitsio::sys;
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use num_complex::{Complex32, Complex64};

use super::{FitsTableError, Reader, Result, cards::Header, fits_status};

mod listing;

pub(super) struct Column {
    pub name: String,
    number: i32,
    code: u32,
    repeat: usize,
    primitive: PrimitiveType,
    promoted: bool,
    heap: bool,
    shape: Option<Vec<usize>>,
    row_shape: Option<usize>,
}

impl Column {
    pub fn read(fits: &mut Reader, header: &Header) -> Result<Vec<Self>> {
        let (mut ncols, mut status) = (0, 0);
        // SAFETY: Ptr has exclusive use and ncols/status are writable.
        unsafe { sys::ffgncl(fits.ptr, &mut ncols, &mut status) };
        fits_status(status)?;
        let mut cols = (1..=ncols)
            .map(|number| Self::describe(fits, header, number))
            .collect::<Result<Vec<_>>>()?;
        let row_shapes = cols
            .iter()
            .enumerate()
            .filter_map(|(i, col)| {
                if col.primitive != PrimitiveType::String {
                    return None;
                }
                let refers_to = col
                    .name
                    .strip_prefix("TDIM")?
                    .parse::<usize>()
                    .ok()?
                    .checked_sub(1)?;
                (refers_to < cols.len()).then_some((refers_to, i))
            })
            .collect::<Vec<_>>();
        for (target, shaping) in row_shapes {
            cols[target].row_shape = Some(shaping);
        }
        Ok(cols)
    }

    fn describe(fits: &mut Reader, header: &Header, number: i32) -> Result<Self> {
        Self::describe_for(fits, header, number, false)
    }

    fn describe_for(
        fits: &mut Reader,
        header: &Header,
        number: i32,
        listing: bool,
    ) -> Result<Self> {
        let (mut code, mut repeat, mut width, mut status) = (0, 0, 0, 0);
        // SAFETY: Ptr has exclusive use and all outputs are writable.
        unsafe {
            sys::ffgtclll(
                fits.ptr,
                number,
                &mut code,
                &mut repeat,
                &mut width,
                &mut status,
            )
        };
        fits_status(status)?;
        let heap = code < 0;
        // CFITSIO may narrow an ASCII I field by its width. FITSTabular
        // exposes every ASCII I as Int, F/E as Float, and D as Double.
        let code = if fits.ascii_table {
            match header
                .text(&format!("TFORM{number}"))
                .and_then(|form| form.as_bytes().first())
            {
                Some(b'I') => sys::TINT,
                Some(b'F' | b'E') => sys::TFLOAT,
                Some(b'D') => sys::TDOUBLE,
                _ => code.unsigned_abs(),
            }
        } else {
            code.unsigned_abs()
        };
        let name = header
            .text(&format!("TTYPE{number}"))
            .unwrap_or("")
            .to_owned();
        let repeat = usize::try_from(if code == sys::TSTRING && !(listing && heap) {
            width.max(repeat)
        } else {
            repeat
        })
        .map_err(|_| invalid_column(&name, "invalid column length"))?;
        use PrimitiveType::*;
        let mut primitive = match code {
            sys::TBIT | sys::TLOGICAL => Bool,
            sys::TBYTE => UInt8,
            sys::TSHORT => Int16,
            sys::TLONG | sys::TINT => Int32,
            sys::TLONGLONG if listing => Int64,
            sys::TFLOAT => Float32,
            sys::TDOUBLE => Float64,
            sys::TCOMPLEX => Complex32,
            sys::TDBLCOMPLEX => Complex64,
            sys::TSTRING => String,
            _ => {
                return Err(invalid_column(
                    &name,
                    &format!("unsupported FITS field type {code}"),
                ));
            }
        };
        let promoted = !listing
            && matches!(primitive, UInt8 | Int16 | Int32)
            && (header.number(&format!("TSCAL{number}")).unwrap_or(1.0) != 1.0
                || header.number(&format!("TZERO{number}")).unwrap_or(0.0) != 0.0);
        if promoted {
            primitive = Float64;
        }
        let shape = if listing || heap || primitive == String {
            None
        } else if let Some(spec) = header.text(&format!("TDIM{number}")) {
            let shape = parse_shape(spec)?;
            if checked_product(&shape)? != repeat {
                return Err(invalid_column(
                    &name,
                    "TDIM product differs from the column length",
                ));
            }
            (shape != [1]).then_some(shape)
        } else if repeat != 1 {
            Some(vec![repeat])
        } else {
            None
        };
        Ok(Self {
            name,
            number,
            code,
            repeat,
            primitive,
            promoted,
            heap,
            shape,
            row_shape: None,
        })
    }

    fn variable(&self) -> bool {
        self.row_shape.is_some() || (self.heap && self.primitive != PrimitiveType::String)
    }

    pub fn schema(&self) -> Result<ColumnSchema> {
        if self.variable() {
            Ok(ColumnSchema::array_variable(
                &self.name,
                self.primitive,
                None,
            ))
        } else if let Some(shape) = &self.shape {
            Ok(
                ColumnSchema::array_fixed(&self.name, self.primitive, shape.clone()).with_options(
                    ColumnOptions {
                        direct: true,
                        undefined: false,
                    },
                )?,
            )
        } else {
            Ok(ColumnSchema::scalar(&self.name, self.primitive))
        }
    }

    pub fn keywords(&self, header: &Header) -> RecordValue {
        let mut keys = RecordValue::default();
        for (suffix, key) in [("TUNIT", "UNIT"), ("TDISP", "DISP")] {
            if let Some(value) = header
                .text(&format!("{suffix}{}", self.number))
                .filter(|value| !value.is_empty())
            {
                keys.upsert(key, Value::Scalar(ScalarValue::String(value.to_owned())));
            }
        }
        if !self.promoted
            && !self.heap
            && matches!(self.code, sys::TBYTE | sys::TSHORT | sys::TINT | sys::TLONG)
            && let Some(value @ ScalarValue::Int32(_)) =
                header.value(&format!("TNULL{}", self.number))
        {
            keys.upsert("NULL", Value::Scalar(value.clone()));
        }
        keys
    }

    pub fn value(&self, fits: &mut Reader, cols: &[Self], row: i64) -> Result<Value> {
        if self.primitive == PrimitiveType::String {
            return Ok(Value::Scalar(ScalarValue::String(self.text(fits, row)?)));
        }
        let available = self.row_length(fits, row)?;
        let row_shape = self
            .row_shape
            .map(|shape| cols[shape].text(fits, row))
            .transpose()?
            .map(|shape| parse_shape(&shape))
            .transpose()?;
        let heap_shape =
            (self.heap && self.primitive != PrimitiveType::String).then_some([available]);
        let shape = row_shape
            .as_deref()
            .or(self.shape.as_deref())
            .or(heap_shape.as_ref().map(|s| s.as_slice()));
        let len = shape.map_or(Ok(1), checked_product)?;
        if len > available && self.primitive != PrimitiveType::String {
            return Err(invalid_column(
                &self.name,
                "row TDIM exceeds the available FITS values",
            ));
        }
        macro_rules! value {
            ($variant:ident, $vec:expr) => {{
                let mut values = $vec;
                if let Some(shape) = shape {
                    let array = ArrayD::from_shape_vec(IxDyn(shape).f(), values)
                        .map_err(|error| invalid_column(&self.name, &error.to_string()))?;
                    Value::Array(ArrayValue::$variant(array))
                } else {
                    Value::Scalar(ScalarValue::$variant(
                        values
                            .pop()
                            .ok_or_else(|| invalid_column(&self.name, "missing scalar"))?,
                    ))
                }
            }};
        }
        Ok(match self.primitive {
            PrimitiveType::Bool => value!(Bool, self.logical(fits, row, len)?),
            PrimitiveType::UInt8 => value!(UInt8, self.numbers::<u8>(fits, row, len, sys::TBYTE)?),
            PrimitiveType::Int16 => {
                value!(Int16, self.numbers::<i16>(fits, row, len, sys::TSHORT)?)
            }
            PrimitiveType::Int32 => value!(Int32, self.numbers::<i32>(fits, row, len, sys::TINT)?),
            PrimitiveType::Float32 => {
                value!(Float32, self.numbers::<f32>(fits, row, len, sys::TFLOAT)?)
            }
            PrimitiveType::Float64 => {
                value!(Float64, self.numbers::<f64>(fits, row, len, sys::TDOUBLE)?)
            }
            PrimitiveType::Complex32 => value!(
                Complex32,
                self.numbers::<[f32; 2]>(fits, row, len, sys::TCOMPLEX)?
                    .into_iter()
                    .map(|[re, im]| Complex32::new(re, im))
                    .collect::<Vec<_>>()
            ),
            PrimitiveType::Complex64 => value!(
                Complex64,
                self.numbers::<[f64; 2]>(fits, row, len, sys::TDBLCOMPLEX)?
                    .into_iter()
                    .map(|[re, im]| Complex64::new(re, im))
                    .collect::<Vec<_>>()
            ),
            _ => unreachable!("FITS description constrains the primitive type"),
        })
    }

    fn row_length(&self, fits: &mut Reader, row: i64) -> Result<usize> {
        if !self.heap {
            return Ok(self.repeat);
        }
        self.descriptor(fits, row).map(|(len, _)| len)
    }

    fn descriptor(&self, fits: &mut Reader, row: i64) -> Result<(usize, i64)> {
        let (mut len, mut offset, mut status) = (0, 0, 0);
        // SAFETY: The cursor is on this binary table and the outputs are writable.
        unsafe {
            sys::ffgdesll(
                fits.ptr,
                self.number,
                row,
                &mut len,
                &mut offset,
                &mut status,
            )
        };
        fits_status(status)?;
        usize::try_from(len)
            .map(|len| (len, offset))
            .map_err(|_| invalid_column(&self.name, "invalid heap descriptor length"))
    }

    fn numbers<T: Default + Clone>(
        &self,
        fits: &mut Reader,
        row: i64,
        len: usize,
        read_as: u32,
    ) -> Result<Vec<T>> {
        let (mut any_null, mut status) = (0, 0);
        let mut buffer = vec![T::default(); len];
        if len > 0 {
            // SAFETY: The call sites supply a C-compatible T with exactly the
            // layout specified by read_as (a pair of floats for complex).
            // The buffer is writable for all len elements; NULL nulval keeps
            // FITS null sentinels instead of substituting an arbitrary value.
            unsafe {
                sys::ffgcv(
                    fits.ptr,
                    read_as as i32,
                    self.number,
                    row,
                    1,
                    len as i64,
                    ptr::null_mut(),
                    buffer.as_mut_ptr().cast(),
                    &mut any_null,
                    &mut status,
                );
            }
            fits_status(status)?;
        }
        Ok(buffer)
    }

    fn logical(&self, fits: &mut Reader, row: i64, len: usize) -> Result<Vec<bool>> {
        Ok(self
            .logical_bytes(fits, row, len)?
            .into_iter()
            .map(|value| value == 1)
            .collect())
    }

    fn logical_bytes(&self, fits: &mut Reader, row: i64, len: usize) -> Result<Vec<c_char>> {
        let mut data: Vec<c_char> = vec![0; len];
        let mut status = 0;
        if len > 0 {
            // SAFETY: Both CFITSIO calls write len C chars; the live buffer
            // and current table cursor stay exclusively borrowed.
            unsafe {
                if self.code == sys::TBIT {
                    sys::ffgcx(
                        fits.ptr,
                        self.number,
                        row,
                        1,
                        len as i64,
                        data.as_mut_ptr(),
                        &mut status,
                    );
                } else {
                    sys::ffgcl(
                        fits.ptr,
                        self.number,
                        row,
                        1,
                        len as i64,
                        data.as_mut_ptr(),
                        &mut status,
                    );
                }
            }
            fits_status(status)?;
        }
        Ok(data)
    }

    fn text(&self, fits: &mut Reader, row: i64) -> Result<String> {
        let width = self.row_length(fits, row)?;
        if width == 0 {
            return Ok(String::new());
        }
        let len = width
            .checked_add(1)
            .ok_or_else(|| invalid_column(&self.name, "text width out of bounds"))?;
        let mut buffer = vec![0; len];
        let (mut any_null, mut status) = (0, 0);
        let mut strings = [buffer.as_mut_ptr()];
        // SAFETY: Single writable string with space for the entire ASCII field
        // and a NUL terminator. CFITSIO's nulval is unset.
        unsafe {
            sys::ffgcvs(
                fits.ptr,
                self.number,
                row,
                1,
                1,
                ptr::null_mut(),
                strings.as_mut_ptr(),
                &mut any_null,
                &mut status,
            )
        };
        fits_status(status)?;
        // SAFETY: CFITSIO terminates the string in buffer on success.
        let s = unsafe { CStr::from_ptr(buffer.as_ptr()) }.to_string_lossy();
        Ok(s.trim_end_matches(' ').to_owned())
    }
}

fn invalid_column(name: &str, note: &str) -> FitsTableError {
    FitsTableError::Fits(format!("FITS column {name}: {note}"))
}

fn parse_shape(text: &str) -> Result<Vec<usize>> {
    let bad = || FitsTableError::Fits(format!("invalid FITS TDIM '{text}'"));
    text.trim()
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(bad)?
        .split(',')
        .map(|dim| dim.trim().parse().map_err(|_| bad()))
        .collect()
}

fn checked_product(shape: &[usize]) -> Result<usize> {
    shape
        .iter()
        .try_fold(1_usize, |size, axis| size.checked_mul(*axis))
        .ok_or_else(|| FitsTableError::Fits("FITS TDIM product is too large".into()))
}
