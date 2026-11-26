// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fmt::{Display, Write};
use std::ops::Range;

use fitsio::sys;

use super::{Column, Header, Reader, Result, fits_status, invalid_column};
use crate::fits_table::listing::real;

impl Column {
    pub(in crate::fits_table) fn describe_listing(
        fits: &mut Reader,
        header: &Header,
        number: i32,
    ) -> Result<Self> {
        let column = Self::describe_for(fits, header, number, true)?;
        let scale = header.number(&format!("TSCAL{number}")).unwrap_or(1.0);
        let zero = header.number(&format!("TZERO{number}")).unwrap_or(0.0);
        if scale != 1.0 || zero != 0.0 {
            let mut status = 0;
            // SAFETY: The table cursor has exclusive use of the open handle.
            // CASA shows the stored fields before scaling. Leave unscaled
            // columns alone because CFITSIO presents tiled images as images
            // for scaling even though the on-disk columns remain readable.
            unsafe { sys::fftscl(fits.ptr, number, 1.0, 0.0, &mut status) };
            fits_status(status)?;
        }
        Ok(column)
    }

    pub(in crate::fits_table) fn listing_description(&self, header: &Header, out: &mut String) {
        let number = self.number;
        let unit = header.text(&format!("TUNIT{number}")).unwrap_or("");
        let (repeat, kind) = if self.heap {
            (1, "VADESC")
        } else {
            (
                self.repeat,
                match self.code {
                    sys::TLOGICAL => "LOGICAL",
                    sys::TBIT => "BIT",
                    sys::TSTRING => "CHAR",
                    sys::TBYTE => "BYTE",
                    sys::TSHORT => "SHORT",
                    sys::TLONG | sys::TINT => "LONG",
                    sys::TLONGLONG => "LONGLONG",
                    sys::TFLOAT => "FLOAT",
                    sys::TDOUBLE => "DOUBLE",
                    sys::TCOMPLEX => "COMPLEX",
                    sys::TDBLCOMPLEX => "DCOMPLEX",
                    _ => "UNKNOWN",
                },
            )
        };
        let _ = writeln!(
            out,
            "Col {}: {repeat} {kind} {} {unit}",
            number - 1,
            self.name
        );
    }

    pub(in crate::fits_table) fn listing_cell(
        &self,
        fits: &mut Reader,
        row: i64,
        row_offset: i64,
        heap: &Range<i64>,
        out: &mut String,
    ) -> Result<()> {
        let (len, value_offset) = if self.heap {
            let (len, offset) = self.descriptor(fits, row)?;
            let value_offset = heap.start.saturating_add(offset);
            if offset < 0 || value_offset.saturating_add(self.value_bytes(len)?) > heap.end {
                return Err(invalid_column(
                    &self.name,
                    "descriptor extends beyond the FITS heap",
                ));
            }
            let _ = write!(out, "[{len},{offset}]  VA: ");
            (len, value_offset)
        } else {
            (self.repeat, row_offset)
        };
        if len == 0 {
            return Ok(());
        }
        match self.code {
            sys::TSTRING => {
                // The string readers trim spaces. The listing shows the
                // stored bytes up to the first NUL, as casacore does.
                let mut bytes = vec![0u8; len];
                let mut status = 0;
                // SAFETY: This HDU-relative offset is within the table row or
                // its validated heap, and bytes holds the requested length.
                unsafe {
                    sys::ffgextn(
                        fits.ptr,
                        value_offset,
                        len as i64,
                        bytes.as_mut_ptr().cast(),
                        &mut status,
                    );
                }
                fits_status(status)?;
                let visible = bytes.split(|&byte| byte == 0).next().unwrap_or_default();
                out.push_str(&String::from_utf8_lossy(visible));
            }
            sys::TBIT => self.listing_bits(fits, row, len, out)?,
            sys::TLOGICAL => join(
                out,
                self.logical_bytes(fits, row, len)?
                    .into_iter()
                    .map(|value| match value {
                        1 => "True",
                        0 => "False",
                        _ => "Undefined",
                    }),
            ),
            sys::TBYTE => join(out, self.numbers::<u8>(fits, row, len, sys::TBYTE)?),
            sys::TSHORT => join(out, self.numbers::<i16>(fits, row, len, sys::TSHORT)?),
            sys::TLONG | sys::TINT => join(out, self.numbers::<i32>(fits, row, len, sys::TINT)?),
            sys::TLONGLONG => join(out, self.numbers::<i64>(fits, row, len, sys::TLONGLONG)?),
            sys::TFLOAT | sys::TDOUBLE => {
                let values = self.numbers::<f64>(fits, row, len, sys::TDOUBLE)?;
                join(out, values.into_iter().map(real));
            }
            sys::TCOMPLEX | sys::TDBLCOMPLEX => {
                let values = self.numbers::<[f64; 2]>(fits, row, len, sys::TDBLCOMPLEX)?;
                join(
                    out,
                    values
                        .into_iter()
                        .map(|[re, im]| format!("({},{})", real(re), real(im))),
                );
            }
            _ => return Err(invalid_column(&self.name, "field type cannot be listed")),
        }
        if !self.heap {
            out.push(' ');
        }
        Ok(())
    }

    pub(in crate::fits_table) fn listing_row_bytes(
        &self,
        header: &Header,
        ascii: bool,
    ) -> Result<i64> {
        let format = header.text(&format!("TFORM{}", self.number)).unwrap_or("");
        if ascii {
            let format_width = format
                .get(1..)
                .unwrap_or("")
                .split('.')
                .next()
                .unwrap_or("");
            return format_width
                .parse::<i64>()
                .map_err(|_| invalid_column(&self.name, "invalid ASCII field width"));
        }
        if self.heap {
            let descriptor_bytes = if format.contains('Q') { 16 } else { 8 };
            Ok((self.repeat as i64).saturating_mul(descriptor_bytes))
        } else {
            self.value_bytes(self.repeat)
        }
    }

    fn value_bytes(&self, len: usize) -> Result<i64> {
        let length = i64::try_from(len).unwrap_or(i64::MAX);
        Ok(match self.code {
            sys::TBIT => len.div_ceil(8) as i64,
            sys::TLOGICAL | sys::TSTRING | sys::TBYTE => length,
            sys::TSHORT => length.saturating_mul(2),
            sys::TLONG | sys::TINT | sys::TFLOAT => length.saturating_mul(4),
            sys::TLONGLONG | sys::TDOUBLE | sys::TCOMPLEX => length.saturating_mul(8),
            sys::TDBLCOMPLEX => length.saturating_mul(16),
            _ => return Err(invalid_column(&self.name, "field type cannot be listed")),
        })
    }

    fn listing_bits(
        &self,
        fits: &mut Reader,
        row: i64,
        len: usize,
        out: &mut String,
    ) -> Result<()> {
        let bits = self.logical_bytes(fits, row, len)?;
        if self.heap {
            join(
                out,
                bits.into_iter()
                    .enumerate()
                    .map(|(index, bit)| bit << (7 - index % 8)),
            );
        } else {
            out.extend(bits.into_iter().map(|bit| if bit == 1 { '1' } else { '0' }));
        }
        Ok(())
    }
}

fn join(out: &mut String, values: impl IntoIterator<Item = impl Display>) {
    for (index, value) in values.into_iter().enumerate() {
        if index != 0 {
            out.push_str(", ");
        }
        let _ = write!(out, "{value}");
    }
}
