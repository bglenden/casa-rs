// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

impl AipsIo {
    /// Write a boolean value in canonical format (1 byte: `0` or `1`).
    ///
    /// Must be called inside a [`putstart`](Self::putstart)/[`putend`](Self::putend) block.
    pub fn put_bool(&mut self, value: bool) -> AipsIoObjectResult<()> {
        self.put_u8(u8::from(value))
    }

    /// Write a signed 8-bit integer value in canonical format (1 byte).
    ///
    /// Must be called inside a [`putstart`](Self::putstart)/[`putend`](Self::putend)
    /// block. All other scalar `put_*` methods follow the same semantics.
    pub fn put_i8(&mut self, value: i8) -> AipsIoObjectResult<()> {
        self.test_put()?;
        self.write_counted(&[value as u8])
    }

    /// Write an unsigned 8-bit integer value (1 byte). See [`put_i8`](Self::put_i8).
    pub fn put_u8(&mut self, value: u8) -> AipsIoObjectResult<()> {
        self.test_put()?;
        self.write_counted(&[value])
    }

    /// Write a signed 16-bit integer in canonical byte order (2 bytes). See [`put_i8`](Self::put_i8).
    pub fn put_i16(&mut self, value: i16) -> AipsIoObjectResult<()> {
        self.test_put()?;
        let bytes = self.encode_i16(value);
        self.write_counted(&bytes)
    }

    /// Write an unsigned 16-bit integer in canonical byte order (2 bytes). See [`put_i8`](Self::put_i8).
    pub fn put_u16(&mut self, value: u16) -> AipsIoObjectResult<()> {
        self.test_put()?;
        let bytes = self.encode_u16(value);
        self.write_counted(&bytes)
    }

    /// Write a signed 32-bit integer in canonical byte order (4 bytes). See [`put_i8`](Self::put_i8).
    pub fn put_i32(&mut self, value: i32) -> AipsIoObjectResult<()> {
        self.test_put()?;
        let bytes = self.encode_i32(value);
        self.write_counted(&bytes)
    }

    /// Write an unsigned 32-bit integer in canonical byte order (4 bytes). See [`put_i8`](Self::put_i8).
    pub fn put_u32(&mut self, value: u32) -> AipsIoObjectResult<()> {
        self.test_put()?;
        let bytes = self.encode_u32(value);
        self.write_counted(&bytes)
    }

    /// Write a signed 64-bit integer in canonical byte order (8 bytes). See [`put_i8`](Self::put_i8).
    pub fn put_i64(&mut self, value: i64) -> AipsIoObjectResult<()> {
        self.test_put()?;
        let bytes = self.encode_i64(value);
        self.write_counted(&bytes)
    }

    /// Write an unsigned 64-bit integer in canonical byte order (8 bytes). See [`put_i8`](Self::put_i8).
    pub fn put_u64(&mut self, value: u64) -> AipsIoObjectResult<()> {
        self.test_put()?;
        let bytes = self.encode_u64(value);
        self.write_counted(&bytes)
    }

    /// Write a 32-bit float as its IEEE 754 bit pattern (4 bytes). See [`put_i8`](Self::put_i8).
    pub fn put_f32(&mut self, value: f32) -> AipsIoObjectResult<()> {
        self.put_u32(value.to_bits())
    }

    /// Write a 64-bit float as its IEEE 754 bit pattern (8 bytes). See [`put_i8`](Self::put_i8).
    pub fn put_f64(&mut self, value: f64) -> AipsIoObjectResult<()> {
        self.put_u64(value.to_bits())
    }

    /// Write a 32-bit complex number as two consecutive `f32` values (8 bytes). See [`put_i8`](Self::put_i8).
    pub fn put_complex32(&mut self, value: Complex32) -> AipsIoObjectResult<()> {
        self.put_f32(value.re)?;
        self.put_f32(value.im)
    }

    /// Write a 64-bit complex number as two consecutive `f64` values (16 bytes). See [`put_i8`](Self::put_i8).
    pub fn put_complex64(&mut self, value: Complex64) -> AipsIoObjectResult<()> {
        self.put_f64(value.re)?;
        self.put_f64(value.im)
    }

    /// Write a UTF-8 string, prefixed by its byte length as a `u32`. See [`put_i8`](Self::put_i8).
    pub fn put_string(&mut self, value: &str) -> AipsIoObjectResult<()> {
        let len = u32::try_from(value.len()).map_err(|_| AipsIoObjectError::LengthOverflow)?;
        self.put_u32(len)?;
        self.test_put()?;
        self.write_counted(value.as_bytes())
    }

    /// Write a slice of boolean values, bit-packed into bytes (1 bit per element).
    ///
    /// When `put_nr` is `true`, the element count is written first as a `u32`,
    /// matching the C++ `AipsIO::put(uInt, const Bool*)` overload. Pass
    /// `put_nr = false` when the count is tracked externally (e.g. inside
    /// [`put_array_value`](Self::put_value) where shape is stored separately).
    /// All other `put_*_slice` methods follow these same semantics.
    pub fn put_bool_slice(&mut self, values: &[bool], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        self.test_put()?;
        let packed = pack_bool_slice(values);
        self.write_counted(&packed)
    }

    /// Write a slice of signed 8-bit integer values.
    ///
    /// When `put_nr` is `true`, the element count is written first as a `u32`.
    /// See [`put_bool_slice`](Self::put_bool_slice) for details.
    pub fn put_i8_slice(&mut self, values: &[i8], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for &value in values {
            self.put_i8(value)?;
        }
        Ok(())
    }

    /// Write a slice of unsigned 8-bit integer values. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_u8_slice(&mut self, values: &[u8], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        self.test_put()?;
        self.write_counted(values)
    }

    /// Write a slice of signed 16-bit integer values. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_i16_slice(&mut self, values: &[i16], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for &value in values {
            self.put_i16(value)?;
        }
        Ok(())
    }

    /// Write a slice of unsigned 16-bit integer values. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_u16_slice(&mut self, values: &[u16], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for &value in values {
            self.put_u16(value)?;
        }
        Ok(())
    }

    /// Write a slice of signed 32-bit integer values. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_i32_slice(&mut self, values: &[i32], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for &value in values {
            self.put_i32(value)?;
        }
        Ok(())
    }

    /// Write a slice of unsigned 32-bit integer values. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_u32_slice(&mut self, values: &[u32], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for &value in values {
            self.put_u32(value)?;
        }
        Ok(())
    }

    /// Write a slice of signed 64-bit integer values. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_i64_slice(&mut self, values: &[i64], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for &value in values {
            self.put_i64(value)?;
        }
        Ok(())
    }

    /// Write a slice of unsigned 64-bit integer values. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_u64_slice(&mut self, values: &[u64], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for &value in values {
            self.put_u64(value)?;
        }
        Ok(())
    }

    /// Write a slice of 32-bit float values. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_f32_slice(&mut self, values: &[f32], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for &value in values {
            self.put_f32(value)?;
        }
        Ok(())
    }

    /// Write a slice of 64-bit float values. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_f64_slice(&mut self, values: &[f64], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for &value in values {
            self.put_f64(value)?;
        }
        Ok(())
    }

    /// Write a slice of 32-bit complex values. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_complex32_slice(
        &mut self,
        values: &[Complex32],
        put_nr: bool,
    ) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for &value in values {
            self.put_complex32(value)?;
        }
        Ok(())
    }

    /// Write a slice of 64-bit complex values. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_complex64_slice(
        &mut self,
        values: &[Complex64],
        put_nr: bool,
    ) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for &value in values {
            self.put_complex64(value)?;
        }
        Ok(())
    }

    /// Write a slice of UTF-8 strings, each prefixed by its byte length. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_string_slice(&mut self, values: &[String], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for value in values {
            self.put_string(value)?;
        }
        Ok(())
    }

    /// Read a boolean value in canonical format (1 byte; any non-zero value is `true`).
    ///
    /// Must be called inside a [`getstart`](Self::getstart)/[`getend`](Self::getend) block.
    pub fn get_bool(&mut self) -> AipsIoObjectResult<bool> {
        Ok(self.get_u8()? != 0)
    }

    /// Read a signed 8-bit integer value (1 byte).
    ///
    /// Must be called inside a [`getstart`](Self::getstart)/[`getend`](Self::getend)
    /// block. All other scalar `get_*` methods follow the same semantics.
    pub fn get_i8(&mut self) -> AipsIoObjectResult<i8> {
        self.test_get()?;
        let mut buf = [0_u8; 1];
        self.read_counted(&mut buf)?;
        Ok(buf[0] as i8)
    }

    /// Read an unsigned 8-bit integer value (1 byte). See [`get_i8`](Self::get_i8).
    pub fn get_u8(&mut self) -> AipsIoObjectResult<u8> {
        self.test_get()?;
        let mut buf = [0_u8; 1];
        self.read_counted(&mut buf)?;
        Ok(buf[0])
    }

    /// Read a signed 16-bit integer in canonical byte order (2 bytes). See [`get_i8`](Self::get_i8).
    pub fn get_i16(&mut self) -> AipsIoObjectResult<i16> {
        self.test_get()?;
        let mut buf = [0_u8; 2];
        self.read_counted(&mut buf)?;
        Ok(self.decode_i16(buf))
    }

    /// Read an unsigned 16-bit integer in canonical byte order (2 bytes). See [`get_i8`](Self::get_i8).
    pub fn get_u16(&mut self) -> AipsIoObjectResult<u16> {
        self.test_get()?;
        let mut buf = [0_u8; 2];
        self.read_counted(&mut buf)?;
        Ok(self.decode_u16(buf))
    }

    /// Read a signed 32-bit integer in canonical byte order (4 bytes). See [`get_i8`](Self::get_i8).
    pub fn get_i32(&mut self) -> AipsIoObjectResult<i32> {
        self.test_get()?;
        let mut buf = [0_u8; 4];
        self.read_counted(&mut buf)?;
        Ok(self.decode_i32(buf))
    }

    /// Read an unsigned 32-bit integer in canonical byte order (4 bytes). See [`get_i8`](Self::get_i8).
    pub fn get_u32(&mut self) -> AipsIoObjectResult<u32> {
        self.test_get()?;
        let mut buf = [0_u8; 4];
        self.read_counted(&mut buf)?;
        Ok(self.decode_u32(buf))
    }

    /// Read a signed 64-bit integer in canonical byte order (8 bytes). See [`get_i8`](Self::get_i8).
    pub fn get_i64(&mut self) -> AipsIoObjectResult<i64> {
        self.test_get()?;
        let mut buf = [0_u8; 8];
        self.read_counted(&mut buf)?;
        Ok(self.decode_i64(buf))
    }

    /// Read an unsigned 64-bit integer in canonical byte order (8 bytes). See [`get_i8`](Self::get_i8).
    pub fn get_u64(&mut self) -> AipsIoObjectResult<u64> {
        self.test_get()?;
        let mut buf = [0_u8; 8];
        self.read_counted(&mut buf)?;
        Ok(self.decode_u64(buf))
    }

    /// Read a 32-bit float from its IEEE 754 bit pattern (4 bytes). See [`get_i8`](Self::get_i8).
    pub fn get_f32(&mut self) -> AipsIoObjectResult<f32> {
        Ok(f32::from_bits(self.get_u32()?))
    }

    /// Read a 64-bit float from its IEEE 754 bit pattern (8 bytes). See [`get_i8`](Self::get_i8).
    pub fn get_f64(&mut self) -> AipsIoObjectResult<f64> {
        Ok(f64::from_bits(self.get_u64()?))
    }

    /// Read a 32-bit complex number as two consecutive `f32` values (8 bytes). See [`get_i8`](Self::get_i8).
    pub fn get_complex32(&mut self) -> AipsIoObjectResult<Complex32> {
        let re = self.get_f32()?;
        let im = self.get_f32()?;
        Ok(Complex32::new(re, im))
    }

    /// Read a 64-bit complex number as two consecutive `f64` values (16 bytes). See [`get_i8`](Self::get_i8).
    pub fn get_complex64(&mut self) -> AipsIoObjectResult<Complex64> {
        let re = self.get_f64()?;
        let im = self.get_f64()?;
        Ok(Complex64::new(re, im))
    }

    /// Read a length-prefixed UTF-8 string. See [`get_i8`](Self::get_i8).
    pub fn get_string(&mut self) -> AipsIoObjectResult<String> {
        let len = self.get_u32()? as usize;
        self.test_get()?;
        let mut buf = vec![0_u8; len];
        self.read_counted(&mut buf)?;
        Ok(String::from_utf8(buf)?)
    }

    /// Read exactly `values.len()` boolean values into the provided slice.
    ///
    /// Booleans are stored bit-packed (1 bit per element). The caller must
    /// supply a slice of exactly the right length; no count prefix is read.
    /// This mirrors C++ `AipsIO::get(uInt, Bool*)`. All other `get_*_into`
    /// methods follow the same caller-supplies-length semantics.
    pub fn get_bool_into(&mut self, values: &mut [bool]) -> AipsIoObjectResult<()> {
        self.test_get()?;
        let mut packed = vec![0_u8; values.len().div_ceil(8)];
        self.read_counted(&mut packed)?;
        unpack_bool_slice(&packed, values);
        Ok(())
    }

    /// Read exactly `values.len()` signed 8-bit values into the provided slice.
    ///
    /// See [`get_bool_into`](Self::get_bool_into) for details.
    pub fn get_i8_into(&mut self, values: &mut [i8]) -> AipsIoObjectResult<()> {
        self.test_get()?;
        let mut buf = vec![0_u8; values.len()];
        self.read_counted(&mut buf)?;
        for (value, byte) in values.iter_mut().zip(buf) {
            *value = byte as i8;
        }
        Ok(())
    }

    /// Read exactly `values.len()` unsigned 8-bit values into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_u8_into(&mut self, values: &mut [u8]) -> AipsIoObjectResult<()> {
        self.test_get()?;
        self.read_counted(values)
    }

    /// Read exactly `values.len()` signed 16-bit values into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_i16_into(&mut self, values: &mut [i16]) -> AipsIoObjectResult<()> {
        let order = self.byte_order;
        self.read_fixed_width_into(values, move |bytes| {
            primitive_codec::decode_i16(bytes, order)
        })
    }

    /// Read exactly `values.len()` unsigned 16-bit values into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_u16_into(&mut self, values: &mut [u16]) -> AipsIoObjectResult<()> {
        let order = self.byte_order;
        self.read_fixed_width_into(values, move |bytes| {
            primitive_codec::decode_u16(bytes, order)
        })
    }

    /// Read exactly `values.len()` signed 32-bit values into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_i32_into(&mut self, values: &mut [i32]) -> AipsIoObjectResult<()> {
        let order = self.byte_order;
        self.read_fixed_width_into(values, move |bytes| {
            primitive_codec::decode_i32(bytes, order)
        })
    }

    /// Read exactly `values.len()` unsigned 32-bit values into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_u32_into(&mut self, values: &mut [u32]) -> AipsIoObjectResult<()> {
        let order = self.byte_order;
        self.read_fixed_width_into(values, move |bytes| {
            primitive_codec::decode_u32(bytes, order)
        })
    }

    /// Read exactly `values.len()` signed 64-bit values into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_i64_into(&mut self, values: &mut [i64]) -> AipsIoObjectResult<()> {
        let order = self.byte_order;
        self.read_fixed_width_into(values, move |bytes| {
            primitive_codec::decode_i64(bytes, order)
        })
    }

    /// Read exactly `values.len()` unsigned 64-bit values into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_u64_into(&mut self, values: &mut [u64]) -> AipsIoObjectResult<()> {
        let order = self.byte_order;
        self.read_fixed_width_into(values, move |bytes| {
            primitive_codec::decode_u64(bytes, order)
        })
    }

    /// Read exactly `values.len()` 32-bit float values into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_f32_into(&mut self, values: &mut [f32]) -> AipsIoObjectResult<()> {
        let order = self.byte_order;
        self.read_fixed_width_into(values, move |bytes| {
            let bits = primitive_codec::decode_u32(bytes, order);
            f32::from_bits(bits)
        })
    }

    /// Read exactly `values.len()` 64-bit float values into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_f64_into(&mut self, values: &mut [f64]) -> AipsIoObjectResult<()> {
        let order = self.byte_order;
        self.read_fixed_width_into(values, move |bytes| {
            let bits = primitive_codec::decode_u64(bytes, order);
            f64::from_bits(bits)
        })
    }

    /// Read exactly `values.len()` 32-bit complex values into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_complex32_into(&mut self, values: &mut [Complex32]) -> AipsIoObjectResult<()> {
        let mut scalars = vec![
            0_f32;
            values
                .len()
                .checked_mul(2)
                .ok_or(AipsIoObjectError::LengthOverflow)?
        ];
        self.get_f32_into(&mut scalars)?;
        for (value, &[re, im]) in values.iter_mut().zip(scalars.as_chunks::<2>().0) {
            *value = Complex32::new(re, im);
        }
        Ok(())
    }

    /// Read exactly `values.len()` 64-bit complex values into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_complex64_into(&mut self, values: &mut [Complex64]) -> AipsIoObjectResult<()> {
        let mut scalars = vec![
            0_f64;
            values
                .len()
                .checked_mul(2)
                .ok_or(AipsIoObjectError::LengthOverflow)?
        ];
        self.get_f64_into(&mut scalars)?;
        for (value, &[re, im]) in values.iter_mut().zip(scalars.as_chunks::<2>().0) {
            *value = Complex64::new(re, im);
        }
        Ok(())
    }

    /// Read exactly `values.len()` UTF-8 strings into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_string_into(&mut self, values: &mut [String]) -> AipsIoObjectResult<()> {
        for value in values {
            *value = self.get_string()?;
        }
        Ok(())
    }

    /// Read a count-prefixed boolean array into a newly allocated `Vec<bool>`.
    ///
    /// Reads a `u32` element count, allocates, and fills using
    /// [`get_bool_into`](Self::get_bool_into). This is the Rust equivalent of
    /// C++ `AipsIO::getnew(uInt*, Bool**)`. All other `getnew_*` methods
    /// follow the same read-count-then-allocate semantics.
    pub fn getnew_bool(&mut self) -> AipsIoObjectResult<Vec<bool>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![false; nrv];
        self.get_bool_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed signed 8-bit array into a newly allocated `Vec<i8>`.
    ///
    /// See [`getnew_bool`](Self::getnew_bool) for details.
    pub fn getnew_i8(&mut self) -> AipsIoObjectResult<Vec<i8>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![0_i8; nrv];
        self.get_i8_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed unsigned 8-bit array into a newly allocated `Vec<u8>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_u8(&mut self) -> AipsIoObjectResult<Vec<u8>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![0_u8; nrv];
        self.get_u8_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed signed 16-bit array into a newly allocated `Vec<i16>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_i16(&mut self) -> AipsIoObjectResult<Vec<i16>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![0_i16; nrv];
        self.get_i16_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed unsigned 16-bit array into a newly allocated `Vec<u16>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_u16(&mut self) -> AipsIoObjectResult<Vec<u16>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![0_u16; nrv];
        self.get_u16_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed signed 32-bit array into a newly allocated `Vec<i32>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_i32(&mut self) -> AipsIoObjectResult<Vec<i32>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![0_i32; nrv];
        self.get_i32_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed unsigned 32-bit array into a newly allocated `Vec<u32>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_u32(&mut self) -> AipsIoObjectResult<Vec<u32>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![0_u32; nrv];
        self.get_u32_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed signed 64-bit array into a newly allocated `Vec<i64>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_i64(&mut self) -> AipsIoObjectResult<Vec<i64>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![0_i64; nrv];
        self.get_i64_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed unsigned 64-bit array into a newly allocated `Vec<u64>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_u64(&mut self) -> AipsIoObjectResult<Vec<u64>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![0_u64; nrv];
        self.get_u64_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed 32-bit float array into a newly allocated `Vec<f32>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_f32(&mut self) -> AipsIoObjectResult<Vec<f32>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![0_f32; nrv];
        self.get_f32_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed 64-bit float array into a newly allocated `Vec<f64>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_f64(&mut self) -> AipsIoObjectResult<Vec<f64>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![0_f64; nrv];
        self.get_f64_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed 32-bit complex array into a newly allocated `Vec<Complex32>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_complex32(&mut self) -> AipsIoObjectResult<Vec<Complex32>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![Complex32::new(0.0, 0.0); nrv];
        self.get_complex32_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed 64-bit complex array into a newly allocated `Vec<Complex64>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_complex64(&mut self) -> AipsIoObjectResult<Vec<Complex64>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![Complex64::new(0.0, 0.0); nrv];
        self.get_complex64_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed string array into a newly allocated `Vec<String>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_string(&mut self) -> AipsIoObjectResult<Vec<String>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![String::new(); nrv];
        self.get_string_into(&mut out)?;
        Ok(out)
    }

    /// Put one dynamically-typed casacore value (scalar, array, table ref, or record).
    ///
    /// The encoded payload contains a small value-kind tag and type metadata,
    /// so [`get_value`](Self::get_value) can reconstruct the value without
    /// extra caller-supplied type information.
    pub fn put_value(&mut self, value: &Value) -> AipsIoObjectResult<()> {
        match value {
            Value::Scalar(scalar) => {
                self.put_u8(VALUE_KIND_SCALAR)?;
                self.put_u8(primitive_type_tag(scalar.primitive_type()))?;
                self.put_scalar_value(scalar)
            }
            Value::Array(array) => {
                self.put_u8(VALUE_KIND_ARRAY)?;
                self.put_u8(primitive_type_tag(array.primitive_type()))?;
                self.put_array_value(array)
            }
            Value::TableRef(path) => {
                self.put_u8(VALUE_KIND_TABLE_REF)?;
                self.put_string(path)
            }
            Value::Record(record) => {
                self.put_u8(VALUE_KIND_RECORD)?;
                self.put_record_value(record)
            }
        }
    }

    /// Get one dynamically-typed casacore value written by [`put_value`](Self::put_value).
    pub fn get_value(&mut self) -> AipsIoObjectResult<Value> {
        let kind = self.get_u8()?;
        match kind {
            VALUE_KIND_SCALAR => {
                let primitive = primitive_type_from_tag(self.get_u8()?)?;
                Ok(Value::Scalar(self.get_scalar_value(primitive)?))
            }
            VALUE_KIND_ARRAY => {
                let primitive = primitive_type_from_tag(self.get_u8()?)?;
                Ok(Value::Array(self.get_array_value(primitive)?))
            }
            VALUE_KIND_TABLE_REF => Ok(Value::TableRef(self.get_string()?)),
            VALUE_KIND_RECORD => Ok(Value::Record(self.get_record_value()?)),
            _ => Err(AipsIoObjectError::InvalidValueKindTag(kind)),
        }
    }

    pub(super) fn put_scalar_value(&mut self, value: &ScalarValue) -> AipsIoObjectResult<()> {
        match value {
            ScalarValue::Bool(v) => self.put_bool(*v),
            ScalarValue::UInt8(v) => self.put_u8(*v),
            ScalarValue::UInt16(v) => self.put_u16(*v),
            ScalarValue::UInt32(v) => self.put_u32(*v),
            ScalarValue::Int16(v) => self.put_i16(*v),
            ScalarValue::Int32(v) => self.put_i32(*v),
            ScalarValue::Int64(v) => self.put_i64(*v),
            ScalarValue::Float32(v) => self.put_f32(*v),
            ScalarValue::Float64(v) => self.put_f64(*v),
            ScalarValue::Complex32(v) => self.put_complex32(*v),
            ScalarValue::Complex64(v) => self.put_complex64(*v),
            ScalarValue::String(v) => self.put_string(v),
        }
    }

    pub(super) fn get_scalar_value(
        &mut self,
        primitive: PrimitiveType,
    ) -> AipsIoObjectResult<ScalarValue> {
        match primitive {
            PrimitiveType::Bool => Ok(ScalarValue::Bool(self.get_bool()?)),
            PrimitiveType::UInt8 => Ok(ScalarValue::UInt8(self.get_u8()?)),
            PrimitiveType::UInt16 => Ok(ScalarValue::UInt16(self.get_u16()?)),
            PrimitiveType::UInt32 => Ok(ScalarValue::UInt32(self.get_u32()?)),
            PrimitiveType::Int16 => Ok(ScalarValue::Int16(self.get_i16()?)),
            PrimitiveType::Int32 => Ok(ScalarValue::Int32(self.get_i32()?)),
            PrimitiveType::Int64 => Ok(ScalarValue::Int64(self.get_i64()?)),
            PrimitiveType::Float32 => Ok(ScalarValue::Float32(self.get_f32()?)),
            PrimitiveType::Float64 => Ok(ScalarValue::Float64(self.get_f64()?)),
            PrimitiveType::Complex32 => Ok(ScalarValue::Complex32(self.get_complex32()?)),
            PrimitiveType::Complex64 => Ok(ScalarValue::Complex64(self.get_complex64()?)),
            PrimitiveType::String => Ok(ScalarValue::String(self.get_string()?)),
        }
    }

    /// The array's shape, then its elements in logical (row-major) order,
    /// the order [`get_array_value`](Self::get_array_value) rebuilds: an
    /// array in C layout straight from memory, any other (a Fortran-layout
    /// array read from a table, a strided view) element by element.
    pub(super) fn put_array_value(&mut self, value: &ArrayValue) -> AipsIoObjectResult<()> {
        let ndim = u32::try_from(value.ndim()).map_err(|_| AipsIoObjectError::LengthOverflow)?;
        self.put_u32(ndim)?;
        for &dim in value.shape() {
            let dim = u64::try_from(dim).map_err(|_| AipsIoObjectError::LengthOverflow)?;
            self.put_u64(dim)?;
        }

        match value {
            ArrayValue::Bool(values) => {
                if let Some(slice) = values.as_slice() {
                    self.put_bool_slice(slice, false)
                } else {
                    let packed: Vec<bool> = values.iter().copied().collect();
                    self.put_bool_slice(&packed, false)
                }
            }
            ArrayValue::UInt8(values) => {
                if let Some(slice) = values.as_slice() {
                    self.put_u8_slice(slice, false)
                } else {
                    let packed: Vec<u8> = values.iter().copied().collect();
                    self.put_u8_slice(&packed, false)
                }
            }
            ArrayValue::UInt16(values) => {
                if let Some(slice) = values.as_slice() {
                    self.put_u16_slice(slice, false)
                } else {
                    let packed: Vec<u16> = values.iter().copied().collect();
                    self.put_u16_slice(&packed, false)
                }
            }
            ArrayValue::UInt32(values) => {
                if let Some(slice) = values.as_slice() {
                    self.put_u32_slice(slice, false)
                } else {
                    let packed: Vec<u32> = values.iter().copied().collect();
                    self.put_u32_slice(&packed, false)
                }
            }
            ArrayValue::Int16(values) => {
                if let Some(slice) = values.as_slice() {
                    self.put_i16_slice(slice, false)
                } else {
                    let packed: Vec<i16> = values.iter().copied().collect();
                    self.put_i16_slice(&packed, false)
                }
            }
            ArrayValue::Int32(values) => {
                if let Some(slice) = values.as_slice() {
                    self.put_i32_slice(slice, false)
                } else {
                    let packed: Vec<i32> = values.iter().copied().collect();
                    self.put_i32_slice(&packed, false)
                }
            }
            ArrayValue::Int64(values) => {
                if let Some(slice) = values.as_slice() {
                    self.put_i64_slice(slice, false)
                } else {
                    let packed: Vec<i64> = values.iter().copied().collect();
                    self.put_i64_slice(&packed, false)
                }
            }
            ArrayValue::Float32(values) => {
                if let Some(slice) = values.as_slice() {
                    self.put_f32_slice(slice, false)
                } else {
                    let packed: Vec<f32> = values.iter().copied().collect();
                    self.put_f32_slice(&packed, false)
                }
            }
            ArrayValue::Float64(values) => {
                if let Some(slice) = values.as_slice() {
                    self.put_f64_slice(slice, false)
                } else {
                    let packed: Vec<f64> = values.iter().copied().collect();
                    self.put_f64_slice(&packed, false)
                }
            }
            ArrayValue::Complex32(values) => {
                if let Some(slice) = values.as_slice() {
                    self.put_complex32_slice(slice, false)
                } else {
                    let packed: Vec<Complex32> = values.iter().copied().collect();
                    self.put_complex32_slice(&packed, false)
                }
            }
            ArrayValue::Complex64(values) => {
                if let Some(slice) = values.as_slice() {
                    self.put_complex64_slice(slice, false)
                } else {
                    let packed: Vec<Complex64> = values.iter().copied().collect();
                    self.put_complex64_slice(&packed, false)
                }
            }
            ArrayValue::String(values) => {
                if let Some(slice) = values.as_slice() {
                    self.put_string_slice(slice, false)
                } else {
                    let packed: Vec<String> = values.iter().cloned().collect();
                    self.put_string_slice(&packed, false)
                }
            }
        }
    }

    pub(super) fn get_array_value(
        &mut self,
        primitive: PrimitiveType,
    ) -> AipsIoObjectResult<ArrayValue> {
        let ndim = self.get_u32()? as usize;
        let mut shape = Vec::with_capacity(ndim);
        let mut element_count = 1_usize;
        for _ in 0..ndim {
            let dim_u64 = self.get_u64()?;
            let dim = usize::try_from(dim_u64)
                .map_err(|_| AipsIoObjectError::DimensionOverflow(dim_u64))?;
            shape.push(dim);
            element_count = element_count
                .checked_mul(dim)
                .ok_or(AipsIoObjectError::LengthOverflow)?;
        }

        match primitive {
            PrimitiveType::Bool => {
                let mut values = vec![false; element_count];
                self.get_bool_into(&mut values)?;
                Ok(ArrayValue::Bool(array_from_shape_vec(&shape, values)?))
            }
            PrimitiveType::UInt8 => {
                let mut values = vec![0_u8; element_count];
                self.get_u8_into(&mut values)?;
                Ok(ArrayValue::UInt8(array_from_shape_vec(&shape, values)?))
            }
            PrimitiveType::UInt16 => {
                let mut values = vec![0_u16; element_count];
                self.get_u16_into(&mut values)?;
                Ok(ArrayValue::UInt16(array_from_shape_vec(&shape, values)?))
            }
            PrimitiveType::UInt32 => {
                let mut values = vec![0_u32; element_count];
                self.get_u32_into(&mut values)?;
                Ok(ArrayValue::UInt32(array_from_shape_vec(&shape, values)?))
            }
            PrimitiveType::Int16 => {
                let mut values = vec![0_i16; element_count];
                self.get_i16_into(&mut values)?;
                Ok(ArrayValue::Int16(array_from_shape_vec(&shape, values)?))
            }
            PrimitiveType::Int32 => {
                let mut values = vec![0_i32; element_count];
                self.get_i32_into(&mut values)?;
                Ok(ArrayValue::Int32(array_from_shape_vec(&shape, values)?))
            }
            PrimitiveType::Int64 => {
                let mut values = vec![0_i64; element_count];
                self.get_i64_into(&mut values)?;
                Ok(ArrayValue::Int64(array_from_shape_vec(&shape, values)?))
            }
            PrimitiveType::Float32 => {
                let mut values = vec![0_f32; element_count];
                self.get_f32_into(&mut values)?;
                Ok(ArrayValue::Float32(array_from_shape_vec(&shape, values)?))
            }
            PrimitiveType::Float64 => {
                let mut values = vec![0_f64; element_count];
                self.get_f64_into(&mut values)?;
                Ok(ArrayValue::Float64(array_from_shape_vec(&shape, values)?))
            }
            PrimitiveType::Complex32 => {
                let mut values = vec![Complex32::new(0.0, 0.0); element_count];
                self.get_complex32_into(&mut values)?;
                Ok(ArrayValue::Complex32(array_from_shape_vec(&shape, values)?))
            }
            PrimitiveType::Complex64 => {
                let mut values = vec![Complex64::new(0.0, 0.0); element_count];
                self.get_complex64_into(&mut values)?;
                Ok(ArrayValue::Complex64(array_from_shape_vec(&shape, values)?))
            }
            PrimitiveType::String => {
                let mut values = vec![String::new(); element_count];
                self.get_string_into(&mut values)?;
                Ok(ArrayValue::String(array_from_shape_vec(&shape, values)?))
            }
        }
    }

    pub(super) fn put_record_value(&mut self, value: &RecordValue) -> AipsIoObjectResult<()> {
        let field_count =
            u32::try_from(value.len()).map_err(|_| AipsIoObjectError::LengthOverflow)?;
        self.put_u32(field_count)?;
        for field in value.fields() {
            self.put_string(&field.name)?;
            self.put_value(&field.value)?;
        }
        Ok(())
    }

    pub(super) fn get_record_value(&mut self) -> AipsIoObjectResult<RecordValue> {
        let field_count = self.get_u32()? as usize;
        let mut fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            let name = self.get_string()?;
            let value = self.get_value()?;
            fields.push(RecordField::new(name, value));
        }
        Ok(RecordValue::new(fields))
    }
}
