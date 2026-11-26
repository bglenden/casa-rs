// SPDX-License-Identifier: LGPL-3.0-or-later
//! ModComp numeric decoding used by legacy VLA archive records.

/// Decode a 16-bit signed integer stored in ModComp canonical order.
pub fn decode_i16(bytes: &[u8]) -> Result<i16, String> {
    let bytes = exact(bytes, 2, "i16")?;
    Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
}

/// Decode a 16-bit unsigned integer stored in ModComp canonical order.
pub fn decode_u16(bytes: &[u8]) -> Result<u16, String> {
    let bytes = exact(bytes, 2, "u16")?;
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

/// Decode a 32-bit signed integer stored in ModComp canonical order.
pub fn decode_i32(bytes: &[u8]) -> Result<i32, String> {
    let bytes = exact(bytes, 4, "i32")?;
    Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

/// Decode a 32-bit unsigned integer stored in ModComp canonical order.
pub fn decode_u32(bytes: &[u8]) -> Result<u32, String> {
    let bytes = exact(bytes, 4, "u32")?;
    Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

/// Decode a 32-bit ModComp floating-point value into IEEE `f32`.
pub fn decode_f32(bytes: &[u8]) -> Result<f32, String> {
    let source = exact(bytes, 4, "f32")?;
    let mut converted = [0_u8; 4];
    converted.copy_from_slice(source);

    let is_negative = (converted[0] & 0x80) != 0;
    if is_negative {
        twos_complement_in_place(&mut converted);
    }

    let is_zero = (converted[1] & 0x3f) == 0 && converted[2..].iter().all(|&byte| byte == 0);
    if is_zero {
        let bits = if is_negative { 0x8000_0000 } else { 0 };
        return Ok(f32::from_bits(bits));
    }

    let mut exponent =
        (((converted[0] & 0x7f) as i32) << 2) | (((converted[1] & 0xc0) >> 6) as i32);

    while (converted[1] & 0x20) == 0 {
        let mut index = converted.len() - 1;
        let mut msb_is_set = (converted[index] & 0x80) != 0;
        converted[index] <<= 1;
        index -= 1;
        while index > 1 {
            let prev_msb_is_set = msb_is_set;
            msb_is_set = (converted[index] & 0x80) != 0;
            converted[index] <<= 1;
            if prev_msb_is_set {
                converted[index] |= 0x01;
            }
            index -= 1;
        }
        converted[1] = ((converted[1] & 0x3f) << 1) | (converted[1] & 0xc0);
        if msb_is_set {
            converted[1] |= 0x01;
        }
        exponent -= 1;
    }

    if exponent > 384 {
        converted[2..].fill(0);
        converted[1] = 0x80;
        converted[0] = if is_negative { 0xff } else { 0x7f };
        return Ok(f32::from_be_bytes(converted));
    }

    if exponent < 108 {
        converted[1..].fill(0);
        converted[0] = if is_negative { 0x80 } else { 0x00 };
        return Ok(f32::from_be_bytes(converted));
    }

    if exponent > 130 {
        exponent -= 130;
        let mut index = converted.len() - 1;
        let mut msbits = converted[index] >> 6;
        converted[index] <<= 2;
        index -= 1;
        while index > 1 {
            let prev_msbits = msbits;
            msbits = converted[index] >> 6;
            converted[index] <<= 2;
            converted[index] |= prev_msbits;
            index -= 1;
        }
        converted[1] = (converted[1] << 2) | msbits;
        if (exponent & 0x0001) == 0 {
            converted[1] &= 0x7f;
        } else {
            converted[1] |= 0x80;
        }
        converted[0] = (exponent as u8) >> 1;
        if is_negative {
            converted[0] |= 0x80;
        }
        return Ok(f32::from_be_bytes(converted));
    }

    if exponent < 129 {
        let mut shift = 129 - exponent;
        while shift > 0 {
            let this_shift = shift.min(8) as u8;
            let comp_shift = 8 - this_shift;
            converted[1] &= 0x3f;
            let mut lsbits = ((converted[1] as u16) << comp_shift) as u8;
            converted[1] = ((converted[1] as u16) >> this_shift) as u8;
            let mut index = 2;
            while index < converted.len() - 1 {
                let prev_lsbits = lsbits;
                lsbits = ((converted[index] as u16) << comp_shift) as u8;
                converted[index] = ((converted[index] as u16) >> this_shift) as u8;
                converted[index] |= prev_lsbits;
                index += 1;
            }
            let last = converted.len() - 1;
            converted[last] = ((converted[last] as u16) >> this_shift) as u8;
            converted[last] |= lsbits;
            shift -= i32::from(this_shift);
        }
    } else if exponent == 130 {
        let mut index = converted.len() - 1;
        let mut msb_is_set = (converted[index] & 0x80) != 0;
        converted[index] <<= 1;
        index -= 1;
        while index > 1 {
            let prev_msb_is_set = msb_is_set;
            msb_is_set = (converted[index] & 0x80) != 0;
            converted[index] <<= 1;
            if prev_msb_is_set {
                converted[index] |= 0x01;
            }
            index -= 1;
        }
        converted[1] = (converted[1] & 0x3f) << 1;
        if msb_is_set {
            converted[1] |= 0x01;
        }
    } else {
        converted[1] &= 0x3f;
    }

    converted[0] = if is_negative { 0x80 } else { 0x00 };
    Ok(f32::from_be_bytes(converted))
}

/// Decode a 64-bit ModComp floating-point value into IEEE `f64`.
pub fn decode_f64(bytes: &[u8]) -> Result<f64, String> {
    let source = exact(bytes, 8, "f64")?;
    let mut converted = [0_u8; 8];
    converted.copy_from_slice(source);

    let is_negative = (converted[0] & 0x80) != 0;
    if is_negative {
        twos_complement_in_place(&mut converted);
    }

    let is_zero = (converted[1] & 0x3f) == 0 && converted[2..].iter().all(|&byte| byte == 0);
    if is_zero {
        let bits = if is_negative {
            0x8000_0000_0000_0000
        } else {
            0
        };
        return Ok(f64::from_bits(bits));
    }

    let mut exponent =
        (((converted[0] & 0x7f) as i32) << 2) | (((converted[1] & 0xc0) >> 6) as i32);

    while (converted[1] & 0x20) == 0 {
        let mut index = converted.len() - 1;
        let mut msb_is_set = (converted[index] & 0x80) != 0;
        converted[index] <<= 1;
        index -= 1;
        while index > 1 {
            let prev_msb_is_set = msb_is_set;
            msb_is_set = (converted[index] & 0x80) != 0;
            converted[index] <<= 1;
            if prev_msb_is_set {
                converted[index] |= 0x01;
            }
            index -= 1;
        }
        converted[1] = ((converted[1] & 0x3f) << 1) | (converted[1] & 0xc0);
        if msb_is_set {
            converted[1] |= 0x01;
        }
        exponent -= 1;
    }

    exponent += 766;
    for index in (2..converted.len()).rev() {
        converted[index] >>= 1;
        converted[index] |= (converted[index - 1] & 0x01) << 7;
    }
    converted[1] >>= 1;
    converted[1] &= 0x0f;
    converted[1] |= ((exponent & 0x000f) as u8) << 4;
    converted[0] = (exponent >> 4) as u8;
    if is_negative {
        converted[0] |= 0x80;
    } else {
        converted[0] &= 0x7f;
    }
    Ok(f64::from_be_bytes(converted))
}

fn exact<'a>(bytes: &'a [u8], expected: usize, ty: &str) -> Result<&'a [u8], String> {
    if bytes.len() != expected {
        return Err(format!(
            "expected {expected} bytes for {ty} decode, got {}",
            bytes.len()
        ));
    }
    Ok(bytes)
}

fn twos_complement_in_place(bytes: &mut [u8]) {
    for byte in bytes.iter_mut() {
        *byte = !*byte;
    }
    for index in (0..bytes.len()).rev() {
        let (value, carry) = bytes[index].overflowing_add(1);
        bytes[index] = value;
        if !carry {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_f32, decode_f64, decode_i16, decode_i32, decode_u16, decode_u32};

    #[test]
    fn decodes_modcomp_integer_examples() {
        assert_eq!(decode_i16(&[0x7e, 0xa5]).unwrap(), 32_421);
        assert_eq!(decode_i16(&[0xff, 0xef]).unwrap(), -17);
        assert_eq!(decode_u16(&[0xff, 0xef]).unwrap(), 65_519);
        assert_eq!(
            decode_i32(&[0x7e, 0xa5, 0x43, 0x21]).unwrap(),
            2_124_759_841
        );
        assert_eq!(decode_i32(&[0xff, 0xef, 0xab, 0xcd]).unwrap(), -1_070_131);
        assert_eq!(
            decode_u32(&[0xff, 0xef, 0xab, 0xcd]).unwrap(),
            4_293_897_165
        );
    }

    #[test]
    fn decodes_modcomp_float_examples_from_casacore() {
        let mut data = [0_u8; 4];
        data[3] = 0x01;

        for exponent_bits in [0_u16, 1, 63, 129, 130, 256, 300, 384, 511] {
            data[0] = (exponent_bits >> 2) as u8;
            let exponent = if exponent_bits > 256 {
                2_f64.powi((exponent_bits - 256) as i32)
            } else {
                1.0 / 2_f64.powi((256 - exponent_bits) as i32)
            };

            for mantissa in [0_u8, 1, 17, 31, 47, 62] {
                data[1] = ((exponent_bits & 0x0003) as u8) << 6 | mantissa;
                let expected =
                    (exponent * (f64::from(mantissa) / 64.0 + 1.0 / (256.0 * 256.0 * 64.0))) as f32;
                let result = decode_f32(&data).unwrap();
                if expected.is_infinite() {
                    assert_eq!(
                        result, expected,
                        "float mismatch for exponent={exponent_bits} mantissa={mantissa}: expected {expected:e}, got {result:e}"
                    );
                } else {
                    assert!(
                        (result - expected).abs() <= f32::from_bits(1),
                        "float mismatch for exponent={exponent_bits} mantissa={mantissa}: expected {expected:e}, got {result:e}"
                    );
                }

                let mut neg = data;
                neg[0] = !neg[0];
                neg[1] = !neg[1];
                neg[2] = !neg[2];
                neg[3] = !neg[3];
                neg[3] = neg[3].wrapping_add(1);
                let neg_result = decode_f32(&neg).unwrap();
                if expected.is_infinite() {
                    assert_eq!(
                        neg_result, -expected,
                        "negative float mismatch for exponent={exponent_bits} mantissa={mantissa}: expected {:e} got {neg_result:e}",
                        -expected
                    );
                } else {
                    assert!(
                        (neg_result + expected).abs() <= f32::from_bits(1),
                        "negative float mismatch for exponent={exponent_bits} mantissa={mantissa}: expected {:e} got {neg_result:e}",
                        -expected
                    );
                }
            }
        }
    }

    #[test]
    fn decodes_modcomp_float_signed_zero_examples() {
        let mut data = [0_u8; 8];
        for exponent_bits in [0_u16, 1, 255, 256, 511] {
            data[0] = (exponent_bits >> 2) as u8;
            data[4] = data[0] | 0x80;
            data[1] = ((exponent_bits & 0x0003) as u8) << 6;
            data[5] = data[1];

            let plus = decode_f32(&data[..4]).unwrap();
            let minus = decode_f32(&data[4..]).unwrap();
            assert_eq!(plus.to_bits(), 0.0f32.to_bits());
            assert_eq!(minus.to_bits(), (-0.0f32).to_bits());
        }
    }

    #[test]
    fn decodes_modcomp_float_unnormalized_examples_from_casacore() {
        assert_eq!(decode_f32(&[0x40, 0x10, 0x00, 0x00]).unwrap(), 0.25);
        assert_eq!(decode_f32(&[0x40, 0x08, 0x00, 0x00]).unwrap(), 0.125);
        assert_eq!(decode_f32(&[0xbf, 0xf0, 0x00, 0x00]).unwrap(), -0.25);
    }

    #[test]
    fn decodes_modcomp_double_examples_from_casacore() {
        let mut data = [0_u8; 8];
        data[7] = 0x02;

        for exponent_bits in [0_u16, 1, 63, 129, 130, 256, 300, 384, 511] {
            data[0] = (exponent_bits >> 2) as u8;
            let exponent = if exponent_bits > 256 {
                2_f64.powi((exponent_bits - 256) as i32)
            } else {
                1.0 / 2_f64.powi((256 - exponent_bits) as i32)
            };

            for mantissa in [0_u8, 1, 17, 31, 47, 62] {
                data[1] = ((exponent_bits & 0x0003) as u8) << 6 | mantissa;
                let expected = exponent
                    * (f64::from(mantissa) / 64.0 + 1.0 / 65_536.0 / 65_536.0 / 65_536.0 / 32.0);
                let result = decode_f64(&data).unwrap();
                assert_eq!(
                    result, expected,
                    "double mismatch for exponent={exponent_bits} mantissa={mantissa}"
                );

                let mut neg = data;
                for byte in &mut neg {
                    *byte = !*byte;
                }
                neg[7] = neg[7].wrapping_add(1);
                let neg_result = decode_f64(&neg).unwrap();
                assert_eq!(
                    neg_result, -expected,
                    "negative double mismatch for exponent={exponent_bits} mantissa={mantissa}"
                );
            }
        }
    }

    #[test]
    fn decodes_modcomp_double_unnormalized_examples_from_casacore() {
        assert_eq!(
            decode_f64(&[0x40, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]).unwrap(),
            0.25
        );
        assert_eq!(
            decode_f64(&[0x40, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]).unwrap(),
            0.125
        );
        assert_eq!(
            decode_f64(&[0xbf, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]).unwrap(),
            -0.25
        );
    }

    #[test]
    fn decodes_modcomp_double_signed_zero_examples() {
        let mut data = [0_u8; 16];
        for exponent_bits in [0_u16, 1, 255, 256, 511] {
            data[0] = (exponent_bits >> 2) as u8;
            data[8] = data[0] | 0x80;
            data[1] = ((exponent_bits & 0x0003) as u8) << 6;
            data[9] = data[1];

            let plus = decode_f64(&data[..8]).unwrap();
            let minus = decode_f64(&data[8..]).unwrap();
            assert_eq!(plus.to_bits(), 0.0f64.to_bits());
            assert_eq!(minus.to_bits(), (-0.0f64).to_bits());
        }
    }
}
