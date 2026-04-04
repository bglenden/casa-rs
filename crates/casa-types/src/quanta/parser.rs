// SPDX-License-Identifier: LGPL-3.0-or-later
//! Recursive descent parser for unit expressions.
//!
//! Parses strings like `"km/s"`, `"kg.m.s-2"`, `"Jy/beam"` into a
//! [`UnitVal`].  The grammar matches the C++ `UnitVal::create()` parser.
//!
//! # Grammar
//!
//! ```text
//! unit_expr = term { ('.' | '/') term }*
//! term      = '(' unit_expr ')' [exponent]
//!           | field [exponent]
//! field     = known_unit                   # try full name
//!           | prefix(1) known_unit         # 1-char prefix + rest
//!           | prefix(2) known_unit         # 2-char prefix + rest
//! exponent  = ['-'] digits
//! ```
//!
//! The separator between terms is `.` (multiply) or `/` (divide).
//! Terms without a separator are multiplied.

use crate::quanta::error::UnitError;
use crate::quanta::registry::global_registry;
use crate::quanta::unit_val::UnitVal;

/// Parses a unit string into a [`UnitVal`].
///
/// On success the result is cached in the global registry for future lookups.
/// Returns `Err(UnitError::InvalidUnit)` if the string cannot be parsed.
pub fn parse_unit(input: &str) -> Result<UnitVal, UnitError> {
    let input = input.trim();
    if input.is_empty() {
        return Ok(UnitVal::NODIM);
    }

    // Check the registry cache first.
    let reg = global_registry();
    if let Some(val) = reg.lookup_unit(input) {
        return Ok(val);
    }

    let bytes = input.as_bytes();
    let mut pos = 0;
    let result = parse_expr(bytes, &mut pos)?;

    if pos != bytes.len() {
        return Err(UnitError::InvalidUnit {
            input: input.to_owned(),
        });
    }

    // Cache the result.
    reg.cache_put(input, result);
    Ok(result)
}

/// Parse a full expression: sequence of terms separated by `.` or `/`.
/// A leading `/` is treated as `1/…` (e.g. `/mol` → mol⁻¹).
fn parse_expr(bytes: &[u8], pos: &mut usize) -> Result<UnitVal, UnitError> {
    let mut result = if *pos < bytes.len() && bytes[*pos] == b'/' {
        UnitVal::NODIM
    } else {
        parse_term(bytes, pos)?
    };

    while *pos < bytes.len() {
        match bytes[*pos] {
            b'.' => {
                *pos += 1;
                let term = parse_term(bytes, pos)?;
                result = result * term;
            }
            b'/' => {
                *pos += 1;
                let term = parse_term(bytes, pos)?;
                result = result / term;
            }
            b')' => break,
            _ => break,
        }
    }
    Ok(result)
}

/// Parse a single term: parenthesised expression or field, optionally with exponent.
fn parse_term(bytes: &[u8], pos: &mut usize) -> Result<UnitVal, UnitError> {
    if *pos >= bytes.len() {
        return Ok(UnitVal::NODIM);
    }

    let val = if bytes[*pos] == b'(' {
        *pos += 1;
        let inner = parse_expr(bytes, pos)?;
        if *pos >= bytes.len() || bytes[*pos] != b')' {
            return Err(UnitError::InvalidUnit {
                input: String::from_utf8_lossy(bytes).into_owned(),
            });
        }
        *pos += 1;
        inner
    } else {
        parse_field(bytes, pos)?
    };

    // Optional exponent.
    let exp = parse_exponent(bytes, pos);
    if exp != 1 { Ok(val.pow(exp)) } else { Ok(val) }
}

/// Parse a field: a known unit name, optionally preceded by a prefix.
fn parse_field(bytes: &[u8], pos: &mut usize) -> Result<UnitVal, UnitError> {
    let start = *pos;
    let reg = global_registry();

    // Collect the field token: alphanumeric + special chars.
    let end = scan_field(bytes, *pos);
    if end == start {
        return Err(UnitError::InvalidUnit {
            input: String::from_utf8_lossy(bytes).into_owned(),
        });
    }

    let field = &bytes[start..end];
    let field_str = std::str::from_utf8(field).unwrap_or("");

    // 1) Try the full field as a unit name.
    if let Some(val) = reg.lookup_unit(field_str) {
        *pos = end;
        return Ok(val);
    }

    // 2) Try 2-char prefix + rest (try longer prefix first to match "da" before "d").
    if field.len() > 2 {
        let prefix2 = std::str::from_utf8(&field[..2]).unwrap_or("");
        let rest2 = std::str::from_utf8(&field[2..]).unwrap_or("");
        if let Some(pfactor) = reg.lookup_prefix(prefix2) {
            if let Some(uval) = reg.lookup_unit(rest2) {
                *pos = end;
                return Ok(UnitVal::new(pfactor * uval.factor, uval.dim));
            }
        }
    }

    // 3) Try 1-char prefix + rest.
    if field.len() > 1 {
        let prefix1 = std::str::from_utf8(&field[..1]).unwrap_or("");
        let rest1 = std::str::from_utf8(&field[1..]).unwrap_or("");
        if let Some(pfactor) = reg.lookup_prefix(prefix1) {
            if let Some(uval) = reg.lookup_unit(rest1) {
                *pos = end;
                return Ok(UnitVal::new(pfactor * uval.factor, uval.dim));
            }
        }
    }

    // Not found.
    Err(UnitError::UnknownUnit {
        name: field_str.to_owned(),
    })
}

/// Scan forward to find the end of a field token.
///
/// A field consists of alphabetic characters plus the special characters
/// `'`, `"`, `_`, `$`, `%`.  Digits following alphabetic characters are
/// included only when the resulting token (alpha+digit) is a registered unit
/// name (e.g. `S0`, `M0`, `R0`).  Otherwise digits are parsed separately as
/// exponents, matching the C++ `UnitVal::create()` behaviour where `"m2"` is
/// field `"m"` with exponent `2`.
fn scan_field(bytes: &[u8], start: usize) -> usize {
    let mut i = start;

    // Special: handle `:` sequences (`:`, `::`, `:::`)
    if i < bytes.len() && bytes[i] == b':' {
        while i < bytes.len() && bytes[i] == b':' {
            i += 1;
        }
        return i;
    }

    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_alphabetic() || b == b'_' || b == b'\'' || b == b'"' || b == b'$' || b == b'%'
        {
            i += 1;
        } else {
            break;
        }
    }

    // Try extending with trailing digits if the result is a known unit.
    // This handles units like S0, M0, R0 whose names end in digits.
    if i > start && i < bytes.len() && bytes[i].is_ascii_digit() {
        let mut j = i;
        while j < bytes.len() && bytes[j].is_ascii_digit() {
            j += 1;
        }
        if let Ok(candidate) = std::str::from_utf8(&bytes[start..j]) {
            let reg = crate::quanta::registry::global_registry();
            if reg.lookup_unit(candidate).is_some() {
                return j;
            }
        }
    }

    i
}

/// Parse an optional integer exponent (possibly negative).
fn parse_exponent(bytes: &[u8], pos: &mut usize) -> i32 {
    if *pos >= bytes.len() {
        return 1;
    }

    let mut sign = 1i32;
    let mut has_digits = false;
    let mut value = 0i32;

    // Check for negative sign.
    if bytes[*pos] == b'-' {
        sign = -1;
        *pos += 1;
        // A bare `-` without digits is not an exponent — undo.
        if *pos >= bytes.len() || !bytes[*pos].is_ascii_digit() {
            *pos -= 1;
            return 1;
        }
    } else if bytes[*pos] == b'+' {
        *pos += 1;
        if *pos >= bytes.len() || !bytes[*pos].is_ascii_digit() {
            *pos -= 1;
            return 1;
        }
    }

    while *pos < bytes.len() && bytes[*pos].is_ascii_digit() {
        value = value * 10 + (bytes[*pos] - b'0') as i32;
        has_digits = true;
        *pos += 1;
    }

    if has_digits { sign * value } else { 1 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quanta::dim::Dimension;

    fn assert_close(a: f64, b: f64, tol: f64) {
        let diff = (a - b).abs();
        let scale = a.abs().max(b.abs()).max(1e-30);
        assert!(
            diff / scale < tol,
            "expected {a} ≈ {b} (rel diff {:.2e})",
            diff / scale,
        );
    }

    #[test]
    fn parse_empty() {
        let val = parse_unit("").unwrap();
        assert_eq!(val.factor, 1.0);
        assert!(val.dim.is_dimensionless());
    }

    #[test]
    fn parse_base_unit() {
        let val = parse_unit("m").unwrap();
        assert_eq!(val.factor, 1.0);
        assert_eq!(val.dim.get(Dimension::Length), 1);
    }

    #[test]
    fn parse_prefixed_unit() {
        let val = parse_unit("km").unwrap();
        assert_close(val.factor, 1e3, 1e-12);
        assert_eq!(val.dim.get(Dimension::Length), 1);
    }

    #[test]
    fn parse_compound() {
        let val = parse_unit("m/s").unwrap();
        assert_eq!(val.factor, 1.0);
        assert_eq!(val.dim.get(Dimension::Length), 1);
        assert_eq!(val.dim.get(Dimension::Time), -1);
    }

    #[test]
    fn parse_with_exponent() {
        let val = parse_unit("m2").unwrap();
        assert_eq!(val.factor, 1.0);
        assert_eq!(val.dim.get(Dimension::Length), 2);
    }

    #[test]
    fn parse_negative_exponent() {
        let val = parse_unit("s-2").unwrap();
        assert_eq!(val.factor, 1.0);
        assert_eq!(val.dim.get(Dimension::Time), -2);
    }

    #[test]
    fn parse_complex_expression() {
        let val = parse_unit("kg.m.s-2").unwrap();
        assert_eq!(val.factor, 1.0);
        assert_eq!(val.dim.get(Dimension::Mass), 1);
        assert_eq!(val.dim.get(Dimension::Length), 1);
        assert_eq!(val.dim.get(Dimension::Time), -2);
    }

    #[test]
    fn parse_parenthesised() {
        let val = parse_unit("(m/s)2").unwrap();
        assert_eq!(val.dim.get(Dimension::Length), 2);
        assert_eq!(val.dim.get(Dimension::Time), -2);
    }

    #[test]
    fn parse_jansky() {
        let val = parse_unit("Jy").unwrap();
        assert_close(val.factor, 1e-26, 1e-12);
    }

    #[test]
    fn parse_km_s() {
        let val = parse_unit("km/s").unwrap();
        assert_close(val.factor, 1e3, 1e-12);
        assert_eq!(val.dim.get(Dimension::Length), 1);
        assert_eq!(val.dim.get(Dimension::Time), -1);
    }

    #[test]
    fn parse_mjy() {
        let val = parse_unit("mJy").unwrap();
        assert_close(val.factor, 1e-29, 1e-12);
    }

    #[test]
    fn parse_degree() {
        let val = parse_unit("deg").unwrap();
        assert_eq!(val.dim.get(Dimension::Angle), 1);
        assert_close(val.factor, std::f64::consts::PI / 180.0, 1e-15);
    }

    #[test]
    fn parse_unknown_unit() {
        let err = parse_unit("xyzzy").unwrap_err();
        assert!(matches!(err, UnitError::UnknownUnit { .. }));
    }

    #[test]
    fn parse_w_m2_hz() {
        let val = parse_unit("W/m2/Hz").unwrap();
        // W = kg.m2.s-3, /m2 = kg.s-3, /Hz = /s-1 = kg.s-2
        assert_eq!(val.dim.get(Dimension::Mass), 1);
        assert_eq!(val.dim.get(Dimension::Time), -2);
        assert_eq!(val.dim.get(Dimension::Length), 0);
    }

    #[test]
    fn parse_multiple_divisions() {
        let val = parse_unit("J/K/mol").unwrap();
        // J = kg.m2.s-2, /K, /mol
        assert_eq!(val.dim.get(Dimension::Mass), 1);
        assert_eq!(val.dim.get(Dimension::Length), 2);
        assert_eq!(val.dim.get(Dimension::Time), -2);
        assert_eq!(val.dim.get(Dimension::Temperature), -1);
        assert_eq!(val.dim.get(Dimension::Amount), -1);
    }
}
