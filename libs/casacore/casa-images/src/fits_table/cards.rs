// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ffi::CStr;

use casa_core::ScalarValue;
use fitsio::sys;

use super::{Reader, Result, fits_status};

pub(super) struct Card {
    pub name: String,
    pub scalar: Option<ScalarValue>,
    pub comment: String,
}

impl Card {
    pub fn is_column(&self) -> bool {
        !matches!(
            self.name.as_str(),
            "COMMENT"
                | "DATAMAX"
                | "DATAMIN"
                | "EXTLEVEL"
                | "EXTNAME"
                | "EXTVER"
                | "HISTORY"
                | "REFERENC"
        )
    }
}

pub(super) struct Header(Vec<Card>);

impl Header {
    pub fn read(fits: &mut Reader) -> Result<Self> {
        Self::visit(fits, false, |_, _| {})
    }

    pub fn visit_listing(
        fits: &mut Reader,
        visit: impl FnMut(&str, Option<&Card>),
    ) -> Result<Self> {
        Self::visit(fits, true, visit)
    }

    fn visit(
        fits: &mut Reader,
        listing: bool,
        mut visit: impl FnMut(&str, Option<&Card>),
    ) -> Result<Self> {
        let (mut count, mut more, mut status) = (0, 0, 0);
        // SAFETY: live selected HDU, with valid output pointers.
        unsafe { sys::ffghsp(fits.ptr, &mut count, &mut more, &mut status) };
        fits_status(status)?;
        let mut cards = Vec::with_capacity(count as usize);
        for index in 1..=count {
            let mut buf = [0; 81];
            // SAFETY: CFITSIO writes at most 80 bytes and a terminator.
            unsafe { sys::ffgrec(fits.ptr, index, buf.as_mut_ptr(), &mut status) };
            fits_status(status)?;
            // SAFETY: ffgrec NUL-terminates a successfully read card.
            let raw = unsafe { CStr::from_ptr(buf.as_ptr()) }.to_string_lossy();
            let card = parse_card(&raw, listing);
            visit(&raw, card.as_ref());
            if let Some(card) = card {
                cards.push(card);
            }
        }
        Ok(Self(cards))
    }

    pub fn keywords(&self) -> impl Iterator<Item = &Card> {
        self.0.iter().filter(|card| !structural_keyword(&card.name))
    }

    pub fn text(&self, name: &str) -> Option<&str> {
        match self.value(name) {
            Some(ScalarValue::String(value)) => Some(value),
            _ => None,
        }
    }

    pub fn number(&self, name: &str) -> Option<f64> {
        match self.value(name) {
            Some(ScalarValue::Int32(value)) => Some(f64::from(*value)),
            Some(ScalarValue::Float32(value)) => Some(f64::from(*value)),
            Some(ScalarValue::Float64(value)) => Some(*value),
            _ => None,
        }
    }

    pub fn value(&self, name: &str) -> Option<&ScalarValue> {
        self.0
            .iter()
            .find(|card| card.name == name)?
            .scalar
            .as_ref()
    }
}

fn structural_keyword(name: &str) -> bool {
    let base = name.trim_end_matches(|ch: char| ch.is_ascii_digit());
    matches!(
        base,
        "BITPIX"
            | "GCOUNT"
            | "NAXIS"
            | "PCOUNT"
            | "TBCOL"
            | "TDIM"
            | "TDISP"
            | "TFIELDS"
            | "THEAP"
            | "TFORM"
            | "TNULL"
            | "TSCAL"
            | "TTYPE"
            | "TUNIT"
            | "TZERO"
            | "XTENSION"
    )
}

fn parse_card(card: &str, listing: bool) -> Option<Card> {
    let name = card.get(..8)?.trim().to_owned();
    let body = card.get(8..)?.strip_prefix("= ")?.trim_start();
    let (scalar, comment) = if let Some(string) = body.strip_prefix('\'') {
        let mut out = String::new();
        let mut chars = string.char_indices().peekable();
        let mut tail = "";
        while let Some((index, ch)) = chars.next() {
            if ch == '\'' {
                if chars.next_if(|(_, ch)| *ch == '\'').is_some() {
                    out.push(ch);
                } else {
                    tail = &string[index + 1..];
                    break;
                }
            } else {
                out.push(ch);
            }
        }
        (
            Some(ScalarValue::String(out.trim_end_matches(' ').to_owned())),
            tail.split_once('/').map_or("", |(_, comment)| comment),
        )
    } else {
        let (value, comment) = body.split_once('/').unwrap_or((body, ""));
        (numeric_or_bool(value.trim(), listing), comment)
    };
    Some(Card {
        name,
        scalar,
        comment: comment.trim_end().to_owned(),
    })
}

fn numeric_or_bool(value: &str, listing: bool) -> Option<ScalarValue> {
    match value {
        "T" => return Some(ScalarValue::Bool(true)),
        "F" => return Some(ScalarValue::Bool(false)),
        _ => {}
    }
    if let Ok(number) = value.parse::<i32>() {
        return Some(ScalarValue::Int32(number));
    }
    let mantissa = value
        .trim_start_matches(['-', '+'])
        .split(['e', 'E'])
        .next()?;
    // FITSReader keeps up to five digits in a Float. If the mantissa
    // starts with a decimal point, zeros before the first digit do not count.
    let (mantissa, float_digits) = if listing {
        (
            mantissa
                .strip_prefix('.')
                .map_or(mantissa, |fraction| fraction.trim_start_matches('0')),
            5,
        )
    } else {
        (mantissa, 7)
    };
    let double = value.contains(['D', 'd'])
        || mantissa.bytes().filter(u8::is_ascii_digit).count() > float_digits;
    let value: f64 = value.replace(['D', 'd'], "E").parse().ok()?;
    Some(if double {
        ScalarValue::Float64(value)
    } else {
        ScalarValue::Float32(value as f32)
    })
}
