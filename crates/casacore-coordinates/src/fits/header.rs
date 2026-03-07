// SPDX-License-Identifier: LGPL-3.0-or-later
//! FITS keyword-value collection.
//!
//! [`FitsHeader`] provides an in-memory representation of FITS header keywords
//! suitable for WCS parsing and emission. It supports the four FITS value
//! types (string, integer, float, logical) and can parse/emit the standard
//! 80-character FITS card format.
//!
//! This is *not* a full FITS file reader --- it handles only the keyword-value
//! pairs needed for WCS interoperability.

use std::fmt;

/// A typed FITS keyword value.
///
/// FITS supports four value types: character strings, integers, floating-point
/// numbers, and logical (boolean) values. This enum models all four.
#[derive(Debug, Clone, PartialEq)]
pub enum FitsValue {
    /// A character string value (enclosed in single quotes in FITS cards).
    String(String),
    /// A 64-bit integer value.
    Integer(i64),
    /// A 64-bit floating-point value.
    Float(f64),
    /// A logical (boolean) value (`T` or `F` in FITS).
    Logical(bool),
}

impl fmt::Display for FitsValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(s) => write!(f, "'{s}'"),
            Self::Integer(i) => write!(f, "{i}"),
            Self::Float(v) => {
                // Use enough precision to round-trip f64
                write!(f, "{v:.15E}")
            }
            Self::Logical(b) => {
                if *b {
                    f.write_str("T")
                } else {
                    f.write_str("F")
                }
            }
        }
    }
}

/// A single FITS header keyword with its value and optional comment.
///
/// Corresponds to one 80-character FITS card image.
#[derive(Debug, Clone)]
pub struct FitsKeyword {
    /// The keyword name (up to 8 characters in standard FITS).
    pub name: String,
    /// The keyword value.
    pub value: FitsValue,
    /// An optional comment string (follows the `/` in FITS cards).
    pub comment: Option<String>,
}

/// An ordered collection of FITS keywords.
///
/// Keywords are stored in insertion order and looked up by name. The `set`
/// method performs an upsert: if a keyword with the same name already exists,
/// its value and comment are replaced; otherwise a new keyword is appended.
#[derive(Debug, Clone, Default)]
pub struct FitsHeader {
    keywords: Vec<FitsKeyword>,
}

impl FitsHeader {
    /// Creates an empty FITS header.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets (upserts) a keyword. If the name already exists, replaces its
    /// value; otherwise appends a new keyword.
    pub fn set(&mut self, name: impl Into<String>, value: FitsValue) {
        let name = name.into();
        if let Some(kw) = self.keywords.iter_mut().find(|k| k.name == name) {
            kw.value = value;
        } else {
            self.keywords.push(FitsKeyword {
                name,
                value,
                comment: None,
            });
        }
    }

    /// Sets a keyword with a comment.
    pub fn set_with_comment(
        &mut self,
        name: impl Into<String>,
        value: FitsValue,
        comment: impl Into<String>,
    ) {
        let name = name.into();
        let comment = Some(comment.into());
        if let Some(kw) = self.keywords.iter_mut().find(|k| k.name == name) {
            kw.value = value;
            kw.comment = comment;
        } else {
            self.keywords.push(FitsKeyword {
                name,
                value,
                comment,
            });
        }
    }

    /// Returns the value of a keyword by name, or `None` if not present.
    pub fn get(&self, name: &str) -> Option<&FitsValue> {
        self.keywords
            .iter()
            .find(|k| k.name == name)
            .map(|k| &k.value)
    }

    /// Returns a float value for the given keyword, auto-converting integers.
    ///
    /// Returns `None` if the keyword is missing or has a non-numeric type.
    pub fn get_float(&self, name: &str) -> Option<f64> {
        match self.get(name)? {
            FitsValue::Float(v) => Some(*v),
            FitsValue::Integer(v) => Some(*v as f64),
            _ => None,
        }
    }

    /// Returns an integer value for the given keyword.
    ///
    /// Returns `None` if the keyword is missing or is not an integer.
    pub fn get_int(&self, name: &str) -> Option<i64> {
        match self.get(name)? {
            FitsValue::Integer(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns a string value for the given keyword.
    ///
    /// Returns `None` if the keyword is missing or is not a string.
    pub fn get_string(&self, name: &str) -> Option<&str> {
        match self.get(name)? {
            FitsValue::String(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Parses FITS card images (80-character lines) into a header.
    ///
    /// Each card should follow the format `KEYWORD = value / comment`.
    /// Cards without `=` (e.g. `COMMENT`, `HISTORY`, `END`) are ignored.
    /// Leading/trailing whitespace on each card is trimmed.
    pub fn from_cards(cards: &[&str]) -> Self {
        let mut header = Self::new();
        for card in cards {
            let card = card.trim();
            if card.is_empty() || card.starts_with("END") {
                continue;
            }
            // Must have '=' in columns 9 (0-indexed 8) or anywhere for our
            // tolerant parser.
            let Some(eq_pos) = card.find('=') else {
                continue;
            };
            let name = card[..eq_pos].trim().to_string();
            if name.is_empty() {
                continue;
            }

            let rest = card[eq_pos + 1..].trim();
            let (value, comment) = parse_value_comment(rest);
            header.keywords.push(FitsKeyword {
                name,
                value,
                comment,
            });
        }
        header
    }

    /// Emits FITS card images for all keywords.
    ///
    /// Each card is an 80-character string (padded with spaces). The format
    /// follows FITS standard: keyword in columns 1-8, `= ` in columns 9-10,
    /// value right-justified in columns 11-30, and optional comment after ` / `.
    pub fn to_cards(&self) -> Vec<String> {
        self.keywords.iter().map(format_card).collect()
    }

    /// Returns the number of keywords.
    pub fn len(&self) -> usize {
        self.keywords.len()
    }

    /// Returns `true` if the header contains no keywords.
    pub fn is_empty(&self) -> bool {
        self.keywords.is_empty()
    }

    /// Returns an iterator over all keywords.
    pub fn iter(&self) -> impl Iterator<Item = &FitsKeyword> {
        self.keywords.iter()
    }
}

/// Parses the value and optional comment from the portion of a FITS card
/// after the `=` sign.
fn parse_value_comment(rest: &str) -> (FitsValue, Option<String>) {
    // String values are enclosed in single quotes
    if let Some(q1) = rest.find('\'') {
        // Find closing quote (doubled quotes '' are literal)
        let after_q1 = &rest[q1 + 1..];
        let mut end = 0;
        let mut chars = after_q1.chars();
        loop {
            match chars.next() {
                None => break,
                Some('\'') => {
                    // Check for doubled quote
                    if chars.clone().next() == Some('\'') {
                        chars.next(); // skip second quote
                        end += 2;
                    } else {
                        break;
                    }
                }
                Some(_) => {
                    end += 1;
                }
            }
        }
        let raw = &after_q1[..end];
        let s = raw.replace("''", "'").trim_end().to_string();
        let comment_start = q1 + 1 + end + 1; // after closing quote
        let comment = if comment_start < rest.len() {
            let c = rest[comment_start..].trim();
            if let Some(stripped) = c.strip_prefix('/') {
                let c = stripped.trim();
                if c.is_empty() {
                    None
                } else {
                    Some(c.to_string())
                }
            } else {
                None
            }
        } else {
            None
        };
        return (FitsValue::String(s), comment);
    }

    // Split on '/' for comment (only outside quotes)
    let (val_str, comment) = if let Some(slash) = rest.find('/') {
        let c = rest[slash + 1..].trim();
        let comment = if c.is_empty() {
            None
        } else {
            Some(c.to_string())
        };
        (rest[..slash].trim(), comment)
    } else {
        (rest.trim(), None)
    };

    // Logical value
    if val_str == "T" {
        return (FitsValue::Logical(true), comment);
    }
    if val_str == "F" {
        return (FitsValue::Logical(false), comment);
    }

    // Integer (no decimal point, no exponent)
    if !val_str.contains('.') && !val_str.contains('E') && !val_str.contains('e') {
        if let Ok(i) = val_str.parse::<i64>() {
            return (FitsValue::Integer(i), comment);
        }
    }

    // Float
    // FITS uses 'D' as exponent delimiter sometimes
    let float_str = val_str.replace('D', "E").replace('d', "e");
    if let Ok(f) = float_str.parse::<f64>() {
        return (FitsValue::Float(f), comment);
    }

    // Fallback: treat as string
    (FitsValue::String(val_str.to_string()), comment)
}

/// Formats a single FITS keyword as an 80-character card.
fn format_card(kw: &FitsKeyword) -> String {
    let val_str = match &kw.value {
        FitsValue::String(s) => {
            // String values: left-justified in single quotes, min 8 chars between quotes
            let padded = format!("{:<8}", s);
            format!("'{padded}'")
        }
        FitsValue::Integer(i) => format!("{i:>20}"),
        FitsValue::Float(f) => format!("{f:>20.15E}"),
        FitsValue::Logical(b) => {
            if *b {
                format!("{:>20}", "T")
            } else {
                format!("{:>20}", "F")
            }
        }
    };

    let base = format!("{:<8}= {val_str}", kw.name);
    let card = if let Some(ref c) = kw.comment {
        let with_comment = format!("{base} / {c}");
        if with_comment.len() > 80 {
            with_comment[..80].to_string()
        } else {
            with_comment
        }
    } else {
        base
    };

    // Pad to 80 characters
    format!("{card:<80}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_get() {
        let mut h = FitsHeader::new();
        h.set("NAXIS", FitsValue::Integer(4));
        h.set("CRVAL1", FitsValue::Float(1.5e9));
        h.set("TELESCOP", FitsValue::String("ALMA".into()));

        assert_eq!(h.get_int("NAXIS"), Some(4));
        assert!((h.get_float("CRVAL1").unwrap() - 1.5e9).abs() < 1.0);
        assert_eq!(h.get_string("TELESCOP"), Some("ALMA"));
        assert!(h.get("MISSING").is_none());
    }

    #[test]
    fn upsert() {
        let mut h = FitsHeader::new();
        h.set("NAXIS", FitsValue::Integer(3));
        h.set("NAXIS", FitsValue::Integer(4));
        assert_eq!(h.get_int("NAXIS"), Some(4));
        assert_eq!(h.len(), 1);
    }

    #[test]
    fn get_float_converts_integer() {
        let mut h = FitsHeader::new();
        h.set("EQUINOX", FitsValue::Integer(2000));
        assert!((h.get_float("EQUINOX").unwrap() - 2000.0).abs() < 1e-10);
    }

    #[test]
    fn from_cards_basic() {
        let cards = [
            "NAXIS   =                    4 / number of axes",
            "CRVAL1  =    1.500000000000000E+09 / reference value",
            "CTYPE1  = 'RA---SIN'           / axis type",
            "SIMPLE  =                    T / standard FITS",
            "END",
        ];
        let h = FitsHeader::from_cards(&cards);
        assert_eq!(h.get_int("NAXIS"), Some(4));
        assert!((h.get_float("CRVAL1").unwrap() - 1.5e9).abs() < 1.0);
        assert_eq!(h.get_string("CTYPE1"), Some("RA---SIN"));
        assert_eq!(h.get("SIMPLE"), Some(&FitsValue::Logical(true)));
    }

    #[test]
    fn to_cards_roundtrip() {
        let mut h = FitsHeader::new();
        h.set("NAXIS", FitsValue::Integer(2));
        h.set("CRVAL1", FitsValue::Float(3.15));
        h.set("CTYPE1", FitsValue::String("RA---TAN".into()));

        let cards: Vec<String> = h.to_cards();
        assert_eq!(cards.len(), 3);
        for card in &cards {
            assert_eq!(card.len(), 80);
        }

        // Parse them back
        let card_refs: Vec<&str> = cards.iter().map(|s| s.as_str()).collect();
        let h2 = FitsHeader::from_cards(&card_refs);
        assert_eq!(h2.get_int("NAXIS"), Some(2));
        assert!((h2.get_float("CRVAL1").unwrap() - 3.15).abs() < 1e-10);
        assert_eq!(h2.get_string("CTYPE1"), Some("RA---TAN"));
    }

    #[test]
    fn fits_value_display() {
        assert_eq!(FitsValue::Integer(42).to_string(), "42");
        assert_eq!(FitsValue::Logical(true).to_string(), "T");
        let s = FitsValue::Float(1.5e9).to_string();
        assert!(s.contains('E'));
    }

    #[test]
    fn empty_header() {
        let h = FitsHeader::new();
        assert!(h.is_empty());
        assert_eq!(h.len(), 0);
    }

    #[test]
    fn card_with_comment() {
        let mut h = FitsHeader::new();
        h.set_with_comment("OBJECT", FitsValue::String("M51".into()), "target name");
        let cards = h.to_cards();
        assert!(cards[0].contains("M51"));
        assert!(cards[0].contains("target name"));
    }
}
