// SPDX-License-Identifier: LGPL-3.0-or-later
//! Parser for IERS finals2000A.data fixed-column ASCII format.
//!
//! The finals2000A.data file contains one row per MJD day with Earth
//! Orientation Parameters (EOP) in fixed-column format. This parser
//! extracts the key parameters needed for astronomical coordinate
//! conversions.
//!
//! # Format reference
//!
//! Columns are 1-indexed per the IERS specification:
//!
//! | Parameter | Columns | Unit | Notes |
//! |-----------|---------|------|-------|
//! | MJD       | 8–15   | days | UTC epoch |
//! | x (PM)    | 19–27  | arcsec | Polar motion X |
//! | y (PM)    | 38–46  | arcsec | Polar motion Y |
//! | dUT1      | 59–68  | seconds | UT1−UTC |
//! | LOD       | 80–86  | ms | Length of day excess |
//! | Flag      | 17, 58 | char | I=measured, P=predicted |
//! | dX (nut)  | 98–106 | mas | Nutation X (IAU2000A) |
//! | dY (nut)  | 117–125| mas | Nutation Y (IAU2000A) |

use super::{EopEntry, EopError};

/// Parse a single line from finals2000A.data into an [`EopEntry`].
///
/// Returns `None` for blank/short lines or lines where the MJD field is empty.
/// Returns `Err` for lines that have a valid MJD but malformed data columns.
pub(crate) fn parse_line(line: &str) -> Result<Option<EopEntry>, EopError> {
    // Lines shorter than 68 chars can't have dUT1 — skip them
    if line.len() < 68 {
        return Ok(None);
    }

    // MJD: columns 8–15 (0-indexed: 7..15)
    let mjd_str = line.get(7..15).unwrap_or("").trim();
    if mjd_str.is_empty() {
        return Ok(None);
    }
    let mjd: f64 = mjd_str
        .parse()
        .map_err(|_| EopError::ParseError(format!("bad MJD: {mjd_str:?}")))?;

    // PM flag: column 17 (0-indexed: 16)
    let pm_flag = line.as_bytes().get(16).copied().unwrap_or(b' ');

    // Polar motion X: columns 19–27 (0-indexed: 18..27)
    let x_arcsec = parse_field(line, 18, 27).unwrap_or(0.0);

    // Polar motion Y: columns 38–46 (0-indexed: 37..46)
    let y_arcsec = parse_field(line, 37, 46).unwrap_or(0.0);

    // dUT1 flag: column 58 (0-indexed: 57)
    let dut1_flag = line.as_bytes().get(57).copied().unwrap_or(b' ');

    // dUT1: columns 59–68 (0-indexed: 58..68)
    let dut1_seconds = match parse_field(line, 58, 68) {
        Some(v) => v,
        None => return Ok(None), // No dUT1 means this row isn't useful
    };

    // LOD: columns 80–86 (0-indexed: 79..86) — in milliseconds
    let lod_ms = parse_field(line, 79, 86).unwrap_or(0.0);
    let lod_seconds = lod_ms / 1000.0;

    // dX: columns 98–106 (0-indexed: 97..106) — in mas
    let dx_mas = if line.len() >= 106 {
        parse_field(line, 97, 106).unwrap_or(0.0)
    } else {
        0.0
    };

    // dY: columns 117–125 (0-indexed: 116..125) — in mas
    let dy_mas = if line.len() >= 125 {
        parse_field(line, 116, 125).unwrap_or(0.0)
    } else {
        0.0
    };

    // Use dUT1 flag primarily; fall back to PM flag
    let is_predicted = dut1_flag == b'P' || (dut1_flag == b' ' && pm_flag == b'P');

    Ok(Some(EopEntry {
        mjd,
        x_arcsec,
        y_arcsec,
        dut1_seconds,
        lod_seconds,
        dx_mas,
        dy_mas,
        is_predicted,
    }))
}

/// Parse a floating-point field from a fixed-column substring.
fn parse_field(line: &str, start: usize, end: usize) -> Option<f64> {
    let s = line.get(start..end)?.trim();
    if s.is_empty() {
        return None;
    }
    s.parse().ok()
}

/// Parse all lines from finals2000A.data content into a vector of entries.
///
/// Skips blank/short lines and lines without valid data.
pub(crate) fn parse_finals2000a(content: &str) -> Result<Vec<EopEntry>, EopError> {
    let mut entries = Vec::with_capacity(15000);

    for (line_num, line) in content.lines().enumerate() {
        match parse_line(line) {
            Ok(Some(entry)) => entries.push(entry),
            Ok(None) => {} // skip blank/incomplete lines
            Err(e) => {
                return Err(EopError::ParseError(format!("line {}: {e}", line_num + 1)));
            }
        }
    }

    if entries.is_empty() {
        return Err(EopError::ParseError(
            "no valid EOP entries found".to_string(),
        ));
    }

    // Sort by MJD (should already be sorted, but be safe)
    entries.sort_by(|a, b| a.mjd.partial_cmp(&b.mjd).unwrap());

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Real line from finals2000A.data
    const SAMPLE_LINE: &str = "92 1 1 48622.00 I  0.182987 0.000672  0.168775 0.000345  I-0.1251659 0.0000207  1.8335 0.0201  I    -0.086    0.202     0.130    0.165   .182400   .167900  -.1253000     0.129    -0.653  ";

    #[test]
    fn parse_sample_line() {
        let entry = parse_line(SAMPLE_LINE).unwrap().unwrap();
        assert!((entry.mjd - 48622.0).abs() < 1e-10);
        assert!((entry.x_arcsec - 0.182987).abs() < 1e-7);
        assert!((entry.y_arcsec - 0.168775).abs() < 1e-7);
        assert!((entry.dut1_seconds - (-0.1251659)).abs() < 1e-8);
        assert!((entry.lod_seconds - 0.0018335).abs() < 1e-7);
        assert!((entry.dx_mas - (-0.086)).abs() < 1e-4);
        assert!((entry.dy_mas - 0.130).abs() < 1e-4);
        assert!(!entry.is_predicted);
    }

    #[test]
    fn parse_short_line_returns_none() {
        assert!(parse_line("short").unwrap().is_none());
        assert!(parse_line("").unwrap().is_none());
    }

    #[test]
    fn parse_predicted_line() {
        // Create a line with P flag at column 58 (0-indexed 57)
        let mut line = SAMPLE_LINE.to_string();
        // Replace char at position 57 (dUT1 flag)
        let bytes = unsafe { line.as_bytes_mut() };
        bytes[57] = b'P';
        let entry = parse_line(&line).unwrap().unwrap();
        assert!(entry.is_predicted);
    }
}
