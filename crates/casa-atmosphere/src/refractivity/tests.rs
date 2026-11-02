// SPDX-License-Identifier: LGPL-3.0-or-later
//! Unit laws for the refractivity internals: ATM's frequency bins, its
//! unsigned window arithmetic and the consistency of its catalogues.

use super::tables::{
    co, hcl, hcn, hdo, hh16o, hh16o_v2, hh17o, hh18o, n2o, no2, o2, o2_16o18o, o2_vib, o3,
    o3_16o16o17o, o3_16o16o18o, o3_16o17o16o, o3_16o18o16o, o3_v1, o3_v2, o3_v3, so2,
};
use super::{frequency_bin, window, window_range};

#[test]
fn frequency_bins_change_at_even_gigahertz() {
    // vpIndex: 0 below 1 GHz, then round((ν + 1)/2) − 1 with halves away
    // from zero, so bin k covers [2k, 2k + 2).
    assert_eq!(frequency_bin(0.0), 0);
    assert_eq!(frequency_bin(0.999), 0);
    assert_eq!(frequency_bin(1.0), 0);
    assert_eq!(frequency_bin(1.999_999), 0);
    assert_eq!(frequency_bin(2.0), 1);
    assert_eq!(frequency_bin(3.999_999), 1);
    assert_eq!(frequency_bin(4.0), 2);
    assert_eq!(frequency_bin(345.796), 172);
    assert_eq!(frequency_bin(1599.999), 799);
    assert_eq!(frequency_bin(-5.0), 0);
}

#[test]
fn window_range_follows_unsigned_semantics() {
    // A normal window.
    assert_eq!(window_range(3, 7), Some(3..=7));
    // ATM skips a window ending at line 0, even when it starts there.
    assert_eq!(window_range(0, 0), None);
    // An empty start entry (0 − 1 wraps): the start exceeds every end.
    assert_eq!(window_range(-1, 5), None);
    // An empty end entry (0 − 1 wraps): `ifin + 1` wraps to 0, no lines.
    assert_eq!(window_range(2, -1), None);
    assert_eq!(window_range(-1, -1), None);
    // An end before the start.
    assert_eq!(window_range(5, 4), None);
}

#[test]
fn windows_look_up_neighbouring_bins() {
    let starts = [1, 2, 3, 4, 5, 6, 7, 8];
    let ends = [2, 3, 4, 5, 6, 7, 8, 8];
    // Bin 4 with half-width 2: start at bin 2 (3 − 1), end at bin 6 (8 − 1).
    assert_eq!(window(4, 2, (&starts, &ends)), Some(2..=7));
    // At or below the half-width the window starts at line 0 (end at bin 4).
    assert_eq!(window(2, 2, (&starts, &ends)), Some(0..=5));
    // The end bin is clamped to the table: start at bin 5, end at bin 7.
    assert_eq!(window(7, 2, (&starts, &ends)), Some(5..=7));
}

/// A catalogue: name, per-line array lengths and its window tables, with
/// the largest line index a table entry may hold (the line count for ATM's
/// usual 1-based entries, one less for the 0-based ground-state ozone).
struct Catalogue {
    name: &'static str,
    columns: Vec<usize>,
    tables: Vec<&'static [u16]>,
    largest_entry: usize,
}

/// A catalogue from a table module, its per-line columns and its window
/// tables.
macro_rules! catalogue {
    ($module:ident, [$($column:ident),*], [$($table:ident),*]) => {
        catalogue!($module, [$($column),*], [$($table),*], $module::FRE.len())
    };
    ($module:ident, [$($column:ident),*], [$($table:ident),*], $largest:expr) => {
        Catalogue {
            name: stringify!($module),
            columns: vec![$($module::$column.len()),*],
            tables: vec![$(&$module::$table[..]),*],
            largest_entry: $largest,
        }
    };
}

fn catalogues() -> Vec<Catalogue> {
    vec![
        catalogue!(so2, [FRE, FLIN, EL, BRDSO2AIR], [INI1, IFIN1]),
        catalogue!(hcl, [FRE, FLIN, EL, BRDHCLAIR, TEXPHCL], []),
        catalogue!(hcn, [FRE, FLIN, EL, BRDHCNAIR, TEXPHCN], []),
        catalogue!(no2, [FRE, FLIN, EL, BRDNO2AIR, TEXPNO2], [INI1, IFIN1]),
        catalogue!(
            n2o,
            [FRE, FLIN, EL, BRDN2OAIR, TEXPN2O],
            [INI1, IFIN1, INI2, IFIN2, INI3, IFIN3]
        ),
        catalogue!(
            co,
            [FRE, FLIN, EL, BRDCOAIR, TEXPCO],
            [INI1, IFIN1, INI2, IFIN2, INI3, IFIN3]
        ),
        catalogue!(
            hh16o,
            [FRE, FLIN, EL, GL, BROADENING],
            [INI1, IFIN1, INI2, IFIN2, INI3, IFIN3]
        ),
        catalogue!(
            hh16o_v2,
            [FRE, FLIN, EL, GL, BROADENING],
            [INI1, IFIN1, INI2, IFIN2, INI3, IFIN3]
        ),
        catalogue!(
            hh17o,
            [FRE, FLIN, EL, GL, DV0, DVLM, TEMP_EXP],
            [INI1, IFIN1, INI2, IFIN2, INI3, IFIN3]
        ),
        catalogue!(
            hh18o,
            [FRE, FLIN, EL, GL, DV0, DVLM, TEMP_EXP],
            [INI1, IFIN1]
        ),
        catalogue!(
            hdo,
            [FRE, FLIN, EL],
            [
                INI11, IFIN11, INI21, IFIN21, INI31, IFIN31, INI12, IFIN12, INI22, IFIN22, INI32,
                IFIN32
            ]
        ),
        catalogue!(o2_vib, [FRE, FLIN, EL], []),
        catalogue!(o2_16o18o, [FRE, FLIN, EL], []),
        catalogue!(o2, [FRE, FLIN, EL, BROADENING], [INI1, IFIN1, INI2, IFIN2]),
        catalogue!(
            o2,
            [FREEXTRA, FLINEXTRA, EUEXTRA, TEXPO2EXTRA, BRDO2AIREXTRA],
            []
        ),
        catalogue!(o3_v1, [FRE, FLIN, EL, BRDO3AIR, TEXPO3], [INI1, IFIN1]),
        catalogue!(o3_v2, [FRE, FLIN, EL, BRDO3AIR, TEXPO3], [INI1, IFIN1]),
        catalogue!(o3_v3, [FRE, FLIN, EL, BRDO3AIR, TEXPO3], [INI1, IFIN1]),
        catalogue!(
            o3,
            [FRE, FLIN, EL, BRDO3AIR, TEXPO3],
            [INI1, IFIN1, INI2, IFIN2, INI3, IFIN3],
            o3::FRE.len() - 1
        ),
        catalogue!(
            o3_16o16o18o,
            [FRE, FLIN, EL, BRDO3AIR, TEXPO3],
            [INI1, IFIN1, INI2, IFIN2, INI3, IFIN3]
        ),
        catalogue!(
            o3_16o16o17o,
            [FRE, FLIN, EL, BRDO3AIR, TEXPO3],
            [INI1, IFIN1, INI2, IFIN2, INI3, IFIN3]
        ),
        catalogue!(
            o3_16o18o16o,
            [FRE, FLIN, EL, BRDO3AIR, TEXPO3],
            [INI1, IFIN1, INI2, IFIN2, INI3, IFIN3]
        ),
        catalogue!(
            o3_16o17o16o,
            [FRE, FLIN, EL, BRDO3AIR, TEXPO3],
            [INI1, IFIN1, INI2, IFIN2, INI3, IFIN3]
        ),
    ]
}

#[test]
fn catalogue_columns_agree_and_windows_stay_inside() {
    for catalogue in catalogues() {
        let lines = catalogue.columns[0];
        assert!(
            catalogue.columns.iter().all(|&n| n == lines),
            "{}: per-line columns differ in length: {:?}",
            catalogue.name,
            catalogue.columns
        );
        for table in &catalogue.tables {
            let largest = usize::from(*table.iter().max().unwrap());
            assert!(
                largest <= catalogue.largest_entry,
                "{}: a window table reaches line {largest} of {lines}",
                catalogue.name
            );
        }
    }
}

#[test]
fn line_frequencies_are_positive() {
    for (name, frequencies) in [
        ("so2", &so2::FRE[..]),
        ("hcn", &hcn::FRE[..]),
        ("no2", &no2::FRE[..]),
        ("hh16o", &hh16o::FRE[..]),
        ("o2", &o2::FRE[..]),
        ("o3", &o3::FRE[..]),
        ("o3_16o16o18o", &o3_16o16o18o::FRE[..]),
    ] {
        assert!(
            frequencies.iter().all(|&f| f > 0.0),
            "{name} has a non-positive line frequency"
        );
    }
}
