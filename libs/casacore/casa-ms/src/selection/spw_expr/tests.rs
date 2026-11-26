// SPDX-License-Identifier: LGPL-3.0-or-later
//! Unit tests of the spectral-window selection port. The CASA oracle
//! (`tests/spw_selection_oracle.rs`) covers the forms against CASA itself;
//! these pin the scanner, the parser and the cases the oracle cannot run.

use super::grammar::{Element, Unit, parse, scan_float};
use super::lexer::{Kind, Lexer};
use super::*;

struct Window {
    name: &'static str,
    freqs: Vec<f64>,
    widths: Vec<f64>,
    reference: f64,
    flag: bool,
}

fn window(name: &'static str, start: f64, width: f64, channels: usize) -> Window {
    let freqs = (0..channels)
        .map(|k| start + width * k as f64)
        .collect::<Vec<_>>();
    Window {
        name,
        reference: freqs[0],
        freqs,
        widths: vec![width; channels],
        flag: false,
    }
}

fn rows(windows: &[Window]) -> Vec<SpectralWindowRow<'_>> {
    windows
        .iter()
        .map(|w| SpectralWindowRow {
            name: w.name,
            chan_freq_hz: &w.freqs,
            chan_width_hz: &w.widths,
            ref_frequency_hz: w.reference,
            total_bandwidth_hz: w.widths.iter().map(|w| w.abs()).sum(),
            flag_row: w.flag,
        })
        .collect()
}

fn one_each(count: usize) -> Vec<DataDescriptionRow> {
    (0..count)
        .map(|spw| DataDescriptionRow {
            spectral_window_id: spw as i32,
            flag_row: false,
        })
        .collect()
}

/// Three 64-channel windows: 4.488 GHz up, 4.616 GHz up, 7.5 GHz down.
fn vla() -> Vec<Window> {
    vec![
        window("EVLA_C#A0C0#0", 4.488e9, 2.0e6, 64),
        window("EVLA_C#A0C0#1", 4.616e9, 2.0e6, 64),
        window("EVLA_C#B0D0#2", 7.5e9, -2.0e6, 64),
    ]
}

fn resolve(windows: &[Window], text: &str) -> MsResult<SpwSelection> {
    resolve_spw_selector(&rows(windows), &one_each(windows.len()), text)
}

fn chans(selection: &SpwSelection) -> Vec<[i64; 4]> {
    selection
        .channel_list()
        .iter()
        .map(|r| {
            [
                r.spw_id as i64,
                i64::from(r.start),
                i64::from(r.stop),
                i64::from(r.step),
            ]
        })
        .collect()
}

fn kinds(text: &str) -> Vec<Kind> {
    let mut lexer = Lexer::new(text);
    let mut kinds = Vec::new();
    loop {
        let token = lexer.next_token();
        if token.kind == Kind::End {
            return kinds;
        }
        kinds.push(token.kind);
    }
}

#[test]
fn the_scanner_takes_flexs_longest_first_match() {
    use Kind::*;
    assert_eq!(kinds("spw1"), [Identifier]);
    assert_eq!(kinds("spw*"), [QString]);
    assert_eq!(kinds("e5"), [Number]);
    assert_eq!(kinds("e5x"), [Identifier]);
    assert_eq!(kinds("+2"), [Number]);
    assert_eq!(kinds("+a"), [Identifier]);
    assert_eq!(kinds("MHz"), [Unit]);
    assert_eq!(kinds("MHzx"), [Identifier]);
    assert_eq!(kinds("km/s"), [Unit]);
    assert_eq!(kinds("Mm/sec"), [Unit]);
    assert_eq!(kinds("1.4GHz"), [Number, Unit]);
    assert_eq!(kinds("LBAND-A"), [Identifier, Number, Identifier]);
    assert_eq!(kinds("1<>3"), [Number, GtNLt, Number]);
    assert_eq!(kinds("0 : 1 ~ 3"), [Number, Colon, Number, Dash, Number]);
    assert_eq!(kinds("0\r"), [Number, Invalid]);
    assert_eq!(kinds("0é1"), [Number], "a high byte ends the input");
    assert_eq!(kinds("0\"abc"), [Number], "an open quote swallows the rest");
    assert_eq!(kinds("0/abc"), [Number]);
    assert_eq!(kinds("0\0,1"), [Number], "a C string ends at NUL");
    let mut lexer = Lexer::new("\"a\\b\nc\"");
    let token = lexer.next_token();
    assert_eq!((token.kind, token.value.as_ref()), (QString, "abc"));
}

#[test]
fn numbers_are_read_as_sscanf_reads_a_float() {
    assert_eq!(scan_float("1d5"), Some(1.0));
    assert_eq!(scan_float("1e1"), Some(10.0));
    assert_eq!(scan_float("1."), Some(1.0));
    assert_eq!(scan_float(".5"), Some(0.5));
    assert_eq!(scan_float("-.5"), Some(-0.5));
    assert_eq!(scan_float("+2"), Some(2.0));
    assert_eq!(scan_float("1e50"), Some(f32::INFINITY));
    for unreadable in ["", ".", "+", "-", "e5", "d1", "+.e3"] {
        assert_eq!(scan_float(unreadable), None, "{unreadable}");
    }
    // 1.4 is the float 1.39999998, and 1.4 GHz its product with 1e9 in
    // single precision.
    let expression = parse("0:1.4GHz").unwrap();
    let element = expression.specs[0].channels.as_ref().unwrap()[0];
    assert_eq!(element.start, 1.4_f32 * 1.0e9_f32);
    assert_eq!(element.unit, Unit::Hertz);
}

#[test]
fn the_parser_builds_casas_elements() {
    let channels = |text: &str| parse(text).unwrap().specs[0].channels.clone().unwrap();
    let index = |start: f32, stop: f32, step: f32| Element {
        start,
        stop,
        step,
        unit: Unit::Index,
    };
    assert_eq!(channels("0:2.9"), [index(2.0, 2.0, 0.0)]);
    assert_eq!(channels("0:1.9~4.1^2.5"), [index(1.0, 4.0, 2.0)]);
    assert_eq!(channels("0:^2.5"), [index(-1.0, -1.0, 2.5)]);
    assert_eq!(channels("0:^2^3"), [index(-1.0, -1.0, 3.0)]);
    let hertz = channels("0:1~2GHz^4MHz")[0];
    assert_eq!((hertz.start, hertz.stop, hertz.step), (1.0e9, 2.0e9, 4.0e6));
    assert_eq!(channels("0:1THz")[0].start, 1.0e12_f32);
    assert_eq!(channels("0:1mHz")[0].start, 1.0e6, "mHz is a megahertz");
    assert!(parse("(0,1)").unwrap().parenthesized);
    for refused in [
        "",
        " ",
        "0:",
        ":0",
        "0,",
        "0 1",
        "0&1",
        "((0))",
        "(0),1",
        "0)",
        "5^2",
        "1GHz^2MHz",
        "1~2~3",
        "0:5~3",
        "2GHz~1GHz",
        "1GHz~2",
        "0:-10km/s~10km/s",
        "0:^4MHz",
        "Hz",
        ".",
        "e5",
        "0:3e9",
        "0~1:2~3^1GHz",
    ] {
        assert!(parse(refused).is_err(), "{refused:?}");
    }
}

#[test]
fn windows_names_and_patterns_select_as_casa_does() {
    let windows = vla();
    let selection = resolve(&windows, "*").unwrap();
    assert_eq!(selection.spw_ids(), [0, 1, 2]);
    assert_eq!(selection.data_description_ids(), [0, 1, 2]);
    assert_eq!(selection.row_data_descriptions(), Some(&[0, 1, 2][..]));
    let selection = resolve(&windows, "EVLA_C#A0C0#1:5~9").unwrap();
    assert_eq!(chans(&selection), [[1, 5, 9, 1]]);
    let selection = resolve(&windows, "\"EVLA_C#A0C0#*\"").unwrap();
    assert_eq!(selection.spw_ids(), [0, 1]);
    let selection = resolve(&windows, "1,0,0").unwrap();
    assert_eq!(selection.spw_ids(), [1, 0, 0], "order and repeats are kept");
    assert_eq!(selection.row_data_descriptions(), Some(&[0, 1][..]));
    for refused in ["3", "0,3", "evla", "^2", ">2", "<0", "2<>1", "0:4.5GHz~4.6"] {
        assert!(resolve(&windows, refused).is_err(), "{refused:?}");
    }
    assert!(resolve(&windows, "").unwrap().selects_all());
}

#[test]
fn channel_ranges_clamp_and_step_as_casa_does() {
    let windows = vla();
    let cases: [(&str, &[[i64; 4]]); 10] = [
        ("0:2~5;7~9", &[[0, 2, 5, 1], [0, 7, 9, 1]]),
        ("*:3~6", &[[0, 3, 6, 1], [1, 3, 6, 1], [2, 3, 6, 1]]),
        ("0:-1", &[[0, 0, 63, 1]]),
        ("0:-3", &[[0, 0, 0, 1]]),
        ("0:64", &[[0, 63, 63, 1]]),
        ("0:60~70", &[[0, 60, 63, 1]]),
        ("0:^3", &[[0, 0, 63, 3]]),
        ("0:0~10^-2", &[[0, 0, 10, 1]]),
        ("0:4.5GHz~4.55GHz^4MHz", &[[0, 6, 31, 2]]),
        ("2:7.4385GHz", &[[2, 30, 31, 1]]),
    ];
    for (text, expected) in cases {
        assert_eq!(chans(&resolve(&windows, text).unwrap()), expected, "{text}");
    }
    let clamped = resolve(&windows, "0:64").unwrap();
    assert_eq!(
        clamped.warnings(),
        [
            "Spot-channel 64 out of range for SPW 0 (valid range 0~63). Limiting it to be \
          within the available range."
        ]
    );
    let selection = resolve(&windows, "0:2~9^3;8~9").unwrap();
    assert_eq!(selection.channels_of(0).unwrap(), Some(vec![2, 5, 8, 9]));
    assert_eq!(selection.channels_of(1).unwrap(), None);
    let frequencies = selection.channel_frequency_list(&rows(&windows)).unwrap();
    assert_eq!(frequencies[0].start_hz, 4.492e9);
    assert_eq!(frequencies[0].stop_hz, 4.506e9);
    assert_eq!(frequencies[0].step_hz, 6.0e6);
}

#[test]
fn a_skipped_frequency_range_shifts_the_next_windows_ranges() {
    // CASA's `selectChannelsFromIDList` skips one triple for a window whose
    // first range found no channel; the oracle shows the same.
    let windows = vla();
    let selection = resolve(&windows, "0~2:4.5GHz~4.55GHz;0~1").unwrap();
    assert_eq!(
        chans(&selection),
        [[0, 6, 31, 1], [0, 0, 1, 1], [2, 0, 1, 1], [2, -1, 63, 1]]
    );
    assert_eq!(selection.spw_ids(), [0, 1, 2]);
    assert!(selection.channels_of(2).is_err());
}

#[test]
fn flags_and_data_descriptions_shape_the_lists() {
    let mut windows = vla();
    windows[1].flag = true;
    let descriptions = [
        DataDescriptionRow {
            spectral_window_id: 0,
            flag_row: false,
        },
        DataDescriptionRow {
            spectral_window_id: 1,
            flag_row: false,
        },
        DataDescriptionRow {
            spectral_window_id: 0,
            flag_row: false,
        },
        DataDescriptionRow {
            spectral_window_id: 2,
            flag_row: true,
        },
    ];
    let rows = rows(&windows);
    let selection = resolve_spw_selector(&rows, &descriptions, "*").unwrap();
    assert_eq!(selection.spw_ids(), [0, 0]);
    assert_eq!(selection.data_description_ids(), [0, 2]);
    assert_eq!(
        selection.channel_list().len(),
        3,
        "flagged windows keep channels"
    );
    assert!(selection.selects_row(2) && !selection.selects_row(1) && !selection.selects_row(3));
    assert!(resolve_spw_selector(&rows, &descriptions, "1").is_err());
    let parenthesized = resolve_spw_selector(&rows, &descriptions, "(0:1~3)").unwrap();
    assert!(!parenthesized.rows_restricted());
    assert!(parenthesized.selects_row(1));
    assert_eq!(chans(&parenthesized), [[0, 1, 3, 1]]);
}

#[test]
fn indices_past_the_rows_are_names_and_huge_ranges_stay_small() {
    let mut windows = vla();
    windows[2].name = "12";
    windows.push(window("x", 1.0e9, 1.0e6, 4));
    let selection = resolve(&windows, "12").unwrap();
    assert_eq!(selection.spw_ids(), [2]);
    let selection = resolve(&windows, "0~3;12").unwrap();
    assert_eq!(selection.spw_ids(), [0, 1, 2, 3, 2]);
    let refused = resolve(&windows, "0~2000000000").unwrap_err().to_string();
    assert!(refused.contains("No match found for 4,"), "{refused}");
    assert!(resolve(&windows, "0~2147483520").is_err());
}

#[test]
fn a_frequency_in_a_window_list_replaces_the_list_but_not_its_position() {
    let windows = vla();
    // 4.5 GHz~4.7 GHz matches windows 0 and 1; `;2` then overwrites
    // position 0 and pads with a zero.
    let selection = resolve(&windows, "4.5GHz~4.7GHz;2").unwrap();
    assert_eq!(selection.spw_ids(), [2, 1, 0]);
    let selection = resolve(&windows, "0;1;4.5GHz~4.6GHz").unwrap();
    assert_eq!(selection.spw_ids(), [0]);
    // Two indices, then one window: the next index writes past the end.
    assert!(resolve(&windows, "0;1;4.55GHz;2").is_err());
}

#[test]
fn stepped_frequency_searches_casa_never_leaves_are_refused() {
    let windows = vla();
    assert!(
        resolve(&windows, "4GHz~5GHz^-1MHz").is_err(),
        "negative widths loop"
    );
    assert!(
        resolve(&windows, "4GHz~5GHz^1Hz").is_err(),
        "the step underflows a float"
    );
    // No 20 MHz step from 4.5 GHz comes within 2 MHz of window 0's
    // midpoint, 4.551 GHz (as in the oracle); 1 MHz steps do.
    assert!(resolve(&windows, "4.5GHz~4.6GHz^20MHz").is_err());
    let selection = resolve(&windows, "4.5GHz~4.6GHz^1MHz").unwrap();
    assert_eq!(selection.spw_ids(), [0]);
    // A negative step steps by the least width, positive here.
    let ascending = vec![
        window("a", 4.488e9, 2.0e6, 64),
        window("b", 4.616e9, 2.0e6, 64),
    ];
    let selection = resolve(&ascending, "4.5GHz~4.6GHz^-1MHz").unwrap();
    assert_eq!(selection.spw_ids(), [0]);
}

#[test]
fn memory_accounting_counts_every_list() {
    let windows = vla();
    let selection = resolve(&windows, "0:1~2;4~5,1,2:70").unwrap();
    let expected = 3 * size_of::<usize>()
        + 3 * size_of::<usize>()
        + 4 * size_of::<ChannelRange>()
        + 3 * size_of::<usize>()
        + size_of::<String>()
        + selection.warnings()[0].capacity();
    assert_eq!(selection.allocated_bytes(), expected);
    assert_eq!(SpwSelection::all().allocated_bytes(), 0);
}
