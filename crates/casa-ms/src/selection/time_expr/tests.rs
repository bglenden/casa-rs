// SPDX-License-Identifier: LGPL-3.0-or-later
//! `timerange` resolution against hand-computed MJD seconds, citing the
//! casacore C++ (`ms/MSSel`, `casa/OS/Time.cc`) for each behaviour.

use super::*;

/// 2017/03/01 00:00:00 UTC: MJD 57813.
const DAY: f64 = 57_813.0 * 86_400.0;
/// The first row: 2017/03/01/09:00:00.5 UTC.
const T0: f64 = DAY + 32_400.5;

fn first_row() -> TimeDefaults {
    TimeDefaults {
        time_mjd_s: T0,
        exposure_s: 2.0,
    }
}

fn resolve(text: &str) -> TimeSelection {
    resolve_time_selector(text, first_row()).unwrap_or_else(|error| panic!("{text:?}: {error}"))
}

fn refusal(text: &str) -> String {
    match resolve_time_selector(text, first_row()) {
        Ok(selection) => panic!("{text:?} resolved to {selection:?}"),
        Err(error) => error.to_string(),
    }
}

fn condition(text: &str) -> TimeCondition {
    match resolve(text).terms() {
        [term] => term.condition,
        terms => panic!("{text:?} has {} terms", terms.len()),
    }
}

/// The centre of a single time.
fn single(text: &str) -> f64 {
    match condition(text) {
        TimeCondition::Near { time_s, .. } => time_s,
        other => panic!("{text:?} is {other:?}"),
    }
}

fn within(text: &str) -> (f64, f64) {
    match condition(text) {
        TimeCondition::Within { lower_s, upper_s } => (lower_s, upper_s),
        other => panic!("{text:?} is {other:?}"),
    }
}

#[test]
fn an_empty_expression_selects_every_row_and_reads_no_defaults() {
    // MSSelection.cc:741: an empty time expression is not parsed.
    let unusable = TimeDefaults {
        time_mjd_s: f64::NAN,
        exposure_s: f64::NAN,
    };
    let selection = resolve_time_selector("", unusable).unwrap();
    assert!(selection.selects_all());
    assert!(selection.holds(f64::MIN) && selection.holds(T0));
    assert_eq!(selection.allocated_bytes(), 0);
    assert_eq!(selection.time_list().len(), 0);
    assert!(TimeSelector::parse("").unwrap().selects_all());
    assert_eq!(TimeSelection::all(), selection);
}

#[test]
fn a_blank_expression_is_a_parse_error() {
    // Only "" skips the parser; the grammar needs at least one term.
    for text in [" ", "\t", "  \n"] {
        assert!(refusal(text).contains("Parse error"), "{text:?}");
    }
}

#[test]
fn a_full_time_is_utc_mjd_seconds_with_no_leap_second_offset() {
    // toTAIInSec converts nothing (MSTimeParse.cc:339-344): 37 s would be TAI.
    assert_eq!(single("2017/03/01/09:00:00.5"), T0);
    assert_eq!(single("2017/3/1/9:0:0.5"), T0);
    assert_eq!(single("  2017 / 03 / 01 / 09 : 00 : 00.5  "), T0);
}

#[test]
fn a_single_time_holds_within_half_the_exposure_inclusive() {
    // abs(TIME - t) <= defaultExposure/2 (MSTimeParse.cc:203-207).
    let selection = resolve("2017/03/01/09:00:01.5");
    assert_eq!(
        selection.terms()[0].condition,
        TimeCondition::Near {
            time_s: T0 + 1.0,
            tolerance_s: 1.0,
        }
    );
    assert!(selection.holds(T0) && selection.holds(T0 + 2.0));
    assert!(!selection.holds(T0 - 1e-6) && !selection.holds(T0 + 2.000_001));
    let entry = selection.time_list().next().unwrap();
    assert_eq!(
        entry,
        TimeListEntry {
            start_s: T0 + 1.0,
            end_s: T0 + 1.0,
            width_s: 1.0,
        }
    );
}

#[test]
fn time_of_day_forms_follow_the_grammars_field_positions() {
    // timeObj (MSTimeGram.yy:215-219), on the first row's date.
    assert_eq!(single("09:00:00"), DAY + 32_400.0);
    assert_eq!(single("09:10:"), DAY + 33_000.0);
    // One colon is minutes and seconds: 00:09:10.
    assert_eq!(single("09:10"), DAY + 550.0);
    assert_eq!(single("2017/03/01/09:30"), DAY + 570.0);
    // A lone trailing colon is seconds: 00:00:09.
    assert_eq!(single("09:"), DAY + 9.0);
    // A bare number is seconds, with the hour and minute defaulted: 09:00:09.
    assert_eq!(single("9"), DAY + 32_409.0);
    assert_eq!(single("2017/03/01/09"), DAY + 32_409.0);
    // Empty fields are zero.
    assert_eq!(single("::"), DAY);
    assert_eq!(single("::5"), DAY + 5.0);
}

#[test]
fn missing_fields_take_the_first_rows_values() {
    // setDefaults(tf, True) (MSTimeParse.cc:357-367), from getDefaults.
    assert_eq!(single("2017/03/01"), T0);
    assert_eq!(single("2017/03/02"), T0 + 86_400.0);
    assert_eq!(single("*"), T0);
    assert_eq!(single("*/*/*/*:*:*"), T0);
    assert_eq!(single("*:*:30"), DAY + 32_430.0);
    assert_eq!(single("09:*:30"), DAY + 32_430.0);
    assert_eq!(single("2017/03/*/09:00:00.5"), T0);
    // A given second keeps zero milliseconds rather than the default 500.
    assert_eq!(single("*:*:0"), DAY + 32_400.0);
}

#[test]
fn defaults_keep_whole_milliseconds_of_the_first_row() {
    // defaultFractionalSec = (Int)((t0-t1)*1E3) (MSTimeParse.cc:180-181).
    let defaults = TimeDefaults {
        time_mjd_s: DAY + 32_400.123_4,
        exposure_s: 2.0,
    };
    let selection = resolve_time_selector("*", defaults).unwrap();
    let TimeCondition::Near { time_s, .. } = selection.terms()[0].condition else {
        panic!("expected a single time");
    };
    assert!((time_s - (DAY + 32_400.123)).abs() < 1e-6, "{time_s}");
    // The default 09:00:00.500 rebuilds the first row's TIME exactly.
    assert_eq!(within("2017/03/01~2017/03/01"), (T0, T0));
}

#[test]
fn seconds_keep_truncated_milliseconds() {
    // splitSec (MSTimeGram.yy:97-104): 1.001 is 1.000999... and keeps 0 ms.
    assert_eq!(single("09:00:01.001"), DAY + 32_401.0);
    let time_s = single("09:00:12.345");
    assert!((time_s - (DAY + 32_412.345)).abs() < 1e-6, "{time_s}");
    // 2.501 keeps 500 ms, so this range starts on the row at 09:00:02.5.
    let (lower_s, _) = within("09:00:02.501~09:00:10");
    assert_eq!(lower_s, T0 + 2.0);
    let (lower_s, _) = within("09:00:00.5009~09:00:10");
    assert_eq!(lower_s, T0);
}

#[test]
fn out_of_range_fields_roll_over_and_dates_are_checked_coarsely() {
    // validate checks only year >= 1858, 1 <= month <= 12 and 1 <= day <= 31
    // (MSTimeParse.cc:388-411); Time::setDate adds the rest arithmetically.
    assert_eq!(single("2017/02/29/09:00:00.5"), T0);
    assert_eq!(single("2017/02/31/09:00:00.5"), T0 + 2.0 * 86_400.0);
    assert_eq!(single("2017/03/01/08:59:60.5"), T0);
    assert_eq!(single("2017/03/01/08:60:00.5"), T0);
    assert_eq!(single("2017/02/28/24:00:00"), DAY);
    assert!(refusal("2017/13/01/09:00:00").contains("MSTime Selection error: Month = 13"));
    assert!(refusal("1800/01/01/00:00:00").contains("Year = 1800 out of range"));
    assert!(refusal("2017/03/32").contains("Day = 32 out of range"));
    assert!(refusal("2017//01/09:00:00").contains("Month = 0 out of range"));
    // Empty or two-field dates leave year 0 (MSTimeGram.yy:212, 222).
    assert!(refusal("/03/01/09:00:00").contains("Year = 0 out of range"));
    assert!(refusal("03/01//09:00:00").contains("Year = 0 out of range"));
}

#[test]
fn the_default_date_does_not_follow_the_time_past_midnight() {
    // A time after midnight is on the first row's date unless the hour says otherwise.
    let defaults = TimeDefaults {
        time_mjd_s: DAY + 86_399.0,
        exposure_s: 2.0,
    };
    let near =
        |text: &str| match resolve_time_selector(text, defaults).unwrap().terms()[0].condition {
            TimeCondition::Near { time_s, .. } => time_s,
            other => panic!("{other:?}"),
        };
    assert_eq!(near("00:00:01"), DAY + 1.0);
    assert_eq!(near("24:00:01"), DAY + 86_401.0);
    let reversed = resolve_time_selector("23:00:00~01:00:00", defaults).unwrap_err();
    assert!(reversed.to_string().contains("lower bound > upper bound"));
}

#[test]
fn bounds_are_inclusive() {
    // selectTimeGT is TIME >= t and selectTimeLT is TIME <= t
    // (MSTimeParse.cc:222, 237), with getTimeList's (t, DBL_MAX) and (0, t).
    let after = resolve(">09:00:00.5");
    assert_eq!(
        after.terms()[0].condition,
        TimeCondition::AtOrAfter { time_s: T0 }
    );
    assert!(after.holds(T0) && !after.holds(T0 - 1e-6));
    assert_eq!(
        after.terms()[0].time_list_entry,
        TimeListEntry {
            start_s: T0,
            end_s: f64::MAX,
            width_s: 2.0,
        }
    );
    let before = resolve("<09:00:00.5");
    assert!(before.holds(T0) && !before.holds(T0 + 1e-6));
    assert_eq!(
        before.terms()[0].time_list_entry,
        TimeListEntry {
            start_s: 0.0,
            end_s: T0,
            width_s: 2.0,
        }
    );
}

#[test]
fn a_range_is_closed_and_its_end_borrows_the_starts_fields() {
    // TIME >= lower && TIME <= upper (MSTimeParse.cc:264-266); copyDefaults
    // (MSTimeGram.yy:156) fills the end from the start.
    assert_eq!(within("09:00:00.5~09:00:02.5"), (T0, T0 + 2.0));
    let selection = resolve("09:00:00.5~09:00:02.5");
    assert!(selection.holds(T0) && selection.holds(T0 + 2.0));
    assert!(!selection.holds(T0 - 1e-6) && !selection.holds(T0 + 2.000_001));
    assert_eq!(selection.terms()[0].time_list_entry.width_s, 0.0);
    assert_eq!(
        within("2017/03/02/09:00:00~10:00:00"),
        (DAY + 86_400.0 + 32_400.0, DAY + 86_400.0 + 36_000.0)
    );
    // The end's seconds come from the start: 09:00:00.5~* is one instant.
    assert_eq!(within("2017/03/01/09:00:00.5~*"), (T0, T0));
    // A date-only end keeps the start's time of day.
    assert_eq!(
        within("2017/03/01/09:00:00.5~2017/03/02"),
        (T0, T0 + 86_400.0)
    );
    assert!(refusal("09:00:00~08:00:00").contains("lower bound > upper bound"));
}

#[test]
fn a_duration_is_added_in_mjd_days_from_the_mjd_epoch() {
    // setDefaults($3, false) and MVEpoch(mjd0, mjd1) (MSTimeGram.yy:164-185).
    assert_eq!(within("09:00:00+10"), (DAY + 32_400.0, DAY + 32_410.0));
    assert_eq!(
        within("09:00:00+00:00:10"),
        (DAY + 32_400.0, DAY + 32_410.0)
    );
    // One colon is minutes and seconds here too: one minute.
    assert_eq!(within("09:00:00+1:00"), (DAY + 32_400.0, DAY + 32_460.0));
    assert_eq!(within("09:00:00.5+:2.5"), (T0, T0 + 2.5));
    assert_eq!(
        within("09:00:00+48:00:00"),
        (DAY + 32_400.0, DAY + 32_400.0 + 172_800.0)
    );
    // A duration is a time, so a bare float is not one.
    assert!(refusal("09:00:00.5+2.5").contains("Parse error at or near token '2.5'"));
}

#[test]
fn brackets_widen_both_edges_by_half_the_exposure_exclusively() {
    // (TIME > lo || |TIME - lo| < w) && (TIME < hi || |TIME - hi| < w)
    // (MSTimeParse.cc:263, 267-270).
    let selection = resolve("[09:00:00.5~09:00:02.5]");
    assert_eq!(
        selection.terms()[0].condition,
        TimeCondition::EdgeInclusive {
            lower_s: T0,
            upper_s: T0 + 2.0,
            edge_s: 1.0,
        }
    );
    assert!(selection.holds(T0 - 0.999) && !selection.holds(T0 - 1.0));
    assert!(selection.holds(T0 + 2.999) && !selection.holds(T0 + 3.0));
    assert_eq!(selection.terms()[0].time_list_entry.width_s, 1.0);
    let duration = resolve("[09:00:00.5+00:00:02]");
    assert_eq!(duration.terms(), selection.terms());
}

#[test]
fn an_explicit_edge_width_needs_a_point_and_zero_opens_the_interval() {
    // LBRACKET: FNUMBER LSQBRACKET (MSTimeGram.yy:148-149).
    let wide = resolve("2.0[09:00:00.5~09:00:02.5]");
    assert!(wide.holds(T0 - 1.999) && !wide.holds(T0 - 2.0) && !wide.holds(T0 + 4.0));
    assert!(resolve("5.[09:00:00.5~09:00:02.5]").holds(T0 + 6.0));
    assert!(resolve(".5[09:00:00.5~09:00:02.5]").holds(T0 + 2.0));
    let open = resolve("0.0[09:00:02.5~09:00:06.5]");
    assert!(!open.holds(T0 + 2.0) && open.holds(T0 + 4.0) && !open.holds(T0 + 6.0));
    assert!(refusal("3[09:00:00~09:01:00]").contains("token '['"));
    for text in ["[09:00:00]", "[>09:00:00]", "[09:00:00~09:00:01"] {
        assert!(refusal(text).contains("Parse error"), "{text:?}");
    }
}

#[test]
fn edge_widths_are_rounded_to_single_precision() {
    // MSTimeEdgeBuffer and edgeWidth_l are C Floats (MSTimeGram.yy:94,
    // MSTimeParse.cc:263).
    let TimeCondition::EdgeInclusive { edge_s, .. } = condition("0.1[1~2]") else {
        panic!("expected brackets");
    };
    assert_eq!(edge_s, 0.1_f32);
    let edge = TimeCondition::EdgeInclusive {
        lower_s: 0.0,
        upper_s: 1.0,
        edge_s,
    };
    // 0.1 in single precision is 0.100000001490116..., so -0.1 is inside.
    assert!(edge.holds(-0.1));
    let odd_exposure = TimeDefaults {
        time_mjd_s: T0,
        exposure_s: 1.0368,
    };
    let selection = resolve_time_selector("[1~2]", odd_exposure).unwrap();
    let TimeCondition::EdgeInclusive { edge_s, .. } = selection.terms()[0].condition else {
        panic!("expected brackets");
    };
    assert_eq!(edge_s, 0.5184_f32);
    assert_eq!(
        selection.terms()[0].time_list_entry.width_s,
        f64::from(0.5184_f32)
    );
}

#[test]
fn terms_are_ored_and_later_terms_read_row_zeros_defaults() {
    // addCondition ORs the terms (MSTimeParse.cc:188-196). getDefaults keeps
    // firstLogicalRow = 0 after its first call (MSTimeParse.cc:116-120).
    let field_one = TimeDefaults {
        time_mjd_s: DAY + 86_400.0 + 37_800.5,
        exposure_s: 6.0,
    };
    let defaults = TimeSelectorDefaults {
        first_term: field_one,
        later_terms: first_row(),
    };
    let selection = resolve_time_selector("10:30:00.5,10:40:00.5", defaults).unwrap();
    assert_eq!(
        selection
            .terms()
            .iter()
            .map(|term| term.condition)
            .collect::<Vec<_>>(),
        [
            TimeCondition::Near {
                time_s: DAY + 86_400.0 + 37_800.5,
                tolerance_s: 3.0,
            },
            TimeCondition::Near {
                time_s: DAY + 38_400.5,
                tolerance_s: 1.0,
            },
        ]
    );
    assert!(selection.holds(DAY + 86_400.0 + 37_803.5));
    assert!(selection.holds(DAY + 38_401.5) && !selection.holds(DAY + 38_402.0));
    assert_eq!(selection.time_list().len(), 2);
    let both = resolve("09:00:00.5~09:00:04.5,09:30:00.5");
    assert!(both.holds(T0 + 4.0) && both.holds(T0 + 1_800.0) && !both.holds(T0 + 6.0));
}

#[test]
fn malformed_expressions_are_refused_as_casa_refuses_them() {
    for text in [
        "2017-03-01",
        "abc",
        "57813.375",
        "09:00:00+10s",
        "09:00:00.5-09:00:02.5",
        "09:00:00~",
        "~09:00:00",
        "09:00:00,",
        ",09:00:00",
        "09:00:00.5~09:00:02.5~09:00:04.5",
        "2017/03/01 09:00:00",
        "2017/03/01//09:00:00",
        "09:00:00.5&&09:00:04.5",
        "'09:00:00'",
        ">=09:00:00",
        "2017/03/01T09:00:00",
    ] {
        assert!(
            refusal(text).contains("MSSelection time error: Parse error"),
            "{text:?}"
        );
    }
    // CASA reports `Year = 3 out of range` here, since bison's default
    // reductions run the term's action before it reads the stray colon; the
    // port parses first, and both refuse.
    assert!(refusal("03/01/09:00:00").contains("token ':'"));
}

#[test]
fn an_unusable_default_row_is_refused() {
    let unusable = TimeDefaults {
        time_mjd_s: f64::INFINITY,
        exposure_s: 1.0,
    };
    let error = resolve_time_selector("09:00:00", unusable).unwrap_err();
    assert!(error.to_string().contains("default row TIME"), "{error}");
}

#[test]
fn the_parsed_selector_resolves_later() {
    let selector = TimeSelector::parse("09:00:00~10:00:00, >11:00:00").unwrap();
    assert!(!selector.selects_all());
    assert_eq!(selector.term_count(), 2);
    assert_eq!(
        selector.resolve(first_row()).unwrap(),
        resolve("09:00:00~10:00:00, >11:00:00")
    );
}

#[test]
fn a_selection_holds_one_term_per_comma_and_nothing_else() {
    // Memory contract: the resolved selection owns exactly its terms.
    let term = std::mem::size_of::<TimeTerm>();
    for (text, terms) in [
        ("09:00:00", 1),
        ("09:00:00~10:00:00,[1~2],>3,<4,5", 5),
        (
            "                                               09:00:00            ",
            1,
        ),
    ] {
        let selection = resolve(text);
        assert_eq!(selection.terms().len(), terms);
        assert_eq!(selection.allocated_bytes(), terms * term, "{text:?}");
    }
    let many = vec!["09:00:00"; 1000].join(",");
    assert_eq!(resolve(&many).allocated_bytes(), 1000 * term);
}

#[test]
fn the_first_logical_row_follows_msselections_default_path() {
    // MSTimeParse.cc:114-149 with honourRowFlags true.
    let flags = [true, true, false, false];
    let mut flag_row = |row: usize| flags[row];
    assert_eq!(
        msselection_first_logical_row(4, LogicalRowFilter::FlagRow(&mut flag_row)).unwrap(),
        2
    );
    let mut all_flagged = |_: usize| true;
    assert_eq!(
        msselection_first_logical_row(4, LogicalRowFilter::FlagRow(&mut all_flagged)).unwrap(),
        0
    );
    // With other selections FLAG_ROW is not read: both branches select alike.
    let selected = [false, true, true, false];
    let mut other = |row: usize| selected[row];
    assert_eq!(
        msselection_first_logical_row(4, LogicalRowFilter::OtherSelections(&mut other)).unwrap(),
        1
    );
    let mut none = |_: usize| false;
    assert_eq!(
        msselection_first_logical_row(4, LogicalRowFilter::OtherSelections(&mut none)).unwrap(),
        0
    );
    let mut unused = |_: usize| -> bool { unreachable!("an empty table has no rows") };
    let error =
        msselection_first_logical_row(0, LogicalRowFilter::FlagRow(&mut unused)).unwrap_err();
    assert!(error.to_string().contains("No logical \"row zero\""));
}
