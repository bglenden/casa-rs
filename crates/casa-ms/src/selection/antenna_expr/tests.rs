// SPDX-License-Identifier: LGPL-3.0-or-later
//! Unit tests of the antenna expression port. Each test names the casacore
//! behaviour (file:line at commit `aaf72eb7`) it pins.

use std::collections::BTreeSet;

use super::lexer::{Kind, LexError, Lexer};
use super::regex::{FullMatch, from_pattern, to_ecma};
use super::units::{UnitError, unit_factor};
use super::{AntennaRow, resolve_antenna_selector};

type Pairs = BTreeSet<(usize, usize)>;

/// Five VLA-like antennas on a line, so baseline lengths are exact:
/// 0–1 100 m, 1–2 150 m, 0–2 250 m, 3–4 600 m, 2–3 750 m, 1–3 900 m,
/// 0–3 1000 m, 2–4 1350 m, 1–4 1500 m, 0–4 1600 m.
fn vla() -> Vec<AntennaRow<'static>> {
    [
        ("ea01", "W08", 0.0),
        ("ea02", "E04", 100.0),
        ("ea03", "N16", 250.0),
        ("ea04", "W10", 1000.0),
        ("ea05", "E08", 1600.0),
    ]
    .into_iter()
    .map(|(name, station, x)| row(name, station, x))
    .collect()
}

fn row(name: &'static str, station: &'static str, x: f64) -> AntennaRow<'static> {
    AntennaRow {
        name,
        station,
        position_m: [x - 1_601_000.0, -5_042_000.0, 3_554_000.0],
    }
}

/// The ordered existing-antenna pairs `text` selects.
fn select(rows: &[AntennaRow<'_>], text: &str) -> Pairs {
    let selection =
        resolve_antenna_selector(rows, text).unwrap_or_else(|error| panic!("{text}: {error}"));
    let count = rows.len();
    (0..count)
        .flat_map(|a1| (0..count).map(move |a2| (a1, a2)))
        .filter(|&(a1, a2)| selection.holds(a1, a2))
        .collect()
}

fn refusal(rows: &[AntennaRow<'_>], text: &str) -> String {
    match resolve_antenna_selector(rows, text) {
        Ok(_) => panic!("{text:?} should be refused"),
        Err(error) => error.to_string(),
    }
}

/// Cross baselines in both orders.
fn cross(pairs: &[(usize, usize)]) -> Pairs {
    pairs.iter().flat_map(|&(a, b)| [(a, b), (b, a)]).collect()
}

fn autos(ids: &[usize]) -> Pairs {
    ids.iter().map(|&id| (id, id)).collect()
}

fn all_cross(count: usize) -> Pairs {
    (0..count)
        .flat_map(|a| (0..count).map(move |b| (a, b)))
        .filter(|(a, b)| a != b)
        .collect()
}

fn union(parts: &[Pairs]) -> Pairs {
    parts.iter().flatten().copied().collect()
}

fn minus(all: &Pairs, removed: &Pairs) -> Pairs {
    all.difference(removed).copied().collect()
}

/// Baselines of `antenna` with every other antenna of five.
fn baselines_of(antenna: usize) -> Pairs {
    cross(
        &(0..5)
            .filter(|other| *other != antenna)
            .map(|other| (antenna, other))
            .collect::<Vec<_>>(),
    )
}

// ---- the empty expression and the data layout -------------------------

/// `MSSelection.cc:590`: an empty antenna expression adds no condition, but
/// blank text is parsed, and `baseline` cannot be empty (`.yy:158-242`).
#[test]
fn empty_text_is_no_constraint_and_blank_text_is_a_syntax_error() {
    let rows = vla();
    let selection = resolve_antenna_selector(&rows, "").unwrap();
    assert!(selection.is_unconstrained());
    assert_eq!(selection.allocated_bytes(), 0);
    assert_eq!(selection.antenna_count(), 5);
    assert!(selection.holds(0, 0) && selection.holds(4, 1) && selection.holds(70, 71));
    assert!(refusal(&rows, " ").contains("Parse error at or near ''"));
}

/// The pair table is `(n + 2)²` bytes: the two extra slots stand for IDs
/// beyond the ANTENNA table, which `IN` and `==` never match
/// (`MSAntennaParse.cc:112-129`), so only their equality matters.
#[test]
fn ids_beyond_the_antenna_table_follow_the_row_predicate() {
    let rows = vla();
    let plain = resolve_antenna_selector(&rows, "ea01").unwrap();
    assert_eq!(plain.allocated_bytes(), 7 * 7);
    assert!(
        plain.holds(0, 30) && plain.holds(30, 0),
        "0 IN [0], 0 != 30"
    );
    assert!(!plain.holds(30, 31) && !plain.holds(30, 30));
    let negated = resolve_antenna_selector(&rows, "!ea01").unwrap();
    assert!(negated.holds(30, 31) && negated.holds(30, 30) && negated.holds(2, 30));
    assert!(!negated.holds(0, 30));
    let autos_only = resolve_antenna_selector(&rows, "!ea01&&&").unwrap();
    assert!(autos_only.holds(30, 30) && autos_only.holds(30, 31) && !autos_only.holds(0, 0));
}

// ---- antenna items ------------------------------------------------------

/// `antid: identstr` (`MSAntennaGram.yy:293-306`) with the `CrossOnly`
/// list form (`.yy:193-201`, `MSAntennaParse.cc:123-130,84-89`).
#[test]
fn a_name_selects_its_cross_baselines_in_either_order() {
    let rows = vla();
    assert_eq!(select(&rows, "ea01"), baselines_of(0));
    assert_eq!(
        select(&rows, "ea01,ea02"),
        union(&[baselines_of(0), baselines_of(1)])
    );
    assert_eq!(select(&rows, "  ea01 , ea02  "), select(&rows, "ea01,ea02"));
}

/// `antidrange: INT` tries the NAME first, keeping its first match, then
/// the ID (`MSAntennaGram.yy:336-357`); a range looks up `ostringstream`
/// decimals (`.yy:358-380`).
#[test]
fn integers_are_names_before_they_are_ids() {
    let rows = vla();
    assert_eq!(select(&rows, "0"), baselines_of(0));
    let mut named = vla();
    named[3].name = "1";
    named[4].name = "01";
    assert_eq!(select(&named, "1"), baselines_of(3), "NAME \"1\" is row 3");
    assert_eq!(
        select(&named, "01"),
        baselines_of(4),
        "NAME \"01\" is row 4"
    );
    assert_eq!(select(&named, "2"), baselines_of(2), "no NAME \"2\": ID 2");
    // The range writes 0 and 1 as "0" and "1": "1" is row 3, "0" is ID 0.
    assert_eq!(select(&named, "0~1&"), cross(&[(0, 3)]));
    let mut twins = vla();
    twins[2].name = "4";
    twins[3].name = "4";
    assert_eq!(select(&twins, "4"), baselines_of(2), "only the first match");
    // A NAME is matched before a STATION.
    let mut stations = vla();
    stations[1].station = "3";
    assert_eq!(select(&stations, "3"), baselines_of(1));
}

/// Ranges past the table are clipped by `matchId` (`MSAntennaIndex.cc:62-78`);
/// an empty result is reported; `Int len = end - start + 1`
/// (`MSAntennaGram.yy:362-366`) fails below one or on overflow.
#[test]
fn ranges_clip_to_existing_antennas_and_refuse_impossible_lengths() {
    let rows = vla();
    assert_eq!(select(&rows, "3~30&"), cross(&[(3, 4)]));
    assert_eq!(select(&rows, "1~2147483647&"), select(&rows, "1~4&"));
    assert!(refusal(&rows, "30").contains("No match found for the antenna specificion"));
    assert!(refusal(&rows, "5~9").contains("No match found"));
    assert!(refusal(&rows, "3~2").contains("No match found"), "length 0");
    assert!(
        refusal(&rows, "4~2").contains("cannot allocate"),
        "length -1"
    );
    assert!(
        refusal(&rows, "0~2147483647").contains("cannot allocate"),
        "Int overflow"
    );
    // atoi keeps the low 32 bits of strtol's value.
    assert_eq!(select(&rows, "4294967296"), baselines_of(0));
    assert_eq!(select(&rows, "4294967297~4294967298&"), cross(&[(1, 2)]));
}

/// `matchAntennaName` falls back to STATION only when no NAME matches
/// (`MSAntennaIndex.cc:146-156`).
#[test]
fn a_station_name_selects_when_no_antenna_has_that_name() {
    let rows = vla();
    assert_eq!(select(&rows, "W08"), baselines_of(0));
    let mut shadowed = vla();
    shadowed[2].name = "W08";
    assert_eq!(select(&shadowed, "W08"), baselines_of(2));
}

/// Bare `PATTERN` tokens and quoted strings are patterns
/// (`MSAntennaGram.ll:111-116,69-75`) converted by `Regex::fromPattern`,
/// matched in full against NAME, else STATION, per antenna; a leading `^`
/// negates (`MSAntennaIndex.cc:92-118`).
#[test]
fn patterns_match_whole_names_or_stations() {
    let rows = vla();
    assert_eq!(select(&rows, "ea0*&"), all_cross(5));
    assert_eq!(select(&rows, "ea0[12]&"), cross(&[(0, 1)]));
    assert_eq!(select(&rows, "ea0?&ea05"), baselines_of(4));
    assert_eq!(select(&rows, "\"ea0{1,3}\"&"), cross(&[(0, 2)]));
    assert_eq!(select(&rows, "'ea0[!1-3]'&"), cross(&[(3, 4)]));
    assert_eq!(
        select(&rows, "^ea01&"),
        minus(&all_cross(5), &baselines_of(0))
    );
    assert_eq!(
        select(&rows, "W*&"),
        cross(&[(0, 3)]),
        "STATION W08 and W10"
    );
}

/// The empty pattern above is reported, as is any pattern matching nothing.
#[test]
fn a_pattern_matching_nothing_is_reported() {
    let rows = vla();
    assert!(refusal(&rows, "\"ea0\"").contains("No match found for token(s) \"ea0\""));
    assert!(
        refusal(&rows, "ea0.*").contains("No match found"),
        "'.' is literal"
    );
}

/// `antid: REGEX` matches with `Regex(text)`, `String::matches` being a full
/// match (`casa/BasicSL/String.cc:367-372`); a leading `^`, written inside
/// or before the slashes, negates (`MSAntennaGram.ll:81`,
/// `MSAntennaIndex.cc:95-98`).
#[test]
fn regexes_match_in_full_and_a_leading_caret_negates() {
    let rows = vla();
    assert_eq!(select(&rows, "/ea0[12]/&"), cross(&[(0, 1)]));
    assert_eq!(select(&rows, "/ea0(1|3)/&"), cross(&[(0, 2)]));
    // No NAME matches, so STATION decides: W08, E04 and E08, not W10.
    assert_eq!(
        select(&rows, "/[EW]0.*/&"),
        cross(&[(0, 1), (0, 4), (1, 4)])
    );
    assert!(refusal(&rows, "/ea0/").contains("No match found"));
    let not_first_two = cross(&[(2, 3), (2, 4), (3, 4)]);
    assert_eq!(select(&rows, "/^ea0[12]/&"), not_first_two);
    assert_eq!(select(&rows, "^/ea0[12]/&"), not_first_two);
    assert!(refusal(&rows, "/ea(0/").contains("Error in regex"));
    assert!(refusal(&rows, r"/(ea0)\1/").contains("backreferences"));
}

/// `({QSTRING}|{ASTRING})+` strips only the outer quotes
/// (`MSAntennaGram.ll:69-75`).
#[test]
fn concatenated_quotes_keep_their_inner_quotes() {
    let mut rows = vla();
    rows[2].name = "ea\"'03";
    assert_eq!(select(&rows, "\"ea\"'03'&ea01"), cross(&[(0, 2)]));
}

/// `ESCNAME` takes everything up to a blank as a name
/// (`MSAntennaGram.ll:59,104-109`); `NAME2` admits colons (`.ll:57`).
#[test]
fn escaped_and_colon_names() {
    let mut rows = vla();
    rows[1].name = "1A";
    rows[2].name = "ea:03";
    assert_eq!(select(&rows, r"\1A"), baselines_of(1));
    // Unescaped, `1A` is INT UNIT: the start of a baseline length in amperes.
    assert!(refusal(&rows, "1A").contains("Unit A must be a distance unit"));
    assert_eq!(select(&rows, "ea:03"), baselines_of(2));
    assert!(
        refusal(&rows, r"\ea01,ea02").contains("\"ea01,ea02\""),
        "one name"
    );
}

// ---- baseline forms -------------------------------------------------------

/// `&`, `&&` and `&&&` (`MSAntennaGram.yy:168-230`): `CrossOnly` masks
/// autocorrelations (`MSAntennaParse.cc:84-89`), `AutoCorrAlso` keeps the
/// pairs with an antenna in both lists, `AutoCorrOnly` keeps only
/// `ANTENNA1 == ANTENNA2 == id` (`.cc:105-121`).
#[test]
fn ampersand_forms_choose_cross_and_autocorrelations() {
    let rows = vla();
    assert_eq!(select(&rows, "ea01&ea02"), cross(&[(0, 1)]));
    assert_eq!(select(&rows, "ea02&ea01"), cross(&[(0, 1)]));
    assert_eq!(select(&rows, "ea01,ea02&"), cross(&[(0, 1)]));
    assert_eq!(select(&rows, "ea01,ea02&ea03"), cross(&[(0, 2), (1, 2)]));
    assert_eq!(select(&rows, "ea01&&ea02"), cross(&[(0, 1)]));
    assert_eq!(
        select(&rows, "ea01,ea02&&ea02"),
        union(&[cross(&[(0, 1)]), autos(&[1])])
    );
    assert_eq!(select(&rows, "ea01&&"), autos(&[0]));
    assert_eq!(
        select(&rows, "ea01,ea02&&"),
        union(&[cross(&[(0, 1)]), autos(&[0, 1])])
    );
    assert_eq!(select(&rows, "ea01,ea02&&&"), autos(&[0, 1]));
    assert_eq!(select(&rows, "*&&&"), autos(&[0, 1, 2, 3, 4]));
    assert_eq!(select(&rows, "ea01&*"), baselines_of(0));
    assert_eq!(select(&rows, "ea01&ea01"), BTreeSet::new());
    for text in ["ea01&&&ea02", "ea01&ea02&ea03", "ea01&&&&"] {
        assert!(refusal(&rows, text).contains("Parse error"), "{text}");
    }
}

/// Every unmatched item is reported and the whole expression refused
/// (`reportError`, `MSAntennaGram.yy:99-121`; `MSSelection.cc:858-864`).
#[test]
fn an_unmatched_item_refuses_the_whole_expression() {
    let rows = vla();
    for text in ["ea01,ea99", "ea01&ea99", "ea99&&&", "ea01;ea99"] {
        assert!(refusal(&rows, text).contains("\"ea99\""), "{text}");
    }
    assert!(refusal(&rows, "!ea99").contains("\"!ea99\""));
    assert!(refusal(&rows, "@X99").contains("Station Expression"));
}

/// `setTEN` (`MSAntennaParse.cc:80-98`): `!` negates a term after its
/// autocorrelation mask; later plain terms OR, later negated terms AND.
#[test]
fn negation_and_semicolons_combine_left_to_right() {
    let rows = vla();
    let everything: Pairs = (0..5).flat_map(|a| (0..5).map(move |b| (a, b))).collect();
    assert_eq!(select(&rows, "!ea01"), minus(&everything, &baselines_of(0)));
    assert!(
        select(&rows, "!ea01").contains(&(0, 0)),
        "ea01's own autocorrelation"
    );
    assert_eq!(
        select(&rows, "ea01;ea02"),
        union(&[baselines_of(0), baselines_of(1)])
    );
    assert_eq!(
        select(&rows, "ea01;!ea02"),
        cross(&[(0, 2), (0, 3), (0, 4)])
    );
    assert_eq!(
        select(&rows, "!ea02;ea01"),
        minus(&everything, &cross(&[(1, 2), (1, 3), (1, 4)]))
    );
    assert_eq!(select(&rows, "!ea01&&&"), minus(&everything, &autos(&[0])));
    assert_eq!(
        select(&rows, "ea01&ea02;ea03&&&"),
        union(&[cross(&[(0, 1)]), autos(&[2])])
    );
    for text in ["ea01;", ";ea01", "!!ea01", "ea01,!ea02", "ea01 ea02"] {
        assert!(refusal(&rows, text).contains("Parse error"), "{text}");
    }
}

/// `antatstation` (`MSAntennaGram.yy:416-438`): the intersection of the
/// antennas and the rows whose STATION matches (`matchStationName`, exact;
/// `matchStationRegexOrPattern`, `MSAntennaIndex.cc:187-263`).
#[test]
fn at_selects_antennas_by_station() {
    let rows = vla();
    assert_eq!(select(&rows, "ea01@W08"), baselines_of(0));
    assert_eq!(select(&rows, "@W08"), baselines_of(0));
    assert_eq!(select(&rows, "0@W08"), baselines_of(0));
    assert_eq!(select(&rows, "(ea01,ea02)@E04"), baselines_of(1));
    assert_eq!(select(&rows, "ea0*@(W08,E04)&"), cross(&[(0, 1)]));
    assert_eq!(select(&rows, "ea0*@W*&"), cross(&[(0, 3)]));
    assert_eq!(
        select(&rows, "@/[EW]0[48]/&"),
        cross(&[(0, 1), (0, 4), (1, 4)])
    );
    assert_eq!(select(&rows, "@'E0?'&"), cross(&[(1, 4)]));
    assert!(refusal(&rows, "ea01@E04").contains("Ant@Station Expression"));
    // A station name is not an antenna name here.
    assert!(refusal(&rows, "@ea01").contains("Station Expression"));
    for text in ["(ea01,ea02)", "ea01@W08@E04", "@(W08", "ea01@"] {
        assert!(refusal(&rows, text).contains("Parse error"), "{text}");
    }
}

/// `blregexlist` and `selectBLRegex` (`MSAntennaGram.ll:83-86`,
/// `MSAntennaParse.cc:250-309`): ordered `NAME&NAME` strings,
/// autocorrelations included, no report when nothing matches.
#[test]
fn baseline_regexes_select_ordered_pairs() {
    let rows = vla();
    assert_eq!(select(&rows, "/ea01&ea02/"), [(0, 1)].into());
    assert_eq!(select(&rows, "/ea02&ea01/"), [(1, 0)].into());
    assert_eq!(
        select(&rows, "/ea0[12]&ea0[12]/"),
        union(&[cross(&[(0, 1)]), autos(&[0, 1])])
    );
    assert_eq!(
        select(&rows, "/ea01&ea02/,/ea05&ea04/"),
        [(0, 1), (4, 3)].into()
    );
    assert_eq!(select(&rows, "/ea01&zz/"), BTreeSet::new());
    let everything: Pairs = (0..5).flat_map(|a| (0..5).map(move |b| (a, b))).collect();
    assert_eq!(
        select(&rows, "^/ea01&.*/"),
        minus(&everything, &(0..5).map(|b| (0, b)).collect())
    );
    assert_eq!(
        select(&rows, "!/ea01&ea02/"),
        minus(&everything, &[(0, 1)].into())
    );
    assert!(refusal(&rows, "/ea01&ea02/,ea03").contains("Parse error"));
    assert!(refusal(&rows, "ea03,/ea01&ea02/").contains("Parse error"));
}

/// `blength` (`MSAntennaGram.yy:465-506`) and `selectLength`
/// (`MSAntennaParse.cc:230-248`): closed intervals in metres,
/// autocorrelations (length 0) included, `<`/`>` moved by 1e-9.
#[test]
fn baseline_lengths_select_by_itrf_distance() {
    let rows = vla();
    let all_autos = autos(&[0, 1, 2, 3, 4]);
    let short = cross(&[(0, 1), (1, 2), (0, 2)]);
    assert_eq!(
        select(&rows, "<200m"),
        union(&[all_autos.clone(), cross(&[(0, 1), (1, 2)])])
    );
    assert_eq!(
        select(&rows, "<=150m"),
        union(&[all_autos.clone(), cross(&[(0, 1), (1, 2)])])
    );
    assert_eq!(
        select(&rows, "<150m"),
        union(&[all_autos.clone(), cross(&[(0, 1)])])
    );
    assert_eq!(
        select(&rows, "<150"),
        select(&rows, "<150m"),
        "metres by default"
    );
    assert_eq!(select(&rows, ">1km"), cross(&[(0, 4), (1, 4), (2, 4)]));
    assert_eq!(
        select(&rows, ">=1km"),
        cross(&[(0, 3), (0, 4), (1, 4), (2, 4)])
    );
    // `1d2` scans as a FLOAT, but atof reads it as 1.
    for text in [
        "100m~250m",
        "0.1~0.25km",
        "100.~250.",
        "0.1km~0.25km",
        "1d2~250.",
    ] {
        assert_eq!(select(&rows, text), short, "{text}");
    }
    assert_eq!(
        select(&rows, "<101m,>=1.5km"),
        union(&[all_autos.clone(), cross(&[(0, 1), (0, 4), (1, 4)])])
    );
    assert_eq!(
        select(&rows, "<400ft"),
        union(&[all_autos.clone(), cross(&[(0, 1)])])
    );
    assert_eq!(
        select(&rows, "<=10dam"),
        select(&rows, "<=100m"),
        "the two-letter prefix"
    );
    assert_eq!(
        select(&rows, "!<200m"),
        minus(&all_cross(5), &cross(&[(0, 1), (1, 2)]))
    );
    assert_eq!(
        select(&rows, "ea05&&&;<101m"),
        union(&[all_autos, cross(&[(0, 1)])])
    );
}

/// Lengths the grammar or `getUnitFactor` refuses.
#[test]
fn baseline_length_refusals() {
    let rows = vla();
    for text in [
        "1.5",
        "1.5km",
        "1.5~2",
        "1.5km~2",
        "<10m&",
        "<10m,ea01",
        "1d2~250",
    ] {
        assert!(refusal(&rows, text).contains("Parse error"), "{text}");
    }
    // `INT DASH INT` is an antenna range; bison's default reduction runs
    // its action, whose empty `matchId` report then outranks the syntax
    // error at the stray unit (`MSSelection.cc:759-765`).
    assert!(refusal(&rows, "100~250m").contains("No match found"));
    assert!(refusal(&rows, "1~2m").contains("Parse error at or near 'm'"));
    assert!(refusal(&rows, "<100s").contains("must be a distance unit"));
    assert!(
        refusal(&rows, "<100min").contains("must be a distance unit"),
        "minutes"
    );
    assert!(refusal(&rows, "<100M").contains("Illegal unit string 'M'"));
}

// ---- scanner ---------------------------------------------------------------

/// The scanner's refusals (`MSAntennaGram.ll:147-157`) and the `~` tip
/// (`MSSelectionError.cc:61-62`).
#[test]
fn scanner_refusals() {
    let rows = vla();
    assert!(refusal(&rows, "ea01,\"ea02").contains("Unterminated string"));
    assert!(refusal(&rows, "ea01,/ea02").contains("Unterminated regex"));
    assert!(refusal(&rows, "ea01#").contains("Parse error at or near '#'"));
    assert!(refusal(&rows, "ea01,é").contains("Parse error at or near 'é'"));
    assert!(
        !refusal(&rows, "1-3").contains("TIP"),
        "an invalid '-' is not counted"
    );
    let mut dashed = vla();
    dashed[0].name = "x-";
    assert!(refusal(&dashed, "x-#").contains("TIP"));
    // `-` is a name character: this is one name that matches nothing.
    assert!(refusal(&rows, "ea01-ea03").contains("\"ea01-ea03\""));
}

/// flex's longest match with ties to the earlier rule
/// (`MSAntennaGram.ll:69-157`).
#[test]
fn the_scanner_takes_the_longest_match() {
    let kinds = |text: &str| {
        let mut lexer = Lexer::new(text);
        let mut out = Vec::new();
        loop {
            let token = lexer.next_token().unwrap();
            if token.kind == Kind::End {
                return out;
            }
            out.push((token.kind, token.value.to_string()));
        }
    };
    let one = |kind: Kind, value: &str| vec![(kind, value.to_string())];
    assert_eq!(kinds("ea01"), one(Kind::Identifier, "ea01"));
    assert_eq!(kinds("ea"), one(Kind::Unit, "ea"));
    assert_eq!(kinds("ea0*"), one(Kind::QString, "ea0*"));
    assert_eq!(kinds("*"), one(Kind::QString, "*"));
    assert_eq!(kinds("\"a b\""), one(Kind::QString, "a b"));
    assert_eq!(kinds("/a b/"), one(Kind::Regex, "a b"));
    assert_eq!(kinds("/a&b/"), one(Kind::BlRegex, "a&b"));
    assert_eq!(kinds("1.5e3"), one(Kind::Float, "1.5e3"));
    assert_eq!(kinds(".5"), one(Kind::Float, ".5"));
    assert_eq!(kinds("<="), one(Kind::Le, "<="));
    assert_eq!(
        kinds(r"\a,b c"),
        vec![(Kind::Identifier, "a,b".into()), (Kind::Unit, "c".into())]
    );
    assert_eq!(
        kinds("10km"),
        vec![(Kind::Int, "10".into()), (Kind::Unit, "km".into())]
    );
    assert_eq!(
        kinds("1e"),
        vec![(Kind::Int, "1".into()), (Kind::Unit, "e".into())]
    );
    let mut caret = Lexer::new("^/a/");
    let token = caret.next_token().unwrap();
    assert_eq!(
        (token.kind, token.value, token.caret),
        (Kind::Regex, "a", true)
    );
    assert_eq!(token.negation(), (true, "a"));
    assert_eq!(
        Lexer::new("'a").next_token().unwrap_err(),
        LexError::UnterminatedString
    );
    assert_eq!(super::index::c_atof("1d3"), 1.0, "strtod stops at D");
    assert_eq!(super::index::c_atof("1."), 1.0);
}

// ---- casacore Regex and units ------------------------------------------------

/// `Regex::fromPattern` (`casa/Utilities/Regex.cc:228-367`).
#[test]
fn from_pattern_is_casacores() {
    let convert = |pattern: &str| String::from_utf8(from_pattern(pattern.as_bytes())).unwrap();
    assert_eq!(convert("ea0*"), "ea0.*");
    assert_eq!(convert("a?b"), "a.b");
    assert_eq!(convert("a.b+c"), r"a\.b\+c");
    assert_eq!(convert("{a,b}"), "(a|b)");
    assert_eq!(convert("x{,a}"), "x(a)?");
    assert_eq!(convert("[!0-3]"), "[^0-3]");
    assert_eq!(convert("[]a]"), "[]a]");
    assert_eq!(convert(r"a\,b"), "a,b");
    assert_eq!(convert(r"a\*"), r"a\*");
    assert_eq!(convert("a}"), r"a\}");
    assert_eq!(convert(r"a\"), r"a\");
}

/// `Regex::toEcma` (`casa/Utilities/Regex.cc:146-226`).
#[test]
fn to_ecma_is_casacores() {
    let convert = |regex: &str| String::from_utf8(to_ecma(regex.as_bytes())).unwrap();
    assert_eq!(convert("[]a]"), r"[\]a]");
    assert_eq!(convert("a]"), r"a\]");
    assert_eq!(convert("[[:alpha:]]x"), "[[:alpha:]]x");
    assert_eq!(convert("[a[b]"), r"[a\[b]");
    assert_eq!(convert(r"(a)\12"), r"(a)\1[2]");
    assert!(FullMatch::compile_pattern("[]a]*").unwrap().matches("]a]"));
    assert!(FullMatch::compile("[[:digit:]]+").unwrap().matches("042"));
    assert!(!FullMatch::compile("a.c").unwrap().matches("a\nc"));
    assert!(FullMatch::compile("[^]").unwrap().matches("\n"));
}

/// `getUnitFactor` (`MSAntennaParse.cc:336-347`) over casacore's unit maps
/// and prefixes (`casa/Quanta/UnitVal.cc:204-226`).
#[test]
fn unit_factors_are_casacores() {
    assert_eq!(unit_factor("m"), Ok(1.0));
    assert_eq!(unit_factor("km"), Ok(1.0e-3));
    assert_eq!(unit_factor("mm"), Ok(1.0e3));
    assert_eq!(unit_factor("dam"), Ok(1.0e-1));
    assert_eq!(unit_factor("am"), Ok(1.0 / 1.0e-18), "atto-metre");
    assert!((1.0 / unit_factor("ft").unwrap() - 0.3048).abs() < 1e-15);
    // casacore's AU is C::c * 499.0047837 s, its parsec AU / arcsec.
    let parsec = 3.085_677_580_649_422e16;
    assert!((1.0 / unit_factor("pc").unwrap() / parsec - 1.0).abs() < 1e-15);
    assert!((1.0 / unit_factor("kpc").unwrap() / (1e3 * parsec) - 1.0).abs() < 1e-15);
    assert_eq!(unit_factor("min"), Err(UnitError::NotDistance));
    assert_eq!(
        unit_factor("cd"),
        Err(UnitError::NotDistance),
        "candela, not centi-day"
    );
    assert_eq!(unit_factor("lambda"), Err(UnitError::NotDistance));
    assert_eq!(unit_factor("M"), Err(UnitError::Unknown));
    assert_eq!(unit_factor("meter"), Err(UnitError::Unknown));
}
