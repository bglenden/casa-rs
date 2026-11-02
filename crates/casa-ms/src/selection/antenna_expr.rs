// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's antenna (baseline) selection expressions, ported from casacore.
//!
//! [`resolve_antenna_selector`] parses the `antenna=` text of an
//! `MSSelection` and resolves it against the ANTENNA table into a
//! [`BaselineSelection`]: for every ordered (`ANTENNA1`, `ANTENNA2`) pair of
//! a MAIN row, whether CASA would select that row. It follows casacore at
//! commit `aaf72eb7` (the casacore of CASA 6.7.6.14): the scanner
//! `ms/MSSel/MSAntennaGram.ll`, the grammar `MSAntennaGram.yy`, the
//! selection logic `MSAntennaParse.cc` and the name lookups
//! `MSAntennaIndex.cc`. Where these have quirks, CASA's behaviour is kept
//! and the quirk is noted below.
//!
//! # Expressions
//!
//! An expression is one or more *terms* separated by `;`. A term is a
//! baseline form applied to antenna lists, a baseline-regex list or a
//! baseline-length list, optionally negated by a leading `!`.
//!
//! | Form | Selects (cross = `ANTENNA1 != ANTENNA2`) | C++ |
//! |---|---|---|
//! | `L` | cross baselines with an antenna of `L` on either side | `MSAntennaGram.yy:193-201`, `MSAntennaParse.cc:123-130,84-89` |
//! | `L1&L2` | cross baselines with one side in `L1` and the other in `L2`, in either order | `.yy:168-178`, `.cc:157-176` |
//! | `L&` | cross baselines between antennas of `L` | `.yy:179-192` |
//! | `L1&&L2` | as `L1&L2`, also the autocorrelations of antennas in both lists | `.yy:202-212` |
//! | `L&&` | as `L&` plus the autocorrelations of `L` | `.yy:213-221` |
//! | `L&&&` | only the autocorrelations of `L` | `.yy:222-230`, `.cc:105-121` |
//! | `/A&B/` (a regex holding `&`) | ordered pairs whose `NAME1&NAME2` the regex matches in full; autocorrelations too | `.ll:83-86`, `.cc:250-277` |
//! | `<x`, `<=x`, `>x`, `>=x`, `a~b` with a length unit | baselines whose ITRF length lies in the interval, autocorrelations (length 0) too | `.yy:453-506`, `.cc:230-248,311-334` |
//! | `!T` | the rows `T` does not select | `.yy:165`, `.cc:91` |
//! | `T1;T2` | left to right: a plain term ORs with what precedes it, a `!` term ANDs | `.cc:92-95` |
//!
//! An antenna list `L` is a comma-separated list of:
//!
//! | Item | Antennas | C++ |
//! |---|---|---|
//! | name, e.g. `ea01` | rows whose NAME equals it; if none, rows whose STATION equals it | `.yy:293-306`, `MSAntennaIndex.cc:126-158` |
//! | integer, e.g. `5` | the first row whose NAME (else STATION) is that text, else antenna ID 5 | `.yy:336-357` |
//! | range `3~7` | each integer as above, its name written in plain decimal | `.yy:358-380` |
//! | pattern, quoted (`"ea0*"`, `'ea{01,02}'`) or bare (`ea0*`, `ea0[1-3]`) | rows whose NAME, else STATION, matches in full; a leading `^` negates | `.ll:46-63,111-116`, `MSAntennaIndex.cc:82-122`, `Regex::fromPattern` |
//! | regex `/ea0[1-3]/`, negated `^/.../` | as a pattern, the text an ECMAScript regex | `.ll:49,77-87` |
//! | `\text` | the name `text`, which may then hold any non-blank characters | `.ll:59,104-109` |
//! | `A@S`, `(A1,A2)@S`, `A@(S1,S2)`, `@S` | antennas of `A` standing at a station of `S` (exact STATION, pattern or regex) | `.yy:249-289,416-438` |
//!
//! Every name, pattern, station or ID that matches nothing is an error,
//! raised once the whole expression has been read (`reportError`,
//! `MSSelection.cc:858-864`), as are scanner and syntax errors, units that
//! are not lengths and regexes that do not compile. The empty string is no
//! constraint (`MSSelection.cc:590`); text of only blanks is a syntax error.
//!
//! # CASA quirks this port keeps
//!
//! - A purely numeric item is a NAME first: with an antenna named `"3"`,
//!   `3` selects that antenna, not ID 3; only its first such row is kept
//!   (`.yy:352-355`). A range looks up `std::ostringstream` decimal names,
//!   so `01~02` looks up `"1"` and `"2"` while a lone `01` looks up `"01"`.
//! - Integers are read with `atoi`: `4294967296` is antenna 0 (glibc keeps
//!   the low 32 bits). A range `a~b` whose length `b - a + 1` is below zero
//!   or overflows `Int` (`0~2147483647`) is an error, as the C++ `Vector`
//!   allocation fails; a range reaching past the last antenna selects the
//!   antennas that exist without complaint.
//! - `!T` also selects every autocorrelation `T` excludes: `!ea01` selects
//!   ea01's own autocorrelation, since `T` masks autocorrelations before
//!   the negation (`.cc:84-91`).
//! - `;` is not commutative: `ea01;!ea02` is (ea01 baselines) AND NOT
//!   (ea02 baselines), but `!ea02;ea01` is (NOT ea02 baselines) OR (ea01
//!   baselines).
//! - A leading `^` inside a regex or pattern is negation, not an anchor:
//!   `/^ea0[12]/` selects every antenna but ea01 and ea02.
//! - Patterns and regexes fall back to STATION per antenna; exact names fall
//!   back to STATION only when no NAME matches at all.
//! - A baseline regex is matched against the ordered string
//!   `NAME(ANTENNA1)&NAME(ANTENNA2)`, so `/ea01&ea02/` selects the rows with
//!   ANTENNA1 = ea01 and ANTENNA2 = ea02 but not the reverse; one that
//!   matches nothing selects nothing silently (`.cc:299-308`).
//! - Length forms: `<x` and `>x` move the bound by 1e-9 m; a range of plain
//!   integers with a unit (`100~200m`) is an antenna-ID range followed by a
//!   stray unit, hence a syntax error: write `100m~200m` or `100.~200.m`.
//! - Concatenated quotes keep their inner quotes (`"ea"'01'` is the
//!   pattern `ea"'01`, `.ll:69-75`).
//!
//! # Not ported
//!
//! - ECMAScript backreferences, lookahead and `\u` escapes in regexes are
//!   refused: the `regex` crate has none of them.
//! - ANTENNA `POSITION` is read as ITRF, as everywhere in casa-ms; CASA
//!   converts other reference frames with `MPosition::Convert`.
//! - The side lists `MSSelection::getAntenna1List`, `getAntenna2List` and
//!   `getBaselineList` are not produced; only the row predicate is.
//! - Messages follow CASA's wording but not its character positions
//!   exactly.

mod grammar;
mod index;
mod lexer;
mod regex;
mod units;

#[cfg(test)]
mod tests;

use crate::error::{MsError, MsResult};
use crate::ms::MeasurementSet;

use self::grammar::{Abort, Report};
use self::index::AntennaIndex;

/// One ANTENNA table row as antenna selection reads it. In a slice of these,
/// row `i` is antenna ID `i`.
///
/// Cf. the NAME, STATION and POSITION columns `MSAntennaIndex` and
/// `MSAntennaParse::getBaselineLengths` read. `FLAG_ROW` plays no part:
/// casacore's checks of it are commented out (`MSAntennaIndex.cc:117`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AntennaRow<'a> {
    /// The NAME column, compared exactly (no trimming, case-sensitive).
    pub name: &'a str,
    /// The STATION column, compared exactly.
    pub station: &'a str,
    /// The POSITION column: ITRF geocentric `[x, y, z]` in metres. Only
    /// baseline-length terms (`<100m`, `1km~2km`) use it.
    pub position_m: [f64; 3],
}

/// A resolved antenna selection: which ordered (`ANTENNA1`, `ANTENNA2`)
/// pairs of MAIN rows CASA's `MSSelection` would keep.
///
/// It holds a dense `(n + 2) × (n + 2)` table of booleans for `n` antennas.
/// The two extra entries on each side stand for antenna IDs that are not
/// ANTENNA rows: CASA's TaQL compares such an ID with no listed antenna, so
/// the answer for one depends only on whether it equals the other side.
/// An empty expression holds no table and selects everything.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BaselineSelection {
    antenna_count: usize,
    /// Row-major over `0..antenna_count + 2`; `None` when unconstrained.
    pairs: Option<Box<[bool]>>,
}

impl BaselineSelection {
    fn unconstrained(antenna_count: usize) -> Self {
        Self {
            antenna_count,
            pairs: None,
        }
    }

    /// The number of ANTENNA rows the expression was resolved against.
    pub fn antenna_count(&self) -> usize {
        self.antenna_count
    }

    /// Whether the expression was empty, so every row is selected.
    pub fn is_unconstrained(&self) -> bool {
        self.pairs.is_none()
    }

    /// Whether a MAIN row with these `ANTENNA1` and `ANTENNA2` values is
    /// selected. The order matters only for baseline-regex terms.
    ///
    /// IDs at or past [`antenna_count`](Self::antenna_count) are answered as
    /// CASA's row predicate answers them: they match no name, station,
    /// pattern or ID, so for example `!ea01` selects them and `ea01` does
    /// not.
    pub fn holds(&self, antenna1: usize, antenna2: usize) -> bool {
        let Some(pairs) = &self.pairs else {
            return true;
        };
        let count = self.antenna_count;
        let row = antenna1.min(count);
        let column = if antenna2 < count {
            antenna2
        } else if antenna2 == antenna1 {
            count
        } else {
            count + 1
        };
        pairs[row * (count + 2) + column]
    }

    /// The heap bytes this selection owns: its pair table, one byte per
    /// entry, `(n + 2)²` for `n` antennas, or nothing when unconstrained.
    pub fn allocated_bytes(&self) -> usize {
        self.pairs.as_ref().map_or(0, |pairs| size_of_val(&**pairs))
    }
}

/// Resolve CASA antenna selection `text` against `antennas`, the ANTENNA
/// table's rows in row order.
///
/// The empty string is no constraint, as `MSSelection` skips an empty
/// antenna expression. See the [module documentation](self) for the
/// expression forms and how each selects baselines.
///
/// # Errors
///
/// [`MsError::InvalidInput`], worded as CASA's
/// `MSSelectionAntennaParseError`, when the text does not scan or parse,
/// when any name, pattern, regex, station or ID in it matches no antenna,
/// when a length unit is unknown or not a distance, or when a regex does
/// not compile or uses an ECMAScript feature the port lacks.
pub fn resolve_antenna_selector(
    antennas: &[AntennaRow<'_>],
    text: &str,
) -> MsResult<BaselineSelection> {
    if text.is_empty() {
        return Ok(BaselineSelection::unconstrained(antennas.len()));
    }
    let index = AntennaIndex::new(antennas);
    let parsed = grammar::parse(&index, text)
        .map_err(|(abort, reports)| failure(text, &reports, Some(abort)))?;
    if !parsed.reports.is_empty() {
        return Err(failure(text, &parsed.reports, None));
    }
    Ok(BaselineSelection {
        antenna_count: antennas.len(),
        pairs: Some(evaluate(&parsed.clauses, antennas.len())),
    })
}

impl MeasurementSet {
    /// Resolve CASA antenna selection `text` against this MeasurementSet's
    /// ANTENNA table (NAME, STATION and POSITION), as
    /// [`resolve_antenna_selector`] does over rows a caller supplies.
    ///
    /// Reads only the ANTENNA subtable; MAIN is not scanned.
    ///
    /// # Errors
    ///
    /// An unreadable ANTENNA table, or any refusal of
    /// [`resolve_antenna_selector`].
    pub fn resolve_antenna_selector(&self, text: &str) -> MsResult<BaselineSelection> {
        let antenna = self.antenna()?;
        let count = antenna.row_count();
        if text.is_empty() {
            return Ok(BaselineSelection::unconstrained(count));
        }
        let mut columns = Vec::with_capacity(count);
        for row in 0..count {
            columns.push((
                antenna.name(row)?,
                antenna.station(row)?,
                antenna.position(row)?,
            ));
        }
        let rows: Vec<AntennaRow<'_>> = columns
            .iter()
            .map(|(name, station, position_m)| AntennaRow {
                name,
                station,
                position_m: *position_m,
            })
            .collect();
        resolve_antenna_selector(&rows, text)
    }
}

/// One term and whether `!` negates it.
struct Clause {
    term: Term,
    negate: bool,
}

/// The row condition of one term, over existing antenna IDs.
enum Term {
    /// `selectAntennaIds(ids, CrossOnly)`: either side in the list.
    Cross(Vec<usize>),
    /// `selectAntennaIds(first, second, type)`: one side in each list.
    Pair {
        first: Vec<usize>,
        second: Vec<usize>,
        autocorrelations: bool,
    },
    /// `selectAntennaIds(ids, AutoCorrOnly)`.
    AutoOnly(Vec<usize>),
    /// `makeBLNode`: an `n × n` row-major table of selected ordered pairs.
    Ordered(Vec<bool>),
}

/// Evaluate the terms over every (`ANTENNA1`, `ANTENNA2`) slot, as
/// `MSAntennaParse::setTEN` (`MSAntennaParse.cc:80-98`) combines them.
fn evaluate(clauses: &[Clause], count: usize) -> Box<[bool]> {
    let side = count + 2;
    let mut selected = vec![false; side * side].into_boxed_slice();
    for (position, clause) in clauses.iter().enumerate() {
        let condition = condition(&clause.term, count);
        for antenna1 in 0..side {
            for antenna2 in 0..side {
                let value = condition(antenna1, antenna2) != clause.negate;
                let cell = &mut selected[antenna1 * side + antenna2];
                *cell = if position == 0 {
                    value
                } else if clause.negate {
                    *cell && value
                } else {
                    *cell || value
                };
            }
        }
    }
    selected
}

/// The row condition of `term` over slots `0..count + 2`; slots from
/// `count` on are IDs no list contains.
fn condition(term: &Term, count: usize) -> Box<dyn Fn(usize, usize) -> bool + '_> {
    let side = count + 2;
    let mask = move |ids: &[usize]| {
        let mut members = vec![false; side];
        for &id in ids {
            members[id] = true;
        }
        members
    };
    match term {
        Term::Cross(ids) => {
            let members = mask(ids);
            Box::new(move |a1, a2| a1 != a2 && (members[a1] || members[a2]))
        }
        Term::Pair {
            first,
            second,
            autocorrelations,
        } => {
            let (first, second) = (mask(first), mask(second));
            let autocorrelations = *autocorrelations;
            Box::new(move |a1, a2| {
                (autocorrelations || a1 != a2)
                    && ((first[a1] && second[a2]) || (second[a1] && first[a2]))
            })
        }
        Term::AutoOnly(ids) => {
            let members = mask(ids);
            Box::new(move |a1, a2| a1 == a2 && members[a1])
        }
        Term::Ordered(table) => {
            Box::new(move |a1, a2| a1 < count && a2 < count && table[a1 * count + a2])
        }
    }
}

/// The `MsError` for a refused expression. "No match" reports win over the
/// error that stopped the parse, as `MSSelection::toTableExprNode` runs
/// the error handler before rethrowing (`MSSelection.cc:759-765`); their
/// text is `MSSelectionErrorHandler::constructMessage`
/// (`MSSelectionErrorHandler.cc:63-77`).
fn failure(text: &str, reports: &[Report], abort: Option<Abort>) -> MsError {
    let message = match (reports.first(), abort) {
        (Some(first), _) => {
            let mut message = first.message.clone();
            for report in reports {
                message.push_str(&report.token);
                message.push(' ');
            }
            message.trim_end().to_string()
        }
        (None, Some(Abort(message))) => message,
        (None, None) => String::from("Antenna Expression: no selection"),
    };
    MsError::InvalidInput(format!("antenna selection {text:?}: {message}"))
}
