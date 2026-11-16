// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's MeasurementSet time selection: the `timerange` expressions of
//! casacore's `MSSelection`, ported from `MSTimeGram.ll`, `MSTimeGram.yy` and
//! `MSTimeParse.cc` at casacore `aaf72eb7`, the commit CASA 6.7.6.14 builds.
//!
//! [`resolve_time_selector`] turns an expression into a [`TimeSelection`]:
//! the conditions on the MAIN `TIME` column that `MSTimeParse` builds as TaQL,
//! with the same double-precision constants. [`TimeSelection::holds`] then
//! tests a row's `TIME` exactly as CASA's TaQL does. `MSSelection` ANDs the
//! time conditions with the conjunction of its other expressions, which is the
//! caller's part.
//!
//! # Forms
//!
//! A `timerange` is one or more comma-separated terms; a row is selected when
//! any term holds (`MSTimeParse::addCondition`, MSTimeParse.cc:188-196). With
//! `t`, `t0`, `t1` the term's times in seconds and `e` the default row's
//! `EXPOSURE` (see [Defaults](#defaults)):
//!
//! | Term | Example | Condition on `TIME` | C++ |
//! |---|---|---|---|
//! | `T` | `2017/03/01/09:00:00.5` | `abs(TIME - t) <= e/2` | MSTimeParse.cc:200-213 |
//! | `>T` | `>09:59:00` | `TIME >= t` (inclusive) | MSTimeParse.cc:217-228 |
//! | `<T` | `<09:00:10` | `TIME <= t` (inclusive) | MSTimeParse.cc:232-244 |
//! | `T0~T1` | `09:00:00~09:01:00` | `TIME >= t0 && TIME <= t1` | MSTimeParse.cc:264-266 |
//! | `T0+dT` | `09:00:00+00:00:10` | as `T0~T1` with `t1 = t0 + dT` | MSTimeGram.yy:164-185 |
//! | `[T0~T1]`, `[T0+dT]` | `[09:00:00~09:01:00]` | `(TIME > t0 \|\| abs(TIME - t0) < w) && (TIME < t1 \|\| abs(TIME - t1) < w)`, `w = e/2` | MSTimeParse.cc:263, 267-270 |
//! | `N[T0~T1]`, `N[T0+dT]` | `3.0[09:00:00~09:01:00]` | as `[T0~T1]` with `w = N` seconds | MSTimeGram.yy:148-151 |
//!
//! The edge width `w` is a C `Float` (`MSTimeEdgeBuffer`, `edgeWidth_l`), so
//! it is rounded to single precision; `N` must be written with a point
//! (`3.0[` or `3.[`), since the grammar takes only an `FNUMBER` there and
//! `3[` is a parse error. `0.0[T0~T1]` is the open interval. A range whose
//! lower bound exceeds its upper bound is refused (MSTimeParse.cc:256-258).
//!
//! Each time `T` is a date and a time of day, either of which may be missing:
//!
//! | Text | Fields | C++ |
//! |---|---|---|
//! | `Y/M/D/h:m:s` | all given | MSTimeGram.yy:215, 228-241 |
//! | `Y/M/D/h:m:` | seconds 0 | MSTimeGram.yy:216 |
//! | `Y/M/D/m:s` | hour 0: one colon is **minutes and seconds**, so `09:30` is 00:09:30 | MSTimeGram.yy:217 |
//! | `Y/M/D/s:` | hour and minute 0: a lone trailing colon is **seconds** | MSTimeGram.yy:218 |
//! | `Y/M/D/s` | a bare number is **seconds**, with the hour and minute defaulted | MSTimeGram.yy:219 |
//! | `Y/M/D` | hour, minute, second and millisecond defaulted | MSTimeGram.yy:242-250 |
//! | any of these without `Y/M/D/` | year, month and day defaulted | MSTimeGram.yy:251-261 |
//! | `*` for a number | that field defaulted (`*` seconds also default the milliseconds) | MSTimeGram.yy:131-133 |
//! | an empty field, as in `/03/01` or `:30` | 0 | MSTimeGram.yy:208, 212 |
//! | `M/D/` (two date fields) | year 0, so refused | MSTimeGram.yy:222 |
//!
//! Seconds keep whole milliseconds, truncated (`splitSec`, MSTimeGram.yy:97-104):
//! `1.001` is the double `1.000999...` and keeps 0 ms, and `2.501` keeps 500.
//! Only the year, month (1 to 12) and day (1 to 31) are checked, with years
//! before 1858 refused (`MSTimeParse::validate`, MSTimeParse.cc:388-411);
//! other values roll over arithmetically, so `2017/02/29` is March 1 and
//! `08:59:60.5` is 09:00:00.5.
//!
//! In `T0~T1`, every field missing from `T1` is copied from `T0` once `T0`
//! has its defaults (`copyDefaults`), so `09:00:00~10:00:00` is on one day and
//! `23:00:00~01:00:00` is refused. In `T0+dT`, `dT` is read as a time with
//! the MJD epoch's defaults (1858/11/17, zero time of day) and its MJD is
//! added to `T0`'s in `MVEpoch` day arithmetic, so `+10` is 10 s, `+1:00` is
//! one minute and `+48:00:00` two days. There are no numeric MJD forms, units
//! or signs: `57813.375`, `10s`, `-` and `2017-03-01` are parse errors.
//! Spaces, tabs and newlines between tokens are ignored, but only the empty
//! string selects everything; a blank expression is a parse error, as in CASA.
//!
//! # Defaults
//!
//! Each term reads its defaults from one MAIN row
//! (`MSTimeParse::getDefaults`, MSTimeParse.cc:114-184): the row's `TIME`,
//! split into year, month and day by `MVTime` and into hour, minute and second
//! by casacore's `Time`, with the milliseconds `(Int)((t0 - t1) * 1E3)`
//! truncated, and the row's `EXPOSURE` (`e`). On `MSSelection`'s path the
//! first term reads the first logical row ([`msselection_first_logical_row`])
//! and every later term reads row 0: `getDefaults` searches only on its first
//! call and otherwise keeps `firstLogicalRow = 0` (MSTimeParse.cc:116-120).
//! [`TimeSelectorDefaults`] carries both rows.
//!
//! # Time scale
//!
//! The expression's times are compared with `TIME` in whatever scale `TIME`
//! holds. `MSTimeParse::toTAIInSec` (MSTimeParse.cc:339-344) despite its name
//! converts nothing (its `MEpoch::Convert` to TAI is commented out), so a UTC
//! `TIME` column is selected by UTC times, with no leap-second offset.
//!
//! # Not ported
//!
//! The grammar keeps the bracket state in process globals that a failed parse
//! inside brackets leaves set, so CASA's next unbracketed range in the same
//! process is edge-inclusive; each parse here starts afresh. A seconds value
//! or a year beyond C `Int` arithmetic, and a default row whose `TIME` is not
//! a finite day number, are undefined behaviour in the C++ and are refused.
//!
//! # Evidence
//!
//! `resources/casa-compat/evidence/msselection-time-casa-20261102.json` holds
//! the rows CASA 6.7.6.14's `ms.msselect` keeps for 1528 expressions (curated
//! forms, seeded random ones, first-logical-row and later-term defaults, and
//! defaults rebuilt from assorted sub-second row times), written by
//! `tools/casa-compat/time_selection_oracle.py`; the tests replay every one.

mod clock;
mod grammar;

use self::clock::{RowDefaults, epoch_days, epoch_seconds, fill, time_of};
use self::grammar::{Edge, RangeJoin, TermSyntax, TimeFields};
use crate::{MsError, MsResult};

/// The `TIME` and `EXPOSURE` of the MAIN row a time expression takes its
/// defaults from.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TimeDefaults {
    /// The row's `TIME`: seconds since MJD 0 in the column's own time scale
    /// (normally UTC), as the MeasurementSet stores it with unit `s`.
    pub time_mjd_s: f64,
    /// The row's `EXPOSURE` in seconds. Half of it is a single time's
    /// tolerance and a bracketed range's edge width.
    pub exposure_s: f64,
}

/// The rows a whole time expression takes its defaults from.
///
/// CASA reads the first comma-separated term's defaults from the first
/// logical row and every later term's from row 0 (MSTimeParse.cc:116-120),
/// a CASA quirk this port keeps. When both are the same row,
/// `TimeSelectorDefaults::from(defaults)` fills both.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TimeSelectorDefaults {
    /// The first logical row's values, for the first term; see
    /// [`msselection_first_logical_row`].
    pub first_term: TimeDefaults,
    /// Row 0's values, for every later term.
    pub later_terms: TimeDefaults,
}

impl From<TimeDefaults> for TimeSelectorDefaults {
    fn from(defaults: TimeDefaults) -> Self {
        Self {
            first_term: defaults,
            later_terms: defaults,
        }
    }
}

/// The predicate `MSSelection` uses to find the first logical row.
///
/// `MSSelection::toTableExprNode` parses the time expression last, passing
/// the conjunction of every other expression as `otherTens`, and constructs
/// `MSTimeParse` with `honourRowFlags` at its default of true
/// (MSSelection.cc:729-744, MSTimeGram.cc:112-118, MSTimeParse.h:97-100).
pub enum LogicalRowFilter<'a> {
    /// No other expression is set (`otherTens` is null): the closure returns
    /// a row's `FLAG_ROW`, and the first row not flagged is the first logical
    /// row (MSTimeParse.cc:136-146).
    FlagRow(&'a mut dyn FnMut(usize) -> bool),
    /// Another expression is set: the closure returns whether the other
    /// expressions together select a row, and the first such row is the first
    /// logical row. `FLAG_ROW` is not consulted here, since both branches of
    /// CASA's flag test read the same condition (MSTimeParse.cc:122-135).
    OtherSelections(&'a mut dyn FnMut(usize) -> bool),
}

/// The first logical row `MSSelection` reads time defaults from, among
/// `row_count` MAIN rows.
///
/// It is the first row `filter` accepts, or row 0 when it accepts none.
/// [`TimeSelectorDefaults::first_term`] takes that row's `TIME` and
/// `EXPOSURE`, and [`TimeSelectorDefaults::later_terms`] row 0's. Find it only
/// for a non-empty expression: CASA reads no defaults for an empty one.
///
/// # Errors
///
/// An empty MAIN table has no logical row zero (MSTimeParse.cc:147-149).
pub fn msselection_first_logical_row(
    row_count: usize,
    filter: LogicalRowFilter<'_>,
) -> MsResult<usize> {
    if row_count == 0 {
        return Err(MsError::InvalidInput(
            "MSTimeParse: No logical \"row zero\" found for time selection".to_string(),
        ));
    }
    let found = match filter {
        LogicalRowFilter::FlagRow(flag_row) => (0..row_count).find(|&row| !flag_row(row)),
        LogicalRowFilter::OtherSelections(selected) => (0..row_count).find(|&row| selected(row)),
    };
    Ok(found.unwrap_or(0))
}

/// One condition on a row's `TIME`, as `MSTimeParse` builds it in TaQL.
/// Every time is in seconds in the `TIME` column's scale.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TimeCondition {
    /// `abs(TIME - time_s) <= tolerance_s`, a single time with half the
    /// default exposure (MSTimeParse.cc:200-213).
    Near {
        /// The time.
        time_s: f64,
        /// Half the default row's `EXPOSURE`.
        tolerance_s: f64,
    },
    /// `TIME >= time_s`, from `>T` (MSTimeParse.cc:217-228).
    AtOrAfter {
        /// The bound.
        time_s: f64,
    },
    /// `TIME <= time_s`, from `<T` (MSTimeParse.cc:232-244).
    AtOrBefore {
        /// The bound.
        time_s: f64,
    },
    /// `TIME >= lower_s && TIME <= upper_s`, from `T0~T1` or `T0+dT`
    /// (MSTimeParse.cc:264-266).
    Within {
        /// The lower bound.
        lower_s: f64,
        /// The upper bound.
        upper_s: f64,
    },
    /// `(TIME > lower_s || abs(TIME - lower_s) < edge_s) && (TIME < upper_s
    /// || abs(TIME - upper_s) < edge_s)`, from a bracketed range
    /// (MSTimeParse.cc:267-270).
    EdgeInclusive {
        /// The lower bound.
        lower_s: f64,
        /// The upper bound.
        upper_s: f64,
        /// The edge width, a C `Float`: half the default exposure, or `N`.
        edge_s: f32,
    },
}

impl TimeCondition {
    /// Whether a row whose `TIME` is `time_s` meets the condition, with the
    /// same double-precision operations as CASA's TaQL.
    #[must_use]
    pub fn holds(&self, time_s: f64) -> bool {
        match *self {
            Self::Near {
                time_s: centre,
                tolerance_s,
            } => (time_s - centre).abs() <= tolerance_s,
            Self::AtOrAfter { time_s: bound } => time_s >= bound,
            Self::AtOrBefore { time_s: bound } => time_s <= bound,
            Self::Within { lower_s, upper_s } => time_s >= lower_s && time_s <= upper_s,
            Self::EdgeInclusive {
                lower_s,
                upper_s,
                edge_s,
            } => {
                let edge = f64::from(edge_s);
                (time_s > lower_s || (time_s - lower_s).abs() < edge)
                    && (time_s < upper_s || (time_s - upper_s).abs() < edge)
            }
        }
    }
}

/// One column of `MSSelection::getTimeList()`: the interval a term records
/// (`MSTimeParse::accumulateTimeList`, MSTimeParse.cc:428-439).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TimeListEntry {
    /// The start: the time, the lower bound, or 0 for `<T`.
    pub start_s: f64,
    /// The end: the time, the upper bound, or `f64::MAX` for `>T`.
    pub end_s: f64,
    /// Half the exposure for a single time, the edge width for a range (0
    /// unbracketed), and the default `EXPOSURE` for `>T` and `<T`.
    pub width_s: f64,
}

/// One resolved comma-separated term.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TimeTerm {
    /// The condition the term puts on `TIME`.
    pub condition: TimeCondition,
    /// The interval the term adds to `MSSelection::getTimeList()`.
    pub time_list_entry: TimeListEntry,
}

/// A resolved `timerange`: the OR of its terms, or every row when the
/// expression was empty.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TimeSelection {
    terms: Box<[TimeTerm]>,
}

impl TimeSelection {
    /// The selection of an empty expression, which puts no condition on `TIME`.
    #[must_use]
    pub fn all() -> Self {
        Self::default()
    }

    /// Whether the selection puts no condition on `TIME`.
    #[must_use]
    pub fn selects_all(&self) -> bool {
        self.terms.is_empty()
    }

    /// Whether a row whose `TIME` is `time_s` is selected: any term holds,
    /// or the expression was empty.
    #[must_use]
    pub fn holds(&self, time_s: f64) -> bool {
        self.terms.is_empty() || self.terms.iter().any(|term| term.condition.holds(time_s))
    }

    /// The resolved terms, in the expression's order.
    #[must_use]
    pub fn terms(&self) -> &[TimeTerm] {
        &self.terms
    }

    /// `MSSelection::getTimeList()`, one entry per term.
    pub fn time_list(&self) -> impl ExactSizeIterator<Item = TimeListEntry> + '_ {
        self.terms.iter().map(|term| term.time_list_entry)
    }

    /// Heap bytes the selection holds: exactly one [`TimeTerm`] per term, and
    /// nothing for an empty expression.
    #[must_use]
    pub fn allocated_bytes(&self) -> usize {
        std::mem::size_of_val::<[TimeTerm]>(&self.terms)
    }
}

/// A parsed `timerange` expression whose defaults are not yet applied.
///
/// Parsing needs no MeasurementSet, so a caller can refuse a malformed
/// expression, and skip finding the default rows for an empty one, before it
/// reads any rows. Errors CASA finds in a term's actions (a bad date, a
/// reversed range) appear at [`resolve`](Self::resolve); where an expression
/// has both, CASA may report whichever its parser reaches first.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TimeSelector {
    terms: Box<[TermSyntax]>,
}

impl TimeSelector {
    /// Parse `text`. The empty string selects every row; any other text,
    /// blank text included, must be a valid expression.
    ///
    /// # Errors
    ///
    /// A parse error, with CASA's message naming the token and position.
    pub fn parse(text: &str) -> MsResult<Self> {
        if text.is_empty() {
            return Ok(Self::default());
        }
        Ok(Self {
            terms: grammar::parse(text)?.into_boxed_slice(),
        })
    }

    /// Whether the expression was empty, which selects every row and reads
    /// no defaults.
    #[must_use]
    pub fn selects_all(&self) -> bool {
        self.terms.is_empty()
    }

    /// The number of comma-separated terms.
    #[must_use]
    pub fn term_count(&self) -> usize {
        self.terms.len()
    }

    /// Apply the defaults and resolve each term's condition.
    ///
    /// # Errors
    ///
    /// A year before 1858, a month outside 1 to 12 or a day outside 1 to 31
    /// (CASA's `MSTime Selection error`), a range whose lower bound exceeds
    /// its upper bound, or a default row or field beyond the arithmetic CASA
    /// defines.
    pub fn resolve(&self, defaults: impl Into<TimeSelectorDefaults>) -> MsResult<TimeSelection> {
        let defaults = defaults.into();
        let terms = self
            .terms
            .iter()
            .enumerate()
            .map(|(index, syntax)| {
                let row = if index == 0 {
                    defaults.first_term
                } else {
                    defaults.later_terms
                };
                let row = clock::row_defaults(row.time_mjd_s, row.exposure_s).ok_or_else(|| {
                    MsError::InvalidInput(format!(
                        "MSTimeParse: default row TIME {} s is outside the days casacore's \
                         Time converts",
                        row.time_mjd_s
                    ))
                })?;
                resolve_term(*syntax, &row, index + 1)
            })
            .collect::<MsResult<Box<[TimeTerm]>>>()?;
        Ok(TimeSelection { terms })
    }
}

/// Parse and resolve a `timerange` expression; see the [module
/// documentation](self) for the forms and defaults.
///
/// Pass a [`TimeDefaults`] when the first logical row is row 0, and a
/// [`TimeSelectorDefaults`] otherwise.
///
/// # Errors
///
/// As [`TimeSelector::parse`] and [`TimeSelector::resolve`].
pub fn resolve_time_selector(
    text: &str,
    defaults: impl Into<TimeSelectorDefaults>,
) -> MsResult<TimeSelection> {
    TimeSelector::parse(text)?.resolve(defaults)
}

/// The grammar action of one term with its default row (MSTimeGram.yy:137-205).
fn resolve_term(syntax: TermSyntax, row: &RowDefaults, number: usize) -> MsResult<TimeTerm> {
    let exposure_s = row.exposure_s;
    let (condition, time_list_entry) = match syntax {
        TermSyntax::Single(fields) => {
            let time_s = time_seconds(clock::fill_from_row(fields, row))?;
            let tolerance_s = exposure_s / 2.0;
            (
                TimeCondition::Near {
                    time_s,
                    tolerance_s,
                },
                listed(time_s, time_s, tolerance_s, exposure_s),
            )
        }
        TermSyntax::AtOrAfter(fields) => {
            let time_s = time_seconds(clock::fill_from_row(fields, row))?;
            (
                TimeCondition::AtOrAfter { time_s },
                listed(time_s, f64::MAX, -1.0, exposure_s),
            )
        }
        TermSyntax::AtOrBefore(fields) => {
            let time_s = time_seconds(clock::fill_from_row(fields, row))?;
            (
                TimeCondition::AtOrBefore { time_s },
                listed(0.0, time_s, -1.0, exposure_s),
            )
        }
        TermSyntax::Range {
            lower,
            join,
            upper,
            edge,
        } => resolve_range(lower, join, upper, edge, row, number)?,
    };
    Ok(TimeTerm {
        condition,
        time_list_entry,
    })
}

/// `rangetimeexpr` and `MSTimeParse::selectTimeRange` (MSTimeGram.yy:153-185,
/// MSTimeParse.cc:248-274).
fn resolve_range(
    lower: TimeFields,
    join: RangeJoin,
    upper: TimeFields,
    edge: Edge,
    row: &RowDefaults,
    number: usize,
) -> MsResult<(TimeCondition, TimeListEntry)> {
    let lower = clock::fill_from_row(lower, row);
    let (lower_s, upper_s) = match join {
        RangeJoin::Span => {
            let upper = fill(upper, lower);
            (time_seconds(lower)?, time_seconds(upper)?)
        }
        RangeJoin::Duration => {
            let duration = clock::fill_from_mjd_reference(upper);
            validate(lower)?;
            validate(duration)?;
            let start_days = date_time(lower)?.modified_julian_day();
            let duration_days = date_time(duration)?.modified_julian_day();
            (
                epoch_seconds(epoch_days(start_days, 0.0)),
                epoch_seconds(epoch_days(start_days, duration_days)),
            )
        }
    };
    if lower_s > upper_s {
        return Err(MsError::InvalidInput(format!(
            "MSSelection time error: lower bound > upper bound in time selection term {number}"
        )));
    }
    // `Float edgeWidth_l = (edgeWidth < 0.0) ? (edgeInclusive ?
    // defaultExposure/2.0 : 0.0) : edgeWidth`.
    #[expect(clippy::cast_possible_truncation, reason = "edgeWidth_l is a C Float")]
    let (condition, width) = match edge {
        Edge::Closed => (TimeCondition::Within { lower_s, upper_s }, 0.0_f32),
        Edge::Exposure | Edge::Width(_) => {
            let edge_s = match edge {
                Edge::Width(width) => width,
                _ => (row.exposure_s / 2.0) as f32,
            };
            (
                TimeCondition::EdgeInclusive {
                    lower_s,
                    upper_s,
                    edge_s,
                },
                edge_s,
            )
        }
    };
    Ok((
        condition,
        listed(lower_s, upper_s, f64::from(width), row.exposure_s),
    ))
}

/// `accumulateTimeList`: a negative width records the default exposure.
fn listed(start_s: f64, end_s: f64, width_s: f64, exposure_s: f64) -> TimeListEntry {
    TimeListEntry {
        start_s,
        end_s,
        width_s: if width_s >= 0.0 { width_s } else { exposure_s },
    }
}

/// `MSTimeParse::yearTimeConvert` then `toTAIInSec` (MSTimeParse.cc:317-344).
fn time_seconds(fields: TimeFields) -> MsResult<f64> {
    validate(fields)?;
    let days = date_time(fields)?.modified_julian_day();
    Ok(epoch_seconds(epoch_days(days, 0.0)))
}

fn date_time(fields: TimeFields) -> MsResult<clock::CasacoreTime> {
    time_of(fields).ok_or_else(|| {
        MsError::InvalidInput(format!(
            "MSTime Selection error: Year = {} is beyond the C int day arithmetic \
             of casacore's Time",
            fields.year
        ))
    })
}

/// `MSTimeParse::validate` (MSTimeParse.cc:388-411).
fn validate(fields: TimeFields) -> MsResult<()> {
    let refusal = if fields.year < 1858 {
        format!("Year = {}", fields.year)
    } else if fields.month <= 0 || fields.month > 12 {
        format!("Month = {}", fields.month)
    } else if fields.day <= 0 || fields.day > 31 {
        format!("Day = {}", fields.day)
    } else {
        return Ok(());
    };
    Err(MsError::InvalidInput(format!(
        "MSTime Selection error: {refusal} out of range"
    )))
}

#[cfg(test)]
mod casa_oracle;
#[cfg(test)]
mod tests;
