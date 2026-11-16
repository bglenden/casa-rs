// SPDX-License-Identifier: LGPL-3.0-or-later
//! The antenna expression parser, a port of `ms/MSSel/MSAntennaGram.yy`.
//!
//! The bison grammar is LALR(1); this recursive-descent parser accepts the
//! same language. The one place the C++ needs its lookahead to choose is an
//! `INT` opening a term: followed by a `UNIT` it begins a baseline length
//! (`flintunit`), otherwise an antenna ID (`antidrange`). Semantic actions run
//! as the bison actions do: name lookups report "no match" to the error
//! handler and carry on (`reportError`, `MSAntennaGram.yy:99-121`), while
//! scanner, syntax, unit and regex errors stop the parse at once.

use super::index::{AntennaIndex, c_atof, c_atoi};
use super::lexer::{Kind, LexError, Lexer, Token};
use super::units::{UnitError, unit_factor};
use super::{Clause, Term};

/// One `MSSelectionErrorHandler::reportError` entry
/// (`MSSelectionErrorHandler.cc:57-61`).
pub(super) struct Report {
    pub(super) token: String,
    pub(super) message: String,
}

/// An error that ends the parse at once, already worded as CASA words it.
pub(super) struct Abort(pub(super) String);

/// The parsed expression: its terms, and the "no match" reports CASA turns
/// into an error once the parse has finished (`MSSelection.cc:858-864`).
pub(super) struct Parsed {
    pub(super) clauses: Vec<Clause>,
    pub(super) reports: Vec<Report>,
}

/// Antenna IDs as the grammar's `Vector<Int>` values, kept only where they
/// name an existing antenna (`matchId` and `set_intersection` drop the
/// rest, so membership is all that later rules see), plus their source
/// text for messages.
struct IdList<'s> {
    ids: Vec<usize>,
    shown: Vec<&'s str>,
}

impl<'s> IdList<'s> {
    fn new(ids: Vec<usize>, shown: &'s str) -> Self {
        Self {
            ids,
            shown: vec![shown],
        }
    }

    fn extend(&mut self, other: Self) {
        self.ids.extend(other.ids);
        self.shown.extend(other.shown);
    }

    fn describe(&self) -> String {
        format!("[{}]", self.shown.join(", "))
    }
}

/// Parse `text` against `index` (`baseMSAntennaGramParseCommand`,
/// `MSAntennaGram.cc:74-101`).
pub(super) fn parse(
    index: &AntennaIndex<'_, '_>,
    text: &str,
) -> Result<Parsed, (Abort, Vec<Report>)> {
    let mut parser = Parser {
        lexer: Lexer::new(text),
        lookahead: Vec::new(),
        index,
        text,
        negate: false,
        clauses: Vec::new(),
        reports: Vec::new(),
    };
    match parser.statement() {
        Ok(()) => Ok(Parsed {
            clauses: parser.clauses,
            reports: parser.reports,
        }),
        Err(abort) => Err((abort, parser.reports)),
    }
}

struct Parser<'s, 'i, 'r, 'a> {
    lexer: Lexer<'s>,
    /// Scanned tokens not yet consumed, next first.
    lookahead: Vec<Token<'s>>,
    index: &'i AntennaIndex<'r, 'a>,
    text: &'s str,
    /// `MSAntennaGramNegate`: whether the current term follows `!`.
    negate: bool,
    clauses: Vec<Clause>,
    reports: Vec<Report>,
}

type Step<T> = Result<T, Abort>;

impl<'s> Parser<'s, '_, '_, '_> {
    // ---- token stream -------------------------------------------------

    fn peek_at(&mut self, depth: usize) -> Step<Token<'s>> {
        while self.lookahead.len() <= depth {
            let token = self
                .lexer
                .next_token()
                .map_err(|error| self.lex_abort(error))?;
            self.lookahead.push(token);
        }
        Ok(self.lookahead[depth])
    }

    fn peek(&mut self) -> Step<Kind> {
        Ok(self.peek_at(0)?.kind)
    }

    fn advance(&mut self) -> Step<Token<'s>> {
        let token = self.peek_at(0)?;
        self.lookahead.remove(0);
        Ok(token)
    }

    fn expect(&mut self, kind: Kind) -> Step<Token<'s>> {
        if self.peek()? == kind {
            self.advance()
        } else {
            Err(self.syntax_error())
        }
    }

    // ---- errors -------------------------------------------------------

    /// `MSAntennaGramerror` (`MSAntennaGram.cc:253-257`) at the lookahead,
    /// with the position note the catch in `MSAntennaGram.cc:94-100` adds.
    fn syntax_error(&self) -> Abort {
        let near = self.lookahead.first().map_or("", |token| token.lexeme);
        self.antenna_error(&format!(
            "Antenna Expression: Parse error at or near '{near}'"
        ))
    }

    fn lex_abort(&self, error: LexError) -> Abort {
        self.antenna_error(error.message())
    }

    /// An `MSSelectionAntennaError` caught by `baseMSAntennaGramParseCommand`,
    /// which appends `constructMessage` (`MSSelectionError.cc:53-64`).
    fn antenna_error(&self, message: &str) -> Abort {
        let position = self.lexer.position();
        let mut text = format!(
            "{message} (near char. {position} in string \"{}\")",
            self.text
        );
        if position > 0 && self.text.as_bytes().get(position - 1) == Some(&b'-') {
            text.push_str(
                " [TIP: Did you know we use \"~\" as the range operator (for a good reason)?]",
            );
        }
        Abort(text)
    }

    /// `reportError` (`MSAntennaGram.yy:99-121`).
    fn report_no_match(&mut self, token: &str, source: &str) {
        let negation = if self.negate { "!" } else { "" };
        self.reports.push(Report {
            token: format!("\"{negation}{token}\""),
            message: format!("{source}: No match found for token(s) "),
        });
    }

    // ---- statements and terms ------------------------------------------

    /// `antennastatement` / `indexcombexpr`: terms separated by `;`.
    fn statement(&mut self) -> Step<()> {
        loop {
            self.term()?;
            match self.peek()? {
                Kind::Semicolon => {
                    self.advance()?;
                }
                Kind::End => return Ok(()),
                _ => return Err(self.syntax_error()),
            }
        }
    }

    /// `gbaseline` and `baseline` (`MSAntennaGram.yy:165-242`).
    fn term(&mut self) -> Step<()> {
        self.negate = self.peek()? == Kind::Not;
        if self.negate {
            self.advance()?;
        }
        let starts_length = match self.peek()? {
            Kind::Lt | Kind::Le | Kind::Gt | Kind::Ge | Kind::Float => true,
            Kind::Int => self.peek_at(1)?.kind == Kind::Unit,
            _ => false,
        };
        let term = if self.peek()? == Kind::BlRegex {
            self.baseline_regexes()?
        } else if starts_length {
            self.baseline_lengths()?
        } else {
            self.antenna_term()?
        };
        self.clauses.push(Clause {
            term,
            negate: self.negate,
        });
        Ok(())
    }

    /// The `antlist` forms of `baseline` (`MSAntennaGram.yy:168-230`).
    fn antenna_term(&mut self) -> Step<Term> {
        let first = self.antenna_list()?;
        if self.peek()? != Kind::Ampersand {
            return Ok(Term::Cross(self.match_id(first)));
        }
        self.advance()?;
        if self.peek()? == Kind::Ampersand {
            self.advance()?;
            if self.peek()? == Kind::Ampersand {
                self.advance()?;
                return Ok(Term::AutoOnly(self.match_id(first)));
            }
            return self.pair(first, true);
        }
        self.pair(first, false)
    }

    /// `antlist & antlist`, `antlist &`, `antlist && antlist`, `antlist &&`.
    /// Without a second list the first stands on both sides.
    fn pair(&mut self, first: IdList<'s>, autocorrelations: bool) -> Step<Term> {
        let second = if starts_antenna_ids(self.peek()?) {
            Some(self.antenna_list()?)
        } else {
            None
        };
        // The action runs once both lists are parsed: matchId(a1), then a2.
        let first = self.match_id(first);
        let second = match second {
            Some(second) => self.match_id(second),
            None => first.clone(),
        };
        Ok(Term::Pair {
            first,
            second,
            autocorrelations,
        })
    }

    /// `MSAntennaIndex::matchId` (`MSAntennaIndex.cc:62-78`): the listed
    /// IDs that exist; none is reported.
    fn match_id(&mut self, list: IdList<'s>) -> Vec<usize> {
        if list.ids.is_empty() {
            self.reports.push(Report {
                token: String::new(),
                message: format!(
                    // CASA's wording, typo included.
                    "No match found for the antenna specificion [ID(s): {}]",
                    list.describe()
                ),
            });
        }
        list.ids
    }

    // ---- antenna lists ---------------------------------------------------

    /// `antlist`: `antids` separated by commas.
    fn antenna_list(&mut self) -> Step<IdList<'s>> {
        let mut list = self.antenna_ids()?;
        while self.peek()? == Kind::Comma {
            self.advance()?;
            let next = self.antenna_ids()?;
            list.extend(next);
        }
        Ok(list)
    }

    /// `antids`: `antid`, `antidrange` or `antatstation`.
    fn antenna_ids(&mut self) -> Step<IdList<'s>> {
        let antennas = match self.peek()? {
            Kind::At => {
                self.advance()?;
                let stations = self.station_component()?;
                if stations.ids.is_empty() {
                    let token = format!("@StationID({})", stations.describe());
                    self.report_no_match(&token, "Station Expression");
                }
                return Ok(stations);
            }
            Kind::LParen => {
                self.advance()?;
                let list = self.antenna_list()?;
                self.expect(Kind::RParen)?;
                if self.peek()? != Kind::At {
                    return Err(self.syntax_error());
                }
                list
            }
            Kind::Identifier | Kind::Unit | Kind::QString | Kind::Regex => self.antenna_id()?,
            Kind::Int => self.antenna_id_range()?,
            _ => return Err(self.syntax_error()),
        };
        if self.peek()? != Kind::At {
            return Ok(antennas);
        }
        self.advance()?;
        let stations = self.station_component()?;
        Ok(self.antenna_at_station(antennas, &stations))
    }

    /// `antcomp AT stationcomp` (`MSAntennaGram.yy:424-431`).
    fn antenna_at_station(&mut self, antennas: IdList<'s>, stations: &IdList<'s>) -> IdList<'s> {
        let ids: Vec<usize> = antennas
            .ids
            .iter()
            .copied()
            .filter(|id| stations.ids.contains(id))
            .collect();
        if ids.is_empty() {
            let token = format!(
                "AntID({})@StationID({})",
                antennas.describe(),
                stations.describe()
            );
            self.report_no_match(&token, "Ant@Station Expression");
        }
        IdList {
            ids,
            shown: antennas.shown,
        }
    }

    /// `antid` (`MSAntennaGram.yy:293-334`).
    fn antenna_id(&mut self) -> Step<IdList<'s>> {
        let token = self.advance()?;
        let ids = match token.kind {
            Kind::Identifier | Kind::Unit => self.index.match_antenna_name(token.value),
            _ => {
                let (negated, text) = token.negation();
                let is_regex = token.kind == Kind::Regex;
                self.index
                    .match_antenna_regex_or_pattern(negated, text, is_regex)
                    .map_err(Abort)?
            }
        };
        if ids.is_empty() {
            self.report_no_match(&token.shown(), "Antenna Expression");
        }
        Ok(IdList::new(ids, token.value))
    }

    /// `antidrange` (`MSAntennaGram.yy:336-380`).
    fn antenna_id_range(&mut self) -> Step<IdList<'s>> {
        let first = self.advance()?;
        if self.peek()? != Kind::Dash {
            // An integer is first a NAME (VLA antennas were named "1",
            // "2", ...), and only then an ID.
            let ids = match self.index.first_antenna_name(first.value) {
                Some(id) => vec![id],
                None => existing(self.index, i64::from(c_atoi(first.value))),
            };
            return Ok(IdList::new(ids, first.value));
        }
        self.advance()?;
        let last = self.expect(Kind::Int)?;
        let shown = &self.text[offset_in(self.text, first.lexeme)
            ..offset_in(self.text, last.lexeme) + last.lexeme.len()];
        let (start, end) = (c_atoi(first.value), c_atoi(last.value));
        // `Int len = end - start + 1` sizes a Vector<Int>: a length below
        // one, including one that overflows Int, allocates nothing usable.
        let length = i64::from(end) - i64::from(start) + 1;
        if length < 0 || length > i64::from(i32::MAX) {
            return Err(Abort(format!(
                "Antenna Expression: the range {shown} has length {length}, which CASA cannot \
                 allocate (Int len = end - start + 1, MSAntennaGram.yy:362)"
            )));
        }
        let ids = if length == 0 {
            Vec::new()
        } else {
            self.index.integer_range(start, end)
        };
        Ok(IdList::new(ids, shown))
    }

    // ---- stations --------------------------------------------------------

    /// `stationcomp`: one `stationid` or a parenthesised `stationlist`.
    fn station_component(&mut self) -> Step<IdList<'s>> {
        if self.peek()? != Kind::LParen {
            return self.station_id();
        }
        self.advance()?;
        let mut list = self.station_id()?;
        while self.peek()? == Kind::Comma {
            self.advance()?;
            let next = self.station_id()?;
            list.extend(next);
        }
        self.expect(Kind::RParen)?;
        Ok(list)
    }

    /// `stationid` (`MSAntennaGram.yy:249-289`).
    fn station_id(&mut self) -> Step<IdList<'s>> {
        let token = self.advance()?;
        let ids = match token.kind {
            Kind::Identifier | Kind::Unit => self.index.match_station_name(token.value),
            Kind::QString | Kind::Regex => {
                let (negated, text) = token.negation();
                let is_regex = token.kind == Kind::Regex;
                self.index
                    .match_station_regex_or_pattern(negated, text, is_regex)
                    .map_err(Abort)?
            }
            _ => {
                self.lookahead.insert(0, token);
                return Err(self.syntax_error());
            }
        };
        if ids.is_empty() {
            self.report_no_match(&token.shown(), "Station Expression");
        }
        Ok(IdList::new(ids, token.value))
    }

    // ---- baseline regexes and lengths ------------------------------------

    /// `blregexlist` (`MSAntennaGram.yy:440-451`) and `selectBLRegex`
    /// (`MSAntennaParse.cc:250-277`): ordered `NAME1&NAME2` strings matched
    /// in full; a regex with no match selects nothing and is not reported.
    fn baseline_regexes(&mut self) -> Step<Term> {
        let rows = self.index.rows();
        let count = rows.len();
        let mut matched = vec![false; count * count];
        loop {
            let token = self.expect(Kind::BlRegex)?;
            let (negated, text) = token.negation();
            let matcher = super::regex::FullMatch::compile(text).map_err(Abort)?;
            let mut baseline = String::new();
            for (antenna1, row1) in rows.iter().enumerate() {
                for (antenna2, row2) in rows.iter().enumerate() {
                    baseline.clear();
                    baseline.push_str(row1.name);
                    baseline.push('&');
                    baseline.push_str(row2.name);
                    if matcher.matches(&baseline) != negated {
                        matched[antenna1 * count + antenna2] = true;
                    }
                }
            }
            if self.peek()? != Kind::Comma {
                return Ok(Term::Ordered(matched));
            }
            self.advance()?;
        }
    }

    /// `blengthlist` (`MSAntennaGram.yy:453-463`) and `selectLength`
    /// (`MSAntennaParse.cc:230-248`): baselines whose length lies in any of
    /// the closed intervals, autocorrelations (length 0) included.
    fn baseline_lengths(&mut self) -> Step<Term> {
        let mut intervals = Vec::new();
        loop {
            intervals.push(self.baseline_length()?);
            if self.peek()? != Kind::Comma {
                break;
            }
            self.advance()?;
        }
        let rows = self.index.rows();
        let count = rows.len();
        let mut matched = vec![false; count * count];
        for (antenna1, row1) in rows.iter().enumerate() {
            for (antenna2, row2) in rows.iter().enumerate() {
                let length = baseline_length(row1.position_m, row2.position_m);
                matched[antenna1 * count + antenna2] = intervals
                    .iter()
                    .any(|(low, high)| length >= *low && length <= *high);
            }
        }
        Ok(Term::Ordered(matched))
    }

    /// `blength` (`MSAntennaGram.yy:465-506`): one interval in metres.
    fn baseline_length(&mut self) -> Step<(f64, f64)> {
        const OPEN: f64 = 1e30;
        const STRICT: f64 = 0.000_000_001;
        let comparison = self.peek()?;
        if matches!(comparison, Kind::Lt | Kind::Le | Kind::Gt | Kind::Ge) {
            self.advance()?;
            let value = self.number_with_optional_unit()?;
            return Ok(match comparison {
                Kind::Lt => (-OPEN, value - STRICT),
                Kind::Le => (-OPEN, value),
                Kind::Gt => (value + STRICT, OPEN),
                _ => (value, OPEN),
            });
        }
        if self.peek()? == Kind::Float && self.peek_at(1)?.kind != Kind::Unit {
            // `flt DASH flt` and `flt DASH flt unit`.
            let low = c_atof(self.advance()?.value);
            self.expect(Kind::Dash)?;
            let high = c_atof(self.expect(Kind::Float)?.value);
            if self.peek()? != Kind::Unit {
                return Ok((low, high));
            }
            let factor = self.unit()?;
            return Ok((low / factor, high / factor));
        }
        // `flintunit DASH flintunit`.
        let low = self.number_with_unit()?;
        self.expect(Kind::Dash)?;
        let high = self.number_with_unit()?;
        Ok((low, high))
    }

    /// `flunit`: `flint`, or `flint unit` converted to metres.
    fn number_with_optional_unit(&mut self) -> Step<f64> {
        let value = self.number()?;
        if self.peek()? != Kind::Unit {
            return Ok(value);
        }
        let factor = self.unit()?;
        Ok(value / factor)
    }

    /// `flintunit`: `flint unit` in metres (`$1/$2`).
    fn number_with_unit(&mut self) -> Step<f64> {
        let value = self.number()?;
        if self.peek()? != Kind::Unit {
            return Err(self.syntax_error());
        }
        let factor = self.unit()?;
        Ok(value / factor)
    }

    /// `flint`: `atof` of a FLOAT or `atoi` of an INT.
    fn number(&mut self) -> Step<f64> {
        match self.peek()? {
            Kind::Float => Ok(c_atof(self.advance()?.value)),
            Kind::Int => Ok(f64::from(c_atoi(self.advance()?.value))),
            _ => Err(self.syntax_error()),
        }
    }

    /// `unit`: `MSAntennaParse::getUnitFactor`.
    fn unit(&mut self) -> Step<f64> {
        let token = self.expect(Kind::Unit)?;
        unit_factor(token.value).map_err(|error| match error {
            // casacore's AipsError passes the antenna catch untouched.
            UnitError::Unknown => Abort(error.message(token.value)),
            UnitError::NotDistance => self.antenna_error(&error.message(token.value)),
        })
    }
}

/// Tokens that can start an `antids`.
fn starts_antenna_ids(kind: Kind) -> bool {
    matches!(
        kind,
        Kind::Identifier
            | Kind::Unit
            | Kind::QString
            | Kind::Regex
            | Kind::Int
            | Kind::At
            | Kind::LParen
    )
}

/// `[id]` when `id` is an existing antenna ID, else nothing.
fn existing(index: &AntennaIndex<'_, '_>, id: i64) -> Vec<usize> {
    usize::try_from(id)
        .ok()
        .filter(|id| *id < index.len())
        .into_iter()
        .collect()
}

/// The byte offset of `part`, a slice of `text`, within it.
fn offset_in(text: &str, part: &str) -> usize {
    part.as_ptr() as usize - text.as_ptr() as usize
}

/// `getBaselineLengths` (`MSAntennaParse.cc:311-334`): the Euclidean
/// distance between ITRF positions, summed in axis order as casacore's
/// `sum(diff*diff)` does.
fn baseline_length(first: [f64; 3], second: [f64; 3]) -> f64 {
    let dx = first[0] - second[0];
    let dy = first[1] - second[1];
    let dz = first[2] - second[2];
    (dx * dx + dy * dy + dz * dz).sqrt()
}
