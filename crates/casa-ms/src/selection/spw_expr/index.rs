// SPDX-License-Identifier: LGPL-3.0-or-later
//! The SPECTRAL_WINDOW lookups of `ms/MSSel/MSSpwIndex.cc`: names and
//! patterns, identifier and reference-frequency comparisons, frequency
//! matching, and the conversion of window and channel lists to indices.

use std::collections::BTreeMap;

use super::SpectralWindowRow;
use super::grammar::{Element, Unit, integer};
use crate::selection::antenna_expr::regex::FullMatch;

/// The most iterations a stepped frequency search may take before it is
/// refused: CASA's loop over single-precision frequencies either ends, stalls
/// (and never returns), or would run for longer than any selection should.
const STEP_LIMIT: u64 = 1 << 27;

/// The most window indices a list mixing indices and frequencies may expand
/// to; such a list is positional (see [`SpwIndex::window_list`]).
const MIXED_LIMIT: u64 = 1 << 24;

/// `MSSpwIndex` over the SPECTRAL_WINDOW rows.
pub(super) struct SpwIndex<'r, 'a> {
    windows: &'r [SpectralWindowRow<'a>],
    /// Windows whose NAME is the decimal text of an integer at or past the
    /// row count, by that integer: the first such row, as
    /// `matchNameAsIntID` keeps (`MSSpwIndex.cc:82-98`).
    numeric_names: BTreeMap<i64, usize>,
}

/// The windows a list of window indices names, after `matchNameAsIntID` and
/// `matchId`.
pub(super) struct WindowList {
    pub(super) ids: Vec<usize>,
    /// Some of the indices that name no window, and how many there are.
    pub(super) missing: Vec<i64>,
    pub(super) missing_count: u64,
}

impl WindowList {
    fn miss(&mut self, id: i64, count: u64) {
        if self.missing.len() < 16 {
            self.missing.push(id);
        }
        self.missing_count += count;
    }
}

impl<'r, 'a> SpwIndex<'r, 'a> {
    pub(super) fn new(windows: &'r [SpectralWindowRow<'a>]) -> Self {
        let count = windows.len() as i64;
        let mut numeric_names = BTreeMap::new();
        for (id, window) in windows.iter().enumerate() {
            if let Ok(value) = window.name.parse::<i32>() {
                let value = i64::from(value);
                if value >= count && value.to_string() == window.name {
                    numeric_names.entry(value).or_insert(id);
                }
            }
        }
        Self {
            windows,
            numeric_names,
        }
    }

    pub(super) fn window(&self, id: usize) -> &SpectralWindowRow<'a> {
        &self.windows[id]
    }

    /// `matchName` (`MSSpwIndex.cc:102-109`): every row whose NAME equals
    /// `name`, flagged rows too.
    pub(super) fn match_name(&self, name: &str) -> Vec<usize> {
        self.ids_where(|id| self.windows[id].name == name)
    }

    /// `matchRegexOrPattern(pattern, False)` (`MSSpwIndex.cc:55-75`): every
    /// row whose whole NAME the shell-style pattern matches.
    pub(super) fn match_pattern(&self, pattern: &str) -> Result<Vec<usize>, String> {
        let matcher = FullMatch::compile_pattern(pattern)?;
        Ok(self.ids_where(|id| matcher.matches(self.windows[id].name)))
    }

    /// The rows `keep` accepts, in row order: `matchGT`, `matchLT` and
    /// `matchGTAndLT` on identifiers (`MSSpwIndex.cc:344-371`).
    pub(super) fn ids_where(&self, keep: impl Fn(usize) -> bool) -> Vec<usize> {
        (0..self.windows.len()).filter(|&id| keep(id)).collect()
    }

    /// The rows whose REF_FREQUENCY `keep` accepts: the frequency forms of
    /// `matchGT`, `matchLT` and `matchGTAndLT` (`MSSpwIndex.cc:313-340`).
    pub(super) fn references_where(&self, keep: impl Fn(f64) -> bool) -> Vec<usize> {
        self.ids_where(|id| keep(self.windows[id].ref_frequency_hz))
    }

    /// `matchFrequencyRange(f0, f1, approx, f3)` (`MSSpwIndex.cc:210-309`),
    /// in single precision: a window's frequency is the `Float` midpoint of
    /// its first and last channel. One frequency (`f0 == f1`, or `f1 < 0`)
    /// selects the first window within its largest signed CHAN_WIDTH, `~f`
    /// every window within its TOTAL_BANDWIDTH, a range every window whose
    /// midpoint it holds, and a stepped range every window one of its steps
    /// falls within the largest signed CHAN_WIDTH of.
    pub(super) fn match_frequency_range(
        &self,
        (f0, f1): (f32, f32),
        approx: bool,
        f3: f32,
    ) -> Result<Vec<usize>, String> {
        let exact = !approx && (f1 < 0.0 || f0 == f1);
        let mut terminations = Vec::new();
        let mut ids = Vec::new();
        for (id, window) in self.windows.iter().enumerate() {
            let (widths, frequencies) = (window.chan_width_hz, window.chan_freq_hz);
            let (Some(first), Some(last)) = (frequencies.first(), frequencies.last()) else {
                return Err(no_channels(id));
            };
            if widths.is_empty() {
                return Err(no_channels(id));
            }
            let max_width = widths.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            #[expect(clippy::cast_possible_truncation, reason = "C++ Float variables")]
            let (step, reference) = (
                if f3 < 0.0 {
                    widths.iter().copied().fold(f64::INFINITY, f64::min) as f32
                } else {
                    f3
                },
                f64::midpoint(*last, *first) as f32,
            );
            let found = if approx {
                #[expect(clippy::cast_possible_truncation, reason = "a C++ Float")]
                let total = window.total_bandwidth_hz as f32;
                (reference - f0).abs() <= total
            } else if exact {
                f64::from((reference - f0).abs()) < max_width
            } else if f3 == 0.0 {
                reference >= f0 && reference <= f1
            } else {
                stepped_match((f0, f1), step, (reference, max_width), &mut terminations)?
            };
            if found {
                ids.push(id);
                if exact {
                    break;
                }
            }
        }
        if ids.is_empty() {
            let range = if f0 == f1 {
                format!("frequency {f0}")
            } else {
                format!("frequency range {f0}~{f1}")
            };
            return Err(format!("No matching SPW found for {range} Hz."));
        }
        Ok(ids)
    }

    /// The windows a list of window elements names: `convertToSpwIndex`
    /// (`MSSpwIndex.cc:641-688`), then `matchNameAsIntID`, which replaces an
    /// index past the last row by the first window NAMEd with its decimal
    /// text, then `matchId` (`MSSpwIndex.cc:113-136`), which keeps the
    /// indices of existing rows in order and reports the others.
    ///
    /// A list of indices alone is resolved without expanding the indices
    /// past the last row, so `0~2000000000` costs no more than its windows.
    /// A frequency element replaces the whole list built so far and leaves
    /// the write position where it was, so a later index element overwrites
    /// it from there and grows it with zeros; such a list is simulated
    /// exactly, and refused where CASA would write past its end.
    pub(super) fn window_list(&self, elements: &[Element]) -> Result<WindowList, String> {
        if elements.iter().any(|element| element.unit == Unit::Hertz) {
            return self.mixed_window_list(elements);
        }
        let mut list = WindowList {
            ids: Vec::new(),
            missing: Vec::new(),
            missing_count: 0,
        };
        let count = self.windows.len() as i64;
        for element in elements {
            let Some(progression) = Progression::of(element)? else {
                continue;
            };
            let negatives = progression.count_below(0);
            if negatives > 0 {
                list.miss(progression.start, negatives);
            }
            let mut value = progression.first_at_or_above(0);
            while value < count && value <= progression.last {
                list.ids
                    .push(usize::try_from(value).expect("a nonnegative row index"));
                value += progression.step;
            }
            let beyond = progression.count_from(count);
            if beyond == 0 {
                continue;
            }
            let mut named = 0;
            for (&value, &id) in self.numeric_names.range(count..=progression.last) {
                if progression.holds(value) {
                    list.ids.push(id);
                    named += 1;
                }
            }
            if beyond > named {
                let first_unnamed = (0..)
                    .map(|k| progression.first_at_or_above(count) + k * progression.step)
                    .find(|value| !self.numeric_names.contains_key(value))
                    .expect("an unnamed index exists");
                list.miss(first_unnamed, beyond - named);
            }
        }
        Ok(list)
    }

    /// [`window_list`](Self::window_list) of a list holding a frequency.
    fn mixed_window_list(&self, elements: &[Element]) -> Result<WindowList, String> {
        let mut values: Vec<i64> = Vec::new();
        let mut position = 0usize;
        for element in elements {
            if element.unit == Unit::Hertz {
                values = self
                    .match_frequency_range((element.start, element.stop), false, element.step)?
                    .into_iter()
                    .map(|id| id as i64)
                    .collect();
                continue;
            }
            let Some(progression) = Progression::of(element)? else {
                continue;
            };
            let added = progression.count_from(progression.start);
            if (values.len() as u64).saturating_add(added) > MIXED_LIMIT {
                return Err(format!(
                    "Spw expression: a window list mixing frequencies and more than \
                     {MIXED_LIMIT} indices is not supported"
                ));
            }
            values.resize(values.len() + added as usize, 0);
            let mut value = progression.start;
            while value <= progression.last {
                let Some(slot) = values.get_mut(position) else {
                    return Err("Spw expression: CASA writes this window list past its end \
                                (an index after a frequency that matched fewer windows than \
                                the indices before it)"
                        .to_string());
                };
                *slot = value;
                position += 1;
                value += progression.step;
            }
        }
        let count = self.windows.len() as i64;
        let mut list = WindowList {
            ids: Vec::new(),
            missing: Vec::new(),
            missing_count: 0,
        };
        for value in values {
            let value = if value >= count {
                self.numeric_names
                    .get(&value)
                    .map_or(value, |&id| id as i64)
            } else {
                value
            };
            match usize::try_from(value) {
                Ok(id) if value < count => list.ids.push(id),
                _ => list.miss(value, 1),
            }
        }
        Ok(list)
    }

    /// `convertToChannelIndex` (`MSSpwIndex.cc:394-589`): a `[start, stop,
    /// step]` per window and element, the whole window when there are no
    /// elements. The messages it logs are appended to `warnings`.
    pub(super) fn channel_triples(
        &self,
        spws: &[usize],
        elements: Option<&[Element]>,
        warnings: &mut Vec<String>,
    ) -> Result<Vec<[i32; 3]>, String> {
        let mut triples = Vec::with_capacity(spws.len() * elements.map_or(1, <[_]>::len));
        let mut log = ChannelLog::default();
        let mut some_failed = false;
        let mut any_found = false;
        for &spw in spws {
            let window = &self.windows[spw];
            let channels = i32::try_from(window.chan_freq_hz.len())
                .ok()
                .filter(|&channels| channels > 0)
                .ok_or_else(|| no_channels(spw))?;
            let Some(elements) = elements else {
                triples.push([0, channels - 1, 1]);
                continue;
            };
            for element in elements {
                let triple = match element.unit {
                    Unit::Index => {
                        let (triple, clamped) =
                            index_triple(element, spw, channels, (&mut log, &mut *warnings))?;
                        some_failed |= clamped;
                        any_found = true;
                        triple
                    }
                    Unit::Hertz => {
                        let triple = frequency_triple(element, spw, window, &mut log)?;
                        let found = triple[0] != -1 && triple[1] != -1;
                        some_failed |= !found;
                        any_found |= found;
                        triple
                    }
                };
                triples.push(triple);
            }
        }
        if some_failed && !any_found {
            let ids = spws.iter().map(ToString::to_string).collect::<Vec<_>>();
            log.post(
                &format!("Found no matching SPW(s) [{}]", ids.join(", ")),
                warnings,
            );
        }
        log.finish(warnings);
        Ok(triples)
    }
}

/// `convertToChannelIndex`'s two message buffers: the `ostringstream`
/// `Mesg`, which every clamp appends to and logs whole, and the `LogIO`
/// `log_l`, which holds text written without `LogIO::POST` (the
/// non-constant width notice) until its next post or the end of the call.
#[derive(Default)]
struct ChannelLog {
    clamps: String,
    pending: String,
}

impl ChannelLog {
    /// A clamp: `Mesg << ...; log_l << Mesg.str() << LogIO::POST`.
    fn clamp(&mut self, message: &str, warnings: &mut Vec<String>) {
        self.clamps.push_str(message);
        let posted = std::mem::take(&mut self.pending) + &self.clamps;
        warnings.push(posted);
    }

    /// `log_l << message << LogIO::POST`.
    fn post(&mut self, message: &str, warnings: &mut Vec<String>) {
        warnings.push(std::mem::take(&mut self.pending) + message);
    }

    /// `log_l << message`, posted later.
    fn hold(&mut self, message: &str) {
        self.pending.push_str(message);
    }

    /// `log_l`'s destructor posts what is left.
    fn finish(self, warnings: &mut Vec<String>) {
        if !self.pending.is_empty() {
            warnings.push(self.pending);
        }
    }
}

/// An index element's `[start, stop, step]` (`MSSpwIndex.cc:421-463`): `-1`
/// is the window's first or last channel, a single channel clamps to the
/// window, a range clamps each end, and a step below 1 is 1. Returns
/// whether CASA logged a clamp.
fn index_triple(
    element: &Element,
    spw: usize,
    channels: i32,
    (log, warnings): (&mut ChannelLog, &mut Vec<String>),
) -> Result<([i32; 3], bool), String> {
    let as_int = |value: f32| integer(value).map(|value| value as i32);
    let (mut start, mut stop, step) = (
        as_int(element.start)?,
        as_int(element.stop)?,
        as_int(element.step)?,
    );
    let last = channels - 1;
    if start == -1 {
        start = 0;
    }
    if stop == -1 {
        stop = last;
    }
    let mut clamped = false;
    if stop == start {
        start = start.max(0);
        stop = start;
        if stop > last {
            log.clamp(
                &format!(
                    "Spot-channel {stop} out of range for SPW {spw} (valid range 0~{last}). \
                     Limiting it to be within the available range."
                ),
                warnings,
            );
            start = last;
            stop = last;
            clamped = true;
        }
    } else {
        if stop > last {
            log.clamp(
                &format!(
                    "Channel {stop} out of range for SPW {spw} (valid range 0~{last}). \
                     Limiting it to be within the available range."
                ),
                warnings,
            );
            clamped = true;
        }
        start = start.min(last).max(0);
        stop = stop.max(0).min(last);
    }
    Ok(([start, stop, if step <= 0 { 1 } else { step }], clamped))
}

/// A frequency element's `[start, stop, step]` (`MSSpwIndex.cc:464-536`):
/// the first channel at or above the start and the last at or below the
/// stop, by channel centres, read in the order the first CHAN_WIDTH's sign
/// gives (not the frequencies' own order), -1 where none is; swapped when
/// reversed; the step in hertz divided by the largest signed width.
fn frequency_triple(
    element: &Element,
    spw: usize,
    window: &SpectralWindowRow<'_>,
    log: &mut ChannelLog,
) -> Result<[i32; 3], String> {
    let (frequencies, widths) = (window.chan_freq_hz, window.chan_width_hz);
    let Some(&first_width) = widths.first() else {
        return Err(no_channels(spw));
    };
    if first_width.abs() == 0.0 {
        return Err("Error in the MS SPECTRAL_WINDOW sub-table (channel width==0).".to_string());
    }
    let ascending = first_width / first_width.abs() > 0.0;
    let mut start = find_channel(element.start, frequencies, true, ascending);
    let mut stop = find_channel(element.stop, frequencies, false, ascending);
    let max_width = widths.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let min_width = widths.iter().copied().fold(f64::INFINITY, f64::min);
    if min_width != max_width {
        log.hold(
            "Channel width across the band is not constant.  Using the maximum of the channel \
             width range.",
        );
    }
    #[expect(clippy::cast_possible_truncation, reason = "a C++ Float")]
    let step = (f64::from(element.step) / max_width).abs() as f32;
    let step = integer(step)? as i32;
    if start > stop {
        std::mem::swap(&mut start, &mut stop);
    }
    Ok([start, stop, if step <= 0 { 1 } else { step }])
}

/// `findChanIndex_p` (`MSSpwIndex.cc:593-637`). The frequency is a C
/// `Float`, compared with the `Double` channel centres.
fn find_channel(frequency: f32, centres: &[f64], at_or_above: bool, ascending: bool) -> i32 {
    let frequency = f64::from(frequency);
    let last = centres.len() - 1;
    let found = match (ascending, at_or_above) {
        (true, true) if frequency <= centres[0] => Some(0),
        (true, true) => centres.iter().position(|&c| c >= frequency),
        (true, false) if frequency >= centres[last] => Some(last),
        (true, false) => centres.iter().rposition(|&c| c <= frequency),
        (false, true) if frequency <= centres[last] => Some(last),
        (false, true) => centres.iter().rposition(|&c| c >= frequency),
        (false, false) if frequency >= centres[0] => Some(0),
        (false, false) => centres.iter().position(|&c| c <= frequency),
    };
    found.map_or(-1, |index| index as i32)
}

fn no_channels(spw: usize) -> String {
    format!("Spw expression: SPECTRAL_WINDOW row {spw} has no channels")
}

/// One stepped frequency search of [`SpwIndex::match_frequency_range`]:
/// `for (Float freq = f0; freq <= f1; freq += step)`, until a step falls
/// within `max_width` of `reference`. A search CASA never leaves (a step
/// that no longer moves the single-precision frequency, or one that never
/// reaches `f1`) is refused; `terminations` caches, per step, whether the
/// whole loop ends, which does not depend on the window.
fn stepped_match(
    (f0, f1): (f32, f32),
    step: f32,
    (reference, max_width): (f32, f64),
    terminations: &mut Vec<(u32, Result<(), String>)>,
) -> Result<bool, String> {
    let near = |frequency: f32| f64::from((frequency - reference).abs()) < max_width;
    if step > 0.0 {
        if max_width > 0.0 {
            let mut frequency = f0;
            let mut count = 0;
            while frequency <= f1 {
                if near(frequency) {
                    return Ok(true);
                }
                // The differences only grow from here.
                if f64::from(frequency - reference) >= max_width {
                    break;
                }
                let next = frequency + step;
                if next <= frequency {
                    return Err(never_returns());
                }
                frequency = next;
                count += 1;
                if count > STEP_LIMIT {
                    return Err(too_long());
                }
            }
        }
        let termination = match terminations
            .iter()
            .find(|(bits, _)| *bits == step.to_bits())
        {
            Some((_, termination)) => termination.clone(),
            None => {
                let termination = terminates((f0, f1), step);
                terminations.push((step.to_bits(), termination.clone()));
                termination
            }
        };
        return termination.map(|()| false);
    }
    if step.is_nan() {
        // One pass: the frequency becomes NaN and leaves the loop.
        return Ok(f0 <= f1 && near(f0));
    }
    // A step of zero or below: the frequency never passes `f1`.
    let mut frequency = f0;
    let mut count = 0;
    while frequency <= f1 {
        if near(frequency) {
            return Ok(true);
        }
        let unreachable = max_width.is_nan() || max_width <= 0.0;
        if unreachable || f64::from(reference - frequency) >= max_width {
            return Err(never_returns());
        }
        let next = frequency + step;
        if next == frequency {
            return Err(never_returns());
        }
        frequency = next;
        count += 1;
        if count > STEP_LIMIT {
            return Err(too_long());
        }
    }
    // `f0` above `f1`: the loop does not run.
    Ok(false)
}

/// Whether `for (Float freq = f0; freq <= f1; freq += step)` ends.
fn terminates((f0, f1): (f32, f32), step: f32) -> Result<(), String> {
    let mut frequency = f0;
    let mut count = 0;
    while frequency <= f1 {
        let next = frequency + step;
        if next <= frequency {
            return Err(never_returns());
        }
        frequency = next;
        count += 1;
        if count > STEP_LIMIT {
            return Err(too_long());
        }
    }
    Ok(())
}

fn never_returns() -> String {
    "Spw expression: CASA's stepped frequency search for this range never ends (its \
     single-precision frequency stops advancing or moves away from the range end)"
        .to_string()
}

fn too_long() -> String {
    format!(
        "Spw expression: a stepped frequency search of more than {STEP_LIMIT} steps is not \
         supported"
    )
}

/// An index element's values: `for (Int ii = start; ii <= stop; ii += step)`
/// with a step below 1 taken as 1 (`MSSpwIndex.cc:658-667`).
struct Progression {
    start: i64,
    last: i64,
    step: i64,
}

impl Progression {
    /// The values of `element`, or `None` when there are none.
    fn of(element: &Element) -> Result<Option<Self>, String> {
        let start = i64::from(integer(element.start)? as i32);
        let stop = i64::from(integer(element.stop)? as i32);
        let step = i64::from(integer(element.step)? as i32).max(1);
        if start > stop {
            return Ok(None);
        }
        let last = start + (stop - start) / step * step;
        if last + step > i64::from(i32::MAX) {
            return Err(format!(
                "Spw expression: the window range {start}~{stop}^{step} overflows CASA's C Int \
                 loop"
            ));
        }
        Ok(Some(Self { start, last, step }))
    }

    fn holds(&self, value: i64) -> bool {
        value >= self.start && value <= self.last && (value - self.start) % self.step == 0
    }

    /// The first value at or above `bound`, past [`last`](Self::last) if
    /// none.
    fn first_at_or_above(&self, bound: i64) -> i64 {
        if bound <= self.start {
            self.start
        } else {
            self.start + (bound - self.start + self.step - 1) / self.step * self.step
        }
    }

    /// How many values lie below `bound`.
    fn count_below(&self, bound: i64) -> u64 {
        self.count_from(self.start) - self.count_from(bound)
    }

    /// How many values lie at or above `bound`.
    fn count_from(&self, bound: i64) -> u64 {
        let first = self.first_at_or_above(bound);
        if first > self.last {
            0
        } else {
            ((self.last - first) / self.step + 1) as u64
        }
    }
}
