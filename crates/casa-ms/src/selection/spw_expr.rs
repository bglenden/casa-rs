// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's MeasurementSet spectral-window and channel selection: the `spw`
//! expressions of casacore's `MSSelection`, ported from `MSSpwGram.ll`,
//! `MSSpwGram.yy`, `MSSpwParse.cc` and `MSSpwIndex.cc` at casacore
//! `aaf72eb7`, the commit CASA 6.7.6.14 builds.
//!
//! [`resolve_spw_selector`] resolves an expression against the
//! SPECTRAL_WINDOW rows ([`SpectralWindowRow`]) and the DATA_DESCRIPTION
//! rows ([`DataDescriptionRow`]) into a [`SpwSelection`] holding what
//! `MSSelection` reports for it: the windows (`getSpwList`), their data
//! descriptions (`getSPWDDIDList`), the channel list (`getChanList`, from
//! which `getChanFreqList` and `getChanSlices` follow) and the data
//! descriptions whose MAIN rows the selection keeps.
//! [`MeasurementSet::resolve_spw_selector`] reads those rows from a
//! MeasurementSet.
//!
//! # Forms
//!
//! An expression is a comma-separated list of window terms, each optionally
//! followed by `:` and a channel list; `( ... )` around the whole list is
//! also accepted (see [Quirks](#casa-quirks-this-port-keeps)). A window term
//! is one of:
//!
//! | Form | Windows | C++ |
//! |---|---|---|
//! | `3`, `1.7` | window 3, window 1 (numbers are truncated) | `MSSpwGram.yy:219-224, 393-408` |
//! | `0~3`, `0~9^3`, `^2` | the indices `0..=3`, every third, and `-1` (no window) | `.yy:182-203`, `MSSpwIndex.cc:641-688` |
//! | `0;2;5` | a `;` list of the above | `.yy:247-265` |
//! | `name` | the windows whose NAME is exactly `name` (letters, digits, `_`, `'`, `{`, `}`, `+`, `#`, not starting with a digit) | `.ll:69-72, 158-164`, `.yy:267-284` |
//! | `spw*`, `"any text*"`, `/text/` | the windows whose whole NAME the shell-style pattern matches (`*`, `?`, `[...]`, `{a,b}`); `/.../` is a pattern too, not a regex | `.ll:61-117, 165-171`, `MSSpwIndex.cc:55-75` |
//! | `1.4GHz` | the first window whose midpoint is within its largest signed CHAN_WIDTH of 1.4 GHz | `MSSpwIndex.cc:210-309` |
//! | `1GHz~2GHz`, `1~2GHz` | the windows whose midpoint lies in the range | same |
//! | `1GHz~2GHz^10MHz` | the windows one step of the range falls within the largest signed CHAN_WIDTH of | same |
//! | `~1.4GHz` | the windows whose midpoint is within their TOTAL_BANDWIDTH of 1.4 GHz | same |
//! | `>2`, `<2`, `2<>5` | identifiers above 2, below 2, strictly between | `MSSpwIndex.cc:344-371` |
//! | `>50GHz`, `<50GHz`, `1GHz<>60GHz` | REF_FREQUENCY above, below, strictly between | `MSSpwIndex.cc:313-340` |
//!
//! A window's midpoint is the single-precision mean of its first and last
//! CHAN_FREQ; all frequencies in an expression are single precision, as in
//! the C++. Frequency units are `Hz`, `kHz`, `MHz`, `GHz` and `THz` in any
//! case, chosen by the first letter (`mHz` is a megahertz); velocity units
//! (`km/s`, `m/s`) are refused, as CASA disables them
//! (`MSSpwGram.yy:117-134`).
//!
//! A channel list is a `;` list of:
//!
//! | Form | Channels (`[start, stop, step]` in [`SpwSelection::channel_list`]) | C++ |
//! |---|---|---|
//! | `5` | `[5, 5, 1]`; past the end, the last channel (logged) | `MSSpwIndex.cc:421-463` |
//! | `2~9`, `2~9^2` | `[2, 9, 1]`, `[2, 9, 2]`; each end clamped to the window (logged when the stop is past it) | same |
//! | `^3` | `[0, last, 3]` | `.yy:196-202` |
//! | `-1`, `-1~5` | `-1` is the first (as a start) or last (as a stop) channel: `-1` alone is every channel | same |
//! | `1.40GHz~1.41GHz` | the first channel centre at or above the start to the last at or below the stop, `-1` where there is none; the step in hertz over the largest signed CHAN_WIDTH | `MSSpwIndex.cc:464-536, 593-637` |
//! | `1.4GHz` | the channels bracketing 1.4 GHz | same |
//!
//! A step below 1 is 1; a range whose start exceeds its stop, as written,
//! is refused.
//!
//! # Refusals
//!
//! Besides syntax errors and reversed ranges, CASA refuses an expression
//! when any window term matches nothing (a name, pattern, `>`/`<`/`<>`
//! bound, or index naming no row): the "no match" reports go to the
//! `MSSelectionErrorHandler`, which throws once parsing ends
//! (`MSSelection.cc:859-883`). A frequency that matches no window throws at
//! once (`MSSpwIndex.cc:298-307`). Without any window that has an
//! unflagged data description, or without any channel range, the expression
//! is refused (`MSSpwParse.cc:307-312`, `MSSpwGram.cc:121-123`).
//!
//! # CASA quirks this port keeps
//!
//! - An index past the last row is looked up as a NAME: in a 10-window
//!   MeasurementSet whose window 4 is named `12`, `12` selects window 4,
//!   while `7` is always window 7 (`MSSpwIndex.cc:82-98`).
//! - A window's FLAG_ROW, and a data description's, remove the window from
//!   [`spw_ids`](SpwSelection::spw_ids) and its rows, but not its channels
//!   from [`channel_list`](SpwSelection::channel_list); a window without a
//!   data description likewise (`MSSpwParse.cc:106-148`). A window with
//!   two data descriptions is listed twice.
//! - [`spw_ids`](SpwSelection::spw_ids), its data descriptions and the
//!   channel list keep the expression's order and repeats.
//! - A channel range in hertz reads the channel order from the sign of the
//!   first CHAN_WIDTH, not from the frequencies, and divides its step by the
//!   largest *signed* width; one frequency selects the channels on either
//!   side of it.
//! - A single window frequency compares with the largest *signed*
//!   CHAN_WIDTH, so it matches no window with negative widths.
//! - When a window's first channel range in hertz finds no channel, the
//!   next window takes its ranges from that window's second range on
//!   (`MSSpwParse.cc:238-250`); ranges after the first may keep a start of
//!   `-1`.
//! - In a window list, a frequency replaces the list built so far but not
//!   the write position, so an index after it overwrites from there and
//!   pads with zeros (window 0): `100GHz~101GHz;0` lists the matched windows
//!   and then 0 again (`MSSpwIndex.cc:641-688`).
//! - `( ... )` skips the step that turns the windows into a row condition,
//!   so the selection keeps every MAIN row ([`SpwSelection::rows_restricted`]
//!   is false) while still listing the windows and channels.
//! - Scanning follows flex: `"text` or `/text` without its closing quote
//!   ends the expression (`0"x` is `0`); a backslash or newline inside quotes
//!   is dropped; a byte from 0x80 up ends the expression (`0é` is `0`);
//!   `-` is no name character, so `LBAND-A` must be quoted; `e5` and `+2`
//!   are numbers; `Hz` and `MHz` are units, not names.
//! - Out-of-range channels are logged, as CASA logs them, in
//!   [`SpwSelection::warnings`].
//!
//! # Not ported
//!
//! - A number `sscanf("%f")` cannot read (`.`, `+`, `e5`), which CASA reads
//!   as an uninitialised `Float`, and an index outside a C `Int`, whose cast
//!   is undefined (x86 and ARM differ), are refused.
//! - A stepped frequency search that CASA never leaves (a negative step on a
//!   window whose least CHAN_WIDTH is negative, or a step too small to move a
//!   single-precision frequency) is refused rather than run forever; one
//!   longer than 2^27 steps is refused too.
//! - A window list mixing frequencies and indices that CASA would write past
//!   its end is refused.
//! - Messages follow CASA's wording but not its character positions, and a
//!   syntax error may be reported where CASA reports a name it looked up
//!   before reaching it.
//!
//! # Evidence
//!
//! `resources/casa-compat/evidence/msselection-spw-casa-20261102.json` holds
//! what CASA 6.7.6.14's `ms.msselect` and `ms.msselectedindices` report for
//! 1552 expressions over two MeasurementSets (curated forms and seeded
//! random ones), written by `tools/casa-compat/spw_selection_oracle.py`;
//! `crates/casa-ms/tests/spw_selection_oracle.rs` replays every one.

mod actions;
mod grammar;
mod index;
mod lexer;

#[cfg(test)]
mod tests;

use crate::error::{MsError, MsResult};
use crate::ms::MeasurementSet;

/// One SPECTRAL_WINDOW row as spectral-window selection reads it. In a slice
/// of these, row `i` is window `i`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SpectralWindowRow<'a> {
    /// NAME, compared exactly (case-sensitive, untrimmed).
    pub name: &'a str,
    /// CHAN_FREQ in hertz, in the column's own frame: CASA converts nothing.
    /// Its length is the window's channel count (NUM_CHAN in a consistent
    /// MeasurementSet).
    pub chan_freq_hz: &'a [f64],
    /// CHAN_WIDTH in hertz, signed.
    pub chan_width_hz: &'a [f64],
    /// REF_FREQUENCY in hertz, which `>`, `<` and `<>` with a unit compare.
    pub ref_frequency_hz: f64,
    /// TOTAL_BANDWIDTH in hertz, which `~f` compares.
    pub total_bandwidth_hz: f64,
    /// FLAG_ROW.
    pub flag_row: bool,
}

/// One DATA_DESCRIPTION row as spectral-window selection reads it. In a
/// slice of these, row `i` is data description `i`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DataDescriptionRow {
    /// SPECTRAL_WINDOW_ID.
    pub spectral_window_id: i32,
    /// FLAG_ROW.
    pub flag_row: bool,
}

/// One row of `MSSelection::getChanList()`: a window and an inclusive
/// channel range with a step.
///
/// `start` is `-1` only in CASA's quirk of a later frequency range that
/// found no channel (see the [module documentation](self)); otherwise
/// `0 <= start <= stop < channels` and `step >= 1`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChannelRange {
    /// The SPECTRAL_WINDOW row.
    pub spw_id: usize,
    /// The first channel.
    pub start: i32,
    /// The last channel, inclusive.
    pub stop: i32,
    /// The channel step.
    pub step: i32,
}

impl ChannelRange {
    /// The channels the range selects, as `MSSelection::getChanSlices`
    /// slices them: `start, start + step, ...` up to `stop`. `None` for a
    /// range starting at `-1`.
    #[must_use]
    pub fn channels(&self) -> Option<impl Iterator<Item = usize> + use<>> {
        let start = usize::try_from(self.start).ok()?;
        let stop = usize::try_from(self.stop)
            .ok()
            .filter(|&stop| stop >= start);
        let step = usize::try_from(self.step).unwrap_or(1).max(1);
        Some(
            stop.into_iter()
                .flat_map(move |stop| (start..=stop).step_by(step)),
        )
    }
}

/// One row of `MSSelection::getChanFreqList()`: a [`ChannelRange`] in
/// hertz.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ChannelFrequencyRange {
    /// The SPECTRAL_WINDOW row.
    pub spw_id: usize,
    /// CHAN_FREQ of the first channel.
    pub start_hz: f64,
    /// CHAN_FREQ of the last channel.
    pub stop_hz: f64,
    /// The step times the window's mean CHAN_WIDTH.
    pub step_hz: f64,
}

/// A resolved `spw` expression: what `MSSelection` reports for it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SpwSelection {
    selects_all: bool,
    spw_ids: Box<[usize]>,
    data_description_ids: Box<[usize]>,
    channels: Box<[ChannelRange]>,
    rows: Option<Box<[usize]>>,
    warnings: Box<[String]>,
}

impl SpwSelection {
    /// The selection of the empty expression, which `MSSelection` skips:
    /// no condition on rows and no lists.
    #[must_use]
    pub fn all() -> Self {
        Self {
            selects_all: true,
            ..Self::default()
        }
    }

    /// Whether the expression was empty.
    #[must_use]
    pub fn selects_all(&self) -> bool {
        self.selects_all
    }

    /// `MSSelection::getSpwList()`: each selected window once per unflagged
    /// data description of it, in the expression's order with repeats;
    /// flagged windows and windows without a data description are absent.
    #[must_use]
    pub fn spw_ids(&self) -> &[usize] {
        &self.spw_ids
    }

    /// `MSSelection::getSPWDDIDList()`: the data description of each entry
    /// of [`spw_ids`](Self::spw_ids).
    #[must_use]
    pub fn data_description_ids(&self) -> &[usize] {
        &self.data_description_ids
    }

    /// `MSSelection::getChanList()` (unsorted, default step 1): one range
    /// per window and channel element, in the expression's order.
    #[must_use]
    pub fn channel_list(&self) -> &[ChannelRange] {
        &self.channels
    }

    /// Whether the selection restricts MAIN rows by `DATA_DESC_ID`: false
    /// for the empty expression and for a parenthesized one.
    #[must_use]
    pub fn rows_restricted(&self) -> bool {
        self.rows.is_some()
    }

    /// The data descriptions whose MAIN rows are selected, ascending, or
    /// `None` when every row is ([`rows_restricted`](Self::rows_restricted)
    /// is false).
    #[must_use]
    pub fn row_data_descriptions(&self) -> Option<&[usize]> {
        self.rows.as_deref()
    }

    /// Whether a MAIN row with this `DATA_DESC_ID` is selected.
    #[must_use]
    pub fn selects_row(&self, data_desc_id: i32) -> bool {
        self.rows.as_ref().is_none_or(|rows| {
            usize::try_from(data_desc_id).is_ok_and(|ddid| rows.binary_search(&ddid).is_ok())
        })
    }

    /// The channels of window `spw_id` its channel ranges select, ascending
    /// and each once, or `None` when no range names the window.
    ///
    /// # Errors
    ///
    /// A range of the window starting at `-1`, where CASA's channel slices
    /// are undefined.
    pub fn channels_of(&self, spw_id: usize) -> MsResult<Option<Vec<usize>>> {
        let mut channels = Vec::new();
        let mut named = false;
        for range in self.channels.iter().filter(|range| range.spw_id == spw_id) {
            named = true;
            channels.extend(range.channels().ok_or_else(|| {
                MsError::InvalidInput(format!(
                    "spw selection: window {spw_id}'s channel range {}~{} starts before its \
                     first channel",
                    range.start, range.stop
                ))
            })?);
        }
        channels.sort_unstable();
        channels.dedup();
        Ok(named.then_some(channels))
    }

    /// `MSSelection::getChanFreqList()` (unsorted): each channel range's
    /// first and last CHAN_FREQ and its step times the window's mean
    /// CHAN_WIDTH, `step * sum(widths) / n` (`MSSelection.cc:1330-1363`).
    ///
    /// # Errors
    ///
    /// A range outside its window in `windows`, which must be the rows the
    /// selection was resolved against.
    pub fn channel_frequency_list(
        &self,
        windows: &[SpectralWindowRow<'_>],
    ) -> MsResult<Vec<ChannelFrequencyRange>> {
        self.channels
            .iter()
            .map(|range| {
                let window = windows.get(range.spw_id);
                let frequency = |channel: i32| {
                    usize::try_from(channel)
                        .ok()
                        .and_then(|channel| window?.chan_freq_hz.get(channel).copied())
                };
                let (Some(window), Some(start_hz), Some(stop_hz)) =
                    (window, frequency(range.start), frequency(range.stop))
                else {
                    return Err(MsError::InvalidInput(format!(
                        "spw selection: channel range {range:?} is outside its window"
                    )));
                };
                let sum: f64 = window.chan_width_hz.iter().sum();
                Ok(ChannelFrequencyRange {
                    spw_id: range.spw_id,
                    start_hz,
                    stop_hz,
                    step_hz: f64::from(range.step) * sum / window.chan_width_hz.len() as f64,
                })
            })
            .collect()
    }

    /// What CASA logs at `WARN` while resolving the expression: channels
    /// clamped to their window, frequency ranges that find no channel, and
    /// non-constant channel widths, worded as CASA words them (messages of
    /// one window list accumulate, as CASA's do).
    #[must_use]
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    /// The heap bytes the selection owns: its lists and warning text.
    #[must_use]
    pub fn allocated_bytes(&self) -> usize {
        size_of_val::<[usize]>(&self.spw_ids)
            + size_of_val::<[usize]>(&self.data_description_ids)
            + size_of_val::<[ChannelRange]>(&self.channels)
            + self.rows.as_deref().map_or(0, size_of_val::<[usize]>)
            + size_of_val::<[String]>(&self.warnings)
            + self.warnings.iter().map(String::capacity).sum::<usize>()
    }
}

/// Resolve CASA spectral-window selection `text` against `windows`, the
/// SPECTRAL_WINDOW rows, and `descriptions`, the DATA_DESCRIPTION rows, in
/// row order. See the [module documentation](self) for the forms.
///
/// The empty string selects everything ([`SpwSelection::all`]), as
/// `MSSelection` skips an empty expression; text of only blanks is a syntax
/// error.
///
/// # Errors
///
/// [`MsError::InvalidInput`], worded as CASA's `MSSelectionSpwError`, for
/// any expression CASA refuses (see [Refusals](self#refusals)) and for the
/// cases listed under [Not ported](self#not-ported).
pub fn resolve_spw_selector(
    windows: &[SpectralWindowRow<'_>],
    descriptions: &[DataDescriptionRow],
    text: &str,
) -> MsResult<SpwSelection> {
    if text.is_empty() {
        return Ok(SpwSelection::all());
    }
    let refuse =
        |message: String| MsError::InvalidInput(format!("spw selection {text:?}: {message}"));
    let expression = grammar::parse(text).map_err(refuse)?;
    let resolved = actions::resolve(&expression, windows, descriptions).map_err(refuse)?;
    Ok(SpwSelection {
        selects_all: false,
        spw_ids: resolved.spw_ids.into_boxed_slice(),
        data_description_ids: resolved.data_description_ids.into_boxed_slice(),
        channels: resolved.channels.into_boxed_slice(),
        rows: resolved.rows.map(Vec::into_boxed_slice),
        warnings: resolved.warnings.into_boxed_slice(),
    })
}

/// The SPECTRAL_WINDOW columns spectral-window selection reads, owned.
struct OwnedWindow {
    name: String,
    chan_freq_hz: Vec<f64>,
    chan_width_hz: Vec<f64>,
    ref_frequency_hz: f64,
    total_bandwidth_hz: f64,
    flag_row: bool,
}

impl MeasurementSet {
    /// Resolve CASA spectral-window selection `text` against this
    /// MeasurementSet's SPECTRAL_WINDOW (NAME, CHAN_FREQ, CHAN_WIDTH,
    /// REF_FREQUENCY, TOTAL_BANDWIDTH, FLAG_ROW) and DATA_DESCRIPTION
    /// (SPECTRAL_WINDOW_ID, FLAG_ROW) tables, as [`resolve_spw_selector`]
    /// does over rows a caller supplies.
    ///
    /// Reads only those two subtables; MAIN is not scanned.
    ///
    /// # Errors
    ///
    /// An unreadable subtable, or any refusal of [`resolve_spw_selector`].
    pub fn resolve_spw_selector(&self, text: &str) -> MsResult<SpwSelection> {
        if text.is_empty() {
            return Ok(SpwSelection::all());
        }
        let table = self.spectral_window()?;
        let owned = (0..table.row_count())
            .map(|row| {
                Ok(OwnedWindow {
                    name: table.name(row)?,
                    chan_freq_hz: table.chan_freq(row)?,
                    chan_width_hz: table.chan_width(row)?,
                    ref_frequency_hz: table.ref_frequency(row)?,
                    total_bandwidth_hz: table.total_bandwidth(row)?,
                    flag_row: table.flag_row(row)?,
                })
            })
            .collect::<MsResult<Vec<_>>>()?;
        let windows = owned
            .iter()
            .map(|window| SpectralWindowRow {
                name: &window.name,
                chan_freq_hz: &window.chan_freq_hz,
                chan_width_hz: &window.chan_width_hz,
                ref_frequency_hz: window.ref_frequency_hz,
                total_bandwidth_hz: window.total_bandwidth_hz,
                flag_row: window.flag_row,
            })
            .collect::<Vec<_>>();
        let table = self.data_description()?;
        let descriptions = (0..table.row_count())
            .map(|row| {
                Ok(DataDescriptionRow {
                    spectral_window_id: table.spectral_window_id(row)?,
                    flag_row: table.flag_row(row)?,
                })
            })
            .collect::<MsResult<Vec<_>>>()?;
        resolve_spw_selector(&windows, &descriptions, text)
    }
}
