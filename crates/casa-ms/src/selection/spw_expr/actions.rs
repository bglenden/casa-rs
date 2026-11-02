// SPDX-License-Identifier: LGPL-3.0-or-later
//! The grammar's table actions and `ms/MSSel/MSSpwParse.cc`: each
//! `FullSpec` resolves its windows and channels and appends them to the
//! parser's lists, and `endOfCeremony` and `baseMSSpwGramParseCommand`
//! check the result.

use super::grammar::{Expression, OneFreq, Spw, Unit, integer};
use super::index::SpwIndex;
use super::{ChannelRange, DataDescriptionRow, SpectralWindowRow};

/// What one expression resolves to, before it becomes a
/// [`super::SpwSelection`].
pub(super) struct Resolved {
    pub(super) spw_ids: Vec<usize>,
    pub(super) data_description_ids: Vec<usize>,
    pub(super) channels: Vec<ChannelRange>,
    pub(super) rows: Option<Vec<usize>>,
    pub(super) warnings: Vec<String>,
}

/// The reports an `MSSelectionErrorHandler` collects during the parse
/// (`MSSelectionErrorHandler.cc:56-77`). The handler `MSSelection`
/// installs is a base-class clone of `MSSSpwErrorHandler`, which does not
/// override `clone()`, so a report refuses the expression once parsing
/// ends (`MSSelection.cc:528-536`, `859-883`), and its message replaces
/// any error that stopped the parse.
#[derive(Default)]
struct Reports {
    message: Option<String>,
    tokens: Vec<String>,
}

impl Reports {
    fn report(&mut self, message: &str, token: String) {
        self.message.get_or_insert_with(|| message.to_string());
        self.tokens.push(token);
    }

    /// `constructMessage`: the first message, then every token followed by
    /// a space.
    fn refusal(&self) -> Option<String> {
        self.message.as_ref().map(|first| {
            let mut message = first.clone();
            for token in &self.tokens {
                message.push_str(token);
                message.push(' ');
            }
            message.trim_end().to_string()
        })
    }

    fn check_empty(&mut self, ids: &[usize], message: &str, token: String) {
        if ids.is_empty() {
            self.report(
                &format!("Spw Expression: {message}"),
                format!("\"{token}\""),
            );
        }
    }
}

/// Resolve `expression` against the SPECTRAL_WINDOW and DATA_DESCRIPTION
/// rows, or return CASA's refusal.
pub(super) fn resolve(
    expression: &Expression,
    windows: &[SpectralWindowRow<'_>],
    descriptions: &[DataDescriptionRow],
) -> Result<Resolved, String> {
    let mut reports = Reports::default();
    let resolved = resolve_specs(expression, windows, descriptions, &mut reports);
    if let Some(refusal) = reports.refusal() {
        return Err(refusal);
    }
    let resolved = resolved?;
    if !expression.parenthesized && resolved.spw_ids.is_empty() {
        return Err("No Spw ID(s) matched specifications".to_string());
    }
    if resolved.spw_ids.is_empty() || resolved.channels.is_empty() {
        return Err("No valid SPW & Chan combination found".to_string());
    }
    Ok(resolved)
}

fn resolve_specs(
    expression: &Expression,
    windows: &[SpectralWindowRow<'_>],
    descriptions: &[DataDescriptionRow],
    reports: &mut Reports,
) -> Result<Resolved, String> {
    let index = SpwIndex::new(windows);
    let mut resolved = Resolved {
        spw_ids: Vec::new(),
        data_description_ids: Vec::new(),
        channels: Vec::new(),
        rows: None,
        warnings: Vec::new(),
    };
    for spec in &expression.specs {
        let spws = windows_of(&spec.spw, &index, reports)?;
        let elements = spec.channels.as_deref();
        let triples = index.channel_triples(&spws, elements, &mut resolved.warnings)?;
        select_channels(
            &spws,
            &triples,
            elements.map_or(1, <[_]>::len),
            &mut resolved.channels,
        );
        for &spw in &spws {
            // `selectSpwIdsFromIDList(ids, False)` (`MSSpwParse.cc:83-155`):
            // a window once per unflagged DATA_DESCRIPTION row naming it,
            // none when the window is flagged.
            if index.window(spw).flag_row {
                continue;
            }
            for (ddid, description) in descriptions.iter().enumerate() {
                if !description.flag_row
                    && usize::try_from(description.spectral_window_id).ok() == Some(spw)
                {
                    resolved.spw_ids.push(spw);
                    resolved.data_description_ids.push(ddid);
                }
            }
        }
    }
    if !expression.parenthesized {
        // `endOfCeremony` (`MSSpwParse.cc:291-317`): the rows of the data
        // descriptions of the unique windows, the same rule again.
        let mut rows = resolved.data_description_ids.clone();
        rows.sort_unstable();
        rows.dedup();
        resolved.rows = Some(rows);
    }
    Ok(resolved)
}

/// The windows of one `Spw` form, after `FullSpec`'s `matchId`.
fn windows_of(
    spw: &Spw,
    index: &SpwIndex<'_, '_>,
    reports: &mut Reports,
) -> Result<Vec<usize>, String> {
    Ok(match spw {
        Spw::Name(name) => {
            let ids = index.match_name(name);
            reports.check_empty(&ids, "No match found for ", name.clone());
            ids
        }
        Spw::Pattern(pattern) => {
            let ids = index.match_pattern(pattern)?;
            reports.check_empty(&ids, "No match found for ", pattern.clone());
            ids
        }
        Spw::Greater(bound) => {
            let ids = compare(index, *bound, None, |value, bound| value > bound)?;
            reports.check_empty(&ids, "No spw ID found for > ", shown(*bound));
            ids
        }
        Spw::Less(bound) => {
            let ids = compare(index, *bound, None, |value, bound| value < bound)?;
            reports.check_empty(&ids, "No spw ID found for < ", shown(*bound));
            ids
        }
        Spw::Between(low, high) => {
            let ids = compare(index, *low, Some(*high), |_, _| false)?;
            reports.check_empty(
                &ids,
                "No spw ID found ",
                format!("{}<>{}", shown(*low), shown(*high)),
            );
            ids
        }
        Spw::Near(frequency) => {
            // `matchFrequencyRange(f, f, True)` refuses when nothing matches.
            index.match_frequency_range((frequency.value, frequency.value), true, 0.0)?
        }
        Spw::List(elements) => {
            let list = index.window_list(elements)?;
            if list.missing_count > 0 {
                let mut token = list
                    .missing
                    .iter()
                    .map(|id| format!("{id},"))
                    .collect::<String>();
                if list.missing_count > list.missing.len() as u64 {
                    token.push_str(&format!("... ({} indices in all)", list.missing_count));
                }
                reports.report("Spw Expression: No match found for ", token);
            }
            list.ids
        }
    })
}

/// `matchGT`, `matchLT` (with `test`) and `matchGTAndLT` (with `high`): on
/// window identifiers when the (first) bound is an index, on REF_FREQUENCY
/// when it is a frequency; the code of the first bound decides for both.
fn compare(
    index: &SpwIndex<'_, '_>,
    bound: OneFreq,
    high: Option<OneFreq>,
    test: impl Fn(f64, f64) -> bool,
) -> Result<Vec<usize>, String> {
    let low = f64::from(bound.value);
    Ok(match (bound.unit, high) {
        (Unit::Index, None) => index.ids_where(|id| test(id as f64, low)),
        (Unit::Hertz, None) => index.references_where(|reference| test(reference, low)),
        (Unit::Index, Some(high)) => {
            // `matchGTAndLT((Int)$1[0], (Int)$3[0])`.
            let high = f64::from(integer(high.value)?);
            index.ids_where(|id| (id as f64) > low && (id as f64) < high)
        }
        (Unit::Hertz, Some(high)) => {
            let high = f64::from(high.value);
            index.references_where(|reference| reference > low && reference < high)
        }
    })
}

fn shown(bound: OneFreq) -> String {
    match bound.unit {
        Unit::Index => format!("{}", bound.value),
        Unit::Hertz => format!("{}Hz", bound.value),
    }
}

/// `selectChannelsFromIDList` (`MSSpwParse.cc:228-257`): each window's
/// `n_specs` triples, kept only when the first of them starts and stops on
/// a channel. A window whose first triple does not is skipped by one triple
/// only, so the next window reads from the skipped window's second triple:
/// a CASA quirk kept for parity.
fn select_channels(
    spws: &[usize],
    triples: &[[i32; 3]],
    n_specs: usize,
    channels: &mut Vec<ChannelRange>,
) {
    let mut next = 0;
    for &spw in spws {
        let [start, stop, _] = triples[next];
        if start != -1 && stop != -1 {
            for &[start, stop, step] in &triples[next..next + n_specs] {
                channels.push(ChannelRange {
                    spw_id: spw,
                    start,
                    stop,
                    step,
                });
            }
            next += n_specs;
        } else {
            next += 1;
        }
    }
}
