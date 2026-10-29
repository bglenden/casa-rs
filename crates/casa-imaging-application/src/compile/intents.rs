// SPDX-License-Identifier: LGPL-3.0-or-later
//! The request's `intent` selection resolved on the observation's
//! catalogue: the observing modes it selects, which name the partitions
//! that hold them (a partition's samples share one mode).
//!
//! A pattern is matched, as CASA's `intent` selection matches it, against a
//! whole observing mode: an MSv2 `STATE::OBS_MODE`, or an MSv4 partition's
//! intents joined by commas in the catalogue's order. A state number
//! selects its MSv2 `STATE` row's mode; an MSv4 has no states.

use std::collections::BTreeSet;

use casa_imaging_model::IntentSelection;
use casa_ms::{IntentSelector, parse_intent_selector};
use casa_observation::{Catalogue, PartitionEntry, StateId};

use super::PrepareError;

/// The resolved selection: the selected modes, and the states selected
/// (by number, or by a pattern matching their mode), which must hold their
/// partitions' samples alone ([`split_state`]).
pub(super) struct Intents {
    pub(super) selection: IntentSelection,
    pub(super) states: BTreeSet<StateId>,
}

/// The observing mode of partition `entry`: its states' `OBS_MODE` (an
/// MSv2 partitions its rows by the whole string), else its intents joined
/// by commas (MSv4). Empty for samples without a state or intents, which
/// no intent selection selects.
pub(super) fn observing_mode(catalogue: &Catalogue, entry: &PartitionEntry) -> String {
    match entry.states.first() {
        Some(state) => catalogue.states[state.0 as usize].obs_mode.clone(),
        None => entry.intents.join(","),
    }
}

/// The intent selection `text` names over `catalogue`.
///
/// # Errors
///
/// [`PrepareError::UnmatchedIntent`] for a pattern no mode matches, a state
/// number the catalogue has no state for (every number on an MSv4), or a
/// selection of no mode at all.
pub(super) fn resolve(text: &str, catalogue: &Catalogue) -> Result<Intents, PrepareError> {
    let unmatched = |selector: String| PrepareError::UnmatchedIntent { selector };
    // The modes a pattern is matched against: every state's when the store
    // states them, else every partition's.
    let known: BTreeSet<String> = if catalogue.states.is_empty() {
        catalogue
            .partitions
            .iter()
            .map(|entry| observing_mode(catalogue, entry))
            .collect()
    } else {
        catalogue
            .states
            .iter()
            .map(|state| state.obs_mode.clone())
            .collect()
    };
    let mut modes = BTreeSet::new();
    let mut states = BTreeSet::new();
    for selector in parse_intent_selector(text) {
        match &selector {
            IntentSelector::State(number) => {
                let state = u32::try_from(*number)
                    .ok()
                    .filter(|state| (*state as usize) < catalogue.states.len())
                    .ok_or_else(|| unmatched(number.to_string()))?;
                modes.insert(catalogue.states[state as usize].obs_mode.clone());
                states.insert(StateId(state));
            }
            IntentSelector::Mode(pattern) => {
                let matching: Vec<&String> =
                    known.iter().filter(|mode| selector.matches(mode)).collect();
                if matching.is_empty() {
                    return Err(unmatched(pattern.clone()));
                }
                // A pattern selects every state of the modes it matches.
                states.extend(
                    (0..catalogue.states.len())
                        .filter(|state| selector.matches(&catalogue.states[*state].obs_mode))
                        .map(|state| StateId(u32::try_from(state).expect("few states"))),
                );
                modes.extend(matching.into_iter().cloned());
            }
        }
    }
    modes.retain(|mode| !mode.is_empty());
    if modes.is_empty() {
        return Err(unmatched(text.to_string()));
    }
    Ok(Intents {
        selection: IntentSelection::Only(modes.into_iter().collect()),
        states,
    })
}

/// Whether partition `entry`, of a selected mode, holds samples of a state
/// the selection leaves out beside a selected one: only a state number can
/// leave one out, and the MSv2 adapter partitions the rows of every state
/// of one `OBS_MODE` together, so such a selection cannot be read by
/// partition.
pub(super) fn split_state(entry: &PartitionEntry, states: &BTreeSet<StateId>) -> bool {
    entry.states.iter().any(|state| states.contains(state))
        && entry.states.iter().any(|state| !states.contains(state))
}
