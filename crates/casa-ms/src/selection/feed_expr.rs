// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA feed selection over FEED_ID metadata and MAIN FEED1/FEED2.

use std::collections::BTreeSet;

use super::int_list;
use super::syntax::{row_id_number, trim_row_id_space};
use crate::subtables::get_i32;
use crate::{MeasurementSet, MsError, MsResult, SubtableId};

#[derive(Clone, Debug)]
pub(super) struct FeedSelection(Vec<FeedTerm>);

#[derive(Clone, Debug)]
struct FeedTerm {
    left: Vec<i32>,
    pair: FeedPair,
    negate: bool,
}

#[derive(Clone, Debug)]
enum FeedPair {
    Either,
    Cross(Vec<i32>),
    Any(Vec<i32>),
    Equal,
}

impl FeedSelection {
    pub(super) fn parse(ms: &MeasurementSet, text: &str) -> MsResult<Self> {
        let table = ms
            .subtable(SubtableId::Feed)
            .ok_or_else(|| MsError::MissingSubtable("FEED".to_string()))?;
        let available = (0..table.row_count())
            .map(|row| get_i32(table, row, "FEED_ID"))
            .collect::<MsResult<BTreeSet<_>>>()?;
        let mut terms = Vec::new();
        for raw in text.split(';') {
            let raw = trim_row_id_space(raw);
            let negate = raw.starts_with('!');
            let text = trim_row_id_space(raw.strip_prefix('!').unwrap_or(raw));
            let (left, pair) = if let Some((left, right)) = text.split_once('&') {
                let right = trim_row_id_space(right);
                let (operator, right) = if let Some(right) = right.strip_prefix('&') {
                    let right = trim_row_id_space(right);
                    if let Some(right) = right.strip_prefix('&') {
                        (3, trim_row_id_space(right))
                    } else {
                        (2, right)
                    }
                } else {
                    (1, right)
                };
                let left = feed_ids(left, &available)?;
                let pair = match (operator, right.is_empty()) {
                    (3, true) => FeedPair::Equal,
                    (3, false) => return Err(invalid_feed(raw)),
                    (1, true) => FeedPair::Cross(left.clone()),
                    (1, false) => FeedPair::Cross(feed_ids(right, &available)?),
                    (_, true) => FeedPair::Any(left.clone()),
                    (_, false) => FeedPair::Any(feed_ids(right, &available)?),
                };
                (left, pair)
            } else {
                (feed_ids(text, &available)?, FeedPair::Either)
            };
            terms.push(FeedTerm { left, pair, negate });
        }
        Ok(Self(terms))
    }

    pub(super) fn holds(&self, first: i32, second: i32) -> bool {
        let mut accepted = self.0[0].negate;
        for term in &self.0 {
            let matches = term.holds(first, second);
            accepted = if term.negate {
                accepted && !matches
            } else {
                accepted || matches
            };
        }
        accepted
    }

    pub(super) fn to_taql(&self) -> String {
        let mut terms = self.0.iter();
        let first = terms.next().expect("a parsed feed selection has a term");
        terms.fold(first.to_taql(), |prev, term| {
            let op = if term.negate { "AND" } else { "OR" };
            format!("({prev} {op} {})", term.to_taql())
        })
    }
}

impl FeedTerm {
    fn holds(&self, first: i32, second: i32) -> bool {
        let left = |id| self.left.binary_search(&id).is_ok();
        let pair = |ids: &[i32]| {
            (left(first) && ids.binary_search(&second).is_ok())
                || (left(second) && ids.binary_search(&first).is_ok())
        };
        match &self.pair {
            FeedPair::Either => first != second && (left(first) || left(second)),
            FeedPair::Cross(right) => first != second && pair(right),
            FeedPair::Any(right) => pair(right),
            FeedPair::Equal => first == second && left(first),
        }
    }

    fn to_taql(&self) -> String {
        let left = int_list(&self.left);
        let pair = |right: &[i32]| {
            let right = int_list(right);
            format!(
                "((FEED1 IN [{left}] AND FEED2 IN [{right}]) OR (FEED1 IN [{right}] AND FEED2 IN [{left}]))"
            )
        };
        let expression = match &self.pair {
            FeedPair::Either => {
                format!("(FEED1!=FEED2 AND (FEED1 IN [{left}] OR FEED2 IN [{left}]))")
            }
            FeedPair::Cross(right) => format!("(FEED1!=FEED2 AND {})", pair(right)),
            FeedPair::Any(right) => pair(right),
            FeedPair::Equal => format!("(FEED1==FEED2 AND FEED1 IN [{left}])"),
        };
        if self.negate {
            format!("NOT {expression}")
        } else {
            expression
        }
    }
}

fn feed_ids(text: &str, available: &BTreeSet<i32>) -> MsResult<Vec<i32>> {
    let mut matched = BTreeSet::new();
    for term in text.split(',').map(trim_row_id_space) {
        let (first, last) = term.split_once('~').unwrap_or((term, term));
        let first = row_id_number(first).ok_or_else(|| invalid_feed(term))?;
        let last = row_id_number(last).ok_or_else(|| invalid_feed(term))?;
        if first > last {
            return Err(invalid_feed(term));
        }
        matched.extend(available.range(first..=last).copied());
    }
    if matched.is_empty() {
        return Err(MsError::InvalidInput(format!(
            "No match found for requested feeds [ID(s): [{}]]",
            trim_row_id_space(text)
        )));
    }
    Ok(matched.into_iter().collect())
}

fn invalid_feed(text: &str) -> MsError {
    MsError::InvalidInput(format!(
        "Feed Expression: invalid feed selector term {text:?}"
    ))
}
