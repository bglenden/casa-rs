// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA ARRAY_ID predicates and their ordered output ID list.

use super::syntax::{canonical_numeric_ranges, row_id_interval, trim_row_id_space};
use crate::{MsError, MsResult};

/// CASA ARRAY_ID ranges and their output reindexing order.
pub struct ArraySelection {
    pub(super) ranges: Vec<(i32, i32)>,
    // Store the first index of each interval. The last matching interval
    // supplies the output index when an ID appears more than once.
    indices: Vec<(i64, i64, i64)>,
}

impl ArraySelection {
    /// Compile the array-ID expression and its output indexing order.
    pub fn parse(text: &str) -> MsResult<Self> {
        const MAX_ARRAY: i64 = 1_000;
        let mut ranges = Vec::new();
        let mut index_ranges = Vec::new();
        let mut singles = Vec::new();
        for term in text.split(',').map(trim_row_id_space) {
            let (first, last) = row_id_interval(term).ok_or_else(|| {
                MsError::InvalidInput(format!(
                    "Array Expression: invalid array selector term {term:?}"
                ))
            })?;
            if first <= last {
                ranges.push((first as i32, last as i32));
            }
            // MSArrayGram adds bounded intervals immediately and appends
            // the individual IDs at the end of the entire expression.
            if term.starts_with('<') {
                index_ranges.push((0, last));
            } else if term.starts_with('>') && !term.contains('&') {
                let end = MAX_ARRAY + i64::from(!term.starts_with(">="));
                if first > end + 1 {
                    return Err(MsError::InvalidInput(format!(
                        "Array Expression: invalid array bound {term:?}"
                    )));
                }
                index_ranges.push((first, end));
            } else if term.contains(['~', '&']) {
                index_ranges.push((first, last));
            } else {
                singles.push((first, last));
            }
        }
        let mut offset = 0;
        let indices = index_ranges
            .into_iter()
            .chain(singles)
            .filter(|(first, last)| first <= last)
            .map(|(first, last)| {
                let start = offset;
                offset += last - first + 1;
                (first, last, start)
            })
            .collect();
        Ok(Self {
            ranges: canonical_numeric_ranges(ranges),
            indices,
        })
    }

    /// Map an input array ID to its output ID after CASA range expansion.
    pub fn reindex(&self, id: i32) -> MsResult<i32> {
        let id = i64::from(id);
        let output = self
            .indices
            .iter()
            .rev()
            .find_map(|&(first, last, offset)| {
                (first..=last).contains(&id).then_some(offset + id - first)
            });
        // An absent key in CASA's nonempty std::map acquires the default
        // output ID 0. With an empty map it copies the original column.
        let output = output.unwrap_or(if self.indices.is_empty() { id } else { 0 });
        i32::try_from(output).map_err(|_| {
            MsError::InvalidInput(format!(
                "Array Expression: output ARRAY_ID {output} is out of range"
            ))
        })
    }
}
