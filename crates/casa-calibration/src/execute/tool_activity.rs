// SPDX-License-Identifier: LGPL-3.0-or-later
//! Visibility-equation counters from the terms executed by the Python tool.

use std::ops::Range;

use casa_types::{RecordField, RecordValue, ScalarValue, Value};

use super::ApplyPlan;
use super::chain::viscal_order;

pub(crate) struct LibraryApplyRegistration {
    pub(crate) name: String,
    pub(crate) subtype: String,
    pub(crate) tables: Range<usize>,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct ApplyTermCounts {
    data: usize,
    flags_in: usize,
    flags_out: usize,
}

struct ApplyTermActivity {
    name: Option<String>,
    subtype: String,
    position: (usize, u8),
    counts: ApplyTermCounts,
}

pub(crate) struct ToolApplyActivity {
    terms: Vec<ApplyTermActivity>,
    table_terms: Vec<usize>,
    first_parang_term: usize,
    visibility_count: usize,
}

impl ToolApplyActivity {
    pub(crate) fn from_plan(
        plan: &ApplyPlan,
        table_names: &[String],
        library_terms: &[LibraryApplyRegistration],
        parang_terms: usize,
        opacity: &[(&'static str, usize)],
        previous: ApplyTermCounts,
    ) -> Self {
        let mut terms = Vec::new();
        let mut table_terms = vec![None; plan.calibration_tables.len()];
        for registration in library_terms {
            let term = terms.len();
            for index in registration.tables.clone() {
                table_terms[index] = Some(term);
            }
            terms.push(ApplyTermActivity {
                name: Some(registration.name.clone()),
                subtype: registration.subtype.clone(),
                position: (registration.tables.start, 1),
                counts: ApplyTermCounts::default(),
            });
        }
        for (index, (table, name)) in plan.calibration_tables.iter().zip(table_names).enumerate() {
            if table_terms[index].is_none() {
                table_terms[index] = Some(terms.len());
                terms.push(ApplyTermActivity {
                    name: Some(name.clone()),
                    subtype: table.summary.table_subtype.clone(),
                    position: (index, 1),
                    counts: ApplyTermCounts::default(),
                });
            }
        }
        terms.extend(opacity.iter().map(|&(subtype, index)| ApplyTermActivity {
            name: Some(String::new()),
            subtype: subtype.into(),
            position: (index, 0),
            counts: ApplyTermCounts::default(),
        }));
        let first_parang_term = terms.len();
        terms.extend((0..parang_terms).map(|index| ApplyTermActivity {
            name: None,
            subtype: "P Jones".into(),
            position: (plan.calibration_tables.len() + index, 1),
            counts: ApplyTermCounts::default(),
        }));
        if let [term] = terms.as_mut_slice() {
            // VisEquation resets term counters when it sorts a chain with
            // multiple entries. A single term keeps accumulating its flags.
            term.counts = previous;
        }
        Self {
            terms,
            table_terms: table_terms
                .into_iter()
                .map(|term| term.expect("planned apply term"))
                .collect(),
            first_parang_term,
            visibility_count: 0,
        }
    }

    pub(super) fn begin_row(&mut self, visibilities: usize) {
        self.visibility_count += visibilities;
    }

    pub(super) fn table_term(&self, table: usize) -> usize {
        self.table_terms[table]
    }

    pub(super) fn parallactic_term(&self, registration: usize) -> usize {
        self.first_parang_term + registration
    }

    pub(super) fn count_term(
        &mut self,
        index: usize,
        visibilities: usize,
        flags_in: usize,
        flags_out: usize,
    ) {
        let counts = &mut self.terms[index].counts;
        counts.data += visibilities;
        counts.flags_in += flags_in;
        counts.flags_out += flags_out;
    }

    pub(crate) fn single_term_counts(&self) -> ApplyTermCounts {
        match self.terms.as_slice() {
            [term] => term.counts,
            _ => ApplyTermCounts::default(),
        }
    }

    pub(crate) fn into_record(mut self, origin: &str) -> RecordValue {
        self.terms
            .sort_by_key(|term| (viscal_order(&term.subtype), term.position));
        let mut entries = self
            .terms
            .into_iter()
            .enumerate()
            .map(|(index, term)| {
                let counts = term.counts;
                let mut fields = vec![
                    RecordField::new(
                        "ndata",
                        Value::Scalar(ScalarValue::Int64(counts.data as i64)),
                    ),
                    RecordField::new(
                        "nflagIn",
                        Value::Scalar(ScalarValue::Int64(counts.flags_in as i64)),
                    ),
                    RecordField::new(
                        "nflagOut",
                        Value::Scalar(ScalarValue::Int64(counts.flags_out as i64)),
                    ),
                ];
                if let Some(table) = term.name {
                    fields.push(RecordField::new(
                        "table",
                        Value::Scalar(ScalarValue::String(table)),
                    ));
                }
                fields.push(RecordField::new(
                    "type",
                    Value::Scalar(ScalarValue::String(term.subtype)),
                ));
                RecordField::new(
                    format!("*{}", index + 1),
                    Value::Record(RecordValue::new(fields)),
                )
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        entries.push(RecordField::new(
            "nVisTotal",
            Value::Scalar(ScalarValue::Int64(self.visibility_count as i64)),
        ));
        RecordValue::new(vec![
            RecordField::new("VisEquation", Value::Record(RecordValue::new(entries))),
            RecordField::new(
                "origin",
                Value::Scalar(ScalarValue::String(origin.to_string())),
            ),
        ])
    }
}
