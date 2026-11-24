// SPDX-License-Identifier: LGPL-3.0-or-later
//! Ordered copies for the public MeasurementSet tool.

use std::path::Path;

use casa_tables::{SortOrder, TableOptions};

use crate::transform::task_history::append_history_messages;
use crate::{MeasurementSet, MsError, MsResult};

mod subtables;

/// Write a sorted, independent MeasurementSet at a staging path.
///
/// The caller owns publication. `scratch` holds the temporary projection of
/// the input and must remain available until the deep copy is complete.
pub fn write_sorted_measurement_set(
    input: &MeasurementSet,
    scratch: &Path,
    staged_output: &Path,
    columns: Option<&[String]>,
    in_place: bool,
    input_name: &Path,
) -> MsResult<()> {
    let main = input.main_table();
    let projection = match columns {
        None => main.sort(&[("TIME", SortOrder::Ascending)])?,
        Some([]) => main.select_rows(&(0..main.row_count()).collect::<Vec<_>>())?,
        Some(columns) => {
            let keys: Vec<_> = columns
                .iter()
                .map(|key| (key.as_str(), SortOrder::Ascending))
                .collect();
            main.sort(&keys)?
        }
    };
    let view_path = scratch.join("ordered.ref");
    projection.save(TableOptions::new(&view_path))?;
    let mut ordered = MeasurementSet::open(&view_path)?;
    // These optional iterator caches refer to the input's row order.
    ordered
        .main_table_mut()
        .keywords_mut()
        .remove("SORTED_TABLE");
    ordered
        .main_table_mut()
        .keywords_mut()
        .remove("SORT_COLUMNS");
    ordered.save_as(staged_output)?;
    subtables::copy_extra_links(main, staged_output)?;
    let (origin, names) = match columns {
        None => ("ms::timesort()", "TIME".to_string()),
        Some(columns) => ("ms::sort()", format!("[{}]", columns.join(","))),
    };
    let message = if in_place {
        format!("Sorted by {names} in ascending order.")
    } else {
        let expression = if columns.is_some() {
            format!(" {names} ")
        } else {
            names
        };
        format!(
            "Generated from {} by sorting by {expression} in ascending order.",
            input_name.display()
        )
    };
    append_history_messages(staged_output, origin, [message])
        .map_err(|error| MsError::InvalidInput(error.to_string()))
}
