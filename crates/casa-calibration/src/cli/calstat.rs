// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's unmasked statistics task over a named table column.

use casa_provider_contracts::CasaValue;
use casa_tables::statistics::column_statistics;
use casa_tables::{Table, TableOptions};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let path = task.input_path("caltable")?;
    let axis = task.text("axis")?.unwrap_or_default();
    let (column, projection) = match axis.as_str() {
        "amp" | "amplitude" | "phase" | "imag" | "imaginary" | "real" => {
            (task.text("datacolumn")?.unwrap_or_default(), axis.as_str())
        }
        column => (column.to_owned(), ""),
    };
    let column = match column.as_str() {
        "corrected" => "CORRECTED_DATA".to_owned(),
        "model" => "MODEL_DATA".to_owned(),
        column => column.to_uppercase(),
    };
    let table = Table::open(TableOptions::new(&path))
        .map_err(|error| TaskFailure::execution(error.to_string()).at("caltable"))?;
    let mut lines = Vec::new();
    let stats = column_statistics(&table, None, &column, projection, |line| lines.push(line))
        .map_err(|error| TaskFailure::execution(error.to_string()))?;
    let values = [
        ("npts", "number of points"),
        ("min", "minimum value"),
        ("max", "maximum value"),
        ("sum", "Sum of values"),
        ("sumsq", "Sum of squared values"),
    ];
    let statistics = [
        ("mean", "Mean of the values"),
        ("var", "Variance of the values"),
        ("stddev", "Standard deviation of the values"),
        ("rms", "Root mean square"),
        ("median", "Median of the pixel values"),
        ("medabsdevmed", "Median of the deviations"),
        ("quartile", "Quartile"),
    ];
    for (column, record) in &stats {
        for (heading, metrics) in [("values", values.as_slice()), ("statistics", &statistics)] {
            lines.push(format!("{column} {heading} ---"));
            for &(key, label) in metrics {
                let value = record[key];
                let value = if key == "npts" {
                    format!("{value:.0}")
                } else {
                    format!("{value:?}")
                };
                lines.push(format!("         -- {label} [{key}]: {value}"));
            }
        }
    }
    let value = CasaValue::dict(stats.iter().map(|(column, record)| {
        (
            column,
            CasaValue::dict(
                record
                    .iter()
                    .map(|(&key, &value)| (key, CasaValue::float(value))),
            ),
        )
    }));
    let mut success = TaskSuccess::new(value, &stats)?;
    for line in lines {
        success = success.with_output_line(line);
    }
    Ok(success)
}
