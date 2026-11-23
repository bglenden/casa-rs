// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read-only attachment and metadata for CASA's calanalysis tool.

use std::path::Path;

use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, ScalarValue, Value};

use crate::constants::{
    COL_ANTENNA1, COL_ANTENNA2, COL_FIELD_ID, COL_PARAMERR, COL_TIME, KEY_MS_NAME, KEY_PAR_TYPE,
    KEY_POL_BASIS, KEY_VIS_CAL,
};

/// A named axis of an attached calibration table.
#[derive(Clone, Copy)]
pub enum CalAnalysisIds {
    /// Field IDs in the main calibration table.
    Fields,
    /// All rows of the antenna subtable.
    Antennas,
    /// First antenna IDs in the main calibration table.
    FirstAntennas,
    /// Second antenna IDs in the main calibration table.
    SecondAntennas,
    /// All rows of the spectral-window subtable.
    SpectralWindows,
}

/// Metadata retained by a `calanalysis` attachment. Large parameter columns
/// can be read separately when needed by a selected query.
pub struct CalAnalysis {
    /// Path spelling supplied by the caller.
    pub name: String,
    /// Name of the source measurement set recorded by the writer.
    pub ms_name: String,
    /// Visibility calibration type, such as `B Jones`.
    pub vis_cal: String,
    /// Parameter family recorded by the writer.
    pub parameter_type: String,
    /// Polarization basis recorded by the writer.
    pub polarization_basis: String,
    /// Feed labels in the order of the parameter axis.
    pub feeds: Vec<String>,
    /// Unique solution times, sorted in seconds.
    pub times: Vec<f64>,
    /// Channel counts in spectral-window row order.
    pub channel_counts: Vec<i64>,
    /// Channel frequencies in Hz in spectral-window row order.
    pub frequencies: Vec<Vec<f64>>,
    fields: Vec<i32>,
    antennas: Vec<i32>,
    first_antennas: Vec<i32>,
    second_antennas: Vec<i32>,
    windows: Vec<i32>,
}

impl CalAnalysis {
    /// Read the table keywords and metadata columns needed at attachment.
    pub fn open(name: String) -> Result<Self, String> {
        let path = Path::new(&name);
        if !path.exists() {
            return Err(format!("Table {name} does not exist"));
        }
        let table = open_table(path)?;
        let ms_name = keyword(&table, KEY_MS_NAME)?;
        let vis_cal = keyword(&table, KEY_VIS_CAL)?;
        let parameter_type = keyword(&table, KEY_PAR_TYPE)?;
        let polarization_basis = keyword(&table, KEY_POL_BASIS)?
            .chars()
            .next()
            .map(|basis| basis.to_ascii_uppercase().to_string())
            .unwrap_or_default();
        let fields = unique_ids(&table, COL_FIELD_ID)?;
        let first_antennas = unique_ids(&table, COL_ANTENNA1)?;
        let second_antennas = unique_ids(&table, COL_ANTENNA2)?;
        let antennas = row_ids(open_table(&path.join("ANTENNA"))?.row_count())?;
        let mut times = read_scalars(&table, COL_TIME, |value| match value {
            ScalarValue::Float64(value) => Some(value),
            _ => None,
        })?;
        times.sort_by(f64::total_cmp);
        times.dedup();
        let feeds = feeds(&table, &polarization_basis)?;
        let spectral = open_table(&path.join("SPECTRAL_WINDOW"))?;
        let windows = row_ids(spectral.row_count())?;
        let channel_counts = read_scalars(&spectral, "NUM_CHAN", |value| match value {
            ScalarValue::Int32(value) => Some(i64::from(value as u32)),
            _ => None,
        })?;
        let frequencies = read_frequencies(&spectral)?;
        Ok(Self {
            name,
            ms_name,
            vis_cal,
            parameter_type,
            polarization_basis,
            feeds,
            times,
            channel_counts,
            frequencies,
            fields,
            antennas,
            first_antennas,
            second_antennas,
            windows,
        })
    }

    /// Number of identifiers on the requested axis.
    pub fn count(&self, axis: CalAnalysisIds) -> usize {
        self.ids(axis).len()
    }

    /// Return the cached number strings or read the current subtable names.
    pub fn labels(&self, axis: CalAnalysisIds, name: bool) -> Result<Vec<String>, String> {
        use CalAnalysisIds::*;
        let ids = self.ids(axis);
        if !name {
            return Ok(ids
                .iter()
                .map(|&id| match axis {
                    Fields => (id as u32).to_string(),
                    _ => id.to_string(),
                })
                .collect());
        }
        let subtable = match axis {
            Fields => "FIELD",
            Antennas | FirstAntennas | SecondAntennas => "ANTENNA",
            SpectralWindows => "SPECTRAL_WINDOW",
        };
        let names = read_scalars(
            &open_table(&Path::new(&self.name).join(subtable))?,
            "NAME",
            |value| match value {
                ScalarValue::String(name) => Some(name),
                _ => None,
            },
        )?;
        // CASA names the selected fields by ordinal rather than by FIELD_ID.
        ids.iter()
            .enumerate()
            .map(|(ordinal, &id)| {
                if matches!(axis, SecondAntennas) && id == -1 {
                    return Ok("NoName".to_owned());
                }
                let index = if matches!(axis, Fields) {
                    ordinal
                } else {
                    id as usize
                };
                names
                    .get(index)
                    .cloned()
                    .ok_or_else(|| format!("missing {subtable} name at row {index}"))
            })
            .collect()
    }

    fn ids(&self, axis: CalAnalysisIds) -> &[i32] {
        match axis {
            CalAnalysisIds::Fields => &self.fields,
            CalAnalysisIds::Antennas => &self.antennas,
            CalAnalysisIds::FirstAntennas => &self.first_antennas,
            CalAnalysisIds::SecondAntennas => &self.second_antennas,
            CalAnalysisIds::SpectralWindows => &self.windows,
        }
    }
}

fn open_table(path: &Path) -> Result<Table, String> {
    Table::open(TableOptions::new(path)).map_err(|error| error.to_string())
}

fn keyword(table: &Table, key: &str) -> Result<String, String> {
    match table.keywords().get(key) {
        Some(Value::Scalar(ScalarValue::String(value))) => Ok(value.clone()),
        _ => Err(format!("missing or invalid calibration keyword {key}")),
    }
}

fn row_ids(count: usize) -> Result<Vec<i32>, String> {
    let count = i32::try_from(count).map_err(|error| error.to_string())?;
    Ok((0..count).collect())
}

fn read_scalars<T>(
    table: &Table,
    column: &str,
    convert: impl Fn(ScalarValue) -> Option<T>,
) -> Result<Vec<T>, String> {
    table
        .column_accessor(column)
        .and_then(|column| column.scalar_cells_owned())
        .map_err(|error| error.to_string())?
        .into_iter()
        .enumerate()
        .map(|(row, value)| {
            value
                .and_then(&convert)
                .ok_or_else(|| format!("missing or invalid {column} at row {row}"))
        })
        .collect()
}

fn unique_ids(table: &Table, column: &str) -> Result<Vec<i32>, String> {
    let mut ids = read_scalars(table, column, |value| match value {
        ScalarValue::Int32(value) => Some(value),
        _ => None,
    })?;
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}

fn feeds(table: &Table, basis: &str) -> Result<Vec<String>, String> {
    let errors = table
        .cell_accessor(0, COL_PARAMERR)
        .and_then(|cell| cell.array())
        .map_err(|error| error.to_string())?;
    let (ArrayValue::Float32(_), Some(&count)) = (errors, errors.shape().first()) else {
        return Err("invalid PARAMERR in the first calibration row".into());
    };
    if count == 0 {
        return Err("the first calibration row contains no feeds".into());
    }
    if count == 1 {
        return Ok(vec!["S".into()]);
    }
    let (first, second) = match basis {
        "L" => ("X", "Y"),
        "R" => ("R", "L"),
        _ => ("1", "2"),
    };
    let mut labels = vec![String::new(); count];
    labels[0] = first.into();
    labels[1] = second.into();
    Ok(labels)
}

fn read_frequencies(table: &Table) -> Result<Vec<Vec<f64>>, String> {
    let rows = (0..table.row_count()).collect::<Vec<_>>();
    table
        .column_accessor("CHAN_FREQ")
        .and_then(|column| column.array_cells_owned(&rows))
        .map_err(|error| error.to_string())?
        .into_iter()
        .enumerate()
        .map(|(row, value)| match value {
            Some(ArrayValue::Float64(values)) if values.ndim() == 1 => {
                Ok(values.into_iter().collect())
            }
            _ => Err(format!("missing or invalid CHAN_FREQ at row {row}")),
        })
        .collect()
}
