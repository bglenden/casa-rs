// SPDX-License-Identifier: LGPL-3.0-or-later
//! Physical calibration solutions and frequency coordinates used by plotbandpass.

use std::path::{Path, PathBuf};

use casa_tables::{Table, TableOptions, add_directory};
use casa_types::{ArrayValue, ScalarValue, Value};

use crate::paths::subtable_owner;

pub(super) struct Solutions {
    table: Table,
    pub(super) path: PathBuf,
    pub(super) measurement_set: Option<PathBuf>,
    pub(super) subtype: String,
    pub(super) polarization_basis: String,
    pub(super) rows: Vec<SolutionRow>,
    pub(super) windows: Vec<SolutionWindow>,
    pub(super) antennas: Vec<String>,
    pub(super) fields: Vec<String>,
    layout: Layout,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct SolutionRow {
    pub(super) antenna: i32,
    pub(super) field: i32,
    pub(super) window: i32,
    pub(super) scan: Option<i32>,
    pub(super) time: f64,
    pub(super) interval: f64,
}

#[derive(Debug, Default)]
pub(super) struct SolutionWindow {
    pub(super) name: String,
    pub(super) original_id: i32,
    pub(super) sky_frequencies_hz: Vec<f64>,
    pub(super) frame: i32,
    pub(super) sideband: i32,
    pub(super) baseband: Option<i32>,
}

pub(super) struct Spectrum {
    pub(super) parameters: ArrayValue,
    pub(super) flags: ArrayValue,
}

enum Layout {
    Current,
    Legacy { spectral_ids: Vec<i32> },
}

impl Solutions {
    pub(super) fn open(path: &Path, vis: Option<&Path>) -> Result<Self, String> {
        let table = open(path)?;
        let owner = subtable_owner(&table, path).map_err(message)?;
        let (layout, stored_ms) = Layout::read(&table, &owner)?;
        let measurement_set = associated_ms(vis, &stored_ms, path);
        let (windows, antennas, fields) =
            layout.coordinates(&table, &owner, measurement_set.as_deref())?;
        let rows = solution_rows(&table, &layout)?;
        let subtype = keyword(&table, "VisCal").unwrap_or_else(|| table.info().sub_type.clone());
        let polarization_basis = keyword(&table, "PolBasis").unwrap_or_default();
        Ok(Self {
            table,
            path: path.into(),
            measurement_set,
            subtype,
            polarization_basis,
            rows,
            windows,
            antennas,
            fields,
            layout,
        })
    }

    pub(super) fn spectrum(&self, row: usize) -> Result<Spectrum, String> {
        let parameters = match self.layout {
            Layout::Current if has_column(&self.table, "CPARAM") => "CPARAM",
            _ if has_column(&self.table, "FPARAM") => "FPARAM",
            _ => "GAIN",
        };
        let parameters = array(&self.table, row, parameters)?;
        let flags = array(&self.table, row, "FLAG")?;
        if !matches!(&flags, ArrayValue::Bool(_)) || flags.shape() != parameters.shape() {
            return Err(format!(
                "calibration row {row} has an incompatible FLAG shape"
            ));
        }
        Ok(Spectrum { parameters, flags })
    }
}

impl Layout {
    fn read(table: &Table, owner: &Path) -> Result<(Self, String), String> {
        if has_column(table, "SPECTRAL_WINDOW_ID") {
            return Ok((Self::Current, keyword(table, "MSName").unwrap_or_default()));
        }
        let descriptions = linked(table, owner, "CAL_DESC")?;
        let windows = (0..descriptions.row_count())
            .map(
                |row| match array(&descriptions, row, "SPECTRAL_WINDOW_ID")? {
                    ArrayValue::Int32(ids) => ids
                        .iter()
                        .next()
                        .copied()
                        .ok_or_else(|| format!("CAL_DESC row {row} has no spectral window")),
                    _ => Err(format!(
                        "CAL_DESC row {row} has invalid spectral window IDs"
                    )),
                },
            )
            .collect::<Result<Vec<_>, _>>()?;
        let ms_name = descriptions
            .cell_accessor(0, "MS_NAME")
            .and_then(|cell| cell.scalar())
            .ok()
            .and_then(|value| match value {
                ScalarValue::String(name) => Some(name.clone()),
                _ => None,
            })
            .unwrap_or_default();
        Ok((
            Self::Legacy {
                spectral_ids: windows,
            },
            ms_name,
        ))
    }

    fn coordinates(
        &self,
        table: &Table,
        owner: &Path,
        measurement_set: Option<&Path>,
    ) -> Result<(Vec<SolutionWindow>, Vec<String>, Vec<String>), String> {
        match self {
            Self::Current => Ok((
                windows(&linked(table, owner, "SPECTRAL_WINDOW")?)?,
                names(&linked(table, owner, "ANTENNA")?)?,
                names(&linked(table, owner, "FIELD")?)?,
            )),
            Self::Legacy { spectral_ids } => {
                let Some(ms) = measurement_set else {
                    return Ok((
                        spectral_ids
                            .iter()
                            .map(|&original_id| SolutionWindow {
                                original_id,
                                ..SolutionWindow::default()
                            })
                            .collect(),
                        Vec::new(),
                        Vec::new(),
                    ));
                };
                let spectral_window = open(&ms.join("SPECTRAL_WINDOW"))?;
                let windows = spectral_ids
                    .iter()
                    .map(|&original_id| window(&spectral_window, original_id))
                    .collect::<Result<_, _>>()?;
                Ok((
                    windows,
                    names(&open(&ms.join("ANTENNA"))?)?,
                    names(&open(&ms.join("FIELD"))?)?,
                ))
            }
        }
    }

    fn window_column(&self) -> &'static str {
        match self {
            Self::Current => "SPECTRAL_WINDOW_ID",
            Self::Legacy { .. } => "CAL_DESC_ID",
        }
    }
}

fn associated_ms(explicit: Option<&Path>, stored: &str, caltable: &Path) -> Option<PathBuf> {
    let vis = explicit
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new(stored));
    if vis.as_os_str().is_empty() {
        return None;
    }
    [vis.to_path_buf(), caltable.parent()?.join(vis)]
        .into_iter()
        .find(|candidate| candidate.is_dir())
}

fn solution_rows(table: &Table, layout: &Layout) -> Result<Vec<SolutionRow>, String> {
    let antenna = integers(table, "ANTENNA1")?;
    let field = integers(table, "FIELD_ID")?;
    let window = integers(table, layout.window_column())?;
    let time = numbers(table, "TIME")?;
    let intervals = numbers(table, "INTERVAL")?;
    let scans = has_column(table, "SCAN_NUMBER")
        .then(|| integers(table, "SCAN_NUMBER"))
        .transpose()?;
    Ok((0..table.row_count())
        .map(|row| SolutionRow {
            antenna: antenna[row],
            field: field[row],
            window: window[row],
            scan: scans.as_ref().map(|scans| scans[row]),
            time: time[row],
            interval: intervals[row],
        })
        .collect())
}

fn windows(table: &Table) -> Result<Vec<SolutionWindow>, String> {
    (0..table.row_count())
        .map(|row| window(table, i32::try_from(row).map_err(message)?))
        .collect()
}

fn window(table: &Table, original_id: i32) -> Result<SolutionWindow, String> {
    let row = usize::try_from(original_id).map_err(message)?;
    let frequencies = match array(table, row, "CHAN_FREQ")? {
        ArrayValue::Float64(values) => values.iter().copied().collect(),
        _ => {
            return Err(format!(
                "spectral window {row} has invalid channel frequencies"
            ));
        }
    };
    let name = table
        .cell_accessor(row, "NAME")
        .and_then(|cell| cell.scalar())
        .ok()
        .and_then(|value| match value {
            ScalarValue::String(name) => Some(name.clone()),
            _ => None,
        })
        .unwrap_or_default();
    let integer = |column| {
        table
            .cell_accessor(row, column)
            .and_then(|cell| cell.scalar())
            .ok()
            .and_then(|value| match value {
                ScalarValue::Int32(value) => Some(*value),
                _ => None,
            })
    };
    Ok(SolutionWindow {
        name,
        original_id,
        sky_frequencies_hz: frequencies,
        frame: integer("MEAS_FREQ_REF").unwrap_or(5),
        sideband: integer("NET_SIDEBAND").unwrap_or(0),
        baseband: integer("BBC_NO"),
    })
}

fn names(table: &Table) -> Result<Vec<String>, String> {
    scalars(table, "NAME", |value| match value {
        ScalarValue::String(name) => Some(name),
        _ => None,
    })
}

fn numbers(table: &Table, name: &str) -> Result<Vec<f64>, String> {
    scalars(table, name, |value| match value {
        ScalarValue::Float64(number) => Some(number),
        ScalarValue::Float32(number) => Some(f64::from(number)),
        _ => None,
    })
}

fn integers(table: &Table, name: &str) -> Result<Vec<i32>, String> {
    scalars(table, name, |value| match value {
        ScalarValue::Int32(number) => Some(number),
        _ => None,
    })
}

fn scalars<T>(
    table: &Table,
    name: &str,
    convert: impl Fn(ScalarValue) -> Option<T>,
) -> Result<Vec<T>, String> {
    table
        .column_accessor(name)
        .and_then(|column| column.scalar_cells_owned())
        .map_err(message)?
        .into_iter()
        .enumerate()
        .map(|(row, value)| {
            value
                .and_then(&convert)
                .ok_or_else(|| format!("invalid {name} at row {row}"))
        })
        .collect()
}

fn linked(table: &Table, owner: &Path, name: &str) -> Result<Table, String> {
    let path = table
        .keywords()
        .get(name)
        .and_then(Value::as_table_ref)
        .map(|location| add_directory(location, owner))
        .unwrap_or_else(|| owner.join(name));
    open(&path)
}

fn keyword(table: &Table, name: &str) -> Option<String> {
    match table.keywords().get(name) {
        Some(Value::Scalar(ScalarValue::String(value))) => Some(value.clone()),
        _ => None,
    }
}

fn has_column(table: &Table, name: &str) -> bool {
    table
        .schema()
        .is_some_and(|schema| schema.column(name).is_some())
}

fn array(table: &Table, row: usize, name: &str) -> Result<ArrayValue, String> {
    table
        .cell_accessor(row, name)
        .and_then(|cell| cell.array())
        .cloned()
        .map_err(message)
}

fn open(path: &Path) -> Result<Table, String> {
    Table::open(TableOptions::new(path)).map_err(message)
}

fn message(error: impl std::fmt::Display) -> String {
    error.to_string()
}
