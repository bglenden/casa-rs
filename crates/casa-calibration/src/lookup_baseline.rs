// SPDX-License-Identifier: LGPL-3.0-or-later
//! Baseline coordinates for supplied additive simulation tables. Numerical
//! loading and interpolation remain the ordinary calibration lookup's.
use super::*;
use casa_tables::TableOptions;
use casa_types::ScalarValue;

impl CalibrationTableLookup {
    /// The query index for a baseline loaded by
    /// [`Self::open_for_baseline_simulation`]. Missing/new antennas have no
    /// supplied parameters; the caller supplies its neutral operator.
    #[must_use]
    pub fn baseline_index(&self, p: usize, q: usize) -> Option<i32> {
        let antennas = self.baseline_antennas?;
        if p >= antennas || q >= antennas {
            return None;
        }
        p.min(q)
            .checked_mul(antennas)?
            .checked_add(p.max(q))
            .and_then(|index| i32::try_from(index).ok())
    }

    pub(super) fn baseline_rows(&mut self, antennas: usize) -> Result<(), ApplyExecutionError> {
        let table = Table::open(TableOptions::new(&self.path))
            .map_err(|e| Self::table_error(&self.path, &e.to_string()))?;
        let mut positions = HashMap::new();
        let mut solutions: HashMap<(i32, i32, i32), Vec<CalibrationSolution>> = HashMap::new();
        for row in 0..table.row_count() {
            let field = integer(&table, row, "FIELD_ID", &self.path)?;
            let window = integer(&table, row, "SPECTRAL_WINDOW_ID", &self.path)?;
            let p = integer(&table, row, "ANTENNA1", &self.path)?;
            let q = integer(&table, row, "ANTENNA2", &self.path)?;
            let key = (field, window, p);
            let position = positions.entry(key).or_insert(0usize);
            let source = self
                .table
                .solutions
                .get(&key)
                .and_then(|series| series.get(*position))
                .ok_or_else(|| {
                    Self::table_error(
                        &self.path,
                        "baseline table row order differs from its loaded parameters",
                    )
                })?;
            *position += 1;
            let baseline = baseline(p, q, antennas, &self.path)?;
            solutions
                .entry((field, window, baseline))
                .or_default()
                .push(CalibrationSolution {
                    time_seconds: source.time_seconds,
                    grid: Arc::clone(&source.grid),
                });
        }
        self.across_fields = None;
        self.table.solutions = solutions;
        self.across_fields = Some(across_fields(&self.table));
        self.baseline_antennas = Some(antennas);
        Ok(())
    }

    pub(super) fn baseline_headers(
        &self,
        headers: &mut [[i32; 5]],
    ) -> Result<(), ApplyExecutionError> {
        let Some(antennas) = self.baseline_antennas else {
            return Ok(());
        };
        let table = Table::open(TableOptions::new(&self.path))
            .map_err(|e| Self::table_error(&self.path, &e.to_string()))?;
        for (row, header) in headers.iter_mut().enumerate() {
            let q = integer(&table, row, "ANTENNA2", &self.path)?;
            header[2] = baseline(header[2], q, antennas, &self.path)?;
        }
        Ok(())
    }
}

fn integer(table: &Table, row: usize, name: &str, path: &Path) -> Result<i32, ApplyExecutionError> {
    let value = table
        .cell_accessor(row, name)
        .and_then(|cell| cell.scalar())
        .map_err(|e| CalibrationTableLookup::table_error(path, &e.to_string()))?;
    match value {
        ScalarValue::Int32(value) => Ok(*value),
        _ => Err(CalibrationTableLookup::table_error(
            path,
            "baseline coordinate is not Int32",
        )),
    }
}

fn baseline(p: i32, q: i32, antennas: usize, path: &Path) -> Result<i32, ApplyExecutionError> {
    let p = usize::try_from(p).ok();
    let q = usize::try_from(q).ok();
    let index = p
        .zip(q)
        .filter(|&(p, q)| p < antennas && q < antennas)
        .and_then(|(p, q)| p.min(q).checked_mul(antennas)?.checked_add(p.max(q)))
        .and_then(|index| i32::try_from(index).ok());
    index.ok_or_else(|| {
        CalibrationTableLookup::table_error(path, "baseline antenna coordinate is out of range")
    })
}
