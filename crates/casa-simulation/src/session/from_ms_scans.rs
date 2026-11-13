// SPDX-License-Identifier: LGPL-3.0-or-later
//! Count stored integration groups before allocating their retained state.
use super::{SimulatorError, SimulatorResult};
use casa_imaging_runtime::{Demand, admit};
use casa_ms::MeasurementSet;
use casa_types::ScalarValue;

pub(super) fn count(
    ms: &MeasurementSet,
    resources: &super::super::SessionResources,
) -> SimulatorResult<usize> {
    let _memory = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "counting stored simulation integrations",
            memory: 256 << 10,
        },
    )
    .map_err(SimulatorError::admission)?;
    let error = |value: casa_tables::TableError| {
        SimulatorError::raised(format!("read simulation integrations: {value}"))
    };
    let columns = [
        "ARRAY_ID",
        "OBSERVATION_ID",
        "SCAN_NUMBER",
        "FIELD_ID",
        "DATA_DESC_ID",
        "TIME",
    ]
    .iter()
    .map(|name| ms.main_table().column_accessor(name).map_err(error))
    .collect::<SimulatorResult<Vec<_>>>()?;
    let mut previous = None;
    let mut count = 0;
    for first in (0..ms.row_count()).step_by(1024) {
        let rows: Vec<_> = (first..ms.row_count().min(first + 1024)).collect();
        let mut keys = vec![[0u64; 6]; rows.len()];
        for (index, column) in columns.iter().enumerate() {
            for (key, cell) in keys
                .iter_mut()
                .zip(column.scalar_cells_owned_for_rows(&rows).map_err(error)?)
            {
                key[index] = match cell {
                    Some(ScalarValue::Int32(value)) if index < 5 => value as u64,
                    Some(ScalarValue::Float64(value)) if index == 5 => value.to_bits(),
                    _ => {
                        return Err(SimulatorError::raised(
                            "invalid stored simulation integration key",
                        ));
                    }
                };
            }
        }
        for key in keys {
            if previous != Some(key) {
                count += 1;
                previous = Some(key);
            }
        }
    }
    Ok(count)
}

impl super::Simulator {
    /// Admit the transient reallocation before extending stored integration state.
    pub(in crate::session) fn reserve_stored_scans(
        &mut self,
        additional: usize,
    ) -> SimulatorResult<()> {
        if self.scan_memory.is_none() || additional <= self.scans.capacity() - self.scans.len() {
            return Ok(());
        }
        let wanted = self
            .scans
            .len()
            .checked_add(additional)
            .ok_or_else(|| SimulatorError::raised("simulation scan count overflow"))?;
        let resources = &self.options.resources;
        let mut memory = admit(
            resources.host(),
            resources.policy(),
            &Demand {
                phase: "extending stored simulation integrations",
                memory: (wanted as u64)
                    .saturating_mul(size_of::<super::Scan>() as u64)
                    .saturating_add(256),
            },
        )
        .map_err(SimulatorError::admission)?;
        let previous = self.scans.capacity();
        self.scans
            .try_reserve_exact(additional)
            .map_err(|error| SimulatorError::raised(error.to_string()))?;
        memory.retain(((self.scans.capacity() - previous) * size_of::<super::Scan>()) as u64);
        self.scan_memory
            .as_mut()
            .expect("stored scan state")
            .join(memory);
        Ok(())
    }
}
