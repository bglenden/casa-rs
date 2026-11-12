// SPDX-License-Identifier: LGPL-3.0-or-later
//! Preserve signed real parameters for simulator time interpolation. Physical
//! Jones factors are evaluated by the simulator at its own field and frequency.
use super::*;
use ndarray::{ArrayD, IxDyn};

pub(super) fn parameters(
    table: &mut LoadedCalibrationTable,
    path: &Path,
) -> Result<(), ApplyExecutionError> {
    for rows in table.solutions.values_mut() {
        for row in rows {
            let grid = match row.grid.as_ref() {
                CalibrationGrid::Complex(_) => continue,
                CalibrationGrid::Delay(grid) => GainGrid {
                    receptor_count: grid.receptor_count,
                    channel_count: grid.channel_count,
                    values: grid.values_ns.mapv(|value| Complex32::new(value, 0.)),
                    flags: grid.flags.clone(),
                },
                CalibrationGrid::Antpos(grid) => GainGrid {
                    receptor_count: 3,
                    channel_count: 1,
                    values: ArrayD::from_shape_vec(
                        IxDyn(&[3, 1]),
                        grid.offsets_m
                            .map(|value| Complex32::new(value, 0.))
                            .to_vec(),
                    )
                    .expect("three antenna offsets"),
                    flags: ArrayD::from_elem(IxDyn(&[3, 1]), grid.flagged),
                },
                CalibrationGrid::Opacity(grid) => GainGrid {
                    receptor_count: 1,
                    channel_count: 1,
                    values: ArrayD::from_elem(IxDyn(&[1, 1]), Complex32::new(grid.tau, 0.)),
                    flags: ArrayD::from_elem(IxDyn(&[1, 1]), grid.flagged),
                },
                _ => {
                    return Err(ApplyExecutionError::UnsupportedCalibrationTable {
                        path: path.display().to_string(),
                        reason: "simulator real-parameter family is not yet implemented (#1291)"
                            .into(),
                    });
                }
            };
            row.grid = Arc::new(CalibrationGrid::Complex(grid));
        }
    }
    Ok(())
}
