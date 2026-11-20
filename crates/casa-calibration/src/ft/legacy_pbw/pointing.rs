// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the old real-valued EP Jones calibration table used by `nPBWProjectFT`.

use std::path::Path;

use casa_imaging_runtime::{Demand, HostResources, Reservation, ResourcePolicy, admit};
use casa_ms::{MeasurementSet, MsResult, ResolvedMsSelection};
use casa_tables::{Table, TableOptions, add_directory};
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::invalid;

const OFFSETS: &str = "casars_legacy_pbw_ep_offsets";
const EPOCHS: &str = "casars_legacy_pbw_ep_epochs";

pub(crate) struct LegacyEpOffsets {
    antennas: usize,
    epochs: Vec<f64>,
    values: Vec<Option<[f64; 2]>>,
    _memory: Reservation,
}

impl LegacyEpOffsets {
    pub(in crate::ft) fn for_selection(
        path: &Path,
        ms: &MeasurementSet,
        selected: &ResolvedMsSelection,
    ) -> MsResult<Self> {
        let first = selected
            .selected_rows
            .first()
            .ok_or_else(|| invalid("no rows selected for legacy pointing offsets"))?;
        let offsets = Self::open(path, ms.antenna()?.row_count(), first.spectral_window_id)?;
        offsets.check_selection(selected)?;
        Ok(offsets)
    }

    pub(in crate::ft) fn check_selection(&self, selected: &ResolvedMsSelection) -> MsResult<()> {
        let Some(first) = selected.selected_rows.first() else {
            return Ok(());
        };
        for row in &selected.selected_rows {
            if row.spectral_window_id != first.spectral_window_id {
                return Err(invalid(
                    "legacy pointing offsets for this spectral-window layout are pending #1520",
                ));
            }
            self.baseline_offset([row.antenna1_id, row.antenna2_id], row.time_mjd_seconds)?;
        }
        Ok(())
    }

    fn open(path: &Path, antennas: usize, spw: i32) -> MsResult<Self> {
        let table = Table::open(TableOptions::new(path))?;
        let info = table.info();
        if info.table_type != "Calibration" || info.sub_type != "EP Jones" || table.row_count() == 0
        {
            return Err(invalid(
                "a legacy EP Jones pointing table with solutions is required",
            ));
        }
        let description_name = table
            .keywords()
            .get("CAL_DESC")
            .and_then(Value::as_table_ref)
            .ok_or_else(|| invalid("legacy EP table has no CAL_DESC table link"))?;
        let description = Table::open(TableOptions::new(add_directory(description_name, path)))?;
        if description.row_count() != 1 {
            return Err(invalid(
                "legacy pointing offsets for this spectral-window layout are pending #1520",
            ));
        }
        let window = description
            .cell_accessor(0, "SPECTRAL_WINDOW_ID")?
            .array()?;
        let matches_window = match window {
            ArrayValue::Int32(values) => values.len() == 1 && values.iter().next() == Some(&spw),
            ArrayValue::Int64(values) => {
                values.len() == 1 && values.iter().next() == Some(&i64::from(spw))
            }
            _ => false,
        };
        if !matches_window {
            return Err(invalid(
                "legacy pointing offsets for this spectral-window layout are pending #1520",
            ));
        }
        let samples = table.row_count();
        let slots = samples
            .checked_div(antennas)
            .filter(|count| count * antennas == samples)
            .ok_or_else(|| {
                invalid("legacy EP table does not match the MeasurementSet antenna count")
            })?;
        let host = HostResources::detect().map_err(invalid)?;
        let mut offsets = Self::empty(antennas, slots, &host)?;
        let _reading = admit(
            &host,
            &ResourcePolicy::Balanced,
            &Demand {
                phase: "legacy EP pointing table read",
                memory: (table.row_count() as u64)
                    .saturating_mul(512)
                    .saturating_add(64 << 10),
            },
        )
        .map_err(invalid)?;
        let mut rows = Vec::with_capacity(samples);
        let gains = table.column_accessor("GAIN")?;
        for first in (0..table.row_count()).step_by(256) {
            let indices: Vec<_> = (first..(first + 256).min(table.row_count())).collect();
            let cells = gains.array_cells_owned_uncached(&indices)?;
            for (&row, value) in indices.iter().zip(cells) {
                if integer(&table, row, "CAL_DESC_ID")? != 0 {
                    return Err(invalid(
                        "legacy pointing offsets for this calibration description are pending #1520",
                    ));
                }
                let ScalarValue::Float64(epoch) = table.cell_accessor(row, "TIME")?.scalar()?
                else {
                    return Err(invalid("legacy EP TIME must be Float64"));
                };
                if !epoch.is_finite() {
                    return Err(invalid("legacy EP solution times must be finite"));
                }
                let antenna =
                    usize::try_from(integer(&table, row, "ANTENNA1")?).map_err(invalid)?;
                if antenna >= antennas {
                    return Err(invalid("legacy EP antenna is outside the selected array"));
                }
                // The legacy apply path reads the first azimuth/elevation pair
                // (entries zero and two) for both visibility correlations.
                let offset = match value {
                    Some(ArrayValue::Float32(value)) if value.shape() == [4, 1] => {
                        [f64::from(value[[0, 0]]), f64::from(value[[2, 0]])]
                    }
                    Some(ArrayValue::Float64(value)) if value.shape() == [4, 1] => [
                        f64::from(value[[0, 0]] as f32),
                        f64::from(value[[2, 0]] as f32),
                    ],
                    _ => {
                        return Err(invalid(
                            "legacy EP GAIN must contain four real offsets on one channel",
                        ));
                    }
                };
                if !offset.iter().all(|value| value.is_finite()) {
                    return Err(invalid("legacy EP pointing offsets must be finite"));
                }
                rows.push((*epoch, antenna, offset));
            }
        }
        rows.sort_unstable_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
        for (slot, rows) in rows.chunks_exact(antennas).enumerate() {
            let epoch = rows[0].0;
            if offsets.epochs.last() == Some(&epoch)
                || rows
                    .iter()
                    .enumerate()
                    .any(|(index, &(time, antenna, _))| time != epoch || antenna != index)
            {
                return Err(invalid(
                    "legacy EP table does not match the MeasurementSet antenna count at each solution time",
                ));
            }
            offsets.epochs.push(epoch);
            for &(_, antenna, offset) in rows {
                offsets.values[slot * antennas + antenna] = Some(offset);
            }
        }
        Ok(offsets)
    }

    fn empty(antennas: usize, slots: usize, host: &HostResources) -> MsResult<Self> {
        let samples = antennas
            .checked_mul(slots)
            .ok_or_else(|| invalid("legacy EP solution size overflows usize"))?;
        let memory = admit(
            host,
            &ResourcePolicy::Balanced,
            &Demand {
                phase: "legacy EP antenna offsets",
                // The retained record uses three Float32 values per antenna.
                memory: (samples as u64)
                    .saturating_mul((size_of::<Option<[f64; 2]>>() + 3 * size_of::<f32>()) as u64)
                    .saturating_add((slots as u64).saturating_mul((2 * size_of::<f64>()) as u64)),
            },
        )
        .map_err(invalid)?;
        Ok(Self {
            antennas,
            epochs: Vec::with_capacity(slots),
            values: vec![None; samples],
            _memory: memory,
        })
    }

    pub(super) fn save(&self, record: &mut RecordValue) {
        let mut shape = vec![3, self.antennas];
        if self.epochs.len() > 1 {
            shape.push(self.epochs.len());
            record.upsert(
                EPOCHS,
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(IxDyn(&[self.epochs.len()]), self.epochs.clone())
                        .expect("one value per epoch"),
                )),
            );
        }
        let packed = self
            .values
            .iter()
            .flat_map(|offset| offset.map_or([0.; 3], |[l, m]| [1., l as f32, m as f32]))
            .collect();
        let packed = ArrayD::from_shape_vec(IxDyn(&shape).f(), packed)
            .expect("three values per antenna and slot");
        record.upsert(OFFSETS, Value::Array(ArrayValue::Float32(packed)));
    }

    pub(crate) fn from_record(record: &RecordValue, antennas: usize) -> MsResult<Option<Self>> {
        let Some(packed) = record.get(OFFSETS) else {
            return Ok(None);
        };
        let Value::Array(ArrayValue::Float32(packed)) = packed else {
            return Err(invalid(
                "saved legacy EP offsets must contain Float32 values",
            ));
        };
        let epochs = match packed.shape() {
            [3, count] if *count == antennas => None,
            [3, count, slots] if *count == antennas && *slots > 1 => {
                let Some(Value::Array(ArrayValue::Float64(epochs))) = record.get(EPOCHS) else {
                    return Err(invalid("saved legacy EP offsets have no solution times"));
                };
                if epochs.shape() != [*slots]
                    || !epochs.iter().all(|epoch| epoch.is_finite())
                    || epochs
                        .iter()
                        .zip(epochs.iter().skip(1))
                        .any(|(left, right)| left >= right)
                {
                    return Err(invalid("saved legacy EP solution times are invalid"));
                }
                Some(epochs)
            }
            _ => {
                return Err(invalid(
                    "saved legacy EP offsets do not match the antenna count",
                ));
            }
        };
        let host = HostResources::detect().map_err(invalid)?;
        let mut offsets = Self::empty(antennas, epochs.map_or(1, ArrayD::len), &host)?;
        if let Some(epochs) = epochs {
            offsets.epochs.extend(epochs.iter().copied());
        } else {
            offsets.epochs.push(0.);
        }
        for (index, slot) in offsets.values.iter_mut().enumerate() {
            let [ok, l, m] = std::array::from_fn(|axis| {
                let indices = [axis, index % antennas, index / antennas];
                packed[IxDyn(&indices[..packed.ndim()])]
            });
            match ok {
                0. => {}
                1. if l.is_finite() && m.is_finite() => *slot = Some([f64::from(l), f64::from(m)]),
                _ => {
                    return Err(invalid(
                        "saved legacy EP offsets contain an invalid antenna entry",
                    ));
                }
            }
        }
        Ok(Some(offsets))
    }

    pub(crate) fn baseline_offset(&self, antennas: [i32; 2], time: f64) -> MsResult<[f64; 2]> {
        let slot = self.nearest_slot(time);
        let values = &self.values[slot * self.antennas..][..self.antennas];
        let offset = |antenna: i32| {
            let antenna = usize::try_from(antenna).map_err(invalid)?;
            values.get(antenna).copied().flatten().ok_or_else(|| {
                invalid(format!(
                    "legacy EP table has no offsets for antenna {antenna}"
                ))
            })
        };
        let left = offset(antennas[0])?;
        let right = offset(antennas[1])?;
        Ok(std::array::from_fn(|axis| (left[axis] + right[axis]) * 0.5))
    }

    fn nearest_slot(&self, time: f64) -> usize {
        let upper = self.epochs.partition_point(|epoch| *epoch < time);
        if upper == 0
            || upper < self.epochs.len()
                && self.epochs[upper] - time < time - self.epochs[upper - 1]
        {
            upper
        } else {
            upper - 1
        }
    }
}

fn integer(table: &Table, row: usize, name: &str) -> MsResult<i64> {
    match table.cell_accessor(row, name)?.scalar()? {
        ScalarValue::Int32(value) => Ok(i64::from(*value)),
        ScalarValue::Int64(value) => Ok(*value),
        _ => Err(invalid(format!("legacy EP {name} must contain integers"))),
    }
}
