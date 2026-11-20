// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the old real-valued EP Jones calibration table used by `nPBWProjectFT`.

use std::path::Path;

use casa_imaging_runtime::{Demand, HostResources, Reservation, ResourcePolicy, admit};
use casa_ms::{MeasurementSet, MsResult, ResolvedMsSelection};
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::invalid;

const OFFSETS: &str = "casars_legacy_pbw_ep_offsets";

pub(crate) struct LegacyEpOffsets {
    values: Vec<Option<[f64; 2]>>,
    _memory: Reservation,
}

impl LegacyEpOffsets {
    pub(super) fn for_selection(
        path: &Path,
        ms: &MeasurementSet,
        selected: &ResolvedMsSelection,
    ) -> MsResult<Self> {
        let first = selected
            .selected_rows
            .first()
            .ok_or_else(|| invalid("no rows selected for legacy pointing offsets"))?;
        let offsets = Self::open(path, ms.antenna()?.row_count(), first.spectral_window_id)?;
        for row in &selected.selected_rows {
            if row.spectral_window_id != first.spectral_window_id {
                return Err(invalid(
                    "legacy pointing offsets for this spectral-window layout are pending #1520",
                ));
            }
            offsets.baseline_offset([row.antenna1_id, row.antenna2_id])?;
        }
        Ok(offsets)
    }

    fn open(path: &Path, antennas: usize, spw: i32) -> MsResult<Self> {
        let table = Table::open(TableOptions::new(path))?;
        if table.info().sub_type != "EP Jones" || table.row_count() == 0 {
            return Err(invalid(
                "a legacy EP Jones pointing table with solutions is required",
            ));
        }
        let description = Table::open(TableOptions::new(path.join("CAL_DESC")))?;
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
        let host = HostResources::detect().map_err(invalid)?;
        let mut offsets = Self::empty(antennas, &host)?;
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
        let mut time = None;
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
                if !epoch.is_finite() || *time.get_or_insert(*epoch) != *epoch {
                    return Err(invalid(
                        "legacy pointing offsets with several solution times are pending #1520",
                    ));
                }
                let antenna =
                    usize::try_from(integer(&table, row, "ANTENNA1")?).map_err(invalid)?;
                let slot = offsets
                    .values
                    .get_mut(antenna)
                    .ok_or_else(|| invalid("legacy EP antenna is outside the selected array"))?;
                if slot.is_some() {
                    return Err(invalid("legacy EP table contains repeated antenna offsets"));
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
                *slot = Some(offset);
            }
        }
        Ok(offsets)
    }

    fn empty(antennas: usize, host: &HostResources) -> MsResult<Self> {
        let memory = admit(
            host,
            &ResourcePolicy::Balanced,
            &Demand {
                phase: "legacy EP antenna offsets",
                // The retained record uses three Float32 values per antenna.
                memory: (antennas as u64)
                    .saturating_mul((size_of::<Option<[f64; 2]>>() + 3 * size_of::<f32>()) as u64),
            },
        )
        .map_err(invalid)?;
        Ok(Self {
            values: vec![None; antennas],
            _memory: memory,
        })
    }

    pub(super) fn save(self, record: &mut RecordValue) {
        let mut packed = ArrayD::zeros(IxDyn(&[3, self.values.len()]).f());
        for (antenna, offset) in self.values.iter().enumerate() {
            if let Some([l, m]) = offset {
                packed[[0, antenna]] = 1.;
                packed[[1, antenna]] = *l as f32;
                packed[[2, antenna]] = *m as f32;
            }
        }
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
        if packed.shape() != [3, antennas] {
            return Err(invalid(
                "saved legacy EP offsets do not match the antenna count",
            ));
        }
        let host = HostResources::detect().map_err(invalid)?;
        let mut offsets = Self::empty(antennas, &host)?;
        for (antenna, slot) in offsets.values.iter_mut().enumerate() {
            let [ok, l, m] = std::array::from_fn(|axis| packed[[axis, antenna]]);
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

    pub(crate) fn baseline_offset(&self, antennas: [i32; 2]) -> MsResult<[f64; 2]> {
        let offset = |antenna: i32| {
            let antenna = usize::try_from(antenna).map_err(invalid)?;
            self.values.get(antenna).copied().flatten().ok_or_else(|| {
                invalid(format!(
                    "legacy EP table has no offsets for antenna {antenna}"
                ))
            })
        };
        let left = offset(antennas[0])?;
        let right = offset(antennas[1])?;
        Ok(std::array::from_fn(|axis| (left[axis] + right[axis]) * 0.5))
    }
}

fn integer(table: &Table, row: usize, name: &str) -> MsResult<i64> {
    match table.cell_accessor(row, name)?.scalar()? {
        ScalarValue::Int32(value) => Ok(i64::from(*value)),
        ScalarValue::Int64(value) => Ok(*value),
        _ => Err(invalid(format!("legacy EP {name} must contain integers"))),
    }
}
