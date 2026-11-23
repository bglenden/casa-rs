// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;

use casa_ms::{MeasurementSet, MsError, MsResult};
use casa_tables::{RequiredScalarColumnDestination, RequiredScalarColumnValuesMut, Table};
use casa_types::ScalarValue;

const CHUNK_ROWS: usize = 1 << 16;

fn tree_bytes(entries: usize, entry_bytes: usize) -> usize {
    if entries == 0 {
        return 0;
    }
    // A root may have one entry. Other B-tree nodes hold at least five, and
    // each node can hold eleven entries, twelve child pointers and a header.
    let nodes = 1 + (entries - 1) / 5;
    nodes.saturating_mul(11 * entry_bytes + 12 * size_of::<usize>() + 32)
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct ScanKey {
    pub(super) observation: i32,
    pub(super) array: i32,
    pub(super) number: i32,
}

#[derive(Default)]
pub(super) struct Scan {
    pub(super) fields: BTreeSet<i32>,
    pub(super) antennas: BTreeSet<i32>,
}

/// MAIN scan membership cached per attached MeasurementSet. Chunk buffers
/// read only scalar keys and never touch visibility or flag cells.
#[derive(Default)]
pub(super) struct ScanIndex {
    pub(super) scans: BTreeMap<ScanKey, Scan>,
    pub(super) baselines: BTreeSet<[i32; 2]>,
    pub(super) arrays: usize,
}

impl ScanIndex {
    pub(super) fn read(ms: &MeasurementSet) -> MsResult<Self> {
        let main = ms.main_table();
        let root = ms.path().expect("an attached MS has a path");
        let direct =
            !main.is_reference_table()? && Table::concatenated_source_paths(root)?.is_none();
        let mut result = Self::default();
        let mut rows = Vec::new();
        let [
            mut observations,
            mut arrays,
            mut numbers,
            mut fields,
            mut ant1,
            mut ant2,
        ] = std::array::from_fn(|_| Vec::new());
        let mut last_max_array = -1;
        for start in (0..main.row_count()).step_by(CHUNK_ROWS) {
            rows.clear();
            rows.extend(start..(start + CHUNK_ROWS).min(main.row_count()));
            let columns = [
                ("OBSERVATION_ID", &mut observations),
                ("ARRAY_ID", &mut arrays),
                ("SCAN_NUMBER", &mut numbers),
                ("FIELD_ID", &mut fields),
                ("ANTENNA1", &mut ant1),
                ("ANTENNA2", &mut ant2),
            ];
            if direct {
                let mut destinations = columns.map(|(name, values)| {
                    RequiredScalarColumnDestination::new(
                        name,
                        RequiredScalarColumnValuesMut::Int32(values),
                    )
                });
                main.required_scalar_columns_for_rows_into(&rows, &mut destinations)?;
            } else {
                // Reference and concatenation descriptors resolve row identity
                // in the general selected-column path.
                for (name, values) in columns {
                    let cells = main
                        .column_accessor(name)?
                        .scalar_cells_owned_for_rows(&rows)?;
                    values.clear();
                    for cell in cells {
                        let Some(ScalarValue::Int32(value)) = cell else {
                            return Err(MsError::InvalidInput(format!(
                                "MAIN column {name} has a missing or non-integer value"
                            )));
                        };
                        values.push(value);
                    }
                }
            }
            let mut previous_scan = None;
            let mut scan = None;
            for index in 0..rows.len() {
                let [first, second] = [ant1[index], ant2[index]];
                let key = ScanKey {
                    observation: observations[index],
                    array: arrays[index],
                    number: numbers[index],
                };
                if previous_scan != Some(key) {
                    last_max_array = last_max_array.max(key.array);
                    previous_scan = Some(key);
                    scan = Some(result.scans.entry(key).or_default());
                }
                let scan = scan.as_deref_mut().expect("the current scan was inserted");
                scan.fields.insert(fields[index]);
                scan.antennas.extend([first, second]);
                result
                    .baselines
                    .insert([first.min(second), first.max(second)]);
            }
        }
        result.arrays = (i64::from(last_max_array) + 1) as usize;
        Ok(result)
    }

    pub(super) fn cached_bytes(&self) -> usize {
        let baseline_bytes = tree_bytes(self.baselines.len(), size_of::<[i32; 2]>());
        let scan_bytes = tree_bytes(self.scans.len(), size_of::<(ScanKey, Scan)>());
        self.scans.values().fold(
            size_of::<Self>()
                .saturating_add(baseline_bytes)
                .saturating_add(scan_bytes),
            |total, scan| {
                total
                    .saturating_add(tree_bytes(scan.fields.len(), size_of::<i32>()))
                    .saturating_add(tree_bytes(scan.antennas.len(), size_of::<i32>()))
            },
        )
    }

    pub(super) fn matching(
        &self,
        observation: i64,
        array: i64,
    ) -> impl Iterator<Item = (&ScanKey, &Scan)> {
        self.scans.iter().filter(move |(key, _)| {
            (observation < 0 || i64::from(key.observation) == observation)
                && (array < 0 || i64::from(key.array) == array)
        })
    }

    pub(super) fn check_scope(
        &self,
        ms: &MeasurementSet,
        observation: i64,
        array: i64,
        require_both: bool,
    ) -> MsResult<()> {
        let observations = ms.observation()?.row_count();
        for (name, id, count) in [
            ("Observation ID", observation, observations),
            ("Array ID", array, self.arrays),
        ] {
            if (require_both && id < 0) || id >= count as i64 {
                return Err(MsError::InvalidInput(format!(
                    "{name} {id} out of range, must be less than {count}"
                )));
            }
        }
        Ok(())
    }
}
