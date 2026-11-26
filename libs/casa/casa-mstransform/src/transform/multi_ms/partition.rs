// SPDX-License-Identifier: LGPL-3.0-or-later
//! Allocate selected MeasurementSet rows to physical Multi-MS members.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};

use casa_ms::MeasurementSet;
use casa_ms::selection::ResolvedMsSelectionRow;

use super::super::{MsPartitionAxis, MsTransformError, scalar_i32};

type Row = ResolvedMsSelectionRow;

pub(super) struct Partition {
    pub rows: Vec<Row>,
    pub history: Option<(&'static str, String)>,
}

impl MsPartitionAxis {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::SpectralWindow => "spw",
            Self::Baseline => "baseline",
            Self::Automatic => "auto",
        }
    }
}

pub(super) fn scans(input: &MeasurementSet, rows: &[Row]) -> Result<Vec<i32>, MsTransformError> {
    let table = input.main_table();
    let error = |source| MsTransformError::MutateMeasurementSet {
        path: table
            .path()
            .map_or_else(String::new, |path| path.display().to_string()),
        source: Box::new(source),
    };
    let column = table.column_accessor("SCAN_NUMBER").map_err(error)?;
    let mut scans = Vec::with_capacity(rows.len());
    for batch in rows.chunks(4_096) {
        let indices = batch.iter().map(|row| row.row_index).collect::<Vec<_>>();
        let cells = column
            .scalar_cells_owned_for_rows(&indices)
            .map_err(error)?;
        for (cell, index) in cells.iter().zip(indices) {
            scans.push(scalar_i32(cell.as_ref(), "SCAN_NUMBER", index)?);
        }
    }
    Ok(scans)
}

pub(super) fn create(
    input: &MeasurementSet,
    rows: Vec<Row>,
    axis: MsPartitionAxis,
    count: usize,
) -> Result<Vec<Partition>, MsTransformError> {
    let partitions = match axis {
        MsPartitionAxis::Scan => {
            let scan_numbers = scans(input, &rows)?;
            contiguous(rows.into_iter().zip(scan_numbers), count, |scans| {
                Some(("scan", quoted_ids(scans)))
            })
        }
        MsPartitionAxis::SpectralWindow => contiguous(
            rows.into_iter().map(|row| {
                let id = row.spectral_window_id;
                (row, id)
            }),
            count,
            |windows| Some(("spw", quoted_ids(windows))),
        ),
        MsPartitionAxis::Baseline => contiguous(
            rows.into_iter().map(|row| {
                let baseline = (row.antenna1_id, row.antenna2_id);
                (row, baseline)
            }),
            count,
            |baselines| {
                Some((
                    "taql",
                    taql(
                        baselines
                            .iter()
                            .map(|(a, b)| format!("(ANTENNA1=={a} && (ANTENNA2 IN {b}))")),
                    ),
                ))
            },
        ),
        MsPartitionAxis::Automatic => return balanced(input, rows, count),
    };
    Ok(partitions)
}

fn contiguous<K: Ord + Copy>(
    rows: impl Iterator<Item = (Row, K)>,
    count: usize,
    history: impl Fn(&[K]) -> Option<(&'static str, String)>,
) -> Vec<Partition> {
    let mut ordered = BTreeMap::<K, Vec<Row>>::new();
    for (row, key) in rows {
        ordered.entry(key).or_default().push(row);
    }
    let count = count.min(ordered.len());
    let mut remaining = ordered.into_iter();
    let per_member = remaining.len() as f64 / count as f64;
    (0..count)
        .map(|part| {
            // CASA rounds the endpoints independently using Python round().
            let start = (per_member * part as f64).round_ties_even() as usize;
            let end = (per_member * (part + 1) as f64).round_ties_even() as usize;
            let groups = remaining.by_ref().take(end - start);
            let (mut keys, mut rows) = (Vec::new(), Vec::new());
            for (key, mut group) in groups {
                keys.push(key);
                rows.append(&mut group);
            }
            // Preserve the input's first physical row and tie-breaking order.
            rows.sort_unstable_by_key(|row| row.row_index);
            Partition {
                rows,
                history: history(&keys),
            }
        })
        .collect()
}

fn quoted_ids(ids: &[i32]) -> String {
    let ids = ids.iter().map(i32::to_string).collect::<Vec<_>>().join(",");
    format!("\"{ids}\"")
}

fn taql(clauses: impl Iterator<Item = String>) -> String {
    format!("\"{}\"", clauses.collect::<Vec<_>>().join(" OR "))
}

#[derive(Default)]
struct Pair {
    field: i32,
    cost: u64,
    rows: Vec<Row>,
}

#[derive(Default)]
struct Member {
    cost: u64,
    scans: BTreeMap<i32, u64>,
    descriptions: BTreeMap<i32, u64>,
    fields: BTreeMap<i32, u64>,
    groups: Vec<((i32, i32), Vec<Row>)>,
}

fn balanced(
    input: &MeasurementSet,
    rows: Vec<Row>,
    count: usize,
) -> Result<Vec<Partition>, MsTransformError> {
    let spectra = input.spectral_window().map_err(metadata_error)?;
    let polarization = input.polarization().map_err(metadata_error)?;
    let scan_numbers = scans(input, &rows)?;
    let mut sizes = BTreeMap::new();
    let mut pairs = BTreeMap::<_, Pair>::new();
    for (row, scan) in rows.into_iter().zip(scan_numbers) {
        let ddi = row.data_desc_id;
        let cells = match sizes.entry(ddi) {
            std::collections::btree_map::Entry::Occupied(value) => *value.get(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                let channels = spectra
                    .num_chan(row.spectral_window_id as usize)
                    .map_err(metadata_error)?;
                let correlations = polarization
                    .num_corr(row.polarization_id as usize)
                    .map_err(metadata_error)?;
                *entry.insert(channels.max(0) as u64 * correlations.max(0) as u64)
            }
        };
        let pair = pairs.entry((scan, ddi)).or_insert_with(|| Pair {
            field: row.field_id,
            ..Pair::default()
        });
        pair.cost = pair.cost.saturating_add(cells);
        pair.rows.push(row);
    }
    let mut pairs = pairs.into_iter().collect::<Vec<_>>();
    pairs.sort_unstable_by_key(|((scan, ddi), pair)| {
        Reverse((pair.cost, scan.to_string(), ddi.to_string()))
    });
    let mut members = (0..count.min(pairs.len()))
        .map(|_| Member::default())
        .collect::<Vec<_>>();
    for ((scan, ddi), pair) in pairs {
        let member = members
            .iter_mut()
            .min_by_key(|member| {
                let already_allocated = member
                    .scans
                    .get(&scan)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(member.descriptions.get(&ddi).copied().unwrap_or(0))
                    .saturating_add(member.fields.get(&pair.field).copied().unwrap_or(0));
                (already_allocated, member.cost)
            })
            .expect("each nonempty plan has a member");
        member.cost = member.cost.saturating_add(pair.cost);
        for entry in [
            member.scans.entry(scan).or_default(),
            member.descriptions.entry(ddi).or_default(),
            member.fields.entry(pair.field).or_default(),
        ] {
            *entry = entry.saturating_add(pair.cost);
        }
        member.groups.push(((scan, ddi), pair.rows));
    }
    Ok(members.into_iter().map(finish_member).collect())
}

fn finish_member(member: Member) -> Partition {
    let mut rows = Vec::new();
    let mut descriptions = Vec::<(i32, BTreeSet<i32>)>::new();
    for ((scan, ddi), mut group) in member.groups {
        if let Some((_, scans)) = descriptions.iter_mut().find(|(id, _)| *id == ddi) {
            scans.insert(scan);
        } else {
            descriptions.push((ddi, BTreeSet::from([scan])));
        }
        rows.append(&mut group);
    }
    rows.sort_unstable_by_key(|row| row.row_index);
    let expression = taql(descriptions.into_iter().map(|(ddi, scans)| {
        let values = scans
            .into_iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        format!("(DATA_DESC_ID=={ddi} && (SCAN_NUMBER IN [{values}]))")
    }));
    Partition {
        rows,
        history: Some(("taql", expression)),
    }
}

fn metadata_error(error: casa_ms::MsError) -> MsTransformError {
    MsTransformError::Spectrum(error.to_string())
}
