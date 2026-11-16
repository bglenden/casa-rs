// SPDX-License-Identifier: LGPL-3.0-or-later
//! What opening a processing set holds: projected from each dataset's
//! array shapes and element sizes before any is decoded, so that the
//! opening can be granted first ([`super::Msv4Observation::open_granted`]),
//! and counted by capacity once it is open
//! ([`super::Msv4Observation::held_bytes`]).
//!
//! A dataset's opening decodes its per-time coordinates (`time`,
//! `field_name`, `scan_name`), its baselines and frequencies, its field
//! datasets and their ephemerides, and keeps the times, each time's scan
//! and, for each data group, each time's field. The names are
//! `fixed_length_utf32`, so their decoded size is known from the array's
//! data type. Array handles hold their metadata, which the count takes
//! from its serialized size.

use std::sync::Arc;

use casa_observation::{FieldId, OrderedBaseline, ScanId};
use zarrs::array::Array;
use zarrs::filesystem::FilesystemStore;
use zarrs::group::Group;

use super::ephemeris::EPHEMERIS_DATASET;
use super::metadata::{data_groups, has_node, open_array, text};
use super::read::{Dataset, GroupArrays};

/// What decoding holds besides the arrays it decodes, at most: the codecs'
/// contexts and the metadata documents parsed while a dataset opens.
pub(super) const WORKSPACE_BYTES: usize = 2 << 20;

/// What a dataset holds beyond its per-time, per-baseline and per-channel
/// arrays, at most, as projected before it opens: its catalogue entries,
/// names and array handles. [`dataset_bytes`] counts them once open.
const DATASET_BYTES: usize = 256 << 10;

/// What an array handle holds beside its metadata document's serialized
/// bytes: its codec chain and chunk grid.
const HANDLE_BYTES: usize = 4 << 10;

/// The heap a handle's parsed metadata takes per byte of its document.
const METADATA_FACTOR: usize = 4;

/// A dataset's opening, projected: what it holds once open, and what
/// decoding it holds beyond that while it opens.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Projection {
    pub(super) held: usize,
    pub(super) transient: usize,
}

/// The opening of the processing set whose datasets `projections` are: what
/// it holds once open and, at its peak, while the largest dataset decodes.
pub(super) fn opening_bytes(projections: &[Projection]) -> usize {
    projections.iter().map(|each| each.held).sum::<usize>()
        + projections
            .iter()
            .map(|each| each.transient)
            .max()
            .unwrap_or(0)
        + WORKSPACE_BYTES
}

/// Dataset `path`'s opening, projected from its metadata. A dataset whose
/// metadata does not state a shape projects nothing for it: opening refuses
/// it, with the reason, before decoding it.
pub(super) fn project(store: &Arc<FilesystemStore>, path: &str) -> Projection {
    let length = |name: &str| shape(store, &format!("{path}/{name}"), 0);
    let times = length("time");
    let baselines = length("baseline_id");
    let channels = length("frequency");
    let names = string_bytes(store, &format!("{path}/field_name"), times);
    let scans = has_node(store, &format!("{path}/scan_name")).unwrap_or(false);
    let scan_names = string_bytes(store, &format!("{path}/scan_name"), times);
    let records = Group::open(store.clone(), path)
        .ok()
        .and_then(|group| data_groups(group.attributes()).ok())
        .unwrap_or_default();
    let mut field_datasets: Vec<&str> = records
        .iter()
        .map(|record| record.field_and_source.as_str())
        .collect();
    field_datasets.sort_unstable();
    field_datasets.dedup();
    let (mut ephemeris_held, mut ephemeris_transient) = (0, 0);
    for name in field_datasets {
        let dataset = format!("{path}/{name}");
        let moving = Group::open(store.clone(), &dataset).is_ok_and(|group| {
            text(group.attributes(), "type", &dataset).is_ok_and(|kind| kind == EPHEMERIS_DATASET)
        });
        let fields = shape(store, &format!("{dataset}/field_name"), 0);
        ephemeris_transient += 2 * string_bytes(store, &format!("{dataset}/field_name"), fields);
        if moving {
            let samples = shape(store, &format!("{dataset}/time_ephemeris"), times);
            // Times, directions (and their pairs), distances and radial
            // velocities; and each time's phase centre while it is read.
            ephemeris_held += samples * 5 * size_of::<f64>();
            ephemeris_transient += samples * 7 * size_of::<f64>() + times * 2 * size_of::<f64>();
        }
    }
    let per_time = size_of::<f64>()
        + usize::from(scans) * size_of::<ScanId>()
        + records.len().max(1) * size_of::<FieldId>();
    Projection {
        held: times * per_time
            + baselines * size_of::<OrderedBaseline>()
            // The partition's frequencies and widths, and its window's.
            + channels * 4 * size_of::<f64>()
            + ephemeris_held
            + DATASET_BYTES,
        // The time coordinate as decoded beside its copy, and the names.
        transient: times * size_of::<f64>() + names + scan_names + ephemeris_transient,
    }
}

/// The length of the one-dimensional array at `path`, or `missing` when it
/// has none.
fn shape(store: &Arc<FilesystemStore>, path: &str, missing: usize) -> usize {
    open_array(store, path)
        .ok()
        .and_then(|array| array.shape().first().copied())
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or(missing)
}

/// What decoding `elements` names of the `fixed_length_utf32` array at
/// `path` holds at its peak: the stored code units, each name's `Vec<char>`
/// and its `String`.
fn string_bytes(store: &Arc<FilesystemStore>, path: &str, elements: usize) -> usize {
    let Some(fixed) = open_array(store, path)
        .ok()
        .and_then(|array| array.data_type().fixed_size())
    else {
        return 0;
    };
    elements * (3 * fixed + size_of::<Vec<char>>() + size_of::<String>())
}

/// What array handle `array` holds: the handle, its metadata as parsed, at
/// [`METADATA_FACTOR`] times its document's serialized bytes, and its codec
/// chain.
pub(super) fn handle_bytes(array: &Array<FilesystemStore>) -> usize {
    size_of::<Array<FilesystemStore>>()
        + HANDLE_BYTES
        + METADATA_FACTOR
            * serde_json::to_vec(&array.metadata()).map_or(0, |document| document.len())
}

/// What `dataset` holds beyond its catalogue entries, by capacity.
pub(super) fn dataset_bytes(dataset: &Dataset) -> usize {
    dataset.path.capacity()
        + dataset.times.capacity() * size_of::<f64>()
        + dataset
            .scans
            .as_ref()
            .map_or(0, |scans| scans.capacity() * size_of::<ScanId>())
        + [&dataset.centroid, &dataset.exposure]
            .into_iter()
            .flatten()
            .map(handle_bytes)
            .sum::<usize>()
        + dataset.groups.capacity() * size_of::<GroupArrays>()
        + dataset.groups.iter().map(group_bytes).sum::<usize>()
        + dataset.antennas.capacity() * size_of::<(String, casa_observation::AntennaId)>()
        + dataset
            .antennas
            .iter()
            .map(|(name, _)| name.capacity())
            .sum::<usize>()
}

/// What data group `group` holds: its record's names, each time's field
/// and its four arrays' handles.
fn group_bytes(group: &GroupArrays) -> usize {
    let record = &group.record;
    [
        &record.name,
        &record.correlated_data,
        &record.flag,
        &record.weight,
        &record.uvw,
        &record.field_and_source,
    ]
    .into_iter()
    .map(String::capacity)
    .sum::<usize>()
        + group.fields.capacity() * size_of::<FieldId>()
        + [&group.values, &group.flags, &group.weights, &group.uvw]
            .into_iter()
            .map(handle_bytes)
            .sum::<usize>()
}
