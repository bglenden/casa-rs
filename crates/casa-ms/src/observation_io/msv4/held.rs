// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounds on what opening an MSv4 processing set holds, taken from its
//! metadata before a value is read: every array's and group's metadata,
//! and each dataset's coordinates and descriptions, which the opening
//! reads whole. Its visibilities and their companions over time and
//! baseline are read in batches, and pointing, weather and the other
//! datasets only when asked for.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use casa_observation::ObservationError;
use zarrs::array::ArrayMetadata;
use zarrs_filesystem::FilesystemStore;

use super::metadata::{open_array, storage};

/// Times their files' size the processing set's metadata (`zarr.json`)
/// holds once parsed: array descriptions and attributes.
const METADATA_FACTOR: usize = 16;

/// What the processing set at `path` holds of its metadata once opened.
pub(super) fn metadata_bytes(path: &Path) -> usize {
    METADATA_FACTOR * metadata_files(path, 3)
}

/// The bytes of the `zarr.json` files at most `depth` levels below
/// `directory`, its own included.
fn metadata_files(directory: &Path, depth: usize) -> usize {
    let own = fs::metadata(directory.join("zarr.json")).map_or(0, |meta| meta.len() as usize);
    if depth == 0 {
        return own;
    }
    let Ok(entries) = fs::read_dir(directory) else {
        return own;
    };
    own + entries
        .flatten()
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .map(|entry| metadata_files(&entry.path(), depth - 1))
        .sum::<usize>()
}

/// What reading the opening's arrays of dataset `dataset` (`/name`) of the
/// processing set at `root` decodes: every array of the dataset but those
/// over time and baseline, and every array of its antenna and
/// field-and-source datasets.
///
/// # Errors
///
/// A dataset directory or array metadata that cannot be read.
pub(super) fn dataset_bytes(
    store: &Arc<FilesystemStore>,
    root: &Path,
    dataset: &str,
) -> Result<usize, ObservationError> {
    let directory = root.join(dataset.trim_start_matches('/'));
    let mut bytes = 0;
    for entry in fs::read_dir(&directory).map_err(storage)?.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let path = entry.path();
        if name.starts_with("antenna_xds") || name.starts_with("field_and_source") {
            for nested in fs::read_dir(&path).map_err(storage)?.flatten() {
                if is_array(&nested.path()) {
                    let nested = nested.file_name().to_string_lossy().into_owned();
                    bytes += decoded(store, &format!("{dataset}/{name}/{nested}"), false)?;
                }
            }
        } else if is_array(&path) {
            bytes += decoded(store, &format!("{dataset}/{name}"), true)?;
        }
    }
    Ok(bytes)
}

/// Whether `directory` holds a Zarr array.
fn is_array(directory: &Path) -> bool {
    fs::read(directory.join("zarr.json"))
        .ok()
        .and_then(|text| serde_json::from_slice::<serde_json::Value>(&text).ok())
        .is_some_and(|metadata| {
            metadata.get("node_type").and_then(|kind| kind.as_str()) == Some("array")
        })
}

/// The bytes reading the array at `path` whole decodes: its elements at
/// its data type's size three times (the codec's bytes, the values, and a
/// string's text beside its characters) with a string's two headers each;
/// nothing for an array whose elements have no fixed size, which no
/// opening reads whole, or, when `batched`, for one over time and baseline.
fn decoded(
    store: &Arc<FilesystemStore>,
    path: &str,
    batched: bool,
) -> Result<usize, ObservationError> {
    let array = open_array(store, path)?;
    if batched && let ArrayMetadata::V3(metadata) = array.metadata() {
        let named = |dimension: &str| {
            metadata
                .dimension_names
                .as_ref()
                .is_some_and(|names| names.iter().any(|name| name.as_deref() == Some(dimension)))
        };
        if named("time") && named("baseline_id") {
            return Ok(0);
        }
    }
    let Some(size) = array.data_type().fixed_size() else {
        return Ok(0);
    };
    let elements = usize::try_from(array.shape().iter().product::<u64>()).unwrap_or(usize::MAX);
    Ok(elements.saturating_mul(3 * size + 2 * size_of::<String>()))
}
