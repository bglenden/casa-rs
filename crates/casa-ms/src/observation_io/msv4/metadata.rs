// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MSv4 profile's metadata conventions: measure attributes, string
//! coordinates and the data-group record.

use std::sync::Arc;

use casa_observation::{ObservationError, VisibilityUnits};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{EpochRef, MjdHighPrec};
use serde_json::{Map, Value};
use zarrs::array::{Array, ArrayMetadata, ArraySubset};
use zarrs::filesystem::FilesystemStore;
use zarrs::storage::{ReadableStorageTraits, StoreKey};

/// The schema version this profile reads and writes.
pub(crate) const SCHEMA_VERSION: &str = "4.0.1";

/// MJD of the Unix epoch, the origin of `format: unix` times.
pub(crate) const UNIX_EPOCH_MJD: f64 = 40_587.0;

pub(crate) fn invalid(reason: impl Into<String>) -> ObservationError {
    ObservationError::InvalidStore {
        reason: reason.into(),
    }
}

pub(crate) fn storage(error: impl std::error::Error + Send + Sync + 'static) -> ObservationError {
    ObservationError::Storage(Box::new(error))
}

/// A string attribute.
pub(crate) fn text<'a>(
    attributes: &'a Map<String, Value>,
    name: &str,
    of: &str,
) -> Result<&'a str, ObservationError> {
    attributes
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid(format!("{of} has no string attribute {name}")))
}

/// Require `units` to be `expected`: the profile's canonical unit, which
/// the reader does not convert.
pub(crate) fn require_units(
    attributes: &Map<String, Value>,
    expected: &str,
    of: &str,
) -> Result<(), ObservationError> {
    let units = text(attributes, "units", of)?;
    if units == expected {
        Ok(())
    } else {
        Err(invalid(format!(
            "{of} is in {units}; the MSv4 schema requires {expected}"
        )))
    }
}

/// The time scale and origin of a `time` measure.
///
/// XRADIO defines its time attributes by astropy's `Time` (schema
/// `measures.py`, `TimeArray`), and astropy's `unix` format counts seconds
/// from 1970-01-01 00:00:00 *UTC* whatever the requested scale: with
/// `scale: tai`, the represented instant is not the Unix epoch read as TAI
/// plus the offset, and re-expressing it needs the leap-second table. The
/// trial profile reads `format: unix` with `scale: utc` only, where the
/// origin is the Unix epoch in UTC and the offsets are UTC seconds, and
/// refuses the rest rather than mislabel the instant.
pub(crate) fn time_reference(
    attributes: &Map<String, Value>,
    of: &str,
) -> Result<(EpochRef, MjdHighPrec), ObservationError> {
    require_units(attributes, "s", of)?;
    if text(attributes, "format", of)? != "unix" {
        return Err(ObservationError::Unsupported {
            what: "an MSv4 time format other than unix",
        });
    }
    if text(attributes, "scale", of)? != "utc" {
        return Err(ObservationError::Unsupported {
            what: "an MSv4 unix-format time in a scale other than utc (astropy's unix epoch is UTC)",
        });
    }
    Ok((EpochRef::UTC, MjdHighPrec::new(UNIX_EPOCH_MJD, 0.0)))
}

/// The time measure attributes the writer records: `format: unix`,
/// `scale: utc`, the only pair whose instant the writer's arithmetic
/// represents ([`time_reference`]).
pub(crate) fn time_attributes(scale: EpochRef) -> Result<Map<String, Value>, ObservationError> {
    if scale != EpochRef::UTC {
        return Err(ObservationError::Unsupported {
            what: "writing MSv4 times in a scale other than UTC",
        });
    }
    let mut attributes = Map::new();
    attributes.insert("type".into(), "time".into());
    attributes.insert("units".into(), "s".into());
    attributes.insert("scale".into(), "utc".into());
    attributes.insert("format".into(), "unix".into());
    Ok(attributes)
}

/// Require `array` to have Zarr data type `data_type` and dimensions
/// `dimensions`, as the profile defines the variable at `path`. Anything
/// else is refused before a byte is decoded: the reader's decode is a raw
/// byte copy, which would reinterpret a same-width type silently.
pub(crate) fn require_layout(
    array: &Array<FilesystemStore>,
    path: &str,
    data_type: &str,
    dimensions: &[&str],
) -> Result<(), ObservationError> {
    let ArrayMetadata::V3(metadata) = array.metadata() else {
        return Err(ObservationError::Unsupported {
            what: "an MSv4 variable that is not Zarr v3",
        });
    };
    let stored = metadata.data_type.name();
    if stored != data_type {
        return Err(invalid(format!(
            "{path} is {stored}; the trial profile reads it only as {data_type}"
        )));
    }
    let names: Option<Vec<&str>> = metadata.dimension_names.as_ref().map(|names| {
        names
            .iter()
            .map(|name| name.as_deref().unwrap_or(""))
            .collect()
    });
    if names.as_deref() != Some(dimensions) {
        return Err(invalid(format!(
            "{path} has dimensions {names:?}, not {dimensions:?}"
        )));
    }
    Ok(())
}

/// XRADIO's spelling of a spectral frame (`observer`) and back. Lowercase
/// names are astropy's; XRADIO keeps the frames astropy lacks in upper case.
const FREQUENCY_FRAMES: [(&str, FrequencyRef); 9] = [
    ("REST", FrequencyRef::REST),
    ("lsrk", FrequencyRef::LSRK),
    ("lsrd", FrequencyRef::LSRD),
    ("BARY", FrequencyRef::BARY),
    ("gcrs", FrequencyRef::GEO),
    ("TOPO", FrequencyRef::TOPO),
    ("GALACTO", FrequencyRef::GALACTO),
    ("LGROUP", FrequencyRef::LGROUP),
    ("CMB", FrequencyRef::CMB),
];

pub(crate) fn frequency_frame(
    attributes: &Map<String, Value>,
    of: &str,
) -> Result<FrequencyRef, ObservationError> {
    require_units(attributes, "Hz", of)?;
    let observer = text(attributes, "observer", of)?;
    FREQUENCY_FRAMES
        .iter()
        .find(|(name, _)| *name == observer)
        .map(|(_, frame)| *frame)
        .ok_or_else(|| invalid(format!("{of} has unknown observer {observer}")))
}

pub(crate) fn frequency_observer(frame: FrequencyRef) -> &'static str {
    FREQUENCY_FRAMES
        .iter()
        .find(|(_, known)| *known == frame)
        .map(|(name, _)| *name)
        .expect("every frequency frame has an MSv4 name")
}

/// XRADIO's `frame` for a direction or UVW measure.
const DIRECTION_FRAMES: [(&str, DirectionRef); 2] =
    [("fk5", DirectionRef::J2000), ("icrs", DirectionRef::ICRS)];

pub(crate) fn direction_frame(
    attributes: &Map<String, Value>,
    of: &str,
) -> Result<DirectionRef, ObservationError> {
    let frame = text(attributes, "frame", of)?;
    DIRECTION_FRAMES
        .iter()
        .find(|(name, _)| *name == frame)
        .map(|(_, reference)| *reference)
        .ok_or(ObservationError::Unsupported {
            what: "an MSv4 direction or UVW frame other than fk5 or icrs",
        })
}

pub(crate) fn direction_name(reference: DirectionRef) -> Result<&'static str, ObservationError> {
    DIRECTION_FRAMES
        .iter()
        .find(|(_, known)| *known == reference)
        .map(|(name, _)| *name)
        .ok_or(ObservationError::Unsupported {
            what: "writing an MSv4 direction or UVW frame other than J2000 (fk5) or ICRS",
        })
}

/// What a correlated-data variable's `units` attribute says.
pub(crate) fn visibility_units(attributes: &Map<String, Value>) -> VisibilityUnits {
    match attributes.get("units").and_then(Value::as_str) {
        Some("Jy") => VisibilityUnits::Jansky,
        Some(other) => VisibilityUnits::Declared(other.to_string()),
        None => VisibilityUnits::Undeclared,
    }
}

/// Whether the store has node (group or array) `path`: a node absent is
/// `false`, a store that cannot say an error.
pub(crate) fn has_node(store: &Arc<FilesystemStore>, path: &str) -> Result<bool, ObservationError> {
    let key =
        StoreKey::new(format!("{}/zarr.json", path.trim_start_matches('/'))).map_err(storage)?;
    store
        .size_key(&key)
        .map(|size| size.is_some())
        .map_err(storage)
}

/// Open array `path`.
pub(crate) fn open_array(
    store: &Arc<FilesystemStore>,
    path: &str,
) -> Result<Array<FilesystemStore>, ObservationError> {
    Array::open(store.clone(), path).map_err(storage)
}

/// Largest decoded physical chunk of `array`. A regular grid has one shape;
/// varying grids are inspected up to a bounded number of chunks so opening
/// never understates a chunk extending beyond the logical array extent.
pub(crate) fn chunk_bytes(array: &Array<FilesystemStore>) -> Result<usize, ObservationError> {
    let grid = array.chunk_grid();
    let metadata = serde_json::to_value(grid.metadata()).map_err(storage)?;
    let size = array.data_type().fixed_size().unwrap_or(0);
    let bytes = |shape: &[u64]| {
        shape.iter().fold(size, |product, extent| {
            product.saturating_mul(usize::try_from(*extent).unwrap_or(usize::MAX))
        })
    };
    let chunk = |indices: &[u64]| -> Result<usize, ObservationError> {
        let shape = grid.chunk_shape_u64(indices).map_err(storage)?.ok_or(
            ObservationError::Unsupported {
                what: "an MSv4 chunk grid without known chunk shapes",
            },
        )?;
        Ok(bytes(&shape))
    };
    let mut indices = vec![0; array.shape().len()];
    if metadata.get("name").and_then(Value::as_str) == Some("regular") {
        return chunk(&indices);
    }
    let grid_shape = grid.grid_shape();
    let chunks = grid_shape.iter().fold(1_usize, |count, extent| {
        count.saturating_mul(usize::try_from(*extent).unwrap_or(usize::MAX))
    });
    if chunks > 4096 {
        return Err(ObservationError::Unsupported {
            what: "an MSv4 varying chunk grid with more than 4096 chunks",
        });
    }
    let mut most = 0;
    for _ in 0..chunks {
        most = most.max(chunk(&indices)?);
        for (index, extent) in indices.iter_mut().zip(grid_shape).rev() {
            *index += 1;
            if *index < *extent {
                break;
            }
            *index = 0;
        }
    }
    Ok(most)
}

/// Every element of a one-dimensional `fixed_length_utf32` coordinate.
pub(crate) fn strings(
    store: &Arc<FilesystemStore>,
    path: &str,
) -> Result<Vec<String>, ObservationError> {
    let array = open_array(store, path)?;
    let subset = ArraySubset::new_with_shape(array.shape().to_vec());
    let values: Vec<Vec<char>> = array
        .retrieve_array_subset_opt(&subset, &super::codec_options())
        .map_err(storage)?;
    Ok(values
        .into_iter()
        .map(|chars| chars.into_iter().collect())
        .collect())
}

/// Every element of a numeric coordinate or small variable.
pub(crate) fn numbers<T: zarrs::array::ElementOwned>(
    store: &Arc<FilesystemStore>,
    path: &str,
) -> Result<(Vec<T>, Map<String, Value>), ObservationError> {
    let array = open_array(store, path)?;
    let subset = ArraySubset::new_with_shape(array.shape().to_vec());
    let values = array
        .retrieve_array_subset_opt(&subset, &super::codec_options())
        .map_err(storage)?;
    Ok((values, array.attributes().clone()))
}

/// One data group of a dataset's `data_groups` attribute.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DataGroupRecord {
    pub(crate) name: String,
    pub(crate) correlated_data: String,
    pub(crate) flag: String,
    pub(crate) weight: String,
    pub(crate) uvw: String,
    pub(crate) field_and_source: String,
}

pub(crate) fn data_groups(
    attributes: &Map<String, Value>,
) -> Result<Vec<DataGroupRecord>, ObservationError> {
    let groups = attributes
        .get("data_groups")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("the dataset has no data_groups"))?;
    groups
        .iter()
        .map(|(name, group)| {
            let group = group
                .as_object()
                .ok_or_else(|| invalid(format!("data group {name} is not a record")))?;
            let role = |role: &str| text(group, role, name).map(str::to_string);
            Ok(DataGroupRecord {
                name: name.clone(),
                correlated_data: role("correlated_data")?,
                flag: role("flag")?,
                weight: role("weight")?,
                uvw: role("uvw")?,
                field_and_source: role("field_and_source")?,
            })
        })
        .collect()
}
