// SPDX-License-Identifier: LGPL-3.0-or-later
//! Coordinate-system, legacy `coords` record, and per-plane beam reduction for
//! `imsubimage`.

use std::collections::{BTreeMap, BTreeSet};

use casa_coordinates::{CoordinateSystem, CoordinateType, StokesType};
use casa_types::{ArrayValue, RecordField, RecordValue, Value};

use crate::analysis::axes::subimage_coordinates_with_stokes;
use crate::{ImageBeamSet, ImageError, ImageInfo};

use super::super::axes::{replace_stokes_in_record, section_record_reference_pixels};

pub(super) struct CoordReductionSpec<'a> {
    pub(super) full_shape: &'a [usize],
    pub(super) start: &'a [usize],
    pub(super) shape: &'a [usize],
    pub(super) stride: Option<&'a [usize]>,
    pub(super) kept_axes: &'a [usize],
    pub(super) selected_stokes: Option<&'a [StokesType]>,
}

pub(super) fn reduce_coordinates_and_beams(
    input_coords: &CoordinateSystem,
    native: Option<RecordValue>,
    spec: CoordReductionSpec<'_>,
    mut image_info: ImageInfo,
) -> Result<(CoordinateSystem, Option<RecordValue>, ImageInfo), ImageError> {
    let ndim = spec.full_shape.len();
    let section_record = |mut record: RecordValue| -> Result<RecordValue, ImageError> {
        section_record_reference_pixels(&mut record, spec.start, spec.shape, spec.stride)?;
        if let Some(stokes) = spec.selected_stokes {
            replace_stokes_in_record(&mut record, stokes)?;
        }
        Ok(record)
    };
    let shifted = if spec.stride.is_some() {
        input_coords.restore_record_with_layout(&section_record(input_coords.to_record())?)?
    } else {
        subimage_coordinates_with_stokes(
            input_coords,
            spec.start,
            spec.shape,
            spec.selected_stokes,
        )?
    };
    let shifted_legacy = native.and_then(|record| section_record(record).ok());

    let (out_coords, out_legacy) = if spec.kept_axes.len() == ndim {
        (shifted, shifted_legacy)
    } else {
        let record = shifted_legacy.unwrap_or_else(|| shifted.to_record());
        let reduced = reduce_subimage_coordinate_record(&record, ndim, spec.kept_axes)?;
        (CoordinateSystem::from_record(&reduced)?, Some(reduced))
    };

    image_info.beam_set = subset_beam_section(
        &image_info.beam_set,
        input_coords,
        ndim,
        spec.start,
        spec.shape,
        spec.stride,
    )?;

    Ok((out_coords, out_legacy, image_info))
}

/// Reduce a native subimage coordinate record to the retained pixel axes,
/// preserving independent world order and any mapped local replacements.
///
/// # Errors
/// The source coordinate record cannot be sliced on the selected axes.
pub fn reduce_subimage_coordinate_record(
    record: &RecordValue,
    ndim: usize,
    kept_axes: &[usize],
) -> Result<RecordValue, ImageError> {
    drop_axes_from_legacy_record(record, ndim, kept_axes).ok_or_else(|| {
        ImageError::InvalidMetadata("could not reduce the subimage axis maps (#1002)".into())
    })
}

fn find_system_axis(coords: &CoordinateSystem, kind: CoordinateType) -> Option<usize> {
    coords
        .coordinate_pixel_axes(coords.find_coordinate(kind)?)?
        .into_iter()
        .flatten()
        .next()
}

pub(in crate::analysis) fn subset_beam_set(
    beam_set: &ImageBeamSet,
    input_coords: &CoordinateSystem,
    ndim: usize,
    start: &[usize],
    shape: &[usize],
) -> Result<ImageBeamSet, ImageError> {
    subset_beam_section(beam_set, input_coords, ndim, start, shape, None)
}

pub(in crate::analysis) fn subset_beam_section(
    beam_set: &ImageBeamSet,
    input_coords: &CoordinateSystem,
    ndim: usize,
    start: &[usize],
    shape: &[usize],
    stride: Option<&[usize]>,
) -> Result<ImageBeamSet, ImageError> {
    if !beam_set.is_multi() {
        return Ok(beam_set.clone());
    }
    let [channels, stokes] =
        beam_section_indices(beam_set, input_coords, ndim, start, shape, stride);
    beam_set.subset(&channels, &stokes)
}

pub(in crate::analysis) fn beam_section_indices(
    beam_set: &ImageBeamSet,
    input_coords: &CoordinateSystem,
    ndim: usize,
    start: &[usize],
    shape: &[usize],
    stride: Option<&[usize]>,
) -> [Vec<usize>; 2] {
    beam_section_indices_for_shape(
        [beam_set.n_channels(), beam_set.n_stokes()],
        input_coords,
        ndim,
        start,
        shape,
        stride,
    )
}

pub(super) fn beam_section_indices_for_shape(
    [channels, stokes]: [usize; 2],
    input_coords: &CoordinateSystem,
    ndim: usize,
    start: &[usize],
    shape: &[usize],
    stride: Option<&[usize]>,
) -> [Vec<usize>; 2] {
    let indices = |kind, length: usize| match find_system_axis(input_coords, kind) {
        Some(axis) if axis < ndim && length > 1 => {
            let step = stride.map_or(1, |stride| stride[axis]);
            (0..shape[axis])
                .map(|index| (start[axis] + step * index).min(length - 1))
                .collect::<Vec<_>>()
        }
        _ => vec![0],
    };
    [
        indices(CoordinateType::Spectral, channels),
        indices(CoordinateType::Stokes, stokes),
    ]
}

fn strip_index_suffix(field: &str) -> (&str, Option<usize>) {
    let digits_start = field
        .rfind(|ch: char| !ch.is_ascii_digit())
        .map_or(0, |pos| pos + 1);
    let prefix = &field[..digits_start];
    let suffix = field[digits_start..].parse::<usize>().ok();
    (prefix, suffix)
}

pub(in crate::analysis) fn drop_axes_from_legacy_record(
    record: &RecordValue,
    ndim: usize,
    kept_axes: &[usize],
) -> Option<RecordValue> {
    let placed = CoordinateSystem::record_coordinates(record).ok()?;
    let axis_remap: Vec<Option<usize>> = (0..ndim)
        .map(|old_ax| kept_axes.iter().position(|&k| k == old_ax))
        .collect();

    struct LegacyCoordEntry<'a> {
        prefix: &'a str,
        old_index: usize,
        coord_val: &'a Value,
        pixelmap: Vec<i32>,
        worldmap: Vec<i32>,
        pixelreplace: Option<&'a Value>,
        worldreplace: Option<&'a Value>,
    }

    let mut kept_entries: Vec<LegacyCoordEntry<'_>> = Vec::new();
    let mut surviving_worlds: BTreeSet<i32> = BTreeSet::new();

    for entry in &placed {
        let (prefix, Some(old_idx)) = strip_index_suffix(&entry.field) else {
            return None;
        };
        let has_kept = entry
            .pixel_axes
            .iter()
            .any(|ax| ax.is_some_and(|a| axis_remap.get(a).copied().flatten().is_some()));
        if !has_kept {
            continue;
        }
        let coord_val = record.get(&entry.field)?;
        let pixelmap = read_i32_array(record.get(&format!("pixelmap{old_idx}"))?)?;
        let worldmap = read_i32_array(record.get(&format!("worldmap{old_idx}"))?)?;
        for (j, &pax) in pixelmap.iter().enumerate() {
            let retained = pax < 0 || axis_remap.get(pax as usize).copied().flatten().is_some();
            if retained
                && let Some(&wax) = worldmap.get(j)
                && wax >= 0
            {
                surviving_worlds.insert(wax);
            }
        }
        kept_entries.push(LegacyCoordEntry {
            prefix,
            old_index: old_idx,
            coord_val,
            pixelmap,
            worldmap,
            pixelreplace: record.get(&format!("pixelreplace{old_idx}")),
            worldreplace: record.get(&format!("worldreplace{old_idx}")),
        });
    }

    // Preserve creation index order when renumbering 0..kept.
    kept_entries.sort_by_key(|e| e.old_index);
    let world_remap: BTreeMap<i32, i32> = surviving_worlds
        .into_iter()
        .enumerate()
        .map(|(new_w, old_w)| (old_w, new_w as i32))
        .collect();

    let mut out_fields = Vec::new();
    for field in record.fields() {
        let name = &field.name;
        let is_coord_key = name.starts_with("pixelmap")
            || name.starts_with("worldmap")
            || name.starts_with("pixelreplace")
            || name.starts_with("worldreplace")
            || name.starts_with("coordinate")
            || [
                "direction",
                "spectral",
                "stokes",
                "linear",
                "tabular",
                "quality",
            ]
            .iter()
            .any(|p| name.starts_with(p) && name[p.len()..].chars().all(|c| c.is_ascii_digit()));
        if !is_coord_key {
            out_fields.push(field.clone());
        }
    }

    for (new_idx, entry) in kept_entries.into_iter().enumerate() {
        let mut new_pmap = Vec::with_capacity(entry.pixelmap.len());
        let mut new_wmap = Vec::with_capacity(entry.worldmap.len());
        for j in 0..entry.pixelmap.len() {
            let old_p = entry.pixelmap[j];
            let old_w = entry.worldmap.get(j).copied().unwrap_or(old_p);
            let kept_pixel = usize::try_from(old_p)
                .ok()
                .and_then(|pixel| axis_remap.get(pixel).copied().flatten());
            new_pmap.push(match (old_p < 0, kept_pixel) {
                (true, _) => old_p,
                (_, Some(pixel)) => pixel as i32,
                _ => -(old_p + 1),
            });
            new_wmap.push(if old_w < 0 {
                old_w
            } else if old_p < 0 || kept_pixel.is_some() {
                *world_remap.get(&old_w)?
            } else {
                -(old_w + 1)
            });
        }
        out_fields.push(RecordField::new(
            format!("{}{new_idx}", entry.prefix),
            entry.coord_val.clone(),
        ));
        out_fields.push(RecordField::new(
            format!("worldmap{new_idx}"),
            Value::Array(ArrayValue::from_i32_vec(new_wmap)),
        ));
        out_fields.push(RecordField::new(
            format!("pixelmap{new_idx}"),
            Value::Array(ArrayValue::from_i32_vec(new_pmap)),
        ));
        for (kind, field, replacement, map) in [
            ("pixel", "crpix", entry.pixelreplace, &entry.pixelmap),
            ("world", "crval", entry.worldreplace, &entry.worldmap),
        ] {
            let Some(replacement) = replacement else {
                continue;
            };
            let mut value = replacement.clone();
            if entry.prefix == "direction"
                && let Value::Record(direction) = entry.coord_val
                && let Value::Array(ArrayValue::Float64(values)) = &mut value
                && let Some(Value::Array(ArrayValue::Float64(reference))) = direction.get(field)
            {
                // Rebuilding the surviving direction coordinate uses its
                // reference values for newly removed axes (dropRemovedAxes).
                for (axis, &old) in map.iter().enumerate() {
                    let retained = if kind == "pixel" {
                        usize::try_from(old)
                            .ok()
                            .is_some_and(|pixel| axis_remap.get(pixel).copied().flatten().is_some())
                    } else {
                        world_remap.contains_key(&old)
                    };
                    if old >= 0 && !retained {
                        values[axis] = reference[axis];
                    }
                }
            }
            out_fields.push(RecordField::new(format!("{kind}replace{new_idx}"), value));
        }
    }

    let out = RecordValue::new(out_fields);
    Some(out)
}

fn read_i32_array(value: &Value) -> Option<Vec<i32>> {
    match value {
        Value::Array(ArrayValue::Int32(arr)) => Some(arr.iter().copied().collect()),
        Value::Array(ArrayValue::Int64(arr)) => {
            arr.iter().map(|&v| i32::try_from(v).ok()).collect()
        }
        _ => None,
    }
}
