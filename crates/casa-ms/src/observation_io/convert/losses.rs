// SPDX-License-Identifier: LGPL-3.0-or-later
//! What a source holds that a conversion cannot carry, found before
//! anything is written.
//!
//! The inventory is of whole tables and columns: an MSv2's `SIGMA`,
//! `FLAG_ROW` flags, `FLAG_CATEGORY` cells, MAIN columns outside the
//! boundary's roles and subtables the catalogue does not carry (pointing,
//! system calibration, weather, flag commands, history, Doppler, frequency
//! offsets, sources, and any further subtable a keyword names); an MSv4's
//! sub-datasets and main-dataset variables the writer does not write; and
//! in either the data groups not selected. Columns of the subtables the
//! catalogue does carry (an antenna's offset, a field's delay direction, a
//! spectral window's resolution) are not inventoried.

use std::path::Path;

use casa_observation::{Catalogue, ObservationError};
use casa_tables::{RequiredScalarColumnDestination, RequiredScalarColumnValuesMut};
use casa_types::Value;

use super::{Loss, LossKind};
use crate::observation_io::update::{request_error, storage};
use crate::schema::SubtableId;

/// The data groups to convert: `requested`, each present in every
/// partition, or every group of the source with `base` first.
pub(super) fn selected_groups(
    catalogue: &Catalogue,
    requested: Option<&[String]>,
) -> Result<Vec<String>, ObservationError> {
    let groups: Vec<String> = match requested {
        Some(requested) => requested.to_vec(),
        None => {
            let mut groups = vec!["base".to_string()];
            for entry in &catalogue.partitions {
                for group in &entry.data_groups {
                    if !groups.contains(&group.name) {
                        groups.push(group.name.clone());
                    }
                }
            }
            groups
        }
    };
    if groups.is_empty() {
        return Err(request_error(
            "a conversion selects at least one data group",
        ));
    }
    for entry in &catalogue.partitions {
        if let Some(missing) = groups
            .iter()
            .find(|group| entry.data_group(group).is_none())
        {
            return Err(request_error(format!(
                "partition {} has no data group {missing}",
                entry.spectral_window
            )));
        }
    }
    Ok(groups)
}

/// Everything `source` holds that converting `groups` would not carry.
pub(super) fn inventory(
    source: &Path,
    catalogue: &Catalogue,
    groups: &[String],
) -> Result<Vec<Loss>, ObservationError> {
    let mut losses = Vec::new();
    for entry in &catalogue.partitions {
        for group in &entry.data_groups {
            let loss = Loss::DataGroup(group.name.clone());
            if !groups.contains(&group.name) && !losses.contains(&loss) {
                losses.push(loss);
            }
        }
    }
    if source.join("zarr.json").is_file() {
        msv4(source, &mut losses)?;
    } else {
        msv2(source, &mut losses)?;
    }
    losses.sort();
    losses.dedup();
    Ok(losses)
}

/// Refuse the first of `losses` whose kind is not in `accept`.
pub(super) fn refuse_unaccepted(
    losses: &[Loss],
    accept: &[LossKind],
) -> Result<(), ObservationError> {
    match losses.iter().find(|loss| !accept.contains(&loss.kind())) {
        Some(loss) => Err(request_error(format!(
            "the conversion would lose information: {loss}; accept {:?} to convert anyway",
            loss.kind()
        ))),
        None => Ok(()),
    }
}

/// MAIN columns the conversion carries: the keys the partitions and grid
/// are made of, the time auxiliaries and the data groups' roles.
const CARRIED_COLUMNS: &[&str] = &[
    "ANTENNA1",
    "ANTENNA2",
    "ARRAY_ID",
    "DATA_DESC_ID",
    "EXPOSURE",
    "FEED1",
    "FEED2",
    "FIELD_ID",
    "FLAG",
    "FLAG_ROW",
    "INTERVAL",
    "OBSERVATION_ID",
    "PROCESSOR_ID",
    "SCAN_NUMBER",
    "STATE_ID",
    "TIME",
    "TIME_CENTROID",
    "UVW",
    "WEIGHT",
    "DATA",
    "CORRECTED_DATA",
    "MODEL_DATA",
    "WEIGHT_SPECTRUM",
];

/// Subtables whose content the catalogue does not carry.
const UNCARRIED_SUBTABLES: &[SubtableId] = &[
    SubtableId::Pointing,
    SubtableId::SysCal,
    SubtableId::Weather,
    SubtableId::FlagCmd,
    SubtableId::History,
    SubtableId::Doppler,
    SubtableId::FreqOffset,
    SubtableId::Source,
];

/// Rows of `FLAG_ROW` read at a time.
const ROW_CHUNK: usize = 1 << 18;

fn msv2(source: &Path, losses: &mut Vec<Loss>) -> Result<(), ObservationError> {
    let ms = crate::MeasurementSet::open_retained_read(source).map_err(storage)?;
    let main = ms.main_table();
    let columns: Vec<String> = main
        .schema()
        .map(|schema| {
            schema
                .columns()
                .iter()
                .map(|column| column.name().to_string())
                .collect()
        })
        .unwrap_or_default();
    for column in &columns {
        match column.as_str() {
            "SIGMA" | "SIGMA_SPECTRUM" => losses.push(Loss::Sigma),
            "FLAG_CATEGORY" => {
                if flag_categories_held(main)? {
                    losses.push(Loss::FlagCategories);
                }
            }
            name if !CARRIED_COLUMNS.contains(&name) => losses.push(Loss::Column(name.to_string())),
            _ => {}
        }
    }
    if columns.iter().any(|name| name == "WEIGHT_SPECTRUM") {
        losses.push(Loss::Column(
            "WEIGHT (the row summary beside WEIGHT_SPECTRUM)".to_string(),
        ));
    }
    let rows = main.row_count();
    let mut flags = Vec::new();
    for start in (0..rows).step_by(ROW_CHUNK) {
        let chunk: Vec<usize> = (start..rows.min(start + ROW_CHUNK)).collect();
        flags.clear();
        let mut destinations = [RequiredScalarColumnDestination::new(
            "FLAG_ROW",
            RequiredScalarColumnValuesMut::Bool(&mut flags),
        )];
        main.required_scalar_columns_for_rows_into(&chunk, &mut destinations)
            .map_err(storage)?;
        if flags.iter().any(|flag| *flag) {
            losses.push(Loss::RowFlags);
            break;
        }
    }
    for id in UNCARRIED_SUBTABLES {
        if ms.subtable(*id).is_some_and(|table| table.row_count() > 0) {
            losses.push(Loss::Table(id.name().to_string()));
        }
    }
    let standard: Vec<&str> = SubtableId::ALL_REQUIRED
        .iter()
        .chain(UNCARRIED_SUBTABLES)
        .map(SubtableId::name)
        .collect();
    for field in main.keywords().fields() {
        if matches!(field.value, Value::TableRef(_)) && !standard.contains(&field.name.as_str()) {
            losses.push(Loss::Table(field.name.clone()));
        }
    }
    Ok(())
}

/// Whether MAIN's `FLAG_CATEGORY` holds categories: a defined cell with
/// elements, at the first or the last row (writers fill the column whole
/// or leave it undefined throughout).
fn flag_categories_held(main: &casa_tables::Table) -> Result<bool, ObservationError> {
    let rows = main.row_count();
    if rows == 0 {
        return Ok(false);
    }
    // Uncached: the inventory then reads FLAG_ROW from the lazy table.
    let cells = main
        .column_accessor("FLAG_CATEGORY")
        .and_then(|column| column.array_cells_owned_uncached(&[0, rows - 1]))
        .map_err(storage)?;
    Ok(cells.iter().flatten().any(|cell| !cell.is_empty()))
}

/// The main dataset's variables the writer writes, coordinates included.
const CARRIED_VARIABLES: &[&str] = &[
    "time",
    "baseline_id",
    "baseline_antenna1_name",
    "baseline_antenna2_name",
    "frequency",
    "polarization",
    "uvw_label",
    "field_name",
    "scan_name",
    "FLAG",
    "WEIGHT",
    "UVW",
    "TIME_CENTROID",
    "EFFECTIVE_INTEGRATION_TIME",
];

fn msv4(source: &Path, losses: &mut Vec<Loss>) -> Result<(), ObservationError> {
    let datasets = std::fs::read_dir(source).map_err(storage)?;
    for dataset in datasets {
        let dataset = dataset.map_err(storage)?.path();
        if !dataset.join("zarr.json").is_file() {
            continue;
        }
        for child in std::fs::read_dir(&dataset).map_err(storage)? {
            let child = child.map_err(storage)?.path();
            let Some(name) = child
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
            else {
                continue;
            };
            if !child.join("zarr.json").is_file() {
                continue;
            }
            let is_group = std::fs::read_to_string(child.join("zarr.json")).is_ok_and(|metadata| {
                metadata.contains("\"node_type\": \"group\"")
                    || metadata.contains("\"node_type\":\"group\"")
            });
            if is_group {
                if name != "antenna_xds" && !name.starts_with("field_and_source_") {
                    losses.push(Loss::Table(name));
                }
            } else if !CARRIED_VARIABLES.contains(&name.as_str()) && !name.starts_with("VISIBILITY")
            {
                losses.push(Loss::Column(name));
            }
        }
    }
    Ok(())
}
