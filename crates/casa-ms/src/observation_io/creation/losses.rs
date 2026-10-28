// SPDX-License-Identifier: LGPL-3.0-or-later
//! What a source holds that a conversion cannot carry, found before
//! anything is written.
//!
//! The inventory is of whole tables and columns: an MSv2's `SIGMA`,
//! `FLAG_ROW` flags, `FLAG_CATEGORY` cells, `ARRAY_ID`s and `PROCESSOR_ID`s
//! other than 0, a `PROCESSOR` of more than one row, `STATE` rows that say
//! more than their intents, `FEED` receptors that vary in time (of which
//! the catalogue keeps the last row, as casacore's `MSIter` does), MAIN
//! columns outside the boundary's roles and subtables the catalogue does
//! not carry (system calibration, weather, flag commands, history, Doppler,
//! frequency offsets, sources, and any further subtable a keyword names);
//! an MSv4's sub-datasets and main-dataset variables the writer does not
//! write; and in either the data groups not selected; and, into an MSv4,
//! antennas no partition's baselines name. What pointing carries is
//! decided by [`super::pointing_plan`]. Columns of the subtables the
//! catalogue does carry (an antenna's offset, a field's delay direction, a
//! spectral window's resolution) are not inventoried. Fields that follow
//! an ephemeris, or whose direction is a polynomial in time, are refused:
//! the catalogue would hold an offset or a constant term as the phase
//! centre.

use std::path::Path;

use casa_observation::{Catalogue, ObservationError};
use casa_tables::{RequiredScalarColumnDestination, RequiredScalarColumnValuesMut};
use casa_types::Value;

use super::{Loss, LossKind};
use crate::observation_io::update::{request_error, storage};
use crate::schema::SubtableId;

/// The data groups to convert: `requested`, each present in every
/// partition, or every group of the source with `base` first.
pub(crate) fn selected_groups(
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
    if let Some(group) = groups
        .iter()
        .enumerate()
        .find_map(|(index, group)| groups[..index].contains(group).then_some(group))
    {
        return Err(request_error(format!(
            "data group {group} is selected twice"
        )));
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

/// The store a conversion creates, which decides what it must refuse.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Store {
    /// An MSv2, which writes every catalogue field.
    Msv2,
    /// An MSv4, which writes the fields its datasets name.
    Msv4,
}

/// Everything `source` holds that converting `groups` into `destination`
/// would not carry.
pub(crate) fn inventory(
    source: &Path,
    catalogue: &Catalogue,
    groups: &[String],
    destination: Store,
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
        msv2(source, catalogue, destination, &mut losses)?;
    }
    losses.sort();
    losses.dedup();
    Ok(losses)
}

/// The antennas of `catalogue` that no partition's baselines name, as a
/// loss when there are any: an MSv4 dataset holds only its baselines'
/// antennas.
#[cfg(feature = "msv4")]
pub(crate) fn unused_antennas(catalogue: &Catalogue) -> Option<Loss> {
    let mut named = vec![false; catalogue.antennas.len()];
    for baseline in catalogue
        .partitions
        .iter()
        .flat_map(|entry| &entry.baselines)
    {
        for antenna in [baseline.antenna1, baseline.antenna2] {
            if let Some(named) = named.get_mut(antenna.0 as usize) {
                *named = true;
            }
        }
    }
    let unused = named.iter().filter(|named| !**named).count();
    (unused > 0).then_some(Loss::UnusedAntennas(unused))
}

/// Refuse the first of `losses` whose kind is not in `accept`.
pub(crate) fn refuse_unaccepted(
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
/// `ARRAY_ID` and `PROCESSOR_ID` are carried only as 0, and reported
/// otherwise ([`main_keys`]).
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

fn msv2(
    source: &Path,
    catalogue: &Catalogue,
    destination: Store,
    losses: &mut Vec<Loss>,
) -> Result<(), ObservationError> {
    let ms = crate::MeasurementSet::open_retained_read(source).map_err(storage)?;
    refuse_moving_fields(&ms, catalogue, destination)?;
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
    main_keys(main, losses)?;
    if crate::observation_io::msv2::catalogue::read_feeds(&ms)?
        .varying()
        .is_some()
    {
        losses.push(Loss::Column(
            "FEED receptors' time dependence (the last row of each antenna, feed and window \
             is kept)"
                .to_string(),
        ));
    }
    subtables(&ms, losses)
}

/// Refuse fields whose direction moves, among those `destination` writes:
/// every `FIELD` row into an MSv2, and into an MSv4 those `catalogue`'s
/// partitions name (an MSv2 catalogue's fields are its `FIELD` rows), as
/// XRADIO's converter refuses the fields it converts. A field moves when it
/// follows an ephemeris, whose directions casacore reads as offsets from
/// it, or when its directions are polynomials in time (`NUM_POLY` above
/// 0); the catalogue holds one constant direction.
fn refuse_moving_fields(
    ms: &crate::MeasurementSet,
    catalogue: &Catalogue,
    destination: Store,
) -> Result<(), ObservationError> {
    let fields = ms.field().map_err(storage)?;
    let mut named = vec![destination == Store::Msv2; fields.row_count()];
    for field in catalogue.partitions.iter().flat_map(|entry| &entry.fields) {
        if let Some(named) = named.get_mut(field.0 as usize) {
            *named = true;
        }
    }
    for row in (0..fields.row_count()).filter(|row| named[*row]) {
        if fields
            .ephemeris_id(row)
            .map_err(storage)?
            .is_some_and(|id| id >= 0)
        {
            return Err(ObservationError::Unsupported {
                what: "converting fields that follow an ephemeris (FIELD EPHEMERIS_ID)",
            });
        }
        if fields.num_poly(row).map_err(storage)? > 0 {
            return Err(ObservationError::Unsupported {
                what: "converting fields whose direction is a polynomial in time (FIELD NUM_POLY)",
            });
        }
    }
    Ok(())
}

/// MAIN's keys the destination does not hold: a set `FLAG_ROW`, and an
/// `ARRAY_ID` or `PROCESSOR_ID` above the 0 an MSv2 destination writes (an
/// MSv4 has neither), read in row chunks. A negative id names nothing:
/// casacore's UVFITS filler writes `PROCESSOR_ID` -1 in every row.
fn main_keys(main: &casa_tables::Table, losses: &mut Vec<Loss>) -> Result<(), ObservationError> {
    let rows = main.row_count();
    let (mut flags, mut arrays, mut processors) = (Vec::new(), Vec::new(), Vec::new());
    let (mut flagged, mut array, mut processor) = (false, false, false);
    for start in (0..rows).step_by(ROW_CHUNK) {
        let chunk: Vec<usize> = (start..rows.min(start + ROW_CHUNK)).collect();
        flags.clear();
        arrays.clear();
        processors.clear();
        let mut destinations = [
            RequiredScalarColumnDestination::new(
                "FLAG_ROW",
                RequiredScalarColumnValuesMut::Bool(&mut flags),
            ),
            RequiredScalarColumnDestination::new(
                "ARRAY_ID",
                RequiredScalarColumnValuesMut::Int32(&mut arrays),
            ),
            RequiredScalarColumnDestination::new(
                "PROCESSOR_ID",
                RequiredScalarColumnValuesMut::Int32(&mut processors),
            ),
        ];
        main.required_scalar_columns_for_rows_into(&chunk, &mut destinations)
            .map_err(storage)?;
        flagged |= flags.iter().any(|flag| *flag);
        array |= arrays.iter().any(|id| *id > 0);
        processor |= processors.iter().any(|id| *id > 0);
        if flagged && array && processor {
            break;
        }
    }
    if flagged {
        losses.push(Loss::RowFlags);
    }
    if array {
        losses.push(Loss::Column("ARRAY_ID".to_string()));
    }
    if processor {
        losses.push(Loss::Column("PROCESSOR_ID".to_string()));
    }
    Ok(())
}

/// Subtables, and subtable content, the destination does not hold: the
/// uncarried subtables with rows, any further table a keyword names, a
/// `PROCESSOR` of more than one row or of another type than a correlator
/// (an MSv2 destination writes one correlator), and `STATE` rows that say
/// more than their `OBS_MODE` (an MSv2 destination writes `SIG`, and no
/// sub-scan, calibration or load).
fn subtables(ms: &crate::MeasurementSet, losses: &mut Vec<Loss>) -> Result<(), ObservationError> {
    for id in UNCARRIED_SUBTABLES {
        if ms.subtable(*id).is_some_and(|table| table.row_count() > 0) {
            losses.push(Loss::Table(id.name().to_string()));
        }
    }
    let processors = ms.processor().map_err(storage)?;
    let correlator = |row| {
        processors
            .string(row, "TYPE")
            .map(|kind| kind == "CORRELATOR")
            .map_err(storage)
    };
    if processors.row_count() > 1 || (processors.row_count() == 1 && !correlator(0)?) {
        losses.push(Loss::Table("PROCESSOR".to_string()));
    }
    let state = ms.state().map_err(storage)?;
    for row in 0..state.row_count() {
        let plain = state.i32(row, "SUB_SCAN").map_err(storage)? == 0
            && state.f64(row, "CAL").map_err(storage)? == 0.0
            && state.f64(row, "LOAD").map_err(storage)? == 0.0
            && state.bool(row, "SIG").map_err(storage)?
            && !state.bool(row, "REF").map_err(storage)?;
        if !plain {
            losses.push(Loss::Column(
                "STATE SUB_SCAN, CAL, LOAD, SIG and REF".to_string(),
            ));
            break;
        }
    }
    let main = ms.main_table();
    let standard: Vec<&str> = SubtableId::ALL_REQUIRED
        .iter()
        .chain(UNCARRIED_SUBTABLES)
        .chain(&[SubtableId::Pointing])
        .map(SubtableId::name)
        .collect();
    for field in main.keywords().fields() {
        if matches!(field.value, Value::TableRef(_)) && !standard.contains(&field.name.as_str()) {
            losses.push(Loss::Table(field.name.clone()));
        }
    }
    Ok(())
}

/// Rows of `FLAG_CATEGORY` read at once: an undefined or empty cell costs
/// nothing to read, and a held one ends the search, so this bounds the
/// cells read with the first held one (256 cells of three categories of
/// 32,768 channels and 4 correlations are about 100 MB).
const CATEGORY_CHUNK: usize = 256;

/// Whether MAIN's `FLAG_CATEGORY` holds categories: a defined cell with
/// elements in any row. The first and last rows are read first, since
/// writers usually fill the column whole or leave it undefined throughout.
fn flag_categories_held(main: &casa_tables::Table) -> Result<bool, ObservationError> {
    let rows = main.row_count();
    if rows == 0 {
        return Ok(false);
    }
    let column = main.column_accessor("FLAG_CATEGORY").map_err(storage)?;
    // Uncached: the inventory then reads FLAG_ROW from the lazy table.
    let held = |chunk: &[usize]| {
        column
            .array_cells_owned_uncached(chunk)
            .map(|cells| cells.iter().flatten().any(|cell| !cell.is_empty()))
            .map_err(storage)
    };
    if held(&[0, rows - 1])? {
        return Ok(true);
    }
    for start in (1..rows.saturating_sub(1)).step_by(CATEGORY_CHUNK) {
        let chunk: Vec<usize> = (start..(rows - 1).min(start + CATEGORY_CHUNK)).collect();
        if held(&chunk)? {
            return Ok(true);
        }
    }
    Ok(false)
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
                if name == "pointing_xds" {
                    pointing_variables(&child, losses)?;
                } else if name != "antenna_xds" && !name.starts_with("field_and_source_") {
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

/// A `pointing_xds`'s variables a conversion does not carry, as table
/// losses: it carries `POINTING_BEAM` and its coordinates.
fn pointing_variables(path: &Path, losses: &mut Vec<Loss>) -> Result<(), ObservationError> {
    const CARRIED: [&str; 6] = [
        "POINTING_BEAM",
        "time_pointing",
        "time",
        "antenna_name",
        "local_sky_dir_label",
        "n_polynomial",
    ];
    for child in std::fs::read_dir(path).map_err(storage)? {
        let name = child
            .map_err(storage)?
            .file_name()
            .to_string_lossy()
            .into_owned();
        if name != "zarr.json" && !CARRIED.contains(&name.as_str()) {
            losses.push(Loss::Table(format!("pointing_xds {name}")));
        }
    }
    Ok(())
}
