// SPDX-License-Identifier: LGPL-3.0-or-later
//! What a source holds that a conversion cannot carry, found before
//! anything is written.
//!
//! The inventory is of whole tables and columns: an MSv2's `SIGMA`,
//! `FLAG_ROW` flags, `FLAG_CATEGORY` cells, `ARRAY_ID`s other than 0,
//! `FEED` receptors that vary in time (of which the catalogue keeps the
//! last row, as casacore's `MSIter` does), MAIN columns outside the
//! boundary's roles and subtables the catalogue does not carry (system
//! calibration, weather, flag commands, history, Doppler, frequency
//! offsets, and any further subtable a keyword names), and into an MSv4,
//! which holds less of them than the catalogue, its `SOURCE` rows,
//! `PROCESSOR_ID`s other than 0, a `PROCESSOR` of more than one row and
//! `STATE` rows that say more than their intents; an MSv4's sub-datasets
//! and main-dataset variables the writer does not write; and in either the
//! data groups not selected; and, into an MSv4, antennas no partition's
//! baselines name. What an MSv2 destination's subtables write as MSv2's
//! default is found by writing them ([`super::msv2_tables::rehearse`]).
//! What pointing carries is
//! decided by [`super::pointing_plan`]. Columns of the subtables the
//! catalogue does carry (an antenna's offset, a field's delay direction, a
//! spectral window's resolution) are not inventoried, but an ephemeris
//! table's are ([`ephemeris_tables`]), since the catalogue carries only the
//! body's directions, distances and velocities. Fields whose direction is a
//! polynomial in time are refused: the catalogue would hold a constant term
//! as the phase centre.

use std::path::Path;

use casa_observation::{Catalogue, FieldEphemeris, ObservationError};
use casa_tables::{
    RequiredScalarColumnDestination, RequiredScalarColumnValuesMut, SelectedArray1DCellsMut,
};
use casa_types::Value;

use super::msv2_rows::sigma as derived_sigma;
use super::{Loss, LossKind};
use crate::observation_io::update::{request_error, storage};
use crate::schema::SubtableId;

/// The data groups to convert into an MSv4: `requested`, each present in
/// every partition, or every group of the source with `base` first.
#[cfg(feature = "msv4")]
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
        // An MSv4 holds no sigma: an MSv2 destination states one.
        if destination == Store::Msv2 {
            losses.push(Loss::SigmaDerived);
        }
    } else {
        msv2(source, catalogue, destination, &mut losses)?;
    }
    losses.sort();
    losses.dedup();
    Ok(losses)
}

/// The most [`inventory`] holds reading an MSv2 of `catalogue`: a block of
/// MAIN's row keys, and a block of `FLAG_CATEGORY` cells of three
/// categories (casacore's `FLAG_CMD`, `ORIGINAL` and `USER`) of its widest
/// partition.
pub(super) fn scan_bytes(catalogue: &Catalogue) -> usize {
    let keys = ROW_CHUNK * (size_of::<usize>() + size_of::<bool>() + 2 * size_of::<i32>());
    let widest = |each: fn(&casa_observation::PartitionEntry) -> usize| {
        catalogue.partitions.iter().map(each).max().unwrap_or(0)
    };
    let cell = widest(|entry| entry.frequencies_hz.len() * entry.correlations.len());
    let categories = CATEGORY_CHUNK * (3 * cell + size_of::<Option<casa_types::ArrayValue>>());
    let correlations = widest(|entry| entry.correlations.len());
    let sigmas = SIGMA_CHUNK * (size_of::<usize>() + 2 * correlations * size_of::<f32>());
    keys.max(categories).max(sigmas)
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

/// That the last samples of `ephemeris` are extrapolated rather than the
/// source's ([`FieldEphemeris::extrapolated`]), as a loss when they are: the
/// destination holds them as samples like any other.
pub(super) fn extrapolated(ephemeris: &FieldEphemeris) -> Option<Loss> {
    let count = ephemeris.extrapolated();
    (count > 0).then(|| {
        Loss::Column(format!(
            "the ephemeris of {}: that its last {} extrapolated, not the source's (continuing \
             the line through the two before to reach the last time's TDB)",
            ephemeris.name(),
            if count == 1 {
                "sample is".to_string()
            } else {
                format!("{count} samples are")
            }
        ))
    })
}

/// Refuse the first of `losses` whose kind is not in `accept`.
pub(crate) fn refuse_unaccepted(
    losses: &[Loss],
    accept: &[LossKind],
) -> Result<(), ObservationError> {
    let refused: Vec<&Loss> = losses
        .iter()
        .filter(|loss| !accept.contains(&loss.kind()))
        .collect();
    if refused.is_empty() {
        return Ok(());
    }
    let mut kinds: Vec<LossKind> = refused.iter().map(|loss| loss.kind()).collect();
    kinds.sort_unstable();
    kinds.dedup();
    let described: Vec<String> = refused.iter().map(ToString::to_string).collect();
    let named: Vec<&str> = kinds.iter().map(|kind| kind.name()).collect();
    Err(request_error(format!(
        "the conversion would lose information: {}; accept {} to convert anyway",
        described.join("; "),
        named.join(", ")
    )))
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
    refuse_polynomial_fields(&ms, catalogue, destination)?;
    ephemeris_tables(source, &ms, catalogue, destination, losses)?;
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
    if columns.iter().any(|name| name == "FLOAT_DATA") && !columns.iter().any(|name| name == "DATA")
    {
        return Err(ObservationError::Unsupported {
            what: "converting single-dish data (FLOAT_DATA without DATA)",
        });
    }
    let spectral = columns.iter().any(|name| name == "WEIGHT_SPECTRUM");
    for column in &columns {
        match column.as_str() {
            "SIGMA" if destination == Store::Msv2 => {
                // An MSv2 destination derives SIGMA from the weights it
                // writes: from WEIGHT_SPECTRUM where it is held.
                if spectral || !sigma_follows_weight(main)? {
                    losses.extend([Loss::Sigma, Loss::SigmaDerived]);
                }
            }
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
    if destination == Store::Msv2 && !columns.iter().any(|name| name == "SIGMA") {
        losses.push(Loss::SigmaDerived);
    }
    if spectral {
        losses.push(Loss::Column(
            "WEIGHT (the row summary beside WEIGHT_SPECTRUM)".to_string(),
        ));
    }
    main_keys(main, destination, losses)?;
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
    subtables(&ms, destination, losses)?;
    if destination == Store::Msv2 {
        super::preservation::inventory(&ms, losses);
        unnamed_feeds(&ms, catalogue, losses)?;
    }
    Ok(())
}

/// Rows of `WEIGHT` and `SIGMA` compared at a time.
const SIGMA_CHUNK: usize = 1 << 16;

/// Whether every stored `SIGMA` is what an MSv2 destination derives from
/// `WEIGHT` (`1/√WEIGHT`, and -1 where the weight is not positive) to
/// within one unit in the last place, so that deriving it loses nothing.
/// A MAIN whose cells cannot be read together (cells of several shapes) is
/// taken as not following.
fn sigma_follows_weight(main: &casa_tables::Table) -> Result<bool, ObservationError> {
    let rows = main.row_count();
    let weight = main.column_accessor("WEIGHT").map_err(storage)?;
    let sigma = main.column_accessor("SIGMA").map_err(storage)?;
    let (mut weights, mut sigmas) = (Vec::new(), Vec::new());
    for start in (0..rows).step_by(SIGMA_CHUNK) {
        let chunk: Vec<usize> = (start..rows.min(start + SIGMA_CHUNK)).collect();
        weights.clear();
        sigmas.clear();
        let read = weight
            .fill_array_cells_1d_typed_uncached(
                &chunk,
                SelectedArray1DCellsMut::Float32(&mut weights),
            )
            .and_then(|_| {
                sigma.fill_array_cells_1d_typed_uncached(
                    &chunk,
                    SelectedArray1DCellsMut::Float32(&mut sigmas),
                )
            });
        if read.is_err() || weights.len() != sigmas.len() {
            return Ok(false);
        }
        if weights
            .iter()
            .zip(&sigmas)
            .any(|(weight, sigma)| !within_one_ulp(*sigma, derived_sigma(*weight)))
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Whether `a` and `b` are equal or adjacent `f32`s.
fn within_one_ulp(a: f32, b: f32) -> bool {
    a == b
        || (a.is_finite()
            && b.is_finite()
            && a.is_sign_positive() == b.is_sign_positive()
            && a.to_bits().abs_diff(b.to_bits()) <= 1)
}

/// The `FEED` rows of antennas no partition's baselines name, a table loss:
/// an MSv2 destination writes the feeds its partitions' receptors need.
fn unnamed_feeds(
    ms: &crate::MeasurementSet,
    catalogue: &Catalogue,
    losses: &mut Vec<Loss>,
) -> Result<(), ObservationError> {
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
    let feeds = ms.feed().map_err(storage)?;
    let mut unnamed = 0;
    for row in 0..feeds.row_count() {
        let antenna = feeds.i32(row, "ANTENNA_ID").map_err(storage)?;
        let used = usize::try_from(antenna)
            .ok()
            .and_then(|antenna| named.get(antenna).copied())
            .unwrap_or(false);
        unnamed += usize::from(!used);
    }
    if unnamed > 0 {
        losses.push(Loss::Table(format!(
            "FEED rows of antennas no baseline names ({unnamed})"
        )));
    }
    Ok(())
}

/// The fields `destination` writes: every `FIELD` row into an MSv2, and
/// into an MSv4 those `catalogue`'s partitions name (an MSv2 catalogue's
/// fields are its `FIELD` rows).
fn written_fields(catalogue: &Catalogue, destination: Store, rows: usize) -> Vec<bool> {
    let mut named = vec![destination == Store::Msv2; rows];
    for field in catalogue.partitions.iter().flat_map(|entry| &entry.fields) {
        if let Some(named) = named.get_mut(field.0 as usize) {
            *named = true;
        }
    }
    named
}

/// Refuse fields whose directions are polynomials in time (`NUM_POLY` above
/// 0) among those `destination` writes, as XRADIO's converter refuses the
/// fields it converts: the catalogue holds one constant direction.
fn refuse_polynomial_fields(
    ms: &crate::MeasurementSet,
    catalogue: &Catalogue,
    destination: Store,
) -> Result<(), ObservationError> {
    let fields = ms.field().map_err(storage)?;
    let named = written_fields(catalogue, destination, fields.row_count());
    for row in (0..fields.row_count()).filter(|row| named[*row]) {
        if fields.num_poly(row).map_err(storage)? > 0 {
            return Err(ObservationError::Unsupported {
                what: "converting fields whose direction is a polynomial in time (FIELD NUM_POLY)",
            });
        }
    }
    Ok(())
}

/// What the ephemeris tables of the fields `destination` writes hold that
/// the catalogue does not carry: columns other than `MJD`, `RA`, `DEC`,
/// `Rho` and `RadVel`, and keywords other than those an MSv2 destination
/// writes. Into an MSv4, also the tables' `NAME` (an MSv4 ephemeris is
/// named by its source) and the fields' `TIME`, at which an image without a
/// phase centre is centred on the body (#987).
fn ephemeris_tables(
    source: &Path,
    ms: &crate::MeasurementSet,
    catalogue: &Catalogue,
    destination: Store,
    losses: &mut Vec<Loss>,
) -> Result<(), ObservationError> {
    let ids = crate::ephemeris::field_ephemeris_ids(ms).map_err(storage)?;
    let named = written_fields(catalogue, destination, ids.len());
    let mut followed: Vec<i32> = ids
        .iter()
        .zip(&named)
        .filter_map(|(id, named)| id.filter(|_| *named))
        .collect();
    followed.sort_unstable();
    followed.dedup();
    if !followed.is_empty() && destination == Store::Msv4 {
        losses.push(Loss::Column(
            "FIELD TIME of fields that follow an ephemeris (when an image without a phase \
             centre is centred on the body)"
                .to_string(),
        ));
    }
    for id in followed {
        let path = crate::ephemeris::find_field_ephemeris(source, id).map_err(storage)?;
        let table =
            casa_tables::Table::open(casa_tables::TableOptions::new(&path)).map_err(storage)?;
        let name = path
            .strip_prefix(source)
            .unwrap_or(&path)
            .display()
            .to_string();
        let columns: Vec<String> = table
            .schema()
            .map(|schema| {
                schema
                    .columns()
                    .iter()
                    .map(|column| column.name().to_string())
                    .filter(|column| {
                        !["MJD", "RA", "DEC", "Rho", "RadVel"].contains(&column.as_str())
                    })
                    .collect()
            })
            .unwrap_or_default();
        if !columns.is_empty() {
            losses.push(Loss::Column(format!("{name} {}", columns.join(", "))));
        }
        let mut keywords: Vec<String> = table
            .keywords()
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .filter(|keyword| {
                !super::msv2_ephemeris::KEYWORDS.contains(&keyword.as_str())
                    || (destination == Store::Msv4 && keyword == "NAME")
            })
            .collect();
        keywords.sort();
        if !keywords.is_empty() {
            losses.push(Loss::Column(format!(
                "{name} keywords {}",
                keywords.join(", ")
            )));
        }
    }
    Ok(())
}

/// MAIN's keys the destination does not hold: a set `FLAG_ROW`, an
/// `ARRAY_ID` above the 0 an MSv2 destination writes, and into an MSv4,
/// which has neither, a `PROCESSOR_ID` above 0 (an MSv2 destination writes
/// each partition's), read in row chunks. A negative id names nothing:
/// casacore's UVFITS filler writes `PROCESSOR_ID` -1 in every row.
fn main_keys(
    main: &casa_tables::Table,
    destination: Store,
    losses: &mut Vec<Loss>,
) -> Result<(), ObservationError> {
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
    if processor && destination == Store::Msv4 {
        losses.push(Loss::Column("PROCESSOR_ID".to_string()));
    }
    Ok(())
}

/// Subtables, and subtable content, the destination does not hold: the
/// uncarried subtables with rows, any further table a keyword names, and
/// into an MSv4 (an MSv2 destination writes them from the catalogue)
/// `SOURCE` rows, a `PROCESSOR` of more than one row or of another type
/// than a correlator, and `STATE` rows that say more than their `OBS_MODE`.
fn subtables(
    ms: &crate::MeasurementSet,
    destination: Store,
    losses: &mut Vec<Loss>,
) -> Result<(), ObservationError> {
    for id in UNCARRIED_SUBTABLES {
        if ms.subtable(*id).is_some_and(|table| table.row_count() > 0) {
            losses.push(Loss::Table(id.name().to_string()));
        }
    }
    let main = ms.main_table();
    let standard: Vec<&str> = SubtableId::ALL_REQUIRED
        .iter()
        .chain(UNCARRIED_SUBTABLES)
        .chain(&[SubtableId::Pointing, SubtableId::Source])
        .map(SubtableId::name)
        .collect();
    for field in main.keywords().fields() {
        if matches!(field.value, Value::TableRef(_)) && !standard.contains(&field.name.as_str()) {
            losses.push(Loss::Table(field.name.clone()));
        }
    }
    if destination == Store::Msv2 {
        return Ok(());
    }
    if ms
        .subtable(SubtableId::Source)
        .is_some_and(|table| table.row_count() > 0)
    {
        losses.push(Loss::Table(SubtableId::Source.name().to_string()));
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
                } else if name.starts_with("field_and_source_") {
                    ephemeris_variables(&child, losses)?;
                } else if name != "antenna_xds" {
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

/// The optional variables of XRADIO's `field_and_source_ephemeris`
/// (from the table's `NP_ang`, `NP_dist`, `DiskLong`, `DiskLat`, `Sl_lon`,
/// `Sl_lat`, `r`, `rdot` and `phang`), which the catalogue does not carry.
const EPHEMERIS_EXTRAS: [&str; 7] = [
    "NORTH_POLE_POSITION_ANGLE",
    "NORTH_POLE_ANGULAR_DISTANCE",
    "SUB_OBSERVER_DIRECTION",
    "SUB_SOLAR_DIRECTION",
    "SUB_SOLAR_DISTANCE",
    "HELIOCENTRIC_RADIAL_VELOCITY",
    "OBSERVER_PHASE_ANGLE",
];

/// A field dataset's ephemeris variables a conversion does not carry
/// ([`EPHEMERIS_EXTRAS`]), as one column loss; and an ephemeris in XRADIO's
/// interpolated layout (no `time_ephemeris`), whose samples start at the
/// dataset's first time, so that an MSv2 written from them cannot be
/// evaluated there (`MeasComet::fillMeas` puts a time on a table's first
/// row at row −1 for some roundings). Their end, the last time, the
/// reader extends ([`extrapolated`]).
fn ephemeris_variables(path: &Path, losses: &mut Vec<Loss>) -> Result<(), ObservationError> {
    let name = |path: &Path| {
        path.file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default()
    };
    let label = format!(
        "{}/{}",
        path.parent().map(name).unwrap_or_default(),
        name(path)
    );
    let moving = std::fs::read_to_string(path.join("zarr.json"))
        .is_ok_and(|metadata| metadata.contains("field_and_source_ephemeris"));
    if moving && !path.join("time_ephemeris").join("zarr.json").is_file() {
        losses.push(Loss::Column(format!(
            "{label} ephemeris samples (interpolated by XRADIO onto the visibility times; the \
             table's own samples are not in the store, nor any before the first time, so \
             casacore's MeasComet cannot evaluate the first time)"
        )));
    }
    let mut lost = Vec::new();
    for child in std::fs::read_dir(path).map_err(storage)? {
        let child = child
            .map_err(storage)?
            .file_name()
            .to_string_lossy()
            .into_owned();
        if EPHEMERIS_EXTRAS.contains(&child.as_str()) {
            lost.push(child);
        }
    }
    if !lost.is_empty() {
        lost.sort();
        losses.push(Loss::Column(format!("{label} {}", lost.join(", "))));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_loss_kind_parses_back_from_its_name() {
        for kind in LossKind::ALL {
            assert_eq!(kind.name().parse::<LossKind>(), Ok(kind));
            assert_eq!(kind.to_string(), kind.name());
            assert_eq!(
                kind.name().to_ascii_uppercase().parse::<LossKind>(),
                Ok(kind)
            );
        }
        let error = "row-flags".parse::<LossKind>().unwrap_err();
        assert!(error.contains("rowflags"), "{error}");
    }

    #[test]
    fn a_refusal_names_every_unaccepted_loss_and_the_kinds_to_accept() {
        let losses = [
            Loss::Sigma,
            Loss::Table("HISTORY".to_string()),
            Loss::Table("WEATHER".to_string()),
            Loss::RowFlags,
        ];
        assert!(refuse_unaccepted(&losses, &LossKind::ALL).is_ok());
        let error = refuse_unaccepted(&losses, &[LossKind::Sigma])
            .unwrap_err()
            .to_string();
        assert!(!error.contains("SIGMA"), "{error}");
        for described in ["HISTORY", "WEATHER", "FLAG_ROW"] {
            assert!(error.contains(described), "{error}");
        }
        assert!(
            error.contains("accept rowflags, tables to convert anyway"),
            "{error}"
        );
    }
}
