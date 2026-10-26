// SPDX-License-Identifier: LGPL-3.0-or-later
//! Either store converted to a new MeasurementSet v2: subtables from the
//! catalogue, and one MAIN row per grid cell (or per acquisition, padding
//! omitted), partition after partition, each partition's rows in time and
//! baseline order.

use std::fs;
use std::path::Path;

use casa_observation::{Catalogue, Observation, ObservationError, Roles, VisibilityUnits};
use casa_tables::table_quantum::TableQuantumDesc;
use casa_types::measures::EpochRef;
use ndarray::Axis;

use super::msv2_rows::{MainRows, Occupancy, acquired};
use super::{
    ConversionOptions, ConversionReport, Loss, PaddingRows, lockstep, losses, msv2_tables,
};
use crate::observation_io::update::{request_error, storage};
use crate::observation_io::{OpenOptions, open};
use crate::write_session::MeasurementSetCreateTarget;
use crate::{MeasurementSet, OptionalMainColumn};

/// The occupancy pass's roles: the weights, whose non-finite values are
/// padding's one mark.
const WEIGHTS: Roles = Roles {
    values: false,
    flags: false,
    weights: true,
    uvw: false,
    time_centroid: false,
    interval: false,
    exposure: false,
    scan: false,
};

/// Seconds per day, for MSv2 `TIME` (MJD seconds).
const SECONDS_PER_DAY: f64 = 86_400.0;

/// Convert the store at `source` (MSv2 or MSv4) into a new MeasurementSet
/// v2 at `destination`.
///
/// The selected data groups ([`ConversionOptions::data_groups`]) become
/// `DATA` (`base`), `CORRECTED_DATA` (`corrected`) and `MODEL_DATA`
/// (`model`), sharing the first group's flags, weights and UVW. Partitions
/// sharing a spectral window, polarization or intents share their
/// subtable rows. Every grid cell becomes one row, padding a flagged row of
/// NaN data and weights; or, with [`PaddingRows::Omitted`], padding becomes
/// no row, which reproduces an original sparse MSv2. `SIGMA` is derived
/// from the weights ([`Loss::SigmaDerived`]), so the caller must accept
/// [`LossKind::Sigma`](super::LossKind::Sigma).
///
/// An MSv2's row count is fixed before its rows are written. With padding
/// omitted it is the acquisitions, which only the weights record, so a
/// first pass reads them to count; otherwise it is the grids' cells. The
/// MSv2 is written by one thread and created in a staging directory renamed
/// into place when complete.
///
/// # Errors
///
/// Before anything is written: a loss not accepted; a data group without an
/// MSv2 column; times in another scale than UTC; feeds other than 0; a
/// source with nothing to write. Weights that vary over channels (an MSv2
/// `WEIGHT` holds one per correlation), refused by the counting pass when
/// there is one and otherwise when reached. Then any error reading the
/// source or writing the destination; the destination is then not
/// created.
pub fn to_msv2(
    source: &Path,
    destination: &Path,
    options: &ConversionOptions,
) -> Result<ConversionReport, ObservationError> {
    let observation = open(source, &OpenOptions::default())?;
    let catalogue = observation.catalogue();
    let groups = losses::selected_groups(catalogue, options.data_groups.as_deref())?;
    let columns = groups
        .iter()
        .map(|group| column(group))
        .collect::<Result<Vec<_>, _>>()?;
    let mut found = losses::inventory(source, catalogue, &groups)?;
    found.push(Loss::SigmaDerived);
    found.sort();
    losses::refuse_unaccepted(&found, &options.accept)?;
    refuse_unwritable(catalogue)?;
    let occupancy = occupancy(observation.as_ref(), &groups[0], options)?;
    let target = MeasurementSetCreateTarget::prepare(destination, false).map_err(storage)?;
    let staging = target.staging_path().to_path_buf();
    let created = create(
        &staging,
        observation.as_ref(),
        &groups,
        &columns,
        &occupancy,
        options,
    )
    .and_then(|report| target.commit().map(|()| report).map_err(storage));
    match created {
        Ok(report) => Ok(ConversionReport {
            losses: found,
            ..report
        }),
        Err(error) => {
            // The staging directory is the only output; nothing is
            // published.
            let _ = fs::remove_dir_all(&staging);
            Err(error)
        }
    }
}

/// The MSv2 column of a data group, as XRADIO's converter names them.
fn column(group: &str) -> Result<OptionalMainColumn, ObservationError> {
    match group {
        "base" => Ok(OptionalMainColumn::Data),
        "corrected" => Ok(OptionalMainColumn::CorrectedData),
        "model" => Ok(OptionalMainColumn::ModelData),
        other => Err(request_error(format!(
            "data group {other} has no MSv2 column: base (DATA), corrected \
             (CORRECTED_DATA) and model (MODEL_DATA) have"
        ))),
    }
}

/// Refuse what the MSv2 written cannot say: another time scale than UTC
/// (`TIME` is written with the MeasurementSet's default reference), or a
/// feed other than the one `FEED` row per antenna the conversion writes.
fn refuse_unwritable(catalogue: &Catalogue) -> Result<(), ObservationError> {
    for entry in &catalogue.partitions {
        if entry.time_scale != EpochRef::UTC {
            return Err(ObservationError::Unsupported {
                what: "writing MSv2 times in another scale than UTC",
            });
        }
        if entry.feeds.is_some_and(|feeds| feeds != [0, 0]) {
            return Err(ObservationError::Unsupported {
                what: "writing MSv2 rows of feeds other than 0",
            });
        }
    }
    Ok(())
}

/// Each partition's rows and interval from its grid, the first selected
/// group's, and the times the grids span; with padding omitted, the
/// acquisitions counted from the weights.
fn occupancy(
    observation: &dyn Observation,
    group: &str,
    options: &ConversionOptions,
) -> Result<Occupancy, ObservationError> {
    let catalogue = observation.catalogue();
    let groups = [group.to_string()];
    let mut rows = Vec::with_capacity(catalogue.partitions.len());
    let mut intervals = Vec::with_capacity(catalogue.partitions.len());
    let mut range = [f64::INFINITY, f64::NEG_INFINITY];
    for id in catalogue.partition_ids() {
        let grid = observation.grid(id, group)?;
        let origin = (grid.time_origin.day() + grid.time_origin.frac()) * SECONDS_PER_DAY;
        if let (Some(first), Some(last)) = (grid.times_s.first(), grid.times_s.last()) {
            range = [range[0].min(origin + first), range[1].max(origin + last)];
        }
        intervals.push(grid.interval_s);
        if options.padding == PaddingRows::Flagged {
            rows.push(grid.times_s.len() * grid.baselines.len());
            continue;
        }
        let mut count = 0;
        lockstep::read(
            observation,
            id,
            &groups,
            WEIGHTS,
            (options.batch_bytes, None),
            |batches| {
                let weights = batches[0]
                    .group
                    .weights
                    .ok_or_else(|| request_error("an occupancy batch carries its weights"))?;
                for sample in weights.axis_iter(Axis(0)) {
                    count += usize::from(acquired(sample)?);
                }
                Ok(())
            },
        )?;
        rows.push(count);
    }
    if range[0] > range[1] {
        range = [0.0, 0.0];
    }
    Ok(Occupancy {
        rows,
        intervals,
        time_range: range,
    })
}

/// Create the MSv2 at `staging`: its subtables, then MAIN's rows from a
/// lockstep read of every partition.
fn create(
    staging: &Path,
    observation: &dyn Observation,
    groups: &[String],
    columns: &[OptionalMainColumn],
    occupancy: &Occupancy,
    options: &ConversionOptions,
) -> Result<ConversionReport, ObservationError> {
    let catalogue = observation.catalogue();
    let builder = msv2_tables::builder(catalogue, columns);
    let mut ms = MeasurementSet::create(staging, builder).map_err(storage)?;
    // The MAIN writers write into the directory before the save.
    fs::create_dir_all(staging).map_err(storage)?;
    for (column, group) in columns.iter().zip(groups) {
        declare_units(&mut ms, catalogue, column.name(), group)?;
    }
    let layout = msv2_tables::populate(&mut ms, catalogue, occupancy.time_range)?;
    let names = columns.iter().map(|column| column.name()).collect();
    let mut main = MainRows::start(
        staging,
        catalogue,
        names,
        occupancy,
        layout,
        options.padding,
    )?;
    let mut report = ConversionReport::default();
    for id in catalogue.partition_ids() {
        let read = lockstep::read(
            observation,
            id,
            groups,
            Roles::ALL,
            (options.batch_bytes, None),
            |batches| main.push(batches),
        )?;
        report.partitions += 1;
        report.batches += read.batches;
        report.samples += read.samples;
    }
    report.rows = main.finish(&mut ms)?;
    Ok(report)
}

/// Declare `column`'s `QuantumUnits` as `group`'s units, which every
/// partition must share; undeclared units declare none, as CASA's `DATA`
/// declares none.
fn declare_units(
    ms: &mut MeasurementSet,
    catalogue: &Catalogue,
    column: &str,
    group: &str,
) -> Result<(), ObservationError> {
    let mut units = catalogue
        .partitions
        .iter()
        .filter_map(|entry| entry.data_group(group).map(|(_, entry)| &entry.units));
    let Some(first) = units.next() else {
        return Ok(());
    };
    if units.any(|units| units != first) {
        return Err(ObservationError::Unsupported {
            what: "writing one MSv2 column of data groups with different units",
        });
    }
    let unit = match first {
        VisibilityUnits::Undeclared => return Ok(()),
        VisibilityUnits::Jansky => "Jy".to_string(),
        VisibilityUnits::Declared(unit) => unit.clone(),
    };
    TableQuantumDesc::with_unit(column, &unit)
        .write(ms.main_table_mut())
        .map_err(storage)
}
