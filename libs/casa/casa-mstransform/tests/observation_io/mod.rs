// SPDX-License-Identifier: LGPL-3.0-or-later
//! Laws of MSv2/MSv4 conversion and format-equivalence across the boundary.

#[cfg(unix)]
#[path = "convert.rs"]
mod convert;
#[cfg(unix)]
#[path = "creation.rs"]
mod creation;
#[cfg(unix)]
#[path = "ephemeris.rs"]
mod ephemeris;
#[cfg(unix)]
#[path = "fields.rs"]
mod fields;
#[cfg(unix)]
#[path = "geometry_inputs.rs"]
mod geometry_inputs;
#[cfg(unix)]
#[path = "namespace.rs"]
mod namespace;
#[cfg(unix)]
#[path = "receptors.rs"]
mod receptors;

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};

use casa_core::measures::MjdHighPrec;
use casa_ms::observation_io::{Msv2Observation, Msv4Observation, OpenOptions};
use casa_observation::{
    AccessRequest, BatchBuffers, Catalogue, CorrelationRequest, Grouping, Observation,
    ObservationError, PartitionId, ReadEvent, Roles, TimeSelection, Traversal, WeightRequest,
};
use flate2::read::GzDecoder;
use num_complex::Complex32;
use tar::Archive;

/// Unpack fixture `name` (`ms_trial.ms`, `ms_trial.ps.zarr` or the
/// ephemeris fixtures, `ms_trial_ephemeris*`).
pub(crate) fn fixture(root: &Path, name: &str) -> PathBuf {
    let archive = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../casacore/casa-ms/tests/fixtures/ms_trial")
        .join(format!("{name}.tgz"));
    Archive::new(GzDecoder::new(File::open(archive).expect("fixture")))
        .unpack(root)
        .expect("unpack");
    root.join(name)
}

/// Relabel the MSv2 fixture at `path`'s twelve times as scans 1, 2 and 3
/// of four times each (ten rows per time, in time order), so that
/// partitioning by scan splits it in three.
pub(crate) fn three_scans(path: &Path) {
    let mut ms = casa_ms::MeasurementSet::open(path).expect("ms");
    for (scan, rows) in [(2, 40..80), (3, 80..120)] {
        ms.main_table_mut()
            .fill_column_range(
                "SCAN_NUMBER",
                casa_tables::RowRange::new(rows.start, rows.end),
                casa_core::Value::Scalar(casa_core::ScalarValue::Int32(scan)),
            )
            .expect("scan");
    }
    ms.save().expect("save");
}

/// Two days of a body near RA 10°, Dec −20°, 1 AU away and receding:
/// MJD, RA and DEC (degrees), Rho (AU) and RadVel (AU a day).
pub(crate) const COMET: [casa_test_support::ephemeris_table::EphemerisRow; 3] = [
    [58_999.0, 10.0, -20.0, 1.0, 0.001],
    [59_000.0, 10.1, -20.05, 1.001, 0.002],
    [59_001.0, 10.2, -20.1, 1.002, 0.003],
];

/// Make field `row` of the MSv2 at `path` follow ephemeris `ids[row]`
/// (−1 for none), each id's table holding [`COMET`].
pub(crate) fn follow_ephemerides(path: &Path, ids: &[i32]) {
    let mut ms = casa_ms::MeasurementSet::open(path).expect("ms");
    let reference_time = ms
        .main_table()
        .cell_accessor(0, "TIME")
        .unwrap()
        .scalar()
        .unwrap()
        .clone();
    let field = ms.subtable_mut(casa_ms::SubtableId::Field).expect("FIELD");
    field
        .add_column(
            casa_tables::ColumnSchema::scalar("EPHEMERIS_ID", casa_core::PrimitiveType::Int32),
            Some(casa_core::Value::Scalar(casa_core::ScalarValue::Int32(-1))),
        )
        .expect("EPHEMERIS_ID");
    for (row, id) in ids.iter().enumerate() {
        if *id >= 0 {
            field
                .fill_column_range(
                    "TIME",
                    casa_tables::RowRange::new(row, row + 1),
                    casa_core::Value::Scalar(reference_time.clone()),
                )
                .unwrap();
        }
        field
            .fill_column_range(
                "EPHEMERIS_ID",
                casa_tables::RowRange::new(row, row + 1),
                casa_core::Value::Scalar(casa_core::ScalarValue::Int32(*id)),
            )
            .expect("an ephemeris id");
    }
    ms.save().expect("save");
    drop(ms);
    let mut written = std::collections::BTreeSet::new();
    for id in ids.iter().copied().filter(|id| *id >= 0) {
        if written.insert(id) {
            casa_test_support::ephemeris_table::write_field_ephemeris(
                path,
                id,
                &format!("COMET{id}"),
                &COMET,
            );
        }
    }
}

/// Options that partition an MSv2 by scan as well.
pub(crate) fn by_scan() -> OpenOptions {
    OpenOptions {
        partition_scheme: casa_ms::observation_io::PartitionScheme {
            scans: true,
            ..Default::default()
        },
        ..OpenOptions::default()
    }
}

/// Every role of `channels` of the first partition, under `batch_bytes`.
pub(crate) fn request(channels: std::ops::Range<usize>, batch_bytes: usize) -> AccessRequest {
    AccessRequest {
        partitions: vec![PartitionId(0)],
        data_group: "base".to_string(),
        roles: Roles::ALL,
        channels,
        times: TimeSelection::All,
        fields: casa_observation::FieldSelection::All,
        weights: WeightRequest::Stored,
        correlations: CorrelationRequest::ALL,
        grouping: Grouping::None,
        batch_bytes,
    }
}

/// `request` budgeted for exactly `times` whole times per batch on
/// `observation`: the estimate's held bytes per time (the batch's and the
/// cursor's scratch), which costs no I/O.
pub(crate) fn per_times(
    observation: &dyn Observation,
    request: AccessRequest,
    times: usize,
) -> AccessRequest {
    let probe = AccessRequest {
        batch_bytes: usize::MAX,
        ..request.clone()
    };
    let estimate = observation.estimate(&probe).expect("estimate");
    let partition = &estimate.partitions[0];
    AccessRequest {
        batch_bytes: partition.held_bytes_per_time() * times,
        ..request
    }
}

/// One sample's payload and time auxiliaries, keyed by MJD seconds and
/// the two antennas' stations, so the formats' different antenna and field
/// naming does not decide the match.
#[derive(Debug, PartialEq)]
pub(crate) struct Sample {
    values: Vec<Complex32>,
    flags: Vec<bool>,
    weights: Vec<f32>,
    uvw: [f64; 3],
    /// MJD seconds of the time centroid.
    centroid: f64,
    interval: f64,
    exposure: f64,
    scan: String,
}

pub(crate) type Key = (u64, String, String);

/// Every sample a traversal delivers, keyed.
pub(crate) fn samples(
    traversal: &mut Traversal<'_>,
    catalogue: &Catalogue,
) -> Result<BTreeMap<Key, Sample>, ObservationError> {
    let stations = stations(catalogue);
    let mjd_zero = MjdHighPrec::new(0.0, 0.0);
    let mut buffers = BatchBuffers::default();
    let mut all = BTreeMap::new();
    while let Some(event) = traversal.next(&mut buffers)? {
        let ReadEvent::Batch(batch) = event else {
            continue;
        };
        let values = batch.group.values.expect("values");
        let flags = batch.group.flags.expect("flags");
        let weights = batch.group.weights.expect("weights");
        let uvw = batch.group.uvw.expect("uvw");
        let centroid = batch.timing.centroid.expect("centroid");
        let per_time = batch.samples_per_time();
        for sample in 0..batch.samples() {
            let coordinate = batch.sample(sample);
            let key = (
                batch
                    .time
                    .seconds_since(sample / per_time, mjd_zero)
                    .to_bits(),
                stations[coordinate.baseline.antenna1.0 as usize].clone(),
                stations[coordinate.baseline.antenna2.0 as usize].clone(),
            );
            let scan = batch.scan(sample).expect("scan");
            let cell = ndarray::s![sample, .., ..];
            let previous = all.insert(
                key,
                Sample {
                    values: values.slice(cell).iter().copied().collect(),
                    flags: flags.slice(cell).iter().copied().collect(),
                    weights: weights.slice(cell).iter().copied().collect(),
                    uvw: uvw.metres_at(sample),
                    centroid: centroid.seconds_since(sample, mjd_zero),
                    interval: batch.interval_s(sample).expect("interval"),
                    exposure: batch.exposure_s(sample).expect("exposure"),
                    scan: catalogue.scans[scan.0 as usize].name.clone(),
                },
            );
            assert!(previous.is_none(), "one acquisition per time and baseline");
        }
    }
    Ok(all)
}

pub(crate) fn stations(catalogue: &Catalogue) -> Vec<String> {
    catalogue
        .antennas
        .iter()
        .map(|antenna| antenna.station.clone())
        .collect()
}

pub(crate) fn open_both(root: &Path) -> (Msv2Observation, Msv4Observation) {
    (
        Msv2Observation::open(fixture(root, "ms_trial.ms")).expect("v2"),
        Msv4Observation::open(fixture(root, "ms_trial.ps.zarr")).expect("v4"),
    )
}
