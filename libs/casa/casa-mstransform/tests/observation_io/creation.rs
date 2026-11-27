// SPDX-License-Identifier: LGPL-3.0-or-later
//! Creation plan parity against [`to_msv2`].

use std::path::Path;

use casa_ms::observation_io::creation::{
    CreationInput, CreationOptions, CreationPlan, Destination, LossKind, PaddingRows,
};
use casa_ms::observation_io::{Msv4Codec, OpenOptions, open};
use casa_mstransform::convert::{ConversionOptions, to_msv2};
use casa_observation::MergeTolerance;

use super::convert::rows_and_sigmas;
use super::fixture;

/// Every loss the trial fixture takes into an MSv2: its SIGMA, its
/// HISTORY and its `OBSERVATION LOG` and `SCHEDULE_TYPE`.
const ACCEPT: [LossKind; 3] = [LossKind::Sigma, LossKind::Tables, LossKind::Columns];

fn options() -> CreationOptions {
    CreationOptions {
        copy_pointing: true,
        data_groups: None,
        accept: ACCEPT.to_vec(),
        batch_bytes: 1 << 20,
        memory_bytes: 1 << 30,
        padding: PaddingRows::Flagged,
        tolerance: MergeTolerance::default(),
        history: Vec::new(),
        creation_date: "2026-10-29T00:00:00".to_string(),
    }
}

/// The catalogue of the store at `path`, printed whole for comparison.
fn catalogue(path: &Path) -> String {
    let observation = open(path, &OpenOptions::default()).expect("open");
    format!("{:?}", observation.catalogue())
}

/// A plan over one input writes the MeasurementSet `to_msv2` writes, and
/// says before writing every loss its report then lists.
#[test]
fn a_plan_over_one_input_writes_what_converting_writes() {
    let root = tempfile::tempdir().expect("root");
    let v2 = fixture(root.path(), "ms_trial.ms");
    let (planned, converted) = (
        root.path().join("planned.ms"),
        root.path().join("converted.ms"),
    );
    let plan = CreationPlan::new(
        &[CreationInput::new(&v2)],
        &options(),
        Destination::New(planned.clone()),
    )
    .expect("plan");
    let losses = plan.losses().to_vec();
    assert!(
        !losses.iter().any(|loss| loss.kind() == LossKind::Sigma),
        "the fixture's SIGMA is 1/√WEIGHT, so deriving it loses nothing: {losses:?}"
    );
    assert!(!planned.exists(), "a plan writes nothing");
    let report = plan.write().expect("write");
    assert_eq!(report.losses, losses);
    let conversion = to_msv2(
        &v2,
        &converted,
        &ConversionOptions {
            data_groups: None,
            accept: ACCEPT.to_vec(),
            batch_bytes: 1 << 20,
            workers: 1,
            chunks: [0; 4],
            codec: Msv4Codec::BloscLz4,
            padding: PaddingRows::Flagged,
            creator: "casa-rs".to_string(),
            creation_date: "2026-10-29T00:00:00+00:00".to_string(),
        },
    )
    .expect("convert");
    assert_eq!(
        (report.partitions, report.samples, report.rows),
        (conversion.partitions, conversion.samples, conversion.rows)
    );
    assert_eq!(rows_and_sigmas(&planned), rows_and_sigmas(&converted));
    assert_eq!(catalogue(&planned), catalogue(&converted));
}
