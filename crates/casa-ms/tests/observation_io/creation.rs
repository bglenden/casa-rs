// SPDX-License-Identifier: LGPL-3.0-or-later
//! Creating a MeasurementSet from observations (#937 PR B): a plan checked
//! before anything is written, then written. Converting into an MSv2 is a
//! plan over one input, so the converter's laws (`convert.rs`) are the
//! one-input laws; these add what a plan says of itself.

use std::path::Path;

use casa_ms::observation_io::convert::{ConversionOptions, to_msv2};
use casa_ms::observation_io::creation::{
    CreationInput, CreationOptions, CreationPlan, Destination, Loss, LossKind, PaddingRows,
};
use casa_ms::observation_io::{Msv4Codec, OpenOptions, open};
use casa_observation::ObservationError;

use super::convert::rows_and_sigmas;
use super::fixture;

/// Every loss the trial fixture takes into an MSv2.
const ACCEPT: [LossKind; 2] = [LossKind::Sigma, LossKind::Tables];

fn options() -> CreationOptions {
    CreationOptions {
        data_groups: None,
        accept: ACCEPT.to_vec(),
        batch_bytes: 1 << 20,
        padding: PaddingRows::Flagged,
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
    let plan = CreationPlan::new(&[CreationInput::new(&v2)], &options()).expect("plan");
    let losses = plan.losses().to_vec();
    assert!(losses.contains(&Loss::SigmaDerived), "{losses:?}");
    assert!(!planned.exists(), "a plan writes nothing");
    let report = plan.write(&planned, Destination::New).expect("write");
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

/// An input's weight scale multiplies every `WEIGHT`, and `SIGMA` is derived
/// from the scaled weight: `1/√(W·w)`.
#[test]
fn weights_are_scaled_and_sigmas_follow_them() {
    let root = tempfile::tempdir().expect("root");
    let v2 = fixture(root.path(), "ms_trial.ms");
    let write = |name: &str, weight_scale: f64| {
        let path = root.path().join(name);
        let input = CreationInput {
            path: v2.clone(),
            weight_scale,
        };
        CreationPlan::new(&[input], &options())
            .and_then(|plan| plan.write(&path, Destination::New))
            .expect("write");
        rows_and_sigmas(&path)
    };
    let (rows, unscaled) = write("unscaled.ms", 1.0);
    let (scaled_rows, scaled) = write("scaled.ms", 4.0);
    assert_eq!(rows, scaled_rows);
    assert!(!unscaled.is_empty());
    for ((weight, sigma), (scaled_weight, scaled_sigma)) in unscaled.iter().zip(&scaled) {
        assert_eq!(scaled_weight.to_bits(), (weight * 4.0).to_bits());
        if *weight > 0.0 {
            assert_eq!(
                scaled_sigma.to_bits(),
                (1.0 / (weight * 4.0).sqrt()).to_bits()
            );
            assert!((scaled_sigma - sigma / 2.0).abs() <= f32::EPSILON * sigma);
        } else {
            assert_eq!(
                *scaled_sigma, -1.0,
                "no sigma where the weight is not positive"
            );
        }
    }
}

/// A weight scale that is not finite and positive is refused by the plan.
#[test]
fn weight_scales_must_be_finite_and_positive() {
    let root = tempfile::tempdir().expect("root");
    let v2 = fixture(root.path(), "ms_trial.ms");
    for weight_scale in [0.0, -1.0, f64::NAN, f64::INFINITY, 1e300] {
        let input = CreationInput {
            path: v2.clone(),
            weight_scale,
        };
        assert!(
            matches!(
                CreationPlan::new(&[input], &options()),
                Err(ObservationError::InvalidRequest { .. })
            ),
            "{weight_scale}"
        );
    }
    assert!(matches!(
        CreationPlan::new(&[], &options()),
        Err(ObservationError::InvalidRequest { .. })
    ));
}

/// A new MeasurementSet is never written over an existing path, which is
/// left as it was.
#[test]
fn an_existing_destination_is_refused_and_kept() {
    let root = tempfile::tempdir().expect("root");
    let v2 = fixture(root.path(), "ms_trial.ms");
    let destination = root.path().join("taken.ms");
    std::fs::create_dir(&destination).expect("taken");
    std::fs::write(destination.join("keep"), b"kept").expect("keep");
    let plan = CreationPlan::new(&[CreationInput::new(&v2)], &options()).expect("plan");
    assert!(plan.write(&destination, Destination::New).is_err());
    assert_eq!(
        std::fs::read(destination.join("keep")).expect("kept"),
        b"kept"
    );
    assert_eq!(std::fs::read_dir(&destination).expect("dir").count(), 1);
}
