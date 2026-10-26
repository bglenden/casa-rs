// SPDX-License-Identifier: LGPL-3.0-or-later
//! The converter: either store into a new MSv4 through the boundary, every
//! acquisition and selected data group carried, every loss refused unless
//! accepted.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_ms::observation_io::convert::{ConversionOptions, Loss, LossKind, to_msv4};
use casa_ms::observation_io::{Msv4Codec, OpenOptions, open, open_update};
use casa_ms::{
    SyntheticAntenna, SyntheticObservationRequest, SyntheticSpectralSetup,
    generate_synthetic_observation_ms,
};
use casa_observation::{
    AccessRequest, CorrelationRequest, Grouping, ObservationError, PartitionId, Roles,
    UpdateTarget, WritableRole,
};
use num_complex::Complex32;

use super::{Key, Sample, fixture, samples};

fn options(accept: &[LossKind]) -> ConversionOptions {
    ConversionOptions {
        data_groups: None,
        accept: accept.to_vec(),
        batch_bytes: 1 << 20,
        workers: 2,
        chunks: [5, 0, 0, 0],
        codec: Msv4Codec::BloscLz4,
        creator: "casa-rs".to_string(),
        creation_date: "2026-10-26T00:00:00+00:00".to_string(),
    }
}

/// Every sample of `group` in partition `id` of the store at `path`.
fn read(path: &Path, id: PartitionId, group: &str) -> BTreeMap<Key, Sample> {
    let observation = open(path, &OpenOptions::default()).expect("open");
    let channels = observation.catalogue().partition(id).frequencies_hz.len();
    let request = AccessRequest {
        partitions: vec![id],
        data_group: group.to_string(),
        roles: Roles::ALL,
        channels: 0..channels,
        correlations: CorrelationRequest::ALL,
        grouping: Grouping::None,
        batch_bytes: usize::MAX,
    };
    samples(
        &mut observation.bind_read(&request).expect("bind"),
        observation.catalogue(),
    )
    .expect("read")
}

/// The MSv2 fixture converts to an MSv4 holding every sample as it is,
/// with XRADIO's names: the same store, sample for sample, as XRADIO's own
/// conversion of it. It loses SIGMA, which the conversion refuses until
/// that loss is accepted.
#[test]
fn an_msv2_converts_to_the_msv4_xradio_makes_of_it() {
    let root = tempfile::tempdir().expect("root");
    let v2 = fixture(root.path(), "ms_trial.ms");
    let xradio = fixture(root.path(), "ms_trial.ps.zarr");
    let out = root.path().join("converted.ps.zarr");
    let Err(ObservationError::InvalidRequest { reason }) = to_msv4(&v2, &out, &options(&[])) else {
        panic!("an unaccepted loss refuses the conversion");
    };
    assert!(reason.contains("SIGMA"), "{reason}");
    assert!(!out.exists(), "nothing is written before the refusal");
    let report =
        to_msv4(&v2, &out, &options(&[LossKind::Sigma, LossKind::Tables])).expect("convert");
    assert_eq!(
        report.losses,
        [
            Loss::Sigma,
            Loss::Table("HISTORY".to_string()),
            Loss::Table("POINTING".to_string())
        ],
        "MSv4 holds no sigma; the writer writes no history or pointing"
    );
    assert_eq!((report.partitions, report.samples), (1, 120));
    let converted = read(&out, PartitionId(0), "base");
    assert_eq!(
        converted,
        read(&v2, PartitionId(0), "base"),
        "the MSv2's samples"
    );
    assert_eq!(
        converted,
        read(&xradio, PartitionId(0), "base"),
        "XRADIO's samples"
    );
    let ours = open(&out, &OpenOptions::default()).expect("open");
    let theirs = open(&xradio, &OpenOptions::default()).expect("open");
    let names = |catalogue: &casa_observation::Catalogue| {
        (
            catalogue
                .antennas
                .iter()
                .map(|a| a.name.clone())
                .collect::<Vec<_>>(),
            catalogue
                .fields
                .iter()
                .map(|f| f.name.clone())
                .collect::<Vec<_>>(),
            catalogue.partitions[0].spectral_window.clone(),
        )
    };
    assert_eq!(
        names(ours.catalogue()),
        names(theirs.catalogue()),
        "XRADIO's names"
    );
}

fn antennas() -> Vec<SyntheticAntenna> {
    vec![
        SyntheticAntenna::vla("VLA01", "N01", [-1_601_185.4, -5_041_977.5, 3_554_875.9]),
        SyntheticAntenna::vla("VLA02", "N02", [-1_601_085.4, -5_041_977.5, 3_554_875.9]),
        SyntheticAntenna::vla("VLA03", "N03", [-1_601_185.4, -5_041_877.5, 3_554_875.9]),
    ]
}

/// A simulated MSv2 of two spectral windows of four and three channels
/// (two partitions of different shapes), with `DATA` and a `MODEL_DATA`
/// written through an update session, each value distinct.
fn two_windows(root: &Path) -> PathBuf {
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        root.join("unused.fits"),
        root.join("two_windows.ms"),
        antennas(),
    );
    request.predict_model = false;
    request.phase_center_rad = [1.23, -0.45];
    request.start_time_mjd_seconds = 59_000.25 * 86_400.0;
    request.duration_seconds = 65.0;
    request.integration_seconds = 10.0;
    request.spectral_windows = vec![
        SyntheticSpectralSetup {
            name: "low".to_string(),
            start_frequency_hz: 672.0e9,
            channel_width_hz: 2.0e6,
            channel_count: 4,
        },
        SyntheticSpectralSetup {
            name: "high".to_string(),
            start_frequency_hz: 676.0e9,
            channel_width_hz: -1.0e6,
            channel_count: 3,
        },
    ];
    generate_synthetic_observation_ms(&request).expect("simulate");
    let path = request.output_ms;
    let targets = [
        UpdateTarget::new("base", WritableRole::Visibilities),
        UpdateTarget {
            create: true,
            ..UpdateTarget::new("model", WritableRole::Visibilities)
        },
    ];
    let session = open_update(&path, &OpenOptions::default(), &targets).expect("session");
    let catalogue = session.observation().catalogue().clone();
    for id in catalogue.partition_ids() {
        let request = AccessRequest {
            partitions: vec![id],
            data_group: "base".to_string(),
            roles: Roles::ALL,
            channels: 0..catalogue.partition(id).frequencies_hz.len(),
            correlations: CorrelationRequest::ALL,
            grouping: Grouping::None,
            batch_bytes: usize::MAX,
        };
        let mut traversal = session.observation().bind_read(&request).expect("bind");
        let mut updater = session.updater(&request, &[0, 1]).expect("updater");
        let mut buffers = casa_observation::BatchBuffers::default();
        while let Some(casa_observation::ReadEvent::Batch(batch)) =
            traversal.next(&mut buffers).expect("read")
        {
            let value = |offset: f32| {
                ndarray::Array3::from_shape_fn(batch.shape(), |(s, c, p)| {
                    Complex32::new(
                        offset + (id.0 * 1000) as f32 + s as f32,
                        (c * 10 + p) as f32,
                    )
                })
            };
            let (data, model) = (value(0.0), value(0.5));
            updater
                .write(
                    &batch,
                    &[
                        casa_observation::PatchValues::Visibilities(data.view()),
                        casa_observation::PatchValues::Visibilities(model.view()),
                    ],
                )
                .expect("write");
        }
        updater.finish().expect("finish");
    }
    path
}

/// Two partitions of different shapes and two data groups convert into
/// two datasets, each holding `VISIBILITY` and `VISIBILITY_MODEL` beside
/// one `FLAG`, `WEIGHT` and `UVW`: every group of every partition reads
/// back equal to the MSv2's. Batches of whole five-time chunks hold
/// nothing.
#[test]
fn partitions_and_data_groups_convert_together() {
    let root = tempfile::tempdir().expect("root");
    let v2 = two_windows(root.path());
    let out = root.path().join("two_windows.ps.zarr");
    let accept = [LossKind::Sigma, LossKind::Tables];
    let report = to_msv4(&v2, &out, &options(&accept)).expect("convert");
    assert_eq!(report.partitions, 2);
    assert_eq!(
        report.held_elements, 0,
        "five-time batches of five-time chunks"
    );
    assert!(
        out.join("two_windows_0/VISIBILITY_MODEL/zarr.json")
            .is_file()
    );
    assert!(
        out.join("two_windows_1/VISIBILITY_MODEL/zarr.json")
            .is_file()
    );
    let converted = open(&out, &OpenOptions::default()).expect("open");
    assert_eq!(converted.catalogue().partitions.len(), 2);
    for id in [PartitionId(0), PartitionId(1)] {
        for group in ["base", "model"] {
            assert_eq!(
                read(&out, id, group),
                read(&v2, id, group),
                "{id:?} {group}"
            );
        }
    }
    let only_base = ConversionOptions {
        data_groups: Some(vec!["base".to_string()]),
        ..options(&accept)
    };
    let refused = to_msv4(&v2, &root.path().join("base_only.ps.zarr"), &only_base);
    assert!(
        matches!(refused, Err(ObservationError::InvalidRequest { ref reason }) if reason.contains("model")),
        "leaving a group behind is a loss: {refused:?}"
    );
}
