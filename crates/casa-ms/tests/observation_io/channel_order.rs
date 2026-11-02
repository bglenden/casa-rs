// SPDX-License-Identifier: LGPL-3.0-or-later
//! Channel order across stores (#1014). CASA's ALMA test MS
//! (`mssel_test_small.ms`) stores spectral window 1's 64 channels
//! descending (−31.25 MHz a channel), as the MSv2 adapter delivers them;
//! XRADIO's conversion of it (`ms_trial/mssel_test_small.ps.zarr`, made by
//! `tools/ms-trial/make_descending_fixture.sh`) writes them ascending.
//! Both orders are allowed: the catalogues bind, the binding states which
//! partitions are reversed, and channel `c` of one store's batch is channel
//! `order.channel(c, n)` of the other's.

use std::collections::BTreeMap;
use std::ops::Range;
use std::path::{Path, PathBuf};

use casa_ms::observation_io::{Msv2Observation, Msv4Observation};
use casa_observation::{
    AccessRequest, BindingTolerance, ChannelOrder, CorrelationRequest, FieldSelection, Grouping,
    Observation, PartitionId, Roles, TimeSelection, WeightRequest,
};

use super::{Key, Sample, samples};

/// Unpack fixture archive `archive` (under `tests/fixtures`) into `root`;
/// the path of `name` in it.
fn unpack(root: &Path, archive: &str, name: &str) -> PathBuf {
    let archive = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(archive);
    tar::Archive::new(flate2::read::GzDecoder::new(
        std::fs::File::open(archive).expect("fixture"),
    ))
    .unpack(root)
    .expect("unpack");
    root.join(name)
}

/// Every sample of `partition`'s `channels` on `observation`, every role.
fn read(
    observation: &dyn Observation,
    partition: PartitionId,
    channels: Range<usize>,
) -> BTreeMap<Key, Sample> {
    let entry = observation.catalogue().partition(partition);
    let request = AccessRequest {
        partitions: vec![partition],
        data_group: entry.data_groups[0].name.clone(),
        roles: Roles::ALL,
        channels,
        times: TimeSelection::All,
        fields: FieldSelection::All,
        weights: WeightRequest::Stored,
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

/// `values`, `channels` blocks of a sample's products, with the blocks in
/// the reverse order.
fn reverse_channels<T: Clone>(values: &mut Vec<T>, channels: usize) {
    assert_eq!(values.len() % channels, 0, "whole channels");
    let per_channel = values.len() / channels;
    *values = values
        .chunks(per_channel)
        .rev()
        .flatten()
        .cloned()
        .collect();
}

/// The MSv2 and XRADIO's conversion bind both ways, window 1's three
/// partitions (one a scan intent) reversed and window 2's (one channel)
/// in the same order. Every partition delivers the same samples from
/// either store, over the whole band and over channel windows mapped
/// through the binding's order, once a reversed batch's channels are put
/// back in the MSv2's order: values, flags, weights, UVW, times, scan.
#[test]
fn a_descending_window_binds_to_xradios_ascending_conversion() {
    let root = tempfile::tempdir().expect("root");
    let v2 = Msv2Observation::open(unpack(
        root.path(),
        "mssel_test_small.ms.tgz",
        "mssel_test_small.ms",
    ))
    .expect("v2");
    let v4 = Msv4Observation::open(unpack(
        root.path(),
        "ms_trial/mssel_test_small.ps.zarr.tgz",
        "mssel_test_small.ps.zarr",
    ))
    .expect("v4");
    let (ours, theirs) = (v2.catalogue(), v4.catalogue());
    let binding = ours
        .bind_to(theirs, &BindingTolerance::EXACT)
        .expect("binds");
    let back = theirs
        .bind_to(ours, &BindingTolerance::EXACT)
        .expect("binds back");
    assert_eq!(ours.partitions.len(), 6);
    let mut reversed = 0;
    for (index, entry) in ours.partitions.iter().enumerate() {
        let partition = PartitionId(u32::try_from(index).expect("partition"));
        let (other, order) = (binding.partitions[index], binding.channel_orders[index]);
        assert_eq!(back.partitions[other.0 as usize], partition);
        assert_eq!(back.channel_orders[other.0 as usize], order);
        let count = entry.frequencies_hz.len();
        let descends = entry
            .frequencies_hz
            .windows(2)
            .any(|pair| pair[1] < pair[0]);
        assert_eq!(
            order == ChannelOrder::Reversed,
            descends,
            "partition {index}"
        );
        reversed += usize::from(descends);
        let windows = if count > 1 {
            vec![0..count, 2..6, count / 2..count - 1, count - 1..count]
        } else {
            vec![0..count]
        };
        for channels in windows {
            let expected = read(&v2, partition, channels.clone());
            assert!(!expected.is_empty(), "partition {index} has samples");
            let mut delivered = read(&v4, other, order.channels(channels.clone(), count));
            if order == ChannelOrder::Reversed {
                for sample in delivered.values_mut() {
                    reverse_channels(&mut sample.values, channels.len());
                    reverse_channels(&mut sample.flags, channels.len());
                    reverse_channels(&mut sample.weights, channels.len());
                }
            }
            assert!(
                delivered == expected,
                "partition {index} ({order:?}), channels {channels:?}"
            );
        }
    }
    assert_eq!(reversed, 3, "window 1's partitions");
}
