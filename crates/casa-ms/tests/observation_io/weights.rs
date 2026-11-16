// SPDX-License-Identifier: LGPL-3.0-or-later
//! The weight role's two readings: the stored weights, or weights made from
//! the stored sigmas as 1/σ², as CASA's solvers weight a solve. An MSv2
//! delivers 1/σ² from `SIGMA_SPECTRUM` when MAIN has both it and
//! `WEIGHT_SPECTRUM`, else from `SIGMA`; a σ that is not positive gives 0;
//! padding stays NaN. An MSv4 holds no sigmas: the request's fallback is
//! taken and recorded, or the request is refused. A weight write is never
//! bound to sigma-derived weights. Both readings hold however the MSv2
//! tiles its weights and sigmas, a correlation to a tile included.

use std::collections::BTreeMap;
use std::path::Path;

use casa_ms::OptionalMainColumn;
use casa_ms::column_def::build_table_schema;
use casa_ms::observation_io::{Msv2Observation, OpenOptions, open_update};
use casa_observation::{
    AccessRequest, BatchBuffers, CorrelationRequest, Deliver, FieldSelection, FlagsFrom, Grouping,
    Observation, ObservationError, PartitionId, PatchValues, ReadEvent, Roles, SigmaFallback,
    TimeSelection, UpdateTarget, WeightChannels, WeightRequest, WeightRule, WeightSource,
    WritableRole, inverse_sigma_squared,
};
use casa_types::measures::MjdHighPrec;
use casa_types::{ArrayValue, ScalarValue, Value};
use ndarray::ArrayD;

use super::{fixture, open_both, per_times, request, samples};

/// The stored weight of row `row`, product `product` and channel
/// `channel`: positive, varying over all three.
fn stored_weight(row: usize, product: usize, channel: usize) -> f32 {
    1.0 + ((row * 4 + product + 3 * channel) % 13) as f32
}

/// The sigma written beside stored weight `weight` of product `product`:
/// not 1/√weight, by a factor that differs between products, and not
/// positive (zero, negative, NaN) for three of the weights.
fn sigma_beside(weight: f32, product: usize) -> f32 {
    match weight as u32 {
        5 => 0.0,
        7 => -1.0,
        11 => f32::NAN,
        _ => 1.0 / (0.5 * weight * (1 + product) as f32).sqrt(),
    }
}

/// Rewrite every MAIN row's weights and sigmas of the MSv2 at `path`
/// (`channels` per row when spectral, else one), then remove row 7 so its
/// cell is padding.
fn rewrite_weights(path: &std::path::Path, spectral: bool) {
    let mut ms = casa_ms::MeasurementSet::open(path).expect("ms");
    let channels = if spectral { 8 } else { 1 };
    let (weights, sigmas) = if spectral {
        ("WEIGHT_SPECTRUM", "SIGMA_SPECTRUM")
    } else {
        ("WEIGHT", "SIGMA")
    };
    if spectral {
        let columns = [
            *OptionalMainColumn::WeightSpectrum.column_def(),
            *OptionalMainColumn::SigmaSpectrum.column_def(),
        ];
        let schema = build_table_schema(&columns).expect("schema");
        for name in [weights, sigmas] {
            let definition = schema.column(name).expect("definition").clone();
            ms.main_table_mut()
                .add_column(definition, None)
                .expect("add column");
        }
    }
    // A cell is [product] or [product, channel], casacore's axis order.
    let shape = if spectral { vec![4, channels] } else { vec![4] };
    for row in 0..ms.main_table().row_count() {
        let mut stored = ArrayD::<f32>::zeros(shape.clone());
        let mut beside = stored.clone();
        for ((index, weight), sigma) in stored.indexed_iter_mut().zip(beside.iter_mut()) {
            let channel = if spectral { index[1] } else { 0 };
            *weight = stored_weight(row, index[0], channel);
            *sigma = sigma_beside(*weight, index[0]);
        }
        for (column, cell) in [(weights, stored), (sigmas, beside)] {
            ms.main_table_mut()
                .cell_accessor_mut(row, column)
                .and_then(|mut target| target.set(Value::Array(ArrayValue::Float32(cell))))
                .expect("set cell");
        }
    }
    ms.main_table_mut().remove_rows(&[7]).expect("remove row 7");
    ms.save().expect("save");
}

/// Every delivered weight of `request`, in traversal order: the values
/// of each sample's cell, `[channel or one, product]` with product
/// fastest.
fn delivered_weights(observation: &dyn Observation, request: &AccessRequest) -> Vec<f32> {
    let mut traversal = observation.bind_read(request).expect("bind");
    let mut buffers = BatchBuffers::default();
    let mut all = Vec::new();
    while let Some(event) = traversal.next(&mut buffers).expect("read") {
        if let ReadEvent::Batch(batch) = event {
            all.extend(batch.group.weights.expect("weights").iter().copied());
        }
    }
    all
}

fn from_sigmas(fallback: SigmaFallback) -> WeightRequest {
    WeightRequest::InverseSigmaSquared { fallback }
}

/// On an MSv2, with and without the spectral columns, sigma-derived weights
/// are 1/σ² of the column CASA's rule picks, product for product and
/// channel for channel; a σ that is not positive gives 0, and the padding
/// cell stays NaN. The stored weights are delivered unchanged beside them,
/// and the estimate records which rule and variable each traversal reads.
#[test]
fn msv2_weights_from_sigmas_are_inverse_sigma_squared() {
    for (spectral, variable) in [(false, "SIGMA"), (true, "SIGMA_SPECTRUM")] {
        let root = tempfile::tempdir().expect("root");
        let path = fixture(root.path(), "ms_trial.ms");
        rewrite_weights(&path, spectral);
        let observation = Msv2Observation::open(&path).expect("v2");
        let stored_request = per_times(&observation, request(0..8, 0), 4);
        let sigma_request = AccessRequest {
            weights: from_sigmas(SigmaFallback::Refuse),
            ..stored_request.clone()
        };
        let source = |request: &AccessRequest| {
            observation.estimate(request).expect("estimate").partitions[0]
                .weights
                .clone()
        };
        let stored_variable = if spectral {
            "WEIGHT_SPECTRUM"
        } else {
            "WEIGHT"
        };
        assert_eq!(
            source(&stored_request),
            Some(WeightSource {
                rule: WeightRule::Stored,
                variable: stored_variable.to_string(),
                per_channel: spectral,
            })
        );
        assert_eq!(
            source(&sigma_request),
            Some(WeightSource {
                rule: WeightRule::InverseSigmaSquared,
                variable: variable.to_string(),
                per_channel: spectral,
            })
        );
        let stored = delivered_weights(&observation, &stored_request);
        let derived = delivered_weights(&observation, &sigma_request);
        assert_eq!(stored.len(), derived.len());
        let (mut padding, mut zero, mut differ) = (0, 0, 0);
        for (i, (&weight, &derived)) in stored.iter().zip(&derived).enumerate() {
            if weight.is_nan() {
                assert!(derived.is_nan(), "{variable}: padding stays NaN at {i}");
                padding += 1;
                continue;
            }
            let expected = inverse_sigma_squared(sigma_beside(weight, i % 4));
            assert_eq!(derived.to_bits(), expected.to_bits(), "{variable} at {i}");
            zero += usize::from(derived == 0.0);
            differ += usize::from(derived != weight);
        }
        // A per-row weight is delivered over the sample's eight channels.
        assert_eq!(padding, 8 * 4, "{variable}: the removed row's cell");
        assert!(zero > 0 && differ > stored.len() / 2, "{variable}");
    }
}

/// Sigma-derived weights of a product subset are the subset's 1/σ²: the
/// transform comes before the gather.
#[test]
fn a_product_subset_takes_its_own_sigmas() {
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ms");
    rewrite_weights(&path, false);
    let observation = Msv2Observation::open(&path).expect("v2");
    let products = observation
        .catalogue()
        .partition(PartitionId(0))
        .correlations
        .clone();
    let subset = vec![products[3], products[0]];
    let all = AccessRequest {
        weights: from_sigmas(SigmaFallback::Refuse),
        ..request(0..8, usize::MAX)
    };
    let some = AccessRequest {
        correlations: CorrelationRequest {
            deliver: Deliver::These(subset),
            flags_from: FlagsFrom::Delivered,
        },
        ..all.clone()
    };
    let every = delivered_weights(&observation, &all);
    let kept = delivered_weights(&observation, &some);
    assert_eq!(kept.len() * 2, every.len());
    for (sample, pair) in kept.chunks(2).enumerate() {
        let full = &every[sample * 4..sample * 4 + 4];
        assert_eq!(
            [pair[0].to_bits(), pair[1].to_bits()],
            [full[0].to_bits(), full[3].to_bits()],
            "sample {sample}: products in the partition's order"
        );
    }
}

/// An MSv4 holds no sigmas. Refusing the fallback refuses a request that
/// reads weights, and only such a request; falling back to the stored
/// weights delivers them, and the estimate records that the stored weights
/// were read.
#[test]
fn msv4_states_its_sigma_fallback() {
    let root = tempfile::tempdir().expect("root");
    let (_, v4) = open_both(root.path());
    let stored_request = request(0..8, usize::MAX);
    let refused = AccessRequest {
        weights: from_sigmas(SigmaFallback::Refuse),
        ..stored_request.clone()
    };
    for result in [
        v4.estimate(&refused).map(|_| ()),
        v4.bind_read(&refused).map(|_| ()),
    ] {
        match result {
            Err(ObservationError::InvalidRequest { reason }) => {
                assert!(reason.contains("no sigmas"), "{reason}");
            }
            other => panic!("refused, not {other:?}"),
        }
    }
    let without_weights = AccessRequest {
        roles: Roles {
            weights: false,
            ..Roles::ALL
        },
        ..refused
    };
    let estimate = v4.estimate(&without_weights).expect("no weights read");
    assert_eq!(estimate.partitions[0].weights, None);
    let fallback = AccessRequest {
        weights: from_sigmas(SigmaFallback::StoredWeights),
        ..stored_request.clone()
    };
    assert_eq!(
        v4.estimate(&fallback).expect("estimate").partitions[0].weights,
        Some(WeightSource {
            rule: WeightRule::Stored,
            variable: "WEIGHT".to_string(),
            per_channel: true,
        })
    );
    let read = |request: &AccessRequest| {
        samples(&mut v4.bind_read(request).expect("bind"), v4.catalogue()).expect("read")
    };
    assert_eq!(read(&fallback), read(&stored_request));
}

/// An updater writing weights is bound only to a traversal of the stored
/// weights: patches made from 1/σ² would write sigmas as weights. A flag
/// write beside a sigma-weighted traversal is bound.
#[test]
fn a_weight_write_refuses_sigma_derived_weights() {
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ms");
    let targets = [
        UpdateTarget::new("base", WritableRole::Weights),
        UpdateTarget::new("base", WritableRole::Flags),
    ];
    let session = open_update(&path, &OpenOptions::default(), &targets).expect("session");
    let sigma_request = AccessRequest {
        weights: from_sigmas(SigmaFallback::Refuse),
        ..request(0..8, usize::MAX)
    };
    match session.updater(&sigma_request, &[0]).map(|_| ()) {
        Err(ObservationError::InvalidRequest { reason }) => {
            assert!(reason.contains("stored weights"), "{reason}");
        }
        other => panic!("refused, not {other:?}"),
    }
    session
        .updater(&sigma_request, &[1])
        .expect("a flag write is bound");
    // Bound to the stored weights, the updater still refuses a weight
    // patch from a batch whose weights were made from the sigmas: each
    // batch states its rule.
    let stored_request = request(0..8, usize::MAX);
    let mut updater = session.updater(&stored_request, &[0]).expect("bound");
    let mut traversal = session
        .observation()
        .bind_read(&sigma_request)
        .expect("bind");
    let mut buffers = BatchBuffers::default();
    let Some(ReadEvent::Batch(batch)) = traversal.next(&mut buffers).expect("read") else {
        panic!("a batch");
    };
    assert_eq!(batch.group.weight_rule, WeightRule::InverseSigmaSquared);
    let weights = batch.group.weights.expect("weights").to_owned();
    match updater.write(
        &batch,
        &[PatchValues::Weights {
            values: weights.view(),
            channels: WeightChannels::All,
        }],
    ) {
        Err(ObservationError::InvalidRequest { reason }) => {
            assert!(reason.contains("made from the sigmas"), "{reason}");
        }
        other => panic!("refused, not {other:?}"),
    }
}

/// An MSv4's fallback reads its stored weights, so a weight write may be
/// bound beside it, and its batches state the stored rule.
#[test]
fn a_weight_write_binds_beside_an_msv4_fallback() {
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ps.zarr");
    let targets = [UpdateTarget::new("base", WritableRole::Weights)];
    let session = open_update(&path, &OpenOptions::default(), &targets).expect("session");
    let fallback = AccessRequest {
        weights: from_sigmas(SigmaFallback::StoredWeights),
        ..request(0..8, usize::MAX)
    };
    let mut updater = session.updater(&fallback, &[0]).expect("bound");
    let mut traversal = session.observation().bind_read(&fallback).expect("bind");
    let mut buffers = BatchBuffers::default();
    while let Some(ReadEvent::Batch(batch)) = traversal.next(&mut buffers).expect("read") {
        assert_eq!(batch.group.weight_rule, WeightRule::Stored);
        let weights = batch.group.weights.expect("weights").to_owned();
        updater
            .write(
                &batch,
                &[PatchValues::Weights {
                    values: weights.view(),
                    channels: WeightChannels::Delivered,
                }],
            )
            .expect("stored weights write back");
    }
    updater.finish().expect("finish");
}

/// A spectral column added and never filled holds no content, and CASA's
/// rule reads past it, as casacore's `hasContent` tells: with
/// `WEIGHT_SPECTRUM` and `SIGMA_SPECTRUM` both empty the stored weights are
/// `WEIGHT` and the sigmas `SIGMA`; with only `SIGMA_SPECTRUM` filled the
/// sigmas are still `SIGMA`, since CASA reads `SIGMA_SPECTRUM` only beside
/// a `WEIGHT_SPECTRUM` with content.
#[test]
fn spectral_columns_without_content_are_read_past() {
    for fill_sigmas in [false, true] {
        let root = tempfile::tempdir().expect("root");
        let path = fixture(root.path(), "ms_trial.ms");
        let plain = {
            let observation = Msv2Observation::open(&path).expect("v2");
            delivered_weights(&observation, &request(0..8, usize::MAX))
        };
        let mut ms = casa_ms::MeasurementSet::open(&path).expect("ms");
        let columns = [
            *OptionalMainColumn::WeightSpectrum.column_def(),
            *OptionalMainColumn::SigmaSpectrum.column_def(),
        ];
        let schema = build_table_schema(&columns).expect("schema");
        for name in ["WEIGHT_SPECTRUM", "SIGMA_SPECTRUM"] {
            let definition = schema.column(name).expect("definition").clone();
            ms.main_table_mut()
                .add_column(definition, None)
                .expect("add column");
        }
        if fill_sigmas {
            for row in 0..ms.main_table().row_count() {
                ms.main_table_mut()
                    .cell_accessor_mut(row, "SIGMA_SPECTRUM")
                    .and_then(|mut cell| {
                        cell.set(Value::Array(ArrayValue::Float32(ArrayD::from_elem(
                            vec![4, 8],
                            0.25,
                        ))))
                    })
                    .expect("set cell");
            }
        }
        ms.save().expect("save");
        drop(ms);
        let observation = Msv2Observation::open(&path).expect("v2");
        let stored = request(0..8, usize::MAX);
        let sigma = AccessRequest {
            weights: from_sigmas(SigmaFallback::Refuse),
            ..stored.clone()
        };
        let source = |request: &AccessRequest| {
            observation.estimate(request).expect("estimate").partitions[0]
                .weights
                .clone()
                .expect("weights read")
        };
        assert_eq!(
            (
                source(&stored).variable.as_str(),
                source(&sigma).variable.as_str()
            ),
            ("WEIGHT", "SIGMA"),
            "SIGMA_SPECTRUM filled: {fill_sigmas}"
        );
        assert_eq!(delivered_weights(&observation, &stored), plain);
    }
}

/// Each partition reads `WEIGHT_SPECTRUM` by its own first row, as CASA
/// asks again at each new spectral window, at the first row it meets there
/// (`VisibilityIteratorImpl2::weightSpectrumExists`): with the column
/// filled for one of two partitions only (two sessions of one window),
/// that partition's stored weights are its `WEIGHT_SPECTRUM` cells, and the
/// other's its `WEIGHT`, whether or not the partition holds MAIN's first
/// row. CASA iterates the two sessions as one window, and so answers once
/// for both.
#[test]
fn each_partition_reads_weight_spectrum_by_its_first_row() {
    for filled in [0, 1] {
        let root = tempfile::tempdir().expect("root");
        let path = fixture(root.path(), "ms_trial.ms");
        super::convert::split_sessions(&path, false);
        let plain = {
            let observation = Msv2Observation::open(&path).expect("v2");
            [0, 1].map(|partition| {
                delivered_weights(
                    &observation,
                    &AccessRequest {
                        partitions: vec![PartitionId(partition)],
                        ..request(0..8, usize::MAX)
                    },
                )
            })
        };
        let mut ms = casa_ms::MeasurementSet::open(&path).expect("ms");
        let definition = build_table_schema(&[*OptionalMainColumn::WeightSpectrum.column_def()])
            .expect("schema")
            .column("WEIGHT_SPECTRUM")
            .expect("definition")
            .clone();
        ms.main_table_mut()
            .add_column(definition, None)
            .expect("add column");
        let session = |row: usize| match ms
            .main_table()
            .cell_accessor(row, "OBSERVATION_ID")
            .and_then(|cell| cell.scalar())
        {
            Ok(casa_types::ScalarValue::Int32(id)) => *id as u32,
            other => panic!("OBSERVATION_ID of row {row}: {other:?}"),
        };
        let rows: Vec<usize> = (0..ms.main_table().row_count())
            .filter(|row| session(*row) == filled)
            .collect();
        for row in &rows {
            let cell =
                ArrayD::from_shape_fn(vec![4, 8], |index| stored_weight(*row, index[0], index[1]));
            ms.main_table_mut()
                .cell_accessor_mut(*row, "WEIGHT_SPECTRUM")
                .and_then(|mut target| target.set(Value::Array(ArrayValue::Float32(cell))))
                .expect("set cell");
        }
        ms.save().expect("save");
        drop(ms);
        let observation = Msv2Observation::open(&path).expect("v2");
        for partition in [0, 1] {
            let spectral = partition == filled;
            let entry = &observation.catalogue().partitions[partition as usize];
            assert_eq!(entry.observation.0, partition, "partition {partition}");
            let request = AccessRequest {
                partitions: vec![PartitionId(partition)],
                ..request(0..8, usize::MAX)
            };
            let source = observation.estimate(&request).expect("estimate").partitions[0]
                .weights
                .clone()
                .expect("weights read");
            let expected = if spectral {
                "WEIGHT_SPECTRUM"
            } else {
                "WEIGHT"
            };
            assert_eq!(
                entry.data_groups[0].weights, expected,
                "partition {partition}"
            );
            assert_eq!(
                (source.variable.as_str(), source.per_channel),
                (expected, spectral),
                "partition {partition}, filled {filled}"
            );
            let delivered = delivered_weights(&observation, &request);
            if spectral {
                let sorted = |mut weights: Vec<f32>| {
                    weights.sort_by(f32::total_cmp);
                    weights
                };
                let cells = rows
                    .iter()
                    .flat_map(|row| {
                        (0..8).flat_map(move |channel| {
                            (0..4).map(move |product| stored_weight(*row, product, channel))
                        })
                    })
                    .collect();
                assert_eq!(sorted(delivered), sorted(cells), "filled {filled}");
            } else {
                assert_eq!(
                    delivered, plain[partition as usize],
                    "partition {partition}"
                );
            }
        }
    }
}

/// A MAIN row of CASA's ALMA test MS by spectral window, time (its bits)
/// and antennas.
type RowKey = (u32, u64, u32, u32);

/// Each MAIN row's `WEIGHT` and `SIGMA` of the MSv2 at `path`, as
/// `casa_ms::MeasurementSet` reads its cells, by [`RowKey`].
fn weights_and_sigmas(path: &Path) -> BTreeMap<RowKey, [Vec<f32>; 2]> {
    let ms = casa_ms::MeasurementSet::open(path).expect("ms");
    let int = |table: &casa_tables::Table, row: usize, column: &str| match table
        .cell_accessor(row, column)
        .and_then(|cell| cell.scalar())
    {
        Ok(ScalarValue::Int32(value)) => u32::try_from(*value).expect("an index"),
        other => panic!("{column} of row {row}: {other:?}"),
    };
    let floats = |row: usize, column: &str| match ms
        .main_table()
        .cell_accessor(row, column)
        .and_then(|cell| cell.array())
    {
        Ok(ArrayValue::Float32(values)) => values.iter().copied().collect::<Vec<_>>(),
        other => panic!("{column} of row {row}: {other:?}"),
    };
    let descriptions = ms
        .subtable(casa_ms::SubtableId::DataDescription)
        .expect("DATA_DESCRIPTION");
    let main = ms.main_table();
    (0..main.row_count())
        .map(|row| {
            let time = match main
                .cell_accessor(row, "TIME")
                .and_then(|cell| cell.scalar())
            {
                Ok(ScalarValue::Float64(time)) => *time,
                other => panic!("TIME of row {row}: {other:?}"),
            };
            let description = int(main, row, "DATA_DESC_ID") as usize;
            let key = (
                int(descriptions, description, "SPECTRAL_WINDOW_ID"),
                time.to_bits(),
                int(main, row, "ANTENNA1"),
                int(main, row, "ANTENNA2"),
            );
            (key, [floats(row, "WEIGHT"), floats(row, "SIGMA")])
        })
        .collect()
}

/// CASA's ALMA test MS (`mssel_test_small.ms`) tiles `WEIGHT` and `SIGMA`
/// one correlation to a tile (`TiledShapeStMan`, tile `[1, 131072]`), so
/// each cell lies in four tiles. Read with every role through the
/// observation boundary, each row's weights are its stored `WEIGHT`, and
/// its sigma-derived weights 1/σ² of its `SIGMA`, correlation by
/// correlation over every channel, in every partition and batch: each of
/// its 1058 rows delivered once a reading. casacore (arcae) reads the same
/// weights, 1.2e8 or 6.96e9, summing to 3.74532e12 a correlation.
#[test]
fn weights_tiled_a_correlation_to_a_tile_are_gathered() {
    let root = tempfile::tempdir().expect("root");
    let archive =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mssel_test_small.ms.tgz");
    tar::Archive::new(flate2::read::GzDecoder::new(
        std::fs::File::open(archive).expect("fixture"),
    ))
    .unpack(root.path())
    .expect("unpack");
    let path = root.path().join("mssel_test_small.ms");
    let main = casa_tables::Table::open(casa_tables::TableOptions::new(&path)).expect("MAIN");
    for column in ["WEIGHT", "SIGMA"] {
        assert_eq!(
            main.array_column_tile_shape(column).expect("tile shape"),
            Some(vec![1, 131_072]),
            "{column}: a correlation to a tile"
        );
    }
    drop(main);
    let expected = weights_and_sigmas(&path);
    assert_eq!(expected.len(), 1058, "a key a row");
    let mut sums = [0.0f64; 4];
    for [weights, _] in expected.values() {
        for (sum, weight) in sums.iter_mut().zip(weights) {
            *sum += f64::from(*weight);
        }
    }
    assert_eq!(sums, [3.74532e12; 4], "casacore's WEIGHT sums");

    let observation = casa_ms::observation_io::open(&path, &OpenOptions::default()).expect("open");
    let catalogue = observation.catalogue().clone();
    let mjd_zero = MjdHighPrec::new(0.0, 0.0);
    for (reading, weights) in [
        (0, WeightRequest::Stored),
        (1, from_sigmas(SigmaFallback::Refuse)),
    ] {
        let mut delivered = 0;
        for (index, partition) in catalogue.partitions.iter().enumerate() {
            let whole = AccessRequest {
                partitions: vec![PartitionId(u32::try_from(index).expect("partition"))],
                data_group: partition.data_groups[0].name.clone(),
                roles: Roles::ALL,
                channels: 0..partition.frequencies_hz.len(),
                times: TimeSelection::All,
                fields: FieldSelection::All,
                weights,
                correlations: CorrelationRequest::ALL,
                grouping: Grouping::None,
                batch_bytes: usize::MAX,
            };
            let request = per_times(observation.as_ref(), whole, 3);
            let mut traversal = observation.bind_read(&request).expect("bind");
            let mut buffers = BatchBuffers::default();
            while let Some(event) = traversal.next(&mut buffers).expect("read") {
                let ReadEvent::Batch(batch) = event else {
                    continue;
                };
                let values = batch.group.weights.expect("weights");
                let per_time = batch.samples_per_time();
                for sample in 0..batch.samples() {
                    let cell = values.slice(ndarray::s![sample, .., ..]);
                    if cell.iter().all(|value| value.is_nan()) {
                        continue;
                    }
                    let baseline = batch.sample(sample).baseline;
                    let key = (
                        partition.window.0,
                        batch
                            .time
                            .seconds_since(sample / per_time, mjd_zero)
                            .to_bits(),
                        baseline.antenna1.0,
                        baseline.antenna2.0,
                    );
                    let row = &expected.get(&key).expect("a MAIN row")[reading];
                    for channel in cell.rows() {
                        let want = row.iter().map(|&value| {
                            if reading == 0 {
                                value
                            } else {
                                inverse_sigma_squared(value)
                            }
                        });
                        assert_eq!(
                            channel
                                .iter()
                                .map(|value| value.to_bits())
                                .collect::<Vec<_>>(),
                            want.map(f32::to_bits).collect::<Vec<_>>(),
                            "reading {reading}, row {key:?}"
                        );
                    }
                    delivered += 1;
                }
            }
        }
        assert_eq!(
            delivered,
            expected.len(),
            "reading {reading}: every row once"
        );
    }
}
