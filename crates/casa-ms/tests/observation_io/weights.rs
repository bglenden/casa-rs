// SPDX-License-Identifier: LGPL-3.0-or-later
//! The weight role's two readings: the stored weights, or weights made from
//! the stored sigmas as 1/σ², as CASA's solvers weight a solve. An MSv2
//! delivers 1/σ² from `SIGMA_SPECTRUM` when MAIN has both it and
//! `WEIGHT_SPECTRUM`, else from `SIGMA`; a σ that is not positive gives 0;
//! padding stays NaN. An MSv4 holds no sigmas: the request's fallback is
//! taken and recorded, or the request is refused. A weight write is never
//! bound to sigma-derived weights.

use casa_ms::OptionalMainColumn;
use casa_ms::column_def::build_table_schema;
use casa_ms::observation_io::{Msv2Observation, OpenOptions, open_update};
use casa_observation::{
    AccessRequest, BatchBuffers, CorrelationRequest, Deliver, FlagsFrom, Observation,
    ObservationError, PartitionId, ReadEvent, Roles, SigmaFallback, UpdateTarget, WeightRequest,
    WeightRule, WeightSource, WritableRole, inverse_sigma_squared,
};
use casa_types::{ArrayValue, Value};
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
}
