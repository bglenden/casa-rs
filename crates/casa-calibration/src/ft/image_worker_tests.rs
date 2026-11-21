// SPDX-License-Identifier: LGPL-3.0-or-later
//! Counting and scheduling law for the legacy imager's model-plane batches.

use super::*;
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use crate::imaging_memory_test::{isolated_test, peak_since, reset_peak};

#[test]
fn model_planes_use_distinct_workers_within_their_charge() {
    if !isolated_test(
        module_path!(),
        "model_planes_use_distinct_workers_within_their_charge",
    ) {
        return;
    }
    check_model_plane_workers(false);
    check_model_plane_workers(true);
}

fn check_model_plane_workers(squinted: bool) {
    let channels = 11;
    let shape = [16, 16];
    let geometry = GridGeometry::new(
        ImageExtent {
            shape,
            increment_rad: [-1e-5, 1e-5],
            reference_pixel: [8, 8],
        },
        GridPadding::None,
    )
    .unwrap();
    let (correlations, stokes) = if squinted {
        ([5, 8], PolarizationCoordinate::StokesV)
    } else {
        ([9, 12], PolarizationCoordinate::StokesI)
    };
    let correlations = correlations.map(|code| Correlation::from_casacore_code(code).unwrap());
    let routing = PolarizationRouting::compile(&correlations, &[stokes]).unwrap();
    let kernel = Spheroidal::new(&geometry, &routing);
    let grid_shape = geometry.grid_shape();
    let grid_polarizations = routing.grid_pols();
    let operator = MeasurementOperator::new(
        geometry,
        Basis::ChannelLocal {
            planes: channels as u32,
        },
        routing,
        Box::new(kernel),
        GridPrecision::F64,
    );
    let mut plane_bytes = model_plane_memory(shape, 1, grid_shape, grid_polarizations);
    if squinted {
        plane_bytes += model_taper_bytes(shape, grid_polarizations);
    }
    let workers = 2;
    let charge = model_worker_memory(workers, plane_bytes);
    let seen = Mutex::new(Vec::new());
    let ready = Condvar::new();
    let mut written = 0;

    let baseline = reset_peak();
    let team = WorkerTeam::new(workers).unwrap();
    let nonzero = prepare_model_planes(
        &team,
        channels,
        |index| Ok(ArrayD::from_elem(IxDyn(&shape), index as f32 + 1.)),
        |index, plane| {
            if index < workers {
                let mut threads = seen.lock().unwrap();
                threads.push(std::thread::current().id());
                ready.notify_all();
                let (threads, _) = ready
                    .wait_timeout_while(threads, Duration::from_secs(3), |seen| {
                        seen.len() < workers
                    })
                    .unwrap();
                assert_eq!(threads.len(), workers);
                assert_ne!(threads[0], threads[1]);
            }
            let image = plane
                .to_owned()
                .into_dimensionality::<ndarray::Ix2>()
                .unwrap();
            let model = ModelImages {
                first_plane: index as u32,
                planes: vec![ModelPlane {
                    images: vec![image],
                }],
            };
            let factors = squinted.then(|| {
                (0..grid_polarizations)
                    .map(|pol| Array2::from_elem((shape[1], shape[0]), 1. - 0.1 * pol as f32))
                    .collect::<Vec<_>>()
            });
            let prescale = factors
                .as_deref()
                .map_or(ModelPrescale::Unit, ModelPrescale::GridPlanes);
            operator.prepare_model(&model, prescale).map_err(invalid)
        },
        |index, grid| {
            assert_eq!(index, written);
            assert!(
                grid.block::<f64>(0, 0, 0)
                    .iter()
                    .any(|value| value.norm_sqr() > 0.)
            );
            written += 1;
            Ok(())
        },
    )
    .unwrap();
    assert!(nonzero);
    assert_eq!(written, channels);
    let measured = peak_since(baseline) + (workers * WORKER_STACK_BYTES) as u64;
    assert!(measured <= charge, "{measured} bytes used over {charge}");
}
