// SPDX-License-Identifier: LGPL-3.0-or-later
//! Counting and scheduling law for the legacy imager's model-plane batches.

use super::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

struct CountingHeap;

#[global_allocator]
static HEAP: CountingHeap = CountingHeap;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

// SAFETY: forward the original layout and pointer, counting only allocations
// that successfully went through the system allocator.
unsafe impl GlobalAlloc for CountingHeap {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            let after = LIVE.fetch_add(layout.size(), Ordering::SeqCst) + layout.size();
            PEAK.fetch_max(after, Ordering::SeqCst);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::SeqCst);
        unsafe { System.dealloc(pointer, layout) };
    }
}

#[test]
fn model_planes_use_distinct_workers_within_their_charge() {
    const CHILD: &str = "CASA_RS_MODEL_PLANE_HEAP_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let test = format!(
            "{}::model_planes_use_distinct_workers_within_their_charge",
            module_path!()
        );
        let name = test.split_once("::").expect("test module").1;
        let output = Command::new(std::env::current_exe().expect("test binary"))
            .args(["--exact", name])
            .env(CHILD, "1")
            .output()
            .expect("isolated counting test");
        let receipt = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && receipt.contains("1 passed"),
            "{receipt}"
        );
        return;
    }

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
    let correlations = [9, 12].map(|code| Correlation::from_casacore_code(code).unwrap());
    let routing =
        PolarizationRouting::compile(&correlations, &[PolarizationCoordinate::StokesI]).unwrap();
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
    let plane_bytes = model_plane_memory(shape, 1, grid_shape, grid_polarizations);
    let workers = 2;
    let charge = model_worker_memory(workers, plane_bytes);
    let seen = Mutex::new(Vec::new());
    let ready = Condvar::new();
    let mut written = 0;

    let baseline = LIVE.load(Ordering::SeqCst);
    PEAK.store(baseline, Ordering::SeqCst);
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
            operator.prepare_model(&model, ModelPrescale::Unit)
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
    let measured = PEAK.load(Ordering::SeqCst) - baseline + workers * WORKER_STACK_BYTES;
    assert!(
        measured as u64 <= charge,
        "{measured} bytes used over {charge}"
    );
}
