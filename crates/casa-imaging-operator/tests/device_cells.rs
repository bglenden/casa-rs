// SPDX-License-Identifier: LGPL-3.0-or-later
//! The host's fence on device cells ([`DeviceCells::defer`]): after device
//! work is deferred, no read or write of the cells returns before that work
//! is done, on any thread and through every accumulator accessor, and a
//! wait that panics leaves the cells unreachable. A thread stands in for
//! the device: once released it writes every cell, and the deferred wait
//! joins it.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Duration;

use casa_imaging_operator::{
    AccumulatorLayout, DeviceCells, GridAccumulator, GridGeometry, GridPadding, GridPrecision,
    GridScalar, GridStorage, ImageExtent, ModeSet, PlaneRange,
};
use num_complex::Complex32;

/// What the stand-in device writes into every cell.
const WRITTEN: Complex32 = Complex32::new(1.0, -1.0);
/// How long a reader that should be waiting is given to return early.
const EARLY: Duration = Duration::from_millis(200);

fn layout() -> AccumulatorLayout {
    let geometry = GridGeometry::new(
        ImageExtent {
            shape: [16, 16],
            increment_rad: [-1.0e-5, 1.0e-5],
            reference_pixel: [8, 8],
        },
        GridPadding::None,
    )
    .expect("geometry");
    AccumulatorLayout::new(
        geometry,
        PlaneRange::single(0),
        1,
        ModeSet::DATA,
        1,
        1,
        None,
    )
}

/// The cells' address, for the stand-in device.
struct Cells(NonNull<Complex32>, usize);

// SAFETY: the stand-in device thread is the only one that writes through
// the address, and only while the fence holds the host back.
unsafe impl Send for Cells {}

impl Cells {
    fn write_all(&self) {
        // SAFETY: the deferred wait joins this thread before the host reaches
        // the cells, so this is their only access.
        unsafe { std::slice::from_raw_parts_mut(self.0.as_ptr(), self.1) }.fill(WRITTEN);
    }
}

/// An accumulator whose cells live in memory the host does not reach
/// directly, as a device backend's do.
fn device_accumulator() -> (GridAccumulator, Cells) {
    let layout = layout();
    let len = layout.cells();
    let mut memory = vec![Complex32::default(); len];
    let address = NonNull::new(memory.as_mut_ptr()).expect("allocated");
    // SAFETY: `memory` holds `len` zeroed cells and keeps them in place
    // while the box owns it; only the stand-in device and these cells reach
    // them.
    let cells = unsafe { DeviceCells::new(address, len, Box::new(memory)) };
    let acc = GridAccumulator::with_storage(layout, GridStorage::Device(cells));
    (acc, Cells(address, len))
}

/// Deferred stand-in device work on `acc`'s cells.
struct Device {
    /// Lets the device write the cells.
    release: Sender<()>,
    /// Signalled when the host starts waiting for the device.
    entered: Receiver<()>,
    /// Set once the device has written every cell.
    done: Arc<AtomicBool>,
}

fn defer_device_work(acc: &GridAccumulator, cells: Cells) -> Device {
    let (release, released) = mpsc::channel();
    let (enter, entered) = mpsc::channel();
    let done = Arc::new(AtomicBool::new(false));
    let writer = {
        let done = Arc::clone(&done);
        thread::spawn(move || {
            if released.recv().is_ok() {
                cells.write_all();
                done.store(true, Ordering::SeqCst);
            }
        })
    };
    let wait = Box::new(move || {
        let _ = enter.send(());
        writer.join().expect("the stand-in device");
    });
    // SAFETY: the wait returns once the device thread has finished, and
    // nothing else writes the cells.
    unsafe { acc.storage().device().expect("device cells").defer(wait) };
    Device {
        release,
        entered,
        done,
    }
}

type Read = fn(&GridAccumulator) -> Vec<Complex32>;

/// Every host path that reads an accumulator's cells.
const READS: [(&str, Read); 5] = [
    ("DeviceCells::cells", |acc| {
        acc.storage().device().expect("device").cells().to_vec()
    }),
    ("GridScalar::cells", |acc| {
        f32::cells(acc.storage()).to_vec()
    }),
    ("GridAccumulator::block", |acc| {
        acc.block::<f32>(0, 0, 0).to_vec()
    }),
    ("Clone", |acc| f32::cells(acc.clone().storage()).to_vec()),
    ("merge_from", |acc| {
        let mut host = GridAccumulator::new(acc.layout().clone(), GridPrecision::F32);
        host.merge_from(acc).expect("same layout");
        f32::cells(host.storage()).to_vec()
    }),
];

/// Two readers on two threads after deferred device work: the first runs
/// the wait and the second arrives while it runs. Neither returns before
/// the device is done, and both see every cell it wrote.
#[test]
fn concurrent_readers_wait_for_deferred_device_work() {
    for (index, (first_name, first)) in READS.into_iter().enumerate() {
        let (second_name, second) = READS[(index + 1) % READS.len()];
        let label = format!("{first_name} then {second_name}");
        let (acc, cells) = device_accumulator();
        let device = defer_device_work(&acc, cells);
        let (returned, results) = mpsc::channel();
        thread::scope(|scope| {
            let read = |read: Read| {
                let returned = returned.clone();
                let (acc, done) = (&acc, &device.done);
                move || {
                    let cells = read(acc);
                    let _ = returned.send((done.load(Ordering::SeqCst), cells));
                }
            };
            scope.spawn(read(first));
            device.entered.recv().expect("the first reader waits");
            scope.spawn(read(second));
            let early = results.recv_timeout(EARLY).ok();
            // Released before any assertion, so a failing law still lets the
            // first reader's wait return and the scope end.
            device.release.send(()).expect("the device is waiting");
            assert!(
                early.is_none(),
                "{label}: a reader returned while the device was writing"
            );
            for _ in 0..2 {
                let (done, cells) = results.recv().expect("both readers return");
                assert!(
                    done,
                    "{label}: a reader returned before the device finished"
                );
                assert!(
                    cells.iter().all(|cell| *cell == WRITTEN),
                    "{label}: a reader saw cells the device had not written"
                );
            }
        });
    }
}

/// A host write after deferred device work waits for it first, so the
/// write lands on the device's cells rather than under them.
#[test]
fn writes_wait_for_deferred_device_work() {
    type Write = fn(&mut GridAccumulator) -> Complex32;
    let writes: [(&str, Write); 2] = [
        ("reset", |acc| {
            acc.reset();
            Complex32::default()
        }),
        ("merge_from", |acc| {
            let host = GridAccumulator::new(acc.layout().clone(), GridPrecision::F32);
            acc.merge_from(&host).expect("same layout");
            WRITTEN
        }),
    ];
    for (name, write) in writes {
        let (mut acc, cells) = device_accumulator();
        let Device {
            release,
            entered,
            done,
        } = defer_device_work(&acc, cells);
        // Released only once the host waits: a write that skipped the wait
        // would never let the device run.
        thread::spawn(move || {
            if entered.recv().is_ok() {
                let _ = release.send(());
            }
        });
        let expected = write(&mut acc);
        assert!(
            done.load(Ordering::SeqCst),
            "{name} returned before the device finished"
        );
        assert!(
            f32::cells(acc.storage())
                .iter()
                .all(|cell| *cell == expected),
            "{name} did not land on the device's cells"
        );
    }
}

/// A wait that panics may have returned before the device finished, so
/// the cells are never handed out after it.
#[test]
fn a_wait_that_panics_leaves_the_cells_unreachable() {
    let (acc, _cells) = device_accumulator();
    let device = acc.storage().device().expect("device cells");
    // SAFETY: the cells are never reached after this wait: the law is that
    // every access panics.
    unsafe { device.defer(Box::new(|| panic!("the device failed"))) };
    for attempt in ["first", "second"] {
        let read = catch_unwind(AssertUnwindSafe(|| device.cells().len()));
        assert!(read.is_err(), "the {attempt} read after a panicked wait");
    }
}
