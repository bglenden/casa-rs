// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! Metal implementation of the imaging gridding backend.
//!
//! This crate is the only place a device API appears in the imaging stack.
//! [`MetalBackend`] implements the `casa-imaging-operator` backend trait for
//! both tap layouts and every [`Work`](casa_imaging_operator::Work) variant
//! with support-generic kernels: one SIMD group per sample, its lanes over
//! the taps, `f32` grids updated with atomic adds (D2). The host locates
//! every sample with the operator's rounding rule and accumulates `sumwt`
//! in `f64`, so tap selection and `sumwt` equal the CPU backend's; only the
//! order of the `f32` grid additions differs.
//!
//! Accumulators live in memory the device addresses
//! ([`MetalBackend::accumulator`]). Every dispatch completes before `apply`
//! returns, except grid work a backend made with
//! [`MetalBackend::with_deferred_grid_completion`] leaves running; the
//! accumulator's cells wait for that work, so the host reads them like any
//! other accumulator. Without a
//! unified-memory Metal 3 device, constructors return
//! [`DeviceFailure::Unavailable`](casa_imaging_operator::DeviceFailure).
//!
//! The design is `docs/imaging-architecture/imaging-foundation-plan-20261007.md`
//! sections 5.3 and 5.9, ADR-0016, and the IF-4 deviations on #653.

#[cfg(all(target_os = "macos", not(coverage)))]
mod backend;
#[cfg(all(target_os = "macos", not(coverage)))]
mod device;
#[cfg(all(target_os = "macos", not(coverage)))]
mod records;
#[cfg(not(all(target_os = "macos", not(coverage))))]
mod unavailable;

use casa_imaging_operator::ConvolutionFunctionSet;

#[cfg(all(target_os = "macos", not(coverage)))]
pub use backend::MetalBackend;
/// The rows and noise terms [`MetalBackend::add_cell_noise`] reads.
pub use casa_numerics::cell_noise::{MetalNoiseRow, MetalNoiseTerm};
#[cfg(not(all(target_os = "macos", not(coverage))))]
pub use unavailable::MetalBackend;

/// The `tracing` target of the event a backend made with stage timing logs
/// when it drops: its dispatches, samples and sub-blocks, the kernel cells
/// it uploaded, the host's preparation time and its waits for the device,
/// and the device's time on its commands (summed, and the span from the
/// first start to the last end). A pass also logs, once per wave, its
/// source stream: the producer's fill time and the pass's waits for it.
pub const METAL_STAGE_TIMING: &str = "casa_imaging::metal_stage_timing";

/// Ring slots of one backend: the host prepares one while the device runs
/// the others. Twice the sub-blocks of one apply ([`SPLIT`]), so that an
/// apply whose grid work is left in flight
/// (`MetalBackend::with_deferred_grid_completion`) seldom waits for the apply
/// before it.
const RING: usize = 2 * SPLIT;
/// Sub-blocks one apply is split into, so the host prepares one while the
/// device runs another.
const SPLIT: usize = 3;
/// The most samples in one ring sub-block.
const MAX_SUB: usize = 32_768;
/// Device bytes per sample of a ring slot: the record, its native endpoints
/// (or a native term), and per visibility polarization a value, a weight,
/// an inverse norm and an output.
const SAMPLE_BYTES: usize = 40;
const SOURCE_BYTES: usize = 24;
const POLARIZATION_BYTES: usize = 8 + 4 + 8 + 8;

/// The most host/device memory one backend's ring holds for blocks of `npol`
/// visibility polarizations;
/// its kernel copies add [`kernel_bytes`], and a
/// native prediction its batch ([`batch_bytes`]).
#[must_use]
pub const fn ring_bytes(npol: usize) -> u64 {
    #[cfg(all(target_os = "macos", not(coverage)))]
    let host = backend::ring_host_bytes();
    #[cfg(not(all(target_os = "macos", not(coverage))))]
    let host = 0;
    (RING * MAX_SUB * (SAMPLE_BYTES + SOURCE_BYTES + npol * POLARIZATION_BYTES)) as u64 + host
}

/// Additional device-grid bytes for the local #991 rounding-loss experiment.
/// Two correction grids each hold an f32 complex pair per accumulation cell.
#[must_use]
pub const fn accumulator_extra_bytes(cells: u64) -> u64 {
    if cfg!(feature = "compensated-f32-prototype") {
        cells.saturating_mul(16)
    } else {
        0
    }
}

/// Device bytes of one kernel table record (`records::TableRecord`).
const TABLE_RECORD_BYTES: u64 = 16;
/// Host bytes a backend keeps per kernel cell beside its norms: its table
/// record, its `KnownTable` and their map entry, at the map's load factor
/// and growth.
const HOST_TABLE_BYTES: u64 = 256;
/// Host and device bytes a backend keeps for its kernel copies before it
/// meets a cell: its empty arenas and table buffer (16 bytes each) and its
/// map's first allocation.
const KERNEL_BASE_BYTES: u64 = 1024;

/// The most memory one backend holds for its copies of `cf`'s kernel
/// cells, which a pass admits for every backend it makes (#793 review):
/// the taps of every cell the set can lend, in a device arena reserved
/// whole from the set's footprint at the first upload, so it never grows
/// past them; a device table record per cell; and the host's norm tables
/// and table map ([`ConvolutionFunctionSet::tap_footprint`]). A backend
/// that meets only some cells holds less; `MetalBackend::kernel_bytes_held`
/// says how much.
#[must_use]
pub fn kernel_bytes(cf: &dyn ConvolutionFunctionSet) -> u64 {
    let footprint = cf.tap_footprint();
    footprint.tap_bytes
        + footprint.norm_bytes
        + footprint.cells as u64 * (TABLE_RECORD_BYTES + HOST_TABLE_BYTES)
        + KERNEL_BASE_BYTES
}

/// Bytes of every device buffer this crate holds now: kernel copies, rings,
/// accumulators, model copies and native cells of every live backend. Zero
/// on a host without Metal.
#[must_use]
pub fn device_bytes() -> u64 {
    #[cfg(all(target_os = "macos", not(coverage)))]
    {
        device::live_bytes()
    }
    #[cfg(not(all(target_os = "macos", not(coverage))))]
    {
        0
    }
}

/// The device memory a backend's predictions of a native-prediction batch
/// of `samples` samples of `npol` polarizations hold.
#[must_use]
pub const fn batch_bytes(npol: usize, samples: usize) -> u64 {
    (samples * npol * size_of::<num_complex::Complex32>()) as u64
}

/// The most device memory a backend holds for cell noise
/// (`MetalBackend::add_cell_noise`) of at most `rows` rows and `terms`
/// terms: the two buffers it copies them to, made at its first noise and
/// grown to the largest, each at least a buffer's 16 bytes. A backend that
/// adds no noise holds none.
#[must_use]
pub const fn noise_bytes(rows: usize, terms: usize) -> u64 {
    (buffer_bytes(rows * size_of::<MetalNoiseRow>())
        + buffer_bytes(terms * size_of::<MetalNoiseTerm>())) as u64
}

/// The bytes of a device buffer made for `bytes`: at least 16.
const fn buffer_bytes(bytes: usize) -> usize {
    if bytes > 16 { bytes } else { 16 }
}

/// Whether this host has a Metal device the backend can use: Metal 3 with
/// memory shared with the host.
#[must_use]
pub fn available() -> bool {
    #[cfg(all(target_os = "macos", not(coverage)))]
    {
        device::Device::present()
    }
    #[cfg(not(all(target_os = "macos", not(coverage))))]
    {
        false
    }
}
