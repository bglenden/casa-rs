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
//! ([`MetalBackend::accumulator`]); every dispatch completes before `apply`
//! returns, so the host reads them like any other accumulator. Without a
//! Metal 3 device, constructors return
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

#[cfg(all(target_os = "macos", not(coverage)))]
pub use backend::MetalBackend;
#[cfg(not(all(target_os = "macos", not(coverage))))]
pub use unavailable::MetalBackend;

/// Whether this host has a Metal device the backend can use.
#[must_use]
pub fn available() -> bool {
    #[cfg(all(target_os = "macos", not(coverage)))]
    {
        device::Device::shared().is_ok()
    }
    #[cfg(not(all(target_os = "macos", not(coverage))))]
    {
        false
    }
}
