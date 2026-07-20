// SPDX-License-Identifier: LGPL-3.0-or-later
//! Workload-aware memory budgeting for interactive image-browser processes.

#[cfg(target_os = "linux")]
use std::fs;

use thiserror::Error;

use crate::movie::{ImageMoviePlan, ImageMoviePlanError, ImageMoviePlanRequest};

/// Strict override for the image-browser process memory budget.
pub const IMAGE_BROWSER_MEMORY_BUDGET_ENV: &str = "CASARS_IMEXPLORE_MEMORY_BUDGET_BYTES";

/// Origin of a process-local image-browser budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageBrowserMemoryBudgetSource {
    ExplicitEnvironment,
    SystemAvailableSnapshot,
    #[cfg(test)]
    Test,
}

/// Memory available to one image-browser process at session start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImageBrowserMemoryBudget {
    total_bytes: u64,
    source: ImageBrowserMemoryBudgetSource,
}

/// Bytes already owned outside the movie renderer but within the same process.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ImageBrowserResourceClaims {
    pub backend_view_cache_bytes: u64,
    pub backend_result_cache_bytes: u64,
    pub static_render_cache_bytes: u64,
    pub protocol_staging_bytes: u64,
}

/// A complete process-local image-browser allocation plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageBrowserResourcePlan {
    pub budget: ImageBrowserMemoryBudget,
    pub fixed_claims: ImageBrowserResourceClaims,
    pub fixed_reserved_bytes: u64,
    pub movie: ImageMoviePlan,
    pub total_reserved_bytes: u64,
    pub unreserved_bytes: u64,
}

/// Inputs for the backend plane-reader/prefetch working set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImageBackendResourceRequest {
    pub plane_pixel_size: (usize, usize),
    pub prefetch_frame_count: usize,
    pub available_parallelism: usize,
}

/// Deterministic backend allocation plan derived from viewport geometry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImageBackendResourcePlan {
    pub view_cache_bytes_per_reader: usize,
    pub plane_result_cache_bytes: u64,
    pub protocol_staging_bytes: u64,
    pub prefetch_worker_count: usize,
    pub prefetch_lookahead_frames: usize,
    pub total_reserved_bytes: u64,
    pub unreserved_bytes: u64,
}

#[derive(Debug, Error)]
pub enum ImageBrowserResourceError {
    #[error("{IMAGE_BROWSER_MEMORY_BUDGET_ENV} must be a positive byte count, got {value:?}")]
    InvalidExplicitBudget { value: String },
    #[error(
        "operating-system available memory is unavailable; set {IMAGE_BROWSER_MEMORY_BUDGET_ENV}"
    )]
    AvailableMemoryUnavailable,
    #[error("image-browser resource claim arithmetic overflowed")]
    ByteOverflow,
    #[error(
        "fixed image-browser resources require {required_bytes} bytes but the session budget is {budget_bytes} bytes"
    )]
    FixedClaimsExceedBudget {
        required_bytes: u64,
        budget_bytes: u64,
    },
    #[error(transparent)]
    Movie(#[from] ImageMoviePlanError),
    #[error("backend plane geometry or available parallelism is invalid")]
    InvalidBackendWorkload,
    #[error(
        "image backend requires at least {required_bytes} bytes but the session budget is {budget_bytes} bytes"
    )]
    BackendWorkingSetExceedsBudget {
        required_bytes: u64,
        budget_bytes: u64,
    },
}

impl ImageBrowserMemoryBudget {
    /// Snapshot the strict override or OS-reported available memory.
    pub fn from_process_snapshot() -> Result<Self, ImageBrowserResourceError> {
        if let Some(value) = std::env::var_os(IMAGE_BROWSER_MEMORY_BUDGET_ENV) {
            let value = value.to_string_lossy().into_owned();
            let total_bytes = value
                .parse::<u64>()
                .ok()
                .filter(|bytes| *bytes > 0)
                .ok_or_else(|| ImageBrowserResourceError::InvalidExplicitBudget {
                    value: value.clone(),
                })?;
            return Ok(Self {
                total_bytes,
                source: ImageBrowserMemoryBudgetSource::ExplicitEnvironment,
            });
        }
        let total_bytes = system_available_memory_bytes()
            .filter(|bytes| *bytes > 0)
            .ok_or(ImageBrowserResourceError::AvailableMemoryUnavailable)?;
        Ok(Self {
            total_bytes,
            source: ImageBrowserMemoryBudgetSource::SystemAvailableSnapshot,
        })
    }

    #[cfg(test)]
    pub(crate) fn for_test(total_bytes: u64) -> Self {
        assert!(total_bytes > 0);
        Self {
            total_bytes,
            source: ImageBrowserMemoryBudgetSource::Test,
        }
    }

    pub fn total_bytes(self) -> u64 {
        self.total_bytes
    }

    pub fn source(self) -> ImageBrowserMemoryBudgetSource {
        self.source
    }

    /// Allocate fixed owners first, then let the nested movie planner reserve
    /// only the workload it needs from the remainder.
    pub fn plan_movie(
        self,
        fixed_claims: ImageBrowserResourceClaims,
        mut request: ImageMoviePlanRequest,
    ) -> Result<ImageBrowserResourcePlan, ImageBrowserResourceError> {
        let fixed_reserved_bytes = fixed_claims.checked_total()?;
        let movie_budget_bytes = self.total_bytes.checked_sub(fixed_reserved_bytes).ok_or(
            ImageBrowserResourceError::FixedClaimsExceedBudget {
                required_bytes: fixed_reserved_bytes,
                budget_bytes: self.total_bytes,
            },
        )?;
        request.memory_budget_bytes = movie_budget_bytes;
        let movie = ImageMoviePlan::build(&request)?;
        let total_reserved_bytes = fixed_reserved_bytes
            .checked_add(movie.reserved_bytes)
            .ok_or(ImageBrowserResourceError::ByteOverflow)?;
        let unreserved_bytes = self
            .total_bytes
            .checked_sub(total_reserved_bytes)
            .ok_or(ImageBrowserResourceError::ByteOverflow)?;
        Ok(ImageBrowserResourcePlan {
            budget: self,
            fixed_claims,
            fixed_reserved_bytes,
            movie,
            total_reserved_bytes,
            unreserved_bytes,
        })
    }

    /// Plan the main image reader, optional prefetch readers, cached plane
    /// results, and one protocol-serialization staging copy.
    pub fn plan_backend(
        self,
        request: ImageBackendResourceRequest,
    ) -> Result<ImageBackendResourcePlan, ImageBrowserResourceError> {
        if request.plane_pixel_size.0 == 0
            || request.plane_pixel_size.1 == 0
            || request.available_parallelism == 0
        {
            return Err(ImageBrowserResourceError::InvalidBackendWorkload);
        }
        let pixels = (request.plane_pixel_size.0 as u64)
            .checked_mul(request.plane_pixel_size.1 as u64)
            .ok_or(ImageBrowserResourceError::ByteOverflow)?;
        // The reader cache can simultaneously own one f64 source sample and
        // one mask byte for every output sample in its current plane.
        let view_cache_bytes_per_reader = pixels
            .checked_mul((std::mem::size_of::<f64>() + std::mem::size_of::<u8>()) as u64)
            .ok_or(ImageBrowserResourceError::ByteOverflow)?;
        // PlaneRaster retains one normalized u8 per output sample.
        let plane_bytes = pixels;
        let protocol_staging_bytes = plane_bytes;
        let main_reader_bytes = view_cache_bytes_per_reader
            .checked_add(plane_bytes)
            .and_then(|bytes| bytes.checked_add(protocol_staging_bytes))
            .ok_or(ImageBrowserResourceError::ByteOverflow)?;
        if main_reader_bytes > self.total_bytes {
            return Err(ImageBrowserResourceError::BackendWorkingSetExceedsBudget {
                required_bytes: main_reader_bytes,
                budget_bytes: self.total_bytes,
            });
        }
        let per_prefetch_bytes = view_cache_bytes_per_reader
            .checked_add(plane_bytes)
            .ok_or(ImageBrowserResourceError::ByteOverflow)?;
        let remaining = self.total_bytes - main_reader_bytes;
        let memory_workers = usize::try_from(remaining / per_prefetch_bytes).unwrap_or(usize::MAX);
        let prefetch_worker_count = request
            .prefetch_frame_count
            .min(request.available_parallelism.saturating_sub(1))
            .min(memory_workers);
        let prefetch_bytes = per_prefetch_bytes
            .checked_mul(prefetch_worker_count as u64)
            .ok_or(ImageBrowserResourceError::ByteOverflow)?;
        let total_reserved_bytes = main_reader_bytes
            .checked_add(prefetch_bytes)
            .ok_or(ImageBrowserResourceError::ByteOverflow)?;
        Ok(ImageBackendResourcePlan {
            view_cache_bytes_per_reader: usize::try_from(view_cache_bytes_per_reader)
                .unwrap_or(usize::MAX),
            plane_result_cache_bytes: plane_bytes
                .checked_mul(prefetch_worker_count.saturating_add(1) as u64)
                .ok_or(ImageBrowserResourceError::ByteOverflow)?,
            protocol_staging_bytes,
            prefetch_worker_count,
            prefetch_lookahead_frames: prefetch_worker_count,
            total_reserved_bytes,
            unreserved_bytes: self.total_bytes - total_reserved_bytes,
        })
    }
}

impl ImageBrowserResourceClaims {
    fn checked_total(self) -> Result<u64, ImageBrowserResourceError> {
        self.backend_view_cache_bytes
            .checked_add(self.backend_result_cache_bytes)
            .and_then(|bytes| bytes.checked_add(self.static_render_cache_bytes))
            .and_then(|bytes| bytes.checked_add(self.protocol_staging_bytes))
            .ok_or(ImageBrowserResourceError::ByteOverflow)
    }
}

#[cfg(target_os = "macos")]
#[allow(deprecated)]
fn system_available_memory_bytes() -> Option<u64> {
    let mut statistics = unsafe { std::mem::zeroed::<libc::vm_statistics64>() };
    let mut count = libc::HOST_VM_INFO64_COUNT;
    let result = unsafe {
        libc::host_statistics64(
            libc::mach_host_self(),
            libc::HOST_VM_INFO64,
            (&mut statistics as *mut libc::vm_statistics64).cast(),
            &mut count,
        )
    };
    if result != libc::KERN_SUCCESS {
        return None;
    }
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return None;
    }
    // These are the immediately free and reclaimable page classes reported by
    // the Mach VM snapshot. Wired, active, and compressed pages are excluded.
    let free = u64::from(statistics.free_count);
    let inactive = u64::from(statistics.inactive_count);
    let speculative = u64::from(statistics.speculative_count);
    free.checked_add(inactive)?
        .checked_add(speculative)?
        .checked_mul(page_size as u64)
}

#[cfg(target_os = "linux")]
fn system_available_memory_bytes() -> Option<u64> {
    let contents = fs::read_to_string("/proc/meminfo").ok()?;
    parse_linux_mem_available_bytes(&contents)
}

#[cfg(target_os = "linux")]
fn parse_linux_mem_available_bytes(contents: &str) -> Option<u64> {
    let line = contents
        .lines()
        .find(|line| line.starts_with("MemAvailable:"))?;
    let kib = line.split_ascii_whitespace().nth(1)?.parse::<u64>().ok()?;
    kib.checked_mul(1024)
}

#[cfg(all(unix, not(any(target_os = "macos", target_os = "linux"))))]
fn system_available_memory_bytes() -> Option<u64> {
    let pages = unsafe { libc::sysconf(libc::_SC_AVPHYS_PAGES) };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if pages <= 0 || page_size <= 0 {
        return None;
    }
    (pages as u64).checked_mul(page_size as u64)
}

#[cfg(not(unix))]
fn system_available_memory_bytes() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::time::Duration;

    use super::*;
    use crate::movie::{ImageMoviePlanRequest, ImageMovieSurfaceMemory};

    struct BudgetEnvironmentRestore(Option<OsString>);

    impl BudgetEnvironmentRestore {
        fn set(value: &str) -> Self {
            let previous = std::env::var_os(IMAGE_BROWSER_MEMORY_BUDGET_ENV);
            // SAFETY: all casars tests that mutate process environment acquire
            // the shared test environment lock first.
            unsafe { std::env::set_var(IMAGE_BROWSER_MEMORY_BUDGET_ENV, value) };
            Self(previous)
        }
    }

    impl Drop for BudgetEnvironmentRestore {
        fn drop(&mut self) {
            // SAFETY: the creating test retains the shared environment lock
            // until this guard has restored the original value.
            unsafe {
                match &self.0 {
                    Some(value) => std::env::set_var(IMAGE_BROWSER_MEMORY_BUDGET_ENV, value),
                    None => std::env::remove_var(IMAGE_BROWSER_MEMORY_BUDGET_ENV),
                }
            }
        }
    }

    fn movie_request() -> ImageMoviePlanRequest {
        ImageMoviePlanRequest {
            surfaces: vec![ImageMovieSurfaceMemory {
                pixel_size: (320, 200),
                resident_bytes_per_pixel: 3,
                worker_bytes_per_pixel: 4,
                resident_fixed_bytes: 0,
                worker_fixed_bytes: 0,
            }],
            frame_count: 20,
            requested_fps_milli: 10_000,
            render_latency: Duration::from_millis(100),
            available_parallelism: 4,
            memory_budget_bytes: 0,
        }
    }

    #[test]
    fn ledger_reserves_fixed_owners_before_movie_work() {
        let budget = ImageBrowserMemoryBudget::for_test(16 * 1024 * 1024);
        let claims = ImageBrowserResourceClaims {
            backend_view_cache_bytes: 1_000,
            backend_result_cache_bytes: 2_000,
            static_render_cache_bytes: 3_000,
            protocol_staging_bytes: 4_000,
        };
        let plan = budget.plan_movie(claims, movie_request()).unwrap();
        assert_eq!(plan.fixed_reserved_bytes, 10_000);
        assert_eq!(
            plan.total_reserved_bytes,
            plan.fixed_reserved_bytes + plan.movie.reserved_bytes
        );
        assert_eq!(
            plan.total_reserved_bytes + plan.unreserved_bytes,
            budget.total_bytes()
        );
    }

    #[test]
    fn ledger_rejects_fixed_claims_larger_than_budget() {
        let error = ImageBrowserMemoryBudget::for_test(9)
            .plan_movie(
                ImageBrowserResourceClaims {
                    static_render_cache_bytes: 10,
                    ..ImageBrowserResourceClaims::default()
                },
                movie_request(),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            ImageBrowserResourceError::FixedClaimsExceedBudget { .. }
        ));
    }

    #[test]
    fn explicit_budget_override_is_strict_and_identifies_its_source() {
        let _lock = crate::test_env_lock();
        let _restore = BudgetEnvironmentRestore::set("1048576");
        let budget = ImageBrowserMemoryBudget::from_process_snapshot().unwrap();
        assert_eq!(budget.total_bytes(), 1_048_576);
        assert_eq!(
            budget.source(),
            ImageBrowserMemoryBudgetSource::ExplicitEnvironment
        );
    }

    #[test]
    fn invalid_explicit_budget_does_not_fall_back_to_system_memory() {
        let _lock = crate::test_env_lock();
        let _restore = BudgetEnvironmentRestore::set("not-a-byte-count");
        let error = ImageBrowserMemoryBudget::from_process_snapshot().unwrap_err();
        assert!(matches!(
            error,
            ImageBrowserResourceError::InvalidExplicitBudget { value }
                if value == "not-a-byte-count"
        ));
    }

    #[test]
    fn backend_plan_counts_readers_results_and_protocol_staging() {
        let budget = ImageBrowserMemoryBudget::for_test(1_000_000);
        let plan = budget
            .plan_backend(ImageBackendResourceRequest {
                plane_pixel_size: (100, 50),
                prefetch_frame_count: 20,
                available_parallelism: 4,
            })
            .unwrap();
        assert_eq!(plan.view_cache_bytes_per_reader, 45_000);
        assert_eq!(plan.prefetch_worker_count, 3);
        assert_eq!(plan.prefetch_lookahead_frames, 3);
        assert_eq!(plan.plane_result_cache_bytes, 20_000);
        assert_eq!(plan.protocol_staging_bytes, 5_000);
        assert_eq!(
            plan.total_reserved_bytes + plan.unreserved_bytes,
            budget.total_bytes()
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parses_linux_available_memory_in_kib() {
        assert_eq!(
            parse_linux_mem_available_bytes("MemTotal: 8 kB\nMemAvailable: 123 kB\n"),
            Some(123 * 1024)
        );
    }
}
