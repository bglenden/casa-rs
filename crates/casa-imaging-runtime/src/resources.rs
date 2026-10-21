// SPDX-License-Identifier: LGPL-3.0-or-later
//! Host resources, the run's resource policy and the admission of each
//! phase's memory against them (plan section 5.4).
//!
//! The host is detected once per process. A policy turns it into the
//! workers and memory a run may use; each phase then admits the memory its
//! owners say it holds ([`admit`]) and keeps it until the returned
//! [`Reservation`] drops. Reservations of every run in the process count
//! against each run's policy.

#[cfg(target_os = "macos")]
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};

/// The host's capacity, detected once per process by
/// [`HostResources::detect`]; tests construct it with pinned values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HostResources {
    /// Logical CPU threads.
    pub threads: usize,
    /// Performance-class cores; `threads` on a host that reports no core
    /// classes.
    pub performance_cores: usize,
    /// Bytes of memory free for new allocations when the host was detected:
    /// free, inactive and speculative pages on macOS, `MemAvailable` on
    /// Linux.
    pub available_memory: u64,
    /// Whether a unified-memory Metal 3 device is present.
    pub metal: bool,
}

/// Why the host could not be detected: one variant per detector, each with
/// what it reported.
#[derive(Clone, Debug, thiserror::Error)]
pub enum HostError {
    /// `std::thread::available_parallelism` could not count the threads.
    #[error("host detection: thread count: {0}")]
    Threads(String),
    /// macOS `vm_stat` did not run or its report had no page size.
    #[error("host detection: vm_stat: {0}")]
    VmStat(String),
    /// Linux `/proc/meminfo` could not be read or has no `MemAvailable`.
    #[error("host detection: /proc/meminfo: {0}")]
    MemInfo(String),
    /// macOS `sysctl` did not run or did not report a count for `name`.
    #[error("host detection: sysctl {name}: {detail}")]
    Sysctl {
        /// The queried name.
        name: String,
        /// What went wrong.
        detail: String,
    },
    /// This platform has no free-memory detector.
    #[error("host detection: free memory is unknown on this platform")]
    UnsupportedPlatform,
}

impl HostResources {
    /// The host, detected on the first call and the same thereafter.
    ///
    /// # Errors
    ///
    /// Fails when the thread count or the free memory cannot be read.
    pub fn detect() -> Result<Self, HostError> {
        static HOST: OnceLock<Result<HostResources, HostError>> = OnceLock::new();
        HOST.get_or_init(|| {
            let threads = std::thread::available_parallelism()
                .map_err(|error| HostError::Threads(error.to_string()))?
                .get();
            Ok(Self {
                threads,
                performance_cores: performance_cores().map_or(threads, |cores| {
                    usize::try_from(cores).unwrap_or(threads).clamp(1, threads)
                }),
                available_memory: available_memory()?,
                metal: casa_imaging_metal::available(),
            })
        })
        .clone()
    }
}

/// Threads a streamed phase keeps busy beside its workers: the producer
/// that fills the source's next block while the workers place the current
/// one (`imaging-source`).
pub const STREAM_PRODUCERS: usize = 1;

/// How one run's phases use the host's cores ([`ResourcePolicy::cores`]).
///
/// A phase that computes on data in memory (minor cycles, products) runs
/// `workers` workers. A phase that streams the source (the major-cycle and
/// imaging-weight passes) also keeps the source's producer busy
/// ([`STREAM_PRODUCERS`]), so it runs `streaming_workers`: as many workers
/// as fit beside the producer within the policy's core budget. Threads past
/// that budget run on efficiency cores or contend for the performance
/// cores' caches and memory bandwidth, which costs more cycles for the same
/// work (#659: four placing workers and the producer on four performance
/// cores took 1.13× the cycles of three and the producer).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CoreAllocation {
    /// Workers of a phase that computes on data in memory.
    pub workers: usize,
    /// Workers of a phase that streams the source; at least one, at most
    /// `workers`.
    pub streaming_workers: usize,
}

impl CoreAllocation {
    /// This allocation with at most `workers` workers in each phase, and at
    /// least one: what a run uses when the policy's workers do not fit its
    /// memory (#737).
    #[must_use]
    pub fn at_most(self, workers: usize) -> Self {
        let workers = workers.clamp(1, self.workers);
        Self {
            workers,
            streaming_workers: self.streaming_workers.min(workers),
        }
    }
}

/// How much of the host one run may use.
///
/// Each policy sets a budget of busy threads: its workers and, while a
/// phase streams the source, the source's producer ([`CoreAllocation`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourcePolicy {
    /// Half the threads, rounded up, and half the free memory.
    Interactive,
    /// Three quarters of the threads, rounded up and at most the
    /// performance cores, and three quarters of the free memory. A streamed
    /// phase's producer shares the performance cores: its workers are as
    /// many as fit beside it there.
    Balanced,
    /// Every thread and all the free memory.
    Exclusive,
    /// At most `workers` threads, a streamed phase's producer among them,
    /// and `memory` bytes, each capped at what the host has.
    Explicit {
        /// Worker ceiling.
        workers: usize,
        /// Memory ceiling in bytes.
        memory: u64,
    },
}

impl ResourcePolicy {
    /// Workers a run may use on `host`; at least one.
    #[must_use]
    pub fn workers(&self, host: &HostResources) -> usize {
        let threads = host.threads;
        match *self {
            Self::Interactive => threads.div_ceil(2),
            Self::Balanced => (threads * 3).div_ceil(4).min(host.performance_cores),
            Self::Exclusive => threads,
            Self::Explicit { workers, .. } => workers.min(threads),
        }
        .max(1)
    }

    /// How a run's phases use `host`: [`Self::workers`] for a phase that
    /// computes in memory, and for a phase that streams the source the
    /// workers that fit beside its producer within the policy's budget of
    /// busy threads. That budget is the performance cores for
    /// [`Self::Balanced`] and the workers for the other policies.
    #[must_use]
    pub fn cores(&self, host: &HostResources) -> CoreAllocation {
        let workers = self.workers(host);
        let budget = match *self {
            Self::Balanced => host.performance_cores,
            Self::Interactive | Self::Exclusive | Self::Explicit { .. } => workers,
        };
        CoreAllocation {
            workers,
            streaming_workers: budget.saturating_sub(STREAM_PRODUCERS).min(workers).max(1),
        }
    }

    /// Bytes of memory a run may use on `host`, before reservations.
    #[must_use]
    pub fn memory(&self, host: &HostResources) -> u64 {
        let free = u128::from(host.available_memory);
        let scaled = |numerator: u128, denominator: u128| {
            u64::try_from(free * numerator / denominator).expect("a fraction of a u64")
        };
        match *self {
            Self::Interactive => scaled(1, 2),
            Self::Balanced => scaled(3, 4),
            Self::Exclusive => host.available_memory,
            Self::Explicit { memory, .. } => memory.min(host.available_memory),
        }
    }
}

/// The memory one phase holds while it runs, as its owners report it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Demand {
    /// The phase, for the rejection message.
    pub phase: &'static str,
    /// Bytes held.
    pub memory: u64,
}

/// A phase's demand did not fit what the policy leaves free.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{phase} needs {required} bytes but the resource policy leaves {available} free")]
pub struct Admission {
    /// The rejected phase.
    pub phase: &'static str,
    /// Bytes the phase needs.
    pub required: u64,
    /// Bytes the policy leaves free.
    pub available: u64,
}

/// Memory admitted to one phase; it is released when this drops.
#[must_use = "the memory is released when the reservation drops"]
#[derive(Debug)]
pub struct Reservation {
    memory: u64,
}

impl Reservation {
    /// A reservation holding nothing, for state not yet formed.
    pub const fn none() -> Self {
        Self { memory: 0 }
    }

    /// Bytes held.
    #[must_use]
    pub const fn memory(&self) -> u64 {
        self.memory
    }

    /// Keep `memory` of the held bytes and release the rest: what a phase
    /// hands on stays charged with that state when its scratch is released.
    ///
    /// # Panics
    ///
    /// When `memory` exceeds the bytes held.
    pub fn retain(&mut self, memory: u64) {
        assert!(
            memory <= self.memory,
            "a reservation retains at most what it holds"
        );
        RESERVED.fetch_sub(self.memory - memory, Ordering::AcqRel);
        self.memory = memory;
    }

    /// Move `memory` of the held bytes into a reservation of their own, for
    /// state that outlives the phase that admitted it.
    ///
    /// # Panics
    ///
    /// When `memory` exceeds the bytes held.
    pub fn split(&mut self, memory: u64) -> Self {
        assert!(
            memory <= self.memory,
            "a reservation splits off at most what it holds"
        );
        self.memory -= memory;
        Self { memory }
    }

    /// Hold `other`'s bytes too, until this reservation drops.
    pub fn join(&mut self, mut other: Self) {
        self.memory += std::mem::take(&mut other.memory);
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        RESERVED.fetch_sub(self.memory, Ordering::AcqRel);
    }
}

/// A value with the memory admitted for it: whoever keeps the value keeps
/// its charge, until the reservation drops.
#[derive(Debug)]
pub struct Admitted<T> {
    /// What the memory was admitted for.
    pub value: T,
    /// The memory.
    pub reservation: Reservation,
}

/// Bytes held by the live reservations of this process. Only [`admit`]
/// raises it, holding [`WINDOWS`]; releases lower it without the lock.
static RESERVED: AtomicU64 = AtomicU64::new(0);

/// The open [`ReservationWindow`]s, each with the most [`RESERVED`] has
/// held since it last closed.
static WINDOWS: Mutex<Windows> = Mutex::new(Windows {
    next: 0,
    open: Vec::new(),
});

struct Windows {
    next: u64,
    open: Vec<(u64, u64)>,
}

fn windows() -> MutexGuard<'static, Windows> {
    WINDOWS.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One observer's view of the process's reservations: the most they held
/// at once over each interval it closes. Every run summary has its own, so
/// runs sharing a process neither reset nor take each other's intervals.
/// Admission raises every open window under the lock that a window closes
/// under, so an admission falls wholly within one interval.
#[derive(Debug)]
pub struct ReservationWindow {
    id: u64,
}

impl ReservationWindow {
    /// A window whose first interval starts at what the reservations hold
    /// now.
    #[must_use]
    pub fn open() -> Self {
        let mut windows = windows();
        let id = windows.next;
        windows.next += 1;
        windows.open.push((id, RESERVED.load(Ordering::Acquire)));
        Self { id }
    }

    /// The most the reservations held at once in the interval this call
    /// closes; the next starts at what they hold now, so a reservation that
    /// outlives an interval counts in the next too.
    pub fn close(&self) -> u64 {
        let mut windows = windows();
        let now = RESERVED.load(Ordering::Acquire);
        let (_, peak) = windows
            .open
            .iter_mut()
            .find(|(id, _)| *id == self.id)
            .expect("a window is open until it drops");
        std::mem::replace(peak, now).max(now)
    }
}

impl Drop for ReservationWindow {
    fn drop(&mut self) {
        windows().open.retain(|(id, _)| *id != self.id);
    }
}

/// Bytes `policy` leaves free on `host` beside the live reservations.
#[must_use]
pub fn free_memory(host: &HostResources, policy: &ResourcePolicy) -> u64 {
    policy
        .memory(host)
        .saturating_sub(RESERVED.load(Ordering::Acquire))
}

/// Bytes the live reservations of this process hold.
#[must_use]
pub fn reserved_memory() -> u64 {
    RESERVED.load(Ordering::Acquire)
}

/// Admit `demand` if it fits what `policy` leaves free on `host`, and raise
/// every open [`ReservationWindow`] to what the process then holds.
///
/// # Errors
///
/// [`Admission`] when the demand exceeds the free memory; nothing is held.
pub fn admit(
    host: &HostResources,
    policy: &ResourcePolicy,
    demand: &Demand,
) -> Result<Reservation, Admission> {
    let capacity = policy.memory(host);
    let mut windows = windows();
    // Only admissions raise the total, and they hold the lock, so releases
    // between this read and the add only leave more free.
    let available = capacity.saturating_sub(RESERVED.load(Ordering::Acquire));
    if demand.memory > available {
        return Err(Admission {
            phase: demand.phase,
            required: demand.memory,
            available,
        });
    }
    let held = RESERVED.fetch_add(demand.memory, Ordering::AcqRel) + demand.memory;
    for (_, peak) in &mut windows.open {
        *peak = (*peak).max(held);
    }
    Ok(Reservation {
        memory: demand.memory,
    })
}

/// Bytes the process-wide caches hold at most: the table-read cache, with a
/// MeasurementSet's tiles, and FFTW's plan cache, which every imaging run
/// and simulator session fills and which keep what they hold after it.
#[must_use]
pub fn process_cache_bytes() -> u64 {
    (casa_ms::table_read_cache_bytes() + casa_imaging_operator::FFT_PLAN_CACHE_BYTES) as u64
}

/// The process caches' charge, taken by the first caller admitted.
static PROCESS_CACHES: OnceLock<Reservation> = OnceLock::new();

/// Charge the process-wide caches ([`process_cache_bytes`]) against
/// `policy` on `host`, unless the process already holds their charge: it is
/// taken once, by the first caller admitted, and held for the process. Of
/// callers admitted at once, one keeps its charge and the others release
/// theirs. A run or session calls this before it fills the caches.
///
/// # Errors
///
/// [`Admission`] when the caches do not fit what `policy` leaves free.
pub fn charge_process_caches(
    host: &HostResources,
    policy: &ResourcePolicy,
) -> Result<(), Admission> {
    if PROCESS_CACHES.get().is_none() {
        let charge = admit(
            host,
            policy,
            &Demand {
                phase: "process caches",
                memory: process_cache_bytes(),
            },
        )?;
        // A concurrent caller that set it first holds the charge, and this
        // one's is released.
        let _ = PROCESS_CACHES.set(charge);
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn performance_cores() -> Option<u64> {
    sysctl_u64("hw.perflevel0.physicalcpu").ok()
}

#[cfg(not(target_os = "macos"))]
fn performance_cores() -> Option<u64> {
    None
}

#[cfg(target_os = "macos")]
fn available_memory() -> Result<u64, HostError> {
    let output = Command::new("/usr/bin/vm_stat")
        .output()
        .map_err(|error| HostError::VmStat(error.to_string()))?;
    if !output.status.success() {
        return Err(HostError::VmStat(format!("exited with {}", output.status)));
    }
    let text =
        String::from_utf8(output.stdout).map_err(|error| HostError::VmStat(error.to_string()))?;
    let page_size = text
        .lines()
        .next()
        .and_then(|line| line.split("page size of ").nth(1))
        .and_then(|value| value.split_ascii_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| HostError::VmStat("reports no page size".to_string()))?;
    let pages = ["Pages free", "Pages inactive", "Pages speculative"]
        .into_iter()
        .map(|name| {
            text.lines()
                .find_map(|line| {
                    let value = line.strip_prefix(name)?.strip_prefix(':')?;
                    value.trim().trim_end_matches('.').parse::<u64>().ok()
                })
                .unwrap_or(0)
        })
        .sum::<u64>();
    Ok(pages.saturating_mul(page_size))
}

#[cfg(target_os = "linux")]
fn available_memory() -> Result<u64, HostError> {
    let contents = std::fs::read_to_string("/proc/meminfo")
        .map_err(|error| HostError::MemInfo(error.to_string()))?;
    contents
        .lines()
        .find_map(|line| {
            line.strip_prefix("MemAvailable:")?
                .split_ascii_whitespace()
                .next()?
                .parse::<u64>()
                .ok()
        })
        .map(|kib| kib.saturating_mul(1024))
        .ok_or_else(|| HostError::MemInfo("has no MemAvailable".to_string()))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn available_memory() -> Result<u64, HostError> {
    Err(HostError::UnsupportedPlatform)
}

#[cfg(target_os = "macos")]
fn sysctl_u64(name: &str) -> Result<u64, HostError> {
    let failed = |detail: String| HostError::Sysctl {
        name: name.to_string(),
        detail,
    };
    let output = Command::new("/usr/sbin/sysctl")
        .args(["-n", name])
        .output()
        .map_err(|error| failed(error.to_string()))?;
    if !output.status.success() {
        return Err(failed(format!("exited with {}", output.status)));
    }
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .map_err(|_| failed("is not a count".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOST: HostResources = HostResources {
        threads: 10,
        performance_cores: 6,
        available_memory: 1 << 30,
        metal: false,
    };

    #[test]
    fn policies_scale_the_host() {
        let cases = [
            (ResourcePolicy::Interactive, 5, 1 << 29),
            (ResourcePolicy::Balanced, 6, 3 << 28),
            (ResourcePolicy::Exclusive, 10, 1 << 30),
            (
                ResourcePolicy::Explicit {
                    workers: 64,
                    memory: u64::MAX,
                },
                10,
                1 << 30,
            ),
            (
                ResourcePolicy::Explicit {
                    workers: 0,
                    memory: 1 << 20,
                },
                1,
                1 << 20,
            ),
        ];
        for (policy, workers, memory) in cases {
            assert_eq!(policy.workers(&HOST), workers, "{policy:?}");
            assert_eq!(policy.memory(&HOST), memory, "{policy:?}");
        }
    }

    #[test]
    fn a_streamed_phase_keeps_its_producer_within_the_core_budget() {
        let host = |threads, performance_cores| HostResources {
            threads,
            performance_cores,
            ..HOST
        };
        let explicit = |workers| ResourcePolicy::Explicit {
            workers,
            memory: u64::MAX,
        };
        let cores = |workers, streaming_workers| CoreAllocation {
            workers,
            streaming_workers,
        };
        let cases = [
            // Four performance and six efficiency cores (M4): the producer
            // takes a performance core from the placing workers.
            (ResourcePolicy::Balanced, host(10, 4), cores(4, 3)),
            // Twelve performance cores of sixteen threads.
            (ResourcePolicy::Balanced, host(16, 12), cores(12, 11)),
            // No core classes: three quarters of the threads leave room.
            (ResourcePolicy::Balanced, host(32, 32), cores(24, 24)),
            (ResourcePolicy::Balanced, host(1, 1), cores(1, 1)),
            (ResourcePolicy::Interactive, host(10, 4), cores(5, 4)),
            (ResourcePolicy::Exclusive, host(10, 4), cores(10, 9)),
            (explicit(4), host(10, 4), cores(4, 3)),
            // One worker streams beside its producer.
            (explicit(1), host(10, 4), cores(1, 1)),
        ];
        for (policy, host, expected) in cases {
            assert_eq!(policy.cores(&host), expected, "{policy:?} on {host:?}");
        }
    }

    /// #737: fewer workers keep the streaming phases at most as wide as
    /// the computing ones, and never go below one.
    #[test]
    fn an_allocation_at_most_some_workers_narrows_both_phases() {
        let four = CoreAllocation {
            workers: 4,
            streaming_workers: 3,
        };
        let cases = [
            (5, (4, 3)),
            (4, (4, 3)),
            (3, (3, 3)),
            (2, (2, 2)),
            (1, (1, 1)),
            (0, (1, 1)),
        ];
        for (workers, (computing, streaming)) in cases {
            assert_eq!(
                four.at_most(workers),
                CoreAllocation {
                    workers: computing,
                    streaming_workers: streaming,
                },
                "at most {workers}"
            );
        }
    }

    #[test]
    fn a_reservation_holds_its_memory_until_it_drops() {
        let policy = ResourcePolicy::Explicit {
            workers: 1,
            memory: 1 << 20,
        };
        let held = admit(
            &HOST,
            &policy,
            &Demand {
                phase: "first",
                memory: 3 << 18,
            },
        )
        .expect("fits");
        assert_eq!(held.memory(), 3 << 18);
        let rejected = admit(
            &HOST,
            &policy,
            &Demand {
                phase: "second",
                memory: 1 << 19,
            },
        )
        .expect_err("only a quarter is free");
        assert_eq!(
            rejected,
            Admission {
                phase: "second",
                required: 1 << 19,
                available: 1 << 18,
            }
        );
        drop(held);
        assert_eq!(free_memory(&HOST, &policy), 1 << 20);
    }

    fn admit_bytes(memory: u64) -> Reservation {
        let policy = ResourcePolicy::Explicit {
            workers: 1,
            memory: 1 << 30,
        };
        admit(
            &HOST,
            &policy,
            &Demand {
                phase: "transient",
                memory,
            },
        )
        .expect("fits")
    }

    // Other tests of this process may hold reservations too, so these laws
    // bound windows from below.

    #[test]
    fn a_window_keeps_its_peak_whatever_other_windows_do() {
        let first = ReservationWindow::open();
        drop(admit_bytes(5 << 20));
        // Another run begins and ends a phase, then finishes, before the
        // first run's phase ends.
        let second = ReservationWindow::open();
        let _ = second.close();
        drop(second);
        assert!(first.close() >= 5 << 20);
    }

    #[test]
    fn a_reservation_held_across_a_close_counts_in_the_next_interval() {
        let window = ReservationWindow::open();
        let held = admit_bytes(3 << 20);
        assert!(window.close() >= 3 << 20);
        assert!(window.close() >= 3 << 20, "still held");
        drop(held);
        assert!(window.close() >= 3 << 20, "held when the interval began");
    }
}
