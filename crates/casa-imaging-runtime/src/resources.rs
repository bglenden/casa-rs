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

/// Why the host could not be detected.
#[derive(Clone, Debug, thiserror::Error)]
pub enum HostError {
    /// A system query failed or reported something unreadable.
    #[error("host detection: {0}")]
    Detection(String),
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
                .map_err(|error| HostError::Detection(error.to_string()))?
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

/// How much of the host one run may use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourcePolicy {
    /// Half the threads, rounded up, and half the free memory.
    Interactive,
    /// Three quarters of the threads, rounded up and at most the
    /// performance cores, and three quarters of the free memory.
    Balanced,
    /// Every thread and all the free memory.
    Exclusive,
    /// At most `workers` threads and `memory` bytes, each capped at what the
    /// host has.
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
        .map_err(|error| HostError::Detection(error.to_string()))?;
    if !output.status.success() {
        return Err(HostError::Detection("vm_stat failed".to_string()));
    }
    let text = String::from_utf8(output.stdout)
        .map_err(|error| HostError::Detection(error.to_string()))?;
    let page_size = text
        .lines()
        .next()
        .and_then(|line| line.split("page size of ").nth(1))
        .and_then(|value| value.split_ascii_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| HostError::Detection("vm_stat reports no page size".to_string()))?;
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
        .map_err(|error| HostError::Detection(error.to_string()))?;
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
        .ok_or_else(|| HostError::Detection("/proc/meminfo has no MemAvailable".to_string()))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn available_memory() -> Result<u64, HostError> {
    Err(HostError::Detection(
        "free memory is unknown on this platform".to_string(),
    ))
}

#[cfg(target_os = "macos")]
fn sysctl_u64(name: &str) -> Result<u64, HostError> {
    let output = Command::new("/usr/sbin/sysctl")
        .args(["-n", name])
        .output()
        .map_err(|error| HostError::Detection(error.to_string()))?;
    if !output.status.success() {
        return Err(HostError::Detection(format!("sysctl {name} failed")));
    }
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .map_err(|_| HostError::Detection(format!("sysctl {name} is not a count")))
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
