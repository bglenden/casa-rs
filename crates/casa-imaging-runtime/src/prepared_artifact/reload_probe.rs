// SPDX-License-Identifier: LGPL-3.0-or-later

//! Opt-in, bounded observation of prepared-cell misses. This module grants no
//! payload, cache, scheduling, or scientific authority.

use std::{
    cell::Cell,
    fmt,
    mem::size_of,
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use super::PreparedArtifactError;

const SEED: u64 = 0x7453_315f_7265_6c64;
const SIZE_BUCKETS: usize = 64;
const STRATA: usize = 3 * 2 * SIZE_BUCKETS;

/// Nested and exclusive observation boundaries; parent intervals must not be
/// added to their children when attributing cost.
#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub enum Stage {
    /// From the first observed miss until loading reservation is granted.
    Admission,
    /// Waiting for another load of the same identity.
    LoadingWait,
    /// Waiting for encoded-decoder workspace.
    WorkspaceWait,
    /// Waiting for an unpinned eviction victim.
    PinWait,
    /// Victim search, removal, accounting and destruction.
    Eviction,
    /// Encoded vector reservation and decoder initialization.
    DecoderAllocate,
    /// Entire reader call, including reader lifecycle/bookkeeping.
    Reader,
    /// Existing transaction payload-consumption envelope.
    Payload,
    /// Payload open; this is wall time, not physical-disk service.
    Open,
    /// Streaming-buffer allocation.
    BufferAllocate,
    /// Counted reads, including operating-system page-cache service.
    Read,
    /// Scalar finite-value validation.
    Finite,
    /// Whole-payload SHA256 updates.
    PayloadHash,
    /// Per-segment SHA256 updates.
    SegmentHash,
    /// Validated byte delivery to the consumer, including byte collection.
    Consumer,
    /// Digest finalization/comparison, final EOF and content identity checks.
    FinalIntegrity,
    /// Byte-to-complex plane conversion.
    DecodePlanes,
    /// Tap copying, kernel validation and cell construction.
    ConstructKernels,
    /// Completion lock acquisition, settlement, insertion and notification.
    PoolComplete,
    /// Lease construction after dropping the pool lock.
    Lease,
    /// Complete sampled miss after initial hit/miss lookup; excludes emission.
    Total,
}

const STAGE_NAMES: [&str; 21] = [
    "admission",
    "loading_wait",
    "workspace_wait",
    "pin_wait",
    "eviction",
    "decoder_allocate",
    "reader",
    "payload",
    "open",
    "buffer_allocate",
    "read",
    "finite",
    "payload_hash",
    "segment_hash",
    "consumer",
    "final_integrity",
    "decode_planes",
    "construct_kernels",
    "pool_complete",
    "lease",
    "total",
];

/// Fixed-size timings for one selected complete load. Disabled observations
/// do not read the clock. No individual tap or scalar is instrumented.
#[derive(Clone, Copy, Debug, Default)]
pub struct Cost {
    enabled: bool,
    nanos: [u64; STAGE_NAMES.len()],
}

impl Cost {
    /// Select observation for this complete load, not individual chunks.
    pub const fn new(enabled: bool) -> Self {
        Self {
            enabled,
            nanos: [0; STAGE_NAMES.len()],
        }
    }

    /// Whether this complete load is selected.
    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    /// Begin a selected boundary without a clock read on unselected loads.
    pub fn start(&self) -> Option<Instant> {
        self.enabled.then(Instant::now)
    }

    /// Finish a boundary. Overflow is visible as a saturated, invalid duration.
    pub fn finish(&mut self, stage: Stage, start: Option<Instant>) {
        if let Some(start) = start {
            let nanos = u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
            self.nanos[stage as usize] = self.nanos[stage as usize].saturating_add(nanos);
        }
    }

    /// Observe an existing operation without changing its return value.
    pub fn measure<T>(&mut self, stage: Stage, operation: impl FnOnce() -> T) -> T {
        let start = self.start();
        let result = operation();
        self.finish(stage, start);
        result
    }

    /// Combine disjoint owners' stage observations from the same load.
    pub fn merge(&mut self, other: &Self) {
        for (target, source) in self.nanos.iter_mut().zip(other.nanos) {
            *target = target.saturating_add(source);
        }
    }
}

impl fmt::Display for Cost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (name, nanos) in STAGE_NAMES.iter().zip(self.nanos) {
            write!(f, " {name}_nanos={nanos}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Default)]
struct Role {
    pass: u32,
    phase: u8,
}

thread_local! {
    static ROLE: Cell<Role> = const { Cell::new(Role { pass: 0, phase: 0 }) };
}

/// Restores the preceding diagnostic role even when the operation fails.
pub(crate) struct RoleScope(Role);

impl RoleScope {
    pub(crate) fn enter(pass: u32, phase: u8) -> Option<Self> {
        enabled().then(|| {
            Self(ROLE.replace(Role {
                pass,
                phase: phase + 1,
            }))
        })
    }
}

impl Drop for RoleScope {
    fn drop(&mut self) {
        ROLE.set(self.0);
    }
}

pub(super) fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("CASA_RS_TRACE_CF_RELOAD_COST").is_some())
}

fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[derive(Default)]
struct Stratum {
    loads: u64,
    bytes: u64,
    samples: u64,
}

struct History {
    identity: [u8; 32],
    loads: u64,
    evictions: u64,
}

/// One fresh provider's diagnostic state, accessed under its existing mutex.
/// History is bounded by the immutable catalog; samples are streamed, not kept.
pub struct Probe {
    history: Box<[History]>,
    strata: [Stratum; STRATA],
    session: u64,
    ordinal: u64,
    denominator: u64,
    reserved_bytes: u64,
}

/// State of one candidate miss. It becomes an exact load only at admission.
pub struct Sample {
    session: u64,
    ordinal: u64,
    index: usize,
    bytes: u64,
    role: Role,
    reload: bool,
    denominator: u64,
    started: Option<Instant>,
    /// Timings belonging to the same selected complete load.
    pub cost: Cost,
}

impl Probe {
    pub(super) fn reservation(
        entries: usize,
        active_loads: u64,
    ) -> Result<u64, PreparedArtifactError> {
        let retained = size_of::<Self>()
            .checked_add(
                entries
                    .checked_mul(size_of::<History>())
                    .ok_or(PreparedArtifactError::ArtifactTooLarge)?,
            )
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        // Two owner-local cost copies and the sample may coexist. Charge a
        // second sample-sized move slot rather than relying on optimization.
        let active = (2 * size_of::<Sample>() + 2 * size_of::<Cost>()) as u64;
        (retained as u64)
            .checked_add(
                active_loads
                    .checked_mul(active)
                    .ok_or(PreparedArtifactError::ArtifactTooLarge)?,
            )
            .ok_or(PreparedArtifactError::ArtifactTooLarge)
    }

    pub(super) fn new(identities: impl Iterator<Item = [u8; 32]>, reserved_bytes: u64) -> Self {
        static SESSION: AtomicU64 = AtomicU64::new(1);
        let mut history = identities
            .map(|identity| History {
                identity,
                loads: 0,
                evictions: 0,
            })
            .collect::<Vec<_>>();
        history.sort_unstable_by_key(|entry| entry.identity);
        Self {
            history: history.into_boxed_slice(),
            strata: std::array::from_fn(|_| Stratum::default()),
            session: SESSION.fetch_add(1, Ordering::Relaxed),
            ordinal: 0,
            denominator: if std::env::var("CASA_RS_TRACE_CF_RELOAD_COST").as_deref()
                == Ok("detailed")
            {
                1
            } else {
                32
            },
            reserved_bytes,
        }
    }

    /// Begin after a miss is known, without touching the resident-hit path.
    pub fn begin(
        &mut self,
        identity: [u8; 32],
        bytes: usize,
    ) -> Result<Sample, PreparedArtifactError> {
        let index = self
            .history
            .binary_search_by_key(&identity, |entry| entry.identity)
            .map_err(|_| PreparedArtifactError::IdentityMismatch)?;
        self.ordinal = self
            .ordinal
            .checked_add(1)
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        let role = ROLE.get();
        let selected =
            mix(SEED ^ self.ordinal ^ (u64::from(role.pass) << 32)) % self.denominator == 0;
        let cost = Cost::new(selected);
        Ok(Sample {
            session: self.session,
            ordinal: self.ordinal,
            index,
            bytes: u64::try_from(bytes).map_err(|_| PreparedArtifactError::ArtifactTooLarge)?,
            role,
            reload: false,
            denominator: self.denominator,
            started: cost.start(),
            cost,
        })
    }

    /// Record the actual physical load after its existing reservation succeeds.
    pub fn admit(&mut self, sample: &mut Sample) -> Result<(), PreparedArtifactError> {
        let entry = &mut self.history[sample.index];
        sample.reload = entry.loads != 0;
        if sample.reload && entry.evictions < entry.loads {
            return Err(PreparedArtifactError::InvalidLayout);
        }
        entry.loads = entry
            .loads
            .checked_add(1)
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        let bucket = 63 - sample.bytes.max(1).leading_zeros() as usize;
        let stratum = &mut self.strata[(usize::from(sample.role.phase) * 2
            + usize::from(sample.reload))
            * SIZE_BUCKETS
            + bucket];
        stratum.loads = stratum
            .loads
            .checked_add(1)
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        stratum.bytes = stratum
            .bytes
            .checked_add(sample.bytes)
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        stratum.samples += u64::from(sample.cost.enabled());
        sample.cost.finish(Stage::Admission, sample.started);
        Ok(())
    }

    /// Record only an actual eviction chosen by the existing pool policy.
    pub fn evicted(&mut self, identity: [u8; 32]) -> Result<(), PreparedArtifactError> {
        let index = self
            .history
            .binary_search_by_key(&identity, |entry| entry.identity)
            .map_err(|_| PreparedArtifactError::IdentityMismatch)?;
        self.history[index].evictions = self.history[index]
            .evictions
            .checked_add(1)
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
        Ok(())
    }

    /// Emit exact denominators once at the normal reader close boundary.
    pub fn emit(&self, aborted: bool) {
        for (index, stratum) in self
            .strata
            .iter()
            .enumerate()
            .filter(|(_, value)| value.loads != 0)
        {
            eprintln!(
                "t51_cf_reload_stratum session={} phase={} reload={} size_bucket={} loads={} bytes={} samples={} p_num=1 p_den={} seed={} catalog_entries={} reserved_bytes={} aborted={}",
                self.session,
                index / (2 * SIZE_BUCKETS),
                (index / SIZE_BUCKETS) % 2,
                index % SIZE_BUCKETS,
                stratum.loads,
                stratum.bytes,
                stratum.samples,
                self.denominator,
                SEED,
                self.history.len(),
                self.reserved_bytes,
                aborted
            );
        }
    }
}

impl Sample {
    /// Emit after the normal completion/lease path. Failed samples cannot be
    /// silently included among successful service-time estimates.
    pub fn emit(mut self, completed: bool) {
        self.cost.finish(Stage::Total, self.started);
        if self.cost.enabled() {
            eprintln!(
                "t51_cf_reload_sample session={} ordinal={} catalog_index={} pass={} phase={} reload={} bytes={} p_num=1 p_den={} seed={} completed={}{}",
                self.session,
                self.ordinal,
                self.index,
                self.role.pass,
                self.role.phase,
                u8::from(self.reload),
                self.bytes,
                self.denominator,
                SEED,
                completed,
                self.cost
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reload_probe_counts_actual_eviction_and_keeps_fresh_histories() {
        let mut probe = Probe::new([[1; 32], [2; 32]].into_iter(), 4096);
        let mut first = probe.begin([1; 32], 1024).unwrap();
        probe.admit(&mut first).unwrap();
        assert!(!first.reload);
        let mut impossible = probe.begin([1; 32], 1024).unwrap();
        assert!(probe.admit(&mut impossible).is_err());
        probe.evicted([1; 32]).unwrap();
        let mut reload = probe.begin([1; 32], 1024).unwrap();
        probe.admit(&mut reload).unwrap();
        assert!(reload.reload);
        assert_eq!(probe.strata[10].loads, 1);
        assert_eq!(probe.strata[SIZE_BUCKETS + 10].loads, 1);
        assert!(probe.begin([3; 32], 1024).is_err());
        let mut fresh = Probe::new([[1; 32]].into_iter(), 4096);
        let mut first = fresh.begin([1; 32], 1024).unwrap();
        fresh.admit(&mut first).unwrap();
        assert!(!first.reload);
    }

    #[test]
    fn reload_probe_sampling_is_reproducible_and_disabled_cost_has_no_clock() {
        let mut a = Probe::new([[1; 32]].into_iter(), 4096);
        let mut b = Probe::new([[1; 32]].into_iter(), 4096);
        a.denominator = 32;
        b.denominator = 32;
        let mut selected = 0;
        for _ in 0..4096 {
            let x = a.begin([1; 32], 1024).unwrap();
            let y = b.begin([1; 32], 1024).unwrap();
            assert_eq!(x.cost.enabled(), y.cost.enabled());
            selected += usize::from(x.cost.enabled());
        }
        assert!((80..180).contains(&selected));
        let mut off = Cost::new(false);
        assert!(off.start().is_none());
        assert_eq!(off.measure(Stage::Read, || 17), 17);
        assert_eq!(off.nanos, [0; STAGE_NAMES.len()]);
        assert!(Probe::reservation(usize::MAX, 1).is_err());
        assert!(Probe::reservation(1024, u64::MAX).is_err());
    }
}
