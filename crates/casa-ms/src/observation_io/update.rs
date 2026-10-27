// SPDX-License-Identifier: LGPL-3.0-or-later
//! Update sessions: one owner that reads a store and writes its declared
//! targets back (`docs/measurement-set/contracts.md`, section 7).
//!
//! [`open_update`] takes this process's write intent on the store, opens it
//! once for reading, and resolves the declared targets before any payload:
//! each names an existing variable (or one the store can create), and one
//! that other data groups share is refused unless its target accepts those
//! aliases. The session lends the same observation for reading
//! ([`UpdateSession::observation`]) and updaters for writing
//! ([`UpdateSession::updater`]), so a pass that reads `DATA` and writes
//! `MODEL_DATA` is one planned owner, not a reader racing a writer.
//!
//! Several updaters of a session may be live at once, each writing its own
//! time slice of a split traversal on its own worker: an updater claims the
//! times of each variable and partition it writes, widened to the store's
//! whole write units (an MSv4 chunk row), and an updater whose claim meets
//! a live one's is refused ([`ObservationError::Conflict`]). MSv4 updaters
//! then write disjoint chunks at once; MSv2 updaters write one batch at a
//! time, each under casacore's table lock.
//!
//! Within the process, a second update session on the same store is
//! refused ([`ObservationError::Conflict`]); independent read-only
//! observations may coexist with it and read what was published. Across
//! processes MSv2 relies on casacore's table lock, taken for each batch an
//! updater writes; MSv4 has no lock protocol ([`UpdateGuarantees`]).

use std::collections::{BTreeMap, HashSet};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock, PoisonError};

use casa_observation::{
    AccessRequest, Catalogue, Observation, ObservationError, PartitionId, ResolvedTarget,
    UpdateGuarantees, UpdateTarget, Updater, WeightRequest, WeightRule, WritableRole,
};

use super::OpenOptions;
use super::msv2::update::Msv2Update;
#[cfg(feature = "msv4")]
use super::msv4::update::Msv4Update;

/// The stores this process holds write intent on.
fn intents() -> &'static Mutex<HashSet<PathBuf>> {
    static INTENTS: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    INTENTS.get_or_init(|| Mutex::new(HashSet::new()))
}

/// This process's write intent on one store, released when dropped.
struct Intent(PathBuf);

impl Intent {
    fn take(path: &Path) -> Result<Self, ObservationError> {
        let path = path.canonicalize().map_err(storage)?;
        let mut held = intents()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !held.insert(path.clone()) {
            return Err(ObservationError::Conflict {
                reason: format!("an update session already holds {}", path.display()),
            });
        }
        Ok(Self(path))
    }
}

impl Drop for Intent {
    fn drop(&mut self) {
        intents()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.0);
    }
}

/// What one updater may write: variable `variable` of partition
/// `partition` at grid times `times`, widened to the store's whole write
/// units.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Unit {
    pub(super) partition: PartitionId,
    pub(super) variable: String,
    pub(super) times: Range<usize>,
}

impl Unit {
    /// Whether `other` writes some of the same native units.
    fn meets(&self, other: &Self) -> bool {
        self.partition == other.partition
            && self.variable == other.variable
            && self.times.start < other.times.end
            && other.times.start < self.times.end
    }
}

/// The units the session's live updaters hold, by updater.
#[derive(Debug, Default)]
struct Claims {
    next: u64,
    live: BTreeMap<u64, Vec<Unit>>,
}

/// A live updater's claim on its units, released when the updater drops.
pub(super) struct Claim<'s> {
    claims: &'s Mutex<Claims>,
    id: u64,
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        self.claims
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .live
            .remove(&self.id);
    }
}

enum Store {
    V2(Box<Msv2Update>),
    #[cfg(feature = "msv4")]
    V4(Msv4Update),
}

/// A store opened for update: one owner that reads it and writes its
/// declared targets.
pub struct UpdateSession {
    store: Store,
    targets: Vec<ResolvedTarget>,
    claims: Mutex<Claims>,
    _intent: Intent,
}

/// Open the store at `path` for update, as [`super::open`] chooses its
/// adapter, writing `targets`.
///
/// # Errors
///
/// [`ObservationError::Conflict`] when another update session of this
/// process holds the store; [`ObservationError::InvalidRequest`] when a
/// target names a group or variable the store lacks and cannot create, or a
/// variable other groups share without accepting them; as [`super::open`]
/// otherwise. Nothing is written.
#[cfg(unix)]
pub fn open_update(
    path: impl AsRef<Path>,
    options: &OpenOptions,
    targets: &[UpdateTarget],
) -> Result<UpdateSession, ObservationError> {
    let path = path.as_ref();
    let intent = Intent::take(path)?;
    let store = if path.join("zarr.json").is_file() {
        #[cfg(feature = "msv4")]
        {
            Store::V4(Msv4Update::open(path, options)?)
        }
        #[cfg(not(feature = "msv4"))]
        return Err(ObservationError::Unsupported {
            what: "an MSv4 store without casa-ms's msv4 feature",
        });
    } else {
        Store::V2(Box::new(Msv2Update::open(path, options)?))
    };
    let catalogue = match &store {
        Store::V2(update) => update.observation().catalogue(),
        #[cfg(feature = "msv4")]
        Store::V4(update) => update.observation().catalogue(),
    };
    let creatable = matches!(store, Store::V2(_));
    // A group the session creates shares the MSv2 flags and weights, so it
    // is an alias of their targets before it exists.
    let creating: Vec<String> = targets
        .iter()
        .filter(|target| {
            creatable
                && target.create
                && !catalogue
                    .partitions
                    .iter()
                    .any(|entry| entry.data_group(&target.data_group).is_some())
        })
        .map(|target| target.data_group.clone())
        .collect();
    let targets = targets
        .iter()
        .map(|target| resolve(catalogue, target, creatable, &creating))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(UpdateSession {
        store,
        targets,
        claims: Mutex::default(),
        _intent: intent,
    })
}

impl UpdateSession {
    /// The observation, for reading: the same store the updaters write.
    #[must_use]
    pub fn observation(&self) -> &dyn Observation {
        match &self.store {
            Store::V2(update) => update.observation(),
            #[cfg(feature = "msv4")]
            Store::V4(update) => update.observation(),
        }
    }

    /// The declared targets as the store resolved them.
    #[must_use]
    pub fn targets(&self) -> &[ResolvedTarget] {
        &self.targets
    }

    /// What this store's updates guarantee on failure.
    #[must_use]
    pub fn guarantees(&self) -> UpdateGuarantees {
        match &self.store {
            Store::V2(_) => UpdateGuarantees {
                atomic_units: false,
                cross_process_lock: true,
            },
            #[cfg(feature = "msv4")]
            Store::V4(_) => UpdateGuarantees {
                atomic_units: true,
                cross_process_lock: false,
            },
        }
    }

    /// An updater for the traversal `request` binds, writing the targets
    /// numbered `targets` (positions in [`Self::targets`]) batch for batch.
    ///
    /// Several may be live at once, each for its own times: a time split's
    /// slices ([`casa_observation::TimeSplit`]), each on its own worker. An
    /// updater claims the grid times of each variable and partition it
    /// writes, widened on MSv4 to whole chunk rows, so slices cut at the
    /// store's natural time blocks write at once there; MSv2 updaters write
    /// one batch at a time.
    ///
    /// # Errors
    ///
    /// [`ObservationError::Conflict`] while a live updater of this session
    /// claims some of the same times of a variable and partition (on MSv4,
    /// the same chunk rows); [`ObservationError::InvalidRequest`] for an
    /// unknown target,
    /// two targets writing one variable (whichever wrote last would win), or
    /// a request the observation cannot bind; a storage error when the
    /// store cannot be prepared for writing (MSv2: casacore's write lock,
    /// a column to create).
    pub fn updater(
        &self,
        request: &AccessRequest,
        targets: &[usize],
    ) -> Result<Box<dyn Updater + '_>, ObservationError> {
        let bound = targets
            .iter()
            .map(|&index| {
                self.targets
                    .get(index)
                    .ok_or_else(|| request_error(format!("the session has no target {index}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for (index, target) in bound.iter().enumerate() {
            if bound[..index]
                .iter()
                .any(|earlier| earlier.variable == target.variable)
            {
                return Err(request_error(format!(
                    "two bound targets write {}; bind it once",
                    target.variable
                )));
            }
        }
        // A weight write replaces stored weights: patches made from 1/σ²
        // weights would write sigmas as weights unannounced. Refused here
        // when the request would make them (an MSv4's fallback reads the
        // stored weights), and batch for batch by the patch check, which
        // reads each batch's weight rule.
        if request.weights != WeightRequest::Stored
            && bound
                .iter()
                .any(|target| target.target.role == WritableRole::Weights)
            && self
                .observation()
                .estimate(request)?
                .partitions
                .iter()
                .filter_map(|partition| partition.weights.as_ref())
                .any(|source| source.rule != WeightRule::Stored)
        {
            return Err(request_error(
                "a weight write is bound to a traversal of the stored weights \
                 (WeightRequest::Stored), not of weights made from the sigmas",
            ));
        }
        let claim = |units| self.claim(units);
        match &self.store {
            Store::V2(update) => update.updater(request, &bound, claim),
            #[cfg(feature = "msv4")]
            Store::V4(update) => update.updater(request, &bound, claim),
        }
    }

    /// Claim `units` for a new updater, unless a live one claims some of
    /// them.
    fn claim(&self, units: Vec<Unit>) -> Result<Claim<'_>, ObservationError> {
        let mut claims = self.claims.lock().unwrap_or_else(PoisonError::into_inner);
        for unit in &units {
            if let Some(held) = claims.live.values().flatten().find(|held| held.meets(unit)) {
                return Err(ObservationError::Conflict {
                    reason: format!(
                        "a live updater of this session writes {} of {:?} at grid times {:?}, \
                         which this one's {:?} meet: concurrent updaters write disjoint times, \
                         on MSv4 whole chunk rows (cut at the natural time blocks)",
                        held.variable, held.partition, held.times, unit.times
                    ),
                });
            }
        }
        let id = claims.next;
        claims.next += 1;
        claims.live.insert(id, units);
        Ok(Claim {
            claims: &self.claims,
            id,
        })
    }
}

/// `target` resolved against `catalogue`: the variable it writes in every
/// partition that has its group, and the other groups reading it, those
/// the session is `creating` included.
fn resolve(
    catalogue: &Catalogue,
    target: &UpdateTarget,
    creatable: bool,
    creating: &[String],
) -> Result<ResolvedTarget, ObservationError> {
    let mut variable: Option<String> = None;
    let mut aliases: Vec<String> = Vec::new();
    for entry in &catalogue.partitions {
        let Some((_, group)) = entry.data_group(&target.data_group) else {
            continue;
        };
        let of = |group: &casa_observation::DataGroupEntry| match target.role {
            WritableRole::Visibilities => group.visibilities.clone(),
            WritableRole::Flags => group.flags.clone(),
            WritableRole::Weights => group.weights.clone(),
        };
        let name = of(group);
        if variable.as_ref().is_some_and(|known| *known != name) {
            return Err(ObservationError::Unsupported {
                what: "a target whose variable differs between partitions",
            });
        }
        for other in &entry.data_groups {
            if other.name != target.data_group
                && of(other) == name
                && !aliases.contains(&other.name)
            {
                aliases.push(other.name.clone());
            }
        }
        variable = Some(name);
    }
    let (variable, created) = match variable {
        Some(variable) => (variable, false),
        None => (created_variable(target, creatable)?, true),
    };
    if target.role != WritableRole::Visibilities {
        for group in creating {
            if *group != target.data_group && !aliases.contains(group) {
                aliases.push(group.clone());
            }
        }
    }
    if !aliases.is_empty() && target.aliases == casa_observation::AliasScope::Refuse {
        return Err(request_error(format!(
            "{:?} of group {} is {variable}, which group(s) {} also read; accept the aliases \
             to write it for all of them",
            target.role,
            target.data_group,
            aliases.join(", ")
        )));
    }
    Ok(ResolvedTarget {
        target: target.clone(),
        variable,
        aliases,
        created,
    })
}

/// The column a target creates, where the store can add one: the
/// visibilities of `corrected` or `model` (the MSv2 updater says how each
/// is seeded).
fn created_variable(target: &UpdateTarget, creatable: bool) -> Result<String, ObservationError> {
    let column = match (target.role, target.data_group.as_str()) {
        (WritableRole::Visibilities, "corrected") => "CORRECTED_DATA",
        (WritableRole::Visibilities, "model") => "MODEL_DATA",
        _ => {
            return Err(request_error(format!(
                "no partition has data group {}",
                target.data_group
            )));
        }
    };
    if !target.create {
        return Err(request_error(format!(
            "no partition has data group {}; declare the target with create to add {column}",
            target.data_group
        )));
    }
    if !creatable {
        return Err(ObservationError::Unsupported {
            what: "adding a data group to an MSv4 store",
        });
    }
    Ok(column.to_string())
}

pub(super) fn request_error(reason: impl Into<String>) -> ObservationError {
    ObservationError::InvalidRequest {
        reason: reason.into(),
    }
}

pub(super) fn storage(error: impl std::error::Error + Send + Sync + 'static) -> ObservationError {
    ObservationError::Storage(Box::new(error))
}

#[cfg(all(test, feature = "msv4", unix))]
mod tests {
    use std::path::Path;

    use casa_observation::{
        BatchBuffers, CorrelationRequest, Grouping, PartitionId, PatchValues, ReadEvent, Roles,
        TimeSelection,
    };
    use num_complex::Complex32;

    use super::super::msv4::store::FailurePoint;
    use super::*;

    /// The MSv4 fixture, unpacked under `root`.
    fn fixture(root: &Path) -> std::path::PathBuf {
        let archive = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/ms_trial/ms_trial.ps.zarr.tgz");
        tar::Archive::new(flate2::read::GzDecoder::new(
            std::fs::File::open(archive).expect("fixture"),
        ))
        .unpack(root)
        .expect("unpack");
        root.join("ms_trial.ps.zarr")
    }

    /// Every channel and product of four whole times per batch.
    fn request(observation: &dyn Observation) -> AccessRequest {
        let all = AccessRequest {
            partitions: vec![PartitionId(0)],
            data_group: "base".to_string(),
            roles: Roles::ALL,
            channels: 0..8,
            times: TimeSelection::All,
            weights: WeightRequest::Stored,
            correlations: CorrelationRequest::ALL,
            grouping: Grouping::None,
            batch_bytes: usize::MAX,
        };
        let held =
            observation.estimate(&all).expect("estimate").partitions[0].held_bytes_per_time();
        AccessRequest {
            batch_bytes: 4 * held,
            ..all
        }
    }

    /// The store's visibilities and flags, `[time, baseline, channel,
    /// product]` in order, read afresh.
    fn contents(path: &Path) -> (Vec<Complex32>, Vec<bool>) {
        let observation = super::super::open(path, &OpenOptions::default()).expect("open");
        let request = request(observation.as_ref());
        let mut traversal = observation.bind_read(&request).expect("bind");
        let mut buffers = BatchBuffers::default();
        let (mut values, mut flags) = (Vec::new(), Vec::new());
        while let Some(ReadEvent::Batch(batch)) = traversal.next(&mut buffers).expect("read") {
            values.extend(batch.group.values.expect("values").iter().copied());
            flags.extend(batch.group.flags.expect("flags").iter().copied());
        }
        (values, flags)
    }

    /// One batch's values, owned.
    enum Owned {
        Visibilities(ndarray::Array3<Complex32>),
        SetFlags(ndarray::Array3<bool>),
    }

    impl Owned {
        fn view(&self) -> PatchValues<'_> {
            match self {
                Self::Visibilities(values) => PatchValues::Visibilities(values.view()),
                Self::SetFlags(set) => PatchValues::Flags {
                    set: Some(set.view()),
                    clear: None,
                },
            }
        }
    }

    fn doubled(batch: &casa_observation::VisibilityBatch<'_>) -> Owned {
        Owned::Visibilities(batch.group.values.expect("values").mapv(|v| v * 2.0))
    }

    /// Open `path` for `target`, fail the replacement after `completed`
    /// ones at `point`, and write every batch with `values`.
    fn failing_pass(
        path: &Path,
        target: WritableRole,
        (completed, point): (usize, FailurePoint),
        values: impl Fn(&casa_observation::VisibilityBatch<'_>) -> Owned,
    ) -> Result<UpdateReport, ObservationError> {
        let session = open_update(
            path,
            &OpenOptions::default(),
            &[UpdateTarget::new("base", target)],
        )?;
        let Store::V4(update) = &session.store else {
            panic!("an MSv4 session");
        };
        update.store().fail_after(completed, point);
        let request = request(session.observation());
        let mut traversal = session.observation().bind_read(&request)?;
        let mut updater = session.updater(&request, &[0])?;
        let mut buffers = BatchBuffers::default();
        while let Some(event) = traversal.next(&mut buffers)? {
            let ReadEvent::Batch(batch) = event else {
                continue;
            };
            updater.write(&batch, &[values(&batch).view()])?;
        }
        updater.finish()
    }

    use casa_observation::UpdateReport;

    /// A replacement that fails after its temporary file is written, as a
    /// process killed there leaves it, publishes nothing of that chunk:
    /// every chunk reads back whole, old or new. The fixture's six
    /// `[5, 10, 4, 4]` chunks are replaced row by row, so failing the
    /// third leaves times 0..5 new and times 5..12 old, with the temporary
    /// file beside the old chunk.
    #[test]
    fn a_failed_replacement_leaves_each_chunk_old_or_new() {
        let root = tempfile::tempdir().expect("root");
        let path = fixture(root.path());
        let (before, _) = contents(&path);
        let failed = failing_pass(
            &path,
            WritableRole::Visibilities,
            (2, FailurePoint::BeforeRename),
            doubled,
        );
        assert!(
            matches!(failed, Err(ObservationError::Storage(_))),
            "{failed:?}"
        );
        let (after, _) = contents(&path);
        let per_time = 10 * 8 * 4;
        for (index, (old, new)) in before.iter().zip(&after).enumerate() {
            let expected = if index / per_time < 5 {
                *old * 2.0
            } else {
                *old
            };
            assert_eq!(*new, expected, "element {index}, time {}", index / per_time);
        }
        let stray = std::fs::read_dir(path.join("ms_trial_0/VISIBILITY/c/1/0/0"))
            .expect("chunk directory")
            .filter_map(Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().contains(".replacing-"));
        assert!(
            stray,
            "the killed replacement's temporary file stays, unread"
        );
    }

    /// An absent chunk (one equal to the fill value is never stored) is
    /// first written by an update; when that write fails it stays absent
    /// and reads as the fill value. `FLAG`'s chunk `c/2/0/0/0` is absent in
    /// the fixture and is the fifth replaced.
    #[test]
    fn a_failed_first_write_leaves_an_absent_chunk_absent() {
        let root = tempfile::tempdir().expect("root");
        let path = fixture(root.path());
        let absent = path.join("ms_trial_0/FLAG/c/2/0/0/0");
        assert!(!absent.exists());
        let (_, before) = contents(&path);
        let set_all = |batch: &casa_observation::VisibilityBatch<'_>| {
            Owned::SetFlags(ndarray::Array3::from_elem(batch.shape(), true))
        };
        let failed = failing_pass(
            &path,
            WritableRole::Flags,
            (4, FailurePoint::BeforeRename),
            set_all,
        );
        assert!(
            matches!(failed, Err(ObservationError::Storage(_))),
            "{failed:?}"
        );
        assert!(!absent.exists(), "still absent");
        let (_, after) = contents(&path);
        let per_time = 10 * 8 * 4;
        for (index, (old, new)) in before.iter().zip(&after).enumerate() {
            let time = index / per_time;
            let channel = index / 4 % 8;
            let replaced = time < 10;
            let expected = replaced || *old;
            assert_eq!(
                *new, expected,
                "element {index} (time {time}, channel {channel})"
            );
        }
    }

    /// A failure before any byte of a replacement is written changes
    /// nothing.
    #[test]
    fn a_failure_before_writing_changes_nothing() {
        let root = tempfile::tempdir().expect("root");
        let path = fixture(root.path());
        let before = contents(&path);
        let failed = failing_pass(
            &path,
            WritableRole::Visibilities,
            (0, FailurePoint::BeforeWrite),
            doubled,
        );
        assert!(
            matches!(failed, Err(ObservationError::Storage(_))),
            "{failed:?}"
        );
        assert_eq!(contents(&path), before);
    }
}
