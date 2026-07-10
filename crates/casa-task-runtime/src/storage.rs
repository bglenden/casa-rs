// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::session::{ParameterSession, ParameterSessionError};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Atomically write an explicitly named sparse profile.
///
/// Unlike managed Last storage this does not take a per-surface lock: the
/// destination is user-selected and a failed concurrent Save As remains an
/// explicit error. The temporary file is nevertheless created beside the
/// destination, flushed, and atomically renamed so readers never observe a
/// partial profile.
pub fn write_parameter_profile_atomic(
    path: impl AsRef<Path>,
    contents: &str,
) -> Result<StateWriteOutcome, ManagedStateError> {
    let target = path.as_ref();
    let parent = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|source| ManagedStateError::Io {
        path: parent.to_path_buf(),
        source,
    })?;
    let filename = target
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| ManagedStateError::Io {
            path: target.to_path_buf(),
            source: io::Error::new(
                io::ErrorKind::InvalidInput,
                "parameter profile path must name a UTF-8 file",
            ),
        })?;
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temp = parent.join(format!(
        ".{filename}.tmp.{}.{}",
        std::process::id(),
        sequence
    ));
    write_and_replace(&temp, target, contents)?;
    Ok(StateWriteOutcome {
        path: target.to_path_buf(),
        bytes_written: contents.len(),
    })
}

/// Managed profile slot for one surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedProfileKind {
    /// Most recently validated task attempt or accepted session state.
    Last,
    /// Most recently successful task invocation.
    LastSuccessful,
}

impl ManagedProfileKind {
    fn filename(self) -> &'static str {
        match self {
            Self::Last => "last.toml",
            Self::LastSuccessful => "last-successful.toml",
        }
    }
}

/// Result of one atomic state write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateWriteOutcome {
    pub path: PathBuf,
    pub bytes_written: usize,
}

/// Common result for automatic persistence. Callers surface failures as a
/// prominent warning without changing the scientific task/session result.
#[derive(Debug)]
pub struct AutomaticSaveReport {
    pub outcome: Option<StateWriteOutcome>,
    pub warning: Option<String>,
}

impl AutomaticSaveReport {
    fn from_result(result: Result<StateWriteOutcome, ManagedStateError>) -> Self {
        match result {
            Ok(outcome) => Self {
                outcome: Some(outcome),
                warning: None,
            },
            Err(error) => Self {
                outcome: None,
                warning: Some(error.to_string()),
            },
        }
    }
}

/// Managed sparse-profile storage rooted at one explicit workspace.
#[derive(Debug, Clone)]
pub struct ManagedStateStore {
    state_root: PathBuf,
}

impl ManagedStateStore {
    /// Resolve the managed state root from `CASA_RS_STATE_DIR` or a workspace.
    ///
    /// `CASA_RS_STATE_DIR` replaces `<workspace>/.casa-rs`; the `parameters`
    /// component remains stable beneath either root.
    pub fn for_workspace(workspace: impl AsRef<Path>) -> Self {
        let state_root = std::env::var_os("CASA_RS_STATE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| workspace.as_ref().join(".casa-rs"));
        Self { state_root }
    }

    /// Construct a store with an explicit state root, primarily for tests and
    /// embedders that must not consult process environment.
    pub fn with_state_root(state_root: impl Into<PathBuf>) -> Self {
        Self {
            state_root: state_root.into(),
        }
    }

    /// Return the directory containing managed profiles for `surface_id`.
    pub fn surface_dir(&self, surface_id: &str) -> Result<PathBuf, ManagedStateError> {
        validate_surface_id(surface_id)?;
        Ok(self.state_root.join("parameters").join(surface_id))
    }

    /// Return the path for one managed profile slot.
    pub fn profile_path(
        &self,
        surface_id: &str,
        kind: ManagedProfileKind,
    ) -> Result<PathBuf, ManagedStateError> {
        Ok(self.surface_dir(surface_id)?.join(kind.filename()))
    }

    /// Read a managed profile if it exists.
    pub fn read(
        &self,
        surface_id: &str,
        kind: ManagedProfileKind,
    ) -> Result<Option<String>, ManagedStateError> {
        let path = self.profile_path(surface_id, kind)?;
        match fs::read_to_string(&path) {
            Ok(text) => Ok(Some(text)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(ManagedStateError::Io { path, source }),
        }
    }

    /// Atomically replace a managed profile while holding the surface lock.
    ///
    /// The temporary file is created in the destination directory, flushed,
    /// and renamed over the old value. A process crash therefore leaves either
    /// the previous complete profile or the new complete profile, never a
    /// partially written destination.
    pub fn write(
        &self,
        surface_id: &str,
        kind: ManagedProfileKind,
        contents: &str,
    ) -> Result<StateWriteOutcome, ManagedStateError> {
        let directory = self.surface_dir(surface_id)?;
        fs::create_dir_all(&directory).map_err(|source| ManagedStateError::Io {
            path: directory.clone(),
            source,
        })?;

        let lock_path = directory.join(".lock");
        let lock = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|source| ManagedStateError::Io {
                path: lock_path.clone(),
                source,
            })?;
        lock.lock_exclusive()
            .map_err(|source| ManagedStateError::Lock {
                path: lock_path.clone(),
                source,
            })?;

        let target = directory.join(kind.filename());
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temp = directory.join(format!(
            ".{}.tmp.{}.{}",
            kind.filename(),
            std::process::id(),
            sequence
        ));
        let write_result = write_and_replace(&temp, &target, contents);
        let unlock_result = FileExt::unlock(&lock);

        if let Err(source) = unlock_result {
            if write_result.is_ok() {
                return Err(ManagedStateError::Lock {
                    path: lock_path,
                    source,
                });
            }
        }
        write_result?;
        Ok(StateWriteOutcome {
            path: target,
            bytes_written: contents.len(),
        })
    }
}

/// Attempted/successful Last lifecycle for one task invocation.
#[derive(Debug)]
pub struct TaskLastState {
    store: ManagedStateStore,
    surface_id: String,
    enabled: bool,
    attempted_snapshot: Option<String>,
}

impl TaskLastState {
    pub fn new(store: ManagedStateStore, surface_id: impl Into<String>, enabled: bool) -> Self {
        Self {
            store,
            surface_id: surface_id.into(),
            enabled,
            attempted_snapshot: None,
        }
    }

    /// Record validated resolved intent immediately before provider execution.
    pub fn before_execution(
        &mut self,
        session: &ParameterSession,
    ) -> Result<AutomaticSaveReport, ParameterSessionError> {
        let snapshot = session.render_sparse()?;
        self.attempted_snapshot = Some(snapshot.clone());
        if !self.enabled {
            return Ok(AutomaticSaveReport {
                outcome: None,
                warning: None,
            });
        }
        Ok(AutomaticSaveReport::from_result(self.store.write(
            &self.surface_id,
            ManagedProfileKind::Last,
            &snapshot,
        )))
    }

    /// Promote exactly the attempted snapshot after successful completion.
    /// Failure and cancellation deliberately do nothing.
    pub fn after_completion(&mut self, successful: bool) -> AutomaticSaveReport {
        let Some(snapshot) = self.attempted_snapshot.take() else {
            return AutomaticSaveReport {
                outcome: None,
                warning: None,
            };
        };
        if !successful || !self.enabled {
            return AutomaticSaveReport {
                outcome: None,
                warning: None,
            };
        }
        AutomaticSaveReport::from_result(self.store.write(
            &self.surface_id,
            ManagedProfileKind::LastSuccessful,
            &snapshot,
        ))
    }
}

/// Successful-open and debounced accepted-change Last lifecycle for a session.
#[derive(Debug)]
pub struct SessionLastState {
    store: ManagedStateStore,
    surface_id: String,
    enabled: bool,
    debounce: Duration,
    opened: bool,
    pending: Option<(Instant, String)>,
}

impl SessionLastState {
    pub fn new(
        store: ManagedStateStore,
        surface_id: impl Into<String>,
        enabled: bool,
        debounce: Duration,
    ) -> Self {
        Self {
            store,
            surface_id: surface_id.into(),
            enabled,
            debounce,
            opened: false,
            pending: None,
        }
    }

    /// Record Last only after the backend has successfully opened the root.
    pub fn opened(
        &mut self,
        session: &ParameterSession,
    ) -> Result<AutomaticSaveReport, ParameterSessionError> {
        let snapshot = session.render_sparse()?;
        self.opened = true;
        self.pending = None;
        if !self.enabled {
            return Ok(AutomaticSaveReport {
                outcome: None,
                warning: None,
            });
        }
        Ok(AutomaticSaveReport::from_result(self.store.write(
            &self.surface_id,
            ManagedProfileKind::Last,
            &snapshot,
        )))
    }

    /// Queue a durable setting only after the backend accepted the change.
    pub fn accepted_durable_change(
        &mut self,
        session: &ParameterSession,
        now: Instant,
    ) -> Result<(), ParameterSessionError> {
        if self.opened && self.enabled {
            self.pending = Some((now + self.debounce, session.render_sparse()?));
        }
        Ok(())
    }

    /// Flush a due debounced update. Transient navigation never calls the
    /// queue method and therefore cannot cause a write.
    pub fn flush_if_due(&mut self, now: Instant) -> AutomaticSaveReport {
        if self
            .pending
            .as_ref()
            .is_none_or(|(deadline, _)| *deadline > now)
        {
            return AutomaticSaveReport {
                outcome: None,
                warning: None,
            };
        }
        self.flush()
    }

    /// Flush pending durable state on clean close.
    pub fn flush(&mut self) -> AutomaticSaveReport {
        let Some((_, snapshot)) = self.pending.take() else {
            return AutomaticSaveReport {
                outcome: None,
                warning: None,
            };
        };
        AutomaticSaveReport::from_result(self.store.write(
            &self.surface_id,
            ManagedProfileKind::Last,
            &snapshot,
        ))
    }
}

fn write_and_replace(temp: &Path, target: &Path, contents: &str) -> Result<(), ManagedStateError> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(temp)
        .map_err(|source| ManagedStateError::Io {
            path: temp.to_path_buf(),
            source,
        })?;
    let operation = (|| {
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
        fs::rename(temp, target)?;
        sync_parent(target)?;
        Ok::<(), io::Error>(())
    })();
    if let Err(source) = operation {
        let _ = fs::remove_file(temp);
        return Err(ManagedStateError::Io {
            path: target.to_path_buf(),
            source,
        });
    }
    Ok(())
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "managed profile has no parent")
    })?;
    File::open(parent)?.sync_all()
}

#[cfg(not(unix))]
fn sync_parent(_path: &Path) -> io::Result<()> {
    Ok(())
}

fn validate_surface_id(surface_id: &str) -> Result<(), ManagedStateError> {
    let valid = !surface_id.is_empty()
        && surface_id
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
    if valid {
        Ok(())
    } else {
        Err(ManagedStateError::InvalidSurfaceId(surface_id.to_string()))
    }
}

/// Managed-state storage failure.
#[derive(Debug, Error)]
pub enum ManagedStateError {
    #[error("invalid surface id {0:?}")]
    InvalidSurfaceId(String),
    #[error("I/O error at {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error("lock error at {path}: {source}")]
    Lock { path: PathBuf, source: io::Error },
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::*;

    #[test]
    fn uses_workspace_convention() {
        let store = ManagedStateStore::with_state_root("/work/.casa-rs");
        assert_eq!(
            store
                .profile_path("imager", ManagedProfileKind::LastSuccessful)
                .unwrap(),
            Path::new("/work/.casa-rs/parameters/imager/last-successful.toml")
        );
    }

    #[test]
    fn rejects_path_traversal_surface_ids() {
        let store = ManagedStateStore::with_state_root("state");
        assert!(store.surface_dir("../imager").is_err());
        assert!(store.surface_dir("Image Browser").is_err());
    }

    #[test]
    fn writes_and_reads_complete_profiles() {
        let temp = tempfile::tempdir().unwrap();
        let store = ManagedStateStore::with_state_root(temp.path());
        store
            .write("imager", ManagedProfileKind::Last, "first\n")
            .unwrap();
        store
            .write("imager", ManagedProfileKind::Last, "second\n")
            .unwrap();
        assert_eq!(
            store.read("imager", ManagedProfileKind::Last).unwrap(),
            Some("second\n".to_string())
        );
    }

    #[test]
    fn explicit_profile_save_is_atomic_and_creates_parent_directories() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("profiles/named.toml");
        let outcome = write_parameter_profile_atomic(&target, "first\n").unwrap();
        assert_eq!(outcome.path, target);
        assert_eq!(outcome.bytes_written, 6);
        write_parameter_profile_atomic(&target, "second\n").unwrap();
        assert_eq!(fs::read_to_string(&target).unwrap(), "second\n");
        assert!(
            fs::read_dir(target.parent().unwrap())
                .unwrap()
                .all(|entry| !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .contains(".tmp."))
        );
    }

    #[test]
    fn concurrent_writers_never_interleave() {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(ManagedStateStore::with_state_root(temp.path()));
        let barrier = Arc::new(Barrier::new(3));
        let mut handles = Vec::new();
        for body in ["a".repeat(32_768), "b".repeat(32_768)] {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                store
                    .write("imager", ManagedProfileKind::Last, &body)
                    .unwrap();
            }));
        }
        barrier.wait();
        for handle in handles {
            handle.join().unwrap();
        }
        let actual = store
            .read("imager", ManagedProfileKind::Last)
            .unwrap()
            .unwrap();
        assert!(actual.bytes().all(|byte| byte == b'a') || actual.bytes().all(|byte| byte == b'b'));
    }
}
