// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared runtime services for the casacore C++ test oracle.

use std::ffi::CString;
use std::fmt;

#[cfg(has_casacore_cpp)]
use std::ffi::CStr;
#[cfg(has_casacore_cpp)]
use std::sync::{Mutex, MutexGuard, OnceLock};
#[cfg(all(has_casacore_cpp, unix))]
use std::{fs::OpenOptions, os::fd::AsRawFd};

/// Independent process-global state domains used by casacore.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OracleDomain {
    MeasuresIau2000A,
    Imaging,
    Tables,
    Quanta,
}

/// Uniform failure model for all C++ oracle operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OracleError {
    Unavailable {
        capability: &'static str,
    },
    InvalidInput {
        context: &'static str,
        message: String,
    },
    CppFailure {
        operation: &'static str,
        message: String,
    },
    InvalidOutput {
        operation: &'static str,
        message: String,
    },
    LockFailure {
        domain: OracleDomain,
        message: String,
    },
}

impl fmt::Display for OracleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable { capability } => {
                write!(f, "casacore C++ oracle is unavailable for {capability}")
            }
            Self::InvalidInput { context, message } => {
                write!(f, "invalid {context}: {message}")
            }
            Self::CppFailure { operation, message } => {
                write!(f, "casacore C++ {operation} failed: {message}")
            }
            Self::InvalidOutput { operation, message } => {
                write!(
                    f,
                    "casacore C++ {operation} returned invalid output: {message}"
                )
            }
            Self::LockFailure { domain, message } => {
                write!(f, "casacore {:?} lock failed: {message}", domain)
            }
        }
    }
}

impl std::error::Error for OracleError {}

pub(crate) struct CasacoreOracleRuntime;

impl CasacoreOracleRuntime {
    pub(crate) fn available() -> bool {
        cfg!(has_casacore_cpp)
    }

    #[allow(dead_code)]
    pub(crate) fn require(capability: &'static str) -> Result<(), OracleError> {
        if Self::available() {
            Ok(())
        } else {
            Err(OracleError::Unavailable { capability })
        }
    }

    #[allow(dead_code)]
    pub(crate) fn c_string(context: &'static str, value: &str) -> Result<CString, OracleError> {
        CString::new(value).map_err(|error| OracleError::InvalidInput {
            context,
            message: error.to_string(),
        })
    }

    #[cfg(has_casacore_cpp)]
    pub(crate) fn c_path(
        context: &'static str,
        path: &std::path::Path,
    ) -> Result<CString, OracleError> {
        let value = path.to_str().ok_or_else(|| OracleError::InvalidInput {
            context,
            message: format!("path is not UTF-8: {}", path.display()),
        })?;
        Self::c_string(context, value)
    }

    #[cfg(has_casacore_cpp)]
    pub(crate) fn output_string(
        operation: &'static str,
        bytes: &[u8],
    ) -> Result<String, OracleError> {
        CStr::from_bytes_until_nul(bytes)
            .map_err(|error| OracleError::InvalidOutput {
                operation,
                message: error.to_string(),
            })?
            .to_str()
            .map(str::to_owned)
            .map_err(|error| OracleError::InvalidOutput {
                operation,
                message: error.to_string(),
            })
    }

    #[cfg(has_casacore_cpp)]
    pub(crate) fn output_c_char_string(
        operation: &'static str,
        bytes: &[std::ffi::c_char],
    ) -> Result<String, OracleError> {
        let bytes = unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast::<u8>(), bytes.len()) };
        Self::output_string(operation, bytes)
    }

    #[cfg(has_casacore_cpp)]
    pub(crate) fn status(operation: &'static str, status: i32) -> Result<(), OracleError> {
        if status == 0 {
            Ok(())
        } else {
            Err(OracleError::CppFailure {
                operation,
                message: format!("status {status}"),
            })
        }
    }

    #[cfg(has_casacore_cpp)]
    pub(crate) unsafe fn cpp_error_message(
        pointer: *mut std::ffi::c_char,
        free: unsafe extern "C" fn(*mut std::ffi::c_char),
    ) -> String {
        if pointer.is_null() {
            "no diagnostic returned".to_owned()
        } else {
            let message = unsafe { CStr::from_ptr(pointer) }
                .to_string_lossy()
                .into_owned();
            unsafe { free(pointer) };
            message
        }
    }

    #[cfg(has_casacore_cpp)]
    pub(crate) unsafe fn cpp_error(
        operation: &'static str,
        pointer: *mut std::ffi::c_char,
        free: unsafe extern "C" fn(*mut std::ffi::c_char),
    ) -> OracleError {
        let message = unsafe { Self::cpp_error_message(pointer, free) };
        OracleError::CppFailure { operation, message }
    }

    #[cfg(has_casacore_cpp)]
    pub(crate) unsafe fn owned_string(
        operation: &'static str,
        pointer: *mut std::ffi::c_char,
        free: unsafe extern "C" fn(*mut std::ffi::c_char),
    ) -> Result<String, OracleError> {
        if pointer.is_null() {
            return Err(OracleError::InvalidOutput {
                operation,
                message: "null string pointer".to_owned(),
            });
        }
        let result = unsafe { CStr::from_ptr(pointer) }
            .to_string_lossy()
            .into_owned();
        unsafe { free(pointer) };
        Ok(result)
    }

    #[cfg(has_casacore_cpp)]
    pub(crate) unsafe fn owned_vec<T: Copy>(
        operation: &'static str,
        pointer: *mut T,
        len: usize,
        free: unsafe extern "C" fn(*mut T),
    ) -> Result<Vec<T>, OracleError> {
        if len == 0 {
            if !pointer.is_null() {
                unsafe { free(pointer) };
            }
            return Ok(Vec::new());
        }
        if pointer.is_null() {
            return Err(OracleError::InvalidOutput {
                operation,
                message: format!("null vector pointer for {len} elements"),
            });
        }
        let result = unsafe { std::slice::from_raw_parts(pointer, len) }.to_vec();
        unsafe { free(pointer) };
        Ok(result)
    }

    #[cfg(has_casacore_cpp)]
    pub(crate) fn lock(domain: OracleDomain) -> Result<OracleGuard, OracleError> {
        let mutex = match domain {
            OracleDomain::MeasuresIau2000A => {
                static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
                LOCK.get_or_init(|| Mutex::new(()))
            }
            OracleDomain::Imaging => {
                static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
                LOCK.get_or_init(|| Mutex::new(()))
            }
            OracleDomain::Tables => {
                static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
                LOCK.get_or_init(|| Mutex::new(()))
            }
            OracleDomain::Quanta => {
                static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
                LOCK.get_or_init(|| Mutex::new(()))
            }
        }
        .lock()
        .map_err(|error| OracleError::LockFailure {
            domain,
            message: error.to_string(),
        })?;

        Ok(OracleGuard {
            _mutex: mutex,
            #[cfg(all(has_casacore_cpp, unix))]
            _file_lock: match domain {
                OracleDomain::Tables | OracleDomain::Imaging => {
                    Some(OracleFileLock::acquire(domain)?)
                }
                OracleDomain::MeasuresIau2000A | OracleDomain::Quanta => None,
            },
        })
    }

    /// Acquire the proven global-state domain for an operation, if it needs one.
    #[cfg(has_casacore_cpp)]
    pub(crate) fn lock_operation(
        operation: &'static str,
    ) -> Result<Option<OracleGuard>, OracleError> {
        let domain = match operation {
            "measures.iau2000_precession_matrix" | "measures.direction_convert_iau2000a" => {
                Some(OracleDomain::MeasuresIau2000A)
            }
            _ => None,
        };
        domain.map(Self::lock).transpose()
    }
}

#[cfg(has_casacore_cpp)]
pub(crate) struct OracleGuard {
    _mutex: MutexGuard<'static, ()>,
    #[cfg(all(has_casacore_cpp, unix))]
    _file_lock: Option<OracleFileLock>,
}

#[cfg(all(has_casacore_cpp, unix))]
struct OracleFileLock {
    file: std::fs::File,
}

#[cfg(all(has_casacore_cpp, unix))]
impl OracleFileLock {
    fn acquire(domain: OracleDomain) -> Result<Self, OracleError> {
        let suffix = match domain {
            OracleDomain::MeasuresIau2000A => "measures-iau2000a",
            OracleDomain::Imaging => "imaging-interop",
            OracleDomain::Tables => "tables",
            OracleDomain::Quanta => "quanta",
        };
        let path = std::env::temp_dir().join(format!("casa-rs-casa-test-support-{suffix}.lock"));
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|error| OracleError::LockFailure {
                domain,
                message: format!("open {}: {error}", path.display()),
            })?;
        let status = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if status != 0 {
            return Err(OracleError::LockFailure {
                domain,
                message: std::io::Error::last_os_error().to_string(),
            });
        }
        Ok(Self { file })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn c_string_rejects_embedded_nul_with_context() {
        let error = CasacoreOracleRuntime::c_string("test value", "bad\0value")
            .expect_err("embedded NUL must be rejected");
        assert!(matches!(
            error,
            OracleError::InvalidInput {
                context: "test value",
                ..
            }
        ));
    }

    #[cfg(not(has_casacore_cpp))]
    #[test]
    fn unavailable_is_typed() {
        assert_eq!(
            CasacoreOracleRuntime::require("quanta"),
            Err(OracleError::Unavailable {
                capability: "quanta"
            })
        );
    }

    #[cfg(has_casacore_cpp)]
    #[test]
    fn output_string_rejects_missing_nul() {
        let error = CasacoreOracleRuntime::output_string("test.output", b"unterminated")
            .expect_err("unterminated output must be rejected");
        assert!(matches!(
            error,
            OracleError::InvalidOutput {
                operation: "test.output",
                ..
            }
        ));
    }

    #[cfg(has_casacore_cpp)]
    #[test]
    fn same_domain_calls_are_serialized() {
        use std::sync::mpsc;
        use std::time::Duration;

        let first = CasacoreOracleRuntime::lock(OracleDomain::MeasuresIau2000A)
            .expect("acquire first measures guard");
        let (attempted_tx, attempted_rx) = mpsc::channel();
        let (acquired_tx, acquired_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            attempted_tx.send(()).expect("report lock attempt");
            let _second = CasacoreOracleRuntime::lock(OracleDomain::MeasuresIau2000A)
                .expect("acquire second measures guard");
            acquired_tx.send(()).expect("report acquired lock");
        });

        attempted_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("worker attempted lock");
        assert!(
            acquired_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "same-domain lock was acquired while the first guard was held"
        );
        drop(first);
        acquired_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("worker acquired released lock");
        worker.join().expect("worker completed");
    }

    #[cfg(has_casacore_cpp)]
    #[test]
    fn independent_domains_can_run_in_parallel() {
        use std::sync::mpsc;
        use std::time::Duration;

        let measures = CasacoreOracleRuntime::lock(OracleDomain::MeasuresIau2000A)
            .expect("acquire measures guard");
        let (acquired_tx, acquired_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let _quanta = CasacoreOracleRuntime::lock(OracleDomain::Quanta)
                .expect("acquire independent quanta guard");
            acquired_tx.send(()).expect("report acquired lock");
        });

        acquired_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("independent domain remained available");
        drop(measures);
        worker.join().expect("worker completed");
    }
}

#[cfg(all(has_casacore_cpp, unix))]
impl Drop for OracleFileLock {
    fn drop(&mut self) {
        unsafe {
            libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}
