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
    pub(crate) unsafe fn cpp_error(
        operation: &'static str,
        pointer: *mut std::ffi::c_char,
        free: unsafe extern "C" fn(*mut std::ffi::c_char),
    ) -> OracleError {
        let message = if pointer.is_null() {
            "no diagnostic returned".to_owned()
        } else {
            let message = unsafe { CStr::from_ptr(pointer) }
                .to_string_lossy()
                .into_owned();
            unsafe { free(pointer) };
            message
        };
        OracleError::CppFailure { operation, message }
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

#[cfg(all(has_casacore_cpp, unix))]
impl Drop for OracleFileLock {
    fn drop(&mut self) {
        unsafe {
            libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CasacoreOracleRuntime, OracleError};

    #[test]
    fn interior_nul_is_a_typed_input_error() {
        let error = CasacoreOracleRuntime::c_string("unit", "m\0s").unwrap_err();
        assert!(matches!(
            error,
            OracleError::InvalidInput {
                context: "unit",
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
}
