// SPDX-License-Identifier: LGPL-3.0-or-later

//! URI resolution and bounded reads for tutorial dataset sources.

use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use ureq::ResponseExt;

use super::TutorialError;

/// URI metadata shown before acquisition approval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TutorialSourceResolution {
    pub resolved_uri: String,
    pub redirects: Vec<String>,
    pub size_bytes: Option<u64>,
}

/// One bounded source read used by resumable acquisition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TutorialReadChunk {
    pub bytes: Vec<u8>,
    pub complete: bool,
}

/// Pluggable scheme handler. Implementations never delegate to a shell or OS opener.
pub trait TutorialUriHandler: Send + Sync {
    fn scheme(&self) -> &str;
    fn resolve(&self, uri: &str) -> Result<TutorialSourceResolution, TutorialError>;
    fn read_chunk(
        &self,
        uri: &str,
        offset: u64,
        limit: u64,
    ) -> Result<TutorialReadChunk, TutorialError>;
}

/// Versioned scheme-to-handler registry. Unknown schemes remain inert.
#[derive(Clone)]
pub struct TutorialUriRegistry {
    version: u32,
    handlers: BTreeMap<String, Arc<dyn TutorialUriHandler>>,
}

impl TutorialUriRegistry {
    /// Construct the v1 registry with `file`, `http`, and `https` handlers.
    #[must_use]
    pub fn v1() -> Self {
        let mut registry = Self {
            version: 1,
            handlers: BTreeMap::new(),
        };
        registry.register(Arc::new(FileTutorialUriHandler));
        registry.register(Arc::new(HttpTutorialUriHandler::new("http")));
        registry.register(Arc::new(HttpTutorialUriHandler::new("https")));
        registry
    }

    /// Registry contract version persisted with acquisition state.
    #[must_use]
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// Install or replace one exact scheme handler.
    pub fn register(&mut self, handler: Arc<dyn TutorialUriHandler>) {
        self.handlers
            .insert(handler.scheme().to_ascii_lowercase(), handler);
    }

    pub(super) fn handler(&self, uri: &str) -> Result<Arc<dyn TutorialUriHandler>, TutorialError> {
        let scheme = uri_scheme(uri)?;
        self.handlers
            .get(&scheme)
            .cloned()
            .ok_or(TutorialError::UnknownScheme { scheme })
    }
}

struct FileTutorialUriHandler;

impl TutorialUriHandler for FileTutorialUriHandler {
    fn scheme(&self) -> &str {
        "file"
    }

    fn resolve(&self, uri: &str) -> Result<TutorialSourceResolution, TutorialError> {
        let path = file_uri_path(uri)?;
        let metadata = fs::metadata(&path).map_err(|source| TutorialError::Io {
            action: "inspect file tutorial source",
            path: path.clone(),
            source,
        })?;
        if !metadata.is_file() {
            return Err(TutorialError::SourceNotRegularFile { path });
        }
        Ok(TutorialSourceResolution {
            resolved_uri: uri.to_owned(),
            redirects: Vec::new(),
            size_bytes: Some(metadata.len()),
        })
    }

    fn read_chunk(
        &self,
        uri: &str,
        offset: u64,
        limit: u64,
    ) -> Result<TutorialReadChunk, TutorialError> {
        let path = file_uri_path(uri)?;
        let mut file = File::open(&path).map_err(|source| TutorialError::Io {
            action: "open file tutorial source",
            path: path.clone(),
            source,
        })?;
        let size = file
            .metadata()
            .map_err(|source| TutorialError::Io {
                action: "inspect file tutorial source",
                path: path.clone(),
                source,
            })?
            .len();
        file.seek(SeekFrom::Start(offset))
            .map_err(|source| TutorialError::Io {
                action: "seek file tutorial source",
                path: path.clone(),
                source,
            })?;
        let mut bytes = Vec::new();
        file.take(limit)
            .read_to_end(&mut bytes)
            .map_err(|source| TutorialError::Io {
                action: "read file tutorial source",
                path,
                source,
            })?;
        let read = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        Ok(TutorialReadChunk {
            bytes,
            complete: offset.saturating_add(read) >= size,
        })
    }
}

struct HttpTutorialUriHandler {
    scheme: &'static str,
    agent: ureq::Agent,
}

impl HttpTutorialUriHandler {
    fn new(scheme: &'static str) -> Self {
        let agent: ureq::Agent = ureq::config::Config::builder()
            .save_redirect_history(true)
            .build()
            .into();
        Self { scheme, agent }
    }
}

impl TutorialUriHandler for HttpTutorialUriHandler {
    fn scheme(&self) -> &str {
        self.scheme
    }

    fn resolve(&self, uri: &str) -> Result<TutorialSourceResolution, TutorialError> {
        let response = self
            .agent
            .head(uri)
            .call()
            .map_err(|source| TutorialError::Network {
                uri: uri.to_owned(),
                detail: source.to_string(),
            })?;
        let resolved_uri = response.get_uri().to_string();
        let redirects = response
            .get_redirect_history()
            .unwrap_or_default()
            .iter()
            .skip(1)
            .map(ToString::to_string)
            .collect();
        let size_bytes = response
            .headers()
            .get("content-length")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok());
        Ok(TutorialSourceResolution {
            resolved_uri,
            redirects,
            size_bytes,
        })
    }

    fn read_chunk(
        &self,
        uri: &str,
        offset: u64,
        limit: u64,
    ) -> Result<TutorialReadChunk, TutorialError> {
        let end = offset.saturating_add(limit.saturating_sub(1));
        let response = self
            .agent
            .get(uri)
            .header("Range", format!("bytes={offset}-{end}"))
            .call()
            .map_err(|source| TutorialError::Network {
                uri: uri.to_owned(),
                detail: source.to_string(),
            })?;
        if offset > 0 && response.status().as_u16() != 206 {
            return Err(TutorialError::ResumeUnsupported {
                uri: uri.to_owned(),
            });
        }
        let content_range_total = response
            .headers()
            .get("content-range")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.rsplit_once('/'))
            .and_then(|(_, total)| total.parse::<u64>().ok());
        let mut bytes = Vec::new();
        response
            .into_body()
            .into_reader()
            .take(limit)
            .read_to_end(&mut bytes)
            .map_err(|source| TutorialError::Network {
                uri: uri.to_owned(),
                detail: source.to_string(),
            })?;
        let read = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        Ok(TutorialReadChunk {
            bytes,
            complete: content_range_total.is_some_and(|total| offset.saturating_add(read) >= total)
                || read < limit,
        })
    }
}

pub(super) fn uri_scheme(uri: &str) -> Result<String, TutorialError> {
    let (scheme, _) = uri
        .split_once(':')
        .ok_or_else(|| TutorialError::InvalidUri {
            uri: uri.to_owned(),
        })?;
    if scheme.is_empty()
        || !scheme.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_alphabetic()
                || (index > 0 && (byte.is_ascii_digit() || matches!(byte, b'+' | b'-' | b'.')))
        })
    {
        return Err(TutorialError::InvalidUri {
            uri: uri.to_owned(),
        });
    }
    Ok(scheme.to_ascii_lowercase())
}

pub(super) fn file_uri_path(uri: &str) -> Result<PathBuf, TutorialError> {
    let scheme = uri_scheme(uri)?;
    if scheme != "file" {
        return Err(TutorialError::InvalidUri {
            uri: uri.to_owned(),
        });
    }
    let remainder = uri
        .strip_prefix("file://")
        .ok_or_else(|| TutorialError::InvalidUri {
            uri: uri.to_owned(),
        })?;
    let path = if let Some(path) = remainder.strip_prefix("localhost/") {
        format!("/{path}")
    } else if remainder.starts_with('/') {
        percent_decode(remainder)?
    } else {
        return Err(TutorialError::InvalidUri {
            uri: uri.to_owned(),
        });
    };
    let path = PathBuf::from(path);
    if !path.is_absolute() {
        return Err(TutorialError::InvalidUri {
            uri: uri.to_owned(),
        });
    }
    Ok(path)
}

fn percent_decode(value: &str) -> Result<String, TutorialError> {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(TutorialError::InvalidUri {
                    uri: value.to_owned(),
                });
            }
            let high = hex_value(bytes[index + 1]).ok_or_else(|| TutorialError::InvalidUri {
                uri: value.to_owned(),
            })?;
            let low = hex_value(bytes[index + 2]).ok_or_else(|| TutorialError::InvalidUri {
                uri: value.to_owned(),
            })?;
            output.push((high << 4) | low);
            index += 3;
        } else {
            output.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(output).map_err(|_| TutorialError::InvalidUri {
        uri: value.to_owned(),
    })
}

const fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}
