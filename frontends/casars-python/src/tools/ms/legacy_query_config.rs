// SPDX-License-Identifier: LGPL-3.0-or-later
//! Casacore resource defaults for legacy TaQL copies.

use std::path::PathBuf;
use std::sync::OnceLock;

use casa_tables::EndianFormat;

pub(super) struct QueryDefaults {
    pub(super) endian: EndianFormat,
    pub(super) storage: Option<String>,
}

pub(super) fn query_defaults() -> &'static QueryDefaults {
    static DEFAULTS: OnceLock<QueryDefaults> = OnceLock::new();
    DEFAULTS.get_or_init(|| {
        let mut endian = None;
        let mut storage = None;
        for path in resource_paths() {
            let Ok(contents) = std::fs::read_to_string(path) else {
                continue;
            };
            for line in contents.lines().map(str::trim) {
                if line.starts_with('#') {
                    continue;
                }
                let Some((name, value)) = line.split_once(':') else {
                    continue;
                };
                let name = name.trim();
                if endian.is_none() && rc_key_matches(name, "table.endianformat") {
                    endian = Some(match value.trim().to_ascii_lowercase().as_str() {
                        "big" => EndianFormat::BigEndian,
                        "little" => EndianFormat::LittleEndian,
                        _ => EndianFormat::LocalEndian,
                    });
                }
                if storage.is_none() && rc_key_matches(name, "table.storage.option") {
                    storage = Some(value.trim().to_ascii_lowercase());
                }
                if endian.is_some() && storage.is_some() {
                    break;
                }
            }
        }
        QueryDefaults {
            endian: endian.unwrap_or_default(),
            storage,
        }
    })
}

fn resource_paths() -> Vec<PathBuf> {
    if let Some(list) = std::env::var_os("CASARCFILES").filter(|value| !value.is_empty()) {
        return list
            .to_string_lossy()
            .split(':')
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .collect();
    }
    let home = PathBuf::from(std::env::var_os("HOME").unwrap_or_default());
    let mut paths = vec![
        home.join(".casarc"),
        home.join(".casa/rc"),
        home.join(".aipsrc"),
    ];
    let installation = std::env::var("CASAPATH")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| std::env::var("AIPSPATH").ok());
    let mut components = installation
        .as_deref()
        .unwrap_or_default()
        .split_whitespace();
    let root = components.next().map_or(home, PathBuf::from);
    let architecture = root.join(components.next().unwrap_or("UnKnOwN"));
    let site = architecture.join(components.next().unwrap_or("UnKnOwN"));
    let host = site.join(components.next().unwrap_or("UnKnOwN"));
    paths.extend([
        root.join(".aipsrc"),
        host.join("aipsrc"),
        site.join("aipsrc"),
        architecture.join("aipsrc"),
    ]);
    paths
}

fn rc_key_matches(pattern: &str, key: &str) -> bool {
    let Some((prefix, rest)) = pattern.split_once('*') else {
        return key == pattern;
    };
    let Some(mut key) = key.strip_prefix(prefix) else {
        return false;
    };
    let mut parts = rest.split('*').peekable();
    while let Some(part) = parts.next() {
        if parts.peek().is_none() {
            return key.ends_with(part);
        }
        let Some((_, after)) = key.split_once(part) else {
            return false;
        };
        key = after;
    }
    false
}
