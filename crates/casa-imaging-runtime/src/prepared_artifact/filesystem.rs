// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn prepare_private_root(path: &Path) -> Result<PathBuf, PreparedArtifactError> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    reject_casa_visible_root(&path)?;
    let mut missing = Vec::new();
    let mut cursor = path.as_path();
    loop {
        match cursor.symlink_metadata() {
            Ok(_) => {
                let existing = validate_existing_private_ancestors(cursor, missing.is_empty())?;
                let root = create_private_missing(existing, &missing)?;
                let canonical = fs::canonicalize(&root)?;
                reject_casa_visible_root(&canonical)?;
                reject_casa_cache_contents(&canonical)?;
                if !canonical.is_dir() {
                    return Err(PreparedArtifactError::UnknownCacheEntry(canonical));
                }
                return Ok(canonical);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let component = cursor
                    .file_name()
                    .ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(path.clone()))?;
                missing.push(component.to_owned());
                cursor = cursor
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new("."));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

pub(super) fn validate_existing_private_ancestors(
    path: &Path,
    inventory_candidate: bool,
) -> Result<PathBuf, PreparedArtifactError> {
    let mut nearest = None;
    for (index, ancestor) in path.ancestors().enumerate() {
        let metadata = ancestor.symlink_metadata()?;
        let canonical = fs::canonicalize(ancestor)?;
        reject_casa_visible_root(&canonical)?;
        if !canonical.is_dir() {
            return Err(PreparedArtifactError::UnknownCacheEntry(
                ancestor.to_path_buf(),
            ));
        }
        if index == 0 && inventory_candidate {
            reject_casa_cache_contents(&canonical)?;
        } else {
            reject_casacore_table_directory(&canonical)?;
        }
        if index == 0 && metadata.file_type().is_symlink() {
            return Err(PreparedArtifactError::UnknownCacheEntry(
                ancestor.to_path_buf(),
            ));
        }
        nearest.get_or_insert(canonical);
    }
    nearest.ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(path.to_path_buf()))
}

pub(super) fn reject_casacore_table_directory(path: &Path) -> Result<(), PreparedArtifactError> {
    for entry in fs::read_dir(path)? {
        let child = entry?.path();
        let name = child
            .file_name()
            .ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(child.clone()))?;
        if casacore_table_marker_name(&name.to_string_lossy()) {
            return Err(PreparedArtifactError::CasaVisiblePath(child));
        }
    }
    Ok(())
}

pub(super) fn create_private_missing(
    mut parent: PathBuf,
    missing: &[std::ffi::OsString],
) -> Result<PathBuf, PreparedArtifactError> {
    for component in missing.iter().rev() {
        let child = parent.join(component);
        match child.symlink_metadata() {
            Ok(metadata) => {
                let canonical = fs::canonicalize(&child)?;
                reject_casa_visible_root(&canonical)?;
                if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
                    return Err(PreparedArtifactError::UnknownCacheEntry(child));
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::create_dir(&child)?;
            }
            Err(error) => return Err(error.into()),
        }
        parent = child;
    }
    Ok(parent)
}

pub(super) fn ensure_private_child_directory(
    parent: &Path,
    child: &Path,
) -> Result<(), PreparedArtifactError> {
    match child.symlink_metadata() {
        Ok(metadata) => {
            let canonical = fs::canonicalize(child)?;
            reject_casa_visible_root(&canonical)?;
            if metadata.file_type().is_symlink() || canonical != child {
                return Err(PreparedArtifactError::UnknownCacheEntry(
                    child.to_path_buf(),
                ));
            }
            if !metadata.file_type().is_dir() {
                return Err(PreparedArtifactError::UnknownCacheEntry(
                    child.to_path_buf(),
                ));
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if fs::canonicalize(parent)? != parent {
                return Err(PreparedArtifactError::UnknownCacheEntry(
                    parent.to_path_buf(),
                ));
            }
            fs::create_dir(child)?;
            let canonical = fs::canonicalize(child)?;
            reject_casa_visible_root(&canonical)?;
            if canonical != child {
                return Err(PreparedArtifactError::UnknownCacheEntry(
                    child.to_path_buf(),
                ));
            }
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

pub(super) fn reject_casa_visible_root(path: &Path) -> Result<(), PreparedArtifactError> {
    if path.components().any(|component| {
        let name = component.as_os_str().to_string_lossy();
        casa_visible_name(&name)
    }) {
        Err(PreparedArtifactError::CasaVisiblePath(path.to_path_buf()))
    } else {
        Ok(())
    }
}

pub(super) fn reject_casa_source_path(
    path: &Path,
    evidence: &mut ValidationEvidence,
) -> Result<(), PreparedArtifactError> {
    reject_casa_visible_root(path)?;
    for ancestor in path.ancestors().skip(1) {
        let table_dat = ancestor.join("table.dat");
        let table_info = ancestor.join("table.info");
        evidence.source_read_operation();
        let has_table_dat = match table_dat.symlink_metadata() {
            Ok(metadata) => metadata.file_type().is_file(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => false,
            Err(error) => return Err(error.into()),
        };
        evidence.source_read_operation();
        let has_table_info = match table_info.symlink_metadata() {
            Ok(metadata) => metadata.file_type().is_file(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => false,
            Err(error) => return Err(error.into()),
        };
        if has_table_dat || has_table_info {
            return Err(PreparedArtifactError::CasaVisiblePath(
                ancestor.to_path_buf(),
            ));
        }
    }
    Ok(())
}

pub(super) fn reject_casa_cache_contents(root: &Path) -> Result<(), PreparedArtifactError> {
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        let name = path
            .file_name()
            .ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(path.clone()))?;
        let name = name.to_string_lossy();
        if casa_visible_name(&name) || casacore_table_marker_name(&name) {
            return Err(PreparedArtifactError::CasaVisiblePath(path));
        }
    }
    Ok(())
}

pub(super) fn casacore_table_marker_name(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    matches!(name.as_str(), "table.dat" | "table.info" | "table.lock")
        || name
            .strip_prefix("table.f")
            .and_then(|suffix| suffix.as_bytes().first())
            .is_some_and(u8::is_ascii_digit)
}

pub(super) fn casa_visible_name(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name.ends_with(".im")
        || name.ends_with(".ms")
        || name.starts_with("cfs_")
        || name.starts_with("wtcfs_")
}

pub(super) fn valid_segment_name(name: &str) -> bool {
    valid_identifier(name)
        && name != MANIFEST_FILE
        && name != PAYLOAD_FILE
        && !name.starts_with("CFS_")
        && !name.starts_with("WTCFS_")
        && !casa_visible_name(name)
}

pub(super) fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'@'))
}

pub(super) fn directory_size_counted(
    path: &Path,
    evidence: &mut ValidationEvidence,
) -> Result<u64, PreparedArtifactError> {
    with_directory_paths_counted(path, evidence, MAX_ENTRY_FILES, |evidence, paths| {
        paths.iter().try_fold(0_u64, |total, entry| {
            evidence.store_read_operation();
            let metadata = entry.symlink_metadata()?;
            if !metadata.file_type().is_file() {
                return Err(PreparedArtifactError::UnknownCacheEntry(
                    entry.to_path_buf(),
                ));
            }
            total
                .checked_add(metadata.len())
                .ok_or(PreparedArtifactError::ArtifactTooLarge)
        })
    })
}

pub(super) fn with_directory_paths_counted<T>(
    path: &Path,
    evidence: &mut ValidationEvidence,
    limit: usize,
    use_paths: impl FnOnce(&mut ValidationEvidence, &[Box<Path>]) -> Result<T, PreparedArtifactError>,
) -> Result<T, PreparedArtifactError> {
    evidence.store_read_operation();
    let entries = fs::read_dir(path)?;
    evidence.observe_file_descriptors(2);
    let mut paths = Vec::with_capacity(limit);
    let inventory = (|| {
        for entry in entries {
            evidence.store_read_operation();
            if paths.len() == limit {
                return Err(PreparedArtifactError::CacheEntryBudgetExceeded {
                    required: limit.saturating_add(1),
                    budget: limit,
                });
            }
            let path = entry?.path();
            let component = path
                .file_name()
                .ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(path.clone()))?;
            if component.as_encoded_bytes().len() > MAX_CACHE_COMPONENT_BYTES {
                return Err(PreparedArtifactError::UnknownCacheEntry(path));
            }
            paths.push(path.into_boxed_path());
        }
        paths.sort_unstable();
        Ok(())
    })();
    let resident_bytes = observed_path_inventory_bytes(&paths, paths.capacity());
    evidence.with_resident(resident_bytes, |evidence| {
        inventory?;
        use_paths(evidence, &paths)
    })
}

pub(super) fn root_inventory_limit(
    budget: PreparedArtifactBudget,
) -> Result<usize, PreparedArtifactError> {
    budget
        .entries
        .checked_mul(2)
        .and_then(|entries| entries.checked_add(1))
        .ok_or(PreparedArtifactError::ArtifactTooLarge)
}

pub(super) fn inventory_resident_reservation(
    cache: &Path,
    budget: PreparedArtifactBudget,
) -> Result<u64, PreparedArtifactError> {
    let cache_path_bytes = cache.as_os_str().as_encoded_bytes().len();
    let root_paths = path_inventory_resident_reservation(
        cache_path_bytes,
        root_inventory_limit(budget)?,
        MAX_CACHE_COMPONENT_BYTES,
    )?;
    let entry_directory_bytes = cache_path_bytes
        .checked_add(1 + MAX_CACHE_COMPONENT_BYTES)
        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    let entry_paths = path_inventory_resident_reservation(
        entry_directory_bytes,
        MAX_ENTRY_FILES,
        MAX_CACHE_COMPONENT_BYTES,
    )?;
    let cache_entries = fixed_vec_resident_reservation::<CacheInventoryEntry>(budget.entries)?;
    let evictions = fixed_vec_resident_reservation::<(ArtifactIdentity, u64)>(budget.entries)?;
    root_paths
        .checked_add(entry_paths)
        .and_then(|bytes| bytes.checked_add(cache_entries))
        .and_then(|bytes| bytes.checked_add(evictions))
        .ok_or(PreparedArtifactError::ArtifactTooLarge)
}

pub(super) fn source_descriptor_reservation(segments: usize) -> Result<u64, PreparedArtifactError> {
    let input_bytes = size_of::<PreparedArtifactSourceSegment>()
        .checked_add(MAX_IDENTIFIER_BYTES)
        .and_then(|bytes| bytes.checked_add(MAX_SOURCE_PATH_BYTES))
        .and_then(|bytes| bytes.checked_mul(segments))
        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    let canonical_path_bytes = size_of::<PathBuf>()
        .checked_add(MAX_SOURCE_PATH_BYTES)
        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    u64::try_from(
        input_bytes
            .checked_add(canonical_path_bytes)
            .ok_or(PreparedArtifactError::ArtifactTooLarge)?,
    )
    .map_err(|_| PreparedArtifactError::ArtifactTooLarge)
}

pub(super) fn path_inventory_resident_reservation(
    directory_bytes: usize,
    entries: usize,
    component_bytes: usize,
) -> Result<u64, PreparedArtifactError> {
    let path_bytes = directory_bytes
        .checked_add(1)
        .and_then(|bytes| bytes.checked_add(component_bytes))
        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    let entry_bytes = size_of::<Box<Path>>()
        .checked_add(path_bytes)
        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    let allocation = entries
        .checked_mul(entry_bytes)
        .and_then(|bytes| bytes.checked_add(size_of::<Vec<Box<Path>>>()))
        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    u64::try_from(allocation).map_err(|_| PreparedArtifactError::ArtifactTooLarge)
}

pub(super) fn fixed_vec_resident_reservation<T>(
    entries: usize,
) -> Result<u64, PreparedArtifactError> {
    let allocation = entries
        .checked_mul(size_of::<T>())
        .and_then(|bytes| bytes.checked_add(size_of::<Vec<T>>()))
        .ok_or(PreparedArtifactError::ArtifactTooLarge)?;
    u64::try_from(allocation).map_err(|_| PreparedArtifactError::ArtifactTooLarge)
}

pub(super) fn sync_directory_counted(
    path: &Path,
    evidence: &mut ValidationEvidence,
) -> Result<(), PreparedArtifactError> {
    evidence.store_control_operation();
    let directory = File::open(path)?;
    evidence.observe_file_descriptors(2);
    evidence.store_write_operation();
    directory.sync_all()?;
    Ok(())
}

pub(super) fn remove_staging_counted(
    path: &Path,
    evidence: &mut ValidationEvidence,
) -> Result<(), PreparedArtifactError> {
    evidence.store_control_operation();
    match path.symlink_metadata() {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
        Ok(metadata) if metadata.file_type().is_dir() => {
            evidence.store_write_operation();
            fs::remove_dir_all(path)?;
            let parent = path
                .parent()
                .ok_or_else(|| PreparedArtifactError::UnknownCacheEntry(path.to_path_buf()))?;
            sync_directory_counted(parent, evidence)
        }
        Ok(_) => Err(PreparedArtifactError::UnknownCacheEntry(path.to_path_buf())),
    }
}

pub(super) fn map_incomplete(error: io::Error) -> PreparedArtifactError {
    if error.kind() == io::ErrorKind::UnexpectedEof || error.kind() == io::ErrorKind::NotFound {
        PreparedArtifactError::IncompleteArtifact
    } else {
        error.into()
    }
}
