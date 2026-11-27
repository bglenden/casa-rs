// SPDX-License-Identifier: LGPL-3.0-or-later
//! Cache selected ISM scalars across repeated reads of several members.

use std::cell::RefCell;
use std::collections::VecDeque;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use casa_core::ScalarValue;

use super::{ColumnDescContents, DataFile, StorageError};

// A small saved selection can span several multi-MS members. Retain those
// selections together so reading one member no longer evicts the previous
// member's scalars. A large selection keeps the same single entry as before.
const ISM_SELECTED_SCALAR_CACHE_BYTES: usize = 512 * 1024;
const ISM_SELECTED_SCALAR_CACHE_ENTRIES: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Key {
    file_path: PathBuf,
    #[cfg(unix)]
    file_identity: (u64, u64),
    #[cfg(unix)]
    file_changed: (i64, i64),
    file_len: u64,
    file_modified: Option<SystemTime>,
    selected_rows: Vec<usize>,
    column_names: Vec<String>,
    preserve_defaults: bool,
}

#[derive(Debug, Clone)]
struct Entry {
    key: Key,
    values_by_column: Vec<Vec<Option<ScalarValue>>>,
    bytes: usize,
}

thread_local! {
    static ISM_SELECTED_SCALAR_ROWS_CACHE: RefCell<VecDeque<Entry>> =
        const { RefCell::new(VecDeque::new()) };
}

pub(super) fn key(
    file_path: &Path,
    col_descs: &[&ColumnDescContents],
    selected_rows: &[usize],
    preserve_defaults: bool,
) -> Result<Key, StorageError> {
    let metadata = DataFile::metadata(file_path)?;
    Ok(Key {
        file_path: file_path.to_path_buf(),
        #[cfg(unix)]
        file_identity: (metadata.dev(), metadata.ino()),
        #[cfg(unix)]
        file_changed: (metadata.ctime(), metadata.ctime_nsec()),
        file_len: metadata.len(),
        file_modified: metadata.modified().ok(),
        selected_rows: selected_rows.to_vec(),
        preserve_defaults,
        column_names: col_descs
            .iter()
            .map(|col_desc| col_desc.col_name.clone())
            .collect(),
    })
}

pub(super) fn get(
    key: &Key,
    target_col_idx: usize,
    expected_len: usize,
) -> Option<Vec<Option<ScalarValue>>> {
    ISM_SELECTED_SCALAR_ROWS_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = cache.iter().position(|entry| &entry.key == key)?;
        if index != 0 {
            let entry = cache.remove(index).expect("cached key was found");
            cache.push_front(entry);
        }
        let values = cache.front()?.values_by_column.get(target_col_idx)?;
        (values.len() == expected_len).then(|| values.clone())
    })
}

pub(super) fn insert(key: Key, values_by_column: Vec<Vec<Option<ScalarValue>>>) {
    let bytes = values_by_column
        .iter()
        .map(|column| {
            column.capacity() * size_of::<Option<ScalarValue>>()
                + column
                    .iter()
                    .filter_map(|value| match value {
                        Some(ScalarValue::String(text)) => Some(text.capacity()),
                        _ => None,
                    })
                    .sum::<usize>()
        })
        .sum::<usize>()
        + key.selected_rows.capacity() * size_of::<usize>();
    ISM_SELECTED_SCALAR_ROWS_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let mut retained: usize = cache.iter().map(|entry| entry.bytes).sum();
        while cache.len() >= ISM_SELECTED_SCALAR_CACHE_ENTRIES
            || retained.saturating_add(bytes) > ISM_SELECTED_SCALAR_CACHE_BYTES
        {
            let Some(removed) = cache.pop_back() else {
                break;
            };
            retained -= removed.bytes;
        }
        cache.push_front(Entry {
            key,
            values_by_column,
            bytes,
        });
    });
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::fs::{File, FileTimes};
    use std::io::Write;

    #[test]
    fn rereads_a_cached_member_after_a_timestamp_preserving_update() {
        let root = tempfile::tempdir().unwrap();
        let first = root.path().join("first");
        let second = root.path().join("second");
        std::fs::write(&first, [1]).unwrap();
        std::fs::write(&second, [2]).unwrap();
        let cache_key = |path: &Path| key(path, &[], &[0], false).unwrap();
        let saved_first = cache_key(&first);
        let saved_second = cache_key(&second);
        insert(saved_first.clone(), vec![vec![Some(ScalarValue::Int32(1))]]);
        insert(
            saved_second.clone(),
            vec![vec![Some(ScalarValue::Int32(2))]],
        );
        assert_eq!(
            get(&saved_first, 0, 1),
            Some(vec![Some(ScalarValue::Int32(1))])
        );
        std::thread::sleep(std::time::Duration::from_millis(2));
        let mut file = File::options().write(true).open(&first).unwrap();
        file.write_all(&[3]).unwrap();
        file.set_times(FileTimes::new().set_modified(saved_first.file_modified.unwrap()))
            .unwrap();
        let current_first = cache_key(&first);
        assert_eq!(saved_first.file_modified, current_first.file_modified);
        assert_eq!(saved_first.file_len, current_first.file_len);
        assert!(get(&current_first, 0, 1).is_none());
        assert_eq!(
            get(&cache_key(&second), 0, 1),
            Some(vec![Some(ScalarValue::Int32(2))])
        );
    }
}
