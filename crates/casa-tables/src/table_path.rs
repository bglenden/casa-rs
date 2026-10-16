// SPDX-License-Identifier: LGPL-3.0-or-later
//! How one table stores the path of another table it refers to.
//!
//! A casacore table refers to other tables in several places: a table keyword
//! (`TpTable` field of a `TableRecord`, such as a MeasurementSet subtable or an
//! image mask region's `mask` field), the parent of a `RefTable`, the parts of
//! a `ConcatTable`, and the table a `ForwardColumnEngine` forwards to. In every
//! case casacore stores the referenced path relative to the *owner*, the table
//! that holds the reference, so a table tree can be moved or copied as a unit.
//!
//! [`strip_directory`] ports casacore's `Path::stripDirectory` (writing) and
//! [`add_directory`] ports `Path::addDirectory` (reading). The stored forms are:
//!
//! | Stored form | Meaning | Example (owner `/data/obs.ms`) |
//! |---|---|---|
//! | `././<rest>` | inside the owner | `././ANTENNA` → `/data/obs.ms/ANTENNA` |
//! | `./<rest>` | under the owner's directory | `./cal.tbl` → `/data/cal.tbl` |
//! | `<rest>/.` | an ancestor of the owner | `obs.ms/.` → `/data` (owner `/data/obs.ms`) |
//! | `/abs/path` | absolute, no common directory | unchanged |
//!
//! casa-rs keeps a table keyword's in-memory value in its stored form, so a
//! caller resolves it with [`add_directory`] against the table's current path.

use std::path::{Component, Path, PathBuf};

/// Returns the path of `table` as `owner` stores a reference to it.
///
/// Port of casacore's `Path::stripDirectory(name, otherName)`. Both paths are
/// made absolute against the working directory and normalised lexically
/// (`.` dropped, `..` removed with its predecessor, symlinks left alone), as
/// casacore's `Path::absoluteName` does. Then the first matching rule wins:
///
/// 1. `table` inside `owner`: `././<rest>`.
/// 2. `table` an ancestor of `owner`: `<rest of owner>/.`.
/// 3. `table` under `owner`'s directory: `./<rest>`.
/// 4. otherwise the absolute path of `table`.
///
/// casacore's last rule returns the name as given rather than absolute; the
/// two agree whenever casacore applies the rule to a table keyword, whose name
/// it always holds in absolute form. casacore also expands `$VAR` and `~`
/// before matching; casa-rs paths are taken literally.
///
/// # Examples
///
/// ```
/// use casa_tables::strip_directory;
/// use std::path::Path;
///
/// let owner = Path::new("/data/obs.ms");
/// assert_eq!(strip_directory(Path::new("/data/obs.ms/ANTENNA"), owner), "././ANTENNA");
/// assert_eq!(strip_directory(Path::new("/data/cal.tbl"), owner), "./cal.tbl");
/// assert_eq!(strip_directory(Path::new("/data"), owner), "obs.ms/.");
/// assert_eq!(strip_directory(Path::new("/elsewhere/x"), owner), "/elsewhere/x");
/// ```
pub fn strip_directory(table: &Path, owner: &Path) -> String {
    let table = lexical_absolute(table);
    let owner = lexical_absolute(owner);
    if let Some(rest) = proper_suffix(&table, &owner) {
        return format!("././{rest}");
    }
    if let Some(rest) = proper_suffix(&owner, &table) {
        return format!("{rest}/.");
    }
    if let Some(rest) = owner.parent().and_then(|dir| proper_suffix(&table, dir)) {
        return format!("./{rest}");
    }
    table.to_string_lossy().into_owned()
}

/// Returns the table path that `owner` refers to by the stored path `stored`.
///
/// Port of casacore's `Path::addDirectory(name, otherName)`, the inverse of
/// [`strip_directory`]: leading `./` components are removed, and
///
/// - one removed: `stored` is under `owner`'s directory;
/// - two or more removed: `stored` is inside `owner`;
/// - none removed, `stored` ending in `/.` and `owner` ending in the part
///   before it: the ancestor of `owner` above that part;
/// - an absolute `stored` is returned unchanged.
///
/// A plain relative name (none of the forms above) diverges from casacore,
/// which leaves it relative to the working directory, where it does not name
/// the intended table. casa-rs builds before #715 wrote subtable keywords that
/// way (an image's `logtable`, MeasurementSet subtables), meaning a table
/// inside `owner`, so such a name resolves inside `owner`.
///
/// # Examples
///
/// ```
/// use casa_tables::add_directory;
/// use std::path::{Path, PathBuf};
///
/// let owner = Path::new("/moved/obs.ms");
/// assert_eq!(add_directory("././ANTENNA", owner), PathBuf::from("/moved/obs.ms/ANTENNA"));
/// assert_eq!(add_directory("./cal.tbl", owner), PathBuf::from("/moved/cal.tbl"));
/// assert_eq!(add_directory("obs.ms/.", owner), PathBuf::from("/moved"));
/// assert_eq!(add_directory("/elsewhere/x", owner), PathBuf::from("/elsewhere/x"));
/// ```
pub fn add_directory(stored: &str, owner: &Path) -> PathBuf {
    let mut name = stored;
    let mut removed = 0usize;
    while let Some(rest) = name.strip_prefix("./") {
        name = rest;
        removed += 1;
    }
    match removed {
        0 => {}
        1 => return owner.parent().unwrap_or(owner).join(name),
        _ => return owner.join(name),
    }
    if let Some(ancestor) = name
        .strip_suffix("/.")
        .and_then(|below| ancestor_above(owner, Path::new(below)))
    {
        return ancestor;
    }
    owner.join(name)
}

/// Absolute form of `path` with `.` and `..` resolved lexically, like
/// casacore's `Path::absoluteName`.
fn lexical_absolute(path: &Path) -> PathBuf {
    let absolute = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    let mut normal = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normal.pop();
            }
            other => normal.push(other.as_os_str()),
        }
    }
    normal
}

/// The non-empty remainder of `path` below `base`, if `path` lies under it.
fn proper_suffix(path: &Path, base: &Path) -> Option<String> {
    let rest = path.strip_prefix(base).ok()?;
    (!rest.as_os_str().is_empty()).then(|| rest.to_string_lossy().into_owned())
}

/// `owner` with the trailing components `below` removed, if it ends in them
/// and something remains above.
fn ancestor_above(owner: &Path, below: &Path) -> Option<PathBuf> {
    if below.as_os_str().is_empty() || below.is_absolute() || !owner.ends_with(below) {
        return None;
    }
    let mut ancestor = owner.to_path_buf();
    for _ in below.components() {
        ancestor.pop();
    }
    (!ancestor.as_os_str().is_empty()).then_some(ancestor)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cwd() -> PathBuf {
        std::env::current_dir().unwrap()
    }

    /// The `stripDirectory` cases of casacore `casa/OS/test/tPath.cc`.
    #[test]
    fn strip_directory_matches_casacore_tpath() {
        let strip = |name: &str, other: &str| strip_directory(Path::new(name), Path::new(other));
        assert_eq!(strip("././abc", "././././abc/de"), "de/.");
        assert_eq!(strip("abc", "abc/de"), "de/.");
        assert_eq!(strip("././abc/de/ef", "././././abc/de"), "././ef");
        assert_eq!(strip("abc/ef", "abc/de"), "./ef");
        assert_eq!(strip("ef", "de"), "./ef");
        assert_eq!(strip("/de/abc/ab/ef", "/de/abc/de"), "./ab/ef");
        assert_eq!(strip("/de/abc/ab/ef", "/de/abc/ab"), "././ef");
        // casacore returns the unmatched name as given ("abc"); casa-rs
        // returns it absolute, naming the same table.
        assert_eq!(
            strip("././abc", "././././abd/de"),
            cwd().join("abc").to_string_lossy()
        );
    }

    /// The `addDirectory` cases of casacore `casa/OS/test/tPath.cc`.
    #[test]
    fn add_directory_matches_casacore_tpath() {
        let owner = Path::new("/ghi/a");
        assert_eq!(add_directory("./de/ab", owner), PathBuf::from("/ghi/de/ab"));
        assert_eq!(
            add_directory("././de/ab", owner),
            PathBuf::from("/ghi/a/de/ab")
        );
        // casacore leaves a plain relative name relative to the working
        // directory ("de/ab"); casa-rs resolves it inside the owner.
        assert_eq!(add_directory("de/ab", owner), PathBuf::from("/ghi/a/de/ab"));
    }

    #[test]
    fn strip_directory_normalises_dots_like_casacore() {
        let owner = Path::new("/data/x/../obs.ms/.");
        assert_eq!(
            strip_directory(Path::new("/data/obs.ms/./ANTENNA"), owner),
            "././ANTENNA"
        );
        // A component prefix, not a string prefix.
        assert_eq!(
            strip_directory(Path::new("/data/obs.ms2/A"), Path::new("/data/obs.ms")),
            "./obs.ms2/A"
        );
        // A table referring to itself is stored by name in its directory.
        assert_eq!(
            strip_directory(Path::new("/data/obs.ms"), Path::new("/data/obs.ms")),
            "./obs.ms"
        );
    }

    #[test]
    fn add_directory_ancestor_form_needs_a_matching_owner_tail() {
        assert_eq!(
            add_directory("b/c/.", Path::new("/root/a/b/c")),
            PathBuf::from("/root/a")
        );
        // The owner does not end in `b/c`: no ancestor to return.
        assert_eq!(
            add_directory("b/c/.", Path::new("/root/a/x/c")),
            PathBuf::from("/root/a/x/c/b/c/.")
        );
    }

    #[test]
    fn stored_forms_resolve_against_a_moved_owner() {
        let owner = Path::new("/old/run/image.tt0");
        let moved = Path::new("/new/disk/deeper/image.tt0");
        let cases = [
            (
                "/old/run/image.tt0/mask0",
                "/new/disk/deeper/image.tt0/mask0",
            ),
            ("/old/run/image.psf", "/new/disk/deeper/image.psf"),
            (
                "/old/run/products/image.pb",
                "/new/disk/deeper/products/image.pb",
            ),
            ("/old/run", "/new/disk/deeper"),
            ("/elsewhere/table", "/elsewhere/table"),
        ];
        for (table, expected) in cases {
            let stored = strip_directory(Path::new(table), owner);
            assert_eq!(add_directory(&stored, owner), PathBuf::from(table));
            assert_eq!(add_directory(&stored, moved), PathBuf::from(expected));
        }
    }

    #[test]
    fn relative_paths_are_made_absolute_on_both_sides() {
        assert_eq!(
            strip_directory(Path::new("img.image/mask0"), Path::new("img.image")),
            "././mask0"
        );
        assert_eq!(
            strip_directory(&cwd().join("img.image/mask0"), Path::new("./img.image")),
            "././mask0"
        );
    }
}
