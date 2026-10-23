// SPDX-License-Identifier: LGPL-3.0-or-later
//! How a native AW cache directory stands against a request (#735): the
//! request manifest generation writes beside its cells, the cells the
//! request names, and any cells it does not.
//!
//! A native cell's content depends on the whole request, not only on its
//! own (PA, frequency, w, Mueller) key. The conjugate beam is the request's
//! listed frequency nearest `√(2f₀² − f²)` (`AWConvFunc::makeConvFunction2`),
//! and with `conjbeams` the catalog looks each row up at the nearest listed
//! frequency (`CFBuffer::initMaps`). Cells are named by index, so a
//! directory generated for another SPW selection can name the same cells
//! and hold different ones. The manifest records every input that enters a
//! cell. A directory serves a request only when its manifest is the
//! request's and it holds exactly the request's cells.

use std::fmt::Write as _;
use std::fs::{File, TryLockError};
use std::path::Path;

use casa_imaging_model::NativeAwRequestInput;

use super::{AwCatalog, AwCatalogError, IMAGING_PREFIX, WEIGHT_PREFIX, cache_error};

/// The request manifest native generation writes beside its cells, last,
/// once every cell is in place.
const MANIFEST: &str = "CasaRsNativeRequest.txt";

/// The lock file runs share while they read the cache and a generation
/// holds alone while it writes ([`NativeCacheLock`]).
const LOCK: &str = "CasaRsNativeCache.lock";

/// The prefix of a cell image being written:
/// [`super::cell_image::write_cell`] builds the image under it and renames
/// it to its cell name only when complete, so a cell name always names a
/// complete image (#739 review).
pub(super) const STAGING_PREFIX: &str = ".staging-";

/// An advisory lock on a native cache directory, held until dropped
/// (#739 review). A run holds it shared for as long as its catalog reads
/// cells, which load lazily. Generation holds it exclusive while it clears
/// and writes. So a cache is never replaced under a run that reads it, and
/// a run never opens one that is being written. Both are taken without
/// waiting: the conflicting run's error names the directory.
#[derive(Debug)]
pub struct NativeCacheLock {
    _file: File,
}

impl NativeCacheLock {
    /// The shared lock a reading run holds on the cache at `root`, created
    /// if missing.
    ///
    /// # Errors
    ///
    /// The directory or its lock file cannot be created, or another run is
    /// writing the cache.
    pub fn shared(root: impl AsRef<Path>) -> Result<Self, AwCatalogError> {
        let root = root.as_ref();
        let file = Self::open(root)?;
        match file.try_lock_shared() {
            Ok(()) => Ok(Self { _file: file }),
            Err(TryLockError::WouldBlock) => Err(cache_error(
                root,
                "another run is generating this native AW cache; wait for it or use another \
                 native_cf_cache",
            )),
            Err(TryLockError::Error(error)) => {
                Err(cache_error(root, format!("cannot lock the cache: {error}")))
            }
        }
    }

    /// The exclusive lock generation holds on the cache at `root`, created
    /// if missing.
    ///
    /// # Errors
    ///
    /// The directory or its lock file cannot be created, or another run is
    /// reading or writing the cache.
    pub fn exclusive(root: impl AsRef<Path>) -> Result<Self, AwCatalogError> {
        let root = root.as_ref();
        let file = Self::open(root)?;
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file }),
            Err(TryLockError::WouldBlock) => Err(cache_error(
                root,
                "another run is using this native AW cache, so it cannot be regenerated for this \
                 request now; wait for that run or use another native_cf_cache",
            )),
            Err(TryLockError::Error(error)) => {
                Err(cache_error(root, format!("cannot lock the cache: {error}")))
            }
        }
    }

    fn open(root: &Path) -> Result<File, AwCatalogError> {
        std::fs::create_dir_all(root)
            .map_err(|error| cache_error(root, format!("cannot create: {error}")))?;
        let path = root.join(LOCK);
        File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .map_err(|error| cache_error(&path, format!("cannot open the lock file: {error}")))
    }
}

/// The suffix a cell's `CFS_` and `WTCFS_` images share,
/// `<pa>_0_CF_<frequency>_<w>_<mueller>.im` (one baseline type).
pub(super) fn cell_suffix(pa: usize, frequency: usize, w: usize, mueller: usize) -> String {
    format!("{pa}_0_CF_{frequency}_{w}_{mueller}.im")
}

/// The number of cells `input` names: one per (parallactic angle,
/// frequency, w plane, Mueller element).
pub(super) const fn request_cells(input: &NativeAwRequestInput) -> usize {
    input.pa_values.len()
        * input.frequencies.len()
        * input.w_values.len()
        * input.mueller_elements.len()
}

/// Heap bytes [`AwCatalog::native_cache_state`] holds for `input`'s cells
/// beside the manifest: a bit per cell for each of its two images.
pub(super) const fn cache_state_bytes(input: &NativeAwRequestInput) -> usize {
    2 * request_cells(input).div_ceil(64) * size_of::<u64>()
}

/// The position, in the order (parallactic angle, frequency, w plane,
/// Mueller element), of the cell of `input` whose suffix is `suffix`, if it
/// names one: exactly the name [`cell_suffix`] gives it.
fn request_cell(input: &NativeAwRequestInput, suffix: &str) -> Option<usize> {
    let (pa, rest) = suffix.strip_suffix(".im")?.split_once("_0_CF_")?;
    let mut fields = rest.split('_');
    let index =
        |field: Option<&str>, len: usize| field?.parse::<usize>().ok().filter(|&index| index < len);
    let pa = index(Some(pa), input.pa_values.len())?;
    let frequency = index(fields.next(), input.frequencies.len())?;
    let w = index(fields.next(), input.w_values.len())?;
    let mueller = index(fields.next(), input.mueller_elements.len())?;
    (cell_suffix(pa, frequency, w, mueller) == suffix).then(|| {
        ((pa * input.frequencies.len() + frequency) * input.w_values.len() + w)
            * input.mueller_elements.len()
            + mueller
    })
}

/// Heap bytes reading and comparing `input`'s manifest holds at most: the
/// file's text (this manifest, in a directory of this request), and the
/// manifest being built, at most twice its length while it grows and three
/// times as it moves, and the frequencies it lists. The text is at most
/// 1 KiB of labels and scalars, 26 bytes for each value listed in `Debug`
/// form (at most 24 characters and a separator) and 12 for each Mueller
/// element.
pub(super) const fn manifest_bytes(input: &NativeAwRequestInput) -> usize {
    let values = input.frequencies.len() + input.w_values.len() + input.pa_values.len();
    let text = 1024 + 26 * values + 12 * input.mueller_elements.len();
    4 * text + input.frequencies.len() * size_of::<f64>()
}

/// The manifest of `input`: one line per input that enters a cell, values
/// in Rust's round-trip `Debug` form, so equal text means equal values
/// (signed zeros differ, which can only cost a needless regeneration). The
/// dish surface is recorded by its sample count and a 64-bit FNV-1a hash of
/// its samples' bits, which identifies it with overwhelming but not
/// certain probability. The channel frequencies and SPW identifiers of a
/// frequency group route rows but do not enter its cell, and the cell bound
/// only limits the request, so neither is recorded. A change to how cells
/// are computed must bump the version line, which invalidates every
/// existing cache.
fn manifest(input: &NativeAwRequestInput) -> String {
    let mut text = String::from("casa-rs native AW request, version 1\n");
    let samples = input.surface.samples();
    let surface_hash = samples
        .iter()
        .flatten()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, value| {
            value
                .to_bits()
                .to_le_bytes()
                .iter()
                .fold(hash, |hash, byte| {
                    (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
                })
        });
    let cf_frequencies = input
        .frequencies
        .iter()
        .map(|group| group.cf_frequency_hz)
        .collect::<Vec<_>>();
    let grid = input.grid;
    let terms = input.terms;
    // Writing to a String cannot fail.
    let _ = writeln!(
        text,
        "surface_samples {} fnv1a64 {surface_hash:016x}",
        samples.len()
    );
    let _ = writeln!(text, "antenna_diameter_m {:?}", input.antenna_diameter_m);
    let _ = writeln!(
        text,
        "reference_frequency_hz {:?}",
        input.reference_frequency_hz
    );
    let _ = writeln!(text, "cf_frequencies_hz {cf_frequencies:?}");
    let _ = writeln!(text, "w_values {:?}", input.w_values);
    let _ = writeln!(text, "w_increment {:?}", input.w_increment);
    let _ = writeln!(text, "pa_values_rad {:?}", input.pa_values);
    let _ = writeln!(text, "mueller_elements {:?}", input.mueller_elements);
    let _ = writeln!(
        text,
        "grid size {} sky_increment_rad {:?} oversampling {}",
        grid.size, grid.sky_increment_rad, grid.oversampling
    );
    let _ = writeln!(
        text,
        "terms aperture {} w_term {} prolate_spheroidal {} wideband {} conjugate_beams {}",
        terms.aperture,
        terms.w_term,
        terms.prolate_spheroidal,
        terms.wideband,
        terms.conjugate_beams
    );
    text
}

/// Remove the manifest from `root`, so the directory claims no request
/// while its cells change (#739 review).
pub(super) fn remove_manifest(root: &Path) -> Result<(), AwCatalogError> {
    let path = root.join(MANIFEST);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(cache_error(&path, format!("cannot remove: {error}"))),
    }
}

/// Write `input`'s manifest into `root`.
pub(super) fn write_manifest(
    root: &Path,
    input: &NativeAwRequestInput,
) -> Result<(), AwCatalogError> {
    let path = root.join(MANIFEST);
    std::fs::write(&path, manifest(input)).map_err(|error| AwCatalogError::Write {
        path,
        detail: format!("cannot write the request manifest: {error}"),
    })
}

/// Whether a native cache directory records the request it was generated
/// for ([`NativeCacheState::manifest`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeManifest {
    /// No manifest: the directory predates manifests or was not written by
    /// native generation.
    Absent,
    /// The manifest records another request.
    Differs,
    /// The manifest records this request.
    Matches,
}

/// How a native cache directory stands against one request
/// ([`AwCatalog::native_cache_state`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeCacheState {
    /// Whether the directory was generated for this request.
    pub manifest: NativeManifest,
    /// Cells the request names whose `CFS_` and `WTCFS_` images are both
    /// present.
    pub present: usize,
    /// Cells the request names.
    pub expected: usize,
    /// `CFS_`/`WTCFS_` images the request does not name.
    pub foreign: usize,
}

impl NativeCacheState {
    /// Generated for this request, complete, and holding nothing else: the
    /// catalog may serve the directory as it is.
    #[must_use]
    pub fn reusable(&self) -> bool {
        self.manifest == NativeManifest::Matches
            && self.present == self.expected
            && self.foreign == 0
    }

    /// Missing cells may be generated beside the present ones: the
    /// directory holds nothing of another request. It was generated for
    /// this request, or it holds no cell at all.
    #[must_use]
    pub fn fillable(&self) -> bool {
        self.foreign == 0
            && match self.manifest {
                NativeManifest::Matches => true,
                NativeManifest::Absent => self.present == 0,
                NativeManifest::Differs => false,
            }
    }
}

impl std::fmt::Display for NativeCacheState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let manifest = match self.manifest {
            NativeManifest::Absent => "no request manifest",
            NativeManifest::Differs => "generated for another request",
            NativeManifest::Matches => "generated for this request",
        };
        write!(
            formatter,
            "{manifest}, {} of the {} cells the request names, {} cell images it does not name",
            self.present, self.expected, self.foreign
        )
    }
}

impl AwCatalog {
    /// How the native cache at `root` stands against `input`: whether its
    /// manifest records `input`, how many of `input`'s cells it holds, and
    /// how many cell images it holds that `input` does not name. A missing
    /// directory holds nothing.
    ///
    /// # Errors
    ///
    /// The directory or its manifest cannot be read.
    pub fn native_cache_state(
        root: impl AsRef<Path>,
        input: &NativeAwRequestInput,
    ) -> Result<NativeCacheState, AwCatalogError> {
        let root = root.as_ref();
        let expected = request_cells(input);
        if !root.is_dir() {
            return Ok(NativeCacheState {
                manifest: NativeManifest::Absent,
                present: 0,
                expected,
                foreign: 0,
            });
        }
        let manifest_path = root.join(MANIFEST);
        let recorded = match std::fs::read_to_string(&manifest_path) {
            Ok(text) if text == manifest(input) => NativeManifest::Matches,
            Ok(_) => NativeManifest::Differs,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => NativeManifest::Absent,
            Err(error) => {
                return Err(cache_error(
                    &manifest_path,
                    format!("cannot read the request manifest: {error}"),
                ));
            }
        };
        let words = expected.div_ceil(64);
        let (mut imaging, mut weight) = (vec![0_u64; words], vec![0_u64; words]);
        let mut foreign = 0;
        let entries = std::fs::read_dir(root)
            .map_err(|error| cache_error(root, format!("cannot list: {error}")))?;
        for entry in entries {
            let entry =
                entry.map_err(|error| cache_error(root, format!("cannot list: {error}")))?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let (held, rest) = if let Some(rest) = name.strip_prefix(WEIGHT_PREFIX) {
                (&mut weight, rest)
            } else if let Some(rest) = name.strip_prefix(IMAGING_PREFIX) {
                (&mut imaging, rest)
            } else {
                continue;
            };
            match request_cell(input, rest) {
                Some(cell) if entry.path().is_dir() => held[cell / 64] |= 1 << (cell % 64),
                _ => foreign += 1,
            }
        }
        let present = imaging
            .iter()
            .zip(&weight)
            .map(|(imaging, weight)| (imaging & weight).count_ones() as usize)
            .sum();
        Ok(NativeCacheState {
            manifest: recorded,
            present,
            expected,
            foreign,
        })
    }

    /// Remove the request manifest, then every `CFS_` and `WTCFS_` cell and
    /// any cell image left half written under `root`, so a regenerated
    /// catalog carries no member of an earlier request. The manifest goes
    /// first, so a clear that stops part way leaves a directory that claims
    /// no request. The lock file stays. The caller holds the exclusive
    /// [`NativeCacheLock`].
    ///
    /// # Errors
    ///
    /// The directory cannot be listed or an entry cannot be removed.
    pub fn clear_native(root: impl AsRef<Path>) -> Result<(), AwCatalogError> {
        let root = root.as_ref();
        if !root.is_dir() {
            return Ok(());
        }
        remove_manifest(root)?;
        let entries = std::fs::read_dir(root)
            .map_err(|error| cache_error(root, format!("cannot list: {error}")))?;
        for entry in entries {
            let entry =
                entry.map_err(|error| cache_error(root, format!("cannot list: {error}")))?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if name.starts_with(IMAGING_PREFIX)
                || name.starts_with(WEIGHT_PREFIX)
                || name.starts_with(STAGING_PREFIX)
            {
                std::fs::remove_dir_all(entry.path()).map_err(|error| {
                    cache_error(entry.path(), format!("cannot remove: {error}"))
                })?;
            }
        }
        Ok(())
    }
}
