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

use std::collections::HashSet;
use std::fmt::Write as _;
use std::path::Path;

use casa_imaging_model::NativeAwRequestInput;

use super::{AwCatalog, AwCatalogError, IMAGING_PREFIX, WEIGHT_PREFIX, cache_error};

/// The request manifest native generation writes beside its cells.
const MANIFEST: &str = "CasaRsNativeRequest.txt";

/// The suffix a cell's `CFS_` and `WTCFS_` images share,
/// `<pa>_0_CF_<frequency>_<w>_<mueller>.im` (one baseline type).
pub(super) fn cell_suffix(pa: usize, frequency: usize, w: usize, mueller: usize) -> String {
    format!("{pa}_0_CF_{frequency}_{w}_{mueller}.im")
}

/// Every cell suffix `input` names.
fn request_suffixes(input: &NativeAwRequestInput) -> HashSet<String> {
    let mut suffixes = HashSet::new();
    for pa in 0..input.pa_values.len() {
        for frequency in 0..input.frequencies.len() {
            for w in 0..input.w_values.len() {
                for mueller in 0..input.mueller_elements.len() {
                    suffixes.insert(cell_suffix(pa, frequency, w, mueller));
                }
            }
        }
    }
    suffixes
}

/// The manifest of `input`: one line per input that enters a cell, values
/// in Rust's round-trip `Debug` form so equal text means equal bits.
/// The channel frequencies and SPW identifiers of a frequency group route
/// rows but do not enter its cell, and the cell bound only limits the
/// request, so neither is recorded.
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
        let names = request_suffixes(input);
        let expected = names.len();
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
        let mut imaging = HashSet::new();
        let mut weight = HashSet::new();
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
            if names.contains(rest) && entry.path().is_dir() {
                held.insert(rest.to_string());
            } else {
                foreign += 1;
            }
        }
        let present = names
            .iter()
            .filter(|suffix| imaging.contains(*suffix) && weight.contains(*suffix))
            .count();
        Ok(NativeCacheState {
            manifest: recorded,
            present,
            expected,
            foreign,
        })
    }

    /// Remove every `CFS_` and `WTCFS_` cell and the request manifest under
    /// `root`, so a regenerated catalog carries no member of an earlier
    /// request.
    ///
    /// # Errors
    ///
    /// The directory cannot be listed or an entry cannot be removed.
    pub fn clear_native(root: impl AsRef<Path>) -> Result<(), AwCatalogError> {
        let root = root.as_ref();
        if !root.is_dir() {
            return Ok(());
        }
        let entries = std::fs::read_dir(root)
            .map_err(|error| cache_error(root, format!("cannot list: {error}")))?;
        for entry in entries {
            let entry =
                entry.map_err(|error| cache_error(root, format!("cannot list: {error}")))?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if name.starts_with(IMAGING_PREFIX) || name.starts_with(WEIGHT_PREFIX) {
                std::fs::remove_dir_all(entry.path()).map_err(|error| {
                    cache_error(entry.path(), format!("cannot remove: {error}"))
                })?;
            } else if name == MANIFEST {
                std::fs::remove_file(entry.path()).map_err(|error| {
                    cache_error(entry.path(), format!("cannot remove: {error}"))
                })?;
            }
        }
        Ok(())
    }
}
