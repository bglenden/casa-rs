// SPDX-License-Identifier: LGPL-3.0-or-later
//! Production-inert exact decision-domain response-cache algebra.
//!
//! The experimental VLASS discriminator keeps the existing multiscale MT-MFS
//! selector authoritative. It replaces only repeated exact major-cycle replay
//! with a linear cache over the values that the selector reads.

use std::collections::BTreeMap;
#[cfg(all(target_os = "macos", not(coverage)))]
use std::fs::{self, OpenOptions};
#[cfg(all(target_os = "macos", not(coverage)))]
use std::io::Write;
#[cfg(all(target_os = "macos", not(coverage)))]
use std::path::{Path, PathBuf};

use rayon::prelude::*;
#[cfg(all(target_os = "macos", not(coverage)))]
use sha2::{Digest, Sha256};

use crate::ImagingError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DecisionAtom {
    pub(crate) scale_index: usize,
    pub(crate) position: (usize, usize),
}

#[derive(Debug, Clone)]
pub(crate) struct DecisionSupport {
    pub(crate) atoms: Vec<DecisionAtom>,
    pub(crate) incumbent_coefficients: Vec<Vec<f32>>,
    pub(crate) trace_components: usize,
    #[cfg(all(target_os = "macos", not(coverage)))]
    pub(crate) receipt_sha256: String,
}

#[derive(Debug)]
pub(crate) struct DecisionTrajectory {
    atom_indices: BTreeMap<DecisionAtom, usize>,
    coefficients: Vec<Vec<f32>>,
    first_unsupported_atom: Option<DecisionAtom>,
}

impl DecisionTrajectory {
    pub(crate) fn new(support: &DecisionSupport, nterms: usize) -> Self {
        let atom_indices = support
            .atoms
            .iter()
            .copied()
            .enumerate()
            .map(|(index, atom)| (atom, index))
            .collect();
        Self {
            atom_indices,
            coefficients: vec![vec![0.0; nterms]; support.atoms.len()],
            first_unsupported_atom: None,
        }
    }

    pub(crate) fn record_update(&mut self, atom: DecisionAtom, applied: &[f32]) {
        let Some(&atom_index) = self.atom_indices.get(&atom) else {
            self.first_unsupported_atom.get_or_insert(atom);
            return;
        };
        let coefficients = &mut self.coefficients[atom_index];
        if coefficients.len() != applied.len() {
            self.first_unsupported_atom.get_or_insert(atom);
            return;
        }
        for (coefficient, delta) in coefficients.iter_mut().zip(applied) {
            *coefficient += *delta;
        }
    }

    pub(crate) fn supports(&self, atom: DecisionAtom) -> bool {
        self.atom_indices.contains_key(&atom)
    }

    pub(crate) fn record_unsupported(&mut self, atom: DecisionAtom) {
        self.first_unsupported_atom.get_or_insert(atom);
    }

    pub(crate) fn extend_support(
        &mut self,
        atom: DecisionAtom,
        nterms: usize,
    ) -> Result<(), ImagingError> {
        if self.supports(atom) || nterms == 0 {
            return Err(invalid(
                "exact decision trajectory support extension is duplicate or has no Taylor terms",
            ));
        }
        let atom_index = self.coefficients.len();
        self.atom_indices.insert(atom, atom_index);
        self.coefficients.push(vec![0.0; nterms]);
        self.first_unsupported_atom = None;
        Ok(())
    }

    pub(crate) fn coefficients(&self) -> &[Vec<f32>] {
        &self.coefficients
    }

    pub(crate) fn first_unsupported_atom(&self) -> Option<DecisionAtom> {
        self.first_unsupported_atom
    }
}

#[derive(Debug)]
pub(crate) struct ExactDecisionResponseCache {
    support: DecisionSupport,
    base: Vec<f32>,
    // Atom-major, then input Taylor term.
    columns: Vec<Vec<f32>>,
}

impl ExactDecisionResponseCache {
    pub(crate) fn new(
        support: DecisionSupport,
        nterms: usize,
        base: Vec<f32>,
        columns: Vec<Vec<f32>>,
    ) -> Result<Self, ImagingError> {
        if base.is_empty() {
            return Err(invalid(
                "exact decision response cache requires a non-empty decision vector",
            ));
        }
        if nterms == 0
            || support.incumbent_coefficients.len() != support.atoms.len()
            || support
                .incumbent_coefficients
                .iter()
                .any(|coefficients| coefficients.len() != nterms)
        {
            return Err(invalid(
                "exact decision response support has inconsistent Taylor dimensions",
            ));
        }
        let expected_columns = support.atoms.len().saturating_mul(nterms);
        if columns.len() != expected_columns
            || columns.iter().any(|column| column.len() != base.len())
        {
            return Err(invalid(format!(
                "exact decision response cache has {} columns of inconsistent length; expected {expected_columns} columns of length {}",
                columns.len(),
                base.len()
            )));
        }
        if base.iter().any(|value| !value.is_finite())
            || columns.iter().flatten().any(|value| !value.is_finite())
        {
            return Err(invalid(
                "exact decision response cache contains a non-finite value",
            ));
        }
        Ok(Self {
            support,
            base,
            columns,
        })
    }

    pub(crate) fn support(&self) -> &DecisionSupport {
        &self.support
    }

    pub(crate) fn decision_values(&self) -> usize {
        self.base.len()
    }

    pub(crate) fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub(crate) fn storage_bytes(&self) -> usize {
        self.base
            .len()
            .saturating_add(self.columns.iter().map(Vec::len).sum())
            .saturating_mul(std::mem::size_of::<f32>())
    }

    pub(crate) fn base(&self) -> &[f32] {
        &self.base
    }

    pub(crate) fn extend_support(
        &mut self,
        atom: DecisionAtom,
        nterms: usize,
        columns: Vec<Vec<f32>>,
    ) -> Result<(), ImagingError> {
        if self.support.atoms.contains(&atom)
            || nterms == 0
            || columns.len() != nterms
            || columns.iter().any(|column| column.len() != self.base.len())
            || columns.iter().flatten().any(|value| !value.is_finite())
        {
            return Err(invalid(
                "exact decision response adaptive support extension has an invalid atom or shape",
            ));
        }
        self.support.atoms.push(atom);
        self.support.incumbent_coefficients.push(vec![0.0; nterms]);
        self.columns.extend(columns);
        Ok(())
    }

    pub(crate) fn synthesize(&self, coefficients: &[Vec<f32>]) -> Result<Vec<f32>, ImagingError> {
        if coefficients.len() != self.support.atoms.len() {
            return Err(invalid(
                "exact decision response coefficient atom count changed",
            ));
        }
        let nterms = self
            .support
            .incumbent_coefficients
            .first()
            .map(Vec::len)
            .ok_or_else(|| invalid("exact decision response support is empty"))?;
        if coefficients
            .iter()
            .any(|atom_coefficients| atom_coefficients.len() != nterms)
        {
            return Err(invalid(
                "exact decision response coefficient Taylor dimensions changed",
            ));
        }
        let flattened = coefficients.iter().flatten().copied().collect::<Vec<_>>();
        if flattened.iter().any(|value| !value.is_finite()) {
            return Err(invalid(
                "exact decision response coefficients contain a non-finite value",
            ));
        }
        let mut synthesized = vec![0.0_f32; self.base.len()];
        synthesized
            .par_iter_mut()
            .enumerate()
            .for_each(|(decision_index, output)| {
                let response = self.columns.iter().zip(&flattened).fold(
                    f64::from(self.base[decision_index]),
                    |value, (column, &coefficient)| {
                        value - f64::from(coefficient) * f64::from(column[decision_index])
                    },
                );
                *output = response as f32;
            });
        Ok(synthesized)
    }

    pub(crate) fn synthesize_incumbent(&self) -> Result<Vec<f32>, ImagingError> {
        self.synthesize(&self.support.incumbent_coefficients)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DecisionLinearityMetrics {
    pub(crate) relative_l2: f64,
    pub(crate) normalized_linf: f64,
    pub(crate) max_abs: f64,
}

pub(crate) fn decision_linearity_metrics(
    synthesized: &[f32],
    replayed: &[f32],
) -> Result<DecisionLinearityMetrics, ImagingError> {
    if synthesized.is_empty() || synthesized.len() != replayed.len() {
        return Err(invalid(
            "decision-linearity vectors must be non-empty and equally sized",
        ));
    }
    let mut difference_power = 0.0_f64;
    let mut reference_power = 0.0_f64;
    let mut max_abs = 0.0_f64;
    let mut reference_linf = 0.0_f64;
    for (&synthesized, &replayed) in synthesized.iter().zip(replayed) {
        if !(synthesized.is_finite() && replayed.is_finite()) {
            return Err(invalid(
                "decision-linearity vectors contain a non-finite value",
            ));
        }
        let difference = f64::from(synthesized) - f64::from(replayed);
        difference_power += difference * difference;
        reference_power += f64::from(replayed) * f64::from(replayed);
        max_abs = max_abs.max(difference.abs());
        reference_linf = reference_linf.max(f64::from(replayed).abs());
    }
    Ok(DecisionLinearityMetrics {
        relative_l2: difference_power.sqrt() / reference_power.sqrt().max(f64::MIN_POSITIVE),
        normalized_linf: max_abs / reference_linf.max(f64::MIN_POSITIVE),
        max_abs,
    })
}

#[cfg(all(target_os = "macos", not(coverage)))]
pub(crate) fn load_decision_support(
    path: &Path,
    nterms: usize,
    scale_sizes: &[f32],
    image_shape: [usize; 2],
) -> Result<DecisionSupport, ImagingError> {
    let bytes = fs::read(path).map_err(|error| {
        invalid(format!(
            "read exact decision response support {}: {error}",
            path.display()
        ))
    })?;
    let receipt: serde_json::Value = serde_json::from_slice(&bytes).map_err(|error| {
        invalid(format!(
            "parse exact decision response support {}: {error}",
            path.display()
        ))
    })?;
    let object = receipt
        .as_object()
        .ok_or_else(|| invalid("exact decision response support must be a JSON object"))?;
    if object
        .get("schema_version")
        .and_then(|value| value.as_u64())
        != Some(1)
        || object.get("kind").and_then(|value| value.as_str())
            != Some("casa-rs-vlass-mtmfs-active-support")
        || object.get("nterms").and_then(|value| value.as_u64()) != Some(nterms as u64)
    {
        return Err(invalid(
            "exact decision response support schema, kind, or nterms is incompatible",
        ));
    }
    let trace_components = object
        .get("trace_components")
        .and_then(|value| value.as_u64())
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| invalid("exact decision response support lacks trace_components"))?;
    let atoms_json = object
        .get("atoms")
        .and_then(|value| value.as_array())
        .ok_or_else(|| invalid("exact decision response support lacks atoms"))?;
    if object.get("unique_atoms").and_then(|value| value.as_u64()) != Some(atoms_json.len() as u64)
        || atoms_json.is_empty()
    {
        return Err(invalid(
            "exact decision response support unique_atoms does not match atoms",
        ));
    }
    let mut atoms = Vec::with_capacity(atoms_json.len());
    let mut incumbent_coefficients = Vec::with_capacity(atoms_json.len());
    for atom_json in atoms_json {
        let atom_object = atom_json
            .as_object()
            .ok_or_else(|| invalid("exact decision response atom must be an object"))?;
        let usize_field = |name: &str| {
            atom_object
                .get(name)
                .and_then(|value| value.as_u64())
                .and_then(|value| usize::try_from(value).ok())
                .ok_or_else(|| invalid(format!("exact decision response atom lacks {name}")))
        };
        let scale_index = usize_field("scale_index")?;
        let x = usize_field("x")?;
        let y = usize_field("y")?;
        if scale_index >= scale_sizes.len() || x >= image_shape[0] || y >= image_shape[1] {
            return Err(invalid(format!(
                "exact decision response atom ({scale_index},{x},{y}) escapes the configured scales or image"
            )));
        }
        let scale_pixels = atom_object
            .get("scale_pixels")
            .and_then(|value| value.as_f64())
            .ok_or_else(|| invalid("exact decision response atom lacks scale_pixels"))?
            as f32;
        if scale_pixels.to_bits() != scale_sizes[scale_index].to_bits() {
            return Err(invalid(format!(
                "exact decision response atom scale {scale_index} has {scale_pixels} pixels; expected {}",
                scale_sizes[scale_index]
            )));
        }
        let coefficients = atom_object
            .get("coalesced_term_deltas_f32_sequential")
            .and_then(|value| value.as_array())
            .ok_or_else(|| {
                invalid("exact decision response atom lacks f32 sequential coefficients")
            })?;
        if coefficients.len() != nterms {
            return Err(invalid(
                "exact decision response atom coefficient term count changed",
            ));
        }
        let coefficients = coefficients
            .iter()
            .map(|value| {
                value
                    .as_f64()
                    .map(|value| value as f32)
                    .filter(|value| value.is_finite())
                    .ok_or_else(|| {
                        invalid("exact decision response atom coefficient is non-finite")
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        atoms.push(DecisionAtom {
            scale_index,
            position: (x, y),
        });
        incumbent_coefficients.push(coefficients);
    }
    if atoms.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid(
            "exact decision response atoms must be unique and canonically ordered",
        ));
    }
    Ok(DecisionSupport {
        atoms,
        incumbent_coefficients,
        trace_components,
        receipt_sha256: format!("{:x}", Sha256::digest(&bytes)),
    })
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[derive(Debug)]
pub(crate) struct ExperimentalDecisionCacheStore {
    binary_path: PathBuf,
    metadata_path: PathBuf,
    metadata: serde_json::Value,
    decision_values: usize,
    columns_total: usize,
    columns_complete: usize,
}

#[cfg(all(target_os = "macos", not(coverage)))]
impl ExperimentalDecisionCacheStore {
    pub(crate) fn open(
        path: &Path,
        support: &DecisionSupport,
        nterms: usize,
        base: &[f32],
    ) -> Result<(Self, Vec<Vec<f32>>), ImagingError> {
        if !path.is_absolute() {
            return Err(invalid(
                "experimental decision-response cache path must be absolute",
            ));
        }
        let metadata_path = PathBuf::from(format!("{}.json", path.display()));
        let binary_path = PathBuf::from(format!("{}.f32le", path.display()));
        let columns_total = support.atoms.len().saturating_mul(nterms);
        let base_bytes = f32_bytes(base);
        let base_sha256 = format!("{:x}", Sha256::digest(&base_bytes));
        let expected = serde_json::json!({
            "schema_version": 1,
            "kind": "casa-rs-experimental-exact-decision-response-cache",
            "support_sha256": support.receipt_sha256,
            "nterms": nterms,
            "atoms": support.atoms.len(),
            "decision_values": base.len(),
            "columns_total": columns_total,
            "base_sha256": base_sha256,
        });

        if metadata_path.exists() || binary_path.exists() {
            if !(metadata_path.is_file() && binary_path.is_file()) {
                return Err(invalid(
                    "experimental decision-response cache metadata and binary must both exist",
                ));
            }
            let metadata_bytes = fs::read(&metadata_path).map_err(|error| {
                invalid(format!(
                    "read decision-response cache metadata {}: {error}",
                    metadata_path.display()
                ))
            })?;
            let metadata: serde_json::Value =
                serde_json::from_slice(&metadata_bytes).map_err(|error| {
                    invalid(format!(
                        "parse decision-response cache metadata {}: {error}",
                        metadata_path.display()
                    ))
                })?;
            for key in [
                "schema_version",
                "kind",
                "support_sha256",
                "nterms",
                "atoms",
                "decision_values",
                "columns_total",
                "base_sha256",
            ] {
                if metadata.get(key) != expected.get(key) {
                    return Err(invalid(format!(
                        "experimental decision-response cache metadata field {key} does not match this run"
                    )));
                }
            }
            let columns_complete = metadata
                .get("columns_complete")
                .and_then(serde_json::Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .filter(|value| *value <= columns_total)
                .ok_or_else(|| {
                    invalid("experimental decision-response cache has invalid columns_complete")
                })?;
            let bytes = fs::read(&binary_path).map_err(|error| {
                invalid(format!(
                    "read decision-response cache binary {}: {error}",
                    binary_path.display()
                ))
            })?;
            let values_expected = base
                .len()
                .saturating_mul(columns_complete.saturating_add(1));
            if bytes.len() != values_expected.saturating_mul(std::mem::size_of::<f32>()) {
                return Err(invalid(format!(
                    "experimental decision-response cache binary has {} bytes; expected {}",
                    bytes.len(),
                    values_expected.saturating_mul(std::mem::size_of::<f32>())
                )));
            }
            let values = f32_values(&bytes)?;
            if values[..base.len()]
                .iter()
                .zip(base)
                .any(|(stored, current)| stored.to_bits() != current.to_bits())
            {
                return Err(invalid(
                    "experimental decision-response cache base vector changed",
                ));
            }
            let columns = values[base.len()..]
                .chunks_exact(base.len())
                .map(<[f32]>::to_vec)
                .collect::<Vec<_>>();
            Ok((
                Self {
                    binary_path,
                    metadata_path,
                    metadata,
                    decision_values: base.len(),
                    columns_total,
                    columns_complete,
                },
                columns,
            ))
        } else {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(|error| {
                    invalid(format!(
                        "create decision-response cache directory {}: {error}",
                        parent.display()
                    ))
                })?;
            }
            let mut binary = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&binary_path)
                .map_err(|error| {
                    invalid(format!(
                        "create decision-response cache binary {}: {error}",
                        binary_path.display()
                    ))
                })?;
            binary.write_all(&base_bytes).map_err(|error| {
                invalid(format!(
                    "write decision-response cache base {}: {error}",
                    binary_path.display()
                ))
            })?;
            binary.sync_all().map_err(|error| {
                invalid(format!(
                    "sync decision-response cache base {}: {error}",
                    binary_path.display()
                ))
            })?;
            let mut metadata = expected;
            metadata["columns_complete"] = serde_json::json!(0);
            write_metadata_exclusive(&metadata_path, &metadata)?;
            Ok((
                Self {
                    binary_path,
                    metadata_path,
                    metadata,
                    decision_values: base.len(),
                    columns_total,
                    columns_complete: 0,
                },
                Vec::new(),
            ))
        }
    }

    pub(crate) fn columns_complete(&self) -> usize {
        self.columns_complete
    }

    pub(crate) fn columns_total(&self) -> usize {
        self.columns_total
    }

    pub(crate) fn append_column(&mut self, column: &[f32]) -> Result<(), ImagingError> {
        if column.len() != self.decision_values || self.columns_complete >= self.columns_total {
            return Err(invalid(
                "experimental decision-response cache append has an invalid shape",
            ));
        }
        let mut binary = OpenOptions::new()
            .append(true)
            .open(&self.binary_path)
            .map_err(|error| {
                invalid(format!(
                    "open decision-response cache binary {}: {error}",
                    self.binary_path.display()
                ))
            })?;
        binary.write_all(&f32_bytes(column)).map_err(|error| {
            invalid(format!(
                "append decision-response cache column {}: {error}",
                self.binary_path.display()
            ))
        })?;
        binary.sync_data().map_err(|error| {
            invalid(format!(
                "sync decision-response cache column {}: {error}",
                self.binary_path.display()
            ))
        })?;
        self.columns_complete += 1;
        self.metadata["columns_complete"] = serde_json::json!(self.columns_complete);
        replace_metadata(&self.metadata_path, &self.metadata)
    }
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn f32_bytes(values: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(values.len().saturating_mul(std::mem::size_of::<f32>()));
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn f32_values(bytes: &[u8]) -> Result<Vec<f32>, ImagingError> {
    if bytes.len() % std::mem::size_of::<f32>() != 0 {
        return Err(invalid(
            "experimental decision-response cache binary length is not f32 aligned",
        ));
    }
    Ok(bytes
        .chunks_exact(std::mem::size_of::<f32>())
        .map(|chunk| f32::from_le_bytes(chunk.try_into().expect("four-byte f32 chunk")))
        .collect())
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn write_metadata_exclusive(path: &Path, metadata: &serde_json::Value) -> Result<(), ImagingError> {
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| {
            invalid(format!(
                "create decision-response cache metadata {}: {error}",
                path.display()
            ))
        })?;
    serde_json::to_writer_pretty(&mut output, metadata).map_err(|error| {
        invalid(format!(
            "serialize decision-response cache metadata {}: {error}",
            path.display()
        ))
    })?;
    output.write_all(b"\n").map_err(|error| {
        invalid(format!(
            "finish decision-response cache metadata {}: {error}",
            path.display()
        ))
    })?;
    output.sync_all().map_err(|error| {
        invalid(format!(
            "sync decision-response cache metadata {}: {error}",
            path.display()
        ))
    })
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn replace_metadata(path: &Path, metadata: &serde_json::Value) -> Result<(), ImagingError> {
    let temporary = PathBuf::from(format!("{}.tmp", path.display()));
    if temporary.exists() {
        fs::remove_file(&temporary).map_err(|error| {
            invalid(format!(
                "remove stale decision-response metadata temporary {}: {error}",
                temporary.display()
            ))
        })?;
    }
    write_metadata_exclusive(&temporary, metadata)?;
    fs::rename(&temporary, path).map_err(|error| {
        invalid(format!(
            "replace decision-response cache metadata {}: {error}",
            path.display()
        ))
    })
}

fn invalid(message: impl Into<String>) -> ImagingError {
    ImagingError::InvalidRequest(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn support() -> DecisionSupport {
        DecisionSupport {
            atoms: vec![
                DecisionAtom {
                    scale_index: 0,
                    position: (1, 2),
                },
                DecisionAtom {
                    scale_index: 2,
                    position: (3, 4),
                },
            ],
            incumbent_coefficients: vec![vec![2.0, -1.0], vec![0.5, 4.0]],
            trace_components: 7,
            #[cfg(all(target_os = "macos", not(coverage)))]
            receipt_sha256: "test".to_string(),
        }
    }

    #[test]
    fn trajectory_coalesces_supported_updates_in_f32_order() {
        let support = support();
        let mut trajectory = DecisionTrajectory::new(&support, 2);
        trajectory.record_update(support.atoms[0], &[0.25, 0.5]);
        trajectory.record_update(support.atoms[0], &[0.75, -0.25]);
        assert_eq!(trajectory.coefficients()[0], [1.0, 0.25]);
        assert_eq!(trajectory.first_unsupported_atom(), None);
    }

    #[test]
    fn trajectory_records_the_first_atom_outside_the_frozen_support() {
        let support = support();
        let mut trajectory = DecisionTrajectory::new(&support, 2);
        let unsupported = DecisionAtom {
            scale_index: 1,
            position: (8, 9),
        };
        trajectory.record_update(unsupported, &[1.0, 2.0]);
        assert_eq!(trajectory.first_unsupported_atom(), Some(unsupported));
    }

    #[test]
    fn trajectory_and_cache_extend_adaptive_support_in_lockstep() {
        let support = support();
        let existing_atoms = support.atoms.len();
        let base = vec![10.0, 20.0];
        let mut cache = ExactDecisionResponseCache::new(
            support.clone(),
            2,
            base,
            vec![
                vec![1.0, 2.0],
                vec![3.0, 4.0],
                vec![5.0, 6.0],
                vec![7.0, 8.0],
            ],
        )
        .expect("cache");
        let mut trajectory = DecisionTrajectory::new(&support, 2);
        let atom = DecisionAtom {
            scale_index: 1,
            position: (8, 9),
        };
        trajectory.record_unsupported(atom);
        cache
            .extend_support(atom, 2, vec![vec![9.0, 10.0], vec![11.0, 12.0]])
            .expect("extend cache");
        trajectory
            .extend_support(atom, 2)
            .expect("extend trajectory");
        trajectory.record_update(atom, &[0.5, -0.25]);

        assert_eq!(cache.support().atoms.len(), existing_atoms + 1);
        assert_eq!(cache.column_count(), (existing_atoms + 1) * 2);
        assert_eq!(trajectory.first_unsupported_atom(), None);
        assert_eq!(trajectory.coefficients()[existing_atoms], [0.5, -0.25]);
    }

    #[test]
    fn cache_synthesizes_the_affine_response_with_f64_accumulation() {
        let cache = ExactDecisionResponseCache::new(
            support(),
            2,
            vec![10.0, 20.0],
            vec![
                vec![1.0, 2.0],
                vec![3.0, 4.0],
                vec![5.0, 6.0],
                vec![7.0, 8.0],
            ],
        )
        .expect("cache");
        let synthesized = cache
            .synthesize(&[vec![2.0, -1.0], vec![0.5, 4.0]])
            .expect("synthesize");
        assert_eq!(synthesized, [-19.5, -15.0]);
    }

    #[test]
    fn linearity_metrics_report_exact_and_perturbed_vectors() {
        let exact = decision_linearity_metrics(&[1.0, -2.0], &[1.0, -2.0]).expect("exact");
        assert_eq!(exact.relative_l2, 0.0);
        assert_eq!(exact.normalized_linf, 0.0);

        let perturbed = decision_linearity_metrics(&[1.0, -1.5], &[1.0, -2.0]).expect("perturbed");
        assert!(perturbed.relative_l2 > 0.2);
        assert_eq!(perturbed.max_abs, 0.5);
    }

    #[cfg(all(target_os = "macos", not(coverage)))]
    #[test]
    fn experimental_store_resumes_only_complete_synced_columns() {
        let temporary = tempfile::tempdir().expect("temporary cache directory");
        let path = temporary.path().join("decision-cache");
        let support = support();
        let base = [1.0, 2.0, 3.0];
        let (mut store, columns) =
            ExperimentalDecisionCacheStore::open(&path, &support, 2, &base).expect("create store");
        assert!(columns.is_empty());
        store.append_column(&[4.0, 5.0, 6.0]).expect("first column");
        store
            .append_column(&[7.0, 8.0, 9.0])
            .expect("second column");
        drop(store);

        let (store, columns) =
            ExperimentalDecisionCacheStore::open(&path, &support, 2, &base).expect("resume store");
        assert_eq!(store.columns_complete(), 2);
        assert_eq!(columns, vec![vec![4.0, 5.0, 6.0], vec![7.0, 8.0, 9.0]]);
    }
}
