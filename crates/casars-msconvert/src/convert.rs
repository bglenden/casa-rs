// SPDX-License-Identifier: LGPL-3.0-or-later
//! Running one checked conversion ([`msconvert`]): the output's guards,
//! the converter, and an existing MeasurementSet exchanged for its
//! replacement in one atomic step ([`Replacement`]).

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::Instant;

use casa_ms::observation_io::convert::{
    ChunkShape, ConversionOptions, ConversionReport, LossKind, Msv4Conversion, to_msv2,
    to_msv4_with, xradio_chunks,
};
use casa_ms::observation_io::creation::Replacement;
use casa_observation::ObservationError;
use casa_task_runtime::TaskFailure;
use casa_types::quanta::MvTime;

use crate::BATCH_BYTES;
use crate::request::{MsconvertRequest, StoreFormat};
use crate::task_contract::MsconvertTaskResult;

/// Convert as `request` says.
///
/// `outputvis` never lies inside `vis`. It must not exist, or, with
/// `overwrite`, must be a MeasurementSet (v2 or v4, not a link) that does
/// not hold `vis`, on a filesystem that exchanges directories, and an MSv2
/// no other process writes; it is held so ([`Replacement::hold`]) before
/// anything is converted. It is replaced only once its replacement is
/// complete: the conversion is written beside it under a hidden name, the
/// two are exchanged in one atomic call, and the old store is removed. A
/// conversion that fails leaves `outputvis` as it was. An MSv2 converted
/// into an MSv4 is partitioned as `request.partition_scheme` says, its
/// pointing carried or not, and each dataset chunked by `request.chunks`
/// or, without one, as XRADIO chunks it ([`xradio_chunks`]). Every loss the
/// output's format imposes is taken, and each is logged as a warning and
/// returned.
///
/// # Errors
///
/// A validation failure (nothing written) for an output not to be written
/// or replaced, or a source the converter does not support; an execution
/// failure for anything that failed while converting or replacing, which
/// leaves no partial output.
pub fn msconvert(request: &MsconvertRequest) -> Result<MsconvertTaskResult, TaskFailure> {
    let started = Instant::now();
    let replacing = guard_output(request)?;
    let options = ConversionOptions {
        data_groups: request.data_groups.clone(),
        // The task takes every loss the destination's format imposes and
        // reports each; the converter's refusal guards its other callers.
        accept: LossKind::ALL.to_vec(),
        batch_bytes: BATCH_BYTES,
        workers: request.workers,
        chunks: request.chunks.unwrap_or_default(),
        codec: request.codec,
        padding: request.padding,
        creator: format!("casa-rs msconvert {}", env!("CARGO_PKG_VERSION")),
        creation_date: utc_now(),
    };
    let target = if replacing.is_some() {
        let target = unused_sibling(&request.outputvis)?;
        tracing::info!(
            "msconvert: converting into {}, to take the place of {}",
            target.display(),
            request.outputvis.display()
        );
        target
    } else {
        request.outputvis.clone()
    };
    let converted = match request.outformat {
        StoreFormat::Msv4 => {
            let fixed;
            let chunk_shape: &ChunkShape = match request.chunks {
                Some(chunks) => {
                    fixed = move |_| chunks;
                    &fixed
                }
                None => &xradio_chunks,
            };
            let conversion = Msv4Conversion {
                partition_scheme: request.partition_scheme,
                pointing: request.pointing,
                chunk_shape,
            };
            to_msv4_with(&request.vis, &target, &options, &conversion)
        }
        StoreFormat::Msv2 => to_msv2(&request.vis, &target, &options),
    };
    let report = converted.map_err(failure)?;
    if let Some(replacement) = replacing {
        replace(replacement, &target)?;
    }
    for loss in &report.losses {
        tracing::warn!(
            "msconvert: not carried into {}: {loss}",
            request.outformat.name()
        );
    }
    Ok(result(request, &report, started))
}

/// The MeasurementSet at `outputvis` held for replacement when it exists;
/// a refusal when `outputvis` may not be written or replaced.
fn guard_output(request: &MsconvertRequest) -> Result<Option<Replacement>, TaskFailure> {
    let output = &request.outputvis;
    let refuse = |reason: String| Err(TaskFailure::validation(reason).at("outputvis"));
    // Written inside vis, a conversion would change it, and replacing a
    // part of it would destroy it.
    if within(output, &request.vis) {
        return refuse(format!(
            "outputvis {} is vis or lies inside it",
            output.display()
        ));
    }
    if std::fs::symlink_metadata(output).is_err() {
        return Ok(None);
    }
    if !request.overwrite {
        return refuse(format!(
            "outputvis {} exists; set overwrite=True to replace it",
            output.display()
        ));
    }
    if within(&request.vis, output) {
        return refuse(format!(
            "outputvis {} holds vis; it cannot be replaced by its own conversion",
            output.display()
        ));
    }
    // A whole MeasurementSet, not a link, on a filesystem that exchanges
    // directories, and an MSv2 no other process writes, held so until it
    // is replaced.
    Replacement::hold(output)
        .map(Some)
        .or_else(|error| refuse(format!("outputvis: {error}")))
}

/// Whether `inner` is `outer` or lies inside it, by file identity (device
/// and inode), so that neither a link, a bind mount nor a case-insensitive
/// file system hides it. A path that does not exist is judged by its
/// nearest existing ancestor.
fn within(inner: &Path, outer: &Path) -> bool {
    let Ok(outer) = std::fs::metadata(outer) else {
        return false;
    };
    let identity = (outer.dev(), outer.ino());
    let Some(existing) = inner
        .ancestors()
        .filter(|ancestor| !ancestor.as_os_str().is_empty())
        .find_map(|ancestor| std::fs::canonicalize(ancestor).ok())
    else {
        return false;
    };
    existing.ancestors().any(|ancestor| {
        std::fs::metadata(ancestor).is_ok_and(|found| (found.dev(), found.ino()) == identity)
    })
}

/// A conversion's error as the task reports it. The converter refuses what
/// it does not support before it writes; any other error is a failure, and
/// the converter's staged destination is removed with it.
fn failure(error: ObservationError) -> TaskFailure {
    match error {
        error @ ObservationError::Unsupported { .. } => {
            TaskFailure::validation(format!("msconvert: {error}"))
        }
        error => TaskFailure::execution(format!("msconvert: {error}")),
    }
}

/// A hidden name beside `path` that nothing uses:
/// `.<name>.msconvert-<process>-<n>`.
fn unused_sibling(path: &Path) -> Result<PathBuf, TaskFailure> {
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .ok_or_else(|| {
            TaskFailure::validation(format!("outputvis {} names no file", path.display()))
                .at("outputvis")
        })?;
    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    let process = std::process::id();
    (0..1000)
        .map(|n| parent.join(format!(".{name}.msconvert-{process}-{n}")))
        .find(|candidate| std::fs::symlink_metadata(candidate).is_err())
        .ok_or_else(|| TaskFailure::execution(format!("no unused name beside {}", path.display())))
}

/// Exchange the complete store at `converted` for the one `replacement`
/// holds, in one atomic call, and remove the old store. If the exchange
/// fails, the output is unchanged and the conversion is kept, complete,
/// where it is.
fn replace(replacement: Replacement, converted: &Path) -> Result<(), TaskFailure> {
    let output = replacement.path().to_path_buf();
    match replacement.publish(converted) {
        Ok(None) => Ok(()),
        Ok(Some(leftover)) => {
            tracing::warn!(
                "msconvert: the replaced store, now at {}, could not be removed",
                leftover.display()
            );
            Ok(())
        }
        Err(error) => Err(TaskFailure::execution(format!(
            "replace {}: {error}; it is unchanged, and the conversion is complete at {}",
            output.display(),
            converted.display()
        ))),
    }
}

/// The current time, UTC, ISO 8601 to the second.
fn utc_now() -> String {
    let unix_seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs());
    // The Unix epoch is MJD 40587.
    let time = MvTime::from_mjd_seconds(40_587.0 * 86_400.0 + unix_seconds as f64);
    format!(
        "{:04}-{:02}-{:02}T{}+00:00",
        time.year(),
        time.month(),
        time.month_day(),
        time.format_time(0)
    )
}

fn result(
    request: &MsconvertRequest,
    report: &ConversionReport,
    started: Instant,
) -> MsconvertTaskResult {
    MsconvertTaskResult {
        vis: request.vis.display().to_string(),
        outputvis: request.outputvis.display().to_string(),
        informat: request.informat.name().to_string(),
        outformat: request.outformat.name().to_string(),
        chunks: match request.outformat {
            StoreFormat::Msv4 => request.chunks,
            StoreFormat::Msv2 => None,
        },
        losses: report.losses.iter().map(ToString::to_string).collect(),
        partitions: report.partitions,
        samples: report.samples,
        rows: report.rows,
        elements: report.elements,
        elapsed_ns: u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_creation_date_is_iso_8601_utc() {
        let now = utc_now();
        assert_eq!(now.len(), "2026-11-02T12:34:56+00:00".len(), "{now}");
        assert_eq!(&now[4..5], "-");
        assert_eq!(&now[10..11], "T");
        assert!(now.ends_with("+00:00"), "{now}");
        assert!(now.as_str() >= "2026-01-01", "{now}");
    }

    #[test]
    fn a_sibling_name_is_hidden_beside_the_output() {
        let root = tempfile::tempdir().expect("root");
        let output = root.path().join("out.ms");
        let sibling = unused_sibling(&output).expect("a name");
        assert_eq!(sibling.parent(), Some(root.path()));
        let name = sibling.file_name().unwrap().to_string_lossy().into_owned();
        assert!(name.starts_with(".out.ms.msconvert-"), "{name}");
        std::fs::write(&sibling, b"taken").expect("take it");
        assert_ne!(unused_sibling(&output).expect("another"), sibling);
    }

    #[test]
    fn containment_is_judged_by_file_identity() {
        let root = tempfile::tempdir().expect("root");
        let vis = root.path().join("a.ms");
        std::fs::create_dir_all(vis.join("ANTENNA")).expect("vis");
        assert!(within(&vis, &vis));
        assert!(within(&vis.join("ANTENNA"), &vis));
        // A path not yet made is judged by its nearest existing ancestor.
        assert!(within(&vis.join("new.ps.zarr"), &vis));
        assert!(within(&vis.join("x/y/new.ps.zarr"), &vis));
        assert!(!within(&root.path().join("a.ps.zarr"), &vis));
        assert!(within(&vis, root.path()));
        // Through a link, and through `..`.
        let link = root.path().join("link");
        std::os::unix::fs::symlink(&vis, &link).expect("link");
        assert!(within(&link.join("ANTENNA"), &vis));
        assert!(within(&vis.join("ANTENNA/.."), &vis));
        assert!(!within(&vis, &root.path().join("missing")));
    }
}
