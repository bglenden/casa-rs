// SPDX-License-Identifier: LGPL-3.0-or-later
//! Running one checked conversion ([`msconvert`]): the destination's
//! guards, the default chunk shape, the converter, and an existing
//! MeasurementSet replaced only once its replacement is complete.

use std::path::{Path, PathBuf};
use std::time::Instant;

use casa_ms::observation_io::convert::{
    ConversionOptions, ConversionReport, LOSS_REFUSAL, to_msv2, to_msv4,
};
use casa_ms::observation_io::{OpenOptions, open};
use casa_observation::ObservationError;
use casa_task_runtime::TaskFailure;
use casa_types::quanta::MvTime;

use crate::chunks::{PayloadShape, default_chunks};
use crate::request::{MsconvertRequest, StoreFormat};
use crate::task_contract::MsconvertTaskResult;

/// The most bytes one time range of every selected group's batches holds:
/// the trial's measured budget.
const BATCH_BYTES: usize = 256 << 20;

/// Convert as `request` says.
///
/// `outputvis` must not exist, or, with `overwrite`, must be a
/// MeasurementSet (v2 or v4) other than `vis` and not holding it. An
/// existing one is replaced only after its replacement is complete beside
/// it: the conversion is written under a temporary name, the old store
/// renamed aside, the new one renamed into place and the old one removed.
/// A conversion that fails leaves `outputvis` as it was.
///
/// # Errors
///
/// A validation failure (nothing written) for an existing output not to be
/// replaced, a loss not accepted, or a request the converter refuses; an
/// execution failure for anything that failed while converting or
/// replacing.
pub fn msconvert(request: &MsconvertRequest) -> Result<MsconvertTaskResult, TaskFailure> {
    let started = Instant::now();
    let replacing = guard_output(request)?;
    let chunks = match request.outformat {
        StoreFormat::Msv4 => Some(match request.chunks {
            Some(chunks) => chunks,
            None => source_chunks(&request.vis)?,
        }),
        StoreFormat::Msv2 => None,
    };
    let options = ConversionOptions {
        data_groups: request.data_groups.clone(),
        accept: request.accept.clone(),
        batch_bytes: BATCH_BYTES,
        workers: request.workers,
        chunks: chunks.unwrap_or_default(),
        codec: request.codec,
        padding: request.padding,
        creator: format!("casa-rs msconvert {}", env!("CARGO_PKG_VERSION")),
        creation_date: utc_now(),
    };
    let target = if replacing {
        unused_sibling(&request.outputvis, "converting")?
    } else {
        request.outputvis.clone()
    };
    let converted = match request.outformat {
        StoreFormat::Msv4 => to_msv4(&request.vis, &target, &options),
        StoreFormat::Msv2 => to_msv2(&request.vis, &target, &options),
    };
    let report = converted.map_err(|error| failure(error, request))?;
    if replacing {
        replace(&target, &request.outputvis)?;
    }
    for loss in &report.losses {
        tracing::warn!("msconvert: accepted loss: {loss}");
    }
    Ok(result(request, chunks, &report, started))
}

/// Whether `outputvis` exists and is to be replaced; a refusal when it
/// exists and may not be.
fn guard_output(request: &MsconvertRequest) -> Result<bool, TaskFailure> {
    let output = &request.outputvis;
    if std::fs::symlink_metadata(output).is_err() {
        return Ok(false);
    }
    let refuse = |reason: String| Err(TaskFailure::validation(reason).at("outputvis"));
    if !request.overwrite {
        return refuse(format!(
            "outputvis {} exists; set overwrite=True to replace it",
            output.display()
        ));
    }
    if !output.is_dir() || !(is_store(output)) {
        return refuse(format!(
            "outputvis {} exists and is not a MeasurementSet; msconvert replaces only a \
             MeasurementSet v2 or MSv4",
            output.display()
        ));
    }
    let canonical = |path: &Path| {
        std::fs::canonicalize(path)
            .map_err(|error| TaskFailure::execution(format!("resolve {}: {error}", path.display())))
    };
    if canonical(&request.vis)?.starts_with(canonical(output)?) {
        return refuse(format!(
            "outputvis {} is vis or holds it; it cannot be replaced by its own conversion",
            output.display()
        ));
    }
    Ok(true)
}

/// Whether `path` is a MeasurementSet store: an MSv4 processing set, or a
/// casacore table (its `table.dat`).
fn is_store(path: &Path) -> bool {
    casa_ms::observation_io::is_msv4_store(path) || path.join("table.dat").is_file()
}

/// XRADIO's default chunk shape for the partitions of the store at `vis`.
fn source_chunks(vis: &Path) -> Result<[u64; 4], TaskFailure> {
    let observation = open(vis, &OpenOptions::default()).map_err(|error| {
        TaskFailure::validation(format!("open {}: {error}", vis.display())).at("vis")
    })?;
    let catalogue = observation.catalogue();
    let shapes: Vec<PayloadShape> = catalogue
        .partition_ids()
        .into_iter()
        .map(|id| {
            let entry = catalogue.partition(id);
            PayloadShape {
                baselines: entry.baselines.len() as u64,
                channels: entry.frequencies_hz.len() as u64,
                correlations: entry.correlations.len() as u64,
            }
        })
        .collect();
    Ok(default_chunks(&shapes))
}

/// A conversion's error as the task reports it. The converter writes
/// nothing before it refuses, and leaves no destination when it fails.
fn failure(error: ObservationError, request: &MsconvertRequest) -> TaskFailure {
    match error {
        ObservationError::InvalidRequest { reason } if reason.starts_with(LOSS_REFUSAL) => {
            let accepted: Vec<&str> = request.accept.iter().map(|kind| kind.name()).collect();
            TaskFailure::validation(format!(
                "{reason} (acceptloss is now [{}]; add the kinds named)",
                accepted.join(", ")
            ))
            .at("acceptloss")
        }
        error
        @ (ObservationError::InvalidRequest { .. } | ObservationError::Unsupported { .. }) => {
            TaskFailure::validation(format!("msconvert: {error}"))
        }
        error => TaskFailure::execution(format!("msconvert: {error}")),
    }
}

/// A name beside `path` that nothing uses: `.<name>.msconvert-<what>-<n>`.
fn unused_sibling(path: &Path, what: &str) -> Result<PathBuf, TaskFailure> {
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
        .map(|n| parent.join(format!(".{name}.msconvert-{what}-{process}-{n}")))
        .find(|candidate| std::fs::symlink_metadata(candidate).is_err())
        .ok_or_else(|| TaskFailure::execution(format!("no unused name beside {}", path.display())))
}

/// Put the complete store at `converted` in the place of the one at
/// `output`, which is removed. If the new store cannot be moved into place
/// the old one is put back.
fn replace(converted: &Path, output: &Path) -> Result<(), TaskFailure> {
    let failed = |what: &str, error: std::io::Error| {
        let _ = std::fs::remove_dir_all(converted);
        TaskFailure::execution(format!(
            "{what}: {error}; {} is unchanged",
            output.display()
        ))
    };
    let retired = unused_sibling(output, "replaced")?;
    std::fs::rename(output, &retired)
        .map_err(|error| failed(&format!("move {} aside", output.display()), error))?;
    if let Err(error) = std::fs::rename(converted, output) {
        let restored = std::fs::rename(&retired, output);
        let _ = std::fs::remove_dir_all(converted);
        return Err(TaskFailure::execution(match restored {
            Ok(()) => format!(
                "move the conversion into {}: {error}; it is unchanged",
                output.display()
            ),
            Err(lost) => format!(
                "move the conversion into {}: {error}; the old store could not be put back \
                 ({lost}) and is at {}",
                output.display(),
                retired.display()
            ),
        }));
    }
    if let Err(error) = std::fs::remove_dir_all(&retired) {
        tracing::warn!(
            "msconvert: the replaced store at {} could not be removed: {error}",
            retired.display()
        );
    }
    Ok(())
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
    chunks: Option<[u64; 4]>,
    report: &ConversionReport,
    started: Instant,
) -> MsconvertTaskResult {
    MsconvertTaskResult {
        vis: request.vis.display().to_string(),
        outputvis: request.outputvis.display().to_string(),
        informat: request.informat.name().to_string(),
        outformat: request.outformat.name().to_string(),
        chunks,
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
        let sibling = unused_sibling(&output, "converting").expect("a name");
        assert_eq!(sibling.parent(), Some(root.path()));
        let name = sibling.file_name().unwrap().to_string_lossy().into_owned();
        assert!(name.starts_with(".out.ms.msconvert-converting-"), "{name}");
        std::fs::write(&sibling, b"taken").expect("take it");
        assert_ne!(
            unused_sibling(&output, "converting").expect("another"),
            sibling
        );
    }
}
