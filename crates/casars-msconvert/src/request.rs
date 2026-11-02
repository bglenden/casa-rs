// SPDX-License-Identifier: LGPL-3.0-or-later
//! The resolved `msconvert` parameters read into one checked
//! [`MsconvertRequest`]; every refusal names its parameter.

use std::path::{Path, PathBuf};

use casa_ms::observation_io::convert::{PaddingRows, selected_data_groups};
use casa_ms::observation_io::{Msv4Codec, OpenOptions, PartitionScheme, open};
use casa_task_runtime::{TaskFailure, TaskInvocation};

use crate::THREAD_BYTES;

/// A store's format.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreFormat {
    /// A MeasurementSet v2: a casacore table directory.
    Msv2,
    /// An MSv4 processing set: a Zarr group.
    Msv4,
}

impl StoreFormat {
    /// The format of the store at `path`, as casa-ms opens it.
    #[must_use]
    pub fn of(path: &Path) -> Self {
        if casa_ms::observation_io::is_msv4_store(path) {
            Self::Msv4
        } else {
            Self::Msv2
        }
    }

    /// `msv2` or `msv4`.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Msv2 => "msv2",
            Self::Msv4 => "msv4",
        }
    }

    /// The format a conversion of this one makes by default.
    #[must_use]
    pub fn other(self) -> Self {
        match self {
            Self::Msv2 => Self::Msv4,
            Self::Msv4 => Self::Msv2,
        }
    }

    /// The extension XRADIO's convention gives a store of this format:
    /// `.ms` for a MeasurementSet v2, `.ps.zarr` for a processing set.
    #[must_use]
    pub fn extension(self) -> &'static str {
        match self {
            Self::Msv2 => ".ms",
            Self::Msv4 => ".ps.zarr",
        }
    }
}

/// The extensions a default output's name drops from its input's, longest
/// first; any case.
const MEASUREMENT_SET_EXTENSIONS: [&str; 3] = [".ps.zarr", ".zarr", ".ms"];

/// The output a conversion of `vis` into `outformat` writes when none is
/// named, by XRADIO's convention: beside `vis`, named after it, its
/// extension (`.ms`, `.zarr` or `.ps.zarr`, any case) replaced by
/// `.ps.zarr` for a processing set or `.ms` for a MeasurementSet v2
/// ([`StoreFormat::extension`]), or that extension added to a name without
/// one: `obs.ms` becomes `obs.ps.zarr`, and `obs.ps.zarr` `obs.ms`. `None`
/// when `vis` names no file, or a name that is not UTF-8.
#[must_use]
pub fn default_outputvis(vis: &Path, outformat: StoreFormat) -> Option<PathBuf> {
    let name = vis.file_name()?.to_str()?;
    let lower = name.to_ascii_lowercase();
    let stem = MEASUREMENT_SET_EXTENSIONS
        .iter()
        .find(|extension| lower.len() > extension.len() && lower.ends_with(*extension))
        .map_or(name, |extension| &name[..name.len() - extension.len()]);
    Some(vis.with_file_name(format!("{stem}{}", outformat.extension())))
}

/// `outputvis` as XRADIO's converter names a processing set it writes: a
/// path that does not end in `ps.zarr` gains `.ps.zarr`, by text, so `out`
/// is written as `out.ps.zarr` (and `.` as `..ps.zarr`, as XRADIO writes
/// it). A trailing `/` is no part of the name.
#[must_use]
pub fn processing_set_path(outputvis: &str) -> PathBuf {
    let trimmed = outputvis.trim_end_matches('/');
    let path = if trimmed.is_empty() {
        outputvis
    } else {
        trimmed
    };
    if path.ends_with("ps.zarr") {
        PathBuf::from(path)
    } else {
        PathBuf::from(format!("{path}{}", StoreFormat::Msv4.extension()))
    }
}

/// One conversion: its parameters read and checked, nothing done yet.
#[derive(Clone, Debug)]
pub struct MsconvertRequest {
    /// The store converted, which exists.
    pub vis: PathBuf,
    /// The store written: `outputvis`, or [`default_outputvis`].
    pub outputvis: PathBuf,
    /// The format of `vis`.
    pub informat: StoreFormat,
    /// The format of `outputvis`.
    pub outformat: StoreFormat,
    /// The data groups converted; `None` for every one.
    pub data_groups: Option<Vec<String>>,
    /// MSv2 into MSv4: the keys the MSv2 is further partitioned by.
    pub partition_scheme: PartitionScheme,
    /// MSv4 output: whether pointing is converted.
    pub pointing: bool,
    /// MSv2 output: what a padded grid cell becomes.
    pub padding: PaddingRows,
    /// MSv4 output: the chunk shape, `None` for XRADIO's, sized for each
    /// dataset
    /// ([`xradio_chunks`](casa_ms::observation_io::convert::xradio_chunks)).
    pub chunks: Option<[u64; 4]>,
    /// MSv4 output: the payload codec.
    pub codec: Msv4Codec,
    /// MSv4 output: partitions converted at once, at least one.
    pub workers: usize,
    /// Replace an existing MeasurementSet at `outputvis`.
    pub overwrite: bool,
}

/// Read the resolved run `task` into its request.
///
/// # Errors
///
/// A validation failure at the parameter that is wrong: `vis` missing, an
/// unknown format, padding or codec, no `outputvis` and none to derive (or
/// only `vis` itself), a data group the conversion would refuse, a
/// partition scheme or pointing choice the conversion does not make, a
/// chunk shape not of four non-negative extents, or a negative `nthreads`.
pub fn request_from(task: &TaskInvocation<'_>) -> Result<MsconvertRequest, TaskFailure> {
    let vis = task.workspace().join(task.input_path("vis")?);
    let informat = StoreFormat::of(&vis);
    let outformat = match lower(task.text("outformat")?).as_str() {
        "" => informat.other(),
        "msv2" => StoreFormat::Msv2,
        "msv4" => StoreFormat::Msv4,
        other => {
            return Err(invalid(
                "outformat",
                format!("unknown outformat {other:?}; expected msv2, msv4 or empty"),
            ));
        }
    };
    let data_groups = data_groups(task, &vis, outformat)?;
    let msv2_into_msv4 = informat == StoreFormat::Msv2 && outformat == StoreFormat::Msv4;
    Ok(MsconvertRequest {
        outputvis: outputvis(task, &vis, outformat)?,
        vis,
        informat,
        outformat,
        data_groups,
        partition_scheme: partition_scheme(task, msv2_into_msv4)?,
        pointing: pointing(task, outformat)?,
        padding: padding(task)?,
        chunks: chunks(task)?,
        codec: codec(task)?,
        workers: workers(task)?,
        overwrite: task.flag("overwrite")?,
    })
}

/// `outputvis`, or the default beside `vis` when it is empty.
fn outputvis(
    task: &TaskInvocation<'_>,
    vis: &Path,
    outformat: StoreFormat,
) -> Result<PathBuf, TaskFailure> {
    let named = task.text("outputvis")?.unwrap_or_default();
    if !named.trim().is_empty() {
        let named = match outformat {
            StoreFormat::Msv4 => processing_set_path(&named),
            StoreFormat::Msv2 => PathBuf::from(named),
        };
        return Ok(task.workspace().join(named));
    }
    let derived = default_outputvis(vis, outformat).ok_or_else(|| {
        invalid(
            "outputvis",
            format!(
                "no output name follows from vis {}; name outputvis",
                vis.display()
            ),
        )
    })?;
    if derived == vis {
        return Err(invalid(
            "outputvis",
            format!(
                "the default {} output name for vis {} would be vis itself; name outputvis",
                outformat.name(),
                vis.display()
            ),
        ));
    }
    Ok(derived)
}

/// The data groups named, checked against the source as the conversion
/// checks them, which opens it only when some are named: each in every
/// partition, none twice, and into an MSv2 `base` first (it becomes
/// `DATA`).
fn data_groups(
    task: &TaskInvocation<'_>,
    vis: &Path,
    outformat: StoreFormat,
) -> Result<Option<Vec<String>>, TaskFailure> {
    let named = names(task, "datagroups")?;
    if named.is_empty() {
        return Ok(None);
    }
    if outformat == StoreFormat::Msv2 && named[0] != "base" {
        return Err(invalid(
            "datagroups",
            format!(
                "an MSv2 output's first data group is base, its DATA; found {}",
                named[0]
            ),
        ));
    }
    let observation = open(vis, &OpenOptions::default())
        .map_err(|error| invalid("vis", format!("open {}: {error}", vis.display())))?;
    selected_data_groups(observation.catalogue(), Some(&named))
        .map_err(|error| invalid("datagroups", error.to_string()))?;
    Ok(Some(named))
}

/// The keys an MSv2 converted into an MSv4 is further partitioned by,
/// XRADIO's `partition_scheme`: `FIELD_ID` and `SCAN_NUMBER`. Refused for
/// any other conversion, which has no MSv2 to partition.
fn partition_scheme(
    task: &TaskInvocation<'_>,
    msv2_into_msv4: bool,
) -> Result<PartitionScheme, TaskFailure> {
    let mut scheme = PartitionScheme::default();
    for key in names(task, "partitionscheme")? {
        match key.to_ascii_uppercase().as_str() {
            "FIELD_ID" => scheme.fields = true,
            "SCAN_NUMBER" => scheme.scans = true,
            _ => {
                return Err(invalid(
                    "partitionscheme",
                    format!("unknown partition key {key:?}; expected FIELD_ID or SCAN_NUMBER"),
                ));
            }
        }
    }
    if scheme != PartitionScheme::default() && !msv2_into_msv4 {
        return Err(invalid(
            "partitionscheme",
            "partitionscheme partitions an MSv2 converted into an MSv4",
        ));
    }
    Ok(scheme)
}

/// Whether pointing is converted: XRADIO's `with_pointing`, an MSv4
/// output's choice; an MSv2 output always carries it.
fn pointing(task: &TaskInvocation<'_>, outformat: StoreFormat) -> Result<bool, TaskFailure> {
    let pointing = task.flag("withpointing")?;
    if !pointing && outformat == StoreFormat::Msv2 {
        return Err(invalid(
            "withpointing",
            "withpointing=False leaves pointing out of an MSv4 output; an MSv2 output carries it",
        ));
    }
    Ok(pointing)
}

fn invalid(parameter: &str, reason: impl Into<String>) -> TaskFailure {
    TaskFailure::validation(reason).at(parameter)
}

/// `name`'s names: a list, or one text of names separated by commas, as
/// CASA writes lists; blank names are none.
fn names(task: &TaskInvocation<'_>, name: &str) -> Result<Vec<String>, TaskFailure> {
    Ok(task
        .texts(name)?
        .iter()
        .flat_map(|text| text.split(','))
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect())
}

fn lower(text: Option<String>) -> String {
    text.unwrap_or_default().trim().to_ascii_lowercase()
}

fn padding(task: &TaskInvocation<'_>) -> Result<PaddingRows, TaskFailure> {
    match lower(task.text("padding")?).as_str() {
        "flagged" => Ok(PaddingRows::Flagged),
        "omitted" => Ok(PaddingRows::Omitted),
        other => Err(invalid(
            "padding",
            format!("unknown padding {other:?}; expected flagged or omitted"),
        )),
    }
}

fn codec(task: &TaskInvocation<'_>) -> Result<Msv4Codec, TaskFailure> {
    match lower(task.text("codec")?).as_str() {
        "blosc" => Ok(Msv4Codec::BloscLz4),
        "zstd" => Ok(Msv4Codec::Zstd),
        "none" => Ok(Msv4Codec::None),
        other => Err(invalid(
            "codec",
            format!("unknown codec {other:?}; expected blosc, zstd or none"),
        )),
    }
}

/// The chunk shape: none for the default, or four non-negative extents.
fn chunks(task: &TaskInvocation<'_>) -> Result<Option<[u64; 4]>, TaskFailure> {
    let extents = task.integers("chunks")?;
    if extents.is_empty() {
        return Ok(None);
    }
    let shape = <[i64; 4]>::try_from(extents.as_slice()).map_err(|_| {
        invalid(
            "chunks",
            format!(
                "chunks takes four extents [time, baseline, frequency, polarization], found {}",
                extents.len()
            ),
        )
    })?;
    let mut chunks = [0; 4];
    for (chunk, extent) in chunks.iter_mut().zip(shape) {
        *chunk = u64::try_from(extent).map_err(|_| {
            invalid(
                "chunks",
                format!("chunk extents are 0 (a whole axis) or more, found {extent}"),
            )
        })?;
    }
    Ok(Some(chunks))
}

/// Partitions converted at once: `nthreads`, or for 0 as many as the cores
/// and the host's memory budget allow, [`THREAD_BYTES`] each, and at least
/// one.
fn workers(task: &TaskInvocation<'_>) -> Result<usize, TaskFailure> {
    let threads = task.integer("nthreads")?;
    match usize::try_from(threads) {
        Ok(0) => {
            let cores = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
            let memory =
                casa_ms::observation_io::creation::default_memory_bytes().unwrap_or(THREAD_BYTES);
            Ok((memory / THREAD_BYTES).clamp(1, cores))
        }
        Ok(threads) => Ok(threads),
        Err(_) => Err(invalid(
            "nthreads",
            format!("nthreads is 0 (every core) or more, found {threads}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(vis: &str, outformat: StoreFormat) -> Option<String> {
        default_outputvis(Path::new(vis), outformat).map(|path| path.display().to_string())
    }

    #[test]
    fn the_default_output_follows_xradios_names() {
        let (v2, v4) = (StoreFormat::Msv2, StoreFormat::Msv4);
        assert_eq!(named("obs.ms", v4).as_deref(), Some("obs.ps.zarr"));
        assert_eq!(
            named("data/obs.ms", v4).as_deref(),
            Some("data/obs.ps.zarr")
        );
        assert_eq!(
            named("/data/obs.MS/", v4).as_deref(),
            Some("/data/obs.ps.zarr")
        );
        assert_eq!(named("obs.ps.zarr", v2).as_deref(), Some("obs.ms"));
        assert_eq!(named("obs.zarr", v2).as_deref(), Some("obs.ms"));
        // A name without one gains the extension; a bare extension is a name.
        assert_eq!(named("obs", v4).as_deref(), Some("obs.ps.zarr"));
        assert_eq!(named("obs.v1", v2).as_deref(), Some("obs.v1.ms"));
        assert_eq!(named(".ms", v4).as_deref(), Some(".ms.ps.zarr"));
        // The same format names the input itself, which the task refuses.
        assert_eq!(named("obs.ps.zarr", v4).as_deref(), Some("obs.ps.zarr"));
        assert_eq!(named("obs.ms", v2).as_deref(), Some("obs.ms"));
        assert_eq!(named("/", v4), None);
    }

    #[test]
    fn a_processing_set_is_named_as_xradio_names_it() {
        let path = |text: &str| processing_set_path(text).display().to_string();
        assert_eq!(path("out"), "out.ps.zarr");
        assert_eq!(path("data/out.zarr"), "data/out.zarr.ps.zarr");
        assert_eq!(path("out.ps.zarr"), "out.ps.zarr");
        assert_eq!(path("out.ps.zarr/"), "out.ps.zarr");
        assert_eq!(path("outps.zarr"), "outps.zarr");
        assert_eq!(path("."), "..ps.zarr");
        assert_eq!(path("/"), "/.ps.zarr");
    }
}
