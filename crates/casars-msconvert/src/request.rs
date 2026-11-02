// SPDX-License-Identifier: LGPL-3.0-or-later
//! The resolved `msconvert` parameters read into one checked
//! [`MsconvertRequest`]; every refusal names its parameter.

use std::path::{Path, PathBuf};

use casa_ms::observation_io::Msv4Codec;
use casa_ms::observation_io::convert::{LossKind, PaddingRows};
use casa_task_runtime::{TaskFailure, TaskInvocation};

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
}

/// One conversion: its parameters read and checked, nothing done yet.
#[derive(Clone, Debug)]
pub struct MsconvertRequest {
    /// The store converted, which exists.
    pub vis: PathBuf,
    /// The store written.
    pub outputvis: PathBuf,
    /// The format of `vis`.
    pub informat: StoreFormat,
    /// The format of `outputvis`.
    pub outformat: StoreFormat,
    /// The data groups converted; `None` for every one.
    pub data_groups: Option<Vec<String>>,
    /// The kinds of loss accepted.
    pub accept: Vec<LossKind>,
    /// MSv2 output: what a padded grid cell becomes.
    pub padding: PaddingRows,
    /// MSv4 output: the chunk shape, `None` for XRADIO's default
    /// ([`crate::default_chunks`]).
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
/// unknown format, loss kind, padding or codec, a chunk shape not of four
/// non-negative extents, or a negative `nthreads`.
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
    let data_groups = names(task, "datagroups")?;
    let accept = names(task, "acceptloss")?
        .iter()
        .map(|kind| kind.parse::<LossKind>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|reason| invalid("acceptloss", reason))?;
    Ok(MsconvertRequest {
        vis,
        outputvis: task.workspace().join(task.path("outputvis")?),
        informat,
        outformat,
        data_groups: (!data_groups.is_empty()).then_some(data_groups),
        accept,
        padding: padding(task)?,
        chunks: chunks(task)?,
        codec: codec(task)?,
        workers: workers(task)?,
        overwrite: task.flag("overwrite")?,
    })
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

/// Partitions converted at once: `nthreads`, or every core for 0.
fn workers(task: &TaskInvocation<'_>) -> Result<usize, TaskFailure> {
    let threads = task.integer("nthreads")?;
    match usize::try_from(threads) {
        Ok(0) => Ok(std::thread::available_parallelism().map_or(1, std::num::NonZero::get)),
        Ok(threads) => Ok(threads),
        Err(_) => Err(invalid(
            "nthreads",
            format!("nthreads is 0 (every core) or more, found {threads}"),
        )),
    }
}
