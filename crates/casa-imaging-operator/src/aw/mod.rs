// SPDX-License-Identifier: LGPL-3.0-or-later
//! The AW-projection kernel set: a catalog of CASA `CFS_*`/`WTCFS_*`
//! convolution-function images read header-first at open and cell by cell
//! on demand into a bounded cache, with the LibRA/CASA index rules pinned
//! (`TransformMachines2/{CFCache,CFBuffer,AWVisResampler,AWProjectFT}.cc`),
//! and native EVLA generation that writes the same images
//! (`AWConvFunc::makeConvFunction2`, `CFCell::makePersistent`).

mod cell_image;
mod evla;
mod native;
mod native_cache;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use casa_imaging_model::{CorrelationType, EvlaAwCellRequest, NativeAwRequestInput};
use num_complex::Complex32;
use thiserror::Error;

pub use evla::{EvlaApertureGrid, EvlaApertureModel};
pub use native::{EvlaAwWorkspace, NativeAwPair, NativeAwPlane};
pub use native_cache::{NativeCacheState, NativeManifest};

use cell_image::{CellMisc, load_group, read_header, write_cell};
#[cfg(test)]
use cell_image::{cf_area, dense_cell_padded};
use native_cache::{cell_suffix, write_manifest};

use crate::convolution::{
    CellHold, ConvolutionFunctionSet, DenseCell, ImageCorrection, KernelNormalisation,
    MuellerRouting, RowContext, TapLayout,
};
use crate::dense::dense_cell_bytes;
use crate::geometry::GridGeometry;
use crate::polarization::PolarizationRouting;
use crate::sample::CfKey;
use crate::weighting::SPEED_OF_LIGHT_M_PER_S;

/// `CFStore2::makePersistent`: the imaging cell prefix.
const IMAGING_PREFIX: &str = "CFS_";
/// `CFStore2::makePersistent` with the `WT` qualifier: the weight cell prefix.
const WEIGHT_PREFIX: &str = "WTCFS_";
/// `AWProjectFT`: Mueller elements of a 4 × 4 matrix, `element = 4·i + j`.
const MUELLER_ELEMENTS: u32 = 16;

/// A native A/W request or numerical calculation could not be completed.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum NativeAwGenerationError {
    /// Dish samples are not finite, regularly spaced, or physically valid.
    #[error("EVLA dish surface requires finite, uniformly spaced radius/height/slope samples")]
    InvalidSurface,
    /// The requested receiver band is outside the implemented EVLA proof.
    #[error("native EVLA aperture supports only L, S and C receiver bands (0.9 to 8 GHz)")]
    UnsupportedFrequency,
    /// Aperture geometry or numerical sampling is invalid.
    #[error(
        "native aperture grid requires a finite positive cell size, frequency, and even extent"
    )]
    InvalidGrid,
    /// The caller did not supply the declared numerical workspace.
    #[error("native A/W generation workspace does not fit the requested grid")]
    WorkspaceMismatch,
    /// A generated value or normalization is invalid.
    #[error("native A/W generation produced an invalid numerical value")]
    InvalidNumerics,
}

/// A CASA convolution-function cache could not be opened or written.
#[derive(Debug, Error)]
pub enum AwCatalogError {
    /// The cache directory cannot be listed or a cell image cannot be read.
    #[error("AW cache {path}: {detail}")]
    Cache {
        /// The directory or cell image.
        path: PathBuf,
        /// What failed.
        detail: String,
    },
    /// A cell could not be generated natively.
    #[error("native EVLA generation: {0}")]
    Generation(#[from] NativeAwGenerationError),
    /// A generated cell image could not be written.
    #[error("writing cell {path}: {detail}")]
    Write {
        /// The cell image.
        path: PathBuf,
        /// What failed.
        detail: String,
    },
}

fn cache_error(path: impl AsRef<Path>, detail: impl Into<String>) -> AwCatalogError {
    AwCatalogError::Cache {
        path: path.as_ref().to_path_buf(),
        detail: detail.into(),
    }
}

/// How a catalog maps rows to cells.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AwIndexing {
    /// `conjbeams`: the cell frequency is `√(2f₀² − f²)` with `f₀` the
    /// image reference frequency (`CFBuffer::initMaps`), else `f`.
    pub conjugate_beams: bool,
    /// The image reference frequency `f₀` in Hz.
    pub image_reference_hz: f64,
}

/// Header of one cell image, read at open (`CFCache::getCFParams`).
#[derive(Clone, Debug)]
struct CellHeader {
    path: PathBuf,
    /// `[nx, ny]` of the image.
    shape: [usize; 2],
    /// `Sampling`.
    sampling: u16,
    /// `Xsupport`, `Ysupport`.
    support: [u16; 2],
    /// `MuellerElement`.
    mueller: u32,
    /// `WValue`.
    w_value: f64,
    /// The working field a native cell was generated on, in radians
    /// (`CasaRsSkyIncrement · CasaRsWorkingSize`); absent in CASA's caches.
    working_field_rad: Option<f64>,
    /// `WIncr`.
    w_increment: f64,
    /// `ParallacticAngle` in degrees.
    pa_deg: f64,
    /// The spectral reference value as CASA holds it (`Float`).
    frequency_hz: f64,
}

/// The imaging and weight images of one (PA, frequency, w, Mueller) cell.
#[derive(Clone, Debug)]
struct CellFiles {
    imaging: CellHeader,
    weight: CellHeader,
}

/// One (PA, frequency, w) group: its Mueller elements in the catalog's
/// element order.
#[derive(Clone, Debug)]
struct Group {
    cells: Vec<CellFiles>,
    /// The largest declared half support over the group's imaging cells:
    /// the imaging dense cell is padded to it, and `AWVisResampler`'s
    /// `onGrid` test for a data row uses it (`CF Support: 4 (6)` in CASA's
    /// log is the imaging support with the weight support in brackets).
    imaging_half_support: [u16; 2],
    /// The same over the weight cells, which the PSF and weight image use.
    weight_half_support: [u16; 2],
    /// The smallest declared half support over both kinds: the row filter
    /// keeps every row some cell of the group fits.
    smallest_half_support: [u16; 2],
}

impl Group {
    fn new(cells: Vec<CellFiles>) -> Self {
        let largest = |select: fn(&CellFiles) -> &CellHeader| {
            cells.iter().map(select).fold([0_u16; 2], |half, header| {
                [
                    half[0].max(header.support[0]),
                    half[1].max(header.support[1]),
                ]
            })
        };
        let imaging_half_support = largest(|cell| &cell.imaging);
        let weight_half_support = largest(|cell| &cell.weight);
        let smallest_half_support = cells
            .iter()
            .flat_map(|cell| [&cell.imaging, &cell.weight])
            .fold([u16::MAX; 2], |half, header| {
                [
                    half[0].min(header.support[0]),
                    half[1].min(header.support[1]),
                ]
            });
        Self {
            cells,
            imaging_half_support,
            weight_half_support,
            smallest_half_support,
        }
    }
}

/// The support `locate_sample` tests a row against: the smallest cell the
/// gridding group or the prediction group can select. Every mode re-tests
/// the support of the cell it reads (`AWVisResampler::DataToGrid` and
/// `GridToData` call `onGrid` with the selected cell's support), so the
/// filter only has to keep every row CASA grids with any of them.
fn placement_half_support(gridding: &Group, prediction: &Group) -> [u16; 2] {
    let (g, p) = (
        gridding.smallest_half_support,
        prediction.smallest_half_support,
    );
    [g[0].min(p[0]), g[1].min(p[1])]
}

/// The loaded taps of one group.
struct Loaded {
    imaging: Arc<DenseCell>,
    weight: Arc<DenseCell>,
    /// The cache clock at the group's last use; eviction takes the oldest.
    last_use: u64,
}

impl Loaded {
    fn bytes(&self) -> usize {
        self.imaging.bytes() + self.weight.bytes()
    }
}

/// The bounded cell cache: least recently used eviction once the loaded
/// cells exceed the bound; a cell a worker holds stays alive through its
/// [`CellHold`] after eviction.
struct Lru {
    loaded: HashMap<usize, Loaded>,
    /// Use counter stamped on every fetch.
    clock: u64,
    bytes: usize,
    bound: usize,
}

/// The AW-projection kernel set over a CASA convolution-function cache.
///
/// Cells are keyed on the parallactic-angle cell, the frequency cell and
/// the w-plane (`CfKey::group`); the Mueller elements live inside the cell
/// as planes, routed by [`MuellerRouting`] with CASA's selection of both
/// the cell and its input polarization (`AWVisResampler`). Prediction
/// divides once by the kernel sum and `sumwt += W·|N|`
/// ([`KernelNormalisation::KernelSum`]); the pointing ramp applies to
/// every row ([`ConvolutionFunctionSet::pointing_ramp`]). A row whose
/// parallactic angle lies between cells takes the nearest cell; CASA would
/// rotate it (`AWConvFunc::rotateCF`, `rotatepastep`), which the frozen
/// 360° steps of the EVLA cache make the same choice.
///
/// # Panics
///
/// [`ConvolutionFunctionSet::taps`] and [`ConvolutionFunctionSet::weight_taps`]
/// are infallible: a cell image that was validated at open but cannot be
/// read when a row first needs it is a catalog broken under the run, and
/// the catalog panics with the cell's path rather than gridding without it.
pub struct AwCatalog {
    root: PathBuf,
    indexing: AwIndexing,
    pa_deg: Vec<f64>,
    frequencies_hz: Vec<f64>,
    w_values: Vec<f64>,
    w_increment: f64,
    mueller_elements: Vec<u32>,
    groups: Vec<Group>,
    oversampling: u16,
    max_half_support: [u16; 2],
    mueller: MuellerRouting,
    correction: ImageCorrection,
    lru: Mutex<Lru>,
}

impl std::fmt::Debug for AwCatalog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AwCatalog")
            .field("root", &self.root)
            .field("pa_deg", &self.pa_deg)
            .field("frequencies_hz", &self.frequencies_hz)
            .field("w_values", &self.w_values)
            .field("mueller_elements", &self.mueller_elements)
            .finish_non_exhaustive()
    }
}

impl AwCatalog {
    /// Open the cache at `root`: read every `CFS_*`/`WTCFS_*` image's
    /// coordinates and misc info (`CFCache::fillCFListFromDisk`), sort the
    /// unique parallactic angles, frequencies, w values and Mueller
    /// elements, and build the routing of `polarization`'s correlations
    /// over `geometry`'s grid. Pixels are read on demand, at most
    /// `bound_bytes` of cells resident.
    ///
    /// # Errors
    ///
    /// The directory cannot be listed, a cell is unreadable or malformed,
    /// a cell lacks its partner, a (PA, frequency, w) group lacks a Mueller
    /// element, or the catalog has no element for a selected correlation.
    pub fn open_casa(
        root: impl AsRef<Path>,
        indexing: AwIndexing,
        geometry: &GridGeometry,
        polarization: &PolarizationRouting,
        bound_bytes: usize,
    ) -> Result<Self, AwCatalogError> {
        let root = root.as_ref().to_path_buf();
        let entries = std::fs::read_dir(&root)
            .map_err(|error| cache_error(&root, format!("cannot list: {error}")))?;
        let mut imaging = HashMap::new();
        let mut weight = HashMap::new();
        for entry in entries {
            let entry =
                entry.map_err(|error| cache_error(&root, format!("cannot list: {error}")))?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if let Some(rest) = name.strip_prefix(WEIGHT_PREFIX) {
                weight.insert(rest.to_string(), read_header(&entry.path())?);
            } else if let Some(rest) = name.strip_prefix(IMAGING_PREFIX) {
                imaging.insert(rest.to_string(), read_header(&entry.path())?);
            }
        }
        if imaging.is_empty() {
            return Err(cache_error(&root, "no CFS_ cell images"));
        }
        // A native cell's working field is the image field times the
        // oversampling: `working_size · working_increment = sampling · n · Δx`.
        let [grid_n, _] = geometry.grid_shape();
        let image_increment = geometry.image().increment_rad[0].abs();
        for header in imaging.values() {
            let Some(working_field) = header.working_field_rad else {
                continue;
            };
            let expected = image_increment * f64::from(header.sampling) * grid_n as f64;
            if (working_field - expected).abs() > 1.0e-6 * expected {
                return Err(cache_error(
                    &header.path,
                    format!(
                        "generated for a working field of {working_field} rad, the image needs \
                         {expected} rad: the cache belongs to another image geometry"
                    ),
                ));
            }
        }
        let mut cells = Vec::with_capacity(imaging.len());
        for (rest, header) in imaging {
            let partner = weight
                .remove(&rest)
                .ok_or_else(|| cache_error(&header.path, "paired WTCFS_ cell is missing"))?;
            if partner.mueller != header.mueller
                || partner.w_value.to_bits() != header.w_value.to_bits()
                || partner.pa_deg.to_bits() != header.pa_deg.to_bits()
                || partner.sampling != header.sampling
            {
                return Err(cache_error(
                    &partner.path,
                    "WTCFS_ cell disagrees with its CFS_ cell",
                ));
            }
            cells.push(CellFiles {
                imaging: header,
                weight: partner,
            });
        }
        if let Some((_, orphan)) = weight.into_iter().next() {
            return Err(cache_error(&orphan.path, "paired CFS_ cell is missing"));
        }
        let unique = |values: Vec<f64>| {
            let mut values = values;
            values.sort_by(f64::total_cmp);
            values.dedup_by(|a, b| a.to_bits() == b.to_bits());
            values
        };
        let pa_deg = unique(cells.iter().map(|cell| cell.imaging.pa_deg).collect());
        let frequencies_hz = unique(cells.iter().map(|cell| cell.imaging.frequency_hz).collect());
        let w_values = unique(cells.iter().map(|cell| cell.imaging.w_value).collect());
        let mut mueller_elements = cells
            .iter()
            .map(|cell| cell.imaging.mueller)
            .collect::<Vec<_>>();
        mueller_elements.sort_unstable();
        mueller_elements.dedup();
        let w_increment = cells[0].imaging.w_increment;
        let oversampling = cells[0].imaging.sampling;
        if cells.iter().any(|cell| {
            cell.imaging.w_increment.to_bits() != w_increment.to_bits()
                || cell.imaging.sampling != oversampling
        }) {
            return Err(cache_error(&root, "cells disagree on WIncr or Sampling"));
        }
        let index_of = |values: &[f64], value: f64| {
            values
                .iter()
                .position(|listed| listed.to_bits() == value.to_bits())
                .expect("listed value")
        };
        let groups_len = pa_deg.len() * frequencies_hz.len() * w_values.len();
        let mut groups: Vec<Vec<Option<CellFiles>>> = (0..groups_len)
            .map(|_| vec![None; mueller_elements.len()])
            .collect();
        let mut max_half_support = [0_u16; 2];
        for cell in cells {
            let group = (index_of(&pa_deg, cell.imaging.pa_deg) * frequencies_hz.len()
                + index_of(&frequencies_hz, cell.imaging.frequency_hz))
                * w_values.len()
                + index_of(&w_values, cell.imaging.w_value);
            let plane = mueller_elements
                .binary_search(&cell.imaging.mueller)
                .expect("listed element");
            if groups[group][plane].is_some() {
                return Err(cache_error(&cell.imaging.path, "duplicate cell key"));
            }
            for (axis, largest) in max_half_support.iter_mut().enumerate() {
                *largest = (*largest)
                    .max(cell.imaging.support[axis])
                    .max(cell.weight.support[axis]);
            }
            groups[group][plane] = Some(cell);
        }
        let groups = groups
            .into_iter()
            .map(|planes| {
                let cells = planes
                    .into_iter()
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(|| {
                        cache_error(&root, "a (PA, frequency, w) group lacks a Mueller element")
                    })?;
                Ok(Group::new(cells))
            })
            .collect::<Result<Vec<_>, AwCatalogError>>()?;
        let mueller = routing(&mueller_elements, polarization).ok_or_else(|| {
            cache_error(
                &root,
                "selected correlations have missing or overlapping Mueller routes",
            )
        })?;
        let [nx, ny] = geometry.grid_shape();
        // `AWProjectFT::getImage` and `getWeightImage` divide every image
        // by the sampling sinc `sin(x)/x`, `x = π(i − n/2)/(n·sampling)`;
        // the spheroidal lives in the cells under `psterm`, and
        // `initializeToVis` leaves the model uncorrected (its `sincConv`
        // is reset to one).
        let sampling_correction = |len: usize| {
            (0..len)
                .map(|index| {
                    if index == len / 2 {
                        return 1.0;
                    }
                    let x = std::f64::consts::PI * (index as f64 - (len / 2) as f64)
                        / (len as f64 * f64::from(oversampling));
                    x / x.sin()
                })
                .collect::<Vec<_>>()
        };
        Ok(Self {
            root,
            indexing,
            pa_deg,
            frequencies_hz,
            w_values,
            w_increment,
            mueller_elements,
            groups,
            oversampling,
            max_half_support,
            mueller,
            correction: ImageCorrection::split(
                sampling_correction(nx),
                sampling_correction(ny),
                vec![1.0; nx],
                vec![1.0; ny],
            ),
            lru: Mutex::new(Lru {
                loaded: HashMap::new(),
                clock: 0,
                bytes: 0,
                bound: bound_bytes,
            }),
        })
    }

    /// Generate every cell of `input` natively into `root` as CASA-format
    /// images (`CFS_<pa>_0_CF_<f>_<w>_<m>.im` and the `WTCFS_` partner, one
    /// baseline type) so [`AwCatalog::open_casa`] serves them, and record
    /// `input` in the directory's request manifest
    /// ([`AwCatalog::native_cache_state`]). Existing cells of the same
    /// names are replaced; with `only_missing`, cells already present are
    /// kept.
    ///
    /// # Errors
    ///
    /// Generation fails or an image cannot be written, or `only_missing`
    /// is asked of a directory that holds cells of another request
    /// ([`NativeCacheState::fillable`]): its present cells would be served
    /// beside cells generated for `input`.
    pub fn generate_native(
        root: impl AsRef<Path>,
        input: &NativeAwRequestInput,
        only_missing: bool,
    ) -> Result<(), AwCatalogError> {
        let root = root.as_ref();
        if only_missing {
            let state = Self::native_cache_state(root, input)?;
            if !state.fillable() {
                return Err(cache_error(
                    root,
                    format!("cannot fill in a cache of another request: {state}"),
                ));
            }
        }
        std::fs::create_dir_all(root)
            .map_err(|error| cache_error(root, format!("cannot create: {error}")))?;
        write_manifest(root, input)?;
        let model = EvlaApertureModel::new(input.surface.clone());
        let grid = input.grid;
        let mut workspace: Option<EvlaAwWorkspace> = None;
        for (ipa, &pa) in input.pa_values.iter().enumerate() {
            for (inu, group) in input.frequencies.iter().enumerate() {
                let frequency = group.cf_frequency_hz;
                let conjugate = conjugate_frequency(frequency, input.reference_frequency_hz);
                // `AWConvFunc::makeConvFunction2`: the conjugate beam uses the
                // nearest listed frequency to `√(2f₀² − f²)`, the first on a tie
                // (`SynthesisUtils::nearestValue`).
                let conjugate_frequency = if input.terms.conjugate_beams {
                    nearest(
                        &input
                            .frequencies
                            .iter()
                            .map(|group| group.cf_frequency_hz)
                            .collect::<Vec<_>>(),
                        conjugate,
                    )
                    .1
                } else {
                    frequency
                };
                // Only the W screen depends on w: the aperture and the weight
                // cell are formed once per (PA, frequency, Mueller element).
                for (im, &mueller) in input.mueller_elements.iter().enumerate() {
                    for (iw, &w) in input.w_values.iter().enumerate() {
                        let suffix = cell_suffix(ipa, inu, iw, im);
                        let imaging_path = root.join(format!("{IMAGING_PREFIX}{suffix}"));
                        let weight_path = root.join(format!("{WEIGHT_PREFIX}{suffix}"));
                        if only_missing && imaging_path.is_dir() && weight_path.is_dir() {
                            continue;
                        }
                        let request = EvlaAwCellRequest {
                            size: grid.size,
                            sky_increment_rad: grid.sky_increment_rad,
                            frequency_hz: frequency,
                            conjugate_frequency_hz: conjugate_frequency,
                            w_wavelengths: w,
                            parallactic_angle_rad: pa,
                            mueller,
                            oversampling: grid.oversampling,
                            prolate_spheroidal: input.terms.prolate_spheroidal,
                            aperture: input.terms.aperture,
                        };
                        let workspace = match &mut workspace {
                            Some(workspace) => workspace,
                            None => workspace.insert(EvlaAwWorkspace::new(model.clone(), request)?),
                        };
                        workspace.prepare(request)?;
                        let pair = workspace.realize(request)?;
                        let misc = CellMisc {
                            sampling: grid.oversampling as f64,
                            pa_deg: pa.to_degrees(),
                            mueller,
                            w_value: w,
                            w_increment: input.w_increment,
                            conjugate_frequency,
                            conjugate_mueller: MUELLER_ELEMENTS - 1 - mueller as u32,
                            diameter_m: input.antenna_diameter_m,
                            sky_increment_rad: grid.sky_increment_rad[1].abs(),
                            working_size: grid.size,
                        };
                        write_cell(&imaging_path, &pair.imaging, frequency, grid, &misc)?;
                        write_cell(&weight_path, &pair.weight, frequency, grid, &misc)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// The cache directory.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Sorted unique parallactic angles of the cells, in degrees.
    #[must_use]
    pub fn parallactic_angles_deg(&self) -> &[f64] {
        &self.pa_deg
    }

    /// Sorted unique cell frequencies in Hz.
    #[must_use]
    pub fn frequencies_hz(&self) -> &[f64] {
        &self.frequencies_hz
    }

    /// Sorted unique w values in wavelengths.
    #[must_use]
    pub fn w_values(&self) -> &[f64] {
        &self.w_values
    }

    /// `WIncr`: `w_index = clamp(round(√(wIncr·|w|)), 0, nW−1)`.
    #[must_use]
    pub const fn w_increment(&self) -> f64 {
        self.w_increment
    }

    /// Sorted unique Mueller elements, the plane order inside every cell.
    #[must_use]
    pub fn mueller_elements(&self) -> &[u32] {
        &self.mueller_elements
    }

    /// Fine offsets per cell (`Sampling`).
    #[must_use]
    pub const fn oversampling(&self) -> u16 {
        self.oversampling
    }

    /// Number of (PA, frequency, w) groups.
    #[must_use]
    pub fn groups(&self) -> usize {
        self.groups.len()
    }

    /// Bytes of cells resident in the cache now.
    #[must_use]
    pub fn resident_bytes(&self) -> usize {
        self.lru.lock().expect("cache lock").bytes
    }

    /// Bytes the catalog holds at most while it lives with `holders`
    /// workers fetching cells: its index and corrections; the cache's bound
    /// and one group past it, which a fetch inserts before the cache
    /// evicts; and for each holder the largest group, which its
    /// [`CellHold`] keeps alive after eviction, and the planes it reads to
    /// build one (each read as an image array and its copy).
    #[must_use]
    pub fn charge_bytes(&self, holders: usize) -> u64 {
        let header = |header: &CellHeader| size_of::<CellHeader>() + header.path.as_os_str().len();
        let index = self.groups.capacity() * size_of::<Group>()
            + self
                .groups
                .iter()
                .flat_map(|group| &group.cells)
                .map(|cell| header(&cell.imaging) + header(&cell.weight))
                .sum::<usize>()
            + (self.pa_deg.len() + self.frequencies_hz.len() + self.w_values.len())
                * size_of::<f64>()
            + self.mueller_elements.len() * size_of::<u32>();
        let group = |group: &Group| {
            let planes = group.cells.len();
            dense_cell_bytes(planes, self.oversampling, group.imaging_half_support)
                + dense_cell_bytes(planes, self.oversampling, group.weight_half_support)
        };
        let read = |group: &Group| {
            let plane = |header: &CellHeader| header.shape[0] * header.shape[1];
            let imaging = group
                .cells
                .iter()
                .map(|cell| plane(&cell.imaging))
                .sum::<usize>();
            let weight = group
                .cells
                .iter()
                .map(|cell| plane(&cell.weight))
                .sum::<usize>();
            (2 * imaging.max(weight) * size_of::<Complex32>()) as u64
        };
        let largest = self.groups.iter().map(group).max().unwrap_or(0);
        let reading = self.groups.iter().map(read).max().unwrap_or(0);
        let bound = self.lru.lock().expect("cache lock").bound as u64;
        index as u64
            + self.correction.resident_bytes()
            + bound
            + (holders as u64 + 1) * largest
            + holders as u64 * reading
    }

    /// The parallactic-angle cell of `pa_deg`: the nearest listed angle on
    /// the circle (`CFCache::fillCFListFromDisk` selects cells within `dPA`
    /// and `SynthesisUtils::stdNearestValue` picks among them).
    #[must_use]
    pub fn pa_cell(&self, pa_deg: f64) -> usize {
        self.pa_deg
            .iter()
            .enumerate()
            .fold((0, f64::INFINITY), |best, (index, listed)| {
                let distance = circular_degrees(pa_deg - listed).abs();
                if distance < best.1 {
                    (index, distance)
                } else {
                    best
                }
            })
            .0
    }

    /// The frequency cell of a row at `freq_hz`: the nearest listed
    /// frequency to `√(2f₀² − f²)` under `conjbeams`, to `f` otherwise
    /// (`CFBuffer::initMaps`, `nearestFreqNdx`).
    #[must_use]
    pub fn frequency_cell(&self, freq_hz: f64) -> usize {
        let target = if self.indexing.conjugate_beams {
            conjugate_frequency(freq_hz, self.indexing.image_reference_hz)
        } else {
            freq_hz
        };
        nearest(&self.frequencies_hz, target).0
    }

    /// The w-plane of `w_lambda` (`CFBuffer::nearestWNdx`, CAS-13191).
    #[must_use]
    pub fn w_cell(&self, w_lambda: f64) -> usize {
        let index = (self.w_increment * w_lambda.abs()).sqrt().round() as usize;
        index.min(self.w_values.len() - 1)
    }

    /// The group of a (PA, frequency, w) cell triple.
    #[must_use]
    pub fn group_index(&self, pa: usize, frequency: usize, w: usize) -> usize {
        (pa * self.frequencies_hz.len() + frequency) * self.w_values.len() + w
    }

    /// Load group `group`'s cells, or fetch them from the cache, and park
    /// them in `hold`.
    fn loaded(&self, group: usize) -> (Arc<DenseCell>, Arc<DenseCell>) {
        let mut lru = self.lru.lock().expect("cache lock");
        lru.clock += 1;
        let now = lru.clock;
        if let Some(loaded) = lru.loaded.get_mut(&group) {
            loaded.last_use = now;
            return (loaded.imaging.clone(), loaded.weight.clone());
        }
        let mut loaded = load_group(&self.groups[group], self.oversampling)
            .unwrap_or_else(|error| panic!("AW cell group {group} cannot be read: {error}"));
        loaded.last_use = now;
        let pair = (loaded.imaging.clone(), loaded.weight.clone());
        lru.bytes += loaded.bytes();
        lru.loaded.insert(group, loaded);
        while lru.bytes > lru.bound && lru.loaded.len() > 1 {
            let oldest = lru
                .loaded
                .iter()
                .min_by_key(|(_, loaded)| loaded.last_use)
                .map(|(group, _)| *group)
                .expect("a loaded group");
            if let Some(cell) = lru.loaded.remove(&oldest) {
                lru.bytes -= cell.bytes();
            }
        }
        pair
    }
}

impl ConvolutionFunctionSet for AwCatalog {
    /// Group: the (PA, frequency, w) cell; cube 0. The parallactic angle is
    /// CASA's visibility polarization operator angle negated back to the
    /// physical angle in degrees, as the cells record it.
    /// `group` is the gridding cell (the conjugate-frequency cell under
    /// `conjbeams`, `DataToGrid` with `conjBeams`); `cube` the cell a
    /// prediction reads: the native frequency's (`GridToData`,
    /// `nearestFreqNdx(spw, chan)`) at the w-plane of the unrotated w
    /// ([`Self::prediction_w`]).
    fn key(&self, row: &RowContext, freq_hz: f64, w_lambda: f64) -> CfKey {
        let pa_deg = (-row.parallactic_angle_rad[0]).to_degrees();
        let pa = self.pa_cell(pa_deg);
        let w = self.w_cell(w_lambda);
        let group = self.group_index(pa, self.frequency_cell(freq_hz), w);
        let prediction_w = self.w_cell(self.prediction_w(row, freq_hz, w_lambda));
        let native = self.group_index(pa, nearest(&self.frequencies_hz, freq_hz).0, prediction_w);
        CfKey {
            group: u16::try_from(group).expect("AW group fits u16"),
            cube: u16::try_from(native).expect("AW group fits u16"),
        }
    }

    fn taps<'s>(&'s self, key: CfKey, hold: &'s mut CellHold) -> TapLayout<'s> {
        hold.lend_keyed(key, 0, || self.loaded(usize::from(key.group)).0)
    }

    fn max_half_support(&self) -> [u16; 2] {
        self.max_half_support
    }

    fn weight_taps<'s>(&'s self, key: CfKey, hold: &'s mut CellHold) -> Option<TapLayout<'s>> {
        Some(hold.lend_keyed(key, 1, || self.loaded(usize::from(key.group)).1))
    }

    /// `AWProjectFT::findConvFunction` maps `cfwts2_p` when `makingPSF`:
    /// the PSF is gridded with the weight cell at the sample's uv position.
    fn psf_taps<'s>(&'s self, key: CfKey, hold: &'s mut CellHold) -> TapLayout<'s> {
        hold.lend_keyed(key, 1, || self.loaded(usize::from(key.group)).1)
    }

    fn prediction_taps<'s>(&'s self, key: CfKey, hold: &'s mut CellHold) -> TapLayout<'s> {
        hold.lend_keyed(key, 2, || self.loaded(usize::from(key.cube)).0)
    }

    /// The smallest support of the cells the key can select, over the
    /// gridding group (imaging and weight cells) and the prediction group
    /// (its native-frequency cell). The data gridding, the PSF and weight
    /// image, and the predictor each re-test the support of the cell they
    /// read (`AWVisResampler::DataToGrid` and `GridToData` call `onGrid`
    /// with the selected cell's), so a row the imaging cell overruns is
    /// still gridded into the PSF and weight image, or predicted, when its
    /// smaller cell fits.
    fn placement_half_support(&self, key: CfKey, _hold: &mut CellHold) -> [u16; 2] {
        placement_half_support(
            &self.groups[usize::from(key.group)],
            &self.groups[usize::from(key.cube)],
        )
    }

    fn mueller(&self) -> &MuellerRouting {
        &self.mueller
    }

    /// `GridToData` keys its w-plane and tap conjugation on
    /// `vb_p->uvw()(2, irow)`, the MeasurementSet w before the
    /// phase-centre rotation, while `DataToGrid` reads the rotated
    /// `uvw_p` (#667).
    fn prediction_w(&self, row: &RowContext, freq_hz: f64, w_lambda: f64) -> f64 {
        row.original_uvw_m.map_or(w_lambda, |uvw_m| {
            uvw_m[2] * freq_hz / SPEED_OF_LIGHT_M_PER_S
        })
    }

    fn image_correction(&self) -> &ImageCorrection {
        &self.correction
    }

    /// `AWVisResampler::DataToGrid`: `sumwt += W·|norm|`; `GridToData`
    /// divides by the summed norm once.
    fn normalisation(&self) -> KernelNormalisation {
        KernelNormalisation::KernelSum
    }

    fn pointing_ramp(&self) -> bool {
        true
    }

    /// `AWProjectWBFT::makeWBSensitivityImage` publishes `abs` of the
    /// polarization-averaged weight transform.
    fn weight_image_magnitude(&self) -> bool {
        true
    }
}

/// `AWVisResampler`'s routes through diagonal Mueller cells. Each selected
/// correlation (the `outer` loop of `DataToGrid` and `GridToData`) has the
/// element `e` of its own hand (`RR, RL, LR, LL` → `0, 5, 10, 15`,
/// `AWProjectFT::makeCFPolMap`) and the partner `15 − e` (`RR ↔ LL`,
/// `RL ↔ LR`), or `e` again when the partner hand is not a selected
/// correlation (`PolOuterProduct::makeConjPol2CFMat` keeps the direct
/// entry). `getConvFunc_p` selects the own cell for `w > 0` and the
/// partner's otherwise in the adjoint, the reverse in the forward
/// transform (`GridToData` passes `mNdx` and `conjMNdx` swapped); the hand
/// of the selected cell is `element % npol`, CASA's index into the
/// visibility axis, so
///
/// `DataToGrid`: `grid[pol_map[outer]] += vis[hand(cell)] ⋆ cell`,
/// `GridToData`: `vis[outer] += model[pol_map[hand(cell)]] ⋆ cell`.
///
/// `None` when a cell is missing or two outer correlations route one
/// (grid, visibility) pair through different cells (a non-canonical
/// three-correlation selection, where `element % npol` collides).
fn routing(elements: &[u32], polarization: &PolarizationRouting) -> Option<MuellerRouting> {
    let pol_map = polarization.pol_map();
    let correlations = polarization.correlations();
    let plane_of = |element: u32| {
        elements
            .binary_search(&element)
            .ok()
            .map(|plane| u8::try_from(plane).expect("planes fit u8"))
    };
    let selected = |element: u32| {
        correlations
            .iter()
            .zip(pol_map)
            .any(|(correlation, target)| {
                target.is_some() && mueller_element(*correlation) == Some(element)
            })
    };
    let hand = |element: u32| element as usize % pol_map.len();
    let empty = || vec![vec![None; pol_map.len()]; polarization.grid_pols()];
    let mut adjoint = [empty(), empty()];
    let mut forward = [empty(), empty()];
    for (outer, (correlation, target)) in correlations.iter().zip(pol_map).enumerate() {
        let Some(gpol) = *target else {
            continue;
        };
        let own = mueller_element(*correlation)?;
        let partner = Some(MUELLER_ELEMENTS - 1 - own)
            .filter(|partner| selected(*partner))
            .unwrap_or(own);
        for positive in [false, true] {
            let cell = if positive { own } else { partner };
            let plane = plane_of(cell)?;
            if adjoint[usize::from(positive)][usize::from(gpol)][hand(cell)]
                .replace(plane)
                .is_some()
            {
                return None;
            }
            if let Some(input_grid) = pol_map[hand(cell)] {
                forward[usize::from(!positive)][usize::from(input_grid)][outer] = Some(plane);
            }
        }
    }
    Some(MuellerRouting { adjoint, forward })
}

/// The diagonal Mueller element of a parallel or cross hand.
fn mueller_element(correlation: CorrelationType) -> Option<u32> {
    Some(match correlation {
        CorrelationType::CircularRr | CorrelationType::LinearXx => 0,
        CorrelationType::CircularRl | CorrelationType::LinearXy => 5,
        CorrelationType::CircularLr | CorrelationType::LinearYx => 10,
        CorrelationType::CircularLl | CorrelationType::LinearYy => 15,
        _ => return None,
    })
}

/// `SynthesisUtils::conjFreq`: `√(2f₀² − f²)`.
fn conjugate_frequency(frequency_hz: f64, reference_hz: f64) -> f64 {
    (2.0 * reference_hz * reference_hz - frequency_hz * frequency_hz).sqrt()
}

/// `SynthesisUtils::nearestValue`: the first listed value at the smallest
/// distance.
fn nearest(values: &[f64], target: f64) -> (usize, f64) {
    let mut best = (0, f64::INFINITY);
    for (index, value) in values.iter().enumerate() {
        let distance = (value - target).abs();
        if distance < best.1 {
            best = (index, distance);
        }
    }
    (best.0, values[best.0])
}

fn circular_degrees(value: f64) -> f64 {
    (value + 180.0).rem_euclid(360.0) - 180.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_complex::Complex64;

    #[test]
    fn casa_routes_the_selected_cell_to_its_visibility_or_grid_hand() {
        use CorrelationType as C;
        use casa_imaging_model::PolarizationCoordinate as P;

        let parallel = [C::CircularRr, C::CircularLl];
        let pol = PolarizationRouting::compile(&parallel, &[P::CircularRr, P::CircularLl])
            .expect("parallel hands");
        let routes = routing(&[0, 15], &pol).expect("routes");
        let diagonal = vec![vec![Some(0), None], vec![None, Some(1)]];
        // `DataToGrid` reads the visibility of the selected cell's hand
        // (`muellerElement % nDataPol`), `GridToData` the model grid of
        // that hand; the outer loop names the output in both.
        assert_eq!(routes.table(true, false), &diagonal);
        assert_eq!(routes.table(false, true), &diagonal);
        assert_eq!(
            routes.table(false, false),
            &[vec![None, Some(1)], vec![Some(0), None]]
        );
        assert_eq!(
            routes.table(true, true),
            &[vec![None, Some(0)], vec![Some(1), None]]
        );

        // PolOuterProduct::makeConjPol2CFMat: an inactive partner leaves
        // the direct mapping in place, even if the cache has both cells.
        for (requested, cells, expected) in [
            (P::CircularRr, vec![0, 15], vec![Some(0), None]),
            (P::CircularLl, vec![0, 15], vec![None, Some(1)]),
            (P::CircularRr, vec![0], vec![Some(0), None]),
        ] {
            let pol = PolarizationRouting::compile(&parallel, &[requested]).expect("one hand");
            let routes = routing(&cells, &pol).expect("one-hand routes");
            for positive in [false, true] {
                for forward in [false, true] {
                    assert_eq!(
                        routes.table(positive, forward),
                        std::slice::from_ref(&expected)
                    );
                }
            }
        }

        let correlations = [C::CircularRr, C::CircularRl, C::CircularLr, C::CircularLl];
        let pol = PolarizationRouting::compile(
            &correlations,
            &[P::CircularRr, P::CircularRl, P::CircularLr, P::CircularLl],
        )
        .expect("four hands");
        let routes = routing(&[0, 5, 10, 15], &pol).expect("four-hand routes");
        for gpol in 0..4 {
            for vpol in 0..4 {
                assert_eq!(
                    routes.table(true, false)[gpol][vpol],
                    (gpol == vpol).then_some(vpol as u8)
                );
                assert_eq!(
                    routes.table(false, false)[gpol][vpol],
                    (gpol + vpol == 3).then_some(vpol as u8)
                );
                assert_eq!(
                    routes.table(true, true)[gpol][vpol],
                    (gpol + vpol == 3).then_some(gpol as u8)
                );
                assert_eq!(
                    routes.table(false, true)[gpol][vpol],
                    (gpol == vpol).then_some(gpol as u8)
                );
            }
        }
        let pol = PolarizationRouting::compile(
            &[C::CircularRr, C::CircularRl, C::CircularLl],
            &[P::StokesI],
        )
        .expect("three correlations");
        assert!(
            routing(&[0, 15], &pol).is_none(),
            "do not overwrite colliding routes"
        );
    }

    #[test]
    fn the_row_filter_keeps_every_row_the_smallest_selectable_cell_fits() {
        let header = |support: [u16; 2]| CellHeader {
            path: PathBuf::new(),
            shape: [16, 16],
            sampling: 1,
            support,
            mueller: 0,
            w_value: 0.0,
            working_field_rad: None,
            w_increment: 0.0,
            pa_deg: 0.0,
            frequency_hz: 1.0e9,
        };
        let group = |cells: &[([u16; 2], [u16; 2])]| {
            Group::new(
                cells
                    .iter()
                    .map(|(imaging, weight)| CellFiles {
                        imaging: header(*imaging),
                        weight: header(*weight),
                    })
                    .collect(),
            )
        };
        // Imaging 6 over weight 4: the PSF and weight image keep rows the
        // imaging cell overruns, so the filter admits down to the weight
        // cell; each mode's own `onGrid` test does the rest.
        let wide_imaging = group(&[([6, 6], [4, 4]), ([5, 6], [4, 3])]);
        assert_eq!(wide_imaging.imaging_half_support, [6, 6]);
        assert_eq!(wide_imaging.weight_half_support, [4, 4]);
        assert_eq!(placement_half_support(&wide_imaging, &wide_imaging), [4, 3]);
        // Weight 6 over imaging 4: the data gridding keeps rows the weight
        // cell overruns.
        let wide_weight = group(&[([4, 4], [6, 6])]);
        assert_eq!(placement_half_support(&wide_weight, &wide_weight), [4, 4]);
        // A native-frequency prediction cell narrower than every gridding
        // cell: `GridToData` predicts rows `DataToGrid` drops.
        let narrow = group(&[([3, 2], [7, 7])]);
        assert_eq!(placement_half_support(&wide_weight, &narrow), [3, 2]);
        assert_eq!(placement_half_support(&narrow, &wide_weight), [3, 2]);
    }

    #[test]
    fn index_rules_follow_casa() {
        // nearestWNdx: round(sqrt(wIncr·|w|)) clamped.
        let catalog_w = |w_increment: f64, planes: usize, w: f64| {
            let index = (w_increment * w.abs()).sqrt().round() as usize;
            index.min(planes - 1)
        };
        assert_eq!(catalog_w(4.0, 5, 0.0), 0);
        assert_eq!(catalog_w(4.0, 5, 0.06), 0); // sqrt(0.24) = 0.49
        assert_eq!(catalog_w(4.0, 5, 0.07), 1); // sqrt(0.28) = 0.53
        assert_eq!(catalog_w(4.0, 5, 100.0), 4);
        // nearestValue: first at the smallest distance.
        assert_eq!(nearest(&[1.0, 2.0, 3.0], 2.5), (1, 2.0));
        assert_eq!(nearest(&[1.0, 2.0, 3.0], 2.6), (2, 3.0));
        // conjFreq.
        assert!((conjugate_frequency(1.0e9, 1.5e9) - (3.5e18_f64).sqrt()).abs() < 1.0);
        assert_eq!(circular_degrees(350.0 - 10.0), -20.0);
        assert_eq!(mueller_element(CorrelationType::CircularLl), Some(15));
        assert_eq!(mueller_element(CorrelationType::LinearXy), Some(5));
    }

    #[test]
    fn padded_cells_read_the_oversampled_plane_and_zero_outside() {
        // 8×8 plane, origin (4, 4), sampling 2, half 1: tap k at offset
        // off reads pixel 4 + 2k + off; a 4×4 second plane is padded.
        let side = 8;
        let values = (0..side * side)
            .map(|index| Complex32::new((index % side) as f32, (index / side) as f32))
            .collect::<Vec<_>>();
        let small = (0..16)
            .map(|index| Complex32::new(100.0 + (index % 4) as f32, (index / 4) as f32))
            .collect::<Vec<_>>();
        let cell = dense_cell_padded(
            &[(values, [side, side], [1, 1]), (small, [4, 4], [1, 1])],
            [1, 1],
            2,
        );
        assert_eq!(cell.support, [3, 3]);
        assert_eq!(cell.mueller_planes, 2);
        let tile = 9;
        // Row (ox, oy) = (0, 2): off = (−1, +1); plane 0 tap (2, 0): pixel
        // (4 + 2 − 1, 4 − 2 + 1) = (5, 3).
        let row = (2 * 3) * 2 * tile;
        assert_eq!(cell.data[row + 2], Complex32::new(5.0, 3.0));
        // Plane 1 in the same row, tap (2, 0): pixel (2 + 2 − 1, 2 − 2 + 1)
        // = (3, 1) of the small plane; tap (2, 2): y = 2 + 2 + 1 = 5 → outside.
        assert_eq!(cell.data[row + tile + 2], Complex32::new(103.0, 1.0));
        assert_eq!(cell.data[row + tile + 8], Complex32::default());
    }

    #[test]
    fn the_area_sums_the_integer_taps_over_the_half_open_support() {
        let shape = [12, 12];
        let plane = (0..144)
            .map(|index| Complex32::new(1.0, if index == 6 * 12 + 6 { 1.0 } else { 0.0 }))
            .collect::<Vec<_>>();
        // support 2, sampling 2: ix, iy ∈ {−2, −1, 0, 1} → 16 taps.
        let area = cf_area(&plane, shape, [2, 2], 2);
        assert_eq!(area, Complex64::new(16.0, 1.0));
    }
}
