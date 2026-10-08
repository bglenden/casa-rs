// SPDX-License-Identifier: LGPL-3.0-or-later
//! The AW-projection kernel set: a catalog of CASA `CFS_*`/`WTCFS_*`
//! convolution-function images read header-first at open and cell by cell
//! on demand into a bounded cache, with the LibRA/CASA index rules pinned
//! (`TransformMachines2/{CFCache,CFBuffer,AWVisResampler,AWProjectFT}.cc`),
//! and native EVLA generation that writes the same images
//! (`AWConvFunc::makeConvFunction2`, `CFCell::makePersistent`).

mod evla;
mod native;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use casa_coordinates::{
    CoordinateSystem, CoordinateType, LinearCoordinate, SpectralCoordinate, StokesCoordinate,
    StokesType,
};
use casa_images::PagedImage;
use casa_imaging_model::{CorrelationType, EvlaAwCellRequest, NativeAwRequestInput};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::{RecordField, RecordValue, ScalarValue, Value};
use num_complex::{Complex32, Complex64};
use thiserror::Error;

pub use evla::{EvlaApertureGrid, EvlaApertureModel};
pub use native::{EvlaAwWorkspace, NativeAwPair, NativeAwPlane};

use crate::convolution::{
    CellHold, ConvolutionFunctionSet, DenseCell, ImageCorrection, KernelNormalisation,
    MuellerRouting, RowContext, TapLayout,
};
use crate::geometry::GridGeometry;
use crate::polarization::PolarizationRouting;
use crate::sample::CfKey;

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
/// as planes, routed by [`MuellerRouting`] with CASA's swapped tables for
/// the conjugate baseline (`AWProjectFT::makeConjPolMap`). Prediction
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
                "the catalog lacks a Mueller element of a selected correlation",
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
    /// baseline type) so [`AwCatalog::open_casa`] serves them. Existing
    /// cells of the same names are replaced.
    ///
    /// # Errors
    ///
    /// Generation fails or an image cannot be written.
    pub fn generate_native(
        root: impl AsRef<Path>,
        input: &NativeAwRequestInput,
        only_missing: bool,
    ) -> Result<(), AwCatalogError> {
        let root = root.as_ref();
        std::fs::create_dir_all(root)
            .map_err(|error| cache_error(root, format!("cannot create: {error}")))?;
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
                for (iw, &w) in input.w_values.iter().enumerate() {
                    for (im, &mueller) in input.mueller_elements.iter().enumerate() {
                        let suffix = format!("{ipa}_0_CF_{inu}_{iw}_{im}.im");
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
                            None => workspace.insert(EvlaAwWorkspace::new(request)?),
                        };
                        let pair = workspace.generate(&model, request)?;
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

    /// How many of the cells `input` names exist under `root` (both the
    /// `CFS_` and `WTCFS_` images), and how many it names.
    pub fn native_cells_present(
        root: impl AsRef<Path>,
        input: &NativeAwRequestInput,
    ) -> (usize, usize) {
        let root = root.as_ref();
        let mut present = 0;
        let mut expected = 0;
        for ipa in 0..input.pa_values.len() {
            for inu in 0..input.frequencies.len() {
                for iw in 0..input.w_values.len() {
                    for im in 0..input.mueller_elements.len() {
                        let suffix = format!("{ipa}_0_CF_{inu}_{iw}_{im}.im");
                        expected += 1;
                        if root.join(format!("{IMAGING_PREFIX}{suffix}")).is_dir()
                            && root.join(format!("{WEIGHT_PREFIX}{suffix}")).is_dir()
                        {
                            present += 1;
                        }
                    }
                }
            }
        }
        (present, expected)
    }

    /// Remove every `CFS_` and `WTCFS_` cell under `root`, so a regenerated
    /// catalog carries no member of an earlier request.
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
    /// `conjbeams`, `DataToGrid` with `conjBeams`); `cube` the
    /// native-frequency cell a prediction reads (`GridToData`,
    /// `nearestFreqNdx(spw, chan)`).
    fn key(&self, row: &RowContext, freq_hz: f64, w_lambda: f64) -> CfKey {
        let pa_deg = (-row.parallactic_angle_rad[0]).to_degrees();
        let pa = self.pa_cell(pa_deg);
        let w = self.w_cell(w_lambda);
        let group = self.group_index(pa, self.frequency_cell(freq_hz), w);
        let native = self.group_index(pa, nearest(&self.frequencies_hz, freq_hz).0, w);
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
}

/// `AWProjectFT::makeCFPolMap` and `makeConjPolMap`: each visibility
/// correlation maps to the plane of its own Mueller element (`4·i + j`
/// over `RR, RL, LR, LL` or `XX, XY, YX, YY`), the conjugate table to the
/// swapped hand (`RR ↔ LL`, `RL ↔ LR`), feeding the grid polarization
/// `pol_map` names; `None` when an element is missing.
fn routing(elements: &[u32], polarization: &PolarizationRouting) -> Option<MuellerRouting> {
    let pol_map = polarization.pol_map();
    let gpols = polarization.grid_pols();
    let plane_of = |element: u32| {
        elements
            .binary_search(&element)
            .ok()
            .map(|plane| u8::try_from(plane).expect("planes fit u8"))
    };
    let mut direct = vec![vec![None; pol_map.len()]; gpols];
    let mut conjugate = vec![vec![None; pol_map.len()]; gpols];
    for (vpol, (correlation, target)) in polarization.correlations().iter().zip(pol_map).enumerate()
    {
        let Some(gpol) = *target else {
            continue;
        };
        let element = mueller_element(*correlation)?;
        direct[usize::from(gpol)][vpol] = Some(plane_of(element)?);
        conjugate[usize::from(gpol)][vpol] = Some(plane_of(MUELLER_ELEMENTS - 1 - element)?);
    }
    Some(MuellerRouting { direct, conjugate })
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

/// `CFCache::getCFParams`: the cell's coordinates and misc info.
fn read_header(path: &Path) -> Result<CellHeader, AwCatalogError> {
    let image = PagedImage::<Complex32>::open(path)
        .map_err(|error| cache_error(path, format!("cannot open Complex32 image: {error}")))?;
    let shape = image.shape();
    if shape.len() != 4 || shape[0] == 0 || shape[1] == 0 || shape[2..] != [1, 1] {
        return Err(cache_error(
            path,
            format!("expected [nx, ny, 1, 1], got {shape:?}"),
        ));
    }
    if shape[0] % 2 != 0 || shape[1] % 2 != 0 {
        return Err(cache_error(path, "cell images must have even extents"));
    }
    let coordinates = image.coordinates();
    let spectral = coordinates
        .find_coordinate(CoordinateType::Spectral)
        .ok_or_else(|| cache_error(path, "missing spectral coordinate"))?;
    let frequency = coordinates
        .coordinate(spectral)
        .reference_value()
        .first()
        .copied()
        .ok_or_else(|| cache_error(path, "spectral coordinate has no reference value"))?;
    // CASA reads the reference value into a `Float` before listing it.
    let frequency_hz = f64::from(frequency as f32);
    if !frequency_hz.is_finite() || frequency_hz <= 0.0 {
        return Err(cache_error(
            path,
            "cell frequency must be finite and positive",
        ));
    }
    let misc = image.misc_info();
    let sampling = required_f64(&misc, "Sampling", path)?;
    if sampling.fract() != 0.0
        || sampling < 2.0
        || sampling > f64::from(u16::MAX)
        || sampling % 2.0 != 0.0
    {
        return Err(cache_error(
            path,
            "Sampling must be a positive even integer",
        ));
    }
    let support = [
        required_support(&misc, "Xsupport", path)?,
        required_support(&misc, "Ysupport", path)?,
    ];
    let mueller = required_i32(&misc, "MuellerElement", path)?;
    let mueller = u32::try_from(mueller)
        .ok()
        .filter(|element| *element < MUELLER_ELEMENTS)
        .ok_or_else(|| cache_error(path, "MuellerElement must lie in 0..16"))?;
    let w_value = required_f64(&misc, "WValue", path)?;
    let w_increment = required_f64(&misc, "WIncr", path)?;
    let pa_deg = required_f64(&misc, "ParallacticAngle", path)?;
    if !w_value.is_finite()
        || w_value < 0.0
        || !w_increment.is_finite()
        || w_increment < 0.0
        || !pa_deg.is_finite()
    {
        return Err(cache_error(
            path,
            "WValue, WIncr and ParallacticAngle must be finite and non-negative",
        ));
    }
    let sampling = sampling as u16;
    for axis in 0..2 {
        if (2 * usize::from(support[axis]) + 1) * usize::from(sampling) > shape[axis] {
            return Err(cache_error(path, "support does not fit the cell image"));
        }
    }
    // Native cells carry the working sky increment they were generated on;
    // CASA's own caches do not.
    let sky_increment_rad = match misc.get("CasaRsSkyIncrement") {
        Some(Value::Scalar(ScalarValue::Float64(value))) => Some(*value),
        _ => None,
    };
    if sky_increment_rad.is_some_and(|value| !value.is_finite() || value <= 0.0) {
        return Err(cache_error(
            path,
            "CasaRsSkyIncrement must be finite and positive",
        ));
    }
    let working_size = match misc.get("CasaRsWorkingSize") {
        Some(Value::Scalar(ScalarValue::Int32(value))) if *value > 0 => Some(*value as usize),
        Some(_) => {
            return Err(cache_error(path, "CasaRsWorkingSize must be positive"));
        }
        None => None,
    };
    let working_field = sky_increment_rad
        .zip(working_size)
        .map(|(increment, size)| increment * size as f64);
    Ok(CellHeader {
        path: path.to_path_buf(),
        shape: [shape[0], shape[1]],
        sampling,
        support,
        mueller,
        w_value,
        w_increment,
        pa_deg,
        frequency_hz,
        working_field_rad: working_field,
    })
}

fn required_support(record: &RecordValue, name: &str, path: &Path) -> Result<u16, AwCatalogError> {
    let value = required_i32(record, name, path)?;
    u16::try_from(value)
        .ok()
        .filter(|support| *support > 0)
        .ok_or_else(|| cache_error(path, format!("{name} must be positive")))
}

fn required_f64(record: &RecordValue, name: &str, path: &Path) -> Result<f64, AwCatalogError> {
    match record.get(name) {
        Some(Value::Scalar(ScalarValue::Float64(value))) => Ok(*value),
        Some(Value::Scalar(ScalarValue::Float32(value))) => Ok(f64::from(*value)),
        Some(value) => Err(cache_error(
            path,
            format!("{name} must be floating point, got {value:?}"),
        )),
        None => Err(cache_error(path, format!("missing miscinfo field {name}"))),
    }
}

fn required_i32(record: &RecordValue, name: &str, path: &Path) -> Result<i32, AwCatalogError> {
    match record.get(name) {
        Some(Value::Scalar(ScalarValue::Int32(value))) => Ok(*value),
        Some(Value::Scalar(ScalarValue::Int64(value))) => i32::try_from(*value)
            .map_err(|_| cache_error(path, format!("{name} is outside i32 range"))),
        Some(value) => Err(cache_error(
            path,
            format!("{name} must be integer, got {value:?}"),
        )),
        None => Err(cache_error(path, format!("missing miscinfo field {name}"))),
    }
}

/// Read the pixels of `header`'s image as a `[y][x]` plane, x fastest.
fn read_plane(header: &CellHeader) -> Result<Vec<Complex32>, AwCatalogError> {
    let image = PagedImage::<Complex32>::open(&header.path)
        .map_err(|error| cache_error(&header.path, format!("cannot open: {error}")))?;
    let pixels = image
        .get()
        .map_err(|error| cache_error(&header.path, format!("cannot read: {error}")))?;
    let [nx, ny] = header.shape;
    if pixels.shape() != [nx, ny, 1, 1] {
        return Err(cache_error(
            &header.path,
            "cell image changed shape since open",
        ));
    }
    let mut plane = Vec::with_capacity(nx * ny);
    for y in 0..ny {
        for x in 0..nx {
            plane.push(pixels[[x, y, 0, 0]]);
        }
    }
    Ok(plane)
}

/// `AWConvFunc::cfArea`: `Σ` over `[−support, support)` of the integer taps
/// about `shape/2`.
fn cf_area(plane: &[Complex32], shape: [usize; 2], support: [u16; 2], sampling: u16) -> Complex64 {
    let origin = [shape[0] / 2, shape[1] / 2];
    let mut area = Complex64::default();
    for iy in -i64::from(support[1])..i64::from(support[1]) {
        for ix in -i64::from(support[0])..i64::from(support[0]) {
            let x = origin[0] as i64 + ix * i64::from(sampling);
            let y = origin[1] as i64 + iy * i64::from(sampling);
            if x < 0 || y < 0 || x as usize >= shape[0] || y as usize >= shape[1] {
                continue;
            }
            let value = plane[y as usize * shape[0] + x as usize];
            area += Complex64::new(f64::from(value.re), f64::from(value.im));
        }
    }
    area
}

/// Load one group's Mueller planes into an imaging and a weight cell:
/// every plane normalised by its own area (`AWConvFunc::cfArea`), tiled at
/// its kind's largest support with zero beyond a smaller plane's image.
fn load_group(group: &Group, sampling: u16) -> Result<Loaded, AwCatalogError> {
    let build = |select: fn(&CellFiles) -> &CellHeader,
                 half: [u16; 2]|
     -> Result<Arc<DenseCell>, AwCatalogError> {
        let headers = group.cells.iter().map(select).collect::<Vec<_>>();
        let mut planes = Vec::with_capacity(headers.len());
        for header in &headers {
            let mut plane = read_plane(header)?;
            let area = cf_area(&plane, header.shape, header.support, header.sampling);
            if !(area.norm().is_finite() && area.norm() > 0.0) {
                return Err(cache_error(&header.path, "cell area is not positive"));
            }
            let scale = Complex32::new((1.0 / area).re as f32, (1.0 / area).im as f32);
            for value in &mut plane {
                *value *= scale;
            }
            planes.push((plane, header.shape, header.support));
        }
        Ok(Arc::new(dense_cell_padded(&planes, half, sampling)))
    };
    Ok(Loaded {
        imaging: build(|cell| &cell.imaging, group.imaging_half_support)?,
        weight: build(|cell| &cell.weight, group.weight_half_support)?,
        last_use: 0,
    })
}

/// The dense cell of `planes` (each `[y][x]` with origin at `shape/2` and
/// its own declared half support) at `half` taps each side: tap `k` at
/// fine offset `off` reads pixel `origin + k·sampling + off`
/// (`accumulateToGrid.inc`: `convOrigin + (Int)(sampling·ix + off)`), zero
/// outside the plane and beyond the plane's own support (`AWVisResampler`
/// takes the support per Mueller cell; the crop buffer around it is not
/// part of the kernel).
fn dense_cell_padded(
    planes: &[(Vec<Complex32>, [usize; 2], [u16; 2])],
    half: [u16; 2],
    sampling: u16,
) -> DenseCell {
    let support = [2 * half[0] + 1, 2 * half[1] + 1];
    let [sx, sy] = [usize::from(support[0]), usize::from(support[1])];
    let rows = usize::from(sampling) + 1;
    let fine_half = i64::from(sampling / 2);
    let mut data = Vec::with_capacity(rows * rows * planes.len() * sx * sy);
    for oy in 0..rows {
        let off_y = oy as i64 - fine_half;
        for ox in 0..rows {
            let off_x = ox as i64 - fine_half;
            for (plane, shape, plane_half) in planes {
                let origin = [(shape[0] / 2) as i64, (shape[1] / 2) as i64];
                for iy in 0..sy {
                    let ky = iy as i64 - i64::from(half[1]);
                    let y = origin[1] + ky * i64::from(sampling) + off_y;
                    for ix in 0..sx {
                        let kx = ix as i64 - i64::from(half[0]);
                        let x = origin[0] + kx * i64::from(sampling) + off_x;
                        let inside = kx.abs() <= i64::from(plane_half[0])
                            && ky.abs() <= i64::from(plane_half[1])
                            && x >= 0
                            && y >= 0
                            && (x as usize) < shape[0]
                            && (y as usize) < shape[1];
                        data.push(if inside {
                            plane[y as usize * shape[0] + x as usize]
                        } else {
                            Complex32::default()
                        });
                    }
                }
            }
        }
    }
    DenseCell {
        data: data.into_boxed_slice(),
        support,
        oversampling: sampling,
        mueller_planes: u8::try_from(planes.len()).expect("Mueller planes fit u8"),
    }
}

/// The misc info `CFCell::makePersistent` writes.
struct CellMisc {
    sampling: f64,
    pa_deg: f64,
    mueller: usize,
    w_value: f64,
    w_increment: f64,
    conjugate_frequency: f64,
    conjugate_mueller: u32,
    diameter_m: f64,
    /// The working sky increment and size the cell was generated on:
    /// private keys (`CasaRsSkyIncrement`, `CasaRsWorkingSize`) the loader
    /// checks against the image geometry so a cache is never reused for
    /// another one.
    sky_increment_rad: f64,
    working_size: usize,
}

/// Write one generated plane as a CASA cell image (`CFCell::makePersistent`
/// with `SynthesisUtils::makeFTCoordSys`'s UU/VV coordinate).
fn write_cell(
    path: &Path,
    plane: &NativeAwPlane<'_>,
    frequency_hz: f64,
    grid: casa_imaging_model::NativeAwGrid,
    misc: &CellMisc,
) -> Result<(), AwCatalogError> {
    let write_error = |detail: String| AwCatalogError::Write {
        path: path.to_path_buf(),
        detail,
    };
    if path.exists() {
        std::fs::remove_dir_all(path)
            .map_err(|error| write_error(format!("cannot replace: {error}")))?;
    }
    let side = plane.size;
    // The uv increment of one oversampled pixel over the working field.
    let increment =
        1.0 / (grid.size as f64 * grid.sky_increment_rad[1].abs()) / grid.oversampling as f64;
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(
        LinearCoordinate::new(
            2,
            vec!["UU".to_string(), "VV".to_string()],
            vec!["lambda".to_string(), "lambda".to_string()],
        )
        .with_reference_value(vec![0.0, 0.0])
        .with_reference_pixel(vec![(side / 2) as f64, (side / 2) as f64])
        .with_increment(vec![-increment, increment]),
    );
    coordinates.add_coordinate(StokesCoordinate::new(vec![if misc.mueller == 0 {
        StokesType::RR
    } else {
        StokesType::LL
    }]));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        frequency_hz,
        1.0,
        0.0,
        frequency_hz,
    ));
    let mut image = PagedImage::<Complex32>::create(vec![side, side, 1, 1], coordinates, path)
        .map_err(|error| write_error(format!("cannot create: {error}")))?;
    let mut pixels = ndarray::ArrayD::<Complex32>::zeros(ndarray::IxDyn(&[side, side, 1, 1]));
    for y in 0..side {
        for x in 0..side {
            pixels[[x, y, 0, 0]] = plane.values[y * side + x];
        }
    }
    image
        .put_slice(&pixels, &[0, 0, 0, 0])
        .map_err(|error| write_error(format!("cannot write pixels: {error}")))?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string();
    let field = |name: &str, value: ScalarValue| RecordField::new(name, Value::Scalar(value));
    image
        .set_misc_info(RecordValue::new(vec![
            field("Xsupport", ScalarValue::Int32(plane.support as i32)),
            field("Ysupport", ScalarValue::Int32(plane.support as i32)),
            field("Sampling", ScalarValue::Float64(misc.sampling)),
            field("ParallacticAngle", ScalarValue::Float64(misc.pa_deg)),
            field("MuellerElement", ScalarValue::Int32(misc.mueller as i32)),
            field("WValue", ScalarValue::Float64(misc.w_value)),
            field("WIncr", ScalarValue::Float64(misc.w_increment)),
            field("Name", ScalarValue::String(name)),
            field("ConjFreq", ScalarValue::Float64(misc.conjugate_frequency)),
            field(
                "ConjPoln",
                ScalarValue::Int32(misc.conjugate_mueller as i32),
            ),
            field("TelescopeName", ScalarValue::String("EVLA".to_string())),
            field("BandName", ScalarValue::String(String::new())),
            field("Diameter", ScalarValue::Float64(misc.diameter_m)),
            field("OpCode", ScalarValue::Bool(false)),
            field(
                "CasaRsSkyIncrement",
                ScalarValue::Float64(misc.sky_increment_rad),
            ),
            field(
                "CasaRsWorkingSize",
                ScalarValue::Int32(
                    i32::try_from(misc.working_size)
                        .map_err(|_| write_error("working size exceeds i32".to_string()))?,
                ),
            ),
        ]))
        .map_err(|error| write_error(format!("cannot write misc info: {error}")))?;
    image
        .save()
        .map_err(|error| write_error(format!("cannot save: {error}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
