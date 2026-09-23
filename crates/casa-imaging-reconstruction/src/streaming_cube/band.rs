// SPDX-License-Identifier: LGPL-3.0-or-later

//! Direct standard-gridder, natural-weight, Stokes-I cube band kernels.
//!
//! The input is borrowed channel-major/correlation-minor numeric arrays, with
//! geometry once per row. Grids are contiguous [channel, x, y] Complex64 arrays;
//! compensation is a separate, equally shaped allocation. Plane views neither
//! allocate nor copy. The coarse epoch job loads bounded model planes before
//! numerical work; payload elements carry no workflow or publication state.

use std::ops::Range;

use casa_imaging_model::{
    ModelSupport, PolarizationCoordinate, ReconstructionBasis, SelectedSampleAddress,
};
#[cfg(test)]
use ndarray::ArrayView3;
use ndarray::{Array3, ArrayView2, Axis, s};
use num_complex::Complex64;
use smallvec::SmallVec;

use super::input::{NativeBlock, NativeLayout};
use crate::spectral_operator::{
    PreparedFft, SPEED_OF_LIGHT_M_PER_S, SpectralOperatorGeometry, SpectralOperatorPass,
    StandardConvolution, append_image_plane, fft_resident_complex_values_for_shape,
    polarization_diagonal,
};
use crate::spectral_sampling::{
    CasaLinearGrid, CasaLinearOutputGrid, CasaLinearRowCursor, CasaSingleChannel,
    NativeRowSpectralGeometry, casa_linear_prediction_terms, interpolate_complex_pair,
};
use crate::{
    ModelGeneration, ModelGenerationId, PolarizationOperator, SpectralOperatorError,
    SpectralOperatorPrimitives, SpectralOperatorSpecification,
};

/// Shape and conservative row-union dependencies for one output band. Runtime
/// chooses the decomposition and admits its physical memory before preparation.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct BandPlan {
    geometry: SpectralOperatorGeometry,
    core: Range<usize>,
    total_channels: usize,
    single_channel: Option<CasaSingleChannel>,
    phase: BandPhase,
    support: BandSupport,
}

impl BandPlan {
    /// Whether the compiled geometry is implemented by the native cube kernel.
    #[must_use]
    pub fn supports(specification: &SpectralOperatorSpecification) -> bool {
        specification.cube_geometry().is_ok()
    }

    /// Compile the supported standard, Stokes-I, linear cube band shape.
    pub fn new(
        specification: &SpectralOperatorSpecification,
        pass: SpectralOperatorPass,
    ) -> Result<Self, SpectralOperatorError> {
        Ok(Self {
            geometry: specification.cube_geometry()?,
            core: specification.slab().core_range(),
            total_channels: specification.slab().total_channels(),
            single_channel: specification.cube_single_channel(),
            phase: match pass {
                SpectralOperatorPass::InitialMajor
                    if specification.is_initial_certified_zero(pass) =>
                {
                    BandPhase::InitialZero
                }
                SpectralOperatorPass::InitialMajor => BandPhase::Full,
                SpectralOperatorPass::ResidualRefresh => BandPhase::Residual,
            },
            support: BandSupport {
                native: 0..0,
                model: Vec::new(),
            },
        })
    }

    /// Observe each full selected native block once during preparation. The
    /// caller retains source/run coverage; this records only band dependencies.
    pub fn observe(
        &mut self,
        block: &NativeBlock,
        output_hz: &[f64],
    ) -> Result<(), SpectralOperatorError> {
        Self::observe_all(std::slice::from_mut(self), block, output_hz).map(|_| ())
    }

    /// Discover every band's row support with one native-pair sweep per distinct
    /// consecutive spectral row. The returned count measures actual pair visits,
    /// independent of the band count.
    /// Only one range per band is temporary; model support is merged directly
    /// into the retained sorted channel lists, without per-row/band vectors.
    pub fn observe_all(
        bands: &mut [Self],
        block: &NativeBlock,
        output_hz: &[f64],
    ) -> Result<u64, SpectralOperatorError> {
        if bands.is_empty()
            || bands
                .iter()
                .any(|band| output_hz.len() != band.total_channels)
            || bands
                .windows(2)
                .any(|pair| pair[0].core.end > pair[1].core.start)
        {
            return Err(SpectralOperatorError::ProblemMismatch);
        }
        block
            .validate_shape(block.metadata.len(), block.channels, block.correlations)
            .map_err(|_| SpectralOperatorError::InvalidSample)?;
        if block.channels < 2 {
            return Err(SpectralOperatorError::InvalidSample);
        }
        if let Some(single) = bands[0].single_channel {
            if bands.len() != 1 || output_hz != [single.centre_hz] {
                return Err(SpectralOperatorError::ProblemMismatch);
            }
            let band = &mut bands[0];
            for frequencies in block.frequencies_hz.chunks_exact(block.channels) {
                let native = single.native_window(frequencies);
                if !native.is_empty() {
                    band.support.native = if band.support.native.is_empty() {
                        native
                    } else {
                        band.support.native.start.min(native.start)
                            ..band.support.native.end.max(native.end)
                    };
                }
            }
            if !band.support.native.is_empty() && band.support.model.is_empty() {
                band.support.model.push(0);
            }
            return Ok(0);
        }
        let mut ranges = vec![block.channels..0; bands.len()];
        let mut pairs = 0_u64;
        for (row, metadata) in block.metadata.iter().enumerate() {
            let native_hz = &block.frequencies_hz[row * block.channels..(row + 1) * block.channels];
            if row > 0
                && metadata.original_pair_hz == block.metadata[row - 1].original_pair_hz
                && native_hz
                    == &block.frequencies_hz[(row - 1) * block.channels..row * block.channels]
            {
                continue;
            }
            ranges.fill(block.channels..0);
            let grid = CasaLinearGrid::compile(
                output_hz,
                metadata.original_pair_hz[0],
                metadata.original_pair_hz[1],
            )
            .ok_or(SpectralOperatorError::MissingRowSpectralGeometry)?;
            let mut next_fine = 0;
            for (left, pair) in native_hz.windows(2).enumerate() {
                pairs = pairs
                    .checked_add(1)
                    .ok_or(SpectralOperatorError::ResidencyOverflow)?;
                for fine in grid
                    .samples_for_pair(&mut next_fine, pair[0], pair[1])
                    .map_err(|_| SpectralOperatorError::InvalidSample)?
                {
                    let channel = fine.output_channel();
                    let band = bands.partition_point(|band| band.core.end <= channel);
                    if band < bands.len() && bands[band].core.contains(&channel) {
                        ranges[band].start = ranges[band].start.min(left);
                        ranges[band].end = ranges[band].end.max(left + 2);
                    }
                }
            }
            for (band, native) in bands.iter_mut().zip(&ranges) {
                if native.is_empty() {
                    continue;
                }
                band.support.native = if band.support.native.is_empty() {
                    native.clone()
                } else {
                    band.support.native.start.min(native.start)
                        ..band.support.native.end.max(native.end)
                };
                // Preserve the complete contiguous native closure, including
                // interior samples that did not themselves emit a fine channel.
                for &frequency in &native_hz[native.clone()] {
                    for term in casa_linear_prediction_terms(
                        output_hz,
                        frequency,
                        metadata.original_pair_hz,
                    )
                    .map_err(|_| SpectralOperatorError::InvalidSample)?
                    {
                        let channel = term.output_channel() as usize;
                        if let Err(position) = band.support.model.binary_search(&channel) {
                            if band.support.model.len() == band.support.model.capacity() {
                                let capacity = band
                                    .support
                                    .model
                                    .capacity()
                                    .saturating_mul(2)
                                    .max(4)
                                    .min(band.total_channels);
                                band.support
                                    .model
                                    .try_reserve_exact(capacity - band.support.model.len())
                                    .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
                            }
                            band.support.model.insert(position, channel);
                        }
                    }
                }
            }
        }
        Ok(pairs)
    }

    /// Selected native ordinals required across all observed rows.
    pub fn native_range(&self) -> Range<usize> {
        self.support.native.clone()
    }

    /// Exclusive output channels.
    pub fn core(&self) -> Range<usize> {
        self.core.clone()
    }

    /// Preparation retains at most one entry per output model channel plus
    /// one row-local range. Capacity growth is capped at this shape bound.
    /// Runtime charges this before observing row-dependent support.
    pub fn preparation_metadata_bytes(&self) -> Result<usize, SpectralOperatorError> {
        self.total_channels
            .checked_mul(size_of::<usize>())
            .and_then(|bytes| bytes.checked_add(size_of::<Self>() + size_of::<Range<usize>>()))
            .ok_or(SpectralOperatorError::ResidencyOverflow)
    }

    /// Initial-phase admission may omit forward grids only for this compiled
    /// certified-empty phase. A seeded model needs its own support-aware plan.
    pub fn is_certified_empty_initial(&self) -> bool {
        self.phase == BandPhase::InitialZero
    }

    /// Reuse fixed dependencies for a later immutable model epoch.
    pub fn residual_refresh(&self) -> Self {
        Self {
            phase: BandPhase::Residual,
            ..self.clone()
        }
    }

    /// Allocate only after runtime admission. Recycled FFT state retains the
    /// same spatial geometry; model reads stay within one admitted plane.
    pub fn prepare<'a>(
        self,
        generation: &'a ModelGeneration,
        fft: Option<PreparedFft>,
    ) -> Result<EpochBand<'a>, SpectralOperatorError> {
        self.memory()?;
        let fft = match fft {
            Some(fft) if fft.shape() == self.geometry.grid_shape => fft,
            Some(_) => return Err(SpectralOperatorError::UnsupportedGeometry),
            None => PreparedFft::new(
                self.geometry.grid_shape,
                fft_resident_complex_values_for_shape(self.geometry.grid_shape)?,
            )?,
        };
        EpochBand::prepare(
            BandWorkspace::new(
                self.geometry,
                self.core,
                self.support.model,
                fft,
                self.phase,
                self.single_channel,
            ),
            generation,
            self.support.native,
        )
    }
}

/// A coarse band job holds the immutable model borrow until completion. Arrays
/// remain plain numerical buffers; epoch association is not per pixel/sample.
#[doc(hidden)]
pub struct EpochBand<'a> {
    generation: &'a ModelGeneration,
    workspace: BandWorkspace,
    native_range: Range<usize>,
}

/// Only the fields actually generated by this phase cross the worker boundary.
#[doc(hidden)]
#[derive(Debug)]
// Band admission bounds the number of live results. Keep their owned buffer
// descriptors inline instead of adding a heap allocation for each initial band.
#[allow(clippy::large_enum_variant)]
pub enum BandResult {
    /// Fresh normal fields from the initial major cycle.
    Initial(SpectralOperatorPrimitives),
    /// A new residual; invariant normal arrays stay with their shared owner.
    Residual(CubeResidual),
}

/// One bounded residual plane range at its frozen model epoch.
#[doc(hidden)]
#[derive(Debug)]
pub struct CubeResidual {
    pub(crate) shape: [usize; 2],
    pub(crate) core: Range<usize>,
    pub(crate) total_channels: usize,
    pub(crate) model: ModelGenerationId,
    pub(crate) values: Box<[Complex64]>,
}

impl CubeResidual {
    /// Exclusive output channels carried by this owned result.
    pub fn core(&self) -> Range<usize> {
        self.core.clone()
    }

    /// Model epoch used for prediction, not the invariant normal source epoch.
    pub fn model_generation(&self) -> ModelGenerationId {
        self.model
    }

    /// Borrow the generated complex values in channel/x/y order.
    pub fn values(&self) -> &[Complex64] {
        &self.values
    }
}

impl<'a> EpochBand<'a> {
    fn prepare(
        mut workspace: BandWorkspace,
        generation: &'a ModelGeneration,
        native_range: Range<usize>,
    ) -> Result<Self, SpectralOperatorError> {
        let shape = generation.shape();
        if shape.domains().len() != 1
            || shape.domains()[0].pixels() != workspace.geometry.image_shape
            || shape.polarizations() != 1
            || !matches!(
                shape.coefficient_space().basis(),
                ReconstructionBasis::ChannelLocal { .. }
            )
            || workspace.core.end > shape.coefficients()
            || workspace
                .model_channels
                .iter()
                .any(|&channel| channel >= shape.coefficients())
        {
            return Err(SpectralOperatorError::ModelShape);
        }
        if workspace.phase == BandPhase::InitialZero
            && generation.origin() != crate::ModelGenerationOrigin::Empty
        {
            return Err(SpectralOperatorError::ReusableNormalStateMismatch);
        }
        let width = workspace.geometry.image_shape[0];
        for plane in 0..workspace.forward.len_of(Axis(0)) {
            // The existing storage owner enforces its admitted window. Only one
            // canonical y/x plane is live alongside the prepared support grids.
            let samples = generation.read_plane(0, workspace.model_channels[plane], 0)?;
            workspace.prepare_plane(plane, |x, y| {
                let sample = samples[y * width + x];
                if sample.support() == ModelSupport::Valid {
                    Complex64::new(sample.value().value(), 0.0)
                } else {
                    Complex64::default()
                }
            })?;
        }
        Ok(Self {
            generation,
            workspace,
            native_range,
        })
    }

    /// Evaluate this block using borrowed subwindows, keeping all writable
    /// grids exclusively within this epoch job.
    pub fn consume(
        &mut self,
        block: &NativeBlock,
        layout: &NativeLayout,
        window: Range<usize>,
        output_hz: &[f64],
        polarization: &PolarizationOperator,
    ) -> Result<(), SpectralOperatorError> {
        if output_hz.len() != self.generation.shape().coefficients() {
            return Err(SpectralOperatorError::ProblemMismatch);
        }
        self.workspace.consume_block(
            block,
            layout,
            window,
            self.native_range.clone(),
            output_hz,
            polarization,
        )
    }

    /// Prepared model planes and actual forward FFTs. Exact-zero planes are
    /// recognized during grid population, not by a separate verification pass.
    pub fn model_plane_counts(&self) -> (usize, usize) {
        (
            self.workspace.forward_nonzero.len(),
            self.workspace
                .forward_nonzero
                .iter()
                .filter(|&&active| active)
                .count(),
        )
    }

    /// Join into owned normal fields for the same model and return reusable FFT
    /// state. Complete source coverage is the runtime owner's separate duty.
    pub fn complete(
        self,
        expected: &ModelGeneration,
    ) -> Result<(BandResult, PreparedFft), SpectralOperatorError> {
        if !std::ptr::eq(self.generation, expected) {
            return Err(SpectralOperatorError::ModelMismatch);
        }
        let channels = self.generation.shape().coefficients();
        let model = self.generation.generation_id();
        let (images, fft) = self.workspace.finish_images()?;
        let result = if images.phase == BandPhase::Residual {
            BandResult::Residual(CubeResidual {
                shape: images.shape,
                core: images.core,
                total_channels: channels,
                model,
                values: images.residual.into_boxed_slice(),
            })
        } else {
            BandResult::Initial(SpectralOperatorPrimitives::from_cube_band(
                images, channels, model,
            )?)
        };
        Ok((result, fft))
    }
}

/// Image-domain buffers move into the existing normal-state owner. There is no
/// alternate controller or persistent product representation at this boundary.
pub(crate) struct BandImages {
    pub(crate) phase: BandPhase,
    pub(crate) shape: [usize; 2],
    pub(crate) core: Range<usize>,
    pub(crate) dirty: Vec<Complex64>,
    pub(crate) residual: Vec<Complex64>,
    pub(crate) psf: Vec<Complex64>,
    pub(crate) sum_weight: Vec<f64>,
    pub(crate) mapped: Vec<u64>,
}

/// Phase liveness is explicit: an empty initial model needs no prediction or
/// residual grid; a refresh only forms residuals and moves its invariant normal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BandPhase {
    InitialZero,
    Full,
    Residual,
}

/// Exact native-pair and model support for one row and output band. Native
/// ordinals address the supplied row, not physical MS channel numbers.
#[derive(Debug, Clone, PartialEq)]
struct BandSupport {
    native: Range<usize>,
    model: Vec<usize>,
}

impl BandSupport {
    #[cfg(test)]
    fn compile(
        output_hz: &[f64],
        core: Range<usize>,
        native_hz: &[f64],
        original_pair_hz: [f64; 2],
    ) -> Result<Self, SpectralOperatorError> {
        let native = Self::native_window(output_hz, core, native_hz, original_pair_hz)?;
        let mut model = Vec::new();
        for frequency in &native_hz[native.clone()] {
            for term in casa_linear_prediction_terms(output_hz, *frequency, original_pair_hz)
                .map_err(|_| SpectralOperatorError::InvalidSample)?
            {
                let channel = term.output_channel() as usize;
                if let Err(position) = model.binary_search(&channel) {
                    model.insert(position, channel);
                }
            }
        }
        Ok(Self { native, model })
    }

    /// Recomputed over the shared decoded window for each row. This narrows
    /// row-varying support without storing a per-row/band directory or predicting
    /// unrelated native channels from a conservatively wider stored window.
    fn native_window(
        output_hz: &[f64],
        core: Range<usize>,
        native_hz: &[f64],
        original_pair_hz: [f64; 2],
    ) -> Result<Range<usize>, SpectralOperatorError> {
        if core.is_empty() || core.end > output_hz.len() || native_hz.len() < 2 {
            return Err(SpectralOperatorError::InvalidSample);
        }
        let grid = CasaLinearGrid::compile(output_hz, original_pair_hz[0], original_pair_hz[1])
            .ok_or(SpectralOperatorError::MissingRowSpectralGeometry)?;
        let mut next_fine = 0;
        let mut native = native_hz.len()..0;
        for (left, pair) in native_hz.windows(2).enumerate() {
            for fine in grid
                .samples_for_pair(&mut next_fine, pair[0], pair[1])
                .map_err(|_| SpectralOperatorError::InvalidSample)?
            {
                if core.contains(&fine.output_channel()) {
                    native.start = native.start.min(left);
                    native.end = native.end.max(left + 2);
                }
            }
        }
        if native.is_empty() {
            native = 0..0;
        }
        Ok(native)
    }

    /// Union during the one preparation traversal. Only band-sized metadata is
    /// retained; no payload is reread after preparation to discover dependencies.
    #[cfg(test)]
    fn include(&mut self, row: Self) {
        if !row.native.is_empty() {
            self.native = if self.native.is_empty() {
                row.native
            } else {
                self.native.start.min(row.native.start)..self.native.end.max(row.native.end)
            };
        }
        for channel in row.model {
            if let Err(position) = self.model.binary_search(&channel) {
                self.model.insert(position, channel);
            }
        }
    }
}

/// Borrowed compact native row, including a selected native-channel window.
///
/// Values/weights retain Complex64/f64 precision, flags occupy one byte each.
/// `flags` have already applied finite-value/input acceptance; `weight_flags`
/// are the native weight-group/parallel-hand/row flags used by nearest weight
/// transfer. Keeping the latter distinct preserves CASA's endpoint behavior.
/// Correlation identity belongs to the block's shared polarization operator.
struct VisibilityRow<'a> {
    address: SelectedSampleAddress,
    uvw_m: [f64; 3],
    phase_shift_m: f64,
    original_pair_hz: [f64; 2],
    channels: &'a [u32],
    frequencies_hz: &'a [f64],
    values: ArrayView2<'a, Complex64>,
    weights: ArrayView2<'a, f64>,
    flags: ArrayView2<'a, bool>,
    weight_flags: ArrayView2<'a, bool>,
}

impl NativeBlock {
    /// Borrow a decoded native window directly; no intermediate sample objects.
    /// `channels` identifies the store window within the shared selected layout.
    fn row<'a>(
        &'a self,
        layout: &'a NativeLayout,
        row: usize,
        channels: Range<usize>,
    ) -> Result<VisibilityRow<'a>, SpectralOperatorError> {
        if row >= self.metadata.len()
            || channels.len() != self.channels
            || channels.end > layout.channels.len()
            || self.correlations != layout.correlations.len()
        {
            return Err(SpectralOperatorError::InvalidSample);
        }
        self.validate_shape(self.metadata.len(), self.channels, self.correlations)
            .map_err(|_| SpectralOperatorError::InvalidSample)?;
        let metadata = self.metadata[row];
        let cells = row * self.channels..(row + 1) * self.channels;
        let samples = cells.start * self.correlations..cells.end * self.correlations;
        let shape = (self.channels, self.correlations);
        Ok(VisibilityRow {
            address: SelectedSampleAddress {
                physical_row: metadata.physical_row,
                ..layout.address
            },
            uvw_m: metadata.uvw_m,
            phase_shift_m: metadata.phase_shift_m,
            original_pair_hz: metadata.original_pair_hz,
            channels: &layout.channels[channels],
            frequencies_hz: &self.frequencies_hz[cells],
            values: ArrayView2::from_shape(shape, &self.values[samples.clone()]).unwrap(),
            weights: ArrayView2::from_shape(shape, &self.weights[samples.clone()]).unwrap(),
            flags: ArrayView2::from_shape(shape, &self.flags[samples.clone()]).unwrap(),
            weight_flags: ArrayView2::from_shape(shape, &self.weight_flags[samples]).unwrap(),
        })
    }
}

impl<'a> VisibilityRow<'a> {
    fn window(self, channels: Range<usize>) -> Result<Self, SpectralOperatorError> {
        if channels.is_empty() || channels.end > self.channels.len() {
            return Err(SpectralOperatorError::InvalidSample);
        }
        Ok(Self {
            channels: &self.channels[channels.clone()],
            frequencies_hz: &self.frequencies_hz[channels.clone()],
            values: self.values.slice_move(s![channels.clone(), ..]),
            weights: self.weights.slice_move(s![channels.clone(), ..]),
            flags: self.flags.slice_move(s![channels.clone(), ..]),
            weight_flags: self.weight_flags.slice_move(s![channels, ..]),
            ..self
        })
    }

    fn validate(&self, correlations: usize) -> Result<(), SpectralOperatorError> {
        let shape = (self.channels.len(), correlations);
        if self.channels.is_empty()
            || self.frequencies_hz.len() != shape.0
            || correlations == 0
            || correlations > 4
            || self.values.dim() != shape
            || self.weights.dim() != shape
            || self.flags.dim() != shape
            || self.weight_flags.dim() != shape
            || !self.values.is_standard_layout()
            || !self.weights.is_standard_layout()
            || !self.flags.is_standard_layout()
            || !self.weight_flags.is_standard_layout()
        {
            return Err(SpectralOperatorError::InvalidSample);
        }
        Ok(())
    }

    fn geometry(&self) -> NativeRowSpectralGeometry {
        NativeRowSpectralGeometry {
            channels: self.channels.len(),
            first: (self.channels[0], self.frequencies_hz[0]),
            second: Some((self.channels[1], self.frequencies_hz[1])),
            lattice_first_pair_hz: Some(self.original_pair_hz),
        }
    }
}

/// Exclusive output band and reusable FFT workspace; the read-only model
/// support may extend beyond the output band. Admission supplies both ranges.
struct BandWorkspace {
    phase: BandPhase,
    single_channel: Option<CasaSingleChannel>,
    geometry: SpectralOperatorGeometry,
    core: Range<usize>,
    model_channels: Vec<usize>,
    convolution: StandardConvolution,
    fft: PreparedFft,
    forward: Array3<Complex64>,
    forward_nonzero: Vec<bool>,
    dirty: Array3<Complex64>,
    dirty_error: Array3<Complex64>,
    residual: Array3<Complex64>,
    residual_error: Array3<Complex64>,
    psf: Array3<Complex64>,
    psf_error: Array3<Complex64>,
    sum_weight: Vec<f64>,
    sum_weight_error: Vec<f64>,
    mapped: Vec<u64>,
}

impl BandWorkspace {
    fn new(
        geometry: SpectralOperatorGeometry,
        core: Range<usize>,
        model_channels: Vec<usize>,
        fft: PreparedFft,
        phase: BandPhase,
        single_channel: Option<CasaSingleChannel>,
    ) -> Self {
        let shape = (core.len(), geometry.grid_shape[0], geometry.grid_shape[1]);
        let normal = if phase == BandPhase::Residual {
            (0, shape.1, shape.2)
        } else {
            shape
        };
        let residual = if phase == BandPhase::InitialZero {
            (0, shape.1, shape.2)
        } else {
            shape
        };
        let normal_planes = normal.0;
        Self {
            phase,
            single_channel,
            forward_nonzero: vec![
                false;
                if phase == BandPhase::InitialZero {
                    0
                } else {
                    model_channels.len()
                }
            ],
            forward: Array3::zeros((
                if phase == BandPhase::InitialZero {
                    0
                } else {
                    model_channels.len()
                },
                shape.1,
                shape.2,
            )),
            dirty: Array3::zeros(normal),
            dirty_error: Array3::zeros(normal),
            residual: Array3::zeros(residual),
            residual_error: Array3::zeros(residual),
            psf: Array3::zeros(normal),
            psf_error: Array3::zeros(normal),
            sum_weight: vec![0.0; normal_planes],
            sum_weight_error: vec![0.0; normal_planes],
            mapped: vec![0; normal_planes],
            convolution: StandardConvolution::new(&geometry),
            geometry,
            core,
            model_channels,
            fft,
        }
    }

    #[cfg(test)]
    fn prepare_model(
        &mut self,
        model: ArrayView3<'_, Complex64>,
    ) -> Result<(), SpectralOperatorError> {
        if model.shape()[1..] != self.geometry.image_shape {
            return Err(SpectralOperatorError::ModelShape);
        }
        for plane in 0..self.model_channels.len() {
            let channel = self.model_channels[plane];
            if channel >= model.len_of(Axis(0)) {
                return Err(SpectralOperatorError::IncompleteSpectralHalo);
            }
            self.prepare_plane(plane, |x, y| model[(channel, x, y)])?;
        }
        Ok(())
    }

    fn prepare_plane(
        &mut self,
        plane: usize,
        value: impl Fn(usize, usize) -> Complex64,
    ) -> Result<bool, SpectralOperatorError> {
        let mut grid = self.forward.index_axis_mut(Axis(0), plane);
        grid.fill(Complex64::default());
        let mut nonzero = false;
        for y in 0..self.geometry.image_shape[1] {
            for x in 0..self.geometry.image_shape[0] {
                let corrected = value(x, y) * self.convolution.image_correction(x, y);
                nonzero |= corrected != Complex64::default();
                grid[(
                    self.geometry.image_blc[0] + x,
                    self.geometry.image_blc[1] + y,
                )] = corrected;
            }
        }
        self.forward_nonzero[plane] = nonzero;
        if !nonzero {
            return Ok(false);
        }
        self.fft.transform(&mut grid, false);
        if grid
            .iter()
            .any(|value| !value.re.is_finite() || !value.im.is_finite())
        {
            return Err(SpectralOperatorError::GeneratedNonfinite);
        }
        Ok(true)
    }

    fn finish_images(self) -> Result<(BandImages, PreparedFft), SpectralOperatorError> {
        let Self {
            phase,
            geometry,
            core,
            convolution,
            mut fft,
            dirty,
            residual,
            psf,
            sum_weight,
            mapped,
            forward,
            forward_nonzero,
            dirty_error,
            residual_error,
            psf_error,
            model_channels,
            sum_weight_error,
            single_channel: _,
        } = self;
        // Prediction and compensation are dead before image allocations begin.
        drop((
            forward,
            forward_nonzero,
            dirty_error,
            residual_error,
            psf_error,
            model_channels,
            sum_weight_error,
        ));
        let mut image = |mut grids: Array3<Complex64>| {
            let mut values = Vec::with_capacity(
                grids.len_of(Axis(0)) * geometry.image_shape[0] * geometry.image_shape[1],
            );
            for mut grid in grids.axis_iter_mut(Axis(0)) {
                fft.transform(&mut grid, true);
                append_image_plane(
                    grid.view(),
                    &geometry,
                    &mut values,
                    |x, y| convolution.image_correction(x, y),
                    false,
                )?;
            }
            Ok::<_, SpectralOperatorError>(values)
        };
        let dirty = image(dirty)?;
        let psf = image(psf)?;
        let residual = image(residual)?;
        Ok((
            BandImages {
                phase,
                shape: geometry.image_shape,
                core,
                dirty,
                residual,
                psf,
                sum_weight,
                mapped,
            },
            fft,
        ))
    }

    fn predict_native(
        &self,
        row: &VisibilityRow<'_>,
        frequency_hz: f64,
        output_hz: &[f64],
        polarization: &PolarizationOperator,
    ) -> Result<SmallVec<[Complex64; 4]>, SpectralOperatorError> {
        if self.phase == BandPhase::InitialZero {
            return Ok(std::iter::repeat_n(
                Complex64::default(),
                polarization.correlations().len(),
            )
            .collect());
        }
        let mut predicted = Complex64::default();
        let terms = if let Some(single) = self.single_channel {
            let mut terms = SmallVec::new();
            if single.contains(frequency_hz) {
                terms.push(
                    casa_imaging_model::SelectedSpectralContribution::new(0, 1.0, frequency_hz)
                        .ok_or(SpectralOperatorError::InvalidSample)?,
                );
            }
            terms
        } else {
            casa_linear_prediction_terms(output_hz, frequency_hz, row.original_pair_hz)
                .map_err(|_| SpectralOperatorError::InvalidSample)?
        };
        for term in terms {
            let plane = self
                .model_channels
                .binary_search(&(term.output_channel() as usize))
                .map_err(|_| SpectralOperatorError::IncompleteSpectralHalo)?;
            if !self.forward_nonzero[plane] {
                continue;
            }
            let frequency = term.evaluation_frequency_hz();
            let wavelength_scale = frequency / SPEED_OF_LIGHT_M_PER_S;
            if let Some(taps) = self.convolution.taps([
                row.uvw_m[0] * wavelength_scale,
                row.uvw_m[1] * wavelength_scale,
            ]) {
                predicted += self
                    .convolution
                    .degrid(&self.forward.index_axis(Axis(0), plane), taps)
                    * phase(row.phase_shift_m, frequency).conj()
                    * term.factor();
            }
        }
        polarization
            .predict(&[predicted])
            .map_err(|_| SpectralOperatorError::GeneratedNonfinite)
    }

    fn begin_row<'a>(
        &'a mut self,
        row: VisibilityRow<'a>,
        output_hz: &'a [f64],
        polarization: &'a PolarizationOperator,
    ) -> Result<RowAccumulator<'a>, SpectralOperatorError> {
        row.validate(polarization.correlations().len())?;
        if row.channels.len() < 2 {
            return Err(SpectralOperatorError::InvalidSample);
        }
        if polarization.model_coordinates() != [PolarizationCoordinate::StokesI] {
            return Err(SpectralOperatorError::InvalidSample);
        }
        let output = CasaLinearOutputGrid::compile(output_hz)
            .ok_or(SpectralOperatorError::MissingRowSpectralGeometry)?;
        Ok(RowAccumulator {
            band: self,
            row,
            output_hz,
            output,
            polarization,
            cursor: CasaLinearRowCursor::new(),
            next: 0,
            previous_prediction: SmallVec::new(),
        })
    }

    fn grid_sample(
        &mut self,
        output_channel: usize,
        frequency_hz: f64,
        row: &VisibilityRow<'_>,
        observed: Complex64,
        predicted: Complex64,
        weight: f64,
    ) -> Result<(), SpectralOperatorError> {
        let plane = output_channel - self.core.start;
        if self.phase != BandPhase::Residual {
            self.mapped[plane] = self.mapped[plane]
                .checked_add(1)
                .ok_or(SpectralOperatorError::CoverageOverflow)?;
        }
        if weight == 0.0 {
            return Ok(());
        }
        let wavelength_scale = frequency_hz / SPEED_OF_LIGHT_M_PER_S;
        let Some(taps) = self.convolution.taps([
            row.uvw_m[0] * wavelength_scale,
            row.uvw_m[1] * wavelength_scale,
        ]) else {
            return Ok(());
        };
        let rotation = phase(row.phase_shift_m, frequency_hz);
        for (grid, error, value) in [
            (
                &mut self.dirty,
                &mut self.dirty_error,
                observed * rotation * weight,
            ),
            (
                &mut self.residual,
                &mut self.residual_error,
                (observed - predicted) * rotation * weight,
            ),
            (
                &mut self.psf,
                &mut self.psf_error,
                Complex64::new(weight, 0.0),
            ),
        ] {
            if grid.is_empty() {
                continue;
            }
            self.convolution.grid_compensated(
                &mut grid.index_axis_mut(Axis(0), plane),
                &mut error.index_axis_mut(Axis(0), plane),
                taps,
                value,
            );
        }
        if self.phase != BandPhase::Residual {
            let increment = weight - self.sum_weight_error[plane];
            let updated = self.sum_weight[plane] + increment;
            self.sum_weight_error[plane] = (updated - self.sum_weight[plane]) - increment;
            self.sum_weight[plane] = updated;
        }
        Ok(())
    }

    fn consume_block(
        &mut self,
        block: &NativeBlock,
        layout: &NativeLayout,
        window: Range<usize>,
        native_range: Range<usize>,
        output_hz: &[f64],
        polarization: &PolarizationOperator,
    ) -> Result<(), SpectralOperatorError> {
        if native_range.is_empty() {
            return Ok(());
        }
        if native_range.start < window.start || native_range.end > window.end {
            return Err(SpectralOperatorError::IncompleteSpectralHalo);
        }
        let local = native_range.start - window.start..native_range.end - window.start;
        for row in 0..block.metadata.len() {
            let row = block
                .row(layout, row, window.clone())?
                .window(local.clone())?;
            if let Some(single) = self.single_channel {
                self.consume_single_row(row, single, output_hz, polarization)?;
                continue;
            }
            let native = BandSupport::native_window(
                output_hz,
                self.core.clone(),
                row.frequencies_hz,
                row.original_pair_hz,
            )?;
            if native.is_empty() {
                continue;
            }
            let channels = native.len();
            let mut accumulator = self.begin_row(row.window(native)?, output_hz, polarization)?;
            accumulator.push(0..channels)?;
            accumulator.finish()?;
        }
        Ok(())
    }

    fn consume_single_row(
        &mut self,
        row: VisibilityRow<'_>,
        single: CasaSingleChannel,
        output_hz: &[f64],
        polarization: &PolarizationOperator,
    ) -> Result<(), SpectralOperatorError> {
        row.validate(polarization.correlations().len())?;
        for channel in single.native_window(row.frequencies_hz) {
            let frequency = row.frequencies_hz[channel];
            let predicted = self.predict_native(&row, frequency, output_hz, polarization)?;
            let values = row.values.row(channel);
            let weights = row.weights.row(channel);
            let flags = row
                .flags
                .row(channel)
                .iter()
                .zip(row.weight_flags.row(channel))
                .map(|(&flag, &weight_flag)| flag || weight_flag)
                .collect::<SmallVec<[bool; 4]>>();
            let (observed, predicted, weight) = polarized_sample(
                polarization,
                values.as_slice().expect("validated contiguous row"),
                &predicted,
                weights.as_slice().expect("validated contiguous row"),
                &flags,
            )?;
            self.grid_sample(0, frequency, &row, observed, predicted, weight)?;
        }
        Ok(())
    }
}

fn polarized_sample(
    polarization: &PolarizationOperator,
    observed: &[Complex64],
    predicted: &[Complex64],
    weights: &[f64],
    flags: &[bool],
) -> Result<(Complex64, Complex64, f64), SpectralOperatorError> {
    let observed_adjoint = polarization
        .weighted_adjoint(observed, weights, flags)
        .map_err(|_| SpectralOperatorError::InvalidSample)?[0];
    let predicted_adjoint = polarization
        .weighted_adjoint(predicted, weights, flags)
        .map_err(|_| SpectralOperatorError::InvalidSample)?[0];
    let weight = polarization_diagonal(polarization, weights, flags)[0];
    let direct = (polarization.feed_basis() == crate::FeedBasis::Stokes)
        .then(|| {
            polarization
                .coefficients()
                .iter()
                .position(|value| *value == Complex64::new(1.0, 0.0))
        })
        .flatten();
    let normalize = |adjoint, values: &[Complex64]| {
        if weight == 0.0 {
            Complex64::default()
        } else {
            direct.map_or_else(|| adjoint / weight, |index| values[index])
        }
    };
    Ok((
        normalize(observed_adjoint, observed),
        normalize(predicted_adjoint, predicted),
        weight,
    ))
}

fn phase(shift_m: f64, frequency_hz: f64) -> Complex64 {
    Complex64::from_polar(
        1.0,
        std::f64::consts::TAU * shift_m * frequency_hz / SPEED_OF_LIGHT_M_PER_S,
    )
}

/// A row cursor survives chunk boundaries; only the previous prediction is
/// retained. All other input accesses are borrowed from the compact row.
struct RowAccumulator<'a> {
    band: &'a mut BandWorkspace,
    row: VisibilityRow<'a>,
    output_hz: &'a [f64],
    output: CasaLinearOutputGrid,
    polarization: &'a PolarizationOperator,
    cursor: CasaLinearRowCursor,
    next: usize,
    previous_prediction: SmallVec<[Complex64; 4]>,
}

impl RowAccumulator<'_> {
    fn push(&mut self, channels: Range<usize>) -> Result<(), SpectralOperatorError> {
        if channels.start != self.next || channels.end > self.row.channels.len() {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        for channel in channels {
            let mut address = self.row.address;
            address.channel_index = self.row.channels[channel];
            let prediction = self.band.predict_native(
                &self.row,
                self.row.frequencies_hz[channel],
                self.output_hz,
                self.polarization,
            )?;
            let fine_points = self
                .cursor
                .push(
                    address,
                    self.row.geometry(),
                    self.row.frequencies_hz[channel],
                    self.output,
                )
                .map_err(|_| SpectralOperatorError::IncompleteCoverage)?;
            if let Some(fine_points) = fine_points {
                for fine in fine_points {
                    if !self.band.core.contains(&fine.output_channel()) {
                        continue;
                    }
                    let nearest = if fine.nearest_is_right() {
                        channel
                    } else {
                        channel - 1
                    };
                    let mut observed = SmallVec::<[Complex64; 4]>::new();
                    let mut predicted = SmallVec::<[Complex64; 4]>::new();
                    let mut weights = SmallVec::<[f64; 4]>::new();
                    let mut flags = SmallVec::<[bool; 4]>::new();
                    for correlation in 0..prediction.len() {
                        observed.push(interpolate_complex_pair(
                            self.row.values[(channel - 1, correlation)],
                            self.row.values[(channel, correlation)],
                            fine.factors(),
                        ));
                        predicted.push(interpolate_complex_pair(
                            self.previous_prediction[correlation],
                            prediction[correlation],
                            fine.factors(),
                        ));
                        weights.push(self.row.weights[(nearest, correlation)]);
                        flags.push(
                            fine.linear_flag(
                                self.row.flags[(channel - 1, correlation)],
                                self.row.flags[(channel, correlation)],
                            ) || self.row.weight_flags[(nearest, correlation)],
                        );
                    }
                    // Interpolate observed and predicted separately, then apply
                    // polarization/weights, then subtract in the existing order.
                    let (observed, predicted, weight) = polarized_sample(
                        self.polarization,
                        &observed,
                        &predicted,
                        &weights,
                        &flags,
                    )?;
                    self.band.grid_sample(
                        fine.output_channel(),
                        fine.frequency_hz(),
                        &self.row,
                        observed,
                        predicted,
                        weight,
                    )?;
                }
            }
            self.previous_prediction = prediction;
            self.next += 1;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<(), SpectralOperatorError> {
        if self.next != self.row.channels.len() {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        self.cursor
            .finish()
            .map_err(|_| SpectralOperatorError::IncompleteCoverage)
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

#[path = "memory.rs"]
mod memory;
pub use memory::BandMemory;
