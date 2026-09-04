// SPDX-License-Identifier: LGPL-3.0-or-later

//! Reconstruction-owned paired A/W convolution operator.
//!
//! The catalog is metadata-only. Pixel ownership remains behind a bounded
//! provider so cold CASA import and warm private reuse converge on the same
//! typed cell without materializing the complete CF cache here.

use std::{collections::BTreeSet, fmt, mem::size_of, sync::Arc};

use casa_imaging_model::{PreparedArtifactScientificIdentity, PreparedArtifactScientificKind};
use num_complex::Complex64;
use thiserror::Error;

const MUELLER_ELEMENTS: u32 = 16;

/// A prepared AW artifact cannot form the requested paired operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum AwOperatorError {
    /// A coordinate, normalization, or active numerical value is non-finite.
    #[error("AW operator received a non-finite value")]
    NonFiniteValue,
    /// A prepared kernel has an impossible logical layout or normalization.
    #[error("AW prepared kernel layout is invalid")]
    InvalidKernelLayout,
    /// A loaded cell does not match its selected metadata.
    #[error("AW loaded cell does not match selected prepared metadata")]
    PreparedCellMismatch,
    /// The catalog is empty, duplicated, or mixes incompatible W laws.
    #[error("AW prepared-cell catalog layout is invalid")]
    InvalidCatalogLayout,
    /// The requested Mueller term is not represented.
    #[error("AW Mueller term is unsupported")]
    UnsupportedMueller,
    /// The requested conjugate-frequency transform has no finite positive result.
    #[error("AW frequency is unsupported")]
    UnsupportedFrequency,
    /// The requested parallactic angle has no represented bin.
    #[error("AW parallactic angle is unsupported")]
    UnsupportedParallacticAngle,
    /// Independently selected axes do not have a corresponding prepared pair.
    #[error("AW prepared catalog is missing the selected cell")]
    MissingCell,
    /// The provider could not supply the selected paired cell.
    #[error("AW selected prepared cell is unavailable")]
    PreparedCellUnavailable,
    /// A selected cell cannot fit under the operator's residency ceiling.
    #[error("AW selected prepared cell exceeds the residency ceiling")]
    ResidencyCeilingExceeded,
    /// A visibility footprint crosses the supplied grid or its shape is invalid.
    #[error("AW convolution footprint does not fit the supplied grid")]
    InvalidGridLayout,
    /// A data weight is negative.
    #[error("AW data weight must be non-negative")]
    InvalidWeight,
    /// Exact operator or provider accounting overflowed.
    #[error("AW operator measurement accounting overflowed")]
    MeasurementOverflow,
}

/// Logical layout of one dense oversampled convolution plane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AwKernelLayout {
    support: [usize; 2],
    oversampling: usize,
}

impl AwKernelLayout {
    /// Validate an integral support radius and oversampling factor.
    pub fn new(support: [usize; 2], oversampling: usize) -> Result<Self, AwOperatorError> {
        if oversampling == 0 || logical_tap_count(support, oversampling).is_none() {
            return Err(AwOperatorError::InvalidKernelLayout);
        }
        Ok(Self {
            support,
            oversampling,
        })
    }

    fn integral_tap_count(self) -> usize {
        (self.support[0] * 2 + 1) * (self.support[1] * 2 + 1)
    }
}

/// One dense oversampled convolution plane in canonical logical layout.
///
/// Taps are ordered by fractional Y, fractional X, integral Y offset, then
/// integral X offset. The supplied normalization is divided out once while
/// fusing the selected fractional plane.
#[derive(Clone, Debug, PartialEq)]
pub struct AwConvolutionKernel {
    layout: AwKernelLayout,
    normalization: Complex64,
    taps: Box<[Complex64]>,
}

impl AwConvolutionKernel {
    /// Validate one canonical prepared convolution plane.
    pub fn new(
        layout: AwKernelLayout,
        normalization: Complex64,
        taps: Vec<Complex64>,
    ) -> Result<Self, AwOperatorError> {
        if logical_tap_count(layout.support, layout.oversampling) != Some(taps.len())
            || !finite(normalization)
            || normalization.norm_sqr() == 0.0
            || taps.iter().any(|tap| !finite(*tap))
        {
            return Err(AwOperatorError::InvalidKernelLayout);
        }
        Ok(Self {
            layout,
            normalization,
            taps: taps.into_boxed_slice(),
        })
    }

    fn tap(&self, fractional: [usize; 2], offset: [usize; 2]) -> Complex64 {
        let width = self.layout.support[0] * 2 + 1;
        let height = self.layout.support[1] * 2 + 1;
        let index = (((fractional[1] * self.layout.oversampling + fractional[0]) * height
            + offset[1])
            * width)
            + offset[0];
        self.taps[index] / self.normalization
    }

    fn resident_bytes(&self) -> usize {
        self.taps.len() * size_of::<Complex64>()
    }
}

/// Metadata-only key and layout for one paired prepared-CF cell.
#[derive(Clone, Debug, PartialEq)]
pub struct AwPreparedCellMetadata {
    identity: PreparedArtifactScientificIdentity,
    frequency_hz: f64,
    w_value_lambda: f64,
    w_increment: f64,
    mueller_element: u32,
    parallactic_angle_deg: f64,
    imaging_layout: AwKernelLayout,
    weight_layout: AwKernelLayout,
}

impl AwPreparedCellMetadata {
    /// Construct one owner-validated paired-cell index entry.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        identity: PreparedArtifactScientificIdentity,
        frequency_hz: f64,
        w_value_lambda: f64,
        w_increment: f64,
        mueller_element: u32,
        parallactic_angle_deg: f64,
        imaging_layout: AwKernelLayout,
        weight_layout: AwKernelLayout,
    ) -> Result<Self, AwOperatorError> {
        if identity.kind() != PreparedArtifactScientificKind::ConvolutionFunction {
            return Err(AwOperatorError::InvalidCatalogLayout);
        }
        if !frequency_hz.is_finite()
            || frequency_hz <= 0.0
            || !w_value_lambda.is_finite()
            || !w_increment.is_finite()
            || w_increment <= 0.0
            || !parallactic_angle_deg.is_finite()
        {
            return Err(AwOperatorError::NonFiniteValue);
        }
        if mueller_element >= MUELLER_ELEMENTS {
            return Err(AwOperatorError::UnsupportedMueller);
        }
        Ok(Self {
            identity,
            frequency_hz,
            w_value_lambda,
            w_increment,
            mueller_element,
            parallactic_angle_deg: circular_degrees(parallactic_angle_deg),
            imaging_layout,
            weight_layout,
        })
    }

    /// Return the owner-minted key used to request the cell from a provider.
    #[must_use]
    pub const fn identity(&self) -> PreparedArtifactScientificIdentity {
        self.identity
    }
}

/// One resident paired imaging/weight prepared-CF cell.
#[derive(Clone, Debug, PartialEq)]
pub struct AwConvolutionCell {
    identity: PreparedArtifactScientificIdentity,
    imaging: AwConvolutionKernel,
    weight: AwConvolutionKernel,
}

impl AwConvolutionCell {
    /// Construct one resident pair. Imaging and weight footprints may differ.
    pub fn new(
        identity: PreparedArtifactScientificIdentity,
        imaging: AwConvolutionKernel,
        weight: AwConvolutionKernel,
    ) -> Result<Self, AwOperatorError> {
        if identity.kind() != PreparedArtifactScientificKind::ConvolutionFunction {
            return Err(AwOperatorError::PreparedCellMismatch);
        }
        Ok(Self {
            identity,
            imaging,
            weight,
        })
    }

    /// Exact complex-pixel payload bytes retained by this pair.
    #[must_use]
    pub fn resident_bytes(&self) -> usize {
        self.imaging.resident_bytes() + self.weight.resident_bytes()
    }
}

/// Immutable metadata-only catalog of paired prepared-CF cells.
#[derive(Clone, Debug, PartialEq)]
pub struct AwPreparedCatalog {
    cells: Box<[AwPreparedCellMetadata]>,
    w_increment: f64,
}

impl AwPreparedCatalog {
    /// Validate and canonically order one complete prepared catalog index.
    pub fn new(mut cells: Vec<AwPreparedCellMetadata>) -> Result<Self, AwOperatorError> {
        let first = cells.first().ok_or(AwOperatorError::InvalidCatalogLayout)?;
        let w_increment = first.w_increment;
        if cells
            .iter()
            .any(|cell| cell.w_increment.to_bits() != w_increment.to_bits())
        {
            return Err(AwOperatorError::InvalidCatalogLayout);
        }
        cells.sort_by(cell_order);
        if cells.windows(2).any(|pair| same_key(&pair[0], &pair[1])) {
            return Err(AwOperatorError::InvalidCatalogLayout);
        }
        Ok(Self {
            cells: cells.into_boxed_slice(),
            w_increment,
        })
    }

    /// Maximum integral imaging-CF support radius represented by this catalog.
    #[must_use]
    pub fn maximum_imaging_support(&self) -> usize {
        self.cells
            .iter()
            .flat_map(|cell| cell.imaging_layout.support)
            .max()
            .unwrap_or(0)
    }

    fn grid_cell(
        &self,
        sample: AwVisibilitySample,
        conjugate_beams: bool,
    ) -> Result<&AwPreparedCellMetadata, AwOperatorError> {
        let frequency = if conjugate_beams {
            conjugate_frequency(sample.frequency_hz, sample.reference_frequency_hz)?
        } else {
            sample.frequency_hz
        };
        let mueller = if sample.w_lambda > 0.0 {
            sample.mueller_element
        } else {
            conjugate_mueller(sample.mueller_element)?
        };
        self.select(
            frequency,
            sample.w_lambda,
            mueller,
            sample.parallactic_angle_deg,
        )
    }

    fn degrid_cell(
        &self,
        sample: AwVisibilitySample,
    ) -> Result<&AwPreparedCellMetadata, AwOperatorError> {
        let mueller = if sample.w_lambda > 0.0 {
            conjugate_mueller(sample.mueller_element)?
        } else {
            sample.mueller_element
        };
        self.select(
            sample.frequency_hz,
            sample.w_lambda,
            mueller,
            sample.parallactic_angle_deg,
        )
    }

    fn select(
        &self,
        frequency: f64,
        w: f64,
        mueller: u32,
        pa: f64,
    ) -> Result<&AwPreparedCellMetadata, AwOperatorError> {
        let mueller_cells = self
            .cells
            .iter()
            .filter(|cell| cell.mueller_element == mueller)
            .collect::<Vec<_>>();
        if mueller_cells.is_empty() {
            return Err(AwOperatorError::UnsupportedMueller);
        }
        let frequencies = unique_sorted(mueller_cells.iter().map(|cell| cell.frequency_hz));
        let selected_frequency = nearest_linear(&frequencies, frequency);
        let w_values = unique_sorted(mueller_cells.iter().map(|cell| cell.w_value_lambda));
        let w_index =
            ((self.w_increment * w.abs()).sqrt().round() as usize).min(w_values.len() - 1);
        let selected_w = w_values[w_index];
        let pa_values = unique_sorted(mueller_cells.iter().map(|cell| cell.parallactic_angle_deg));
        let selected_pa = pa_values
            .iter()
            .copied()
            .min_by(|left, right| {
                circular_degrees(*left - pa)
                    .abs()
                    .total_cmp(&circular_degrees(*right - pa).abs())
                    .then_with(|| left.total_cmp(right))
            })
            .ok_or(AwOperatorError::UnsupportedParallacticAngle)?;
        mueller_cells
            .into_iter()
            .find(|cell| {
                cell.frequency_hz.to_bits() == selected_frequency.to_bits()
                    && cell.w_value_lambda.to_bits() == selected_w.to_bits()
                    && cell.parallactic_angle_deg.to_bits() == selected_pa.to_bits()
            })
            .ok_or(AwOperatorError::MissingCell)
    }
}

/// Provider disposition for one bounded prepared-cell lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AwPreparedCellDisposition {
    /// The exact cell was already resident.
    Resident,
    /// The provider loaded or decoded the cell for this request.
    Loaded,
}

/// One provider-owned immutable cell plus exact cache-work measurements.
pub struct AwPreparedCellLease {
    cell: Arc<AwConvolutionCell>,
    disposition: AwPreparedCellDisposition,
    evicted_bytes: usize,
    copied_bytes: usize,
    release: Option<Box<dyn FnOnce() + Send + 'static>>,
}

impl fmt::Debug for AwPreparedCellLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AwPreparedCellLease")
            .field("cell", &self.cell)
            .field("disposition", &self.disposition)
            .field("evicted_bytes", &self.evicted_bytes)
            .field("copied_bytes", &self.copied_bytes)
            .finish_non_exhaustive()
    }
}

impl AwPreparedCellLease {
    /// Bind one immutable cell and its exact provider accounting.
    #[must_use]
    pub fn new(
        cell: Arc<AwConvolutionCell>,
        disposition: AwPreparedCellDisposition,
        evicted_bytes: usize,
        copied_bytes: usize,
    ) -> Self {
        Self {
            cell,
            disposition,
            evicted_bytes,
            copied_bytes,
            release: None,
        }
    }

    /// Wake the provider when this non-cloneable pin is released.
    #[must_use]
    pub fn with_release_notifier(mut self, release: impl FnOnce() + Send + 'static) -> Self {
        self.release = Some(Box::new(release));
        self
    }

    /// Borrow the selected immutable cell for the lifetime of this pin.
    #[must_use]
    pub fn cell(&self) -> &AwConvolutionCell {
        &self.cell
    }

    /// Return whether the request hit resident state or loaded pixels.
    #[must_use]
    pub const fn disposition(&self) -> AwPreparedCellDisposition {
        self.disposition
    }

    /// Return payload bytes evicted while satisfying this request.
    #[must_use]
    pub const fn evicted_bytes(&self) -> usize {
        self.evicted_bytes
    }

    /// Return payload bytes copied while satisfying this request.
    #[must_use]
    pub const fn copied_bytes(&self) -> usize {
        self.copied_bytes
    }
}

impl Drop for AwPreparedCellLease {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release();
        }
    }
}

/// Bounded pixel provider implemented by the prepared-artifact owner.
pub trait AwPreparedCellProvider {
    /// Supply one selected pair while respecting the stated total byte ceiling.
    fn load(
        &mut self,
        metadata: &AwPreparedCellMetadata,
        resident_byte_ceiling: usize,
    ) -> Result<AwPreparedCellLease, AwOperatorError>;
}

impl<T: AwPreparedCellProvider + ?Sized> AwPreparedCellProvider for Box<T> {
    fn load(
        &mut self,
        metadata: &AwPreparedCellMetadata,
        resident_byte_ceiling: usize,
    ) -> Result<AwPreparedCellLease, AwOperatorError> {
        (**self).load(metadata, resident_byte_ceiling)
    }
}

type AwProviderFactory = dyn Fn() -> Box<dyn AwPreparedCellProvider + Send> + Send + Sync + 'static;

/// Cloneable, opaque AW binding carried from application preparation to each
/// reconstruction-owned physical chart.
#[derive(Clone)]
pub struct PreparedAwProjection {
    catalog: AwPreparedCatalog,
    provider: Arc<AwProviderFactory>,
    conjugate_beams: bool,
    resident_byte_ceiling: usize,
}

impl fmt::Debug for PreparedAwProjection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedAwProjection")
            .field("catalog", &self.catalog)
            .field("conjugate_beams", &self.conjugate_beams)
            .field("resident_byte_ceiling", &self.resident_byte_ceiling)
            .finish_non_exhaustive()
    }
}

impl PreparedAwProjection {
    /// Freeze a validated metadata catalog and a cloneable bounded cell provider.
    pub fn new<P>(
        catalog: AwPreparedCatalog,
        provider: P,
        conjugate_beams: bool,
        resident_byte_ceiling: usize,
    ) -> Result<Self, AwOperatorError>
    where
        P: AwPreparedCellProvider + Clone + Send + Sync + 'static,
    {
        if resident_byte_ceiling == 0 {
            return Err(AwOperatorError::ResidencyCeilingExceeded);
        }
        Ok(Self {
            catalog,
            provider: Arc::new(move || Box::new(provider.clone())),
            conjugate_beams,
            resident_byte_ceiling,
        })
    }

    /// Exact maximum simultaneously resident paired-CF payload bytes.
    #[must_use]
    pub const fn resident_byte_ceiling(&self) -> usize {
        self.resident_byte_ceiling
    }

    /// Maximum imaging-CF support radius needed by bounded tile replay.
    #[must_use]
    pub fn maximum_imaging_support(&self) -> usize {
        self.catalog.maximum_imaging_support()
    }

    pub(crate) fn instantiate(
        &self,
    ) -> Result<AwProjectionOperator<Box<dyn AwPreparedCellProvider + Send>>, AwOperatorError> {
        AwProjectionOperator::new(
            self.catalog.clone(),
            (self.provider)(),
            self.conjugate_beams,
            self.resident_byte_ceiling,
        )
    }
}

/// Row-local coordinates needed to select and place one AW visibility.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AwVisibilitySample {
    frequency_hz: f64,
    reference_frequency_hz: f64,
    w_lambda: f64,
    mueller_element: u32,
    parallactic_angle_deg: f64,
    grid_position: [f64; 2],
    uv_lambda: [f64; 2],
    pointing_offset_lm: [f64; 2],
}

impl AwVisibilitySample {
    /// Validate the complete row-local operator coordinates.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        frequency_hz: f64,
        reference_frequency_hz: f64,
        w_lambda: f64,
        mueller_element: u32,
        parallactic_angle_deg: f64,
        grid_position: [f64; 2],
        uv_lambda: [f64; 2],
        pointing_offset_lm: [f64; 2],
    ) -> Result<Self, AwOperatorError> {
        if !frequency_hz.is_finite()
            || frequency_hz <= 0.0
            || !reference_frequency_hz.is_finite()
            || reference_frequency_hz <= 0.0
            || !w_lambda.is_finite()
            || !parallactic_angle_deg.is_finite()
            || grid_position
                .into_iter()
                .chain(uv_lambda)
                .chain(pointing_offset_lm)
                .any(|value| !value.is_finite())
        {
            return Err(AwOperatorError::NonFiniteValue);
        }
        if mueller_element >= MUELLER_ELEMENTS {
            return Err(AwOperatorError::UnsupportedMueller);
        }
        Ok(Self {
            frequency_hz,
            reference_frequency_hz,
            w_lambda,
            mueller_element,
            parallactic_angle_deg,
            grid_position,
            uv_lambda,
            pointing_offset_lm,
        })
    }
}

/// Deterministic reconstruction-local work and bounded-residency counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AwOperatorDiagnostics {
    /// Catalog selections performed.
    pub selections: u64,
    /// Prediction/degridding passes.
    pub degrid_passes: u64,
    /// Weighted-adjoint gridding passes.
    pub grid_passes: u64,
    /// Imaging tap coefficients evaluated.
    pub imaging_taps: u64,
    /// Weight tap coefficients accumulated.
    pub weight_taps: u64,
    /// Provider resident hits.
    pub provider_hits: u64,
    /// Provider cell loads.
    pub provider_loads: u64,
    /// Provider-reported evicted bytes.
    pub evicted_bytes: u64,
    /// Provider-reported payload copy bytes.
    pub copied_bytes: u64,
    /// Explicit provider residency ceiling.
    pub resident_byte_ceiling: usize,
}

/// A metadata catalog bound to one bounded prepared-cell provider.
pub struct AwProjectionOperator<P> {
    catalog: AwPreparedCatalog,
    provider: P,
    conjugate_beams: bool,
    diagnostics: AwOperatorDiagnostics,
}

impl<P: AwPreparedCellProvider> AwProjectionOperator<P> {
    /// Bind a catalog and provider under a nonzero resident-payload ceiling.
    pub fn new(
        catalog: AwPreparedCatalog,
        provider: P,
        conjugate_beams: bool,
        resident_byte_ceiling: usize,
    ) -> Result<Self, AwOperatorError> {
        if resident_byte_ceiling == 0 {
            return Err(AwOperatorError::ResidencyCeilingExceeded);
        }
        Ok(Self {
            catalog,
            provider,
            conjugate_beams,
            diagnostics: AwOperatorDiagnostics {
                resident_byte_ceiling,
                ..AwOperatorDiagnostics::default()
            },
        })
    }
    /// Return exact deterministic operator and provider counters.
    #[must_use]
    pub const fn diagnostics(&self) -> AwOperatorDiagnostics {
        self.diagnostics
    }

    /// Select the gridding metadata and return the exact integral footprint
    /// center and tap count without loading the paired payload.
    pub(crate) fn grid_footprint(
        &self,
        shape: [usize; 2],
        sample: AwVisibilitySample,
    ) -> Result<([usize; 2], usize), AwOperatorError> {
        let metadata = self.catalog.grid_cell(sample, self.conjugate_beams)?;
        let (x, _) = fractional_bin(
            sample.grid_position[0],
            metadata.imaging_layout.oversampling,
        )?;
        let (y, _) = fractional_bin(
            sample.grid_position[1],
            metadata.imaging_layout.oversampling,
        )?;
        let center = [
            usize::try_from(x).map_err(|_| AwOperatorError::InvalidGridLayout)?,
            usize::try_from(y).map_err(|_| AwOperatorError::InvalidGridLayout)?,
        ];
        for axis in 0..2 {
            let support = metadata.imaging_layout.support[axis];
            if center[axis] < support
                || center[axis]
                    .checked_add(support)
                    .is_none_or(|end| end >= shape[axis])
            {
                return Err(AwOperatorError::InvalidGridLayout);
            }
        }
        Ok((center, metadata.imaging_layout.integral_tap_count()))
    }
    /// Predict one visibility with CASA's normal-frequency/swapped-Mueller selection.
    pub fn degrid(
        &mut self,
        grid: &[Complex64],
        shape: [usize; 2],
        sample: AwVisibilitySample,
    ) -> Result<Complex64, AwOperatorError> {
        validate_grid(grid, shape)?;
        let metadata = self.catalog.degrid_cell(sample)?;
        let cell = load_cell(&mut self.provider, metadata, &mut self.diagnostics)?;
        let taps = fused_taps(&cell.cell().imaging, shape, sample, true)?;
        add_measurement(&mut self.diagnostics.selections, 1)?;
        add_measurement(&mut self.diagnostics.degrid_passes, 1)?;
        add_measurement(&mut self.diagnostics.imaging_taps, taps.len() as u64)?;
        Ok(taps
            .into_iter()
            .map(|tap| tap.coefficient * grid[tap.index])
            .sum())
    }
    /// Apply CASA's conjugate-frequency grid selection, exact adjoint, and paired weight CF.
    pub fn grid(
        &mut self,
        image_grid: &mut [Complex64],
        weight_grid: &mut [Complex64],
        shape: [usize; 2],
        sample: AwVisibilitySample,
        visibility: Complex64,
        data_weight: f64,
    ) -> Result<(), AwOperatorError> {
        if !finite(visibility) || !data_weight.is_finite() {
            return Err(AwOperatorError::NonFiniteValue);
        }
        if data_weight < 0.0 {
            return Err(AwOperatorError::InvalidWeight);
        }
        validate_grid(image_grid, shape)?;
        validate_grid(weight_grid, shape)?;
        let metadata = self.catalog.grid_cell(sample, self.conjugate_beams)?;
        let cell = load_cell(&mut self.provider, metadata, &mut self.diagnostics)?;
        let imaging = fused_taps(&cell.cell().imaging, shape, sample, true)?;
        let weight = fused_taps(&cell.cell().weight, shape, sample, false)?;
        for tap in &imaging {
            image_grid[tap.index] += tap.coefficient.conj() * visibility * data_weight;
        }
        for tap in &weight {
            weight_grid[tap.index] += tap.coefficient * data_weight;
        }
        add_measurement(&mut self.diagnostics.selections, 1)?;
        add_measurement(&mut self.diagnostics.grid_passes, 1)?;
        add_measurement(&mut self.diagnostics.imaging_taps, imaging.len() as u64)?;
        add_measurement(&mut self.diagnostics.weight_taps, weight.len() as u64)?;
        Ok(())
    }

    pub(crate) fn grid_imaging_compensated(
        &mut self,
        image_grid: &mut [Complex64],
        compensation: &mut [Complex64],
        shape: [usize; 2],
        sample: AwVisibilitySample,
        value: Complex64,
    ) -> Result<(), AwOperatorError> {
        validate_grid(image_grid, shape)?;
        validate_grid(compensation, shape)?;
        let metadata = self.catalog.grid_cell(sample, self.conjugate_beams)?;
        let cell = load_cell(&mut self.provider, metadata, &mut self.diagnostics)?;
        let taps = fused_taps(&cell.cell().imaging, shape, sample, true)?;
        compensated_taps(image_grid, compensation, &taps, value, true);
        add_measurement(&mut self.diagnostics.selections, 1)?;
        add_measurement(&mut self.diagnostics.grid_passes, 1)?;
        add_measurement(&mut self.diagnostics.imaging_taps, taps.len() as u64)?;
        Ok(())
    }

    pub(crate) fn grid_weight_compensated(
        &mut self,
        weight_grid: &mut [Complex64],
        compensation: &mut [Complex64],
        shape: [usize; 2],
        sample: AwVisibilitySample,
        coefficient: f64,
    ) -> Result<(), AwOperatorError> {
        if !coefficient.is_finite() {
            return Err(AwOperatorError::InvalidWeight);
        }
        validate_grid(weight_grid, shape)?;
        validate_grid(compensation, shape)?;
        let metadata = self.catalog.grid_cell(sample, self.conjugate_beams)?;
        let cell = load_cell(&mut self.provider, metadata, &mut self.diagnostics)?;
        let taps = fused_taps(&cell.cell().weight, shape, sample, false)?;
        compensated_taps(
            weight_grid,
            compensation,
            &taps,
            Complex64::new(coefficient, 0.0),
            false,
        );
        add_measurement(&mut self.diagnostics.selections, 1)?;
        add_measurement(&mut self.diagnostics.grid_passes, 1)?;
        add_measurement(&mut self.diagnostics.weight_taps, taps.len() as u64)?;
        Ok(())
    }
}

fn compensated_taps(
    grid: &mut [Complex64],
    compensation: &mut [Complex64],
    taps: &[FusedTap],
    value: Complex64,
    adjoint: bool,
) {
    for tap in taps {
        let coefficient = if adjoint {
            tap.coefficient.conj()
        } else {
            tap.coefficient
        };
        let contribution = coefficient * value - compensation[tap.index];
        let updated = grid[tap.index] + contribution;
        compensation[tap.index] = (updated - grid[tap.index]) - contribution;
        grid[tap.index] = updated;
    }
}

fn load_cell<P: AwPreparedCellProvider>(
    provider: &mut P,
    metadata: &AwPreparedCellMetadata,
    diagnostics: &mut AwOperatorDiagnostics,
) -> Result<AwPreparedCellLease, AwOperatorError> {
    let lease = provider.load(metadata, diagnostics.resident_byte_ceiling)?;
    if lease.cell().identity != metadata.identity
        || lease.cell().imaging.layout != metadata.imaging_layout
        || lease.cell().weight.layout != metadata.weight_layout
    {
        return Err(AwOperatorError::PreparedCellMismatch);
    }
    if lease.cell().resident_bytes() > diagnostics.resident_byte_ceiling {
        return Err(AwOperatorError::ResidencyCeilingExceeded);
    }
    match lease.disposition() {
        AwPreparedCellDisposition::Resident => add_measurement(&mut diagnostics.provider_hits, 1)?,
        AwPreparedCellDisposition::Loaded => add_measurement(&mut diagnostics.provider_loads, 1)?,
    }
    add_measurement(&mut diagnostics.evicted_bytes, lease.evicted_bytes() as u64)?;
    add_measurement(&mut diagnostics.copied_bytes, lease.copied_bytes() as u64)?;
    Ok(lease)
}

fn add_measurement(counter: &mut u64, amount: u64) -> Result<(), AwOperatorError> {
    *counter = counter
        .checked_add(amount)
        .ok_or(AwOperatorError::MeasurementOverflow)?;
    Ok(())
}

#[derive(Clone, Copy)]
struct FusedTap {
    index: usize,
    coefficient: Complex64,
}
fn fused_taps(
    kernel: &AwConvolutionKernel,
    shape: [usize; 2],
    sample: AwVisibilitySample,
    apply_pointing: bool,
) -> Result<Vec<FusedTap>, AwOperatorError> {
    let (base_x, frac_x) = fractional_bin(sample.grid_position[0], kernel.layout.oversampling)?;
    let (base_y, frac_y) = fractional_bin(sample.grid_position[1], kernel.layout.oversampling)?;
    let pointing = if apply_pointing {
        Complex64::from_polar(
            1.0,
            -std::f64::consts::TAU
                * (sample.uv_lambda[0] * sample.pointing_offset_lm[0]
                    + sample.uv_lambda[1] * sample.pointing_offset_lm[1]),
        )
    } else {
        Complex64::new(1.0, 0.0)
    };
    let mut taps = Vec::with_capacity(kernel.layout.integral_tap_count());
    for oy in 0..=kernel.layout.support[1] * 2 {
        let y = placed(base_y, oy, kernel.layout.support[1], shape[1])?;
        for ox in 0..=kernel.layout.support[0] * 2 {
            let x = placed(base_x, ox, kernel.layout.support[0], shape[0])?;
            let mut coefficient = kernel.tap([frac_x, frac_y], [ox, oy]);
            if sample.w_lambda > 0.0 {
                coefficient = coefficient.conj();
            }
            taps.push(FusedTap {
                index: x * shape[1] + y,
                coefficient: coefficient * pointing,
            });
        }
    }
    Ok(taps)
}
fn conjugate_frequency(frequency: f64, reference: f64) -> Result<f64, AwOperatorError> {
    let radicand = 2.0 * reference * reference - frequency * frequency;
    if !radicand.is_finite() || radicand <= 0.0 {
        Err(AwOperatorError::UnsupportedFrequency)
    } else {
        Ok(radicand.sqrt())
    }
}
fn conjugate_mueller(mueller: u32) -> Result<u32, AwOperatorError> {
    (MUELLER_ELEMENTS - 1)
        .checked_sub(mueller)
        .ok_or(AwOperatorError::UnsupportedMueller)
}
fn logical_tap_count(support: [usize; 2], oversampling: usize) -> Option<usize> {
    support[0]
        .checked_mul(2)?
        .checked_add(1)?
        .checked_mul(support[1].checked_mul(2)?.checked_add(1)?)?
        .checked_mul(oversampling)?
        .checked_mul(oversampling)
}
fn fractional_bin(position: f64, oversampling: usize) -> Result<(isize, usize), AwOperatorError> {
    let mut base = position.floor();
    let mut fraction = ((position - base) * oversampling as f64).round() as usize;
    if fraction == oversampling {
        base += 1.0;
        fraction = 0;
    }
    if base < isize::MIN as f64 || base > isize::MAX as f64 {
        Err(AwOperatorError::InvalidGridLayout)
    } else {
        Ok((base as isize, fraction))
    }
}
fn placed(
    base: isize,
    offset: usize,
    support: usize,
    bound: usize,
) -> Result<usize, AwOperatorError> {
    base.checked_add(offset as isize)
        .and_then(|value| value.checked_sub(support as isize))
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value < bound)
        .ok_or(AwOperatorError::InvalidGridLayout)
}
fn validate_grid(grid: &[Complex64], shape: [usize; 2]) -> Result<(), AwOperatorError> {
    if shape.contains(&0) || shape[0].checked_mul(shape[1]) != Some(grid.len()) {
        Err(AwOperatorError::InvalidGridLayout)
    } else {
        Ok(())
    }
}
fn cell_order(left: &AwPreparedCellMetadata, right: &AwPreparedCellMetadata) -> std::cmp::Ordering {
    left.mueller_element
        .cmp(&right.mueller_element)
        .then_with(|| left.w_value_lambda.total_cmp(&right.w_value_lambda))
        .then_with(|| left.frequency_hz.total_cmp(&right.frequency_hz))
        .then_with(|| {
            left.parallactic_angle_deg
                .total_cmp(&right.parallactic_angle_deg)
        })
        .then_with(|| left.identity.as_bytes().cmp(&right.identity.as_bytes()))
}
fn same_key(left: &AwPreparedCellMetadata, right: &AwPreparedCellMetadata) -> bool {
    left.mueller_element == right.mueller_element
        && left.w_value_lambda.to_bits() == right.w_value_lambda.to_bits()
        && left.frequency_hz.to_bits() == right.frequency_hz.to_bits()
        && left.parallactic_angle_deg.to_bits() == right.parallactic_angle_deg.to_bits()
}
fn unique_sorted(values: impl IntoIterator<Item = f64>) -> Vec<f64> {
    let mut bits = BTreeSet::new();
    bits.extend(values.into_iter().map(f64::to_bits));
    let mut values = bits.into_iter().map(f64::from_bits).collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    values
}
fn nearest_linear(values: &[f64], requested: f64) -> f64 {
    values
        .iter()
        .copied()
        .min_by(|left, right| {
            (*left - requested)
                .abs()
                .total_cmp(&(*right - requested).abs())
                .then_with(|| left.total_cmp(right))
        })
        .expect("validated non-empty axis")
}
fn circular_degrees(value: f64) -> f64 {
    (value + 180.0).rem_euclid(360.0) - 180.0
}
fn finite(value: Complex64) -> bool {
    value.re.is_finite() && value.im.is_finite()
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_imaging_model::{PreparedArtifactAwInterpretation, PreparedArtifactCellSemantics};
    use std::collections::BTreeMap;

    fn identity(
        frequency: f64,
        w: f64,
        mueller: u32,
        pa: f64,
    ) -> PreparedArtifactScientificIdentity {
        PreparedArtifactScientificIdentity::convolution_function(
            PreparedArtifactCellSemantics::new(
                frequency,
                w,
                mueller,
                mueller,
                pa,
                frequency,
                15 - mueller,
                "EVLA",
                "L",
                25.0,
                1.0,
                PreparedArtifactAwInterpretation::Wavelength,
                false,
                "discrete-complex-sum",
            )
            .unwrap(),
        )
        .unwrap()
    }
    fn layout(support: [usize; 2], os: usize) -> AwKernelLayout {
        AwKernelLayout::new(support, os).unwrap()
    }
    fn metadata(f: f64, w: f64, m: u32, pa: f64) -> AwPreparedCellMetadata {
        AwPreparedCellMetadata::new(
            identity(f, w, m, pa),
            f,
            w,
            1.0,
            m,
            pa,
            layout([1, 1], 2),
            layout([2, 1], 1),
        )
        .unwrap()
    }
    fn resident(meta: &AwPreparedCellMetadata) -> Arc<AwConvolutionCell> {
        let make = |layout: AwKernelLayout, scale: f64| {
            AwConvolutionKernel::new(
                layout,
                Complex64::new(2.0, 0.5),
                (0..logical_tap_count(layout.support, layout.oversampling).unwrap())
                    .map(|i| Complex64::new(scale * (i + 1) as f64, scale * 0.25))
                    .collect(),
            )
            .unwrap()
        };
        Arc::new(
            AwConvolutionCell::new(
                meta.identity,
                make(meta.imaging_layout, 1.0),
                make(meta.weight_layout, 0.5),
            )
            .unwrap(),
        )
    }
    #[derive(Clone)]
    struct Provider {
        cells: BTreeMap<[u8; 32], Arc<AwConvolutionCell>>,
        seen: BTreeSet<[u8; 32]>,
    }
    impl AwPreparedCellProvider for Provider {
        fn load(
            &mut self,
            metadata: &AwPreparedCellMetadata,
            ceiling: usize,
        ) -> Result<AwPreparedCellLease, AwOperatorError> {
            let cell = self
                .cells
                .get(&metadata.identity.as_bytes())
                .cloned()
                .ok_or(AwOperatorError::PreparedCellUnavailable)?;
            if cell.resident_bytes() > ceiling {
                return Err(AwOperatorError::ResidencyCeilingExceeded);
            }
            let loaded = self.seen.insert(metadata.identity.as_bytes());
            Ok(AwPreparedCellLease::new(
                cell,
                if loaded {
                    AwPreparedCellDisposition::Loaded
                } else {
                    AwPreparedCellDisposition::Resident
                },
                0,
                0,
            ))
        }
    }
    fn operator(entries: Vec<AwPreparedCellMetadata>) -> AwProjectionOperator<Provider> {
        let cells = entries
            .iter()
            .map(|m| (m.identity.as_bytes(), resident(m)))
            .collect();
        AwProjectionOperator::new(
            AwPreparedCatalog::new(entries).unwrap(),
            Provider {
                cells,
                seen: BTreeSet::new(),
            },
            true,
            64 * 1024,
        )
        .unwrap()
    }
    fn sample(f: f64, w: f64, m: u32, pa: f64) -> AwVisibilitySample {
        AwVisibilitySample::new(f, 10.0, w, m, pa, [4.25, 4.75], [17.0, -5.0], [2e-4, -3e-4])
            .unwrap()
    }

    #[test]
    fn t51_grid_and_prediction_use_distinct_casa_selection_laws() {
        let mut entries = Vec::new();
        for m in [3, 12] {
            for f in [8.0, 10.0, 12.0] {
                for w in [0.0, 1.0, 2.0] {
                    for pa in [-179.0, 20.0] {
                        entries.push(metadata(f, w, m, pa));
                    }
                }
            }
        }
        let catalog = AwPreparedCatalog::new(entries).unwrap();
        let s = sample(56.0_f64.sqrt(), -4.0, 3, 179.5);
        let grid = catalog.grid_cell(s, true).unwrap();
        assert_eq!(
            (
                grid.mueller_element,
                grid.frequency_hz,
                grid.w_value_lambda,
                grid.parallactic_angle_deg
            ),
            (12, 12.0, 2.0, -179.0)
        );
        let degrid = catalog.degrid_cell(s).unwrap();
        assert_eq!(
            (
                degrid.mueller_element,
                degrid.frequency_hz,
                degrid.w_value_lambda,
                degrid.parallactic_angle_deg
            ),
            (3, 8.0, 2.0, -179.0)
        );
        let direct = sample(10.0, 4.0, 3, 20.0);
        assert_eq!(catalog.degrid_cell(direct).unwrap().mueller_element, 12);
        assert_eq!(catalog.grid_cell(direct, false).unwrap().mueller_element, 3);
    }

    #[test]
    fn t51_vlass_endpoint_frequency_and_w_requests_follow_casa_nearest_maps() {
        const LOWEST_CF_FREQUENCY_HZ: f64 = 2_091_000_000.0;
        const HIGHEST_CF_FREQUENCY_HZ: f64 = 4_011_000_000.0;
        const VLASS_REFERENCE_FREQUENCY_HZ: f64 = 2_987_890_978.473_236_6;
        const HIGHEST_CHANNEL_CONJUGATE_HZ: f64 = 1_329_234_365.521_561_9;

        let mut entries = Vec::new();
        for frequency in [LOWEST_CF_FREQUENCY_HZ, HIGHEST_CF_FREQUENCY_HZ] {
            for w in [0.0, 1.0, 2.0] {
                entries.push(metadata(frequency, w, 3, 0.0));
            }
        }
        let catalog = AwPreparedCatalog::new(entries).unwrap();
        let sample = AwVisibilitySample::new(
            HIGHEST_CF_FREQUENCY_HZ,
            VLASS_REFERENCE_FREQUENCY_HZ,
            100.0,
            3,
            0.0,
            [4.25, 4.75],
            [17.0, -5.0],
            [2e-4, -3e-4],
        )
        .unwrap();
        let requested_conjugate =
            conjugate_frequency(sample.frequency_hz, sample.reference_frequency_hz).unwrap();
        assert!((requested_conjugate - HIGHEST_CHANNEL_CONJUGATE_HZ).abs() < 1.0e-6);
        assert!(requested_conjugate < LOWEST_CF_FREQUENCY_HZ);

        let direct_spw2 = AwVisibilitySample::new(
            1_965_000_000.0,
            VLASS_REFERENCE_FREQUENCY_HZ,
            1.0,
            3,
            0.0,
            [4.25, 4.75],
            [17.0, -5.0],
            [2e-4, -3e-4],
        )
        .unwrap();
        let direct_lower_endpoint = catalog.grid_cell(direct_spw2, false).unwrap();
        assert_eq!(direct_lower_endpoint.frequency_hz, LOWEST_CF_FREQUENCY_HZ);

        let lower_endpoint = catalog.grid_cell(sample, true).unwrap();
        assert_eq!(lower_endpoint.frequency_hz, LOWEST_CF_FREQUENCY_HZ);
        assert_eq!(lower_endpoint.w_value_lambda, 2.0);

        let upper_endpoint = catalog
            .select(HIGHEST_CF_FREQUENCY_HZ + 1.0e9, -100.0, 3, 0.0)
            .unwrap();
        assert_eq!(upper_endpoint.frequency_hz, HIGHEST_CF_FREQUENCY_HZ);
        assert_eq!(upper_endpoint.w_value_lambda, 2.0);
    }

    #[test]
    fn t51_frequency_midpoint_tie_selects_the_lower_casa_cell() {
        let catalog = AwPreparedCatalog::new(vec![
            metadata(10.0, 0.0, 3, 0.0),
            metadata(12.0, 0.0, 3, 0.0),
        ])
        .unwrap();

        assert_eq!(
            catalog.select(11.0, 0.0, 3, 0.0).unwrap().frequency_hz,
            10.0
        );
    }

    #[test]
    fn t51_asymmetric_pair_is_bounded_and_applied_with_separate_footprints() {
        let entries = vec![metadata(10.0, 0.0, 12, 0.0)];
        let mut op = operator(entries);
        let s = sample(10.0, -0.1, 3, 0.0);
        let mut image = vec![Complex64::default(); 100];
        let mut weight = vec![Complex64::default(); 100];
        op.grid(
            &mut image,
            &mut weight,
            [10, 10],
            s,
            Complex64::new(1.0, 0.0),
            1.0,
        )
        .unwrap();
        assert_eq!(image.iter().filter(|v| v.norm_sqr() > 0.0).count(), 9);
        assert_eq!(weight.iter().filter(|v| v.norm_sqr() > 0.0).count(), 15);
        let d = op.diagnostics();
        assert_eq!(
            (
                d.provider_loads,
                d.imaging_taps,
                d.weight_taps,
                d.copied_bytes
            ),
            (1, 9, 15, 0)
        );
    }

    #[test]
    fn t51_weight_kernel_accepts_signed_taylor_moment_coefficients() {
        let entries = vec![metadata(10.0, 0.0, 3, 0.0)];
        let mut op = operator(entries);
        let s = sample(10.0, 0.1, 3, 0.0);
        let mut positive = vec![Complex64::default(); 100];
        let mut positive_error = vec![Complex64::default(); 100];
        op.grid_weight_compensated(&mut positive, &mut positive_error, [10, 10], s, 2.0)
            .unwrap();
        let mut negative = vec![Complex64::default(); 100];
        let mut negative_error = vec![Complex64::default(); 100];
        op.grid_weight_compensated(&mut negative, &mut negative_error, [10, 10], s, -2.0)
            .unwrap();

        for (positive, negative) in positive.iter().zip(negative) {
            assert!((*positive + negative).norm() < 1.0e-12);
        }
        let mut image = vec![Complex64::default(); 100];
        let mut weight = vec![Complex64::default(); 100];
        assert_eq!(
            op.grid(
                &mut image,
                &mut weight,
                [10, 10],
                s,
                Complex64::new(1.0, 0.0),
                -1.0,
            ),
            Err(AwOperatorError::InvalidWeight)
        );
    }

    #[test]
    fn t51_forward_and_weighted_adjoint_obey_inner_product_law() {
        let entries = vec![metadata(10.0, 0.0, 3, 0.0), metadata(10.0, 0.0, 12, 0.0)];
        let mut op = operator(entries);
        let s = sample(10.0, 0.1, 3, 0.0);
        let model = (0..100)
            .map(|i| Complex64::new(i as f64 * 0.03 - 0.7, i as f64 * -0.02 + 0.1))
            .collect::<Vec<_>>();
        let y = Complex64::new(-0.7, 1.2);
        let predicted = op.degrid(&model, [10, 10], s).unwrap();
        let mut adj = vec![Complex64::default(); 100];
        let mut wg = vec![Complex64::default(); 100];
        op.grid(&mut adj, &mut wg, [10, 10], s, y, 1.0).unwrap();
        let left = predicted.conj() * y;
        let right: Complex64 = model.iter().zip(adj).map(|(x, a)| x.conj() * a).sum();
        assert!((left - right).norm() < 1e-12, "{left:?} {right:?}");
    }
    #[test]
    fn t51_reports_missing_nonfinite_layout_and_residency_separately() {
        let sparse = AwPreparedCatalog::new(vec![
            metadata(10.0, 0.0, 3, 0.0),
            metadata(12.0, 1.0, 3, 20.0),
        ])
        .unwrap();
        assert_eq!(
            sparse.select(11.8, 0.0, 3, 18.0),
            Err(AwOperatorError::MissingCell)
        );
        assert_eq!(
            conjugate_frequency(15.0, 10.0),
            Err(AwOperatorError::UnsupportedFrequency)
        );
        assert_eq!(
            AwVisibilitySample::new(
                f64::NAN,
                10.0,
                0.0,
                3,
                0.0,
                [1.0, 1.0],
                [0.0, 0.0],
                [0.0, 0.0]
            ),
            Err(AwOperatorError::NonFiniteValue)
        );
        assert_eq!(
            AwProjectionOperator::new(
                sparse,
                Provider {
                    cells: BTreeMap::new(),
                    seen: BTreeSet::new()
                },
                false,
                0
            )
            .err(),
            Some(AwOperatorError::ResidencyCeilingExceeded)
        );
    }
    #[test]
    fn t51_catalog_order_and_provider_receipts_are_deterministic() {
        let cells = vec![metadata(12.0, 1.0, 3, 20.0), metadata(10.0, 0.0, 3, 0.0)];
        assert_eq!(
            AwPreparedCatalog::new(cells.clone()).unwrap(),
            AwPreparedCatalog::new(cells.into_iter().rev().collect()).unwrap()
        );
    }

    #[test]
    fn t51_prepared_binding_instantiates_independent_bounded_chart_providers() {
        let entries = vec![metadata(10.0, 0.0, 3, 0.0), metadata(10.0, 0.0, 12, 0.0)];
        let cells = entries
            .iter()
            .map(|entry| (entry.identity.as_bytes(), resident(entry)))
            .collect();
        let prepared = PreparedAwProjection::new(
            AwPreparedCatalog::new(entries).unwrap(),
            Provider {
                cells,
                seen: BTreeSet::new(),
            },
            true,
            64 * 1024,
        )
        .unwrap();
        let mut first = prepared.instantiate().unwrap();
        let mut second = prepared.instantiate().unwrap();
        let sample = sample(10.0, 0.1, 3, 0.0);
        let grid = vec![Complex64::new(1.0, 0.0); 100];
        first.degrid(&grid, [10, 10], sample).unwrap();
        second.degrid(&grid, [10, 10], sample).unwrap();
        assert_eq!(first.diagnostics().provider_loads, 1);
        assert_eq!(second.diagnostics().provider_loads, 1);
        assert_eq!(prepared.resident_byte_ceiling(), 64 * 1024);
    }
}
