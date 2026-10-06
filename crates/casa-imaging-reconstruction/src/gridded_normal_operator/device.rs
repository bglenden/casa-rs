// SPDX-License-Identifier: LGPL-3.0-or-later

//! The scalar normal operator's bounded device handoff. Model preparation and
//! image conversion remain the same as the CPU operator; no CPU tile pool lives
//! alongside the device accumulation grids.

use super::*;
use crate::spectral_operator::shifted_plane_values;

/// Standard Stokes-I single-chart MFS uses the scalar shared normal operator.
pub fn supports_device_normal(problem: &CompiledProblem) -> bool {
    matches!(
        problem.reconstruction().basis(),
        ReconstructionBasis::Constant
    ) && SpectralOperatorSpecification::new(problem).is_ok_and(|spec| {
        spec.chart_count() == 1
            && spec.polarization_count() == 1
            && spec.aw_projection().is_none()
            && spec.w_projection().is_none()
    })
}

/// Seven-tap geometry, complex forward coefficient and weighted adjoint role.
#[doc(hidden)]
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, bytemuck::Pod, bytemuck::Zeroable)]
pub struct DeviceNormalRecord {
    pub x: u32,
    pub y: u32,
    pub x_weights: u32,
    pub y_weights: u32,
    pub scale: [f32; 2],
    pub weight: f32,
    pub group: u32,
    /// Both=0, prediction-only=1, accumulation-only=2.
    pub role: u32,
    pub padding: u32,
}

/// One indivisible prediction group in a device batch.
#[doc(hidden)]
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, bytemuck::Pod, bytemuck::Zeroable)]
pub struct DeviceNormalGroup {
    pub start: u32,
    pub end: u32,
}

/// Model and exact ordered coverage of a scalar device normal application.
#[doc(hidden)]
pub struct DeviceNormalApply {
    program: GriddedNormalOperatorProgram,
    specification: Arc<SpectralOperatorSpecification>,
    recycle: PreparedSpectralOperatorRecycle,
    operator: SpectralSlabOperator,
    reusable_domains: Vec<ReusableNormalState>,
    model_generation: crate::ModelGenerationId,
    next_frame: u64,
    records: u64,
    groups: u64,
}

/// A position in a trusted in-process packing operation, not a content digest.
#[doc(hidden)]
#[derive(Clone)]
pub struct DeviceNormalPosition {
    program: GriddedNormalOperatorProgram,
    frame: u64,
    records: u64,
    groups: u64,
}

/// Ordered coverage produced by successful packing of one immutable batch.
#[doc(hidden)]
pub struct DeviceNormalPreparedBatch {
    start: DeviceNormalPosition,
    end: DeviceNormalPosition,
}

impl DeviceNormalPreparedBatch {
    /// Number of complete source frames packed into this batch.
    pub fn frames(&self) -> u64 {
        self.end.frame - self.start.frame
    }

    /// Device record and prediction-group counts, with batch-local indices.
    pub fn counts(&self) -> (usize, usize) {
        (
            (self.end.records - self.start.records) as usize,
            (self.end.groups - self.start.groups) as usize,
        )
    }
}

impl GriddedNormalOperatorProgram {
    /// Ordered frame sizes used to retain the existing whole-frame GPU batches.
    pub fn device_normal_frame_records(&self) -> impl Iterator<Item = usize> + '_ {
        self.manifest
            .descriptors
            .iter()
            .map(|frame| frame.record_count as usize)
    }
    /// Fail closed unless the compiled operator is one standard scalar plane.
    pub fn device_normal_shape(&self) -> Result<[usize; 2], SpectralOperatorError> {
        let spec = &self.manifest.specification;
        if self.manifest.record_layout != GriddedNormalRecordLayout::Scalar
            || self.manifest.aw_projection
            || !self.manifest.w_projection_diagnostics.is_empty()
            || spec.chart_count() != 1
            || spec.polarization_count() != 1
        {
            return Err(SpectralOperatorError::UnsupportedGriddedReplay);
        }
        Ok(spec.grid_shape())
    }

    /// Prepare the shared model FFT without allocating CPU routing/merge grids.
    pub fn begin_device_normal(
        &self,
        problem: &CompiledProblem,
        model: &ModelGeneration,
        prior: &mut GriddedNormalReplaySource,
        prepared: PreparedSpectralOperator,
    ) -> Result<DeviceNormalApply, SpectralOperatorError> {
        self.device_normal_shape()?;
        let (specification, recycle, mut operators, reusable_domains, model_generation) =
            self.prepare_apply_model(problem, model, prior, prepared, FftGridOrder::Origin)?;
        if operators.len() != 1 {
            return Err(SpectralOperatorError::UnsupportedGriddedReplay);
        }
        let operator = operators.pop().expect("single chart");
        operator.device_normal_model()?;
        Ok(DeviceNormalApply {
            program: self.clone(),
            specification,
            recycle,
            operator,
            reusable_domains,
            model_generation,
            next_frame: 0,
            records: 0,
            groups: 0,
        })
    }
}

impl DeviceNormalApply {
    /// Capture structural coverage before packing an immutable batch.
    pub fn position(&self) -> DeviceNormalPosition {
        DeviceNormalPosition {
            program: self.program.clone(),
            frame: self.next_frame,
            records: self.records,
            groups: self.groups,
        }
    }

    /// Bind a batch to the live program that produced it. No content reread is
    /// needed when the immutable packed owner survives to the next application.
    pub fn prepared_since(
        &self,
        start: DeviceNormalPosition,
    ) -> Result<DeviceNormalPreparedBatch, SpectralOperatorError> {
        if !Arc::ptr_eq(&start.program.manifest, &self.program.manifest)
            || start.frame >= self.next_frame
            || start.records >= self.records
            || start.groups >= self.groups
        {
            return Err(SpectralOperatorError::BlockSequence);
        }
        Ok(DeviceNormalPreparedBatch {
            start,
            end: self.position(),
        })
    }

    /// Consume exactly the next immutable batch from this same live program.
    pub fn accept_prepared(
        &mut self,
        batch: &DeviceNormalPreparedBatch,
    ) -> Result<(), SpectralOperatorError> {
        if !Arc::ptr_eq(&batch.start.program.manifest, &self.program.manifest)
            || batch.start.frame != self.next_frame
            || batch.start.records != self.records
            || batch.start.groups != self.groups
        {
            return Err(SpectralOperatorError::BlockSequence);
        }
        self.next_frame = batch.end.frame;
        self.records = batch.end.records;
        self.groups = batch.end.groups;
        Ok(())
    }

    /// Convert the shared FFT's model directly into the centered device grid,
    /// returning the canonical convolution table. Centering is fused with the
    /// required precision conversion, not another full-grid pass.
    pub fn pack_model(
        &self,
        output: &mut [[f32; 2]],
    ) -> Result<Vec<[f32; 7]>, SpectralOperatorError> {
        let (model, weights) = self.operator.device_normal_model()?;
        if output.len() != model.len() {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        let shape = self.program.device_normal_shape()?;
        for (source, target) in shifted_plane_values(model, shape).zip(output) {
            *target = [source.re as f32, source.im as f32];
            if !target.iter().all(|n| n.is_finite()) {
                return Err(SpectralOperatorError::GeneratedNonfinite);
            }
        }
        Ok(weights)
    }

    /// Decode an ordered whole frame directly into reusable shared staging.
    /// Group boundaries never cross frames or device batches.
    pub fn pack_frame(
        &mut self,
        sequence: u64,
        encoded: &[u8],
        records: &mut [DeviceNormalRecord],
        groups: &mut [DeviceNormalGroup],
        record_offset: usize,
        group_offset: usize,
    ) -> Result<(usize, usize), SpectralOperatorError> {
        if sequence != self.next_frame {
            return Err(SpectralOperatorError::BlockSequence);
        }
        let descriptor = self
            .program
            .manifest
            .descriptors
            .get(sequence as usize)
            .ok_or(SpectralOperatorError::GriddedRecordMismatch)?;
        validate_encoded_block(descriptor, encoded, GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)?;
        let count = encoded.len() / GRIDDED_NORMAL_OPERATOR_RECORD_BYTES;
        if record_offset
            .checked_add(count)
            .is_none_or(|n| n > records.len())
            || group_offset
                .checked_add(count)
                .is_none_or(|n| n > groups.len())
        {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        let shape = self.program.device_normal_shape()?;
        let mut group = group_offset;
        let mut start = record_offset;
        for (ordinal, encoded) in encoded
            .as_chunks::<GRIDDED_NORMAL_OPERATOR_RECORD_BYTES>()
            .0
            .iter()
            .enumerate()
        {
            let decoded = decode_record_for_shape(encoded, shape, 1)?;
            if decoded.chart_ordinal != 0 {
                return Err(SpectralOperatorError::GriddedRecordMismatch);
            }
            let scale = [
                decoded.forward_scale.re as f32,
                decoded.forward_scale.im as f32,
            ];
            let weight = decoded.imaging_weight as f32;
            if !scale.iter().all(|v| v.is_finite()) || !weight.is_finite() {
                return Err(SpectralOperatorError::GeneratedNonfinite);
            }
            let index = record_offset + ordinal;
            records[index] = DeviceNormalRecord {
                x: decoded.taps.x.start as u32,
                y: decoded.taps.y.start as u32,
                x_weights: decoded.taps.x.weight_index as u32,
                y_weights: decoded.taps.y.weight_index as u32,
                scale,
                weight,
                group: group as u32,
                role: match decoded.role {
                    RecordRole::Both => 0,
                    RecordRole::Prediction => 1,
                    RecordRole::Accumulation => 2,
                },
                padding: 0,
            };
            if decoded.group_end {
                groups[group] = DeviceNormalGroup {
                    start: start as u32,
                    end: (index + 1) as u32,
                };
                group += 1;
                start = index + 1;
            }
        }
        if start != record_offset + count {
            return Err(SpectralOperatorError::InvalidGriddedRecord);
        }
        self.next_frame += 1;
        self.records += count as u64;
        self.groups += (group - group_offset) as u64;
        Ok((count, group - group_offset))
    }

    /// Import the completed normal grid once, then use the shared inverse FFT
    /// and prior-state subtraction. Structural coverage remains mandatory.
    pub fn finish(
        mut self,
        normal: &[[f32; 2]],
    ) -> Result<
        (
            CompleteDataOwnerResult,
            GriddedNormalRoutingMeasurements,
            PreparedSpectralOperatorRecycle,
        ),
        SpectralOperatorError,
    > {
        let shape = self.program.device_normal_shape()?;
        if self.next_frame != self.program.block_count()
            || self.records != self.program.record_count()
            || normal.len() != shape[0] * shape[1]
        {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        let mut values = Vec::with_capacity(normal.len());
        for v in shifted_plane_values(normal, shape) {
            if !v.iter().all(|n| n.is_finite()) {
                return Err(SpectralOperatorError::GeneratedNonfinite);
            }
            values.push(Complex64::new(v[0] as f64, v[1] as f64));
        }
        let grid = Array2::from_shape_vec((shape[0], shape[1]), values)
            .map_err(|_| SpectralOperatorError::UnsupportedGeometry)?;
        let (update, fft) = self.operator.finish_gridded_normal_from_grids(
            self.model_generation,
            vec![grid],
            FftGridOrder::Origin,
        )?;
        self.recycle.ffts.push(fft);
        let domains = combine_chart_updates(
            &self.specification,
            self.reusable_domains,
            std::iter::once(Ok(update)),
        )?;
        let manifest = &self.program.manifest;
        let result = CompleteDataOwnerResult {
            domains,
            completion: CompleteDataOwnerCompletion {
                problem: manifest.specification.problem_id(),
                geometry: manifest.specification.geometry_id(),
                numerics: manifest.specification.numerics_id(),
                weighting_commitment: manifest.specification.weighting_commitment_id(),
                weighting_generation: manifest.weighting_generation,
                replay: manifest.replay,
                primitives: SpectralPrimitiveCatalog::UnnormalizedPlaneV1,
                sample_count: manifest.sample_count,
                block_count: self.program.source_block_count(),
            },
        };
        Ok((
            result,
            GriddedNormalRoutingMeasurements {
                frames_routed: self.next_frame,
                encoded_records: self.records,
                prediction_groups: self.groups,
                ..Default::default()
            },
            self.recycle,
        ))
    }
}
