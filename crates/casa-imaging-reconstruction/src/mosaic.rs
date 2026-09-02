// SPDX-License-Identifier: LGPL-3.0-or-later

//! Deterministic field/pointing accumulation for direction-dependent normal state.

use std::collections::BTreeMap;
use std::mem::size_of;

use casa_imaging_model::{DirectionFrame, SelectedPointingDirections, SkyDirection};
use ndarray::Array2;
use num_complex::{Complex32, Complex64};

use crate::{
    SpectralOperatorError,
    primary_beam::PreparedPrimaryBeamPower,
    spectral_operator::{
        PreparedFft, SpectralOperatorGeometry, fft_planning_words_for_shape,
        fft_resident_complex_values_for_shape,
    },
    weighting::ExactF64Sum,
};

pub(crate) const MOSAIC_OVERSAMPLING: usize = 10;

const BTREE_NODE_ENTRY_CAPACITY: usize = 11;
const BTREE_NON_ROOT_MINIMUM_ENTRIES: usize = 5;
const BTREE_NODE_POINTER_WORDS: usize = 20;

fn bounded_tree_bytes<K, V>(
    entries_per_map: usize,
    map_count: usize,
) -> Result<usize, SpectralOperatorError> {
    if entries_per_map == 0 || map_count == 0 {
        return Ok(0);
    }
    let nodes_per_map = 1usize
        .checked_add((entries_per_map - 1).div_ceil(BTREE_NON_ROOT_MINIMUM_ENTRIES))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let node_bytes = BTREE_NODE_ENTRY_CAPACITY
        .checked_mul(
            size_of::<K>()
                .checked_add(size_of::<V>())
                .ok_or(SpectralOperatorError::ResidencyOverflow)?,
        )
        .and_then(|bytes| {
            BTREE_NODE_POINTER_WORDS
                .checked_mul(size_of::<usize>())
                .and_then(|pointers| bytes.checked_add(pointers))
        })
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    node_bytes
        .checked_mul(nodes_per_map)
        .and_then(|bytes| bytes.checked_mul(map_count))
        .ok_or(SpectralOperatorError::ResidencyOverflow)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MosaicResidencyProjection {
    pub(crate) retained_bytes: usize,
    pub(crate) workspace_bytes: usize,
}

pub(crate) fn response_residency_projection(
    response_capacity: usize,
    selection_bytes: usize,
    maximum_selection_channels: usize,
) -> Result<MosaicResidencyProjection, SpectralOperatorError> {
    if response_capacity == 0 {
        return Ok(MosaicResidencyProjection {
            retained_bytes: 0,
            workspace_bytes: 0,
        });
    }
    let response_route_bytes = bounded_tree_bytes::<
        (casa_imaging_model::MeasurementSetIdentity, u32, u32),
        (u64, f64),
    >(response_capacity, 1)?;
    let channel_map_bytes =
        bounded_tree_bytes::<u32, (f64, f64, f64)>(maximum_selection_channels, 1)?;
    let response_plan_workspace = maximum_selection_channels
        .checked_mul(
            size_of::<(f64, f64, f64)>()
                + 2 * size_of::<f64>()
                + size_of::<usize>()
                + 2 * size_of::<u64>(),
        )
        .and_then(|bytes| bytes.checked_add(channel_map_bytes))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    Ok(MosaicResidencyProjection {
        retained_bytes: response_route_bytes
            .checked_add(selection_bytes)
            .ok_or(SpectralOperatorError::ResidencyOverflow)?,
        workspace_bytes: response_plan_workspace,
    })
}

pub(crate) fn residency_projection(
    image_shape: [usize; 2],
    grid_shape: [usize; 2],
    response_capacity: usize,
    field_capacity: usize,
    normal_entry_capacity: usize,
    normal_addition_capacity: usize,
    normal_accumulator_count: usize,
) -> Result<MosaicResidencyProjection, SpectralOperatorError> {
    if response_capacity == 0 || field_capacity == 0 {
        return Ok(MosaicResidencyProjection {
            retained_bytes: 0,
            workspace_bytes: 0,
        });
    }
    let conv_size = mosaic_convolution_size(image_shape);
    let temp_side = conv_size / 4;
    let maximum_support = temp_side / 2 - 1;
    let kernel_side = 2 * (maximum_support + 2) * MOSAIC_OVERSAMPLING;
    let kernel_bytes = kernel_side
        .checked_mul(kernel_side)
        .and_then(|values| values.checked_mul(size_of::<Complex32>()))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    // Two base kernels and one reusable active phased data kernel per response.
    let projector_tree_bytes = bounded_tree_bytes::<u64, MosaicProjector>(response_capacity, 1)?;
    let projector_bytes = kernel_bytes
        .checked_mul(3)
        .and_then(|bytes| bytes.checked_mul(response_capacity))
        .and_then(|bytes| bytes.checked_add(projector_tree_bytes))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let phase_metadata_bytes =
        bounded_tree_bytes::<i32, ([u64; 2], [f64; 2])>(field_capacity, response_capacity)?;
    let normal_entry_bytes = bounded_tree_bytes::<
        (PointingKey, MosaicWeightSupportKey),
        ExactF64Sum,
    >(normal_entry_capacity, normal_accumulator_count)?;
    let normal_accumulator_bytes = size_of::<MosaicNormalAccumulator>()
        .checked_mul(normal_accumulator_count)
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let total_normal_additions = normal_addition_capacity
        .checked_mul(normal_accumulator_count)
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    // One addition can create at most one new exponent bin. The maximum heap
    // occurs when every addition lands in a distinct one-bin accumulator.
    let normal_bin_bytes = bounded_tree_bytes::<i16, u128>(1, total_normal_additions)?;
    let retained_bytes = projector_bytes
        .checked_add(PreparedPrimaryBeamPower::casa_aca_mosaic_retained_table_bytes())
        .and_then(|bytes| bytes.checked_add(phase_metadata_bytes))
        .and_then(|bytes| bytes.checked_add(normal_entry_bytes))
        .and_then(|bytes| bytes.checked_add(normal_accumulator_bytes))
        .and_then(|bytes| bytes.checked_add(normal_bin_bytes))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;

    let screen_bytes = conv_size
        .checked_mul(conv_size)
        .and_then(|values| values.checked_mul(size_of::<Complex64>()))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let fft_bytes = fft_resident_complex_values_for_shape([conv_size, conv_size])?
        .checked_mul(size_of::<Complex64>())
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let fft_planning_bytes = fft_planning_words_for_shape([conv_size, conv_size])?
        .checked_mul(size_of::<usize>())
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let temp_bytes = temp_side
        .checked_mul(temp_side)
        .and_then(|values| values.checked_mul(size_of::<Complex32>()))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let cropped_side = 2 * (maximum_support + 2);
    let cropped_bytes = cropped_side
        .checked_mul(cropped_side)
        .and_then(|values| values.checked_mul(size_of::<Complex32>()))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    // One previous FFT crop remains live while the second is built. Finished
    // kernels use retained-state capacity; the extra kernel covers a field
    // switch before the prior active kernel is dropped.
    let double_temp_bytes = temp_bytes
        .checked_mul(2)
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let double_cropped_bytes = cropped_bytes
        .checked_mul(2)
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let double_kernel_bytes = kernel_bytes
        .checked_mul(2)
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let screen_build_workspace = screen_bytes
        .checked_add(fft_bytes)
        .and_then(|bytes| bytes.checked_add(fft_planning_bytes))
        .and_then(|bytes| bytes.checked_add(double_temp_bytes))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let resample_workspace = double_temp_bytes
        .checked_add(double_cropped_bytes)
        .and_then(|bytes| bytes.checked_add(double_kernel_bytes))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let active_phase_replacement_workspace = kernel_bytes;
    let build_workspace = screen_build_workspace
        .max(resample_workspace)
        .max(active_phase_replacement_workspace);
    let completion_workspace = grid_shape
        .into_iter()
        .try_fold(1_usize, |cells, side| cells.checked_mul(side))
        .and_then(|cells| cells.checked_mul(2 * size_of::<Complex64>()))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    Ok(MosaicResidencyProjection {
        retained_bytes,
        workspace_bytes: build_workspace.max(completion_workspace),
    })
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MosaicSamplePlan {
    field_id: i32,
    center_in_bounds: bool,
    loc: [isize; 2],
    offset: [isize; 2],
    x: [isize; 2],
    y: [isize; 2],
    weight_x: [isize; 2],
    weight_y: [isize; 2],
}

struct PhasedKernel {
    field_id: i32,
    imaging: Array2<Complex32>,
}

pub(crate) struct MosaicProjector {
    grid_shape: [usize; 2],
    du_lambda: f64,
    dv_lambda: f64,
    support: usize,
    kernel_center: usize,
    kernel: Array2<Complex32>,
    weight_kernel: Array2<Complex32>,
    field_capacity: usize,
    field_phase_gradients: BTreeMap<i32, ([u64; 2], [f64; 2])>,
    active_phased_kernel: Option<PhasedKernel>,
}

impl MosaicProjector {
    pub(crate) fn new(
        geometry: SpectralOperatorGeometry,
        response: &PreparedPrimaryBeamPower,
        frequency_hz: f64,
        field_capacity: usize,
    ) -> Result<Self, SpectralOperatorError> {
        if field_capacity == 0 {
            return Err(SpectralOperatorError::UnsupportedProblem);
        }
        let conv_size = mosaic_convolution_size(geometry.image_shape);
        if conv_size < 16 || conv_size % 2 != 0 {
            return Err(SpectralOperatorError::UnsupportedGeometry);
        }
        let imaging = screen_fft_temp(geometry, response, frequency_hz, conv_size, 1)?;
        let weight = screen_fft_temp(geometry, response, frequency_hz, conv_size, 2)?;
        let support = find_support(&weight, 1);
        if support == 0 {
            return Err(SpectralOperatorError::UnsupportedGeometry);
        }
        let center = imaging.dim().0 / 2;
        let normalization = plane_sum(&imaging, center, support, 1).re;
        if !normalization.is_finite() || normalization <= 1.0e-6 {
            return Err(SpectralOperatorError::GeneratedNonfinite);
        }
        let cropped_size = 2 * (support + 2);
        let cropped_center = cropped_size / 2;
        let mut cropped = Array2::zeros((cropped_size, cropped_size));
        for y in 0..cropped_size {
            for x in 0..cropped_size {
                cropped[(x, y)] = imaging
                    [(center + x - cropped_center, center + y - cropped_center)]
                    / normalization;
            }
        }
        let kernel = lanczos_resample(&cropped, MOSAIC_OVERSAMPLING);
        let weight_center = weight.dim().0 / 2;
        let weight_normalization = plane_sum(&weight, weight_center, support, 1).re;
        if !weight_normalization.is_finite() || weight_normalization <= 1.0e-6 {
            return Err(SpectralOperatorError::GeneratedNonfinite);
        }
        let mut cropped_weight = Array2::zeros((cropped_size, cropped_size));
        for y in 0..cropped_size {
            for x in 0..cropped_size {
                cropped_weight[(x, y)] = weight[(
                    weight_center + x - cropped_center,
                    weight_center + y - cropped_center,
                )] / weight_normalization;
            }
        }
        let weight_kernel = lanczos_resample(&cropped_weight, MOSAIC_OVERSAMPLING);
        Ok(Self {
            grid_shape: geometry.grid_shape,
            du_lambda: 1.0 / (geometry.grid_shape[0] as f64 * geometry.increment_rad[0].abs()),
            dv_lambda: 1.0 / (geometry.grid_shape[1] as f64 * geometry.increment_rad[1].abs()),
            support,
            kernel_center: kernel.dim().0 / 2,
            kernel,
            weight_kernel,
            field_capacity,
            field_phase_gradients: BTreeMap::new(),
            active_phased_kernel: None,
        })
    }

    pub(crate) fn plan(
        &mut self,
        field_id: i32,
        uv_lambda: [f64; 2],
        pointing_pixel: [f64; 2],
        reference_pixel: [f64; 2],
        image_shape: [usize; 2],
    ) -> Result<Option<MosaicSamplePlan>, SpectralOperatorError> {
        let position = [
            uv_lambda[0] / self.du_lambda + self.grid_shape[0] as f64 / 2.0,
            -uv_lambda[1] / self.dv_lambda + self.grid_shape[1] as f64 / 2.0,
        ];
        if position.iter().any(|value| !value.is_finite()) {
            return Ok(None);
        }
        let loc = [position[0].round() as isize, position[1].round() as isize];
        let offset = [
            ((loc[0] as f64 - position[0]) * MOSAIC_OVERSAMPLING as f64).round() as isize,
            ((loc[1] as f64 - position[1]) * MOSAIC_OVERSAMPLING as f64).round() as isize,
        ];
        let support = self.support as isize;
        let weight_x = [
            (-support).max(-loc[0]),
            support.min(self.grid_shape[0] as isize - 1 - loc[0]),
        ];
        let weight_y = [
            (-support).max(-loc[1]),
            support.min(self.grid_shape[1] as isize - 1 - loc[1]),
        ];
        // CASA MosaicFT partitions the grid into worker sectors whose union ends
        // one cell before the allocated upper x/y edges. Preserve that effective
        // scientific domain independently of this runtime's worker count.
        let upper_x = self.grid_shape[0] as isize - 2;
        let upper_y = self.grid_shape[1] as isize - 2;
        let x = [(-support).max(-loc[0]), support.min(upper_x - loc[0])];
        let y = [(-support).max(-loc[1]), support.min(upper_y - loc[1])];
        if x[0] > x[1] || y[0] > y[1] {
            return Ok(None);
        }
        let pointing_pixel_bits = pointing_pixel.map(f64::to_bits);
        match self.field_phase_gradients.get(&field_id) {
            Some((bits, _)) if *bits != pointing_pixel_bits => {
                return Err(SpectralOperatorError::UnsupportedProblem);
            }
            Some(_) => {}
            None => {
                if self.field_phase_gradients.len() == self.field_capacity {
                    return Err(SpectralOperatorError::ResidencyOverflow);
                }
                let phase_gradient = [
                    -(pointing_pixel[0] - reference_pixel[0]) * std::f64::consts::TAU
                        / (image_shape[0] as f64 * MOSAIC_OVERSAMPLING as f64),
                    -(pointing_pixel[1] - reference_pixel[1]) * std::f64::consts::TAU
                        / (image_shape[1] as f64 * MOSAIC_OVERSAMPLING as f64),
                ];
                self.field_phase_gradients
                    .insert(field_id, (pointing_pixel_bits, phase_gradient));
            }
        }
        let (_, phase_gradient) = self.field_phase_gradients[&field_id];
        if self
            .active_phased_kernel
            .as_ref()
            .is_none_or(|kernel| kernel.field_id != field_id)
        {
            let center = self.kernel_center as isize;
            let mut imaging = self.kernel.clone();
            for ((kernel_x, kernel_y), value) in imaging.indexed_iter_mut() {
                let signed_x = kernel_x as isize - center;
                let signed_y = kernel_y as isize - center;
                let phase_x = signed_x as f64 * phase_gradient[0];
                let phase_y = signed_y as f64 * phase_gradient[1];
                let phasor_x = Complex32::new(phase_x.cos() as f32, phase_x.sin() as f32);
                let phasor_y = Complex32::new(phase_y.cos() as f32, phase_y.sin() as f32);
                *value = *value * phasor_x * phasor_y;
            }
            self.active_phased_kernel = Some(PhasedKernel { field_id, imaging });
        }
        Ok(Some(MosaicSamplePlan {
            field_id,
            center_in_bounds: loc[0] >= 0 && loc[0] <= upper_x && loc[1] >= 0 && loc[1] <= upper_y,
            loc,
            offset,
            x,
            y,
            weight_x,
            weight_y,
        }))
    }

    pub(crate) const fn contributes_to_normalization(plan: MosaicSamplePlan) -> bool {
        plan.center_in_bounds
    }

    pub(crate) fn grid_compensated(
        &self,
        grid: &mut Array2<Complex64>,
        _compensation: &mut Array2<Complex64>,
        plan: MosaicSamplePlan,
        value: Complex64,
    ) {
        let kernel = &self
            .active_phased_kernel
            .as_ref()
            .filter(|kernel| kernel.field_id == plan.field_id)
            .expect("sample plan keeps its field kernel active")
            .imaging;
        for iy in plan.y[0]..=plan.y[1] {
            let kernel_y = (self.kernel_center as isize
                + iy * MOSAIC_OVERSAMPLING as isize
                + plan.offset[1]) as usize;
            for ix in plan.x[0]..=plan.x[1] {
                let kernel_x = (self.kernel_center as isize
                    + ix * MOSAIC_OVERSAMPLING as isize
                    + plan.offset[0]) as usize;
                let tap = kernel[(kernel_x, kernel_y)];
                let cell = ((plan.loc[0] + ix) as usize, (plan.loc[1] + iy) as usize);
                // CASA constructs the sample value through its Complex input
                // boundary, then promotes both it and the Complex tap for the
                // DComplex grid product.
                let value = Complex32::new(value.re as f32, value.im as f32);
                let value = Complex64::new(f64::from(value.re), f64::from(value.im));
                let tap = Complex64::new(f64::from(tap.re), f64::from(tap.im));
                grid[cell] += value * tap;
            }
        }
    }

    pub(crate) fn degrid(&self, grid: &Array2<Complex64>, plan: MosaicSamplePlan) -> Complex64 {
        let kernel = &self
            .active_phased_kernel
            .as_ref()
            .filter(|kernel| kernel.field_id == plan.field_id)
            .expect("sample plan keeps its field kernel active")
            .imaging;
        let mut value = Complex64::default();
        for iy in plan.weight_y[0]..=plan.weight_y[1] {
            let kernel_y = (self.kernel_center as isize
                + iy * MOSAIC_OVERSAMPLING as isize
                + plan.offset[1]) as usize;
            for ix in plan.weight_x[0]..=plan.weight_x[1] {
                let kernel_x = (self.kernel_center as isize
                    + ix * MOSAIC_OVERSAMPLING as isize
                    + plan.offset[0]) as usize;
                let tap = kernel[(kernel_x, kernel_y)];
                let tap = Complex64::new(f64::from(tap.re), f64::from(tap.im));
                value +=
                    tap.conj() * grid[((plan.loc[0] + ix) as usize, (plan.loc[1] + iy) as usize)];
            }
        }
        value
    }

    fn grid_weight_at_center(
        &self,
        field_id: i32,
        support: MosaicWeightSupportKey,
        value: f64,
        grid: &mut Array2<Complex64>,
        compensation: &mut Array2<Complex64>,
    ) -> Result<(), SpectralOperatorError> {
        let (_, phase_gradient) = self
            .field_phase_gradients
            .get(&field_id)
            .ok_or(SpectralOperatorError::ProblemMismatch)?;
        let center = [self.grid_shape[0] / 2, self.grid_shape[1] / 2];
        for y in support.min_y..=support.max_y {
            let kernel_y =
                (self.kernel_center as isize + y * MOSAIC_OVERSAMPLING as isize) as usize;
            for x in support.min_x..=support.max_x {
                let kernel_x =
                    (self.kernel_center as isize + x * MOSAIC_OVERSAMPLING as isize) as usize;
                let phase_x = kernel_x as isize - self.kernel_center as isize;
                let phase_y = kernel_y as isize - self.kernel_center as isize;
                let phase_x = phase_x as f64 * phase_gradient[0];
                let phase_y = phase_y as f64 * phase_gradient[1];
                let phasor_x = Complex32::new(phase_x.cos() as f32, phase_x.sin() as f32);
                let phasor_y = Complex32::new(phase_y.cos() as f32, phase_y.sin() as f32);
                let tap = self.weight_kernel[(kernel_x, kernel_y)] * phasor_x * phasor_y;
                let contribution = Complex32::new(value as f32, 0.0) * tap;
                let contribution =
                    Complex64::new(f64::from(contribution.re), f64::from(contribution.im));
                let cell = (
                    (center[0] as isize + x) as usize,
                    (center[1] as isize + y) as usize,
                );
                let corrected = contribution - compensation[cell];
                let updated = grid[cell] + corrected;
                compensation[cell] = (updated - grid[cell]) - corrected;
                grid[cell] = updated;
            }
        }
        Ok(())
    }
}

fn mosaic_convolution_size(image_shape: [usize; 2]) -> usize {
    let support = image_shape.into_iter().max().unwrap_or(0).max(64);
    let mut convolution_size = support + 1;
    while convolution_size % 2 != 0 || !is_composite_fft_length(convolution_size) {
        convolution_size += 1;
    }
    (convolution_size / 16 * 16).max(64)
}

fn is_composite_fft_length(mut value: usize) -> bool {
    for factor in [2, 3, 5] {
        while value > 1 && value % factor == 0 {
            value /= factor;
        }
    }
    value == 1
}

fn screen_fft_temp(
    geometry: SpectralOperatorGeometry,
    response: &PreparedPrimaryBeamPower,
    frequency_hz: f64,
    conv_size: usize,
    power: u32,
) -> Result<Array2<Complex32>, SpectralOperatorError> {
    let scale = [
        geometry.increment_rad[0].abs() * geometry.image_shape[0] as f64 / conv_size as f64,
        geometry.increment_rad[1].abs() * geometry.image_shape[1] as f64 / conv_size as f64,
    ];
    let center = conv_size as isize / 2;
    let mut screen = Array2::zeros((conv_size, conv_size));
    for y in 0..conv_size {
        let m = (y as isize - center) as f64 * scale[1];
        for x in 0..conv_size {
            let l = (x as isize - center) as f64 * scale[0];
            screen[(x, y)] = Complex64::new(
                f64::from(response.mosaic_convolution_power_at_offsets(l, m, frequency_hz)?)
                    .powi(power as i32),
                0.0,
            );
        }
    }
    let mut fft = PreparedFft::new(
        [conv_size, conv_size],
        fft_resident_complex_values_for_shape([conv_size, conv_size])?,
    )?;
    fft.transform(&mut screen, false);
    let peak = screen[(conv_size / 2, conv_size / 2)].norm();
    if !peak.is_finite() || peak <= 0.0 {
        return Err(SpectralOperatorError::GeneratedNonfinite);
    }
    let size = conv_size / 4;
    let start = conv_size * 3 / 8;
    let mut output = Array2::zeros((size, size));
    for y in 0..size {
        for x in 0..size {
            let value = screen[(start + x, start + y)];
            output[(x, y)] = Complex32::new(value.re as f32, value.im as f32);
        }
    }
    Ok(output)
}

fn find_support(weights: &Array2<Complex32>, sampling: usize) -> usize {
    let size = weights.dim().0;
    let mut peak = 0.0_f32;
    let mut peak_position = (size / 2, size / 2);
    for ((x, y), value) in weights.indexed_iter() {
        if value.norm() > peak {
            peak = value.norm();
            peak_position = (x, y);
        }
    }
    if peak <= 0.0 {
        return 0;
    }
    let mut trial = 0;
    for candidate in 0..size.saturating_sub(peak_position.0.max(peak_position.1) + 2) {
        if weights[(peak_position.0.saturating_sub(candidate), peak_position.1)].norm()
            < 2.5e-2 * peak
            && weights[(peak_position.0, peak_position.1.saturating_sub(candidate))].norm()
                < 2.5e-2 * peak
        {
            trial = candidate;
            break;
        }
    }
    if trial == 0 {
        trial = size / 2 - 4 * sampling;
    }
    if trial < 5 * sampling {
        trial = if 10 * sampling < size {
            5 * sampling
        } else {
            size / 2 - 4 * sampling
        };
    }
    ((((trial as f32) / sampling as f32) + 0.5).floor() as usize + 1).min(size / 2 / sampling - 1)
}

fn plane_sum(
    kernel: &Array2<Complex32>,
    center: usize,
    support: usize,
    sampling: usize,
) -> Complex32 {
    let mut sum = Complex32::default();
    for y in -(support as isize)..=support as isize {
        for x in -(support as isize)..=support as isize {
            sum += kernel[(
                (center as isize + x * sampling as isize) as usize,
                (center as isize + y * sampling as isize) as usize,
            )];
        }
    }
    sum
}

fn lanczos_resample(input: &Array2<Complex32>, factor: usize) -> Array2<Complex32> {
    let shape = input.dim();
    let output_shape = ((shape.0 * factor / 2) * 2, (shape.1 * factor / 2) * 2);
    let mut output = Array2::zeros(output_shape);
    for y in 0..output_shape.1 {
        let source_y = y as f64 / output_shape.1 as f64 * shape.1 as f64;
        for x in 0..output_shape.0 {
            let source_x = x as f64 / output_shape.0 as f64 * shape.0 as f64;
            output[(x, y)] = lanczos_sample(input, source_x, source_y);
        }
    }
    output
}

fn lanczos_sample(input: &Array2<Complex32>, x: f64, y: f64) -> Complex32 {
    let shape = input.dim();
    let floor = [x.floor(), y.floor()];
    if shape.0 > 6
        && shape.1 > 6
        && (floor[0] < 3.0
            || floor[0] >= shape.0 as f64 - 3.0
            || floor[1] < 3.0
            || floor[1] >= shape.1 as f64 - 3.0)
    {
        return Complex32::default();
    }
    let mut output = Complex32::default();
    for source_x in (floor[0] as isize - 2)..=(floor[0] as isize + 3) {
        if !(0..shape.0 as isize).contains(&source_x) {
            continue;
        }
        let dx = (x - source_x as f64) as f32;
        let wx = [casa_sinc(dx), casa_sinc(dx / 3.0)];
        for source_y in (floor[1] as isize - 2)..=(floor[1] as isize + 3) {
            if !(0..shape.1 as isize).contains(&source_y) {
                continue;
            }
            let dy = (y - source_y as f64) as f32;
            let wy = [casa_sinc(dy), casa_sinc(dy / 3.0)];
            let value = input[(source_x as usize, source_y as usize)];
            output.re += (f64::from(value.re)
                * f64::from(wx[0])
                * f64::from(wx[1])
                * f64::from(wy[0])
                * f64::from(wy[1])) as f32;
            output.im += (f64::from(value.im)
                * f64::from(wx[0])
                * f64::from(wx[1])
                * f64::from(wy[0])
                * f64::from(wy[1])) as f32;
        }
    }
    output
}

fn casa_sinc(value: f32) -> f32 {
    if value == 0.0 {
        1.0
    } else {
        let argument = std::f64::consts::PI * f64::from(value);
        (argument.sin() / argument) as f32
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct PointingKey {
    field_id: i32,
    response_key: u64,
    frame: DirectionFrame,
    longitude_bits: u64,
    latitude_bits: u64,
    frequency_bits: u64,
}

impl PointingKey {
    fn new(field_id: i32, response_key: u64, pointing: SkyDirection, frequency_hz: f64) -> Self {
        Self {
            field_id,
            response_key,
            frame: pointing.frame(),
            longitude_bits: pointing.longitude_rad().to_bits(),
            latitude_bits: pointing.latitude_rad().to_bits(),
            frequency_bits: frequency_hz.to_bits(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct MosaicWeightSupportKey {
    min_x: isize,
    max_x: isize,
    min_y: isize,
    max_y: isize,
}

impl From<MosaicSamplePlan> for MosaicWeightSupportKey {
    fn from(plan: MosaicSamplePlan) -> Self {
        Self {
            min_x: plan.weight_x[0],
            max_x: plan.weight_x[1],
            min_y: plan.weight_y[0],
            max_y: plan.weight_y[1],
        }
    }
}

/// Reconstruction-owned exact reduction of homogeneous mosaic pointing weights.
///
/// The accumulator stores one scalar per unique field, pointing, and evaluation
/// frequency. It therefore stays independent of source block boundaries and
/// worker partitions. T48 extends the response law for heterogeneous antenna
/// pairs; T47 deliberately requires the two evaluated antenna pointings to be
/// identical.
pub(crate) struct MosaicNormalAccumulator {
    weights: BTreeMap<(PointingKey, MosaicWeightSupportKey), ExactF64Sum>,
    entry_capacity: usize,
    addition_capacity: usize,
    additions: usize,
}

impl Default for MosaicNormalAccumulator {
    fn default() -> Self {
        Self {
            weights: BTreeMap::new(),
            entry_capacity: usize::MAX,
            addition_capacity: usize::MAX,
            additions: 0,
        }
    }
}

impl MosaicNormalAccumulator {
    pub(crate) const fn with_capacity(entry_capacity: usize, addition_capacity: usize) -> Self {
        Self {
            weights: BTreeMap::new(),
            entry_capacity,
            addition_capacity,
            additions: 0,
        }
    }

    pub(crate) fn accumulate(
        &mut self,
        field_id: i32,
        response_key: u64,
        pointings: SelectedPointingDirections,
        frequency_hz: f64,
        plan: MosaicSamplePlan,
        weight: f64,
    ) -> Result<(), SpectralOperatorError> {
        if pointings.antenna1 != pointings.antenna2
            || !frequency_hz.is_finite()
            || frequency_hz <= 0.0
            || !weight.is_finite()
            || weight < 0.0
        {
            return Err(SpectralOperatorError::UnsupportedProblem);
        }
        let key = (
            PointingKey::new(field_id, response_key, pointings.antenna1, frequency_hz),
            plan.into(),
        );
        if !self.weights.contains_key(&key) && self.weights.len() == self.entry_capacity {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        if self.additions == self.addition_capacity {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        self.weights
            .entry(key)
            .or_default()
            .add(weight)
            .map_err(|_| SpectralOperatorError::GeneratedNonfinite)?;
        self.additions = self
            .additions
            .checked_add(1)
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        Ok(())
    }

    #[cfg(test)]
    fn merge(&mut self, other: Self) -> Result<(), SpectralOperatorError> {
        if other.additions > self.addition_capacity.saturating_sub(self.additions) {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        for (key, weight) in other.weights {
            if !self.weights.contains_key(&key) && self.weights.len() == self.entry_capacity {
                return Err(SpectralOperatorError::ResidencyOverflow);
            }
            self.weights
                .entry(key)
                .or_default()
                .merge(weight)
                .map_err(|_| SpectralOperatorError::GeneratedNonfinite)?;
        }
        self.additions += other.additions;
        Ok(())
    }

    pub(crate) fn gridded_sensitivity(
        self,
        image_shape: [usize; 2],
        image_blc: [usize; 2],
        projectors: &BTreeMap<u64, MosaicProjector>,
        fft: &mut PreparedFft,
    ) -> Result<Vec<f64>, SpectralOperatorError> {
        let grid_shape = projectors
            .values()
            .next()
            .map(|projector| projector.grid_shape)
            .ok_or(SpectralOperatorError::ProblemMismatch)?;
        let mut grid = Array2::zeros((grid_shape[0], grid_shape[1]));
        let mut compensation = Array2::zeros((grid_shape[0], grid_shape[1]));
        for ((pointing, support), weight) in self.weights {
            let weight = weight.value();
            if weight == 0.0 {
                continue;
            }
            let projector = projectors
                .get(&pointing.response_key)
                .ok_or(SpectralOperatorError::ProblemMismatch)?;
            projector.grid_weight_at_center(
                pointing.field_id,
                support,
                weight,
                &mut grid,
                &mut compensation,
            )?;
        }
        fft.transform(&mut grid, true);
        let mut sensitivity = Vec::with_capacity(image_shape[0] * image_shape[1]);
        for x in 0..image_shape[0] {
            for y in 0..image_shape[1] {
                sensitivity.push(grid[(image_blc[0] + x, image_blc[1] + y)].re.max(0.0));
            }
        }
        Ok(sensitivity)
    }
}

#[cfg(test)]
mod tests {
    use casa_imaging_model::{DirectionFrame, SelectedPointingDirections, SkyDirection};

    use super::*;

    fn point(longitude: f64) -> SelectedPointingDirections {
        let direction = SkyDirection::new(DirectionFrame::J2000, longitude, -0.5);
        SelectedPointingDirections {
            antenna1: direction,
            antenna2: direction,
        }
    }

    fn projector() -> MosaicProjector {
        let image_shape = [128, 128];
        let reference_pixel = [64.0, 64.0];
        let increment_rad = [
            -std::f64::consts::PI / 180.0 / 3600.0,
            std::f64::consts::PI / 180.0 / 3600.0,
        ];
        let direction = casa_imaging_model::DirectionCoordinateSpec::new(
            casa_imaging_model::Projection::Sin,
            SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
            reference_pixel,
            increment_rad,
            [[1.0, 0.0], [0.0, 1.0]],
            [180.0, 0.0],
        );
        let geometry = SpectralOperatorGeometry {
            image_shape,
            grid_shape: [160, 160],
            image_blc: [16, 16],
            reference_pixel,
            increment_rad,
            direction,
        };
        let response = PreparedPrimaryBeamPower::casa_alma_aca_interferometric_direct(
            reference_pixel,
            increment_rad,
            image_shape,
            0.0,
        )
        .expect("response")
        .with_casa_aca_hetarray_convolution();
        MosaicProjector::new(geometry, &response, 230.0e9, 8).expect("projector")
    }

    #[test]
    fn projector_reuses_one_active_phased_kernel_and_obeys_adjoint_taps() {
        let mut projector = projector();
        let pointing = [67.0, 61.0];
        let plan = projector
            .plan(3, [0.0, 0.0], pointing, [64.0, 64.0], [128, 128])
            .expect("plan")
            .expect("in-grid sample");
        let repeated = projector
            .plan(3, [0.0, 0.0], pointing, [64.0, 64.0], [128, 128])
            .expect("repeated plan")
            .expect("in-grid sample");
        assert_eq!(projector.field_phase_gradients.len(), 1);
        assert_eq!(projector.active_phased_kernel.as_ref().unwrap().field_id, 3);
        assert!(
            projector
                .plan(3, [0.0, 0.0], [68.0, 61.0], [64.0, 64.0], [128, 128])
                .is_err()
        );

        let mut grid = Array2::zeros((160, 160));
        let mut compensation = Array2::zeros((160, 160));
        let input = Complex64::new(0.75, -0.25);
        projector.grid_compensated(&mut grid, &mut compensation, plan, input);
        let data = Array2::from_shape_fn((160, 160), |(x, y)| {
            Complex64::new((x as f64 * 0.01).sin(), (y as f64 * 0.02).cos())
        });
        let left = grid
            .iter()
            .zip(&data)
            .fold(Complex64::default(), |sum, (actual, datum)| {
                sum + actual.conj() * datum
            });
        let right = input.conj() * projector.degrid(&data, repeated);
        assert!(
            (left - right).norm() <= 5.0e-8,
            "CASA's Complex product boundary must remain adjoint within single-precision rounding: left={left:?} right={right:?}"
        );
    }

    #[test]
    fn casa_mosaic_kernel_extent_is_derived_from_the_unpadded_image() {
        assert_eq!(mosaic_convolution_size([128, 128]), 144);
        assert_eq!(mosaic_convolution_size([512, 512]), 528);
        assert_eq!(mosaic_convolution_size([63, 48]), 64);
    }

    #[test]
    fn residency_scales_with_scientific_mosaic_cardinality() {
        let base = residency_projection([128, 128], [160, 160], 1, 1, 100, 100, 1).unwrap();
        let larger_image = residency_projection([512, 512], [640, 640], 1, 1, 100, 100, 1).unwrap();
        let more_responses =
            residency_projection([128, 128], [160, 160], 2, 1, 100, 100, 1).unwrap();
        let more_fields = residency_projection([128, 128], [160, 160], 1, 6, 100, 100, 1).unwrap();
        let more_normals = residency_projection([128, 128], [160, 160], 1, 1, 100, 100, 2).unwrap();

        assert!(base.retained_bytes > 0);
        assert!(base.workspace_bytes > 0);
        assert!(larger_image.retained_bytes > base.retained_bytes);
        assert!(larger_image.workspace_bytes > base.workspace_bytes);
        assert!(more_responses.retained_bytes > base.retained_bytes);
        assert!(more_fields.retained_bytes > base.retained_bytes);
        assert!(more_normals.retained_bytes > base.retained_bytes);
    }

    #[test]
    fn normal_accumulator_fails_before_exceeding_key_or_addition_capacity() {
        let plan = MosaicSamplePlan {
            field_id: 0,
            center_in_bounds: true,
            loc: [0, 0],
            offset: [0, 0],
            x: [-1, 1],
            y: [-1, 1],
            weight_x: [-1, 1],
            weight_y: [-1, 1],
        };
        let mut additions = MosaicNormalAccumulator::with_capacity(2, 1);
        additions
            .accumulate(0, 0, point(1.0), 1.0e9, plan, 1.0)
            .unwrap();
        assert_eq!(
            additions.accumulate(0, 0, point(1.0), 1.0e9, plan, 1.0),
            Err(SpectralOperatorError::ResidencyOverflow)
        );

        let mut entries = MosaicNormalAccumulator::with_capacity(1, 2);
        entries
            .accumulate(0, 0, point(1.0), 1.0e9, plan, 1.0)
            .unwrap();
        assert_eq!(
            entries.accumulate(1, 0, point(1.1), 1.0e9, plan, 1.0),
            Err(SpectralOperatorError::ResidencyOverflow)
        );
    }

    #[test]
    fn cropped_edge_taps_do_not_contribute_to_scalar_normalization() {
        let mut projector = projector();
        let u_lambda = (-81.0) * projector.du_lambda;
        let plan = projector
            .plan(3, [u_lambda, 0.0], [64.0, 64.0], [64.0, 64.0], [128, 128])
            .expect("plan")
            .expect("kernel wings still intersect the grid");
        assert!(!MosaicProjector::contributes_to_normalization(plan));
    }

    #[test]
    fn casa_mosaic_effective_domain_excludes_the_final_grid_edges() {
        let mut projector = projector();
        let last = projector.grid_shape[0] as f64 - 1.0;
        let u_lambda = (last - projector.grid_shape[0] as f64 / 2.0) * projector.du_lambda;
        let v_lambda = -(last - projector.grid_shape[1] as f64 / 2.0) * projector.dv_lambda;
        let plan = projector
            .plan(
                3,
                [u_lambda, v_lambda],
                [64.0, 64.0],
                [64.0, 64.0],
                [128, 128],
            )
            .expect("plan")
            .expect("kernel wings still intersect CASA's effective grid domain");

        assert_eq!(plan.loc, [159, 159]);
        assert_eq!(plan.x[1], -1);
        assert_eq!(plan.y[1], -1);
        let weight_support = MosaicWeightSupportKey::from(plan);
        assert_eq!(weight_support.max_x, 0);
        assert_eq!(weight_support.max_y, 0);
        assert!(!MosaicProjector::contributes_to_normalization(plan));
    }

    #[test]
    fn field_pointing_reduction_is_partition_invariant() {
        let samples = [
            (0, point(1.0), 1.0e9, 1.0),
            (1, point(1.1), 1.0e9, 2.0),
            (0, point(1.0), 1.0e9, 3.0),
            (1, point(1.1), 1.0e9, 4.0),
        ];
        let plan = MosaicSamplePlan {
            field_id: 0,
            center_in_bounds: true,
            loc: [0, 0],
            offset: [0, 0],
            x: [-3, 3],
            y: [-3, 3],
            weight_x: [-3, 3],
            weight_y: [-3, 3],
        };
        let mut serial = MosaicNormalAccumulator::default();
        for (field, pointing, frequency, weight) in samples {
            serial
                .accumulate(field, 0, pointing, frequency, plan, weight)
                .expect("serial sample");
        }
        let mut left = MosaicNormalAccumulator::default();
        let mut right = MosaicNormalAccumulator::default();
        for (index, (field, pointing, frequency, weight)) in samples.into_iter().enumerate() {
            (if index % 2 == 0 {
                &mut left
            } else {
                &mut right
            })
            .accumulate(field, 0, pointing, frequency, plan, weight)
            .expect("partition sample");
        }
        left.merge(right).expect("deterministic merge");
        let expected = serial
            .weights
            .into_iter()
            .map(|(key, weight)| (key, weight.value()))
            .collect::<Vec<_>>();
        let actual = left
            .weights
            .into_iter()
            .map(|(key, weight)| (key, weight.value()))
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }
}
