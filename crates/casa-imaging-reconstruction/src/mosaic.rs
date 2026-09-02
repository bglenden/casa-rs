// SPDX-License-Identifier: LGPL-3.0-or-later

//! Deterministic field/pointing accumulation for direction-dependent normal state.

use std::collections::BTreeMap;
use std::mem::size_of;

use casa_imaging_model::{DirectionFrame, SelectedAntennaResponses, SelectedPointingDirections};
use ndarray::Array2;
use num_complex::{Complex32, Complex64};

use crate::{
    SpectralOperatorError,
    primary_beam::PreparedPrimaryBeamPower,
    spectral_operator::{
        MosaicProjectorKey, PreparedFft, SpectralOperatorGeometry, fft_planning_words_for_shape,
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
        (
            casa_imaging_model::MeasurementSetIdentity,
            u32,
            u32,
            casa_imaging_model::AntennaResponseClass,
            casa_imaging_model::AntennaResponseClass,
            casa_imaging_model::AntennaResponseClass,
        ),
        (
            MosaicProjectorKey,
            f64,
            f64,
            casa_imaging_model::SelectedAntennaResponses,
        ),
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
    let projector_tree_bytes =
        bounded_tree_bytes::<MosaicProjectorKey, MosaicProjector>(response_capacity, 1)?;
    let projector_bytes = kernel_bytes
        .checked_mul(3)
        .and_then(|bytes| bytes.checked_mul(response_capacity))
        .and_then(|bytes| bytes.checked_add(projector_tree_bytes))
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
    let normal_entry_bytes = bounded_tree_bytes::<
        (PointingKey, MosaicWeightSupportKey),
        SignedExactF64Sum,
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
    pointing: MosaicPointingPairKey,
    center_in_bounds: bool,
    loc: [isize; 2],
    offset: [isize; 2],
    x: [isize; 2],
    y: [isize; 2],
    weight_x: [isize; 2],
    weight_y: [isize; 2],
}

struct PhasedKernel {
    pointing: MosaicPointingPairKey,
    imaging: Array2<Complex32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct MosaicPointingPairKey {
    field_id: i32,
    frame: DirectionFrame,
    antenna1_longitude_bits: u64,
    antenna1_latitude_bits: u64,
    antenna2_longitude_bits: u64,
    antenna2_latitude_bits: u64,
}

impl MosaicPointingPairKey {
    fn new(field_id: i32, pointings: SelectedPointingDirections) -> Self {
        Self {
            field_id,
            frame: pointings.antenna1.frame(),
            antenna1_longitude_bits: pointings.antenna1.longitude_rad().to_bits(),
            antenna1_latitude_bits: pointings.antenna1.latitude_rad().to_bits(),
            antenna2_longitude_bits: pointings.antenna2.longitude_rad().to_bits(),
            antenna2_latitude_bits: pointings.antenna2.latitude_rad().to_bits(),
        }
    }
}

fn pointing_pair_geometry(
    geometry: SpectralOperatorGeometry,
    pointing: MosaicPointingPairKey,
) -> Result<([[f64; 2]; 2], [f64; 2]), SpectralOperatorError> {
    if pointing.frame != geometry.direction.reference_direction().frame() {
        return Err(SpectralOperatorError::UnsupportedProblem);
    }
    let directions = [
        [
            f64::from_bits(pointing.antenna1_longitude_bits),
            f64::from_bits(pointing.antenna1_latitude_bits),
        ],
        [
            f64::from_bits(pointing.antenna2_longitude_bits),
            f64::from_bits(pointing.antenna2_latitude_bits),
        ],
    ];
    let pixels = directions.map(|direction| {
        crate::mask::direction_world_to_pixel(geometry.direction, direction)
            .map_err(|_| SpectralOperatorError::UnsupportedGeometry)
    });
    let pixels = [pixels[0]?, pixels[1]?];
    let midpoint = [
        (pixels[0][0] + pixels[1][0]) * 0.5,
        (pixels[0][1] + pixels[1][1]) * 0.5,
    ];
    Ok((pixels, midpoint))
}

pub(crate) struct MosaicProjector {
    geometry: SpectralOperatorGeometry,
    antenna_responses: SelectedAntennaResponses,
    frequency_hz: f64,
    grid_shape: [usize; 2],
    du_lambda: f64,
    dv_lambda: f64,
    normalization_support: usize,
    support: usize,
    kernel_center: usize,
    kernel: Array2<Complex32>,
    weight_kernel: Array2<Complex32>,
    active_phased_kernel: Option<PhasedKernel>,
}

impl MosaicProjector {
    pub(crate) fn new(
        geometry: SpectralOperatorGeometry,
        response: &PreparedPrimaryBeamPower,
        antenna_responses: SelectedAntennaResponses,
        frequency_hz: f64,
        support_frequency_hz: f64,
        field_capacity: usize,
    ) -> Result<Self, SpectralOperatorError> {
        if field_capacity == 0 {
            return Err(SpectralOperatorError::UnsupportedProblem);
        }
        let conv_size = mosaic_convolution_size(geometry.image_shape);
        if conv_size < 16 || conv_size % 2 != 0 {
            return Err(SpectralOperatorError::UnsupportedGeometry);
        }
        let family_pair = SelectedAntennaResponses {
            antenna1: antenna_responses.family_envelope,
            antenna2: antenna_responses.family_envelope,
            family_envelope: antenna_responses.family_envelope,
        };
        let family_support = if antenna_responses.antenna1 == antenna_responses.family_envelope
            && antenna_responses.antenna2 == antenna_responses.family_envelope
        {
            None
        } else {
            let family_weight = screen_fft_temp(
                geometry,
                response,
                family_pair,
                support_frequency_hz,
                conv_size,
                2,
            )?;
            Some(find_support(&family_weight, 1))
        };
        // Resolve a distinct support-frequency crop to a scalar before
        // retaining either science-frequency crop. This keeps the build peak
        // at the planner's two temporary crops instead of allowing imaging,
        // weight, and support crops to overlap.
        let support_frequency_support = if frequency_hz.to_bits() == support_frequency_hz.to_bits()
        {
            None
        } else {
            Some(screen_support(
                geometry,
                response,
                antenna_responses,
                support_frequency_hz,
                conv_size,
            )?)
        };
        let imaging = screen_fft_temp(
            geometry,
            response,
            antenna_responses,
            frequency_hz,
            conv_size,
            1,
        )?;
        let weight = screen_fft_temp(
            geometry,
            response,
            antenna_responses,
            frequency_hz,
            conv_size,
            2,
        )?;
        let normalization_support =
            support_frequency_support.unwrap_or_else(|| find_support(&weight, 1));
        if normalization_support == 0 {
            return Err(SpectralOperatorError::UnsupportedGeometry);
        }
        let center = imaging.dim().0 / 2;
        let normalization = plane_sum(&imaging, center, normalization_support, 1).re;
        if !normalization.is_finite() || normalization <= 1.0e-6 {
            return Err(SpectralOperatorError::GeneratedNonfinite);
        }
        let family_support = family_support.unwrap_or(normalization_support);
        if family_support < normalization_support {
            return Err(SpectralOperatorError::UnsupportedGeometry);
        }
        let cropped_size = 2 * (family_support + 2);
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
        // ACA mosaic execution uses CASA HetArrayConvFunc, whose lattice path
        // normalizes the weight convolution function by its own support sum.
        let weight_normalization = plane_sum(&weight, weight_center, normalization_support, 1).re;
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
            geometry,
            antenna_responses,
            frequency_hz,
            grid_shape: geometry.grid_shape,
            du_lambda: 1.0 / (geometry.grid_shape[0] as f64 * geometry.increment_rad[0].abs()),
            dv_lambda: 1.0 / (geometry.grid_shape[1] as f64 * geometry.increment_rad[1].abs()),
            normalization_support,
            support: family_support,
            kernel_center: kernel.dim().0 / 2,
            kernel,
            weight_kernel,
            active_phased_kernel: None,
        })
    }

    pub(crate) fn plan(
        &mut self,
        response: &PreparedPrimaryBeamPower,
        field_id: i32,
        uv_lambda: [f64; 2],
        pointings: SelectedPointingDirections,
    ) -> Result<Option<MosaicSamplePlan>, SpectralOperatorError> {
        if pointings.antenna1.frame() != pointings.antenna2.frame()
            || pointings.antenna1.frame() != self.geometry.direction.reference_direction().frame()
        {
            return Err(SpectralOperatorError::UnsupportedProblem);
        }
        let pointing_pixels = [pointings.antenna1, pointings.antenna2].map(|pointing| {
            crate::mask::direction_world_to_pixel(
                self.geometry.direction,
                [pointing.longitude_rad(), pointing.latitude_rad()],
            )
            .map_err(|_| SpectralOperatorError::UnsupportedGeometry)
        });
        let [antenna1_pixel, antenna2_pixel] = [pointing_pixels[0]?, pointing_pixels[1]?];
        let pointing_pixel = [
            (antenna1_pixel[0] + antenna2_pixel[0]) * 0.5,
            (antenna1_pixel[1] + antenna2_pixel[1]) * 0.5,
        ];
        let pointing = MosaicPointingPairKey::new(field_id, pointings);
        self.activate_imaging_kernel(
            response,
            pointing,
            [antenna1_pixel, antenna2_pixel],
            pointing_pixel,
        )?;
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
        Ok(Some(MosaicSamplePlan {
            pointing,
            center_in_bounds: loc[0] >= 0 && loc[0] <= upper_x && loc[1] >= 0 && loc[1] <= upper_y,
            loc,
            offset,
            x,
            y,
            weight_x,
            weight_y,
        }))
    }

    fn activate_imaging_kernel(
        &mut self,
        response: &PreparedPrimaryBeamPower,
        pointing: MosaicPointingPairKey,
        pointing_pixels: [[f64; 2]; 2],
        midpoint_pixel: [f64; 2],
    ) -> Result<(), SpectralOperatorError> {
        if self
            .active_phased_kernel
            .as_ref()
            .is_some_and(|kernel| kernel.pointing == pointing)
        {
            return Ok(());
        }
        let relative_offsets = pointing_pixels.map(|pixel| {
            image_plane_offset_rad(
                self.geometry,
                [pixel[0] - midpoint_pixel[0], pixel[1] - midpoint_pixel[1]],
            )
        });
        let mut imaging = if pointing_pixels[0] == pointing_pixels[1] {
            self.kernel.clone()
        } else {
            let conv_size = mosaic_convolution_size(self.geometry.image_shape);
            let temporary = screen_fft_temp_at_offsets(
                self.geometry,
                response,
                self.antenna_responses,
                self.frequency_hz,
                conv_size,
                1,
                relative_offsets,
            )?;
            normalized_resampled_kernel(&temporary, self.normalization_support(), self.support)?
        };
        let phase_gradient = phase_gradient(
            midpoint_pixel,
            self.geometry.reference_pixel,
            self.geometry.image_shape,
        );
        apply_phase_gradient(&mut imaging, phase_gradient);
        self.active_phased_kernel = Some(PhasedKernel { pointing, imaging });
        Ok(())
    }

    fn normalization_support(&self) -> usize {
        self.normalization_support
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
            .filter(|kernel| kernel.pointing == plan.pointing)
            .expect("sample plan keeps its pointing-pair kernel active")
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
            .filter(|kernel| kernel.pointing == plan.pointing)
            .expect("sample plan keeps its pointing-pair kernel active")
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
        &mut self,
        response: &PreparedPrimaryBeamPower,
        pointing: MosaicPointingPairKey,
        support: MosaicWeightSupportKey,
        value: f64,
        grid: &mut Array2<Complex64>,
        compensation: &mut Array2<Complex64>,
    ) -> Result<(), SpectralOperatorError> {
        let (pointing_pixels, midpoint_pixel) = pointing_pair_geometry(self.geometry, pointing)?;
        let relative_offsets = pointing_pixels.map(|pixel| {
            image_plane_offset_rad(
                self.geometry,
                [pixel[0] - midpoint_pixel[0], pixel[1] - midpoint_pixel[1]],
            )
        });
        let differential_weight;
        let weight_kernel = if pointing_pixels[0] == pointing_pixels[1] {
            &self.weight_kernel
        } else {
            let temporary = screen_fft_temp_at_offsets(
                self.geometry,
                response,
                self.antenna_responses,
                self.frequency_hz,
                mosaic_convolution_size(self.geometry.image_shape),
                2,
                relative_offsets,
            )?;
            differential_weight =
                normalized_resampled_kernel(&temporary, self.normalization_support, self.support)?;
            &differential_weight
        };
        let phase_gradient = phase_gradient(
            midpoint_pixel,
            self.geometry.reference_pixel,
            self.geometry.image_shape,
        );
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
                let tap = weight_kernel[(kernel_x, kernel_y)] * phasor_x * phasor_y;
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
    antenna_responses: SelectedAntennaResponses,
    frequency_hz: f64,
    conv_size: usize,
    power: u32,
) -> Result<Array2<Complex32>, SpectralOperatorError> {
    screen_fft_temp_at_offsets(
        geometry,
        response,
        antenna_responses,
        frequency_hz,
        conv_size,
        power,
        [[0.0; 2]; 2],
    )
}

fn screen_fft_temp_at_offsets(
    geometry: SpectralOperatorGeometry,
    response: &PreparedPrimaryBeamPower,
    antenna_responses: SelectedAntennaResponses,
    frequency_hz: f64,
    conv_size: usize,
    power: u32,
    antenna_offsets_rad: [[f64; 2]; 2],
) -> Result<Array2<Complex32>, SpectralOperatorError> {
    let center = conv_size as isize / 2;
    let mut screen = Array2::zeros((conv_size, conv_size));
    for y in 0..conv_size {
        for x in 0..conv_size {
            let image_offset = image_plane_offset_rad(
                geometry,
                [
                    (x as isize - center) as f64 * geometry.image_shape[0] as f64
                        / conv_size as f64,
                    (y as isize - center) as f64 * geometry.image_shape[1] as f64
                        / conv_size as f64,
                ],
            );
            screen[(x, y)] = Complex64::new(
                f64::from(response.paired_mosaic_voltage_at_offsets(
                    antenna_responses,
                    [
                        image_offset[0] - antenna_offsets_rad[0][0],
                        image_offset[1] - antenna_offsets_rad[0][1],
                    ],
                    [
                        image_offset[0] - antenna_offsets_rad[1][0],
                        image_offset[1] - antenna_offsets_rad[1][1],
                    ],
                    frequency_hz,
                )?)
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

fn image_plane_offset_rad(geometry: SpectralOperatorGeometry, pixel_offset: [f64; 2]) -> [f64; 2] {
    let pc = geometry.direction.pc();
    [
        geometry.increment_rad[0] * (pc[0][0] * pixel_offset[0] + pc[0][1] * pixel_offset[1]),
        geometry.increment_rad[1] * (pc[1][0] * pixel_offset[0] + pc[1][1] * pixel_offset[1]),
    ]
}

fn phase_gradient(
    pointing_pixel: [f64; 2],
    reference_pixel: [f64; 2],
    image_shape: [usize; 2],
) -> [f64; 2] {
    [
        -(pointing_pixel[0] - reference_pixel[0]) * std::f64::consts::TAU
            / (image_shape[0] as f64 * MOSAIC_OVERSAMPLING as f64),
        -(pointing_pixel[1] - reference_pixel[1]) * std::f64::consts::TAU
            / (image_shape[1] as f64 * MOSAIC_OVERSAMPLING as f64),
    ]
}

fn apply_phase_gradient(kernel: &mut Array2<Complex32>, phase_gradient: [f64; 2]) {
    let center = kernel.dim().0 as isize / 2;
    for ((kernel_x, kernel_y), value) in kernel.indexed_iter_mut() {
        let signed_x = kernel_x as isize - center;
        let signed_y = kernel_y as isize - center;
        let phase_x = signed_x as f64 * phase_gradient[0];
        let phase_y = signed_y as f64 * phase_gradient[1];
        let phasor_x = Complex32::new(phase_x.cos() as f32, phase_x.sin() as f32);
        let phasor_y = Complex32::new(phase_y.cos() as f32, phase_y.sin() as f32);
        *value = *value * phasor_x * phasor_y;
    }
}

fn normalized_resampled_kernel(
    temporary: &Array2<Complex32>,
    normalization_support: usize,
    family_support: usize,
) -> Result<Array2<Complex32>, SpectralOperatorError> {
    let center = temporary.dim().0 / 2;
    let normalization = plane_sum(temporary, center, normalization_support, 1).re;
    if !normalization.is_finite() || normalization <= 1.0e-6 {
        return Err(SpectralOperatorError::GeneratedNonfinite);
    }
    let cropped_size = 2 * (family_support + 2);
    let cropped_center = cropped_size / 2;
    let mut cropped = Array2::zeros((cropped_size, cropped_size));
    for y in 0..cropped_size {
        for x in 0..cropped_size {
            cropped[(x, y)] = temporary[(center + x - cropped_center, center + y - cropped_center)]
                / normalization;
        }
    }
    Ok(lanczos_resample(&cropped, MOSAIC_OVERSAMPLING))
}

fn screen_support(
    geometry: SpectralOperatorGeometry,
    response: &PreparedPrimaryBeamPower,
    antenna_responses: SelectedAntennaResponses,
    frequency_hz: f64,
    conv_size: usize,
) -> Result<usize, SpectralOperatorError> {
    let support_weight = screen_fft_temp(
        geometry,
        response,
        antenna_responses,
        frequency_hz,
        conv_size,
        2,
    )?;
    Ok(find_support(&support_weight, 1))
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
    pointing: MosaicPointingPairKey,
    response_key: MosaicProjectorKey,
    frequency_bits: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct MosaicWeightSupportKey {
    min_x: isize,
    max_x: isize,
    min_y: isize,
    max_y: isize,
}

#[derive(Default)]
struct SignedExactF64Sum {
    positive: ExactF64Sum,
    negative: ExactF64Sum,
}

impl SignedExactF64Sum {
    fn add(&mut self, value: f64) -> Result<(), crate::WeightingError> {
        if value.is_sign_negative() {
            self.negative.add(-value)
        } else {
            self.positive.add(value)
        }
    }

    fn value(&self) -> f64 {
        self.positive.value() - self.negative.value()
    }

    #[cfg(test)]
    fn merge(&mut self, other: Self) -> Result<(), crate::WeightingError> {
        self.positive.merge(other.positive)?;
        self.negative.merge(other.negative)
    }
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

/// Reconstruction-owned exact reduction of mosaic pointing-pair weights.
///
/// The accumulator stores one scalar per unique field, pointing, and evaluation
/// frequency. It therefore stays independent of source block boundaries and
/// worker partitions while retaining independent antenna boresights.
pub(crate) struct MosaicNormalAccumulator {
    weights: BTreeMap<(PointingKey, MosaicWeightSupportKey), SignedExactF64Sum>,
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
        response_key: MosaicProjectorKey,
        pointings: SelectedPointingDirections,
        frequency_hz: f64,
        plan: MosaicSamplePlan,
        weight: f64,
    ) -> Result<(), SpectralOperatorError> {
        if pointings.antenna1.frame() != pointings.antenna2.frame()
            || !frequency_hz.is_finite()
            || frequency_hz <= 0.0
            || !weight.is_finite()
        {
            return Err(SpectralOperatorError::UnsupportedProblem);
        }
        let pointing = MosaicPointingPairKey::new(field_id, pointings);
        if plan.pointing != pointing {
            return Err(SpectralOperatorError::ProblemMismatch);
        }
        let key = (
            PointingKey {
                pointing,
                response_key,
                frequency_bits: frequency_hz.to_bits(),
            },
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
        response: &PreparedPrimaryBeamPower,
        projectors: &mut BTreeMap<MosaicProjectorKey, MosaicProjector>,
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
                .get_mut(&pointing.response_key)
                .ok_or(SpectralOperatorError::ProblemMismatch)?;
            projector.grid_weight_at_center(
                response,
                pointing.pointing,
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
                sensitivity.push(grid[(image_blc[0] + x, image_blc[1] + y)].re);
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

    fn point_at_pixels(
        geometry: SpectralOperatorGeometry,
        antenna1: [f64; 2],
        antenna2: [f64; 2],
    ) -> SelectedPointingDirections {
        let direction = |pixel| {
            let world = crate::mask::direction_pixel_to_world(geometry.direction, pixel)
                .expect("test pointing pixel");
            SkyDirection::new(DirectionFrame::J2000, world[0], world[1])
        };
        SelectedPointingDirections {
            antenna1: direction(antenna1),
            antenna2: direction(antenna2),
        }
    }

    fn aca_pair() -> SelectedAntennaResponses {
        SelectedAntennaResponses {
            antenna1: casa_imaging_model::AntennaResponseClass::CasaAca7m,
            antenna2: casa_imaging_model::AntennaResponseClass::CasaAca7m,
            family_envelope: casa_imaging_model::AntennaResponseClass::CasaAca7m,
        }
    }

    fn response_key() -> MosaicProjectorKey {
        crate::spectral_operator::mosaic_response_key(230.0e9, 230.0e9, aca_pair())
    }

    fn projector_inputs() -> (SpectralOperatorGeometry, PreparedPrimaryBeamPower) {
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
        (geometry, response)
    }

    #[test]
    fn heterogeneous_projector_reuses_one_active_differential_pointing_kernel_and_obeys_adjoint_taps()
     {
        let (geometry, response) = projector_inputs();
        let mut projector =
            MosaicProjector::new(geometry, &response, aca_pair(), 230.0e9, 230.0e9, 8)
                .expect("projector");
        let pointing = point_at_pixels(geometry, [67.0, 61.0], [67.0, 61.0]);
        let plan = projector
            .plan(&response, 3, [0.0, 0.0], pointing)
            .expect("plan")
            .expect("in-grid sample");
        projector
            .plan(&response, 3, [0.0, 0.0], pointing)
            .expect("repeated plan")
            .expect("in-grid sample");
        let shared_pointing_kernel = projector
            .active_phased_kernel
            .as_ref()
            .expect("shared pointing kernel")
            .imaging
            .clone();
        assert_eq!(
            projector
                .active_phased_kernel
                .as_ref()
                .unwrap()
                .pointing
                .field_id,
            3
        );

        let differential = point_at_pixels(geometry, [66.5, 61.0], [67.5, 61.0]);
        let differential_plan = projector
            .plan(&response, 3, [0.0, 0.0], differential)
            .expect("differential plan")
            .expect("differential in-grid sample");
        assert_ne!(plan.pointing, differential_plan.pointing);
        assert_ne!(
            projector
                .active_phased_kernel
                .as_ref()
                .expect("differential kernel")
                .imaging,
            shared_pointing_kernel
        );

        let mut grid = Array2::zeros((160, 160));
        let mut compensation = Array2::zeros((160, 160));
        let input = Complex64::new(0.75, -0.25);
        projector.grid_compensated(&mut grid, &mut compensation, differential_plan, input);
        let data = Array2::from_shape_fn((160, 160), |(x, y)| {
            Complex64::new((x as f64 * 0.01).sin(), (y as f64 * 0.02).cos())
        });
        let left = grid
            .iter()
            .zip(&data)
            .fold(Complex64::default(), |sum, (actual, datum)| {
                sum + actual.conj() * datum
            });
        let right = input.conj() * projector.degrid(&data, differential_plan);
        assert!(
            (left - right).norm() <= 5.0e-8,
            "CASA's Complex product boundary must remain adjoint within single-precision rounding: left={left:?} right={right:?}"
        );

        let key = response_key();
        let mut normal = MosaicNormalAccumulator::with_capacity(1, 1);
        normal
            .accumulate(3, key, differential, 230.0e9, differential_plan, 1.0)
            .expect("differential normal contribution");
        let mut projectors = BTreeMap::from([(key, projector)]);
        let mut fft = PreparedFft::new(
            geometry.grid_shape,
            fft_resident_complex_values_for_shape(geometry.grid_shape).expect("FFT residency"),
        )
        .expect("normal FFT");
        let sensitivity = normal
            .gridded_sensitivity(
                geometry.image_shape,
                geometry.image_blc,
                &response,
                &mut projectors,
                &mut fft,
            )
            .expect("differential sensitivity");
        assert!(sensitivity.iter().all(|value| value.is_finite()));
        assert!(sensitivity.iter().any(|value| *value > 0.0));
    }

    #[test]
    fn casa_mosaic_kernel_extent_is_derived_from_the_unpadded_image() {
        assert_eq!(mosaic_convolution_size([128, 128]), 144);
        assert_eq!(mosaic_convolution_size([512, 512]), 528);
        assert_eq!(mosaic_convolution_size([63, 48]), 64);
    }

    #[test]
    fn heterogeneous_supports_match_casa_512_pixel_oracle() {
        let (mut geometry, _) = projector_inputs();
        geometry.image_shape = [512, 512];
        geometry.grid_shape = [640, 640];
        geometry.image_blc = [64, 64];
        geometry.reference_pixel = [256.0, 256.0];
        let response = PreparedPrimaryBeamPower::casa_alma_aca_interferometric_direct(
            geometry.reference_pixel,
            geometry.increment_rad,
            geometry.image_shape,
            0.0,
        )
        .unwrap()
        .with_casa_aca_hetarray_convolution();
        let alma = casa_imaging_model::AntennaResponseClass::CasaAlma12m;
        let aca = casa_imaging_model::AntennaResponseClass::CasaAca7m;
        let supports = [(alma, alma), (alma, aca), (aca, aca)].map(|(antenna1, antenna2)| {
            MosaicProjector::new(
                geometry,
                &response,
                SelectedAntennaResponses {
                    antenna1,
                    antenna2,
                    family_envelope: alma,
                },
                230.0e9,
                230.0e9,
                7,
            )
            .map(|projector| {
                (
                    projector.normalization_support,
                    projector.support,
                    projector.kernel.dim(),
                )
            })
            .unwrap()
        });
        assert_eq!(supports.map(|entry| entry.0), [29, 24, 18]);
        assert_eq!(supports.map(|entry| entry.1), [29; 3]);
        assert_eq!(supports.map(|entry| entry.2), [(620, 620); 3]);
    }

    #[test]
    fn distinct_support_frequency_is_scalarized_before_retained_kernel_construction() {
        let (geometry, response) = projector_inputs();
        let conv_size = mosaic_convolution_size(geometry.image_shape);
        let support_frequency_hz = 115.0e9;
        let expected_support = screen_support(
            geometry,
            &response,
            aca_pair(),
            support_frequency_hz,
            conv_size,
        )
        .expect("support-frequency scalar");
        let projector = MosaicProjector::new(
            geometry,
            &response,
            aca_pair(),
            230.0e9,
            support_frequency_hz,
            8,
        )
        .expect("projector with distinct support frequency");
        assert_eq!(projector.normalization_support, expected_support);

        let residency = residency_projection(geometry.image_shape, [16, 16], 1, 1, 100, 100, 1)
            .expect("mosaic residency");
        let temp_side = conv_size / 4;
        let temp_bytes = temp_side * temp_side * size_of::<Complex32>();
        let screen_bytes = conv_size * conv_size * size_of::<Complex64>();
        let fft_bytes = fft_resident_complex_values_for_shape([conv_size, conv_size])
            .expect("FFT residency")
            * size_of::<Complex64>();
        let planning_bytes = fft_planning_words_for_shape([conv_size, conv_size])
            .expect("FFT planning residency")
            * size_of::<usize>();
        let charged_screen_peak = screen_bytes + fft_bytes + planning_bytes + 2 * temp_bytes;
        assert!(
            residency.workspace_bytes >= charged_screen_peak,
            "the workspace must charge the retained science crop and the support FFT output crop"
        );
    }

    #[test]
    fn weight_convolution_function_uses_casa_hetarray_weight_sum() {
        let (geometry, response) = projector_inputs();
        let projector = MosaicProjector::new(geometry, &response, aca_pair(), 230.0e9, 230.0e9, 8)
            .expect("projector");
        let conv_size = mosaic_convolution_size(geometry.image_shape);
        let imaging = screen_fft_temp(geometry, &response, aca_pair(), 230.0e9, conv_size, 1)
            .expect("imaging CF");
        let weight = screen_fft_temp(geometry, &response, aca_pair(), 230.0e9, conv_size, 2)
            .expect("weight CF");
        let support = find_support(&weight, 1);
        let center = imaging.dim().0 / 2;
        let imaging_pb_sum = plane_sum(&imaging, center, support, 1).re;
        let weight_pb_sum = plane_sum(&weight, center, support, 1).re;
        assert!(
            (imaging_pb_sum - weight_pb_sum).abs() > 1.0e-3,
            "fixture must distinguish CASA's shared pbSum from independent normalization"
        );

        let cropped_size = 2 * (support + 2);
        let cropped_center = cropped_size / 2;
        let mut shared_pb_sum = Array2::zeros((cropped_size, cropped_size));
        let mut casa_weight = Array2::zeros((cropped_size, cropped_size));
        for y in 0..cropped_size {
            for x in 0..cropped_size {
                let value = weight[(center + x - cropped_center, center + y - cropped_center)];
                shared_pb_sum[(x, y)] = value / imaging_pb_sum;
                casa_weight[(x, y)] = value / weight_pb_sum;
            }
        }
        let casa_weight = lanczos_resample(&casa_weight, MOSAIC_OVERSAMPLING);
        let shared_pb_sum = lanczos_resample(&shared_pb_sum, MOSAIC_OVERSAMPLING);

        assert_eq!(projector.weight_kernel, casa_weight);
        assert_ne!(projector.weight_kernel, shared_pb_sum);
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
        assert_eq!(
            more_fields.retained_bytes, base.retained_bytes,
            "pointing pairs replace one active kernel and do not retain a per-field cache"
        );
        assert!(more_normals.retained_bytes > base.retained_bytes);
    }

    #[test]
    fn normal_accumulator_fails_before_exceeding_key_or_addition_capacity() {
        let plan = MosaicSamplePlan {
            pointing: MosaicPointingPairKey::new(0, point(1.0)),
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
            .accumulate(0, response_key(), point(1.0), 1.0e9, plan, 1.0)
            .unwrap();
        assert_eq!(
            additions.accumulate(0, response_key(), point(1.0), 1.0e9, plan, 1.0),
            Err(SpectralOperatorError::ResidencyOverflow)
        );

        let mut entries = MosaicNormalAccumulator::with_capacity(1, 2);
        entries
            .accumulate(0, response_key(), point(1.0), 1.0e9, plan, 1.0)
            .unwrap();
        let second_plan = MosaicSamplePlan {
            pointing: MosaicPointingPairKey::new(1, point(1.1)),
            ..plan
        };
        assert_eq!(
            entries.accumulate(1, response_key(), point(1.1), 1.0e9, second_plan, 1.0,),
            Err(SpectralOperatorError::ResidencyOverflow)
        );
    }

    #[test]
    fn cropped_edge_taps_do_not_contribute_to_scalar_normalization() {
        let (geometry, response) = projector_inputs();
        let mut projector =
            MosaicProjector::new(geometry, &response, aca_pair(), 230.0e9, 230.0e9, 8)
                .expect("projector");
        let u_lambda = (-81.0) * projector.du_lambda;
        let plan = projector
            .plan(
                &response,
                3,
                [u_lambda, 0.0],
                point_at_pixels(geometry, [64.0, 64.0], [64.0, 64.0]),
            )
            .expect("plan")
            .expect("kernel wings still intersect the grid");
        assert!(!MosaicProjector::contributes_to_normalization(plan));
    }

    #[test]
    fn casa_mosaic_effective_domain_excludes_the_final_grid_edges() {
        let (geometry, response) = projector_inputs();
        let mut projector =
            MosaicProjector::new(geometry, &response, aca_pair(), 230.0e9, 230.0e9, 8)
                .expect("projector");
        let last = projector.grid_shape[0] as f64 - 1.0;
        let u_lambda = (last - projector.grid_shape[0] as f64 / 2.0) * projector.du_lambda;
        let v_lambda = -(last - projector.grid_shape[1] as f64 / 2.0) * projector.dv_lambda;
        let plan = projector
            .plan(
                &response,
                3,
                [u_lambda, v_lambda],
                point_at_pixels(geometry, [64.0, 64.0], [64.0, 64.0]),
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
            (1, point(1.1), 1.0e9, -2.0),
            (0, point(1.0), 1.0e9, 3.0),
            (1, point(1.1), 1.0e9, -4.0),
        ];
        let plan = MosaicSamplePlan {
            pointing: MosaicPointingPairKey::new(0, point(1.0)),
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
            let sample_plan = MosaicSamplePlan {
                pointing: MosaicPointingPairKey::new(field, pointing),
                ..plan
            };
            serial
                .accumulate(
                    field,
                    response_key(),
                    pointing,
                    frequency,
                    sample_plan,
                    weight,
                )
                .expect("serial sample");
        }
        let mut left = MosaicNormalAccumulator::default();
        let mut right = MosaicNormalAccumulator::default();
        for (index, (field, pointing, frequency, weight)) in samples.into_iter().enumerate() {
            let sample_plan = MosaicSamplePlan {
                pointing: MosaicPointingPairKey::new(field, pointing),
                ..plan
            };
            (if index % 2 == 0 {
                &mut left
            } else {
                &mut right
            })
            .accumulate(
                field,
                response_key(),
                pointing,
                frequency,
                sample_plan,
                weight,
            )
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
