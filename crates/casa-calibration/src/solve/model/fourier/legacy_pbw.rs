// SPDX-License-Identifier: LGPL-3.0-or-later
//! Restore the cache cell retained with a legacy nPBWProjectFT recipe.

use casa_core::{ArrayValue, Complex32, RecordValue, ScalarValue, Value};
use casa_images::PagedImage;
use casa_imaging_operator::{GridGeometry, LegacyPbWConvolution, PolarizationRouting};
use casa_imaging_runtime::{Demand, HostResources, Reservation, ResourcePolicy, admit};

use super::{FourierRow, GainSolveError, complex_model_image_path, model_error, record_number};
use crate::ft::legacy_pbw::KERNEL_IMAGE;

#[derive(Default)]
pub(super) struct FeedAngle(Option<(i32, f64, f64)>);

impl FeedAngle {
    pub(super) fn for_row(&mut self, row: &FourierRow<'_>) -> Result<f64, GainSolveError> {
        let field = row.row.field_id;
        let time = row.row.time_seconds;
        if let Some((last_field, last_time, angle)) = self.0
            && (field, time) == (last_field, last_time)
        {
            return Ok(angle);
        }
        let angle = -row
            .engine
            .feed_parallactic_angle(time, field as usize, 0)
            .map_err(model_error)?;
        self.0 = Some((field, time, angle));
        Ok(angle)
    }
}

pub(super) fn load(
    record: &RecordValue,
    row: &FourierRow<'_>,
    geometry: &GridGeometry,
    polarization: &PolarizationRouting,
    pointing: bool,
) -> Result<(LegacyPbWConvolution, Reservation), GainSolveError> {
    let dimension = |field| {
        let value = match record.get(field) {
            Some(Value::Scalar(ScalarValue::Int32(value))) => i64::from(*value),
            Some(Value::Scalar(ScalarValue::Int64(value))) => *value,
            _ => return Err(model_error(format!("legacy PB/W recipe has no {field}"))),
        };
        u16::try_from(value).map_err(model_error)
    };
    let support = match record.get("casars_legacy_pbw_support") {
        Some(Value::Array(ArrayValue::Int32(widths))) if widths.ndim() == 1 => widths
            .iter()
            .copied()
            .map(u16::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(model_error)?,
        _ => vec![dimension("casars_legacy_pbw_support")?],
    };
    let sampling = dimension("casars_legacy_pbw_sampling")?;
    let angle = record_number(record, "casars_legacy_pbw_angle")
        .filter(|angle| angle.is_finite())
        .ok_or_else(|| model_error("legacy PB/W recipe has no valid reference angle"))?;
    let path = complex_model_image_path(record, row.ms)?.join(KERNEL_IMAGE);
    let kernel = PagedImage::<Complex32>::open_with_cache(path, 1).map_err(model_error)?;
    let correlations = row.correlations.len();
    LegacyPbWConvolution::validate_shape(kernel.shape(), sampling, &support, correlations)
        .map_err(model_error)?;
    let side = kernel.shape()[0];
    let cache_bytes = kernel
        .shape()
        .iter()
        .fold(size_of::<Complex32>() as u64, |bytes, &axis| {
            bytes.saturating_mul(axis as u64)
        });
    let retained = LegacyPbWConvolution::resident_bytes(
        side,
        correlations,
        sampling,
        &support,
        geometry.grid_shape(),
    );
    let memory = reserve("legacy PB/W cache image", retained)?;
    let _read_memory = reserve("legacy PB/W cache read", cache_bytes.saturating_mul(2))?;
    let samples = kernel.get().map_err(model_error)?;
    let mut terms = LegacyPbWConvolution::new(
        samples,
        sampling,
        support,
        geometry,
        polarization.grid_pols(),
        polarization.pol_map(),
        angle,
    )
    .map_err(model_error)?
    .with_antenna_pointing(pointing);
    let scale = "casars_legacy_pbw_w_scale";
    if record.get(scale).is_some() {
        let scale = record_number(record, scale)
            .ok_or_else(|| model_error("legacy PB/W recipe has an invalid W scale"))?;
        terms = terms.with_w_scale(scale).map_err(model_error)?;
    }
    Ok((terms, memory))
}

pub(super) fn reserve(phase: &'static str, memory: u64) -> Result<Reservation, GainSolveError> {
    let host = HostResources::detect().map_err(model_error)?;
    admit(&host, &ResourcePolicy::Balanced, &Demand { phase, memory }).map_err(model_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::imaging_memory_test::{isolated_test, live_since, peak_since, reset_peak};
    use casa_imaging_operator::{
        CellHold, ConvolutionFunctionSet, GridPadding, ImageExtent, RowContext,
    };
    use ndarray::{ArrayD, IxDyn};

    #[test]
    fn legacy_pbw_repacking_fits_its_charge() {
        if !isolated_test(module_path!(), "legacy_pbw_repacking_fits_its_charge") {
            return;
        }
        let grid = [256, 256];
        let scale = (15.0_f64 / 3600.).to_radians();
        let geometry = GridGeometry::new(
            ImageExtent {
                shape: grid,
                increment_rad: [-scale, scale],
                reference_pixel: [128; 2],
            },
            GridPadding::None,
        )
        .unwrap();
        let support = [3, 1, 2];
        let budget = LegacyPbWConvolution::resident_bytes(161, 2, 20, &support, grid);
        let baseline = reset_peak();
        let kernel = ArrayD::from_elem(
            IxDyn(&[161, 161, 2, support.len()]),
            Complex32::new(0.3, -0.2),
        );
        let taps = LegacyPbWConvolution::new(
            kernel,
            20,
            support.to_vec(),
            &geometry,
            4,
            &[Some(0), Some(3)],
            0.,
        )
        .unwrap();
        let mut first = CellHold::new();
        let mut second = CellHold::new();
        let mut row = RowContext {
            native_frequency_ratio: 1.,
            original_uvw_m: None,
            time_s: 0.,
            antennas: [0, 1],
            antenna_types: [0; 2],
            parallactic_angle_rad: [0.; 2],
            field: 0,
            spectral_window: 0,
        };
        let initial = taps.taps(taps.key(&row, 1.4e9, 0.), &mut first);
        let wavelengths = |plane: usize| (plane * plane) as f64 / (scale * support.len() as f64);
        for (plane, &half_support) in support.iter().enumerate() {
            let key = taps.key(&row, 1.4e9, wavelengths(plane));
            assert_eq!(usize::from(key.w_plane), plane);
            assert_eq!(
                taps.taps(key, &mut second).half_support(),
                [half_support; 2]
            );
        }
        row.parallactic_angle_rad = [0.5; 2];
        let rotated = taps.taps(taps.key(&row, 1.4e9, 0.), &mut second);
        assert_eq!(initial.half_support(), rotated.half_support());
        let peak = peak_since(baseline);
        assert!(peak <= budget, "{peak} bytes used over {budget}");
        assert!(live_since(baseline) < budget);
        assert_eq!(taps.max_half_support(), [3; 2]);
        assert!(taps.admits(500.));
        assert!(!taps.admits(-wavelengths(support.len())));
    }

    #[test]
    fn pbw_shape_covers_both_fine_offset_endpoints() {
        assert!(LegacyPbWConvolution::validate_shape(&[121, 121, 2, 1], 20, &[3], 2).is_err());
        assert!(LegacyPbWConvolution::validate_shape(&[9, 9, 2, 1], 3, &[1], 2).is_err());
        assert!(LegacyPbWConvolution::validate_shape(&[11, 11, 2, 1], 3, &[1], 2).is_ok());
    }

    #[test]
    fn shared_pbw_grids_hold_one_reservation_between_layouts() {
        use super::super::FourierGrids;
        use casa_core::Correlation;
        use casa_imaging_model::PolarizationCoordinate;
        use casa_imaging_operator::{
            Basis, Boxcar, GridPrecision, MeasurementOperator, PlaneRange,
        };
        use casa_imaging_runtime::reserved_memory;
        use std::sync::Arc;

        if !isolated_test(
            module_path!(),
            "shared_pbw_grids_hold_one_reservation_between_layouts",
        ) {
            return;
        }
        let geometry = GridGeometry::new(
            ImageExtent {
                shape: [32; 2],
                increment_rad: [-1e-4, 1e-4],
                reference_pixel: [16; 2],
            },
            GridPadding::None,
        )
        .unwrap();
        let routing = PolarizationRouting::from_saved_planes(
            &[Correlation::RR],
            &[PolarizationCoordinate::CircularRr],
        )
        .unwrap();
        let kernels = Box::new(Boxcar::new(&geometry, &routing));
        let operator = MeasurementOperator::new(
            geometry,
            Basis::Constant,
            routing,
            kernels,
            GridPrecision::F32,
        );
        let before = reserved_memory();
        let peak = (32 * 32 * size_of::<Complex32>()) as u64 * 5;
        let memory = reserve("test PB/W grid read", peak).unwrap();
        let grids = operator.empty_model_grids(PlaneRange::new(0, 1)).unwrap();
        let bytes = grids.bytes();
        let layout = Arc::new(FourierGrids::retaining(grids, Some(memory)));
        let second_layout = Arc::clone(&layout);
        assert_eq!(reserved_memory() - before, bytes);
        drop(layout);
        assert_eq!(reserved_memory() - before, bytes);
        drop(second_layout);
        assert_eq!(reserved_memory(), before);
    }
}
