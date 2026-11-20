// SPDX-License-Identifier: LGPL-3.0-or-later
//! Publish the Fourier image and its matching legacy nPBWProjectFT taps.

pub(crate) mod pointing;

use std::path::Path;

use casa_coordinates::{CoordinateModel, CoordinateSystem, DirectionCoordinate, StokesType};
use casa_images::PagedImage;
use casa_imaging_operator::{LegacyEpConvolution, LegacyPbWConvolution};
use casa_imaging_runtime::HostResources;
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::virtual_model::ft_record::{FirstBuffer, GridFt, casa_spectral};
use casa_ms::{MeasurementSet, MsResult, ResolvedMsSelection};
use casa_types::{ArrayValue, Complex32, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use crate::calibrater_lifecycle::{
    ep_cache::EpCache,
    legacy_ep::{convolution, first_buffer, model_geometry, reserve_model},
};

use super::image::{ImageModel, match_channels, selection_mask};
use super::moving_source::first_imager_row;
use super::settings::{ImagerLegacyPbWOptions, PredictionGeometry};
use super::{invalid, spectral_with_runtime};
use pointing::LegacyEpOffsets;

pub(crate) const KERNEL_IMAGE: &str = "CASARS_LEGACY_PBW_KERNEL";

pub(super) fn prepare_model(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    model: &PagedImage<f32>,
    geometry: &PredictionGeometry,
    options: (&ImagerLegacyPbWOptions, Option<&LegacyEpOffsets>),
    keep_zero: bool,
    scratch: bool,
) -> MsResult<Option<ImageModel>> {
    let (options, pointing) = options;
    if options.w_planes > 1 {
        return Err(invalid(
            "this legacy pbwproject configuration is pending #1520",
        ));
    }
    if options.mosaic
        && let Some(first) = selected.selected_rows.first()
        && selected
            .selected_rows
            .iter()
            .any(|row| row.field_id != first.field_id)
    {
        return Err(invalid(
            "legacy pbmosaic prediction with several fields is pending #1520",
        ));
    }
    let (shape, increment, coordinates) = model_geometry(model)?;
    let [side, _, _, _] = shape;
    let host = HostResources::detect().map_err(invalid)?;
    let _memory = reserve_model(&host, side)?;
    let data = model.get().map_err(invalid)?;
    if data.iter().any(|sample| !sample.is_finite()) {
        return Err(invalid(
            "legacy PB/W model contains a non-finite image cell",
        ));
    }
    let nonzero = data.iter().any(|&sample| sample != 0.);
    let cache = EpCache::open(&options.cache_directory)?;
    let buffer = first_buffer(ms, selected)?;
    let products = convolution(&cache, 360., &host, &buffer, shape, increment, &coordinates)?;
    let support = u16::try_from(products.support).map_err(invalid)?;
    let sampling = u16::try_from(products.sampling).map_err(invalid)?;
    LegacyPbWConvolution::validate_shape(
        products.convolution.shape(),
        sampling,
        support,
        buffer.correlations.len(),
    )
    .map_err(invalid)?;
    let angle_rad = buffer.angle_rad;
    let cutoff = options
        .correct_primary_beam
        .then_some(options.primary_beam_limit as f32);
    let fourier = LegacyEpConvolution::prepare_pbw_model(
        &data,
        &products.average_beam,
        products.sampling,
        cutoff,
    )
    .map_err(invalid)?;
    LegacyFourierImage {
        shape,
        increment,
        coordinates,
        correlations: buffer.correlations,
        fourier,
        nonzero,
    }
    .publish(
        ms,
        selected,
        geometry,
        keep_zero,
        scratch,
        |path, record| {
            let mut kernel = PagedImage::<Complex32>::create(
                products.convolution.shape().to_vec(),
                products.convolution_coordinates,
                path.join(KERNEL_IMAGE),
            )
            .map_err(invalid)?;
            kernel
                .put_slice(&products.convolution, &[0; 4])
                .map_err(invalid)?;
            kernel.save().map_err(invalid)?;
            let machine = if options.mosaic {
                "PBMosaicFT"
            } else {
                "nPBWProjectFT"
            };
            record.upsert("name", Value::Scalar(ScalarValue::String(machine.into())));
            record.upsert(
                "casars_legacy_pbw_support",
                Value::Scalar(ScalarValue::Int32(i32::from(support))),
            );
            record.upsert(
                "casars_legacy_pbw_sampling",
                Value::Scalar(ScalarValue::Int32(i32::from(sampling))),
            );
            record.upsert(
                "casars_legacy_pbw_angle",
                Value::Scalar(ScalarValue::Float64(angle_rad)),
            );
            if let Some(pointing) = pointing {
                pointing.save(record);
            }
            Ok(())
        },
    )
}

pub(super) struct LegacyFourierImage {
    pub(super) shape: [usize; 4],
    pub(super) increment: [f64; 2],
    pub(super) coordinates: CoordinateSystem,
    pub(super) correlations: Vec<StokesType>,
    pub(super) fourier: [Vec<Complex32>; 4],
    pub(super) nonzero: bool,
}

impl LegacyFourierImage {
    pub(super) fn publish(
        self,
        ms: &MeasurementSet,
        selected: &ResolvedMsSelection,
        geometry: &PredictionGeometry,
        keep_zero: bool,
        scratch: bool,
        finish: impl FnOnce(&Path, &mut RecordValue) -> MsResult<()>,
    ) -> MsResult<Option<ImageModel>> {
        let Self {
            shape,
            increment,
            coordinates,
            correlations,
            fourier,
            nonzero,
        } = self;
        let [side, _, _, _] = shape;
        let CoordinateModel::Direction(direction) = coordinates.coordinate(0) else {
            unreachable!("validated legacy PB/W direction");
        };
        let CoordinateModel::Spectral(spectral) = coordinates.coordinate(2) else {
            unreachable!("validated legacy PB/W frequency");
        };
        let spectral = if geometry.frequency_frame_valid {
            spectral_with_runtime(spectral)?.into_owned()
        } else {
            spectral.clone().without_conversion()
        };
        let frequencies = [spectral.world_frequency_hz(0.).map_err(invalid)?];
        let first = first_imager_row(ms, selected)?;
        let band = ms
            .spectral_window()?
            .chan_freq(first.spectral_window_id as usize)?;
        let first = FirstBuffer {
            field: -1,
            channel_map: match_channels(&spectral, 1, &band),
            frequencies_lsrk_hz: band,
            correlations: correlations.iter().map(|&pol| pol as i32).collect(),
            polarization_map: correlations.iter().map(|&pol| pol as i32 - 5).collect(),
        };
        let engine = MsCalEngine::new(ms)?;
        let (longitude, latitude, radius) = engine.observatory_position().as_spherical();
        ImageModel::prepare(ms, selected, keep_zero, scratch, |path| {
            let mut image =
                PagedImage::<Complex32>::create(shape.to_vec(), coordinates.clone(), path)
                    .map_err(invalid)?;
            for (pol, plane) in fourier.into_iter().enumerate() {
                let pixels = ArrayD::from_shape_vec(IxDyn(&[side, side, 1, 1]).f(), plane)
                    .map_err(invalid)?;
                image.put_slice(&pixels, &[0, 0, pol, 0]).map_err(invalid)?;
            }
            image.save().map_err(invalid)?;
            let mut record = GridFt {
                disk_image: path,
                grid: [side; 2],
                polarizations: 4,
                image_frequencies_hz: &frequencies,
                increment_rad: increment,
                phase_centre_rad: image_centre(direction, side)?,
                direction_frame: direction.direction_ref().as_str(),
                observatory: geometry
                    .settings
                    .observatory
                    .unwrap_or([longitude, latitude, radius]),
                spectral: casa_spectral(&CoordinateModel::Spectral(Box::new(spectral)).to_record()),
                buffer: &first,
                interpolation: 0,
                frequency_frame_valid: geometry.frequency_frame_valid,
            }
            .record();
            record.upsert(
                "spwchanselflag",
                Value::Array(ArrayValue::Int32(selection_mask(ms, selected)?)),
            );
            finish(path, &mut record)?;
            Ok((record, nonzero))
        })
    }
}

fn image_centre(direction: &DirectionCoordinate, side: usize) -> MsResult<[f64; 2]> {
    direction
        .pixel_to_world([(side / 2) as f64; 2])
        .map_err(invalid)
}
