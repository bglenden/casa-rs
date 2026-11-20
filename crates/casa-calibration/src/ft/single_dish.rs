// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prepare a complex image-plane model for the legacy SDGrid predictor.

use std::path::Path;

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, StokesCoordinate,
};
use casa_images::PagedImage;
use casa_imaging_operator::PolarizationRouting;
use casa_imaging_runtime::{Demand, HostResources, ResourcePolicy, admit};
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::virtual_model::ft_record::{FirstBuffer, GridFt, casa_spectral};
use casa_ms::{MeasurementSet, MsResult, ResolvedMsSelection};
use casa_types::{Complex32, Correlation, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::image::{
    ImageModel, coordinate_index, first_correlations, grid_stokes, match_channels,
    polarization_coordinate, record_selection,
};
use super::invalid;
use super::settings::{ImagerSingleDishKernel, PredictionGeometry};

pub(super) fn prepare_model(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    path: &Path,
    kernel: ImagerSingleDishKernel,
    geometry: &PredictionGeometry,
    keep_zero: bool,
    scratch: bool,
) -> MsResult<Option<ImageModel>> {
    let image = PagedImage::<f32>::open(path).map_err(invalid)?;
    let mut coordinates = image.coordinates().clone();
    let pol = coordinate_index(&coordinates, CoordinateType::Stokes)?;
    let CoordinateModel::Stokes(axis) = coordinates.coordinate(pol) else {
        unreachable!("Stokes coordinate index");
    };
    let selected_correlations = first_correlations(ms, selected)?;
    let correlations = selected_correlations
        .iter()
        .map(|&code| {
            Correlation::from_casacore_code(code).ok_or_else(|| invalid("invalid correlation"))
        })
        .collect::<MsResult<Vec<_>>>()?;
    let planes = axis
        .stokes()
        .iter()
        .map(|&code| {
            polarization_coordinate(code)
                .ok_or_else(|| invalid("unsupported single-dish model polarization"))
        })
        .collect::<MsResult<Vec<_>>>()?;
    let routing = PolarizationRouting::compile(&correlations, &planes).map_err(invalid)?;
    let grid_axis = StokesCoordinate::new(
        routing
            .grid()
            .iter()
            .map(grid_stokes)
            .collect::<MsResult<_>>()?,
    );
    coordinates.replace_coordinate(pol, grid_axis);
    ImageModel::prepare(ms, selected, keep_zero, scratch, |path| {
        let (shape, nonzero) = write_planes(&image, coordinates.clone(), &routing, path)?;
        let record = model_record(
            ms,
            selected,
            &coordinates,
            path,
            shape,
            kernel,
            geometry,
            selected_correlations,
            &routing,
        )?;
        Ok((record, nonzero))
    })
}

fn write_planes(
    source: &PagedImage<f32>,
    coordinates: CoordinateSystem,
    routing: &PolarizationRouting,
    path: &Path,
) -> MsResult<([usize; 4], bool)> {
    let [nx, ny, npol, channels] = *source.shape() else {
        return Err(invalid("single-dish ft needs four model image axes"));
    };
    if npol != routing.requested().len() {
        return Err(invalid(
            "single-dish image Stokes axis does not match its planes",
        ));
    }
    let host = HostResources::detect().map_err(invalid)?;
    let _memory = admit(
        &host,
        &ResourcePolicy::Balanced,
        &Demand {
            phase: "legacy single-dish model image preparation",
            memory: write_bytes(source, [nx, ny, npol]),
        },
    )
    .map_err(invalid)?;
    let shape = [nx, ny, routing.grid_pols(), channels];
    let mut output = PagedImage::<Complex32>::create_with_tile_shape_and_cache(
        shape.to_vec(),
        vec![nx.min(128), ny.min(128), 1, 1],
        coordinates,
        path,
        nx.saturating_mul(ny).saturating_mul(size_of::<Complex32>()),
    )
    .map_err(invalid)?;
    let mut nonzero = false;
    for channel in 0..channels {
        let values = source
            .get_slice(&[0, 0, 0, channel], &[nx, ny, npol, 1])
            .map_err(invalid)?;
        nonzero |= values.iter().any(|&pixel| pixel != 0.);
        for pol in 0..routing.grid_pols() {
            let coefficients: Vec<Complex32> = (0..npol)
                .map(|input| {
                    let coefficient = routing.from_requested(pol, input);
                    Complex32::new(coefficient.re as f32, coefficient.im as f32)
                })
                .collect();
            let plane = ArrayD::from_shape_fn(IxDyn(&[nx, ny, 1, 1]).f(), |pixel| {
                coefficients
                    .iter()
                    .copied()
                    .enumerate()
                    .map(|(input, coefficient)| {
                        coefficient * values[[pixel[0], pixel[1], input, 0]]
                    })
                    .sum()
            });
            output
                .put_slice(&plane, &[0, 0, pol, channel])
                .map_err(invalid)?;
        }
    }
    output.save().map_err(invalid)?;
    Ok((shape, nonzero))
}

fn write_bytes(source: &PagedImage<f32>, [nx, ny, input_pols]: [usize; 3]) -> u64 {
    // Source Stokes planes, two sets of complex plane cells for the output
    // and its accumulation, and the output's one-plane tile cache.
    let bytes_per_pixel = input_pols.saturating_mul(size_of::<f32>()) + 3 * size_of::<Complex32>();
    let tiled_bytes = source
        .shape()
        .iter()
        .zip(source.tile_shape())
        .fold(size_of::<f32>(), |bytes, (&size, &tile)| {
            bytes.saturating_mul(size.div_ceil(tile).saturating_mul(tile))
        });
    let source_cache = tiled_bytes.min(casa_tables::DEFAULT_TILED_ARRAY_CACHE_BYTES);
    (nx as u64)
        .saturating_mul(ny as u64)
        .saturating_mul(bytes_per_pixel as u64)
        .saturating_add(source_cache as u64)
        .saturating_add(65_536)
}

#[allow(clippy::too_many_arguments)]
fn model_record(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    coordinates: &CoordinateSystem,
    path: &Path,
    [nx, ny, npol, channels]: [usize; 4],
    kernel: ImagerSingleDishKernel,
    geometry: &PredictionGeometry,
    correlations: Vec<i32>,
    routing: &PolarizationRouting,
) -> MsResult<RecordValue> {
    let find =
        |kind| coordinate_index(coordinates, kind).map(|index| coordinates.coordinate(index));
    let CoordinateModel::Direction(direction) = find(CoordinateType::Direction)? else {
        unreachable!("direction coordinate index");
    };
    let CoordinateModel::Spectral(spectral) = find(CoordinateType::Spectral)? else {
        unreachable!("spectral coordinate index");
    };
    let axis = spectral.clone().without_conversion();
    let frequencies = (0..channels)
        .map(|channel| axis.world_frequency_hz(channel as f64).map_err(invalid))
        .collect::<MsResult<Vec<_>>>()?;
    let buffer_frequencies = ms
        .spectral_window()?
        .chan_freq(selected.selected_rows[0].spectral_window_id as usize)?;
    let buffer = FirstBuffer {
        field: -1,
        channel_map: match_channels(&axis, channels, &buffer_frequencies),
        frequencies_lsrk_hz: buffer_frequencies,
        correlations,
        polarization_map: routing
            .pol_map()
            .iter()
            .map(|pol| pol.map_or(-1, i32::from))
            .collect(),
    };
    let engine = MsCalEngine::new(ms)?;
    let (longitude, latitude, radius) = engine.observatory_position().as_spherical();
    let mut record = GridFt {
        disk_image: path,
        grid: [nx, ny],
        polarizations: npol,
        image_frequencies_hz: &frequencies,
        increment_rad: [direction.increment()[0], direction.increment()[1]],
        phase_centre_rad: direction
            .pixel_to_world([nx as f64 / 2., ny as f64 / 2.])
            .map_err(invalid)?,
        direction_frame: direction.direction_ref().as_str(),
        observatory: geometry
            .settings
            .observatory
            .unwrap_or([longitude, latitude, radius]),
        spectral: casa_spectral(&CoordinateModel::Spectral(Box::new(axis.clone())).to_record()),
        buffer: &buffer,
        interpolation: geometry.settings.frequency_interpolation,
        frequency_frame_valid: geometry.frequency_frame_valid,
    }
    .record();
    record.upsert("name", Value::Scalar(ScalarValue::String("SDGrid".into())));
    record.upsert(
        "casars_sd_kernel",
        Value::Scalar(ScalarValue::String(kernel.name().into())),
    );
    record.upsert("douvwrotation", Value::Scalar(ScalarValue::Bool(false)));
    record.upsert("usedoublegrid", Value::Scalar(ScalarValue::Bool(false)));
    record.upsert(
        "numthreads",
        Value::Scalar(ScalarValue::Int32(geometry.settings.threads)),
    );
    record_selection(&mut record, ms, selected, &axis, channels)?;
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::imaging_memory_test::{cube_coordinates, isolated_test, peak_since, reset_peak};
    use casa_coordinates::StokesType;
    use casa_imaging_model::PolarizationCoordinate;

    #[test]
    fn complex_model_writer_holds_at_most_its_charge() {
        if !isolated_test(
            module_path!(),
            "complex_model_writer_holds_at_most_its_charge",
        ) {
            return;
        }
        let scratch = tempfile::tempdir().unwrap();
        let source_path = scratch.path().join("real.im");
        let [nx, ny, pols, channels] = [128, 96, 2, 3];
        let source_coords = cube_coordinates(vec![StokesType::I, StokesType::Q]);
        let mut image =
            PagedImage::<f32>::create(vec![nx, ny, pols, channels], source_coords, &source_path)
                .unwrap();
        image
            .put_slice(
                &ArrayD::from_elem(IxDyn(&[nx, ny, pols, channels]), 0.5),
                &[0, 0, 0, 0],
            )
            .unwrap();
        image.save().unwrap();
        drop(image);
        let image = PagedImage::open(source_path).unwrap();
        let routing = PolarizationRouting::compile(
            &[
                Correlation::from_casacore_code(9).unwrap(),
                Correlation::from_casacore_code(12).unwrap(),
            ],
            &[
                PolarizationCoordinate::StokesI,
                PolarizationCoordinate::StokesQ,
            ],
        )
        .unwrap();
        let output_coords = cube_coordinates(vec![StokesType::XX, StokesType::YY]);
        let output_path = scratch.path().join("complex.im");
        let baseline = reset_peak();
        let (shape, nonzero) = write_planes(&image, output_coords, &routing, &output_path).unwrap();
        let measured = peak_since(baseline);
        assert!(nonzero);
        assert_eq!(shape, [nx, ny, pols, channels]);
        let charge = write_bytes(&image, [nx, ny, pols]);
        assert!(measured <= charge, "{measured} bytes used over {charge}");
        let output = PagedImage::<Complex32>::open(output_path).unwrap();
        assert_eq!(output.get().unwrap()[[0, 0, 0, 2]], Complex32::new(1., 0.));
    }
}
