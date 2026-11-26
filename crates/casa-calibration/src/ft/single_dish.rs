// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prepare a complex image-plane model for the legacy SDGrid predictor.

use std::path::Path;

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, StokesCoordinate,
};
use casa_core::measures::frequency::FrequencyRef;
use casa_core::{ArrayValue, Complex32, Correlation, RecordValue, ScalarValue, Value};
use casa_images::PagedImage;
use casa_imaging_operator::PolarizationRouting;
use casa_imaging_runtime::{Demand, HostResources, ResourcePolicy, admit};
use casa_imaging_singledish::{GridFunction, KernelParameters, KernelTable};
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::schema::ft_record::{FirstBuffer, GridFt, casa_spectral};
use casa_ms::{
    MeasurementSet, MsResult, ResolvedMsSelection, SubtableId,
    convert_frequency_to_frame_with_frame,
};
use casa_numerics::{BeamPattern, PrimaryBeam};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::image::{
    ImageModel, coordinate_index, first_correlations, grid_stokes, match_channels,
    polarization_coordinate, record_selection, selection_mask, validate_short_cubic,
};
use super::moving_source::first_imager_row;
use super::settings::{ImagerSingleDishKernel, PredictionGeometry};
use super::voltage_pattern::{
    PreparedVoltagePattern, legacy_radial_pattern, reserve_radial_lookup,
};
use super::{invalid, spectral_with_runtime};

pub(super) fn prepare_model(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    image: &PagedImage<f32>,
    kernel: ImagerSingleDishKernel,
    geometry: &PredictionGeometry,
    keep_zero: bool,
    scratch: bool,
) -> MsResult<Option<ImageModel>> {
    let context = &geometry.single_dish_setup;
    if !matches!(context.convert_first.as_str(), "NEVER" | "ALWAYS" | "AUTO") {
        return Err(invalid(format!(
            "illegal ConvertFirst name {}",
            context.convert_first
        )));
    }
    let coords = image.coordinates();
    let spatial = coordinate_index(coords, CoordinateType::Direction)?;
    let CoordinateModel::Direction(direction) = coords.coordinate(spatial) else {
        unreachable!("direction coordinate")
    };
    let cell = geometry
        .single_dish_cell_rad
        .unwrap_or(direction.increment()[1]);
    let parameters = context.kernel_parameters(kernel, cell)?;
    let custom_beam = if kernel == ImagerSingleDishKernel::PrimaryBeam {
        geometry
            .single_dish_voltage_table
            .as_ref()
            .map(|path| {
                let first = first_imager_row(ms, selected)?;
                let ScalarValue::Int32(array) = ms
                    .main_table()
                    .cell_accessor(first.row_index, "ARRAY_ID")?
                    .scalar()?
                else {
                    return Err(invalid("ARRAY_ID must be Int32"));
                };
                let telescope = ms
                    .observation()?
                    .string(usize::try_from(*array).map_err(invalid)?, "TELESCOPE_NAME")?;
                legacy_radial_pattern(path, &telescope)
            })
            .transpose()?
    } else {
        None
    };
    let convert_first = match context.convert_first.as_str() {
        "AUTO" => {
            let pointing_rows = ms
                .subtable(SubtableId::Pointing)
                .map_or(0, |table| table.row_count());
            if selected.selected_rows.len() > pointing_rows {
                "ALWAYS"
            } else {
                "NEVER"
            }
        }
        mode => mode,
    };
    validate_model(
        ms,
        selected,
        image,
        kernel,
        parameters,
        &context.pointing_column,
        custom_beam,
    )?;
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
        let (shape, nonzero) = write_planes(
            image,
            coordinates.clone(),
            &routing,
            path,
            geometry.voltage_pattern.as_ref(),
        )?;
        let mut record = model_record(
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
        let mut saved = RecordValue::default();
        saved.upsert(
            "convsupport",
            Value::Scalar(ScalarValue::Int64(parameters.convsupport)),
        );
        for (name, value) in [
            ("truncate", parameters.truncate),
            ("gwidth", parameters.gwidth),
            ("jwidth", parameters.jwidth),
        ] {
            saved.upsert(name, Value::Scalar(ScalarValue::Float64(value)));
        }
        record.upsert("casars_sd_parameters", Value::Record(saved));
        record.upsert(
            "pointingdircol",
            Value::Scalar(ScalarValue::String(context.pointing_column.clone())),
        );
        record.upsert(
            "casars_sd_convert_first",
            Value::Scalar(ScalarValue::String(convert_first.into())),
        );
        record.upsert(
            "casars_sd_cache_pointings",
            Value::Scalar(ScalarValue::Bool(context.cache_pointings)),
        );
        if let Some(beam) = custom_beam {
            record.upsert(
                "casars_sd_primary_beam",
                Value::Record(saved_radial_beam(beam)),
            );
        }
        Ok((record, nonzero))
    })
}

fn validate_model(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    image: &PagedImage<f32>,
    kernel: ImagerSingleDishKernel,
    parameters: KernelParameters,
    pointing_column: &str,
    custom_beam: Option<PrimaryBeam>,
) -> MsResult<()> {
    let [nx, ny, ..] = *image.shape() else {
        return Err(invalid("single-dish model needs direction axes"));
    };
    let function = kernel_function(kernel.name(), parameters)?;
    let support = match function.support() {
        Some(support) => support,
        None => {
            if ms.subtable(SubtableId::Pointing).is_some() {
                let pointing = ms.pointing()?;
                if pointing.row_count() != 0 {
                    let first = first_imager_row(ms, selected)?;
                    let ScalarValue::Float64(interval) = ms
                        .main_table()
                        .cell_accessor(first.row_index, "INTERVAL")?
                        .scalar()?
                    else {
                        return Err(invalid("MAIN INTERVAL must be a double"));
                    };
                    let coordinates = pointing.single_dish_directions(
                        &[(first.antenna1_id, first.time_mjd_seconds, *interval)],
                        pointing_column,
                        None,
                    )?;
                    if coordinates[0].is_none() {
                        return Err(invalid(
                            "single-dish PB prediction needs pointing information for the first visibility",
                        ));
                    }
                }
            }
            let coordinates = image.coordinates();
            let spatial = coordinate_index(coordinates, CoordinateType::Direction)?;
            let spectral = coordinate_index(coordinates, CoordinateType::Spectral)?;
            let CoordinateModel::Direction(direction) = coordinates.coordinate(spatial) else {
                unreachable!("direction coordinate")
            };
            let CoordinateModel::Spectral(axis) = coordinates.coordinate(spectral) else {
                unreachable!("spectral coordinate")
            };
            let frequency = spectral_with_runtime(axis)?
                .world_frequency_hz(0.)
                .map_err(invalid)?;
            let increment = direction.increment()[0];
            let host = HostResources::detect().map_err(invalid)?;
            let _pattern_memory = reserve_radial_lookup(&host)?;
            let beam = primary_beam(ms, frequency, custom_beam)?;
            let _memory = admit(
                &host,
                &ResourcePolicy::Balanced,
                &Demand {
                    phase: "legacy single-dish primary beam validation",
                    memory: function.forming_bytes(beam.support_pixels(increment, frequency)),
                },
            )
            .map_err(invalid)?;
            KernelTable::primary_beam(&beam, increment, frequency).support()
        }
    };
    validate_support([nx, ny], support)
}

pub(crate) fn kernel_function(name: &str, parameters: KernelParameters) -> MsResult<GridFunction> {
    if name.eq_ignore_ascii_case("SF") && parameters.convsupport == 0 {
        // A forward transform with no support produces a zero prediction.
        // The imaging gridding parser rejects this unusable weight kernel.
        return Ok(GridFunction::Spheroidal { support: 0 });
    }
    GridFunction::from_casa(name, parameters).map_err(invalid)
}

pub(crate) fn read_kernel_parameters(record: &RecordValue) -> MsResult<KernelParameters> {
    let Some(value) = record.get("casars_sd_parameters") else {
        if has_saved_setup(record) {
            return Err(invalid("single-dish recipe has no kernel parameters"));
        }
        return Ok(KernelParameters::DEFAULT);
    };
    let Value::Record(saved) = value else {
        return Err(invalid("invalid single-dish kernel parameters"));
    };
    let numeric = |name| match saved.get(name) {
        Some(Value::Scalar(ScalarValue::Float64(value))) if value.is_finite() => Ok(*value),
        _ => Err(invalid(format!("invalid single-dish kernel {name}"))),
    };
    let convsupport = match saved.get("convsupport") {
        Some(Value::Scalar(ScalarValue::Int64(value))) => *value,
        Some(Value::Scalar(ScalarValue::Int32(value))) => i64::from(*value),
        _ => return Err(invalid("invalid single-dish kernel convsupport")),
    };
    Ok(KernelParameters {
        convsupport,
        truncate: numeric("truncate")?,
        gwidth: numeric("gwidth")?,
        jwidth: numeric("jwidth")?,
    })
}

pub(crate) fn has_saved_setup(record: &RecordValue) -> bool {
    [
        "casars_sd_parameters",
        "casars_sd_convert_first",
        "casars_sd_cache_pointings",
    ]
    .iter()
    .any(|name| record.get(name).is_some())
}

pub(crate) fn validate_support([nx, ny]: [usize; 2], radius: usize) -> MsResult<()> {
    let width = radius.saturating_mul(2);
    if nx.min(ny) <= width {
        return Err(invalid(format!(
            "single-dish model axes must exceed the gridding support of {width} pixels"
        )));
    }
    Ok(())
}

pub(crate) fn primary_beam(
    ms: &MeasurementSet,
    frequency: f64,
    custom: Option<PrimaryBeam>,
) -> MsResult<BeamPattern> {
    if let Some(beam) = custom {
        return Ok(beam.pattern());
    }
    let telescope = ms.observation()?.string(0, "TELESCOPE_NAME")?;
    if let Some(beam) = PrimaryBeam::for_telescope(&telescope, frequency) {
        return Ok(beam.pattern());
    }
    BeamPattern::for_dish_diameter(ms.antenna()?.dish_diameter(0)?)
        .ok_or_else(|| invalid("no usable single-dish primary beam"))
}

fn saved_radial_beam(beam: PrimaryBeam) -> RecordValue {
    let mut record = RecordValue::default();
    let mut quantity = |key, number, unit: &str| {
        let mut value = RecordValue::default();
        value.upsert("value", Value::Scalar(ScalarValue::Float64(number)));
        value.upsert("unit", Value::Scalar(ScalarValue::String(unit.into())));
        record.upsert(key, Value::Record(value));
    };
    quantity("reffreq", 1., "GHz");
    let name = match beam {
        PrimaryBeam::Airy {
            dish_diameter_m,
            blockage_diameter_m,
            maximum_radius_arcmin_ghz,
        } => {
            quantity("dishdiam", dish_diameter_m, "m");
            quantity("blockagediam", blockage_diameter_m, "m");
            quantity("maxrad", maximum_radius_arcmin_ghz, "arcmin");
            "AIRY"
        }
        PrimaryBeam::Gaussian {
            half_width_arcmin_ghz,
            maximum_radius_arcmin_ghz,
            is_voltage,
        } => {
            quantity("halfwidth", half_width_arcmin_ghz, "arcmin");
            quantity("maxrad", maximum_radius_arcmin_ghz, "arcmin");
            record.upsert("isthisvp", Value::Scalar(ScalarValue::Bool(is_voltage)));
            "GAUSS"
        }
        _ => unreachable!("the custom single-dish loader returns a radial law"),
    };
    record.upsert("name", Value::Scalar(ScalarValue::String(name.into())));
    record
}

fn write_planes(
    source: &PagedImage<f32>,
    coordinates: CoordinateSystem,
    routing: &PolarizationRouting,
    path: &Path,
    voltage_pattern: Option<&PreparedVoltagePattern>,
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
            memory: write_bytes(source, [nx, ny, npol], voltage_pattern.is_some()),
        },
    )
    .map_err(invalid)?;
    let beam = voltage_pattern
        .map(|beam| {
            let coordinates = source.coordinates();
            let spatial = coordinate_index(coordinates, CoordinateType::Direction)?;
            let spectral = coordinate_index(coordinates, CoordinateType::Spectral)?;
            let CoordinateModel::Direction(direction) = coordinates.coordinate(spatial) else {
                unreachable!("direction coordinate");
            };
            let CoordinateModel::Spectral(spectral) = coordinates.coordinate(spectral) else {
                unreachable!("spectral coordinate");
            };
            Ok::<_, casa_ms::MsError>((
                beam.pattern(),
                beam.separations(direction, [nx, ny])?,
                spectral_with_runtime(spectral)?,
            ))
        })
        .transpose()?;
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
        let mut values = source
            .get_slice(&[0, 0, 0, channel], &[nx, ny, npol, 1])
            .map_err(invalid)?;
        nonzero |= values.iter().any(|&pixel| pixel != 0.);
        if let Some((beam, separations, spectral)) = &beam {
            let frequency = spectral
                .world_frequency_hz(channel as f64)
                .map_err(invalid)?;
            let pattern = beam.at_frequency(frequency);
            let scale = 60. * frequency / 1e9;
            for ((y, x), separation) in separations.indexed_iter() {
                let gain = pattern.power(separation.to_degrees() * scale);
                for pol in 0..npol {
                    values[[x, y, pol, 0]] *= gain;
                }
            }
        }
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

fn write_bytes(source: &PagedImage<f32>, [nx, ny, input_pols]: [usize; 3], with_beam: bool) -> u64 {
    // Source Stokes planes, two sets of complex plane cells for the output
    // and its accumulation, and the output's one-plane tile cache.
    let bytes_per_pixel = input_pols.saturating_mul(size_of::<f32>())
        + 3 * size_of::<Complex32>()
        + usize::from(with_beam) * size_of::<f64>();
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
    let axis = if geometry.frequency_frame_valid {
        spectral_with_runtime(spectral)?.into_owned()
    } else {
        spectral.clone().without_conversion()
    };
    let frequencies = (0..channels)
        .map(|channel| axis.world_frequency_hz(channel as f64).map_err(invalid))
        .collect::<MsResult<Vec<_>>>()?;
    let first = first_imager_row(ms, selected)?;
    let windows = ms.spectral_window()?;
    let spw = first.spectral_window_id as usize;
    let mut buffer_frequencies = windows.chan_freq(spw)?;
    let engine = MsCalEngine::new(ms)?;
    if geometry.frequency_frame_valid {
        let frame =
            engine.spectral_frame_observatory(first.time_mjd_seconds, first.field_id as usize)?;
        let ratio = convert_frequency_to_frame_with_frame(
            windows.frequency_reference(spw)?,
            FrequencyRef::LSRK,
            1.,
            Some(&frame),
        )?;
        for frequency in &mut buffer_frequencies {
            *frequency *= ratio;
        }
    }
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
    record.upsert(
        "spwchanselflag",
        Value::Array(ArrayValue::Int32(selection_mask(ms, selected)?)),
    );
    if channels > 1 && geometry.settings.frequency_interpolation != 0 {
        let input_axis = spectral_with_runtime(spectral)?;
        let input = (0..channels)
            .map(|channel| {
                input_axis
                    .world_frequency_hz(channel as f64)
                    .map_err(invalid)
            })
            .collect::<MsResult<Vec<_>>>()?;
        if geometry.settings.frequency_interpolation == 2 && channels < 4 {
            validate_short_cubic(ms, selected, &frequencies, geometry.frequency_frame_valid)?;
        }
        record.upsert(
            "casars_live_image_frequency",
            Value::Array(ArrayValue::from_f64_vec(input)),
        );
    }
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
        let beam = PreparedVoltagePattern::test_pattern();
        let baseline = reset_peak();
        let (shape, nonzero) =
            write_planes(&image, output_coords, &routing, &output_path, Some(&beam)).unwrap();
        let measured = peak_since(baseline);
        assert!(nonzero);
        assert_eq!(shape, [nx, ny, pols, channels]);
        let charge = write_bytes(&image, [nx, ny, pols], true);
        assert!(measured <= charge, "{measured} bytes used over {charge}");
        let output = PagedImage::<Complex32>::open(output_path).unwrap();
        assert_eq!(
            output.get().unwrap()[[nx / 2, ny / 2, 0, 2]],
            Complex32::new(1., 0.)
        );
    }
}
