// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sample a legacy SDGrid model at each autocorrelation's pointing.

mod spectral;

use std::sync::Mutex;

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateType, DirectionCoordinate, SpectralCoordinate,
    StokesCoordinate,
};
use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::record::direction_from_record;
use casa_core::{ArrayValue, Complex32, Correlation, RecordValue, ScalarValue, Value};
use casa_images::PagedImage;
use casa_imaging_operator::PolarizationRouting;
use casa_imaging_runtime::{Demand, HostResources, Reservation, ResourcePolicy, admit};
use casa_imaging_singledish::{GridFunction, KernelTable};
use casa_ms::{MeasurementSet, voltage_pattern};
use casa_numerics::BeamPattern;
use ndarray::{ArrayD, Axis, Ix2};

use crate::ft::image::{coordinate_index, match_channel, polarization_coordinate};
use crate::ft::single_dish::{
    has_saved_setup, kernel_function, primary_beam, read_kernel_parameters, validate_support,
};

use super::fourier::{
    FourierRow, complex_model_image_path, lsrk_frequency_ratio, scratch_channel_masked,
};
use super::{GainSolveError, model_error, read_error};
use spectral::ImageSpectrum;

const IMAGE_METADATA_BYTES: u64 = 65_536;

pub(super) struct SingleDishModel {
    record: RecordValue,
    pointing: PointingQuery,
    prepared: Mutex<Option<PreparedModel>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct PointingQuery {
    pub(super) column: String,
    pub(super) convert_first: Option<DirectionRef>,
}

struct PreparedModel {
    direction: DirectionCoordinate,
    spectral: SpectralCoordinate,
    stokes: StokesCoordinate,
    cells: ArrayD<Complex32>,
    kernel: KernelTable,
    pixel: Option<[f64; 2]>,
    sampled: Vec<Option<Complex32>>,
    interpolated: Option<ImageSpectrum>,
    convert_frequencies: bool,
    cache_pointings: bool,
    cached_pointing: Option<(PointingFrame, Option<[f64; 2]>)>,
    _memory: Reservation,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct PointingFrame {
    antenna: i32,
    time: u64,
    reference: DirectionRef,
    position: [u64; 2],
}

impl SingleDishModel {
    pub(super) fn new(record: RecordValue) -> Result<Self, GainSolveError> {
        let explicit = has_saved_setup(&record);
        let name = |field, default| match record.get(field) {
            Some(Value::Scalar(ScalarValue::String(name))) => Ok(name.as_str()),
            None if !explicit => Ok(default),
            _ => Err(model_error(format!(
                "invalid single-dish recipe field {field}"
            ))),
        };
        let convert_first = match name("casars_sd_convert_first", "NEVER")? {
            "NEVER" => None,
            "ALWAYS" => {
                let Some(Value::Record(direction)) = record.get("mimage_rec") else {
                    return Err(model_error("SDGrid recipe has no image direction"));
                };
                Some(
                    direction_from_record(direction)
                        .map_err(model_error)?
                        .refer(),
                )
            }
            mode => return Err(model_error(format!("illegal ConvertFirst name {mode}"))),
        };
        let column = name("pointingdircol", "DIRECTION")?;
        if !matches!(
            column,
            "DIRECTION" | "TARGET" | "ENCODER" | "POINTING_OFFSET" | "SOURCE_OFFSET"
        ) {
            return Err(model_error(format!(
                "unknown single-dish POINTING column {column}"
            )));
        }
        read_kernel_parameters(&record).map_err(model_error)?;
        recipe_cache(&record)?;
        let pointing = PointingQuery {
            column: column.to_owned(),
            convert_first,
        };
        Ok(Self {
            record,
            pointing,
            prepared: Mutex::default(),
        })
    }

    pub(super) fn pointing(&self) -> &PointingQuery {
        &self.pointing
    }

    pub(super) fn predict(
        &self,
        row: &FourierRow<'_>,
        pointing: &MDirection,
        output: &mut ArrayD<Complex32>,
    ) -> Result<(), GainSolveError> {
        let mut prepared = self.prepared.lock().expect("single-dish model preparation");
        if prepared.is_none() {
            *prepared = Some(PreparedModel::load(&self.record, row.ms)?);
        }
        let selection_mask = match self.record.get("spwchanselflag") {
            Some(Value::Array(ArrayValue::Int32(mask))) => Some(mask),
            _ => None,
        };
        prepared
            .as_mut()
            .expect("the single-dish image is loaded")
            .predict(row, pointing, selection_mask, output)
    }
}

impl PreparedModel {
    fn load(record: &RecordValue, ms: &MeasurementSet) -> Result<Self, GainSolveError> {
        let name = match record.get("casars_sd_kernel") {
            Some(Value::Scalar(ScalarValue::String(name))) => name.as_str(),
            _ => {
                return Err(model_error(
                    "SDGrid virtual recipe has no single-dish convolution settings, which CASA 6.7.6.14 omits",
                ));
            }
        };
        let parameters = read_kernel_parameters(record).map_err(model_error)?;
        let function = kernel_function(name, parameters).map_err(model_error)?;
        let host = HostResources::detect().map_err(model_error)?;
        let _opening = admit(
            &host,
            &ResourcePolicy::Balanced,
            &Demand {
                phase: "open legacy single-dish model",
                memory: IMAGE_METADATA_BYTES
                    + 128 * 128 * size_of::<Complex32>() as u64
                    + if matches!(function, GridFunction::PrimaryBeam) {
                        BeamPattern::BYTES
                    } else {
                        0
                    },
            },
        )
        .map_err(model_error)?;
        // The image is read once and its pixels stay resident. One tile is
        // sufficient for reading ragged tiles into that retained array.
        let image =
            PagedImage::<Complex32>::open_with_cache(complex_model_image_path(record, ms)?, 1)
                .map_err(model_error)?;
        let [nx, ny, pols, channels] = *image.shape() else {
            return Err(model_error("SDGrid needs a four-axis complex image"));
        };
        if let Some(radius) = function.support() {
            validate_support([nx, ny], radius).map_err(model_error)?;
        }
        let coordinates = image.coordinates();
        let find = |kind| {
            coordinate_index(coordinates, kind)
                .map(|index| coordinates.coordinate(index))
                .map_err(model_error)
        };
        let CoordinateModel::Direction(direction) = find(CoordinateType::Direction)? else {
            unreachable!("direction coordinate index");
        };
        let CoordinateModel::Spectral(spectral) = find(CoordinateType::Spectral)? else {
            unreachable!("spectral coordinate index");
        };
        let CoordinateModel::Stokes(stokes) = find(CoordinateType::Stokes)? else {
            unreachable!("Stokes coordinate index");
        };
        let convert_frequencies = matches!(
            record.get("freqframevalid"),
            Some(Value::Scalar(ScalarValue::Bool(true)))
        );
        let increment = direction.increment()[0];
        let beam = if matches!(function, GridFunction::PrimaryBeam) {
            // SDGrid forms its PB in a one-row copy of the image coordinates.
            // Its reporting frame can differ from the channel matching frame.
            let frequency = crate::ft::spectral_with_runtime(spectral)
                .map_err(model_error)?
                .world_frequency_hz(0.)
                .map_err(model_error)?;
            let custom = match record.get("casars_sd_primary_beam") {
                None => None,
                Some(Value::Record(beam)) => {
                    Some(voltage_pattern::radial_pattern(beam).map_err(model_error)?)
                }
                _ => return Err(model_error("invalid saved single-dish primary beam")),
            };
            let beam = primary_beam(ms, frequency, custom).map_err(model_error)?;
            Some((beam, frequency))
        } else {
            None
        };
        let spectral = if convert_frequencies {
            crate::ft::spectral_with_runtime(spectral)
                .map_err(model_error)?
                .into_owned()
        } else {
            spectral.clone().without_conversion()
        };
        let beam_pixels = beam
            .as_ref()
            .map_or(0, |(beam, freq)| beam.support_pixels(increment, *freq));
        let image_bytes = (nx as u64)
            .saturating_mul(ny as u64)
            .saturating_mul(pols as u64)
            .saturating_mul(channels as u64)
            .saturating_mul(size_of::<Complex32>() as u64);
        let tile_bytes = image
            .tile_shape()
            .iter()
            .fold(size_of::<Complex32>() as u64, |size, &axis| {
                size.saturating_mul(axis as u64)
            });
        let sample_count = channels.saturating_mul(pols);
        let sample_bytes =
            (sample_count as u64).saturating_mul(size_of::<Option<Complex32>>() as u64);
        let mut memory = admit(
            &host,
            &ResourcePolicy::Balanced,
            &Demand {
                phase: "legacy single-dish model image",
                memory: read_bytes(
                    image_bytes,
                    tile_bytes,
                    sample_bytes,
                    function.forming_bytes(beam_pixels),
                ),
            },
        )
        .map_err(model_error)?;
        let cells = image.get().map_err(model_error)?;
        let kernel = match beam {
            Some((beam, frequency)) => KernelTable::primary_beam(&beam, increment, frequency),
            None => KernelTable::new(function).map_err(model_error)?,
        };
        validate_support([nx, ny], kernel.support()).map_err(model_error)?;
        let direction = direction.clone();
        let stokes = stokes.clone();
        drop(image);
        let interpolated = ImageSpectrum::load(record, &spectral, channels, ms, &host)?;
        memory.retain(
            image_bytes
                .saturating_add(sample_bytes)
                .saturating_add(kernel.bytes())
                .saturating_add(IMAGE_METADATA_BYTES),
        );
        Ok(Self {
            direction,
            spectral,
            stokes,
            cells,
            kernel,
            pixel: None,
            sampled: vec![None; sample_count],
            interpolated,
            convert_frequencies,
            cache_pointings: recipe_cache(record)?,
            cached_pointing: None,
            _memory: memory,
        })
    }

    fn pointing_pixel(
        &mut self,
        row: &FourierRow<'_>,
        pointing: &MDirection,
    ) -> Result<Option<[f64; 2]>, GainSolveError> {
        let (longitude, latitude) = pointing.as_angles();
        let key = PointingFrame {
            antenna: row.row.antenna1,
            time: row.row.time_seconds.to_bits(),
            reference: pointing.refer(),
            position: [longitude.to_bits(), latitude.to_bits()],
        };
        if self.cache_pointings
            && let Some((prior, pixel)) = self.cached_pointing
            && prior == key
        {
            return Ok(pixel);
        }
        let coordinates = if pointing.refer() == self.direction.direction_ref() {
            [longitude, latitude]
        } else {
            let frame = row
                .engine
                .antenna_frame(row.row.time_seconds, row.row.antenna1 as usize)
                .map_err(|error| read_error(row.ms, error))?;
            let direction = pointing
                .convert_to(self.direction.direction_ref(), &frame)
                .map_err(model_error)?;
            let (longitude, latitude) = direction.as_angles();
            [longitude, latitude]
        };
        let pixel = self.direction.world_to_pixel(coordinates).ok();
        if self.cache_pointings {
            self.cached_pointing = Some((key, pixel));
        }
        Ok(pixel)
    }

    fn predict(
        &mut self,
        row: &FourierRow<'_>,
        pointing: &MDirection,
        selection_mask: Option<&ArrayD<i32>>,
        output: &mut ArrayD<Complex32>,
    ) -> Result<(), GainSolveError> {
        let Some(pixel) = self.pointing_pixel(row, pointing)? else {
            return Ok(());
        };
        if self.pixel != Some(pixel) {
            self.pixel = Some(pixel);
            self.sampled.fill(None);
        }
        let correlations = row
            .correlations
            .iter()
            .map(|&code| {
                Correlation::from_casacore_code(code)
                    .ok_or_else(|| model_error("invalid SDGrid correlation"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let planes = self
            .stokes
            .stokes()
            .iter()
            .map(|&code| {
                polarization_coordinate(code)
                    .ok_or_else(|| model_error("unsupported SDGrid image polarization"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let routing =
            PolarizationRouting::from_saved_planes(&correlations, &planes).map_err(model_error)?;
        let ratio = if self.convert_frequencies {
            lsrk_frequency_ratio(row)?
        } else {
            1.
        };
        let sampled = &mut self.sampled;
        let cells = &self.cells;
        let kernel = &self.kernel;
        let mut sample = |plane: usize, pol: usize| {
            *sampled[plane * cells.shape()[2] + pol].get_or_insert_with(|| {
                let spectra = cells.index_axis(Axis(3), plane);
                let pixels = spectra
                    .index_axis(Axis(2), pol)
                    .into_dimensionality::<Ix2>()
                    .expect("individual SDGrid model plane");
                kernel.degrid_complex(pixels.reversed_axes(), pixel).conj()
            })
        };
        if let Some(interpolated) = &mut self.interpolated
            && interpolated.predict(row, (ratio, selection_mask), output, |plane, values| {
                for (value, &pol) in values.iter_mut().zip(routing.pol_map()) {
                    if let Some(pol) = pol {
                        *value = sample(plane as usize, pol as usize);
                    }
                }
            })?
        {
            return Ok(());
        }
        for (channel, &frequency) in row.frequencies.iter().enumerate() {
            let plane = match_channel(&self.spectral, self.cells.shape()[3], frequency * ratio);
            if plane < 0
                || scratch_channel_masked(selection_mask, row.scratch_channels, channel, true)
            {
                continue;
            }
            for (correlation, &pol) in routing.pol_map().iter().enumerate() {
                let Some(pol) = pol else { continue };
                if row.raw_flags.get([correlation, channel].as_slice()) == Some(&true) {
                    continue;
                }
                output[[correlation, channel]] += sample(plane as usize, pol as usize);
            }
        }
        Ok(())
    }
}

fn recipe_cache(record: &RecordValue) -> Result<bool, GainSolveError> {
    match record.get("casars_sd_cache_pointings") {
        Some(Value::Scalar(ScalarValue::Bool(cache))) => Ok(*cache),
        None if !has_saved_setup(record) => Ok(false),
        _ => Err(model_error(
            "invalid single-dish recipe field casars_sd_cache_pointings",
        )),
    }
}

fn read_bytes(image: u64, tile: u64, samples: u64, kernel: u64) -> u64 {
    image
        .saturating_mul(3)
        .saturating_add(tile.saturating_mul(2))
        .saturating_add(samples)
        .saturating_add(kernel)
        .saturating_add(IMAGE_METADATA_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_coordinates::StokesType;
    use casa_images::PagedImage;
    use casa_ms::MeasurementSetBuilder;
    use ndarray::IxDyn;

    use crate::imaging_memory_test::{
        cube_coordinates, isolated_test, live_since, peak_since, reset_peak,
    };

    #[test]
    fn loaded_single_dish_model_holds_at_most_its_charge() {
        if !isolated_test(
            module_path!(),
            "loaded_single_dish_model_holds_at_most_its_charge",
        ) {
            return;
        }
        let scratch = tempfile::tempdir().unwrap();
        let path = scratch.path().join("complex.im");
        let shape = [129, 129, 2, 3];
        let input = Complex32::new(0.5, 1.5);
        let coordinates = cube_coordinates(vec![StokesType::XX, StokesType::YY]);
        let tile_shape = [128, 128, 1, 1];
        let mut image = PagedImage::<Complex32>::create_with_tile_shape_and_cache(
            shape.to_vec(),
            tile_shape.to_vec(),
            coordinates,
            &path,
            0,
        )
        .unwrap();
        image
            .put_slice(&ArrayD::from_elem(IxDyn(&shape), input), &[0; 4])
            .unwrap();
        image.save().unwrap();
        drop(image);
        let ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
        let mut recipe = RecordValue::default();
        recipe.upsert(
            "diskimage",
            Value::Scalar(ScalarValue::String(path.to_string_lossy().into_owned())),
        );
        recipe.upsert(
            "casars_sd_kernel",
            Value::Scalar(ScalarValue::String("SF".into())),
        );
        let baseline = reset_peak();
        let prepared = PreparedModel::load(&recipe, &ms).unwrap();
        let measured = peak_since(baseline);
        let retained = live_since(baseline);
        let image_bytes =
            shape.into_iter().product::<usize>() as u64 * size_of::<Complex32>() as u64;
        let sample_bytes = 6 * size_of::<Option<Complex32>>() as u64;
        let charge = read_bytes(
            image_bytes,
            tile_shape.into_iter().product::<usize>() as u64 * size_of::<Complex32>() as u64,
            sample_bytes,
            GridFunction::SPHEROIDAL.forming_bytes(0),
        );
        assert!(measured <= charge, "{measured} bytes used over {charge}");
        assert!(retained <= prepared._memory.memory());
        assert_eq!(prepared.cells[[0, 0, 0, 2]], input);
    }
}
