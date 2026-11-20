// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sample a legacy SDGrid model at each autocorrelation's pointing.

use std::sync::Mutex;

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateType, DirectionCoordinate, SpectralCoordinate,
    StokesCoordinate,
};
use casa_imaging_operator::PolarizationRouting;
use casa_imaging_runtime::{Demand, HostResources, Reservation, ResourcePolicy, admit};
use casa_imaging_singledish::{GridFunction, KernelParameters, KernelTable};
use casa_ms::MeasurementSet;
use casa_numerics::{BeamPattern, PrimaryBeam};
use casa_types::measures::direction::MDirection;
use casa_types::{Complex32, Correlation, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, Axis, Ix2};

use crate::ft::image::{coordinate_index, match_channels, polarization_coordinate};

use super::fourier::{FourierRow, open_complex_model_image};
use super::{GainSolveError, model_error, read_error};

const IMAGE_METADATA_BYTES: u64 = 65_536;

pub(super) struct SingleDishModel {
    record: RecordValue,
    prepared: Mutex<Option<PreparedModel>>,
}

struct PreparedModel {
    direction: DirectionCoordinate,
    spectral: SpectralCoordinate,
    stokes: StokesCoordinate,
    cells: ArrayD<Complex32>,
    kernel: KernelTable,
    pixel: Option<[f64; 2]>,
    sampled: Vec<Option<Complex32>>,
    _memory: Reservation,
}

impl SingleDishModel {
    pub(super) fn new(record: RecordValue) -> Self {
        Self {
            record,
            prepared: Mutex::default(),
        }
    }

    pub(super) fn predict(
        &self,
        row: &FourierRow<'_>,
        pointing: &MDirection,
        output: &mut ArrayD<Complex32>,
    ) -> Result<(), GainSolveError> {
        if row.row.antenna1 != row.row.antenna2 || row.row.flag_row {
            return Ok(());
        }
        let mut prepared = self.prepared.lock().expect("single-dish model preparation");
        if prepared.is_none() {
            *prepared = Some(PreparedModel::load(&self.record, row.ms)?);
        }
        prepared
            .as_mut()
            .expect("the single-dish image is loaded")
            .predict(row, pointing, output)
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
        let function =
            GridFunction::from_casa(name, KernelParameters::DEFAULT).map_err(model_error)?;
        let image = open_complex_model_image(record, ms)?;
        let [nx, ny, pols, channels] = *image.shape() else {
            return Err(model_error("SDGrid needs a four-axis complex image"));
        };
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
        let first_frequency = spectral
            .clone()
            .without_conversion()
            .world_frequency_hz(0.)
            .map_err(model_error)?;
        let increment = direction.increment()[0];
        let beam = if matches!(function, GridFunction::PrimaryBeam) {
            Some(default_beam(ms, first_frequency)?)
        } else {
            None
        };
        let beam_pixels = beam
            .as_ref()
            .map_or(0, |beam| beam.support_pixels(increment, first_frequency));
        let image_bytes = (nx as u64)
            .saturating_mul(ny as u64)
            .saturating_mul(pols as u64)
            .saturating_mul(channels as u64)
            .saturating_mul(size_of::<Complex32>() as u64);
        let host = HostResources::detect().map_err(model_error)?;
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
                    sample_bytes,
                    function.forming_bytes(beam_pixels),
                ),
            },
        )
        .map_err(model_error)?;
        let cells = image.get().map_err(model_error)?;
        let kernel = match beam {
            Some(beam) => KernelTable::primary_beam(&beam, increment, first_frequency),
            None => KernelTable::new(function).map_err(model_error)?,
        };
        if nx.min(ny) <= kernel.support() {
            return Err(model_error(format!(
                "single-dish model axes must exceed the gridding support of {} pixels",
                kernel.support()
            )));
        }
        let direction = direction.clone();
        let spectral = spectral.clone().without_conversion();
        let stokes = stokes.clone();
        drop(image);
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
            _memory: memory,
        })
    }

    fn predict(
        &mut self,
        row: &FourierRow<'_>,
        pointing: &MDirection,
        output: &mut ArrayD<Complex32>,
    ) -> Result<(), GainSolveError> {
        let frame = row
            .engine
            .antenna_frame(row.row.time_seconds, row.row.antenna1 as usize)
            .map_err(|error| read_error(row.ms, error))?;
        let direction = pointing
            .convert_to(self.direction.direction_ref(), &frame)
            .map_err(model_error)?;
        let (longitude, latitude) = direction.as_angles();
        let Ok(pixel) = self.direction.world_to_pixel([longitude, latitude]) else {
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
        let channels = match_channels(&self.spectral, self.cells.shape()[3], row.frequencies);
        for (channel, &plane) in channels.iter().enumerate() {
            if plane < 0 {
                continue;
            }
            let spectra = self.cells.index_axis(Axis(3), plane as usize);
            for (correlation, &pol) in routing.pol_map().iter().enumerate() {
                let Some(pol) = pol else { continue };
                if row.raw_flags.get([correlation, channel].as_slice()) == Some(&true) {
                    continue;
                }
                let pixels = spectra
                    .index_axis(Axis(2), pol as usize)
                    .into_dimensionality::<Ix2>()
                    .expect("individual SDGrid model plane");
                let sample = self.sampled[plane as usize * self.cells.shape()[2] + pol as usize]
                    .get_or_insert_with(|| {
                        self.kernel
                            .degrid_complex(pixels.reversed_axes(), pixel)
                            .conj()
                    });
                output[[correlation, channel]] += *sample;
            }
        }
        Ok(())
    }
}

fn read_bytes(image: u64, samples: u64, kernel: u64) -> u64 {
    image
        .saturating_mul(3)
        .saturating_add(samples)
        .saturating_add(kernel)
        .saturating_add(IMAGE_METADATA_BYTES)
}

fn default_beam(ms: &MeasurementSet, frequency: f64) -> Result<BeamPattern, GainSolveError> {
    let telescope = ms
        .observation()
        .and_then(|table| table.string(0, "TELESCOPE_NAME"))
        .map_err(|error| read_error(ms, error))?;
    if let Some(beam) = PrimaryBeam::for_telescope(&telescope, frequency) {
        return Ok(beam.pattern());
    }
    let diameter = ms
        .antenna()
        .and_then(|table| table.dish_diameter(0))
        .map_err(|error| read_error(ms, error))?;
    BeamPattern::for_dish_diameter(diameter)
        .ok_or_else(|| model_error("no usable single-dish primary beam"))
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
        let shape = [128, 96, 2, 3];
        let input = Complex32::new(0.5, 1.5);
        let coordinates = cube_coordinates(vec![StokesType::XX, StokesType::YY]);
        let mut image =
            PagedImage::<Complex32>::create(shape.to_vec(), coordinates, &path).unwrap();
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
            sample_bytes,
            GridFunction::SPHEROIDAL.forming_bytes(0),
        );
        assert!(measured <= charge, "{measured} bytes used over {charge}");
        assert!(retained <= prepared._memory.memory());
        assert_eq!(prepared.cells[[0, 0, 0, 2]], input);
    }
}
