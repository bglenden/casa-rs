// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reopen the primary-beam convolution used by a stored mosaic grid.

mod image_beam;
mod saved;

use casa_coordinates::{Coordinate, DirectionCoordinate};
use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::frequency::FrequencyRef;
use casa_core::{ArrayValue, Complex32, RecordValue, Value};
use casa_imaging_operator::{
    AiryDish, ConvolutionFunctionSet, GridGeometry, MosaicBufferBand, MosaicPb, MosaicRules,
    MosaicWindow, PolarizationRouting, SavedMosaicPlanes,
};
use casa_ms::{SubtableId, convert_frequency_to_frame};
use casa_numerics::{BeamPattern, PrimaryBeam};

use super::{FourierRow, GainSolveError, model_error, record_text};

pub(super) fn is_mosaic(record: &RecordValue) -> bool {
    matches!(record_text(record, "name"), Ok("MosaicFT" | "MosaicFTNew"))
}

pub(super) fn is_legacy_mosaic(record: &RecordValue) -> bool {
    matches!(record.get("pbconvfunc"), Some(Value::Record(beam)) if saved::rules(record, beam) == MosaicRules::Simulator)
}

pub(super) struct MosaicLayout {
    direction: DirectionCoordinate,
    centre: [f64; 2],
    shape: [usize; 2],
    increment: [f64; 2],
    antenna_classes: Vec<u8>,
    legacy: bool,
    clip_pointing: bool,
    vla_image: bool,
    converted_pointing: Option<(usize, MDirection)>,
    pub(super) autocorrelations: bool,
}

impl MosaicLayout {
    pub(super) fn load(
        record: &RecordValue,
        row: &FourierRow<'_>,
        (direction, vla_image): (&DirectionCoordinate, bool),
        (geometry, polarization): (&GridGeometry, &PolarizationRouting),
        reference_hz: f64,
        convert_frequencies: bool,
        scratch: bool,
    ) -> Result<(Box<dyn ConvolutionFunctionSet>, Self), GainSolveError> {
        let Some(Value::Record(beam)) = record.get("pbconvfunc") else {
            return Err(model_error("virtual mosaic has no beam convolution record"));
        };
        let rules = saved::rules(record, beam);
        let legacy = rules == MosaicRules::Simulator;
        let common = match record_text(beam, "name")? {
            "HetArrayConvFunc" => false,
            "SimplePBConvFunc" if legacy => true,
            _ => return Err(model_error("unrecognized virtual mosaic beam convolution")),
        };
        let (window, complete_window) =
            beam_window(row, convert_frequencies, legacy.then_some(record))?;
        let (kernels, antenna_classes) = if common {
            common_kernels(
                record,
                row,
                (direction, vla_image),
                geometry,
                polarization,
                &window,
                scratch,
            )?
        } else {
            array_kernels(
                (record, beam),
                row,
                (direction, geometry),
                polarization,
                reference_hz,
                rules,
                (window, complete_window),
            )?
        };
        let grid = geometry.image();
        Ok((
            Box::new(kernels),
            Self {
                direction: direction.clone(),
                centre: grid.reference_pixel.map(|pixel| pixel as f64),
                shape: grid.shape,
                increment: grid.increment_rad,
                antenna_classes,
                legacy,
                clip_pointing: legacy && !common,
                vla_image,
                converted_pointing: None,
                autocorrelations: legacy
                    && matches!(
                        record.get("usezero"),
                        Some(Value::Scalar(casa_core::ScalarValue::Bool(true)))
                    ),
            },
        ))
    }

    pub(super) fn pointing_offset(
        &mut self,
        row: &FourierRow<'_>,
        shift: [f64; 2],
    ) -> Result<Option<[f64; 2]>, GainSolveError> {
        // Legacy MosaicFT takes the first antenna direction from the
        // buffer. The newer saved grids reopen without their POINTING
        // selector and use the field direction.
        let reference = self.direction.conversion_direction_ref();
        let pointing = if let Some(pointing) = row.legacy_mosaic_pointing.filter(|_| self.legacy) {
            if let Some((buffer, direction)) = &self.converted_pointing
                && *buffer == pointing.buffer
            {
                direction.clone()
            } else {
                let direction = beam_direction(
                    row,
                    &pointing.direction,
                    (pointing.epoch, pointing.antenna),
                    reference,
                    self.vla_image,
                )?;
                self.converted_pointing = Some((pointing.buffer, direction.clone()));
                direction
            }
        } else {
            row.field
                .convert_to(reference, row.frame)
                .map_err(model_error)?
        };
        let (longitude, latitude) = pointing.as_angles();
        if self.clip_pointing {
            // HetArrayConvFunc truncates to integer image pixels and returns
            // empty convolution support for pointings outside the grid.
            let Ok(pixel) = self.direction.world_to_pixel([longitude, latitude]) else {
                return Ok(None);
            };
            if pixel
                .into_iter()
                .zip(self.shape)
                .any(|(pixel, length)| !(0. ..length as f64).contains(&pixel.trunc()))
            {
                return Ok(None);
            }
        }
        let pixel = self
            .direction
            .world_to_pixel([longitude - shift[0], latitude - shift[1]])
            .map_err(model_error)?;
        Ok(Some(std::array::from_fn(|axis| {
            (pixel[axis] - self.centre[axis]) * self.increment[axis]
        })))
    }

    pub(super) fn antenna_types(&self, row: &FourierRow<'_>) -> Result<[u8; 2], GainSolveError> {
        let [Some(first), Some(second)] = [row.row.antenna1, row.row.antenna2]
            .map(|antenna| self.antenna_classes.get(antenna as usize).copied())
        else {
            return Err(model_error(
                "virtual mosaic refers to an unavailable antenna",
            ));
        };
        Ok([first, second])
    }
}

fn array_kernels(
    (record, beam): (&RecordValue, &RecordValue),
    row: &FourierRow<'_>,
    (direction, geometry): (&DirectionCoordinate, &GridGeometry),
    polarization: &PolarizationRouting,
    reference_hz: f64,
    rules: MosaicRules,
    (window, complete_window): (MosaicWindow, Option<MosaicWindow>),
) -> Result<(MosaicPb, Vec<u8>), GainSolveError> {
    let legacy = rules == MosaicRules::Simulator;
    let telescope = row
        .ms
        .observation()
        .map_err(model_error)?
        .string(row.row.observation_id as usize, "TELESCOPE_NAME")
        .map_err(model_error)?;
    // HetArrayConvFunc::fromRecord clears its remembered MS. On the
    // first buffer of that MS it regenerates the legacy convolution
    // with the current antenna metadata.
    let mut loaded = if legacy {
        None
    } else {
        saved::planes(record, beam, &window)?
    };
    if loaded.is_none()
        && let Some(complete) = complete_window
        && window
            .shares_beam_frequencies(&complete, rules)
            .map_err(model_error)?
    {
        loaded = saved::planes(record, beam, &complete)?;
    }
    let class = saved::integer(beam, "pbclass")?;
    let result = if class == 8 {
        let path = std::path::Path::new(record_text(beam, "vptable")?);
        let beams = image_beam::ImageBeams::load(path, row)?;
        let kernels = match restore(rules, geometry, polarization, &window, loaded, beams.len())? {
            Some(kernels) => kernels,
            None => image_kernels(
                if legacy { rules } else { MosaicRules::Tclean },
                geometry,
                direction,
                polarization,
                reference_hz,
                window,
                &beams,
            )?,
        };
        (kernels, beams.classes)
    } else {
        let (dishes, classes) = antenna_beams(row, geometry, &telescope, class, beam, rules)?;
        let kernels = match restore(rules, geometry, polarization, &window, loaded, dishes.len())? {
            Some(kernels) => kernels,
            None => MosaicPb::new(
                rules,
                geometry,
                polarization,
                reference_hz,
                &dishes,
                &[window],
            )
            .map_err(model_error)?,
        };
        (kernels, classes)
    };
    Ok(result)
}

fn common_kernels(
    record: &RecordValue,
    row: &FourierRow<'_>,
    (direction, vla_image): (&DirectionCoordinate, bool),
    geometry: &GridGeometry,
    polarization: &PolarizationRouting,
    window: &MosaicWindow,
    scratch: bool,
) -> Result<(MosaicPb, Vec<u8>), GainSolveError> {
    let telescope = record_text(record, "telescope")?;
    let frequency = if scratch {
        row.ms
            .spectral_window()
            .map_err(model_error)?
            .ref_frequency(0)
            .map_err(model_error)?
    } else {
        // MosaicFT::fromRecord recreates its VPSkyJones at 1 THz.
        1.0e12
    };
    let antennas = row.ms.antenna().map_err(model_error)?;
    let pattern = match telescope {
        "NRAO140FT" => BeamPattern::for_dish_diameter(43.),
        "NONE" => BeamPattern::polynomial(180. * 60., &[1.], true, false),
        // VPSkyJones builds a numeric beam from the first dish when its
        // telescope lookup returns UNKNOWN. Retain it for virtual models.
        "DEFAULT" => {
            BeamPattern::for_dish_diameter(antennas.dish_diameter(0).map_err(model_error)?)
        }
        _ => PrimaryBeam::for_telescope(telescope, frequency).map(PrimaryBeam::pattern),
    }
    .ok_or_else(|| model_error(format!("no legacy common mosaic beam for {telescope}")))?;
    let offset = common_beam_offset(row, direction, vla_image, telescope.contains("VLA"))?;
    let kernels = MosaicPb::simple_shifted_pattern_screens(
        geometry,
        polarization,
        &pattern,
        std::slice::from_ref(window),
        offset,
    )
    .and_then(|screens| screens.finish())
    .map_err(model_error)?;
    Ok((kernels, vec![0; antennas.row_count()]))
}

fn common_beam_offset(
    row: &FourierRow<'_>,
    coordinates: &DirectionCoordinate,
    vla_image: bool,
    vla_telescope: bool,
) -> Result<[f64; 2], GainSolveError> {
    let Some(first) = row.legacy_mosaic_pointing else {
        return Ok([0.; 2]);
    };
    let selected = if row.ms.subtable(SubtableId::Pointing).is_some() {
        row.ms
            .pointing()
            .and_then(|pointing| pointing.legacy_direction(first.second_antenna, first.epoch))
            .map_err(model_error)?
    } else {
        None
    };
    let second = match selected {
        Some(direction) => direction,
        None => super::super::field_direction(
            row.ms,
            row.engine,
            row.row.field_id as usize,
            first.epoch,
        )
        .map_err(model_error)?,
    };
    let reference = coordinates.conversion_direction_ref();
    let buffer = (first.epoch, first.antenna);
    let first = beam_direction(row, &first.direction, buffer, reference, vla_image)?;
    // Both antennas share a single SkyJones beam index. Its painting
    // direction comes from antenna two in the first buffer of the window.
    let second = beam_direction(row, &second, buffer, reference, vla_telescope)?;
    let (lon, lat) = first.as_angles();
    let (beam_lon, beam_lat) = second.as_angles();
    let local = coordinates
        .clone()
        .with_reference_value([lon, lat])
        .with_reference_pixel([0.; 2]);
    let pixel = local
        .world_to_pixel([beam_lon, beam_lat])
        .map_err(model_error)?;
    let increment = local.increment();
    Ok(std::array::from_fn(|axis| {
        (pixel[axis] * increment[axis]).to_degrees()
    }))
}

fn beam_direction(
    row: &FourierRow<'_>,
    direction: &MDirection,
    (epoch, antenna): (f64, usize),
    reference: DirectionRef,
    observatory: bool,
) -> Result<MDirection, GainSolveError> {
    if direction.refer() == reference {
        return Ok(direction.clone());
    }
    let frame = if observatory {
        row.engine
            .spectral_frame_observatory_direction(epoch, row.field.clone())
    } else {
        row.engine.antenna_frame(epoch, antenna)
    }
    .map_err(model_error)?;
    direction.convert_to(reference, &frame).map_err(model_error)
}

fn restore(
    rules: MosaicRules,
    geometry: &GridGeometry,
    polarization: &PolarizationRouting,
    window: &MosaicWindow,
    loaded: Option<SavedMosaicPlanes<'_>>,
    dishes: usize,
) -> Result<Option<MosaicPb>, GainSolveError> {
    let Some(planes) = loaded else {
        return Ok(None);
    };
    if planes.dishes != dishes {
        return Err(model_error(
            "virtual mosaic beam classes do not match the saved convolution",
        ));
    }
    MosaicPb::from_saved_planes(rules, geometry, polarization, window, planes).map_err(model_error)
}

fn image_kernels(
    rules: MosaicRules,
    geometry: &GridGeometry,
    direction: &DirectionCoordinate,
    polarization: &PolarizationRouting,
    reference_hz: f64,
    window: MosaicWindow,
    patterns: &image_beam::ImageBeams,
) -> Result<MosaicPb, GainSolveError> {
    let patterns = patterns.screens(rules == MosaicRules::Simulator)?;
    let support = patterns
        .iter()
        .map(|beam| beam.support_pixels(geometry.image().increment_rad[0], reference_hz))
        .max()
        .unwrap_or(0);
    let pc = direction.pc_matrix();
    MosaicPb::from_image_screens_with_rules(
        rules,
        geometry,
        polarization,
        reference_hz,
        (patterns.len(), support),
        &[window],
        |[first, second], side, increment, hz, voltage, power| {
            let centre = (side / 2) as f64;
            for (i, (voltage, power)) in voltage.iter_mut().zip(power).enumerate() {
                let x = (i % side) as f64 - centre;
                let y = (i / side) as f64 - centre;
                let offset = std::array::from_fn(|axis| {
                    increment[axis] * (pc[[axis, 0]] * x + pc[[axis, 1]] * y)
                });
                let a = patterns[first].value(offset, hz);
                let b = patterns[second].value(offset, hz);
                *voltage = a * b;
                *power = Complex32::new(a.norm_sqr() * b.norm_sqr(), 0.);
            }
        },
    )
    .map_err(model_error)
}

fn beam_window(
    row: &FourierRow<'_>,
    convert_frequencies: bool,
    legacy: Option<&RecordValue>,
) -> Result<(MosaicWindow, Option<MosaicWindow>), GainSolveError> {
    let spw = row.row.data_spw_id as usize;
    let windows = row.ms.spectral_window().map_err(model_error)?;
    let channel_width_hz = windows
        .chan_width(spw)
        .map_err(model_error)?
        .into_iter()
        .next()
        .ok_or_else(|| model_error("virtual mosaic window has no channel width"))?;
    let ratio = if convert_frequencies && row.frequency_ref != FrequencyRef::LSRK {
        let frequency = row.frequencies[0];
        convert_frequency_to_frame(
            row.frequency_ref,
            FrequencyRef::LSRK,
            frequency,
            row.row.time_seconds,
            row.row.field_id as usize,
            row.engine,
        )
        .map_err(model_error)?
            / frequency
    } else {
        1.0
    };
    let full = MosaicWindow {
        spectral_window: spw as u32,
        window_frequencies_hz: row.frequencies.to_vec(),
        channel_width_hz,
        selected_frequencies_hz: row
            .frequencies
            .iter()
            .map(|frequency| frequency * ratio)
            .collect(),
        buffer_band: None,
    };
    if let Some(record) = legacy {
        let selected = if let Some([_, start, count, stride]) = row.scratch_channels {
            full.selected_frequencies_hz
                .iter()
                .copied()
                .skip(start as usize)
                .step_by((stride as usize).max(1))
                .take(count as usize)
                .collect()
        } else if let Some(selection) = &row.row.channel_selection {
            selection
                .indices(row.frequencies.len())
                .map_err(model_error)?
                .into_iter()
                .map(|index| full.selected_frequencies_hz[index])
                .collect()
        } else {
            full.selected_frequencies_hz.clone()
        };
        let image_frequencies = match record.get("casars_live_image_frequency") {
            Some(Value::Array(ArrayValue::Float64(frequencies))) if selected.len() > 1 => {
                Some(frequencies.iter().copied().collect())
            }
            _ => None,
        };
        let mut window = full;
        if let Some(frequencies) = image_frequencies {
            window.buffer_band = Some(MosaicBufferBand {
                channels: selected.len(),
                lowest_topocentric_hz: selected.iter().copied().fold(f64::INFINITY, f64::min),
            });
            window.selected_frequencies_hz = frequencies;
        } else {
            // In the nearest/bypass path getInterpolateArrays uses the
            // iterator's native frequencies before any frame conversion.
            window.selected_frequencies_hz = selected
                .into_iter()
                .map(|frequency| frequency / ratio)
                .collect();
        }
        return Ok((window, None));
    }
    if let Some(selection) = &row.row.channel_selection {
        let selected = selection
            .indices(row.frequencies.len())
            .map_err(model_error)?;
        let mut window = full.clone();
        window.selected_frequencies_hz = selected
            .into_iter()
            .map(|index| full.selected_frequencies_hz[index])
            .collect();
        Ok((window, Some(full)))
    } else {
        Ok((full, None))
    }
}

fn antenna_beams(
    row: &FourierRow<'_>,
    geometry: &GridGeometry,
    telescope: &str,
    class: i32,
    beam: &RecordValue,
    rules: MosaicRules,
) -> Result<(Vec<PrimaryBeam>, Vec<u8>), GainSolveError> {
    let antennas = row.ms.antenna().map_err(model_error)?;
    if !matches!(class, 1 | 2) {
        let path = std::path::Path::new(record_text(beam, "vptable")?);
        let table = casa_ms::VoltagePatternTable::open(path).map_err(model_error)?;
        let record = table
            .default_record(telescope)
            .map_err(model_error)?
            .ok_or_else(|| model_error("no custom mosaic voltage pattern for the telescope"))?;
        let pattern = casa_ms::radial_pattern(&record).map_err(model_error)?;
        return Ok((vec![pattern], vec![0; antennas.row_count()]));
    }
    if class == 1 {
        let reference = row
            .ms
            .spectral_window()
            .map_err(model_error)?
            .ref_frequency(0)
            .map_err(model_error)?;
        if let Some(beam) = PrimaryBeam::for_telescope(telescope, reference) {
            return Ok((vec![beam], vec![0; antennas.row_count()]));
        }
    }
    let alma = matches!(telescope, "ALMA" | "ACA");
    let mut dishes = Vec::new();
    let mut diameters = Vec::new();
    let mut antenna_classes = Vec::with_capacity(antennas.row_count());
    for antenna in 0..antennas.row_count() {
        let diameter = antennas.dish_diameter(antenna).map_err(model_error)?;
        let dish = if alma && rules == MosaicRules::Simulator {
            AiryDish::casa_alma_simulator(diameter)
        } else if alma {
            AiryDish::casa_alma(diameter, geometry)
        } else {
            AiryDish::casa_airy(diameter)
        };
        let beam = PrimaryBeam::from(dish);
        let class = if rules == MosaicRules::Simulator {
            diameters.iter().position(|known| *known == diameter)
        } else {
            dishes.iter().position(|known| *known == beam)
        }
        .unwrap_or_else(|| {
            dishes.push(beam);
            diameters.push(diameter);
            dishes.len() - 1
        });
        antenna_classes.push(u8::try_from(class).map_err(model_error)?);
    }
    Ok((dishes, antenna_classes))
}
