// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reopen the primary-beam convolution used by a stored mosaic grid.

mod saved;

use casa_coordinates::DirectionCoordinate;
use casa_imaging_operator::{
    AiryDish, ConvolutionFunctionSet, GridGeometry, MosaicPb, MosaicRules, MosaicWindow,
    PolarizationRouting,
};
use casa_ms::convert_frequency_to_frame;
use casa_numerics::PrimaryBeam;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::{RecordValue, Value};

use super::{FourierRow, GainSolveError, model_error, record_text};

pub(super) fn is_mosaic(record: &RecordValue) -> bool {
    matches!(record_text(record, "name"), Ok("MosaicFT" | "MosaicFTNew"))
}

pub(super) struct MosaicLayout {
    direction: DirectionCoordinate,
    centre: [f64; 2],
    increment: [f64; 2],
    antenna_classes: Vec<u8>,
}

impl MosaicLayout {
    pub(super) fn load(
        record: &RecordValue,
        row: &FourierRow<'_>,
        direction: &DirectionCoordinate,
        geometry: &GridGeometry,
        polarization: &PolarizationRouting,
        reference_hz: f64,
        convert_frequencies: bool,
    ) -> Result<(Box<dyn ConvolutionFunctionSet>, Self), GainSolveError> {
        let Some(Value::Record(beam)) = record.get("pbconvfunc") else {
            return Err(model_error("virtual mosaic has no beam convolution record"));
        };
        if record_text(beam, "name")? != "HetArrayConvFunc" {
            return Err(model_error(
                "virtual mosaic requires an array beam convolution",
            ));
        }
        let telescope = row
            .ms
            .observation()
            .map_err(model_error)?
            .string(row.row.observation_id as usize, "TELESCOPE_NAME")
            .map_err(model_error)?;
        let (window, complete_window) = beam_window(row, convert_frequencies)?;
        let mut loaded = saved::planes(record, beam, &window)?;
        if loaded.is_none()
            && let Some(complete) = complete_window
            && window
                .shares_beam_frequencies(&complete)
                .map_err(model_error)?
        {
            loaded = saved::planes(record, beam, &complete)?;
        }
        let class = saved::integer(beam, "pbclass")?;
        let custom = !matches!(class, 1 | 2);
        let (dishes, antenna_classes) = match (custom, loaded.as_ref()) {
            (true, Some(planes)) if planes.dishes == 1 => {
                let path = record_text(beam, "vptable")?;
                if path.is_empty() || !std::path::Path::new(path).is_dir() {
                    return Err(model_error(
                        "virtual mosaic voltage-pattern table is unavailable",
                    ));
                }
                let antennas = row.ms.antenna().map_err(model_error)?;
                (Vec::new(), vec![0; antennas.row_count()])
            }
            (true, _) => {
                return Err(model_error(
                    "virtual mosaic custom voltage-pattern selection requires a saved single-class convolution",
                ));
            }
            (false, _) => antenna_beams(row, geometry, &telescope, class)?,
        };
        let restored = if let Some(planes) = loaded {
            if planes.dishes != dishes.len() && !custom {
                return Err(model_error(
                    "virtual mosaic beam classes do not match the saved convolution",
                ));
            }
            MosaicPb::from_saved_planes(geometry, polarization, &window, planes)
                .map_err(model_error)?
        } else {
            None
        };
        let kernels = match restored {
            Some(kernels) => kernels,
            None if custom => {
                return Err(model_error(
                    "virtual mosaic has no saved convolution for the selected custom beam band",
                ));
            }
            None => MosaicPb::new(
                MosaicRules::Tclean,
                geometry,
                polarization,
                reference_hz,
                &dishes,
                &[window],
            )
            .map_err(model_error)?,
        };
        let grid = geometry.image();
        Ok((
            Box::new(kernels),
            Self {
                direction: direction.clone(),
                centre: grid.reference_pixel.map(|pixel| pixel as f64),
                increment: grid.increment_rad,
                antenna_classes,
            },
        ))
    }

    pub(super) fn pointing_offset(&self, row: &FourierRow<'_>) -> Result<[f64; 2], GainSolveError> {
        // HetArrayConvFunc does not save its inherited POINTING selector.
        // Reopened MosaicFT calls get without initializing a new predictor,
        // so its convolution phases use the field direction.
        let pointing = row
            .field
            .convert_to(self.direction.conversion_direction_ref(), row.frame)
            .map_err(model_error)?;
        let (longitude, latitude) = pointing.as_angles();
        let pixel = self
            .direction
            .world_to_pixel([longitude, latitude])
            .map_err(model_error)?;
        Ok(std::array::from_fn(|axis| {
            (pixel[axis] - self.centre[axis]) * self.increment[axis]
        }))
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

fn beam_window(
    row: &FourierRow<'_>,
    convert_frequencies: bool,
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
) -> Result<(Vec<PrimaryBeam>, Vec<u8>), GainSolveError> {
    let antennas = row.ms.antenna().map_err(model_error)?;
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
    let mut antenna_classes = Vec::with_capacity(antennas.row_count());
    for antenna in 0..antennas.row_count() {
        let diameter = antennas.dish_diameter(antenna).map_err(model_error)?;
        let dish = if alma {
            AiryDish::casa_alma(diameter, geometry)
        } else {
            AiryDish::casa_airy(diameter)
        };
        let beam = PrimaryBeam::from(dish);
        let class = dishes
            .iter()
            .position(|known| *known == beam)
            .unwrap_or_else(|| {
                dishes.push(beam);
                dishes.len() - 1
            });
        antenna_classes.push(u8::try_from(class).map_err(model_error)?);
    }
    Ok((dishes, antenna_classes))
}
