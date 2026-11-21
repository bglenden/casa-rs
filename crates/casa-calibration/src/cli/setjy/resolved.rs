// SPDX-License-Identifier: LGPL-3.0-or-later
//! Select the calibrator image's flux and frequency coverage for a field.

use std::collections::BTreeSet;
use std::path::Path;

use casa_ms::{MeasurementSet, ResolvedMsSelection};
use casa_task_runtime::TaskFailure;
use casa_types::measures::frame::MeasFrame;
use casa_types::measures::frequency::{FrequencyRef, MFrequency};

use super::{CalibratorSpectrum, ManualFlux, MsCalEngine, execution};
use crate::ft::image::ImageModel;
use crate::ft::setjy::image::{ImageFlux, ImageFrequencies, calibrator_image};
use crate::session::resolve_casa_calibration_selection;

pub(super) struct ImageParameters<'a> {
    pub(super) path: &'a Path,
    pub(super) by_channel: bool,
    pub(super) scratch: bool,
}

pub(super) enum ResolvedFlux<'a> {
    Manual(&'a ManualFlux),
    Catalog(&'a CalibratorSpectrum<'a>, f64),
    Unscaled,
}

impl ImageParameters<'_> {
    pub(super) fn prepare(
        &self,
        ms: &MeasurementSet,
        selected: &ResolvedMsSelection,
        frame: &MeasFrame,
        field: i32,
        flux: ResolvedFlux<'_>,
    ) -> Result<Option<ImageModel>, TaskFailure> {
        let mut request = selected.request.clone();
        request.field = Some(field.to_string());
        let mut selected = resolve_casa_calibration_selection(ms, &request).map_err(execution)?;
        selected.channel_selections.clear();
        let frequencies = image_frequencies(ms, &selected, field)?;
        let image_flux = if self.by_channel && frequencies.samples.len() > 1 {
            let samples = match flux {
                ResolvedFlux::Manual(manual) => Some(
                    frequencies
                        .samples
                        .iter()
                        .map(|&hz| {
                            manual
                                .intensity_at(&MFrequency::new(hz, FrequencyRef::LSRK), frame)
                                .map_err(execution)
                        })
                        .collect::<Result<_, _>>()?,
                ),
                ResolvedFlux::Catalog(spectrum, _) => Some(
                    frequencies
                        .samples
                        .iter()
                        .map(|&hz| spectrum.flux(hz))
                        .collect(),
                ),
                ResolvedFlux::Unscaled => None,
            };
            ImageFlux::PerChannel(samples)
        } else {
            let jy = match flux {
                ResolvedFlux::Manual(manual) => {
                    let center = MFrequency::new(frequencies.center(), FrequencyRef::LSRK);
                    Some(manual.intensity_at(&center, frame).map_err(execution)?)
                }
                ResolvedFlux::Catalog(_, first_jy) => Some(first_jy),
                ResolvedFlux::Unscaled => None,
            };
            ImageFlux::Constant(jy)
        };
        calibrator_image(
            ms,
            &selected,
            self.path,
            &frequencies,
            image_flux,
            frame,
            self.scratch,
        )
        .map_err(execution)
    }
}

fn image_frequencies(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    field: i32,
) -> Result<ImageFrequencies, TaskFailure> {
    let windows = ms.spectral_window().map_err(execution)?;
    let ids = selected
        .selected_rows
        .iter()
        .map(|row| row.spectral_window_id)
        .collect::<BTreeSet<_>>();
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    let mut step = f64::INFINITY;
    let mut engine = None;
    for id in ids {
        let frequencies = windows.chan_freq(id as usize).map_err(execution)?;
        let widths = windows.chan_width(id as usize).map_err(execution)?;
        let reference = windows
            .frequency_reference(id as usize)
            .map_err(execution)?;
        let mut low = f64::INFINITY;
        let mut high = f64::NEG_INFINITY;
        for (hz, width) in frequencies.into_iter().zip(widths) {
            let width = width.abs();
            low = low.min(hz - width * 0.5);
            high = high.max(hz + width * 0.5);
            step = step.min(width);
        }
        if reference == FrequencyRef::LSRK {
            min = min.min(low);
            max = max.max(high);
        } else {
            if engine.is_none() {
                engine = Some(MsCalEngine::new(ms).map_err(execution)?);
            }
            let engine = engine.as_ref().expect("frequency conversion engine");
            let times = selected
                .selected_rows
                .iter()
                .filter(|row| row.spectral_window_id == id)
                .map(|row| row.time_mjd_seconds.to_bits())
                .collect::<BTreeSet<_>>();
            for time in times {
                let frame = engine
                    .spectral_frame_observatory(f64::from_bits(time), field as usize)
                    .map_err(execution)?;
                let ratio = MFrequency::new(1., reference)
                    .convert_to(FrequencyRef::LSRK, &frame)
                    .map_err(execution)?
                    .hz();
                min = min.min(low * ratio);
                max = max.max(high * ratio);
            }
        }
    }
    ImageFrequencies::new(min, max, step).map_err(execution)
}
