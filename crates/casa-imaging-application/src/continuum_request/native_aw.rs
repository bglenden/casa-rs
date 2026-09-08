// SPDX-License-Identifier: LGPL-3.0-or-later

//! Resolve the native EVLA request from the already selected observation.

use std::io::Read;

use casa_imaging_model::{
    EvlaDishSurface, NativeAwFrequencyGroup, NativeAwGrid, NativeAwRequestInput, NativeAwTerms,
};

use super::*;

/// Bounded metadata acquisition; pixel generation remains in the admitted phase.
#[allow(clippy::too_many_arguments)]
pub(super) fn resolve(
    request: &ContinuumImagingRequest,
    controls: &NativeEvlaAwCache,
    ms: &MeasurementSet,
    windows: &[SourceSpectralWindow],
    spectral: &PreparedSpectralAxis,
    first_row: SelectedObservationRow,
    geometry_engine: &casa_ms::derived::engine::MsCalEngine,
) -> Result<NativeAwRequestInput, crate::ApplicationError> {
    let aw = request.aw_projection.as_ref().expect("native AW source");
    let observation = ms.observation()?;
    if observation
        .string(first_row.observation_id() as usize, "TELESCOPE_NAME")?
        .trim()
        != "EVLA"
    {
        return Err(boxed(
            "native aperture generation requires an EVLA observation",
        ));
    }
    let antenna = ms.antenna()?;
    for row in 0..antenna.row_count() {
        if antenna.dish_diameter(row)? != 25.0 {
            return Err(boxed(
                "native EVLA generation requires the explicit homogeneous 25 m dish model",
            ));
        }
    }
    // The reference data is an explicit input with bounded acquisition, never
    // discovered through a CASA installation or downloaded during execution.
    let mut bytes = Vec::new();
    std::fs::File::open(&controls.surface)?
        .take(1_048_577)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 1_048_576 {
        return Err(boxed(
            "native EVLA surface exceeds the 1 MiB reference-data bound",
        ));
    }
    let surface = EvlaDishSurface::from_surface_text(std::str::from_utf8(&bytes)?)?;
    let mut frequencies = windows
        .iter()
        .map(|window| {
            let selected = spectral
                .selected_source_channels
                .get(&window.spw_id)
                .ok_or_else(|| boxed("native AW selected SPW is absent from the spectral owner"))?;
            let channel_frequencies_hz = selected
                .iter()
                .map(|index| {
                    window
                        .frequencies_hz
                        .get(*index)
                        .copied()
                        .ok_or_else(|| boxed("native AW selected channel is outside its SPW"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            // TransformMachines2::makeFreqValList selects each SPW's high endpoint.
            let cf_frequency_hz = channel_frequencies_hz
                .iter()
                .copied()
                .reduce(f64::max)
                .ok_or_else(|| boxed("native AW SPW has no selected frequencies"))?;
            Ok(NativeAwFrequencyGroup {
                spectral_window: u32::try_from(window.spw_id)?,
                channel_frequencies_hz,
                cf_frequency_hz,
            })
        })
        .collect::<Result<Vec<_>, crate::ApplicationError>>()?;
    frequencies.sort_by(|left, right| left.cf_frequency_hz.total_cmp(&right.cf_frequency_hz));
    if !aw.wideband {
        let selected_frequencies = frequencies
            .iter()
            .flat_map(|g| g.channel_frequencies_hz.iter().copied())
            .collect();
        frequencies = vec![NativeAwFrequencyGroup {
            spectral_window: frequencies[0].spectral_window,
            channel_frequencies_hz: selected_frequencies,
            cf_frequency_hz: spectral.reference_frequency_hz,
        }];
    }
    let planes = aw.w_plane_count.expect("validated native AW W-plane count");
    let sky_cell = request.cell_arcsec.to_radians() / 3600.0;
    // CASA AWConvFunc derives its W grid from the requested field of view,
    // not the observed W envelope: maxUVW=1/(4*sky_increment), w=i²/wScale.
    let max_w = 1.0 / (sky_cell * 4.0);
    let w_increment = ((planes - 1)
        .checked_mul(planes - 1)
        .ok_or_else(|| boxed("native AW W-grid size overflowed"))? as f32)
        as f64
        / max_w;
    let w_values = (0..planes)
        .map(|index| (index * index) as f64 / w_increment)
        .collect();
    let working_cell = sky_cell * controls.oversampling as f64 * request.image_size as f64
        / controls.working_size as f64;
    let pa = geometry_engine.parallactic_angle(
        first_row.time_mjd_seconds(),
        first_row.field_id() as usize,
        0,
    )? as f32;
    let feed_angle = receptor_zero_angle(
        ms,
        first_row.time_mjd_seconds(),
        frequencies[0].spectral_window,
    )? as f32;
    let pa = f64::from(pa + feed_angle);
    let mueller_elements = match request.polarizations.as_slice() {
        [PolarizationCoordinate::CircularRr] => vec![0],
        [PolarizationCoordinate::CircularLl] => vec![15],
        [PolarizationCoordinate::StokesI] => vec![0, 15],
        _ => {
            return Err(boxed(
                "native EVLA AW currently supports Stokes I or one circular parallel hand",
            ));
        }
    };
    Ok(NativeAwRequestInput {
        surface,
        antenna_diameter_m: 25.0,
        frequencies,
        w_values,
        w_increment,
        pa_values: vec![pa],
        mueller_elements,
        reference_frequency_hz: spectral.reference_frequency_hz,
        grid: NativeAwGrid {
            size: controls.working_size,
            sky_increment_rad: [-working_cell, working_cell],
            oversampling: controls.oversampling,
        },
        terms: NativeAwTerms {
            aperture: aw.a_term,
            w_term: true,
            prolate_spheroidal: aw.ps_term,
            wideband: aw.wideband,
            conjugate_beams: aw.conjugate_beams,
        },
        maximum_cells: controls.maximum_cells,
    })
}

fn receptor_zero_angle(
    ms: &MeasurementSet,
    time: f64,
    spw: u32,
) -> Result<f64, crate::ApplicationError> {
    let feed = ms.feed()?;
    let mut angle = None;
    for row in 0..feed.row_count() {
        if feed.i32(row, "ANTENNA_ID")? != 0 || feed.i32(row, "FEED_ID")? != 0 {
            continue;
        }
        let spectral_window = feed.i32(row, "SPECTRAL_WINDOW_ID")?;
        if spectral_window != -1 && spectral_window != spw as i32 {
            continue;
        }
        let center = feed.f64(row, "TIME")?;
        let interval = feed.f64(row, "INTERVAL")?;
        if interval != 0.0 && (time - center).abs() > interval / 2.0 {
            continue;
        }
        let ArrayValue::Float64(values) = feed.array(row, "RECEPTOR_ANGLE")? else {
            return Err(boxed(
                "native EVLA feed receptor angles require Float64 metadata",
            ));
        };
        let value = values
            .iter()
            .next()
            .copied()
            .filter(|value| value.is_finite())
            .ok_or_else(|| boxed("native EVLA feed has no finite receptor-zero angle"))?;
        if angle.is_some_and(|previous: f64| previous.to_bits() != value.to_bits()) {
            return Err(boxed(
                "native EVLA receptor-zero angle is ambiguous for the selected epoch/SPW",
            ));
        }
        angle = Some(value);
    }
    angle.ok_or_else(|| boxed("native EVLA request has no applicable antenna-zero feed metadata"))
}
