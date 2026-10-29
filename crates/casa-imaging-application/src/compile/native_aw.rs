// SPDX-License-Identifier: LGPL-3.0-or-later

//! Resolve the native EVLA request from the already selected observation.

use std::io::Read;

use casa_imaging_model::{
    EvlaDishSurface, NativeAwFrequencyGroup, NativeAwGrid, NativeAwRequestInput, NativeAwTerms,
    PolarizationCoordinate,
};
use casa_observation::{AntennaId, Catalogue};
use casa_types::quanta::Unit;

use super::spectral::PreparedSpectralAxis;
use super::survey::{FirstCrossSample, Survey};
use super::{PrepareError, Surveyed};
use crate::{AwCfSource, AwProjection, ImagingRequest};

/// Bounded metadata acquisition; pixel generation remains in the admitted phase.
pub(super) fn resolve(
    request: &ImagingRequest,
    aw: &AwProjection,
    surveyed: &Surveyed<'_>,
    first_cross: FirstCrossSample,
) -> Result<NativeAwRequestInput, PrepareError> {
    let AwCfSource::NativeEvla {
        evla_surface,
        native_cf_working_size,
        native_cf_oversampling,
        native_cf_maximum_cells,
        ..
    } = &aw.cf_source
    else {
        unreachable!("native AW resolves only a native cache");
    };
    let surface = evla_dish(surveyed.catalogue, first_cross, evla_surface)?;
    let frequencies = frequency_groups(surveyed.survey, surveyed.spectral);
    // AW projection images square grids of square cells (the availability
    // gate refuses others), so x stands for both axes.
    let sky_cell = request.cell[0].to_radians() / 3600.0;
    let (w_values, w_increment) = w_grid(
        aw.wprojplanes
            .expect("a validated AW request names its W planes")
            .get(),
        sky_cell,
    );
    let working_cell = sky_cell * *native_cf_oversampling as f64 * request.imsize[0] as f64
        / *native_cf_working_size as f64;
    let pa = surveyed.engine.parallactic_angle(
        first_cross.time_mjd_seconds,
        usize::try_from(first_cross.field_id).expect("a surveyed field is a FIELD row"),
        0,
    )? as f32;
    let feed_angle = receptor_zero_angle(
        surveyed.catalogue,
        surveyed.survey,
        frequencies[0].spectral_window,
    )? as f32;
    Ok(NativeAwRequestInput {
        surface,
        antenna_diameter_m: 25.0,
        frequencies,
        w_values,
        w_increment,
        pa_values: vec![f64::from(pa + feed_angle)],
        mueller_elements: mueller_elements(&request.stokes)?,
        reference_frequency_hz: surveyed.spectral.reference_frequency_hz,
        grid: NativeAwGrid {
            size: *native_cf_working_size,
            sky_increment_rad: [-working_cell, working_cell],
            oversampling: *native_cf_oversampling,
        },
        // The installed A-projection (see `specification::aw_projection`).
        terms: NativeAwTerms {
            aperture: true,
            w_term: true,
            prolate_spheroidal: false,
            wideband: true,
            conjugate_beams: true,
        },
        maximum_cells: *native_cf_maximum_cells,
    })
}

/// The EVLA dish surface, after checking that the observation is EVLA with
/// the homogeneous 25 m dishes the native model describes.
fn evla_dish(
    catalogue: &Catalogue,
    first_cross: FirstCrossSample,
    evla_surface: &std::path::Path,
) -> Result<EvlaDishSurface, PrepareError> {
    let telescope = catalogue
        .observations
        .get(usize::try_from(first_cross.observation_id).expect("an observation is a row"))
        .map(|observation| observation.telescope.trim());
    if telescope != Some("EVLA") {
        return Err(PrepareError::NativeAwTelescope);
    }
    if super::instrument::dish_diameters(catalogue)
        .iter()
        .any(|diameter| *diameter != 25.0)
    {
        return Err(PrepareError::NativeAwDishes);
    }
    // The reference data is an explicit input with bounded acquisition, never
    // discovered through a CASA installation or downloaded during execution.
    let mut bytes = Vec::new();
    std::fs::File::open(evla_surface)
        .and_then(|file| file.take(1_048_577).read_to_end(&mut bytes))
        .map_err(|source| PrepareError::EvlaSurface {
            path: evla_surface.to_path_buf(),
            source,
        })?;
    if bytes.len() > 1_048_576 {
        return Err(PrepareError::NativeAwSurfaceSize);
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| PrepareError::EvlaSurfaceText {
        path: evla_surface.to_path_buf(),
    })?;
    Ok(EvlaDishSurface::from_surface_text(text)?)
}

/// One frequency group per selected SPW, in increasing CF frequency.
fn frequency_groups(
    survey: &Survey,
    spectral: &PreparedSpectralAxis,
) -> Vec<NativeAwFrequencyGroup> {
    let mut frequencies = survey
        .spectral_windows
        .iter()
        .map(|window| {
            let channel_frequencies_hz = spectral
                .window_channels(window.spw_id)
                .iter()
                .map(|index| {
                    *window
                        .frequencies_hz
                        .get(*index)
                        .expect("selected channels index their window's frequency axis")
                })
                .collect::<Vec<_>>();
            // TransformMachines2::makeFreqValList selects each SPW's high endpoint.
            let cf_frequency_hz = channel_frequencies_hz
                .iter()
                .copied()
                .reduce(f64::max)
                .expect("the spectral axis selects at least one channel of every window");
            NativeAwFrequencyGroup {
                spectral_window: u32::try_from(window.spw_id)
                    .expect("SPW ids are nonnegative stored i32 values"),
                channel_frequencies_hz,
                cf_frequency_hz,
            }
        })
        .collect::<Vec<_>>();
    frequencies.sort_by(|left, right| left.cf_frequency_hz.total_cmp(&right.cf_frequency_hz));
    frequencies
}

/// The W values of `planes` planes and their increment. CASA AWConvFunc
/// derives its W grid from the requested field of view, not the observed W
/// envelope: maxUVW=1/(4*sky_increment), w=i²/wScale.
fn w_grid(planes: usize, sky_cell: f64) -> (Vec<f64>, f64) {
    let max_w = 1.0 / (sky_cell * 4.0);
    let w_increment = ((planes - 1) * (planes - 1)) as f32 as f64 / max_w;
    let w_values = (0..planes)
        .map(|index| (index * index) as f64 / w_increment)
        .collect();
    (w_values, w_increment)
}

/// The Mueller elements of the imaged polarization. The catalog routes
/// each hand through its own element and, for the conjugate baseline, the
/// opposite hand (`makeConjPolMap`), so a single-hand image still needs
/// both diagonal elements.
fn mueller_elements(stokes: &[PolarizationCoordinate]) -> Result<Vec<usize>, PrepareError> {
    match stokes {
        [PolarizationCoordinate::CircularRr]
        | [PolarizationCoordinate::CircularLl]
        | [PolarizationCoordinate::StokesI] => Ok(vec![0, 15]),
        _ => Err(PrepareError::NativeAwPolarization),
    }
}

/// Antenna 0's first receptor angle, radians, in the selected partitions
/// of window `spw`: the feed angle CASA's AW projection adds to the
/// parallactic angle. The partitions state their receptors as the store
/// gives them (MSv2 `FEED`, the row applying to the partition's window and
/// feed), and must agree.
fn receptor_zero_angle(
    catalogue: &Catalogue,
    survey: &Survey,
    spw: u32,
) -> Result<f64, PrepareError> {
    let radians = Unit::new("rad").expect("radians are a unit");
    let mut angle = None;
    for selected in &survey.partitions {
        if selected.spectral_window_id() != spw {
            continue;
        }
        let entry = catalogue.partition(selected.partition());
        let Some(receptors) = entry.receptors.get(&AntennaId(0)) else {
            continue;
        };
        let value = receptors
            .angles
            .first()
            .and_then(|angle| angle.get_value_in(&radians).ok())
            .filter(|value| value.is_finite())
            .ok_or(PrepareError::FeedAngleNotFinite)?;
        if angle.is_some_and(|previous: f64| previous.to_bits() != value.to_bits()) {
            return Err(PrepareError::FeedAngleAmbiguous);
        }
        angle = Some(value);
    }
    angle.ok_or(PrepareError::NoApplicableFeed)
}
