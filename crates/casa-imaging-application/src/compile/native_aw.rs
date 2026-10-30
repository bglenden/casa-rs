// SPDX-License-Identifier: LGPL-3.0-or-later

//! Resolve the native EVLA request from the already selected observation.

use std::io::Read;

use casa_imaging_model::{
    EvlaDishSurface, NativeAwFrequencyGroup, NativeAwGrid, NativeAwRequestInput, NativeAwTerms,
    PolarizationCoordinate,
};
use casa_ms::observation_io::Msv2Observation;
use casa_observation::{AntennaId, Catalogue, Receptors, WindowId};
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
        surveyed.msv2,
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

/// Antenna 0's first receptor angle in window `spw`, radians: the feed
/// angle CASA's AW projection adds to the parallactic angle. CASA reads it
/// as casacore's `MSIter` states feed 0's (`feed_pa`:
/// `receptorAngles().xyPlane(0)`), whatever feeds MAIN's rows name and
/// whether or not antenna 0 has a selected baseline; on an MSv2 the adapter
/// states that `FEED` row ([`Msv2Observation::feed_receptors`]). An MSv4
/// numbers no feeds: each partition's `antenna_xds` states the receptors of
/// its antennas, so there the angle is antenna 0's as the window's
/// partitions, selected or not, state it, which must agree.
fn receptor_zero_angle(
    catalogue: &Catalogue,
    msv2: Option<&Msv2Observation>,
    spw: u32,
) -> Result<f64, PrepareError> {
    let radians = Unit::new("rad").expect("radians are a unit");
    let first_angle = |receptors: &Receptors| {
        receptors
            .angles
            .first()
            .and_then(|angle| angle.get_value_in(&radians).ok())
            .filter(|value| value.is_finite())
            .ok_or(PrepareError::FeedAngleNotFinite)
    };
    if let Some(ms) = msv2 {
        return ms
            .feed_receptors(AntennaId(0), 0, WindowId(spw))
            .ok_or(PrepareError::NoApplicableFeed)
            .and_then(first_angle);
    }
    let mut angle = None;
    for entry in catalogue
        .partitions
        .iter()
        .filter(|entry| entry.window == WindowId(spw))
    {
        let Some(receptors) = entry.receptors.get(&AntennaId(0)) else {
            continue;
        };
        let value = first_angle(receptors)?;
        if angle.is_some_and(|previous: f64| previous.to_bits() != value.to_bits()) {
            return Err(PrepareError::FeedAngleAmbiguous);
        }
        angle = Some(value);
    }
    angle.ok_or(PrepareError::NoApplicableFeed)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use casa_ms::observation_io::Msv4Observation;
    use casa_ms::observation_io::convert::{ConversionOptions, LossKind, PaddingRows, to_msv4};
    use casa_ms::{MeasurementSet, SubtableId};
    use casa_types::{ArrayValue, ScalarValue, Value};

    use super::*;

    /// `ms_trial.ms`, unpacked into `root`, with antenna 0's feed 0 at
    /// `angle` on both receptors.
    fn trial(root: &Path, angle: f64) -> (PathBuf, MeasurementSet) {
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../casa-ms/tests/fixtures/ms_trial/ms_trial.ms.tgz");
        tar::Archive::new(flate2::read::GzDecoder::new(
            std::fs::File::open(fixture).expect("the fixture"),
        ))
        .unpack(root)
        .expect("unpacked");
        let path = root.join("ms_trial.ms");
        let mut ms = MeasurementSet::open(&path).expect("an editable MeasurementSet");
        let rows: Vec<usize> = {
            let feed = ms.feed().expect("FEED");
            (0..feed.row_count())
                .filter(|row| {
                    feed.i32(*row, "ANTENNA_ID").expect("ANTENNA_ID") == 0
                        && feed.i32(*row, "FEED_ID").expect("FEED_ID") == 0
                })
                .collect()
        };
        assert!(!rows.is_empty(), "antenna 0 has a feed 0");
        let feed = ms.subtable_mut(SubtableId::Feed).expect("FEED");
        for row in rows {
            feed.cell_accessor_mut(row, "RECEPTOR_ANGLE")
                .expect("RECEPTOR_ANGLE")
                .set(Value::Array(ArrayValue::Float64(
                    ndarray::arr1(&[angle, angle]).into_dyn(),
                )))
                .expect("set");
        }
        (path, ms)
    }

    fn msv2_angle(path: &Path) -> Result<f64, PrepareError> {
        let observation = Msv2Observation::open(path).expect("the MeasurementSet opens");
        let window = observation.catalogue().partitions[0].window;
        receptor_zero_angle(observation.catalogue(), Some(&observation), window.0)
    }

    /// CASA reads feed 0's angle whatever feeds MAIN's rows name (Astra's
    /// review of #1013): with MAIN on feed 1 at 0.2 rad, the angle is
    /// still feed 0's.
    #[test]
    fn native_aw_reads_feed_zero_whatever_feeds_main_names() {
        let root = tempfile::tempdir().expect("a temporary directory");
        let (path, mut ms) = trial(root.path(), 0.0);
        let feed = ms.subtable_mut(SubtableId::Feed).expect("FEED");
        for row in 0..feed.row_count() {
            let mut copy = feed.row_accessor().row(row).expect("a FEED row").clone();
            copy.upsert("FEED_ID", Value::Scalar(ScalarValue::Int32(1)));
            copy.upsert(
                "RECEPTOR_ANGLE",
                Value::Array(ArrayValue::Float64(ndarray::arr1(&[0.2, 0.2]).into_dyn())),
            );
            feed.add_row(copy).expect("feed 1");
        }
        let rows = ms.row_count();
        for column in ["FEED1", "FEED2"] {
            ms.main_table_mut()
                .fill_column_range(
                    column,
                    casa_tables::RowRange::new(0, rows),
                    Value::Scalar(ScalarValue::Int32(1)),
                )
                .expect("MAIN on feed 1");
        }
        ms.save().expect("saved");
        drop(ms);
        assert_eq!(msv2_angle(&path).expect("an angle"), 0.0);
    }

    /// CASA reads antenna 0's feed whether or not antenna 0 has a selected
    /// baseline (Astra's review of #1013).
    #[test]
    fn native_aw_reads_antenna_zero_without_its_baselines() {
        let root = tempfile::tempdir().expect("a temporary directory");
        let (path, mut ms) = trial(root.path(), 0.3);
        let antenna = |ms: &MeasurementSet, column: &str, row: usize| match ms
            .main_table()
            .column_accessor(column)
            .expect("an antenna column")
            .get(row)
            .expect("a row")
        {
            Some(Value::Scalar(ScalarValue::Int32(id))) => *id,
            other => panic!("{column} is {other:?}"),
        };
        let rows: Vec<usize> = (0..ms.row_count())
            .filter(|row| {
                antenna(&ms, "ANTENNA1", *row) == 0 || antenna(&ms, "ANTENNA2", *row) == 0
            })
            .collect();
        assert!(rows.len() < ms.row_count(), "other baselines remain");
        ms.main_table_mut()
            .remove_rows(&rows)
            .expect("antenna 0's rows removed");
        ms.save().expect("saved");
        drop(ms);
        assert_eq!(msv2_angle(&path).expect("an angle"), 0.3);
    }

    /// An MSv4 states the angle its MeasurementSet's feed 0 does.
    #[test]
    fn an_msv4_states_the_feed_angle_of_its_measurement_set() {
        let root = tempfile::tempdir().expect("a temporary directory");
        let (path, mut ms) = trial(root.path(), 0.3);
        ms.save().expect("saved");
        drop(ms);
        let v4 = root.path().join("ms_trial.ps.zarr");
        to_msv4(
            &path,
            &v4,
            &ConversionOptions {
                data_groups: None,
                accept: vec![LossKind::Sigma, LossKind::Tables],
                batch_bytes: 1 << 20,
                workers: 1,
                chunks: [0, 0, 0, 0],
                codec: casa_ms::observation_io::Msv4Codec::BloscLz4,
                padding: PaddingRows::Flagged,
                creator: "feed angle law".into(),
                creation_date: "2026-10-30T00:00:00+00:00".into(),
            },
        )
        .expect("converted");
        let converted = Msv4Observation::open(&v4).expect("the processing set opens");
        let window = converted.catalogue().partitions[0].window;
        assert_eq!(
            receptor_zero_angle(converted.catalogue(), None, window.0).expect("an angle"),
            msv2_angle(&path).expect("an angle")
        );
    }
}
