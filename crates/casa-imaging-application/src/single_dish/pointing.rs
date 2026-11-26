// SPDX-License-Identifier: LGPL-3.0-or-later
//! Place spectra through the observation's POINTING window, in the image frame.
use super::{TotalPowerError, failure};
use casa_coordinates::{Coordinate, DirectionCoordinate};
use casa_core::measures::{direction::MDirection, frame::MeasFrame};
use casa_imaging_singledish::Pointing;
use casa_observation::{Observation, PointingDirection, PointingRequest, VisibilityBatch};

type ProjectedPointings = (Vec<Option<[f64; 2]>>, Vec<Pointing>);

pub(super) fn pixels(
    observation: &dyn Observation,
    batch: &VisibilityBatch<'_>,
    coordinate: &DirectionCoordinate,
    measures: &std::sync::Arc<dyn casa_core::measures::MeasuresProvider>,
    selected_antennas: &[casa_observation::AntennaId],
) -> Result<ProjectedPointings, TotalPowerError> {
    let times = batch.time.offsets_s();
    let antennas = batch
        .baselines
        .iter()
        .map(|baseline| baseline.antenna1)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let pointing = observation
        .pointing(&PointingRequest {
            partition: batch.partition,
            direction: PointingDirection::Beam,
            antennas,
            times: times[0]..=*times.last().ok_or_else(|| failure("empty batch"))?,
            budget_bytes: 8 << 20,
        })
        .map_err(failure)?;
    let mut pixels = Vec::with_capacity(batch.samples());
    let mut sampling = Vec::new();
    for row in 0..batch.samples() {
        let sample = batch.sample(row);
        if !sample.baseline.is_auto()
            || (!selected_antennas.is_empty()
                && !selected_antennas.contains(&sample.baseline.antenna1))
        {
            pixels.push(None);
            continue;
        }
        let field = &batch.catalogue.fields[sample.field.0 as usize];
        if field.ephemeris.is_some() {
            return Err(TotalPowerError::Unsupported {
                behavior: "moving-source total-power imaging",
                issue: 938,
            });
        }
        let direction = if !pointing.store_has_pointing {
            Some(field.phase_centre.clone())
        } else {
            let series = pointing.antennas.get(&sample.baseline.antenna1);
            let sample_interval = batch.interval_s(row).unwrap_or(0.);
            if let Some(series) = series {
                selected_direction(
                    series,
                    sample.time_offset_s,
                    sample_interval,
                    pointing.frame,
                )?
            } else {
                None
            }
        };
        let Some(direction) = direction else {
            pixels.push(None);
            continue;
        };
        let frame = MeasFrame::new()
            .with_measures(measures.clone())
            .with_epoch(batch.time.get(row / batch.samples_per_time()))
            .with_position(
                batch.catalogue.antennas[sample.baseline.antenna1.0 as usize]
                    .position
                    .clone(),
            );
        let direction = direction
            .convert_to(coordinate.direction_ref(), &frame)
            .map_err(failure)?;
        let (longitude, latitude) = direction.as_angles();
        let pixel = coordinate
            .to_pixel(&[longitude, latitude])
            .map_err(failure)?;
        pixels.push(Some([pixel[0], pixel[1]]));
        sampling.push(Pointing {
            time_s: sample.time_offset_s,
            direction_rad: [longitude, latitude],
        });
    }
    Ok((pixels, sampling))
}

/// CASA's two covering tolerances; interval-less MSv4 pointings use the
/// nearest sample within half a MAIN integration. No interpolation is guessed.
fn selected_direction(
    series: &casa_observation::AntennaPointing,
    time: f64,
    interval: f64,
    frame: casa_core::measures::direction::DirectionRef,
) -> Result<Option<MDirection>, TotalPowerError> {
    let epsilon = time.abs() * f64::EPSILON;
    let selected = if let Some(intervals) = series.intervals_s() {
        [epsilon, interval / 2.].into_iter().find_map(|tolerance| {
            series
                .times_s()
                .iter()
                .enumerate()
                .find(|(i, knot)| {
                    intervals[*i] > 0. && (**knot - time).abs() <= intervals[*i] / 2. + tolerance
                })
                .map(|(i, _)| i)
        })
    } else {
        series
            .times_s()
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| (*a - time).abs().total_cmp(&(*b - time).abs()))
            .filter(|(_, knot)| (**knot - time).abs() <= interval / 2. + epsilon)
            .map(|(i, _)| i)
    };
    let Some(index) = selected else {
        return Ok(None);
    };
    if series.intervals_s().is_some_and(|v| interval < v[index])
        && (series.times_s()[index] - time).abs() > epsilon
    {
        return Err(TotalPowerError::Unsupported {
            behavior: "POINTING time interpolation",
            issue: 938,
        });
    }
    let direction = series.direction_at(index, time);
    if direction.iter().any(|v| !v.is_finite()) {
        return Err(failure("nonfinite pointing direction"));
    }
    Ok(Some(MDirection::from_angles(
        direction[0],
        direction[1],
        frame,
    )))
}
