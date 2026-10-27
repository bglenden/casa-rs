// SPDX-License-Identifier: LGPL-3.0-or-later
//! The antenna pointing pixels CASA's `AWProjectFT` grids with: each
//! antenna's pointing on each chart, grouped into bins of the pointing
//! offset tolerance, and regrouped only when an epoch's pointing has moved
//! (`AWProjectFT::findPointingOffsets`, `PointingOffsets`).

use std::collections::BTreeMap;
use std::sync::Arc;

use casa_coordinates::{
    Coordinate, DirectionCoordinate, Projection as CoordinateProjection, ProjectionType,
};
use casa_types::measures::direction::DirectionRef;
use ndarray::arr2;

use crate::compiled_problem::CompiledProblem;
use crate::geometry::{
    DirectionCoordinateSpec, DirectionFrame, PointingCentreLaw, Projection, SkyDirection,
};

/// Why AW pointing pixels cannot be formed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AwPointingError {
    /// A chart is not a SIN projection.
    #[error("an AW-projection chart is not a SIN projection")]
    UnsupportedProjection,
    /// A pointing does not land on a chart's pixels.
    #[error("an antenna's pointing has no finite pixel on a chart")]
    NonFinitePixel,
    /// An epoch has no antennas, or the grouping tolerance is not usable.
    #[error("an AW pointing epoch has no antennas or no usable grouping")]
    EmptyGroup,
    /// A count overflowed.
    #[error("an AW pointing count overflowed")]
    Overflow,
}

/// Each antenna's grouped pixel on each chart, charts domain-major with
/// each domain's facets in turn.
pub type AwChartPixels<K> = Arc<[BTreeMap<K, [f64; 2]>]>;

/// What an AW-projection run with pointing groups by: its charts and the
/// contract's tolerances.
#[derive(Clone, Debug)]
pub struct AwPointingGrouping {
    charts: Vec<(DirectionCoordinate, [f64; 2])>,
    grouping_arcsec: f64,
    primary_increment_norm: f64,
    refresh_threshold_rad: f64,
}

impl AwPointingGrouping {
    /// The grouping of `problem`, when it grids with AW projection and its
    /// antennas' own pointing (an observation pointing law); `None`
    /// otherwise.
    ///
    /// # Errors
    ///
    /// [`AwPointingError::UnsupportedProjection`] for a chart that is not
    /// a SIN projection.
    pub fn new(problem: &CompiledProblem) -> Result<Option<Self>, AwPointingError> {
        let Some(contract) = problem
            .science()
            .measurement_equation()
            .aw_projection()
            .filter(|contract| contract.use_pointing())
        else {
            return Ok(None);
        };
        if !matches!(
            problem.geometry().centres().pointing(),
            PointingCentreLaw::Observation(_)
        ) {
            return Ok(None);
        }
        let domains = problem.geometry().domains();
        let charts = domains
            .iter()
            .flat_map(|domain| domain.facets().iter().map(|facet| facet.direction()))
            .map(|spec| Ok((chart(spec)?, spec.increment_rad())))
            .collect::<Result<Vec<_>, _>>()?;
        let [grouping_arcsec, refresh_arcsec] = contract.pointing_offset_sigdev_arcsec();
        let primary_increment = domains
            .first()
            .ok_or(AwPointingError::EmptyGroup)?
            .direction()
            .increment_rad();
        Ok(Some(Self {
            charts,
            grouping_arcsec: if grouping_arcsec > 0.0 {
                grouping_arcsec
            } else {
                1.0e-3
            },
            primary_increment_norm: primary_increment[0].hypot(primary_increment[1]),
            refresh_threshold_rad: refresh_arcsec.to_radians() / 3600.0,
        }))
    }

    /// Each antenna's pixel on each chart for its J2000 pointing.
    ///
    /// # Errors
    ///
    /// [`AwPointingError::NonFinitePixel`] for a pointing off a chart.
    pub fn pixels<K: Ord + Copy>(
        &self,
        directions: &BTreeMap<K, SkyDirection>,
    ) -> Result<Vec<BTreeMap<K, [f64; 2]>>, AwPointingError> {
        self.charts
            .iter()
            .map(|(coordinate, _)| {
                directions
                    .iter()
                    .map(|(&antenna, direction)| {
                        coordinate
                            .to_pixel(&[direction.longitude_rad(), direction.latitude_rad()])
                            .ok()
                            .and_then(|pixel| {
                                pixel
                                    .get(0..2)
                                    .filter(|pixel| pixel.iter().all(|value| value.is_finite()))
                                    .map(|pixel| (antenna, [pixel[0], pixel[1]]))
                            })
                            .ok_or(AwPointingError::NonFinitePixel)
                    })
                    .collect()
            })
            .collect()
    }
}

fn chart(spec: DirectionCoordinateSpec) -> Result<DirectionCoordinate, AwPointingError> {
    if spec.projection() != Projection::Sin {
        return Err(AwPointingError::UnsupportedProjection);
    }
    let reference = spec.reference_direction();
    let frame = match reference.frame() {
        DirectionFrame::Icrs => DirectionRef::ICRS,
        DirectionFrame::J2000 => DirectionRef::J2000,
        DirectionFrame::B1950 => DirectionRef::B1950,
        DirectionFrame::Galactic => DirectionRef::GALACTIC,
    };
    Ok(DirectionCoordinate::new(
        frame,
        CoordinateProjection::new(ProjectionType::SIN),
        [reference.longitude_rad(), reference.latitude_rad()],
        spec.increment_rad(),
        spec.reference_pixel(),
    )
    .with_pc_matrix(arr2(&spec.pc()))
    .with_longpole(spec.pole_deg()[0].to_radians())
    .with_latpole(spec.pole_deg()[1].to_radians()))
}

/// The grouped pixels of consecutive AW pointing epochs: an epoch keeps the
/// previous epoch's groups unless its field, its row count or its antennas
/// changed, or its antennas' mean pointing on the first chart moved by the
/// refresh tolerance or more.
#[derive(Clone, Debug)]
pub struct AwPointingEpochs<K> {
    field: Option<u32>,
    row_count: usize,
    antenna_pixels: BTreeMap<K, [f64; 2]>,
    grouped: AwChartPixels<K>,
}

impl<K> Default for AwPointingEpochs<K> {
    fn default() -> Self {
        Self {
            field: None,
            row_count: 0,
            antenna_pixels: BTreeMap::new(),
            grouped: Arc::from([]),
        }
    }
}

impl<K: Ord + Copy> AwPointingEpochs<K> {
    /// The grouped pixels of the next epoch: `row_count` rows of `field`
    /// whose antennas' pixels on each chart are `pixels_by_chart`
    /// ([`AwPointingGrouping::pixels`]).
    ///
    /// # Errors
    ///
    /// [`AwPointingError::EmptyGroup`] for an epoch without antennas.
    pub fn next(
        &mut self,
        grouping: &AwPointingGrouping,
        field: u32,
        row_count: usize,
        pixels_by_chart: &[BTreeMap<K, [f64; 2]>],
    ) -> Result<AwChartPixels<K>, AwPointingError> {
        let current = pixels_by_chart.first().ok_or(AwPointingError::EmptyGroup)?;
        let same_antennas = self.antenna_pixels.len() == current.len()
            && self.antenna_pixels.keys().eq(current.keys());
        let refresh = if self.field != Some(field) || self.row_count != row_count || !same_antennas
        {
            true
        } else {
            let residual = self.antenna_pixels.iter().zip(current).fold(
                [0.0, 0.0],
                |[sum_x, sum_y], ((_, cached), (_, current))| {
                    [
                        sum_x + cached[0] - current[0],
                        sum_y + cached[1] - current[1],
                    ]
                },
            );
            residual[0].hypot(residual[1]) / current.len().max(1) as f64
                * grouping.primary_increment_norm
                >= grouping.refresh_threshold_rad
        };
        if refresh {
            let grouped = pixels_by_chart
                .iter()
                .zip(&grouping.charts)
                .map(|(pixels, (_, increment))| {
                    aw_grouped_pixels(pixels, grouping.grouping_arcsec, *increment)
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.grouped = Arc::from(grouped);
        }
        self.field = Some(field);
        self.row_count = row_count;
        self.antenna_pixels.clone_from(current);
        Ok(Arc::clone(&self.grouped))
    }
}

/// CASA's grouping of antenna pixels into bins `grouping_arcsec` wide from
/// the lowest pixel, on a chart of `increment_rad` pixels, each antenna
/// given its bin's mean, accumulated in single precision as CASA
/// accumulates it.
///
/// # Errors
///
/// [`AwPointingError::EmptyGroup`] for no pixels or an unusable width.
pub fn aw_grouped_pixels<K: Ord + Copy>(
    pixels: &BTreeMap<K, [f64; 2]>,
    grouping_arcsec: f64,
    increment_rad: [f64; 2],
) -> Result<BTreeMap<K, [f64; 2]>, AwPointingError> {
    let bin_width =
        grouping_arcsec.to_radians() / 3600.0 / increment_rad[0].hypot(increment_rad[1]);
    if !(bin_width.is_finite() && bin_width > 0.0) || pixels.is_empty() {
        return Err(AwPointingError::EmptyGroup);
    }
    let min_x = pixels
        .values()
        .map(|pixel| pixel[0])
        .fold(f64::INFINITY, f64::min);
    let min_y = pixels
        .values()
        .map(|pixel| pixel[1])
        .fold(f64::INFINITY, f64::min);
    let mut bins = BTreeMap::<K, (i64, i64)>::new();
    let mut sums = BTreeMap::<(i64, i64), (f32, f32, usize)>::new();
    for (&antenna, &pixel) in pixels {
        let bin = (
            ((pixel[0] - min_x) / bin_width).floor() as i64,
            ((pixel[1] - min_y) / bin_width).floor() as i64,
        );
        bins.insert(antenna, bin);
        let sum = sums.entry(bin).or_insert((0.0, 0.0, 0));
        sum.0 = (f64::from(sum.0) + pixel[0]) as f32;
        sum.1 = (f64::from(sum.1) + pixel[1]) as f32;
        sum.2 = sum.2.checked_add(1).ok_or(AwPointingError::Overflow)?;
    }
    let means = sums
        .into_iter()
        .map(|(bin, (x, y, count))| (bin, [(x / count as f32) as f64, (y / count as f32) as f64]))
        .collect::<BTreeMap<_, _>>();
    Ok(bins
        .into_iter()
        .map(|(antenna, bin)| (antenna, means[&bin]))
        .collect())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::aw_grouped_pixels as group;

    /// CASA's default tolerance collapses a compact array to one bin whose
    /// mean is accumulated in single precision.
    #[test]
    fn casa_default_pointing_group_collapses_compact_array_to_f32_mean() {
        let pixels = BTreeMap::from([
            (0, [256.125_000_01, 255.875_000_01]),
            (1, [257.250_000_02, 256.500_000_02]),
            (2, [255.750_000_03, 257.125_000_03]),
        ]);
        let grouped = group(
            &pixels,
            600.0,
            [0.6_f64.to_radians() / 3600.0, 0.6_f64.to_radians() / 3600.0],
        )
        .expect("CASA default pointing grouping");
        let casa_mean = |values: [f64; 3]| {
            let sum = values
                .into_iter()
                .fold(0.0_f32, |sum, value| (f64::from(sum) + value) as f32);
            f64::from(sum / 3.0)
        };
        let expected_x = casa_mean([256.125_000_01, 257.250_000_02, 255.750_000_03]);
        let expected_y = casa_mean([255.875_000_01, 256.500_000_02, 257.125_000_03]);
        assert_eq!(grouped.len(), pixels.len());
        assert!(
            grouped
                .values()
                .all(|pixel| *pixel == [expected_x, expected_y])
        );
    }

    /// Antennas further apart than the bin width keep their own bins.
    #[test]
    fn casa_pointing_group_keeps_separated_bins_independent() {
        let pixels = BTreeMap::from([(0, [10.0, 20.0]), (1, [10.2, 20.2]), (2, [30.0, 40.0])]);
        let grouped = group(&pixels, 1.0, [1.0_f64.to_radians() / 3600.0, 0.0])
            .expect("separated CASA pointing groups");
        assert_eq!(
            grouped[&0],
            [10.100_000_381_469_727, 20.100_000_381_469_727]
        );
        assert_eq!(grouped[&1], grouped[&0]);
        assert_eq!(grouped[&2], [30.0, 40.0]);
    }
}
