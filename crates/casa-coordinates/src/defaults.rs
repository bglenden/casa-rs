// SPDX-License-Identifier: LGPL-3.0-or-later
//! Default coordinate systems, a port of casacore's `CoordinateUtil`
//! (`coordinates/Coordinates/CoordinateUtil.cc`).
//!
//! CASA builds an image's coordinate system from these when a script gives
//! none (`ia.fromshape`, `ia.fromarray`), and `coordsys.newcoordsys` adds
//! these coordinates. They are casacore's: a J2000 SIN direction at the
//! origin with 1 arcmin pixels, Stokes I, Q, U and V as the axis length
//! allows, an LSRK frequency of 1415 MHz in 1 kHz channels with the HI line
//! as rest frequency, and linear axes in km.

use casa_types::measures::direction::DirectionRef;
use casa_types::measures::epoch::{EpochRef, MEpoch};
use casa_types::measures::frequency::FrequencyRef;

use crate::coordinate_system::CoordinateSystem;
use crate::direction::DirectionCoordinate;
use crate::linear::LinearCoordinate;
use crate::obs_info::ObsInfo;
use crate::projection::{Projection, ProjectionType};
use crate::spectral::SpectralCoordinate;
use crate::stokes::{StokesCoordinate, StokesType};

/// The HI line's rest frequency in Hz, casacore's `QC::HI()`
/// (1420.405751786 MHz) as CASA 6.7.6.14 reports it in Hz after casacore's
/// unit conversion, `coordsys.newcoordsys(spectral=True)` then
/// `restfrequency()`: 1420405751.7860003, one ulp above the literal.
pub const HI_REST_FREQUENCY_HZ: f64 = 1_420_405_751.786_000_3;

/// The reference frequency of the default spectral axis, Hz.
const DEFAULT_FREQUENCY_HZ: f64 = 1415.0e6;

/// The channel width of the default spectral axis, Hz.
const DEFAULT_CHANNEL_HZ: f64 = 1.0e3;

/// One arcminute in radians.
const ARCMIN: f64 = std::f64::consts::PI / 10_800.0;

/// The default direction coordinate, casacore's `addDirAxes`: J2000, SIN,
/// the reference at RA 0 and Dec 0 on pixel (0, 0), increments of -1 and
/// +1 arcmin, presented in arcmin. The coordinate trait still returns its
/// values and increments in radians.
pub fn direction_coordinate() -> DirectionCoordinate {
    DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [0.0, 0.0],
        [-ARCMIN, ARCMIN],
        [0.0, 0.0],
    )
    // wcslib's LATPOLE for a zenithal projection is the reference latitude.
    .with_latpole(0.0)
    .with_world_units(["'", "'"])
    .expect("arcmin is an angle unit")
}

/// The default Stokes coordinate for an axis of `length` pixels,
/// casacore's `addStokesAxis`: I, then Q, U and V; `None` for a length
/// outside 1 to 4.
pub fn stokes_coordinate(length: usize) -> Option<StokesCoordinate> {
    const ORDER: [StokesType; 4] = [StokesType::I, StokesType::Q, StokesType::U, StokesType::V];
    (1..=4)
        .contains(&length)
        .then(|| StokesCoordinate::new(ORDER[..length].to_vec()))
}

/// The default spectral coordinate, casacore's `addFreqAxis`: LSRK
/// frequency, 1415 MHz at pixel 0, 1 kHz channels, the HI line as rest
/// frequency.
pub fn spectral_coordinate() -> SpectralCoordinate {
    SpectralCoordinate::new(
        FrequencyRef::LSRK,
        DEFAULT_FREQUENCY_HZ,
        DEFAULT_CHANNEL_HZ,
        0.0,
        HI_REST_FREQUENCY_HZ,
    )
}

/// The default linear coordinate over axes `names`, casacore's
/// `addLinearAxes`: reference value 0 and increment 1 in km on each axis,
/// the reference pixel `(length + 1) / 2` (integer division) of each axis
/// when `shape` gives one length per name, else 0.
///
/// # Panics
///
/// Panics if `names` is empty, because a linear coordinate needs an axis.
pub fn linear_coordinate(names: &[String], shape: &[usize]) -> LinearCoordinate {
    let n = names.len();
    let crpix = if shape.len() == n {
        shape
            .iter()
            .map(|&length| length.div_ceil(2) as f64)
            .collect()
    } else {
        vec![0.0; n]
    };
    LinearCoordinate::new(n, names.to_vec(), vec!["km".to_string(); n]).with_reference_pixel(crpix)
}

/// The observation information casacore's `makeCoordinateSystem` attaches:
/// observer Karl Jansky, telescope ALMA, observed 2000-01-01 00:00:00.0001
/// UTC. casacore also sets the telescope's position from its observatory
/// table; that needs the measures data, so callers that have it add it.
pub fn obs_info() -> ObsInfo {
    // `Time(2000, 1, 1, 0, 0, 0.0001)`: MJD 51544 and 0.1 ms, which keeps
    // the time from rounding to the previous day.
    let date = MEpoch::from_mjd(51_544.0 + 1.0e-4 / 86_400.0, EpochRef::UTC);
    ObsInfo::new("ALMA")
        .with_observer("Karl Jansky")
        .with_date(date)
}

/// The names `linear1`, `linear2`, ... of `n` linear axes.
fn linear_names(n: usize) -> Vec<String> {
    (1..=n).map(|axis| format!("linear{axis}")).collect()
}

/// The coordinate system casacore's `CoordinateUtil::makeCoordinateSystem`
/// makes for an image of `shape`, with [`obs_info`].
///
/// This is the raw coordinate utility default, not the complete behavior of
/// `ia.fromshape` or `ia.fromarray`. An image-layer caller must apply CASA's
/// image-specific reference-pixel recentering after it builds this system.
///
/// With `linear`, every axis is linear. Otherwise one axis is spectral; two
/// or more start with a direction; a third axis is Stokes when its length is
/// 1 to 4, else spectral; a fourth is spectral after a Stokes axis, else
/// Stokes when it can be, else linear; the rest are linear.
pub fn coordinate_system_for_shape(shape: &[usize], linear: bool) -> CoordinateSystem {
    let n = shape.len();
    let mut csys = CoordinateSystem::new().with_obs_info(obs_info());
    if linear {
        if n > 0 {
            csys.add_coordinate(linear_coordinate(&linear_names(n), shape));
        }
        return csys;
    }
    if n == 1 {
        csys.add_coordinate(spectral_coordinate());
        return csys;
    }
    if n >= 2 {
        csys.add_coordinate(direction_coordinate());
    }
    let mut done_stokes = false;
    let mut done_frequency = false;
    if n >= 3 {
        if let Some(stokes) = stokes_coordinate(shape[2]) {
            csys.add_coordinate(stokes);
            done_stokes = true;
        } else {
            csys.add_coordinate(spectral_coordinate());
            done_frequency = true;
        }
    }
    let mut n_done = 0;
    if n >= 4 {
        n_done = 4;
        if done_stokes {
            csys.add_coordinate(spectral_coordinate());
        } else if let Some(stokes) = stokes_coordinate(shape[3]) {
            csys.add_coordinate(stokes);
        } else if !done_frequency {
            csys.add_coordinate(spectral_coordinate());
        } else {
            n_done = 3;
        }
    }
    if n_done == 3 || n >= 5 {
        let rest = &shape[n_done..];
        if !rest.is_empty() {
            csys.add_coordinate(linear_coordinate(&linear_names(rest.len()), rest));
        }
    }
    csys
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinate::{Coordinate, CoordinateType};

    fn types(csys: &CoordinateSystem) -> Vec<CoordinateType> {
        (0..csys.n_coordinates())
            .map(|index| csys.coordinate(index).coordinate_type())
            .collect()
    }

    #[test]
    fn shapes_get_casacores_coordinates() {
        use CoordinateType::{Direction, Linear, Spectral, Stokes};
        let cases: [(&[usize], &[CoordinateType]); 10] = [
            (&[10], &[Spectral]),
            (&[4, 5], &[Direction]),
            (&[4, 5, 3], &[Direction, Stokes]),
            (&[4, 5, 6], &[Direction, Spectral]),
            (&[4, 5, 3, 2], &[Direction, Stokes, Spectral]),
            (&[4, 5, 6, 3], &[Direction, Spectral, Stokes]),
            (&[4, 5, 6, 7], &[Direction, Spectral, Linear]),
            (&[4, 5, 6, 3, 7], &[Direction, Spectral, Stokes, Linear]),
            (&[4, 5, 6, 5, 3], &[Direction, Spectral, Linear]),
            (&[4, 5, 4, 6, 3], &[Direction, Stokes, Spectral, Linear]),
        ];
        for (shape, expected) in cases {
            let csys = coordinate_system_for_shape(shape, false);
            assert_eq!(types(&csys), expected, "{shape:?}");
            assert_eq!(csys.n_pixel_axes(), shape.len(), "{shape:?}");
        }
        let linear = coordinate_system_for_shape(&[4, 5], true);
        assert_eq!(types(&linear), [Linear]);
        assert_eq!(linear.coordinate(0).reference_pixel(), [2.0, 3.0]);
        assert_eq!(linear.coordinate(0).axis_names(), ["linear1", "linear2"]);

        let no_axes = coordinate_system_for_shape(&[], false);
        assert_eq!(no_axes.n_coordinates(), 0);
        assert_eq!(no_axes.n_pixel_axes(), 0);

        let mismatched_shape = linear_coordinate(&["linear1".into(), "linear2".into()], &[4]);
        assert_eq!(mismatched_shape.reference_pixel(), [0.0, 0.0]);
    }

    #[test]
    fn defaults_carry_casacores_values() {
        let direction = direction_coordinate();
        assert_eq!(direction.world_units(), &["'".to_string(), "'".to_string()]);
        assert!((direction.increment()[0] + ARCMIN).abs() < 1e-18);
        assert_eq!(direction.reference_pixel(), [0.0, 0.0]);
        let spectral = spectral_coordinate();
        assert_eq!(spectral.rest_frequency(), 1_420_405_751.786_000_3);
        assert_eq!(spectral.reference_value(), [1.415e9]);
        let stokes = stokes_coordinate(3).expect("three Stokes");
        assert_eq!(
            stokes.stokes(),
            [StokesType::I, StokesType::Q, StokesType::U]
        );
        assert!(stokes_coordinate(0).is_none());
        assert_eq!(
            stokes_coordinate(1).expect("one Stokes").stokes(),
            [StokesType::I]
        );
        assert_eq!(
            stokes_coordinate(4).expect("four Stokes").stokes(),
            [StokesType::I, StokesType::Q, StokesType::U, StokesType::V]
        );
        assert!(stokes_coordinate(5).is_none());
        let info = obs_info();
        assert_eq!(
            (info.telescope.as_str(), info.observer.as_str()),
            ("ALMA", "Karl Jansky")
        );
        let date = info.date.as_ref().expect("the default epoch");
        assert_eq!(date.refer(), EpochRef::UTC);
        assert!((date.value().as_mjd() - (51_544.0 + 1.0e-4 / 86_400.0)).abs() < 1e-15);
        assert!(info.telescope_position.is_none());

        let system = coordinate_system_for_shape(&[4, 5, 3, 2], false);
        let pixel = vec![0.0; 4];
        let world = system
            .to_world(&pixel)
            .expect("the default maps its origin");
        let round_trip = system.to_pixel(&world).expect("the mapping is invertible");
        assert!(
            round_trip
                .iter()
                .zip(pixel)
                .all(|(got, expected)| (got - expected).abs() < 1e-12),
            "{round_trip:?} is not within 1e-12 pixel of the origin"
        );
    }
}
