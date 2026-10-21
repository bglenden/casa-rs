// SPDX-License-Identifier: LGPL-3.0-or-later
//! The antenna array: `sm.setconfig`.
//!
//! CASA's `Simulator::setconfig` broadcasts single-valued diameters, mounts,
//! offsets and names to every antenna, then `NewMSSimulator::initAnt`
//! appends one ANTENNA row per antenna, converting local positions to ITRF.

use casa_types::measures::MPosition;

use super::error::{SimulatorError, SimulatorResult};

/// How `setconfig`'s antenna coordinates are given (`coordsystem`).
#[derive(Debug, Clone)]
pub enum CoordinateSystem {
    /// ITRF x, y, z in metres (`'global'`).
    Global,
    /// East, north and up offsets in metres from `reference` (`'local'`).
    Local {
        /// The reference position (`referencelocation`).
        reference: MPosition,
    },
    /// Longitude, latitude and height (`'longlat'`), which CASA does not
    /// implement.
    LongLat,
}

/// The arguments of `sm.setconfig`, with CASA's names in the docs.
#[derive(Debug, Clone)]
pub struct ArrayConfig {
    /// The telescope (`telescopename`), written to OBSERVATION and used to
    /// look up the observatory position.
    pub telescope: String,
    /// First coordinate of each antenna (`x`), metres.
    pub x: Vec<f64>,
    /// Second coordinate of each antenna (`y`), metres.
    pub y: Vec<f64>,
    /// Third coordinate of each antenna (`z`), metres.
    pub z: Vec<f64>,
    /// Dish diameters (`dishdiameter`), metres; one value applies to all.
    pub dish_diameter: Vec<f64>,
    /// Axis offsets (`offset`), metres; one value applies to all. CASA does
    /// not write them.
    pub offset: Vec<f64>,
    /// Mounts (`mount`); one value applies to all.
    pub mount: Vec<String>,
    /// Antenna names (`antname`); one value applies to all.
    pub antenna_names: Vec<String>,
    /// Pad (station) names (`padname`); one value applies to all.
    pub pad_names: Vec<String>,
    /// How `x`, `y` and `z` are given (`coordsystem`).
    pub coordinate_system: CoordinateSystem,
}

/// One ANTENNA row.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Antenna {
    pub(crate) name: String,
    pub(crate) station: String,
    pub(crate) mount: String,
    pub(crate) diameter_m: f64,
    pub(crate) position_itrf_m: [f64; 3],
}

impl ArrayConfig {
    /// The ANTENNA rows these arguments add, as CASA would write them.
    pub(crate) fn antennas(&self) -> SimulatorResult<Vec<Antenna>> {
        let count = self.x.len();
        let diameters = broadcast(&self.dish_diameter, count, 0.0);
        let offsets = broadcast(&self.offset, count, 0.0);
        let mounts = broadcast(&self.mount, count, "alt-az".to_string());
        let names = broadcast(&self.antenna_names, count, "UNKNOWN".to_string());
        let pads = broadcast(&self.pad_names, count, "UNKNOWN".to_string());
        for (lengths, what) in [
            (self.y.len(), "y_p"),
            (self.z.len(), "z_p"),
            (diameters.len(), "diam_p"),
            (offsets.len(), "offset_p"),
            (mounts.len(), "mount_p"),
        ] {
            if lengths != count {
                return Err(SimulatorError::raised(format!(
                    "Failed AlwaysAssert (nn == {what}.nelements())"
                )));
            }
        }
        for (lengths, what) in [(names.len(), "antname"), (pads.len(), "padname")] {
            if lengths != count {
                return Err(SimulatorError::raised(format!(
                    "{what} has {lengths} entries for {count} antennas"
                )));
            }
        }
        let positions = match &self.coordinate_system {
            CoordinateSystem::Global => (0..count)
                .map(|antenna| [self.x[antenna], self.y[antenna], self.z[antenna]])
                .collect::<Vec<_>>(),
            CoordinateSystem::Local { reference } => {
                let origin = reference.as_itrf();
                (0..count)
                    .map(|antenna| {
                        local_to_global([self.x[antenna], self.y[antenna], self.z[antenna]], origin)
                    })
                    .collect()
            }
            // CASA logs "not yet implemented" and writes zero positions.
            CoordinateSystem::LongLat => {
                return Err(SimulatorError::raised(
                    "NewMSSimulator::longlat2global not yet implemented",
                ));
            }
        };
        Ok((0..count)
            .map(|antenna| Antenna {
                name: names[antenna].clone(),
                station: pads[antenna].clone(),
                mount: mounts[antenna].clone(),
                diameter_m: diameters[antenna],
                position_itrf_m: positions[antenna],
            })
            .collect())
    }
}

/// `values` for `count` antennas: one value repeated, an empty list as
/// `empty`, and anything else as given.
fn broadcast<T: Clone>(values: &[T], count: usize, empty: T) -> Vec<T> {
    match values {
        [] => vec![empty; count],
        [value] => vec![value.clone(); count],
        _ => values.to_vec(),
    }
}

/// casacore's `NewMSSimulator::local2global`: east, north and up offsets
/// rotated by the geocentric longitude and latitude of the ITRF origin
/// (`MVPosition::getAngle`), then added to it.
pub(crate) fn local_to_global(local: [f64; 3], origin: [f64; 3]) -> [f64; 3] {
    let [x, y, z] = local;
    let longitude = origin[1].atan2(origin[0]);
    let latitude = origin[2].atan2(origin[0].hypot(origin[1]));
    let (sin_lon, cos_lon) = longitude.sin_cos();
    let (sin_lat, cos_lat) = latitude.sin_cos();
    let x_g1 = -sin_lat * y + cos_lat * z;
    let y_g1 = x;
    [
        cos_lon * x_g1 - sin_lon * y_g1 + origin[0],
        sin_lon * x_g1 + cos_lon * y_g1 + origin[1],
        cos_lat * y + sin_lat * z + origin[2],
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_offsets_rotate_about_the_origin() {
        // At (lon, lat) = (0, 0), east is +y, north +z and up +x.
        let origin = [6_378_137.0, 0.0, 0.0];
        let global = local_to_global([100.0, 200.0, 3.0], origin);
        assert!((global[0] - (origin[0] + 3.0)).abs() < 1e-9);
        assert!((global[1] - 100.0).abs() < 1e-9);
        assert!((global[2] - 200.0).abs() < 1e-9);
    }

    #[test]
    fn single_values_apply_to_every_antenna() {
        assert_eq!(broadcast(&[25.0], 3, 0.0), vec![25.0; 3]);
        assert_eq!(broadcast::<f64>(&[], 2, 0.0), vec![0.0; 2]);
        assert_eq!(broadcast(&[1.0, 2.0], 3, 0.0), vec![1.0, 2.0]);
    }
}
