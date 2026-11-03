// SPDX-License-Identifier: LGPL-3.0-or-later
//! Observatory catalog data loaded from casacore `geodetic/Observatories`.

use std::mem::size_of;

use casa_types::measures::ObservatoryPosition;

/// A single observatory row from casacore `geodetic/Observatories`.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservatoryEntry {
    /// Table `MJD` column.
    pub mjd: f64,
    /// Table `Name` column.
    pub name: String,
    /// Table `Type` column.
    pub observatory_type: String,
    /// Table `Long` column, in degrees.
    pub longitude_deg: f64,
    /// Table `Lat` column, in degrees.
    pub latitude_deg: f64,
    /// Table `Height` column, in metres.
    pub height_m: f64,
    /// Table `X` column, in metres.
    pub x_m: f64,
    /// Table `Y` column, in metres.
    pub y_m: f64,
    /// Table `Z` column, in metres.
    pub z_m: f64,
    /// Table `Source` column.
    pub source: String,
    /// Table `Comment` column.
    pub comment: String,
    /// Table `AntennaResponses` column.
    pub antenna_responses: String,
}

impl ObservatoryEntry {
    /// The observatory's position, as casacore's `MeasTable` reads it: the
    /// `Long`, `Lat` and `Height` columns for either `Type`.
    ///
    /// For a `WGS84` entry they are geodetic longitude, latitude and height
    /// above the ellipsoid. For an `ITRF` entry casacore takes them as a
    /// spherical vector, `Height` its length from the Earth's centre, and
    /// does not read `X`, `Y` and `Z`; so does this. Where an entry's two
    /// descriptions disagree, its position is CASA's, recorded in
    /// `docs/CASA (C++) bugs.md`. `None` for any other `Type`.
    pub fn position(&self) -> Option<ObservatoryPosition> {
        let observatory_type = self.observatory_type.trim();
        let longitude_rad = self.longitude_deg.to_radians();
        let latitude_rad = self.latitude_deg.to_radians();
        if observatory_type.eq_ignore_ascii_case("ITRF") {
            let (sin_lon, cos_lon) = longitude_rad.sin_cos();
            let (sin_lat, cos_lat) = latitude_rad.sin_cos();
            // casacore's MVPosition order: the unit vector, then its length.
            Some(ObservatoryPosition::Itrf {
                x_m: cos_lon * cos_lat * self.height_m,
                y_m: sin_lon * cos_lat * self.height_m,
                z_m: sin_lat * self.height_m,
            })
        } else if observatory_type.eq_ignore_ascii_case("WGS84") {
            Some(ObservatoryPosition::Wgs84 {
                longitude_rad,
                latitude_rad,
                height_m: self.height_m,
            })
        } else {
            None
        }
    }
}

/// In-memory catalog of observatory metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservatoryCatalog {
    entries: Vec<ObservatoryEntry>,
}

impl ObservatoryCatalog {
    /// Build a catalog from explicitly provided entries.
    pub fn from_entries(entries: Vec<ObservatoryEntry>) -> Self {
        Self { entries }
    }

    /// Iterate over the catalog entries in source order.
    pub fn iter(&self) -> impl Iterator<Item = &ObservatoryEntry> {
        self.entries.iter()
    }

    /// Borrow all entries.
    pub fn entries(&self) -> &[ObservatoryEntry] {
        &self.entries
    }

    /// Look up an observatory by case-insensitive name.
    pub fn get(&self, name: &str) -> Option<&ObservatoryEntry> {
        let name = name.trim();
        self.entries
            .iter()
            .find(|entry| entry.name.trim().eq_ignore_ascii_case(name))
    }

    pub(crate) fn retained_heap_bytes(&self) -> Option<usize> {
        let mut bytes = self
            .entries
            .capacity()
            .checked_mul(size_of::<ObservatoryEntry>())?;
        for entry in &self.entries {
            bytes = bytes
                .checked_add(entry.name.capacity())?
                .checked_add(entry.observatory_type.capacity())?
                .checked_add(entry.source.capacity())?
                .checked_add(entry.comment.capacity())?
                .checked_add(entry.antenna_responses.capacity())?;
        }
        Some(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(observatory_type: &str, long_lat_height: [f64; 3], xyz: [f64; 3]) -> ObservatoryEntry {
        ObservatoryEntry {
            mjd: 0.0,
            name: "SMA".to_owned(),
            observatory_type: observatory_type.to_owned(),
            longitude_deg: long_lat_height[0],
            latitude_deg: long_lat_height[1],
            height_m: long_lat_height[2],
            x_m: xyz[0],
            y_m: xyz[1],
            z_m: xyz[2],
            source: String::new(),
            comment: String::new(),
            antenna_responses: String::new(),
        }
    }

    /// The SMA row of the measures data's `Observatories` table, whose `X`,
    /// `Y`, `Z` lie 13.8 km from its `Long`, `Lat`, `Height`, and the
    /// Cartesian position CASA 6.7.6.14 gives SMA
    /// (`crates/casa-simulation/tests/data/readantenna_casa.json`).
    #[test]
    fn an_itrf_entry_is_where_casacore_places_it() {
        let sma = entry(
            "ITRF",
            [-155.477_522_222_222_2, 19.824_205_555_555_555, 6_382_248.0],
            [-5_464_555.493, -2_492_927.989, 2_150_797.176],
        );
        let Some(ObservatoryPosition::Itrf { x_m, y_m, z_m }) = sma.position() else {
            panic!("an ITRF position");
        };
        let casa = [
            -5_462_448.949_394_94,
            -2_491_969.916_598_48,
            2_164_446.105_479_709,
        ];
        for (found, casa) in [x_m, y_m, z_m].into_iter().zip(casa) {
            assert!((found - casa).abs() < 1e-6, "{found} {casa}");
        }
    }

    #[test]
    fn a_wgs84_entry_is_geodetic_and_another_type_has_no_position() {
        let alma = entry("WGS84 ", [-67.754_929, -23.022_886, 5056.8], [0.0; 3]);
        assert_eq!(
            alma.position(),
            Some(ObservatoryPosition::Wgs84 {
                longitude_rad: -1.182_546_595_504_989_2,
                latitude_rad: -0.401_825_164_011_307_2,
                height_m: 5056.8,
            })
        );
        assert_eq!(entry("J2000", [0.0; 3], [0.0; 3]).position(), None);
    }
}
