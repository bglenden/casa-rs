// SPDX-License-Identifier: LGPL-3.0-or-later
//! `simutil`'s geodesy: its datum table and its conversions between UTM,
//! geodetic and Earth-centred coordinates, over
//! [`casa_simulation::readantenna::geodesy`], which `readantenna` uses and
//! which records where it corrects CASA.
//!
//! Each returns CASA 6.7.6.14's tuple of Python numbers, for scalar
//! arguments:
//! - `getdatum(datumcode)`: `(dx, dy, dz, er, rf)`, the offsets `int` where
//!   CASA's table writes them so;
//! - `tconpc`: `(eps, r, so, v0, v2, v4, v6)`; `tmgeod`:
//!   `(lat, lon, conv, kp)`, the longitude west-positive;
//! - `utm2long`: `(lon, lat)`; `xyz2long`: `(lon, lat, height)`;
//!   `xyz2long_old`: `(lon, lat)`; `long2xyz`, `utm2xyz` and
//!   `locxyz2itrf`: `(x, y, z)`;
//! - `itrf2locname`: `itrf2loc`'s arrays about the named observatory.
//!
//! An unknown datum raises CASA's "unknown datum" as a `RuntimeError`.
//! CASA's `utm2long` with `verbose` also logs the position it found; casa-rs
//! does not.

use casa_simulation::readantenna::geodesy::{
    self, Datum, geodetic_to_xyz, grid_to_geodetic, local_to_itrf, utm_to_geodetic, utm_to_xyz,
    xyz_to_geodetic, xyz_to_geodetic_old, zone_constants,
};
use casa_types::measures::{MPosition, PositionRef};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::{LOGGER, PySimutil};
use crate::convert::DoubleList;
use crate::convert::value::CasaValue;
use crate::tools::{LogLine, emit};

/// The datum CASA's `getdatum` names `code`, or its refusal.
fn named_datum(code: &str) -> PyResult<&'static Datum> {
    geodesy::datum(code).ok_or_else(|| PyRuntimeError::new_err(format!("unknown datum {code}")))
}

/// A datum offset as CASA's table holds it: an `int` where it is whole.
fn offset(value: f64) -> CasaValue {
    if value.fract() == 0.0 && value.abs() < 1e15 {
        #[allow(clippy::cast_possible_truncation)]
        CasaValue::Int(value as i64)
    } else {
        CasaValue::Float(value)
    }
}

fn floats(values: &[f64]) -> CasaValue {
    CasaValue::Tuple(values.iter().copied().map(CasaValue::Float).collect())
}

#[pymethods]
impl PySimutil {
    /// The offsets from ITRF and the ellipsoid of a datum: `(dx, dy, dz,
    /// er, rf)`, metres and the reciprocal flattening.
    #[pyo3(signature = (datumcode, verbose = false), text_signature = "($self, datumcode, verbose=False)")]
    fn getdatum(&self, py: Python<'_>, datumcode: &str, verbose: bool) -> PyResult<Py<PyAny>> {
        let datum = named_datum(datumcode)?;
        if verbose {
            emit(
                py,
                LOGGER,
                &[LogLine::info(format!(
                    "Using {} datum with {} ellipsoid",
                    datum.name, datum.ellipsoid.name
                ))],
            )?;
        }
        CasaValue::Tuple(vec![
            offset(datum.dx_m),
            offset(datum.dy_m),
            offset(datum.dz_m),
            CasaValue::Float(datum.ellipsoid.equatorial_radius_m),
            CasaValue::Float(datum.ellipsoid.inverse_flattening),
        ])
        .into_python(py)
    }

    /// The transverse Mercator zone constants `(eps, r, so, v0, v2, v4,
    /// v6)`.
    #[pyo3(signature = (sf, orlim, er, esq, rf), text_signature = "($self, sf, orlim, er, esq, rf)")]
    fn tconpc(
        &self,
        py: Python<'_>,
        sf: f64,
        orlim: f64,
        er: f64,
        esq: f64,
        rf: f64,
    ) -> PyResult<Py<PyAny>> {
        floats(&zone_constants(sf, orlim, er, esq, rf)).into_python(py)
    }

    /// Grid coordinates to `(lat, lon, conv, kp)`: geodetic latitude and
    /// west-positive longitude (radians), convergence and point scale
    /// factor.
    #[pyo3(
        signature = (n, e, eps, cm, fe, sf, so, r, v0, v2, v4, v6, r#fn, er, esq),
        text_signature = "($self, n, e, eps, cm, fe, sf, so, r, v0, v2, v4, v6, fn, er, esq)"
    )]
    // CASA's signature: one value each.
    #[allow(clippy::too_many_arguments)]
    fn tmgeod(
        &self,
        py: Python<'_>,
        n: f64,
        e: f64,
        eps: f64,
        cm: f64,
        fe: f64,
        sf: f64,
        so: f64,
        r: f64,
        v0: f64,
        v2: f64,
        v4: f64,
        v6: f64,
        r#fn: f64,
        er: f64,
        esq: f64,
    ) -> PyResult<Py<PyAny>> {
        let found = grid_to_geodetic(
            n,
            e,
            eps,
            cm,
            fe,
            sf,
            so,
            r,
            [v0, v2, v4, v6],
            r#fn,
            er,
            esq,
        );
        floats(&found).into_python(py)
    }

    /// UTM easting and northing in `zone` to `(lon, lat)`, radians,
    /// longitude east-positive; `nors` is `'N'` or `'S'`.
    #[pyo3(signature = (east, north, zone, datum, nors), text_signature = "($self, east, north, zone, datum, nors)")]
    fn utm2long(
        &self,
        py: Python<'_>,
        east: f64,
        north: f64,
        zone: i64,
        datum: &str,
        nors: &str,
    ) -> PyResult<Py<PyAny>> {
        let datum = named_datum(datum)?;
        let (lon, lat) = utm_to_geodetic(east, north, zone, datum, nors == "S");
        floats(&[lon, lat]).into_python(py)
    }

    /// Geodetic longitude, latitude (radians) and elevation on `datum` to
    /// Earth-centred `(x, y, z)`, metres.
    #[pyo3(signature = (lon, lat, elevation, datum), text_signature = "($self, lon, lat, elevation, datum)")]
    fn long2xyz(
        &self,
        py: Python<'_>,
        lon: f64,
        lat: f64,
        elevation: f64,
        datum: &str,
    ) -> PyResult<Py<PyAny>> {
        floats(&geodetic_to_xyz(lon, lat, elevation, named_datum(datum)?)).into_python(py)
    }

    /// Earth-centred x, y, z to `(lon, lat, height)` on `datum`; CASA's
    /// `(-1, -1, -1)` for an ellipsoid it cannot use.
    #[pyo3(signature = (x, y, z, datum), text_signature = "($self, x, y, z, datum)")]
    fn xyz2long(&self, py: Python<'_>, x: f64, y: f64, z: f64, datum: &str) -> PyResult<Py<PyAny>> {
        match xyz_to_geodetic(x, y, z, named_datum(datum)?) {
            Some((lon, lat, height)) => floats(&[lon, lat, height]).into_python(py),
            None => CasaValue::Tuple(vec![CasaValue::Int(-1); 3]).into_python(py),
        }
    }

    /// Earth-centred x, y, z to `(lon, lat)` on `datum`, by CASA's older
    /// iteration.
    #[pyo3(signature = (x, y, z, datum), text_signature = "($self, x, y, z, datum)")]
    fn xyz2long_old(
        &self,
        py: Python<'_>,
        x: f64,
        y: f64,
        z: f64,
        datum: &str,
    ) -> PyResult<Py<PyAny>> {
        let (lon, lat) = xyz_to_geodetic_old(x, y, z, named_datum(datum)?);
        floats(&[lon, lat]).into_python(py)
    }

    /// UTM easting, northing and elevation to Earth-centred `(x, y, z)`.
    #[pyo3(
        signature = (easting, northing, elevation, zone, datum, nors),
        text_signature = "($self, easting, northing, elevation, zone, datum, nors)"
    )]
    // CASA's signature: one value each.
    #[allow(clippy::too_many_arguments)]
    fn utm2xyz(
        &self,
        py: Python<'_>,
        easting: f64,
        northing: f64,
        elevation: f64,
        zone: i64,
        datum: &str,
        nors: &str,
    ) -> PyResult<Py<PyAny>> {
        let datum = named_datum(datum)?;
        floats(&utm_to_xyz(
            easting,
            northing,
            elevation,
            zone,
            datum,
            nors == "S",
        ))
        .into_python(py)
    }

    /// Local offsets `(locx, locy, locz)` (east, north, up, metres) about a
    /// point at `lat`, `longitude` (degrees) and `alt` to Earth-centred
    /// `(x, y, z)`.
    #[pyo3(
        signature = (lat, longitude, alt, locx = 0.0, locy = 0.0, locz = 0.0),
        text_signature = "($self, lat, longitude, alt, locx=0.0, locy=0.0, locz=0.0)"
    )]
    // CASA's signature: one value each.
    #[allow(clippy::too_many_arguments)]
    fn locxyz2itrf(
        &self,
        py: Python<'_>,
        lat: f64,
        longitude: f64,
        alt: f64,
        locx: f64,
        locy: f64,
        locz: f64,
    ) -> PyResult<Py<PyAny>> {
        floats(&local_to_itrf(lat, longitude, alt, [locx, locy, locz])).into_python(py)
    }

    /// Earth-centred positions relative to a known observatory: `itrf2loc`
    /// about the observatory's WGS84 position, as CASA finds it with
    /// `me.observatory` and `long2xyz`.
    #[pyo3(signature = (x, y, z, obsname), text_signature = "($self, x, y, z, obsname)")]
    fn itrf2locname(
        &self,
        py: Python<'_>,
        x: DoubleList,
        y: DoubleList,
        z: DoubleList,
        obsname: &str,
    ) -> PyResult<Py<PyAny>> {
        let centre = py
            .detach(|| {
                let measures = casa_ms::open_measures_runtime()
                    .map_err(|error| format!("measures data: {error}"))?;
                let position = MPosition::from_observatory_name(obsname, &*measures)
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| format!("Unknown observatory {obsname}"))?;
                let wgs84 = position
                    .convert_to(PositionRef::WGS84)
                    .map_err(|error| error.to_string())?;
                let [lon, lat, height] = wgs84.values();
                let datum = geodesy::datum("WGS84").expect("WGS84 is one of casa-rs's datums");
                Ok::<_, String>(geodetic_to_xyz(lon, lat, height, datum))
            })
            .map_err(PyRuntimeError::new_err)?;
        self.itrf2loc(py, x, y, z, centre[0], centre[1], centre[2])
    }
}
