// SPDX-License-Identifier: LGPL-3.0-or-later
//! The geodesy of CASA's `simutil`: its datum and ellipsoid tables and its
//! conversions between UTM, geodetic and Earth-centred coordinates.
//!
//! | CASA (`simutil`) | casa-rs |
//! |---|---|
//! | `getdatum(datumcode)` | [`datum`], [`DATUMS`], [`ELLIPSOIDS`] |
//! | `utm2long(east, north, zone, datum, nors)` | [`utm_to_geodetic`] |
//! | `long2xyz(lon, lat, elevation, datum)` | [`geodetic_to_xyz`] |
//! | `utm2xyz(easting, northing, elevation, zone, datum, nors)` | [`utm_to_xyz`] |
//! | `xyz2long(x, y, z, datum)` | [`xyz_to_geodetic`] |
//! | `locxyz2itrf(lat, longitude, alt, locx, locy, locz)` | [`local_to_itrf`] |
//!
//! The arithmetic follows CASA's Python operation by operation (including
//! its `180/π` divisions and its `**` powers), so results agree with CASA's
//! to rounding. As in CASA, the "ITRF" these functions produce is the
//! nominal right-handed Earth-centred frame of the datum's ellipsoid shifted
//! by the datum's offsets, not a realised ITRF.
//!
//! The UTM conversion is T. Vincenty's transverse Mercator inversion
//! (subroutines `tconpc` and `tmgeod` of the NGS `UTMS` program, 1985), which
//! R. Indebetouw translated from Fortran for CASA in 2009.

/// A reference ellipsoid of CASA's `getdatum` table.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Ellipsoid {
    /// CASA's two-letter code, e.g. `"WE"` for WGS 84.
    pub code: &'static str,
    /// Equatorial radius (semi-major axis), metres.
    pub equatorial_radius_m: f64,
    /// Reciprocal of the flattening.
    pub inverse_flattening: f64,
    /// CASA's descriptive name.
    pub name: &'static str,
}

/// A geodetic datum of CASA's `getdatum` table: an ellipsoid and the
/// offsets that take the datum's Earth-centred coordinates to WGS 84's.
///
/// CASA's docstring: one takes local Earth-centred x, y, z coordinates and
/// adds `dx`, `dy`, `dz` to get WGS 84 Earth-centred ones.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Datum {
    /// CASA's datum code, e.g. `"SAM56"` (the `datum=` header value).
    pub code: &'static str,
    /// Offset added to x, metres.
    pub dx_m: f64,
    /// Offset added to y, metres.
    pub dy_m: f64,
    /// Offset added to z, metres.
    pub dz_m: f64,
    /// The datum's ellipsoid.
    pub ellipsoid: Ellipsoid,
    /// CASA's descriptive name.
    pub name: &'static str,
}

const fn ellipsoid(
    code: &'static str,
    equatorial_radius_m: f64,
    inverse_flattening: f64,
    name: &'static str,
) -> Ellipsoid {
    Ellipsoid {
        code,
        equatorial_radius_m,
        inverse_flattening,
        name,
    }
}

const AW: Ellipsoid = ellipsoid("AW", 6_377_563.396, 299.324_964_7, "Airy 1830");
const AM: Ellipsoid = ellipsoid("AM", 6_377_340.189, 299.324_964_7, "Modified Airy");
const AN: Ellipsoid = ellipsoid("AN", 6_378_160.0, 298.25, "Australian National");
const BR: Ellipsoid = ellipsoid("BR", 6_377_397.155, 299.152_812_8, "Bessel 1841");
const CC: Ellipsoid = ellipsoid("CC", 6_378_206.4, 294.978_698_2, "Clarke 1866");
const CD: Ellipsoid = ellipsoid("CD", 6_378_249.145, 293.465, "Clarke 1880");
const EA: Ellipsoid = ellipsoid("EA", 6_377_276.345, 300.801_7, "Everest (India 1830)");
const RF: Ellipsoid = ellipsoid(
    "RF",
    6_378_137.0,
    298.257_222_101,
    "Geodetic Reference System 1980",
);
const HE: Ellipsoid = ellipsoid("HE", 6_378_200.0, 298.30, "Helmert 1906");
const HO: Ellipsoid = ellipsoid("HO", 6_378_270.0, 297.00, "Hough 1960");
const IN: Ellipsoid = ellipsoid("IN", 6_378_388.0, 297.00, "International 1924");
const SA: Ellipsoid = ellipsoid("SA", 6_378_160.0, 298.25, "South American 1969");
const WD: Ellipsoid = ellipsoid("WD", 6_378_135.0, 298.26, "World Geodetic System 1972");
const WE: Ellipsoid = ellipsoid(
    "WE",
    6_378_137.0,
    298.257_223_563,
    "World Geodetic System 1984",
);

/// CASA's ellipsoid table (`getdatum`'s `ellipsoids`), in CASA's order.
pub const ELLIPSOIDS: [Ellipsoid; 14] = [AW, AM, AN, BR, CC, CD, EA, RF, HE, HO, IN, SA, WD, WE];

const fn datum_entry(
    code: &'static str,
    offsets_m: [f64; 3],
    ellipsoid: Ellipsoid,
    name: &'static str,
) -> Datum {
    Datum {
        code,
        dx_m: offsets_m[0],
        dy_m: offsets_m[1],
        dz_m: offsets_m[2],
        ellipsoid,
        name,
    }
}

/// CASA's datum table (`getdatum`'s `datums`), in CASA's order.
pub const DATUMS: [Datum; 24] = [
    datum_entry(
        "AGD66",
        [-133.0, -48.0, 148.0],
        AN,
        "Australian Geodetic Datum 1966",
    ),
    datum_entry(
        "AGD84",
        [-134.0, -48.0, 149.0],
        AN,
        "Australian Geodetic Datum 1984",
    ),
    datum_entry(
        "ASTRO",
        [-104.0, -129.0, 239.0],
        IN,
        "Camp Area Astro (Antarctica)",
    ),
    datum_entry("CAPE", [-136.0, -108.0, -292.0], CD, "CAPE (South Africa)"),
    datum_entry("ED50", [-87.0, -98.0, -121.0], IN, "European 1950"),
    datum_entry("ED79", [-86.0, -98.0, -119.0], IN, "European 1979"),
    datum_entry("GRB36", [375.0, -111.0, 431.0], AW, "Great Britain 1936"),
    datum_entry(
        "HAWAII",
        [89.0, -279.0, -183.0],
        IN,
        "Hawaiian Hawaii (Old)",
    ),
    datum_entry("KAUAI", [45.0, -290.0, -172.0], IN, "Hawaiian Kauai (Old)"),
    datum_entry("MAUI", [65.0, -290.0, -190.0], IN, "Hawaiian Maui (Old)"),
    datum_entry("OAHU", [56.0, -284.0, -181.0], IN, "Hawaiian Oahu (Old)"),
    datum_entry("INDIA", [289.0, 734.0, 257.0], EA, "Indian"),
    datum_entry("NAD83", [0.0, 0.0, 0.0], RF, "N. American 1983"),
    datum_entry(
        "CANADA",
        [-10.0, 158.0, 187.0],
        CC,
        "N. American Canada 1927",
    ),
    datum_entry(
        "ALASKA",
        [-5.0, 135.0, 172.0],
        CC,
        "N. American Alaska 1927",
    ),
    datum_entry("NAD27", [-8.0, 160.0, 176.0], CC, "N. American Conus 1927"),
    datum_entry("CARIBB", [-7.0, 152.0, 178.0], CC, "N. American Caribbean"),
    datum_entry("MEXICO", [-12.0, 130.0, 190.0], CC, "N. American Mexico"),
    datum_entry(
        "CAMER",
        [0.0, 125.0, 194.0],
        CC,
        "N. American Central America",
    ),
    datum_entry(
        "SAM56",
        [-288.0, 175.0, -376.0],
        IN,
        "South American (Provisional1956)",
    ),
    datum_entry("SAM69", [-57.0, 1.0, -41.0], SA, "South American 1969"),
    datum_entry(
        "CAMPO",
        [-148.0, 136.0, 90.0],
        IN,
        "S. American Campo Inchauspe (Argentina)",
    ),
    datum_entry("WGS72", [0.0, 0.0, 4.5], WD, "World Geodetic System - 72"),
    datum_entry("WGS84", [0.0, 0.0, 0.0], WE, "World Geodetic System - 84"),
];

/// The WGS 84 datum, which `xyz2long` uses for an unknown observatory.
pub const WGS84: Datum = DATUMS[23];

/// Look a datum up by its CASA code (`getdatum`).
///
/// The match is exact and case-sensitive, as CASA's dictionary lookup is:
/// `"NAD27"` is found, `"nad27"` is not. Returns `None` where CASA logs
/// `unknown datum <code>`.
#[must_use]
pub fn datum(code: &str) -> Option<&'static Datum> {
    DATUMS.iter().find(|datum| datum.code == code)
}

/// The zone constants of the transverse Mercator inversion that `tconpc`
/// returns: `eps` (the second eccentricity squared), `r` (the radius of
/// the rectifying sphere), `so` (the scaled meridional distance from the
/// equator to the southernmost parallel `orlim`) and the foot-point series
/// coefficients `v0`, `v2`, `v4`, `v6`.
struct ZoneConstants {
    eps: f64,
    r: f64,
    so: f64,
    v: [f64; 4],
}

/// `tconpc`: the zone constants for scale factor `sf`, southernmost
/// parallel `orlim`, equatorial radius `er`, eccentricity squared `esq` and
/// reciprocal flattening `rf`.
fn tconpc(sf: f64, orlim: f64, er: f64, esq: f64, rf: f64) -> ZoneConstants {
    let f = 1.0 / rf;
    let eps = esq / (1.0 - esq);
    let pr = (1.0 - f) * er;
    let en = (er - pr) / (er + pr);
    let en2 = en * en;
    let en3 = en * en * en;
    let en4 = en2 * en2;

    let c2 = -3.0 * en / 2.0 + 9.0 * en3 / 16.0;
    let c4 = 15.0 * en2 / 16.0 - 15.0 * en4 / 32.0;
    let c6 = -35.0 * en3 / 48.0;
    let c8 = 315.0 * en4 / 512.0;
    let u0 = 2.0 * (c2 - 2.0 * c4 + 3.0 * c6 - 4.0 * c8);
    let u2 = 8.0 * (c4 - 4.0 * c6 + 10.0 * c8);
    let u4 = 32.0 * (c6 - 6.0 * c8);
    let u6 = 128.0 * c8;

    let c2 = 3.0 * en / 2.0 - 27.0 * en3 / 32.0;
    let c4 = 21.0 * en2 / 16.0 - 55.0 * en4 / 32.0;
    let c6 = 151.0 * en3 / 96.0;
    let c8 = 1097.0 * en4 / 512.0;
    let v0 = 2.0 * (c2 - 2.0 * c4 + 3.0 * c6 - 4.0 * c8);
    let v2 = 8.0 * (c4 - 4.0 * c6 + 10.0 * c8);
    let v4 = 32.0 * (c6 - 6.0 * c8);
    let v6 = 128.0 * c8;

    let r = er * (1.0 - en) * (1.0 - en * en) * (1.0 + 2.25 * en * en + (225.0 / 64.0) * en4);
    let cosor = orlim.cos();
    let omo = orlim
        + orlim.sin()
            * cosor
            * (u0 + u2 * cosor * cosor + u4 * cosor.powf(4.0) + u6 * cosor.powf(6.0));
    let so = sf * r * omo;
    ZoneConstants {
        eps,
        r,
        so,
        v: [v0, v2, v4, v6],
    }
}

/// The projection of one UTM zone: what `tmgeod` takes besides the point.
struct Projection {
    cm: f64,
    fe: f64,
    fn_: f64,
    sf: f64,
    er: f64,
    esq: f64,
}

/// `tmgeod`: grid coordinates to geodetic latitude and west-positive
/// longitude, radians. (CASA also computes the convergence and the point
/// scale factor, which it only logs; they are omitted.)
fn tmgeod(n: f64, e: f64, zone: &ZoneConstants, p: &Projection) -> (f64, f64) {
    let [v0, v2, v4, v6] = zone.v;
    let om = (n - p.fn_ + zone.so) / (zone.r * p.sf);
    let cosom = om.cos();
    let foot = om
        + om.sin()
            * cosom
            * (v0 + v2 * cosom * cosom + v4 * cosom.powf(4.0) + v6 * cosom.powf(6.0));
    let sinf = foot.sin();
    let cosf = foot.cos();
    let tn = sinf / cosf;
    let ts = tn * tn;
    let ets = zone.eps * cosf * cosf;
    let rn = p.er * p.sf / (1.0 - p.esq * sinf * sinf).sqrt();
    let q = (e - p.fe) / rn;
    let qs = q * q;
    let b2 = -tn * (1.0 + ets) / 2.0;
    let b4 = -(5.0 + 3.0 * ts + ets * (1.0 - 9.0 * ts) - 4.0 * ets * ets) / 12.0;
    let b6 = (61.0 + 45.0 * ts * (2.0 + ts) + ets * (46.0 - 252.0 * ts - 60.0 * ts * ts)) / 360.0;
    let b1 = 1.0;
    let b3 = -(1.0 + ts + ts + ets) / 6.0;
    let b5 = (5.0 + ts * (28.0 + 24.0 * ts) + ets * (6.0 + 8.0 * ts)) / 120.0;
    let b7 = -(61.0 + 662.0 * ts + 1320.0 * ts * ts + 720.0 * ts.powf(3.0)) / 5040.0;
    let lat = foot + b2 * qs * (1.0 + qs * (b4 + b6 * qs));
    let l = b1 * q * (1.0 + qs * (b3 + qs * (b5 + b7 * qs)));
    let lon = -l / cosf + p.cm;
    (lat, lon)
}

/// Convert UTM coordinates to geodetic longitude and latitude (CASA's
/// `utm2long`).
///
/// - `east`, `north`: easting and northing, metres (false easting 500 km;
///   false northing 10 000 km when `south`).
/// - `zone`: the UTM zone number. As in CASA it is not range-checked: zones
///   below 30 take the central meridian `183 − 6·zone` degrees west, the
///   others `543 − 6·zone` degrees west.
/// - `datum`: only its ellipsoid is used here; [`geodetic_to_xyz`] applies
///   the offsets.
/// - `south`: the southern-hemisphere flag (CASA's `nors == 'S'`).
///
/// Returns `(longitude, latitude)` in radians, longitude east-positive. The
/// longitude is not normalised: for zones 30 and above it is near −360°
/// plus the east longitude, exactly as CASA returns it.
///
/// CASA scale factor 0.9996, southernmost parallel 0, and no adjacent-zone
/// correction (CASA has that code commented out).
#[must_use]
pub fn utm_to_geodetic(east: f64, north: f64, zone: i64, datum: &Datum, south: bool) -> (f64, f64) {
    let rad = 180.0 / std::f64::consts::PI;
    let er = datum.ellipsoid.equatorial_radius_m;
    let rf = datum.ellipsoid.inverse_flattening;
    let f = 1.0 / rf;
    let esq = 2.0 * f - f * f;
    // CASA's longitudes in this code are west-positive.
    let icm = if zone < 30 {
        183_i128 - 6 * i128::from(zone)
    } else {
        543_i128 - 6 * i128::from(zone)
    };
    // Python's float(icm): exact for any real UTM zone.
    let cm = icm as f64 / rad;
    let fn_ = if south { 10_000_000.0 } else { 0.0 };
    let fe = 500_000.0;
    let sf = 0.9996;
    let orlim = 0.0;
    let constants = tconpc(sf, orlim, er, esq, rf);
    let projection = Projection {
        cm,
        fe,
        fn_,
        sf,
        er,
        esq,
    };
    let (lat, lon) = tmgeod(north, east, &constants, &projection);
    // CASA: "convert to more usual convention of negative lon = W".
    (-lon, lat)
}

/// Convert geodetic longitude, latitude and elevation on a datum to
/// Earth-centred x, y, z (CASA's `long2xyz`).
///
/// `longitude_rad` is east-positive, `elevation_m` is above the datum's
/// ellipsoid. The datum's offsets are added to the result, so for a datum
/// other than WGS 84 the coordinates are WGS 84-centred. CASA's comment
/// quotes the NGS check point S23°12′00″ W67°48′00″, 0 m on GRS 80, at
/// (2216194.4188, −5430618.6472, −2497092.5213) m.
#[must_use]
pub fn geodetic_to_xyz(
    longitude_rad: f64,
    latitude_rad: f64,
    elevation_m: f64,
    datum: &Datum,
) -> [f64; 3] {
    let er = datum.ellipsoid.equatorial_radius_m;
    let f = 1.0 / datum.ellipsoid.inverse_flattening;
    let esq = 2.0 * f - f.powf(2.0);
    let nu = er / (1.0 - esq * latitude_rad.sin().powf(2.0)).sqrt();
    let x = (nu + elevation_m) * latitude_rad.cos() * longitude_rad.cos() + datum.dx_m;
    let y = (nu + elevation_m) * latitude_rad.cos() * longitude_rad.sin() + datum.dy_m;
    let z = ((1.0 - esq) * nu + elevation_m) * latitude_rad.sin() + datum.dz_m;
    [x, y, z]
}

/// Convert UTM easting, northing and elevation to Earth-centred x, y, z
/// (CASA's `utm2xyz`): [`utm_to_geodetic`] then [`geodetic_to_xyz`].
#[must_use]
pub fn utm_to_xyz(
    easting_m: f64,
    northing_m: f64,
    elevation_m: f64,
    zone: i64,
    datum: &Datum,
    south: bool,
) -> [f64; 3] {
    let (lon, lat) = utm_to_geodetic(easting_m, northing_m, zone, datum, south);
    geodetic_to_xyz(lon, lat, elevation_m, datum)
}

/// Convert Earth-centred x, y, z to geodetic longitude, latitude and height
/// on a datum (CASA's `xyz2long`, a transcription of SOFA's `gc2gde`).
///
/// Returns `(longitude_rad, latitude_rad, height_m)`, or `None` where CASA
/// returns `-1, -1, -1` for invalid ellipsoid parameters (never for the
/// tabulated datums). The datum's offsets are subtracted first.
///
/// One correction to CASA: CASA computes the longitude as
/// `atan2(y − dx, x − dx)`, subtracting the x offset from y. This function
/// subtracts `dy` from y. The two agree for WGS 84, the only datum
/// `readantenna` converts with.
#[must_use]
pub fn xyz_to_geodetic(x: f64, y: f64, z: f64, datum: &Datum) -> Option<(f64, f64, f64)> {
    let er = datum.ellipsoid.equatorial_radius_m;
    let f = 1.0 / datum.ellipsoid.inverse_flattening;
    if !(0.0..1.0).contains(&f) || er <= 0.0 {
        return None;
    }
    let e2 = (2.0 - f) * f;
    let e4t = e2 * e2 * 1.5;
    let ec2 = 1.0 - e2;
    if ec2 <= 0.0 {
        return None;
    }
    let ec = ec2.sqrt();
    let (xr, yr, zr) = (x - datum.dx_m, y - datum.dy_m, z - datum.dz_m);
    // Distance from the polar axis.
    let r = (xr.powf(2.0) + yr.powf(2.0)).sqrt();
    let lon = if r > 0.0 { yr.atan2(xr) } else { 0.0 };
    // Newton correction factors.
    let s0 = zr.abs() / er;
    let c0 = ec * r / er;
    let a0 = (c0.powf(2.0) + s0.powf(2.0)).sqrt();
    let d0 = ec * s0 * a0.powf(3.0) + e2 * s0.powf(3.0);
    let f0 = r / er * a0.powf(3.0) - e2 * c0.powf(3.0);
    // Halley correction factor.
    let b0 = e4t * s0.powf(2.0) * c0.powf(2.0) * r / er * (a0 - ec);
    let s1 = d0 * f0 - b0 * s0;
    let cc = ec * (f0 * f0 - b0 * c0);
    let mut lat = s1.atan2(cc);
    let height = (r * cc + zr.abs() * s1 - er * (ec2 * s1.powf(2.0) + cc.powf(2.0)).sqrt())
        / (s1.powf(2.0) + cc.powf(2.0)).sqrt();
    if zr < 0.0 {
        lat = -lat;
    }
    Some((lon, lat, height))
}

/// Convert local tangent-plane offsets to Earth-centred x, y, z (CASA's
/// `locxyz2itrf`, from Rob Reid).
///
/// - `latitude_deg`, `longitude_deg`: geodetic WGS 84 position of the
///   reference point, degrees (CASA passes the observatory's position).
/// - `altitude_m`: the reference point's height above the ellipsoid.
/// - `local_m`: the offsets `[x, y, z]` from that point: x east, y north,
///   z along the ellipsoid normal, metres.
///
/// CASA uses the WGS 84 semi-axes `a = 6378137.0` m and
/// `b = 6356752.3142` m (the rounded polar radius), as here.
#[must_use]
pub fn local_to_itrf(
    latitude_deg: f64,
    longitude_deg: f64,
    altitude_m: f64,
    local_m: [f64; 3],
) -> [f64; 3] {
    let [locx, locy, locz] = local_m;
    let phi = latitude_deg.to_radians();
    let lmbda = longitude_deg.to_radians();
    let sphi = phi.sin();
    let a = 6_378_137.0_f64;
    let b = 6_356_752.314_2_f64;
    let ae = (b / a).acos();
    let n = a / (1.0 - (ae.sin() * sphi).powf(2.0)).sqrt();
    let nploczcphimlocysphi = (n + locz + altitude_m) * phi.cos() - locy * sphi;
    let clmb = lmbda.cos();
    let slmb = lmbda.sin();
    let x = nploczcphimlocysphi * clmb - locx * slmb;
    let y = nploczcphimlocysphi * slmb + locx * clmb;
    let z = (n * (b / a).powf(2.0) + locz + altitude_m) * sphi + locy * phi.cos();
    [x, y, z]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datum_lookup_is_exact_and_case_sensitive() {
        let nad27 = datum("NAD27").expect("NAD27");
        assert_eq!(nad27.ellipsoid.code, "CC");
        assert_eq!([nad27.dx_m, nad27.dy_m, nad27.dz_m], [-8.0, 160.0, 176.0]);
        assert!(datum("nad27").is_none());
        assert!(datum("ITRF").is_none());
        assert_eq!(datum("WGS84"), Some(&WGS84));
        assert_eq!(datum("WGS72").map(|d| d.dz_m), Some(4.5));
        assert!(DATUMS.iter().all(|d| ELLIPSOIDS.contains(&d.ellipsoid)));
    }

    #[test]
    fn geodetic_to_xyz_matches_casas_ngs_check_point() {
        // simutil.long2xyz's comment: NGS xyz_getxyz for S231200.0
        // W0674800.0 0.0000 on GRS80.
        let lat = -(23.0 + 12.0 / 60.0_f64).to_radians();
        let lon = -(67.0 + 48.0 / 60.0_f64).to_radians();
        let xyz = geodetic_to_xyz(lon, lat, 0.0, datum("NAD83").expect("NAD83"));
        let expected = [2_216_194.418_8, -5_430_618.647_2, -2_497_092.521_3];
        for (got, want) in xyz.iter().zip(expected) {
            assert!((got - want).abs() < 1e-3, "{xyz:?}");
        }
    }

    #[test]
    fn utm_central_meridian_on_the_equator() {
        // Easting 500 km on the equator is the zone's central meridian.
        let (lon, lat) = utm_to_geodetic(500_000.0, 0.0, 19, &WGS84, false);
        assert!(lat.abs() < 1e-15);
        assert!((lon.to_degrees() + 69.0).abs() < 1e-12);
        let (lon, lat) = utm_to_geodetic(500_000.0, 10_000_000.0, 19, &WGS84, true);
        assert!(lat.abs() < 1e-15);
        assert!((lon.to_degrees() + 69.0).abs() < 1e-12);
        // Zone 32 (>= 30): CASA's west-positive 351° becomes −351°, i.e. 9° E.
        let (lon, _) = utm_to_geodetic(500_000.0, 0.0, 32, &WGS84, false);
        assert!((lon.to_degrees() + 351.0).abs() < 1e-12);
    }

    #[test]
    fn xyz_to_geodetic_inverts_geodetic_to_xyz() {
        let (lon, lat, h) = (-1.878_294_258, 0.594_787_498, 2114.89);
        let [x, y, z] = geodetic_to_xyz(lon, lat, h, &WGS84);
        let (lon2, lat2, h2) = xyz_to_geodetic(x, y, z, &WGS84).expect("valid ellipsoid");
        assert!((lon2 - lon).abs() < 1e-14);
        assert!((lat2 - lat).abs() < 1e-14);
        assert!((h2 - h).abs() < 1e-8);
        // Southern hemisphere restores the sign of the latitude.
        let [x, y, z] = geodetic_to_xyz(lon, -lat, h, &WGS84);
        let (_, lat3, _) = xyz_to_geodetic(x, y, z, &WGS84).expect("valid ellipsoid");
        assert!((lat3 + lat).abs() < 1e-14);
    }

    #[test]
    fn local_to_itrf_axes_point_east_north_and_up() {
        let (lat, lon, alt) = (-23.022_886, -67.754_929, 5056.8);
        let origin = local_to_itrf(lat, lon, alt, [0.0; 3]);
        let unit = |local: [f64; 3]| {
            let p = local_to_itrf(lat, lon, alt, local);
            [p[0] - origin[0], p[1] - origin[1], p[2] - origin[2]]
        };
        let (phi, lam) = (lat.to_radians(), lon.to_radians());
        let east = [-lam.sin(), lam.cos(), 0.0];
        let north = [-phi.sin() * lam.cos(), -phi.sin() * lam.sin(), phi.cos()];
        let up = [phi.cos() * lam.cos(), phi.cos() * lam.sin(), phi.sin()];
        for (local, axis) in [
            ([1.0, 0.0, 0.0], east),
            ([0.0, 1.0, 0.0], north),
            ([0.0, 0.0, 1.0], up),
        ] {
            let d = unit(local);
            for k in 0..3 {
                assert!(
                    (d[k] - axis[k]).abs() < 1e-8,
                    "{local:?}: {d:?} vs {axis:?}"
                );
            }
        }
        // The origin is the geodetic point (to the rounding of CASA's b).
        let reference = geodetic_to_xyz(lon.to_radians(), lat.to_radians(), alt, &WGS84);
        for k in 0..3 {
            assert!((origin[k] - reference[k]).abs() < 1e-3);
        }
    }
}
