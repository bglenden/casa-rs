// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Edges in the direction conversion routing graph.
pub(super) const ROUTING_EDGES: &[(DirectionRef, DirectionRef)] = &[
    (DirectionRef::J2000, DirectionRef::GALACTIC),
    (DirectionRef::J2000, DirectionRef::ICRS),
    (DirectionRef::GALACTIC, DirectionRef::SUPERGAL),
    (DirectionRef::J2000, DirectionRef::ECLIPTIC),
    (DirectionRef::J2000, DirectionRef::JMEAN),
    (DirectionRef::JMEAN, DirectionRef::JTRUE),
    (DirectionRef::JMEAN, DirectionRef::MECLIPTIC),
    (DirectionRef::JTRUE, DirectionRef::TECLIPTIC),
    (DirectionRef::JNAT, DirectionRef::J2000),
    (DirectionRef::JNAT, DirectionRef::APP),
    (DirectionRef::HADEC, DirectionRef::AZEL),
    (DirectionRef::HADEC, DirectionRef::AZELGEO),
    (DirectionRef::AZEL, DirectionRef::AZELSW),
    (DirectionRef::AZELGEO, DirectionRef::AZELSWGEO),
    // casacore reaches HADEC from APP through TOPO.
    (DirectionRef::APP, DirectionRef::TOPO),
    (DirectionRef::TOPO, DirectionRef::HADEC),
    (DirectionRef::J2000, DirectionRef::B1950),
    // casacore converts between GALACTIC and B1950 directly, not via J2000.
    (DirectionRef::GALACTIC, DirectionRef::B1950),
    (DirectionRef::HADEC, DirectionRef::ITRF),
    (DirectionRef::J2000, DirectionRef::B1950Vla),
    (DirectionRef::B1950, DirectionRef::B1950Vla),
    (DirectionRef::B1950, DirectionRef::BMEAN),
    (DirectionRef::BMEAN, DirectionRef::BTRUE),
];

/// Maps a DirectionRef to a unique index for BFS.
pub(super) fn dir_ref_index(r: DirectionRef) -> usize {
    match r {
        DirectionRef::J2000 => 0,
        DirectionRef::JMEAN => 1,
        DirectionRef::JTRUE => 2,
        DirectionRef::APP => 3,
        DirectionRef::JNAT => 4,
        DirectionRef::B1950 => 5,
        DirectionRef::GALACTIC => 6,
        DirectionRef::SUPERGAL => 7,
        DirectionRef::ICRS => 8,
        DirectionRef::ECLIPTIC => 9,
        DirectionRef::MECLIPTIC => 10,
        DirectionRef::TECLIPTIC => 11,
        DirectionRef::HADEC => 12,
        DirectionRef::AZEL => 13,
        DirectionRef::AZELSW => 14,
        DirectionRef::AZELGEO => 15,
        DirectionRef::AZELSWGEO => 16,
        DirectionRef::ITRF => 17,
        DirectionRef::TOPO => 18,
        DirectionRef::B1950Vla => 19,
        DirectionRef::BMEAN => 20,
        DirectionRef::BTRUE => 21,
        _ => unreachable!("dynamical references resolve before rotation routing"),
    }
}

/// Pre-computed BFS paths for all reachable (source, target) pairs.
pub(super) type DirPathMap = HashMap<(DirectionRef, DirectionRef), Option<Vec<DirectionRef>>>;

pub(super) static DIR_PATH_CACHE: LazyLock<DirPathMap> = LazyLock::new(|| {
    let mut cache = HashMap::new();
    for &src in &DirectionRef::ALL {
        for &tgt in &DirectionRef::ALL {
            if src != tgt {
                cache.insert((src, tgt), bfs_find_dir_path(src, tgt));
            }
        }
    }
    cache
});

/// Finds the shortest path from `source` to `target` in the routing graph.
/// Results are cached in `DIR_PATH_CACHE`.
pub(super) fn find_path(
    source: DirectionRef,
    target: DirectionRef,
) -> Result<Vec<DirectionRef>, MeasureError> {
    DIR_PATH_CACHE
        .get(&(source, target))
        .expect("all pairs pre-computed")
        .clone()
        .ok_or_else(|| MeasureError::NotYetImplemented {
            route: format!("{source} → {target} (no route found)"),
        })
}

/// BFS implementation used to populate the path cache.
pub(super) fn bfs_find_dir_path(
    source: DirectionRef,
    target: DirectionRef,
) -> Option<Vec<DirectionRef>> {
    let mut visited = [false; NUM_DIR_REFS];
    let mut parent: [Option<DirectionRef>; NUM_DIR_REFS] = [None; NUM_DIR_REFS];
    let mut queue = VecDeque::new();

    let src_idx = dir_ref_index(source);
    visited[src_idx] = true;
    queue.push_back(source);

    let mut found = false;

    while let Some(current) = queue.pop_front() {
        if current == target {
            found = true;
            break;
        }

        for &(a, b) in ROUTING_EDGES {
            let neighbor = if a == current {
                b
            } else if b == current {
                a
            } else {
                continue;
            };
            let idx = dir_ref_index(neighbor);
            if !visited[idx] {
                visited[idx] = true;
                parent[idx] = Some(current);
                queue.push_back(neighbor);
            }
        }
    }

    if found {
        let mut path = Vec::new();
        let mut cur = target;
        while cur != source {
            path.push(cur);
            cur = parent[dir_ref_index(cur)].expect("BFS parent must be set");
        }
        path.reverse();
        Some(path)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Conversion context: caches epoch-derived values across hops
// ---------------------------------------------------------------------------

/// Caches expensive epoch-derived values (TT JD pair, UT1 JD pair, GAST)
/// so they are computed at most once per `convert_to` call.
pub(super) struct ConvCtx {
    pub(super) distance_m: f64,
    pub(super) tt_jd: RefCell<Option<(f64, f64)>>,
    pub(super) ut1_jd: RefCell<Option<(f64, f64)>>,
    pub(super) gast: RefCell<Option<f64>>,
    pub(super) precession: RefCell<Option<[[f64; 3]; 3]>>,
    pub(super) nutation: RefCell<Option<[[f64; 3]; 3]>>,
    pub(super) earth_vel: RefCell<Option<([f64; 3], f64)>>,
}

impl ConvCtx {
    pub(super) fn new() -> Self {
        Self {
            distance_m: 0.0,
            tt_jd: RefCell::new(None),
            ut1_jd: RefCell::new(None),
            gast: RefCell::new(None),
            precession: RefCell::new(None),
            nutation: RefCell::new(None),
            earth_vel: RefCell::new(None),
        }
    }

    pub(super) fn get_tt_jd(&self, frame: &MeasFrame) -> Result<(f64, f64), MeasureError> {
        if let Some(v) = *self.tt_jd.borrow() {
            return Ok(v);
        }
        let v = frame.cached_tt_jd(|| compute_tt_jd(frame))?;
        *self.tt_jd.borrow_mut() = Some(v);
        Ok(v)
    }

    pub(super) fn get_ut1_jd(&self, frame: &MeasFrame) -> Result<(f64, f64), MeasureError> {
        if let Some(v) = *self.ut1_jd.borrow() {
            return Ok(v);
        }
        let v = frame.cached_ut1_jd(|| compute_ut1_jd(frame))?;
        *self.ut1_jd.borrow_mut() = Some(v);
        Ok(v)
    }

    pub(super) fn get_gast(&self, frame: &MeasFrame) -> Result<f64, MeasureError> {
        if let Some(v) = *self.gast.borrow() {
            return Ok(v);
        }
        let v = frame.cached_gast(|| compute_gast(frame, self))?;
        *self.gast.borrow_mut() = Some(v);
        Ok(v)
    }

    pub(super) fn get_precession(&self, frame: &MeasFrame) -> Result<[[f64; 3]; 3], MeasureError> {
        if let Some(v) = *self.precession.borrow() {
            return Ok(v);
        }
        let v = frame.cached_precession(|| {
            let (tt1, tt2) = self.get_tt_jd(frame)?;
            Ok(match frame.iau_model() {
                IauModel::Iau1976_1980 => sofars::pnp::pmat76(tt1, tt2),
                IauModel::Iau2006_2000A => sofars::pnp::pmat00(tt1, tt2),
            })
        })?;
        *self.precession.borrow_mut() = Some(v);
        Ok(v)
    }

    pub(super) fn get_nutation(&self, frame: &MeasFrame) -> Result<[[f64; 3]; 3], MeasureError> {
        if let Some(v) = *self.nutation.borrow() {
            return Ok(v);
        }
        let v = frame.cached_nutation(|| {
            let (tt1, tt2) = self.get_tt_jd(frame)?;
            Ok(match frame.iau_model() {
                IauModel::Iau1976_1980 => sofars::pnp::nutm80(tt1, tt2),
                IauModel::Iau2006_2000A => sofars::pnp::num00a(tt1, tt2),
            })
        })?;
        *self.nutation.borrow_mut() = Some(v);
        Ok(v)
    }

    pub(super) fn get_earth_vel(&self, frame: &MeasFrame) -> Result<([f64; 3], f64), MeasureError> {
        if let Some(v) = *self.earth_vel.borrow() {
            return Ok(v);
        }
        let v = frame.cached_earth_velocity_au_per_day(|| {
            let (tt1, tt2) = self.get_tt_jd(frame)?;
            let (pvh, pvb) =
                sofars::eph::epv00(tt1, tt2).ok_or(MeasureError::SofarsError { code: -1 })?;
            let sun_dist = sofars::vm::pm(pvh[0]);
            Ok((pvb[1], sun_dist))
        })?;
        *self.earth_vel.borrow_mut() = Some(v);
        Ok(v)
    }
}

// ---------------------------------------------------------------------------
// Hop implementations
// ---------------------------------------------------------------------------

/// Applies a single conversion hop.
pub(super) fn apply_hop(
    cosines: [f64; 3],
    from: DirectionRef,
    to: DirectionRef,
    frame: &MeasFrame,
    ctx: &ConvCtx,
) -> Result<[f64; 3], MeasureError> {
    use DirectionRef::*;

    match (from, to) {
        // ----- Constant matrices -----

        // J2000, B1950 and SUPERGAL ↔ GALACTIC (casacore's matrices)
        (J2000, GALACTIC) => Ok(casacore_frames::j2000_to_galactic(cosines)),
        (GALACTIC, J2000) => Ok(casacore_frames::galactic_to_j2000(cosines)),
        (B1950, GALACTIC) => Ok(casacore_frames::b1950_to_galactic(cosines)),
        (GALACTIC, B1950) => Ok(casacore_frames::galactic_to_b1950(cosines)),
        (SUPERGAL, GALACTIC) => Ok(casacore_frames::supergalactic_to_galactic(cosines)),
        (GALACTIC, SUPERGAL) => Ok(casacore_frames::galactic_to_supergalactic(cosines)),

        // J2000 ↔ ICRS (frame tie, ~17 mas)
        (ICRS, J2000) => {
            let m = icrs_to_j2000_matrix();
            Ok(rotate(&m, &cosines))
        }
        (J2000, ICRS) => {
            let m = icrs_to_j2000_matrix();
            Ok(rotate_t(&m, &cosines))
        }

        // AZEL ↔ AZELSW (sign flip: Az_SW = Az + π, El_SW = El)
        // In direction cosines: negate x and y, keep z.
        (AZEL, AZELSW) | (AZELSW, AZEL) => Ok([-cosines[0], -cosines[1], cosines[2]]),

        // AZELGEO ↔ AZELSWGEO (same sign flip)
        (AZELGEO, AZELSWGEO) | (AZELSWGEO, AZELGEO) => Ok([-cosines[0], -cosines[1], cosines[2]]),

        // J2000 ↔ JNAT (solar gravitational deflection)
        (J2000, JNAT) => apply_solar_deflection(cosines, frame),
        (JNAT, J2000) => remove_solar_deflection(cosines, frame),

        // JNAT ↔ APP. C++ casacore combines annual aberration with the
        // J2000-to-true precession/nutation rotation in this hop.
        (JNAT, APP) => {
            let beta = casacore_aberration_beta(frame)?;
            let aberrated_j2000 = apply_relativistic_aberration(cosines, beta);
            let precession = precession_matrix(frame, ctx)?;
            let nutation = nutation_matrix(frame, ctx)?;
            Ok(rotate(&nutation, &rotate(&precession, &aberrated_j2000)))
        }
        (APP, JNAT) => {
            let precession = precession_matrix(frame, ctx)?;
            let nutation = nutation_matrix(frame, ctx)?;
            let aberrated_j2000 = rotate_t(&precession, &rotate_t(&nutation, &cosines));
            let beta = casacore_aberration_beta(frame)?.map(|component| -component);
            Ok(apply_relativistic_aberration(aberrated_j2000, beta))
        }

        // ----- Epoch-dependent matrices -----

        // J2000 → JMEAN (precession with frame bias)
        (J2000, JMEAN) => {
            let rp = precession_matrix(frame, ctx)?;
            Ok(rotate(&rp, &cosines))
        }
        (JMEAN, J2000) => {
            let rp = precession_matrix(frame, ctx)?;
            Ok(rotate_t(&rp, &cosines))
        }

        // JMEAN → JTRUE (nutation)
        (JMEAN, JTRUE) => {
            let rn = nutation_matrix(frame, ctx)?;
            Ok(rotate(&rn, &cosines))
        }
        (JTRUE, JMEAN) => {
            let rn = nutation_matrix(frame, ctx)?;
            Ok(rotate_t(&rn, &cosines))
        }

        // J2000 ↔ ECLIPTIC (casacore's mean obliquity at J2000.0, no bias)
        (J2000, ECLIPTIC) => Ok(rotate(&j2000_ecliptic_rotation(), &cosines)),
        (ECLIPTIC, J2000) => Ok(rotate_t(&j2000_ecliptic_rotation(), &cosines)),

        // JMEAN ↔ MECLIPTIC (casacore's mean obliquity at the TDB epoch)
        (JMEAN, MECLIPTIC) | (MECLIPTIC, JMEAN) => {
            let eps = casacore_frames::mean_obliquity_rad(tdb_mjd(frame)?);
            Ok(rotate_obliquity(eps, &cosines, from == JMEAN))
        }

        // JTRUE ↔ TECLIPTIC (true obliquity of casacore's IAU 1980
        // nutation, `Nutation::STANDARD`, under either IAU model)
        (JTRUE, TECLIPTIC) | (TECLIPTIC, JTRUE) => {
            let eps = standard_true_obliquity_rad(tdb_mjd(frame)?);
            Ok(rotate_obliquity(eps, &cosines, from == JTRUE))
        }

        // JTRUE → APP (aberration)
        (JTRUE, APP) => apply_annual_aberration(cosines, frame, ctx),
        // APP → JTRUE (inverse aberration — iterative)
        (APP, JTRUE) => remove_annual_aberration(cosines, frame, ctx),

        // APP ↔ TOPO: casacore's `applyAPPtoTOPO` shifts only a direction
        // with a distance (a planet's), so a sky direction is unchanged.
        (APP, TOPO) => apparent_parallax(cosines, -ctx.distance_m, frame, ctx),
        (TOPO, APP) => apparent_parallax(cosines, ctx.distance_m, frame, ctx),

        // TOPO → HADEC (diurnal aberration + polar motion + sidereal time)
        (TOPO, HADEC) => topocentric_to_hadec(cosines, frame, ctx),
        // HADEC → TOPO (inverse of above)
        (HADEC, TOPO) => hadec_to_topocentric(cosines, frame, ctx),

        // HADEC ↔ AZEL (horizon coordinates using geocentric latitude)
        // C++ casacore uses geocentric latitude for AZEL (MeasMath::applyHADECtoAZEL
        // calls getInfo(LAT), which is asin(z/r) from ITRF XYZ).
        (HADEC, AZEL) => {
            let lat = get_geocentric_latitude(frame)?;
            let (ha, dec) = sofars::vm::c2s(&cosines);
            let [az, el] = sofars::coords::hd2ae(ha, dec, lat);
            Ok(sofars::vm::s2c(az, el))
        }
        (AZEL, HADEC) => {
            let lat = get_geocentric_latitude(frame)?;
            let (az, el) = sofars::vm::c2s(&cosines);
            let [ha, dec] = sofars::coords::ae2hd(az, el, lat);
            Ok(sofars::vm::s2c(ha, dec))
        }

        // HADEC ↔ AZELGEO (horizon coordinates using geodetic latitude)
        // C++ casacore uses geodetic latitude for AZELGEO (MeasMath::applyHADECtoAZELGEO
        // calls getInfo(LATGEO), which is from WGS84 conversion).
        (HADEC, AZELGEO) => {
            let lat = get_latitude(frame)?;
            let (ha, dec) = sofars::vm::c2s(&cosines);
            let [az, el] = sofars::coords::hd2ae(ha, dec, lat);
            Ok(sofars::vm::s2c(az, el))
        }
        (AZELGEO, HADEC) => {
            let lat = get_latitude(frame)?;
            let (az, el) = sofars::vm::c2s(&cosines);
            let [ha, dec] = sofars::coords::ae2hd(az, el, lat);
            Ok(sofars::vm::s2c(ha, dec))
        }

        (J2000, B1950Vla) => Ok(casacore_frames::j2000_to_b1950(cosines, 19.799 - 20.0)),
        (B1950Vla, J2000) => Ok(casacore_frames::b1950_to_j2000(cosines, 19.799 - 20.0)),
        (B1950, B1950Vla) => Ok(casacore_frames::j2000_to_b1950(
            casacore_frames::b1950_to_j2000(cosines, b1950_epoch_centuries(frame, ctx)?),
            19.799 - 20.0,
        )),
        (B1950Vla, B1950) => Ok(casacore_frames::j2000_to_b1950(
            casacore_frames::b1950_to_j2000(cosines, 19.799 - 20.0),
            b1950_epoch_centuries(frame, ctx)?,
        )),
        (B1950, APP) | (APP, B1950) => vector_rotation::fk4_apparent_direction(cosines, to, frame),
        (B1950, BMEAN) | (BMEAN, B1950) => Ok(casacore_frames::besselian_precession(
            cosines,
            tdb_mjd(frame)?,
            from == BMEAN,
        )),
        (BMEAN, BTRUE) | (BTRUE, BMEAN) => Ok(casacore_frames::besselian_nutation(
            cosines,
            tdb_mjd(frame)?,
            from == BTRUE,
        )),

        // ----- B1950 (FK4, casacore's rotation and E-terms) -----
        (J2000, B1950) => Ok(casacore_frames::j2000_to_b1950(
            cosines,
            b1950_epoch_centuries(frame, ctx)?,
        )),
        (B1950, J2000) => Ok(casacore_frames::b1950_to_j2000(
            cosines,
            b1950_epoch_centuries(frame, ctx)?,
        )),

        // ----- ITRF (via HADEC, matching C++ MeasMath::applyHADECtoITRF) -----

        // HADEC → ITRF: rotate by -longitude around z, then negate y.
        //
        // C++ MeasMath::applyHADECtoITRF does `in *= Rz(lon)` (which means
        // `in = Rz(lon)^T * in = Rz(-lon) * in`) then negates y.
        (HADEC, ITRF) => {
            let lon = get_longitude(frame)?;
            let (s, c) = lon.sin_cos();
            // Rz(-lon) * in
            let x = c * cosines[0] + s * cosines[1];
            let y = -s * cosines[0] + c * cosines[1];
            let z = cosines[2];
            // Negate y
            Ok([x, -y, z])
        }
        // ITRF → HADEC: negate y, then rotate by +longitude around z.
        //
        // C++ MeasMath::deapplyHADECtoITRF negates y then does
        // `in = Rz(lon) * in`.
        (ITRF, HADEC) => {
            let lon = get_longitude(frame)?;
            let (s, c) = lon.sin_cos();
            // Negate y first
            let ny = -cosines[1];
            // Rz(lon) * in
            let x = c * cosines[0] - s * ny;
            let y = s * cosines[0] + c * ny;
            let z = cosines[2];
            Ok([x, y, z])
        }

        _ => Err(MeasureError::NotYetImplemented {
            route: format!("{from} → {to}"),
        }),
    }
}

/// JTRUE → APP hop: apply annual aberration from the Earth's barycentric
/// velocity, rotated into the true equatorial frame.
pub(super) fn apply_annual_aberration(
    cosines: [f64; 3],
    frame: &MeasFrame,
    ctx: &ConvCtx,
) -> Result<[f64; 3], MeasureError> {
    let (v_au_day, sun_dist) = earth_velocity_au_per_day(frame, ctx)?;
    // Convert velocity to units of c
    let v = [
        v_au_day[0] / C_AU_PER_DAY,
        v_au_day[1] / C_AU_PER_DAY,
        v_au_day[2] / C_AU_PER_DAY,
    ];
    // Rotate velocity to true equatorial frame
    let (tt1, tt2) = get_tt_jd(frame, ctx)?;
    let rbpn = bias_precession_nutation(frame, tt1, tt2);
    let v_true = rotate(&rbpn, &v);
    // Reciprocal of Lorentz factor: sqrt(1 - |v|²)
    let v2 = v[0] * v[0] + v[1] * v[1] + v[2] * v[2];
    let bm1 = (1.0 - v2).sqrt();
    Ok(sofars::astro::ab(&cosines, &v_true, sun_dist, bm1))
}

/// APP → JTRUE hop: remove annual aberration by fixed-point iteration on
/// the forward transform of [`apply_annual_aberration`].
pub(super) fn remove_annual_aberration(
    cosines: [f64; 3],
    frame: &MeasFrame,
    ctx: &ConvCtx,
) -> Result<[f64; 3], MeasureError> {
    let (v_au_day, sun_dist) = earth_velocity_au_per_day(frame, ctx)?;
    let v = [
        v_au_day[0] / C_AU_PER_DAY,
        v_au_day[1] / C_AU_PER_DAY,
        v_au_day[2] / C_AU_PER_DAY,
    ];
    let (tt1, tt2) = get_tt_jd(frame, ctx)?;
    let rbpn = bias_precession_nutation(frame, tt1, tt2);
    let v_true = rotate(&rbpn, &v);
    let v2 = v[0] * v[0] + v[1] * v[1] + v[2] * v[2];
    let bm1 = (1.0 - v2).sqrt();

    // Iterative removal: start with apparent as guess for natural
    let mut pnat = cosines;
    for _ in 0..5 {
        let papp = sofars::astro::ab(&pnat, &v_true, sun_dist, bm1);
        // Correct: pnat += (cosines - papp)
        for k in 0..3 {
            pnat[k] += cosines[k] - papp[k];
        }
        // Re-normalize
        let (_, unit) = sofars::vm::pn(&pnat);
        pnat = unit;
    }
    Ok(pnat)
}

/// Observer displacement along the local geocentric zenith, in APP axes.
pub(super) fn apparent_parallax(
    cosines: [f64; 3],
    signed_distance_m: f64,
    frame: &MeasFrame,
    ctx: &ConvCtx,
) -> Result<[f64; 3], MeasureError> {
    if signed_distance_m == 0.0 {
        return Ok(cosines);
    }
    let last = get_gast(frame, ctx)? + get_longitude(frame)?;
    let zenith = sofars::vm::s2c(last, get_geocentric_latitude(frame)?);
    let scale = get_geocentric_radius(frame)? / signed_distance_m;
    let shifted = std::array::from_fn(|i| cosines[i] + scale * zenith[i]);
    Ok(sofars::vm::pn(&shifted).1)
}

/// TOPO → HADEC hop (diurnal aberration + polar motion + sidereal time),
/// casacore's `MeasMath::applyTOPOtoHADEC`. The steps are:
///   1. Diurnal aberration: add v/c in zenith direction, renormalize
///   2. Polar motion + LAST rotation via Euler R = Ry(xp)×Rx(yp)×Rz(LAST)
///   3. Apply R^T and negate y to get (HA, Dec)
pub(super) fn topocentric_to_hadec(
    cosines: [f64; 3],
    frame: &MeasFrame,
    ctx: &ConvCtx,
) -> Result<[f64; 3], MeasureError> {
    let gast = get_gast(frame, ctx)?;
    let lon = get_longitude(frame)?;
    let last = gast + lon;

    // Diurnal aberration: shift in direction of (LAST, geocentric_lat)
    let geo_lat = get_geocentric_latitude(frame)?;
    let radius = get_geocentric_radius(frame)?;
    let v_c = diurnal_aberration_factor(radius);
    let aberr_dir = sofars::vm::s2c(last, geo_lat);
    let shifted = [
        cosines[0] + v_c * aberr_dir[0],
        cosines[1] + v_c * aberr_dir[1],
        cosines[2] + v_c * aberr_dir[2],
    ];
    let (_, unit) = sofars::vm::pn(&shifted);

    // Polar motion + LAST rotation + y-negate via Euler matrix
    // C++ uses: in *= R(xp, Ry, yp, Rx, LAST, Rz); in.y = -in.y
    // which is: in = R^T * in, then negate y
    let (_, ut2) = get_ut1_jd(frame, ctx)?;
    let (xp, yp) = get_polar_motion_rad(frame, ut2)?;
    let r = polar_motion_euler(-xp, -yp, last);
    let result = rotate_t(&r, &unit);
    Ok([result[0], -result[1], result[2]])
}

/// HADEC → TOPO hop: the inverse of [`topocentric_to_hadec`].
pub(super) fn hadec_to_topocentric(
    cosines: [f64; 3],
    frame: &MeasFrame,
    ctx: &ConvCtx,
) -> Result<[f64; 3], MeasureError> {
    let gast = get_gast(frame, ctx)?;
    let lon = get_longitude(frame)?;
    let last = gast + lon;

    // applyPolarMotion: negate y, then apply R
    let (_, ut2) = get_ut1_jd(frame, ctx)?;
    let (xp, yp) = get_polar_motion_rad(frame, ut2)?;
    let r = polar_motion_euler(-xp, -yp, last);
    let negated = [cosines[0], -cosines[1], cosines[2]];
    let rotated = rotate(&r, &negated);

    // Undo diurnal aberration
    let geo_lat = get_geocentric_latitude(frame)?;
    let radius = get_geocentric_radius(frame)?;
    let v_c = diurnal_aberration_factor(radius);
    let aberr_dir = sofars::vm::s2c(last, geo_lat);
    let unshifted = [
        rotated[0] - v_c * aberr_dir[0],
        rotated[1] - v_c * aberr_dir[1],
        rotated[2] - v_c * aberr_dir[2],
    ];
    let (_, unit) = sofars::vm::pn(&unshifted);
    Ok(unit)
}

// ---------------------------------------------------------------------------
// Public helper: get J2000 direction (used by frequency module)
// ---------------------------------------------------------------------------

impl MDirection {
    /// Evaluate geocentric apparent place for the horizon-crossing calculation.
    ///
    /// Casacore uses its direct FK4 precession, nutation, and aberration
    /// for a B1950 input. Other references use the normal APP conversion.
    pub fn to_apparent(&self, frame: &MeasFrame) -> Result<Self, MeasureError> {
        if self.refer != DirectionRef::B1950 {
            return self.convert_to(DirectionRef::APP, frame);
        }
        let cosines =
            vector_rotation::fk4_apparent_direction(self.cosines, DirectionRef::APP, frame)?;
        Ok(Self {
            refer: DirectionRef::APP,
            cosines,
        })
    }

    /// Returns this direction converted to J2000 cosines.
    ///
    /// This is used internally by frequency conversions to compute the
    /// dot product with velocity vectors defined in J2000.
    pub fn to_j2000(&self, frame: &MeasFrame) -> Result<[f64; 3], MeasureError> {
        if self.refer == DirectionRef::J2000 {
            return Ok(self.cosines);
        }
        let j2000 = self.convert_to(DirectionRef::J2000, frame)?;
        Ok(j2000.cosines)
    }
}
