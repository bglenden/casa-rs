// SPDX-License-Identifier: LGPL-3.0-or-later
//! Rigid baseline and UVW reference changes. Unlike sky coordinates,
//! corrections for aberration and deflection rotate every vector about the
//! same frame direction, as in casacore's MCBaseline and MCuvw.

use super::{
    ConvCtx, DirectionRef, MDirection, MeasFrame, MeasureError, apply_hop,
    apply_relativistic_aberration, apply_solar_deflection, b1950_epoch_centuries,
    casacore_aberration_beta, casacore_frames, dir_ref_index, get_polar_motion_rad,
    nutation_matrix, polar_motion_euler, precession_matrix, tdb_mjd,
};
use crate::measures::baseline::{
    diurnal_aberration_factor, inverse_aberration_shift, inverse_solar_deflection_shift,
    j2000_baseline_uvw, multiply_matrices, rotate_shift_matrix,
};

type Matrix = [[f64; 3]; 3];
const IDENTITY: Matrix = [[1., 0., 0.], [0., 1., 0.], [0., 0., 1.]];
const FLIP_Y: Matrix = [[1., 0., 0.], [0., -1., 0.], [0., 0., 1.]];

/// Compile the rotation for baseline Cartesian coordinates or, with
/// `uvw`, for the projected coordinates at the given phase centre.
/// The resulting matrix can be applied to a whole vector of baselines.
/// A reference change requiring a frame element reports it when absent.
pub fn baseline_rotation(
    from: DirectionRef,
    to: DirectionRef,
    frame: &MeasFrame,
    uvw: bool,
) -> Result<Matrix, MeasureError> {
    if from == to {
        return Ok(IDENTITY);
    }
    if from.casacore_code() >= 32 || to.casacore_code() >= 32 {
        return Err(MeasureError::NotYetImplemented {
            route: format!("baseline {from} to {to}"),
        });
    }
    let context = ConvCtx::new();
    let mut source = from;
    let mut rotation = IDENTITY;
    for target in route(from, to) {
        let mut hop = baseline_hop(source, target, frame, &context)?;
        if uvw {
            let sky = phase_center(frame)?;
            let reference = uvw_hop_direction(sky, source, target, frame)?;
            let moved = if matches!(
                (source, target),
                (DirectionRef::B1950, DirectionRef::APP) | (DirectionRef::APP, DirectionRef::B1950)
            ) {
                fk4_apparent_direction(reference, target, frame)?
            } else if matches!(
                (source, target),
                (DirectionRef::B1950, DirectionRef::B1950Vla)
                    | (DirectionRef::B1950Vla, DirectionRef::B1950)
            ) {
                // MCuvw makes this label change without rotating the baseline.
                hop = IDENTITY;
                reference
            } else {
                apply_hop(reference, source, target, frame, &context)?
            };
            let start = projection(source, reference);
            let end = projection(target, moved);
            hop = compose(end, compose(hop, transpose(start)));
            use DirectionRef::*;
            if matches!(
                (source, target),
                (HADEC, ITRF | TOPO) | (ITRF | TOPO, HADEC)
            ) {
                // The MCuvw pole rotation reverses both transverse axes in
                // these two boundary hops. Its HADEC/AZEL rotations already
                // agree with the usual projection, so leave those unchanged.
                hop[0] = hop[0].map(|v| -v);
                hop[1] = hop[1].map(|v| -v);
            }
        }
        rotation = compose(hop, rotation);
        source = target;
    }
    Ok(rotation)
}

// Casacore MCBase counts every edge plus its FK4 penalty. The direct
// APP/B1950 conversion is part of the baseline graph even when a caller
// starts in a terrestrial frame. Direct conversions retain their own edge.
fn route(from: DirectionRef, to: DirectionRef) -> Vec<DirectionRef> {
    use super::{NUM_DIR_REFS, ROUTING_EDGES};
    use DirectionRef::*;
    let edges = ROUTING_EDGES.iter().copied().chain([(B1950, APP)]);
    let adjacent = |a, b| {
        edges
            .clone()
            .any(|(x, y)| (a, b) == (x, y) || (a, b) == (y, x))
    };
    if adjacent(from, to) {
        return vec![to];
    }
    let mut costs = [usize::MAX; NUM_DIR_REFS];
    let mut previous = [None; NUM_DIR_REFS];
    let mut seen = [false; NUM_DIR_REFS];
    costs[dir_ref_index(from)] = 0;
    loop {
        let next = DirectionRef::ALL
            .iter()
            .copied()
            .filter(|&r| !seen[dir_ref_index(r)])
            .min_by_key(|&r| costs[dir_ref_index(r)])
            .expect("baseline references are connected");
        if next == to {
            break;
        }
        let index = dir_ref_index(next);
        seen[index] = true;
        for (a, b) in edges.clone() {
            let edge = if next == a {
                b
            } else if next == b {
                a
            } else {
                continue;
            };
            let penalty = if matches!(
                (a, b),
                (J2000, B1950 | B1950Vla)
                    | (GALACTIC, B1950)
                    | (B1950, APP | BMEAN)
                    | (BMEAN, BTRUE)
            ) {
                2
            } else {
                0
            };
            let cost = costs[index] + 1 + penalty;
            let edge_index = dir_ref_index(edge);
            if cost < costs[edge_index] {
                costs[edge_index] = cost;
                previous[edge_index] = Some(next);
            }
        }
    }
    let mut path = Vec::new();
    let mut current = to;
    while current != from {
        path.push(current);
        current = previous[dir_ref_index(current)].expect("baseline route has a predecessor");
    }
    path.reverse();
    path
}

fn phase_center(frame: &MeasFrame) -> Result<&MDirection, MeasureError> {
    frame.direction().ok_or(MeasureError::MissingFrameData {
        what: "direction (for baseline rotation)",
    })
}

fn baseline_hop(
    from: DirectionRef,
    to: DirectionRef,
    frame: &MeasFrame,
    context: &ConvCtx,
) -> Result<Matrix, MeasureError> {
    use DirectionRef::*;
    match (from, to) {
        (HADEC, TOPO) | (TOPO, HADEC) => horizon_rotation(to, frame),
        (J2000, JNAT) | (JNAT, J2000) => solar_rotation(to, frame),
        (JNAT, APP) | (APP, JNAT) => aberration_rotation(to, frame, context),
        (B1950, APP) | (APP, B1950) => fk4_apparent_rotation(to, frame),
        (J2000, B1950 | B1950Vla) | (B1950 | B1950Vla, J2000) => {
            let vla = matches!(from, B1950Vla) || matches!(to, B1950Vla);
            let epoch = if vla {
                19.799 - 20.0
            } else {
                b1950_epoch_centuries(frame, context)?
            };
            let sky = phase_center(frame)?.convert_to(B1950, frame)?.cosines();
            let matrix = casacore_frames::fk4_baseline_rotation(sky, epoch);
            Ok(if to == J2000 {
                matrix
            } else {
                transpose(matrix)
            })
        }
        (B1950, B1950Vla) | (B1950Vla, B1950) => Ok(compose(
            baseline_hop(J2000, to, frame, context)?,
            baseline_hop(from, J2000, frame, context)?,
        )),
        _ => {
            let mut columns = IDENTITY;
            for column in &mut columns {
                *column = apply_hop(*column, from, to, frame, context)?;
            }
            Ok(transpose(columns))
        }
    }
}

fn horizon_rotation(to: DirectionRef, frame: &MeasFrame) -> Result<Matrix, MeasureError> {
    // MCFrame::getLASTr takes the fractional part of the serialized LAST
    // day. Reproduce that rounding before the polar-motion rotation.
    let epoch = frame
        .epoch()
        .ok_or(MeasureError::MissingFrameData { what: "epoch" })?;
    let last = epoch
        .convert_to(crate::measures::EpochRef::LAST, frame)?
        .value()
        .as_mjd()
        .fract()
        * std::f64::consts::TAU;
    let (xp, yp) = get_polar_motion_rad(frame, tdb_mjd(frame)?)?;
    let pole = polar_motion_euler(-xp, -yp, last);
    let position = frame.position().ok_or(MeasureError::MissingFrameData {
        what: "position (for baseline rotation)",
    })?;
    let factor = diurnal_aberration_factor(sofars::vm::pm(position.as_itrf()));
    let offset = sofars::vm::s2c(last, position.geocentric_latitude_rad());
    let app = phase_center(frame)?
        .convert_to(DirectionRef::APP, frame)?
        .cosines();
    if to == DirectionRef::TOPO {
        let correction = rotate_shift_matrix(offset.map(|v| -v * factor), app);
        Ok(compose(correction, compose(pole, FLIP_Y)))
    } else {
        let correction = rotate_shift_matrix(offset.map(|v| v * factor), app);
        Ok(compose(FLIP_Y, compose(transpose(pole), correction)))
    }
}

fn solar_rotation(to: DirectionRef, frame: &MeasFrame) -> Result<Matrix, MeasureError> {
    let sky = phase_center(frame)?.convert_to(DirectionRef::J2000, frame)?;
    let source = sky.cosines();
    let shift = if to == DirectionRef::JNAT {
        let shifted = apply_solar_deflection(source, frame)?;
        std::array::from_fn(|i| shifted[i] - source[i])
    } else if apply_solar_deflection(source, frame)? == source {
        return Ok(IDENTITY);
    } else {
        inverse_solar_deflection_shift(&sky, frame)?
    };
    Ok(rotate_shift_matrix(shift, source))
}

fn aberration_rotation(
    to: DirectionRef,
    frame: &MeasFrame,
    context: &ConvCtx,
) -> Result<Matrix, MeasureError> {
    let sky = phase_center(frame)?.convert_to(DirectionRef::J2000, frame)?;
    let source = sky.cosines();
    let precession_nutation = compose(
        nutation_matrix(frame, context)?,
        precession_matrix(frame, context)?,
    );
    if to == DirectionRef::APP {
        let shifted = apply_relativistic_aberration(source, casacore_aberration_beta(frame)?);
        let correction =
            rotate_shift_matrix(std::array::from_fn(|i| shifted[i] - source[i]), source);
        Ok(compose(precession_nutation, correction))
    } else {
        let correction = rotate_shift_matrix(inverse_aberration_shift(&sky, frame)?, source);
        Ok(compose(correction, transpose(precession_nutation)))
    }
}

/// The E-term displacement at a B1950 source (MeasMath::applyETerms).
fn e_terms_shift(source: [f64; 3]) -> [f64; 3] {
    // applyPrecNutatB1950 uses MeasMath::applyETerms' default epoch
    // argument, distinct from the epoch-centuries in the J2000 route.
    let terms = casacore_frames::e_terms(2000.0);
    let dot = source.iter().zip(terms).map(|(a, b)| a * b).sum::<f64>();
    std::array::from_fn(|i| dot * source[i] - terms[i])
}

fn fk4_apparent_rotation(to: DirectionRef, frame: &MeasFrame) -> Result<Matrix, MeasureError> {
    let epoch = tdb_mjd(frame)?;
    let direction = phase_center(frame)?;
    let b1950 = direction.convert_to(DirectionRef::B1950, frame)?.cosines();
    let app = direction.convert_to(DirectionRef::APP, frame)?.cosines();
    let e_terms = rotate_shift_matrix(e_terms_shift(b1950), b1950);
    let aberration = rotate_shift_matrix(casacore_frames::besselian_aberration(epoch), app);
    let columns = IDENTITY.map(|v| {
        let v = casacore_frames::besselian_precession(v, epoch, false);
        casacore_frames::besselian_nutation(v, epoch, false)
    });
    let matrix = compose(aberration, compose(transpose(columns), e_terms));
    Ok(if to == DirectionRef::APP {
        matrix
    } else {
        transpose(matrix)
    })
}

fn fk4_apparent_direction(
    source: [f64; 3],
    to: DirectionRef,
    frame: &MeasFrame,
) -> Result<[f64; 3], MeasureError> {
    let epoch = tdb_mjd(frame)?;
    let aberration = casacore_frames::besselian_aberration(epoch);
    let add = |a: [f64; 3], b: [f64; 3]| sofars::vm::pn(&std::array::from_fn(|i| a[i] + b[i])).1;
    if to == DirectionRef::APP {
        let v = add(source, e_terms_shift(source));
        let v = casacore_frames::besselian_precession(v, epoch, false);
        let v = casacore_frames::besselian_nutation(v, epoch, false);
        Ok(add(v, aberration))
    } else {
        let v = add(source, aberration.map(|v| -v));
        let v = casacore_frames::besselian_nutation(v, epoch, true);
        let v = casacore_frames::besselian_precession(v, epoch, true);
        Ok(add(v, e_terms_shift(v).map(|v| -v)))
    }
}

fn uvw_hop_direction(
    sky: &MDirection,
    from: DirectionRef,
    to: DirectionRef,
    frame: &MeasFrame,
) -> Result<[f64; 3], MeasureError> {
    use DirectionRef::*;
    // MCuvw builds this phase centre with precession alone for the
    // JTRUE/TECLIPTIC hop. Other hops start in their named reference.
    let source = match (from, to) {
        (JTRUE, TECLIPTIC) => sky.convert_to(JMEAN, frame)?,
        (TECLIPTIC, JTRUE) => {
            let mean = sky.convert_to(JMEAN, frame)?;
            let value = apply_hop(mean.cosines(), JTRUE, TECLIPTIC, frame, &ConvCtx::new())?;
            return Ok(value);
        }
        _ => sky.convert_to(from, frame)?,
    };
    Ok(source.cosines())
}

fn projection(refer: DirectionRef, sky: [f64; 3]) -> Matrix {
    let direction = MDirection::from_cosines(sky, refer);
    transpose(IDENTITY.map(|basis| j2000_baseline_uvw(basis, &direction)))
}

fn compose(a: Matrix, b: Matrix) -> Matrix {
    multiply_matrices(&a, &b)
}

fn transpose(m: Matrix) -> Matrix {
    std::array::from_fn(|i| std::array::from_fn(|j| m[j][i]))
}
