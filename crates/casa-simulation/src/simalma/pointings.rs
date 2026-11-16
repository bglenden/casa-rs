// SPDX-License-Identifier: LGPL-3.0-or-later
//! TP rectangles or disjoint patches, and their integration-time adjustment.
use super::{Parameters, failure, plan::Plan};
use crate::SessionResources;
use crate::{
    simobserve::{SimobserveError, SimobserveParameters},
    simutil::{
        direction::{Directions, average_direction},
        pointings::{calc_pointings, read_pointings},
        qa,
    },
};
use casa_imaging_runtime::{Demand, Reservation, admit};

pub(super) fn observation(
    p: &Parameters,
    plan: &Plan,
    resources: SessionResources,
) -> Result<(SimobserveParameters, Reservation), SimobserveError> {
    let pb = plan.primary_beam;
    let spacing = pb * 0.96 / (3. * 1.13);
    let qspacing = qa::read(&format!("{spacing}arcsec"))?;
    let rect = p.flag("setpointings") && p.strings("direction").len() <= 1;
    let mut mapsize = plan
        .observation
        .mapsize
        .iter()
        .take(2)
        .map(|value| qa::value_in(&qa::read(value)?, "arcsec"))
        .collect::<Result<Vec<_>, _>>()?;
    if mapsize.len() == 1 {
        mapsize.push(mapsize[0]);
    }
    let upper = if rect {
        ((mapsize[0] + pb) / spacing + 4.).ceil() * ((mapsize[1] + pb) / spacing + 4.).ceil()
    } else {
        let best = plan
            .arrays
            .iter()
            .min_by(|a, b| a.resolution.total_cmp(&b.resolution))
            .unwrap();
        // Even minimal pointing-file lines exceed four bytes; each input
        // needs at most an 8x8 patch, plus container/copy overhead.
        std::fs::metadata(plan.directory.join(format!("{}.ptg.txt", best.stem)))
            .map_err(failure)?
            .len() as f64
            * 32.
    };
    let memory = (upper as u64)
        .checked_mul(1024)
        .and_then(|bytes| bytes.checked_add(32 << 20))
        .ok_or_else(|| failure("TP pointing layout overflows"))?;
    let reservation = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "ALMA TP pointing layout",
            memory,
        },
    )
    .map_err(failure)?;
    let mut direction = plan.observation.direction.clone();
    if matches!(&direction,Directions::One(value) if value.is_empty()) {
        direction = Directions::One(plan.model.direction.clone());
    }
    let count = if rect {
        mapsize.iter_mut().for_each(|value| *value += pb);
        (mapsize[0] / spacing).trunc() * (mapsize[1] / spacing).trunc()
    } else {
        let best = plan
            .arrays
            .iter()
            .min_by(|a, b| a.resolution.total_cmp(&b.resolution))
            .unwrap();
        let pointings =
            read_pointings(&plan.directory.join(format!("{}.ptg.txt", best.stem)))?.pointings;
        let centred = average_direction(&pointings)?;
        for (axis, value) in mapsize.iter_mut().enumerate() {
            let low = centred
                .offsets
                .iter()
                .map(|offset| offset[axis])
                .fold(f64::INFINITY, f64::min);
            let high = centred
                .offsets
                .iter()
                .map(|offset| offset[axis])
                .fold(f64::NEG_INFINITY, f64::max);
            *value = (high - low) * 3600. + 2. * pb;
        }
        let local = qa::read(&format!("{}arcsec", 2. * pb))?;
        let first = calc_pointings(
            &qspacing,
            [&local, &local],
            "square",
            &Directions::One(pointings[0].clone()),
            0.5,
            0.,
        )?;
        let patches = pointings.len() * first.len();
        let rectangle = (mapsize[0] / spacing).trunc() * (mapsize[1] / spacing).trunc();
        if (patches as f64) < rectangle {
            let mut all = Vec::with_capacity(patches);
            for pointing in pointings {
                all.extend(calc_pointings(
                    &qspacing,
                    [&local, &local],
                    "square",
                    &Directions::One(pointing),
                    0.5,
                    0.,
                )?);
            }
            direction = Directions::List(all);
            patches as f64
        } else {
            direction = Directions::One(centred.centre);
            rectangle
        }
    };
    let seconds = plan.tp_minutes.trunc() * 60.;
    let integration = qa::value_in(&qa::read(p.text("integration"))?, "s")?;
    if !integration.is_finite() || integration <= 0. || count <= 0. {
        return Err(failure(
            "TP pointing count and integration must be positive",
        ));
    }
    let dumps = (seconds / integration).trunc();
    let integration = if dumps < count {
        integration * dumps / count
    } else {
        integration
    };
    let mut observation = plan.observation.clone();
    observation.setpointings = true;
    observation.ptgfile = "$project.ptg.txt".into();
    observation.direction = direction;
    observation.mapsize = if matches!(observation.direction, Directions::List(_)) {
        vec![String::new(); 2]
    } else {
        mapsize.iter().map(|size| format!("{size}arcsec")).collect()
    };
    observation.integration = format!("{integration}s");
    observation.totaltime = format!("{}min", plan.tp_minutes.trunc());
    observation.maptype = "square".into();
    observation.pointingspacing = format!("{spacing}arcsec");
    observation.obsmode = "sd".into();
    observation.antennalist = String::new();
    observation.sdantlist = plan.tp_configuration.clone();
    let correlators = p.strings("correlator");
    observation.correlator = if correlators.len() > plan.arrays.len() {
        correlators.last().cloned().unwrap_or_default()
    } else {
        String::new()
    };
    Ok((observation, reservation))
}
