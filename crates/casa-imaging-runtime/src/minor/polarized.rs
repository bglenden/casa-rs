// SPDX-License-Identifier: LGPL-3.0-or-later
//! Experimental joint-polarization Clark runtime.

use super::{
    Inputs, MinorCycleRunError, PlaneData, PlaneKey, Solved, TracedComponent, clean, load,
    plane_pixels, plane_shape, result_bytes,
};
use crate::{Demand, HostResources, ResourcePolicy, admit};
use casa_imaging_deconvolution::{
    ClarkPolarized, Component, CycleControls, MinorCycleView, PlaneStatistics, PsfSummary, Support,
    peak_magnitude,
};
use casa_imaging_model::{ModelCell, ModelDeltaTerm, ModelValue};

pub(super) fn solve<const N: usize>(
    inputs: &Inputs<'_>,
    admission: (&HostResources, &ResourcePolicy),
    keys: &[PlaneKey],
    statistics: &[PlaneStatistics],
    summary: &PsfSummary,
    controls: &CycleControls,
) -> Result<Solved, MinorCycleRunError> {
    assert_eq!(keys.len(), N);
    let mut planes = keys
        .iter()
        .map(|key| {
            load(inputs, *key).map(|plane| plane.unwrap_or_else(|| blank_plane(inputs, *key)))
        })
        .collect::<Result<Vec<PlaneData<'_>>, _>>()?;
    let mut residual = Vec::with_capacity(N);
    for plane in &mut planes {
        residual.push(plane.residual.pop().expect("one residual per polarization"));
    }
    let first = &planes[0];
    // CASA uses polarization zero's PSF even when it is blank. Its fitted
    // beam set gives that plane the domain's largest beam.
    let blank_summary = if !super::holds_data(inputs.normal, false, keys[0]) {
        Some(summary.with_domain_beam(&first.psf[0], first.shape, summary.beam()))
    } else {
        None
    };
    let summary = blank_summary.as_ref().unwrap_or(summary);
    let view = MinorCycleView {
        shape: first.shape,
        residual: &residual,
        psf: &first.psf,
        summary,
        support: &first.support,
    };
    let mut combined = statistics[0];
    for stats in &statistics[1..] {
        combined.peak_in_mask = combined.peak_in_mask.max(stats.peak_in_mask);
        combined.peak_no_mask = combined.peak_no_mask.max(stats.peak_no_mask);
    }
    let mut outcome = clean(&ClarkPolarized::<N>::new(None), &view, controls, &combined)?;
    let end = std::mem::take(&mut outcome.end_residual);
    let mut peak_no_mask = 0.0_f64;
    for (p, plane) in planes.iter().enumerate() {
        let Some(values) = end.get(p) else {
            peak_no_mask = peak_no_mask.max(statistics[p].peak_no_mask);
            continue;
        };
        let pixels = plane_pixels(inputs, keys[p], plane)?;
        peak_no_mask = peak_no_mask.max(peak_magnitude(
            values,
            pixels.residual_mask(inputs.setup.residual_pixel_mask),
        ));
    }
    let delta = std::mem::take(&mut outcome.delta);
    let entries = (0..N).map(|p| delta.term(p).count()).sum::<usize>();
    let charge = admit(
        admission.0,
        admission.1,
        &Demand {
            phase: "minor-cycle results",
            memory: result_bytes(entries),
        },
    )?;
    let mut terms = Vec::with_capacity(entries);
    for p in 0..N {
        let physical = planes[p].physical()?;
        for (index, flux) in delta.term(p) {
            let value = physical.apply(flux, index)?;
            if value == 0.0 {
                continue;
            }
            let key = keys[p];
            let cell = ModelCell::new(
                key.domain,
                key.channel,
                key.polarization,
                planes[p].shape.pixel(index),
            );
            let flat = inputs
                .base
                .shape()
                .flat_index(cell)
                .ok_or(super::ModelLifecycleError::CellOutsideShape)?;
            terms.push((
                flat,
                ModelDeltaTerm::new(
                    cell,
                    ModelValue::new(value).map_err(super::ModelLifecycleError::Contract)?,
                ),
            ));
        }
    }
    let trace = group_trace(&outcome.trace, &planes, keys);
    Ok(Solved {
        outcome,
        peak_no_mask,
        terms,
        trace,
        charge,
    })
}

/// The zero-valued residual and PSF of a polarization with no data. CASA's
/// joint Clark uses the first polarization's mask even when that plane is blank.
fn blank_plane<'a>(inputs: &Inputs<'a>, key: PlaneKey) -> PlaneData<'a> {
    let shape = plane_shape(inputs.normal, key);
    let mask = inputs
        .masks
        .get(key.domain)
        .and_then(|m| {
            m.plane(
                key.channel - inputs.normal.slab().core_range().start,
                key.polarization,
            )
        })
        .expect("mask for every plane");
    PlaneData {
        shape,
        residual: vec![vec![0.0; shape.len()]],
        psf: vec![vec![0.0; shape.len()]],
        support: Support::new(shape, mask.to_vec()),
        valid: Support::new(shape, vec![false; shape.len()]),
        response: None,
    }
}

fn group_trace(
    components: &[Component],
    planes: &[PlaneData<'_>],
    keys: &[PlaneKey],
) -> Vec<TracedComponent> {
    components
        .iter()
        .map(|component| {
            let key = keys[component.term];
            TracedComponent {
                cell: ModelCell::new(
                    key.domain,
                    key.channel,
                    key.polarization,
                    planes[component.term].shape.pixel(component.index),
                ),
                flux: component.flux,
                scale_px: 0.0,
            }
        })
        .collect()
}
