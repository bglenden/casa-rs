// SPDX-License-Identifier: LGPL-3.0-or-later
//! Experimental joint-polarization Clark runtime.

use super::{
    Inputs, MinorCycleRunError, PlaneData, PlaneKey, Solved, TracedComponent, clean, load,
    plane_pixels, result_bytes,
};
use crate::{Demand, HostResources, ResourcePolicy, admit};
use casa_imaging_deconvolution::{
    ClarkPolarized, Component, CycleControls, MinorCycleView, PlaneStatistics, PsfSummary,
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
            load(inputs, *key)
                .map(|plane| plane.expect("experiment requires data in every polarization"))
        })
        .collect::<Result<Vec<PlaneData<'_>>, _>>()?;
    let mut residual = Vec::with_capacity(N);
    for plane in &mut planes {
        residual.push(plane.residual.pop().expect("one residual per polarization"));
    }
    let first = &planes[0];
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
