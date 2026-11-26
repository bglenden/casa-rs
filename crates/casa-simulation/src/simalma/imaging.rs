// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native TP gridding/scaling, individual synthesis and fixed-weight concat.
use super::{Parameters, failure, plan::Plan};
use crate::{
    SessionResources,
    plot_sink::SimulationPlotSink,
    simanalyze::{
        SimanalyzeRequest, simanalyze_with_controls, simanalyze_with_plot_sink_and_override,
    },
    simobserve::{Cancel, SimobserveError, check_cancel},
    simutil::analysis,
};
use casa_images::{ImageInterface, PagedImage};
use casa_imaging_runtime::{Demand, admit};
use casa_mstransform::concat::{ConcatRequest, concat_with_memory};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};

pub(super) fn execute<'a, 'b>(
    p: &Parameters,
    plan: &Plan,
    resources: SessionResources,
    mut plot_sink: Option<&'a mut (dyn SimulationPlotSink + 'b)>,
    outcome: &mut crate::simobserve::SimobserveOutcome,
    cancel: &Cancel,
) -> Result<(), SimobserveError> {
    let mut products = Vec::new();
    if !plan.tp_vis.is_empty() {
        products.extend(total_power(
            p,
            plan,
            resources,
            plot_sink.as_deref_mut(),
            cancel,
        )?);
    }
    for array in &plan.arrays {
        let result = synthesis(
            p,
            plan,
            &plan.vis(array),
            resources,
            plot_sink.as_deref_mut(),
            cancel,
        )?;
        products.extend(result.products);
    }
    let high = if plan.arrays.len() > 1 {
        let path = plan.directory.join(format!("{}.concat.ms", plan.project));
        concatenate(
            plan.arrays.iter().map(|array| plan.vis(array)).collect(),
            plan.arrays
                .iter()
                .map(|array| if array.aca { 0.34 } else { 1. })
                .collect(),
            &path,
            resources,
        )?;
        products.push(path.clone());
        let result = synthesis(p, plan, &path, resources, plot_sink, cancel)?;
        products.extend(result.products);
        plan.directory
            .join(format!("{}.concat.image", plan.project))
    } else {
        plan.vis(&plan.arrays[0]).with_extension("image")
    };
    if !plan.tp_vis.is_empty() {
        check_cancel(cancel)?;
        products.extend(super::combine::execute(plan, &high, resources)?);
    }
    outcome.products.extend(products);
    Ok(())
}

fn synthesis<'a, 'b>(
    p: &Parameters,
    plan: &Plan,
    vis: &Path,
    resources: SessionResources,
    plot_sink: Option<&'a mut (dyn SimulationPlotSink + 'b)>,
    cancel: &Cancel,
) -> Result<crate::simanalyze::SimanalyzeOutcome, SimobserveError> {
    let cell = if p.text("cell").is_empty() {
        format!("{}arcsec", plan.model.cell[0])
    } else {
        p.text("cell").to_owned()
    };
    let imsize = if p.0["imsize"]
        .as_array()
        .and_then(|v| v.first())
        .and_then(Value::as_i64)
        .unwrap_or(0)
        > 0
    {
        p.0["imsize"].clone()
    } else {
        json!((plan.model.size[0] / plan.model.cell[0]) as usize)
    };
    let request = SimanalyzeRequest {
        workspace: Some(plan.directory.clone()),
        resources,
        parameters: json!({"project":plan.directory,"vis":vis,"image":true,"modelimage":"","cell":cell,"imsize":imsize,"imdirection":if p.text("imdirection").is_empty(){plan.model.direction.as_str()}else{p.text("imdirection")},"niter":p.0["niter"],"threshold":p.text("threshold"),"weighting":"briggs","mask":[],"outertaper":[],"stokes":"I","analyze":true,"graphics":if plot_sink.is_some(){super::graphics_mode(p)}else{"none"},"verbose":p.flag("verbose"),"overwrite":true})
            .as_object()
            .unwrap()
            .clone(),
    };
    simanalyze_with_controls(&request, plot_sink, cancel)
}

fn total_power<'a, 'b>(
    p: &Parameters,
    plan: &Plan,
    resources: SessionResources,
    plot_sink: Option<&'a mut (dyn SimulationPlotSink + 'b)>,
    cancel: &Cancel,
) -> Result<Vec<PathBuf>, SimobserveError> {
    let primary_beam =
        casa_imaging_singledish::primary_beam_fwhm_arcsec(plan.model.frequency, 12., 0.75, 10.);
    let cell = format!("{}arcsec", primary_beam / 9.);
    let raw = plan.directory.join(format!("{}.sd.image0", plan.project));
    // The single-MS gridder sees all TP dishes through a private native
    // concatenation; retain their row weights, antenna and pointing metadata.
    let staging = tempfile::tempdir_in(&plan.directory).map_err(failure)?;
    let input = if plan.tp_vis.len() > 1 {
        let path = staging.path().join("tp.ms");
        concatenate(plan.tp_vis.clone(), Vec::new(), &path, resources)?;
        path
    } else {
        plan.tp_vis[0].clone()
    };
    let sparse = json!({"infiles":[input],"outfile":raw,"imsize":p.0["imsize"],"cell":[cell.clone(),cell],"phasecenter":plan.model.direction,"mode":"channel","nchan":plan.model.channels,"gridfunction":"sf","convsupport":6});
    let result = casa_imaging_application::single_dish::image_from_sparse_parameters(
        sparse.as_object().unwrap(),
        &crate::simanalyze::imaging::context(resources, cancel),
    )
    .map_err(failure)?;
    let beam = result
        .beam
        .ok_or_else(|| failure("TP beam could not be measured"))?;
    let ratio =
        beam.major_in("arcsec").map_err(failure)?.powi(2) / (plan.primary_beam * 0.96).powi(2);
    let ratio: f32 = format!("{ratio:.6}").parse().map_err(failure)?;
    let image = PagedImage::<f32>::open(&result.image).map_err(failure)?;
    let memory = image
        .shape()
        .iter()
        .try_fold(32u64, |bytes, &n| bytes.checked_mul(n as u64))
        .and_then(|bytes| bytes.checked_add(32 << 20))
        .ok_or_else(|| failure("TP dimensions overflow"))?;
    let _memory = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "ALMA TP intensity scaling",
            memory,
        },
    )
    .map_err(failure)?;
    let output = plan.directory.join(format!("{}.sd.image", plan.project));
    analysis::write_product(
        &output,
        &image.get().map_err(failure)?.mapv(|v| v * ratio),
        image.coordinates().clone(),
        image.units(),
        &image.image_info().map_err(failure)?,
        image.default_mask().map_err(failure)?.as_ref(),
    )
    .map_err(failure)?;
    drop(image);
    std::fs::rename(&result.image, &raw).map_err(failure)?;
    drop(_memory);
    let skymodel = plan.directory.join(format!("{}.skymodel", plan.tp_stem));
    let request = SimanalyzeRequest {
        workspace: Some(plan.directory.clone()),
        resources,
        parameters: json!({"project":plan.directory,"image":false,"imagename":output,"skymodel":skymodel,"analyze":true,"showuv":false,"showpsf":false,"showconvolved":true,"graphics":if plot_sink.is_some(){super::graphics_mode(p)}else{"none"},"overwrite":true})
            .as_object()
            .unwrap()
            .clone(),
    };
    let analyzed = match plot_sink {
        Some(sink) => simanalyze_with_plot_sink_and_override(
            &request,
            sink,
            Some(
                plan.directory
                    .join(format!("{}.analysis.png", plan.project)),
            ),
            cancel,
        )?,
        None => simanalyze_with_controls(&request, None, cancel)?,
    };
    let mut products = vec![raw, result.weight, output];
    products.extend(analyzed.products);
    Ok(products)
}

fn concatenate(
    vis: Vec<PathBuf>,
    weights: Vec<f64>,
    target: &Path,
    resources: SessionResources,
) -> Result<(), SimobserveError> {
    let memory = resources
        .policy()
        .memory(resources.host())
        .saturating_sub(casa_imaging_runtime::reserved_memory());
    let _memory = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "ALMA concatenation",
            memory,
        },
    )
    .map_err(failure)?;
    concat_with_memory(
        &ConcatRequest {
            vis,
            concatvis: target.to_path_buf(),
            freqtol: json!(""),
            dirtol: json!(""),
            respectname: false,
            timesort: false,
            copypointing: true,
            visweightscale: weights,
            forcesingleephemfield: json!(""),
        },
        usize::try_from(memory).map_err(failure)?,
    )
    .map(|_| ())
    .map_err(failure)
}
