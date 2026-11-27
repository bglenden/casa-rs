// SPDX-License-Identifier: LGPL-3.0-or-later
//! Polarization modes (`pola`, `poli`, `lpoli`, `tpoli`) for `immath`.

use std::path::{Path, PathBuf};

use casa_coordinates::{CoordinateModel, CoordinateType, StokesType};
use casa_core::ArrayD;
use casa_core::quanta::{
    Quantity,
    casacore::{casacore_unit, get_conformant_value},
};
use ndarray::{ArrayViewD, ArrayViewMutD, Axis, IxDyn, ShapeBuilder, Slice, Zip};

use crate::{ImageMathSummary, ImmathRequest};
use casa_images::{ImageError, ImageInterface, PagedImage};

use super::super::axes::{
    legacy_subimage_coordinates_with_stokes, replace_stokes_in_record,
    subimage_coordinates_with_stokes,
};
use super::selection::{ImmathSelection, RawImage, evaluate_immath_mask, open_raw_image};

struct PolInputs {
    stokes_axis: usize,
    bunit: String,
    q: Option<ArrayD<f32>>,
    u: Option<ArrayD<f32>>,
    v: Option<ArrayD<f32>>,
    mask_q: Option<ArrayD<bool>>,
    mask_u: Option<ArrayD<bool>>,
    mask_v: Option<ArrayD<bool>>,
    // Zero-copy backing block when Q/U[/V] share one multi-Stokes input file.
    shared_block: Option<ArrayD<f32>>,
    shared_mask: Option<ArrayD<bool>>,
    shared_offsets: [Option<usize>; 3],
    has_q: bool,
    has_u: bool,
    has_v: bool,
    mask_override: Option<ArrayD<bool>>,
    ref_coords: casa_coordinates::CoordinateSystem,
    ref_image_info: casa_images::ImageInfo,
    ref_misc_info: casa_core::RecordValue,
    ref_legacy_coords: Option<casa_core::RecordValue>,
    out_shape: Vec<usize>,
    sel: ImmathSelection,
}

impl PolInputs {
    fn plane_view(&self, idx: usize) -> Option<ArrayViewD<'_, f32>> {
        if let (Some(block), Some(rel)) = (&self.shared_block, self.shared_offsets[idx]) {
            return Some(block.slice_axis(Axis(self.stokes_axis), Slice::from(rel..rel + 1)));
        }
        match idx {
            0 => self.q.as_ref().map(|a| a.view()),
            1 => self.u.as_ref().map(|a| a.view()),
            2 => self.v.as_ref().map(|a| a.view()),
            _ => None,
        }
    }

    fn plane_mask_view(&self, idx: usize) -> Option<ArrayViewD<'_, bool>> {
        if let (Some(block), Some(rel)) = (&self.shared_mask, self.shared_offsets[idx]) {
            return Some(block.slice_axis(Axis(self.stokes_axis), Slice::from(rel..rel + 1)));
        }
        match idx {
            0 => self.mask_q.as_ref().map(|a| a.view()),
            1 => self.mask_u.as_ref().map(|a| a.view()),
            2 => self.mask_v.as_ref().map(|a| a.view()),
            _ => None,
        }
    }
}

pub(super) fn run_pola(
    request: &ImmathRequest,
    outfile: &Path,
    filenames: &[PathBuf],
    varnames: &[String],
) -> Result<ImageMathSummary, ImageError> {
    let polithresh = request
        .polithresh
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    // CASA ignores mask when polithresh is set in pola mode.
    let effective_mask = if polithresh.is_some() {
        None
    } else {
        request.mask.as_deref()
    };

    let inputs = gather_pol_inputs(request, filenames, varnames, effective_mask, false)?;
    require_q_u(&inputs)?;

    let q_view = inputs.plane_view(0).expect("Q checked");
    let u_view = inputs.plane_view(1).expect("U checked");
    let mq_view = inputs.plane_mask_view(0);
    let mu_view = inputs.plane_mask_view(1);
    let mo_view = if polithresh.is_none() {
        inputs.mask_override.as_ref().map(|a| a.view())
    } else {
        None
    };
    let thresh = polithresh
        .map(|s| parse_polithresh(s, &inputs.bunit))
        .transpose()?;

    let mut out_data = ArrayD::from_elem(IxDyn(&inputs.out_shape).f(), 0.0_f32);
    let mut valid = ArrayD::from_elem(IxDyn(&inputs.out_shape).f(), true);
    let total_len = out_data.len();

    let false_count = match (split_axis_for(&inputs.out_shape, total_len), thresh) {
        (Some(ax), Some(th)) => parallel_pola_chunks(
            ax,
            out_data.view_mut(),
            valid.view_mut(),
            q_view,
            u_view,
            mq_view,
            mu_view,
            None,
            Some(th),
        ),
        (Some(ax), None) => parallel_pola_chunks(
            ax,
            out_data.view_mut(),
            valid.view_mut(),
            q_view,
            u_view,
            mq_view,
            mu_view,
            mo_view,
            None,
        ),
        (None, _) => eval_pola_chunk(
            out_data.view_mut(),
            valid.view_mut(),
            q_view,
            u_view,
            mq_view,
            mu_view,
            mo_view,
            thresh,
        ),
    };
    let valid_pixels = total_len - false_count;

    save_pol_output(
        request,
        outfile,
        filenames,
        out_data,
        valid,
        valid_pixels,
        &inputs,
        StokesType::Pangle,
        "deg",
    )
}

#[allow(clippy::too_many_arguments)]
fn parallel_pola_chunks(
    ax: Axis,
    mut out_data: ArrayViewMutD<'_, f32>,
    mut valid: ArrayViewMutD<'_, bool>,
    q: ArrayViewD<'_, f32>,
    u: ArrayViewD<'_, f32>,
    mq: Option<ArrayViewD<'_, bool>>,
    mu: Option<ArrayViewD<'_, bool>>,
    mo: Option<ArrayViewD<'_, bool>>,
    thresh: Option<f64>,
) -> usize {
    let chunk = worker_chunk_size(out_data.shape()[ax.index()]);
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        let out_it = out_data.axis_chunks_iter_mut(ax, chunk);
        let valid_it = valid.axis_chunks_iter_mut(ax, chunk);
        let q_it = q.axis_chunks_iter(ax, chunk);
        let u_it = u.axis_chunks_iter(ax, chunk);
        let mut mq_it = mq.as_ref().map(|m| m.axis_chunks_iter(ax, chunk));
        let mut mu_it = mu.as_ref().map(|m| m.axis_chunks_iter(ax, chunk));
        let mut mo_it = mo.as_ref().map(|m| m.axis_chunks_iter(ax, chunk));

        for (((oc, vc), qc), uc) in out_it.zip(valid_it).zip(q_it).zip(u_it) {
            let mqc = mq_it.as_mut().and_then(Iterator::next);
            let muc = mu_it.as_mut().and_then(Iterator::next);
            let moc = mo_it.as_mut().and_then(Iterator::next);
            handles
                .push(scope.spawn(move || eval_pola_chunk(oc, vc, qc, uc, mqc, muc, moc, thresh)));
        }
        handles
            .into_iter()
            .map(|h| h.join().expect("pola worker"))
            .sum()
    })
}

#[allow(clippy::too_many_arguments)]
fn eval_pola_chunk(
    mut out_c: ArrayViewMutD<'_, f32>,
    mut valid_c: ArrayViewMutD<'_, bool>,
    q_c: ArrayViewD<'_, f32>,
    u_c: ArrayViewD<'_, f32>,
    mq_c: Option<ArrayViewD<'_, bool>>,
    mu_c: Option<ArrayViewD<'_, bool>>,
    mo_c: Option<ArrayViewD<'_, bool>>,
    thresh: Option<f64>,
) -> usize {
    let half_deg_per_rad = 90.0_f64 / std::f64::consts::PI;
    let has_mask = apply_chunk_masks(&mut valid_c, [mq_c, mu_c, mo_c]);

    let mut false_count = 0usize;
    if let Some(th) = thresh {
        let thresh_sq = th * th;
        let use_sq = th > 1.0e-150 && th < 1.0e150 && thresh_sq.is_finite();
        Zip::from(&mut out_c)
            .and(&mut valid_c)
            .and(&q_c)
            .and(&u_c)
            .for_each(|dst, vf, &qv, &uv| {
                let qf = f64::from(qv);
                let uf = f64::from(uv);
                *dst = (half_deg_per_rad * uf.atan2(qf)) as f32;
                let below = if qf.abs() >= th || uf.abs() >= th {
                    false
                } else if use_sq && qf.abs() < 1.0e100 && uf.abs() < 1.0e100 {
                    qf * qf + uf * uf < thresh_sq
                } else {
                    qf.hypot(uf) < th
                };
                if below {
                    *vf = false;
                }
                if !*vf {
                    false_count += 1;
                }
            });
    } else if has_mask {
        Zip::from(&mut out_c)
            .and(&valid_c)
            .and(&q_c)
            .and(&u_c)
            .for_each(|dst, &vf, &qv, &uv| {
                *dst = (half_deg_per_rad * f64::from(uv).atan2(f64::from(qv))) as f32;
                if !vf {
                    false_count += 1;
                }
            });
    } else {
        Zip::from(&mut out_c)
            .and(&q_c)
            .and(&u_c)
            .for_each(|dst, &qv, &uv| {
                *dst = (half_deg_per_rad * f64::from(uv).atan2(f64::from(qv))) as f32;
            });
    }
    false_count
}

pub(super) fn run_poli(
    request: &ImmathRequest,
    outfile: &Path,
    filenames: &[PathBuf],
    varnames: &[String],
    mode: &str,
) -> Result<ImageMathSummary, ImageError> {
    let need_v = mode != "lpoli";
    let inputs = gather_pol_inputs(
        request,
        filenames,
        varnames,
        request.mask.as_deref(),
        need_v,
    )?;
    let out_stokes = match mode {
        "tpoli" => {
            if !inputs.has_q {
                return Err(ImageError::InvalidMetadata(
                    "Stokes Q is required for mode tpoli but was not found".into(),
                ));
            }
            if !inputs.has_u {
                return Err(ImageError::InvalidMetadata(
                    "Stokes U is required for mode tpoli but was not found".into(),
                ));
            }
            if !inputs.has_v {
                return Err(ImageError::InvalidMetadata(
                    "Stokes V is required for mode tpoli but was not found".into(),
                ));
            }
            StokesType::Ptotal
        }
        "lpoli" => {
            require_q_u(&inputs)?;
            StokesType::Plinear
        }
        "poli" => {
            if inputs.has_v {
                // CASA imagepol.totpolint computes Ptotal whenever V exists (even V-only).
                if inputs.has_q ^ inputs.has_u {
                    return Err(ImageError::InvalidMetadata(
                        "This Stokes coordinate has only one of Q and U. This is not useful".into(),
                    ));
                }
                StokesType::Ptotal
            } else {
                require_q_u(&inputs)?;
                StokesType::Plinear
            }
        }
        _ => unreachable!(),
    };

    let sigma_sq = parse_sigma_sq(&request.sigma, &inputs.bunit)?;
    let mut out_data = ArrayD::from_elem(IxDyn(&inputs.out_shape).f(), 0.0_f32);
    let mut valid = ArrayD::from_elem(IxDyn(&inputs.out_shape).f(), true);
    let total_len = out_data.len();

    let q_view = inputs.plane_view(0);
    let u_view = inputs.plane_view(1);
    let v_view = if out_stokes == StokesType::Ptotal {
        inputs.plane_view(2)
    } else {
        None
    };
    let mq_view = inputs.plane_mask_view(0);
    let mu_view = inputs.plane_mask_view(1);
    let mv_view = if out_stokes == StokesType::Ptotal {
        inputs.plane_mask_view(2)
    } else {
        None
    };
    let mo_view = inputs.mask_override.as_ref().map(|a| a.view());

    let false_count = match split_axis_for(&inputs.out_shape, total_len) {
        Some(ax) => parallel_poli_chunks(
            ax,
            out_data.view_mut(),
            valid.view_mut(),
            q_view,
            u_view,
            v_view,
            mq_view,
            mu_view,
            mv_view,
            mo_view,
            sigma_sq,
        ),
        None => eval_poli_chunk(
            out_data.view_mut(),
            valid.view_mut(),
            q_view,
            u_view,
            v_view,
            mq_view,
            mu_view,
            mv_view,
            mo_view,
            sigma_sq,
        ),
    };
    let valid_pixels = total_len - false_count;

    let bunit = inputs.bunit.clone();
    save_pol_output(
        request,
        outfile,
        filenames,
        out_data,
        valid,
        valid_pixels,
        &inputs,
        out_stokes,
        &bunit,
    )
}

#[allow(clippy::too_many_arguments)]
fn parallel_poli_chunks(
    ax: Axis,
    mut out_data: ArrayViewMutD<'_, f32>,
    mut valid: ArrayViewMutD<'_, bool>,
    q: Option<ArrayViewD<'_, f32>>,
    u: Option<ArrayViewD<'_, f32>>,
    v: Option<ArrayViewD<'_, f32>>,
    mq: Option<ArrayViewD<'_, bool>>,
    mu: Option<ArrayViewD<'_, bool>>,
    mv: Option<ArrayViewD<'_, bool>>,
    mo: Option<ArrayViewD<'_, bool>>,
    sigma_sq: Option<f64>,
) -> usize {
    let chunk = worker_chunk_size(out_data.shape()[ax.index()]);
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        let out_it = out_data.axis_chunks_iter_mut(ax, chunk);
        let valid_it = valid.axis_chunks_iter_mut(ax, chunk);
        let mut q_it = q.as_ref().map(|a| a.axis_chunks_iter(ax, chunk));
        let mut u_it = u.as_ref().map(|a| a.axis_chunks_iter(ax, chunk));
        let mut v_it = v.as_ref().map(|a| a.axis_chunks_iter(ax, chunk));
        let mut mq_it = mq.as_ref().map(|m| m.axis_chunks_iter(ax, chunk));
        let mut mu_it = mu.as_ref().map(|m| m.axis_chunks_iter(ax, chunk));
        let mut mv_it = mv.as_ref().map(|m| m.axis_chunks_iter(ax, chunk));
        let mut mo_it = mo.as_ref().map(|m| m.axis_chunks_iter(ax, chunk));

        for (oc, vc) in out_it.zip(valid_it) {
            let qc = q_it.as_mut().and_then(Iterator::next);
            let uc = u_it.as_mut().and_then(Iterator::next);
            let v_chunk = v_it.as_mut().and_then(Iterator::next);
            let mqc = mq_it.as_mut().and_then(Iterator::next);
            let muc = mu_it.as_mut().and_then(Iterator::next);
            let mvc = mv_it.as_mut().and_then(Iterator::next);
            let moc = mo_it.as_mut().and_then(Iterator::next);
            handles.push(scope.spawn(move || {
                eval_poli_chunk(oc, vc, qc, uc, v_chunk, mqc, muc, mvc, moc, sigma_sq)
            }));
        }
        handles
            .into_iter()
            .map(|h| h.join().expect("poli worker"))
            .sum()
    })
}

#[allow(clippy::too_many_arguments)]
fn eval_poli_chunk(
    mut out_c: ArrayViewMutD<'_, f32>,
    mut valid_c: ArrayViewMutD<'_, bool>,
    q_c: Option<ArrayViewD<'_, f32>>,
    u_c: Option<ArrayViewD<'_, f32>>,
    v_c: Option<ArrayViewD<'_, f32>>,
    mq_c: Option<ArrayViewD<'_, bool>>,
    mu_c: Option<ArrayViewD<'_, bool>>,
    mv_c: Option<ArrayViewD<'_, bool>>,
    mo_c: Option<ArrayViewD<'_, bool>>,
    sigma_sq: Option<f64>,
) -> usize {
    let has_mask = apply_chunk_masks(&mut valid_c, [mq_c, mu_c, mv_c, mo_c]) || sigma_sq.is_some();

    let mut false_count = 0usize;
    match (q_c, u_c, v_c) {
        (Some(qa), Some(ua), Some(va)) => {
            if has_mask {
                Zip::from(&mut out_c)
                    .and(&mut valid_c)
                    .and(&qa)
                    .and(&ua)
                    .and(&va)
                    .for_each(|dst, vf, &qv, &uv, &vv| {
                        let qf = f64::from(qv);
                        let uf = f64::from(uv);
                        let vf_val = f64::from(vv);
                        assign_debiased(dst, vf, qf * qf + uf * uf + vf_val * vf_val, sigma_sq);
                        if !*vf {
                            false_count += 1;
                        }
                    });
            } else {
                Zip::from(&mut out_c)
                    .and(&qa)
                    .and(&ua)
                    .and(&va)
                    .for_each(|dst, &qv, &uv, &vv| {
                        let qf = f64::from(qv);
                        let uf = f64::from(uv);
                        let vf_val = f64::from(vv);
                        *dst = (qf * qf + uf * uf + vf_val * vf_val).sqrt() as f32;
                    });
            }
        }
        (None, None, Some(va)) => {
            if has_mask {
                Zip::from(&mut out_c)
                    .and(&mut valid_c)
                    .and(&va)
                    .for_each(|dst, vf, &vv| {
                        let vf_val = f64::from(vv);
                        assign_debiased(dst, vf, vf_val * vf_val, sigma_sq);
                        if !*vf {
                            false_count += 1;
                        }
                    });
            } else {
                Zip::from(&mut out_c).and(&va).for_each(|dst, &vv| {
                    *dst = f64::from(vv).abs() as f32;
                });
            }
        }
        (Some(qa), Some(ua), None) => {
            if has_mask {
                Zip::from(&mut out_c)
                    .and(&mut valid_c)
                    .and(&qa)
                    .and(&ua)
                    .for_each(|dst, vf, &qv, &uv| {
                        let qf = f64::from(qv);
                        let uf = f64::from(uv);
                        assign_debiased(dst, vf, qf * qf + uf * uf, sigma_sq);
                        if !*vf {
                            false_count += 1;
                        }
                    });
            } else {
                Zip::from(&mut out_c)
                    .and(&qa)
                    .and(&ua)
                    .for_each(|dst, &qv, &uv| {
                        let qf = f64::from(qv);
                        let uf = f64::from(uv);
                        *dst = (qf * qf + uf * uf).sqrt() as f32;
                    });
            }
        }
        _ => unreachable!(),
    }
    false_count
}

fn split_axis_for(shape: &[usize], total_len: usize) -> Option<Axis> {
    if total_len < 65_536 {
        return None;
    }
    shape
        .iter()
        .rposition(|&d| d > 1)
        .filter(|&ax| shape[ax] >= 2)
        .map(Axis)
}

fn worker_chunk_size(axis_len: usize) -> usize {
    let workers = std::thread::available_parallelism()
        .map_or(1, usize::from)
        .clamp(1, 8)
        .min(axis_len);
    axis_len.div_ceil(workers)
}

fn apply_chunk_masks<const N: usize>(
    valid_c: &mut ArrayViewMutD<'_, bool>,
    masks: [Option<ArrayViewD<'_, bool>>; N],
) -> bool {
    let mut any = false;
    for mask in masks.into_iter().flatten() {
        any = true;
        Zip::from(&mut *valid_c)
            .and(&mask)
            .for_each(|a, &b| *a = *a && b);
    }
    any
}

#[inline]
fn assign_debiased(dst: &mut f32, valid: &mut bool, p_sq: f64, sigma_sq: Option<f64>) {
    if let Some(s_sq) = sigma_sq {
        let diff = p_sq - s_sq;
        if diff >= 0.0 {
            *dst = diff.sqrt() as f32;
        } else {
            *dst = 0.0;
            *valid = false;
        }
    } else {
        *dst = p_sq.sqrt() as f32;
    }
}

fn require_q_u(inputs: &PolInputs) -> Result<(), ImageError> {
    if inputs.has_q && inputs.has_u {
        return Ok(());
    }
    if inputs.has_q ^ inputs.has_u {
        return Err(ImageError::InvalidMetadata(
            "This Stokes coordinate has only one of Q and U. This is not useful".into(),
        ));
    }
    Err(ImageError::InvalidMetadata(
        "This image has no Stokes Q, U, nor V.  This is not useful".into(),
    ))
}

fn parse_polithresh(raw: &str, bunit: &str) -> Result<f64, ImageError> {
    let q = Quantity::read(raw)
        .map_err(|_| ImageError::InvalidMetadata(format!("Invalid polithresh quantity: {raw}")))?;
    let init_unit = q.unit().name().to_string();
    if init_unit.is_empty() {
        return Ok(q.value());
    }
    if bunit.is_empty() {
        return Err(ImageError::InvalidMetadata(format!(
            "Units of polithresh {init_unit} do not conform to input image units of {bunit} so cannot perform thresholding. Please correct units and try again."
        )));
    }
    let target = casacore_unit(bunit).map_err(|_| {
        ImageError::InvalidMetadata(format!(
            "Units of polithresh {init_unit} do not conform to input image units of {bunit} so cannot perform thresholding. Please correct units and try again."
        ))
    })?;
    get_conformant_value(&q, &target).map_err(|_| {
        ImageError::InvalidMetadata(format!(
            "Units of polithresh {init_unit} do not conform to input image units of {bunit} so cannot perform thresholding. Please correct units and try again."
        ))
    })
}

fn parse_sigma_sq(raw: &str, bunit: &str) -> Result<Option<f64>, ImageError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let q = Quantity::read(trimmed)
        .map_err(|_| ImageError::InvalidMetadata(format!("Invalid sigma quantity: {trimmed}")))?;
    if q.value() <= 0.0 {
        return Ok(None);
    }
    let init_unit = q.unit().name().to_string();
    let sigma_val = if init_unit.is_empty() {
        if bunit.is_empty() {
            q.value()
        } else {
            return Err(ImageError::InvalidMetadata(format!(
                "Units of sigma {init_unit:?} do not conform to input image units of {bunit}"
            )));
        }
    } else if bunit.is_empty() {
        return Err(ImageError::InvalidMetadata(format!(
            "Units of sigma {init_unit} do not conform to input image units of {bunit}"
        )));
    } else {
        let target = casacore_unit(bunit).map_err(|_| {
            ImageError::InvalidMetadata(format!(
                "Units of sigma {init_unit} do not conform to input image units of {bunit}"
            ))
        })?;
        get_conformant_value(&q, &target).map_err(|_| {
            ImageError::InvalidMetadata(format!(
                "Units of sigma {init_unit} do not conform to input image units of {bunit}"
            ))
        })?
    };
    Ok(Some(sigma_val * sigma_val))
}

fn gather_pol_inputs(
    request: &ImmathRequest,
    filenames: &[PathBuf],
    varnames: &[String],
    mask_expr: Option<&str>,
    need_v: bool,
) -> Result<PolInputs, ImageError> {
    let mut raws: Vec<RawImage<f32>> = Vec::with_capacity(filenames.len());
    for p in filenames {
        raws.push(open_raw_image::<f32>(p)?);
    }
    let first = raws[0].as_interface();
    let bunit = first.units().to_string();
    let stokes_axis = first
        .find_axis(CoordinateType::Stokes)
        .ok_or_else(|| ImageError::InvalidMetadata("This image has no Stokes coordinate".into()))?;

    let mut q_loc: Option<(usize, usize)> = None;
    let mut u_loc: Option<(usize, usize)> = None;
    let mut v_loc: Option<(usize, usize)> = None;

    for (file_idx, raw) in raws.iter().enumerate() {
        let iface = raw.as_interface();
        let axis = iface.find_axis(CoordinateType::Stokes).ok_or_else(|| {
            ImageError::InvalidMetadata("All input images must have a Stokes axis".into())
        })?;
        if axis != stokes_axis || !non_stokes_match(first.shape(), iface.shape(), stokes_axis) {
            return Err(ImageError::ShapeMismatch {
                expected: first.shape().to_vec(),
                got: iface.shape().to_vec(),
            });
        }
        let stokes_list = stokes_types_on_axis(iface.coordinates(), axis)?;
        for (plane_idx, st) in stokes_list.into_iter().enumerate() {
            match st {
                StokesType::Q if q_loc.is_none() => q_loc = Some((file_idx, plane_idx)),
                StokesType::U if u_loc.is_none() => u_loc = Some((file_idx, plane_idx)),
                StokesType::V if v_loc.is_none() => v_loc = Some((file_idx, plane_idx)),
                _ => {}
            }
        }
    }

    // Polarization modes ignore stokes= parameter.
    let sel = ImmathSelection::from_raw(&raws[0], request, true)?;
    let mut out_shape = sel.shape.clone();
    out_shape[stokes_axis] = 1;

    let mut aliases: Vec<(String, &dyn ImageInterface<f32>)> = Vec::new();
    for (idx, raw) in raws.iter().enumerate() {
        if let Some(vn) = varnames.get(idx) {
            aliases.push((vn.clone(), raw.as_interface()));
        } else {
            aliases.push((format!("IM{idx}"), raw.as_interface()));
        }
        aliases.push((filenames[idx].display().to_string(), raw.as_interface()));
    }
    let mask_override = if let Some(expr) = mask_expr.filter(|s| !s.trim().is_empty()) {
        let mut w_start = sel.start.clone();
        let mut w_shape = sel.shape.clone();
        w_start[stokes_axis] = 0;
        w_shape[stokes_axis] = 1;
        evaluate_immath_mask(
            Some(expr),
            first.shape(),
            &aliases,
            request.stretch,
            Some((&w_start, &w_shape)),
        )?
    } else {
        None
    };

    let v_needed_loc = if need_v { v_loc } else { None };
    let needed = [q_loc, u_loc, v_needed_loc];
    let shared_file = shared_file_index(&needed);

    let (q, u, v, mask_q, mask_u, mask_v, shared_block, shared_mask, shared_offsets) =
        if let Some(fidx) = shared_file {
            let min_p = needed
                .iter()
                .flatten()
                .map(|&(_, p)| p)
                .min()
                .expect("min_p");
            let max_p = needed
                .iter()
                .flatten()
                .map(|&(_, p)| p)
                .max()
                .expect("max_p");
            let iface = raws[fidx].as_interface();
            let mut span_sel = sel.clone();
            span_sel.start[stokes_axis] = min_p;
            span_sel.shape[stokes_axis] = max_p - min_p + 1;
            span_sel.stokes_axis = None;
            span_sel.stokes_indices = None;
            let stride = vec![1; iface.ndim()];
            let block = iface.get_slice(&span_sel.start, &span_sel.shape, &stride)?;
            let block_mask = super::selection::selection_and_mask(iface, &span_sel, None)?;
            let offsets = [
                q_loc.map(|(_, p)| p - min_p),
                u_loc.map(|(_, p)| p - min_p),
                v_needed_loc.map(|(_, p)| p - min_p),
            ];
            (
                None,
                None,
                None,
                None,
                None,
                None,
                Some(block),
                block_mask,
                offsets,
            )
        } else {
            let (q, mask_q) = extract_stokes_plane(&raws, q_loc, stokes_axis, &sel)?;
            let (u, mask_u) = extract_stokes_plane(&raws, u_loc, stokes_axis, &sel)?;
            let (v, mask_v) = extract_stokes_plane(&raws, v_needed_loc, stokes_axis, &sel)?;
            (q, u, v, mask_q, mask_u, mask_v, None, None, [None; 3])
        };

    let ref_coords = first.coordinates().clone();
    let ref_image_info = first.image_info()?;
    let ref_misc_info = first.misc_info();
    let mut sub_start = sel.start.clone();
    sub_start[stokes_axis] = 0;
    let mut sub_shape = sel.shape.clone();
    sub_shape[stokes_axis] = 1;
    let ref_legacy_coords = match &raws[0] {
        RawImage::Paged { image, .. } => {
            legacy_subimage_coordinates_with_stokes(image, &sub_start, &sub_shape, None)
        }
        RawImage::Memory(_) => None,
    };

    Ok(PolInputs {
        stokes_axis,
        bunit,
        q,
        u,
        v,
        mask_q,
        mask_u,
        mask_v,
        shared_block,
        shared_mask,
        shared_offsets,
        has_q: q_loc.is_some(),
        has_u: u_loc.is_some(),
        has_v: v_loc.is_some(),
        mask_override,
        ref_coords,
        ref_image_info,
        ref_misc_info,
        ref_legacy_coords,
        out_shape,
        sel,
    })
}

fn shared_file_index(locs: &[Option<(usize, usize)>; 3]) -> Option<usize> {
    let mut count = 0usize;
    let mut file = None;
    for loc in locs.iter().flatten() {
        count += 1;
        match file {
            None => file = Some(loc.0),
            Some(prev) if prev != loc.0 => return None,
            Some(_) => {}
        }
    }
    if count >= 2 { file } else { None }
}

fn non_stokes_match(a: &[usize], b: &[usize], stokes_axis: usize) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .enumerate()
            .all(|(ax, (&da, &db))| ax == stokes_axis || da == db)
}

fn stokes_types_on_axis(
    coords: &casa_coordinates::CoordinateSystem,
    axis: usize,
) -> Result<Vec<StokesType>, ImageError> {
    if let Some((index, _)) = coords.pixel_axis_owner(axis)
        && let CoordinateModel::Stokes(stokes) = coords.coordinate(index)
    {
        return Ok(stokes.stokes().to_vec());
    }
    Err(ImageError::InvalidMetadata(
        "missing Stokes coordinate on Stokes axis".into(),
    ))
}

type PolInputSlice = (Option<ArrayD<f32>>, Option<ArrayD<bool>>);

fn extract_stokes_plane(
    raws: &[RawImage<f32>],
    loc: Option<(usize, usize)>,
    stokes_axis: usize,
    sel: &ImmathSelection,
) -> Result<PolInputSlice, ImageError> {
    let Some((file_idx, plane_idx)) = loc else {
        return Ok((None, None));
    };
    let iface = raws[file_idx].as_interface();
    let mut plane_sel = sel.clone();
    plane_sel.start[stokes_axis] = plane_idx;
    plane_sel.shape[stokes_axis] = 1;
    plane_sel.stokes_axis = None;
    plane_sel.stokes_indices = None;
    let stride = vec![1; iface.ndim()];
    let data = iface.get_slice(&plane_sel.start, &plane_sel.shape, &stride)?;
    let mask = super::selection::selection_and_mask(iface, &plane_sel, None)?;
    Ok((Some(data), mask))
}

#[allow(clippy::too_many_arguments)]
fn save_pol_output(
    request: &ImmathRequest,
    outfile: &Path,
    filenames: &[PathBuf],
    data: ArrayD<f32>,
    valid: ArrayD<bool>,
    valid_pixels: usize,
    inputs: &PolInputs,
    out_stokes: StokesType,
    bunit: &str,
) -> Result<ImageMathSummary, ImageError> {
    let mut sub_start = inputs.sel.start.clone();
    sub_start[inputs.stokes_axis] = 0;
    let mut sub_shape = inputs.sel.shape.clone();
    sub_shape[inputs.stokes_axis] = 1;

    let out_coords = subimage_coordinates_with_stokes(
        &inputs.ref_coords,
        &sub_start,
        &sub_shape,
        Some(&[out_stokes]),
    )?;
    let has_false = valid_pixels != valid.len();

    super::remove_existing_outfile_if_overwrite(outfile, request.overwrite)?;

    let mut image = PagedImage::<f32>::create(inputs.out_shape.clone(), out_coords, outfile)?;
    image.set_units(bunit.to_string())?;
    image.set_image_info(&inputs.ref_image_info)?;
    image.set_misc_info(inputs.ref_misc_info.clone())?;
    if let Some(mut lc) = inputs.ref_legacy_coords.clone() {
        let _ = replace_stokes_in_record(&mut lc, &[out_stokes]);
        image.set_coords_record_keyword(lc);
    }
    image.put_slice(&data, &vec![0; inputs.out_shape.len()])?;
    if has_false {
        image.put_mask("mask0", &valid)?;
        image.set_default_mask("mask0")?;
    }
    image.save()?;

    Ok(ImageMathSummary {
        imagename: filenames.iter().map(|p| p.display().to_string()).collect(),
        outfile: outfile.display().to_string(),
        expr: request.expr.clone(),
        shape: inputs.out_shape.clone(),
        units: bunit.to_string(),
        valid_pixels,
        beam_warnings: Vec::new(),
    })
}
