// SPDX-License-Identifier: LGPL-3.0-or-later
//! A dirty run's `summaryminor`: `ImagerReturnDict.construct_summary_minor`,
//! which reads the products `tclean(niter=0)` wrote, as it runs no
//! deconvolver to ask.

use std::path::{Path, PathBuf};

use casa_images::PagedImage;
use casa_provider_contracts::CasaValue;

use super::{floats, nested};

/// The summary of the image domains named `names`, the first masked by the
/// image `given_mask` when there is one. A run that wrote no residual holds
/// the empty rows CASA initializes its record with.
pub(super) fn summary_minor(
    names: &[PathBuf],
    given_mask: Option<&Path>,
    fullsummary: bool,
) -> CasaValue {
    let mut planes = Vec::new();
    for (field, name) in names.iter().enumerate() {
        let Some(residual) = open_first(name, &[".residual.tt0", ".residual"]) else {
            return nested([([0, 0, 0], empty_rows())]);
        };
        let model = open_first(name, &[".model.tt0", ".model"]);
        let mask = given_mask
            .filter(|_| field == 0)
            .and_then(|path| PagedImage::<f32>::open(path).ok())
            .or_else(|| open_first(name, &[".mask"]))
            .filter(|mask| mask.shape() == residual.shape());
        let shape = residual.shape();
        let (stokes, channels) = (shape[2], shape[3]);
        for channel in 0..channels {
            for polarization in 0..stokes {
                let read = |image: &PagedImage<f32>| {
                    image
                        .get_slice(&[0, 0, polarization, channel], &[shape[0], shape[1], 1, 1])
                        .ok()
                };
                let Some(data) = read(&residual) else {
                    return nested([([0, 0, 0], empty_rows())]);
                };
                let mask = mask.as_ref().and_then(read);
                let model_flux = model
                    .as_ref()
                    .and_then(read)
                    .map_or(0.0, |plane| plane.iter().map(|v| f64::from(*v)).sum());
                let unmasked = amax(data.iter().copied());
                let peak = mask.as_ref().map_or(unmasked, |mask| {
                    amax(
                        data.iter()
                            .zip(mask.iter())
                            .map(|(value, kept)| value * kept),
                    )
                });
                let mask_sum = mask
                    .as_ref()
                    .map_or(0.0, |mask| mask.iter().map(|v| f64::from(*v)).sum());
                planes.push((
                    [field, channel, polarization],
                    plane_rows(peak, unmasked, model_flux, mask_sum, fullsummary),
                ));
            }
        }
    }
    nested(planes)
}

fn open_first(name: &Path, suffixes: &[&str]) -> Option<PagedImage<f32>> {
    suffixes.iter().find_map(|suffix| {
        let mut path = name.as_os_str().to_owned();
        path.push(suffix);
        Path::new(&path)
            .exists()
            .then(|| PagedImage::<f32>::open(&path).ok())
            .flatten()
    })
}

/// `np.amax`: the largest value, signed.
fn amax(values: impl Iterator<Item = f32>) -> f64 {
    f64::from(values.fold(f32::NEG_INFINITY, f32::max))
}

/// One plane's rows, in the order CASA's dirty record makes them, with
/// integer stop code 3 and, where there is no model, integer zero fluxes
/// (probed with CASA 6.7.6.14, `fullsummary` or not).
fn plane_rows(
    peak: f64,
    unmasked: f64,
    model_flux: f64,
    mask_sum: f64,
    fullsummary: bool,
) -> CasaValue {
    let int = |value: i64| CasaValue::List {
        items: vec![CasaValue::Int { value }],
    };
    let zero = || floats([0.0]);
    let flux = || {
        if model_flux == 0.0 {
            int(0)
        } else {
            floats([model_flux])
        }
    };
    let mut rows = vec![
        ("iterDone", zero()),
        ("peakRes", floats([peak])),
        ("modelFlux", flux()),
        ("cycleThresh", zero()),
    ];
    if fullsummary {
        rows.extend([
            ("cycleStartIters", zero()),
            ("startIterDone", zero()),
            ("startPeakRes", floats([peak])),
            ("startModelFlux", flux()),
            ("startPeakResNM", floats([unmasked])),
            ("peakResNM", floats([unmasked])),
            ("masksum", floats([mask_sum])),
            ("mpiServer", zero()),
            ("stopCode", int(3)),
        ]);
    }
    CasaValue::dict(rows)
}

/// `_initialize_summary_minor`'s rows, in its order.
fn empty_rows() -> CasaValue {
    CasaValue::dict(
        [
            "iterDone",
            "peakRes",
            "modelFlux",
            "cycleThresh",
            "cycleStartIters",
            "startIterDone",
            "startPeakRes",
            "startModelFlux",
            "startPeakResNM",
            "peakResNM",
            "masksum",
            "mpiServer",
            "stopCode",
        ]
        .map(|name| (name, CasaValue::List { items: Vec::new() })),
    )
}
