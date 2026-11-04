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
        let residual_path = first_existing(name, &[".residual.tt0", ".residual"]);
        let Some(residual_path) = residual_path else {
            return nested([([0, 0, 0], empty_rows())]);
        };
        let Ok(residual) = PagedImage::<f32>::open(&residual_path) else {
            planes.push(([field, 0, 0], unreadable(fullsummary)));
            continue;
        };
        let model = first_existing(name, &[".model.tt0", ".model"]);
        let mask = given_mask
            .filter(|_| field == 0)
            .map(Path::to_path_buf)
            .or_else(|| first_existing(name, &[".mask"]));
        let model = model.map(|path| PagedImage::<f32>::open(path).ok());
        let mask = mask.map(|path| {
            PagedImage::<f32>::open(path)
                .ok()
                .filter(|mask| mask.shape() == residual.shape())
        });
        let shape = residual.shape();
        for channel in 0..shape[3] {
            for polarization in 0..shape[2] {
                let read = |image: &PagedImage<f32>| {
                    image
                        .get_slice(&[0, 0, polarization, channel], &[shape[0], shape[1], 1, 1])
                        .ok()
                };
                // An image that exists and can't be read gives nan: CASA's
                // task raises on it, and casa-rs's products are published.
                let rows = plane_summary(
                    read(&residual).as_ref(),
                    mask.as_ref().map(|mask| mask.as_ref().and_then(read)),
                    model.as_ref().map(|model| model.as_ref().and_then(read)),
                );
                let rows = rows.map_or_else(
                    || unreadable(fullsummary),
                    |[peak, unmasked, flux, sum]| {
                        plane_rows(peak, unmasked, flux, sum, fullsummary)
                    },
                );
                planes.push(([field, channel, polarization], rows));
            }
        }
    }
    nested(planes)
}

type Plane = ndarray::ArrayD<f32>;

/// One plane's `[peak, unmasked peak, model flux, mask sum]`: `None` when an
/// image that exists could not be read.
fn plane_summary(
    data: Option<&Plane>,
    mask: Option<Option<Plane>>,
    model: Option<Option<Plane>>,
) -> Option<[f64; 4]> {
    let data = data?;
    let sum = |plane: &Plane| plane.iter().map(|value| f64::from(*value)).sum::<f64>();
    let unmasked = amax(data.iter().copied());
    let (peak, mask_sum) = match mask {
        None => (unmasked, 0.0),
        Some(mask) => {
            let mask = mask?;
            let kept = data
                .iter()
                .zip(mask.iter())
                .map(|(value, kept)| value * kept);
            (amax(kept), sum(&mask))
        }
    };
    let flux = match model {
        None => 0.0,
        Some(model) => sum(&model?),
    };
    Some([peak, unmasked, flux, mask_sum])
}

fn first_existing(name: &Path, suffixes: &[&str]) -> Option<PathBuf> {
    suffixes.iter().find_map(|suffix| {
        let mut path = name.as_os_str().to_owned();
        path.push(suffix);
        let path = PathBuf::from(path);
        path.exists().then_some(path)
    })
}

/// One plane's rows when its images can't be read.
fn unreadable(fullsummary: bool) -> CasaValue {
    plane_rows(f64::NAN, f64::NAN, f64::NAN, f64::NAN, fullsummary)
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

#[cfg(test)]
mod tests {
    use super::*;
    use casa_provider_contracts::{CasaEntry, CasaKey};

    fn names(rows: &CasaValue) -> Vec<&str> {
        let CasaValue::Dict { entries } = rows else {
            panic!("a dictionary");
        };
        entries
            .iter()
            .map(|CasaEntry { key, .. }| match key {
                CasaKey::String(name) => name.as_str(),
                CasaKey::Int(_) => panic!("a row name"),
            })
            .collect()
    }

    fn row<'a>(rows: &'a CasaValue, name: &str) -> &'a CasaValue {
        let CasaValue::Dict { entries } = rows else {
            panic!("a dictionary");
        };
        &entries
            .iter()
            .find(|entry| entry.key == CasaKey::String(name.into()))
            .expect("the row")
            .value
    }

    const FULL: [&str; 13] = [
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
    ];

    /// Astra's #1015 F9: a dirty run's full summary holds CASA's residual
    /// rows, in their order and types (probed with CASA 6.7.6.14): integer
    /// stop code 3 and, with no model, integer zero fluxes.
    #[test]
    fn a_dirty_full_summary_holds_casas_residual_rows() {
        let rows = plane_rows(2.0, 3.0, 0.0, 4.0, true);
        assert_eq!(names(&rows), FULL);
        let one_int = |value| CasaValue::List {
            items: vec![CasaValue::Int { value }],
        };
        assert_eq!(row(&rows, "modelFlux"), &one_int(0));
        assert_eq!(row(&rows, "startModelFlux"), &one_int(0));
        assert_eq!(row(&rows, "stopCode"), &one_int(3));
        assert_eq!(row(&rows, "peakRes"), &floats([2.0]));
        assert_eq!(row(&rows, "startPeakResNM"), &floats([3.0]));
        assert_eq!(row(&rows, "masksum"), &floats([4.0]));
        assert_eq!(names(&plane_rows(2.0, 3.0, 0.0, 4.0, false))[..], FULL[..4]);
        assert_eq!(
            row(&plane_rows(1.0, 1.0, 5.0, 0.0, false), "modelFlux"),
            &floats([5.0])
        );
    }

    /// A run that wrote no residual holds the rows CASA initializes, empty.
    #[test]
    fn a_run_with_no_residual_holds_empty_rows() {
        let directory = tempfile::tempdir().expect("scratch");
        let summary = summary_minor(&[directory.path().join("absent")], None, true);
        let mut plane = &summary;
        for _ in 0..3 {
            let CasaValue::Dict { entries } = plane else {
                panic!("a dictionary");
            };
            plane = &entries[0].value;
        }
        assert_eq!(names(plane), FULL);
        assert_eq!(
            row(plane, "peakRes"),
            &CasaValue::List { items: Vec::new() }
        );
    }

    /// An image that exists and can't be read gives nan, not a value that
    /// reads as measured.
    #[test]
    fn an_unreadable_image_gives_no_value() {
        let data = Plane::from_elem(ndarray::IxDyn(&[2, 2]), 1.0_f32);
        assert!(plane_summary(None, None, None).is_none());
        assert!(plane_summary(Some(&data), Some(None), None).is_none());
        assert!(plane_summary(Some(&data), None, Some(None)).is_none());
        assert_eq!(
            plane_summary(Some(&data), None, None),
            Some([1.0, 1.0, 0.0, 0.0])
        );
        let mask = Plane::zeros(ndarray::IxDyn(&[2, 2]));
        assert_eq!(
            plane_summary(Some(&data), Some(Some(mask)), None),
            Some([0.0, 1.0, 0.0, 0.0])
        );
    }
}
