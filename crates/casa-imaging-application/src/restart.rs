// SPDX-License-Identifier: LGPL-3.0-or-later

//! tclean's continuation on an earlier run's products (#944, #980).
//!
//! CASA's `SIImageStore` opens every product that exists at `imagename`
//! and uses its pixels: the model is the start of the solve, a CLEAN mask is
//! reused when no new mask is given (`SynthesisDeconvolver::setupMask`), and
//! the PSF, sums of weights and residual stand unless `calcpsf` or `calcres`
//! forms them again (`task_tclean.py`). casa-rs reads them on the same grid
//! ([`check_grid`]) for a run of the main image of every spectral mode,
//! Taylor family and Stokes set it images. What it does not continue is
//! refused here, before a run summary or any product is written, and the
//! earlier products are preserved.

mod existing;
pub(crate) use existing::ExistingProducts;
mod grid;
pub(crate) use grid::check_grid;
mod mask;
pub(crate) use mask::MaskInput;
mod numbering;
pub(crate) use numbering::next_prefix;

use std::{
    fmt, fs, io,
    path::{Path, PathBuf},
};

use crate::{Deconvolver, Gridder, ImagingRequest, PrepareError, UseMask};

/// The images of a run: one image per product, or a Taylor family of
/// `terms` coefficients (`mtmfs`), whose PSF, sums of weights and weight
/// images have `2·terms − 1` moments and whose primary beam is term 0's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Family {
    /// `.model`, `.psf`, ...
    Single,
    /// `.model.tt0`, `.psf.tt0`, ...
    Taylor {
        /// The Taylor coefficients.
        terms: usize,
    },
}

impl fmt::Display for Family {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Single => formatter.write_str("single-term"),
            Self::Taylor { terms } => write!(formatter, "{terms}-term Taylor"),
        }
    }
}

impl Family {
    pub(crate) fn of(request: &ImagingRequest) -> Self {
        match request.deconvolver {
            Deconvolver::Mtmfs => Self::Taylor {
                terms: request.nterms,
            },
            _ => Self::Single,
        }
    }

    /// The suffixes of `base`'s images (`model`, `psf`, ...), in term order.
    pub(crate) fn names(self, base: &str) -> Vec<String> {
        match self {
            Self::Single => vec![base.to_string()],
            Self::Taylor { terms } => {
                let count = match base {
                    "psf" | "sumwt" | "weight" => 2 * terms - 1,
                    "pb" => 1,
                    _ => terms,
                };
                (0..count).map(|term| format!("{base}.tt{term}")).collect()
            }
        }
    }

    /// Whether `suffix` names a product of a run of this family: the
    /// images, their primary-beam corrections, the CLEAN mask and a Taylor
    /// family's spectral index.
    fn admits(self, suffix: &str) -> bool {
        const BASES: [&str; 7] = ["model", "image", "residual", "psf", "sumwt", "weight", "pb"];
        if suffix == "mask" {
            return true;
        }
        let image = suffix.strip_suffix(".pbcor").unwrap_or(suffix);
        let pbcor = image.len() != suffix.len();
        match self {
            Self::Single => {
                BASES.contains(&image) && (!pbcor || image == "image")
                    || matches!(suffix, "image.pbcor")
            }
            Self::Taylor { .. } => {
                matches!(
                    suffix,
                    "alpha" | "alpha.error" | "alpha.pbcor" | "beta" | "beta.error"
                ) || BASES
                    .iter()
                    .any(|base| self.names(base).iter().any(|name| name == image))
                    && (!pbcor || image.starts_with("image."))
            }
        }
    }
}

/// Check that `request` can continue on what exists at its `imagename`.
/// Task entry points call this before creating a run summary.
pub(crate) fn check(request: &ImagingRequest) -> Result<(), PrepareError> {
    let existing = products(&request.imagename)?;
    if let (Some(path), Some(_)) = (existing.first(), &request.outlierfile) {
        return Err(PrepareError::ExistingImageProduct { path: path.clone() });
    }
    let family = Family::of(request);
    for path in &existing {
        let suffix = suffix(&request.imagename, path);
        let refuse = |reason: String| PrepareError::Restart {
            path: path.clone(),
            reason,
        };
        if !family.admits(&suffix) {
            return Err(refuse(format!(
                "it is not a product of this run's {family} images; choose a new imagename"
            )));
        }
        if suffix.starts_with("model") && request.startmodel.is_some() {
            return Err(refuse(
                "startmodel cannot be supplied when the output model already exists".into(),
            ));
        }
        if suffix == "mask" && request.niter > 0 {
            if request.usemask == UseMask::AutoMultithresh {
                return Err(refuse(
                    "continuing auto-multithresh on an existing CLEAN mask is not implemented yet"
                        .into(),
                ));
            }
            if request.mask_image.is_some()
                || !request.mask_box.is_empty()
                || request.explicit_empty_mask
            {
                return Err(refuse(
                    "merging a new mask into an existing CLEAN mask is not implemented yet".into(),
                ));
            }
        }
    }
    check_inputs(request, !existing.is_empty())?;
    if let Some(path) = &request.outlierfile {
        super::compile::check_outlier_outputs(path)?;
    }
    Ok(())
}

/// Check that what `calcpsf=False` and `calcres=False` read exists and that
/// casa-rs reads it for the request's gridder; `continuing` is whether the
/// run continues on existing products at all.
pub(crate) fn check_inputs(request: &ImagingRequest, continuing: bool) -> Result<(), PrepareError> {
    if request.calcpsf && request.calcres {
        return Ok(());
    }
    let refuse = |path: PathBuf, reason: String| Err(PrepareError::Restart { path, reason });
    let flags = match (request.calcpsf, request.calcres) {
        (false, false) => "calcpsf=False and calcres=False",
        (false, true) => "calcpsf=False",
        _ => "calcres=False",
    };
    let unsupported = match &request.gridder {
        _ if request.outlierfile.is_some() => Some("with outlier fields"),
        Gridder::Standard { facets } if facets.get() > 1 => Some("with facets"),
        Gridder::Standard { .. } | Gridder::Wproject { .. } => None,
        Gridder::Mosaic { .. } => Some("with the mosaic gridder"),
        Gridder::Awproject(_) => Some("with A-projection"),
    };
    if let Some(unsupported) = unsupported {
        return refuse(
            request.imagename.clone(),
            format!("{flags} {unsupported} is not implemented yet (#980)"),
        );
    }
    let family = Family::of(request);
    let missing = |base: &str| {
        family
            .names(base)
            .into_iter()
            .map(|name| product_path(&request.imagename, &name))
            .find(|path| !continuing || !path.exists())
    };
    if !request.calcpsf {
        for base in ["psf", "sumwt"] {
            if let Some(path) = missing(base) {
                return refuse(
                    path,
                    "calcpsf=False reads the PSF and sums of weights an earlier run left at \
                     imagename, and this one does not exist"
                        .into(),
                );
            }
        }
    }
    if !request.calcres
        && request.niter > 0
        && let Some(path) = missing("residual")
    {
        return refuse(
            path,
            "calcres=False with niter > 0 cleans the residual an earlier run left at imagename, \
             and this one does not exist"
                .into(),
        );
    }
    Ok(())
}

/// Whether a `calcres=False` run reads a residual: it continues on an
/// earlier run whose residual exists. Without one a run of no iterations
/// makes the PSF alone, and writes no residual, model or restored image,
/// as CASA does.
pub(crate) fn reads_residual(request: &ImagingRequest) -> bool {
    !request.calcres
        && request.tclean_restart
        && Family::of(request)
            .names("residual")
            .iter()
            .all(|name| product_path(&request.imagename, name).exists())
}

/// Whether a continuation starts from the model an earlier run left.
pub(crate) fn continues_model(request: &ImagingRequest) -> bool {
    request.tclean_restart
        && Family::of(request)
            .names("model")
            .iter()
            .any(|name| product_path(&request.imagename, name).exists())
}

/// Whether the run forms or reads a residual, and so writes the residual,
/// the model and, with `restoration`, the restored image.
pub(crate) fn has_residual(request: &ImagingRequest) -> bool {
    request.calcres || reads_residual(request)
}

/// The product `prefix` names with `suffix`, appended to the prefix's
/// bytes as publication appends it.
pub(crate) fn product_path(prefix: &Path, suffix: &str) -> PathBuf {
    let mut path = prefix.as_os_str().to_os_string();
    path.push(".");
    path.push(suffix);
    path.into()
}

/// The suffix of `path`, a product of `prefix`, after its dot.
pub(crate) fn suffix(prefix: &Path, path: &Path) -> String {
    let path = path.as_os_str().to_string_lossy();
    let prefix = prefix.as_os_str().to_string_lossy();
    path.strip_prefix(prefix.as_ref())
        .and_then(|suffix| suffix.strip_prefix('.'))
        .unwrap_or("")
        .to_string()
}

pub(crate) fn check_prefix(prefix: &Path) -> Result<(), PrepareError> {
    if let Some(path) = products(prefix)?.into_iter().next() {
        return Err(PrepareError::ExistingImageProduct { path });
    }
    Ok(())
}

pub(crate) fn products(prefix: &Path) -> Result<Vec<PathBuf>, PrepareError> {
    let mut products = Vec::new();
    // Publication appends to the original path bytes, including a trailing
    // slash: `out/` writes `out/.model`, not the sibling `out.model`.
    let model = product_path(prefix, "model");
    let directory = model
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
            ) =>
        {
            return Ok(products);
        }
        Err(source) => {
            return Err(PrepareError::InspectOutputDirectory {
                path: directory.into(),
                source,
            });
        }
    };
    let Some(stem) = model.file_name() else {
        return Ok(products);
    };
    let stem = stem.to_string_lossy();
    let stem = stem.strip_suffix(".model").expect("appended suffix");
    for entry in entries {
        let entry = entry.map_err(|source| PrepareError::InspectOutputDirectory {
            path: directory.into(),
            source,
        })?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(suffix) = name.strip_prefix(stem).and_then(|s| s.strip_prefix('.')) else {
            continue;
        };
        // Includes Taylor terms and derived products (.image.tt0.pbcor),
        // orphan masks/PBs and dangling symlinks. A failed run's summary
        // alone is not a scientific product and must permit a retry.
        if matches!(
            suffix.split('.').next(),
            Some(
                "image"
                    | "model"
                    | "residual"
                    | "psf"
                    | "sumwt"
                    | "weight"
                    | "pb"
                    | "mask"
                    | "alpha"
                    | "beta"
                    | "gridwt"
            )
        ) {
            products.push(entry.path());
        }
    }
    products.sort();
    Ok(products)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checks_scientific_names_including_orphans_and_taylor_terms() {
        let directory = tempfile::tempdir().unwrap();
        let prefix = directory.path().join("result");
        for suffix in [
            "model",
            "psf.tt0",
            "mask",
            "pb",
            "image.tt0.pbcor",
            "alpha.error",
        ] {
            let path = directory.path().join(format!("result.{suffix}"));
            fs::write(&path, "previous science").unwrap();
            let error = check_prefix(&prefix).unwrap_err();
            assert!(matches!(error, PrepareError::ExistingImageProduct { .. }));
            assert_eq!(fs::read_to_string(&path).unwrap(), "previous science");
            fs::remove_file(path).unwrap();
        }
        for name in [
            "result.summary.json",
            "result.notes",
            "result_2.model",
            "result2.image",
        ] {
            fs::write(directory.path().join(name), "unrelated or retryable").unwrap();
        }
        check_prefix(&prefix).unwrap();
        check_prefix(&directory.path().join("new/subdirectory/image")).unwrap();
    }

    #[test]
    fn a_trailing_slash_checks_the_actual_publication_directory() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(".model");
        fs::write(&path, "previous model").unwrap();
        let prefix = PathBuf::from(format!("{}/", directory.path().display()));
        assert!(matches!(
            check_prefix(&prefix),
            Err(PrepareError::ExistingImageProduct { .. })
        ));
        assert_eq!(fs::read_to_string(path).unwrap(), "previous model");
    }

    #[test]
    #[cfg(unix)]
    fn a_dangling_product_link_still_occupies_the_namespace() {
        let directory = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink("absent", directory.path().join("result.model")).unwrap();
        assert!(check_prefix(&directory.path().join("result")).is_err());
    }

    #[test]
    fn each_family_admits_its_own_products_only() {
        let single = Family::Single;
        let taylor = Family::Taylor { terms: 2 };
        for suffix in [
            "model",
            "image",
            "image.pbcor",
            "residual",
            "psf",
            "sumwt",
            "weight",
            "pb",
            "mask",
        ] {
            assert!(single.admits(suffix), "{suffix}");
        }
        for suffix in ["model.tt0", "psf.tt0", "alpha", "residual.pbcor"] {
            assert!(!single.admits(suffix), "{suffix}");
        }
        for suffix in [
            "model.tt1",
            "image.tt0",
            "image.tt1.pbcor",
            "residual.tt1",
            "psf.tt2",
            "sumwt.tt2",
            "weight.tt2",
            "pb.tt0",
            "alpha",
            "alpha.error",
            "alpha.pbcor",
            "mask",
        ] {
            assert!(taylor.admits(suffix), "{suffix}");
        }
        for suffix in [
            "model",
            "psf",
            "model.tt2",
            "psf.tt3",
            "pb.tt1",
            "psf.tt0.pbcor",
        ] {
            assert!(!taylor.admits(suffix), "{suffix}");
        }
        assert_eq!(taylor.names("psf"), ["psf.tt0", "psf.tt1", "psf.tt2"]);
        assert_eq!(taylor.names("residual"), ["residual.tt0", "residual.tt1"]);
        assert_eq!(taylor.names("pb"), ["pb.tt0"]);
        assert_eq!(single.names("sumwt"), ["sumwt"]);
    }

    fn request(prefix: &Path, overrides: serde_json::Value) -> ImagingRequest {
        let mut values = serde_json::json!({"imagename": prefix});
        values
            .as_object_mut()
            .unwrap()
            .extend(overrides.as_object().unwrap().clone());
        crate::request::tests::request(values)
    }

    #[test]
    fn calcpsf_false_needs_the_earlier_psf_and_sums_on_a_supported_gridder() {
        let directory = tempfile::tempdir().unwrap();
        let prefix = directory.path().join("result");
        let read = request(&prefix, serde_json::json!({"calcpsf": false}));
        let refusal = check(&read).unwrap_err().to_string();
        assert!(refusal.contains("result.psf"), "{refusal}");
        fs::create_dir(directory.path().join("result.psf")).unwrap();
        let refusal = check(&read).unwrap_err().to_string();
        assert!(refusal.contains("result.sumwt"), "{refusal}");
        fs::create_dir(directory.path().join("result.sumwt")).unwrap();
        check(&read).unwrap();
        let mosaic = request(
            &prefix,
            serde_json::json!({"calcpsf": false, "gridder": "mosaic"}),
        );
        let refusal = check(&mosaic).unwrap_err().to_string();
        assert!(refusal.contains("mosaic gridder"), "{refusal}");
    }

    #[test]
    fn calcres_false_cleans_only_an_existing_residual() {
        let directory = tempfile::tempdir().unwrap();
        let prefix = directory.path().join("result");
        // No residual and no iterations: the PSF alone.
        check(&request(&prefix, serde_json::json!({"calcres": false}))).unwrap();
        let cleaning = request(&prefix, serde_json::json!({"calcres": false, "niter": 5}));
        let refusal = check(&cleaning).unwrap_err().to_string();
        assert!(refusal.contains("result.residual"), "{refusal}");
        fs::create_dir(directory.path().join("result.residual")).unwrap();
        check(&cleaning).unwrap();
        let mut continued = cleaning.with_tclean_restart().unwrap();
        assert!(reads_residual(&continued));
        continued.calcres = true;
        assert!(!reads_residual(&continued) && has_residual(&continued));
    }

    #[test]
    fn a_new_or_automatic_mask_over_an_existing_one_is_refused_only_when_cleaning() {
        let directory = tempfile::tempdir().unwrap();
        let prefix = directory.path().join("result");
        fs::create_dir(directory.path().join("result.mask")).unwrap();
        for overrides in [
            serde_json::json!({"usemask": "auto-multithresh"}),
            serde_json::json!({"mask": "box[[1pix,1pix],[4pix,4pix]]"}),
        ] {
            let mut cleaning = overrides.clone();
            cleaning["niter"] = serde_json::json!(5);
            assert!(check(&request(&prefix, cleaning)).is_err());
            check(&request(&prefix, overrides)).unwrap();
        }
        check(&request(&prefix, serde_json::json!({"niter": 5}))).unwrap();
        let taylor = request(
            &prefix,
            serde_json::json!({"deconvolver": "mtmfs", "nterms": 2}),
        );
        check(&taylor).unwrap();
        fs::create_dir(directory.path().join("result.model")).unwrap();
        let refusal = check(&taylor).unwrap_err().to_string();
        assert!(refusal.contains("Taylor images"), "{refusal}");
    }
}
