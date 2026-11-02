// SPDX-License-Identifier: LGPL-3.0-or-later

//! Bounded default restart (#944): same-grid, single-term Stokes-I MFS.
//! Unsupported products remain protected before execution or summary publication.

mod numbering;
pub(crate) use numbering::next_prefix;

mod grid;
pub(crate) use grid::check_grid;
mod mask;
pub(crate) use mask::MaskInput;

use std::{
    fs, io,
    path::{Path, PathBuf},
};

use crate::{ImagingRequest, PrepareError};

pub(crate) fn check(request: &ImagingRequest) -> Result<(), PrepareError> {
    let existing = products(&request.imagename)?;
    if !existing.is_empty() {
        let supported = request.specmode == crate::SpecMode::Mfs
            && request.deconvolver != crate::Deconvolver::Mtmfs
            && request.stokes == [casa_imaging_model::PolarizationCoordinate::StokesI]
            && request.outlierfile.is_none()
            && request.usemask == crate::UseMask::User
            && request.mask_image.is_none()
            && request.mask_box.is_empty()
            && !request.explicit_empty_mask
            && matches!(request.gridder, crate::Gridder::Standard { facets } if facets.get() == 1);
        for path in existing {
            let suffix = path.as_os_str().to_string_lossy();
            let prefix = request.imagename.as_os_str().to_string_lossy();
            let suffix = suffix.strip_prefix(prefix.as_ref()).unwrap_or("");
            if !supported
                || !matches!(
                    suffix,
                    ".model" | ".image" | ".residual" | ".psf" | ".sumwt" | ".pb" | ".mask"
                )
            {
                return Err(PrepareError::ExistingImageProduct { path });
            }
            if suffix == ".model" && request.startmodel.is_some() {
                return Err(PrepareError::Restart {
                    path,
                    reason: "startmodel cannot be supplied when the output model already exists"
                        .into(),
                });
            }
        }
    }
    if let Some(path) = &request.outlierfile {
        super::compile::check_outlier_outputs(path)?;
    }
    Ok(())
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
    let mut model = prefix.as_os_str().to_os_string();
    model.push(".model");
    let model = PathBuf::from(model);
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
}
