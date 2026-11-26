// SPDX-License-Identifier: LGPL-3.0-or-later
//! Names and publication of a complete set of moment images.

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use casa_tables::publication::{exchange_directories, rename_without_replacing};
use casa_tables::{Table, TableOptions};

use super::PixelClip;
use crate::ImmomentsRequest;
use casa_images::ImageError;

pub(super) const SUFFIXES: [&str; 13] = [
    ".average",
    ".integrated",
    ".weighted_coord",
    ".weighted_dispersion_coord",
    ".median",
    ".median_coord",
    ".standard_deviation",
    ".rms",
    ".abs_mean_dev",
    ".maximum",
    ".maximum_coord",
    ".minimum",
    ".minimum_coord",
];

pub(super) struct Destinations {
    pub(super) paths: Vec<PathBuf>,
    parent: PathBuf,
    source: PathBuf,
    overwrite: bool,
}

pub(super) fn destinations(
    request: &ImmomentsRequest,
    clip: PixelClip,
) -> Result<Destinations, ImageError> {
    if request.moments.is_empty() || request.moments.iter().any(|i| !(-1..=11).contains(i)) {
        return Err(ImageError::InvalidMetadata(
            "immoments takes moment numbers -1 through 11".into(),
        ));
    }
    if request.moments.contains(&4) && !clip.permits_median_coordinate() {
        return Err(ImageError::CasaSelection(
            "The median coordinate moment requires an include or exclude pixel range that is all positive or all negative".into(),
        ));
    }
    let name = request
        .outfile
        .file_name()
        .ok_or_else(|| ImageError::CasaSelection("outfile must name an image".into()))?;
    let absolute = std::path::absolute(&request.outfile).map_err(output_error)?;
    let parent = absolute
        .parent()
        .ok_or(ImageError::NotPersistent)?
        .canonicalize()
        .map_err(output_error)?;
    let base = parent.join(name);
    let source = request.imagename.canonicalize().map_err(output_error)?;
    check_destination(&base, &source, request.overwrite)?;
    let paths = if request.moments.len() == 1 {
        vec![base]
    } else {
        let mut unique = BTreeSet::new();
        let mut paths = Vec::with_capacity(request.moments.len());
        for &moment in &request.moments {
            let mut name = name.to_owned();
            name.push(SUFFIXES[(moment + 1) as usize]);
            let path = parent.join(name);
            if !unique.insert(moment) {
                return Err(already_exists(&path));
            }
            check_destination(&path, &source, request.overwrite)?;
            paths.push(path);
        }
        paths
    };
    Ok(Destinations {
        paths,
        parent,
        source,
        overwrite: request.overwrite,
    })
}

fn check_destination(path: &Path, source: &Path, overwrite: bool) -> Result<(), ImageError> {
    let overlaps_source = |path: &Path| source.starts_with(path) || path.starts_with(source);
    if overlaps_source(path) || path.canonicalize().is_ok_and(|path| overlaps_source(&path)) {
        return Err(ImageError::CasaSelection(
            "outfile must be separate from the input image directory".into(),
        ));
    }
    let metadata = match path.symlink_metadata() {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(output_error(error)),
    };
    if !overwrite {
        return Err(already_exists(path));
    }
    let replaceable = metadata.is_dir()
        && (Table::open_metadata_only(TableOptions::new(path))
            .is_ok_and(|table| table.info().table_type == "Image")
            || fs::read_dir(path).is_ok_and(|mut entries| entries.next().is_none()));
    if !replaceable {
        return Err(ImageError::CasaSelection(format!(
            "{} is not a CASA image or an empty directory",
            path.display()
        )));
    }
    Ok(())
}

impl Destinations {
    pub(super) fn stage(&self) -> Result<tempfile::TempDir, ImageError> {
        tempfile::tempdir_in(&self.parent).map_err(output_error)
    }

    pub(super) fn publish(&self, stage: tempfile::TempDir) -> Result<(), ImageError> {
        let mut exchanges = Vec::with_capacity(self.paths.len());
        for (index, destination) in self.paths.iter().enumerate() {
            let staged = stage.path().join(index.to_string());
            match self.publish_one(&staged, destination) {
                Ok(exchanged) => exchanges.push(exchanged),
                Err(error) => {
                    let mut failed = Vec::new();
                    for (i, exchanged) in exchanges.into_iter().enumerate().rev() {
                        let staged = stage.path().join(i.to_string());
                        let path = &self.paths[i];
                        let reverted = if exchanged {
                            exchange_directories(&staged, path)
                        } else {
                            rename_without_replacing(path, &staged)
                        };
                        if let Err(error) = reverted {
                            failed.push(format!("{}: {error}", path.display()));
                        }
                    }
                    if failed.is_empty() {
                        return Err(error);
                    }
                    let preserved = stage.keep();
                    return Err(ImageError::Io(format!(
                        "{error}; could not restore {}; staging preserved at {}",
                        failed.join(", "),
                        preserved.display()
                    )));
                }
            }
        }
        Ok(())
    }

    fn publish_one(&self, staged: &Path, destination: &Path) -> Result<bool, ImageError> {
        match rename_without_replacing(staged, destination) {
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists && self.overwrite => {
                check_destination(destination, &self.source, true)?;
                exchange_directories(staged, destination).map_err(output_error)?;
                Ok(true)
            }
            result => result.map(|()| false).map_err(output_error),
        }
    }
}

fn already_exists(path: &Path) -> ImageError {
    ImageError::CasaSelection(format!("moment output already exists: {}", path.display()))
}

fn output_error(error: io::Error) -> ImageError {
    ImageError::Io(error.to_string())
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;

    #[test]
    fn a_late_failure_restores_replaced_outputs_and_removes_new_ones() {
        let workspace = tempfile::tempdir().unwrap();
        let output = Destinations {
            paths: ["old", "new", "failed"]
                .map(|name| workspace.path().join(name))
                .to_vec(),
            parent: workspace.path().to_owned(),
            source: workspace.path().join("input"),
            overwrite: true,
        };
        fs::create_dir(&output.paths[0]).unwrap();
        let stage = output.stage().unwrap();
        for i in 0..2 {
            let product = stage.path().join(i.to_string());
            fs::create_dir(&product).unwrap();
            fs::write(product.join("new pixels"), "").unwrap();
        }
        // The third staged directory is missing at publication time.
        assert!(output.publish(stage).is_err());
        assert!(output.paths[0].is_dir());
        assert!(output.paths[0].read_dir().unwrap().next().is_none());
        assert!(!output.paths[1].exists());
        assert!(!output.paths[2].exists());
    }
}
