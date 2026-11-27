// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.fromfits` / `ia.newimagefromfits` / `ia.maketestimage` wrapper delegating to [`casa_imageanalysis::tool::fits`].

use std::path::{Path, PathBuf};

use casa_imageanalysis::tool::fits::{ImportFitsRequest, import_fits};
use pyo3::prelude::*;

use super::PyImageTool;
use super::handle::{external_open_check, raised};
use super::lifecycle::resolve_image_path;

fn command(
    outfile: &str,
    infile: &str,
    whichrep: i64,
    whichhdu: i64,
    zeroblanks: bool,
    overwrite: bool,
) -> String {
    format!(
        "ia.fromfits(outfile={outfile:?}, fitsfile={infile:?}, whichrep={whichrep}, whichhdu={whichhdu}, zeroBlanks={zeroblanks}, overwrite={overwrite})"
    )
}

fn test_image_path() -> PathBuf {
    let sample = Path::new("demo/Images/imagetestimage.fits");
    if sample.is_file() {
        return sample.to_path_buf();
    }
    let measures_root = std::env::var_os("CASA_RS_MEASURESPATH").map(PathBuf::from);
    let legacy_root = ["CASAPATH", "AIPSPATH"].into_iter().find_map(|variable| {
        std::env::var(variable).ok().and_then(|value| {
            value
                .split_whitespace()
                .next()
                .map(|root| Path::new(root).join("data"))
        })
    });
    measures_root
        .into_iter()
        .chain(legacy_root)
        .map(|root| root.join(sample))
        .find(|candidate| candidate.is_file())
        .unwrap_or_else(|| sample.to_path_buf())
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (outfile="", infile="", whichrep=0, whichhdu=0, zeroblanks=false, overwrite=false), text_signature = "($self, outfile='', infile='', whichrep=0, whichhdu=0, zeroblanks=False, overwrite=False)")]
    #[allow(clippy::too_many_arguments)]
    fn fromfits(
        &self,
        py: Python<'_>,
        outfile: &str,
        infile: &str,
        whichrep: i64,
        whichhdu: i64,
        zeroblanks: bool,
        overwrite: bool,
    ) -> PyResult<bool> {
        let expanded = resolve_image_path(py, infile)?;
        py.detach(|| {
            let mut state = self.state();
            let request = ImportFitsRequest {
                outfile,
                infile,
                resolved_infile: Path::new(expanded.as_ref()),
                whichrep,
                whichhdu,
                zeroblanks,
                overwrite,
            };
            let history = state.history.then(|| {
                (
                    "fromfits",
                    command(outfile, infile, whichrep, whichhdu, zeroblanks, overwrite),
                )
            });
            state.image =
                Some(import_fits(&request, history, external_open_check()).map_err(raised)?);
            Ok(true)
        })
    }

    #[pyo3(signature = (outfile="", infile="", whichrep=0, whichhdu=0, zeroblanks=false, overwrite=false), text_signature = "($self, outfile='', infile='', whichrep=0, whichhdu=0, zeroblanks=False, overwrite=False)")]
    #[allow(clippy::too_many_arguments)]
    fn newimagefromfits(
        &self,
        py: Python<'_>,
        outfile: &str,
        infile: &str,
        whichrep: i64,
        whichhdu: i64,
        zeroblanks: bool,
        overwrite: bool,
    ) -> PyResult<Py<Self>> {
        let expanded = resolve_image_path(py, infile)?;
        let image = py.detach(|| {
            let request = ImportFitsRequest {
                outfile,
                infile,
                resolved_infile: Path::new(expanded.as_ref()),
                whichrep,
                whichhdu,
                zeroblanks,
                overwrite,
            };
            import_fits(&request, None, external_open_check()).map_err(raised)
        })?;
        Py::new(py, Self::attached(image))
    }

    #[pyo3(signature = (outfile="", overwrite=false), text_signature = "($self, outfile='', overwrite=False)")]
    fn maketestimage(&self, py: Python<'_>, outfile: &str, overwrite: bool) -> PyResult<bool> {
        py.detach(|| {
            let mut state = self.state();
            state.image = None;
            let sample = test_image_path();
            let infile = sample
                .to_str()
                .ok_or_else(|| raised("The CASA test image path is not UTF-8"))?;
            let request = ImportFitsRequest {
                outfile,
                infile,
                resolved_infile: &sample,
                whichrep: 0,
                whichhdu: 0,
                zeroblanks: false,
                overwrite,
            };
            let history = state.history.then(|| {
                (
                    "maketestimage",
                    format!("ia.maketestimage(outfile={outfile:?}, overwrite={overwrite})"),
                )
            });
            state.image =
                Some(import_fits(&request, history, external_open_check()).map_err(raised)?);
            Ok(true)
        })
    }
}
