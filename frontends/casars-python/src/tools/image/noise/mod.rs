// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.addnoise` wrapper delegating to [`casa_imageanalysis::tool::ops_edit::add_noise`].

use casa_imageanalysis::tool::ops_edit::add_noise;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString};

use crate::convert::Variant;

use super::PyImageTool;
use super::handle::raised;
use super::subimage::Region;

mod arguments;
use arguments::{NoiseParameters, NoiseSeeds};

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (r#type="normal", pars=NoiseParameters(vec![0.0, 1.0]), region=None, zero=false, seeds=NoiseSeeds(Vec::new())), text_signature = "($self, type='normal', pars=[0.0, 1.0], region={}, zero=False, seeds=[])")]
    fn addnoise(
        &self,
        py: Python<'_>,
        r#type: &str,
        pars: NoiseParameters,
        region: Option<&Bound<'_, PyAny>>,
        zero: bool,
        seeds: NoiseSeeds,
    ) -> PyResult<bool> {
        if py.detach(|| self.state().image.is_none()) {
            return Ok(false);
        }
        let region_history = match region {
            Some(value) if value.cast::<PyString>().is_ok() => {
                let text = value.cast::<PyString>()?.to_str()?;
                format!("\"{}\"", shorten(text))
            }
            Some(value)
                if !value.is_instance_of::<PyDict>()
                    && value.extract::<Variant>()?.is_empty_sequence() =>
            {
                "[]".into()
            }
            _ => "{...}".into(),
        };
        let region = Region::argument(py, region, "addnoise")?;
        let seeds = seeds.0;
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = &state.image else {
                return Ok(false);
            };
            let history_command = state.history.then(|| {
                let parameters = pars
                    .0
                    .iter()
                    .map(|value| {
                        if value.is_nan() {
                            "nan".into()
                        } else {
                            format!("{value:.6}")
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                let parameters = format!("[{parameters}]");
                let seed_text = format!(
                    "[{}]",
                    seeds
                        .iter()
                        .map(i64::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                );
                format!(
                    "ia.addnoise(type=\"{}\", pars={}, region={region_history}, zeroit={zero}, seeds={})",
                    shorten(r#type),
                    shorten(&parameters),
                    shorten(&seed_text),
                )
            });
            add_noise(
                attachment,
                r#type,
                &pars.0,
                &seeds,
                region.as_ref().map(Region::as_region),
                zero,
                history_command,
            )
            .map_err(raised)
        })
    }
}

fn shorten(value: &str) -> &str {
    if value.len() > 300 { "..." } else { value }
}
