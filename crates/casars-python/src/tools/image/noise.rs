// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_images::analysis::ImageNoise;
use num_complex::{Complex32, Complex64};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString};

use crate::convert::Variant;

use super::PyImageTool;
use super::handle::{ImageHandle, Storage, raised, with_storage};
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
            let mut image = attachment.lock();
            if image.fits().is_some() {
                return Err(raised("LatticeIterator cannot be constructed; lattice is not writable"));
            }
            let selection = region.as_ref().map(Region::as_region);
            let mut noise = ImageNoise::new(r#type, &pars.0, &seeds).map_err(raised)?;
            attachment.require_lock(&mut image, casa_tables::LockType::Write)?;
            match &mut *image {
                ImageHandle::Float(storage) => with_storage!(storage, target => {
                    noise.apply(target.as_mut(), selection, zero, |value, generator| {
                        (f64::from(value) + generator.draw()) as f32
                    })
                }),
                ImageHandle::Double(storage) => with_storage!(storage, target => {
                    noise.apply(target.as_mut(), selection, zero, |value, generator| {
                        value + generator.draw()
                    })
                }),
                ImageHandle::Complex(storage) => with_storage!(storage, target => {
                    noise.apply(target.as_mut(), selection, zero, |value, generator| {
                        Complex32::new(
                            (f64::from(value.re) + generator.draw()) as f32,
                            (f64::from(value.im) + generator.draw()) as f32,
                        )
                    })
                }),
                ImageHandle::DComplex(storage) => with_storage!(storage, target => {
                    noise.apply(target.as_mut(), selection, zero, |value, generator| {
                        Complex64::new(value.re + generator.draw(), value.im + generator.draw())
                    })
                }),
            }.map_err(raised)?;
            if state.history {
                let parameters = pars.0.iter().map(|value| {
                    if value.is_nan() { "nan".into() } else { format!("{value:.6}") }
                }).collect::<Vec<_>>().join(",");
                let parameters = format!("[{parameters}]");
                let seed_text = format!("[{}]", seeds.iter().map(i64::to_string).collect::<Vec<_>>().join(","));
                image.record_history(
                    "addnoise",
                    format!(
                        "ia.addnoise(type=\"{}\", pars={}, region={region_history}, zeroit={zero}, seeds={})",
                        shorten(r#type), shorten(&parameters), shorten(&seed_text),
                    ),
                )?;
            }
            image.save().map_err(raised)?;
            Ok(true)
        })
    }
}

fn shorten(value: &str) -> &str {
    if value.len() > 300 { "..." } else { value }
}
