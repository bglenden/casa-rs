// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_images::analysis::{
    ImageMomentOptions, SubimageRegion, ensure_artifacts_do_not_overlap,
    ensure_outfile_does_not_alias_input, make_image_moments, moment_output_names,
};
use casa_images::image::image_history_rows;
use casa_types::measures::doppler::DopplerRef;
use pyo3::prelude::*;

use crate::convert::{DoubleList, IntList, StringList, Variant};
use crate::tools::{LogLine, emit};

use super::PyImageTool;
use super::attachment::{AttachedImage, read_locked};
use super::create::NamedImageStages;
use super::handle::{ImageHandle, Storage, raised};
use super::subimage::{RegionArgument, mask_argument, require_bool};

#[pymethods]
impl PyImageTool {
    /// Collapse masked image profiles and return the first requested moment image.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (moments=None, axis=-10, region=None, mask=None, method=None, smoothaxes=None, smoothtypes=None, smoothwidths=None, includepix=None, excludepix=None, peaksnr=3., stddev=0., doppler="RADIO", outfile="", smoothout="", overwrite=false, drop=true, stretch=false), text_signature = "($self, moments=[0], axis=-10, region={}, mask='', method=[], smoothaxes=[-1], smoothtypes=[], smoothwidths=[0.0], includepix=[-1.0], excludepix=[-1.0], peaksnr=3.0, stddev=0.0, doppler='RADIO', outfile='', smoothout='', overwrite=False, drop=True, stretch=False)")]
    fn moments(
        &self,
        py: Python<'_>,
        moments: Option<IntList>,
        axis: i64,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        method: Option<StringList>,
        smoothaxes: Option<IntList>,
        smoothtypes: Option<&Bound<'_, PyAny>>,
        smoothwidths: Option<DoubleList>,
        includepix: Option<DoubleList>,
        excludepix: Option<DoubleList>,
        peaksnr: f64,
        stddev: f64,
        doppler: &str,
        outfile: &str,
        smoothout: &str,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        #[pyo3(from_py_with = require_bool)] drop: bool,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
    ) -> PyResult<Py<Self>> {
        let moments = moments
            .map_or_else(|| vec![0], |values| values.0)
            .into_iter()
            .map(i32::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(raised)?;
        let region: RegionArgument =
            region.map_or_else(|| Ok(Default::default()), |value| value.extract())?;
        let mask = mask_argument(mask)?;
        let methods = method.map_or_else(Vec::new, |value| value.0);
        let axes = smoothaxes.map_or_else(|| vec![-1], |value| value.0);
        let kernels = match smoothtypes
            .map(|value| value.extract::<Variant>())
            .transpose()?
        {
            Some(Variant::Text(name)) => name
                .split(|character: char| character == ',' || character.is_ascii_whitespace())
                .filter(|name| !name.is_empty())
                .map(str::to_owned)
                .collect(),
            Some(Variant::Texts(names)) => names,
            _ => Vec::new(),
        };
        let widths = smoothwidths
            .filter(|value| !value.0.is_empty())
            .map_or_else(|| vec![0.], |value| value.0);
        let include = includepix.map_or_else(|| vec![-1.], |value| value.0);
        let exclude = excludepix.map_or_else(|| vec![-1.], |value| value.0);
        let parsed = if doppler.eq_ignore_ascii_case("TRUE") {
            Ok(DopplerRef::BETA)
        } else {
            doppler.parse::<DopplerRef>()
        };
        let logs = (!doppler.is_empty() && parsed.is_err())
            .then(|| LogLine::warning(format!("Illegal velocity type {doppler}. Using RADIO")))
            .into_iter()
            .collect::<Vec<_>>();
        let history = format!(
            "ia.moments(moments={moments:?}, axis={axis}, region={}, mask={mask:?}, method={methods:?}, smoothaxes={axes:?}, smoothtypes={kernels:?}, smoothwidths={widths:?}, includepix={include:?}, excludepix={exclude:?}, peaksnr={peaksnr}, stddev={stddev}, doppler={doppler:?}, outfile={outfile:?}, smoothout={smoothout:?}, overwrite={}, drop={}, stretch={})",
            region.history(),
            python_bool(overwrite),
            python_bool(drop),
            python_bool(stretch),
        );
        let options = ImageMomentOptions {
            moments: &moments,
            axis,
            region: region.selection(),
            mask: Some(&mask),
            stretch,
            drop,
            methods: &methods,
            smoothing: (&axes, &kernels, &widths),
            inclusion: &include,
            exclusion: &exclude,
            peak_snr: peaksnr as f32,
            noise: stddev as f32,
            doppler: parsed.unwrap_or(DopplerRef::RADIO),
            destinations: &[],
        };
        let output = py.detach(|| {
            let state = self.state();
            let Some(input) = &state.image else {
                return Ok(None);
            };
            build(
                input,
                options,
                outfile.trim(),
                smoothout.trim(),
                overwrite,
                state.history.then_some(history),
            )
            .map(Some)
        })?;
        if output.is_some() {
            emit(py, "casars.tools.image", &logs)?;
        }
        Py::new(py, output.map_or_else(Self::new, Self::attached))
    }
}

fn build(
    input: &AttachedImage,
    options: ImageMomentOptions<'_>,
    outfile: &str,
    smoothout: &str,
    overwrite: bool,
    history: Option<String>,
) -> PyResult<AttachedImage> {
    let (stages, output) = read_locked(input, |handle| {
        let ImageHandle::Float(storage) = handle else {
            return Err(raised("This method only supports float-valued images"));
        };
        let mut names = moment_output_names(
            options.moments,
            outfile,
            handle.path().and_then(Path::to_str),
        )
        .map_err(raised)?;
        let temporary = names[0].is_empty();
        names.push(smoothout.to_owned());
        let destinations = names
            .iter()
            .filter(|name| !name.is_empty())
            .map(Path::new)
            .collect::<Vec<_>>();
        for destination in &destinations {
            for source in handle
                .path()
                .into_iter()
                .chain(input.operand_paths().iter().map(|path| path.as_path()))
            {
                ensure_outfile_does_not_alias_input(destination, source).map_err(raised)?;
            }
            if let Some(region) = options.region.and_then(SubimageRegion::external_path) {
                ensure_artifacts_do_not_overlap(destination, region).map_err(raised)?;
            }
        }
        let mut stages = NamedImageStages::new(names.iter().map(String::as_str), overwrite)?;
        let mut products = storage.with_float_view(|image| {
            let mut beams = image.image_info().map_err(raised)?.to_record();
            input.beam_view().apply_to(&mut beams);
            let options = ImageMomentOptions {
                destinations: &destinations,
                ..options
            };
            make_image_moments(image, handle.coordinate_record(), Some(&beams), options)
                .map_err(raised)
        })?;
        let entries = match storage {
            Storage::Paged(image) => {
                image_history_rows(image.name().expect("persistent image")).map_err(raised)?
            }
            Storage::Temp { image, .. } => image.history_rows().map_err(raised)?.into_owned(),
        };
        let command = history.map(|details| {
            [
                concat!("version: casa-rs ", env!("CARGO_PKG_VERSION")).to_owned(),
                details,
            ]
        });
        let paths = stages.paths();
        for (path, image) in paths.iter().zip(&mut products.moments) {
            image.append_history_entries(&entries).map_err(raised)?;
            if let Some(command) = &command {
                image
                    .append_history_at("image::moments", command)
                    .map_err(raised)?;
            }
            if let Some(path) = path {
                image.save_as(path).map_err(raised)?;
            }
        }
        if let (Some(path), Some(image)) = (paths[products.moments.len()], &products.smoothed) {
            image.save_as(path).map_err(raised)?;
        }
        if products.smoothed.is_none() {
            stages.omit(products.moments.len());
        }
        let output = temporary.then(|| products.moments.remove(0));
        Ok((stages, output))
    })?;
    let primary = stages.publish_and_open_primary()?;
    match (primary, output) {
        (None, Some(image)) => {
            let record = image.coordinates().to_record();
            Ok(AttachedImage::temporary(ImageHandle::Float(
                Storage::Temp {
                    image: Box::new(image),
                    fits: None,
                    record,
                },
            )))
        }
        (Some(image), None) => Ok(image),
        _ => Err(raised("Expected exactly one primary moment image")),
    }
}

fn python_bool(value: bool) -> &'static str {
    if value { "True" } else { "False" }
}
