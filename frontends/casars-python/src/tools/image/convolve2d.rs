// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_core::RecordValue;
use casa_core::quanta::{Quantity, casacore::stream_double};
use casa_imageanalysis::analysis::{
    ConvolutionOptions, ConvolutionPixel, GaussianConvolutionOptions, ImageGaussianConvolution,
    ensure_outfile_does_not_alias_input,
};
use casa_images::{BeamComponent, GaussianBeam, ImageInfo, ImageInterface, StoredGaussianBeam};
use pyo3::prelude::*;

use crate::convert::{IntList, Variant, quantity};

use super::PyImageTool;
use super::attachment::{AttachedImage, read_locked};
use super::beams::{image_info_record, stored_beam_record};
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised, with_storage};
use super::subimage::{RegionArgument, mask_argument, require_bool};

fn parsed_widths(widths: &[Variant; 3], beam: &Variant) -> PyResult<[Quantity; 3]> {
    let parse = |value: &Variant| quantity(value).map_err(raised);
    let Variant::Record(beam) = beam else {
        if beam.is_empty_sequence() {
            return Ok([parse(&widths[0])?, parse(&widths[1])?, parse(&widths[2])?]);
        }
        return Err(raised("beam must be a record"));
    };
    if beam.is_empty() {
        return Ok([parse(&widths[0])?, parse(&widths[1])?, parse(&widths[2])?]);
    }
    if widths
        .iter()
        .any(|field| !matches!(field, Variant::Text(value) if value.is_empty()))
    {
        return Err(raised(
            "major, minor, and/or pa may not be specified if beam is specified",
        ));
    }
    if beam.len() != 3 {
        return Err(raised("If given, beam must have exactly three fields"));
    }
    let major = beam
        .get("major")
        .ok_or_else(|| raised("Beam must have a 'major' field"))?;
    let minor = beam
        .get("minor")
        .ok_or_else(|| raised("Beam must have a 'minor' field"))?;
    let angle = beam
        .get("pa")
        .or_else(|| beam.get("positionangle"))
        .ok_or_else(|| raised("Beam must have a 'positionangle' or 'pa' field"))?;
    for field in [major, minor, angle] {
        if !matches!(field, Variant::Text(_) | Variant::Record(_)) {
            return Err(raised("Unsupported data type for beam"));
        }
    }
    Ok([parse(major)?, parse(minor)?, parse(angle)?])
}

struct Request<'a> {
    outfile: &'a str,
    axes: IntList,
    r#type: &'a str,
    widths: [Variant; 3],
    scale: f64,
    region: RegionArgument,
    mask: String,
    overwrite: bool,
    stretch: bool,
    targetres: bool,
    beam: Variant,
    history: bool,
}

impl Request<'_> {
    fn run(&self, attachment: &AttachedImage) -> PyResult<AttachedImage> {
        let mut live_coordinates = None;
        let mut output = publish_image(self.outfile, self.overwrite, |path| {
            let staged = read_locked(attachment, |source| self.build(attachment, source, path))?;
            live_coordinates = AttachedImage::capture_live_coordinates(&staged)?;
            Ok(staged)
        })?;
        if let Some((record, system)) = live_coordinates {
            output.keep_live_coordinates(record, system);
        }
        Ok(output)
    }

    fn check_kernel(&self) -> PyResult<()> {
        let kind = self.r#type.to_ascii_lowercase();
        let gaussian = !kind.is_empty() && "gaussian".starts_with(&kind);
        let has_beam = matches!(&self.beam, Variant::Record(record) if !record.is_empty());
        if !gaussian && has_beam {
            return Err(raised("beam can only be given with a gaussian kernel"));
        }
        if gaussian {
            return Ok(());
        }
        if !kind.is_empty() && "boxcar".starts_with(&kind) {
            if self.targetres {
                return Err(raised(
                    "targetres can only be true for a Gaussian convolving kernel",
                ));
            }
            return Err(raised("Boxcar kernel not yet implemented"));
        }
        if !kind.is_empty() && "hanning".starts_with(&kind) {
            return Err(raised("The kernel type HANNING is not supported"));
        }
        Err(raised(format!("Illegal kernel type {}.", self.r#type)))
    }

    fn build(
        &self,
        attachment: &AttachedImage,
        source: &ImageHandle,
        destination: Option<&Path>,
    ) -> PyResult<ImageHandle> {
        if !matches!(source, ImageHandle::Float(_) | ImageHandle::Double(_)) {
            return Err(raised("convolve2d only supports real-valued images"));
        }
        if !self.outfile.is_empty() {
            for input in source
                .path()
                .into_iter()
                .chain(attachment.operand_paths().iter().map(|path| path.as_path()))
            {
                ensure_outfile_does_not_alias_input(Path::new(self.outfile), input)
                    .map_err(raised)?;
            }
        }
        self.check_kernel()?;
        let widths = parsed_widths(&self.widths, &self.beam)?;
        let geometry = GaussianConvolutionOptions {
            axes: self.axes.0.clone(),
            widths,
            scale: self.scale,
            target_resolution: self.targetres,
        };
        let coordinates = source.coordinate_record();
        let beams = attachment.beam_view().clone();
        let (mut output, updated) = match source {
            ImageHandle::Float(storage) => {
                let (product, updated) = match storage {
                    Storage::Temp {
                        image,
                        fits: Some(fits),
                        ..
                    } => self.convolve(
                        &fits.image_view(image),
                        coordinates,
                        beams.record(),
                        geometry,
                        destination,
                    ),
                    _ => with_storage!(storage, image => self.convolve(
                        image.as_ref(), coordinates, beams.record(), geometry, destination,
                    )),
                }?;
                (ImageHandle::Float(product), updated)
            }
            ImageHandle::Double(storage) => {
                let (product, updated) = with_storage!(storage, image => self.convolve(
                    image.as_ref(), coordinates, beams.record(), geometry, destination,
                ))?;
                (ImageHandle::Double(product), updated)
            }
            _ => unreachable!("checked real image precision"),
        };
        if let Some(record) = updated {
            output.set_coordinate_record(record)?;
        }
        output.append_history_entries(&source.history_entries()?)?;
        if self.history {
            output.record_history("convolve2d", self.history_call())?;
        }
        output.append_history_at(
            "Image2DConvolver::_dealWithRestoringBeam",
            &[
                beam_history(
                    &format!(
                        "Original {}",
                        source.path().map_or_else(
                            || "Temporary_Image".to_owned(),
                            |path| path.display().to_string()
                        )
                    ),
                    Some(beams.record()),
                )?,
                beam_history(
                    &format!("Output {}", self.outfile),
                    image_info_record(&output),
                )?,
            ],
        )?;
        output.save().map_err(raised)?;
        Ok(output)
    }

    fn convolve<T: ConvolutionPixel>(
        &self,
        input: &dyn ImageInterface<T>,
        coordinates: RecordValue,
        beams: &RecordValue,
        geometry: GaussianConvolutionOptions,
        destination: Option<&Path>,
    ) -> PyResult<(Storage<T>, Option<RecordValue>)> {
        let plan = ImageGaussianConvolution::new(
            input,
            coordinates,
            beams,
            self.region.selection(),
            geometry,
        )
        .map_err(raised)?;
        let options = ConvolutionOptions {
            scale: self.scale,
            mask: Some(&self.mask),
            stretch: self.stretch,
            destination: (!self.outfile.is_empty()).then(|| Path::new(self.outfile)),
        };
        let section = plan.section();
        let mut result = Storage::create(
            section.shape.clone(),
            section.coordinates.clone(),
            destination,
            None,
        )
        .map_err(raised)?;
        with_storage!(&mut result, image => plan.copy_to(input, options, image.as_mut()))
            .map_err(raised)?;
        with_storage!(&mut result, image => image.set_image_info_record(plan.image_info_record().clone())).map_err(raised)?;
        Ok((result, section.coordinate_record.clone()))
    }

    fn history_call(&self) -> String {
        let Self {
            outfile,
            axes,
            r#type,
            widths,
            scale,
            region,
            mask,
            overwrite,
            stretch,
            targetres,
            ..
        } = self;
        let widths = widths.each_ref().map(|value| match value {
            Variant::Text(text) => format!("{text:?}"),
            value => value.describe(),
        });
        let [major, minor, pa] = widths;
        let axes = axes
            .0
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let region = region.history();
        let beam = "{...}";
        let kind = r#type;
        format!(
            "ia.convolve2d(outfile={outfile:?}, axes=[{axes}], type={kind:?}, major={major}, minor={minor}, pa={pa}, scale={scale:.6}, region={region}, mask={mask:?}, overwrite={overwrite}, stretch={stretch}, targetres={targetres}, beam={beam})"
        )
    }
}

fn beam_history(description: &str, record: Option<&RecordValue>) -> PyResult<String> {
    let Some(info) = record else {
        return Ok(format!("{description} has no beam"));
    };
    let beams = ImageInfo::from_record(info).map_err(raised)?.beam_set;
    let format_beam = |beam: GaussianBeam| -> PyResult<String> {
        let mut stored = None;
        for pol in 0..beams.n_stokes() {
            for channel in 0..beams.n_channels() {
                if *beams.beam(channel, pol) == beam {
                    stored = Some(stored_beam_record(Some(info), &beams, channel, pol));
                    break;
                }
            }
        }
        let record = stored.unwrap_or_else(|| beam.to_record());
        let stored = StoredGaussianBeam::from_record(&record).map_err(raised)?;
        Ok([
            BeamComponent::Major,
            BeamComponent::Minor,
            BeamComponent::PositionAngle,
        ]
        .into_iter()
        .zip(["major", "minor", "pa"])
        .map(|(component, name)| {
            let q = stored.component(component);
            format!("{name}: {} {}", stream_double(q.value()), q.unit().name())
        })
        .collect::<Vec<_>>()
        .join(", "))
    };
    if beams.is_multi() {
        let min = format_beam(beams.min_area_beam().copied().unwrap_or_default())?;
        let max = format_beam(beams.max_area_beam().copied().unwrap_or_default())?;
        let median = format_beam(beams.median_area_beam().unwrap_or_default())?;
        Ok(format!(
            "{description} has multiple beams. Min area beam: {min}. Max area beam: {max}. Median area beam {median}"
        ))
    } else if let Some(beam) = beams.single_beam().filter(|beam| !beam.is_null()) {
        Ok(format!("{description} resolution {}", format_beam(beam)?))
    } else {
        Ok(format!("{description} has no beam"))
    }
}

#[pymethods]
impl PyImageTool {
    /// Convolve two image axes with a Gaussian or to a requested resolution.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", axes=IntList::of(&[0, 1]), r#type="gaussian", major=None, minor=None, pa=None, scale=-1.0, region=RegionArgument::default(), mask=None, overwrite=false, stretch=false, targetres=false, beam=None), text_signature = "($self, outfile='', axes=[0, 1], type='gaussian', major='0deg', minor='0deg', pa='0deg', scale=-1.0, region={}, mask='', overwrite=False, stretch=False, targetres=False, beam={})")]
    fn convolve2d(
        &self,
        py: Python<'_>,
        outfile: &str,
        axes: IntList,
        r#type: &str,
        major: Option<Variant>,
        minor: Option<Variant>,
        pa: Option<Variant>,
        scale: f64,
        region: RegionArgument,
        mask: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
        #[pyo3(from_py_with = require_bool)] targetres: bool,
        beam: Option<Variant>,
    ) -> PyResult<Py<Self>> {
        let mask = mask_argument(mask)?;
        let widths =
            [major, minor, pa].map(|width| width.unwrap_or_else(|| Variant::Text("0deg".into())));
        let output = py.detach(|| {
            let state = self.state();
            let Some(source) = state.image.as_ref() else {
                return Ok(None);
            };
            Request {
                outfile,
                axes,
                r#type,
                widths,
                scale,
                region,
                mask,
                overwrite,
                stretch,
                targetres,
                beam: beam.unwrap_or(Variant::Empty),
                history: state.history,
            }
            .run(source)
            .map(Some)
        })?;
        Py::new(py, output.map_or_else(Self::new, Self::attached))
    }
}
