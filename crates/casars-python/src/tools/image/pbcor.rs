// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_coordinates::CoordinateType;
use casa_images::{
    ImageInterface,
    analysis::{
        ImagePbcor, ImagePbcorBeam, ImagePbcorOptions, ensure_outfile_does_not_alias_input,
    },
    image::{ImageHistoryRow, image_history_rows},
};
use casa_types::{ArrayD, RecordValue};
use numpy::{PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyFloat, PyInt, PyList, PySlice, PyString, PyTuple};

use super::PyImageTool;
use super::attachment::{AttachedImage, read_locked};
use super::beam_view::BeamView;
use super::beams::subset_beam_record;
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised, with_storage};
use super::pixels::{Pixels, snapshot_array};
use super::subimage::{RegionArgument, require_bool};

enum BeamArgument {
    File(String),
    Pixels {
        data: ArrayD<f32>,
        shape: Vec<usize>,
        history: String,
    },
    Unsupported(&'static str),
}

impl Default for BeamArgument {
    fn default() -> Self {
        Self::File(String::new())
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for BeamArgument {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(text) = value.cast::<PyString>() {
            return Ok(Self::File(text.to_str()?.to_owned()));
        }
        if let Ok(bytes) = value.cast::<PyBytes>() {
            return Ok(Self::File(
                std::str::from_utf8(bytes.as_bytes())
                    .map_err(raised)?
                    .to_owned(),
            ));
        }
        let py = value.py();
        // Discover the numeric variant of a nested sequence before taking
        // the snapshot, which would otherwise promote integer data to f64.
        let converted = if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
            if value.len()? == 0 {
                return Ok(Self::Unsupported("boolvec"));
            }
            Some(py.import("numpy")?.call_method1("asarray", (&*value,))?)
        } else {
            None
        };
        let raw = converted.as_ref().unwrap_or(&value);
        if let Ok(array) = raw.cast::<PyUntypedArray>() {
            let dtype = array.dtype();
            return match (dtype.kind(), dtype.itemsize()) {
                (b'f', 4 | 8) if array.ndim() != 0 => capture_float_beam(py, array),
                (b'f', 4 | 8) => Ok(Self::Unsupported("double")),
                (b'i' | b'u', _) => Ok(Self::Unsupported("intvec")),
                (b'c', _) => Ok(Self::Unsupported("complexvec")),
                (b'S' | b'U', _) => Ok(Self::Unsupported("stringvec")),
                _ => Ok(Self::Unsupported("boolvec")),
            };
        }
        let kind = if value.is_instance_of::<PyDict>() {
            "record"
        } else if value.is_instance_of::<PyFloat>() {
            "double"
        } else if value.is_instance_of::<PyInt>() {
            "int"
        } else {
            "boolvec"
        };
        Ok(Self::Unsupported(kind))
    }
}

fn capture_float_beam(py: Python<'_>, array: &Bound<'_, PyUntypedArray>) -> PyResult<BeamArgument> {
    let shape = array.shape().to_vec();
    let history = if array.len() > 100 {
        "\"(...)\"".into()
    } else {
        Pixels::capture(py, Some(array.as_any()), false)?.history()
    };
    // A broadcast view can describe an entire beam plane from one stored
    // value. Snapshot only the addressable data on the repeated axes.
    let compact = if array.strides().contains(&0) {
        let slices = shape.iter().zip(array.strides()).map(|(&length, &stride)| {
            PySlice::new(py, 0, if stride == 0 { 1 } else { length as isize }, 1)
        });
        Some(
            array
                .get_item(PyTuple::new(py, slices)?)?
                .cast_into::<PyUntypedArray>()?,
        )
    } else {
        None
    };
    let data = snapshot_array(py, compact.as_ref().unwrap_or(array))?;
    Ok(BeamArgument::Pixels {
        data,
        shape,
        history,
    })
}

struct Request<'a> {
    beam: &'a BeamArgument,
    outfile: &'a str,
    overwrite: bool,
    spatial: &'a str,
    region: &'a RegionArgument,
    chans: &'a str,
    stokes: &'a str,
    mask: &'a str,
    mode: &'a str,
    cutoff: f32,
    stretch: bool,
    history: bool,
}

impl Request<'_> {
    fn run(&self, source: &AttachedImage) -> PyResult<AttachedImage> {
        if !matches!(&*source.lock(), ImageHandle::Float(_)) {
            return Err(raised("This method only supports Float valued images"));
        }
        let beam = match self.beam {
            BeamArgument::File(name) if name.is_empty() => {
                return Err(raised("The image filename is empty"));
            }
            BeamArgument::File(name) => Some(Path::new(name)),
            BeamArgument::Pixels { .. } => None,
            BeamArgument::Unsupported(kind) => {
                return Err(raised(format!("Unsupported type {kind} for pbimage")));
            }
        };
        self.region
            .validate_selectors(self.spatial, self.chans, self.stokes)?;
        let same_native = beam
            .and_then(|path| path.canonicalize().ok())
            .is_some_and(|path| source.native_path() == Some(&path));
        let beam_attachment = beam
            .filter(|_| !same_native)
            .map(|path| {
                AttachedImage::open_for_ordered_read(path).map_err(|error| {
                    raised(format!(
                        "Unable to open primary beam image {}: {error}",
                        path.display()
                    ))
                })
            })
            .transpose()?;
        publish_image(self.outfile, self.overwrite, |path| {
            if let Some(beam) = &beam_attachment {
                self.stage_two(source, beam, path)
            } else {
                read_locked(source, |image| {
                    self.stage(source, image, same_native.then_some(image), path)
                })
            }
        })
    }

    fn stage_two(
        &self,
        source: &AttachedImage,
        beam: &AttachedImage,
        path: Option<&Path>,
    ) -> PyResult<ImageHandle> {
        let source_first = source
            .native_path()
            .zip(beam.native_path())
            .is_none_or(|(source, beam)| source < beam);
        if source_first {
            read_locked(source, |image| {
                read_locked(beam, |beam_image| {
                    self.stage(source, image, Some(beam_image), path)
                })
            })
        } else {
            read_locked(beam, |beam_image| {
                read_locked(source, |image| {
                    self.stage(source, image, Some(beam_image), path)
                })
            })
        }
    }

    fn stage(
        &self,
        source: &AttachedImage,
        image: &ImageHandle,
        beam: Option<&ImageHandle>,
        path: Option<&Path>,
    ) -> PyResult<ImageHandle> {
        if !self.outfile.is_empty() {
            for input in image
                .path()
                .into_iter()
                .chain(beam.and_then(ImageHandle::path))
                .chain(
                    source
                        .operand_paths()
                        .iter()
                        .map(|operand| operand.as_path()),
                )
            {
                ensure_outfile_does_not_alias_input(Path::new(self.outfile), input)
                    .map_err(raised)?;
            }
        }
        let info = source.beam_view().clone();
        let coordinates = image.coordinate_record();
        let mut output = float_view(image, |pixels| {
            if let Some(beam) = beam {
                float_view(beam, |beam_pixels| {
                    self.correct(
                        pixels,
                        coordinates,
                        ImagePbcorBeam::Image(beam_pixels),
                        &info,
                        path,
                    )
                })
            } else {
                let BeamArgument::Pixels { data, shape, .. } = self.beam else {
                    unreachable!("resolved beam")
                };
                self.correct(
                    pixels,
                    coordinates,
                    ImagePbcorBeam::Array {
                        pixels: data,
                        shape,
                    },
                    &info,
                    path,
                )
            }
        })?;
        self.append_history(&mut output, image, beam)?;
        output.save().map_err(raised)?;
        Ok(output)
    }

    fn correct(
        &self,
        image: &dyn ImageInterface<f32>,
        coordinates: RecordValue,
        beam: ImagePbcorBeam<'_>,
        info: &BeamView,
        path: Option<&Path>,
    ) -> PyResult<ImageHandle> {
        let options = ImagePbcorOptions {
            mode: self.mode,
            cutoff: self.cutoff,
            region: self.region.selection(),
            box_selection: text_option(self.spatial),
            channels: text_option(self.chans),
            stokes: text_option(self.stokes),
            mask: text_option(self.mask),
            stretch: self.stretch,
            destination: text_option(self.outfile).map(Path::new),
        };
        let mut correction =
            ImagePbcor::new(image, coordinates, beam, options, info.record()).map_err(raised)?;
        let source_beams = info.beam_set().map_err(raised)?;
        let mut metadata = correction.section.image_info().to_record();
        if source_beams.is_multi() {
            let channels = correction.beam_planes(
                image.find_axis(CoordinateType::Spectral),
                source_beams.n_channels(),
            );
            let polarizations = correction.beam_planes(
                image.find_axis(CoordinateType::Stokes),
                source_beams.n_stokes(),
            );
            subset_beam_record(
                &mut metadata,
                Some(info.record()),
                &source_beams,
                &channels,
                &polarizations,
            )
            .map_err(raised)?;
        } else {
            info.apply_to(&mut metadata);
        }
        let mut output = Storage::create(
            correction.section.shape.clone(),
            correction.section.coordinates.clone(),
            path,
            None,
        )
        .map_err(raised)?;
        with_storage!(&mut output, dest => correction.copy_to(image, dest.as_mut()))
            .map_err(raised)?;
        with_storage!(&mut output, dest => dest.set_image_info_record(metadata)).map_err(raised)?;
        let mut output = ImageHandle::Float(output);
        if let Some(record) = correction.section.coordinate_record.take() {
            output.set_coordinate_record(record)?;
        }
        Ok(output)
    }

    fn append_history(
        &self,
        output: &mut ImageHandle,
        image: &ImageHandle,
        beam: Option<&ImageHandle>,
    ) -> PyResult<()> {
        let ImageHandle::Float(dest) = output else {
            unreachable!("correction produces float pixels")
        };
        if let Some(beam) = beam {
            for operand in [image, beam] {
                let name = operand.path().map_or_else(
                    || "Temporary_Image".to_owned(),
                    |path| path.display().to_string(),
                );
                output_history(dest, "::image::pbcor", &format!("History of image {name}"))?;
                let history = history_rows(operand)?;
                with_storage!(dest, image => image.append_history_entries(&history))
                    .map_err(raised)?;
                output_history(
                    dest,
                    "::image::pbcor",
                    &format!("End history of image {name}"),
                )?;
            }
        } else {
            let history = history_rows(image)?;
            with_storage!(dest, image => image.append_history_entries(&history)).map_err(raised)?;
            if self.history
                && let BeamArgument::Pixels { history, .. } = self.beam
            {
                let region = self.region.history();
                let Self {
                    outfile,
                    overwrite,
                    spatial,
                    chans,
                    stokes,
                    mask,
                    mode,
                    cutoff,
                    stretch,
                    ..
                } = self;
                output.record_history("pbcor", format!(
                    "ia.pbcor(pbimage={history}, outfile={outfile:?}, overwrite={overwrite}, box={spatial:?}, region={region}, chans={chans:?}, stokes={stokes:?}, mask={mask:?}, mode={mode:?}, cutoff={cutoff:.6}, stretch={stretch})"
                ))?;
            }
        }
        Ok(())
    }
}

fn output_history(dest: &mut Storage<f32>, origin: &str, message: &str) -> PyResult<()> {
    with_storage!(dest, image => image.append_history_at(origin, &[message.to_owned()]))
        .map_err(raised)
}

fn text_option(text: &str) -> Option<&str> {
    (!text.is_empty()).then_some(text)
}

fn float_view<T>(
    image: &ImageHandle,
    read: impl FnOnce(&dyn ImageInterface<f32>) -> PyResult<T>,
) -> PyResult<T> {
    let ImageHandle::Float(source) = image else {
        return Err(raised("This method only supports Float valued images"));
    };
    match source {
        Storage::Temp {
            image,
            fits: Some(fits),
            ..
        } => read(&fits.image_view(image)),
        _ => with_storage!(source, pixels => read(pixels.as_ref())),
    }
}

fn history_rows(image: &ImageHandle) -> PyResult<Vec<ImageHistoryRow>> {
    match image {
        ImageHandle::Float(Storage::Paged(image)) => {
            image_history_rows(image.name().expect("native image path")).map_err(raised)
        }
        ImageHandle::Float(Storage::Temp { image, .. }) => {
            Ok(image.history_rows().map_err(raised)?.into_owned())
        }
        _ => unreachable!("checked Float image"),
    }
}

#[pymethods]
impl PyImageTool {
    /// Multiply or divide a selected image by an aligned primary beam.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (pbimage=BeamArgument::default(), outfile="", overwrite=false, r#box="", region=RegionArgument::default(), chans="", stokes="", mask="", mode="divide", cutoff=-1.0, stretch=false), text_signature = "($self, pbimage='', outfile='', overwrite=False, box='', region={}, chans='', stokes='', mask='', mode='divide', cutoff=-1.0, stretch=False)")]
    fn pbcor(
        &self,
        py: Python<'_>,
        pbimage: BeamArgument,
        outfile: &str,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        r#box: &str,
        region: RegionArgument,
        chans: &str,
        stokes: &str,
        mask: &str,
        mode: &str,
        cutoff: f32,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
    ) -> PyResult<Py<Self>> {
        let output = py.detach(|| {
            let state = self.state();
            let source = state
                .image
                .as_ref()
                .ok_or_else(|| raised("Unable to create image"))?;
            Request {
                beam: &pbimage,
                outfile,
                overwrite,
                spatial: r#box,
                region: &region,
                chans,
                stokes,
                mask,
                mode,
                cutoff,
                stretch,
                history: state.history,
            }
            .run(source)
        })?;
        Py::new(py, Self::attached(output))
    }
}
