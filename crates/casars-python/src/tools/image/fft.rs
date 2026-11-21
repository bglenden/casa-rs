// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_images::{
    ImageInterface, ImagePixel,
    analysis::{FourierOptions, FourierPixel, ImageFourier, ensure_outfile_does_not_alias_input},
};
use num_complex::{Complex, Complex32, Complex64};
use pyo3::prelude::*;

use crate::convert::IntList;

use super::PyImageTool;
use super::attachment::{AttachedImage, read_locked};
use super::create::publish_new_images;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};
use super::subimage::{RegionArgument, mask_argument, require_bool};

struct Request<'a> {
    outfiles: [&'a str; 5],
    axes: IntList,
    region: RegionArgument,
    mask: String,
    stretch: bool,
    history: bool,
}

trait ProductPixel: ImagePixel {
    fn wrap(image: Storage<Self>) -> ImageHandle;
}

macro_rules! product_pixels {
    ($($pixel:ty => $kind:ident),+ $(,)?) => {
        $(impl ProductPixel for $pixel {
            fn wrap(image: Storage<Self>) -> ImageHandle {
                ImageHandle::$kind(image)
            }
        })+
    };
}

product_pixels!(
    f32 => Float,
    f64 => Double,
    Complex32 => Complex,
    Complex64 => DComplex,
);

impl Request<'_> {
    fn run(&self, attachment: &AttachedImage) -> PyResult<()> {
        let destinations: Vec<_> = self
            .outfiles
            .iter()
            .filter(|name| !name.is_empty())
            .map(Path::new)
            .collect();
        if destinations.is_empty() {
            return Err(raised("No output image names have been specified"));
        }
        publish_new_images(self.outfiles, |staged| {
            read_locked(attachment, |source| {
                for target in &destinations {
                    for input in source
                        .path()
                        .into_iter()
                        .chain(attachment.operand_paths().iter().map(|path| path.as_path()))
                    {
                        ensure_outfile_does_not_alias_input(target, input).map_err(raised)?;
                    }
                }
                if let ImageHandle::Float(Storage::Temp {
                    image,
                    fits: Some(fits),
                    ..
                }) = source
                {
                    self.compute(
                        attachment,
                        source,
                        &fits.image_view(image),
                        &destinations,
                        staged,
                    )
                } else {
                    with_pixels!(source, pixels => with_storage!(
                        pixels, image => self.compute(attachment, source, image.as_ref(), &destinations, staged)
                    ))
                }
            })
        })
    }

    fn compute<T: FourierPixel>(
        &self,
        attachment: &AttachedImage,
        source: &ImageHandle,
        input: &dyn ImageInterface<T>,
        destinations: &[&Path],
        staged: [Option<&Path>; 5],
    ) -> PyResult<()>
    where
        T::Scalar: ProductPixel,
        Complex<T::Scalar>: ProductPixel,
    {
        let plan = ImageFourier::new(
            input,
            source.coordinate_record(),
            attachment.beam_view().record(),
            FourierOptions {
                region: self.region.selection(),
                axes: &self.axes.0,
                mask: Some(&self.mask),
                stretch: self.stretch,
                destinations,
            },
        )
        .map_err(raised)?;
        let scratch = plan.transform(input).map_err(raised)?;
        let components: [fn(Complex<T::Scalar>) -> T::Scalar; 4] =
            [|value| value.re, |value| value.im, T::amplitude, T::phase];
        let history = source.history_entries()?;
        let call = self.history.then(|| self.history_call());
        for (index, component) in components.into_iter().enumerate() {
            if let Some(path) = staged[index] {
                let mut output = Storage::create(
                    plan.shape().to_vec(),
                    plan.coordinates().clone(),
                    Some(path),
                    None,
                )
                .map_err(raised)?;
                with_storage!(&mut output, image =>
                    plan.write_product(input, &scratch, image.as_mut(), index == 3, component)
                )
                .map_err(raised)?;
                let mut output = T::Scalar::wrap(output);
                output.append_history_entries(&history)?;
                if let Some(call) = &call {
                    output.record_history("fft", call.clone())?;
                }
                output.save().map_err(raised)?;
            }
        }
        if let Some(path) = staged[4] {
            let mut output = Storage::create(
                plan.shape().to_vec(),
                plan.coordinates().clone(),
                Some(path),
                None,
            )
            .map_err(raised)?;
            with_storage!(&mut output, image => plan.write_product(input, &scratch, image.as_mut(), false, |value| value))
                .map_err(raised)?;
            let mut output = Complex::<T::Scalar>::wrap(output);
            output.append_history_entries(&history)?;
            if let Some(call) = call {
                output.record_history("fft", call)?;
            }
            output.save().map_err(raised)?;
        }
        Ok(())
    }

    fn history_call(&self) -> String {
        let [real, imag, amp, phase, complex] = self.outfiles;
        let axes = self
            .axes
            .0
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let region = self.region.history();
        let mask = &self.mask;
        let stretch = self.stretch;
        format!(
            "ia.fft(real={real:?}, imag={imag:?}, amp={amp:?}, phase={phase:?}, axes=[{axes}], region={region}, mask={mask:?}, stretch={stretch}, complex={complex:?})"
        )
    }
}

#[pymethods]
impl PyImageTool {
    /// Write selected products of the forward Fourier transform of an image.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (real="", imag="", amp="", phase="", axes=IntList::of(&[-1]), region=RegionArgument::default(), mask=None, stretch=false, complex=""), text_signature = "($self, real='', imag='', amp='', phase='', axes=[-1], region={}, mask='', stretch=False, complex='')")]
    fn fft(
        &self,
        py: Python<'_>,
        real: &str,
        imag: &str,
        amp: &str,
        phase: &str,
        axes: IntList,
        region: RegionArgument,
        mask: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
        complex: &str,
    ) -> PyResult<bool> {
        let mask = mask_argument(mask)?;
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(false);
            };
            Request {
                outfiles: [real, imag, amp, phase, complex],
                axes,
                region,
                mask,
                stretch,
                history: state.history,
            }
            .run(attachment)?;
            Ok(true)
        })
    }
}
