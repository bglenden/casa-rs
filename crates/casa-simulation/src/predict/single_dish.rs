// SPDX-License-Identifier: LGPL-3.0-or-later
//! `setoptions(ftmachine='sd')`: total power, as CASA's simulator predicts
//! it (`SDGrid`, built by `Simulator::createSkyEquation`), by sampling the
//! model in the image plane.
//!
//! For a row pointed at pixel `(x, y)` of the model, `SDGrid::get` calls
//! `dgridsd` (`fgridsd.f`):
//! - the pointing's nearest pixel is `loc = nint(x + 1)` (one-based) and its
//!   offset from it, in hundredths of a pixel, `off = nint((loc − x − 1) ·
//!   100)`; a pointing whose nearest pixel is off the image predicts zero;
//! - each pixel within `support` pixels of `loc` along both axes adds the
//!   model's value times the radial kernel at `floor(√(rx² + ry²))`
//!   hundredths, with `r = 100 i + off`, summed in single precision;
//! - the visibility is the sum's conjugate.
//!
//! The kernels (`SDGrid::init`, `findPBAsConvFunction`) are tabulated at a
//! hundredth of a pixel:
//! - `BOX`: 1 out to half a pixel, support 0;
//! - `SF`: CASA's spheroid `(1 − ν²) grdsf(ν)` over 3 pixels;
//! - `PB`: the telescope's power pattern along the x axis, at the frequency
//!   of model plane 0 for every channel, tabulated over `max(128, support)`
//!   pixels (`PBMath1D::support`), cut at its first value below 1e-3, of
//!   which CASA keeps one fewer (`Slice(0, cfLen − 1)`); the support is the
//!   kept length in whole pixels.
//!
//! The model is a Stokes I image: the parallel hands take its value and the
//! cross hands none. A channel takes the model plane `matchChannel` gives
//! it, and none off the model's spectral axis. Every row of a scan is
//! pointed at the scan's field, so a field's samples serve all its rows.
//!
//! Only autocorrelations are predicted, and flagged rows are not. CASA
//! skips a row with a positive uvw component, which leaves a cross
//! correlation with no positive component predicted as if it were one dish;
//! casa-rs predicts autocorrelations only (`docs/CASA (C++) bugs.md`).

use casa_imaging_operator::{SpectralAxis, grdsf};
use casa_imaging_runtime::Reservation;
use num_complex::Complex32;

use super::{BeamPattern, PredictError, SkyModel};

/// Samples per pixel of every `SDGrid` kernel (`convSampling`).
const SAMPLING: usize = 100;

/// `findPBAsConvFunction`: the PB kernel ends before its first sample
/// below this.
const PB_CUT: f32 = 1.0e-3;

/// `findPBAsConvFunction`: the PB kernel is tabulated over at least this
/// many pixels.
const PB_MINIMUM_PIXELS: usize = 128;

/// The `SDGrid` kernel `setoptions(gridfunction=…)` names (`SDGrid::init`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SingleDishKernel {
    /// `PB`: the telescope's primary beam.
    Pb,
    /// `BOX`: the nearest pixel.
    Box,
    /// `SF`: CASA's prolate spheroid over 3 pixels.
    Spheroidal,
}

impl SingleDishKernel {
    /// The kernel `gridfunction` names, in any case; `None` for the others
    /// (`GAUSS` and `GJINC`, which CASA also has, among them).
    #[must_use]
    pub fn from_grid_function(grid_function: &str) -> Option<Self> {
        match grid_function.to_lowercase().as_str() {
            "pb" => Some(Self::Pb),
            "box" => Some(Self::Box),
            "sf" => Some(Self::Spheroidal),
            _ => None,
        }
    }
}

/// A radial kernel at a hundredth of a pixel (`convFunc`) and its support
/// in pixels.
#[derive(Debug)]
struct Kernel {
    table: Vec<f32>,
    support: usize,
}

impl Kernel {
    fn new(kind: SingleDishKernel, model: &SkyModel, beam: Option<&BeamPattern>) -> Self {
        let tabulated = |support: usize, values: Vec<f32>| {
            let mut table = vec![0.0; SAMPLING * (2 * support + 2)];
            table[..values.len()].copy_from_slice(&values);
            Self { table, support }
        };
        match kind {
            SingleDishKernel::Box => tabulated(0, vec![1.0; SAMPLING]),
            SingleDishKernel::Spheroidal => {
                let support = 3;
                let samples = SAMPLING * support;
                tabulated(
                    support,
                    (0..samples)
                        .map(|i| {
                            let nu = i as f64 / samples as f64;
                            ((1.0 - nu * nu) * grdsf(nu)) as f32
                        })
                        .collect(),
                )
            }
            SingleDishKernel::Pb => {
                let beam = beam.expect("a PB kernel has a beam");
                let frequency_hz = model.plane_frequency_hz(0);
                let increment = model.increment_rad[0] * (1.0 / SAMPLING as f64);
                let pixels = PB_MINIMUM_PIXELS
                    .max(beam.support_pixels(model.increment_rad[0], frequency_hz));
                let mut values: Vec<f32> = (0..SAMPLING * pixels)
                    .map(|i| beam.power_on_axis(i as f64 * increment, frequency_hz))
                    .take_while(|&power| power >= PB_CUT)
                    .collect();
                // CASA keeps `Slice(0, cfLen − 1)` of the `cfLen` samples
                // before the cut, and at least one counted.
                let kept = values.len().max(1) - 1;
                values.truncate(kept);
                tabulated(values.len() / SAMPLING, values)
            }
        }
    }

    fn bytes(&self) -> u64 {
        (self.table.len() * size_of::<f32>()) as u64
    }
}

/// A window the model is predicted into: its channels and correlations.
#[derive(Debug, Clone)]
pub(crate) struct SingleDishWindow {
    /// Channel frequencies, Hz.
    pub(crate) frequencies_hz: Vec<f64>,
    /// casacore `Stokes::StokesTypes` codes, one per correlation.
    pub(crate) correlations: Vec<i32>,
}

/// A model prepared for total-power prediction: each observed field's
/// samples in each window.
#[derive(Debug)]
pub(crate) struct SingleDishPrediction {
    /// `[field][window]`: `[channel][correlation]`, or `None` for a field
    /// not observed.
    samples: Vec<Option<Vec<Vec<Complex32>>>>,
    /// The bytes [`Self::bytes`] counts.
    _reservation: Reservation,
}

impl SingleDishPrediction {
    /// Bytes a prediction of `windows` for `fields` observed fields holds,
    /// known before it is formed.
    pub(crate) fn bytes(windows: &[SingleDishWindow], fields: &[(usize, [f64; 2])]) -> u64 {
        let slots = fields.iter().map(|(id, _)| id + 1).max().unwrap_or(0);
        let per_field: usize = windows
            .iter()
            .map(|window| {
                size_of::<Vec<Complex32>>()
                    + window.frequencies_hz.len()
                        * window.correlations.len()
                        * size_of::<Complex32>()
            })
            .sum();
        (slots * size_of::<Option<Vec<Vec<Complex32>>>>() + fields.len() * per_field) as u64
    }

    /// Bytes preparing the prediction holds beside it: the kernel, as
    /// tabulated and as kept (`beam` is the `PB` kernel's pattern, `None`
    /// for the others), and a model plane read, with the beam's power plane
    /// and their product when `setvp(dovp=True)` applies it first.
    pub(crate) fn preparation_bytes(model: &SkyModel, beam: Option<&BeamPattern>) -> u64 {
        let pixels = beam.map_or(3, |beam| {
            PB_MINIMUM_PIXELS
                .max(beam.support_pixels(model.increment_rad[0], model.plane_frequency_hz(0)))
        });
        let kernel = SAMPLING * pixels + SAMPLING * (2 * pixels + 2);
        (kernel * size_of::<f32>()) as u64 + 2 * model.plane_bytes()
    }

    /// Prepare `model` with `kernel` for `windows` and each of `fields`
    /// (`(field id, J2000 pointing rad)`), holding `reservation`, its
    /// admission of [`Self::bytes`]. `beams` are the telescope's pattern,
    /// which the `PB` kernel needs, and the pattern `setvp(dovp=True)`
    /// applies to the model first, centred on each pointing, as CASA's
    /// `SkyEquation` does whatever the kernel (so a `PB` kernel then weights
    /// the model by the power pattern twice).
    ///
    /// # Errors
    ///
    /// The model's pixels could not be read, or a pointing could not be
    /// placed on them.
    pub(crate) fn new(
        model: &SkyModel,
        kernel: SingleDishKernel,
        (beam, voltage_pattern): (Option<&BeamPattern>, Option<&BeamPattern>),
        (windows, fields): (&[SingleDishWindow], &[(usize, [f64; 2])]),
        reservation: Reservation,
    ) -> Result<Self, PredictError> {
        let kernel = Kernel::new(kernel, model, beam);
        let axis = SpectralAxis::new(
            model.first_frequency_hz,
            model.frequency_increment_hz,
            u32::try_from(model.planes())
                .map_err(|_| PredictError::UnsupportedModel("too many model planes".into()))?,
        )?;
        // Each window channel's model plane (`matchChannel`), or none.
        let planes_of: Vec<Vec<Option<usize>>> = windows
            .iter()
            .map(|window| {
                window
                    .frequencies_hz
                    .iter()
                    .map(|&frequency| axis.nearest_channel(frequency).map(|plane| plane as usize))
                    .collect()
            })
            .collect();
        let pixels: Vec<[f64; 2]> = fields
            .iter()
            .map(|(_, direction)| model.pixel_of(*direction))
            .collect::<Result<_, _>>()?;
        // `[plane][field index]`: the sampled model, one plane read at a
        // time.
        let mut sampled = vec![vec![0.0_f32; fields.len()]; model.planes()];
        for (plane, by_field) in sampled.iter_mut().enumerate() {
            if !planes_of
                .iter()
                .flatten()
                .any(|&mapped| mapped == Some(plane))
            {
                continue;
            }
            let values = model.read_plane(plane)?;
            for (value, pixel) in by_field.iter_mut().zip(&pixels) {
                *value = match voltage_pattern {
                    Some(pattern) => {
                        let apparent = &values * &pattern.power_plane(model, plane, *pixel);
                        sample(&apparent, *pixel, &kernel)
                    }
                    None => sample(&values, *pixel, &kernel),
                };
            }
        }
        let slots = fields.iter().map(|(id, _)| id + 1).max().unwrap_or(0);
        let mut samples: Vec<Option<Vec<Vec<Complex32>>>> = (0..slots).map(|_| None).collect();
        for (index, &(field, _)) in fields.iter().enumerate() {
            samples[field] = Some(
                windows
                    .iter()
                    .zip(&planes_of)
                    .map(|(window, planes)| {
                        // Sized exactly: it is kept, and charged by its length.
                        let mut cells =
                            Vec::with_capacity(planes.len() * window.correlations.len());
                        for plane in planes {
                            let value = plane.map_or(0.0, |plane| sampled[plane][index]);
                            cells.extend(window.correlations.iter().map(|&code| {
                                // The parallel hands carry Stokes I.
                                let parallel = matches!(code, 1 | 5 | 8 | 9 | 12);
                                Complex32::new(if parallel { value } else { 0.0 }, 0.0)
                            }));
                        }
                        cells
                    })
                    .collect(),
            );
        }
        tracing::debug!(
            support = kernel.support,
            bytes = kernel.bytes(),
            "single-dish kernel"
        );
        Ok(Self {
            samples,
            _reservation: reservation,
        })
    }

    /// The samples of field `field` in window `window`,
    /// `[channel][correlation]`; empty for a field not prepared.
    pub(crate) fn samples(&self, field: usize, window: usize) -> &[Complex32] {
        self.samples
            .get(field)
            .and_then(Option::as_ref)
            .map_or(&[], |windows| windows[window].as_slice())
    }
}

/// `dgridsd` for one pointing at fractional pixel `pixel` of `plane`
/// (`[y][x]`): the real part of its conjugated sum.
fn sample(plane: &ndarray::Array2<f32>, pixel: [f64; 2], kernel: &Kernel) -> f32 {
    let (ny, nx) = plane.dim();
    // `sgridsd`, in single precision and one-based.
    let position = pixel.map(|value| (value + 1.0) as f32);
    let location = position.map(|value| value.round() as i64);
    let offset = [0, 1]
        .map(|axis| ((location[axis] as f32 - position[axis]) * SAMPLING as f32).round() as i64);
    // `ogridsd` with no support: the nearest pixel is on the image.
    let on_image = |value: i64, size: usize| value >= 1 && value <= size as i64;
    if !on_image(location[0], nx) || !on_image(location[1], ny) {
        return 0.0;
    }
    let support = kernel.support as i64;
    let mut sum = 0.0_f32;
    for iy in -support..=support {
        let y = location[1] + iy;
        if !on_image(y, ny) {
            continue;
        }
        let ry = (SAMPLING as i64 * iy + offset[1]) as f32;
        for ix in -support..=support {
            let x = location[0] + ix;
            if !on_image(x, nx) {
                continue;
            }
            let rx = (SAMPLING as i64 * ix + offset[0]) as f32;
            let radius = (rx * rx + ry * ry).sqrt() as usize;
            let weight = kernel.table.get(radius).copied().unwrap_or(0.0);
            sum += weight * plane[[(y - 1) as usize, (x - 1) as usize]];
        }
    }
    sum
}

#[cfg(test)]
mod tests;
