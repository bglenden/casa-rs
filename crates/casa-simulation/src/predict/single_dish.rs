// SPDX-License-Identifier: LGPL-3.0-or-later
//! `setoptions(ftmachine='sd')`: total power, as CASA's simulator predicts
//! it (`SDGrid`, built by `Simulator::createSkyEquation`), by sampling the
//! model in the image plane.
//!
//! For a row pointed at pixel `(x, y)` of the model, `SDGrid::get` calls
//! `dgridsd` (`fgridsd.f`), the model summed through the kernel around the
//! pointing's nearest pixel; the visibility is the sum's conjugate.
//! `casa-imaging-singledish` owns the kernels and that sum, which the
//! single-dish gridder shares ([`KernelTable::degrid`]):
//! - `BOX`: 1 out to a pixel, reaching the nearest pixel only;
//! - `SF`: CASA's spheroid `(1 − ν²) grdsf(ν)` over 3 pixels;
//! - `PB`: the telescope's power pattern along the x axis, at the frequency
//!   of model plane 0 for every channel ([`KernelTable::primary_beam`]).
//!
//! Stokes planes become the selected receptor products through the shared
//! polarization route, and SDGrid conjugates each sampled coherency.
//! Channels select their nearest model plane; off-axis channels stay zero.
//! Fixed frames share samples at identical pointings. Local image frames
//! use each row's epoch and antenna position; saved image/squinted beams
//! retain their orientation at CASA's requested angle threshold.
//!
//! Only autocorrelations are predicted, and flagged rows are not. CASA
//! skips a row with a positive uvw component, which leaves a cross
//! correlation with no positive component predicted as if it were one dish;
//! casa-rs predicts autocorrelations only (`docs/CASA (C++) bugs.md`).

use super::image_beam::ImageBeam;
use casa_imaging_model::PolarizationCoordinate;
use casa_imaging_operator::{PolarizationRouting, SpectralAxis};
use casa_imaging_runtime::Reservation;
use casa_imaging_singledish::{GridFunction, KernelTable};
use casa_types::Correlation;
use num_complex::Complex32;

use super::{BeamPattern, PredictError, SkyModel};

/// The `SDGrid` kernel `setoptions(gridfunction=…)` names (`SDGrid::init`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SingleDishKernel {
    /// `PB`: the telescope's primary beam.
    Pb,
    /// `BOX`: the nearest pixel.
    Box,
    /// `SF`: CASA's prolate spheroid over 3 pixels.
    Spheroidal,
    /// CASA's default Gaussian kernel.
    Gauss,
    /// CASA's default Gaussian-times-jinc kernel.
    Gjinc,
}

impl SingleDishKernel {
    /// The kernel `gridfunction` names, in any case; `None` for the others
    /// (an unknown name is refused at prediction).
    #[must_use]
    pub fn from_grid_function(grid_function: &str) -> Option<Self> {
        match grid_function.to_lowercase().as_str() {
            "pb" => Some(Self::Pb),
            "box" => Some(Self::Box),
            "sf" => Some(Self::Spheroidal),
            "gauss" => Some(Self::Gauss),
            "gjinc" => Some(Self::Gjinc),
            _ => None,
        }
    }

    /// The kernel with the simulator's support, CASA's default.
    fn grid_function(self) -> GridFunction {
        match self {
            Self::Pb => GridFunction::PrimaryBeam,
            Self::Box => GridFunction::BOX,
            Self::Spheroidal => GridFunction::SPHEROIDAL,
            Self::Gauss => GridFunction::Gauss {
                hwhm: std::f64::consts::LN_2.sqrt(),
                truncate: (3. * std::f64::consts::LN_2.sqrt()) as f32,
            },
            Self::Gjinc => GridFunction::Gjinc {
                hwhm: 2.52 * std::f64::consts::LN_2.sqrt(),
                jwidth: 1.55,
                truncate: None,
            },
        }
    }

    /// The kernel's table on `model`'s pixels; `beam` is the `PB` kernel's
    /// pattern.
    fn table(self, model: &SkyModel, beam: Option<&BeamPattern>) -> KernelTable {
        match (self.grid_function(), beam) {
            (GridFunction::PrimaryBeam, Some(beam)) => {
                KernelTable::primary_beam(beam, model.increment_rad[0], model.plane_frequency_hz(0))
            }
            (GridFunction::PrimaryBeam, None) => unreachable!("a PB kernel has a beam"),
            (function, _) => KernelTable::new(function).expect("a kernel of fixed support"),
        }
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

/// Where a total-power prediction samples the model: each distinct pointing
/// direction of the scans it covers, and each scan's pointing. A scan
/// points at its phase centre, which `observemany` can move off its field
/// (the POINTING table casa-rs writes follows it).
#[derive(Debug)]
pub(crate) struct SingleDishPointings {
    /// Each distinct pointing's J2000 direction, rad.
    pub(crate) directions_rad: Vec<[f64; 2]>,
    /// `[scan]`: the scan's pointing, an index into `directions_rad`.
    pub(crate) of_scan: Vec<usize>,
    /// Epoch/antenna-dependent pixels, where a local image frame needs them.
    pub(crate) pixels: Option<Vec<[f64; 2]>>,
    /// Sorted (scan, stored time bits, antenna) to prepared sample.
    pub(crate) visits: Vec<((usize, u64, usize), usize)>,
    pub(crate) angles: Vec<f64>,
}

impl SingleDishPointings {
    /// The pointings of scans pointed at `phase_centres_rad`, in scan order.
    pub(crate) fn of_scans(phase_centres_rad: impl ExactSizeIterator<Item = [f64; 2]>) -> Self {
        let mut directions_rad: Vec<[f64; 2]> = Vec::new();
        let of_scan = phase_centres_rad
            .map(|centre| {
                let same = |known: &[f64; 2]| known.map(f64::to_bits) == centre.map(f64::to_bits);
                directions_rad.iter().position(same).unwrap_or_else(|| {
                    directions_rad.push(centre);
                    directions_rad.len() - 1
                })
            })
            .collect();
        Self {
            directions_rad,
            of_scan,
            pixels: None,
            visits: Vec::new(),
            angles: Vec::new(),
        }
    }
}

/// A model prepared for total-power prediction: each pointing's samples in
/// each window.
#[derive(Debug)]
pub(crate) struct SingleDishPrediction {
    /// `[pointing][window]`: `[channel][correlation]`.
    samples: Vec<Vec<Vec<Complex32>>>,
    /// `[scan]`: the scan's pointing.
    of_scan: Vec<usize>,
    visits: Vec<((usize, u64, usize), usize)>,
    /// The bytes [`Self::bytes`] counts.
    _reservation: Reservation,
}

impl SingleDishPrediction {
    /// Bytes a prediction of `windows` at `pointings` holds, known before it
    /// is formed: the samples and the scans' pointings.
    pub(crate) fn bytes(windows: &[SingleDishWindow], pointings: &SingleDishPointings) -> u64 {
        let per_pointing: usize = size_of::<Vec<Vec<Complex32>>>()
            + windows
                .iter()
                .map(|window| {
                    size_of::<Vec<Complex32>>()
                        + window.frequencies_hz.len()
                            * window.correlations.len()
                            * size_of::<Complex32>()
                })
                .sum::<usize>();
        (pointings.directions_rad.len() * per_pointing
            + pointings.of_scan.len() * size_of::<usize>()
            + pointings.visits.capacity() * size_of::<((usize, u64, usize), usize)>())
            as u64
    }

    /// Bytes preparing the prediction holds beside it:
    /// - the kernel, as tabulated and as kept (`beam` is the `PB` kernel's
    ///   pattern, `None` for the others);
    /// - a model plane read, with the beam's power plane and their product
    ///   when `setvp(dovp=True)` applies it first, and the image's tile
    ///   cache ([`SkyModel::cache_bytes`]);
    /// - each window channel's plane, and each pointing's direction, pixel
    ///   and sample of the plane read.
    pub(crate) fn preparation_bytes(
        model: &SkyModel,
        kind: SingleDishKernel,
        transformed: bool,
        beam: Option<&BeamPattern>,
        (windows, pointings): (&[SingleDishWindow], &SingleDishPointings),
    ) -> u64 {
        // The PB kernel's extent, or the selected fixed kernel's support.
        let kernel = match beam {
            Some(beam) => GridFunction::PrimaryBeam.forming_bytes(
                beam.support_pixels(model.increment_rad[0], model.plane_frequency_hz(0)),
            ),
            None => kind.grid_function().forming_bytes(0),
        };
        let channels: usize = windows.iter().map(|w| w.frequencies_hz.len()).sum();
        let planes_of =
            windows.len() * size_of::<Vec<Option<usize>>>() + channels * size_of::<Option<usize>>();
        let per_pointing = 2 * size_of::<[f64; 2]>() + size_of::<f32>() + size_of::<f64>();
        kernel
            + (planes_of + pointings.directions_rad.len() * per_pointing) as u64
            + if transformed { 4 } else { 2 } * model.plane_bytes()
            + model.cache_bytes()
            + windows.len() as u64 * 4096 // At most four products and Stokes coordinates per route.
    }

    /// Prepare `model` with `kernel` for `windows` at `pointings`, holding
    /// `reservation`, its admission of [`Self::bytes`]. `beams` are the
    /// telescope's pattern, which the `PB` kernel needs, and the pattern
    /// `setvp(dovp=True)` applies to the model first, centred on each
    /// pointing, as CASA's `SkyEquation` does whatever the kernel (so a
    /// `PB` kernel then weights the model by the power pattern twice).
    ///
    /// # Errors
    ///
    /// The model's pixels could not be read, or a pointing could not be
    /// placed on them.
    pub(crate) fn new(
        model: &SkyModel,
        kernel: SingleDishKernel,
        (beam, voltage_pattern, image_beam): (
            Option<&BeamPattern>,
            Option<&BeamPattern>,
            Option<&ImageBeam>,
        ),
        (windows, pointings): (&[SingleDishWindow], SingleDishPointings),
        reservation: Reservation,
    ) -> Result<Self, PredictError> {
        let SingleDishPointings {
            directions_rad,
            of_scan,
            pixels: supplied_pixels,
            visits,
            angles,
        } = pointings;
        let kernel = kernel.table(model, beam);
        if model.shape().iter().any(|&n| n <= 2 * kernel.support()) {
            return Err(PredictError::InvalidModel(format!(
                "number of image pixels {:?} is smaller than or equal to gridding support {}; use a larger image",
                model.shape(),
                2 * kernel.support()
            )));
        }
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
        let pixels = match supplied_pixels {
            Some(pixels) => pixels,
            None => directions_rad
                .iter()
                .map(|direction| model.pixel_of(*direction))
                .collect::<Result<_, _>>()?,
        };
        // The samples, zero until a plane fills them. Sized exactly: they
        // are kept, and charged by their length.
        let mut samples: Vec<Vec<Vec<Complex32>>> = directions_rad
            .iter()
            .map(|_| {
                windows
                    .iter()
                    .map(|window| {
                        vec![
                            Complex32::new(0.0, 0.0);
                            window.frequencies_hz.len() * window.correlations.len()
                        ]
                    })
                    .collect()
            })
            .collect();
        // One plane read at a time, each pointing's sample of it written
        // into the channels it feeds, so no plane × pointing matrix is held
        // (#874 review).
        let full_stokes = [
            PolarizationCoordinate::StokesI,
            PolarizationCoordinate::StokesQ,
            PolarizationCoordinate::StokesU,
            PolarizationCoordinate::StokesV,
        ];
        let stokes = if image_beam.is_some() {
            &full_stokes[..]
        } else {
            model.polarizations()
        };
        let routing = windows
            .iter()
            .map(|window| {
                let correlations = window
                    .correlations
                    .iter()
                    .map(|&code| {
                        Correlation::from_casacore_code(code)
                            .expect("validated simulator correlation")
                    })
                    .collect::<Vec<_>>();
                PolarizationRouting::compile(&correlations, stokes)
            })
            .collect::<Result<Vec<_>, _>>()?;
        for plane in 0..model.planes() {
            if !planes_of
                .iter()
                .flatten()
                .any(|&mapped| mapped == Some(plane))
            {
                continue;
            }
            let sky = model.read_polarizations(plane)?;
            for (pointing, (&direction, &pixel)) in directions_rad.iter().zip(&pixels).enumerate() {
                let apparent;
                let values = match image_beam {
                    Some(beam) => {
                        apparent = beam.apply_plane(
                            model,
                            plane,
                            sky.clone(),
                            direction,
                            angles[pointing],
                        )?;
                        &apparent
                    }
                    None => &sky,
                };
                let mut sampled = [0_f32; 4];
                for (sample, values) in sampled.iter_mut().zip(values) {
                    *sample = match voltage_pattern {
                        Some(pattern) => {
                            let apparent =
                                values * &super::beam::power_plane(pattern, model, plane, pixel);
                            kernel.degrid(apparent.view(), pixel)
                        }
                        None => kernel.degrid(values.view(), pixel),
                    };
                }
                for (((cells, window), planes), route) in samples[pointing]
                    .iter_mut()
                    .zip(windows)
                    .zip(&planes_of)
                    .zip(&routing)
                {
                    let correlations = window.correlations.len();
                    for (channel, _) in planes
                        .iter()
                        .enumerate()
                        .filter(|(_, mapped)| **mapped == Some(plane))
                    {
                        for (cell, &gpol) in cells[channel * correlations..][..correlations]
                            .iter_mut()
                            .zip(route.pol_map())
                        {
                            if let Some(gpol) = gpol {
                                for (polarization, &value) in
                                    sampled.iter().take(stokes.len()).enumerate()
                                {
                                    // dgridsd conjugates the sampled complex model.
                                    let coefficient = route
                                        .from_requested(usize::from(gpol), polarization)
                                        .conj();
                                    *cell += Complex32::new(
                                        coefficient.re as f32,
                                        coefficient.im as f32,
                                    ) * value;
                                }
                            }
                        }
                    }
                }
            }
        }
        tracing::debug!(
            support = kernel.support(),
            bytes = kernel.bytes(),
            "single-dish kernel"
        );
        Ok(Self {
            samples,
            of_scan,
            visits,
            _reservation: reservation,
        })
    }

    pub(crate) fn needs_epoch(&self) -> bool {
        !self.visits.is_empty()
    }

    pub(crate) fn samples_at(
        &self,
        scan: usize,
        window: usize,
        time: f64,
        antenna: usize,
    ) -> &[Complex32] {
        if self.visits.is_empty() {
            return self.samples(scan, window);
        }
        self.visits
            .binary_search_by_key(&(scan, time.to_bits(), antenna), |v| v.0)
            .ok()
            .map_or(&[], |index| &self.samples[self.visits[index].1][window])
    }

    /// The samples of scan `scan`'s pointing in window `window`,
    /// `[channel][correlation]`; empty for a scan not prepared.
    pub(crate) fn samples(&self, scan: usize, window: usize) -> &[Complex32] {
        self.of_scan
            .get(scan)
            .map_or(&[], |&pointing| self.samples[pointing][window].as_slice())
    }
}

#[cfg(test)]
mod tests;
