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
//! The model is a Stokes I image: the parallel hands take its value and the
//! cross hands none. A channel takes the model plane `matchChannel` gives
//! it, and none off the model's spectral axis. Every row of a scan is
//! pointed at the scan's field, so a field's samples serve all its rows.
//!
//! Only autocorrelations are predicted, and flagged rows are not. CASA
//! skips a row with a positive uvw component, which leaves a cross
//! correlation with no positive component predicted as if it were one dish;
//! casa-rs predicts autocorrelations only (`docs/CASA (C++) bugs.md`).

use casa_imaging_operator::SpectralAxis;
use casa_imaging_runtime::Reservation;
use casa_imaging_singledish::{GridFunction, KernelTable};
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

    /// The kernel with the simulator's support, CASA's default.
    const fn grid_function(self) -> GridFunction {
        match self {
            Self::Pb => GridFunction::PrimaryBeam,
            Self::Box => GridFunction::BOX,
            Self::Spheroidal => GridFunction::SPHEROIDAL,
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
            + pointings.of_scan.len() * size_of::<usize>()) as u64
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
        beam: Option<&BeamPattern>,
        (windows, pointings): (&[SingleDishWindow], &SingleDishPointings),
    ) -> u64 {
        // The PB kernel's, or the larger of BOX's and SF's.
        let kernel = match beam {
            Some(beam) => GridFunction::PrimaryBeam.forming_bytes(
                beam.support_pixels(model.increment_rad[0], model.plane_frequency_hz(0)),
            ),
            None => GridFunction::SPHEROIDAL.forming_bytes(0),
        };
        let channels: usize = windows.iter().map(|w| w.frequencies_hz.len()).sum();
        let planes_of =
            windows.len() * size_of::<Vec<Option<usize>>>() + channels * size_of::<Option<usize>>();
        let per_pointing = 2 * size_of::<[f64; 2]>() + size_of::<f32>();
        kernel
            + (planes_of + pointings.directions_rad.len() * per_pointing) as u64
            + 2 * model.plane_bytes()
            + model.cache_bytes()
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
        (beam, voltage_pattern): (Option<&BeamPattern>, Option<&BeamPattern>),
        (windows, pointings): (&[SingleDishWindow], SingleDishPointings),
        reservation: Reservation,
    ) -> Result<Self, PredictError> {
        let SingleDishPointings {
            directions_rad,
            of_scan,
        } = pointings;
        let kernel = kernel.table(model, beam);
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
        let pixels: Vec<[f64; 2]> = directions_rad
            .iter()
            .map(|direction| model.pixel_of(*direction))
            .collect::<Result<_, _>>()?;
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
        let mut by_pointing = vec![0.0_f32; directions_rad.len()];
        for plane in 0..model.planes() {
            if !planes_of
                .iter()
                .flatten()
                .any(|&mapped| mapped == Some(plane))
            {
                continue;
            }
            let values = model.read_plane(plane)?;
            for (value, pixel) in by_pointing.iter_mut().zip(&pixels) {
                *value = match voltage_pattern {
                    Some(pattern) => {
                        let apparent =
                            &values * &super::beam::power_plane(pattern, model, plane, *pixel);
                        kernel.degrid(apparent.view(), *pixel)
                    }
                    None => kernel.degrid(values.view(), *pixel),
                };
            }
            for (pointing_windows, &value) in samples.iter_mut().zip(&by_pointing) {
                for ((cells, window), planes) in
                    pointing_windows.iter_mut().zip(windows).zip(&planes_of)
                {
                    let correlations = window.correlations.len();
                    for (channel, _) in planes
                        .iter()
                        .enumerate()
                        .filter(|(_, mapped)| **mapped == Some(plane))
                    {
                        let channel_cells = &mut cells[channel * correlations..][..correlations];
                        for (cell, &code) in channel_cells.iter_mut().zip(&window.correlations) {
                            // The parallel hands carry Stokes I.
                            if matches!(code, 1 | 5 | 8 | 9 | 12) {
                                *cell = Complex32::new(value, 0.0);
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
            _reservation: reservation,
        })
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
