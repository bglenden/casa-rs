// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.predict(complist=…)`: a component list's visibilities, as CASA's
//! simulator predicts them (`SkyEquation::get` with its
//! `SimpleComponentFTMachine`), analytically and with no gridding.
//!
//! For each row and component (`SimpleComponentFTMachine::get`):
//! - the row's uvw is rotated from the scan's phase centre to the
//!   component's reference direction, as `GridFT` rotates it
//!   (`rotateUVW`, [`GridFtProjection`]);
//! - the component's visibility is its flux, sampled by its spectral model
//!   at the channel's frequency and converted to the window's polarisation
//!   basis, times its shape's visibility at the rotated baseline
//!   (`SkyCompRep::visibility`, [`SkyComponent::visibility`]);
//! - with `setvp(dovp=True)`, the flux is scaled first by the primary beam's
//!   power at the component's separation from the field's pointing, at each
//!   channel's frequency (`BeamSkyJones::apply` on a component, with its
//!   full-spectral option, as `SkyEquation::applySkyJones` calls it);
//! - the rotation's phase multiplies it: `e^{−2πi s ν/c}` with `s` the
//!   phase shift of [`GridFtProjection::apply`].
//!
//! What does not depend on the row is prepared once: each component's flux
//! in each window's correlations and the beam's power, per channel; each
//! shape's transform ([`ShapeTransform`]); and, for each observed field,
//! each component's rotation (a scan is phased on its field's direction).
//! A row then takes one rotation and one phasor per component, and each
//! channel advances the phasor by the channel spacing's rotation: a
//! window's channels are evenly spaced (`setspwindow`).
//!
//! The visibilities add to what DATA holds. Unlike an image's prediction,
//! a component's is formed for flagged rows too: CASA's component machine
//! does not read the flags.
//!
//! Divergences, recorded in `docs/CASA (C++) bugs.md`:
//! - **A negative flux keeps its sign under the beam.** CASA's full-spectral
//!   beam scales each channel by the flux at that channel divided by the
//!   flux's magnitude, and so turns a negative flux positive.
//! - **The beam is taken at the data's frequencies.** CASA tabulates the
//!   beam-scaled flux at each channel's LSRK frequency and then samples that
//!   table at the data's (here TOPO) frequencies, about 1e-4 of the
//!   frequency away.
//!
//! casa-rs evaluates J2000 components in a circular or linear basis.

use casa_components::{ComponentList, Polarisation, ShapeTransform, SkyComponent};
use casa_imaging_runtime::Reservation;
use casa_ms::derived::GridFtProjection;
use casa_types::measures::direction::DirectionRef;
use num_complex::{Complex32, Complex64};

use super::BeamPattern;

/// The speed of light, m/s.
const SPEED_OF_LIGHT_M_PER_S: f64 = 299_792_458.0;

/// A window's channels and correlations, as the components are predicted
/// into them.
#[derive(Debug, Clone)]
pub(crate) struct ComponentWindow {
    /// The first channel's frequency, Hz.
    pub(crate) first_hz: f64,
    /// The channel spacing, Hz: a window's channels are evenly spaced.
    pub(crate) step_hz: f64,
    pub(crate) channels: usize,
    /// casacore `Stokes::StokesTypes` codes, one per correlation.
    pub(crate) correlations: Vec<i32>,
}

impl ComponentWindow {
    fn frequency_hz(&self, channel: usize) -> f64 {
        self.first_hz + channel as f64 * self.step_hz
    }
}

/// A failure to prepare a component list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ComponentRefusal {
    /// Something casa-rs does not evaluate yet.
    NotYetImplemented(String),
}

/// What a component list is predicted into: the session's windows, the
/// distinct pointings of its scans (`(pointing, J2000 phase centre rad)`),
/// each scan's pointing, and the primary beam with `setvp(dovp=True)`. A
/// scan is phased, and the beam centred, on its phase centre, which
/// `observemany` can move off the scan's field.
#[derive(Clone, Copy)]
pub(crate) struct ComponentTargets<'a> {
    pub(crate) windows: &'a [ComponentWindow],
    pub(crate) pointings: &'a [(usize, [f64; 2])],
    /// `[scan]`: the scan's pointing.
    pub(crate) pointing_of_scan: &'a [usize],
    pub(crate) beam: Option<&'a BeamPattern>,
}

impl ComponentTargets<'_> {
    /// Bytes a prediction of `components` components into these targets
    /// holds ([`ComponentPrediction::bytes`]), known before it is formed.
    pub(crate) fn bytes(&self, components: usize) -> u64 {
        let samples: usize = self
            .windows
            .iter()
            .map(|window| window.channels * window.correlations.len())
            .sum();
        let channels: usize = self.windows.iter().map(|window| window.channels).sum();
        let field_slots = self
            .pointings
            .iter()
            .map(|(id, _)| id + 1)
            .max()
            .unwrap_or(0);
        let per_field = components * size_of::<GridFtProjection>()
            + self.beam.map_or(0, |_| {
                self.windows.len() * size_of::<Vec<f32>>()
                    + components * channels * size_of::<f32>()
            });
        (components * (size_of::<ShapeTransform>() + samples * size_of::<Complex64>())
            + self.windows.len() * size_of::<PreparedWindow>()
            + field_slots * size_of::<Option<FieldTerms>>()
            + self.pointings.len() * per_field
            + size_of_val(self.pointing_of_scan)) as u64
    }

    /// Bytes [`ComponentPrediction::new`] holds only while it prepares
    /// `components` components: their J2000 directions.
    pub(crate) fn preparation_bytes(components: usize) -> u64 {
        (components * size_of::<[f64; 2]>()) as u64
    }
}

/// A component list prepared for prediction into a session's windows.
#[derive(Debug)]
pub(crate) struct ComponentPrediction {
    /// Each component's shape transform.
    transforms: Vec<ShapeTransform>,
    windows: Vec<PreparedWindow>,
    /// `[pointing]`: a pointing's terms, `None` for one not prepared.
    fields: Vec<Option<FieldTerms>>,
    /// `[scan]`: the scan's pointing.
    of_scan: Vec<usize>,
    /// The bytes [`Self::bytes`] counts, admitted before they were formed
    /// and held with them.
    _reservation: Reservation,
}

#[derive(Debug)]
struct PreparedWindow {
    first_hz: f64,
    step_hz: f64,
    channels: usize,
    correlations: usize,
    /// The components' fluxes in the window's correlations, Jy:
    /// `[component][channel][correlation]`.
    fluxes: Vec<Complex64>,
}

#[derive(Debug)]
struct FieldTerms {
    /// Each component's rotation from the field's direction.
    projections: Vec<GridFtProjection>,
    /// `[window]`: the beam's power `[component][channel]`, with
    /// `setvp(dovp=True)`.
    powers: Option<Vec<Vec<f32>>>,
}

impl ComponentPrediction {
    /// Prepare `list` for prediction into `targets`, holding `reservation`,
    /// its admission of [`ComponentTargets::bytes`] for the list's length.
    ///
    /// # Errors
    ///
    /// [`ComponentRefusal::NotYetImplemented`]: a component direction not in
    /// J2000, a window whose correlations are not all circular or all
    /// linear, or a flux, spectrum or shape casa-components does not
    /// evaluate.
    pub(crate) fn new(
        list: &ComponentList,
        targets: ComponentTargets<'_>,
        reservation: Reservation,
    ) -> Result<Self, ComponentRefusal> {
        let refused = ComponentRefusal::NotYetImplemented;
        let components = list.components();
        let mut directions_rad = Vec::with_capacity(components.len());
        let mut transforms = Vec::with_capacity(components.len());
        for (index, component) in components.iter().enumerate() {
            let direction = component.shape().reference_direction();
            if direction.refer() != DirectionRef::J2000 {
                return Err(refused(format!(
                    "predicting component {index} in {:?}: casa-rs predicts J2000 components",
                    direction.refer()
                )));
            }
            let (ra, dec) = direction.as_angles();
            directions_rad.push([ra, dec]);
            transforms.push(
                component
                    .shape()
                    .transform()
                    .map_err(|error| refused(format!("component {index}: {error}")))?,
            );
        }
        let windows = targets
            .windows
            .iter()
            .map(|window| prepare_window(components, window))
            .collect::<Result<Vec<_>, _>>()?;
        let slots = targets
            .pointings
            .iter()
            .map(|(id, _)| id + 1)
            .max()
            .unwrap_or(0);
        let mut fields: Vec<Option<FieldTerms>> = (0..slots).map(|_| None).collect();
        for &(field, direction) in targets.pointings {
            let projections = directions_rad
                .iter()
                .map(|component| GridFtProjection::between_j2000_directions(direction, *component))
                .collect();
            let powers = targets.beam.map(|pattern| {
                targets
                    .windows
                    .iter()
                    .map(|window| {
                        let mut powers = Vec::with_capacity(directions_rad.len() * window.channels);
                        for component in &directions_rad {
                            let separation = separation_rad(*component, direction);
                            powers.extend((0..window.channels).map(|channel| {
                                pattern.power_at(separation, window.frequency_hz(channel))
                            }));
                        }
                        powers
                    })
                    .collect()
            });
            fields[field] = Some(FieldTerms {
                projections,
                powers,
            });
        }
        Ok(Self {
            transforms,
            windows,
            fields,
            of_scan: targets.pointing_of_scan.to_vec(),
            _reservation: reservation,
        })
    }

    /// Bytes the prediction holds beside the list it was read from: its
    /// fluxes, the beam's powers, the shapes' transforms, the pointings'
    /// rotations and the scans' pointings.
    pub(crate) fn bytes(&self) -> u64 {
        let windows: usize = self
            .windows
            .iter()
            .map(|window| {
                size_of::<PreparedWindow>() + window.fluxes.len() * size_of::<Complex64>()
            })
            .sum();
        let fields: usize = self
            .fields
            .iter()
            .map(|field| {
                size_of::<Option<FieldTerms>>()
                    + field.as_ref().map_or(0, |terms| {
                        let powers = terms.powers.as_ref().map_or(0, |windows| {
                            windows
                                .iter()
                                .map(|powers| {
                                    size_of::<Vec<f32>>() + powers.len() * size_of::<f32>()
                                })
                                .sum()
                        });
                        terms.projections.len() * size_of::<GridFtProjection>() + powers
                    })
            })
            .sum();
        (self.transforms.len() * size_of::<ShapeTransform>()
            + windows
            + fields
            + self.of_scan.len() * size_of::<usize>()) as u64
    }

    /// Add to `out` the visibilities of `rows` (each row's raw uvw in
    /// metres) of scan `scan` in window `window`, at the scan's pointing.
    /// `out` holds the rows' samples back to back,
    /// `[row][channel][correlation]`.
    pub(crate) fn add_rows(
        &self,
        (scan, window): (usize, usize),
        rows: impl Iterator<Item = [f64; 3]>,
        out: &mut [Complex32],
    ) {
        let Some(terms) = self
            .of_scan
            .get(scan)
            .and_then(|&pointing| self.fields.get(pointing))
            .and_then(Option::as_ref)
        else {
            // A prediction covers the scans observed before it, whose
            // pointings it prepared.
            debug_assert!(false, "scan {scan} was not observed before the prediction");
            return;
        };
        let prepared = &self.windows[window];
        let (channels, correlations) = (prepared.channels, prepared.correlations);
        let powers = terms
            .powers
            .as_ref()
            .map(|powers| powers[window].as_slice());
        for (row, uvw_m) in rows.enumerate() {
            let samples = &mut out[row * channels * correlations..][..channels * correlations];
            for (component, (transform, projection)) in
                self.transforms.iter().zip(&terms.projections).enumerate()
            {
                let (rotated, phase_shift_m) = projection.apply(uvw_m);
                let baseline = transform.at_baseline(rotated);
                // e^{−2πi s ν/c}, advanced a channel at a time.
                let radians_per_hz =
                    -std::f64::consts::TAU * phase_shift_m / SPEED_OF_LIGHT_M_PER_S;
                let mut phasor = Complex64::from_polar(1.0, radians_per_hz * prepared.first_hz);
                let step = Complex64::from_polar(1.0, radians_per_hz * prepared.step_hz);
                let fluxes = &prepared.fluxes[component * channels * correlations..]
                    [..channels * correlations];
                let powers = powers.map(|powers| &powers[component * channels..][..channels]);
                for channel in 0..channels {
                    let frequency_hz = prepared.first_hz + channel as f64 * prepared.step_hz;
                    let mut scale = baseline.at_frequency(frequency_hz);
                    if let Some(powers) = powers {
                        scale *= f64::from(powers[channel]);
                    }
                    let value = phasor * scale;
                    let cells = channel * correlations..(channel + 1) * correlations;
                    for (cell, flux) in samples[cells.clone()].iter_mut().zip(&fluxes[cells]) {
                        let sample = flux * value;
                        *cell += Complex32::new(sample.re as f32, sample.im as f32);
                    }
                    phasor *= step;
                }
            }
        }
    }
}

/// `window`'s components' fluxes in its correlations
/// ([`PreparedWindow::fluxes`]).
fn prepare_window(
    components: &[SkyComponent],
    window: &ComponentWindow,
) -> Result<PreparedWindow, ComponentRefusal> {
    let refused = ComponentRefusal::NotYetImplemented;
    let basis = |code: i32| match code {
        5..=8 => Some((Polarisation::Circular, (code - 5) as usize)),
        9..=12 => Some((Polarisation::Linear, (code - 9) as usize)),
        _ => None,
    };
    let mapped = window
        .correlations
        .iter()
        .map(|&code| basis(code))
        .collect::<Option<Vec<_>>>()
        .filter(|mapped| mapped.windows(2).all(|pair| pair[0].0 == pair[1].0))
        .ok_or_else(|| {
            refused(format!(
                "predicting components into the correlations {:?}: casa-rs predicts them into \
                 circular or linear correlations",
                window.correlations
            ))
        })?;
    let polarisation = mapped
        .first()
        .map_or(Polarisation::Circular, |(basis, _)| *basis);
    let mut fluxes = Vec::with_capacity(components.len() * window.channels * mapped.len());
    for (index, component) in components.iter().enumerate() {
        for channel in 0..window.channels {
            let flux = component
                .flux_in(window.frequency_hz(channel), polarisation)
                .map_err(|error| refused(format!("component {index}: {error}")))?;
            fluxes.extend(mapped.iter().map(|&(_, basis)| flux[basis]));
        }
    }
    Ok(PreparedWindow {
        first_hz: window.first_hz,
        step_hz: window.step_hz,
        channels: window.channels,
        correlations: mapped.len(),
        fluxes,
    })
}

/// The great-circle separation of two J2000 directions, radians
/// (`MVDirection::separation`: twice the arcsine of half the chord).
fn separation_rad(a: [f64; 2], b: [f64; 2]) -> f64 {
    let unit = |[ra, dec]: [f64; 2]| [dec.cos() * ra.cos(), dec.cos() * ra.sin(), dec.sin()];
    let (a, b) = (unit(a), unit(b));
    let chord = ((a[0] - b[0]).powi(2) + (a[1] - b[1]).powi(2) + (a[2] - b[2]).powi(2)).sqrt();
    2.0 * (chord / 2.0).asin()
}

#[cfg(test)]
mod tests;
