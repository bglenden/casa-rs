// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.predict(complist=…)`: a component list's visibilities, as CASA's
//! simulator predicts them (`SkyEquation::get` with its
//! `SimpleComponentFTMachine`), analytically and with no gridding.
//!
//! For each row and component (`SimpleComponentFTMachine::get`):
//! - the row's uvw is rotated from the scan's phase centre to the
//!   component's reference direction through `ComponentFTMachine`'s
//!   projected [`UvwMachine`] at the integration epoch and antenna 0;
//! - the component's visibility is its flux, sampled by its spectral model
//!   at the channel's frequency and converted to the window's polarisation
//!   basis, times its shape's visibility at the rotated baseline
//!   (`SkyCompRep::visibility`, [`SkyComponent::visibility`]);
//! - with `setvp(dovp=True)`, the flux is scaled first by the primary beam's
//!   power at the component's separation from the field's pointing, at each
//!   channel's frequency (`BeamSkyJones::apply` on a component, with its
//!   full-spectral option, as `SkyEquation::applySkyJones` calls it);
//! - the rotation's phase multiplies it: `e^{−2πi s ν/c}` with `s` the
//!   phase shift of [`UvwMachine::convert_projected`], in CASA's sign.
//!
//! What does not depend on the row is prepared once: each component's flux
//! in each window's correlations and the beam's power, per channel; each
//! shape's transform ([`ShapeTransform`]); and, for each observed field,
//! each component's rotation (a scan is phased on its field's direction).
//! Native-frame transforms are rebuilt once per integration, as CASA's
//! component machine does, using the first antenna's position.
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
//! Native direction references retain their axes when evaluating shapes.

use casa_components::{ComponentList, Polarisation, ShapeTransform, SkyComponent};
use casa_imaging_runtime::{Admitted, Reservation};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{EpochRef, MEpoch, MeasFrame, UvwMachine};
use num_complex::{Complex32, Complex64};
use std::sync::Arc;

use super::{BeamPattern, PrimaryBeam};

mod squint;
use squint::Squint;
pub(crate) use squint::{BeamContext, Config as ComponentBeamConfig};

/// The speed of light, m/s.
const SPEED_OF_LIGHT_M_PER_S: f64 = 299_792_458.0;

/// A window's channels and correlations, as the components are predicted
/// into them.
#[derive(Debug, Clone)]
pub(crate) struct ComponentWindow {
    pub(crate) stored_frequencies: Option<Arc<Admitted<Vec<f64>>>>,
    /// The first channel's frequency, Hz.
    pub(crate) first_hz: f64,
    /// The channel spacing, Hz, for the generated linear axis.
    pub(crate) step_hz: f64,
    pub(crate) channels: usize,
    /// casacore `Stokes::StokesTypes` codes, one per correlation.
    pub(crate) correlations: Vec<i32>,
}

impl ComponentWindow {
    fn frequency_hz(&self, channel: usize) -> f64 {
        self.stored_frequencies.as_ref().map_or_else(
            || self.first_hz + channel as f64 * self.step_hz,
            |frequencies| frequencies.value[channel],
        )
    }
}

/// A failure to prepare a component list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ComponentRefusal {
    /// Something casa-rs does not evaluate yet.
    NotYetImplemented(String),
}

/// What a component list is predicted into: the session's windows, the
/// distinct pairs of visibility phase centre and antenna beam centre,
/// each scan's pair, and the primary beam with `setvp(dovp=True)`.
/// FIELD determines phase; stored POINTING independently determines beam
/// attenuation in a resumed simulation.
#[derive(Clone)]
pub(crate) struct ComponentTargets<'a> {
    pub(crate) windows: &'a [ComponentWindow],
    /// Slot, visibility phase centre, and antenna beam centre, J2000 rad.
    pub(crate) pointings: &'a [(usize, [f64; 2], [f64; 2])],
    /// `[scan]`: the scan's pointing.
    pub(crate) pointing_of_scan: &'a [usize],
    pub(crate) beam: Option<&'a BeamPattern>,
    pub(crate) frame: &'a MeasFrame,
    pub(crate) squint: Option<squint::Config>,
    pub(crate) workers: usize,
}

impl ComponentTargets<'_> {
    /// Bytes a prediction of `components` components into these targets
    /// holds ([`ComponentPrediction::bytes`]), known before it is formed.
    pub(crate) fn bytes(&self, components: usize) -> u64 {
        let samples: usize = self
            .windows
            .iter()
            .map(|window| {
                window.channels
                    * if self.squint.is_some() {
                        4
                    } else {
                        window.correlations.len()
                    }
            })
            .sum();
        let channels: usize = self.windows.iter().map(|window| window.channels).sum();
        let field_slots = self
            .pointings
            .iter()
            .map(|(id, _, _)| id + 1)
            .max()
            .unwrap_or(0);
        let per_field = components * size_of::<UvwMachine>()
            + self.beam.filter(|_| self.squint.is_none()).map_or(0, |_| {
                self.windows.len() * size_of::<Vec<f32>>()
                    + components * channels * size_of::<f32>()
            });
        (components
            * (size_of::<ShapeTransform>()
                + size_of::<MDirection>()
                + samples * size_of::<Complex64>())
            + self.windows.len() * size_of::<PreparedWindow>()
            + field_slots * size_of::<Option<FieldTerms>>()
            + self.pointings.len() * per_field
            + size_of_val(self.pointing_of_scan)) as u64
            + self.squint.as_ref().map_or(0, |config| {
                Squint::bytes(
                    self.workers,
                    self.windows
                        .iter()
                        .map(|window| window.channels)
                        .max()
                        .unwrap_or(0),
                    config.radial(),
                )
            })
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
    directions: Vec<MDirection>,
    frame: MeasFrame,
    squint: Option<Box<Squint>>,
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
    stored_frequencies: Option<Arc<Admitted<Vec<f64>>>>,
    first_hz: f64,
    step_hz: f64,
    channels: usize,
    correlations: usize,
    basis: Polarisation,
    products: [usize; 4],
    /// The components' fluxes in the window's correlations, Jy:
    /// `[component][channel][correlation]`.
    fluxes: Vec<Complex64>,
}

#[derive(Debug)]
struct FieldTerms {
    phase: MDirection,
    beam_centre: [f64; 2],
    /// Each component's rotation from the field's direction.
    projections: Vec<UvwMachine>,
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
    /// [`ComponentRefusal::NotYetImplemented`]: a time-dependent component
    /// beam needing separate observer context, correlations not all circular or all
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
            if (targets.beam.is_some() || targets.squint.is_some())
                && !matches!(
                    direction.refer(),
                    DirectionRef::J2000
                        | DirectionRef::B1950
                        | DirectionRef::B1950Vla
                        | DirectionRef::GALACTIC
                        | DirectionRef::ICRS
                        | DirectionRef::SUPERGAL
                        | DirectionRef::ECLIPTIC
                )
            {
                return Err(refused(format!(
                    "component {index} in {:?} needs its beam's actual array observer context (#1297)",
                    direction.refer()
                )));
            }
            let (ra, dec) = direction
                .convert_to(DirectionRef::J2000, targets.frame)
                .map_err(|error| refused(error.to_string()))?
                .as_angles();
            directions_rad.push([ra, dec]);
            transforms.push(
                component
                    .transform()
                    .map_err(|error| refused(format!("component {index}: {error}")))?,
            );
        }
        let windows = targets
            .windows
            .iter()
            .map(|window| prepare_window(components, window, targets.squint.is_some()))
            .collect::<Result<Vec<_>, _>>()?;
        let directions: Vec<MDirection> = components
            .iter()
            .map(|component| component.shape().reference_direction().clone())
            .collect();
        let slots = targets
            .pointings
            .iter()
            .map(|(id, _, _)| id + 1)
            .max()
            .unwrap_or(0);
        let mut fields: Vec<Option<FieldTerms>> = (0..slots).map(|_| None).collect();
        for &(field, direction, beam_centre) in targets.pointings {
            let phase = MDirection::from_angles(direction[0], direction[1], DirectionRef::J2000);
            let projections = directions
                .iter()
                .map(|component| {
                    UvwMachine::with_frame(component, &phase, targets.frame)
                        .map_err(|error| refused(error.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let powers = targets
                .beam
                .filter(|_| targets.squint.is_none())
                .map(|pattern| {
                    targets
                        .windows
                        .iter()
                        .map(|window| {
                            let mut powers =
                                Vec::with_capacity(directions_rad.len() * window.channels);
                            for component in &directions_rad {
                                let separation = separation_rad(*component, beam_centre);
                                powers.extend((0..window.channels).map(|channel| {
                                    pattern.power_at(separation, window.frequency_hz(channel))
                                }));
                            }
                            powers
                        })
                        .collect()
                });
            fields[field] = Some(FieldTerms {
                phase,
                beam_centre,
                projections,
                powers,
            });
        }
        Ok(Self {
            directions,
            frame: targets.frame.clone(),
            squint: targets.squint.map(|config| {
                Box::new(Squint::new(
                    config,
                    targets.beam,
                    targets.workers,
                    targets
                        .windows
                        .iter()
                        .map(|window| window.channels)
                        .max()
                        .unwrap_or(0),
                ))
            }),
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
                        terms.projections.len() * size_of::<UvwMachine>() + powers
                    })
            })
            .sum();
        (self.transforms.len() * size_of::<ShapeTransform>()
            + self.directions.len() * size_of::<MDirection>()
            + windows
            + fields
            + self.of_scan.len() * size_of::<usize>()) as u64
            + self
                .squint
                .as_ref()
                .map_or(0, |squint| squint.resident_bytes())
    }

    pub(crate) fn needs_epoch(&self) -> bool {
        self.squint.is_some()
            || self
                .directions
                .iter()
                .any(|direction| direction.refer() != DirectionRef::J2000)
    }

    pub(crate) fn covers_scan(&self, scan: usize) -> bool {
        scan < self.of_scan.len()
    }

    pub(crate) fn has_squint(&self) -> bool {
        self.squint.is_some()
    }

    pub(crate) fn prepare_beam_job(
        &self,
        scan: usize,
        field: usize,
        times: impl Iterator<Item = f64>,
    ) -> Result<Option<BeamContext>, casa_components::ComponentError> {
        self.squint
            .as_ref()
            .map(|squint| {
                let pointing = self.of_scan[scan];
                let terms = self.fields[pointing].as_ref().expect("prepared pointing");
                squint.prepare_job((field, pointing), times, &terms.phase, &self.frame)
            })
            .transpose()
    }

    /// Add to `out` the visibilities of `rows` (each row's raw uvw in
    /// metres) of scan `scan` with DATA_DESCRIPTION `description`, at the
    /// scan's pointing. A description supplies both frequencies and products.
    /// `out` holds the rows' samples back to back,
    /// `[row][channel][correlation]`.
    pub(crate) fn add_rows(
        &self,
        (scan, description): (usize, usize),
        rows: impl Iterator<Item = ([f64; 3], f64)> + Clone,
        out: &mut [Complex32],
        context: Option<BeamContext>,
        worker: usize,
    ) -> Result<(), casa_components::ComponentError> {
        let Some(terms) = self
            .of_scan
            .get(scan)
            .and_then(|&pointing| self.fields.get(pointing))
            .and_then(Option::as_ref)
        else {
            // A prediction covers the scans observed before it, whose
            // pointings it prepared.
            debug_assert!(false, "scan {scan} was not observed before the prediction");
            return Ok(());
        };
        let prepared = &self.windows[description];
        let (channels, correlations) = (prepared.channels, prepared.correlations);
        let powers = terms
            .powers
            .as_ref()
            .map(|powers| powers[description].as_slice());
        let mut workspace = self
            .squint
            .as_ref()
            .map(|squint| squint.workspace(worker))
            .transpose()?;
        for (component, (transform, initial_projection)) in
            self.transforms.iter().zip(&terms.projections).enumerate()
        {
            let direction = &self.directions[component];
            let physical = if self.squint.is_some() {
                let (ra, dec) = direction
                    .convert_to(DirectionRef::J2000, &self.frame)
                    .map_err(|error| casa_components::ComponentError::Visibility {
                        shape: "component beam",
                        reason: error.to_string(),
                    })?
                    .as_angles();
                [ra, dec]
            } else {
                [0.; 2]
            };
            let mut beam_context = context;
            let mut flux_angle = None;
            let mut projection = *initial_projection;
            let mut epoch = None;
            for (row, (uvw_m, time_s)) in rows.clone().enumerate() {
                let angle = match (&self.squint, &mut beam_context) {
                    (Some(squint), Some(context)) => context.advance(
                        time_s,
                        &terms.phase,
                        &self.frame,
                        squint.config.increment,
                        squint.config.alt_az,
                    )?,
                    _ => 0.,
                };
                if direction.refer() != DirectionRef::J2000 && epoch != Some(time_s) {
                    let current = self
                        .frame
                        .clone()
                        .with_epoch(MEpoch::from_mjd(time_s / 86400., EpochRef::UTC));
                    projection = UvwMachine::with_frame(direction, &terms.phase, &current)
                        .map_err(|error| casa_components::ComponentError::Visibility {
                            shape: "component direction",
                            reason: error.to_string(),
                        })?;
                    epoch = Some(time_s);
                }
                let samples = &mut out[row * channels * correlations..][..channels * correlations];
                let (rotated, phase) =
                    projection.convert_projected([-uvw_m[0], -uvw_m[1], uvw_m[2]]);
                let rotated = [-rotated[0], -rotated[1], rotated[2]];
                let phase_shift_m = -phase;
                let baseline = transform.at_baseline(rotated);
                // e^{−2πi s ν/c}, advanced a channel at a time.
                let radians_per_hz =
                    -std::f64::consts::TAU * phase_shift_m / SPEED_OF_LIGHT_M_PER_S;
                let mut phasor = Complex64::from_polar(1.0, radians_per_hz * prepared.first_hz);
                let step = Complex64::from_polar(1.0, radians_per_hz * prepared.step_hz);
                let stored = if self.squint.is_some() {
                    4
                } else {
                    correlations
                };
                let fluxes = &prepared.fluxes[component * channels * stored..][..channels * stored];
                let powers = powers.map(|powers| &powers[component * channels..][..channels]);
                if let Some(squint) = &self.squint
                    && flux_angle != Some(angle)
                {
                    let workspace = workspace.as_mut().expect("squint has a workspace");
                    for channel in 0..channels {
                        let frequency = prepared.stored_frequencies.as_ref().map_or_else(
                            || prepared.first_hz + channel as f64 * prepared.step_hz,
                            |values| values.value[channel],
                        );
                        let circular = std::array::from_fn(|product| fluxes[channel * 4 + product]);
                        let flux = squint.flux(
                            circular,
                            physical,
                            terms.beam_centre,
                            angle,
                            frequency,
                            prepared.basis,
                        );
                        workspace[channel * 4..(channel + 1) * 4].copy_from_slice(&flux);
                    }
                    flux_angle = Some(angle);
                }
                for channel in 0..channels {
                    let frequency_hz = prepared.stored_frequencies.as_ref().map_or_else(
                        || prepared.first_hz + channel as f64 * prepared.step_hz,
                        |frequencies| frequencies.value[channel],
                    );
                    let channel_phasor = if prepared.stored_frequencies.is_some() {
                        Complex64::from_polar(1.0, radians_per_hz * frequency_hz)
                    } else {
                        phasor
                    };
                    let mut scale = baseline.at_frequency(frequency_hz)?;
                    if let Some(powers) = powers {
                        scale *= f64::from(powers[channel]);
                    }
                    let value = channel_phasor * scale;
                    let cells = &mut samples[channel * correlations..(channel + 1) * correlations];
                    if self.squint.is_some() {
                        let flux = &workspace.as_ref().expect("squint workspace")
                            [channel * 4..(channel + 1) * 4];
                        for (cell, &product) in
                            cells.iter_mut().zip(&prepared.products[..correlations])
                        {
                            let sample = flux[product] * value;
                            *cell += Complex32::new(sample.re as f32, sample.im as f32);
                        }
                    } else {
                        for (cell, flux) in cells
                            .iter_mut()
                            .zip(&fluxes[channel * stored..(channel + 1) * stored])
                        {
                            let sample = flux * value;
                            *cell += Complex32::new(sample.re as f32, sample.im as f32);
                        }
                    }
                    phasor *= step;
                }
            }
        }
        Ok(())
    }
}

/// `window`'s components' fluxes in its correlations
/// ([`PreparedWindow::fluxes`]).
fn prepare_window(
    components: &[SkyComponent],
    window: &ComponentWindow,
    squinted: bool,
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
    let stored = if squinted { 4 } else { mapped.len() };
    let mut fluxes = Vec::with_capacity(components.len() * window.channels * stored);
    for (index, component) in components.iter().enumerate() {
        for channel in 0..window.channels {
            let flux = component
                .flux_in(
                    window.frequency_hz(channel),
                    if squinted {
                        Polarisation::Circular
                    } else {
                        polarisation
                    },
                )
                .map_err(|error| refused(format!("component {index}: {error}")))?;
            if squinted {
                fluxes.extend(flux);
            } else {
                fluxes.extend(mapped.iter().map(|&(_, basis)| flux[basis]));
            }
        }
    }
    Ok(PreparedWindow {
        stored_frequencies: window.stored_frequencies.clone(),
        first_hz: window.first_hz,
        step_hz: window.step_hz,
        channels: window.channels,
        correlations: mapped.len(),
        basis: polarisation,
        products: std::array::from_fn(|index| mapped.get(index).map_or(0, |&(_, product)| product)),
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
