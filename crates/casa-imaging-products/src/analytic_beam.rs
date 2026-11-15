// SPDX-License-Identifier: LGPL-3.0-or-later

//! CASA's analytic primary beam: the `.pb` of a standard or W-projection
//! run, a telescope's beam centred on each selected field and summed
//! (`SynthesisImagerVi2::makePrimaryBeam`, `SIMapper::addPB`), divided by
//! its peak where that exceeds one and zero where it is not above
//! `|pblimit|` (`SIImageStore::makePBImage`).
//!
//! The products publish it as `.pb` and divide `.image.pbcor` by it; the
//! minor cycle measures its statistics inside its support, as CASA's
//! residual carries the `.pb`'s mask (`SIImageStore::divideResidualByWeight`,
//! `calcRobustRMS`).

use std::borrow::Cow;

use casa_imaging_model::{
    CompiledProblem, ImageDomainRole, ProductSupportComparison, ReconstructionBasis,
};
use casa_numerics::{AnnularApertureVoltageTable, EvlaBand, EvlaPowerPattern, VlaBand};
use casa_numerics::{BeamPattern, PrimaryBeam};

use crate::error::ProductsError;
use crate::plan::AnalyticPrimaryBeamModel;
use crate::source::PrimaryBeamCentres;

/// Bytes per pixel [`AnalyticPrimaryBeam::support`] holds beside the
/// support it returns: the plane's beam, one `f32` per pixel, which
/// [`AnalyticPrimaryBeam::pb`] returns.
pub const ANALYTIC_PRIMARY_BEAM_BYTES_PER_CELL: u64 = size_of::<f32>() as u64;

/// The circular voltage lookup read from a supplied CASA VPManager table.
pub struct SavedRadialPrimaryBeam(BeamPattern);

impl SavedRadialPrimaryBeam {
    /// Tabulate one admitted Gaussian or Airy voltage law for this run.
    #[must_use]
    pub fn new(beam: PrimaryBeam) -> Self {
        Self(beam.pattern())
    }
}

impl std::fmt::Debug for SavedRadialPrimaryBeam {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SavedRadialPrimaryBeam")
            .finish_non_exhaustive()
    }
}

/// The beam laws an analytic primary beam takes.
#[derive(Debug, Clone, Copy)]
enum BeamLaw<'a> {
    /// CASA's common EVLA beam ([`AnalyticPrimaryBeamModel::CasaEvlaCommon`]).
    EvlaCommon,
    /// CASA's legacy VLA beams ([`AnalyticPrimaryBeamModel::CasaVlaBand`]).
    VlaBand,
    /// An ALMA or ACA Airy beam.
    Airy(CasaAlmaAiry),
    Saved(&'a SavedRadialPrimaryBeam),
}

/// One run's analytic primary beam: the model its products and minor cycle
/// share, on the problem's image domains and output channels, centred on
/// the selected fields' pointings.
///
/// It borrows the compiled problem, which gives each domain's grid and
/// each channel's frequency, and the pointings; a plane is formed only when
/// asked for, so the beam holds nothing per plane.
#[derive(Debug, Clone, Copy)]
pub struct AnalyticPrimaryBeam<'a> {
    problem: &'a CompiledProblem,
    law: BeamLaw<'a>,
    model: AnalyticPrimaryBeamModel,
    centres: Option<&'a PrimaryBeamCentres>,
    vla_selection_hz: Option<f64>,
}

impl<'a> AnalyticPrimaryBeam<'a> {
    /// The analytic beam of `model` on `problem`'s domains, centred on
    /// `centres` (each domain's reference pixel without them); `None` for
    /// [`AnalyticPrimaryBeamModel::MosaicSensitivity`], whose beam is each
    /// plane's weight image.
    ///
    /// # Errors
    ///
    /// [`ProductsError::UnsupportedPrimaryBeamFrequency`] at the first
    /// output channel the beam would be formed at, whose frequency the
    /// model does not cover: channel zero of a Taylor problem, every
    /// channel otherwise.
    ///
    /// # Panics
    ///
    /// If `centres` lacks a domain of the problem.
    pub fn new(
        problem: &'a CompiledProblem,
        model: AnalyticPrimaryBeamModel,
        centres: Option<&'a PrimaryBeamCentres>,
    ) -> Result<Option<Self>, ProductsError> {
        Self::checked(problem, model, centres, None)
    }

    /// Bind the run's admitted saved radial beam on the image domains.
    ///
    /// # Errors
    /// An output channel does not have a positive finite frequency.
    /// # Panics
    /// If the supplied pointing map lacks a domain of the problem.
    pub fn from_saved(
        problem: &'a CompiledProblem,
        centres: Option<&'a PrimaryBeamCentres>,
        saved: &'a SavedRadialPrimaryBeam,
    ) -> Result<Self, ProductsError> {
        Self::checked(
            problem,
            AnalyticPrimaryBeamModel::SavedRadial,
            centres,
            Some(saved),
        )?
        .ok_or(ProductsError::InvalidControls)
    }

    fn checked(
        problem: &'a CompiledProblem,
        model: AnalyticPrimaryBeamModel,
        centres: Option<&'a PrimaryBeamCentres>,
        saved: Option<&'a SavedRadialPrimaryBeam>,
    ) -> Result<Option<Self>, ProductsError> {
        if model == AnalyticPrimaryBeamModel::SavedRadial && saved.is_none() {
            return Err(ProductsError::InvalidControls);
        }
        let Some(beam) = Self::bind(problem, model, centres, saved) else {
            return Ok(None);
        };
        assert!(
            centres.is_none_or(|centres| {
                problem
                    .geometry()
                    .domains()
                    .iter()
                    .all(|domain| centres.contains_key(domain.role()))
            }),
            "the pointings cover every image domain"
        );
        check_frequencies(problem, model)?;
        Ok(Some(beam))
    }

    /// [`Self::new`] for a problem whose beam frequencies were checked
    /// ([`crate::ContinuumProductControls::validate_for_problem`]).
    pub(crate) fn bind(
        problem: &'a CompiledProblem,
        model: AnalyticPrimaryBeamModel,
        centres: Option<&'a PrimaryBeamCentres>,
        saved: Option<&'a SavedRadialPrimaryBeam>,
    ) -> Option<Self> {
        let law = match model {
            AnalyticPrimaryBeamModel::CasaEvlaCommon => BeamLaw::EvlaCommon,
            AnalyticPrimaryBeamModel::CasaVlaBand => BeamLaw::VlaBand,
            AnalyticPrimaryBeamModel::CasaAlma12mAiry => BeamLaw::Airy(CasaAlmaAiry::ALMA_12M),
            AnalyticPrimaryBeamModel::CasaAca7mAiry => BeamLaw::Airy(CasaAlmaAiry::ACA_7M),
            AnalyticPrimaryBeamModel::SavedRadial => BeamLaw::Saved(saved?),
            AnalyticPrimaryBeamModel::MosaicSensitivity => return None,
        };
        Some(Self {
            problem,
            law,
            model,
            centres,
            vla_selection_hz: None,
        })
    }

    /// Select the legacy VLA aperture at `frequency_hz`; each output plane
    /// still evaluates that aperture at its own frequency.
    ///
    /// # Panics
    ///
    /// If the selection frequency is non-finite or not positive.
    #[must_use]
    pub fn with_vla_selection_frequency(mut self, frequency_hz: f64) -> Self {
        assert!(frequency_hz.is_finite() && frequency_hz > 0.0);
        self.vla_selection_hz = Some(frequency_hz);
        self
    }

    /// The beam's model.
    #[must_use]
    pub const fn model(&self) -> AnalyticPrimaryBeamModel {
        self.model
    }

    /// CASA's beam on the plane of image domain `domain` at output channel
    /// `output_channel`, x-major, before the `pblimit` cutoff: what `.pb`
    /// publishes where it exceeds the cutoff. A Taylor family's beam is its
    /// channel zero's. A domain with no pointing has a zero beam.
    ///
    /// # Errors
    ///
    /// [`ProductsError::UnsupportedProblem`] when `domain` is not one of
    /// the problem's domains or `output_channel` has no frequency.
    pub fn plane(
        &self,
        domain: &ImageDomainRole,
        output_channel: usize,
    ) -> Result<Vec<f32>, ProductsError> {
        let BeamGeometry {
            shape,
            centres,
            increment_rad,
            frequency_hz,
        } = self.geometry(domain, output_channel)?;
        Ok(match self.law {
            BeamLaw::EvlaCommon => {
                let pattern =
                    evla_common_pattern(frequency_hz).ok_or(ProductsError::UnsupportedProblem)?;
                summed_beams(shape, &centres, |plane, centre| {
                    add_evla_common_power(
                        plane,
                        shape,
                        centre,
                        increment_rad,
                        frequency_hz,
                        pattern,
                    );
                })
            }
            BeamLaw::VlaBand => {
                // CASA's SynthesisImager::makePBImage chooses the legacy VLA
                // aperture once, from output channel zero. PBMath1D then
                // scales that same aperture by each plane's frequency, even
                // when the cube crosses a band boundary.
                let first_frequency_hz = self
                    .problem
                    .geometry()
                    .spectral()
                    .channel_centre_hz(0)
                    .ok_or(ProductsError::UnsupportedProblem)?;
                let table =
                    vla_band_voltage_table(self.vla_selection_hz.unwrap_or(first_frequency_hz))?;
                summed_beams(shape, &centres, |plane, centre| {
                    add_annular_aperture_power(
                        plane,
                        shape,
                        centre,
                        increment_rad,
                        frequency_hz,
                        &table,
                    );
                })
            }
            BeamLaw::Airy(beam) => {
                let table = beam.table();
                summed_beams(shape, &centres, |plane, centre| {
                    add_annular_aperture_power(
                        plane,
                        shape,
                        centre,
                        increment_rad,
                        frequency_hz,
                        &table,
                    );
                })
            }
            BeamLaw::Saved(beam) => {
                let pattern = beam.0.at_frequency(frequency_hz);
                summed_beams(shape, &centres, |plane, centre| {
                    add_radial_power(
                        plane,
                        shape,
                        centre,
                        increment_rad,
                        frequency_hz,
                        beam.0.maximum_radius_arcmin_ghz(),
                        |radius| pattern.power(radius),
                    );
                })
            }
        })
    }

    /// The `.pb` CASA publishes on the plane of image domain `domain` at
    /// `output_channel`, x-major: [`Self::plane`] where it exceeds the
    /// problem's primary-beam cutoff (`|pblimit|`), and zero where it does
    /// not (`SIImageStore::makePBImage`). A level above zero keeps the
    /// pixels whose `.pb` exceeds it, as tclean's `pbmask` reads them
    /// (`pb > pbmask` in `SDMaskHandler::makePBMask` and
    /// `SIImageStore::calcRobustRMS`).
    ///
    /// # Errors
    ///
    /// As [`Self::plane`].
    pub fn pb(
        &self,
        domain: &ImageDomainRole,
        output_channel: usize,
    ) -> Result<Vec<f32>, ProductsError> {
        let policy = self.problem.products().validity().primary_beam();
        let mut plane = self.plane(domain, output_channel)?;
        for value in &mut plane {
            let kept = match policy.comparison() {
                ProductSupportComparison::StrictlyGreater => *value > policy.cutoff(),
            };
            if !kept {
                *value = 0.0;
            }
        }
        Ok(plane)
    }

    /// The pixels of [`Self::plane`] where the beam exceeds the problem's
    /// primary-beam cutoff (`|pblimit|`), x-major: the `.pb`'s mask
    /// (`iif(pb > |pblimit|, True, False)` in `makePBImage`), where
    /// [`Self::pb`] is positive. It holds the plane's beam
    /// ([`ANALYTIC_PRIMARY_BEAM_BYTES_PER_CELL`]) while it forms the
    /// support.
    ///
    /// # Errors
    ///
    /// As [`Self::plane`].
    pub fn support(
        &self,
        domain: &ImageDomainRole,
        output_channel: usize,
    ) -> Result<Vec<bool>, ProductsError> {
        Ok(self
            .pb(domain, output_channel)?
            .into_iter()
            .map(|value| value > 0.0)
            .collect())
    }

    /// The grid, pointings and frequency of `domain`'s plane at
    /// `output_channel`.
    fn geometry(
        &self,
        role: &ImageDomainRole,
        output_channel: usize,
    ) -> Result<BeamGeometry<'a>, ProductsError> {
        let domain = self
            .problem
            .geometry()
            .domains()
            .iter()
            .find(|domain| domain.role() == role)
            .ok_or(ProductsError::UnsupportedProblem)?;
        let direction = domain.direction();
        let frequency_hz = self
            .problem
            .geometry()
            .spectral()
            .channel_centre_hz(output_channel)
            .ok_or(ProductsError::UnsupportedProblem)?;
        Ok(BeamGeometry {
            shape: domain.shape().pixels(),
            centres: self.centres.map_or_else(
                || Cow::Owned(vec![direction.reference_pixel()]),
                |centres| Cow::Borrowed(centres[role].as_slice()),
            ),
            increment_rad: direction.increment_rad(),
            frequency_hz,
        })
    }
}

/// Check that `model` covers every output frequency of `problem` its beam
/// is formed at: channel zero of a Taylor problem, every channel otherwise.
pub(crate) fn check_frequencies(
    problem: &CompiledProblem,
    model: AnalyticPrimaryBeamModel,
) -> Result<(), ProductsError> {
    let spectral = problem.geometry().spectral();
    let channels = if matches!(
        problem.reconstruction().basis(),
        ReconstructionBasis::Taylor { .. }
    ) {
        1
    } else {
        spectral.output_channels()
    };
    for output_channel in 0..channels {
        let frequency_hz = spectral
            .channel_centre_hz(output_channel)
            .expect("an output channel has a centre frequency");
        if !primary_beam_frequency_supported(model, frequency_hz) {
            return Err(ProductsError::UnsupportedPrimaryBeamFrequency {
                model,
                output_channel,
                frequency_hz,
            });
        }
    }
    Ok(())
}

/// The grid (`[width, height]`), beam centres (pixels), cell increments
/// (radians) and frequency of one domain's plane.
struct BeamGeometry<'a> {
    shape: [usize; 2],
    centres: Cow<'a, [[f64; 2]]>,
    increment_rad: [f64; 2],
    frequency_hz: f64,
}

/// CASA's analytic PB on one plane of `shape`: a beam centred on each
/// pointing, summed (`SIMapper::addPB`), and divided by its peak where that
/// exceeds one (`SIImageStore::makePBImage`). With no pointing it is zero.
/// `add_beam` adds one pointing's beam into the plane, which is the only
/// allocation whatever the number of pointings.
fn summed_beams(
    shape: [usize; 2],
    centres: &[[f64; 2]],
    add_beam: impl Fn(&mut [f32], [f64; 2]),
) -> Vec<f32> {
    let mut sum = vec![0.0; shape[0] * shape[1]];
    for centre in centres {
        add_beam(&mut sum, *centre);
    }
    let peak = sum.iter().fold(0.0_f32, |peak, value| peak.max(*value));
    if peak > 1.0 {
        for value in &mut sum {
            *value /= peak;
        }
    }
    sum
}

/// Add the EVLA common beam centred on pixel `centre` into `values`.
fn add_evla_common_power(
    values: &mut [f32],
    shape: [usize; 2],
    centre: [f64; 2],
    increment_rad: [f64; 2],
    frequency_hz: f64,
    pattern: EvlaPowerPattern,
) {
    add_radial_power(
        values,
        shape,
        centre,
        increment_rad,
        frequency_hz,
        f64::INFINITY,
        |radius| pattern.power(radius),
    );
}

/// One of CASA's common Airy beams of ALMA's dishes (`PBMath::initByTelescope`
/// for `PBMath::ALMA` and `PBMath::ACA`): the effective aperture, with a
/// 0.75 m blockage, and the radius at 1 GHz to which `PBMath1DAiry` tables
/// it in 10000 samples, which sets the table's sampling.
#[derive(Clone, Copy, Debug, PartialEq)]
struct CasaAlmaAiry {
    effective_diameter_m: f64,
    maximum_radius_deg: f64,
}

impl CasaAlmaAiry {
    /// ALMA's 12 m dish: a 10.7 m aperture to 1.784°.
    const ALMA_12M: Self = Self {
        effective_diameter_m: 10.7,
        maximum_radius_deg: 1.784,
    };
    /// ACA's 7 m dish: a 6.25 m aperture to 3.568°.
    const ACA_7M: Self = Self {
        effective_diameter_m: 6.25,
        maximum_radius_deg: 3.568,
    };

    /// The beam's voltage table, its radius in arcmin at 1 GHz.
    fn table(self) -> AnnularApertureVoltageTable {
        AnnularApertureVoltageTable::new(
            self.effective_diameter_m,
            0.75,
            self.maximum_radius_deg * 60.0,
        )
    }
}

/// Add the annular-aperture beam power centred on pixel `centre` into
/// `values`.
fn add_annular_aperture_power(
    values: &mut [f32],
    shape: [usize; 2],
    centre: [f64; 2],
    increment_rad: [f64; 2],
    frequency_hz: f64,
    table: &AnnularApertureVoltageTable,
) {
    add_radial_power(
        values,
        shape,
        centre,
        increment_rad,
        frequency_hz,
        f64::INFINITY,
        |radius| {
            let voltage = table.evaluate(radius);
            voltage * voltage
        },
    );
}

/// Add a circular beam with PBMath1D's Float radius rounding before lookup.
fn add_radial_power(
    values: &mut [f32],
    shape: [usize; 2],
    centre: [f64; 2],
    increment_rad: [f64; 2],
    frequency_hz: f64,
    maximum_arcmin_ghz: f64,
    power: impl Fn(f64) -> f32,
) {
    let increment_deg = increment_rad.map(f64::to_degrees);
    let frequency_factor = 60.0 * frequency_hz / 1.0e9;
    let maximum_degrees_squared = (maximum_arcmin_ghz / frequency_factor).powi(2);
    for x in 0..shape[0] {
        let rx2 = ((x as f64 - centre[0]) * increment_deg[0]).powi(2) as f32;
        for y in 0..shape[1] {
            let ry2 = ((y as f64 - centre[1]) * increment_deg[1]).powi(2) as f32;
            let radius_squared = rx2 + ry2;
            // PBMath1D::applyXLine clips the Float squared offset before
            // rounding the frequency-scaled lookup radius to Float.
            if f64::from(radius_squared) > maximum_degrees_squared {
                continue;
            }
            let radius = (f64::from(radius_squared.sqrt()) * frequency_factor) as f32;
            // The squared-distance check has already admitted this
            // pixel. A rounded-up final radius still names the last sample.
            values[x * shape[1] + y] += power(f64::from(radius).min(maximum_arcmin_ghz));
        }
    }
}

fn vla_band_voltage_table(frequency_hz: f64) -> Result<AnnularApertureVoltageTable, ProductsError> {
    if !vla_band_supported(frequency_hz) {
        return Err(ProductsError::UnsupportedProblem);
    }
    // PBMath::whichCommonPBtoUse selects VLA_NVSS outside the named open
    // intervals. All named bands share one aperture; SIMapper::addPB applies
    // BeamSquint::NONE, so their different squint parameters do not enter here.
    let (diameter_m, obstruction_m) = if VlaBand::of(frequency_hz).is_some() {
        (25.0, 2.36)
    } else {
        (24.5, 0.0)
    };
    Ok(AnnularApertureVoltageTable::new(
        diameter_m,
        obstruction_m,
        0.8564 * 60.0,
    ))
}

fn vla_band_supported(frequency_hz: f64) -> bool {
    frequency_hz.is_finite() && frequency_hz > 0.0
}

fn primary_beam_frequency_supported(model: AnalyticPrimaryBeamModel, frequency_hz: f64) -> bool {
    match model {
        AnalyticPrimaryBeamModel::CasaVlaBand => vla_band_supported(frequency_hz),
        AnalyticPrimaryBeamModel::CasaEvlaCommon => evla_common_pattern(frequency_hz).is_some(),
        AnalyticPrimaryBeamModel::CasaAlma12mAiry
        | AnalyticPrimaryBeamModel::CasaAca7mAiry
        | AnalyticPrimaryBeamModel::MosaicSensitivity => true,
        AnalyticPrimaryBeamModel::SavedRadial => frequency_hz.is_finite() && frequency_hz > 0.0,
    }
}

/// The EVLA common beam CASA applies at `frequency_hz`
/// ([`EvlaPowerPattern::at`]) in the bands imaging supports, L, S and C
/// ([`EvlaBand::of`], with CASA's precedence where they overlap). Other
/// frequencies fail closed instead of borrowing another band's beam.
fn evla_common_pattern(frequency_hz: f64) -> Option<EvlaPowerPattern> {
    matches!(
        EvlaBand::of(frequency_hz),
        Some(EvlaBand::L | EvlaBand::S | EvlaBand::C)
    )
    .then(|| EvlaPowerPattern::at(frequency_hz))
}

#[cfg(test)]
mod tests;
