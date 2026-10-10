// SPDX-License-Identifier: LGPL-3.0-or-later

//! Backend-free row and channel values of a selected observation.

use std::{mem::size_of, sync::Arc};

use crate::geometry::{Epoch, FrequencyFrame, SkyDirection, UvwCoordinateLaw};

/// Per-antenna pointing directions evaluated for one baseline sample.
///
/// Retaining both directions preserves antenna-dependent boresight offsets. A downstream
/// operator may derive a baseline-effective direction according to its own closed response law.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedPointingDirections {
    /// Evaluated direction for MAIN `ANTENNA1`.
    pub antenna1: SkyDirection,
    /// Evaluated direction for MAIN `ANTENNA2`.
    pub antenna2: SkyDirection,
}

/// One visibility-coordinate projection to a declared image phase centre.
///
/// Raw MeasurementSet coordinates remain row facts stored once in
/// [`SelectedSampleCoordinates`]. This value carries only the operator-ready
/// transform and signed geometric path to one compiled semantic centre.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedPhaseCentreProjection {
    transformed_uvw_m: [f64; 3],
    phase_shift_m: f64,
}

impl SelectedPhaseCentreProjection {
    /// Construct one finite UVW and phase-path projection.
    #[must_use]
    pub fn new(transformed_uvw_m: [f64; 3], phase_shift_m: f64) -> Option<Self> {
        (transformed_uvw_m.iter().all(|value| value.is_finite()) && phase_shift_m.is_finite())
            .then_some(Self {
                transformed_uvw_m,
                phase_shift_m,
            })
    }

    /// Return operator-ready UVW coordinates in metres.
    #[must_use]
    pub const fn transformed_uvw_m(self) -> [f64; 3] {
        self.transformed_uvw_m
    }

    /// Return the signed geometric phase-shift path in metres.
    #[must_use]
    pub const fn phase_shift_m(self) -> f64 {
        self.phase_shift_m
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum SelectedPsfPhaseCentreProjection {
    SharedWithModel,
    Distinct(SelectedPhaseCentreProjection),
}

/// Model and PSF coordinate projections for one canonical image domain.
///
/// The ordinal indexes [`crate::CompiledGeometry::domains`] directly. Domain
/// role strings therefore remain compiler-owned and are not repeated for each
/// selected row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedImageDomainProjection {
    domain_ordinal: u32,
    facet_ordinal: u32,
    model: SelectedPhaseCentreProjection,
    psf: SelectedPsfPhaseCentreProjection,
    aw_pointing_pixel: Option<[f64; 2]>,
}

impl SelectedImageDomainProjection {
    /// Construct one domain with independently declared model and PSF projections.
    #[must_use]
    pub const fn new(
        domain_ordinal: u32,
        model: SelectedPhaseCentreProjection,
        psf: SelectedPhaseCentreProjection,
    ) -> Self {
        Self::new_facet(domain_ordinal, 0, model, psf)
    }

    /// Construct one facet chart with independently declared model and PSF projections.
    #[must_use]
    pub const fn new_facet(
        domain_ordinal: u32,
        facet_ordinal: u32,
        model: SelectedPhaseCentreProjection,
        psf: SelectedPhaseCentreProjection,
    ) -> Self {
        Self {
            domain_ordinal,
            facet_ordinal,
            model,
            psf: SelectedPsfPhaseCentreProjection::Distinct(psf),
            aw_pointing_pixel: None,
        }
    }

    /// Construct a domain whose compiled PSF and model centres are identical.
    #[must_use]
    pub const fn with_shared_psf(
        domain_ordinal: u32,
        model: SelectedPhaseCentreProjection,
    ) -> Self {
        Self::facet_with_shared_psf(domain_ordinal, 0, model)
    }

    /// Construct a facet chart whose compiled PSF and model centres are identical.
    #[must_use]
    pub const fn facet_with_shared_psf(
        domain_ordinal: u32,
        facet_ordinal: u32,
        model: SelectedPhaseCentreProjection,
    ) -> Self {
        Self {
            domain_ordinal,
            facet_ordinal,
            model,
            psf: SelectedPsfPhaseCentreProjection::SharedWithModel,
            aw_pointing_pixel: None,
        }
    }

    /// Attach the exact CASA chart-local baseline pointing pixel used by AW projection.
    ///
    /// The coordinate is the already averaged two-antenna pointing pixel in
    /// this chart. Raw pointing directions remain available separately for
    /// primary-beam and mosaic evaluation.
    #[must_use]
    pub fn with_aw_pointing_pixel(mut self, pixel: [f64; 2]) -> Option<Self> {
        if pixel.iter().any(|value| !value.is_finite()) {
            return None;
        }
        self.aw_pointing_pixel = Some(pixel);
        Some(self)
    }

    /// Return the index into canonical compiled image domains.
    #[must_use]
    pub const fn domain_ordinal(self) -> u32 {
        self.domain_ordinal
    }

    /// Return the index into the domain's canonical compiled facet charts.
    #[must_use]
    pub const fn facet_ordinal(self) -> u32 {
        self.facet_ordinal
    }

    /// Return the chart-model phase-centre projection.
    #[must_use]
    pub const fn model(self) -> SelectedPhaseCentreProjection {
        self.model
    }

    /// Return the point-spread-function phase-centre projection.
    #[must_use]
    pub const fn psf(self) -> SelectedPhaseCentreProjection {
        match self.psf {
            SelectedPsfPhaseCentreProjection::SharedWithModel => self.model,
            SelectedPsfPhaseCentreProjection::Distinct(psf) => psf,
        }
    }

    /// Return whether model and PSF centres share one evaluated projection.
    #[must_use]
    pub const fn psf_shares_model(self) -> bool {
        matches!(self.psf, SelectedPsfPhaseCentreProjection::SharedWithModel)
    }

    /// Return the exact CASA chart-local baseline pointing pixel, when evaluated.
    #[must_use]
    pub const fn aw_pointing_pixel(self) -> Option<[f64; 2]> {
        self.aw_pointing_pixel
    }
}

/// Canonically ordered, compiled-chart-bounded projections for one selected row.
///
/// Entries are required to be contiguous in domain-major, facet-minor order.
/// The collection is immutable and cheaply shared between a retained source
/// block and scientific consumers.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedImageDomainProjections {
    entries: Arc<[SelectedImageDomainProjection]>,
}

impl SelectedImageDomainProjections {
    /// Construct the canonical one-domain collection when model and PSF centres are identical.
    #[must_use]
    pub fn one_domain_with_shared_psf(model: SelectedPhaseCentreProjection) -> Self {
        Self {
            entries: Arc::from([SelectedImageDomainProjection::with_shared_psf(0, model)]),
        }
    }

    /// Construct a non-empty canonical projection sequence.
    #[must_use]
    pub fn new(entries: impl IntoIterator<Item = SelectedImageDomainProjection>) -> Option<Self> {
        let entries = entries.into_iter().collect::<Vec<_>>();
        if entries
            .first()
            .is_none_or(|entry| entry.domain_ordinal() != 0 || entry.facet_ordinal() != 0)
            || entries.windows(2).any(|pair| {
                let previous = pair[0];
                let next = pair[1];
                !((next.domain_ordinal() == previous.domain_ordinal()
                    && previous.facet_ordinal().checked_add(1) == Some(next.facet_ordinal()))
                    || (previous.domain_ordinal().checked_add(1) == Some(next.domain_ordinal())
                        && next.facet_ordinal() == 0))
            })
        {
            return None;
        }
        Some(Self {
            entries: Arc::from(entries.into_boxed_slice()),
        })
    }

    /// Return the exact number of compiled-domain projections.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return whether no domain projection is present.
    ///
    /// Canonically constructed collections are never empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterate in canonical compiled-domain order (main first, then outliers).
    pub fn iter(&self) -> impl Iterator<Item = SelectedImageDomainProjection> + '_ {
        self.entries.iter().copied()
    }

    /// Return facet zero for one canonical compiled-domain ordinal.
    #[must_use]
    pub fn get(&self, domain_ordinal: u32) -> Option<SelectedImageDomainProjection> {
        self.get_facet(domain_ordinal, 0)
    }

    /// Return one projection by canonical compiled domain and facet ordinals.
    #[must_use]
    pub fn get_facet(
        &self,
        domain_ordinal: u32,
        facet_ordinal: u32,
    ) -> Option<SelectedImageDomainProjection> {
        self.entries
            .binary_search_by_key(&(domain_ordinal, facet_ordinal), |entry| {
                (entry.domain_ordinal(), entry.facet_ordinal())
            })
            .ok()
            .map(|index| self.entries[index])
    }

    /// Return retained heap payload bytes for a compiled domain cardinality.
    #[doc(hidden)]
    #[must_use]
    pub const fn retained_heap_bytes_for_len(domain_count: usize) -> Option<usize> {
        match domain_count.checked_mul(size_of::<SelectedImageDomainProjection>()) {
            Some(payload) => payload.checked_add(2 * size_of::<usize>()),
            None => None,
        }
    }
}

/// Reported evaluated coordinates consumed by weighting and paired operators.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedSampleCoordinates {
    /// Raw MeasurementSet `[u, v, w]` coordinate in metres.
    pub raw_uvw_m: [f64; 3],
    /// `[u, v, w]` coordinate used for weighting-density evaluation, in metres.
    pub density_uvw_m: [f64; 3],
    /// `[u, v, w]` coordinate transformed for operator evaluation, in metres.
    pub transformed_uvw_m: [f64; 3],
    /// Signed geometric phase-shift path length in metres.
    pub phase_shift_m: f64,
    /// Compiled UVW convention.
    pub uvw_law: UvwCoordinateLaw,
    /// MAIN `TIME` epoch.
    pub time: Epoch,
    /// MAIN `TIME_CENTROID` epoch.
    pub time_centroid: Epoch,
    /// MAIN `INTERVAL` in seconds.
    pub interval_seconds: f64,
    /// MAIN `EXPOSURE` in seconds.
    pub exposure_seconds: f64,
    /// Evaluated parallactic angles for `ANTENNA1` and `ANTENNA2`, in radians,
    /// or `None` when the compiled operator does not require feed rotation.
    ///
    /// FEED receptor-angle offsets remain instrument-response inputs and are
    /// deliberately not folded into this source-derived coordinate.
    pub parallactic_angles_rad: Option<[f64; 2]>,
    /// Evaluated phase direction.
    pub phase_direction: SkyDirection,
    /// Evaluated delay direction.
    pub delay_direction: SkyDirection,
    /// Evaluated per-antenna pointing directions.
    pub pointing_directions: SelectedPointingDirections,
}

/// CASA aperture class selected from owner-controlled ANTENNA metadata.
///
/// These are scientific response identities rather than physical antenna IDs.
/// Multiple antennas with the same class therefore share one cached response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AntennaResponseClass {
    /// Physical 12 m ALMA dish evaluated with CASA's 10.7 m effective aperture.
    CasaAlma12m,
    /// Physical 7 m ACA dish evaluated with CASA's 6.25 m effective aperture.
    CasaAca7m,
}

/// Ordered response classes for one selected interferometric baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedAntennaResponses {
    /// Response class of MAIN `ANTENNA1`.
    pub antenna1: AntennaResponseClass,
    /// Response class of MAIN `ANTENNA2`.
    pub antenna2: AntennaResponseClass,
    /// Largest response class present in this MeasurementSet response family.
    ///
    /// CASA crops every heterogeneous convolution plane to one family-wide
    /// extent so resampling has the same boundary domain for every pair.
    pub family_envelope: AntennaResponseClass,
}

/// Reported per-sample MeasurementSet provenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedSampleMetadata {
    /// MAIN `FIELD_ID`.
    pub field_id: i32,
    /// MAIN `ANTENNA1`.
    pub antenna1: i32,
    /// MAIN `ANTENNA2`.
    pub antenna2: i32,
    /// Owner-derived paired response, present for a direction-dependent model.
    pub antenna_responses: Option<SelectedAntennaResponses>,
    /// MAIN `FEED1`.
    pub feed1: i32,
    /// MAIN `FEED2`.
    pub feed2: i32,
    /// MAIN `SCAN_NUMBER`.
    pub scan_number: i32,
    /// MAIN `STATE_ID`.
    pub state_id: i32,
    /// MAIN `OBSERVATION_ID`.
    pub observation_id: i32,
    /// MAIN `ARRAY_ID`.
    pub array_id: i32,
}

/// Row-shared values of one selected MAIN row.
///
/// A storage owner evaluates them once per row and lends them to every
/// selected channel and correlation of that row.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedObservationRunRow {
    /// The MeasurementSet's position in the compiled observation snapshot.
    pub measurement_set: usize,
    /// Physical MAIN row number.
    pub physical_row: u64,
    /// MAIN `DATA_DESC_ID`.
    pub data_description_id: i32,
    /// Resolved `SPECTRAL_WINDOW_ID`.
    pub spectral_window_id: u32,
    /// Resolved `POLARIZATION_ID`.
    pub polarization_id: u32,
    /// MAIN `FLAG_ROW` value.
    pub row_flag: bool,
    /// Evaluated science coordinates shared by the row.
    pub coordinates: SelectedSampleCoordinates,
    /// Canonical model and PSF projections shared by every row member.
    pub domain_projections: SelectedImageDomainProjections,
    /// Per-row MeasurementSet provenance.
    pub metadata: SelectedSampleMetadata,
}

impl SelectedObservationRunRow {
    /// Return canonical compiled-domain projections for this selected row.
    #[must_use]
    pub const fn domain_projections(&self) -> &SelectedImageDomainProjections {
        &self.domain_projections
    }
}

/// Channel-shared values of one selected native channel.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedObservationRunChannel {
    /// Zero-based native channel index.
    pub channel_index: u32,
    /// Native channel centre frequency in hertz.
    pub frequency_centre_hz: f64,
    /// Lower native channel boundary in hertz.
    pub frequency_lower_hz: f64,
    /// Upper native channel boundary in hertz.
    pub frequency_upper_hz: f64,
    /// Signed native channel width in hertz.
    pub channel_width_hz: f64,
    /// Reference frame of the channel frequencies.
    pub frequency_frame: FrequencyFrame,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_domain_projections_require_canonical_ordinals_and_share_equal_psf_values() {
        let model = SelectedPhaseCentreProjection::new([12.0, -4.0, 2.0], 0.125)
            .expect("finite model projection");
        let psf = SelectedPhaseCentreProjection::new([11.0, -3.0, 1.0], -0.25)
            .expect("finite PSF projection");
        let main = SelectedImageDomainProjection::with_shared_psf(0, model);
        let main_second = SelectedImageDomainProjection::facet_with_shared_psf(0, 1, psf);
        let outlier = SelectedImageDomainProjection::new(1, model, psf);
        let projections = SelectedImageDomainProjections::new([main, main_second, outlier])
            .expect("canonical main-first projections");

        assert_eq!(
            projections.iter().collect::<Vec<_>>(),
            vec![main, main_second, outlier]
        );
        assert_eq!(projections.get(0), Some(main));
        assert_eq!(projections.get(1), Some(outlier));
        assert_eq!(projections.get_facet(0, 1), Some(main_second));
        assert!(main.psf_shares_model());
        assert_eq!(main.psf(), main.model());
        assert!(!outlier.psf_shares_model());
        assert_eq!(outlier.psf(), psf);
        assert_eq!(main.aw_pointing_pixel(), None);
        let exact = main
            .with_aw_pointing_pixel([256.25, 255.75])
            .expect("finite AW pointing pixel");
        assert_eq!(exact.aw_pointing_pixel(), Some([256.25, 255.75]));
        assert!(main.with_aw_pointing_pixel([f64::NAN, 0.0]).is_none());
        assert!(main.with_aw_pointing_pixel([0.0, f64::INFINITY]).is_none());
        assert!(SelectedImageDomainProjections::new([outlier, main]).is_none());
        assert!(SelectedImageDomainProjections::new([main, outlier, main_second]).is_none());
        assert!(SelectedImageDomainProjections::new([main, outlier]).is_some());
        assert!(SelectedImageDomainProjections::new(std::iter::empty()).is_none());
        assert!(SelectedPhaseCentreProjection::new([f64::NAN, 0.0, 0.0], 0.0).is_none());
    }
}
