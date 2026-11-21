// SPDX-License-Identifier: LGPL-3.0-or-later
//! Standalone flux-calibrator component prediction shared by imager and task.

use std::path::Path;

use casa_components::{ComponentList, ComponentListError, ComponentShape, SkyComponent};
use casa_ms::{MsResult, open_measures_runtime};
use casa_tables::publication::rename_without_replacing;
use casa_types::measures::epoch::MEpoch;
use casa_types::measures::frame::MeasFrame;
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::quanta::casacore::stream_double;

use super::invalid;
use super::setjy::sampled_shape;
use super::setjy::solar2010::Solar2010;
use super::setjy::standards::{
    CatalogMatch, FluxCatalog, FluxScale, canonical_tool_standard, source_direction,
};
use crate::interpolation::Curve;

/// The standalone imager's component table. The filename is also the tool's return value.
pub struct PredictedComponent {
    /// Table filename, including the user-supplied prefix.
    pub filename: String,
    /// Calibrated source with flux and spectrum evaluated at the requested frequencies.
    pub component: SkyComponent,
}

/// Evaluate a point or disk flux calibrator for the standalone legacy imager.
///
/// Both Horizons labels use the 2010 model in the imager tool. The frequencies
/// are in Hz, in the input order. An unknown calibrator on a fixed or catalogue
/// scale returns no component.
///
/// # Errors
///
/// An unrecognized standard, invalid frequency or missing model data.
pub fn predict_calibrator_component(
    workspace: &Path,
    object: &str,
    standard: &str,
    epoch: &MEpoch,
    frequency_hz: &[f64],
    prefix: &str,
) -> MsResult<Option<PredictedComponent>> {
    let standard = canonical_tool_standard(standard)
        .ok_or_else(|| invalid(format!("{standard} is not a recognized flux density scale")))?;
    let Some(&first) = frequency_hz.first() else {
        return Err(invalid("predictcomp requires at least one frequency"));
    };
    if !frequency_hz.iter().all(|hz| hz.is_finite() && *hz > 0.) {
        return Err(invalid("predictcomp requires positive finite frequencies"));
    }
    let frequencies: Vec<_> = frequency_hz
        .iter()
        .map(|&hz| MFrequency::new(hz, FrequencyRef::REST))
        .collect();
    let mjd = epoch.value().as_mjd();
    let mut component = if standard == "Butler-JPL-Horizons 2010" {
        let frame = MeasFrame::new()
            .with_measures(open_measures_runtime()?)
            .with_epoch(epoch.clone());
        Solar2010::open(workspace, object, epoch, &frame)
            .and_then(|model| model.component(None, &frequencies))
            .map_err(|_| invalid("Unknown is not a supported component type."))?
            .0
    } else {
        let scale = FluxScale::named(standard)
            .ok_or_else(|| invalid(format!("{standard} is not a recognized flux density scale")))?;
        let Some(direction) = source_direction(object) else {
            return Ok(None);
        };
        let catalog = FluxCatalog::open(scale)?;
        let spectrum = match catalog.spectrum(object, &direction, mjd, Curve::Nearest)? {
            CatalogMatch::Spectrum(spectrum) => spectrum,
            CatalogMatch::UnknownSource => return Ok(None),
            CatalogMatch::MissingCoefficients(source) => {
                return Err(catalog.missing_coefficients(source));
            }
        };
        let stokes = frequency_hz
            .iter()
            .map(|&hz| [spectrum.flux(hz), 0., 0., 0.])
            .collect();
        sampled_shape(ComponentShape::point(direction), &frequencies, stokes)?.0
    };
    // CASA rejects a tabulation without a nonzero frequency range.
    if frequency_hz.len() > 1 && frequency_hz.iter().all(|&hz| hz == first) {
        return Err(invalid(
            "Interpolate1D::operator() data has repeated x values",
        ));
    }
    // The underlying tabulation stays in REST. CASA writes the first input
    // frequency as the component's LSRK reference value.
    component
        .spectrum_mut()
        .set_reference_frequency(MFrequency::new(first, FrequencyRef::LSRK));
    let filename = format!(
        "{prefix}spw0_{object}_{}GHz{}d.cl",
        stream_double(first / 1e9),
        stream_double(mjd)
    );
    Ok(Some(PredictedComponent {
        filename,
        component,
    }))
}

impl PredictedComponent {
    /// Write a component list unless a directory already owns this filename.
    ///
    /// CASA reuses existing component lists when the rounded epoch and frequency
    /// yield the same path, including when the channel grid or standard changes.
    ///
    /// # Errors
    ///
    /// The component table cannot be written.
    pub fn write_if_absent(&self, workspace: &Path) -> MsResult<()> {
        let path = workspace.join(&self.filename);
        if path.is_dir() {
            return Ok(());
        }
        let parent = path.parent().unwrap_or(workspace);
        let staging = tempfile::tempdir_in(parent).map_err(invalid)?;
        let staged = staging.path().join("model.cl");
        ComponentList::from(vec![self.component.clone()])
            .write_table(&staged)
            .map_err(invalid)?;
        match rename_without_replacing(&staged, &path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if path.is_dir() {
                    Ok(())
                } else {
                    Err(invalid(ComponentListError::Exists(path)))
                }
            }
            Err(error) => Err(invalid(error)),
        }
    }
}
