// SPDX-License-Identifier: LGPL-3.0-or-later
//! Standalone flux-calibrator component prediction shared by imager and task.

use std::path::Path;

use casa_components::{ComponentList, ComponentListError, ComponentShape, SkyComponent};
use casa_core::measures::epoch::MEpoch;
use casa_core::measures::frame::MeasFrame;
use casa_core::measures::frequency::{FrequencyRef, MFrequency};
use casa_core::quanta::casacore::stream_double;
use casa_ms::{MsResult, open_measures_runtime};
use casa_tables::publication::{
    check_write_access, exchange_directories, lock_table_tree_for_replacement,
    rename_without_replacing,
};
use casa_tables::{Table, TableOptions};

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
        let staging = self.stage(path.parent().unwrap_or(workspace))?;
        let staged = staging.path().join("model.cl");
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

impl PredictedComponent {
    fn stage(&self, parent: &Path) -> MsResult<tempfile::TempDir> {
        let staging = tempfile::tempdir_in(parent).map_err(invalid)?;
        ComponentList::from(vec![self.component.clone()])
            .write_table(staging.path().join("model.cl"))
            .map_err(invalid)?;
        Ok(staging)
    }

    /// Publish the public task's fresh component list, replacing a previous
    /// component list with the same rounded name atomically.
    ///
    /// # Errors
    ///
    /// The output path cannot be written or would replace another file type.
    pub fn write_for_task(&self, workspace: &Path) -> MsResult<()> {
        let path = workspace.join(&self.filename);
        let parent = path.parent().unwrap_or(workspace);
        std::fs::create_dir_all(parent).map_err(invalid)?;
        let staging = self.stage(parent)?;
        let staged = staging.path().join("model.cl");
        match rename_without_replacing(&staged, &path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if !path
                    .symlink_metadata()
                    .is_ok_and(|metadata| metadata.is_dir())
                    || !Table::open_metadata_only(TableOptions::new(&path))
                        .is_ok_and(|table| table.info().table_type == "Component List")
                {
                    return Err(invalid(ComponentListError::Exists(path)));
                }
                check_write_access(parent).map_err(invalid)?;
                check_write_access(&path).map_err(invalid)?;
                let _locks = lock_table_tree_for_replacement(&path).map_err(invalid)?;
                exchange_directories(&staged, &path).map_err(invalid)
            }
            Err(error) => Err(invalid(error)),
        }
    }
}
/// Evaluate the public task's 2012 Horizons disk model at the ALMA site.
///
/// The input frequencies are channel centres. The 2012 task defines a narrow
/// bandwidth around each centre, and writes it in the TOPO frame.
///
/// # Errors
///
/// The epoch or frequency grid is invalid, or no flux samples are supported.
pub fn predict_solar_component_2012(
    object: &str,
    epoch: &MEpoch,
    frequency_hz: &[f64],
    prefix: &str,
) -> MsResult<PredictedComponent> {
    use casa_core::measures::position::MPosition;

    use super::setjy::solar2012::Solar2012;

    let first = *frequency_hz
        .first()
        .ok_or_else(|| invalid("predictcomp requires at least one frequency"))?;
    let mjd = epoch.value().as_mjd();
    if mjd == 0. {
        return Err(invalid("Invalid epoch, 0.0d"));
    }
    let frequencies: Vec<_> = frequency_hz
        .iter()
        .map(|&hz| MFrequency::new(hz, FrequencyRef::TOPO))
        .collect();
    let model = Solar2012::open(object, mjd)?;
    let measures = open_measures_runtime()?;
    let position = MPosition::from_observatory_name("ALMA", measures.as_ref())
        .map_err(invalid)?
        .ok_or_else(|| invalid("the Measures catalog has no ALMA observatory"))?;
    let frame = MeasFrame::new()
        .with_measures(measures)
        .with_epoch(epoch.clone())
        .with_position(position)
        .with_direction(model.direction().clone());
    let half_width = frequency_hz
        .get(1)
        .map_or(first * 1e-4, |hz| (hz - first).abs() * 1e-2);
    let bands: Vec<_> = frequency_hz
        .iter()
        .map(|hz| [hz - half_width, hz + half_width])
        .collect();
    let (mut component, _, supported) = model.component(
        model.direction().clone(),
        &frame,
        &frequencies,
        &bands,
        true,
    )?;
    if !supported {
        return Err(invalid("Flux densities cannot be determined"));
    }
    // Unlike setjy, predictcomp writes the band centre as the reference.
    component
        .spectrum_mut()
        .set_reference_frequency(frequencies[0].clone());
    let label = format!("{object}_spw0_{:.3}GHz_{mjd:.1}d", first / 1e9);
    component.set_label(&label);
    Ok(PredictedComponent {
        filename: format!("{prefix}{label}.cl"),
        component,
    })
}
