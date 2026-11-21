// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibration dispatch for the attached imager's legacy setjy method.

use std::path::PathBuf;

use casa_ms::{MeasurementSet, MsError, MsSelection};
use casa_provider_contracts::CasaValue;
use casa_tables::TableWriteLock;
use casa_types::measures::frequency::MFrequency;

use super::{FluxParameters, ManualFlux, prepare, requested_windows};
use crate::attachment::{AttachmentError, ImagerAttachments};
use crate::ft::setjy::standards::{FluxCatalog, FluxScale};
use crate::interpolation::Curve;
use crate::session::resolve_casa_calibration_selection;

/// Parameters to setjy on an attached CASA imager.
pub struct ImagerSetJyRequest {
    /// Directory for looking up relative model and user ephemeris tables.
    pub workspace: PathBuf,
    /// Row and spectral-window selectors supplied for this setjy call.
    pub selection: MsSelection,
    /// Literal resolved model image when requested.
    pub image: Option<PathBuf>,
    /// Requested Stokes I, Q, U and V. Negative I selects a flux standard.
    pub flux_jy: [f64; 4],
    /// Name of the CASA flux standard.
    pub standard: String,
    /// Evaluate a catalogue standard at each channel.
    pub by_channel: bool,
    /// Spectral index coefficients for a user-specified flux.
    pub spectral_index: Vec<f64>,
    /// Reference frequency for a user-specified flux.
    pub reference: MFrequency,
    /// Linear polarization fraction polynomial.
    pub polarization_index: Vec<f64>,
    /// Linear polarization angle polynomial.
    pub polarization_angle: Vec<f64>,
    /// Rotation measure in radians per metre squared.
    pub rotation_measure: f64,
    /// Time interpolation for a time-varying calibrator catalogue.
    pub interpolation: String,
}

/// Predict a model through the current imager association and its write policy.
///
/// # Errors
///
/// A rejected attachment, model preparation error, or failed publication.
pub fn imager_setjy(
    attachments: &mut ImagerAttachments,
    request: &ImagerSetJyRequest,
) -> Result<CasaValue, AttachmentError> {
    let input = attachments
        .active_inputs()
        .last()
        .ok_or(AttachmentError::Unattached)?;
    let Some(flux) = flux(request)? else {
        return Ok(CasaValue::Dict {
            entries: Vec::new(),
        });
    };
    let (reply, next) = input.with_write(|ms, _, lock| {
        setjy_holding(ms, lock, input.options().use_scratch, request, &flux)
    })?;
    if let Some(selection) = next {
        attachments.retain_setjy_selection(selection)?;
    }
    Ok(reply)
}

fn setjy_holding(
    ms: &mut MeasurementSet,
    lock: &mut TableWriteLock,
    scratch: bool,
    request: &ImagerSetJyRequest,
    flux: &FluxParameters,
) -> Result<(CasaValue, Option<MsSelection>), AttachmentError> {
    let mut selected =
        resolve_casa_calibration_selection(ms, &request.selection).map_err(preparation)?;
    if selected.selected_rows.is_empty() {
        return Ok((super::flux_dictionary(Vec::new()), None));
    }
    selected.channel_selections.clear();
    let windows = requested_windows(ms, request.selection.spw.as_deref())
        .map_err(|error| preparation(error.message))?;
    let (models, reply, completed) = prepare(
        ms,
        &selected,
        flux,
        &windows,
        request.by_channel,
        request.image.as_deref().map(|image| (image, scratch)),
    )
    .map_err(|error| preparation(error.message))?;
    let next = models.keys().next().and_then(|&field| {
        let window = selected
            .selected_rows
            .iter()
            .filter(|row| row.field_id == field)
            .map(|row| row.spectral_window_id)
            .max()?;
        let mut selection = request.selection.clone();
        selection.field = Some(field.to_string());
        selection.spw = Some(window.to_string());
        Some(selection)
    });
    if scratch {
        if !completed || models.values().any(|model| model.entries.is_empty()) {
            selected.selected_rows.retain(|row| {
                models
                    .get(&row.field_id)
                    .is_some_and(|model| !model.entries.is_empty())
            });
        }
        let phase_directions =
            matches!(flux, FluxParameters::Solar2010(_)) && request.image.is_none();
        super::publish_scratch(ms, lock, &selected, models, phase_directions)
            .map_err(|error| preparation(error.message))?;
    } else {
        super::publish_virtual(ms, lock, models, &windows)
            .map_err(|error| preparation(error.message))?;
    }
    Ok((reply, next))
}

fn flux(request: &ImagerSetJyRequest) -> Result<Option<FluxParameters>, AttachmentError> {
    let intensity = request.flux_jy[0];
    let solar = matches!(
        request.standard.as_str(),
        "Butler-JPL-Horizons 2010" | "Butler-JPL-Horizons 2012"
    );
    let catalog = FluxScale::named(&request.standard);
    if !solar && catalog.is_none() {
        return Ok(None);
    }
    if intensity < 0. && solar {
        return Ok(Some(FluxParameters::Solar2010(request.workspace.clone())));
    }
    if intensity < 0.
        && let Some(scale) = catalog
    {
        let interpolation = if scale.time_interpolation() {
            match request.interpolation.as_str() {
                "nearest" => Curve::Nearest,
                "linear" => Curve::Linear,
                "spline" => Curve::Spline,
                "cubic" => Curve::Cubic,
                _ => return Ok(None),
            }
        } else {
            Curve::Nearest
        };
        let catalog = FluxCatalog::open(scale).map_err(preparation)?;
        return Ok(Some(FluxParameters::Standard {
            catalog,
            interpolation,
        }));
    }
    let flux = ManualFlux::new(
        request.flux_jy,
        request.reference.clone(),
        request.spectral_index.clone(),
        request.polarization_index.clone(),
        request.polarization_angle.clone(),
        request.rotation_measure,
    )
    .map_err(preparation)?;
    Ok(Some(FluxParameters::Manual(flux)))
}

fn preparation(error: impl std::fmt::Display) -> AttachmentError {
    AttachmentError::PredictionPreparation(MsError::InvalidInput(error.to_string()))
}
