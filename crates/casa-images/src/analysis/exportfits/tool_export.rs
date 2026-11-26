// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selection and spectral options specific to the public image tool.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_coordinates::FitsValue;
use casa_core::RecordValue;

use crate::analysis::{
    ExportFitsRequest, FitsExportSummary, SPEED_OF_LIGHT_M_S, SubimageOptions, SubimagePlan,
    SubimageRegion, ensure_outfile_does_not_alias_input,
};
use crate::image::ImageHistoryEntry;
use crate::{ImageError, ImageInterface, ScratchSpace, TempImage, TempStoragePolicy};

use super::{FitsWriteOptions, export_fits_typed, validate_output_target};

/// Image-tool selection and spectral export options that differ from the task.
#[derive(Default)]
pub struct ImageFitsExportOptions<'a> {
    /// Selection from a CASA region record or region string.
    pub region: Option<SubimageRegion<'a>>,
    /// Additional LEL mask expression.
    pub mask: Option<&'a str>,
    /// Broadcast a compatible mask across additional axes.
    pub stretch: bool,
    /// Move remaining singleton axes to the end.
    pub deglast: bool,
    /// Write wavelength instead of frequency.
    pub wavelength: bool,
    /// Use wavelength in air when writing wavelength.
    pub airwavelength: bool,
    /// Saved restoring beams from the attached tool.
    pub beam_record: Option<&'a RecordValue>,
    /// Resolved paths read by a lazy expression attachment.
    pub operand_paths: &'a [PathBuf],
}

/// Check the destination of an attached image export before reading pixels.
///
/// # Errors
/// Returns an error for invalid output options or a destination that aliases an input.
pub fn validate_attached_fits_target<'a>(
    request: &ExportFitsRequest,
    inputs: impl IntoIterator<Item = &'a Path>,
) -> Result<(), ImageError> {
    for source in inputs {
        ensure_outfile_does_not_alias_input(&request.fitsimage, source)?;
    }
    validate_output_target(request)
}

/// Write a selected part of an attached float image using the shared FITS writer.
pub fn export_attached_fits(
    request: &ExportFitsRequest,
    image: &dyn ImageInterface<f32>,
    coordinates: RecordValue,
    history: &[ImageHistoryEntry],
    options: ImageFitsExportOptions<'_>,
) -> Result<FitsExportSummary, ImageError> {
    validate_attached_fits_target(
        request,
        image
            .name()
            .into_iter()
            .chain(options.operand_paths.iter().map(PathBuf::as_path)),
    )?;
    let wavelength = options.wavelength.then_some(if options.airwavelength {
        Wavelength::Air
    } else {
        Wavelength::Vacuum
    });
    let mut write_options = FitsWriteOptions {
        deglast: options.deglast,
        wavelength,
        tool_metadata: true,
        beam_record: options.beam_record,
    };
    if options.region.is_none() && options.mask.is_none() {
        return export_fits_typed(request, image, history, None, write_options);
    }
    let selection = SubimageOptions {
        region: options.region,
        dropdeg: false,
        keepaxes: &[],
    };
    let plan =
        SubimagePlan::new_with_beams(image, Some(coordinates), selection, options.beam_record)?;
    let mut selected = TempImage::new(
        plan.shape.clone(),
        plan.coordinates.clone(),
        TempStoragePolicy::Auto {
            memory_budget_bytes: 256 * 1024 * 1024,
            scratch: ScratchSpace::SystemTemp,
        },
    )?;
    crate::analysis::with_mask_output_paths(
        request.overwrite.then_some(request.fitsimage.as_path()),
        || plan.copy_to(image, &mut selected, options.mask, options.stretch),
    )?;
    write_options.beam_record = None;
    export_fits_typed(request, &selected, history, None, write_options)
}

#[derive(Clone, Copy)]
pub(super) enum Wavelength {
    Vacuum,
    Air,
}

impl Wavelength {
    pub(super) fn cards(
        self,
        axis: usize,
        frequency: f64,
        increment: f64,
    ) -> Result<BTreeMap<String, FitsValue>, ImageError> {
        if frequency <= 0.0 || frequency + increment <= 0.0 {
            return Err(ImageError::InvalidMetadata(
                "Zero or negative reference frequency".into(),
            ));
        }
        let wavelength = |frequency: f64| {
            let vacuum = SPEED_OF_LIGHT_M_S / frequency;
            match self {
                Self::Vacuum => vacuum,
                Self::Air => {
                    // FITS Paper III (Greisen et al. 2006), with wavelength in µm.
                    let squared = (vacuum * 1e6).powi(2);
                    vacuum
                        / (1.0 + 1e-6 * (287.6155 + 1.62887 / squared + 0.01360 / squared.powi(2)))
                }
            }
        };
        let reference = wavelength(frequency);
        let delta = wavelength(frequency + increment) - reference;
        let (unit, scale) = match reference {
            value if value >= 0.1 => ("m", 1.0),
            value if value >= 0.1e-3 => ("mm", 1e3),
            value if value >= 1e-6 => ("um", 1e6),
            _ => ("nm", 1e9),
        };
        let kind = match self {
            Self::Vacuum => "WAVE",
            Self::Air => "AWAV",
        };
        Ok(BTreeMap::from([
            (format!("CTYPE{axis}"), FitsValue::String(kind.into())),
            (format!("CUNIT{axis}"), FitsValue::String(unit.into())),
            (format!("CRVAL{axis}"), FitsValue::Float(reference * scale)),
            (format!("CDELT{axis}"), FitsValue::Float(delta * scale)),
        ]))
    }
}
