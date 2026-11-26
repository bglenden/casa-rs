// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA task policy around the shared FITS image reader.

use std::fs;
use std::io;
use std::path::Path;

use casa_coordinates::CoordinateSystem;
use casa_core::quanta::{Quantity, Unit, casacore::convert};
use casa_core::{RecordValue, Value};
use casa_tables::publication::{exchange_directories, rename_without_replacing};
use casa_tables::{Table, TableOptions};

use super::{FitsImportSummary, ImportFitsRequest, ImportFitsValue};
use casa_images::fits_read::FitsToolImport;
use casa_images::{ImageError, ImageInfo, PagedImage, StoredGaussianBeam};

mod axes;

/// Import the selected FITS image as a single-precision native CASA image.
///
/// The task applies FITS blanking, appends optional degenerate standard axes
/// in CASA order, and can override FITS restoring beams. Source metadata and
/// log rows are preserved. A fully written output is published atomically.
///
/// # Errors
///
/// Returns an error for an unreadable FITS image, invalid selection or
/// metadata, or an output that cannot be created or replaced.
pub fn importfits(request: &ImportFitsRequest) -> Result<FitsImportSummary, ImageError> {
    if request.imagename.as_os_str().is_empty() {
        return Err(reject("imagename must be specified"));
    }
    let output = std::path::absolute(&request.imagename).map_err(io_error)?;
    check_output(request, &output)?;
    let which_rep = usize::try_from(request.whichrep)
        .map_err(|_| reject("The Coordinate Representation index must be non-negative."))?;
    let beam = restoring_beam(&request.beam)?;
    let mut source = FitsToolImport::open(&request.fitsimage, which_rep, request.whichhdu)?;
    let order = axes::apply(&mut source, request)?;
    let shape = order
        .iter()
        .map(|&axis| source.shape().get(axis).copied().unwrap_or(1))
        .collect::<Vec<_>>();

    if let Some(beam) = beam {
        replace_restoring_beams(&mut source.image_info, beam);
    }
    let info = ImageInfo::from_record(&source.image_info)?;
    let stage = tempfile::tempdir_in(output.parent().ok_or_else(|| reject("invalid imagename"))?)
        .map_err(io_error)?;
    let product = stage.path().join("image");
    let coordinates = CoordinateSystem::from_record(&source.coordinate_record)?;
    let mut image = PagedImage::<f32>::create(shape.clone(), coordinates, &product)?;
    source.copy_pixels_with_axes(&mut image, request.zeroblanks, &order)?;
    image.set_coordinate_record(source.coordinate_record)?;
    image.set_units(source.units.clone())?;
    image.set_image_info_record(source.image_info)?;
    image.set_misc_info(source.misc_info)?;
    image.append_history_entries(&source.history)?;
    image.append_history_at("::importfits", &[task_history(request)])?;
    image.save()?;
    drop(image);

    match rename_without_replacing(&product, &output) {
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists && request.overwrite => {
            check_output(request, &output)?;
            exchange_directories(&product, &output).map_err(io_error)?;
        }
        result => result.map_err(io_error)?,
    }

    Ok(FitsImportSummary {
        fitsimage: request.fitsimage.display().to_string(),
        imagename: request.imagename.display().to_string(),
        shape,
        pixel_type: "Float32".into(),
        units: source.units,
        object_name: info.object_name,
        has_restoring_beam: info
            .beam_set
            .single_beam()
            .is_some_and(|beam| !beam.is_null()),
    })
}

fn check_output(request: &ImportFitsRequest, output: &Path) -> Result<(), ImageError> {
    let metadata = match output.symlink_metadata() {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io_error(error)),
    };
    if !request.overwrite {
        return Err(reject(
            "Output image already exists and you did not set overwrite to True.",
        ));
    }
    let existing = output.canonicalize().map_err(io_error)?;
    if request
        .fitsimage
        .canonicalize()
        .is_ok_and(|input| input.starts_with(existing))
    {
        return Err(reject("imagename matches or contains the input FITS file"));
    }
    let is_image = metadata.is_dir()
        && Table::open_metadata_only(TableOptions::new(output))
            .is_ok_and(|table| table.info().table_type == "Image");
    let is_empty =
        metadata.is_dir() && fs::read_dir(output).is_ok_and(|mut entries| entries.next().is_none());
    if !is_image && !is_empty {
        return Err(reject(format!(
            "{} is not a CASA image directory",
            output.display()
        )));
    }
    Ok(())
}

fn restoring_beam(values: &[ImportFitsValue]) -> Result<Option<StoredGaussianBeam>, ImageError> {
    if values.is_empty() {
        return Ok(None);
    }
    let [
        ImportFitsValue::String(major),
        ImportFitsValue::String(minor),
        ImportFitsValue::String(pa),
    ] = values
    else {
        return Err(invalid_type(
            "beam",
            "Parameter beam is invalid (should be list of 3 strings or empty)",
        ));
    };
    let radians = Unit::new("rad").expect("built-in angle unit");
    let seconds = Unit::new("s").expect("built-in time unit");
    let arcseconds = Unit::new("arcsec").expect("built-in angle unit");
    let angle = |text: &str| {
        Quantity::read(text)
            .ok()
            .filter(|angle| {
                angle.value().is_finite()
                    && (angle.unit().conformant(&radians) || angle.unit().conformant(&seconds))
            })
            .ok_or_else(|| {
                invalid_type(
                    "beam",
                    format!("Parameter beam is invalid (should be an angle): {text}"),
                )
            })
    };
    let major = angle(major)?;
    let minor = angle(minor)?;
    let pa = angle(pa)?;
    let as_arcseconds = |value: &Quantity| {
        convert(value, Some(&arcseconds))
            .map(|converted| converted.value())
            .map_err(|error| invalid_type("beam", error.to_string()))
    };
    if as_arcseconds(&major)? < as_arcseconds(&minor)? {
        return Err(invalid_type(
            "beam",
            "major axis must be greater or equal to minor axis",
        ));
    }
    // qa.toangle accepts time during validation, but ia.setrestoringbeam
    // receives the original units.
    for (label, angle) in [
        ("Major axis", &major),
        ("Minor axis", &minor),
        ("Position angle", &pa),
    ] {
        if !angle.unit().conformant(&radians) {
            return Err(reject(format!(
                "Exception: {label} must have angular units ({} is not)..",
                angle.unit().name()
            )));
        }
    }
    if major.value() <= 0.0 || minor.value() <= 0.0 {
        return Err(reject(
            "Both the major and minor axes must be non-negative.",
        ));
    }
    StoredGaussianBeam::new(major, minor, pa)
        .map(Some)
        .map_err(|error| invalid_type("beam", error.to_string()))
}

fn replace_restoring_beams(info: &mut RecordValue, beam: StoredGaussianBeam) {
    let beam = Value::Record(beam.to_record());
    if let Some(Value::Record(grid)) = info.get_mut("perplanebeams") {
        let entries: Vec<_> = grid
            .fields()
            .iter()
            .filter(|field| field.name.starts_with('*'))
            .map(|field| field.name.clone())
            .collect();
        for field in entries {
            grid.upsert(field, beam.clone());
        }
    } else {
        info.upsert("restoringbeam", beam);
    }
}

fn format_importfits_list(values: &[ImportFitsValue]) -> String {
    let items: Vec<String> = values
        .iter()
        .map(|value| match value {
            ImportFitsValue::String(text) => {
                if !text.contains('\'') {
                    format!("'{text}'")
                } else {
                    format!("{text:?}")
                }
            }
            ImportFitsValue::Integer(number) => number.to_string(),
            ImportFitsValue::Float(number) => format!("{number:?}"),
            ImportFitsValue::Bool(flag) => if *flag { "True" } else { "False" }.to_string(),
            ImportFitsValue::Other => "None".to_string(),
        })
        .collect();
    format!("[{}]", items.join(", "))
}

fn task_history(request: &ImportFitsRequest) -> String {
    let boolean = |flag| if flag { "True" } else { "False" };
    format!(
        "importfits(fitsimage={:?}, imagename={:?}, whichrep={}, whichhdu={}, zeroblanks={}, overwrite={}, defaultaxes={}, defaultaxesvalues={}, beam={})",
        request.fitsimage.display().to_string(),
        request.imagename.display().to_string(),
        request.whichrep,
        request.whichhdu,
        boolean(request.zeroblanks),
        boolean(request.overwrite),
        boolean(request.defaultaxes),
        format_importfits_list(&request.defaultaxesvalues),
        format_importfits_list(&request.beam),
    )
}

fn reject(message: impl Into<String>) -> ImageError {
    ImageError::CasaSelection(message.into())
}

fn invalid_type(parameter: &'static str, message: impl Into<String>) -> ImageError {
    ImageError::TaskArgumentType {
        parameter,
        message: message.into(),
    }
}

fn io_error(error: io::Error) -> ImageError {
    ImageError::Io(error.to_string())
}
