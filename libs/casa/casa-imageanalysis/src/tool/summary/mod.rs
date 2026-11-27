// SPDX-License-Identifier: LGPL-3.0-or-later

//! Image summary metadata and text report generation (`ia.summary`).

pub(crate) mod coordinates;
pub(crate) mod plane_beams;

use std::fmt::Write;

use casa_coordinates::CoordinateSystem;
use casa_coordinates::display::format_stream_value;
use casa_core::RecordValue;
use casa_core::measures::doppler::DopplerRef;
use casa_images::{ImageInterface, ImageType};

use super::attachment::AttachedImage;
use super::beams::{image_info, restoring_beam_record};
use super::error::{ImageToolResult, ToolLogLine};
use super::handle::{Storage, with_pixels, with_storage};
use coordinates::{format_coordinate_summary_report, summary_coordinate_fields};
use plane_beams::{append_plane_beam_summary, normalized_angle};

/// Structured summary of an image's header, axes, masks, and beams (`ia.summary`).
#[derive(Debug, Clone)]
pub struct ImageSummaryReport {
    /// Image type string (e.g. `"Intensity"`).
    pub imagetype: String,
    /// Number of pixel axes.
    pub ndim: usize,
    /// Full pixel shape.
    pub shape: Vec<usize>,
    /// Tile/chunk shape on disk.
    pub tileshape: Vec<usize>,
    /// Pixel or world axis names.
    pub axisnames: Vec<String>,
    /// 0-based reference pixel per pixel axis.
    pub refpix: Vec<f64>,
    /// Reference value per axis.
    pub refval: Vec<f64>,
    /// Coordinate increment per axis.
    pub incr: Vec<f64>,
    /// Unit string per axis.
    pub axisunits: Vec<String>,
    /// Brightness unit string.
    pub unit: String,
    /// Name of the default mask, if any.
    pub defaultmask: String,
    /// Names of all stored pixel masks.
    pub masks: Vec<String>,
    /// Whether the image has any mask.
    pub hasmask: bool,
    /// Object name from metadata.
    pub object_name: String,
    /// Formatted header and coordinate table sections for logger output.
    pub messages: Vec<String>,
    /// Effective `imageinfo` record (with attachment beam view applied).
    pub imageinfo: RecordValue,
    /// Single restoring beam record when a single beam is present.
    pub restoringbeam: Option<RecordValue>,
    /// Per-plane restoring beam record when multiple beams are present.
    pub perplanebeams: Option<RecordValue>,
}

/// Options controlling `ia.summary`.
#[derive(Debug, Clone)]
pub struct SummaryOptions {
    /// Selected velocity Doppler frame (`""` uses `"RADIO"`).
    pub doppler: String,
    /// Log formatted report lines to CASA logger when `true`.
    pub list: bool,
    /// Report axis descriptors in pixel order when `true`, world order when `false`.
    pub pixel_order: bool,
    /// Show velocity along spectral axis.
    pub velocity: bool,
    /// Use optical velocity convention.
    pub optical: bool,
    /// Include verbose per-plane beam details.
    pub verbose: bool,
}

impl Default for SummaryOptions {
    fn default() -> Self {
        Self {
            doppler: String::new(),
            list: true,
            pixel_order: true,
            velocity: false,
            optical: false,
            verbose: false,
        }
    }
}

fn mask_description(masks: &[String], default: &str, has_mask: bool) -> String {
    match masks {
        [] if has_mask => "Parent is masked".into(),
        [] => "None".into(),
        [only] if only == default => default.into(),
        _ => {
            let other = masks
                .iter()
                .filter(|name| name.as_str() != default)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ");
            format!("{default} [{other}]")
        }
    }
}

/// Summarizes `attached` according to `options`, returning the structured report and logs.
pub fn summarize_image(
    attached: &AttachedImage,
    options: &SummaryOptions,
) -> ImageToolResult<(ImageSummaryReport, Vec<ToolLogLine>)> {
    let mut logs = Vec::new();
    let doppler_str = if options.doppler.is_empty() {
        "RADIO"
    } else {
        options.doppler.as_str()
    };
    let doppler = if doppler_str.eq_ignore_ascii_case("TRUE") {
        DopplerRef::BETA
    } else {
        doppler_str.parse().unwrap_or_else(|_| {
            logs.push(ToolLogLine::warning("Illegal velocity type, using RADIO"));
            DopplerRef::RADIO
        })
    };

    let mut image = attached.lock();
    attached.require_lock(&mut image, casa_tables::LockType::Read)?;
    let beam_view = attached.beam_view();

    let shape = image.shape().to_vec();
    let tileshape = if let Some(fits) = image.fits() {
        fits.tile_shape.clone()
    } else {
        with_pixels!(&*image, storage => match storage {
            Storage::Paged(image) => image.tile_shape().to_vec(),
            Storage::Temp { image, .. } if image.is_in_memory() => image.shape().to_vec(),
            Storage::Temp { image, .. } => {
                let lattice: &dyn ImageInterface<_> = image.as_ref();
                lattice.nice_cursor_shape()
            }
        })
    };
    let (has_mask_raw, defaultmask, masks, unit) = with_pixels!(&*image, storage =>
        with_storage!(storage, img => (
            img.has_pixel_mask(),
            img.default_mask_name().unwrap_or_default(),
            img.mask_names(),
            img.units().to_owned(),
        ))
    );
    let hasmask = image.fits().map_or(has_mask_raw, |fits| fits.has_mask);
    let regions = with_pixels!(&*image, storage => match storage {
        Storage::Paged(image) => image.region_names(),
        Storage::Temp { .. } => Vec::new(),
    });
    let name = image.path().and_then(|path| path.file_name()).map_or_else(
        || "Temporary_Image".into(),
        |name| name.to_string_lossy().into_owned(),
    );
    let kind = image.image_type_name();
    let mut info = image_info(&image)?;
    let mut info_record = info.to_record();
    beam_view.apply_to(&mut info_record);
    if let Ok(parsed) = casa_images::ImageInfo::from_record(&info_record) {
        info = parsed;
    }
    let imagetype = match info.image_type {
        ImageType::Undefined => ImageType::Intensity,
        other => other,
    }
    .to_string();
    let masks_text = mask_description(&masks, &defaultmask, hasmask);
    let regions_text = if regions.is_empty() {
        "None".into()
    } else {
        regions.join(", ")
    };
    let mut header = format!(
        "\nImage name       : {name}\nObject name      : {}\nImage type       : {kind}\nImage quantity   : {imagetype}\nPixel mask(s)    : {masks_text}\nRegion(s)        : {regions_text}",
        info.object_name
    );
    if !unit.is_empty() {
        write!(header, "\nImage units      : {unit}").unwrap();
    }

    let record = image.coordinate_record();
    let coords = CoordinateSystem::from_record(&record)?;
    let fields = summary_coordinate_fields(&record, options.pixel_order)?;
    let beams = beam_view
        .beam_set()
        .map_err(|err| super::error::ImageToolError::new(err.to_string()))?;

    let mut restoringbeam = None;
    let mut perplanebeams = None;
    if let Some(beam) = beams.single_beam() {
        let width_unit = if beam.minor_in("deg")? < 1.0 {
            "arcsec"
        } else {
            "deg"
        };
        let major = format_stream_value(beam.major_in(width_unit)?);
        let minor = format_stream_value(beam.minor_in(width_unit)?);
        let pa = format_stream_value(normalized_angle(&beam)?);
        write!(
            header,
            "\nRestoring Beam   : {major} {width_unit}, {minor} {width_unit}, {pa} deg"
        )
        .unwrap();
        restoringbeam = Some(restoring_beam_record(
            &beam_view,
            || record.clone(),
            -1,
            -1,
            "list",
        )?);
    } else if beams.is_multi() {
        append_plane_beam_summary(
            &mut header,
            &record,
            &coords,
            &beams,
            options.list && options.verbose,
        )?;
        perplanebeams = Some(restoring_beam_record(
            &beam_view,
            || record.clone(),
            -1,
            -1,
            "list",
        )?);
    }

    let coord_report =
        format_coordinate_summary_report(&record, &coords, &shape, &tileshape, doppler)?;
    let messages = vec![header, coord_report];
    if options.list {
        logs.extend(messages.iter().cloned().map(ToolLogLine::info));
    }

    Ok((
        ImageSummaryReport {
            imagetype,
            ndim: shape.len(),
            shape,
            tileshape,
            axisnames: fields.axisnames,
            refpix: fields.refpix,
            refval: fields.refval,
            incr: fields.incr,
            axisunits: fields.axisunits,
            unit,
            defaultmask,
            masks,
            hasmask,
            object_name: info.object_name,
            messages,
            imageinfo: info_record,
            restoringbeam,
            perplanebeams,
        },
        logs,
    ))
}
