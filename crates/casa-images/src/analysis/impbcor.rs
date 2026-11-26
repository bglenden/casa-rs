// SPDX-License-Identifier: LGPL-3.0-or-later
//! Primary-beam correction of a selected image lattice.

pub(super) mod beam;

use std::fs;
use std::io;
use std::ops::{Div, Mul};
use std::path::{Path, PathBuf};

use casa_contracts::{ParameterValue, parameter_value_to_casacore};
use casa_coordinates::CoordinateType;
use casa_tables::publication::{exchange_directories, rename_without_replacing};
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayD, RecordValue, Value};
use ndarray::{IxDyn, Zip};

use super::imsubimage::{SubimagePlan, SubimageRegion, region::SubimageSelection};
use super::{combined_selection_mask, is_fits_file};
use crate::{
    AnyPagedImage, ImageError, ImagePixel, ImpbcorRequest, ImportFitsRequest, PagedImage,
    PbcorBeam, PbcorRegion, PbcorSummary, importfits,
};

pub(super) fn run(request: &ImpbcorRequest) -> Result<PbcorSummary, ImageError> {
    let correction = Correction::parse(&request.mode)?;
    let destination = destination(request)?;
    let input = import_fits_for_reading(&request.imagename)?;
    let image = AnyPagedImage::open(
        input
            .as_ref()
            .map_or(request.imagename.as_path(), |(_, path)| path),
    )?;
    let beam_path = match &request.pbimage {
        PbcorBeam::Image(path) => Some(path.as_path()),
        PbcorBeam::Pixels(_) => None,
    };
    let converted = beam_path
        .map(import_fits_for_reading)
        .transpose()?
        .flatten();
    let beam = beam_path
        .map(|path| AnyPagedImage::open(converted.as_ref().map_or(path, |(_, path)| path)))
        .transpose()?;
    match (&image, &beam) {
        (AnyPagedImage::Float32(image), Some(AnyPagedImage::Float32(beam))) => {
            run_typed(image, Some(beam), request, correction, &destination)
        }
        (AnyPagedImage::Float64(image), Some(AnyPagedImage::Float64(beam))) => {
            run_typed(image, Some(beam), request, correction, &destination)
        }
        (AnyPagedImage::Float32(image), None) => {
            run_typed(image, None, request, correction, &destination)
        }
        (AnyPagedImage::Float64(image), None) => {
            run_typed(image, None, request, correction, &destination)
        }
        _ => Err(ImageError::InvalidMetadata(
            "impbcor requires matching real-valued image pixel types".into(),
        )),
    }
}

fn import_fits_for_reading(
    path: &Path,
) -> Result<Option<(tempfile::TempDir, PathBuf)>, ImageError> {
    if !path.is_file() || !is_fits_file(path)? {
        return Ok(None);
    }
    let stage = tempfile::tempdir().map_err(output_error)?;
    let imagename = stage.path().join("image");
    importfits(&ImportFitsRequest {
        fitsimage: path.to_path_buf(),
        imagename: imagename.clone(),
        zeroblanks: false,
        ..ImportFitsRequest::default()
    })?;
    Ok(Some((stage, imagename)))
}

fn run_typed<T>(
    image: &PagedImage<T>,
    pb: Option<&PagedImage<T>>,
    request: &ImpbcorRequest,
    correction: Correction,
    destination: &Path,
) -> Result<PbcorSummary, ImageError>
where
    T: ImagePixel + Into<f64> + From<f32> + Copy + Mul<Output = T> + Div<Output = T>,
{
    let record = region_record(image, request.region.as_ref())?;
    let region = match request.region.as_ref() {
        Some(PbcorRegion::Text(text)) if record.is_none() && !text.trim().is_empty() => {
            Some(SubimageRegion::Text(text))
        }
        _ => record.as_ref().map(SubimageRegion::Record),
    };
    if region.is_some() {
        let supplied = |value: Option<&str>| value.is_some_and(|text| !text.trim().is_empty());
        if supplied(request.box_selection.as_deref()) {
            return Err(ImageError::CasaSelection(
                "box and regionPtr cannot be simultaneously specified".into(),
            ));
        }
        if supplied(request.chans.as_deref()) || supplied(request.stokes.as_deref()) {
            return Err(ImageError::CasaSelection(
                "regionPtr and regionName, chans, and/or stokes cannot be simultaneously specified"
                    .into(),
            ));
        }
    }
    let chans = request.chans.as_deref().map(str::trim).filter(|text| {
        !text.eq_ignore_ascii_case("ALL") && image.find_axis(CoordinateType::Spectral).is_some()
    });
    let stokes = normalize_stokes(image, request.stokes.as_deref());
    let box_pixels = request
        .box_selection
        .as_deref()
        .filter(|text| !text.trim().is_empty())
        .map(|text| normalize_box(image, text))
        .transpose()?;
    let selection = SubimageSelection::resolve(
        image,
        box_pixels.as_deref(),
        region,
        chans,
        stokes.as_deref(),
    )?;
    if selection.has_multiple_regions {
        return Err(ImageError::CasaSelection(
            "Only a single n-dimensional rectangular region is supported".into(),
        ));
    }
    let mut data = image.get_slice(&selection.start, &selection.shape)?;
    let mut mask =
        combined_selection_mask(image, &selection, request.mask.as_deref(), request.stretch)?;
    selection.apply_selection_masks(&mut mask);
    let beam = match (&request.pbimage, pb) {
        (PbcorBeam::Pixels(values), _) => beam::array(image, values, &selection)?,
        (PbcorBeam::Image(_), Some(beam)) => beam::image(image, beam, &selection)?,
        _ => unreachable!("a named primary beam was opened before execution"),
    };
    let mut valid = mask.unwrap_or_else(|| ArrayD::from_elem(IxDyn(&selection.shape), true));
    correction.apply_to_window(
        &mut data,
        &mut valid,
        &beam.pixels,
        beam.mask.as_ref(),
        request.cutoff as f32,
    )?;
    let coords = match image.table().keywords().get("coords") {
        Some(Value::Record(record)) => Some(record.clone()),
        _ => None,
    };
    let plan = SubimagePlan::from_selection(image, coords, selection, false, &[], None)?;
    let valid_pixels = valid.iter().filter(|&&good| good).count();
    let parent = destination.parent().ok_or(ImageError::NotPersistent)?;
    let stage = tempfile::tempdir_in(parent).map_err(output_error)?;
    let product = stage.path().join("image");
    let mut output = PagedImage::<T>::create(plan.shape.clone(), plan.coordinates, &product)?;
    if let Some(record) = plan.coordinate_record {
        output.set_coords_record_keyword(record);
    }
    output.set_units(image.units())?;
    output.set_image_info(&plan.image_info)?;
    output.set_misc_info(image.misc_info())?;
    output.put_slice(&data, &vec![0; plan.shape.len()])?;
    if valid_pixels != valid.len() {
        output.put_mask("mask0", &valid)?;
        output.set_default_mask("mask0")?;
    }
    let pb_label = match &request.pbimage {
        PbcorBeam::Image(path) => path.display().to_string(),
        PbcorBeam::Pixels(_) => format!("floating-point beam array {:?}", beam.input_shape),
    };
    write_history(&mut output, request, &pb_label)?;
    output.save()?;
    drop(output);
    match rename_without_replacing(&product, destination) {
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists && request.overwrite => {
            check_destination(destination, request)?;
            exchange_directories(&product, destination).map_err(output_error)?;
        }
        result => result.map_err(output_error)?,
    }
    Ok(PbcorSummary {
        imagename: request.imagename.display().to_string(),
        pbimage: pb_label,
        outfile: request.outfile.display().to_string(),
        mode: request.mode.clone(),
        cutoff: request.cutoff,
        shape: plan.shape,
        units: image.units().to_owned(),
        valid_pixels,
    })
}

pub(super) fn normalize_stokes<T: ImagePixel>(
    image: &dyn crate::ImageInterface<T>,
    text: Option<&str>,
) -> Option<String> {
    text.map(str::trim)
        .map(|text| text.trim_start_matches('[').trim_end_matches(']').trim())
        .filter(|text| {
            !text.eq_ignore_ascii_case("ALL") && image.find_axis(CoordinateType::Stokes).is_some()
        })
        .map(|text| text.replace(';', ","))
}

pub(super) fn normalize_box<T: ImagePixel>(
    image: &dyn crate::ImageInterface<T>,
    text: &str,
) -> Result<String, ImageError> {
    let plane = super::axes::direction_plane_axes(image)?;
    text.split(',')
        .enumerate()
        .map(|(i, value)| {
            let axis_len = image.shape()[plane[i % 2]];
            let coordinate = value.trim().parse::<f64>().map_err(|_| {
                ImageError::CasaSelection(format!("invalid numeric box corner {value:?}"))
            })?;
            if !coordinate.is_finite() || !(0.0..axis_len as f64).contains(&coordinate) {
                return Err(ImageError::CasaSelection(format!(
                    "box corner {coordinate} is outside the image"
                )));
            }
            Ok((coordinate.round() as usize).min(axis_len - 1).to_string())
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|values| values.join(","))
}

fn region_record<T: ImagePixel>(
    image: &PagedImage<T>,
    region: Option<&PbcorRegion>,
) -> Result<Option<RecordValue>, ImageError> {
    match region {
        Some(PbcorRegion::Record(fields)) if !fields.is_empty() => {
            let value = parameter_value_to_casacore(&ParameterValue::Table(fields.clone()))
                .map_err(|error| {
                    ImageError::CasaSelection(format!("invalid region record: {error}"))
                })?;
            let Value::Record(record) = value else {
                unreachable!("a parameter table converts to a record")
            };
            Ok(Some(record))
        }
        Some(PbcorRegion::Text(name))
            if !name.trim().is_empty()
                && !name.contains(['[', ':', '\n'])
                && !Path::new(name).exists() =>
        {
            Ok(image.get_region_record(name.trim()).ok())
        }
        _ => Ok(None),
    }
}

#[derive(Clone, Copy)]
pub(super) enum Correction {
    Divide,
    Multiply,
}

impl Correction {
    pub(super) fn parse(mode: &str) -> Result<Self, ImageError> {
        match mode.trim().as_bytes().first().map(u8::to_ascii_lowercase) {
            Some(b'd') => Ok(Self::Divide),
            Some(b'm') => Ok(Self::Multiply),
            _ => Err(ImageError::CasaSelection(format!("Unknown mode {mode}"))),
        }
    }

    fn keeps(self, beam: f64, cutoff: f32) -> bool {
        cutoff.is_nan()
            || cutoff < 0.0
            || match self {
                Self::Divide => beam >= f64::from(cutoff),
                Self::Multiply => beam <= f64::from(cutoff),
            }
    }

    pub(super) fn apply_to_window<T>(
        self,
        data: &mut ArrayD<T>,
        validity: &mut ArrayD<bool>,
        beam: &ArrayD<T>,
        beam_mask: Option<&ArrayD<bool>>,
        cutoff: f32,
    ) -> Result<(), ImageError>
    where
        T: Copy + Into<f64> + Div<Output = T> + Mul<Output = T>,
    {
        let shape = data.raw_dim();
        let pixels = beam
            .broadcast(shape.clone())
            .ok_or_else(|| ImageError::ShapeMismatch {
                expected: data.shape().to_vec(),
                got: beam.shape().to_vec(),
            })?;
        Zip::from(data)
            .and(&mut *validity)
            .and(pixels)
            .for_each(|pixel, good, &pb| {
                *good &= self.keeps(pb.into(), cutoff);
                *pixel = self.apply(*pixel, pb);
            });
        if let Some(mask) = beam_mask {
            let mask = mask.broadcast(shape).expect("beam and mask share a shape");
            Zip::from(validity)
                .and(mask)
                .for_each(|good, &mask| *good &= mask);
        }
        Ok(())
    }

    fn apply<T: Div<Output = T> + Mul<Output = T>>(self, image: T, beam: T) -> T {
        match self {
            Self::Divide => image / beam,
            Self::Multiply => image * beam,
        }
    }
}

fn write_history<T: ImagePixel>(
    output: &mut PagedImage<T>,
    request: &ImpbcorRequest,
    beam: &str,
) -> Result<(), ImageError> {
    let source = &request.imagename;
    if let PbcorBeam::Image(path) = &request.pbimage {
        for input in [source, path] {
            output.append_history_at(
                "::impbcor",
                &[format!("History of image {}", input.display())],
            )?;
            output.append_history_entries(&crate::image::image_history_rows(input)?)?;
            output.append_history_at(
                "::impbcor",
                &[format!("End history of image {}", input.display())],
            )?;
        }
    } else {
        output.append_history_entries(&crate::image::image_history_rows(source)?)?;
    }
    output.append_history_at("::impbcor", &[
        concat!("version: casa-rs ", env!("CARGO_PKG_VERSION")).into(),
        format!(
        "impbcor(imagename={:?}, pbimage={beam:?}, outfile={:?}, overwrite={}, box={:?}, region={:?}, chans={:?}, stokes={:?}, mask={:?}, mode={:?}, cutoff={}, stretch={})",
        source.display().to_string(), request.outfile.display().to_string(), request.overwrite,
        request.box_selection, request.region, request.chans, request.stokes, request.mask, request.mode,
        request.cutoff, request.stretch,
    )])
}

fn destination(request: &ImpbcorRequest) -> Result<PathBuf, ImageError> {
    let name = request
        .outfile
        .file_name()
        .ok_or_else(|| ImageError::CasaSelection("outfile must be specified".into()))?;
    let output = std::path::absolute(&request.outfile).map_err(output_error)?;
    let parent = output
        .parent()
        .ok_or(ImageError::NotPersistent)?
        .canonicalize()
        .map_err(output_error)?;
    let destination = parent.join(name);
    check_destination(&destination, request)?;
    Ok(destination)
}

fn check_destination(destination: &Path, request: &ImpbcorRequest) -> Result<(), ImageError> {
    let beam = match &request.pbimage {
        PbcorBeam::Image(path) => Some(path.as_path()),
        PbcorBeam::Pixels(_) => None,
    };
    for input in [Some(request.imagename.as_path()), beam]
        .into_iter()
        .flatten()
    {
        super::ensure_outfile_does_not_alias_input(destination, input)?;
        if input
            .canonicalize()
            .is_ok_and(|input| destination.starts_with(input))
        {
            return Err(ImageError::CasaSelection(
                "outfile must be outside the input image".into(),
            ));
        }
    }
    let metadata = match destination.symlink_metadata() {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(output_error(error)),
    };
    if !request.overwrite {
        return Err(ImageError::CasaSelection(format!(
            "impbcor output already exists: {}",
            destination.display()
        )));
    }
    if !metadata.is_dir()
        || !(Table::open_metadata_only(TableOptions::new(destination))
            .is_ok_and(|table| table.info().table_type == "Image")
            || fs::read_dir(destination).is_ok_and(|mut entries| entries.next().is_none()))
    {
        return Err(ImageError::CasaSelection(format!(
            "{} is not a CASA image or an empty directory",
            destination.display()
        )));
    }
    Ok(())
}

fn output_error(error: io::Error) -> ImageError {
    ImageError::Io(error.to_string())
}
