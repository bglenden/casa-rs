// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA moment images from a selected, masked image.

mod axis;
mod collapse;
mod output;
mod smooth;
mod tool;
mod window;

pub use tool::{ImageMomentOptions, ImageMomentProducts, make_image_moments, moment_output_names};

use crate::analysis::MomentAxis;
use crate::image::{ImageHistoryEntry, image_history_entries};
use casa_core::{RecordValue, Value};

use super::axes::{subimage_coordinate_record, subimage_coordinates_with_stokes};
use super::imsubimage::{SubimageRegion, coords::subset_beam_set, region::SubimageSelection};
use super::{combined_selection_mask, patch_saved_coords_keyword};
use crate::{
    ImageBeamSet, ImageError, ImageInterface, ImagePixel, ImmomentsRequest, MomentImageSummary,
    MomentMapSummary, PagedImage, TempImage,
};

pub(super) use self::collapse::PixelClip;
#[cfg(test)]
pub(super) use self::{axis::ProfileCoordinates, collapse::collapse_moment};

pub(super) fn run_paged<T>(
    image: &PagedImage<T>,
    request: &ImmomentsRequest,
) -> Result<MomentMapSummary, ImageError>
where
    T: ImagePixel + Into<f64> + From<f32> + Copy,
{
    let record = match image.table().keywords().get("coords") {
        Some(Value::Record(record)) => Some(record),
        _ => None,
    };
    let history = image_history_entries(&request.imagename)?;
    run(image, record, &history, request)
}

pub(super) fn run<T>(
    image: &dyn ImageInterface<T>,
    record: Option<&RecordValue>,
    history: &[ImageHistoryEntry],
    request: &ImmomentsRequest,
) -> Result<MomentMapSummary, ImageError>
where
    T: ImagePixel + Into<f64> + From<f32> + Copy,
{
    let clip = PixelClip::from_request(request)?;
    let destinations = output::destinations(request, clip)?;
    let selection = SubimageSelection::resolve(
        image,
        request.box_pixels.as_deref(),
        request.region.as_deref().map(SubimageRegion::Text),
        request.chans.as_deref(),
        request.stokes.as_deref(),
    )?;
    let axis = axis::resolve(image, request.axis.as_ref(), &selection.shape)?;
    let mut input = image.get_slice(&selection.start, &selection.shape, &vec![1; image.ndim()])?;
    let mut mask =
        combined_selection_mask(image, &selection, request.mask.as_deref(), request.stretch)?;
    selection.apply_selection_masks(&mut mask);
    let mut shape = selection.shape.clone();
    shape[axis] = 1;
    let coordinates = subimage_coordinates_with_stokes(
        image.coordinates(),
        &selection.start,
        &selection.shape,
        selection.selected_stokes.as_deref(),
    )?;
    let record = record.cloned().and_then(|record| {
        subimage_coordinate_record(
            record,
            &selection.start,
            &selection.shape,
            selection.selected_stokes.as_deref(),
        )
    });
    let mut info = image.image_info()?;
    info.beam_set = subset_beam_set(
        &info.beam_set,
        image.coordinates(),
        image.ndim(),
        &selection.start,
        &selection.shape,
    )?;
    let brightness = if image.find_axis(casa_coordinates::CoordinateType::Spectral) == Some(axis)
        && info.beam_set.is_multi()
    {
        let (target, brightness) = crate::convolve2d::convolve_to_common_beam(
            &mut input,
            mask.as_ref(),
            &coordinates,
            image.units(),
            &info.beam_set,
        )?;
        info.beam_set = ImageBeamSet::new(target);
        brightness
    } else {
        image.units().to_owned()
    };
    if selection.stokes_axis == Some(axis) && info.beam_set.n_stokes() > 1 {
        return Err(ImageError::CasaSelection(
            "Number of polarizations is not consistent with the restoring beam set".into(),
        ));
    }
    let axis = axis::MomentAxis::new(&coordinates, axis)?;
    let positions = axis.profiles(&selection.shape)?;
    let results = collapse::collapse_moments(
        &input,
        mask.as_ref(),
        axis.index,
        &positions,
        axis.integrated_scale()?,
        clip,
        &request.moments,
    );
    let misc = image.misc_info();
    let command = [
        concat!("version: casa-rs ", env!("CARGO_PKG_VERSION")).into(),
        task_history(request),
    ];
    let stage = destinations.stage()?;
    let mut products = Vec::with_capacity(destinations.paths.len());
    for (i, (&moment, result)) in request.moments.iter().zip(results).enumerate() {
        let units = moment_units(&brightness, &axis.unit, moment);
        let mut output = TempImage::<f32>::new(
            shape.clone(),
            coordinates.clone(),
            casa_lattices::TempStoragePolicy::Auto {
                memory_budget_bytes: 64 * 1024 * 1024,
                scratch: casa_lattices::ScratchSpace::SystemTemp,
            },
        )?;
        output.set_units(units.clone())?;
        output.set_image_info(&info)?;
        output.set_misc_info(misc.clone())?;
        output.put_slice(&result.values, &vec![0; shape.len()])?;
        output.put_mask("mask0", &result.valid)?;
        output.set_default_mask("mask0")?;
        let staged = stage.path().join(i.to_string());
        output.save_as(&staged)?;
        if let Some(record) = &record {
            patch_saved_coords_keyword(&staged, record.clone())?;
        }
        crate::image::append_image_history_entries(&staged, history)?;
        crate::image::add_image_history(&staged, "immoments", &command)?;
        products.push(MomentImageSummary {
            outfile: destinations.paths[i].display().to_string(),
            moment,
            units,
            valid_profiles: result.valid.iter().filter(|&&value| value).count(),
        });
    }
    destinations.publish(stage)?;
    Ok(MomentMapSummary {
        imagename: request.imagename.display().to_string(),
        shape,
        products,
    })
}

fn moment_units(brightness: &str, axis: &str, moment: i32) -> String {
    match moment {
        0 if !brightness.is_empty() && !axis.is_empty() => format!("{brightness}.{axis}"),
        0 => String::new(),
        2 if axis != "km/s" && !axis.is_empty() => format!("{axis}.{axis}"),
        1 | 2 | 4 | 9 | 11 => axis.to_owned(),
        _ => brightness.to_owned(),
    }
}

fn task_history(request: &ImmomentsRequest) -> String {
    let option = |value: Option<&str>| value.unwrap_or("").to_owned();
    let axis = match &request.axis {
        Some(MomentAxis::Index(index)) => index.to_string(),
        Some(MomentAxis::Name(name)) => format!("{name:?}"),
        None => "-1".to_string(),
    };
    format!(
        "immoments(imagename={:?}, moments={:?}, axis={}, region={:?}, box={:?}, chans={:?}, stokes={:?}, mask={:?}, includepix={:?}, excludepix={:?}, outfile={:?}, stretch={})",
        request.imagename.display().to_string(),
        request.moments,
        axis,
        option(request.region.as_deref()),
        option(request.box_pixels.as_deref()),
        option(request.chans.as_deref()),
        option(request.stokes.as_deref()),
        option(request.mask.as_deref()),
        request.includepix,
        request.excludepix,
        request.outfile.display().to_string(),
        if request.stretch { "True" } else { "False" },
    )
}
