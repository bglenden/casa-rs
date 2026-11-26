// SPDX-License-Identifier: LGPL-3.0-or-later

use std::sync::Arc;

use casa_core::RecordValue;
use casa_images::{ImageBeamSet, ImageError, ImageInfo};

use super::beams::{image_info, image_info_record};
use super::handle::ImageHandle;

const BEAM_INFO_FIELDS: [&str; 3] = ["restoringbeam", "perplanebeams", "imagetype"];

/// PagedImage's cached beam metadata belongs to one open CASA image.
/// Pixels and the native table lock remain shared across local attachments.
#[derive(Clone, Default)]
pub(super) struct BeamView {
    record: RecordValue,
    invalid_info: Option<Arc<ImageError>>,
    extrema: [usize; 2],
    pub(super) empty_angle_degrees: f64,
}

impl BeamView {
    pub(super) fn from_image(image: &ImageHandle) -> Self {
        let mut view = Self::default();
        if let Some(info) = image_info_record(image) {
            view.capture(info);
        } else {
            match image_info(image) {
                Ok(info) => view.capture(&info.to_record()),
                Err(error) => view.invalid_info = Some(Arc::new(error)),
            }
        }
        view
    }

    pub(super) fn record(&self) -> &RecordValue {
        &self.record
    }

    pub(super) fn apply_to(&self, info: &mut RecordValue) {
        for field in BEAM_INFO_FIELDS {
            if let Some(value) = self.record.get(field) {
                info.upsert(field, value.clone());
            } else {
                info.remove(field);
            }
        }
    }

    pub(super) fn beam_set(&self) -> Result<ImageBeamSet, Arc<ImageError>> {
        if let Some(error) = &self.invalid_info {
            return Err(Arc::clone(error));
        }
        ImageInfo::from_record(&self.record)
            .map(|info| info.beam_set)
            .map_err(Arc::new)
    }

    pub(super) fn extrema(&self) -> [usize; 2] {
        self.extrema
    }

    pub(super) fn capture(&mut self, info: &RecordValue) {
        self.invalid_info = None;
        for field in BEAM_INFO_FIELDS {
            if let Some(beam) = info.get(field) {
                self.record.upsert(field, beam.clone());
            } else {
                self.record.remove(field);
            }
        }
        self.empty_angle_degrees = 0.0;
        let Ok(beams) = self.beam_set() else {
            return;
        };
        let count = beams.nelements();
        if count == 0 {
            self.extrema = [0, 0];
            return;
        }
        let area = |index| {
            let channels = beams.n_channels();
            beams.beam(index % channels, index / channels).area()
        };
        let [mut least, mut most] = self.extrema.map(|index| index.min(count - 1));
        for index in 0..count {
            let candidate = area(index);
            if candidate < area(least) {
                least = index;
            }
            if candidate > area(most) {
                most = index;
            }
        }
        // Equal-area replacements keep the previous selected plane.
        self.extrema = [least, most];
    }
}
