// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_images::{ImageBeamSet, ImageError, ImageInfo};
use casa_types::RecordValue;

use super::beams::{image_info, image_info_record};
use super::handle::ImageHandle;

const BEAM_FIELDS: [&str; 2] = ["restoringbeam", "perplanebeams"];

/// PagedImage's cached beam metadata belongs to one open CASA image.
/// Pixels and the native table lock remain shared across local attachments.
#[derive(Clone, Default)]
pub(super) struct BeamView {
    record: RecordValue,
    extrema: [usize; 2],
    pub(super) empty_angle_degrees: f64,
}

impl BeamView {
    pub(super) fn from_image(image: &ImageHandle) -> Self {
        let mut view = Self::default();
        if let Some(info) = image_info_record(image) {
            view.capture(info);
        } else if let Ok(info) = image_info(image) {
            view.capture(&info.to_record());
        }
        view
    }

    pub(super) fn record(&self) -> &RecordValue {
        &self.record
    }

    pub(super) fn beam_set(&self) -> Result<ImageBeamSet, ImageError> {
        ImageInfo::from_record(&self.record).map(|info| info.beam_set)
    }

    pub(super) fn extrema(&self) -> [usize; 2] {
        self.extrema
    }

    pub(super) fn capture(&mut self, info: &RecordValue) {
        for field in BEAM_FIELDS {
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
