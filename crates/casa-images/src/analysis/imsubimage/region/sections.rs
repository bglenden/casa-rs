// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::{RecordValue, ScalarValue, Value};

use crate::{ImageError, ImageInterface, ImagePixel, PolygonRegion};

use super::super::SubimageRegion;
use super::{EMPTY_SECTION, SubimageSelection, direction_plane_axes, empty_section, saved};

pub(in crate::analysis::imsubimage) struct StridedSection {
    pub(in crate::analysis::imsubimage) start: Vec<usize>,
    pub(in crate::analysis::imsubimage) count: Vec<usize>,
    pub(in crate::analysis::imsubimage) stride: Vec<usize>,
}

impl StridedSection {
    pub(in crate::analysis::imsubimage) fn resolve<T: ImagePixel>(
        image: &dyn ImageInterface<T>,
        region: Option<SubimageRegion<'_>>,
    ) -> Result<Self, ImageError> {
        if let Some(SubimageRegion::Record(record)) = region {
            match record.get("name") {
                Some(Value::Scalar(ScalarValue::String(name))) if name == "WCPolygon" => {
                    if let Some(section) =
                        saved::relative_polygon_section(record, image.coordinates(), image.shape())?
                    {
                        return Ok(section);
                    }
                    let polygon =
                        PolygonRegion::from_record("region", record, image.coordinates())?;
                    let bounds = polygon
                        .bounds(image.coordinates(), image.shape())?
                        .ok_or_else(empty_section)?;
                    return Ok(Self {
                        start: bounds.start,
                        count: bounds.shape,
                        stride: vec![1; image.ndim()],
                    });
                }
                Some(Value::Scalar(ScalarValue::String(name))) if name == "WCUnion" => {
                    return Self::union(image, record);
                }
                Some(Value::Scalar(ScalarValue::String(name)))
                    if matches!(
                        name.as_str(),
                        "WCIntersection"
                            | "WCDifference"
                            | "WCComplement"
                            | "WCExtension"
                            | "WCConcatenation"
                    ) =>
                {
                    return saved::compound_section(
                        record,
                        image.coordinates(),
                        image.shape(),
                        direction_plane_axes(image)?,
                    );
                }
                _ => {}
            }
            if let Some(section) =
                saved::lc_slicer_section(record, image.coordinates(), image.shape())?
            {
                return Ok(section);
            }
        }
        let selection = SubimageSelection::resolve_section(image, None, region, None, None)?;
        Ok(Self {
            count: selection.packed_shape(),
            start: selection.start,
            stride: selection.stride.unwrap_or_else(|| vec![1; image.ndim()]),
        })
    }

    fn union<T: ImagePixel>(
        image: &dyn ImageInterface<T>,
        record: &RecordValue,
    ) -> Result<Self, ImageError> {
        let Some(Value::Record(members)) = record.get("regions") else {
            return Err(ImageError::InvalidMetadata(
                "WCUnion is missing its regions".into(),
            ));
        };
        let mut start = image.shape().to_vec();
        let mut end = vec![0; image.ndim()];
        for member in members.fields().iter().filter(|field| field.name != "nr") {
            let Value::Record(child) = &member.value else {
                return Err(ImageError::InvalidMetadata(
                    "WCUnion member is missing its region record".into(),
                ));
            };
            let section = match Self::resolve(image, Some(SubimageRegion::Record(child))) {
                Ok(section) => section,
                Err(ImageError::CasaSelection(message)) if message == EMPTY_SECTION => continue,
                Err(error) => return Err(error),
            };
            for axis in 0..image.ndim() {
                start[axis] = start[axis].min(section.start[axis]);
                end[axis] = end[axis].max(
                    section.start[axis] + (section.count[axis] - 1) * section.stride[axis] + 1,
                );
            }
        }
        if end.contains(&0) {
            return Err(empty_section());
        }
        let count = end
            .into_iter()
            .zip(&start)
            .map(|(end, start)| end - start)
            .collect();
        Ok(Self {
            start,
            count,
            stride: vec![1; image.ndim()],
        })
    }
}
