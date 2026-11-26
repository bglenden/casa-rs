// SPDX-License-Identifier: LGPL-3.0-or-later
//! `immoments` - CASA-style spectral moment maps, on the shared task front
//! end.

use crate::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};
use casa_contracts::ParameterValue;
use casa_images::{ImmomentsRequest, MomentAxis, immoments};

use super::front_end::{image_task_front_end, image_task_success};
use super::task_contract::ImageAnalysisTaskResult;

/// The front end of the `immoments` binary.
pub fn immoments_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    image_task_front_end("immoments", &["immoments"], run)
}

pub(crate) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let axis = match task.value("axis") {
        None => None,
        Some(ParameterValue::Integer(index)) => Some(MomentAxis::Index(*index)),
        Some(ParameterValue::String(name)) => Some(MomentAxis::Name(name.clone())),
        Some(_) => return Err(TaskFailure::execution("axis must be an index or axis name")),
    };
    let request = ImmomentsRequest {
        imagename: task.input_path("imagename")?,
        outfile: task.path("outfile")?,
        moments: moment_numbers(&task.integers("moments")?)?,
        axis,
        region: super::front_end::region_in_workspace(task)?,
        box_pixels: task.text("box")?,
        stokes: task.text("stokes")?,
        chans: task.text("chans")?,
        includepix: pixel_range("includepix", &task.numbers("includepix")?)?,
        excludepix: pixel_range("excludepix", &task.numbers("excludepix")?)?,
        mask: task.text("mask")?,
        stretch: task.flag("stretch")?,
        overwrite: task.flag("overwrite")?,
    };
    image_task_success(immoments(&request).map(ImageAnalysisTaskResult::Immoments))
}

fn moment_numbers(moments: &[i64]) -> Result<Vec<i32>, TaskFailure> {
    moments
        .iter()
        .map(|&moment| {
            if (-1..=11).contains(&moment) {
                Ok(moment as i32)
            } else {
                Err(
                    TaskFailure::validation("immoments takes moment numbers -1 through 11")
                        .at("moments"),
                )
            }
        })
        .collect()
}

/// CASA's `includepix`, an inclusive range of the pixel values a moment
/// reads: `-1` (or `[]`) includes every pixel; one value `b` is
/// `[-|b|, |b|]`; two values are the range's ends, in either order
/// (casacore's `MomentsBase::setInExCludeRange`; CASA 6.7.6.14 runs
/// `includepix=-2` as `[-2, 2]` and `[5, 2]` as `[2, 5]`).
fn pixel_range(name: &str, values: &[f64]) -> Result<Option<[f64; 2]>, TaskFailure> {
    match *values {
        [] => Ok(None),
        [-1.0] => Ok(None),
        [value] => Ok(Some([-value.abs(), value.abs()])),
        [a, b] => Ok(Some([a.min(b), a.max(b)])),
        _ => Err(TaskFailure::validation(format!(
            "{name} {values:?}: give -1 for every pixel, one value b for [-|b|, |b|], or two \
             for a range, as CASA takes it"
        ))
        .at(name)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn documented_moment_numbers_and_lists() {
        assert_eq!(moment_numbers(&[-1, 0, 8, 11]).unwrap(), vec![-1, 0, 8, 11]);
        assert!(moment_numbers(&[12]).is_err());
    }

    #[test]
    fn includepix_reads_as_casa_does() {
        // Only -1 is CASA's "every pixel".
        assert_eq!(pixel_range("includepix", &[]).unwrap(), None);
        assert_eq!(pixel_range("includepix", &[-1.0]).unwrap(), None);
        // Any other single value b is [-|b|, |b|].
        assert_eq!(
            pixel_range("includepix", &[-2.0]).unwrap(),
            Some([-2.0, 2.0])
        );
        assert_eq!(
            pixel_range("includepix", &[2.0]).unwrap(),
            Some([-2.0, 2.0])
        );
        assert_eq!(
            pixel_range("includepix", &[-1.5]).unwrap(),
            Some([-1.5, 1.5])
        );
        assert_eq!(pixel_range("includepix", &[0.0]).unwrap(), Some([0.0, 0.0]));
        // Two values are the ends, in either order.
        assert_eq!(
            pixel_range("includepix", &[0.1, 10.0]).unwrap(),
            Some([0.1, 10.0])
        );
        assert_eq!(
            pixel_range("includepix", &[5.0, 2.0]).unwrap(),
            Some([2.0, 5.0])
        );
        assert!(pixel_range("includepix", &[1.0, 2.0, 3.0]).is_err());
    }
}
