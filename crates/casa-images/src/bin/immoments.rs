// SPDX-License-Identifier: LGPL-3.0-or-later
//! `immoments` - CASA-style spectral moment maps, on the shared task front
//! end.

use std::process::ExitCode;

use casa_images::{
    ImageAnalysisTaskResult, ImmomentsRequest, image_task_front_end, image_task_success, immoments,
};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

fn main() -> ExitCode {
    image_task_front_end("immoments", &["immoments"], run).main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}

fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let request = ImmomentsRequest {
        imagename: task.path("imagename")?,
        outfile: task.path("outfile")?,
        moments: one_moment(&task.integers("moments")?)?,
        chans: task.text("chans")?,
        includepix: include_range(&task.numbers("includepix")?)?,
        mask: task.text("mask")?,
        overwrite: task.flag("overwrite")?,
    };
    image_task_success(immoments(&request).map(ImageAnalysisTaskResult::Immoments))
}

/// The one moment casa-rs computes per run: CASA's `moments` takes several,
/// each written beside `outfile`, which is #977.
fn one_moment(moments: &[i64]) -> Result<i32, TaskFailure> {
    let refused = |message: String| TaskFailure::validation(message).at("moments");
    match moments {
        [moment @ -1..=3] => Ok(*moment as i32),
        [moment] => Err(refused(format!(
            "moment {moment} is not implemented by casa-rs yet (#977); it computes -1 through 3"
        ))),
        _ => Err(refused(format!(
            "{} moments in one run are not implemented by casa-rs yet (#977); give one",
            moments.len()
        ))),
    }
}

/// CASA's `includepix`, an inclusive range of the pixel values a moment
/// reads: `-1` (or `[]`) includes every pixel; one value `b` is
/// `[-|b|, |b|]`; two values are the range's ends, in either order
/// (casacore's `MomentsBase::setInExCludeRange`; CASA 6.7.6.14 runs
/// `includepix=-2` as `[-2, 2]` and `[5, 2]` as `[2, 5]`).
fn include_range(values: &[f64]) -> Result<Option<[f64; 2]>, TaskFailure> {
    match *values {
        [] => Ok(None),
        [value] if value == -1.0 => Ok(None),
        [value] => Ok(Some([-value.abs(), value.abs()])),
        [a, b] => Ok(Some([a.min(b), a.max(b)])),
        _ => Err(TaskFailure::validation(format!(
            "includepix {values:?}: give -1 for every pixel, one value b for [-|b|, |b|], or two \
             for a range, as CASA takes it"
        ))
        .at("includepix")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_moment_from_minus_one_to_three_is_computed() {
        assert_eq!(one_moment(&[0]).unwrap(), 0);
        assert_eq!(one_moment(&[-1]).unwrap(), -1);
        assert!(one_moment(&[8]).unwrap_err().message.contains("#977"));
        assert!(one_moment(&[0, 1]).unwrap_err().message.contains("#977"));
    }

    #[test]
    fn includepix_reads_as_casa_does() {
        // Only -1 is CASA's "every pixel".
        assert_eq!(include_range(&[]).unwrap(), None);
        assert_eq!(include_range(&[-1.0]).unwrap(), None);
        // Any other single value b is [-|b|, |b|].
        assert_eq!(include_range(&[-2.0]).unwrap(), Some([-2.0, 2.0]));
        assert_eq!(include_range(&[2.0]).unwrap(), Some([-2.0, 2.0]));
        assert_eq!(include_range(&[-1.5]).unwrap(), Some([-1.5, 1.5]));
        assert_eq!(include_range(&[0.0]).unwrap(), Some([0.0, 0.0]));
        // Two values are the ends, in either order.
        assert_eq!(include_range(&[0.1, 10.0]).unwrap(), Some([0.1, 10.0]));
        assert_eq!(include_range(&[5.0, 2.0]).unwrap(), Some([2.0, 5.0]));
        assert!(include_range(&[1.0, 2.0, 3.0]).is_err());
    }
}
