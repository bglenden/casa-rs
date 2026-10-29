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

/// CASA's `includepix`: `-1` includes every pixel, `[min, max]` a range.
fn include_range(values: &[f64]) -> Result<Option<[f64; 2]>, TaskFailure> {
    match values {
        [] => Ok(None),
        [value] if *value < 0.0 => Ok(None),
        [min, max] => Ok(Some([*min, *max])),
        _ => Err(TaskFailure::validation(format!(
            "includepix {values:?} is not implemented by casa-rs yet (#977); give -1 or [min, max]"
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
    fn includepix_is_minus_one_or_a_range() {
        assert_eq!(include_range(&[-1.0]).unwrap(), None);
        assert_eq!(include_range(&[0.1, 10.0]).unwrap(), Some([0.1, 10.0]));
        assert!(include_range(&[5.0]).is_err());
    }
}
