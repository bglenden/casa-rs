// SPDX-License-Identifier: LGPL-3.0-or-later

//! Cooperative control inside synchronous preparation, without a second scheduler.

use std::cell::RefCell;

use super::*;

pub(super) struct WorkControl<'a> {
    poll: &'a dyn Fn() -> RunDirective,
    request: RefCell<Option<RunDirective>>,
}

impl fmt::Debug for WorkControl<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkControl")
            .field("request", &self.request.borrow())
            .finish_non_exhaustive()
    }
}

impl WorkControl<'_> {
    pub(super) fn stop_requested(&self) -> bool {
        if self.request.borrow().is_some() {
            return true;
        }
        let directive = (self.poll)();
        if directive == RunDirective::Continue {
            false
        } else {
            *self.request.borrow_mut() = Some(directive);
            true
        }
    }
}

pub(super) fn execute<I: WorkImplementation, C: RunController>(
    implementation: &I,
    context: WorkExecutionContext<'_>,
    controller: &mut C,
    mut status: ExecutionStatus,
) -> (Result<WorkMeasurements, I::Error>, Option<RunDirective>) {
    status.lease_epoch = context.lease_epoch();
    status.knobs = context.knobs().clone();
    // Work is active: adaptation still belongs to a globally idle scheduler cut.
    status.eligible_adaptations.clear();
    let controller = RefCell::new(controller);
    let poll = || controller.borrow_mut().directive(&status);
    let control = WorkControl {
        poll: &poll,
        request: RefCell::new(None),
    };
    let result = implementation.execute(WorkExecutionContext {
        control: Some(&control),
        ..context
    });
    (result, control.request.into_inner())
}
