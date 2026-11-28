// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::cancellation::Cancellation;

pub(crate) const BATCH_SIZE: usize = 4096;

#[derive(Debug)]
pub(crate) struct StatisticsInterrupted;

impl From<StatisticsInterrupted> for crate::LatticeError {
    fn from(_: StatisticsInterrupted) -> Self {
        Self::Interrupted
    }
}

pub(crate) fn check(cancellation: Option<&Cancellation>) -> Result<(), StatisticsInterrupted> {
    if cancellation.is_some_and(Cancellation::is_cancelled) {
        return Err(StatisticsInterrupted);
    }
    Ok(())
}

pub(crate) fn batches<'a, T>(
    values: &'a [T],
    cancellation: Option<&'a Cancellation>,
) -> impl Iterator<Item = Result<&'a [T], StatisticsInterrupted>> {
    values.chunks(BATCH_SIZE).map(move |batch| {
        check(cancellation)?;
        Ok(batch)
    })
}
