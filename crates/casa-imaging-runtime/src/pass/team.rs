// SPDX-License-Identifier: LGPL-3.0-or-later
//! The bounded worker team every parallel imaging stage runs on.

use rayon::prelude::*;

use super::PassError;

/// Stack of each team worker. Kernels keep their scratch on the heap; per-plane
/// FFTs are single-threaded inside a worker (plan section 5.9).
pub const WORKER_STACK_BYTES: usize = 2 * 1024 * 1024;

/// A fixed set of worker threads.
///
/// One worker runs inline on the caller and starts no thread. Work handed to
/// [`WorkerTeam::for_each_mut`] is joined before the call returns, so no job
/// outlives the data it borrows.
pub struct WorkerTeam {
    workers: usize,
    pool: Option<rayon::ThreadPool>,
}

impl WorkerTeam {
    /// A team of `workers` threads; `workers` must be positive.
    pub fn new(workers: usize) -> Result<Self, PassError> {
        if workers == 0 {
            return Err(PassError::Workers);
        }
        let pool = (workers > 1)
            .then(|| {
                rayon::ThreadPoolBuilder::new()
                    .num_threads(workers)
                    .stack_size(WORKER_STACK_BYTES)
                    .thread_name(|index| format!("imaging-worker-{index}"))
                    .build()
                    .map_err(|_| PassError::Workers)
            })
            .transpose()?;
        Ok(Self { workers, pool })
    }

    /// Number of workers.
    #[must_use]
    pub const fn workers(&self) -> usize {
        self.workers
    }

    /// Run `operation` on every item, one item per job, and join. Items are
    /// independent; the first error in item order is returned.
    pub fn for_each_mut<T: Send, E: Send>(
        &self,
        items: &mut [T],
        operation: impl Fn(usize, &mut T) -> Result<(), E> + Send + Sync,
    ) -> Result<(), E> {
        match &self.pool {
            None => items
                .iter_mut()
                .enumerate()
                .try_for_each(|(index, item)| operation(index, item)),
            Some(pool) => {
                let mut results = Vec::with_capacity(items.len());
                pool.install(|| {
                    items
                        .par_iter_mut()
                        .enumerate()
                        .map(|(index, item)| operation(index, item))
                        .collect_into_vec(&mut results);
                });
                results.into_iter().collect()
            }
        }
    }
}

impl std::fmt::Debug for WorkerTeam {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerTeam")
            .field("workers", &self.workers)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_item_runs_once_and_the_first_error_wins() {
        for workers in [1, 3] {
            let team = WorkerTeam::new(workers).expect("team");
            let mut items = vec![0_u32; 17];
            team.for_each_mut(&mut items, |index, item| {
                *item += index as u32 + 1;
                Ok::<_, ()>(())
            })
            .expect("no error");
            assert!(items.iter().enumerate().all(|(i, v)| *v == i as u32 + 1));
            let error = team
                .for_each_mut(&mut items, |index, _| {
                    if index == 4 || index == 9 {
                        Err(index)
                    } else {
                        Ok(())
                    }
                })
                .expect_err("errors propagate");
            assert_eq!(error, 4);
        }
        assert!(matches!(WorkerTeam::new(0), Err(PassError::Workers)));
    }
}
