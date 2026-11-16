// SPDX-License-Identifier: LGPL-3.0-or-later
//! Adjacent MAIN runs sharing a cell shape, with their allocation owner.
use casa_imaging_runtime::{Admission, Admitted, Demand, HostResources, ResourcePolicy, admit};
use std::ops::Range;

pub(super) type Run = (Range<usize>, (usize, usize));

pub(super) fn plan(
    rows: impl ExactSizeIterator<Item = Run>,
    host: &HostResources,
    policy: &ResourcePolicy,
) -> Result<Admitted<Vec<Run>>, Admission> {
    let reservation = admit(
        host,
        policy,
        &Demand {
            phase: "the simulator's mixed-window patch ranges",
            memory: (rows.len() * size_of::<Run>() + 128) as u64,
        },
    )?;
    let mut value = Vec::<Run>::with_capacity(rows.len());
    for (range, cell) in rows {
        if let Some((last, shape)) = value.last_mut()
            && *shape == cell
            && last.end == range.start
        {
            last.end = range.end;
        } else {
            value.push((range, cell));
        }
    }
    Ok(Admitted { value, reservation })
}
