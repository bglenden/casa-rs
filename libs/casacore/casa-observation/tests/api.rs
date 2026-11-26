// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_observation::{FieldId, FieldSelection, TimeRuns};

#[test]
fn traversal_batches_do_not_span_unselected_grid_times() {
    let source = FieldId(6);
    let other = FieldId(8);
    let fields = [other, source, source, source, other, source, other];
    let runs = TimeRuns::selected(1..7, &fields, &FieldSelection::Only(vec![source]));
    assert_eq!(runs.runs(), &[1..4, 5..6]);
    assert_eq!(runs.blocks(2).collect::<Vec<_>>(), vec![1..3, 3..4, 5..6]);
}
