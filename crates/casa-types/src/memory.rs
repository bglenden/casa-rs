// SPDX-License-Identifier: LGPL-3.0-or-later
//! Allocation bounds for shared container representations.

/// Bound the node allocations of a nonempty `BTreeMap<K, V>` or
/// `BTreeSet<K>` (with `V = ()`), excluding heap owned by keys and values.
///
/// Rust's degree-six tree has eleven key/value slots and twelve edges per
/// internal node. The root may hold one entry; every other node holds at
/// least five. Charging an internal node for each possible node therefore
/// also covers partially filled trees and small maps with one allocated
/// leaf. Slots are charged at the pair's padded size, with aligned headers
/// and edges, so separate key/value arrays fit within the bound.
///
/// Zero entries returns zero for a freshly constructed empty map. A map
/// emptied by removal may retain its root; charge one entry in that case.
#[must_use]
pub const fn btree_node_bytes<K, V>(entries: usize) -> usize {
    if entries == 0 {
        return 0;
    }
    const SLOTS: usize = 11;
    const MIN_ENTRIES: usize = 5;
    let alignment = if align_of::<(K, V)>() > align_of::<usize>() {
        align_of::<(K, V)>()
    } else {
        align_of::<usize>()
    };
    let header = (2 * size_of::<usize>()).next_multiple_of(alignment);
    let node = (header + SLOTS * size_of::<(K, V)>() + (SLOTS + 1) * size_of::<usize>())
        .next_multiple_of(alignment);
    entries.div_ceil(MIN_ENTRIES).saturating_mul(node)
}
