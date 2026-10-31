// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `table` tools of this process, as casacore's table cache lists the
//! tables a process has open.
//!
//! casacore's tools share one table object per table, so a call on one tool
//! can see another's lock, refuse to replace a table another tool holds
//! (`prepareCopyRename`) and release every automatic lock of the process
//! (`Table::relinquishAutoLocks`). Here each tool has a handle of its own;
//! this registry lets a call visit the other tools.
//!
//! A call holds its own tool's mutex while it visits the others, so the
//! others are visited with `try_lock`: one that is busy in another thread is
//! reported as such, never waited for (two tools visiting each other would
//! deadlock).

use std::sync::{Arc, Mutex, PoisonError, TryLockError, Weak};

use super::state::State;

/// Every tool made in this process that is still alive, with its
/// `State::id`.
static TOOLS: Mutex<Vec<(u64, Weak<Mutex<State>>)>> = Mutex::new(Vec::new());

/// `state` as a tool's shared state, listed with the process's tools.
pub(super) fn track(state: State) -> Arc<Mutex<State>> {
    let id = state.id;
    let tool = Arc::new(Mutex::new(state));
    let mut tools = TOOLS.lock().unwrap_or_else(PoisonError::into_inner);
    tools.retain(|(_, other)| other.strong_count() > 0);
    tools.push((id, Arc::downgrade(&tool)));
    tool
}

/// Calls `visit` on the state of every tool but `me` (a tool's `State::id`,
/// the caller's, whose mutex the caller holds): with `Some(state)` for a tool
/// that is free, with `None` for one that is busy in another call.
pub(super) fn for_each_other(me: u64, mut visit: impl FnMut(Option<&mut State>)) {
    let tools: Vec<Arc<Mutex<State>>> = TOOLS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter(|(id, _)| *id != me)
        .filter_map(|(_, tool)| tool.upgrade())
        .collect();
    for tool in tools {
        match tool.try_lock() {
            Ok(mut state) => visit(Some(&mut state)),
            Err(TryLockError::Poisoned(poisoned)) => visit(Some(&mut poisoned.into_inner())),
            Err(TryLockError::WouldBlock) => visit(None),
        }
    }
}
