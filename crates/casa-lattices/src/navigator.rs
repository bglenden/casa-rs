// SPDX-License-Identifier: LGPL-3.0-or-later
//! Abstract navigation strategy for lattice iteration.

/// Strategy for stepping through a lattice in rectangular chunks.
///
/// Corresponds to the C++ `LatticeNavigator` abstract base class.
/// Navigators define how a cursor moves through a lattice. Different
/// implementations (e.g., [`LatticeStepper`](crate::LatticeStepper),
/// [`TiledLineStepper`](crate::TiledLineStepper),
/// [`TileStepper`](crate::TileStepper)) optimize for different access
/// patterns.
///
/// The navigator tracks a current position and cursor shape within the
/// lattice. Calling [`next`](Self::next) advances to the next chunk;
/// the cursor shape may change near lattice boundaries (hangover
/// handling).
pub trait LatticeNavigator {
    /// Returns the overall lattice shape.
    fn lattice_shape(&self) -> &[usize];

    /// Returns the current cursor shape.
    ///
    /// Near lattice boundaries, this may be smaller than the requested
    /// cursor shape (hangover).
    fn cursor_shape(&self) -> &[usize];

    /// Returns the current position (start of the cursor window).
    fn position(&self) -> &[usize];

    /// Returns `true` if the navigator has passed the last chunk.
    fn at_end(&self) -> bool;

    /// Advances to the next chunk. Returns `true` if the new position
    /// is valid (not at end).
    fn next(&mut self) -> bool;

    /// Returns to the previous chunk. Returns `true` if successful.
    fn prev(&mut self) -> bool;

    /// Resets the navigator to the first chunk.
    fn reset(&mut self);

    /// Returns the total number of steps to traverse the entire lattice.
    fn n_steps(&self) -> usize;
}
