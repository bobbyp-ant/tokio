//! Run a paused runtime until it is quiescent.
//
// (Module-level docs are completed along with the public quiesce API.)

use crate::time::Instant;

/// Report produced when a quiesce future resolves.
///
/// Constructed by the runtime; there is no public constructor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct QuiescedState {
    /// The virtual instant at which the runtime quiesced.
    ///
    /// Equal to `Instant::now()` observed immediately after the resolving call returns.
    pub now: Instant,

    /// A lower bound on the earliest pending timer deadline strictly after `now`, or
    /// `None` if no timers remain registered with the runtime.
    ///
    /// Exact when that deadline lies in the timer wheel's bottom level (within 64 ms
    /// of `now`); the start of the occupied wheel slot otherwise.
    pub next_timer: Option<Instant>,
}
