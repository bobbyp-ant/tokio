//! Run a paused runtime until it is quiescent.
//!
//! See [`quiesce`] and [`quiesce_until`] for details.

use crate::runtime::scheduler;
use crate::runtime::time::QuiescePoll;
use crate::time::Instant;

use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{self, Poll};

/// Report produced when a [`Quiesce`] future resolves.
///
/// Constructed by the runtime; there is no public constructor.
///
/// [`Quiesce`]: struct@Quiesce
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct QuiescedState {
    /// The virtual instant at which the runtime quiesced.
    ///
    /// Equal to `Instant::now()` observed immediately after the resolving call
    /// returns. The clock only ever stops at positions auto-advance moves it to: the
    /// deadline of a timer that fired, or — for timers in the timer wheel's upper
    /// levels — the start of an occupied wheel slot. See [`quiesce_until`] for the
    /// full description of where the clock stops.
    pub now: Instant,

    /// A lower bound on the earliest pending timer deadline strictly after `now`, or
    /// `None` if no timers remain registered with the runtime.
    ///
    /// Exact when that deadline lies in the timer wheel's bottom level (the same
    /// 64-millisecond aligned window as `now`); the start of the occupied wheel slot
    /// — at or before the actual deadline — otherwise. Stepping further with
    /// [`quiesce_until`] refines the bound: each step either fires the timer or
    /// moves the clock closer to it.
    pub next_timer: Option<Instant>,
}

pin_project! {
    /// Future returned by [`quiesce`] and [`quiesce_until`].
    ///
    /// Resolves with a [`QuiescedState`] report when the paused `current_thread`
    /// runtime that polls it has nothing left to do at or before the requested
    /// virtual-time bound.
    ///
    /// The future is bound to the runtime that first polls it. Validation and
    /// registration happen at that first poll — not at construction — so the future
    /// can be created outside a runtime context, for example as the argument to
    /// [`Runtime::block_on`].
    ///
    /// Dropping a `Quiesce` that has not yet resolved deregisters it from that
    /// runtime; the runtime's subsequent auto-advance behavior is unchanged.
    ///
    /// [`Runtime::block_on`]: crate::runtime::Runtime::block_on
    #[project(!Unpin)]
    #[derive(Debug)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Quiesce {
        // Inclusive virtual-time bound; `None` means unbounded (resolve only when
        // no timers remain).
        bound: Option<Instant>,

        // Registration state. All validation happens on first poll so that
        // `rt.block_on(time::quiesce_until(..))` works (the future is constructed
        // before `block_on` establishes the runtime context).
        state: State,
    }

    impl PinnedDrop for Quiesce {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if let State::Registered { id, ref handle } = *this.state {
                // Deregister from the originating runtime's time driver. The driver
                // handle was captured at registration, so this targets the right
                // runtime regardless of the ambient context (idempotent if the
                // waiter was already collected or the driver shut down).
                if let Some(time_handle) = handle.driver().time.as_ref() {
                    time_handle.deregister_quiesce_waiter(id);
                }
            }
        }
    }
}

#[derive(Debug)]
enum State {
    /// Created; not yet polled.
    Init,
    /// Registered with the time driver of the runtime that first polled this future.
    Registered { id: u64, handle: scheduler::Handle },
    /// Resolved; the report has been collected and returned.
    Done,
}

/// Waits until the paused runtime has no work left to do and no pending timers
/// remain.
///
/// Equivalent to [`quiesce_until`] with no bound: the future resolves only once the
/// runtime's timer wheel is empty, nothing is runnable, and no [`spawn_blocking`]
/// task spawned on this runtime is outstanding. The resolution contract, supported
/// calling patterns, auto-advance guard interaction, and determinism caveats
/// documented on [`quiesce_until`] all apply here, with "at or before the bound"
/// read as "at any point in the future".
///
/// **Important:** a workload with a recurring timer — an [`interval`], or any task
/// that re-arms a [`sleep`] each time it fires — never empties the timer wheel, so
/// an unbounded `quiesce()` never resolves on it. `quiesce()` is intended for
/// workloads that finish on their own; stepped simulations and workloads with
/// periodic timers should use [`quiesce_until`].
///
/// # Panics
///
/// The returned future panics when polled if:
/// - polled outside a Tokio runtime context,
/// - the runtime is not the `current_thread` flavor,
/// - the runtime's clock is not paused, or
/// - the runtime has no time driver (`enable_time()`/`enable_all()` not called).
///
/// While the future is registered (polled at least once and not yet resolved),
/// calling [`resume`] or [`advance`] on the runtime panics: an explicit clock change
/// during a quiesce step would move the clock out from under the step.
///
/// # Examples
///
/// ```
/// use tokio::time::{self, Duration};
///
/// #[tokio::main(flavor = "current_thread", start_paused = true)]
/// async fn main() {
///     tokio::spawn(async {
///         time::sleep(Duration::from_millis(10)).await;
///     });
///
///     // Runs the spawned task (firing its timer); resolves once no pending
///     // timers remain and nothing is runnable.
///     let state = time::quiesce().await;
///     assert!(state.next_timer.is_none());
/// }
/// ```
///
/// [`interval`]: crate::time::interval()
/// [`sleep`]: crate::time::sleep()
/// [`spawn_blocking`]: crate::task::spawn_blocking
/// [`resume`]: crate::time::resume
/// [`advance`]: crate::time::advance
pub fn quiesce() -> Quiesce {
    Quiesce {
        bound: None,
        state: State::Init,
    }
}

/// Waits until the paused runtime has run everything that can possibly happen at or
/// before `deadline` (inclusive), then reports where the virtual clock ended up and
/// when the next pending timer is due.
///
/// When the returned [`Quiesce`] future resolves, all of the following held at the
/// moment of resolution: nothing was runnable, no [`spawn_blocking`] task spawned on
/// this runtime was outstanding, and every timer with a deadline at or before
/// `deadline` had fired. This is a statement about the past, not a promise
/// about the future: a wake delivered from outside the runtime after resolution (a
/// message sent by another thread, for example) is the caller's to manage —
/// typically by stepping again.
///
/// The bound is inclusive: a timer with a deadline exactly equal to `deadline` fires
/// within the call. Callers building windowing protocols can layer their own
/// half-open convention on top of this.
///
/// A `deadline` at or before the current virtual time resolves as soon as nothing is
/// runnable, with the clock unchanged — a cheap "drain without advancing".
///
/// # Where the clock stops
///
/// The clock is never advanced to `deadline` itself. During the step it only moves
/// to positions auto-advance has a reason to visit: the deadline of a pending timer,
/// or — for timers stored in the timer wheel's coarser upper levels — the start of
/// an occupied wheel slot. [`QuiescedState::now`] reports the position the clock
/// stopped at; it is never past `deadline` (rounded up to Tokio's millisecond timer
/// resolution) and always equals [`Instant::now()`] observed after the call returns.
///
/// When every relevant timer is near the clock — in the wheel's bottom level, the
/// same 64-millisecond aligned window as the clock's position — the stop position is
/// exact: `now` is the deadline of the last timer that fired, or the starting
/// instant if none fired. A timer whose deadline lies further out is approached in
/// refinement hops: auto-advance moves the clock to the start of the timer's
/// occupied slot, the wheel re-sorts that slot into finer levels, and the process
/// repeats. A `deadline` that lies beyond such a slot start can therefore observe
/// `now` resting at a slot boundary where no timer actually fired.
///
/// [`QuiescedState::next_timer`] follows the same precision rule: it is a lower
/// bound on the earliest pending timer deadline strictly after `now` — exact for
/// deadlines in the bottom wheel level, slot-aligned otherwise — and is `None`
/// exactly when no timers remain. Stepping an "empty" window (one in which nothing
/// fires) refines the bound.
///
/// # Supported calling patterns
///
/// Step the runtime either by passing this future to [`Runtime::block_on`] on the
/// thread that drives the runtime — the typical shape for a stepping loop driven
/// from synchronous test code — or by awaiting it inside a task spawned on the
/// runtime.
///
/// **Important:** [`Handle::block_on`] does not drive a `current_thread` runtime's
/// scheduler or its IO and timer drivers. A `Quiesce` awaited there does not resolve
/// unless another thread is concurrently driving the runtime with
/// [`Runtime::block_on`]. See [`Handle::block_on`] for details.
///
/// # Interaction with auto-advance guards
///
/// While an [`AutoAdvanceGuard`] is held and a timer with a deadline at or before
/// `deadline` is pending, the step cannot complete: it waits in real time until the
/// guard is dropped (necessarily from another thread), then finishes in place
/// without restarting. A held guard does not prevent resolution when no timer at or
/// before `deadline` is pending — the guard blocks auto-advance, not quiescence
/// itself.
///
/// **Important:** holding such a guard on the same thread that then calls
/// `block_on(quiesce_until(..))` waits forever: the guard cannot be dropped while
/// the thread is blocked, and the step cannot complete while the guard is held.
///
/// # Determinism caveats
///
/// Quiescence stepping makes the runtime's virtual-time scheduling observable and
/// repeatable; it does not by itself make a workload deterministic:
///
/// - [`spawn_blocking`] work runs on real operating-system threads. A step waits for
///   outstanding blocking tasks so their effects are not lost, but how long that
///   takes — and how blocking work interleaves with other threads — is real-world
///   timing, not virtual time.
/// - A task that re-wakes itself in a loop (for example by calling [`yield_now`]
///   repeatedly) never lets the runtime quiesce, so a step never resolves. Tasks
///   must wait on real wakeups: timers, [`Notify`], channels, or IO.
/// - When several [`select!`] branches are ready at once, the winning branch is
///   randomized. Seed the runtime's random number generator (`Builder::rng_seed`, a
///   `tokio_unstable` API) to make that choice reproducible.
///
/// # Panics
///
/// The returned future panics when polled if:
/// - polled outside a Tokio runtime context,
/// - the runtime is not the `current_thread` flavor,
/// - the runtime's clock is not paused, or
/// - the runtime has no time driver (`enable_time()`/`enable_all()` not called).
///
/// While the future is registered (polled at least once and not yet resolved),
/// calling [`resume`] or [`advance`] on the runtime panics: an explicit clock change
/// during a quiesce step would move the clock past the step's bound.
///
/// # Examples
///
/// Stepping a paused runtime one window at a time from outside the runtime:
///
/// ```
/// use tokio::time::{self, Duration, Instant};
///
/// let rt = tokio::runtime::Builder::new_current_thread()
///     .enable_time()
///     .start_paused(true)
///     .build()
///     .unwrap();
///
/// let start = {
///     let _enter = rt.enter();
///     Instant::now()
/// };
///
/// rt.spawn(async move {
///     time::sleep_until(start + Duration::from_millis(15)).await;
///     // ... work that happens at t = 15ms ...
/// });
///
/// // Window 1: nothing is due at or before 10ms; the clock does not move.
/// let state = rt.block_on(time::quiesce_until(start + Duration::from_millis(10)));
/// assert_eq!(state.now, start);
/// assert_eq!(state.next_timer, Some(start + Duration::from_millis(15)));
///
/// // Window 2: the 15ms timer fires; the clock stops there, not at the bound.
/// let state = rt.block_on(time::quiesce_until(start + Duration::from_millis(20)));
/// assert_eq!(state.now, start + Duration::from_millis(15));
/// assert_eq!(state.next_timer, None);
/// ```
///
/// Awaiting inside the runtime:
///
/// ```
/// use tokio::time::{self, Duration, Instant};
///
/// #[tokio::main(flavor = "current_thread", start_paused = true)]
/// async fn main() {
///     let start = Instant::now();
///
///     tokio::spawn(async {
///         time::sleep(Duration::from_millis(10)).await;
///     });
///
///     // Run everything due in the first 50ms of virtual time.
///     let state = time::quiesce_until(start + Duration::from_millis(50)).await;
///
///     // The clock stopped at the timer that fired, not at the bound.
///     assert_eq!(state.now, start + Duration::from_millis(10));
/// }
/// ```
///
/// [`spawn_blocking`]: crate::task::spawn_blocking
/// [`yield_now`]: crate::task::yield_now()
/// [`Notify`]: crate::sync::Notify
/// [`select!`]: crate::select
/// [`Runtime::block_on`]: crate::runtime::Runtime::block_on
/// [`Handle::block_on`]: crate::runtime::Handle::block_on
/// [`AutoAdvanceGuard`]: crate::time::AutoAdvanceGuard
/// [`Instant::now()`]: crate::time::Instant::now
/// [`resume`]: crate::time::resume
/// [`advance`]: crate::time::advance
pub fn quiesce_until(deadline: Instant) -> Quiesce {
    Quiesce {
        bound: Some(deadline),
        state: State::Init,
    }
}

impl Quiesce {
    /// First-poll validation and registration. Returns the registration id and the
    /// scheduler handle.
    #[track_caller]
    fn register(bound: Option<Instant>, waker: &task::Waker) -> (u64, scheduler::Handle) {
        // Panics with CONTEXT_MISSING_ERROR outside a runtime context (AC3.3).
        let handle = scheduler::Handle::current();

        // Flavor check: quiesce only exists for the current_thread flavor (AC3.1).
        // LocalRuntime also uses the CurrentThread scheduler handle, so it passes.
        match &handle {
            scheduler::Handle::CurrentThread(_) => {}
            #[cfg(feature = "rt-multi-thread")]
            scheduler::Handle::MultiThread(_) => panic!(
                "`time::quiesce()` requires the `current_thread` Tokio runtime. \
                 This is the default Runtime used by `#[tokio::test]`."
            ),
        }

        // Time-driver presence check (AC3.4): panics with the existing
        // "timers are disabled" message.
        let time_handle = handle.driver().time();

        // Paused-clock check (AC3.2).
        let clock = handle.driver().clock();
        if !clock.is_paused() {
            panic!(
                "`time::quiesce()` requires the runtime's clock to be paused \
                 (see `tokio::time::pause()`)"
            );
        }

        let id = time_handle.register_quiesce_waiter(bound, waker);

        // This poll may be running on a thread that merely holds a `Handle::enter`
        // guard while the target runtime is parked on its own thread. Nothing else
        // would cause a parked runtime to re-run its drain-park hook and notice the
        // new waiter, so unpark its driver explicitly. (The full driver unpark is
        // needed -- the time handle alone only sets `did_wake` and cannot wake the
        // runtime thread.)
        handle.driver().unpark();

        (id, handle.clone())
    }
}

impl Future for Quiesce {
    type Output = QuiescedState;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<QuiescedState> {
        let this = self.project();

        match this.state {
            State::Init => {
                let (id, handle) = Quiesce::register(*this.bound, cx.waker());
                *this.state = State::Registered { id, handle };
                Poll::Pending
            }
            State::Registered { id, handle } => {
                let time_handle = handle.driver().time();

                // Mirror TimerEntry: a quiesce outliving its runtime is a bug.
                if time_handle.is_shutdown() {
                    panic!("{}", crate::util::error::RUNTIME_SHUTTING_DOWN_ERROR);
                }

                match time_handle.poll_quiesce_waiter(*id, cx.waker()) {
                    QuiescePoll::Ready(report) => {
                        *this.state = State::Done;
                        Poll::Ready(report)
                    }
                    QuiescePoll::Pending => Poll::Pending,
                    QuiescePoll::Missing => {
                        // The registry is only drained wholesale by the driver's
                        // shutdown, which sets the shutdown flag first; the shutdown
                        // raced with the `is_shutdown` check above.
                        debug_assert!(time_handle.is_shutdown());
                        panic!("{}", crate::util::error::RUNTIME_SHUTTING_DOWN_ERROR);
                    }
                }
            }
            State::Done => panic!("`Quiesce` polled after completion"),
        }
    }
}
