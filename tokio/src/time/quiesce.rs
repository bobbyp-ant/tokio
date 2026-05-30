//! Run a paused runtime until it is quiescent.
//!
//! See [`quiesce`] and [`quiesce_until`] for details.
//
// (Module-level docs are completed along with the public quiesce API.)

use crate::runtime::scheduler;
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
    /// Equal to `Instant::now()` observed immediately after the resolving call returns.
    pub now: Instant,

    /// A lower bound on the earliest pending timer deadline strictly after `now`, or
    /// `None` if no timers remain registered with the runtime.
    ///
    /// Exact when that deadline lies in the timer wheel's bottom level (within 64 ms
    /// of `now`); the start of the occupied wheel slot otherwise.
    pub next_timer: Option<Instant>,
}

pin_project! {
    /// Future returned by [`quiesce`] and [`quiesce_until`].
    ///
    /// Resolves with a [`QuiescedState`] report when the paused `current_thread`
    /// runtime that polls it has nothing left to do at or before the requested
    /// virtual-time bound.
    ///
    /// (Full API documentation, including all caveats, is completed in Phase 3.)
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
/// runtime's timer wheel is empty and nothing is runnable. A workload with a
/// recurring timer (such as [`interval`]) never reaches that state; use
/// [`quiesce_until`] for such workloads.
///
/// (Full API documentation is completed in Phase 3.)
///
/// # Panics
///
/// The returned future panics when polled if:
/// - polled outside a Tokio runtime context,
/// - the runtime is not the `current_thread` flavor,
/// - the runtime's clock is not paused, or
/// - the runtime has no time driver (`enable_time()`/`enable_all()` not called).
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
/// (Full API documentation is completed in Phase 3.)
///
/// # Panics
///
/// Same conditions as [`quiesce`].
///
/// # Examples
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
                 This is the default Runtime used by `#[tokio::test]."
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
                    Some(report) => {
                        *this.state = State::Done;
                        Poll::Ready(report)
                    }
                    None => Poll::Pending,
                }
            }
            State::Done => panic!("`Quiesce` polled after completion"),
        }
    }
}
