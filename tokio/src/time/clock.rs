#![cfg_attr(not(feature = "rt"), allow(dead_code))]

//! Source of time abstraction.
//!
//! By default, `std::time::Instant::now()` is used. However, when the
//! `test-util` feature flag is enabled, the values returned for `now()` are
//! configurable.

cfg_not_test_util! {
    use crate::time::{Instant};

    #[derive(Debug, Clone)]
    pub(crate) struct Clock {}

    pub(crate) fn now() -> Instant {
        Instant::from_std(std::time::Instant::now())
    }

    impl Clock {
        pub(crate) fn new(_enable_pausing: bool, _start_paused: bool) -> Clock {
            Clock {}
        }

        pub(crate) fn now(&self) -> Instant {
            now()
        }
    }
}

cfg_test_util! {
    use crate::time::{Duration, Instant};
    use crate::loom::sync::Mutex;
    use crate::loom::sync::atomic::Ordering;
    use std::sync::atomic::AtomicBool as StdAtomicBool;

    cfg_rt! {
        #[track_caller]
        fn with_clock<R>(f: impl FnOnce(Option<&Clock>) -> Result<R, &'static str>) -> R {
            use crate::runtime::Handle;

            let res = match Handle::try_current() {
                Ok(handle) => f(Some(handle.inner.driver().clock())),
                Err(ref e) if e.is_missing_context() => f(None),
                Err(_) => panic!("{}", crate::util::error::THREAD_LOCAL_DESTROYED_ERROR),
            };

            match res {
                Ok(ret) => ret,
                Err(msg) => panic!("{}", msg),
            }
        }
    }

    cfg_not_rt! {
        #[track_caller]
        fn with_clock<R>(f: impl FnOnce(Option<&Clock>) -> Result<R, &'static str>) -> R {
            match f(None) {
                Ok(ret) => ret,
                Err(msg) => panic!("{}", msg),
            }
        }
    }

    /// A handle to a source of time.
    #[derive(Debug)]
    pub(crate) struct Clock {
        inner: Mutex<Inner>,
    }

    // Used to track if the clock was ever paused. This is an optimization to
    // avoid touching the mutex if `test-util` was accidentally enabled in
    // release mode.
    //
    // A static is used so we can avoid accessing the thread-local as well. The
    // `std` AtomicBool is used directly because loom does not support static
    // atomics.
    static DID_PAUSE_CLOCK: StdAtomicBool = StdAtomicBool::new(false);

    #[derive(Debug)]
    struct Inner {
        /// True if the ability to pause time is enabled.
        enable_pausing: bool,

        /// Instant to use as the clock's base instant.
        base: std::time::Instant,

        /// Instant at which the clock was last unfrozen.
        unfrozen: Option<std::time::Instant>,

        /// Number of `spawn_blocking` tasks still outstanding; each inhibits
        /// auto-advance because a blocking task implies future work.
        blocking_inhibit_count: usize,

        /// Number of live `AutoAdvanceGuard`s; each inhibits auto-advance.
        user_inhibit_count: usize,
    }

    /// Pauses time.
    ///
    /// The current value of `Instant::now()` is saved and all subsequent calls
    /// to `Instant::now()` will return the saved value. The saved value can be
    /// changed by [`advance`] or by the time auto-advancing once the runtime
    /// has no work to do. This only affects the `Instant` type in Tokio, and
    /// the `Instant` in std continues to work as normal.
    ///
    /// Pausing time requires the `current_thread` Tokio runtime. This is the
    /// default runtime used by `#[tokio::test]`. The runtime can be initialized
    /// with time in a paused state using the `Builder::start_paused` method.
    ///
    /// For cases where time is immediately paused, it is better to pause
    /// the time using the `main` or `test` macro:
    /// ```
    /// #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// async fn main() {
    ///    println!("Hello world");
    /// }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if time is already frozen or if called from outside of a
    /// `current_thread` Tokio runtime.
    ///
    /// # Auto-advance
    ///
    /// If time is paused and the runtime has no work to do, the clock is
    /// auto-advanced to the next pending timer. This means that [`Sleep`] or
    /// other timer-backed primitives can cause the runtime to advance the
    /// current time when awaited.
    ///
    /// [`Sleep`]: crate::time::Sleep
    /// [`advance`]: crate::time::advance
    #[track_caller]
    pub fn pause() {
        with_clock(|maybe_clock| {
            match maybe_clock {
                Some(clock) => clock.pause(),
                None => Err("time cannot be frozen from outside the Tokio runtime"),
            }
        });
    }

    /// Resumes time.
    ///
    /// Clears the saved `Instant::now()` value. Subsequent calls to
    /// `Instant::now()` will return the value returned by the system call.
    ///
    /// # Panics
    ///
    /// Panics if time is not frozen or if called from outside of the Tokio
    /// runtime.
    #[track_caller]
    pub fn resume() {
        with_clock(|maybe_clock| {
            let clock = match maybe_clock {
                Some(clock) => clock,
                None => return Err("time cannot be frozen from outside the Tokio runtime"),
            };

            let mut inner = clock.inner.lock();

            if inner.unfrozen.is_some() {
                return Err("time is not frozen");
            }

            inner.unfrozen = Some(std::time::Instant::now());
            Ok(())
        });
    }

    /// Advances time.
    ///
    /// Increments the saved `Instant::now()` value by `duration`. Subsequent
    /// calls to `Instant::now()` will return the result of the increment.
    ///
    /// This function will make the current time jump forward by the given
    /// duration in one jump. This means that all `sleep` calls with a deadline
    /// before the new time will immediately complete "at the same time", and
    /// the runtime is free to poll them in any order.  Additionally, this
    /// method will not wait for the `sleep` calls it advanced past to complete.
    /// If you want to do that, you should instead call [`sleep`] and rely on
    /// the runtime's auto-advance feature.
    ///
    /// Note that calls to `sleep` are not guaranteed to complete the first time
    /// they are polled after a call to `advance`. For example, this can happen
    /// if the runtime has not yet touched the timer driver after the call to
    /// `advance`. However if they don't, the runtime will poll the task again
    /// shortly.
    ///
    /// # Panics
    ///
    /// Panics if any of the following conditions are met:
    ///
    /// - The clock is not frozen, which means that you must
    ///   call [`pause`] before calling this method.
    /// - If called outside of the Tokio runtime.
    /// - If the input `duration` is too large (such as [`Duration::MAX`])
    ///   to be safely added to the current time without causing an overflow.
    ///
    /// # Caveats
    ///
    /// Using a very large `duration` is not recommended,
    /// as it may cause panicking due to overflow.
    ///
    /// # Auto-advance
    ///
    /// If the time is paused and there is no work to do, the runtime advances
    /// time to the next timer. See [`pause`](pause#auto-advance) for more
    /// details.
    ///
    /// [`sleep`]: fn@crate::time::sleep
    pub async fn advance(duration: Duration) {
        with_clock(|maybe_clock| {
            let clock = match maybe_clock {
                Some(clock) => clock,
                None => return Err("time cannot be frozen from outside the Tokio runtime"),
            };

            clock.advance(duration)
        });

        crate::task::yield_now().await;
    }

    /// Returns the current instant, factoring in frozen time.
    pub(crate) fn now() -> Instant {
        if !DID_PAUSE_CLOCK.load(Ordering::Acquire) {
            return Instant::from_std(std::time::Instant::now());
        }

        with_clock(|maybe_clock| {
            Ok(if let Some(clock) = maybe_clock {
                clock.now()
            } else {
                Instant::from_std(std::time::Instant::now())
            })
        })
    }

    /// An RAII guard that prevents the paused clock from auto-advancing while it is
    /// alive.
    ///
    /// Returned by [`inhibit_auto_advance`]. Dropping the guard re-enables
    /// auto-advance — once no other guards and no outstanding [`spawn_blocking`]
    /// tasks inhibit it — and unparks the runtime the guard was created on, so a
    /// parked runtime notices the release promptly.
    ///
    /// The guard always affects the runtime it was created on, regardless of which
    /// runtime context (if any) is current when it is dropped.
    ///
    /// # Caution
    ///
    /// Holding a guard on the same thread that then blocks the runtime on a future
    /// that can only complete via auto-advance (for example, a `sleep` on a paused
    /// runtime with no other pending work) waits in real time until the guard is
    /// dropped from another thread. Hold and drop guards from outside the runtime,
    /// or from tasks that are woken by external events.
    ///
    /// [`spawn_blocking`]: crate::task::spawn_blocking
    #[derive(Debug)]
    #[must_use = "auto-advance is re-enabled when the guard is dropped"]
    pub struct AutoAdvanceGuard {
        handle: crate::runtime::Handle,
    }

    impl Drop for AutoAdvanceGuard {
        fn drop(&mut self) {
            use crate::runtime::scheduler;

            match &self.handle.inner {
                scheduler::Handle::CurrentThread(handle) => {
                    handle.driver.clock.allow_auto_advance_user();
                    handle.driver.unpark();
                }
                // `inhibit_auto_advance()` panics on the multi-thread flavor, so a
                // guard can never hold a multi-thread handle. Do nothing rather than
                // risk panicking in drop.
                #[cfg(feature = "rt-multi-thread")]
                scheduler::Handle::MultiThread(_) => {}
            }
        }
    }

    /// Prevents the clock from auto-advancing while the returned guard is held.
    ///
    /// When the runtime's clock is [paused](pause) and the runtime has no work left
    /// to do, the clock normally "auto-advances" to the next pending timer so that
    /// timers fire without waiting in real time. While any [`AutoAdvanceGuard`] is
    /// alive, that auto-advance is disabled: timers fire only if the clock is moved
    /// explicitly with [`advance`], or after every guard has been dropped.
    ///
    /// Guards are counted: auto-advance stays disabled until the last guard drops.
    ///
    /// This is useful when a test needs virtual time to stand still while something
    /// outside the runtime (real I/O, another thread, an external process) catches
    /// up.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime context, or on a runtime whose
    /// flavor is not `current_thread` (auto-advance, like [`pause`], only exists on
    /// the `current_thread` flavor).
    ///
    /// # Examples
    ///
    /// ```
    /// use tokio::time::{self, Duration, Instant};
    ///
    /// #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// async fn main() {
    ///     let guard = time::inhibit_auto_advance();
    ///
    ///     // While the guard is held, the clock only moves when advanced explicitly.
    ///     let start = Instant::now();
    ///     time::advance(Duration::from_millis(100)).await;
    ///     assert_eq!(Instant::now() - start, Duration::from_millis(100));
    ///
    ///     drop(guard);
    ///
    ///     // Auto-advance is enabled again: this sleep completes by advancing the
    ///     // paused clock instead of waiting in real time.
    ///     time::sleep(Duration::from_secs(1)).await;
    /// }
    /// ```
    #[track_caller]
    pub fn inhibit_auto_advance() -> AutoAdvanceGuard {
        use crate::runtime::scheduler;
        use crate::runtime::Handle;

        let handle = match Handle::try_current() {
            Ok(handle) => handle,
            Err(ref e) if e.is_missing_context() => {
                panic!("auto-advance cannot be inhibited from outside the Tokio runtime")
            }
            Err(_) => panic!("{}", crate::util::error::THREAD_LOCAL_DESTROYED_ERROR),
        };

        match &handle.inner {
            scheduler::Handle::CurrentThread(current_thread) => {
                current_thread.driver.clock.inhibit_auto_advance_user();
            }
            #[cfg(feature = "rt-multi-thread")]
            scheduler::Handle::MultiThread(_) => panic!(
                "`time::inhibit_auto_advance()` requires the `current_thread` Tokio runtime. \
                 This is the default Runtime used by `#[tokio::test]."
            ),
        }

        AutoAdvanceGuard { handle }
    }

    impl Clock {
        /// Returns a new `Clock` instance that uses the current execution context's
        /// source of time.
        pub(crate) fn new(enable_pausing: bool, start_paused: bool) -> Clock {
            let now = std::time::Instant::now();

            let clock = Clock {
                inner: Mutex::new(Inner {
                    enable_pausing,
                    base: now,
                    unfrozen: Some(now),
                    blocking_inhibit_count: 0,
                    user_inhibit_count: 0,
                }),
            };

            if start_paused {
                if let Err(msg) = clock.pause() {
                    panic!("{}", msg);
                }
            }

            clock
        }

        pub(crate) fn pause(&self) -> Result<(), &'static str> {
            let mut inner = self.inner.lock();

            if !inner.enable_pausing {
                return Err("`time::pause()` requires the `current_thread` Tokio runtime. \
                        This is the default Runtime used by `#[tokio::test].");
            }

            // Track that we paused the clock
            DID_PAUSE_CLOCK.store(true, Ordering::Release);

            let elapsed = match inner.unfrozen.as_ref() {
                Some(v) => v.elapsed(),
                None => return Err("time is already frozen")
            };
            inner.base += elapsed;
            inner.unfrozen = None;

            Ok(())
        }

        /// Temporarily stop auto-advancing the clock (see `tokio::time::pause`)
        /// on behalf of an outstanding blocking task.
        pub(crate) fn inhibit_auto_advance_blocking(&self) {
            let mut inner = self.inner.lock();
            inner.blocking_inhibit_count += 1;
        }

        pub(crate) fn allow_auto_advance_blocking(&self) {
            let mut inner = self.inner.lock();
            inner.blocking_inhibit_count -= 1;
        }

        /// Temporarily stop auto-advancing the clock on behalf of a user-held
        /// `AutoAdvanceGuard`.
        pub(crate) fn inhibit_auto_advance_user(&self) {
            let mut inner = self.inner.lock();
            inner.user_inhibit_count += 1;
        }

        pub(crate) fn allow_auto_advance_user(&self) {
            let mut inner = self.inner.lock();
            inner.user_inhibit_count -= 1;
        }

        pub(crate) fn can_auto_advance(&self) -> bool {
            let inner = self.inner.lock();
            inner.unfrozen.is_none()
                && inner.blocking_inhibit_count == 0
                && inner.user_inhibit_count == 0
        }

        /// Returns true if any `spawn_blocking` task spawned on this runtime is still
        /// outstanding. Used by the quiesce drain-park hook: outstanding blocking work
        /// implies future wakes, so the runtime is not quiescent.
        pub(crate) fn has_blocking_inhibits(&self) -> bool {
            let inner = self.inner.lock();
            inner.blocking_inhibit_count > 0
        }

        /// Returns true if the clock is currently paused (frozen).
        pub(crate) fn is_paused(&self) -> bool {
            let inner = self.inner.lock();
            inner.unfrozen.is_none()
        }

        pub(crate) fn advance(&self, duration: Duration) -> Result<(), &'static str> {
            let mut inner = self.inner.lock();

            if inner.unfrozen.is_some() {
                return Err("time is not frozen");
            }

            inner.base += duration;
            Ok(())
        }

        pub(crate) fn now(&self) -> Instant {
            let inner = self.inner.lock();

            let mut ret = inner.base;

            if let Some(unfrozen) = inner.unfrozen {
                ret += unfrozen.elapsed();
            }

            Instant::from_std(ret)
        }
    }
}
