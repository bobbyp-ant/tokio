#![warn(rust_2018_idioms)]
#![cfg(feature = "full")]
#![cfg(not(miri))] // Whole-runtime tests; too slow on miri.

use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;

use tokio::time::{self, Duration, Instant};
use tokio_test::{assert_pending, task};

#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_until_fires_timers_within_bound() {
    let start = Instant::now();
    let fired = Arc::new(AtomicUsize::new(0));

    for i in 1..=5u64 {
        let fired = fired.clone();
        tokio::spawn(async move {
            time::sleep(Duration::from_millis(i * 10)).await;
            fired.fetch_add(1, SeqCst);
        });
    }

    // Bound at 30ms: the 10/20/30ms timers fire (inclusive bound); 40/50ms do not.
    let state = time::quiesce_until(start + Duration::from_millis(30)).await;

    assert_eq!(fired.load(SeqCst), 3);
    // The clock sits at the last fired timer (30ms), not at the bound.
    assert_eq!(state.now, start + Duration::from_millis(30));
    // The 40ms timer is within 64ms of `now` => bottom wheel level => exact.
    assert_eq!(state.next_timer, Some(start + Duration::from_millis(40)));
    // AC1.7: the clock has not moved between resolution and return.
    assert_eq!(Instant::now(), state.now);
}

#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_until_drains_transitive_work() {
    let start = Instant::now();
    let result = Arc::new(AtomicUsize::new(0));

    let (tx, rx) = tokio::sync::oneshot::channel();

    {
        let result = result.clone();
        tokio::spawn(async move {
            // Woken by the channel send below (not by a timer).
            rx.await.unwrap();
            // Chain further: spawn another task and wait for it.
            let result2 = result.clone();
            tokio::spawn(async move {
                result2.store(42, SeqCst);
            })
            .await
            .unwrap();
        });
    }

    tokio::spawn(async move {
        time::sleep(Duration::from_millis(5)).await;
        tx.send(()).unwrap();
    });

    let state = time::quiesce_until(start + Duration::from_millis(10)).await;

    // The full chain (timer -> channel -> task -> spawned task) completed.
    assert_eq!(result.load(SeqCst), 42);
    // AC1.2: the clock stops at the last timer (5ms), never advanced to the bound.
    assert_eq!(state.now, start + Duration::from_millis(5));
    assert_eq!(state.next_timer, None);
    assert_eq!(Instant::now(), state.now);
}

#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_until_no_timers_leaves_clock_unchanged() {
    let start = Instant::now();

    let state = time::quiesce_until(start + Duration::from_millis(100)).await;

    assert_eq!(state.now, start);
    assert_eq!(state.next_timer, None);
    assert_eq!(Instant::now(), start);
}

#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_until_bound_is_inclusive() {
    let start = Instant::now();
    let fired_at_bound = Arc::new(AtomicUsize::new(0));
    let fired_after_bound = Arc::new(AtomicUsize::new(0));

    {
        let fired_at_bound = fired_at_bound.clone();
        tokio::spawn(async move {
            time::sleep_until(start + Duration::from_millis(10)).await;
            fired_at_bound.fetch_add(1, SeqCst);
        });
    }
    {
        let fired_after_bound = fired_after_bound.clone();
        tokio::spawn(async move {
            time::sleep_until(start + Duration::from_millis(11)).await;
            fired_after_bound.fetch_add(1, SeqCst);
        });
    }

    let state = time::quiesce_until(start + Duration::from_millis(10)).await;

    assert_eq!(fired_at_bound.load(SeqCst), 1);
    assert_eq!(fired_after_bound.load(SeqCst), 0);
    assert_eq!(state.now, start + Duration::from_millis(10));
    assert_eq!(state.next_timer, Some(start + Duration::from_millis(11)));
}

#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_unbounded_resolves_when_wheel_empties() {
    let start = Instant::now();
    let fired = Arc::new(AtomicUsize::new(0));

    for i in 1..=3u64 {
        let fired = fired.clone();
        tokio::spawn(async move {
            time::sleep(Duration::from_millis(i * 10)).await;
            fired.fetch_add(1, SeqCst);
        });
    }

    let state = time::quiesce().await;

    assert_eq!(fired.load(SeqCst), 3);
    assert_eq!(state.now, start + Duration::from_millis(30));
    assert_eq!(state.next_timer, None);
}

#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_unbounded_empty_wheel_does_not_hang() {
    let start = Instant::now();

    let state = time::quiesce().await;

    assert_eq!(state.now, start);
    assert_eq!(state.next_timer, None);
}

#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_next_timer_is_lower_bound() {
    let start = Instant::now();

    // A timer far in the future: lives in an upper wheel level, so the reported
    // next_timer is the start of its occupied slot -- a lower bound.
    tokio::spawn(async move {
        time::sleep_until(start + Duration::from_millis(10_000)).await;
    });

    let state = time::quiesce_until(start + Duration::from_millis(10)).await;

    // No timer fired; clock unchanged (10s timer's slot start is way beyond 10ms).
    assert_eq!(state.now, start);

    let next = state.next_timer.expect("a timer is pending");
    // Lower bound: never later than the actual deadline...
    assert!(
        next <= start + Duration::from_millis(10_000),
        "next_timer: {next:?}"
    );
    // ...and strictly after `now`.
    assert!(next > state.now, "next_timer: {next:?}");
}

/// When the only pending timer lives in an upper wheel level and the quiesce bound
/// reaches past that timer's occupied-slot start, tokio's existing auto-advance moves
/// the clock to the slot start (a refinement hop) before the bound check can resolve
/// the waiter. The universal invariants still hold: now <= bound, next_timer is a
/// lower bound strictly after now, and Instant::now() == reported now.
#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_until_far_timer_refinement_hops() {
    let start = Instant::now();

    tokio::spawn(async move {
        time::sleep_until(start + Duration::from_millis(5_000)).await;
    });

    let bound = start + Duration::from_millis(4_500);
    let state = time::quiesce_until(bound).await;

    // The 5000ms timer did not fire.
    // `now` never exceeds the bound...
    assert!(state.now <= bound, "now: {:?}", state.now);
    // ...and the clock may have hopped to the timer's occupied-slot start (an
    // existing auto-advance behavior), which is at or after `start`.
    assert!(state.now >= start);
    // next_timer is a lower bound strictly after now, never later than the deadline.
    let next = state.next_timer.expect("timer still pending");
    assert!(next > state.now);
    assert!(next <= start + Duration::from_millis(5_000));
    // AC1.7 always holds.
    assert_eq!(Instant::now(), state.now);
}

#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_until_bound_in_past_drains_without_advancing() {
    let start = Instant::now();

    // Move the clock forward 100ms first.
    time::advance(Duration::from_millis(100)).await;
    let now = Instant::now();
    assert_eq!(now, start + Duration::from_millis(100));

    // A pending timer in the future. 20ms keeps the timer's deadline (tick 120) in
    // the same 64-tick wheel window as the current elapsed tick (100), i.e. in the
    // wheel's bottom level, so the reported next_timer is exact rather than
    // slot-aligned.
    tokio::spawn(async move {
        time::sleep(Duration::from_millis(20)).await;
    });

    // Bound before the current instant: resolves once nothing is runnable, clock
    // unchanged, pending timer untouched.
    let state = time::quiesce_until(start).await;

    assert_eq!(state.now, now);
    assert_eq!(state.next_timer, Some(now + Duration::from_millis(20)));
    assert_eq!(Instant::now(), now);
}

#[cfg(feature = "test-util")]
#[test]
fn quiesce_as_block_on_root_future() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();

    // Read the virtual clock (requires the runtime context).
    let start = {
        let _enter = rt.enter();
        Instant::now()
    };

    let fired = Arc::new(AtomicUsize::new(0));
    {
        let fired = fired.clone();
        let _enter = rt.enter();
        rt.spawn(async move {
            time::sleep(Duration::from_millis(10)).await;
            fired.fetch_add(1, SeqCst);
        });
    }

    // NOTE: the Quiesce future is constructed HERE, outside any runtime context.
    // This must not panic: all validation happens on first poll, inside block_on.
    let state = rt.block_on(time::quiesce_until(start + Duration::from_millis(20)));

    assert_eq!(fired.load(SeqCst), 1);
    assert_eq!(state.now, start + Duration::from_millis(10));
    assert_eq!(state.next_timer, None);
}

#[cfg(feature = "test-util")]
#[test]
fn quiesce_windowed_stepping() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();

    let start = {
        let _enter = rt.enter();
        Instant::now()
    };

    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    {
        let log = log.clone();
        let _enter = rt.enter();
        rt.spawn(async move {
            for i in 1..=6u64 {
                time::sleep_until(start + Duration::from_millis(i * 5)).await;
                log.lock().unwrap().push(i);
            }
        });
    }

    // Step in 10ms windows: each window should run exactly two of the 5ms-spaced
    // events.
    let mut window_end = start;
    let mut reports = Vec::new();
    for _ in 0..3 {
        window_end += Duration::from_millis(10);
        let state = rt.block_on(time::quiesce_until(window_end));
        reports.push(state);
    }

    assert_eq!(*log.lock().unwrap(), vec![1, 2, 3, 4, 5, 6]);
    // Window reports: clock stops at the last event of each window.
    assert_eq!(reports[0].now, start + Duration::from_millis(10));
    assert_eq!(reports[1].now, start + Duration::from_millis(20));
    assert_eq!(reports[2].now, start + Duration::from_millis(30));
    assert_eq!(
        reports[0].next_timer,
        Some(start + Duration::from_millis(15))
    );
    assert_eq!(
        reports[1].next_timer,
        Some(start + Duration::from_millis(25))
    );
    assert_eq!(reports[2].next_timer, None);
}

#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_awaited_in_spawned_task() {
    let start = Instant::now();

    tokio::spawn(async move {
        time::sleep(Duration::from_millis(10)).await;
    });

    let waiter =
        tokio::spawn(async move { time::quiesce_until(start + Duration::from_millis(20)).await });

    let state = waiter.await.unwrap();
    assert_eq!(state.now, start + Duration::from_millis(10));
    assert_eq!(state.next_timer, None);
}

// ===== Drain-park hook re-check coverage =====
//
// The tests in this section exercise the scheduler hook's runnable-work re-check:
// work that becomes visible only at the drain park (IO readiness, cross-thread
// wakes, outstanding blocking tasks). The hook's zero-timeout driver poll consumes
// any pending wakeup, so discovering work there and parking anyway would hang the
// runtime forever.

/// IO readiness that arrives between the scheduler's last driver poll and the
/// drain park is surfaced by the hook's zero-timeout poll. The woken task must run
/// (the park must be skipped) instead of the runtime parking on a wakeup that the
/// poll just consumed.
#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_io_readiness_discovered_at_drain_park() {
    use std::io::Write;
    use tokio::io::AsyncReadExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    let (mut server, _) = listener.accept().await.unwrap();

    let read_task = tokio::spawn(async move {
        let mut buf = [0u8; 5];
        server.read_exact(&mut buf).await.unwrap();
        buf
    });
    // Let the read task register IO interest.
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    // Readiness arrives between the last driver poll and the drain park.
    client.write_all(b"hello").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Unbounded quiesce on an empty wheel: must NOT hang.
    let state = time::quiesce().await;
    assert!(state.next_timer.is_none());
    assert_eq!(&read_task.await.unwrap(), b"hello");
}

/// An outstanding `spawn_blocking` task inhibits quiesce resolution: the hook's
/// blocking-inhibit branch parks the runtime, and the blocking task's completion
/// (which releases the inhibit and then unparks the driver) lets the quiesce
/// resolve afterwards.
#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_waits_for_outstanding_blocking_task() {
    use std::sync::atomic::AtomicBool;

    let done = Arc::new(AtomicBool::new(false));

    let blocking = {
        let done = done.clone();
        tokio::task::spawn_blocking(move || {
            std::thread::sleep(std::time::Duration::from_millis(200));
            done.store(true, SeqCst);
        })
    };

    // Unbounded quiesce: must not resolve while the blocking task is outstanding
    // (outstanding blocking work implies future wakes).
    let state = time::quiesce().await;

    // The store happens before the blocking task completes, which happens before
    // the inhibit release that allows the quiesce to resolve.
    assert!(
        done.load(SeqCst),
        "quiesce resolved while a blocking task was still outstanding"
    );
    assert!(state.next_timer.is_none());
    blocking.await.unwrap();
}

/// A cross-thread wake (a foreign thread completing a oneshot a spawned task is
/// awaiting) that lands during a quiesce step must run the woken task before the
/// quiesce resolves, and must never strand the runtime in a park whose wakeup was
/// already consumed.
#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_cross_thread_wake_during_step() {
    use std::sync::atomic::AtomicBool;

    let (tx, rx) = tokio::sync::oneshot::channel::<u32>();
    let received = Arc::new(AtomicBool::new(false));

    let task = {
        let received = received.clone();
        tokio::spawn(async move {
            let value = rx.await.unwrap();
            received.store(true, SeqCst);
            value
        })
    };

    // Let the task register with the channel.
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }

    // The blocking-pool thread is a foreign thread: the send wakes the channel task
    // through the cross-thread schedule path (inject queue push + driver unpark).
    // The send happens before the blocking task completes, so the quiesce (which
    // cannot resolve until the blocking task's inhibit is released) is still in
    // progress when the wake lands.
    let blocking = tokio::task::spawn_blocking(move || {
        tx.send(42).unwrap();
    });

    let state = time::quiesce().await;

    // The cross-thread woken task ran to completion before the quiesce resolved.
    assert!(
        received.load(SeqCst),
        "quiesce resolved before the cross-thread woken task ran"
    );
    assert!(state.next_timer.is_none());
    assert_eq!(task.await.unwrap(), 42);
    blocking.await.unwrap();
}

#[cfg(feature = "test-util")]
#[tokio::test]
#[should_panic(expected = "requires the runtime's clock to be paused")]
async fn quiesce_unpaused_clock_panics() {
    // Clock not paused (no start_paused, no time::pause()).
    let _ = time::quiesce().await;
}

#[cfg(feature = "test-util")]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[should_panic(expected = "requires the `current_thread` Tokio runtime")]
async fn quiesce_multi_thread_panics() {
    let _ = time::quiesce().await;
}

/// A `Quiesce` future first-polled from a thread that only holds a `Handle::enter`
/// guard must wake the target runtime: registration unparks the runtime's driver so
/// a parked runtime re-runs its drain-park hook and notices the new waiter. Without
/// the unpark, the waiter would only resolve when the runtime woke for some other
/// reason.
#[cfg(feature = "test-util")]
#[test]
fn quiesce_from_enter_guard_thread_wakes_parked_runtime() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();

    let handle = rt.handle().clone();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();
    let (root_tx, root_rx) = tokio::sync::oneshot::channel::<()>();

    // Park the runtime on its own thread, blocked on a future that resolves only
    // after the quiesce below completes.
    let rt_thread = std::thread::spawn(move || {
        rt.block_on(async move {
            ready_tx.send(()).unwrap();
            root_rx.await.unwrap();
        });
    });

    // Wait for the runtime to start, then give it time to reach its park. (If it has
    // not parked yet, the test still passes -- the hook sees the waiter on the way to
    // the park -- it just does not exercise the interesting interleaving.)
    ready_rx.recv().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(50));

    // From this thread, holding only an enter guard, an unbounded quiesce must
    // resolve: nothing else will wake the parked runtime.
    {
        let _enter = handle.enter();
        let state = futures::executor::block_on(time::quiesce());
        assert!(state.next_timer.is_none());
    }

    // Unblock the root future and shut down cleanly.
    root_tx.send(()).unwrap();
    rt_thread.join().unwrap();
}

/// A `Quiesce` future that outlives its runtime panics with the standard
/// runtime-shutdown message when polled, not an internal registry error. The driver
/// drains the waiter registry at shutdown, so the waiter is gone by the time this
/// poll runs.
#[cfg(feature = "test-util")]
#[test]
#[should_panic(expected = "A Tokio 1.x context was found, but it is being shutdown.")]
fn quiesce_polled_after_shutdown_panics() {
    use futures::task::noop_waker_ref;
    use std::future::Future;
    use std::task::Context;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();

    let mut quiesce = Box::pin(time::quiesce());

    // First poll registers the waiter with the runtime's time driver.
    {
        let _enter = rt.enter();
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(quiesce.as_mut().poll(&mut cx).is_pending());
    }

    // Shutting the runtime down drains the waiter registry.
    drop(rt);

    // The orphaned future must report the shutdown when polled again.
    let mut cx = Context::from_waker(noop_waker_ref());
    let _ = quiesce.as_mut().poll(&mut cx);
}

/// `advance()` while a quiesce step is registered panics: an explicit advance would
/// move the clock past the step's bound and break reproducibility.
#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
#[should_panic(expected = "cannot be called while a `quiesce()` is in progress")]
async fn advance_during_quiesce_panics() {
    let start = Instant::now();

    // A spawned task holds an unbounded quiesce open (never resolves: the timer
    // below keeps the wheel non-empty).
    tokio::spawn(async {
        let _ = time::quiesce().await;
    });
    // A pending timer so the quiesce waiter cannot resolve.
    tokio::spawn(async move {
        time::sleep_until(start + Duration::from_millis(100)).await;
    });

    // Let the spawned tasks run (and the waiter register) by yielding a few times.
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    time::advance(Duration::from_millis(10)).await;
}

#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
#[should_panic(expected = "cannot be called while a `quiesce()` is in progress")]
async fn resume_during_quiesce_panics() {
    let start = Instant::now();

    tokio::spawn(async {
        let _ = time::quiesce().await;
    });
    tokio::spawn(async move {
        time::sleep_until(start + Duration::from_millis(100)).await;
    });

    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    time::resume();
}

// ===== Interaction semantics (rt-quiesce.AC2) =====

/// rt-quiesce.AC2.1: an outstanding `spawn_blocking` task defers quiesce resolution;
/// the step returns only after the blocking task completed AND the async task
/// awaiting it has been polled (its completion processed).
///
/// Complements `quiesce_waits_for_outstanding_blocking_task` (unbounded quiesce
/// observing the blocking closure's own side effect): this test uses a bounded step
/// and observes the completion processing of an async task awaiting the blocking
/// task's `JoinHandle`, plus that the clock never moves.
#[cfg(feature = "test-util")]
#[test]
fn quiesce_until_processes_blocking_task_completion() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();

    let start = {
        let _enter = rt.enter();
        Instant::now()
    };

    let completion_processed = Arc::new(AtomicUsize::new(0));
    {
        let completion_processed = completion_processed.clone();
        let _enter = rt.enter();
        rt.spawn(async move {
            tokio::task::spawn_blocking(|| {
                std::thread::sleep(Duration::from_millis(100));
            })
            .await
            .unwrap();
            // This line is "the completion has been processed".
            completion_processed.fetch_add(1, SeqCst);
        });
    }

    let wall_start = std::time::Instant::now();
    let state = rt.block_on(time::quiesce_until(start + Duration::from_millis(10)));
    let elapsed = wall_start.elapsed();

    // The step waited for the blocking task (~100ms wall time) and the awaiting
    // task ran to completion before resolution.
    assert_eq!(completion_processed.load(SeqCst), 1);
    assert!(
        elapsed >= Duration::from_millis(100),
        "elapsed: {elapsed:?}"
    );
    // No timer was involved; the clock did not move.
    assert_eq!(state.now, start);
    assert_eq!(state.next_timer, None);
}

/// rt-quiesce.AC2.2: a held `AutoAdvanceGuard` with a timer at-or-below the bound
/// makes the step wait in real time; dropping the guard from another thread lets the
/// in-progress step complete (timer fires, work runs, then resolution) without
/// restarting it.
#[cfg(feature = "test-util")]
#[test]
fn quiesce_waits_for_guard_when_timer_within_bound() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();

    let start = {
        let _enter = rt.enter();
        Instant::now()
    };

    let guard = {
        let _enter = rt.enter();
        time::inhibit_auto_advance()
    };

    let fired = Arc::new(AtomicUsize::new(0));
    {
        let fired = fired.clone();
        let _enter = rt.enter();
        rt.spawn(async move {
            time::sleep_until(start + Duration::from_millis(5)).await;
            fired.fetch_add(1, SeqCst);
        });
    }

    let wall_start = std::time::Instant::now();
    let th = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        drop(guard);
    });

    // Timer at 5ms <= bound 10ms: cannot resolve while the guard blocks auto-advance.
    let state = rt.block_on(time::quiesce_until(start + Duration::from_millis(10)));
    let elapsed = wall_start.elapsed();

    // The guard was honored: the step did not complete before the drop...
    assert!(
        elapsed >= Duration::from_millis(100),
        "elapsed: {elapsed:?}"
    );
    // ...and the drop completed the in-progress step promptly.
    assert!(elapsed < Duration::from_secs(10), "elapsed: {elapsed:?}");
    // The timer fired (after the guard dropped) and its work ran before resolution.
    assert_eq!(fired.load(SeqCst), 1);
    assert_eq!(state.now, start + Duration::from_millis(5));
    assert_eq!(state.next_timer, None);
    th.join().unwrap();
}

/// rt-quiesce.AC2.3: a held guard does NOT prevent resolution when no timer at or
/// below the bound is pending (the guard only blocks auto-advance, not quiescence).
#[cfg(feature = "test-util")]
#[test]
fn quiesce_resolves_despite_guard_when_no_timer_within_bound() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();

    let start = {
        let _enter = rt.enter();
        Instant::now()
    };

    // Hold a guard for the entire test (dropped at the end, same thread).
    let guard = {
        let _enter = rt.enter();
        time::inhibit_auto_advance()
    };

    // A timer strictly BEYOND the bound. 50ms keeps its deadline within the wheel's
    // bottom level (within 64ms of `now`), so the reported next_timer is exact.
    {
        let _enter = rt.enter();
        rt.spawn(async move {
            time::sleep_until(start + Duration::from_millis(50)).await;
        });
    }

    let wall_start = std::time::Instant::now();
    let state = rt.block_on(time::quiesce_until(start + Duration::from_millis(10)));
    let elapsed = wall_start.elapsed();

    // Resolved promptly in real time (no waiting for the guard).
    assert!(elapsed < Duration::from_secs(5), "elapsed: {elapsed:?}");
    assert_eq!(state.now, start);
    // The 50ms timer is still pending and within the bottom wheel level => exact.
    assert_eq!(state.next_timer, Some(start + Duration::from_millis(50)));

    drop(guard);
}

/// rt-quiesce.AC2.4: concurrent waiters resolve according to their own bounds. On a
/// single drain-park, every waiter whose bound lies below the next expiration
/// resolves, and the clock does not advance on a cycle that resolved waiters.
#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn multiple_waiters_resolve_per_their_bounds() {
    let start = Instant::now();

    // One timer at 50ms (within the wheel's bottom level => exact next_timer).
    tokio::spawn(async move {
        time::sleep_until(start + Duration::from_millis(50)).await;
    });

    // Three waiters: bounds at 10ms and 30ms (below the timer) and 100ms (above it).
    let a =
        tokio::spawn(async move { time::quiesce_until(start + Duration::from_millis(10)).await });
    let b =
        tokio::spawn(async move { time::quiesce_until(start + Duration::from_millis(30)).await });
    let c =
        tokio::spawn(async move { time::quiesce_until(start + Duration::from_millis(100)).await });

    let (ra, rb, rc) = tokio::join!(a, b, c);
    let (ra, rb, rc) = (ra.unwrap(), rb.unwrap(), rc.unwrap());

    // A and B resolved with the clock untouched: their bounds are below the 50ms
    // timer, and no advance happened on the resolving cycle.
    assert_eq!(ra.now, start);
    assert_eq!(rb.now, start);
    assert_eq!(ra.next_timer, Some(start + Duration::from_millis(50)));
    assert_eq!(rb.next_timer, Some(start + Duration::from_millis(50)));

    // C resolved only after the 50ms timer fired.
    assert_eq!(rc.now, start + Duration::from_millis(50));
    assert_eq!(rc.next_timer, None);
}

/// rt-quiesce.AC2.7: dropping an unresolved `Quiesce` deregisters its waiter, and
/// subsequent auto-advance behavior is unchanged.
///
/// Deregistration is observed two ways: (1) `advance()` works again (it panics while
/// any waiter is registered), and (2) ordinary auto-advance still fires timers.
#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn dropping_unresolved_quiesce_deregisters() {
    let start = Instant::now();

    // A pending timer that keeps the wheel non-empty (so the waiter cannot resolve).
    tokio::spawn(async move {
        time::sleep_until(start + Duration::from_millis(100)).await;
    });
    // Make sure the spawned task has registered its timer.
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }

    {
        // Manually poll a quiesce future so it registers, then drop it unresolved.
        let mut quiesce = task::spawn(time::quiesce_until(start + Duration::from_millis(200)));
        assert_pending!(quiesce.poll());
    } // <- dropped here; the waiter must be deregistered

    // (1) If a waiter were still registered, this would panic
    //     ("cannot be called while a `quiesce()` is in progress").
    time::advance(Duration::from_millis(1)).await;

    // (2) Normal auto-advance still works: the 100ms timer fires by sleeping to it.
    time::sleep_until(start + Duration::from_millis(100)).await;
    assert_eq!(Instant::now(), start + Duration::from_millis(100));
}

/// rt-quiesce.AC2.5: many paused runtimes in one process step independently, driven
/// concurrently from different controller threads, without affecting each other's
/// clocks or reports.
#[cfg(feature = "test-util")]
#[test]
fn many_runtimes_step_independently_from_threads() {
    // Each "island" gets its own timer cadence; each is stepped by its own thread.
    let mut threads = Vec::new();

    for island in 1..=4u64 {
        threads.push(std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .unwrap();

            let start = {
                let _enter = rt.enter();
                Instant::now()
            };

            let log = Arc::new(std::sync::Mutex::new(Vec::new()));
            {
                let log = log.clone();
                let _enter = rt.enter();
                // Island i fires events every i*10 ms.
                rt.spawn(async move {
                    for n in 1..=4u64 {
                        time::sleep_until(start + Duration::from_millis(n * island * 10)).await;
                        log.lock().unwrap().push(n * island * 10);
                    }
                });
            }

            // Step in 4 windows of island*10 ms each: exactly one event per window.
            let mut nows = Vec::new();
            for w in 1..=4u64 {
                let state = rt.block_on(time::quiesce_until(
                    start + Duration::from_millis(w * island * 10),
                ));
                // Report positions are island-local virtual offsets.
                nows.push(state.now - start);
            }

            let events = log.lock().unwrap().clone();
            (island, events, nows)
        }));
    }

    for th in threads {
        let (island, log, nows) = th.join().unwrap();
        // Each island saw exactly its own cadence, unaffected by the other islands
        // stepping concurrently in the same process.
        let expected_log: Vec<u64> = (1..=4).map(|n| n * island * 10).collect();
        assert_eq!(log, expected_log, "island {island}");
        let expected_nows: Vec<Duration> = (1..=4)
            .map(|n| Duration::from_millis(n * island * 10))
            .collect();
        assert_eq!(nows, expected_nows, "island {island}");
    }
}

/// rt-quiesce.AC2.8 (quiesce-context variant): a guard targets the runtime it was
/// created on. Dropping A's guard while B's context is current releases A's inhibit,
/// letting A's in-progress quiesce step complete. (The complementary assertion --
/// that the drop never releases the AMBIENT runtime's inhibit -- is covered by
/// `auto_advance_guard_targets_originating_runtime` in tests/time_pause.rs.)
#[cfg(feature = "test-util")]
#[test]
fn guard_dropped_in_other_runtime_context_releases_originator() {
    let rt_a = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();
    let rt_b = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();

    let start_a = {
        let _enter = rt_a.enter();
        Instant::now()
    };

    let guard_a = {
        let _enter = rt_a.enter();
        time::inhibit_auto_advance()
    };

    // A timer within A's bound, so A's step can only complete after guard_a drops.
    {
        let _enter = rt_a.enter();
        rt_a.spawn(async move {
            time::sleep_until(start_a + Duration::from_millis(5)).await;
        });
    }

    let handle_b = rt_b.handle().clone();
    let wall_start = std::time::Instant::now();
    let th = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        // Drop A's guard while B's context is current on this thread.
        let _enter_b = handle_b.enter();
        drop(guard_a);
    });

    // A's step completes after ~100ms (guard_a released A despite B being current at
    // drop time).
    let state = rt_a.block_on(time::quiesce_until(start_a + Duration::from_millis(10)));
    let elapsed = wall_start.elapsed();

    assert!(
        elapsed >= Duration::from_millis(100),
        "elapsed: {elapsed:?}"
    );
    assert!(elapsed < Duration::from_secs(10), "elapsed: {elapsed:?}");
    assert_eq!(state.now, start_a + Duration::from_millis(5));
    assert_eq!(state.next_timer, None);

    th.join().unwrap();
    drop(rt_b);
}

/// rt-quiesce.AC2.6: the API behaves identically on `LocalRuntime`, including with
/// !Send tasks spawned via `spawn_local`.
#[cfg(feature = "test-util")]
#[test]
fn quiesce_on_local_runtime() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build_local(tokio::runtime::LocalOptions::default())
        .unwrap();

    let start = {
        let _enter = rt.enter();
        Instant::now()
    };

    let fired = Arc::new(AtomicUsize::new(0));
    {
        let fired = fired.clone();
        let _enter = rt.enter();
        rt.spawn_local(async move {
            // A !Send value held across an await proves this is really a local task.
            let rc = std::rc::Rc::new(1u64);
            time::sleep_until(start + Duration::from_millis(10)).await;
            fired.fetch_add(*rc as usize, SeqCst);
        });
    }

    let state = rt.block_on(time::quiesce_until(start + Duration::from_millis(20)));

    assert_eq!(fired.load(SeqCst), 1);
    assert_eq!(state.now, start + Duration::from_millis(10));
    assert_eq!(state.next_timer, None);
    assert_eq!(
        {
            let _enter = rt.enter();
            Instant::now()
        },
        state.now
    );
}

/// EXPLORATORY (non-contractual): `Quiesce` awaited inside `LocalSet::run_until`.
///
/// `LocalSet` is explicitly out of scope for the quiesce contract (see the design
/// plan); this test documents observed behavior rather than a guarantee. If it
/// fails after a tokio upgrade, re-evaluate rather than treating it as a regression
/// of the quiesce contract.
#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn quiesce_inside_local_set_run_until_exploratory() {
    let start = Instant::now();
    let local = tokio::task::LocalSet::new();

    let fired = Arc::new(AtomicUsize::new(0));
    {
        let fired = fired.clone();
        local.spawn_local(async move {
            time::sleep_until(start + Duration::from_millis(10)).await;
            fired.fetch_add(1, SeqCst);
        });
    }

    let state = local
        .run_until(time::quiesce_until(start + Duration::from_millis(20)))
        .await;

    // Observed behavior: LocalSet's self-waking design composes with quiesce; the
    // local task's timer fires and the step resolves after it.
    assert_eq!(fired.load(SeqCst), 1);
    assert_eq!(state.now, start + Duration::from_millis(10));
}
