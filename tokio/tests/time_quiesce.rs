#![warn(rust_2018_idioms)]
#![cfg(feature = "full")]
#![cfg(not(miri))] // Whole-runtime tests; too slow on miri.

use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;

use tokio::time::{self, Duration, Instant};

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
