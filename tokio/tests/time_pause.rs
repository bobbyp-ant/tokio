#![warn(rust_2018_idioms)]
#![cfg(feature = "full")]
#![cfg(not(miri))] // Too slow on miri.

use rand::SeedableRng;
use rand::{rngs::StdRng, Rng};
use tokio::time::{self, Duration, Instant, Sleep};
use tokio_test::{assert_elapsed, assert_pending, assert_ready, assert_ready_eq, task};

#[cfg(not(target_os = "wasi"))]
use tokio_test::assert_err;

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

#[tokio::test]
async fn pause_time_in_main() {
    tokio::time::pause();
}

#[tokio::test]
async fn pause_time_in_task() {
    let t = tokio::spawn(async {
        tokio::time::pause();
    });

    t.await.unwrap();
}

#[cfg(all(feature = "full", not(target_os = "wasi")))] // Wasi doesn't support threads
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[should_panic]
async fn pause_time_in_main_threads() {
    tokio::time::pause();
}

#[cfg_attr(panic = "abort", ignore)]
#[cfg(all(feature = "full", not(target_os = "wasi")))] // Wasi doesn't support threads
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn pause_time_in_spawn_threads() {
    let t = tokio::spawn(async {
        tokio::time::pause();
    });

    assert_err!(t.await);
}

#[test]
fn paused_time_is_deterministic() {
    let run_1 = paused_time_stress_run();
    let run_2 = paused_time_stress_run();

    assert_eq!(run_1, run_2);
}

#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn paused_time_stress_run() -> Vec<Duration> {
    let mut rng = StdRng::seed_from_u64(1);

    let mut times = vec![];
    let start = Instant::now();
    for _ in 0..10_000 {
        let sleep = rng.random_range(Duration::from_secs(0)..Duration::from_secs(1));
        time::sleep(sleep).await;
        times.push(start.elapsed());
    }

    times
}

#[tokio::test(start_paused = true)]
async fn advance_after_poll() {
    time::sleep(ms(1)).await;

    let start = Instant::now();

    let mut sleep = task::spawn(time::sleep_until(start + ms(300)));

    assert_pending!(sleep.poll());

    let before = Instant::now();
    time::advance(ms(100)).await;
    assert_elapsed!(before, ms(100));

    assert_pending!(sleep.poll());
}

#[tokio::test(start_paused = true)]
async fn sleep_no_poll() {
    let start = Instant::now();

    // TODO: Skip this
    time::advance(ms(1)).await;

    let mut sleep = task::spawn(time::sleep_until(start + ms(300)));

    let before = Instant::now();
    time::advance(ms(100)).await;
    assert_elapsed!(before, ms(100));

    assert_pending!(sleep.poll());
}

enum State {
    Begin,
    AwaitingAdvance(Pin<Box<dyn Future<Output = ()>>>),
    AfterAdvance,
}

struct Tester {
    sleep: Pin<Box<Sleep>>,
    state: State,
    before: Option<Instant>,
    poll: bool,
}

impl Future for Tester {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.state {
            State::Begin => {
                if self.poll {
                    assert_pending!(self.sleep.as_mut().poll(cx));
                }
                self.before = Some(Instant::now());
                let advance_fut = Box::pin(time::advance(ms(100)));
                self.state = State::AwaitingAdvance(advance_fut);
                self.poll(cx)
            }
            State::AwaitingAdvance(ref mut advance_fut) => match advance_fut.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(()) => {
                    self.state = State::AfterAdvance;
                    self.poll(cx)
                }
            },
            State::AfterAdvance => {
                assert_elapsed!(self.before.unwrap(), ms(100));

                assert_pending!(self.sleep.as_mut().poll(cx));

                Poll::Ready(())
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn sleep_same_task() {
    let start = Instant::now();

    // TODO: Skip this
    time::advance(ms(1)).await;

    let sleep = Box::pin(time::sleep_until(start + ms(300)));

    Tester {
        sleep,
        state: State::Begin,
        before: None,
        poll: true,
    }
    .await;
}

#[tokio::test(start_paused = true)]
async fn sleep_same_task_no_poll() {
    let start = Instant::now();

    // TODO: Skip this
    time::advance(ms(1)).await;

    let sleep = Box::pin(time::sleep_until(start + ms(300)));

    Tester {
        sleep,
        state: State::Begin,
        before: None,
        poll: false,
    }
    .await;
}

#[tokio::test(start_paused = true)]
async fn interval() {
    let start = Instant::now();

    // TODO: Skip this
    time::advance(ms(1)).await;

    let mut i = task::spawn(time::interval_at(start, ms(300)));

    assert_ready_eq!(poll_next(&mut i), start);
    assert_pending!(poll_next(&mut i));

    let before = Instant::now();
    time::advance(ms(100)).await;
    assert_elapsed!(before, ms(100));
    assert_pending!(poll_next(&mut i));

    let before = Instant::now();
    time::advance(ms(200)).await;
    assert_elapsed!(before, ms(200));
    assert_ready_eq!(poll_next(&mut i), start + ms(300));
    assert_pending!(poll_next(&mut i));

    let before = Instant::now();
    time::advance(ms(400)).await;
    assert_elapsed!(before, ms(400));
    assert_ready_eq!(poll_next(&mut i), start + ms(600));
    assert_pending!(poll_next(&mut i));

    let before = Instant::now();
    time::advance(ms(500)).await;
    assert_elapsed!(before, ms(500));
    assert_ready_eq!(poll_next(&mut i), start + ms(900));
    assert_ready_eq!(poll_next(&mut i), start + ms(1200));
    assert_pending!(poll_next(&mut i));
}

#[tokio::test(start_paused = true)]
async fn test_time_advance_sub_ms() {
    let now = Instant::now();

    let dur = Duration::from_micros(51_592);
    time::advance(dur).await;

    assert_eq!(now.elapsed(), dur);

    let now = Instant::now();
    let dur = Duration::from_micros(1);
    time::advance(dur).await;

    assert_eq!(now.elapsed(), dur);
}

#[tokio::test(start_paused = true)]
async fn test_time_advance_3ms_and_change() {
    let now = Instant::now();

    let dur = Duration::from_micros(3_141_592);
    time::advance(dur).await;

    assert_eq!(now.elapsed(), dur);

    let now = Instant::now();
    let dur = Duration::from_micros(3_123_456);
    time::advance(dur).await;

    assert_eq!(now.elapsed(), dur);
}

#[tokio::test(start_paused = true)]
async fn regression_3710_with_submillis_advance() {
    let start = Instant::now();

    time::advance(Duration::from_millis(1)).await;

    let mut sleep = task::spawn(time::sleep_until(start + Duration::from_secs(60)));

    assert_pending!(sleep.poll());

    let before = Instant::now();
    let dur = Duration::from_micros(51_592);
    time::advance(dur).await;
    assert_eq!(before.elapsed(), dur);

    assert_pending!(sleep.poll());
}

#[tokio::test(start_paused = true)]
async fn exact_1ms_advance() {
    let now = Instant::now();

    let dur = Duration::from_millis(1);
    time::advance(dur).await;

    assert_eq!(now.elapsed(), dur);

    let now = Instant::now();
    let dur = Duration::from_millis(1);
    time::advance(dur).await;

    assert_eq!(now.elapsed(), dur);
}

#[tokio::test(start_paused = true)]
async fn advance_once_with_timer() {
    let mut sleep = task::spawn(time::sleep(Duration::from_millis(1)));
    assert_pending!(sleep.poll());

    time::advance(Duration::from_micros(250)).await;
    assert_pending!(sleep.poll());

    time::advance(Duration::from_micros(1500)).await;

    assert!(sleep.is_woken());
    assert_ready!(sleep.poll());
}

#[tokio::test(start_paused = true)]
async fn advance_multi_with_timer() {
    // Round to the nearest ms
    // time::sleep(Duration::from_millis(1)).await;

    let mut sleep = task::spawn(time::sleep(Duration::from_millis(1)));
    assert_pending!(sleep.poll());

    time::advance(Duration::from_micros(250)).await;
    assert_pending!(sleep.poll());

    time::advance(Duration::from_micros(250)).await;
    assert_pending!(sleep.poll());

    time::advance(Duration::from_micros(250)).await;
    assert_pending!(sleep.poll());

    time::advance(Duration::from_micros(250)).await;
    assert!(sleep.is_woken());
    assert_ready!(sleep.poll());
}

fn poll_next(interval: &mut task::Spawn<time::Interval>) -> Poll<Instant> {
    interval.enter(|cx, mut interval| interval.poll_tick(cx))
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// While an `AutoAdvanceGuard` is held, a paused runtime cannot auto-advance, so a
/// sleep waits in real time. Dropping the guard from another thread unparks the
/// runtime promptly and lets auto-advance fire the sleep.
#[cfg(feature = "test-util")]
#[test]
fn auto_advance_guard_inhibits_and_drop_unparks() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();

    let guard = {
        let _enter = rt.enter();
        time::inhibit_auto_advance()
    };

    let wall_start = std::time::Instant::now();

    let th = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        drop(guard);
    });

    // 15s of virtual time. With the guard held this cannot auto-advance; it can only
    // complete after the guard drops at ~100ms of real time.
    rt.block_on(async { time::sleep(Duration::from_secs(15)).await });

    let elapsed = wall_start.elapsed();
    // The guard was honored: the sleep did not complete before the drop.
    assert!(
        elapsed >= Duration::from_millis(100),
        "elapsed: {elapsed:?}"
    );
    // The drop unparked the runtime promptly: we did not wait out the 15s in real time.
    assert!(elapsed < Duration::from_secs(10), "elapsed: {elapsed:?}");
    th.join().unwrap();
}

/// Auto-advance resumes only after the LAST guard drops.
#[cfg(feature = "test-util")]
#[test]
fn auto_advance_guards_are_counted() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();

    let (g1, g2) = {
        let _enter = rt.enter();
        (time::inhibit_auto_advance(), time::inhibit_auto_advance())
    };

    let wall_start = std::time::Instant::now();

    let th = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        drop(g1);
        std::thread::sleep(Duration::from_millis(100));
        drop(g2);
    });

    rt.block_on(async { time::sleep(Duration::from_secs(15)).await });

    let elapsed = wall_start.elapsed();
    // If the first drop had released the inhibit (counting bug), the sleep would have
    // completed at ~100ms.
    assert!(
        elapsed >= Duration::from_millis(200),
        "elapsed: {elapsed:?}"
    );
    assert!(elapsed < Duration::from_secs(10), "elapsed: {elapsed:?}");
    th.join().unwrap();
}

/// Holding a guard only disables AUTO-advance; explicit `advance()` still moves the
/// clock.
#[cfg(feature = "test-util")]
#[tokio::test(start_paused = true)]
async fn explicit_advance_works_while_guard_held() {
    let _guard = time::inhibit_auto_advance();

    let start = Instant::now();
    time::advance(Duration::from_millis(100)).await;
    assert_eq!(Instant::now() - start, Duration::from_millis(100));
}

/// A guard affects only the runtime it was created on. Dropping it while a different
/// runtime's context is current releases the ORIGINATING runtime's inhibit; dropping
/// a guard with no runtime context at all also works.
#[cfg(feature = "test-util")]
#[test]
fn auto_advance_guard_targets_originating_runtime() {
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

    let guard_a = {
        let _enter = rt_a.enter();
        time::inhibit_auto_advance()
    };
    let guard_b = {
        let _enter = rt_b.enter();
        time::inhibit_auto_advance()
    };

    let handle_b = rt_b.handle().clone();
    let wall_start = std::time::Instant::now();

    let th = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        {
            // Drop A's guard while B's context is current on this thread. This must
            // release A's inhibit (not B's).
            let _enter_b = handle_b.enter();
            drop(guard_a);
        }
        std::thread::sleep(Duration::from_millis(100));
        // Drop B's guard with no runtime context current at all.
        drop(guard_b);
    });

    // A unblocks at ~100ms: guard_a's drop released A even though B's context was
    // current at drop time.
    rt_a.block_on(async { time::sleep(Duration::from_secs(15)).await });
    let elapsed_a = wall_start.elapsed();
    assert!(
        elapsed_a >= Duration::from_millis(100),
        "elapsed_a: {elapsed_a:?}"
    );
    assert!(
        elapsed_a < Duration::from_secs(10),
        "elapsed_a: {elapsed_a:?}"
    );

    // B unblocks only at ~200ms: guard_a's drop did NOT release B's inhibit even
    // though B's context was current; only guard_b's own drop did.
    rt_b.block_on(async { time::sleep(Duration::from_secs(15)).await });
    let elapsed_b = wall_start.elapsed();
    assert!(
        elapsed_b >= Duration::from_millis(200),
        "elapsed_b: {elapsed_b:?}"
    );
    assert!(
        elapsed_b < Duration::from_secs(20),
        "elapsed_b: {elapsed_b:?}"
    );

    th.join().unwrap();
}

/// Runs a paused runtime with two independent auto-advance inhibits in place: an
/// `AutoAdvanceGuard` dropped from another thread after `guard_drop_after` of real
/// time, and a `spawn_blocking` task that runs for `blocking_runs_for` of real time.
/// Returns the wall-clock time at which a long virtual-time `sleep` completed; the
/// sleep can only complete once BOTH inhibits are released.
#[cfg(feature = "test-util")]
fn sleep_elapsed_with_inhibits(
    guard_drop_after: Duration,
    blocking_runs_for: Duration,
) -> Duration {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();

    let guard = {
        let _enter = rt.enter();
        time::inhibit_auto_advance()
    };

    let wall_start = std::time::Instant::now();

    rt.block_on(async {
        let blocking = tokio::task::spawn_blocking(move || {
            std::thread::sleep(blocking_runs_for);
        });

        let th = std::thread::spawn(move || {
            std::thread::sleep(guard_drop_after);
            drop(guard);
        });

        // Completes only after every inhibit (guard and blocking task) is released.
        time::sleep(Duration::from_secs(15)).await;

        // Capture the sleep's completion time before joining the helpers below, so
        // that their own wall-clock completion times cannot mask when the sleep
        // actually fired.
        let elapsed = wall_start.elapsed();

        blocking.await.unwrap();
        th.join().unwrap();

        elapsed
    })
}

/// Guard inhibits and blocking-task inhibits are tracked independently: the blocking
/// task finishing while a guard is still held does not allow auto-advance; the sleep
/// completes only once the guard also drops.
#[cfg(feature = "test-util")]
#[test]
fn auto_advance_guard_independent_of_blocking_inhibit() {
    // The blocking task finishes first (~100ms); the guard (dropped at ~200ms) must
    // keep auto-advance inhibited on its own.
    let elapsed = sleep_elapsed_with_inhibits(
        Duration::from_millis(200), // guard_drop_after
        Duration::from_millis(100), // blocking_runs_for
    );

    assert!(
        elapsed >= Duration::from_millis(200),
        "elapsed: {elapsed:?}"
    );
    assert!(elapsed < Duration::from_secs(10), "elapsed: {elapsed:?}");
}

/// The reverse ordering of `auto_advance_guard_independent_of_blocking_inhibit`:
/// dropping the guard while a blocking task is still running does not allow
/// auto-advance; the sleep completes only once the blocking task also finishes.
#[cfg(feature = "test-util")]
#[test]
fn auto_advance_blocking_inhibit_independent_of_guard() {
    // The guard drops first (~100ms); the still-running blocking task (finishing at
    // ~200ms) must keep auto-advance inhibited on its own.
    let elapsed = sleep_elapsed_with_inhibits(
        Duration::from_millis(100), // guard_drop_after
        Duration::from_millis(200), // blocking_runs_for
    );

    assert!(
        elapsed >= Duration::from_millis(200),
        "elapsed: {elapsed:?}"
    );
    assert!(elapsed < Duration::from_secs(10), "elapsed: {elapsed:?}");
}

/// An `AutoAdvanceGuard` may outlive the runtime it was created on: the guard holds
/// a runtime handle that keeps the driver alive, so dropping the guard after the
/// runtime has been dropped must not panic.
#[cfg(feature = "test-util")]
#[test]
fn auto_advance_guard_outlives_dropped_runtime() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();

    let guard = {
        let _enter = rt.enter();
        time::inhibit_auto_advance()
    };

    drop(rt);
    drop(guard);
}

/// Like `auto_advance_guard_outlives_dropped_runtime`, but the runtime is shut down
/// with `shutdown_background()` instead of dropped.
#[cfg(feature = "test-util")]
#[test]
fn auto_advance_guard_outlives_shutdown_background_runtime() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();

    let guard = {
        let _enter = rt.enter();
        time::inhibit_auto_advance()
    };

    rt.shutdown_background();
    drop(guard);
}

/// `inhibit_auto_advance()` outside any runtime context panics.
#[cfg(feature = "test-util")]
#[test]
#[should_panic(expected = "auto-advance cannot be inhibited from outside the Tokio runtime")]
fn inhibit_auto_advance_outside_runtime_panics() {
    let _guard = time::inhibit_auto_advance();
}

/// `inhibit_auto_advance()` on the multi-thread flavor panics.
#[cfg(feature = "test-util")]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[should_panic(expected = "requires the `current_thread` Tokio runtime")]
async fn inhibit_auto_advance_multi_thread_panics() {
    let _guard = time::inhibit_auto_advance();
}
