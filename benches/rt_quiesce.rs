//! Benchmarks for externally-driven deterministic stepping of paused runtimes
//! (`tokio::time::quiesce_until`).
//!
//! Measures per-window stepping overhead and multi-core scaling of a balanced
//! synthetic island world: N paused current_thread runtimes, each with a 1 ms
//! periodic timer, a fixed CPU budget per tick, and a ring message to its neighbor,
//! stepped in lookahead-sized windows by a pool of controller threads.
//!
//! Requires the `test-util` feature:
//!
//!     cargo bench --features test-util --bench rt_quiesce
//!
//! Methodology notes:
//! - All configurations run with paused clocks, i.e. on the post-pause
//!   `Instant::now()` slow path (process-wide, one-way). This is representative of
//!   real simulation builds; configurations are mutually comparable.
//! - Island timers are 1 ms periodic regardless of the stepping lookahead, so
//!   sub-millisecond lookahead configurations measure stepping overhead (mostly
//!   empty windows), not extra timer work.
//! - The per-tick CPU budget is a fixed iteration count; its wall-clock cost is
//!   measured and printed at startup so results can be read as "this much useful
//!   work per island per 1 ms window".

#[cfg(feature = "test-util")]
mod bench {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use criterion::{black_box, criterion_group, BenchmarkId, Criterion};
    use tokio::runtime::Runtime;
    use tokio::sync::mpsc;
    use tokio::time::{self, Instant};

    /// Fixed CPU budget per island per 1 ms virtual tick: SPIN_ITERS rounds of SplitMix64.
    /// The wall-clock cost is measured and printed once at startup (see `measure_spin_cost`).
    const SPIN_ITERS: u64 = 10_000;

    /// Virtual time horizon simulated per benchmark iteration.
    const VIRTUAL_HORIZON: Duration = Duration::from_millis(20);

    /// Island timers are 1 ms periodic regardless of the stepping lookahead.
    const TICK: Duration = Duration::from_millis(1);

    fn spin(seed: u64) -> u64 {
        // SplitMix64 rounds; black_box prevents the loop from being optimized away.
        let mut x = seed;
        for _ in 0..SPIN_ITERS {
            x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = x;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            x = black_box(z ^ (z >> 31));
        }
        x
    }

    /// Measures and returns the wall-clock cost of one spin() call (printed at startup).
    fn measure_spin_cost() -> Duration {
        let start = std::time::Instant::now();
        let mut acc = 0u64;
        for i in 0..1000u64 {
            acc = acc.wrapping_add(spin(i));
        }
        black_box(acc);
        start.elapsed() / 1000
    }

    /// One island: a paused runtime with a periodic-tick task.
    struct Island {
        rt: Runtime,
        start: Instant,
        /// Controller -> island: ring messages due in the upcoming window.
        inbox_tx: mpsc::UnboundedSender<Duration>, // payload = delivery time (SimTime)
        /// Island -> controller: ring messages sent this window (their send SimTimes).
        outbox: Arc<Mutex<Vec<Duration>>>,
        /// Count of ring messages received (proves the ring is live; read at teardown).
        received: Arc<AtomicU64>,
    }

    /// Builds one island. `work` controls whether the island has the spin budget
    /// (false for the pure-overhead benchmark).
    fn build_island(id: usize, work: bool, horizon: Duration) -> Island {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();

        let start = {
            let _enter = rt.enter();
            Instant::now()
        };

        let (inbox_tx, mut inbox_rx) = mpsc::unbounded_channel::<Duration>();
        let outbox: Arc<Mutex<Vec<Duration>>> = Arc::new(Mutex::new(Vec::new()));
        let received = Arc::new(AtomicU64::new(0));

        {
            let _enter = rt.enter();

            // Mailbox: receive ring messages, schedule their observation at delivery time.
            let received_clone = received.clone();
            rt.spawn(async move {
                while let Some(delivery_time) = inbox_rx.recv().await {
                    let received = received_clone.clone();
                    tokio::spawn(async move {
                        time::sleep_until(start + delivery_time).await;
                        received.fetch_add(1, Relaxed);
                    });
                }
            });

            // The periodic worker: tick every 1 ms, spin, send a ring message.
            let outbox_clone = outbox.clone();
            rt.spawn(async move {
                let mut tick = TICK;
                while tick <= horizon {
                    time::sleep_until(start + tick).await;
                    if work {
                        black_box(spin(id as u64 ^ tick.as_millis() as u64));
                    }
                    outbox_clone.lock().unwrap().push(tick);
                    tick += TICK;
                }
            });
        }

        Island {
            rt,
            start,
            inbox_tx,
            outbox,
            received,
        }
    }

    /// Runs the windowed simulation across `threads` controller threads. Returns the
    /// number of windows stepped. ONLY this function is inside the timed section.
    fn run_world(
        islands: &[Island],
        threads: usize,
        lookahead: Duration,
        horizon: Duration,
    ) -> usize {
        let n_workers = threads.min(islands.len()).max(1);
        let mut window_end = Duration::ZERO;
        let mut windows = 0usize;
        // Ring messages in flight: (delivery_time, dst) pairs, kept sorted by insertion
        // discipline (delivery times are monotone per source).
        let mut in_flight: Vec<(Duration, usize)> = Vec::new();

        // The simulation must run a fixed amount past the horizon so messages sent in the
        // last tick still get delivered and observed (two lookaheads of slack: one window
        // for the message to come due, one more to step its destination past it).
        let end = horizon + lookahead + lookahead;

        while window_end < end {
            window_end += lookahead;
            windows += 1;

            // (a) Deliver due ring messages.
            in_flight.retain(|&(delivery_time, dst)| {
                if delivery_time <= window_end {
                    // Ignore send errors at teardown (island tasks may have finished).
                    let _ = islands[dst].inbox_tx.send(delivery_time);
                    false
                } else {
                    true
                }
            });

            // (b) Step all islands in parallel (strided assignment).
            std::thread::scope(|scope| {
                let mut handles = Vec::new();
                for worker in 0..n_workers {
                    let islands_ref = &islands;
                    handles.push(scope.spawn(move || {
                        let mut idx = worker;
                        while idx < islands_ref.len() {
                            let island = &islands_ref[idx];
                            let deadline = island.start + window_end;
                            let _state = island.rt.block_on(time::quiesce_until(deadline));
                            idx += n_workers;
                        }
                    }));
                }
                for h in handles {
                    h.join().expect("controller worker panicked");
                }
            });

            // (c) Collect outboxes (island-index order) and queue ring messages with
            //     latency exactly == lookahead.
            for (i, island) in islands.iter().enumerate() {
                let sent: Vec<Duration> = island.outbox.lock().unwrap().drain(..).collect();
                let dst = (i + 1) % islands.len();
                for sent_at in sent {
                    in_flight.push((sent_at + lookahead, dst));
                }
            }
        }

        windows
    }

    /// Controller thread counts swept by the benchmarks, clamped to the machine.
    fn parallelism_levels() -> Vec<usize> {
        let max_parallelism = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);
        [1, 8, 32, 64]
            .into_iter()
            .filter(|&n| n <= max_parallelism)
            .collect()
    }

    /// Total ring messages received across all islands. Read at teardown to verify
    /// the ring was live (or, for the overhead benchmark, that it stayed silent).
    fn total_received(islands: &[Island]) -> u64 {
        islands
            .iter()
            .map(|island| island.received.load(Relaxed))
            .sum()
    }

    fn bench_scaling(c: &mut Criterion) {
        let spin_cost = measure_spin_cost();
        println!(
            "rt_quiesce: spin budget = {SPIN_ITERS} iters = {spin_cost:?} per island per 1ms tick"
        );

        let mut group = c.benchmark_group("rt_quiesce_scaling");
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(15));
        group.warm_up_time(Duration::from_secs(3));

        for &islands in &[8usize, 32, 128] {
            for &lookahead_us in &[1000u64, 250, 100] {
                let lookahead = Duration::from_micros(lookahead_us);
                for threads in parallelism_levels() {
                    group.bench_with_input(
                        BenchmarkId::new(format!("islands{islands}_la{lookahead_us}us"), threads),
                        &threads,
                        |b, &threads| {
                            b.iter_custom(|iters| {
                                let mut total = Duration::ZERO;
                                for _ in 0..iters {
                                    // World construction: NOT timed.
                                    let world: Vec<Island> = (0..islands)
                                        .map(|id| build_island(id, true, VIRTUAL_HORIZON))
                                        .collect();

                                    // The simulation: timed.
                                    let t0 = std::time::Instant::now();
                                    let windows =
                                        run_world(&world, threads, lookahead, VIRTUAL_HORIZON);
                                    total += t0.elapsed();

                                    // Teardown sanity (not timed): the ring was live.
                                    black_box(windows);
                                    assert!(
                                        total_received(&world) > 0,
                                        "no ring messages were delivered"
                                    );
                                    drop(world);
                                }
                                total
                            });
                        },
                    );
                }
            }
        }
        group.finish();
    }

    fn bench_lookahead_sensitivity(c: &mut Criterion) {
        // Same measurement as bench_scaling but restricted to islands=32 and threads in
        // {1, 8, 32}; exists as a separate, smaller group so the lookahead sensitivity
        // curve can be re-run quickly without the full matrix.
        let mut group = c.benchmark_group("rt_quiesce_lookahead");
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(15));
        group.warm_up_time(Duration::from_secs(3));

        let islands = 32usize;
        let threads_levels: Vec<usize> = parallelism_levels()
            .into_iter()
            .filter(|&t| t == 1 || t == 8 || t == 32)
            .collect();

        for &lookahead_us in &[1000u64, 250, 100] {
            let lookahead = Duration::from_micros(lookahead_us);
            for &threads in &threads_levels {
                group.bench_with_input(
                    BenchmarkId::new(format!("la{lookahead_us}us"), threads),
                    &threads,
                    |b, &threads| {
                        b.iter_custom(|iters| {
                            let mut total = Duration::ZERO;
                            for _ in 0..iters {
                                // World construction: NOT timed.
                                let world: Vec<Island> = (0..islands)
                                    .map(|id| build_island(id, true, VIRTUAL_HORIZON))
                                    .collect();

                                // The simulation: timed.
                                let t0 = std::time::Instant::now();
                                let windows =
                                    run_world(&world, threads, lookahead, VIRTUAL_HORIZON);
                                total += t0.elapsed();

                                // Teardown sanity (not timed): the ring was live.
                                black_box(windows);
                                assert!(
                                    total_received(&world) > 0,
                                    "no ring messages were delivered"
                                );
                                drop(world);
                            }
                            total
                        });
                    },
                );
            }
        }
        group.finish();
    }

    fn bench_per_window_overhead(c: &mut Criterion) {
        let mut group = c.benchmark_group("rt_quiesce_overhead");
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(10));

        for &islands in &[8usize, 32] {
            group.bench_with_input(
                BenchmarkId::new("empty_windows", islands),
                &islands,
                |b, &islands| {
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            // World construction: NOT timed. work = false and
                            // horizon = ZERO mean no spin budget and no timers at all,
                            // so every window is empty: stepping a fixed 100 windows
                            // over this world measures pure per-window overhead.
                            let world: Vec<Island> = (0..islands)
                                .map(|id| build_island(id, false, Duration::ZERO))
                                .collect();

                            // The simulation: timed.
                            let t0 = std::time::Instant::now();
                            let windows = run_world(
                                &world,
                                1,
                                Duration::from_millis(1),
                                Duration::from_millis(100),
                            );
                            total += t0.elapsed();

                            // Teardown sanity (not timed): pure overhead means the
                            // ring stayed silent.
                            black_box(windows);
                            assert_eq!(total_received(&world), 0);
                            drop(world);
                        }
                        total
                    });
                },
            );
        }
        group.finish();
    }

    criterion_group!(
        rt_quiesce,
        bench_scaling,
        bench_lookahead_sensitivity,
        bench_per_window_overhead
    );
}

#[cfg(feature = "test-util")]
criterion::criterion_main!(bench::rt_quiesce);

#[cfg(not(feature = "test-util"))]
fn main() {
    eprintln!("rt_quiesce benchmark requires the `test-util` feature:");
    eprintln!("    cargo bench --features test-util --bench rt_quiesce");
}
