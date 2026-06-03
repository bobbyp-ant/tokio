//! Deterministic simulation of a small distributed system on paused Tokio runtimes.
//!
//! The simulated world has three kinds of "islands" — frontends, a single
//! orchestrator (island 0), and backends — each running inside its own paused
//! `current_thread` runtime. An external controller steps every island in
//! lookahead-sized virtual-time windows using `tokio::time::quiesce_until`
//! (a conservative parallel-discrete-event-simulation loop), exchanging cross-island
//! messages between windows over a simulated network with seeded latencies.
//!
//! Because each island's behavior within a window depends only on its inputs and its
//! own virtual clock, the whole simulation reproduces bit-for-bit: the example runs
//! the same seed twice (once with the configured controller thread count, once
//! single-threaded) and asserts both runs produce the identical event-log digest.
//!
//! The elapsed wall-clock times are printed for information only. This controller
//! spawns fresh threads every window, and that dispatch cost dominates at these
//! workload sizes, so the times do not demonstrate the scaling a persistent
//! worker-pool controller achieves (see the `rt_quiesce` benchmark for that); they
//! are not a measure of the cost of `quiesce_until` itself.
//!
//! Run with:
//!
//!     cargo run --release --example sim-islands
//!     cargo run --release --example sim-islands -- [islands] [threads] [seed] [lookahead_us] [duration_ms]
//!
//! Defaults: 8 islands, 4 controller threads, seed 42, 1000us lookahead, 200ms of
//! virtual time.

#![warn(rust_2018_idioms)]

use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::time::{self, Instant};

/// Simulation time: a duration since the simulation epoch. Island-local `Instant`s
/// are never exchanged across islands (each runtime has its own clock origin).
type SimTime = Duration;

// ---------------------------------------------------------------------------
// Deterministic primitives (no external crates: rand's StdRng makes no
// cross-version stability guarantee, which would silently break the digest).
// ---------------------------------------------------------------------------

/// SplitMix64: a tiny, fast, well-distributed PRNG with a stable definition.
/// Used both as a seeded stream and as a hash (one round on a key).
#[derive(Clone, Copy)]
struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        SplitMix64(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// One-shot deterministic hash of a key tuple, for per-message/per-entity values
/// that must not depend on any draw order.
fn mix(seed: u64, parts: &[u64]) -> u64 {
    let mut s = SplitMix64::new(seed);
    let mut acc = s.next_u64();
    for &p in parts {
        let mut s = SplitMix64::new(acc ^ p);
        acc = s.next_u64();
    }
    acc
}

/// FNV-1a 64-bit digest, fed incrementally.
struct Digest(u64);

impl Digest {
    fn new() -> Self {
        Digest(0xCBF2_9CE4_8422_2325)
    }

    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 ^= u64::from(b);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01B3);
        }
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

// ---------------------------------------------------------------------------
// Messages and identity
// ---------------------------------------------------------------------------

/// Identifies an island. Island 0 is the orchestrator, 1..=num_frontends are
/// frontends, the rest are backends.
type IslandId = usize;

/// A cross-island message (the simulated network's payload).
#[derive(Debug, Clone)]
enum Payload {
    /// Frontend -> orchestrator: a new request.
    Request { request_id: u64, from: IslandId },
    /// Orchestrator -> backend: routed request.
    Routed { request_id: u64, from: IslandId },
    /// Backend -> orchestrator: processing finished.
    Reply { request_id: u64, from: IslandId },
    /// Orchestrator -> frontend: response delivered.
    Response { request_id: u64 },
}

/// A message in flight on the simulated network.
#[derive(Debug, Clone)]
struct Envelope {
    src: IslandId,
    dst: IslandId,
    /// Per-source sequence number (for deterministic tiebreaks and latency hashing).
    seq: u64,
    /// Virtual time at which the destination must observe the message.
    delivery_time: SimTime,
    payload: Payload,
}

/// What an island's tasks hand to the controller for cross-island sends:
/// (destination, payload, send time). The controller assigns latency/delivery time.
#[derive(Debug)]
struct OutboundMsg {
    dst: IslandId,
    payload: Payload,
    sent_at: SimTime,
}

/// An island-local event log entry.
type Event = (SimTime, String);

// ---------------------------------------------------------------------------
// The island world model
// ---------------------------------------------------------------------------

/// One partition of the simulated system: a paused current_thread runtime plus the
/// channels the controller uses to exchange messages with it.
struct Island {
    id: IslandId,
    rt: Runtime,
    /// This island's clock origin; SimTime <-> Instant conversions are local to it.
    start: Instant,
    /// Controller -> island: delivery of in-window messages.
    inbox_tx: mpsc::UnboundedSender<Envelope>,
    /// Island -> controller: messages sent by tasks during a window.
    outbox: Arc<Mutex<Vec<OutboundMsg>>>,
    /// The island's append-only event log.
    log: Arc<Mutex<Vec<Event>>>,
    /// Per-source sequence counter for outgoing messages.
    next_seq: u64,
}

/// Builds one island: a paused runtime with its role's task tree already spawned
/// (but not yet polled; the controller's first window step starts it).
fn build_island(
    id: IslandId,
    seed: u64,
    num_frontends: usize,
    num_backends: usize,
    duration: SimTime,
) -> Island {
    let mut builder = tokio::runtime::Builder::new_current_thread();
    builder.enable_time().start_paused(true);
    // Full select!-order determinism needs the runtime RNG seeded; the API is still
    // unstable upstream, and this workload avoids select! entirely, so this is
    // belt-and-suspenders for tokio_unstable builds only.
    #[cfg(tokio_unstable)]
    builder.rng_seed(tokio::runtime::RngSeed::from_bytes(
        &mix(seed, &[id as u64]).to_le_bytes(),
    ));
    let rt = builder.build().expect("island runtime");

    let start = {
        let _enter = rt.enter();
        Instant::now()
    };

    let (inbox_tx, inbox_rx) = mpsc::unbounded_channel::<Envelope>();
    let outbox: Arc<Mutex<Vec<OutboundMsg>>> = Arc::new(Mutex::new(Vec::new()));
    let log: Arc<Mutex<Vec<Event>>> = Arc::new(Mutex::new(Vec::new()));

    // Spawn the island's application task tree.
    {
        let _enter = rt.enter();
        let role_input = inbox_rx;
        let outbox = outbox.clone();
        let log = log.clone();
        if id == 0 {
            rt.spawn(orchestrator(
                role_input,
                outbox,
                log,
                start,
                num_backends,
                num_frontends,
            ));
        } else if id <= num_frontends {
            rt.spawn(frontend(id, role_input, outbox, log, start, seed, duration));
        } else {
            rt.spawn(backend(id, role_input, outbox, log, start, seed));
        }
    }

    Island {
        id,
        rt,
        start,
        inbox_tx,
        outbox,
        log,
        next_seq: 0,
    }
}

/// Records an event in the island's log, stamped with the island's current virtual time.
fn log_event(log: &Mutex<Vec<Event>>, start: Instant, text: String) {
    let now = Instant::now() - start;
    log.lock().unwrap().push((now, text));
}

/// Queues an outbound message, stamped with the island's current virtual time.
fn send_out(outbox: &Mutex<Vec<OutboundMsg>>, start: Instant, dst: IslandId, payload: Payload) {
    let sent_at = Instant::now() - start;
    outbox.lock().unwrap().push(OutboundMsg {
        dst,
        payload,
        sent_at,
    });
}

/// Dispatches delivered envelopes to the role's logic at their delivery times.
///
/// The controller pre-sorts deliveries; sub-tasks woken at the same tick are woken
/// in a deterministic order, so processing order is reproducible across runs.
async fn mailbox(
    mut inbox: mpsc::UnboundedReceiver<Envelope>,
    logic_tx: mpsc::UnboundedSender<Payload>,
    start: Instant,
) {
    while let Some(envelope) = inbox.recv().await {
        let logic_tx = logic_tx.clone();
        tokio::spawn(async move {
            time::sleep_until(start + envelope.delivery_time).await;
            // The receiver hangs up only at simulation teardown.
            let _ = logic_tx.send(envelope.payload);
        });
    }
}

/// A frontend island: generates requests on a deterministic per-frontend period and
/// logs the responses it gets back.
async fn frontend(
    id: IslandId,
    inbox: mpsc::UnboundedReceiver<Envelope>,
    outbox: Arc<Mutex<Vec<OutboundMsg>>>,
    log: Arc<Mutex<Vec<Event>>>,
    start: Instant,
    seed: u64,
    duration: SimTime,
) {
    let (logic_tx, mut logic_rx) = mpsc::unbounded_channel::<Payload>();
    tokio::spawn(mailbox(inbox, logic_tx, start));

    // Response handler, concurrent with the request generator below.
    {
        let log = log.clone();
        tokio::spawn(async move {
            while let Some(payload) = logic_rx.recv().await {
                match payload {
                    Payload::Response { request_id } => {
                        log_event(&log, start, format!("fe{id} resp-received {request_id}"));
                    }
                    other => unreachable!("frontend received unexpected payload: {other:?}"),
                }
            }
        });
    }

    // Request generator. The first request goes out one period after the simulation
    // starts, never at virtual time zero: this initial value is part of the
    // deterministic contract both self-verification runs depend on.
    let period = Duration::from_millis(5 + mix(seed, &[id as u64, 1]) % 10);
    let mut next_send = period;
    let mut n: u64 = 0;
    while next_send <= duration {
        time::sleep_until(start + next_send).await;
        n += 1;
        let request_id = (id as u64) << 32 | n;
        send_out(
            &outbox,
            start,
            0,
            Payload::Request {
                request_id,
                from: id,
            },
        );
        log_event(&log, start, format!("fe{id} req-sent {request_id}"));
        next_send += period;
    }
}

/// The orchestrator island: routes requests to backends round-robin (in request
/// arrival order, which is deterministic) and routes replies back to the requesting
/// frontend.
async fn orchestrator(
    inbox: mpsc::UnboundedReceiver<Envelope>,
    outbox: Arc<Mutex<Vec<OutboundMsg>>>,
    log: Arc<Mutex<Vec<Event>>>,
    start: Instant,
    num_backends: usize,
    num_frontends: usize,
) {
    let (logic_tx, mut logic_rx) = mpsc::unbounded_channel::<Payload>();
    tokio::spawn(mailbox(inbox, logic_tx, start));

    let mut route_counter: usize = 0;
    while let Some(payload) = logic_rx.recv().await {
        match payload {
            Payload::Request { request_id, from } => {
                let backend = num_frontends + 1 + (route_counter % num_backends);
                route_counter += 1;
                send_out(
                    &outbox,
                    start,
                    backend,
                    Payload::Routed { request_id, from },
                );
                log_event(
                    &log,
                    start,
                    format!("orch req-routed {request_id} -> be{backend}"),
                );
            }
            Payload::Reply { request_id, from } => {
                // The requesting frontend is encoded in the request id's upper bits.
                let requester = (request_id >> 32) as usize;
                send_out(&outbox, start, requester, Payload::Response { request_id });
                log_event(
                    &log,
                    start,
                    format!("orch resp-routed {request_id} <- be{from}"),
                );
            }
            other => unreachable!("orchestrator received unexpected payload: {other:?}"),
        }
    }
}

/// A backend island: simulates per-request processing time, then replies to the
/// orchestrator. Requests are processed one at a time, in arrival order.
async fn backend(
    id: IslandId,
    inbox: mpsc::UnboundedReceiver<Envelope>,
    outbox: Arc<Mutex<Vec<OutboundMsg>>>,
    log: Arc<Mutex<Vec<Event>>>,
    start: Instant,
    seed: u64,
) {
    let (logic_tx, mut logic_rx) = mpsc::unbounded_channel::<Payload>();
    tokio::spawn(mailbox(inbox, logic_tx, start));

    while let Some(payload) = logic_rx.recv().await {
        match payload {
            Payload::Routed { request_id, from } => {
                let processing = Duration::from_millis(1 + mix(seed, &[request_id]) % 4);
                time::sleep(processing).await;
                send_out(
                    &outbox,
                    start,
                    0,
                    Payload::Reply {
                        request_id,
                        from: id,
                    },
                );
                log_event(
                    &log,
                    start,
                    format!("be{id} req-processed {request_id} for fe{from}"),
                );
            }
            other => unreachable!("backend received unexpected payload: {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// The conservative-PDES reference controller
// ---------------------------------------------------------------------------

/// Configuration for one simulation run.
#[derive(Clone, Copy)]
struct Config {
    islands: usize,
    threads: usize,
    seed: u64,
    /// The simulated network's minimum latency == the stepping window size.
    lookahead: Duration,
    /// Virtual time horizon after which frontends stop generating requests.
    duration: SimTime,
}

/// Runs one full simulation and returns (event-log digest, number of windows stepped,
/// total events logged).
fn run_simulation(config: Config) -> (u64, usize, usize) {
    let num_frontends = (config.islands - 1) / 2;
    let num_backends = config.islands - 1 - num_frontends;

    // Build all islands.
    let mut islands: Vec<Island> = (0..config.islands)
        .map(|id| {
            build_island(
                id,
                config.seed,
                num_frontends,
                num_backends,
                config.duration,
            )
        })
        .collect();

    // Messages in flight, keyed for deterministic iteration:
    // (delivery_time, src, seq) -> Envelope.
    let mut in_flight: BTreeMap<(SimTime, IslandId, u64), Envelope> = BTreeMap::new();

    let mut window_end: SimTime = Duration::ZERO;
    let mut windows_stepped = 0usize;

    loop {
        // ---- (a) Deliver messages due in this window, pre-sorted by the BTreeMap. ----
        let due: Vec<Envelope> = {
            let keys: Vec<_> = in_flight
                .range(..=(window_end, usize::MAX, u64::MAX))
                .map(|(k, _)| *k)
                .collect();
            keys.iter().map(|k| in_flight.remove(k).unwrap()).collect()
        };
        for envelope in due {
            let dst = envelope.dst;
            islands[dst]
                .inbox_tx
                .send(envelope)
                .expect("island inbox closed");
        }

        // ---- (b) Step all islands in parallel to window_end. ----
        // Workers take islands by index stride; each worker steps its islands in
        // island-index order. Reports are written into a pre-sized slot vector so
        // the result layout is independent of thread scheduling.
        let n_workers = config.threads.min(islands.len()).max(1);
        let mut reports: Vec<Option<time::QuiescedState>> = vec![None; islands.len()];

        {
            // Per-island report slots behind Mutexes: each worker writes only the slots
            // for the islands it owns (strided assignment), so there is never contention
            // on any single slot; the Mutex exists purely to satisfy the shared-borrow
            // rules of scoped threads.
            let report_slots: Vec<Mutex<Option<time::QuiescedState>>> =
                islands.iter().map(|_| Mutex::new(None)).collect();
            let islands_ref: Vec<&Island> = islands.iter().collect();

            std::thread::scope(|scope| {
                let mut handles = Vec::new();
                for worker in 0..n_workers {
                    let islands_ref = &islands_ref;
                    let report_slots = &report_slots;
                    handles.push(scope.spawn(move || {
                        // Worker `worker` steps islands worker, worker + n_workers, ...
                        // sequentially, in island-index order.
                        let mut idx = worker;
                        while idx < islands_ref.len() {
                            let island = islands_ref[idx];
                            let deadline = island.start + window_end;
                            let state = island.rt.block_on(time::quiesce_until(deadline));
                            *report_slots[idx].lock().unwrap() = Some(state);
                            idx += n_workers;
                        }
                    }));
                }
                for h in handles {
                    h.join().expect("controller worker panicked");
                }
            });

            // Copy results out of the slots (the scope has ended; workers are joined).
            for (slot, report) in report_slots.iter().zip(reports.iter_mut()) {
                *report = *slot.lock().unwrap();
            }
        }
        windows_stepped += 1;

        // ---- (c) Drain outboxes in island-index order (deterministic). ----
        let mut any_new_messages = false;
        for island in islands.iter_mut() {
            let outgoing: Vec<OutboundMsg> = island.outbox.lock().unwrap().drain(..).collect();
            for msg in outgoing {
                any_new_messages = true;
                let seq = island.next_seq;
                island.next_seq += 1;
                // Latency derives from the message identity only (never a shared
                // sequential RNG): deterministic regardless of drain or thread order.
                let jitter_ns = mix(config.seed, &[island.id as u64, msg.dst as u64, seq])
                    % config.lookahead.as_nanos() as u64;
                let latency = config.lookahead + Duration::from_nanos(jitter_ns);
                let delivery_time = msg.sent_at + latency;
                let envelope = Envelope {
                    src: island.id,
                    dst: msg.dst,
                    seq,
                    delivery_time,
                    payload: msg.payload,
                };
                in_flight.insert(
                    (envelope.delivery_time, envelope.src, envelope.seq),
                    envelope,
                );
            }
        }

        // ---- (d) Termination and the next window (GVT jump via next_timer). ----
        let earliest_delivery: Option<SimTime> = in_flight.keys().next().map(|k| k.0);
        let earliest_timer: Option<SimTime> = islands
            .iter()
            .zip(reports.iter())
            .filter_map(|(island, report)| {
                report
                    .as_ref()
                    .and_then(|r| r.next_timer)
                    .map(|t| t - island.start)
            })
            .min();

        let gvt: Option<SimTime> = match (earliest_delivery, earliest_timer) {
            (Some(d), Some(t)) => Some(d.min(t)),
            (Some(d), None) => Some(d),
            (None, Some(t)) => Some(t),
            (None, None) => None,
        };

        match gvt {
            // Nothing pending anywhere and no new messages: the world is drained.
            None if !any_new_messages => break,
            _ => {}
        }

        // Conservative window advance with GVT jump. The jump is safe because:
        // (i) no pending delivery and no island timer lies in the skipped span --
        //     `gvt` is by definition the minimum over both -- so skipping ahead
        //     skips no event; and
        // (ii) any message SENT during a window is delivered at >= its send time +
        //      lookahead, i.e. strictly after the window in which it was sent, so
        //      the delivery pass at the top of a later iteration always delivers it
        //      before its destination island is stepped past it. In a jumped
        //      window, (i) also means no task runs before the window end, so every
        //      send in such a window occurs exactly at the window end.
        // Both properties hold regardless of how far the cursor jumps.
        let next_by_lookahead = window_end + config.lookahead;
        window_end = match gvt {
            Some(gvt) if gvt > next_by_lookahead => gvt,
            _ => next_by_lookahead,
        };

        // Safety valve: a draining workload finishes well before this horizon. The
        // post-duration drain tail scales with lookahead: a request's round trip is
        // 4 network hops (frontend -> orch -> backend -> orch -> frontend), each with
        // latency in [lookahead, 2*lookahead), so the last response lands within
        // 8*lookahead of the last request (plus backend processing, plus up to one
        // window of cursor overshoot). 16*lookahead is a 2x margin over that, and the
        // absolute 10s grace covers processing time at tiny lookaheads. Panic rather
        // than break: a silent truncation here would hit both self-verification runs
        // identically and turn a liveness bug into a false "OK".
        if window_end > config.duration + 16 * config.lookahead + Duration::from_secs(10) {
            panic!("simulation failed to drain by {window_end:?}");
        }
    }

    // ---- Compute the digest over all island logs, in island-index order. ----
    let mut digest = Digest::new();
    let mut total_events = 0usize;
    for island in &islands {
        let log = island.log.lock().unwrap();
        for (t, text) in log.iter() {
            digest.write(&(t.as_nanos() as u64).to_le_bytes());
            // Length-prefix the text so the serialization is injective: without it,
            // two different event sequences could feed identical bytes to the digest.
            digest.write(&(text.len() as u64).to_le_bytes());
            digest.write(text.as_bytes());
            total_events += 1;
        }
    }

    (digest.finish(), windows_stepped, total_events)
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let islands: usize = args.get(1).map_or(Ok(8), |s| s.parse())?;
    let threads: usize = args.get(2).map_or(Ok(4), |s| s.parse())?;
    let seed: u64 = args.get(3).map_or(Ok(42), |s| s.parse())?;
    let lookahead_us: u64 = args.get(4).map_or(Ok(1000), |s| s.parse())?;
    let duration_ms: u64 = args.get(5).map_or(Ok(200), |s| s.parse())?;

    assert!(
        islands >= 3,
        "need at least 3 islands (orchestrator + frontend + backend)"
    );
    assert!(threads >= 1, "need at least 1 controller thread");
    assert!(
        lookahead_us >= 1,
        "lookahead must be at least 1 microsecond"
    );

    let config = Config {
        islands,
        threads,
        seed,
        lookahead: Duration::from_micros(lookahead_us),
        duration: Duration::from_millis(duration_ms),
    };

    println!(
        "sim-islands: {islands} islands, {threads} controller threads, seed {seed}, \
         lookahead {lookahead_us}us, {duration_ms}ms virtual time"
    );

    // Run 1: configured thread count.
    let wall = std::time::Instant::now();
    let (digest_a, windows_a, events_a) = run_simulation(config);
    let elapsed_a = wall.elapsed();

    // Run 2: same seed, single controller thread. The digest MUST be identical:
    // determinism is independent of the controller's parallelism.
    let single = Config {
        threads: 1,
        ..config
    };
    let wall = std::time::Instant::now();
    let (digest_b, windows_b, events_b) = run_simulation(single);
    let elapsed_b = wall.elapsed();

    // The elapsed times are informational only; this controller's per-window thread
    // spawning dominates them (see the note in the doc comment at the top).
    println!(
        "run A ({} threads): digest {digest_a:016x}, {windows_a} windows, {events_a} events, {elapsed_a:?}",
        config.threads
    );
    println!(
        "run B (1 thread):   digest {digest_b:016x}, {windows_b} windows, {events_b} events, {elapsed_b:?}"
    );

    assert_eq!(
        digest_a, digest_b,
        "DETERMINISM VIOLATION: digests differ between thread counts"
    );
    assert_eq!(windows_a, windows_b);
    assert_eq!(events_a, events_b);

    println!("OK: bit-identical results across controller thread counts");
    Ok(())
}
