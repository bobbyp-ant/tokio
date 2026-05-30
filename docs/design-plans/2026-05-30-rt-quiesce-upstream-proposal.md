# Upstream Proposal: deterministic stepping control surface for paused runtimes

Status: DRAFT — not yet filed. Filing is a separate decision.
Target: tokio-rs/tokio issue tracker, then a PR series.

## Part 1: The proposal issue (ready to copy into a new GitHub issue)

---

Title: time: control surface for externally-driven deterministic stepping of paused runtimes

### Problem

Deterministic simulation testing of a distributed system: run each simulated process — a
set of frontends, an orchestrator, a pool of backends — inside its own paused
`current_thread` runtime (an "island"), and have an external controller step every island
through bounded windows of virtual time, exchanging cross-island messages between windows.
This is conservative parallel discrete-event simulation. Within a window, an island's
behavior depends only on its inputs and its own virtual clock, so a whole-system test
reproduces bit-for-bit from a seed; islands can be stepped by different controller threads,
so one simulated world scales across cores. In a benchmark of this pattern, stepping 32
islands from 32 controller threads completes the same simulated horizon about 17x faster
than stepping them from one thread.

Tokio's paused clock and auto-advance do most of the work already, but three things are
missing:

1. There is no way to run a paused runtime "until it has nothing left to do at or before
   virtual time T, then stop". The closest approximation, `block_on(sleep_until(T))`,
   perturbs the timer state being observed (the boundary sleep is itself a pending timer),
   advances the clock to T even when nothing real happens at T, and proves only that the
   boundary was reached — not that work woken at or before it has drained.
2. There is no way to observe "when is this runtime's next pending timer due?". A
   controller needs this to skip empty windows and to compute the simulation's global lower
   bound on the next event (GVT — global virtual time — in simulation terms).
3. There is no public way to hold auto-advance off while something outside the runtime
   catches up. This is the ask in #4522, open since 2022.

The mechanisms behind all three already exist inside the runtime: the paused clock,
auto-advance to the next timer deadline, and the internal counter that inhibits
auto-advance while `spawn_blocking` tasks are outstanding (added by #5115). What is missing
is a small, `test-util`-gated control surface over them.

### Proposed API

All of it behind the existing `test-util` feature, in `tokio::time`:

```rust
pub fn quiesce() -> Quiesce;                         // unbounded
pub fn quiesce_until(deadline: Instant) -> Quiesce;  // bounded (inclusive)

pub struct Quiesce { /* named future, like Sleep */ }
impl Future for Quiesce { type Output = QuiescedState; }

#[non_exhaustive]
pub struct QuiescedState {
    pub now: Instant,
    pub next_timer: Option<Instant>,
}

pub fn inhibit_auto_advance() -> AutoAdvanceGuard;   // counted RAII guard
pub struct AutoAdvanceGuard { /* releases on drop */ }
```

**Resolution rule.** A `Quiesce` future resolves when, at the moment of resolution, nothing
is runnable on the runtime, no `spawn_blocking` task spawned on this runtime is
outstanding, and no timer with a deadline at or before the bound remains unfired
(`quiesce()` is the unbounded case: it resolves only once no timers remain at all). The
call itself never advances the clock. During a step the clock moves only to positions
auto-advance already visits — timer deadlines, and occupied-slot boundaries for far-out
timers — and a step in which nothing fires and whose bound reaches no such slot boundary
leaves the clock untouched (see the precision contract below). Stepping is driven either
by passing the future to `Runtime::block_on` from the controlling thread, or by awaiting
it inside a task on the runtime.

**Precision contract.** `QuiescedState::now` is where the clock stopped; it is never past
the bound. `QuiescedState::next_timer` is a lower bound on the earliest pending timer
deadline strictly after `now`, and is `None` exactly when no timers remain. The bound is
exact when that deadline lies in the timer wheel's bottom level (the same 64 ms window as
`now`); for deadlines stored in the wheel's coarser upper levels it is the start of the
occupied slot, which is at or before the actual deadline. The upper wheel levels do not
store exact deadlines, so an exact `next_timer` for far-out timers would require a
different data structure; a lower bound is the honest contract, and stepping refines it —
each `quiesce_until` step that reaches the timer's occupied slot either fires the timer
or moves the clock closer to it.

**Misuse panics.** The future panics when polled outside a runtime context, on the
multi-thread flavor, on an unpaused clock, on a runtime built without a time driver, or on
a runtime that has shut down. While a step is in progress, `resume()` and `advance()`
panic: an explicit clock change would move the clock out from under the step.
`inhibit_auto_advance()` panics outside a runtime context and on the multi-thread flavor,
like `pause()`.

### Prior art in this repository

#### #4522: the guard half of this proposal, open since 2022

Issue #4522 ("Ability to (temporarily) disable time auto-advance when paused") is the
longest-standing ask for the guard half of this proposal, filed in February 2022 and still
open. The reported problem:

> The timer wheel auto-advancing while in pause is obviously helpful when tasks blocked on timers however when doing a bit of I/O as well things get quite unpredictable.

and the request:

> The ability to pause time while turning auto-advance on and off again so it's easier for the author to ensure there are no auto-advances for at least a given scope.

The maintainer position on record is one sentence, from Darksonn in July 2022:

> If someone wants to write a PR, then I am ok with adding this.

That is the entire statement: one sentence expressing openness to a PR, from 2022 — not a
reviewed design, and not a commitment. Three PRs have attempted the feature since, and none
merged: #4523 (2022) was closed as stale, #5200 (2022) was closed as a duplicate, and #6113
(2023) is still open but has been stalled since October 2024 on review questions. This
proposal includes the feature (the `inhibit_auto_advance()` guard) as its first PR, and
answers those review questions directly below.

#### Why not just disable time mocking entirely?

PR #6113 — the still-open attempt at #4522 — stalled on review questions from carllerche
that any version of this feature has to answer, so here they are, head-on. First:

> I read the discussion around the original issue, and I'm not sure I fully understand the motivation for this option. Could you explain the use case a bit more? What is the problem caused by auto-advancing mocked time, and why not just disable time mocking entirely?

and, of the proposed builder option:

> ...does this function need to exist (I couldn't say because I don't fully understand the use case yet).

The answer, for the simulation use case: it needs virtual time. The simulation exists to
test timer-driven behavior — timeouts, heartbeats, leader election, retry backoff — at
virtual timestamps, reproducibly, and faster than wall clock. Disabling time mocking gives
real time, which is both non-deterministic (every run takes a different interleaving) and
slow (a test that simulates ten minutes of timeouts takes ten minutes). What the use case
needs is virtual time that moves only under external control: auto-advance bounded by a
window (the quiesce half of this proposal), and auto-advance held off entirely while
something outside the runtime catches up (the guard half — #4522's original request).

carllerche also asked, of #6113's builder-option shape:

> Should this panic if using the multi-thread runtime flavor?

This proposal's answer is yes: `inhibit_auto_advance()` panics on the multi-thread flavor,
exactly as `pause()` does today.

#### #5115: the merged counter that anticipated this API

The only related change that ever merged is #5115 (December 2022), which fixed the
`spawn_blocking` auto-advance bug (#4614) by adding an internal counter that the blocking
pool holds while blocking tasks are outstanding. Its author chose the name with a future
public API in mind — quoting the PR body:

> I named the counter `auto_advance_inhibit_count`, instead of something obvious like `num_blocking_tasks`, because it might make sense in the future to provide an API whereby users can manually inhibit auto-advance.

`inhibit_auto_advance()` is precisely that API, built on the counter #5115 added.

#### #8091 and #4879: deterministic-simulation users exist upstream

carllerche's own #4879 (2022) asked for an API for controlling the runtime's
non-determinism, and #8091 — open, approved, and CI-green at the time of writing —
stabilizes `Builder::rng_seed` to close it. The #8091 author's stated motivation is
independent corroboration of the use case behind this proposal:

> this is for doing deterministic simulation testing, but where I can't use `tokio_unstable` in my codebase and also want more control over the determinism facade than `turmoil`.

`rng_seed` and this proposal compose but are independent: a seeded RNG makes the runtime's
own random choices (such as `select!` branch ordering) reproducible, while quiescence
stepping makes virtual-time scheduling observable and externally driven.

#### #7463: timer precision and firing semantics are unchanged

#7463 ("Stable advancement of timers", closed 2025) asked for changes to when timers fire,
and was declined:

> Tokio timers are intentionally not precise, and delayed polling is by design.

(ADD-SP), and:

> This feature already exists, and it's called auto-advance.

(Darksonn). To be explicit: this proposal does not change timer precision or firing
semantics in any way. Timers fire exactly when they fire today, and auto-advance moves the
clock exactly as it does today. Quiescence stepping only bounds how far auto-advance may go
in one step and reports where it stopped.

#### #1845: simulation infrastructure stays outside Tokio

#1845 ("Consider exposing simulation's types in Tokio") was closed as not-planned. This
proposal does not ask Tokio to take simulation infrastructure in. The simulation harness —
message exchange between islands, windowing, lookahead, GVT computation — stays outside
Tokio, in user code. Tokio gains only a minimal observation and stepping control surface
over its existing test-util clock, in the same spirit as `pause()` and `advance()`.

### Why not existing tools

#### Why not turmoil?

turmoil is tokio-rs's own deterministic-simulation framework. It is actively maintained
(v0.7.2 released in April 2026, recently reorganized into a turmoil / turmoil-net /
turmoil-fs / turmoil-io-uring workspace) and good at what it does. But it is built around
choices that rule out the workload described above:

- It runs the entire simulated world on one OS thread. From its README:

  > It runs multiple concurrent hosts within a single thread

  `Sim::step()` iterates hosts sequentially on the calling thread, so one simulated world
  cannot use more than one core — and using many cores is the point of stepping islands in
  parallel from a controller thread pool.
- The system under test must be written against `turmoil::net` (and the newer shim crates)
  rather than `tokio::net`.
- Its stepping is fixed-time-quantum: `Sim::step()` advances every host by a fixed
  `tick_duration` using auto-advance. There is no "run until idle" primitive, no way to
  know a host is idle, and no way to see when its next event is due — so empty ticks
  cannot be skipped and per-tick auto-advance cannot be held off.
- Its own runtime-RNG seeding is gated on `tokio_unstable`.

This proposal is the primitive a turmoil-shaped tool could build on, not a competitor:
quiescence stepping plus next-timer observation would let a turmoil-style harness skip
empty ticks and parallelize hosts across threads.

#### Why not loom?

loom is a model checker for synchronization primitives: it exhaustively explores the thread
interleavings of a small, bounded model. It is not a runtime for application-scale
workloads — a distributed-system simulation with thousands of timers and messages is far
beyond any model checker's state-space budget. The two are complementary; the
implementation of this proposal uses loom to verify its own synchronization.

#### Why not block_on(sleep_until(window_end)) plus a next-deadline query?

This is the obvious smaller alternative: step a window by blocking on a sleep at the window
boundary, and add only an API for querying the next timer deadline. It falls short on three
counts:

1. The boundary sleep perturbs the very state being reported: it is itself a pending timer,
   so it becomes the wheel's next expiration and shadows the real one.
2. Auto-advance then moves the clock to the window end even when nothing real happens
   there. The simulation loses the "time only moves to event times" property that the GVT
   computation needs: the controller can no longer distinguish "the island did something at
   T" from "the island was dragged to T".
3. The sleep firing proves only that the boundary was reached. It does not prove that work
   woken at or before the boundary has drained — which is exactly what a stepping protocol
   needs to know before it exchanges messages for the next window.

### Non-goals

- **No cross-version determinism guarantee.** Scheduling order may change between Tokio
  releases; determinism holds for a fixed Tokio version.
- **No multi-thread-flavor virtual time.** The proposed signatures are flavor-agnostic, so
  a future implementation would need no API change, but this proposal covers only the
  `current_thread` flavor (and `LocalRuntime`, which uses the same scheduler).
- **No scheduler-policy determinism** beyond what `rng_seed` (#8091) provides.
- **No determinism for `spawn_blocking` work.** It runs on real OS threads; quiescence
  waits for it so its effects are not lost, but how long it takes is real-world timing. The
  existing blocking-task inhibit semantics are preserved for compatibility, not
  reproducibility.
- **No IO-driver simulation.** Deterministic stepping assumes the IO driver is unused or
  externally quiet during a step.

### Naming

"Quiesce" is one vocabulary; "idle" is the other natural one — `run_until_idle()` returning
an `IdleState`, say. The semantics would be identical and I have no strong preference.
One semantic detail worth pinning down regardless of the name: the bound is inclusive (a
timer at exactly the deadline fires within the step), called out explicitly in the docs so
windowing protocols can build their own half-open conventions on top.

### Implementation sketch

The resolution decision lives at the `current_thread` scheduler's drain-park — the park
taken from `Context::park` once the run queue has fully drained — not in the time driver's
timeout path (`Driver::park_thread_timeout`). Three reasons: the park on an empty timer
wheel never reaches the time driver's timeout path at all; the yield-park
(`Context::park_yield`) does reach it, but with work still queued, so being inside that
path does not imply quiescence; and IO-readiness wakeups do not set the time driver's
`did_wake` flag, so the time driver alone cannot distinguish a real wake from an empty
park.

Quiesce waiters register their waker and bound with the time driver under the existing
wheel mutex, with an atomic waiter count in front of it: on any runtime that is not being
stepped, the cost on the park path is one relaxed atomic load. All of it sits behind the
existing `cfg_test_util!` gate; builds without `test-util` compile exactly what they
compile today.

The clock's auto-advance inhibit counter (from #5115) splits into two counts — one for
outstanding blocking tasks, one for user guards — so that a held `AutoAdvanceGuard` blocks
auto-advance but never blocks quiescence resolution itself. Everything else is unchanged:
the drain-before-park property, advance-to-next-deadline, the blocking-task inhibit, and
the `did_wake` veto on auto-advance all keep their current behavior.

I have a working implementation of all of the above — documentation, tests, loom models for
the cross-thread races, and a scaling benchmark — ready to submit as a reviewable PR series
if maintainers are open to the direction.

---

## Part 2: PR series

The work splits into independently reviewable PRs. PR1 has standalone value and is the
smallest possible first step; PR2 is the core feature; PR3 is optional supporting material.
The example that accompanies the implementation (a deterministic multi-island simulation)
stays in the proposing repository unless maintainers ask for it — precedent: the
alternative timer (#7467) and `LocalRuntime` (#6808) landed tests-only, with no `examples/`
entry.

Each description below uses the two headings from Tokio's PR template, at the heading level
the template uses, so it can be copied into a PR body as-is.

### PR1: time: add inhibit_auto_advance() and AutoAdvanceGuard

Closes #4522. Overlaps with #6113, which proposes the same capability as a runtime-builder
option; coordinate with its author before filing (see "Filing order" below).

## Motivation

When the clock is paused, auto-advance fires the next pending timer as soon as the runtime
has nothing else to do. That is exactly right for pure-timer tests, and exactly wrong when
a test needs virtual time to hold still while something outside the runtime — real I/O,
another thread, an external process — catches up. This is #4522, open since 2022: the
reporter asked for a way to ensure "there are no auto-advances for at least a given scope".
The runtime already inhibits auto-advance internally while `spawn_blocking` tasks are
outstanding; #5115 added that counter and named it `auto_advance_inhibit_count` precisely
"because it might make sense in the future to provide an API whereby users can manually
inhibit auto-advance". This PR is that API.

Two shape decisions, both responsive to the review questions raised on #6113:

- A counted RAII guard rather than a boolean toggle: counted guards compose. Two
  independent test helpers can each hold one without coordinating; with a boolean toggle,
  the inner helper's "re-enable" stomps the outer helper's hold.
- A free function returning a guard rather than a runtime-builder option: the need is
  scoped and dynamic — #4522 asks for "at least a given scope" — not a static property of
  the whole runtime.

The reviewer on #6113 also asked whether the feature should panic on the multi-thread
runtime flavor. Yes: `inhibit_auto_advance()` panics on the multi-thread flavor, the same
way `pause()` does.

## Solution

The clock's internal inhibit state becomes two counters: a blocking-task count (held by
outstanding `spawn_blocking` tasks, exactly as today) and a user-guard count (held by live
`AutoAdvanceGuard`s). `Clock::can_auto_advance` requires both to be zero, so existing
behavior is preserved; `BlockingSchedule` is re-pointed at the blocking-task variant (a
rename — no behavior change). The split keeps "a user is holding time still" and "blocking
work is outstanding" distinguishable, which the follow-up quiesce PR relies on.

`inhibit_auto_advance()` resolves the current runtime handle (panicking outside a runtime
context or on the multi-thread flavor), increments the user-guard count, and returns an
`AutoAdvanceGuard` that owns that handle. Dropping the guard decrements the count and
unparks the originating runtime's driver, so a runtime parked waiting for auto-advance
notices the release promptly. The guard always affects the runtime it was created on,
regardless of which runtime context (if any) is current at drop time.

Tests: ten new tests in `tokio/tests/time_pause.rs` covering inhibit plus prompt unpark on
drop, guard counting, cross-runtime drop targeting, explicit `advance()` while a guard is
held, independence of the guard and blocking-task counts, guards outliving a dropped or
shut-down runtime, and the misuse panics; plus one loom model
(`auto_advance_guard_drop_vs_park`) for a guard dropped on another thread racing the
runtime's park.

Files:

- `tokio/src/time/clock.rs`
- `tokio/src/runtime/blocking/schedule.rs`
- `tokio/src/time/mod.rs`
- `tokio/tests/time_pause.rs`
- `tokio/src/runtime/tests/loom_current_thread.rs`
- `spellcheck.dic`

### PR2: time: add quiesce() and quiesce_until() for paused runtimes

Depends on PR1 (the counter split). Closes the proposal issue.

## Motivation

With the clock paused there is no way to run a `current_thread` runtime "until it has
nothing left to do at or before virtual time T, then stop and report". That primitive is
what an external controller needs in order to step many paused runtimes — each simulating
one process of a distributed system — through bounded virtual-time windows, exchanging
messages between windows: conservative parallel discrete-event simulation, reproducible
bit-for-bit and parallel across cores. The proposal issue covers the use case, prior art
(#4522, #5115, #8091), and alternatives in detail.

The closest existing approximation, `block_on(sleep_until(window_end))`, fails in three
ways: the boundary sleep is itself a pending timer, so it perturbs the wheel state a
controller needs to observe; auto-advance drags the clock to the window end even when
nothing real happens there, destroying the "time only moves to event times" property; and
the sleep firing proves only that the boundary was reached, not that work woken at or
before it has drained. A correct stepping primitive has to live inside the runtime, where
"nothing left to do" is observable.

## Solution

`tokio::time::quiesce()` / `quiesce_until(deadline)` return a `Quiesce` future that
resolves with a `QuiescedState { now, next_timer }` report once nothing is runnable, no
`spawn_blocking` task spawned on this runtime is outstanding, and no timer at or before the
bound remains unfired. The pieces:

- The `Quiesce` future validates and registers on first poll, not at construction, so
  `rt.block_on(time::quiesce_until(w))` works — the future is created before `block_on`
  establishes the runtime context. Misuse (no runtime context, wrong flavor, unpaused
  clock, no time driver, shut-down runtime) panics at that first poll.
- Waiters live in a registry in the time driver, guarded by the existing wheel mutex, with
  an atomic waiter count in front of it (`Handle::register_quiesce_waiter`,
  `Handle::poll_quiesce_waiter`, `Handle::deregister_quiesce_waiter`,
  `Handle::resolve_quiesce_waiters`). When no waiter is registered, the park path pays one
  relaxed atomic load.
- The resolution decision runs in a drain-park hook in the `current_thread` scheduler
  (`Context::park` -> `Context::park_drain` -> `Context::quiesce_park_hook`), inside a
  single scheduler `enter` scope: a zero-timeout driver poll (fires already-due timers,
  surfaces IO readiness, lands raced cross-thread wakes), then a runnable-work re-check
  (run queue, deferred wakers, root-future woken flag, inject queue, blocking-task count),
  then either skip the park (work appeared, or a waiter resolved) or fall through to the
  normal park. The clock is never advanced on a resolving cycle.
- While any waiter is registered, `resume()` and `advance()` panic: an explicit clock
  change would move the clock out from under the step.
- `QuiescedState::now` and `next_timer` carry a documented precision contract: exact when
  the next deadline lies in the wheel's bottom level, a slot-aligned lower bound for
  deadlines in the coarser upper levels.

Tests: 36 tests in `tokio/tests/time_quiesce.rs` (core stepping semantics, bound
inclusivity, guard / blocking / IO interactions, `LocalRuntime` and `LocalSet`, misuse
panics, shutdown handling, and run-twice determinism of a windowed stepping loop), and six
loom models: `quiesce_vs_cross_thread_schedule`, `quiesce_guard_drop_vs_park`,
`quiesce_both_waiter_shapes`, `quiesce_poll_vs_shutdown`, and
`quiesce_first_poll_vs_shutdown` in the current-thread suite, plus
`quiesce_vs_blocking_release` in the blocking suite.

Files:

- `tokio/src/time/quiesce.rs` (new)
- `tokio/src/time/clock.rs`
- `tokio/src/time/mod.rs`
- `tokio/src/runtime/time/mod.rs`
- `tokio/src/runtime/time/handle.rs`
- `tokio/src/runtime/time/source.rs`
- `tokio/src/runtime/scheduler/current_thread/mod.rs`
- `tokio/tests/time_quiesce.rs`
- `tokio/src/runtime/tests/loom_current_thread.rs`
- `tokio/src/runtime/tests/loom_blocking.rs`
- `spellcheck.dic`

### PR3 (optional): benches: add rt_quiesce stepping benchmark

## Motivation

The quiesce feature adds a hook to the `current_thread` scheduler's park path. This
benchmark quantifies the per-window stepping overhead and the multi-core scaling of
externally stepped paused runtimes, so future changes to the park path or the time driver
can be checked for regressions. It is gated behind the benches crate's existing `test-util`
feature, so `cargo check --benches` stays green when the feature is off.

## Solution

A balanced synthetic world: N paused `current_thread` runtimes ("islands"), each with a
1 ms periodic timer, a fixed per-tick CPU budget (calibrated and printed at startup, about
35 µs per island per tick), and a ring message to its neighbor each tick; a persistent pool
of controller worker threads steps all islands in lookahead-sized windows. Three criterion
groups measure scaling across island count, controller thread count, and lookahead;
lookahead sensitivity; and per-window overhead on an empty world.

Headline numbers from a machine with 32 physical cores (64 logical CPUs): at 1 ms lookahead
and 32 controller threads, stepping completes 17.42x faster than single-threaded for 32
islands, and 19.74x faster for 128 islands. Per-window overhead on an empty world is about
1.7 µs fixed plus about 0.57 µs per island.

One caveat the benchmark is explicit about: the stepping API itself is cheap — about 0.6 µs
per island per window — and is not what limits scaling; controller design is. An earlier
version of this benchmark that spawned fresh controller threads every window measured only
1.16x at 32 threads on the same workload, because per-window thread spawning dominated the
useful work. The persistent worker pool is what reaches 17x. The benchmark's comments
document this so that users do not attribute controller dispatch costs to the API.

Files:

- `benches/rt_quiesce.rs` (new)
- `benches/Cargo.toml`

### Filing order and coordination notes

1. Before filing anything: comment on #6113 to coordinate with its author (l4l). PR1 covers
   the same ground with a different API shape, and hijacking a stalled PR's feature without
   acknowledgment is poor form.
2. File the proposal issue first; link it from PR1.
3. PR1 and the issue can go out together. PR2 goes out only after PR1 has maintainer
   engagement.
4. Loom CI labels: Tokio's loom workflows run pre-merge only when the matching `R-loom-*`
   label is on the PR, and the repository's labeler applies labels from runtime source
   paths, not from the loom test files themselves. PR1 picks up `R-loom-blocking`
   automatically (it touches the blocking schedule) but needs `R-loom-current-thread` added
   manually so its new model in the current-thread loom suite runs. PR2 picks up
   `R-loom-current-thread` and `R-loom-time-driver` automatically but needs
   `R-loom-blocking` added manually for its blocking-release model.
5. The upstream PRs are assembled fresh from the file lists and descriptions above. The
   proposing repository's own development history — including its planning documents and
   the commits that touch them — stays local and is not pushed as any part of the upstream
   series. Files appearing in both PR1's and PR2's lists (`clock.rs`, `time/mod.rs`,
   `loom_current_thread.rs`, `spellcheck.dic`) carry an intermediate state in PR1:
   everything referencing quiesce symbols — the quiesce-waiter panic integration and doc
   cross-references in `clock.rs`, the quiesce module declaration and re-exports in
   `time/mod.rs`, the `quiesce_*` loom models, and the quiesce-related dictionary entries —
   must be excluded from PR1's copies, since those symbols do not exist until PR2. Run the
   full local CI equivalent (build, rustdoc with warnings denied, spellcheck, and a
   `--cfg loom` build of the current-thread loom suite) on the assembled PR1 tree, since
   that intermediate state never existed in this repository's history.
