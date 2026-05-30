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
    // Filled in by subsequent tasks. For now, a minimal compiling criterion setup:
    use criterion::{criterion_group, Criterion};

    fn placeholder(c: &mut Criterion) {
        c.bench_function("rt_quiesce_placeholder", |b| b.iter(|| ()));
    }

    criterion_group!(rt_quiesce, placeholder);
}

#[cfg(feature = "test-util")]
criterion::criterion_main!(bench::rt_quiesce);

#[cfg(not(feature = "test-util"))]
fn main() {
    eprintln!("rt_quiesce benchmark requires the `test-util` feature:");
    eprintln!("    cargo bench --features test-util --bench rt_quiesce");
}
