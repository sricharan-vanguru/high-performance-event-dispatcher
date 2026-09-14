# Benchmark Methodology

The Phase 1 executable measures the correctness-first `bounded_mutex_queue` in
four topologies: SPSC, MPSC, SPMC, and MPMC. Every case transfers 100,000
eight-byte events through a capacity-1024 queue.

Reported publication latency includes the complete blocking `wait_push` call.
It therefore captures both uncontended queue cost and time spent under
backpressure. The executable reports throughput plus p50, p95, and p99 latency.

Build and run an optimized baseline:

```bash
cmake --preset benchmarks
cmake --build --preset benchmarks
./build/benchmarks/benchmarks/event_dispatcher_bounded_mutex_queue_benchmark
```

Results are not comparable unless the CPU, operating system, compiler, build
flags, queue capacity, event size, thread counts, and machine load are also
recorded. The benchmark intentionally uses no external framework yet; a later
phase may add one after workload semantics stabilize.

## Snapshot registry baseline

`event_dispatcher_snapshot_registry_benchmark` measures immutable snapshot
acquisition and copy-on-write membership changes at 1, 10, 100, and 1,000
subscribers. A subscribe/reset iteration counts as two membership changes.

The expected trade-off is inexpensive, low-lock dispatch reads in exchange for
`O(subscriber_count)` writes. Results must be recorded with the same machine and
compiler context described above.
