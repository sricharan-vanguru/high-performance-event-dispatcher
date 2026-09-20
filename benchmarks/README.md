# Benchmark Methodology

The queue benchmark compares `bounded_mutex_queue` and
`bounded_lock_free_queue` under identical SPSC, MPSC, SPMC, and MPMC
topologies. Every case transfers 100,000 eight-byte events through a
capacity-1024 queue.

Reported publication latency includes the complete blocking `wait_push` call.
It therefore captures both uncontended queue cost and time spent under
backpressure. The executable reports throughput plus p50, p95, and p99 latency.
The blocking adapters are measured because they represent dispatcher behavior;
the result is not a pure atomic-core microbenchmark.

Build and run an optimized baseline:

```bash
cmake --preset benchmarks
cmake --build --preset benchmarks
./build/benchmarks/benchmarks/event_dispatcher_queue_comparison_benchmark
```

Results are not comparable unless the CPU, operating system, compiler, build
flags, queue capacity, event size, thread counts, and machine load are also
recorded. The benchmark intentionally uses no external framework yet; a later
phase may add one after workload semantics stabilize.

Lock freedom does not guarantee a performance win under every topology. Keep
both results even when the mutex baseline is faster.

## Allocation and worker-batch measurements

`event_dispatcher_allocation_batch_benchmark` reports successful lock-free queue
allocation counts, small/large subscription allocation counts, direct versus
`std::function` call cost, and dispatcher throughput for worker batch sizes 1,
4, 16, and 64:

```bash
./build/benchmarks/benchmarks/event_dispatcher_allocation_batch_benchmark
```

The executable overrides allocation functions only inside that benchmark
process. Results are observational and machine-specific; they are not API
guarantees for allocations performed inside user Event or callback code.

## Snapshot registry baseline

`event_dispatcher_snapshot_registry_benchmark` measures immutable snapshot
acquisition and copy-on-write membership changes at 1, 10, 100, and 1,000
subscribers. A subscribe/reset iteration counts as two membership changes.

The expected trade-off is inexpensive, low-lock dispatch reads in exchange for
`O(subscriber_count)` writes. Results must be recorded with the same machine and
compiler context described above.

## Subscriber delivery-policy comparison

`event_dispatcher_delivery_policy_benchmark` compares concurrent, serialized,
and isolated lossless delivery for 100,000 events with four dispatcher workers,
a worker batch size of 16, and one subscriber:

```bash
./build/benchmarks/benchmarks/event_dispatcher_delivery_policy_benchmark
```

It reports publication throughput and p50, p95, and p99 publication latency.
The isolated result includes bounded-mailbox backpressure and the dedicated
executor handoff. It does not measure callback completion latency, so use it to
compare publication-path behavior rather than end-to-end service time.
