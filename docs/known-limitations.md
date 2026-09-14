# Known Limitations and Performance Boundaries

The mutex queue remains the default correctness reference. The lock-free queue
is an explicit policy because performance and target atomic support must be
measured rather than assumed.

- Default publication uses the mutex-protected bounded MPMC queue. Select
  `bounded_lock_free_queue` explicitly for a lock-free non-blocking queue data
  path when `data_path_is_lock_free()` reports true on the target.
- Lock-free queue capacity must be a power of two and at least two.
- Lock-free waiting APIs still park, timed waits use a condition variable, and
  close/discard transitions use a lifecycle mutex. The complete dispatcher is
  not lock-free.
- Each publication updates one relaxed global atomic counter. Under extreme
  producer counts this counter can become a contended cache line.
- Dispatch loads an atomic `shared_ptr` snapshot. The C++ standard does not
  guarantee that this shared-pointer specialization is lock-free.
- Subscription changes copy the active subscriber vector and are therefore
  `O(subscriber_count)`. Dispatch snapshot acquisition remains approximately
  constant-time in the v1.0 baseline.
- Callbacks and error handlers use `std::function`; large captures may allocate.
- Multiple workers may invoke the same subscriber concurrently. Per-subscriber
  serialization and slow-subscriber isolation are future delivery policies.
- Weak object ownership, intrusive lifetime management, batching, queue
  sharding, CPU affinity, adaptive waiting, and NUMA awareness are not yet part
  of the stable implementation.
- Shutdown from a dispatcher callback is prohibited because a worker cannot
  synchronously join itself.
- A blocking publication from a callback can deadlock if all workers are
  occupied and the bounded queue is full. Reentrant callbacks should use
  `try_publish` or a timed/cancellable publication strategy.
- Metrics use 64-bit monotonic counters and do not implement saturation; an
  individual counter wraps after `2^64` recorded outcomes.

The local baseline uses an Intel i7-10750H and GCC 13.3 in Release mode. Exact
numbers are intentionally kept outside the repository because machine load and
frequency were not controlled. The reproducible workload and interpretation
are documented in [the benchmark methodology](../benchmarks/README.md).
