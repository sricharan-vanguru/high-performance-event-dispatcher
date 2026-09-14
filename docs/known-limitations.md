# v1.0 Known Limitations and Performance Boundaries

The v1.0 release is the correctness reference. It deliberately avoids claims
that belong to later measured implementations.

- Publication uses one mutex-protected bounded MPMC queue. Non-blocking
  `try_publish` is a non-waiting API, not a lock-free operation.
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
