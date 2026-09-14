# Implementation Roadmap

## Completed

### Phase 0 — Architecture contracts

- Concurrency vocabulary and progress guarantees
- Module boundaries and performance-aware SOLID decisions
- Sanitizer-aware build configuration

### Phase 1 / v0.2.0 — Correct bounded mutex queue

- Fixed-capacity MPMC ring storage
- Non-blocking and interruptible blocking operations
- Explicit close, stop, full, and empty behavior
- Concurrent stress tests and latency baseline

### Phase 2 / v0.3.0 — Safe subscriber registry

- Shared subscriber control blocks
- Immutable copy-on-write registry snapshots
- Strong synchronous unsubscribe
- Deadlock-free self-unsubscribe
- Old-snapshot and lifetime race tests

## Next

### Phase 3 / v0.4.0 — Worker pool and reference dispatcher

- `std::jthread` worker lifecycle
- Dispatcher facade connecting queue and registry
- Broadcast semantics and callback error containment
- End-to-end concurrency tests and examples

### Phase 4 / v0.5.0 — Shutdown and backpressure hardening

- Drain and discard shutdown state machines
- Reject, blocking, and timeout backpressure
- Shutdown race matrix and observable rejection counters

### Phase 5 / v1.0.0 — Stable correctness-first release

- Public API review and thread-safety documentation
- GCC/Clang CI and sanitizer jobs
- Installation, packaging, license, and reproducible baseline

## Advanced track

- v1.1.0: bounded lock-free MPMC queue
- v1.2.0: allocation control, emplacement, and batching
- v1.3.0: delivery policies and slow-subscriber isolation
- v1.4.0: weak ownership
- v1.5.0: intrusive lifetime experiment
- v2.0.0: epoch/RCU-style reclamation
- v2.1.0: queue sharding
- v2.2.0: Linux affinity and adaptive waiting
- v2.3.0: low-overhead observability
- v3.0.0: NUMA-aware experimental dispatcher
