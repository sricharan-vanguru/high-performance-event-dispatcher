# Implementation roadmap

## Phase 1: Correct reference implementation

- Mutex-backed bounded queue
- Subscriber control block and immutable registry snapshot
- Move-only RAII subscription
- Worker lifecycle, backpressure, exception handling, and shutdown
- Deterministic unit tests and race-oriented stress tests

## Phase 2: Performance publication path

- Bounded MPMC ring buffer with per-cell sequence numbers
- In-place event construction and destruction
- Cache-line layout review
- Mutex baseline versus MPMC queue benchmarks

## Phase 3: Lifetime strategy comparison

- Shared-ownership callback state
- Weak-ownership subscriber helper
- Intrusive or epoch-based experimental policy
- Documented memory, latency, complexity, and safety trade-offs

## Phase 4: Production hardening

- AddressSanitizer, UndefinedBehaviorSanitizer, and ThreadSanitizer jobs
- Fault injection and shutdown stress testing
- API documentation and usage examples
- Reproducible benchmark reports
