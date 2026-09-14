# Architecture

## Design goals

1. Keep publication latency predictable under producer contention.
2. Make subscription changes safe without placing a registry lock on dispatch.
3. Provide an explicit guarantee against callbacks after synchronous unsubscribe.
4. Separate queueing, dispatch, registry, execution, and lifetime management.
5. Measure every performance-oriented change against a simple baseline.

## Module boundaries

```text
Public API
   |
   +-- Dispatcher orchestration
          |
          +-- Queue policy (mutex baseline / bounded MPMC)
          +-- Registry policy (immutable snapshot)
          +-- Execution policy (inline / worker pool)
          +-- Lifetime policy (shared / weak / intrusive experiment)
          +-- Backpressure policy (reject / wait / timeout)
          +-- Error policy (callback failure handling)
          +-- Metrics policy (disabled / local counters)
```

The public dispatcher will coordinate policies without exposing their internal
synchronization. Low-level algorithms remain independently testable. The exact
responsibilities and dependency direction are specified in
[policy boundaries](policy-boundaries.md).

## Dependency direction

```text
Application
    |
Public dispatcher API
    |
Dispatcher orchestration
    +-- queue
    +-- registry
    +-- executor
    +-- lifetime/reclamation
    +-- backpressure
    +-- error handling
    `-- metrics
```

Dependencies point inward from orchestration to narrow component contracts.
Queue, registry, and reclamation implementations never call the public facade.

## Performance rules

- No allocation is permitted on the successful bounded-queue publication path.
- Subscriber callbacks run without holding the registry modification mutex.
- Shared cache-line write ownership is minimized and measured.
- Memory ordering must be justified next to each non-trivial atomic operation.
- A lock-free claim applies only to a specifically documented operation.
- Benchmarks must include latency distributions, not only average throughput.
- Virtual dispatch is excluded from measured hot paths unless measurement shows
  that the flexibility is worth its cost.

## Safety rules

- Dispatcher destruction joins every worker before releasing shared state.
- Subscription state remains alive while any dispatcher snapshot references it.
- Synchronous unsubscribe waits for in-flight callbacks, except self-unsubscribe,
  which transitions to inactive and completes reclamation after callback exit.
- Raw `this` capture is not presented as lifetime-safe; weak ownership is the
  default helper for externally owned subscriber objects.

The precise vocabulary, lifecycle, ordering, and unsubscribe guarantees are in
[the concurrency contract](concurrency-contract.md).

## Design discipline

- Start with one correct concrete implementation behind each boundary.
- Add a policy abstraction only when an alternative implementation or isolated
  test seam exists.
- Prefer composition and ownership-explicit RAII objects.
- Use compile-time polymorphism only for measured hot-path substitution.
- Use ordinary runtime callbacks at API edges where user behavior is inherently
  dynamic.
- Keep synchronization private to the component that owns the protected state.
- Do not expose atomics as a substitute for a behavioral contract.
