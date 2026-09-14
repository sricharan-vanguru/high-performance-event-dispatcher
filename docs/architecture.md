# Architecture

## Design goals

1. Keep publication latency predictable under producer contention.
2. Make subscription changes safe without placing a registry lock on dispatch.
3. Provide an explicit guarantee against callbacks after synchronous unsubscribe.
4. Separate queueing, dispatch, registry, execution, and lifetime management.
5. Measure every performance-oriented change against a simple baseline.

## Planned module boundaries

```text
Public API
   |
   +-- Dispatcher orchestration
          |
          +-- Queue policy (mutex baseline / bounded MPMC)
          +-- Registry policy (immutable snapshot)
          +-- Execution policy (inline / worker pool)
          +-- Lifetime policy (shared / weak / intrusive experiment)
          +-- Backpressure and error policies
```

The public dispatcher will coordinate policies without exposing their internal
synchronization. Low-level algorithms remain independently testable.

## Performance rules

- No allocation is permitted on the successful bounded-queue publication path.
- Subscriber callbacks run without holding the registry modification mutex.
- Shared cache-line write ownership is minimized and measured.
- Memory ordering must be justified next to each non-trivial atomic operation.
- A lock-free claim applies only to a specifically documented operation.
- Benchmarks must include latency distributions, not only average throughput.

## Safety rules

- Dispatcher destruction joins every worker before releasing shared state.
- Subscription state remains alive while any dispatcher snapshot references it.
- Synchronous unsubscribe waits for in-flight callbacks, except self-unsubscribe,
  which transitions to inactive and completes reclamation after callback exit.
- Raw `this` capture is not presented as lifetime-safe; weak ownership is the
  default helper for externally owned subscriber objects.
