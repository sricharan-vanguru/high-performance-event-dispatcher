# Changelog

## 1.2.0 — Unreleased

- Added direct queue and dispatcher emplacement without an Event temporary.
- Added ordered-prefix batch publication with explicit partial-success results.
- Added configurable worker batching with one subscriber snapshot per batch.
- Added allocation, callback-erasure, and batch-throughput measurements plus
  construction, exception, ownership, metrics, and snapshot-consistency tests.

## 1.1.0

- Added an optional bounded lock-free MPMC queue policy using DISC 2019 SCQ
  index rings and fixed preallocated event storage.
- Preserved cancellation, timeout, drain/discard, and throwing-copy behavior
  through blocking adapters and explicit lifecycle gates.
- Added wraparound, lifetime, exception, topology, shutdown-race, and dispatcher
  integration tests plus a mutex-versus-lock-free comparison benchmark.

## 1.0.0

- Stable correctness-first asynchronous dispatcher API.
- Bounded mutex MPMC queue with reject, blocking, cancellation, and timeout outcomes.
- Safe copy-on-write subscriber registry and synchronous unsubscription.
- Drain and discard shutdown with observable lifecycle and accounting.
- CMake installation/package exports, CI matrix, examples, and sanitizer tests.

## 0.5.0

- Hardened shutdown, discard, backpressure, and lifecycle metrics.

## 0.4.0

- Added the worker pool and first end-to-end reference dispatcher.

## 0.3.0

- Added safe subscriber state and immutable snapshot registration.

## 0.2.0

- Added the correctness-first bounded mutex queue.
