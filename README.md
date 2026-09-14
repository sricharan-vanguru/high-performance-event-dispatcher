# High-Performance Event Dispatcher

A modular C++20 event dispatcher for studying high-throughput publication,
safe concurrent subscription changes, and subscriber lifetime management.

The repository currently contains the architecture contracts and the first
functional component: a correctness-first bounded mutex queue. Runtime dispatch
will be added incrementally before introducing a bounded lock-free MPMC path.

## Intended guarantees

- Multiple producers and worker threads
- Bounded publication with explicit backpressure
- No registry modification lock on the callback dispatch path
- Safe synchronous unsubscribe with no callback executing after it returns
- RAII subscriptions and documented self-unsubscription behavior
- Pluggable ownership experiments: shared, weak, and intrusive

See the [architecture](docs/architecture.md),
[concurrency contract](docs/concurrency-contract.md),
[policy boundaries](docs/policy-boundaries.md),
[bounded mutex queue design](docs/bounded-mutex-queue.md), and the
[public roadmap](docs/roadmap.md).

## Build

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build --output-on-failure
```

Optional switches:

```text
EVENT_DISPATCHER_BUILD_TESTS=ON
EVENT_DISPATCHER_BUILD_EXAMPLES=ON
EVENT_DISPATCHER_BUILD_BENCHMARKS=OFF
EVENT_DISPATCHER_ENABLE_SANITIZERS=OFF
EVENT_DISPATCHER_ENABLE_THREAD_SANITIZER=OFF
EVENT_DISPATCHER_WARNINGS_AS_ERRORS=OFF
```

AddressSanitizer/UndefinedBehaviorSanitizer and ThreadSanitizer intentionally
use separate build directories because those runtimes are incompatible.

## Current status

Phase 1 bounded mutex queue implemented. No lock-free claim is made before the
corresponding implementation, correctness tests, and benchmarks exist.
