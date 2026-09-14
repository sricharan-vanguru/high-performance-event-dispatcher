# High-Performance Event Dispatcher

A modular C++20 event dispatcher for studying high-throughput publication,
safe concurrent subscription changes, and subscriber lifetime management.

The repository now contains a complete correctness-first asynchronous dispatcher:
a bounded mutex queue, copy-on-write subscriber registry, and `std::jthread`
worker pool. This reference path establishes behavior before a bounded lock-free
MPMC queue is introduced.

## Project structure

```text
include/event_dispatcher/   Public API and event-dependent templates
src/                        Compiled non-template implementation
tests/                      Correctness, concurrency, and lifetime tests
examples/                   Small executable usage examples
benchmarks/                 Reproducible performance workloads
docs/                       Contracts, designs, and decisions
```

Templates such as `dispatcher<Event>` and `bounded_mutex_queue<Event>` remain
header-visible so applications can instantiate them for arbitrary event types.
Type-independent worker ownership, subscription-token operations, and hardware
configuration detection are compiled once in the library.

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
[bounded mutex queue design](docs/bounded-mutex-queue.md),
[subscriber state and snapshot registry](docs/snapshot-registry.md),
[worker pool and reference dispatcher](docs/reference-dispatcher.md), and the
[public roadmap](docs/roadmap.md).

## Build

```bash
cmake --preset release
cmake --build --preset release
ctest --preset release
```

All generated configurations are kept below one ignored directory:

```text
build/
├── debug/
├── release/
├── asan-ubsan/
├── tsan/
└── benchmarks/
```

Other standard workflows:

```bash
cmake --preset debug && cmake --build --preset debug && ctest --preset debug
cmake --preset asan-ubsan && cmake --build --preset asan-ubsan
ctest --preset asan-ubsan
cmake --preset benchmarks && cmake --build --preset benchmarks
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

Phase 3 worker pool and reference dispatcher implemented. No
lock-free claim is made before the corresponding implementation, correctness
tests, and benchmarks exist.
