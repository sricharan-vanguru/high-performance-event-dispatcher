# High-Performance Event Dispatcher

A modular C++20 event dispatcher for studying high-throughput publication,
safe concurrent subscription changes, and subscriber lifetime management.

The repository currently contains the buildable architecture skeleton. Runtime
dispatch behavior will be added incrementally, beginning with a correctness-first
reference implementation and then a bounded MPMC publication path.

## Intended guarantees

- Multiple producers and worker threads
- Bounded publication with explicit backpressure
- No registry modification lock on the callback dispatch path
- Safe synchronous unsubscribe with no callback executing after it returns
- RAII subscriptions and documented self-unsubscription behavior
- Pluggable ownership experiments: shared, weak, and intrusive

See [architecture](docs/architecture.md) and [roadmap](docs/roadmap.md).

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
EVENT_DISPATCHER_WARNINGS_AS_ERRORS=OFF
```

## Current status

Architecture skeleton only. No performance or lock-free claim is made before
the corresponding implementation, correctness tests, and benchmarks exist.
