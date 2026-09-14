# High-Performance Event Dispatcher

A modular C++20 asynchronous event dispatcher with bounded publication,
concurrent subscribers, and safe callback lifetime management.

The implementation combines interchangeable mutex and lock-free bounded MPMC
queues, immutable subscriber snapshots, safe synchronous unsubscription, and a
`std::jthread` worker pool.

## Features

- Multiple producers and worker threads
- Optional bounded lock-free MPMC publication policy
- Direct in-place event construction and ordered-prefix batch publication
- Configurable worker batching with explicit snapshot semantics
- Bounded publication with explicit backpressure
- No registry modification lock on the callback dispatch path
- Safe synchronous unsubscribe with no callback executing after it returns
- RAII subscriptions and documented self-unsubscription behavior
- Drain and discard shutdown policies
- Callback exception containment and observable delivery accounting

## Quick start

```cpp
#include "event_dispatcher/event_dispatcher.hpp"

event_dispatcher::dispatcher<int> dispatcher;

auto subscription = dispatcher.subscribe([](const int& event) {
    // Process the event. Multiple worker threads may invoke callbacks concurrently.
});

const auto result = dispatcher.publish(42);
dispatcher.shutdown(); // Drain accepted events and join workers.
```

Keep the subscription token alive for as long as the callback should remain
registered. See the API reference for shutdown, backpressure, and thread-safety
details.

Select the lock-free queue policy explicitly when the target reports the
required atomics as lock-free:

```cpp
using fast_dispatcher = event_dispatcher::dispatcher<
    int,
    event_dispatcher::queue::bounded_lock_free_queue>;

event_dispatcher::dispatcher_config config;
config.queue_capacity = 1024; // Power of two and at least two.
fast_dispatcher dispatcher{config};
```

Construct directly in queue storage or publish an ordered batch:

```cpp
dispatcher.try_emplace(42); // Constructs int directly in the queue slot.

std::array<int, 3> events{1, 2, 3};
const auto result = dispatcher.try_publish_batch(std::span<int>{events});
// [0, result.accepted) was accepted. Processing stopped at result.status.
```

## Documentation

- [Architecture](docs/architecture.md)
- [Public API and thread safety](docs/api-reference.md)
- [Concurrency contract](docs/concurrency-contract.md)
- [Policy boundaries](docs/policy-boundaries.md)
- [Bounded mutex queue](docs/bounded-mutex-queue.md)
- [Bounded lock-free MPMC queue](docs/bounded-lock-free-queue.md)
- [Allocation control, emplacement, and batching](docs/allocation-emplacement-batching.md)
- [Subscriber state and snapshot registry](docs/snapshot-registry.md)
- [Worker pool and reference dispatcher](docs/reference-dispatcher.md)
- [Shutdown and backpressure](docs/shutdown-backpressure.md)
- [Verification and corner-case coverage](docs/testing.md)
- [Known limitations](docs/known-limitations.md)

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

## Install and consume with CMake

```bash
cmake --install build/release --prefix /your/install/prefix
```

```cmake
find_package(HighPerformanceEventDispatcher 1.0 REQUIRED)
target_link_libraries(your_target PRIVATE event_dispatcher::event_dispatcher)
```

The package uses semantic version compatibility within major version 1.

## License

This project is available under the [MIT License](LICENSE).
