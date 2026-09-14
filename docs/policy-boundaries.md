# Policy Boundaries

The dispatcher is modular through responsibility boundaries, not through an
unbounded number of abstract base classes. A boundary becomes a C++ policy only
when it has an implementation, tests, and a real substitution need.

## Boundary responsibilities

| Boundary | Owns | Must not own |
|---|---|---|
| Queue | event storage, capacity, producer/consumer synchronization, close wakeups | callbacks, subscribers, worker lifetime |
| Registry | subscriber membership and immutable read snapshots | event storage, callback execution, worker threads |
| Executor | worker creation, waits, stop requests, joins, dispatch scheduling | subscription membership or reclamation rules |
| Lifetime/reclamation | callback-state reachability and safe retirement | queue ordering or backpressure decisions |
| Backpressure | caller-visible response to unavailable queue capacity | queue storage or subscriber behavior |
| Error handling | callback failure reporting and containment | queue state transitions or worker ownership |
| Metrics | low-overhead observation and aggregation | correctness decisions or control flow required for safety |
| Dispatcher | configuration validation and orchestration of all boundaries | component-specific synchronization details |

Each synchronization primitive belongs to the component whose invariant it
protects. User callbacks execute outside all queue and registry writer locks.

## Polymorphism decisions

### Compile-time substitution

Queue algorithms and, later, reclamation algorithms are candidates for concepts
or template policies because they run on measured hot paths and need direct
comparison without virtual calls. Concrete types remain internal where possible.

### Runtime behavior

Callbacks, error handlers, configuration, and optional external executors are
runtime concerns. They use ordinary composition or type erasure where dynamic
behavior is part of the feature, after allocation and call overhead are measured.

### No abstraction yet

Backpressure, metrics, and worker execution begin as concrete components. They
will not receive template policy layers until a second behavior requires clean
substitution. This avoids speculative templates and excessive compile time.

## Intended dependency rules

1. The public facade depends on component contracts.
2. Components do not include the public facade.
3. Registry and queue do not depend on each other.
4. Lifetime logic can be tested without worker threads.
5. Queue algorithms can be tested without subscribers.
6. Executors receive operations to perform; they do not define subscription
   validity.
7. Metrics observe outcomes and never decide safety-critical state transitions.
8. Platform-specific affinity and NUMA code remain optional leaf modules.

## SOLID interpretation for a performance library

- **Single responsibility**: each boundary owns one set of invariants.
- **Open/closed**: alternative measured algorithms can satisfy narrow contracts
  without modifying dispatcher orchestration.
- **Liskov substitution**: every implementation preserves documented semantics;
  a faster queue cannot silently weaken close or ownership behavior.
- **Interface segregation**: producers, workers, and subscription management use
  only operations they require.
- **Dependency inversion**: orchestration depends on contracts, while concrete
  synchronization stays behind those contracts.

SOLID does not require virtual functions everywhere. Static polymorphism,
composition, RAII, and narrow concepts are preferred when they preserve the
same design purpose with lower hot-path overhead.
