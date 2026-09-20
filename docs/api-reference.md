# Public API and Thread-Safety Reference

This document defines the supported public surface. Unless stated otherwise, the
dispatcher object must outlive every thread calling it. Destruction requires
external lifetime coordination and must not race with public method calls.

## `dispatcher<Event, QueuePolicy>`

`Event` must be nothrow move-constructible. The default queue policy is
`bounded_mutex_queue`; the optional `bounded_lock_free_queue` and other
alternatives must satisfy `concurrent_queue`.
`try_emplace` participates in overload resolution only when an alternative
queue policy also satisfies the optional `emplacing_queue` capability.
The dispatcher is neither copyable nor movable because worker threads and
lifecycle synchronization have stable ownership.

`dispatcher_config` contains four fields: `queue_capacity` must satisfy the
selected queue policy, `worker_count` and `worker_batch_size` must be non-zero,
and `shutdown` selects the policy used by parameterless shutdown and destruction.
`hardware_concurrency_defaults()` returns at least one worker even when the
platform cannot report its topology.

| Method | Concurrent use | Result and exceptions |
|---|---|---|
| Constructor | Not applicable | May throw for invalid configuration, allocation failure, or thread creation failure |
| `subscribe(callback, options)` | Safe with publication and other subscriptions | Options default to concurrent delivery; throws `invalid_argument` for an empty callback, invalid mailbox/threshold settings, or isolated delivery of a non-copyable Event, and `logic_error` after shutdown begins; allocation or isolated thread creation may throw |
| `try_publish(event)` | Safe for multiple producers | Returns immediately with `success`, `full`, or `closed`; event construction/copy exceptions propagate |
| `try_emplace(args...)` | Safe for multiple producers | Constructs directly after capacity reservation; `full`/`closed` do not consume arguments; constructor exceptions propagate after rollback |
| `try_publish_batch(span)` | Safe for multiple producers | Accepts an ordered prefix and stops at the first failure; reports requested count, accepted prefix, and terminal status |
| `publish(event, stop)` | Safe for multiple producers | Waits interruptibly; returns `success`, `closed`, or `stopped` |
| `publish_for(event, timeout, stop)` | Safe for multiple producers | Also returns `timeout` when capacity remains unavailable |
| `publish_until(event, deadline, stop)` | Safe for multiple producers | Uses `steady_clock`; immediate capacity wins over an expired deadline or requested stop |
| `shutdown()` | Safe for concurrent callers | First caller uses the configured policy; callback-side use throws `logic_error` |
| `shutdown(policy)` | Safe for concurrent callers | First caller selects the policy; later callers wait for the chosen transition |
| `worker_count()` | Safe | Immutable diagnostic value |
| `subscriber_count()` | Safe | Synchronized snapshot that may become stale immediately |
| `lifecycle()` | Safe | Atomic lifecycle observation |
| `metrics()` | Safe | Relaxed atomic diagnostic snapshot; exact after producers stop and shutdown completes |

Workers dequeue at most `worker_batch_size` events and use one subscriber
snapshot for that complete batch. See the
[batching contract](allocation-emplacement-batching.md).

Successful publication transfers event responsibility to the dispatcher. A
failed rvalue publication does not move from the supplied event. If copying an
event throws, the queue remains structurally unchanged and no outcome counter
is incremented because the publication produced no result.

## Callback and error-handler concurrency

No queue mutex or registry writer mutex is held while user code executes.
Concurrent subscriptions can run on multiple dispatcher workers, serialized
subscriptions prevent overlap with a per-subscriber mutex, and isolated
subscriptions run on a dedicated subscriber thread. The configured error
handler may still run concurrently for different subscribers and must protect
shared mutable state. Callback exceptions are sent to the error handler and do
not terminate workers or isolated executors; error-handler exceptions are
swallowed. See [subscriber delivery policies](delivery-policies.md).

## `subscription`

`subscription` is move-only RAII ownership of a registration. Destruction and
`reset()` perform synchronous unsubscription. After normal unsubscription
returns, no callback is executing and no later callback can enter.

Different subscriber states can be retired concurrently. The same token object
must not be read, moved, reset, or destroyed concurrently without external
synchronization. Self-unsubscription is supported: it prevents future entry
immediately and completes retirement when the current callback exits.

| Method | Concurrent use on the same token | Behavior |
|---|---|---|
| Default constructor | Not applicable | Creates an empty token |
| Move constructor/assignment | Requires external synchronization | Transfers ownership; assignment first retires the destination's old registration |
| Destructor | Requires external synchronization | Synchronously unsubscribes when non-empty |
| `reset()` | Requires external synchronization | Idempotently unsubscribes and empties the token |
| `subscribed()` / `operator bool()` | Requires external synchronization against mutation | Reports whether the controlled subscriber remains active |
| `metrics()` | Requires external synchronization against token mutation | Returns a relaxed atomic per-subscriber diagnostic snapshot |

## `bounded_mutex_queue<Event>`

All queue methods are safe for concurrent producers and consumers; destruction
requires external lifetime coordination. `try_push` and `try_pop` do not
intentionally wait, but they still acquire the queue mutex and are not lock-free.
`close()` preserves queued events, while `close_and_discard()` destroys them
under the queue's exclusive state transition.

| Method | Behavior |
|---|---|
| Constructor | Allocates fixed ring storage; rejects zero capacity |
| `try_push` | Returns immediately with `success`, `full`, or `closed` |
| `try_emplace` | Direct construction with argument retention on `full`/`closed` and exception rollback |
| `wait_push` | Waits for capacity, close, or cancellation |
| `wait_push_until` | Adds steady-clock deadline expiry |
| `try_pop` | Returns immediately with an event, `empty`, or `closed` |
| `wait_pop` | Waits for an event, close, or cancellation |
| `close` | Idempotently rejects pushes while preserving queued events |
| `close_and_discard` | Idempotently closes, destroys queued events, and returns their count |
| `capacity` | Immutable and safe without locking |
| `size` / `closed` | Synchronized diagnostic snapshots that can immediately become stale |

## `bounded_lock_free_queue<Event>`

This policy preserves the same publication, waiting, close, and event-lifetime
contract with two SCQ index rings and preallocated Event slots. Capacity must be
a power of two and at least two. `try_push` and `try_pop` use no mutex; waiting
APIs are blocking adapters and lifecycle transitions are serialized
independently.

`data_path_is_lock_free()` verifies the participating atomics on the running
target. The lock-free progress claim excludes user Event operations, wake
notification internals, waiting APIs, shutdown, and the dispatcher as a whole.
See the [algorithm and memory-ordering rationale](bounded-lock-free-queue.md).

## `snapshot_registry<Event>`

`subscribe()` is safe for concurrent writers and may allocate while copying the
active list. `acquire_snapshot()` is safe and does not take the writer mutex;
the returned immutable shared snapshot keeps subscriber state alive. `active_count()`
is a synchronized diagnostic observation. Registry destruction must not race
with calls on the registry facade, but previously returned tokens and snapshots
remain memory-safe afterward.
