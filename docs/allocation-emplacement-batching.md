# Allocation Control, Emplacement, and Batching

This release reduces avoidable hot-path work while keeping ownership and
membership semantics explicit.

## Direct emplacement

Both queue policies and `dispatcher` provide `try_emplace(args...)`. Capacity
and lifecycle are checked before the arguments are forwarded, so a `full` or
`closed` result does not consume them.

```text
reserve capacity -> construct Event in slot -> publish occupied slot
                         |
                  constructor throws
                         |
             return reservation and rethrow
```

The mutex queue constructs inside its protected tail slot. The lock-free queue
first owns an available index, constructs in that stable slot, and publishes
the index only after construction succeeds. A constructor exception returns
the index to the available ring. There is never a visible partially constructed
Event.

Successful lock-free `try_emplace` and `try_pop` operations performed zero
allocations across 100,000 measured pairs on the local baseline. Event
constructors can still allocate internally; that work belongs to `Event`.

## Ordered-prefix batch publication

`try_publish_batch` accepts a `std::span<Event>` for moving or a
`std::span<const Event>` for copying. It processes input order and stops at the
first non-success result.

`batch_publish_result` reports `requested`, the exact accepted prefix in
`accepted`, the first failure in `status`, and `complete()` for full success.
Events before `accepted` belong to the dispatcher. The failing event and the
remaining suffix stay with the caller. Only the failed attempt increments the
rejected metric; the unattempted suffix does not.

This API deliberately does not promise all-or-nothing publication. Reserving a
whole atomic batch would increase contention, complicate shutdown, and make
capacity unavailable to unrelated producers.

## Worker batch consumption

`dispatcher_config::worker_batch_size` controls the maximum events a worker
claims before callback delivery. It must be non-zero and defaults to `1`.
The dispatcher allocates and reserves one reusable vector per worker before any
worker starts; later batches reuse that storage. Allocation failure therefore
propagates from dispatcher construction instead of terminating a worker.

```text
blocking pop of first event
        |
non-blocking pops up to worker_batch_size
        |
acquire one immutable subscriber snapshot
        |
deliver every batch event to that snapshot in event-major order
```

One worker preserves its claimed queue order inside a batch. Multiple workers
can still execute different batches concurrently, so the dispatcher does not
promise global callback order.

The membership snapshot is acquired after the full batch is dequeued and
before its first callback. A subscriber added during a batch starts with a
later batch. Synchronous unsubscribe remains stronger: inactive state prevents
later callback entry even when the old batch snapshot still owns the state.

## Measured optimization decisions

The local Release baseline compared worker batch sizes 1, 4, 16, and 64.
Larger batches improved this single-worker workload, but the relative results
changed noticeably between runs. The best value is workload- and
machine-dependent, so the conservative default remains `1` and applications
must measure their latency/throughput tradeoff.

Subscription measurement found four allocations for a small callback and five
for a 1 KiB capture, including subscriber state and snapshot replacement.
The measured `std::function` call overhead over its direct target was modest on
this machine. Subscription is a cold path and the incremental cost did not
justify a new custom type-erasure implementation with a larger correctness
surface.

`std::pmr` was also evaluated but is not exposed yet. Snapshot and subscriber
states can outlive the registry facade through tokens and old snapshots, so a
caller-supplied memory resource would need a strict, easy-to-misuse lifetime
contract. A future allocator policy should own its resource rather than merely
store a borrowed pointer.
