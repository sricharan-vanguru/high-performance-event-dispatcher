# Subscriber State and Snapshot Registry

Phase 2 separates two concerns:

- `subscriber_state<Event>` owns callback activity and safe retirement.
- `snapshot_registry<Event>` owns membership and immutable list publication.

The registry is optimized for frequent dispatch reads and relatively infrequent
membership changes. It is low-lock, not universally lock-free:
`atomic<shared_ptr<T>>` is not required to be lock-free on every platform.

## Dispatch and writer paths

```text
Dispatch worker                         Subscribe/unsubscribe writer
---------------                         ----------------------------
atomic load snapshot                    lock writer mutex
       |                                copy/filter current list
       v                                publish new atomic snapshot
iterate immutable vector                unlock writer mutex
       |
try_invoke subscriber state
```

Dispatch never takes the registry writer mutex. An acquired snapshot owns every
state it exposes, so it remains valid after a newer snapshot is published or the
registry facade is destroyed.

## Callback entry versus unsubscribe

Logical activity and the in-flight count use the same state mutex:

```text
Callback entry                         Unsubscribe
--------------                         -----------
lock state                             lock state
active?                                active = false  <-- linearization point
in_flight++                            unlock state
unlock state                           remove from future snapshots
run user callback                      wait until in_flight == 0
lock state                             return
in_flight--
unlock; notify waiter
```

Whichever operation obtains the state mutex first determines the result:

1. Unsubscribe first: callback entry observes inactive and skips.
2. Callback entry first: unsubscribe observes the in-flight count and waits.

Normal synchronous unsubscribe therefore guarantees that after it returns, no
callback for that subscription is executing and no future callback can begin.

## Why an old snapshot is safe

```text
Worker loads snapshot A
                         unsubscribe marks state inactive
                         registry publishes snapshot B without state
Worker reads state from A
Worker try_invoke -> false
Snapshot A released -> state may now be reclaimed
```

Physical removal alone is insufficient because a worker may already own
snapshot A. Logical deactivation prevents callback entry; shared ownership
prevents use-after-free.

## Self-unsubscribe

A callback waiting for its own in-flight count would deadlock:

```text
callback -> unsubscribe -> wait for callback -> impossible
```

Callback entry places a linked context node in thread-local storage. It uses
stack memory and performs no heap allocation. Self-unsubscribe detects that
context, marks the state inactive, removes it from future snapshots, and returns
without waiting. The invocation guard decrements the final in-flight count when
the callback exits. Future entry is prevented immediately.

Nested callback execution is supported because the context is a linked stack
rather than one Boolean flag.

## Callback exceptions

`try_invoke` does not choose application error policy. Callback exceptions
propagate to the future dispatcher, while an RAII guard releases the in-flight
count during stack unwinding. Phase 3 will contain exceptions at the worker
boundary and forward them to an error policy.

## Ownership boundaries

Registry snapshots share ownership of callback control blocks. The move-only
`subscription` token owns the right to retire one registration. Move-assigning a
token first retires its previous registration and then accepts the incoming one.

The callback's captured application object has a separate lifetime. Capturing a
raw `this` is not made safe by retaining the control block. A later phase will
add a weak-ownership helper for externally owned objects.

## Complexity

- Snapshot acquisition: `O(1)` atomic shared-ownership acquisition.
- Snapshot iteration: `O(active subscribers)`.
- Subscribe/unsubscribe: `O(active subscribers)` copy-on-write publication.
- Callback entry/exit: one short state-mutex critical section each.
- Normal unsubscribe: may block for callbacks already in flight.

Later phases will compare intrusive and epoch-based reclamation with this
understandable shared-ownership baseline.
