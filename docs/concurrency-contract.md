# Concurrency Contract

This document defines the vocabulary and guarantees that every implementation
must preserve. Algorithm-specific guarantees may be stronger, but never weaker.

## Operation vocabulary

- **Acceptance**: the dispatcher has taken responsibility for an event. A
  successful publication result is the acceptance linearization point.
- **Enqueue**: an accepted event becomes visible to a queue consumer.
- **Dequeue**: exactly one worker obtains ownership of one queued event.
- **Snapshot acquisition**: that worker selects the immutable subscriber set
  used for this delivery.
- **Broadcast**: the worker offers the dequeued event once to every eligible
  subscriber in the acquired snapshot.
- **Callback entry**: subscriber state grants one invocation permission and
  records it as in flight.
- **Callback completion**: the callback has returned or thrown, and its in-flight
  permission has been released.

Queue delivery and subscriber broadcast are different guarantees. Multiple
workers compete to dequeue an event; subscribers do not compete for that event.
One successful dequeue leads to one broadcast operation.

## Publication result

A failed publication transfers no event responsibility to the dispatcher. A
successful publication means the event will follow the selected shutdown and
delivery policies; it does not mean callbacks have started or completed.

The concrete result states for full, closed, stopped, and timed-out publication
will be finalized with the Phase 1 queue API.

## Ordering

- A queue implementation documents the order in which successful publications
  become available to consumers.
- With one worker, broadcasts begin in dequeue order.
- With multiple workers, callback start and completion order across different
  events is not globally guaranteed.
- Subscriber serialization, if selected later, adds a per-subscriber ordering
  policy rather than changing global queue semantics.
- Subscribe/unsubscribe races are resolved by snapshot acquisition and callback
  entry rules, not by wall-clock timestamps.

## Synchronous unsubscribe guarantee

Normal synchronous unsubscribe has this postcondition:

> After unsubscribe returns, no callback for that subscription is executing,
> and no future callback for that subscription can begin.

Unsubscribe first prevents new callback entry, removes the subscription from
future registry snapshots, and waits for invocations already in flight.

Self-unsubscribe cannot wait for its own invocation without deadlocking. It
therefore prevents future entry immediately and defers final retirement until
the current callback completes. This exception must be detectable, documented,
and tested; it must not weaken normal cross-thread unsubscribe.

## Lifetime guarantee

Removing a subscriber from the newest registry snapshot does not destroy state
that an older snapshot can still access. Reclamation occurs only after both:

1. no snapshot or token retains the subscription state; and
2. no callback invocation remains in flight.

The dispatcher protects callback state. A user object captured through a raw
pointer remains the user's responsibility. Recommended object subscriptions
will use weak ownership so callback entry can establish temporary strong
ownership safely.

## Shutdown lifecycle

The intended dispatcher states are:

```text
constructed -> running -> stopping_drain   -> stopped
                       `-> stopping_discard -> stopped
```

- Shutdown is idempotent.
- Once stopping begins, new publications are rejected.
- Drain processes every accepted queued event before workers stop.
- Discard destroys queued events without broadcasting them.
- A callback already in flight is allowed to finish in either mode.
- Destruction joins every worker before releasing shared dispatcher state.
- Blocking producers and consumers are awakened when shutdown changes whether
  their operation can succeed.

## Progress terminology

- **Blocking**: an operation may park and wait for another thread or condition.
- **Non-blocking API**: this particular call returns without intentionally
  waiting; it does not imply a formal system-wide progress guarantee.
- **Obstruction-free**: an operation completes if it eventually runs alone.
- **Lock-free**: system-wide progress is guaranteed; an individual operation may
  starve while some operation completes.
- **Wait-free**: every operation completes in a bounded number of its own steps.

A mutex queue is blocking. A future bounded MPMC `try_push`/`try_pop` may be
lock-free after its algorithm and target atomics are verified. Waiting wrappers,
subscription modification, shared ownership, callbacks, and the dispatcher as a
whole must not inherit that claim.

## Reentrancy

Callbacks may eventually publish, subscribe, and unsubscribe. No dispatcher
internal mutex may be held across user callback execution. Recursive publication
is still subject to capacity and backpressure. Self-unsubscribe follows the
special rule above. Dispatcher destruction from one of its own worker callbacks
will be explicitly prohibited or given a non-blocking handoff design before the
public dispatcher is implemented.
