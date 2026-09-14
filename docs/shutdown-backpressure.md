# Shutdown and Backpressure

The dispatcher makes overload and lifecycle behavior explicit and observable.

## Lifecycle

```text
created -> running -> drain-stopping   -> stopped
                   \
                    -> discard-stopping -> stopped
```

`created` exists as the construction state. A successfully returned dispatcher
is already `running`. The first shutdown caller chooses drain or discard; later
concurrent callers wait for that transition and return when the dispatcher is
`stopped`. Repeated shutdown after that is a no-op.

Queue close is the publication acceptance boundary. A publication already
racing with shutdown either wins the queue lock and becomes accepted, or sees
the closed queue and is rejected. New calls observe the stopping state and
reject without touching the queue.

## Drain and discard

Drain performs this sequence:

```text
stop acceptance -> wake blocked producers -> consume queued events
                -> finish callbacks -> join workers -> stopped
```

Discard atomically closes the queue and destroys events that have not been
dequeued:

```text
stop acceptance -> destroy queued events -> wake blocked producers/consumers
                -> finish active callbacks -> join workers -> stopped
```

Discard never interrupts a callback that has already started. Such an event is
not counted as dropped because a worker already owns it. Each event removed
from queue storage is destroyed exactly once and increments `dropped`.

Calling shutdown from a dispatcher callback throws `std::logic_error` rather
than attempting to join the current worker.

## Backpressure APIs

- `try_publish(event)` is the reject policy. It returns `full` immediately.
- `publish(event, stop_token)` is interruptible blocking publication.
- `publish_for(event, timeout, stop_token)` bounds the wait duration.
- `publish_until(event, deadline, stop_token)` uses a steady-clock deadline.

A failed move publication does not consume its event. `timeout` means the
deadline expired while the queue remained full. `stopped` means producer-side
cancellation won. `closed` means dispatcher shutdown prevented acceptance.

## Metrics

`metrics()` returns three monotonic counters:

- `accepted`: publication operations that successfully entered the queue.
- `rejected`: full, timeout, cancellation, or closed publication outcomes.
- `dropped`: accepted events removed from queue storage by discard shutdown.

During active traffic, the three atomic loads form a diagnostic snapshot and
may reflect slightly different instants. After producers finish and shutdown
returns, the values are stable and suitable for exact accounting.

## Subscriptions

Subscriptions are accepted only while the dispatcher is running. A subscription
racing with shutdown is serialized against the lifecycle transition: it either
completes before stopping begins or throws `std::logic_error`. Existing tokens
remain safe to query or reset during and after shutdown.
