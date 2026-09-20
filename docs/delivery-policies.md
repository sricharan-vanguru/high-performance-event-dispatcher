# Subscriber Delivery Policies

Each subscription independently selects how callbacks execute. The dispatcher
queue remains shared and bounded; isolated subscriptions add a second bounded
queue between dispatcher workers and that subscriber.

## Policies

| Policy | Execution context | Same callback overlap | Per-subscriber FIFO |
|---|---|---|---|
| `concurrent` | Dispatcher workers | Allowed | Not guaranteed with multiple workers |
| `serialized` | Dispatcher workers under a subscriber mutex | Prevented | Not guaranteed with multiple workers |
| `isolated` | One dedicated subscriber thread | Prevented | Yes, in mailbox acceptance order |

`concurrent` is the default and has no extra mailbox handoff. `serialized`
protects callbacks that are not thread-safe, but workers racing for its mutex
can acquire it in a different order from queue dequeue order. `isolated` is the
choice for both slow-subscriber separation and per-subscriber FIFO processing.
It requires a copy-constructible event because each delivery is copied into the
subscriber mailbox.

## Isolated mailbox overload

Every isolated subscription has a non-zero `mailbox_capacity` and selects one
delivery guarantee:

- `best_effort` keeps dispatcher workers moving. A delivery is counted as
  dropped when the mailbox is full.
- `lossless` blocks the dispatcher worker until mailbox capacity becomes
  available or the subscription stops accepting deliveries. This preserves
  accepted deliveries but can propagate backpressure to the shared queue.

Lossless refers to the path from a dispatcher worker into an active isolated
subscription. Unsubscription and discard shutdown intentionally cancel pending
mailbox entries.

## Lifetime and shutdown

Unsubscription first prevents new callback entry, removes the subscriber from
future snapshots, stops its executor, discards queued mailbox entries, and then
waits for an active callback to finish. Self-unsubscription is supported and
returns from the callback without joining its own executor thread.

Drain shutdown lets workers enqueue every accepted dispatcher event, drains all
isolated mailboxes, and joins their executors. Discard shutdown clears both the
shared event queue and pending isolated mailbox entries. A callback already in
progress is never interrupted. Calling dispatcher shutdown from either a
direct or isolated callback throws `std::logic_error`.

## Per-subscription metrics

`subscription::metrics()` returns a relaxed atomic diagnostic snapshot:

- `offered`: worker delivery attempts, including attempts after retirement
  observed through an older snapshot.
- `delivered`: callbacks that completed, including callbacks that threw.
- `dropped`: best-effort overflow or pending isolated entries cancelled by
  unsubscription/discard.
- `rejected`: delivery could not enter because the subscriber stopped accepting,
  or an event copy failed.
- `callback_errors`: callbacks that threw.
- `slow_callbacks`: callback execution durations at or above
  `slow_callback_threshold`; zero disables slow-call counting.

Metrics can change while traffic is active. Read them after shutdown, or after
traffic stops and before resetting the token, when exact accounting is required.
An empty or reset token returns a zero-valued snapshot.

## Reentrancy

Callbacks may use non-blocking `try_publish`. Blocking publication can still
deadlock when all dispatcher workers are blocked by full queues or lossless
mailboxes, so callback-side publication should remain non-blocking, timed, or
cancellable.
