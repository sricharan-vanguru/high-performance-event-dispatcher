# Worker Pool and Reference Dispatcher

The reference dispatcher connects the bounded queue and snapshot registry into
a complete asynchronous delivery path. It defines the correctness contract for
alternative queue and lifetime policies.

## Event flow

```text
producer threads                 worker threads
---------------                  --------------
publish(event)
      |
      v
+-----------------+   dequeue   +----------------------+
| bounded MPMC    | ----------> | acquire one immutable|
| event queue     |             | subscriber snapshot  |
+-----------------+             +----------+-----------+
                                           |
                                           v
                                 concurrent callback(event)
                                 serialized callback(event)
                                           |
                                           v
                                 isolated mailbox -> executor
```

Exactly one worker dequeues each accepted event. A worker claims up to its
configured batch size, then acquires one subscriber snapshot and broadcasts
every claimed event to every active subscriber in that snapshot. Subscription
changes made after snapshot acquisition apply to a later batch; synchronous
unsubscription can still make entry from an old snapshot fail safely. The
default batch size is one, preserving the original per-event snapshot behavior.

## Ownership and lifetime

Workers capture a `shared_ptr` to one internal state containing the queue,
registry, and error handler. They never capture the facade's raw `this`
pointer. Construction therefore follows this order:

```text
construct queue + registry + error handler
                    |
                    v
            start jthread workers
```

The dispatcher must not be destroyed from inside one of its own callbacks. A
callback calling `shutdown()` receives `std::logic_error`, because joining the
current worker would deadlock. Normal destruction closes the queue, drains all
accepted events, and joins every worker.

Subscription tokens should remain alive until draining completes when their
callbacks are expected to receive already queued events. Destroying a token is
a real synchronous unsubscribe, so that callback is intentionally skipped even
if a worker later encounters an older snapshot.

## Publication and shutdown

- `try_publish` never waits and returns `success`, `full`, or `closed`.
- `publish` waits for capacity and can be interrupted with `std::stop_token`.
- `shutdown` is idempotent and supports drain and discard policies.
- Queue close is the acceptance boundary: earlier accepted events drain; later
  publication returns `closed`.
- Concurrent calls to `shutdown` are serialized.

See [shutdown and backpressure](shutdown-backpressure.md) for lifecycle states,
timeout publication, discard semantics, and accounting.

## Ordering and concurrency

The bounded queue removes events in FIFO order, but multiple workers may run
callbacks for different events concurrently. Consequently, callback completion
order is not globally FIFO. With one worker, broadcasts occur serially in queue
dequeue order. Within one broadcast, subscribers are visited in snapshot order.

The default concurrent policy can execute the same subscriber concurrently for
different events when more than one worker exists. Serialized delivery prevents
overlap but does not impose dequeue order on workers racing for its mutex.
Isolated delivery accepts events into one bounded per-subscriber mailbox and
executes them FIFO on a dedicated thread. See
[subscriber delivery policies](delivery-policies.md) for overload and shutdown
semantics.

## Callback failures

Every callback invocation has its own exception boundary. A thrown exception is
sent to the configured error handler, and the worker continues with later
subscribers and events. If the error handler itself throws, that secondary
exception is swallowed so error reporting cannot kill a worker.

The default empty error handler ignores callback failures. Applications that
need logging or metrics should provide a non-throwing handler.
