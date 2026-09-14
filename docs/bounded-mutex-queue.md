# Bounded Mutex Queue

`bounded_mutex_queue<Event>` is the correctness-first queue and performance
baseline. It supports multiple producers and consumers, bounded memory, explicit
backpressure, cooperative cancellation, and deterministic close behavior.

## Why implement the mutex queue first?

A lock-free queue is meaningful only when compared with a simpler implementation
that has identical externally visible behavior. This queue gives later algorithms
a reference for FIFO behavior, capacity, cancellation, close, and event lifetime.

## States

```text
                         close()
         +----------------------------------------+
         |                                        v
  +------+------+     pop final item      +------+------+
  |    open     | -----------------------> | closed and |
  | 0..capacity |                          |    empty   |
  +------+------+                          +------+------+
         |                                        |
         | close() with queued items              | push -> closed
         v                                        | pop  -> closed
  +------+------+                                 |
  | closed with | -- pop events in FIFO order ----+
  |    items    |
  +-------------+
```

`close()` never discards an accepted event. It prevents future pushes, wakes all
waiters, and lets consumers drain existing events. `close_and_discard()` closes
the same acceptance boundary but atomically destroys queued events and reports
their count. Events already dequeued remain worker-owned and complete normally.

## Operation results

| Status | Meaning |
|---|---|
| `success` | Event ownership transferred successfully |
| `full` | Non-blocking push found no capacity |
| `empty` | Non-blocking pop found no event while queue remains open |
| `closed` | The requested operation can never succeed because the queue closed |
| `stopped` | A stop request cancelled an operation that was waiting |
| `timeout` | A timed push deadline expired while the queue stayed full |

Failed pushes do not move from the caller's event. This supports retry logic:

```cpp
auto event = make_event();
if (queue.try_push(std::move(event)) == queue_status::full) {
    // event still owns its original value here
}
```

## Push flow

```text
lock
 |
 +-- closed? ---- yes --> return closed without moving event
 |
 +-- full? ------ yes --> try_push: return full without moving event
 |                         wait_push: wait for space/close/stop
 |
 +-- construct event in tail slot
 +-- advance tail and size
unlock
notify one consumer
```

The notification happens after unlocking. An awakened consumer can acquire the
mutex immediately instead of waking only to block behind the producer.

## Pop flow

```text
lock
 |
 +-- event available? -- yes --> move event from head slot
 |                               destroy slot value
 |                               advance head and size
 |                               unlock; notify producer
 |
 +-- open and empty? ---------> try_pop: empty
 |                              wait_pop: wait
 |
 `-- closed and empty? -------> closed permanently
```

## Stop versus immediate progress

A stop token cancels a blocking operation only while its queue condition is not
satisfied. If space or an event is already available, that immediate operation
may succeed even when a stop request races with it. This avoids discarding useful
work after the queue has already granted progress.

## Event lifetime and exceptions

The constructor allocates `vector<optional<Event>>` once. `optional` creates an
explicit lifetime only in occupied slots, so `Event` need not be default
constructible. Successful push constructs one slot; successful pop moves and
then destroys that slot.

If copy construction during push throws, head, tail, and size are unchanged.
`Event` must provide a non-throwing move constructor. This explicit constraint
ensures dequeue and result transfer cannot lose an accepted event after the ring
slot has been released. It is also the normal event-type design for a
performance-critical queue.

## Thread-safety boundary

All member functions are safe to call concurrently except destruction. `size()`
and `closed()` return synchronized observations, but another thread may change
the queue immediately afterward; they must not be used for check-then-act logic.
The owner must stop and join all users before destroying the queue.

## Complexity and progress

- Push/pop: constant-time ring operations, excluding event construction cost.
- Memory: `O(capacity)` allocated during construction.
- Non-blocking methods: return without intentionally waiting, but take a mutex.
- Waiting methods: blocking and cooperatively cancellable.
- Progress classification: blocking, not lock-free.
