# Bounded Lock-Free MPMC Queue

`bounded_lock_free_queue<Event>` is the optional optimized queue policy. It
adapts Ruslan Nikolaev's Scalable Circular Queue (SCQ), a bounded, linearizable,
lock-free MPMC FIFO published at DISC 2019. The generic Event wrapper preserves
the project's backpressure, cancellation, timeout, close, and lifetime contract.

Primary references:

- [DISC 2019 paper](https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.DISC.2019.28)
- [Author's MIT-licensed reference implementation](https://github.com/rusnikola/lfqueue)

The adapted index-ring file retains the original copyright and MIT license.

## Why SCQ instead of the common sequence-cell ring?

Dmitry Vyukov's bounded MPMC sequence-cell algorithm is compact and fast, but
its author explicitly classifies it as not lock-free in the formal sense. A
producer can reserve a FIFO position and stall before publishing it, preventing
consumers from reaching completed later positions. That does not meet the strict
progress terminology established by this project.

SCQ uses fetch-and-add positions, cycle-encoded entries, a threshold, catch-up,
and helping transitions to remain linearizable and lock-free without external
memory reclamation. That stricter progress proof matches this project's defined
terminology.

## Generic event layout

SCQ stores bounded integer indices. The wrapper owns two SCQ rings plus stable,
preallocated Event storage:

```text
available-indices SCQ --> [stable Event slots] --> published-indices SCQ
        producer                                      consumer
```

Initially, every slot index is in `available_indices` and
`published_indices` is empty.

### Push

```text
dequeue one available index
        |
construct Event in that stable slot
        |
enqueue index into published_indices  <-- successful push linearization
```

If no available index exists, `try_push` returns `full` without moving from the
caller's event. If copy construction throws, the index is returned to the free
ring and no event becomes visible.

### Pop

```text
dequeue one published index           <-- pop ownership linearization
        |
move Event out and destroy slot value
        |
enqueue index back into available_indices
```

`Event` must be nothrow move-constructible and nothrow destructible. These
requirements ensure a consumer cannot strand an accepted Event after claiming
its published index.

## Capacity and allocation

Capacity must be a power of two and at least two. The constructor allocates:

- `capacity` stable Event slots;
- two SCQ rings, each containing `2 * capacity` atomic entries.

Successful `try_push` and `try_pop` perform no dynamic allocation. Event objects
have explicit lifetimes inside aligned raw storage and are destroyed exactly
once by pop, discard, or externally coordinated queue destruction.

## Memory ordering

SCQ's head and tail tickets use atomic fetch-and-add. Entry transitions use
acquire/release compare-exchange and fetch-or operations. These transitions
both select ownership and publish index-ring state across threads. The original
paper supplies the complete linearizability and lock-freedom argument; the
adaptation intentionally preserves its state encoding and catch-up behavior.

The Event itself is written before its index is enqueued into the published
SCQ. A consumer obtains that index through acquire/release SCQ operations before
reading the Event. After moving and destroying the Event, the consumer returns
the index through the available SCQ before a future producer reuses the slot.

## Close coordination

An atomic producer gate combines a closed bit with an active-operation count.
`close()` sets the bit, rejects later producers, and waits for producers that
already entered to publish their Event or return their slot. Only then does it
mark production quiescent and allow an empty consumer to report `closed`.

`close_and_discard()` also closes the consumer gate, waits for active consumers,
and destroys every remaining published Event exactly once. A mutex serializes
only these cold lifecycle transitions; it is not used by `try_push` or
`try_pop`.

## Waiting adapters

`wait_push` and `wait_pop` retry the lock-free core and park on C++20 atomic
epochs. Stop callbacks change the relevant epoch so cancellation wakes parked
threads. Timed publication uses a condition variable because C++20 atomic wait
has no deadline overload; bounded rechecks prevent a missed notification from
delaying stop or timeout observation indefinitely.

Waiting APIs and lifecycle transitions are blocking operations. They are not
included in the lock-free claim.

## Exact progress claim

`data_path_is_lock_free()` checks every atomic type used by the queue data path
on the running target. When it returns true:

- the SCQ index operations underlying `try_push` and `try_pop` are lock-free;
- the wrapper performs no mutex acquisition or dynamic allocation on a
  successful non-blocking data-path operation;
- user-defined Event construction and destruction are outside the progress
  claim;
- an individual operation may still starve while system-wide progress occurs;
- waiting, shutdown, subscriptions, callbacks, and the complete dispatcher are
  not claimed to be lock-free.

Lock freedom does not imply wait freedom, faster performance under every
topology, bounded latency, or real-time behavior.
