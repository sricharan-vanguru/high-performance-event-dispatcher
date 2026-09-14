# Verification and Corner-Case Coverage

The tests emphasize externally observable contracts rather than private
implementation details.

| Area | Covered cases |
|---|---|
| Queue capacity | Invalid capacities, empty/full, FIFO, tiny-ring wraparound, move-only events |
| Publication | Copy/move, immediate rejection, blocking, timeout, cancellation, close wake-up, event retention on failure |
| Queue lifecycle | Drain, exact discard count, exact destruction, repeated close/discard |
| Topologies | SPSC, MPSC, SPMC, MPMC and multi-subscriber broadcast accounting |
| Subscription | Empty callback, token moves, repeated/concurrent reset, old snapshots, facade lifetime |
| Callback lifetime | Unsubscribe-before-entry, unsubscribe-during-callback, self-unsubscribe, exception unwinding |
| Dispatcher lifecycle | Zero configuration, empty registry, repeated construction/destruction, drain/discard, post-stop rejection |
| Shutdown races | Blocked producers, concurrent callers, first-policy-wins, alternating drain/discard under traffic |
| Reentrancy | Callback-side non-blocking publication and self-shutdown rejection |
| Error handling | Throwing callback, healthy later callback, throwing error handler |
| Accounting | Full, timeout, cancellation, closed, drain, discard, and stable post-shutdown totals |
| Packaging | Isolated install, versioned `find_package`, external compile, link, and execution |
| Lock-free queue | Runtime atomic check, cancelled copy reservation, exact lifetime, long reuse, topology stress, close races, dispatcher policy integration |
| Emplacement and batching | No Event temporary, argument retention, constructor rollback, ordered partial success, per-event metrics, shared batch snapshot |

The transition stress invariant is:

```text
completed callback deliveries + discarded queued events == accepted events
```

It runs with multiple producers and workers under both shutdown policies and is
repeated in release verification.

## Deliberate gaps

- Operating-system thread-creation failure cleanup is RAII-designed but not
  fault-injected because the worker pool has no test-only thread factory.
- General allocator exhaustion is not injected. Throwing event copies are
  tested and preserve queue structure.
- A 64-bit metrics wraparound test is impractical; wrap behavior is documented.
- Dispatcher destruction racing with method calls and concurrent mutation of
  the same subscription token are outside the lifetime contract and are not
  tested as supported behavior.
- Local ThreadSanitizer executables build, but this host intermittently rejects
  runtime initialization with `unexpected memory mapping`. CI runs TSan on a
  clean GitHub runner.
- Multi-hour soak, weak/intrusive ownership,
  affinity, and NUMA cases belong to their corresponding advanced phases.
