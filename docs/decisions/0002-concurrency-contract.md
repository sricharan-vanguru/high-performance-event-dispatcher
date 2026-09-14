# ADR 0002: Define concurrency semantics before algorithms

Status: accepted

## Context

Terms such as publish, deliver, lock-free, ordered, and safe unsubscribe are
often used ambiguously. An optimized algorithm can appear correct while exposing
different behavior from the reference implementation.

## Decision

The repository maintains one implementation-independent concurrency contract.
Every queue, registry, executor, and reclamation strategy must preserve it.
Algorithm-specific progress claims are attached only to the exact operation and
supported platform for which they have been demonstrated.

Normal synchronous unsubscribe prevents future callback entry and waits for all
in-flight invocations. Self-unsubscribe prevents future entry immediately and
defers final retirement until the current invocation exits.

## Consequences

Implementations can be substituted and benchmarked without silently changing
observable semantics. New algorithms require a short correctness argument and
tests for their linearization, ordering, lifetime, and shutdown behavior. The
public APIs preserve these terms through explicit success, capacity, lifecycle,
cancellation, and timeout outcomes.
