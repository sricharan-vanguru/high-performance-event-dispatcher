# ADR 0001: Policy-based internal boundaries

Status: accepted

## Context

Queue algorithms, callback execution, subscriber registries, and reclamation
have different correctness and performance trade-offs. A monolithic dispatcher
would make those trade-offs difficult to test and compare.

## Decision

Keep a small public API and divide internals into queue, registry, execution,
lifetime, backpressure, and error policies. Begin with a simple reference
policy before introducing the lock-free queue.

## Consequences

Algorithms can be tested and benchmarked independently. Template boundaries
must be controlled to avoid excessive public implementation detail and compile
time, so only performance-relevant policies will use static polymorphism.
