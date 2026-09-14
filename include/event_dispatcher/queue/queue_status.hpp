#pragma once

namespace event_dispatcher::queue {

// Every queue operation reports why it completed. A Boolean cannot distinguish
// temporary backpressure (full/empty) from a permanent lifecycle transition
// (closed) or cooperative cancellation (stopped).
enum class queue_status {
    success,
    full,
    empty,
    closed,
    stopped,
};

} // namespace event_dispatcher::queue
