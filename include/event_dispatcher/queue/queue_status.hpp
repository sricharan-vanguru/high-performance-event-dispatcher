#pragma once

namespace event_dispatcher::queue {

// Every queue operation reports why it completed. A Boolean cannot distinguish
// temporary backpressure (full/empty) from a permanent lifecycle transition
// (closed), cooperative cancellation (stopped), or deadline expiry (timeout).
enum class queue_status {
    success,
    full,
    empty,
    closed,
    stopped,
    timeout,
};

} // namespace event_dispatcher::queue
