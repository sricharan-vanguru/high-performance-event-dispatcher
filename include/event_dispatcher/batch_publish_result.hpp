#pragma once

#include "event_dispatcher/queue/queue_status.hpp"

#include <cstddef>

namespace event_dispatcher {

// Batch publication deliberately stops at the first event that cannot be
// accepted. Events in [0, accepted) belong to the dispatcher; the failing event
// and every later event remain owned by the caller.
struct batch_publish_result final {
    std::size_t requested{0U};
    std::size_t accepted{0U};
    queue::queue_status status{queue::queue_status::success};

    [[nodiscard]] bool complete() const noexcept {
        return accepted == requested && status == queue::queue_status::success;
    }
};

} // namespace event_dispatcher
