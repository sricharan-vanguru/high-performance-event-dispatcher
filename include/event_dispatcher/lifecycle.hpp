#pragma once

namespace event_dispatcher {

// Created exists to make the construction transition explicit, although a
// successfully returned dispatcher is already running.
enum class lifecycle_state {
    created,
    running,
    drain_stopping,
    discard_stopping,
    stopped,
};

} // namespace event_dispatcher
