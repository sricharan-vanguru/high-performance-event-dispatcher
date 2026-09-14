#pragma once

#include <concepts>
#include <cstddef>
#include <utility>

namespace event_dispatcher::queue {

// Queue implementations are policies. The dispatcher depends on this contract,
// not on a particular ring-buffer algorithm.
template <typename Queue, typename Event>
concept concurrent_queue = requires(Queue& queue, Event& destination, Event event) {
    { queue.try_push(std::move(event)) } -> std::same_as<bool>;
    { queue.try_pop(destination) } -> std::same_as<bool>;
    { queue.capacity() } noexcept -> std::same_as<std::size_t>;
};

} // namespace event_dispatcher::queue
