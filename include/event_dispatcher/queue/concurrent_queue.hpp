#pragma once

#include "event_dispatcher/queue/pop_result.hpp"
#include "event_dispatcher/queue/queue_status.hpp"

#include <concepts>
#include <cstddef>
#include <stop_token>
#include <utility>

namespace event_dispatcher::queue {

// Queue implementations are policies. The dispatcher depends on this contract,
// not on a particular ring-buffer algorithm.
template <typename Queue, typename Event>
concept concurrent_queue = requires(Queue& queue, Event event, std::stop_token stop) {
    { queue.try_push(std::move(event)) } -> std::same_as<queue_status>;
    { queue.wait_push(std::move(event), stop) } -> std::same_as<queue_status>;
    { queue.try_pop() } -> std::same_as<pop_result<Event>>;
    { queue.wait_pop(stop) } -> std::same_as<pop_result<Event>>;
    { queue.close() } -> std::same_as<void>;
    { queue.capacity() } noexcept -> std::same_as<std::size_t>;
    { queue.size() } -> std::same_as<std::size_t>;
    { queue.closed() } -> std::same_as<bool>;
};

} // namespace event_dispatcher::queue
