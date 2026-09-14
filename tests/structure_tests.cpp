#include "event_dispatcher/event_dispatcher.hpp"

#include <cstddef>
#include <type_traits>

int main() {
    using event_dispatcher::subscription;

    static_assert(!std::is_copy_constructible_v<subscription>);
    static_assert(std::is_nothrow_move_constructible_v<subscription>);
    static_assert(std::is_nothrow_move_assignable_v<subscription>);
    static_assert(std::is_same_v<decltype(event_dispatcher::dispatcher_config{}.queue_capacity),
                                 std::size_t>);

    const auto config = event_dispatcher::dispatcher_config{};
    if (config.queue_capacity == 0U || config.worker_count == 0U) {
        return 1;
    }

    const auto hardware_config =
        event_dispatcher::dispatcher_config::hardware_concurrency_defaults();
    if (hardware_config.worker_count == 0U) {
        return 1;
    }
}
