#include "event_dispatcher/event_dispatcher.hpp"

#include <string_view>
#include <type_traits>

int main() {
    using event_dispatcher::subscription;

    static_assert(!std::is_copy_constructible_v<subscription>);
    static_assert(std::is_nothrow_move_constructible_v<subscription>);

    const auto config = event_dispatcher::dispatcher_config{};
    if (config.queue_capacity == 0U || config.worker_count == 0U) {
        return 1;
    }
    if (event_dispatcher::version() != std::string_view{"0.1.0"}) {
        return 1;
    }
}
