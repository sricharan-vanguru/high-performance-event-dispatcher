#include "event_dispatcher/event_dispatcher.hpp"

#include <array>
#include <atomic>
#include <span>

int main() {
    std::atomic<int> received{0};
    event_dispatcher::dispatcher<int> dispatcher;

    event_dispatcher::subscription_options options;
    options.delivery = event_dispatcher::delivery_policy::isolated;
    options.guarantee = event_dispatcher::delivery_guarantee::lossless;
    options.mailbox_capacity = 4U;
    auto token = dispatcher.subscribe(
        [&](const int& event) { received.store(event, std::memory_order_relaxed); }, options);
    if (dispatcher.try_emplace(40) != event_dispatcher::queue::queue_status::success) {
        return 1;
    }
    std::array<int, 2> batch{41, 42};
    if (!dispatcher.try_publish_batch(std::span<int>{batch}).complete()) {
        return 1;
    }
    dispatcher.shutdown();
    return received.load(std::memory_order_relaxed) == 42 && token.metrics().delivered == 3U ? 0
                                                                                             : 1;
}
