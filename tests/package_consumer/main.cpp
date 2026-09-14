#include "event_dispatcher/event_dispatcher.hpp"

#include <array>
#include <atomic>
#include <span>

int main() {
    std::atomic<int> received{0};
    event_dispatcher::dispatcher<int> dispatcher;
    auto token = dispatcher.subscribe(
        [&](const int& event) { received.store(event, std::memory_order_relaxed); });
    if (dispatcher.try_emplace(40) != event_dispatcher::queue::queue_status::success) {
        return 1;
    }
    std::array<int, 2> batch{41, 42};
    if (!dispatcher.try_publish_batch(std::span<int>{batch}).complete()) {
        return 1;
    }
    dispatcher.shutdown();
    static_cast<void>(token);
    return received.load(std::memory_order_relaxed) == 42 ? 0 : 1;
}
