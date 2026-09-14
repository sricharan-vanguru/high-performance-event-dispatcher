#include "event_dispatcher/event_dispatcher.hpp"

#include <atomic>

int main() {
    std::atomic<int> received{0};
    event_dispatcher::dispatcher<int> dispatcher;
    auto token = dispatcher.subscribe(
        [&](const int& event) { received.store(event, std::memory_order_relaxed); });
    if (dispatcher.publish(42) != event_dispatcher::queue::queue_status::success) {
        return 1;
    }
    dispatcher.shutdown();
    static_cast<void>(token);
    return received.load(std::memory_order_relaxed) == 42 ? 0 : 1;
}
