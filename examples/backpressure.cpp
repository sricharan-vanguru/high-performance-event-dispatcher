#include "event_dispatcher/dispatcher.hpp"

#include <chrono>
#include <iostream>
#include <thread>

int main() {
    using namespace std::chrono_literals;

    auto config = event_dispatcher::dispatcher_config{};
    config.queue_capacity = 1U;
    event_dispatcher::dispatcher<int> dispatcher{config};
    auto token = dispatcher.subscribe([](const int&) {
        std::this_thread::sleep_for(10ms); // Simulate a slow subscriber.
    });

    for (int event = 0; event < 10; ++event) {
        const auto result = dispatcher.publish_for(event, 1ms);
        if (result == event_dispatcher::queue::queue_status::timeout) {
            std::cout << "timed out publishing " << event << '\n';
        }
    }

    dispatcher.shutdown();
    const auto counters = dispatcher.metrics();
    std::cout << "accepted=" << counters.accepted << ", rejected=" << counters.rejected << '\n';
    static_cast<void>(token);
}
