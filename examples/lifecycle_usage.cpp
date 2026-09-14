#include "event_dispatcher/dispatcher.hpp"

#include <chrono>
#include <iostream>

int main() {
    using namespace std::chrono_literals;

    auto config = event_dispatcher::dispatcher_config{};
    config.queue_capacity = 64U;
    config.worker_count = 1U;
    config.shutdown = event_dispatcher::shutdown_policy::drain;

    event_dispatcher::dispatcher<int> dispatcher{config};
    auto subscription =
        dispatcher.subscribe([](const int& event) { std::cout << "received " << event << '\n'; });

    // The three publication APIs expose distinct overload strategies:
    // try_publish rejects immediately, publish blocks interruptibly, and
    // publish_for bounds how long the producer waits for queue capacity.
    static_cast<void>(dispatcher.try_publish(1));
    static_cast<void>(dispatcher.publish(2));
    static_cast<void>(dispatcher.publish_for(3, 10ms));

    dispatcher.shutdown();
    const auto metrics = dispatcher.metrics();
    std::cout << "accepted=" << metrics.accepted << ", rejected=" << metrics.rejected
              << ", dropped=" << metrics.dropped << '\n';
    static_cast<void>(subscription);
}
