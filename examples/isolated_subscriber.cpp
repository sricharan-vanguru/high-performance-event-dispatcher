#include "event_dispatcher/event_dispatcher.hpp"

#include <chrono>
#include <iostream>
#include <thread>

int main() {
    event_dispatcher::dispatcher<int> dispatcher;

    event_dispatcher::subscription_options options;
    options.delivery = event_dispatcher::delivery_policy::isolated;
    options.guarantee = event_dispatcher::delivery_guarantee::best_effort;
    options.mailbox_capacity = 16U;
    options.slow_callback_threshold = std::chrono::milliseconds{5};

    auto slow_subscriber = dispatcher.subscribe(
        [](const int& event) {
            std::this_thread::sleep_for(std::chrono::milliseconds{10});
            std::cout << "isolated subscriber received " << event << '\n';
        },
        options);

    for (int event = 1; event <= 5; ++event) {
        static_cast<void>(dispatcher.publish(event));
    }
    dispatcher.shutdown();

    const auto metrics = slow_subscriber.metrics();
    std::cout << "delivered=" << metrics.delivered << ", dropped=" << metrics.dropped
              << ", slow=" << metrics.slow_callbacks << '\n';
}
