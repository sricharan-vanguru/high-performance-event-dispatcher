#include "event_dispatcher/dispatcher.hpp"

#include <exception>
#include <iostream>

int main() {
    auto config = event_dispatcher::dispatcher_config{};
    config.queue_capacity = 256U;
    config.worker_count = 2U;

    event_dispatcher::dispatcher<int> dispatcher{
        config, [](std::exception_ptr error) {
            try {
                if (error) {
                    std::rethrow_exception(error);
                }
            } catch (const std::exception& exception) {
                std::cerr << "subscriber failed: " << exception.what() << '\n';
            }
        }};

    auto first = dispatcher.subscribe([](const int& event) {
        std::cout << "first subscriber received " << event << '\n';
    });
    auto second = dispatcher.subscribe([](const int& event) {
        std::cout << "second subscriber received " << event << '\n';
    });

    // Blocking publish applies backpressure when the bounded queue is full.
    if (dispatcher.publish(42) != event_dispatcher::queue::queue_status::success) {
        return 1;
    }

    // Drain while subscription tokens are still alive. After shutdown returns,
    // every accepted event has completed its subscriber snapshot.
    dispatcher.shutdown();
    static_cast<void>(first);
    static_cast<void>(second);
}
