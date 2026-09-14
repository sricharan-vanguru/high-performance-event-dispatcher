#include "event_dispatcher/event_dispatcher.hpp"

#include <iostream>

int main() {
    const auto config = event_dispatcher::dispatcher_config::hardware_concurrency_defaults();
    std::cout << "event-dispatcher " << event_dispatcher::version()
              << ": workers=" << config.worker_count
              << ", queue-capacity=" << config.queue_capacity << '\n';
}
