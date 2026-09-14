#include "event_dispatcher/event_dispatcher.hpp"

#include <iostream>

int main() {
    const auto config = event_dispatcher::dispatcher_config::hardware_concurrency_defaults();
    std::cout << "event-dispatcher: workers=" << config.worker_count
              << ", worker-batch=" << config.worker_batch_size
              << ", queue-capacity=" << config.queue_capacity << '\n';
}
