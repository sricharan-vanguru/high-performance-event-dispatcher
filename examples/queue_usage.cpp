#include "event_dispatcher/queue/bounded_mutex_queue.hpp"

#include <iostream>
#include <string>

int main() {
    using event_dispatcher::queue::bounded_mutex_queue;
    using event_dispatcher::queue::queue_status;

    bounded_mutex_queue<std::string> queue{2U};

    // try_push never waits. This makes backpressure visible to the producer
    // instead of allowing memory to grow without a bound.
    if (queue.try_push("connected") != queue_status::success ||
        queue.try_push("data-ready") != queue_status::success) {
        std::cerr << "unexpected publication failure\n";
        return 1;
    }

    std::string retryable{"retry-later"};
    if (queue.try_push(std::move(retryable)) == queue_status::full) {
        // A rejected push does not move from the event, so a caller may retry
        // it later or route it through a different backpressure strategy.
        std::cout << "queue full; retained event: " << retryable << '\n';
    }

    queue.close();

    // close() prevents new pushes but preserves accepted events. Consumers
    // drain them and finally receive queue_status::closed.
    while (true) {
        auto result = queue.wait_pop();
        if (result.status() == queue_status::closed) {
            break;
        }
        std::cout << "received: " << result.value() << '\n';
    }
}
