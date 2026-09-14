#include "event_dispatcher/queue/bounded_mutex_queue.hpp"
#include "event_dispatcher/queue/concurrent_queue.hpp"

#include <atomic>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <future>
#include <memory>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

using event_dispatcher::queue::bounded_mutex_queue;
using event_dispatcher::queue::queue_status;
using namespace std::chrono_literals;

void expect(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error{message};
    }
}

template <typename Future>
decltype(auto) get_ready(Future& future) {
    expect(future.wait_for(2s) == std::future_status::ready, "timed out waiting for test thread");
    return future.get();
}

void test_capacity_and_fifo() {
    bool rejected_zero = false;
    try {
        bounded_mutex_queue<int> invalid{0U};
    } catch (const std::invalid_argument&) {
        rejected_zero = true;
    }
    expect(rejected_zero, "zero capacity must be rejected");

    bounded_mutex_queue<int> queue{2U};
    static_assert(event_dispatcher::queue::concurrent_queue<decltype(queue), int>);

    expect(queue.capacity() == 2U, "capacity mismatch");
    expect(queue.size() == 0U, "new queue must be empty");
    expect(queue.try_pop().status() == queue_status::empty, "empty pop status mismatch");

    expect(queue.try_push(10) == queue_status::success, "first push failed");
    expect(queue.try_push(20) == queue_status::success, "second push failed");
    expect(queue.try_push(30) == queue_status::full, "full queue must reject push");
    expect(queue.size() == 2U, "full queue size mismatch");

    auto first = queue.try_pop();
    auto second = queue.try_pop();
    expect(first && first.value() == 10, "FIFO first value mismatch");
    expect(second && second.value() == 20, "FIFO second value mismatch");
    expect(queue.try_pop().status() == queue_status::empty, "queue must be empty after pops");
}

void test_copy_and_move_only_events() {
    bounded_mutex_queue<std::string> copy_queue{1U};
    const std::string copied{"copy me"};
    expect(copy_queue.try_push(copied) == queue_status::success, "copy push failed");
    expect(copy_queue.try_pop().value() == copied, "copied value mismatch");

    bounded_mutex_queue<std::unique_ptr<int>> move_queue{1U};
    auto first = std::make_unique<int>(7);
    auto retryable = std::make_unique<int>(9);

    expect(move_queue.try_push(std::move(first)) == queue_status::success, "move push failed");
    expect(first == nullptr, "successful push must consume the event");
    expect(move_queue.try_push(std::move(retryable)) == queue_status::full,
           "full move queue status mismatch");
    expect(retryable != nullptr, "failed push must not consume the event");

    auto popped = move_queue.try_pop();
    expect(popped && *popped.value() == 7, "move-only pop mismatch");
}

void test_close_and_drain() {
    bounded_mutex_queue<int> queue{2U};
    expect(queue.try_push(1) == queue_status::success, "push before close failed");
    expect(queue.try_push(2) == queue_status::success, "push before close failed");

    queue.close();
    queue.close();
    expect(queue.closed(), "queue must report closed");

    int rejected = 3;
    expect(queue.try_push(std::move(rejected)) == queue_status::closed,
           "push after close must report closed");
    expect(rejected == 3, "closed push must not consume event");

    expect(queue.try_pop().value() == 1, "first drain value mismatch");
    expect(queue.wait_pop().value() == 2, "second drain value mismatch");
    expect(queue.try_pop().status() == queue_status::closed,
           "drained closed queue must remain closed");
    expect(queue.wait_pop().status() == queue_status::closed,
           "wait on drained queue must report closed");
}

void test_waiting_consumer_wakes_for_event() {
    bounded_mutex_queue<int> queue{1U};
    std::promise<void> entered;
    std::promise<event_dispatcher::queue::pop_result<int>> completed;
    auto entered_future = entered.get_future();
    auto completed_future = completed.get_future();

    std::thread consumer{[&] {
        entered.set_value();
        completed.set_value(queue.wait_pop());
    }};

    get_ready(entered_future);
    expect(queue.try_push(42) == queue_status::success, "push for waiter failed");
    auto result = get_ready(completed_future);
    consumer.join();
    expect(result && result.value() == 42, "waiting consumer received wrong event");
}

void test_waiting_consumer_wakes_for_close_and_stop() {
    {
        bounded_mutex_queue<int> queue{1U};
        std::promise<void> entered;
        std::promise<queue_status> completed;
        auto entered_future = entered.get_future();
        auto completed_future = completed.get_future();

        std::thread consumer{[&] {
            entered.set_value();
            completed.set_value(queue.wait_pop().status());
        }};
        get_ready(entered_future);
        queue.close();
        expect(get_ready(completed_future) == queue_status::closed,
               "close must wake waiting consumer");
        consumer.join();
    }

    {
        bounded_mutex_queue<int> queue{1U};
        std::stop_source stop_source;
        std::promise<void> entered;
        std::promise<queue_status> completed;
        auto entered_future = entered.get_future();
        auto completed_future = completed.get_future();

        std::thread consumer{[&] {
            entered.set_value();
            completed.set_value(queue.wait_pop(stop_source.get_token()).status());
        }};
        get_ready(entered_future);
        stop_source.request_stop();
        expect(get_ready(completed_future) == queue_status::stopped,
               "stop must wake waiting consumer");
        consumer.join();
    }
}

void test_waiting_producer_wakes_for_space_close_and_stop() {
    {
        bounded_mutex_queue<int> queue{1U};
        expect(queue.try_push(1) == queue_status::success, "queue setup failed");

        std::promise<void> entered;
        std::promise<queue_status> completed;
        auto entered_future = entered.get_future();
        auto completed_future = completed.get_future();
        std::thread producer{[&] {
            entered.set_value();
            completed.set_value(queue.wait_push(2));
        }};

        get_ready(entered_future);
        expect(queue.try_pop().value() == 1, "space-producing pop failed");
        expect(get_ready(completed_future) == queue_status::success,
               "pop must wake waiting producer");
        producer.join();
        expect(queue.try_pop().value() == 2, "waiting producer event mismatch");
    }

    {
        bounded_mutex_queue<std::unique_ptr<int>> queue{1U};
        auto first = std::make_unique<int>(1);
        expect(queue.try_push(std::move(first)) == queue_status::success, "queue setup failed");
        auto blocked = std::make_unique<int>(2);

        std::promise<void> entered;
        std::promise<queue_status> completed;
        auto entered_future = entered.get_future();
        auto completed_future = completed.get_future();
        std::thread producer{[&] {
            entered.set_value();
            completed.set_value(queue.wait_push(std::move(blocked)));
        }};

        get_ready(entered_future);
        queue.close();
        expect(get_ready(completed_future) == queue_status::closed,
               "close must wake waiting producer");
        producer.join();
        expect(blocked != nullptr, "closed wait_push must not consume event");
    }

    {
        bounded_mutex_queue<std::unique_ptr<int>> queue{1U};
        auto first = std::make_unique<int>(1);
        expect(queue.try_push(std::move(first)) == queue_status::success, "queue setup failed");
        auto blocked = std::make_unique<int>(2);
        std::stop_source stop_source;

        std::promise<void> entered;
        std::promise<queue_status> completed;
        auto entered_future = entered.get_future();
        auto completed_future = completed.get_future();
        std::thread producer{[&] {
            entered.set_value();
            completed.set_value(queue.wait_push(std::move(blocked), stop_source.get_token()));
        }};

        get_ready(entered_future);
        stop_source.request_stop();
        expect(get_ready(completed_future) == queue_status::stopped,
               "stop must wake waiting producer");
        producer.join();
        expect(blocked != nullptr, "stopped wait_push must not consume event");
    }
}

void test_immediate_progress_wins_over_stop() {
    bounded_mutex_queue<int> queue{1U};
    std::stop_source stop_source;
    stop_source.request_stop();

    expect(queue.wait_push(11, stop_source.get_token()) == queue_status::success,
           "available capacity must permit immediate push");
    auto result = queue.wait_pop(stop_source.get_token());
    expect(result && result.value() == 11, "available event must permit immediate pop");
}

struct throwing_copy_event {
    explicit throwing_copy_event(int event_value) : value(event_value) {}

    throwing_copy_event(const throwing_copy_event& other) {
        if (throw_on_copy.exchange(false, std::memory_order_relaxed)) {
            throw std::runtime_error{"requested copy failure"};
        }
        value = other.value;
    }

    throwing_copy_event& operator=(const throwing_copy_event&) = delete;
    throwing_copy_event(throwing_copy_event&&) noexcept = default;
    throwing_copy_event& operator=(throwing_copy_event&&) = delete;

    inline static std::atomic<bool> throw_on_copy{false};
    int value;
};

void test_throwing_copy_preserves_queue_structure() {
    bounded_mutex_queue<throwing_copy_event> queue{1U};
    const throwing_copy_event event{17};

    throwing_copy_event::throw_on_copy.store(true, std::memory_order_relaxed);
    bool push_threw = false;
    try {
        static_cast<void>(queue.try_push(event));
    } catch (const std::runtime_error&) {
        push_threw = true;
    }
    expect(push_threw, "requested push copy must throw");
    expect(queue.size() == 0U, "failed push must not occupy a slot");

    expect(queue.try_push(event) == queue_status::success,
           "queue must remain usable after failed copy");
    auto recovered = queue.try_pop();
    expect(recovered && recovered.value().value == 17,
           "queue value mismatch after recovered copy");
}

void run_concurrent_accounting(std::size_t producer_count,
                               std::size_t consumer_count,
                               std::size_t events_per_producer) {
    const std::size_t total_events = producer_count * events_per_producer;

    bounded_mutex_queue<std::size_t> queue{64U};
    std::vector<std::atomic<unsigned int>> seen(total_events);
    std::atomic<std::size_t> consumed{0U};
    std::vector<std::thread> consumers;
    consumers.reserve(consumer_count);

    for (std::size_t index = 0; index < consumer_count; ++index) {
        consumers.emplace_back([&] {
            while (true) {
                auto result = queue.wait_pop();
                if (result.status() == queue_status::closed) {
                    return;
                }
                const auto value = result.value();
                expect(value < total_events, "consumer received out-of-range event");
                seen[value].fetch_add(1U, std::memory_order_relaxed);
                consumed.fetch_add(1U, std::memory_order_relaxed);
            }
        });
    }

    std::vector<std::thread> producers;
    producers.reserve(producer_count);
    for (std::size_t producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&, producer] {
            const std::size_t begin = producer * events_per_producer;
            const std::size_t end = begin + events_per_producer;
            for (std::size_t value = begin; value < end; ++value) {
                expect(queue.wait_push(value) == queue_status::success,
                       "producer failed before close");
            }
        });
    }

    for (auto& producer : producers) {
        producer.join();
    }
    queue.close();
    for (auto& consumer : consumers) {
        consumer.join();
    }

    expect(consumed.load(std::memory_order_relaxed) == total_events,
           "concurrent event count mismatch");
    for (const auto& count : seen) {
        expect(count.load(std::memory_order_relaxed) == 1U,
               "concurrent queue lost or duplicated an event");
    }
}

void test_concurrent_topologies() {
    run_concurrent_accounting(4U, 1U, 4'000U); // MPSC
    run_concurrent_accounting(1U, 4U, 4'000U); // SPMC
    run_concurrent_accounting(4U, 4U, 4'000U); // MPMC
}

void test_close_during_concurrent_traffic() {
    constexpr std::size_t rounds = 20U;
    constexpr std::size_t producer_count = 2U;
    constexpr std::size_t consumer_count = 2U;

    for (std::size_t round = 0; round < rounds; ++round) {
        bounded_mutex_queue<std::size_t> queue{8U};
        std::barrier start_line{
            static_cast<std::ptrdiff_t>(producer_count + consumer_count + 1U)};
        std::atomic<std::size_t> accepted{0U};
        std::atomic<std::size_t> consumed{0U};
        std::vector<std::thread> producers;
        std::vector<std::thread> consumers;

        for (std::size_t index = 0; index < consumer_count; ++index) {
            consumers.emplace_back([&] {
                start_line.arrive_and_wait();
                while (true) {
                    auto result = queue.wait_pop();
                    if (result.status() == queue_status::closed) {
                        return;
                    }
                    consumed.fetch_add(1U, std::memory_order_relaxed);
                }
            });
        }

        for (std::size_t index = 0; index < producer_count; ++index) {
            producers.emplace_back([&] {
                start_line.arrive_and_wait();
                std::size_t value = 0U;
                while (queue.wait_push(value) == queue_status::success) {
                    accepted.fetch_add(1U, std::memory_order_relaxed);
                    ++value;
                }
            });
        }

        start_line.arrive_and_wait();
        for (std::size_t spin = 0; spin < 100U; ++spin) {
            std::this_thread::yield();
        }
        queue.close();

        for (auto& producer : producers) {
            producer.join();
        }
        for (auto& consumer : consumers) {
            consumer.join();
        }
        expect(accepted.load(std::memory_order_relaxed) ==
                   consumed.load(std::memory_order_relaxed),
               "close must drain every accepted event exactly once");
    }
}

} // namespace

int main() {
    test_capacity_and_fifo();
    test_copy_and_move_only_events();
    test_close_and_drain();
    test_waiting_consumer_wakes_for_event();
    test_waiting_consumer_wakes_for_close_and_stop();
    test_waiting_producer_wakes_for_space_close_and_stop();
    test_immediate_progress_wins_over_stop();
    test_throwing_copy_preserves_queue_structure();
    test_concurrent_topologies();
    test_close_during_concurrent_traffic();
}
